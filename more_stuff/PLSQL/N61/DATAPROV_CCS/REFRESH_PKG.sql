create or replace PACKAGE refresh_pkg IS
/*
|| Name : refresh_pkg
|| Type : Package
|| Database : chordo and ccs021n
|| Schema : dataprov
|| Author : Marcos Gibram Fonseca (for original procedure parallel_daily_load)
|| Date : 09-Nov-2016
|| Purpose : Called from cron, user sitescope, server unora0za, every morning 5am or 6am - sitescope@unora0za:/home/sitescope/ptt/scripts/dataprov_cron_parallel.bash <dbname>
||    Also run (with i_master_run => TRUE ) post environment refresh from sitescope@unora0za:/home/sitescope/ptt/scripts/post_env_refresh.bash
|| Usage : 'exec dataprov.refresh_pkg.main'
||    Optional parameters:
||       1) i_threads = number of simultaneous pool refresh jobs running at any one time. Default is 5.
||       2) i_master_run = TRUE or FALSE. TRUE = master_run is typically only done once after each environment refresh from production. Default is FALSE = daily_run.
|| Depends on : All driven by table dataprov.run_job_parallel_control. Spawns dbms_scheduler jobs.
|| Documentation : https://confluence.bskyb.com/display/nonfuntst/Parallel+Data+Provision+Load
|| Change History:
||    23-JAN-2024  Stuart Mason Adpated this package from N01/N02.
*/
   PROCEDURE p_slow ;  -- runs in separate cron.
   PROCEDURE main ( i_master_run IN BOOLEAN DEFAULT FALSE ) ;
   PROCEDURE p_finish ;
   FUNCTION dependencies_done (i_deps IN dataprov.run_job_parallel_control.job_dependence%TYPE) RETURN varchar2;
   FUNCTION dependencies_done (i_deps IN dataprov.run_job_parallel_control.job_dependence%TYPE, i_proc_date IN DATE) RETURN varchar2;
END refresh_pkg ;
/

create or replace PACKAGE BODY refresh_pkg IS  

g_parent_run_id NUMBER := dataprov.dp_test_refresh_seq.NEXTVAL ;
g_proc_date DATE := SYSDATE ;

PROCEDURE p_runJob ( i_jobName IN VARCHAR2 , i_pack_name IN VARCHAR2 ) IS
BEGIN
   dbms_scheduler.create_job (
        job_name => 'P' || i_jobName
      , job_type => 'STORED_PROCEDURE'
      , job_action => i_pack_name || '.' || i_jobName
      , enabled => TRUE
      ) ;
   UPDATE dataprov.run_job_parallel_control SET start_time = SYSDATE , paused = NULL WHERE job_name = i_jobName ;
END p_runJob ;

PROCEDURE p_debug ( i_message IN VARCHAR2 ) IS
/*
|| p_debug = private prodedure, parameters:
||   1) i_message = text message to output. No default.
|| Depends on table debug. DDL for that is:
||   CREATE TABLE dataprov.debug ( message VARCHAR2(4000) , logTime DATE DEFAULT SYSDATE ) ;
*/
   --PRAGMA AUTONOMOUS_TRANSACTION ;
BEGIN
   -- INSERT INTO dataprov.debug ( message , logTime ) VALUES ( SUBSTR ( i_message , 1 , 4000 ) , SYSDATE ) ;
   -- COMMIT ;
   dbms_output.put_line ( TO_CHAR ( SYSDATE , 'HH24:MI:SS' ) || ' ' || SUBSTR ( i_message , 1 , 32000 ) ) ;
END p_debug ;

PROCEDURE p_start IS
/*
|| p_start = Private prodedure, no parameters.
|| Runs initial preliminary steps.
*/
BEGIN
   INSERT INTO dataprov.runningFocus t ( t.startDate , t.running ) VALUES ( g_proc_date , 'main' ) ;
   COMMIT ;
   logger.debug ( 'p_start completed' ) ;
END p_start ;

PROCEDURE p_parallel_daily_load ( i_stat_dyn IN VARCHAR2 DEFAULT 'S' , i_threads IN NUMBER DEFAULT 6 ) IS
/*
|| p_parallel_daily_load = private prodedure, parameters:
||   1) i_stat_dyn = one of 'S' (static pools in daily refresh), 'M' (static pools in one-off master refresh only).
||   2) i_threads = number of simultaneous pool refresh jobs running at any one time.
|| Depends on : All driven by table dataprov.run_job_parallel_control. Spawns dbms_scheduler jobs.
*/
   l_load_type VARCHAR2(30) := 'PARALLEL' ;
   l_remain NUMBER ;
   l_job_run NUMBER ;
   l_run_id NUMBER ;
   l_sql VARCHAR2(4000) ;
BEGIN
   logger.debug ( 'p_parallel_daily_load Starting' ) ;
   SELECT COUNT(*) INTO l_remain FROM dataprov.run_job_parallel_control WHERE start_time IS NULL AND stat_dyn = i_stat_dyn ;
   WHILE l_remain > 0
   LOOP
      FOR i IN (
         SELECT a.job_name
              , a.package_name AS pack_name
           FROM dataprov.run_job_parallel_control a
          WHERE a.start_time IS NULL
            AND a.stat_dyn = i_stat_dyn
            AND (
                    a.job_dependence IS NULL
                 OR 'P' || a.job_dependence IN (
                       SELECT s.job_name
                         FROM user_scheduler_job_run_details s
                        WHERE s.actual_start_date >= g_proc_date
                       )
                 )
           ORDER BY a.priority
         )
      LOOP
         dbms_lock.sleep ( seconds => 1 ) ;
         SELECT COUNT(*) INTO l_job_run
           FROM (
                 SELECT 'P' || a.job_name FROM dataprov.run_job_parallel_control a WHERE a.stat_dyn = i_stat_dyn AND a.start_time IS NOT NULL
                  MINUS 
                 SELECT s.job_name FROM user_scheduler_job_run_details s WHERE s.log_date > g_proc_date
                )
         ;
         IF l_job_run >= 1
         THEN
            WHILE l_job_run = i_threads  -- define max concurrent processes
            LOOP
               dbms_lock.sleep ( seconds => 5 ) ;
               SELECT COUNT(*) INTO l_job_run
                 FROM (
                       SELECT 'P' || a.job_name FROM dataprov.run_job_parallel_control a WHERE a.stat_dyn = i_stat_dyn AND a.start_time IS NOT NULL
                        MINUS 
                       SELECT s.job_name FROM user_scheduler_job_run_details s WHERE s.log_date > g_proc_date
                      )
               ;
            END LOOP  ;
         END IF  ;
         p_runJob ( i_jobName => i.job_name , i_pack_name => i.pack_name ) ;
         SELECT COUNT(*) INTO l_remain
           FROM dataprov.run_job_parallel_control
          WHERE start_time is NULL
            AND stat_dyn = i_stat_dyn
         ;
         COMMIT ;
      END LOOP  ;
      dbms_lock.sleep ( seconds => 1 ) ;  -- 25-May-2017 Andrew Fraser to reduce executions of d3swg6x918483 (for i in sql above)
   END LOOP  ;
   -- Verify if all jobs have finished
   logger.debug('Verifiying all jobs have completed');
   SELECT COUNT(*) INTO l_job_run
     FROM user_scheduler_running_jobs r
    WHERE ROWNUM < 2
      AND r.job_name LIKE 'P%'
   ;
   WHILE l_job_run > 0
   LOOP
      dbms_lock.sleep ( seconds => 10 ) ;
       SELECT COUNT(*) INTO l_job_run
         FROM user_scheduler_running_jobs r
        WHERE ROWNUM < 2
          AND r.job_name like 'P%'
       ;
   END LOOP ;
   logger.debug('All jobs completed!');
   UPDATE dataprov.run_job_parallel_control a
      SET a.end_time = (
          SELECT MAX ( b.log_date )
            FROM user_scheduler_job_run_details b
           WHERE 'P' || a.job_name = b.job_name
             AND b.log_date >= g_proc_date
          )
   ;
   COMMIT ;
   logger.debug ( 'p_parallel_daily_load Completed' ) ;
END p_parallel_daily_load ;

PROCEDURE p_finish IS
BEGIN
   logger.debug ( 'dbms_stats completed' ) ;

   DELETE FROM dataprov.runningFocus t WHERE t.startDate = g_proc_date AND t.running = 'main' ;
   logger.debug ( 'p_finish completed' ) ;
END p_finish ;

PROCEDURE p_slow IS
-- 12-Jan-2018 Andrew Fraser moved to new cron job because slow.
BEGIN
   INSERT INTO dataprov.runningFocus t ( t.startDate , t.running ) VALUES ( g_proc_date , 'p_slow' ) ;
   COMMIT ;
/*  Commented out for N61. This procedure will not be called.

*/
END p_slow ;

PROCEDURE main ( i_master_run IN BOOLEAN DEFAULT FALSE ) IS
/*
|| main = Public prodedure, parameters
||   1) i_threads = number of simultaneous pool refresh jobs running at any one time.
||   2) i_master_run = TRUE or FALSE. TRUE = master_run is typically only done once after each environment refresh from production. Default is FALSE = daily_run.
*/
  l_failed number := 0 ;
  l_stat_fail number := 0 ;
BEGIN
   p_start ;
   IF i_master_run  -- non-default option, typically only done once after each environment refresh from production.
   THEN
      UPDATE dataprov.run_job_parallel_control t SET t.end_time = NULL , t.start_time = NULL
       WHERE t.stat_dyn = 'M'
      ;
      COMMIT ;
      p_parallel_daily_load ( i_stat_dyn => 'M' ) ;  -- one-off 'master-only' pools

--     The following is not done in N61
--      dataprov.data_postcode.populate_dprov_postcode_mdu ;  -- in separate package, only needs done after refresh from production.
--      dataprov.retail_agent_creation.addRetailUsers ;

      logger.debug ( 'master_run completed' ) ;
   ELSE
      UPDATE dataprov.run_job_parallel_control t SET t.end_time = NULL , t.start_time = NULL
       WHERE t.stat_dyn = 'S'
      ;
      COMMIT ;
      p_parallel_daily_load ;
      logger.debug ( 'Static p_parallel_daily_load completed' ) ;
      -- Added 04/22/2019 by Alex Hyslop to automatically re-run failed jobs
      logger.debug ( 'Checking for Failed jobs and re-run' ) ;
      SELECT COUNT ( DISTINCT job_name ) INTO l_failed
        FROM user_scheduler_job_log 
       WHERE status IN ( 'FAILED' , 'STOPPED' )
         AND log_date > g_proc_date --(select min(start_time) from run_job_parallel_control)
      ;
      IF l_failed > 0
      THEN
         logger.debug('Failed jobs found');
         UPDATE run_job_parallel_control
            SET end_time = NULL , start_time = NULL
          WHERE job_name IN (
                SELECT job_name
                  FROM run_job_parallel_control 
                 WHERE stat_dyn = 'S'
                 START WITH job_name IN (
                       SELECT DISTINCT SUBSTR ( job_name , 2 , 100 )
                         FROM user_scheduler_job_log 
                        WHERE status IN ( 'FAILED' , 'STOPPED' )  
                          AND log_date > g_proc_date  --( select min(start_time) from run_job_parallel_control ) )
                       )
               CONNECT BY PRIOR job_name = job_dependence
                )
        ;
        COMMIT ;
        -- get count of 'S' pools need rebuilt
        SELECT COUNT(*) INTO l_stat_fail
          FROM run_job_parallel_control
         WHERE start_time IS NULL
           AND end_time IS NULL
           AND stat_dyn = 'S'
        ;
        IF l_stat_fail > 0
        THEN
           p_parallel_daily_load ;
        END IF ;
      END IF ;
      logger.debug ( 'Completed failed job re-run' ) ;
   END IF ;
   p_finish ;
END main ;


FUNCTION dependencies_done (i_deps IN dataprov.run_job_parallel_control.job_dependence%TYPE) RETURN varchar2

as
l_list varchar2(200) ;
l_query varchar2(400);
l_query2 varchar2(400);
l_counter number;
l_dependency_count number;

TYPE job_t IS TABLE OF user_scheduler_job_run_details.job_name%type  index by pls_integer;
l_job job_t;

begin
l_list := i_deps;
if l_list is null then
 return 'TRUE';
end if;

l_dependency_count := coalesce(length(l_list) - length(replace(l_list,',',null))+1, length(l_list), 0);
l_query := q'#select count(distinct job_name) cnt from user_scheduler_job_run_details j where actual_start_date >= :l_date and status = 'SUCCEEDED' and job_name in  ('P#'||replace(l_list, ',', ''',''P')||q'#')#';
execute immediate (l_query) into l_counter using g_proc_date;
if l_counter = l_dependency_count then
 return 'TRUE';
else 
 return 'FALSE';
end if;


end dependencies_done;

FUNCTION dependencies_done (i_deps IN dataprov.run_job_parallel_control.job_dependence%TYPE, i_proc_date IN DATE ) RETURN varchar2

as
l_list varchar2(200) ;
l_query varchar2(400);
l_query2 varchar2(400);
l_counter number;
l_dependency_count number;
l_proc_date date;

TYPE job_t IS TABLE OF user_scheduler_job_run_details.job_name%type  index by pls_integer;
l_job job_t;

begin
l_proc_date := i_proc_date;
l_list := i_deps;
if l_list is null then
 return 'TRUE';
end if;

l_dependency_count := coalesce(length(l_list) - length(replace(l_list,',',null))+1, length(l_list), 0);
l_query := q'#select count(distinct job_name) cnt from user_scheduler_job_run_details j where actual_start_date >= :l_date and status = 'SUCCEEDED' and job_name in  ('P#'||replace(l_list, ',', ''',''P')||q'#')#';
execute immediate (l_query) into l_counter using l_proc_date;
if l_counter = l_dependency_count then
 return 'TRUE';
else 
 return 'FALSE';
end if;


end dependencies_done;


END refresh_pkg ;
/