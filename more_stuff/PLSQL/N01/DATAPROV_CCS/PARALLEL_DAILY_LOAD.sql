--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure PARALLEL_DAILY_LOAD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."PARALLEL_DAILY_LOAD" (p_thread_number in number) is 
/*
-- Create by MGI18 
--
-- Last change 04/08/2016
--
--
--
--
*/

  V_PROC_DATE      DATE;
  V_START_DATE     DATE;
  V_END_DATE       DATE;
  V_REMAIN         NUMBER;
  V_JOB_RUN        NUMBER;
  V_SQL            VARCHAR2(4000);
  V_RUN_ID         NUMBER;
  V_PARENT_RUN_ID  NUMBER;
  V_LOAD_TYPE      VARCHAR2(30);
  V_TEST_NAME      VARCHAR2(50);
  V_TEST_SCRIPT_ID number;
  
begin
-- set constants
  V_PROC_DATE         := sysdate;
  V_LOAD_TYPE         := 'PARALLEL';
  V_TEST_NAME         := 'PARALLEL LOAD';
  
  SELECT DATAPROV.DP_TEST_REFRESH_SEQ.NEXTVAL INTO V_PARENT_RUN_ID FROM DUAL;
  V_RUN_ID:=V_PARENT_RUN_ID;
  
-- Prepare control table to execute parallel
    update DATAPROV.RUN_JOB_PARALLEL_CONTROL set END_TIME = null, start_time = null;
    commit;
  
-- FRAMEWORK DYNAMIC
--    DATAPROV.data_prep_framework.get_test_script_id(v_test_script_name=>v_test_name,v_test_script_id => v_test_script_id);

      --truncate main DATAPROV table
      execute immediate 'truncate table DATAPROV.dprov_accounts';

  --DATAPROV.data_prep_framework.test_logging(v_parent_run_id,'MASTER','Full master refresh',v_parent_run_id,v_parent_run_id);
  DATAPROV.data_prep_framework.test_logging(v_test_script_id => v_test_script_id
                        ,V_TEST_NAME        => V_TEST_NAME
                        ,v_test_load_type  => v_load_type
                        ,v_current_run_id  => v_run_id
                        ,v_master_run_id   => v_parent_run_id
                        );
						


  -- Verify if have processes to run
  
  -- RUN DINAMIC PROCESS
  
  select count(*) into V_REMAIN from RUN_JOB_PARALLEL_CONTROL where START_TIME is null and stat_dyn = 'D';
  
    while V_REMAIN    > 0 loop
  
  -- create cursor with candidate processes
  
  
    for I in
           (select a.JOB_NAME,
                   DECODE(STAT_DYN, 'D', 'DATAPROV.DATA_PREP', 'S', 'DATAPROV.DATA_PREP_STATIC') PACK_NAME
            from RUN_JOB_PARALLEL_CONTROL a
            where ( 'P'||a.JOB_DEPENDENCE in
                                           (select JOB_NAME
                                            from USER_SCHEDULER_JOB_RUN_DETAILS
                                            where ACTUAL_START_DATE >= V_PROC_DATE)
                     or a.JOB_DEPENDENCE is null)
            and a.START_TIME    is null
            and a.stat_dyn = 'D'
            order by PRIORITY)
    loop

 -- Verify limit or processes
    DBMS_LOCK.SLEEP(1);
            select count(*) into V_JOB_RUN from (
                    select 'P'||job_name from RUN_JOB_PARALLEL_CONTROL a where stat_dyn = 'D' and start_time is not null
                    minus 
                    select job_name from USER_SCHEDULER_JOB_RUN_DETAILS where log_date > v_proc_date);
      
    if V_JOB_RUN     >= 1 then
            while V_JOB_RUN = p_thread_number  -- define max concurrent processes
        loop
            DBMS_LOCK.SLEEP(5);
            select count(*) into V_JOB_RUN from (
                    select 'P'||job_name from RUN_JOB_PARALLEL_CONTROL a where stat_dyn = 'D' and start_time is not null
                    minus 
                    select job_name from USER_SCHEDULER_JOB_RUN_DETAILS where log_date > v_proc_date);
        end loop;
    end if;
      
  -- Create and submit a job
      select DATAPROV.DP_TEST_REFRESH_SEQ.NEXTVAL into V_RUN_ID from DUAL;
     
      V_SQL := 'DBMS_SCHEDULER.CREATE_JOB (JOB_NAME => '''||'P'||I.JOB_NAME||''',JOB_TYPE => ''PLSQL_BLOCK'', JOB_ACTION => ''BEGIN '||
                                                       I.PACK_NAME||'.'||I.JOB_NAME||'('||''''''||V_LOAD_TYPE||''''''||', '||V_RUN_ID||
                                                       ', '||V_PARENT_RUN_ID||') ; END;'' ,ENABLED => TRUE);';
     
      execute immediate ('begin '||V_SQL||' end;');
     
--      DBMS_OUTPUT.PUT_LINE(V_SQL);
     
      update RUN_JOB_PARALLEL_CONTROL set START_TIME = sysdate
      where JOB_NAME = i.JOB_NAME;

      select count(*) into V_REMAIN
      from RUN_JOB_PARALLEL_CONTROL
      where START_TIME is null
      and stat_dyn = 'D';
      commit;

    END LOOP;

  END LOOP;

-- Verify if all jobs have finished

  select count(*) INTO V_JOB_RUN FROM USER_SCHEDULER_RUNNING_JOBS WHERE rownum    < 2;
 
     while V_JOB_RUN > 0
     loop
        dbms_lock.sleep(10);
        select count(*)  into V_JOB_RUN from USER_SCHEDULER_RUNNING_JOBS;
     end loop;
   
   -- update END_TIME on control table
   
       update RUN_JOB_PARALLEL_CONTROL a
          set END_TIME = (select B.LOG_DATE from USER_SCHEDULER_JOB_RUN_DETAILS B
                          where 'P'||a.JOB_NAME = B.JOB_NAME and LOG_DATE  >= V_PROC_DATE );
       commit;

-- RUN STATIC PROCESS

--FRAMEWORK STATIC
  select count(*) into V_REMAIN from RUN_JOB_PARALLEL_CONTROL where START_TIME is null and stat_dyn = 'S';
  
    while V_REMAIN    > 0 loop
  
  -- create cursor with candidate processes
    for I in
           (select a.JOB_NAME,
                   DECODE(STAT_DYN, 'D', 'DATAPROV.DATA_PREP', 'S', 'DATAPROV.DATA_PREP_STATIC') PACK_NAME
            from RUN_JOB_PARALLEL_CONTROL a
            where ( 'P'||a.JOB_DEPENDENCE in
                                           (select JOB_NAME
                                            from USER_SCHEDULER_JOB_RUN_DETAILS
                                            where ACTUAL_START_DATE >= V_PROC_DATE)
                     or a.JOB_DEPENDENCE is null)
            and a.START_TIME    is null
            and a.stat_dyn = 'S'
            order by PRIORITY)
    loop

 -- Verify limit or processes
     DBMS_LOCK.SLEEP(1);
                select count(*) into V_JOB_RUN from (
                    select 'P'||job_name from RUN_JOB_PARALLEL_CONTROL a where stat_dyn = 'S' and start_time is not null
                    minus 
                    select job_name from USER_SCHEDULER_JOB_RUN_DETAILS where log_date > v_proc_date);
      
    if V_JOB_RUN     >= 1 then
            while V_JOB_RUN = p_thread_number  -- define max concurrent processes
        loop
            DBMS_LOCK.SLEEP(5);
                        select count(*) into V_JOB_RUN from (
                    select 'P'||job_name from RUN_JOB_PARALLEL_CONTROL a where stat_dyn = 'S' and start_time is not null
                    minus 
                    select job_name from USER_SCHEDULER_JOB_RUN_DETAILS where log_date > v_proc_date);
        end loop;
    end if;
      
  -- Create and submit a job
      select DATAPROV.DP_TEST_REFRESH_SEQ.NEXTVAL into V_RUN_ID from DUAL;
     
      V_SQL := 'DBMS_SCHEDULER.CREATE_JOB (JOB_NAME => '''||'P'||I.JOB_NAME||''',JOB_TYPE => ''PLSQL_BLOCK'', JOB_ACTION => ''BEGIN '||
                                                       I.PACK_NAME||'.'||I.JOB_NAME||'('||''''''||V_LOAD_TYPE||''''''||', '||V_RUN_ID||
                                                       ', '||V_PARENT_RUN_ID||') ; END;'' ,ENABLED => TRUE);';
     
      execute immediate ('begin '||V_SQL||' end;');
     
--      DBMS_OUTPUT.PUT_LINE(V_SQL);
     
      update RUN_JOB_PARALLEL_CONTROL set START_TIME = sysdate
      where JOB_NAME = i.JOB_NAME;

      select count(*) into V_REMAIN
      from RUN_JOB_PARALLEL_CONTROL
      where START_TIME is null
      and stat_dyn = 'S';
      commit;

    END LOOP;

  END LOOP;

-- Verify if all jobs have finished

  select count(*) INTO V_JOB_RUN FROM USER_SCHEDULER_RUNNING_JOBS WHERE rownum    < 2;
 
     while V_JOB_RUN > 0
     loop
        dbms_lock.sleep(10);
        select count(*)  into V_JOB_RUN from USER_SCHEDULER_RUNNING_JOBS;
     end loop;
   
   -- update END_TIME on control table
   
       update RUN_JOB_PARALLEL_CONTROL a
          set END_TIME = (select B.LOG_DATE from USER_SCHEDULER_JOB_RUN_DETAILS B
                          where 'P'||a.JOB_NAME = B.JOB_NAME and LOG_DATE  >= V_PROC_DATE );
       commit;



    
-- Final execution STATIC

   dbms_stats.gather_Table_stats(tabname => 'DPROV_ACCOUNTS_STATIC',
                                  ownname => 'DATAPROV',
                                  cascade => TRUE,
                                  degree  => 4);

-- Final execution DYNAMIC

     dbms_stats.gather_Table_stats(tabname=>'DPROV_ACCOUNTS',ownname=>'DATAPROV',cascade=>TRUE,degree=>4);
   DATAPROV.data_prep_framework.test_logging(v_test_script_id => v_test_script_id
                        ,V_TEST_NAME        => V_TEST_NAME
                        ,v_test_load_type  => v_load_type
                        ,v_current_run_id  => v_run_id
                        ,v_master_run_id   => v_parent_run_id
                        );
  
end;

/

  GRANT EXECUTE ON "DATAPROV"."PARALLEL_DAILY_LOAD" TO "BATCHPROCESS_USER";
