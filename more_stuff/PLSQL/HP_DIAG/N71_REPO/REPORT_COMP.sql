CREATE OR REPLACE PACKAGE REPORT_COMP AS 

/* This is the REPOSITORY of Procedures & Functions common for 
   all the COMPARISON utilities within the Confluenece Reports Generation 
*/ 

   TYPE g_tvc2 IS TABLE OF VARCHAR2(4000) ;
   
   FUNCTION Get_chart_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Best previous release'
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION Get_pct_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Best previous release'
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION Get_top25_comparison_brief (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_desc    IN varchar2 default null
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION Get_top25_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION Get_top25_metrics (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION Get_top25_long_comp (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION Get_load_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE ,
        i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION Get_load_comparison_colour (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION Get_top25_long_comp_db (
        i_dbname  IN varchar2
       ,i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
       ,i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION Get_top_n_long_comp_db ( i_dbname IN varchar2 
                                ,i_limit IN number DEFAULT 25 
                                ,i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE
                                ,i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
                                ,i_testId3 IN hp_diag.test_result_metrics.test_id%TYPE 
                                ,i_testId4 IN hp_diag.test_result_metrics.test_id%TYPE 
                               ) RETURN g_tvc2 PIPELINED;

   FUNCTION Get_all_db_top25_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Best previous release'
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION Get_rlw_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Best previous release'
      ) RETURN g_tvc2 PIPELINED ;  


END REPORT_COMP;
/


CREATE OR REPLACE PACKAGE BODY REPORT_COMP AS

FUNCTION Get_chart_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED
AS
   l_metric_name g_tvc2 ;
   l_idx VARCHAR2(100) ;
   l_header_row VARCHAR2(4000) ;
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;
BEGIN
   -- 1) Set default parameters
   l_testId1 := i_testId1 ;
   l_testId2 := i_testId2 ;

   --PIPE ROW ( 'h3. Chart Comparisons' ) ;
   PIPE ROW ( 'h5. Comparison With Previous Release' ) ;

   -- 2) return data
   --l_metric_name := NEW g_tvc2 ( 'Host CPU Utilization (%)' , 'Average Active Sessions' , 'Total PGA Allocated' , 'Current OS Load' , 'SQL Service Response Time' , 'Total Table Scans Per Sec', 'Database Wait Time Ratio', 'User Commits Per Sec', 'Physical Read Total Bytes Per Sec' ) ;
   l_metric_name := NEW g_tvc2 ( 'Host CPU Utilization (%)' , 'Average Active Sessions' , 'Current OS Load' , 'User Commits Per Sec', 'Physical Read Total Bytes Per Sec' ) ;
   l_idx := l_metric_name.FIRST ;
   WHILE l_idx IS NOT NULL
   LOOP
      -- 2.1) Chart header rows
      PIPE ROW ( '{chart:type=bar | width=1000 | height=1500 | title = ' || l_metric_name ( l_idx ) || ' | orientation = horizontal}' ) ;
        WITH q AS (
         SELECT DISTINCT database_name
           FROM hp_diag.test_result_metrics
          WHERE metric_name = l_metric_name ( l_idx )
            AND test_id IN ( l_testId1 , l_testId2 )
         )
      SELECT '|| LIST_TESTS || ' || LISTAGG ( database_name  , '||' ) WITHIN GROUP ( ORDER BY database_name ) || '||' INTO l_header_row
        FROM q
      ;
      PIPE ROW ( l_header_row ) ;

      -- 2.2) Chart detail rows.
      FOR r1 IN (
         SELECT NVL ( test_description , test_id )
              , '|' || NVL ( test_description , test_id ) || '|'
                 || LISTAGG (
                       ROUND ( CASE metric_name WHEN 'Total PGA Allocated' THEN average /1024/1024/1024 ELSE average END , 2 )
                       , '|' ) WITHIN GROUP ( ORDER BY database_name )
                 || '|' AS text_output
           FROM hp_diag.test_result_metrics
          WHERE metric_name = l_metric_name ( l_idx )
            AND test_id IN ( l_testId1 , l_testId2 )
          GROUP BY NVL ( test_description , test_id )
          ORDER BY NVL ( test_description , test_id )
      )
      LOOP
         PIPE ROW ( r1.text_output ) ;
      END LOOP ;  -- FOR r1 IN

      -- 2.3) Chart footer rows.
      PIPE ROW ( '{chart}' ) ;
      l_idx := l_metric_name.NEXT ( l_idx ) ;
   END LOOP ;  -- WHILE l_idx IS NOT NULL

   PIPE ROW ( 'h5. Average Active Session Breakdown' ) ;
   -- 2.5 Get the AAS breakdown per database
   FOR rh IN (
      --select distinct database_name from TEST_RESULT_WAIT_CLASS t where t.test_id IN ( l_testId1 , l_testId2 ) order by 1
      select distinct database_name from TEST_RESULT_WAIT_CLASS t where t.test_id IN ( l_testId1 ) order by 1
   )
   LOOP
      PIPE ROW ( '{chart:type=bar | width=500 | height=750 | title = Average Session Breakdown for ' || rh.database_name || ' | orientation = horizontal}' ) ;
      with q as (select distinct wait_class from TEST_RESULT_WAIT_CLASS where test_id IN (l_testId1, l_testId2 ) order by 1)
      select '|| LIST_TESTS || ' ||  listagg(wait_class, '||') within group (order by wait_class) || '||' INTO l_header_row
        from q;
      --PIPE ROW ( l_header_row ) ;
      PIPE ROW ( '|| LIST_TESTS || Administrative || Application || CPU || Commit || Concurrency || Configuration || Idle || Network || Other || Queueing || Scheduler || System I/O || User I/O ||' ) ;

      for r1 in (
          select '|' || test_id || 
           '|' || Administrative || 
           '|' || Application || 
           '|' || CPU || 
           '|' || Commit || 
           '|' || Concurrency || 
           '|' || Configuration || 
           '|' || Idle || 
           '|' || Network || 
           '|' || Other || 
           '|' || Queueing || 
           '|' || Scheduler || 
           '|' || System_IO || 
           '|' || User_IO || '|' as text_output
            from (SELECT test_id
                       , nvl(max(decode(wait_class, 'Administrative', avg_sessions)),0) Administrative
                       , nvl(max(decode(wait_class, 'Application',    avg_sessions)),0) Application
                       , nvl(max(decode(wait_class, 'CPU',            avg_sessions)),0) CPU
                       , nvl(max(decode(wait_class, 'Commit',         avg_sessions)),0) Commit
                       , nvl(max(decode(wait_class, 'Concurrency',    avg_sessions)),0) Concurrency
                       , nvl(max(decode(wait_class, 'Configuration',  avg_sessions)),0) Configuration
                       , nvl(max(decode(wait_class, 'Idle',           avg_sessions)),0) Idle
                       , nvl(max(decode(wait_class, 'Network',        avg_sessions)),0) Network
                       , nvl(max(decode(wait_class, 'Other',          avg_sessions)),0) Other
                       , nvl(max(decode(wait_class, 'Queueing',       avg_sessions)),0) Queueing
                       , nvl(max(decode(wait_class, 'Scheduler',      avg_sessions)),0) Scheduler
                       , nvl(max(decode(wait_class, 'System I/O',     avg_sessions)),0) System_IO
                       , nvl(max(decode(wait_class, 'User I/O',       avg_sessions)),0) User_IO
                    FROM hp_diag.TEST_RESULT_WAIT_CLASS a
                   where test_id in (l_testId1 , l_testId2 )
                     and database_name = rh.database_name
                  group by test_id, database_name
                  order by 1)
      )
      loop
         PIPE ROW ( r1.text_output ) ;
      end loop;

      -- extract the required data from TEST_RESULT_WAIT_CLASS
      PIPE ROW ( '{chart}' ) ;  
   END LOOP ;  

   -- 3) return data in figures as well as in chart - chordiant.
   PIPE ROW ( 'h5.Comparison with previous release in numbers, Chordiant database' ) ;
   PIPE ROW ( '{csv:allowExport=true|columnTypes=s,f,f,f,f,s|id=ComparisonNumbersChordiant}' ) ;
   -- header row - despite the loop, will only output 1 row because of the group by.
   FOR rh IN (
--      SELECT MIN ( CASE m.test_id WHEN l_testId1 THEN m.test_description END ) AS test_description1
--           , MIN ( CASE m.test_id WHEN l_testId2 THEN m.test_description END ) AS test_description2
      SELECT MIN ( CASE m.test_id WHEN l_testId1 THEN 'Current Test' END ) AS test_description1
           , MIN ( CASE m.test_id WHEN l_testId2 THEN 'Previous Test' END ) AS test_description2
        FROM hp_diag.test_result_master m
       WHERE m.test_id IN ( l_testId1 , l_testId2 )
   )
   LOOP
      PIPE ROW ( '"Metric","' || NVL ( rh.test_description1 , l_testid1 ) || '","'
         || NVL ( rh.test_description2 , l_testid2 ) || '","Pct Increase","Value Increase","Units"' ) ;
   END LOOP ;
   -- data rows
   FOR r1 IN (
      WITH base AS (
         SELECT test_id
              , CASE metric_name WHEN 'Physical Read Total Bytes Per Sec' THEN 'Physical Read Total MB Per Sec' ELSE metric_name END AS metric_name
              , CASE WHEN metric_name IN ( 'Physical Read Total Bytes Per Sec' , 'Temp Space Used' , 'Total PGA Allocated' ) THEN average/1024/1024 ELSE average END AS average
              , CASE metric_unit WHEN 'bytes' THEN 'mb' WHEN 'Bytes Per Second' THEN 'mb per Second' ELSE metric_unit END AS metric_unit
           FROM hp_diag.test_result_metrics
          WHERE database_name IN ( 'CHORDO' , 'CCS021N' )
            AND test_id IN ( l_testId1 , l_testId2 )
      ) , t1 AS (
         SELECT * FROM base WHERE test_id = l_testId1
      ) , t2 AS (
         SELECT * FROM base WHERE test_id = l_testId2
      )
      SELECT '"' || t1.metric_name
             || '","' || TO_CHAR ( ROUND ( t1.average , 2 ) , 'fm999g999g999g999g999g999g990d00' )
             || '","' || TO_CHAR ( ROUND ( t2.average , 2 ) , 'fm999g999g999g999g999g999g990d00' )
             || '","' || CASE WHEN t2.average != 0 THEN TO_CHAR ( ROUND ( 100 * ( t1.average - t2.average ) / t2.average , 2 ) , 'fm999g999g999g999g999g999g990d00' ) ELSE NULL END
             || '","' || TO_CHAR ( ROUND ( t1.average - t2.average , 2 ) , 'fm999g999g999g999g999g999g990d00' )
             || '","' || t1.metric_unit
             || '"' AS col1
        FROM t1
        JOIN t2 ON t1.metric_name = t2.metric_name
       ORDER BY t1.metric_name
   )
   LOOP
      PIPE ROW ( r1.col1 ) ;
   END LOOP ;
   PIPE ROW ( '{csv}' ) ;

   -- 4) return data in figures as well as in chart - sal/sis/iss.
   PIPE ROW ( 'h5.Comparison with previous release in numbers, SAL/SIS database' ) ;
   PIPE ROW ( '{csv:allowExport=true|columnTypes=s,f,f,f,f,s|id=ComparisonNumbersSal}' ) ;
   -- header row - despite the loop, will only output 1 row because of the group by.
   FOR rh IN (
--      SELECT MIN ( CASE m.test_id WHEN l_testId1 THEN m.test_description END ) AS test_description1
--           , MIN ( CASE m.test_id WHEN l_testId2 THEN m.test_description END ) AS test_description2
      SELECT MIN ( CASE m.test_id WHEN l_testId1 THEN 'Current Test' END ) AS test_description1
           , MIN ( CASE m.test_id WHEN l_testId2 THEN 'Previous Test' END ) AS test_description2
        FROM hp_diag.test_result_master m
       WHERE m.test_id IN ( l_testId1 , l_testId2 )
   )
   LOOP
      PIPE ROW ( '"Metric","' || NVL ( rh.test_description1 , l_testid1 ) || '","'
         || NVL ( rh.test_description2 , l_testid2 ) || '","Pct Increase","Value Increase","Units"' ) ;
   END LOOP ;
   -- data rows
   FOR r1 IN (
      WITH base AS (
         SELECT test_id
              , CASE metric_name WHEN 'Physical Read Total Bytes Per Sec' THEN 'Physical Read Total MB Per Sec' ELSE metric_name END AS metric_name
              , CASE WHEN metric_name IN ( 'Physical Read Total Bytes Per Sec' , 'Temp Space Used' , 'Total PGA Allocated' ) THEN average/1024/1024 ELSE average END AS average
              , CASE metric_unit WHEN 'bytes' THEN 'mb' WHEN 'Bytes Per Second' THEN 'mb per Second' ELSE metric_unit END AS metric_unit
           FROM hp_diag.test_result_metrics
          WHERE database_name IN ( 'ISS011N' , 'ISS021N' )
            AND test_id IN ( l_testId1 , l_testId2 )
      ) , t1 AS (
         SELECT * FROM base WHERE test_id = l_testId1
      ) , t2 AS (
         SELECT * FROM base WHERE test_id = l_testId2
      )
      SELECT '"' || t1.metric_name
             || '","' || TO_CHAR ( ROUND ( t1.average , 2 ) , 'fm999g999g999g999g999g999g990d00' )
             || '","' || TO_CHAR ( ROUND ( t2.average , 2 ) , 'fm999g999g999g999g999g999g990d00' )
             || '","' || CASE WHEN t2.average != 0 THEN TO_CHAR ( ROUND ( 100 * ( t1.average - t2.average ) / t2.average , 2 ) , 'fm999g999g999g999g999g999g990d00' ) ELSE NULL END
             || '","' || TO_CHAR ( ROUND ( t1.average - t2.average , 2 ) , 'fm999g999g999g999g999g999g990d00' )
             || '","' || t1.metric_unit
             || '"' AS col1
        FROM t1
        JOIN t2 ON t1.metric_name = t2.metric_name
       ORDER BY t1.metric_name
   )
   LOOP
      PIPE ROW ( r1.col1 ) ;
   END LOOP ;
   PIPE ROW ( '{csv}' ) ;

   -- 5) return data in figures as well as in chart - oms.
   PIPE ROW ( 'h5.Comparison with previous release in numbers, OMS database' ) ;
   PIPE ROW ( '{csv:allowExport=true|columnTypes=s,f,f,f,f,s|id=ComparisonNumbersOms}' ) ;
   -- header row - despite the loop, will only output 1 row because of the group by.
   FOR rh IN (
--      SELECT MIN ( CASE m.test_id WHEN l_testId1 THEN m.test_description END ) AS test_description1
--           , MIN ( CASE m.test_id WHEN l_testId2 THEN m.test_description END ) AS test_description2
      SELECT MIN ( CASE m.test_id WHEN l_testId1 THEN 'Current Test' END ) AS test_description1
           , MIN ( CASE m.test_id WHEN l_testId2 THEN 'Previous Test' END ) AS test_description2
        FROM hp_diag.test_result_master m
       WHERE m.test_id IN ( l_testId1 , l_testId2 )
   )
   LOOP
      PIPE ROW ( '"Metric","' || NVL ( rh.test_description1 , l_testid1 ) || '","'
         || NVL ( rh.test_description2 , l_testid2 ) || '","Pct Increase","Value Increase","Units"' ) ;
   END LOOP ;
   -- data rows
   FOR r1 IN (
      WITH base AS (
         SELECT test_id
              , CASE metric_name WHEN 'Physical Read Total Bytes Per Sec' THEN 'Physical Read Total MB Per Sec' ELSE metric_name END AS metric_name
              , CASE WHEN metric_name IN ( 'Physical Read Total Bytes Per Sec' , 'Temp Space Used' , 'Total PGA Allocated' ) THEN average/1024/1024 ELSE average END AS average
              , CASE metric_unit WHEN 'bytes' THEN 'mb' WHEN 'Bytes Per Second' THEN 'mb per Second' ELSE metric_unit END AS metric_unit
           FROM hp_diag.test_result_metrics
          WHERE database_name IN ( 'OMS011N' , 'OMS021N' )
            AND test_id IN ( l_testId1 , l_testId2 )
      ) , t1 AS (
         SELECT * FROM base WHERE test_id = l_testId1
      ) , t2 AS (
         SELECT * FROM base WHERE test_id = l_testId2
      )
      SELECT '"' || t1.metric_name
             || '","' || TO_CHAR ( ROUND ( t1.average , 2 ) , 'fm999g999g999g999g999g999g990d00' )
             || '","' || TO_CHAR ( ROUND ( t2.average , 2 ) , 'fm999g999g999g999g999g999g990d00' )
             || '","' || CASE WHEN t2.average != 0 THEN TO_CHAR ( ROUND ( 100 * ( t1.average - t2.average ) / t2.average , 2 ) , 'fm999g999g999g999g999g999g990d00' ) ELSE NULL END
             || '","' || TO_CHAR ( ROUND ( t1.average - t2.average , 2 ) , 'fm999g999g999g999g999g999g990d00' )
             || '","' || t1.metric_unit
             || '"' AS col1
        FROM t1
        JOIN t2 ON t1.metric_name = t2.metric_name
       ORDER BY t1.metric_name
   )
   LOOP
      PIPE ROW ( r1.col1 ) ;
   END LOOP ;
   PIPE ROW ( '{csv}' ) ;
END Get_chart_comparison ;

FUNCTION Get_pct_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE
      ) RETURN g_tvc2 PIPELINED
AS
   l_metric_name g_tvc2 ;
   l_idx VARCHAR2(100) ;
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;

   l_avg_1   number;
   l_max_1   number;
   l_avg_2   number;
   l_max_2   number;
   l_avg_pct number;
   l_max_pct number;   
   l_row     varchar2(4000);
BEGIN
   -- 1) Set default parameters
   l_testId1 := i_testId1 ;
   l_testId2 := i_testId2 ;

   -- Wait classes we are intersted in
   l_metric_name := NEW g_tvc2 ( 'Administrative', 'Application', 'CPU', 'Commit', 'Concurrency', 'Configuration', 'Idle', 'Network', 'Other', 'Queueing', 'Scheduler', 'System I/O', 'User I/O' ) ;

   -- loop through DBs in the test
   --FOR db_name in (SELECT DISTINCT database_name FROM hp_diag.test_result_metrics WHERE test_id IN ( l_testId1 , l_testId2 ) order by 1)
   FOR r_dbName IN (
      SELECT DISTINCT t.database_name
        FROM hp_diag.test_result_wait_class t
       WHERE t.test_id = l_testId1
         AND t.avg_sessions IS NOT NULL
       ORDER BY 1
   )
   LOOP
     -- pipe header
     l_row := 'h3. WAIT CLASS COMPARISONS ';
     l_row := ' ';
     l_row := 'h5. Wait Class Comparison for ' || r_dbName.database_name ;
     PIPE ROW ( l_row ) ;
     l_idx := l_metric_name.FIRST ;
     WHILE l_idx IS NOT NULL
     LOOP
        begin
            with t1 as (SELECT nvl(max(decode(wait_class, l_metric_name(l_idx), avg_sessions)),0) avg_val
                          FROM hp_diag.TEST_RESULT_WAIT_CLASS a
                         where test_id = l_testId1
                           and database_name = r_dbName.database_name
                        group by test_id),
                 t2 as (SELECT nvl(max(decode(wait_class, l_metric_name(l_idx), avg_sessions)),0) avg_val
                          FROM hp_diag.TEST_RESULT_WAIT_CLASS a
                         where test_id = l_testId2
                           and database_name = r_dbName.database_name
                        group by test_id)
            select t1.avg_val, t2.avg_val
            into l_avg_1, l_avg_2 
            from t1, t2;    

            if (l_avg_2 = 0 and l_avg_1 = 0) then
              -- metric not seen in any of the runs
              null; 
            elsif (l_avg_1 = 0) then
              -- metric not seen in run1
              null;
            elsif (l_avg_2 = 0) then
              -- metric not seen in run2
              l_row := '"' || l_metric_name(l_idx) || '" event class was not been seen in comparative run ' || l_testId2 || '. Event class has increased from 0 to ' || l_avg_1 || '.';
              PIPE ROW ( l_row ) ;
            else
              -- metric seen in both
              l_avg_pct := round(((l_avg_1 - l_avg_2)/l_avg_2)*100,2);

              if l_avg_pct > 0 then
                l_row := '"' || l_metric_name(l_idx) || '" event class has increased from ' || l_avg_2 || ' to ' || l_avg_1 || '. This is a pct increase of ' || l_avg_pct || '%.';
                PIPE ROW ( l_row ) ;
              end if;
            end if;            
        exception
            WHEN NO_DATA_FOUND THEN
               null;
        end;
        l_idx := l_metric_name.NEXT ( l_idx ) ;
        l_row := '';
     END LOOP ;  -- WHILE l_idx IS NOT NULL
   END LOOP;
END Get_pct_comparison ;


FUNCTION Get_load_comparison_colour (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED
AS
   l_metric_name g_tvc2 ;
   l_idx VARCHAR2(100) ;
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;

   l_avg_1   number;
   l_max_1   number;
   l_avg_2   number;
   l_max_2   number;
   l_avg_pct number;
   l_max_pct number;   
   l_row     varchar2(4000);
BEGIN
   -- 1) Set default parameters
   l_testId1 := i_testId1 ;
   l_testId2 := i_testId2 ;

   PIPE ROW ( 'h5. Load Comparison With Previous Releases (brief))' ) ;
   PIPE ROW ( 'Show how the previous tests compare to each other regarding the overall load put on the individual databases' ) ;

   PIPE ROW ( '||Overall Status||Database||DB Time Per Sec||DB CPU Per Sec||Execs Per Sec||' ) ;
   FOR r1 IN (
      WITH cur AS (
         SELECT a.database_name, a.db_time_per_sec, a.db_cpu_per_sec, a.execs_per_sec
           FROM (
                  select database_name, db_time_per_sec, db_cpu_per_sec, execs_per_sec 
                    from hp_diag.test_result_db_stats 
                   where test_id = l_testId1
                   ORDER BY 1
                ) a
      ) , prev AS (
         SELECT x.database_name, x.db_time_per_sec, x.db_cpu_per_sec, x.execs_per_sec
           FROM (
                  select database_name, db_time_per_sec, db_cpu_per_sec, execs_per_sec from hp_diag.test_result_db_stats where test_id = l_testId2
                  ORDER BY 1
                ) x
      ) , b AS (
         SELECT cur.database_name
              , CASE WHEN cur.db_time_per_sec  IS NULL AND prev.db_time_per_sec IS NULL THEN 'Green'
                     WHEN cur.db_time_per_sec  IS NULL THEN 'Red'
                     WHEN prev.db_time_per_sec IS NULL THEN 'Red'
                     WHEN cur.db_time_per_sec  >= 10 * prev.db_time_per_sec THEN 'Red'
                     WHEN prev.db_time_per_sec >= 10 * cur.db_time_per_sec THEN 'Red'
                     WHEN cur.db_time_per_sec  >= 2 *  prev.db_time_per_sec THEN 'Amber'
                     WHEN prev.db_time_per_sec >= 2 *  cur.db_time_per_sec THEN 'Amber'
                     ELSE 'Green' END AS db_time_per_sec_status
              , CASE WHEN cur.db_cpu_per_sec  IS NULL AND prev.db_time_per_sec IS NULL THEN 'Green'
                     WHEN cur.db_cpu_per_sec  IS NULL THEN 'Red'
                     WHEN prev.db_cpu_per_sec IS NULL THEN 'Red'
                     WHEN cur.db_cpu_per_sec  >= 10 * prev.db_cpu_per_sec THEN 'Red'
                     WHEN prev.db_cpu_per_sec >= 10 * cur.db_cpu_per_sec THEN 'Red'
                     WHEN cur.db_cpu_per_sec  >= 2 *  prev.db_cpu_per_sec THEN 'Amber'
                     WHEN prev.db_cpu_per_sec >= 2 *  cur.db_cpu_per_sec THEN 'Amber'
                     ELSE 'Green' END AS db_cpu_per_sec_status
              , CASE WHEN cur.execs_per_sec  IS NULL AND prev.execs_per_sec IS NULL THEN 'Green'
                     WHEN cur.execs_per_sec  IS NULL THEN 'Red'
                     WHEN prev.execs_per_sec IS NULL THEN 'Red'
                     WHEN cur.execs_per_sec  >= 10 * prev.execs_per_sec THEN 'Red'
                     WHEN prev.execs_per_sec >= 10 * cur.execs_per_sec THEN 'Red'
                     WHEN cur.execs_per_sec  >= 2 *  prev.execs_per_sec THEN 'Amber'
                     WHEN prev.execs_per_sec >= 2 *  cur.execs_per_sec THEN 'Amber'
                     ELSE 'Green' END AS execs_per_sec_status
           FROM cur
           LEFT OUTER JOIN prev ON cur.database_name = prev.database_name
      ) , c AS (
         SELECT CASE WHEN db_time_per_sec_status = 'Red' THEN 'Red'
                     WHEN db_cpu_per_sec_status  = 'Red' THEN 'Red'
                     WHEN execs_per_sec_status   = 'Red' THEN 'Red'
                     WHEN db_time_per_sec_status = 'Amber' THEN 'Amber'
                     WHEN execs_per_sec_status   = 'Amber' THEN 'Amber'
                     WHEN db_cpu_per_sec_status  = 'Amber' THEN 'Amber'
                     ELSE 'Green' END AS overall_status
              , LOWER ( b.database_name ) AS database_name
              , b.db_time_per_sec_status
              , b.db_cpu_per_sec_status
              , b.execs_per_sec_status
           FROM b
      )
      SELECT '|{status:colour=' || CASE WHEN c.overall_status = 'Amber' THEN 'Yellow' ELSE c.overall_status END
                || '|title=' || c.overall_status || '}|'
             || c.database_name
             || '|{status:colour=' || CASE WHEN c.db_time_per_sec_status = 'Amber' THEN 'Yellow' ELSE c.db_time_per_sec_status END
                || '|title=' || c.db_time_per_sec_status
             || '}|{status:colour=' || CASE WHEN c.db_cpu_per_sec_status = 'Amber' THEN 'Yellow' ELSE c.db_cpu_per_sec_status END
                || '|title=' || c.db_cpu_per_sec_status
             || '}|{status:colour=' || CASE WHEN c.execs_per_sec_status = 'Amber' THEN 'Yellow' ELSE c.execs_per_sec_status END
                || '|title=' || c.execs_per_sec_status
             || '}|' AS col1
        FROM c
       ORDER BY c.database_name
   )
   LOOP
      PIPE ROW ( r1.col1 ) ;
   END LOOP ;

END Get_load_comparison_colour ;


FUNCTION Get_top25_comparison_brief (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_desc    IN varchar2 default null
      ) RETURN g_tvc2 PIPELINED
AS
   l_metric_name g_tvc2 ;
   l_idx VARCHAR2(100) ;
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;
   l_desc    varchar2(100) ;
   l_row     varchar2(4000);

BEGIN
   -- 1) Set default parameters
   l_testId1 := i_testId1 ;
   l_testId2 := i_testId2 ;
   l_desc    := i_desc ;

   PIPE ROW ( 'h3. SQL Comparison' ) ;
   PIPE ROW ( 'h5. Top 25 SQL Comparison Across Database by Elapsed Time (Brief)' ) ;
   -- 17-Apr-2023 Andrew Fraser add RAG status list
   PIPE ROW ( '||Overall Status||Sql Id||Database||TPS Status||Duration Status||Details||' ) ;
   FOR r1 IN (
      WITH cur AS (
         SELECT ROWNUM AS rnum , a.database_name , a.sql_id , a.tps , a.elapsed_time_per_exec_seconds AS els , a.test_description
           FROM (
                  SELECT s.*
                    FROM hp_diag.test_result_sql s
                   WHERE s.test_id = l_testId1
                    AND lower(s.module) not in('dbms_scheduler')
                  ORDER BY s.elapsed_time_seconds DESC
                ) a
          WHERE ROWNUM <= 25
      ) , prev AS (
         SELECT s.database_name , s.sql_id , s.tps , s.elapsed_time_per_exec_seconds AS els
           FROM hp_diag.test_result_sql s
          WHERE s.test_id = l_testId2
          AND lower(s.module) not in('dbms_scheduler')
      ) , b AS (
         SELECT cur.rnum
              , cur.sql_id
              , cur.database_name
              , CASE WHEN cur.tps IS NULL AND prev.tps IS NULL THEN 'Green'
                     WHEN cur.tps IS NULL THEN 'Red'
                     WHEN prev.tps IS NULL THEN 'Red'
                     WHEN cur.tps >= 10 * prev.tps THEN 'Red'
                     WHEN prev.tps >= 10 * cur.tps THEN 'Red'
                     WHEN cur.tps >= 2 * prev.tps THEN 'Amber'
                     WHEN prev.tps >= 2 * cur.tps THEN 'Amber'
                     ELSE 'Green' END AS tps_status
              , CASE WHEN cur.els IS NULL AND prev.els IS NULL THEN 'Green'
                     WHEN prev.els IS NULL AND cur.els >= 1/1000 THEN 'Red'
                     WHEN prev.els IS NULL AND cur.els >= 1/10/1000 THEN 'Amber'
                     WHEN prev.els IS NULL THEN 'Green'
                     WHEN cur.els >= 10 * prev.els THEN 'Red'
                     WHEN cur.els >= 2 * prev.els THEN 'Amber'
                     ELSE 'Green' END AS duration_status
              , cur.test_description
           FROM cur
           LEFT OUTER JOIN prev ON cur.database_name = prev.database_name AND cur.sql_id = prev.sql_id
      ) , c AS (
         SELECT CASE WHEN tps_status = 'Red' THEN 'Red'
                     WHEN duration_status = 'Red' THEN 'Red'
                     WHEN tps_status = 'Amber' THEN 'Amber'
                     WHEN duration_status = 'Amber' THEN 'Amber'
                     ELSE 'Green' END AS overall_status
              , b.sql_id
              , LOWER ( b.database_name ) AS database_name
              , b.tps_status
              , b.duration_status
              , b.test_description
              , b.rnum
           FROM b
      )
      SELECT '|{status:colour=' || CASE WHEN c.overall_status = 'Amber' THEN 'Yellow' ELSE c.overall_status END
                || '|title=' || c.overall_status || '}|'
             || c.sql_id || '|'
             || c.database_name
             || '|{status:colour=' || CASE WHEN c.tps_status = 'Amber' THEN 'Yellow' ELSE c.tps_status END
                || '|title=' || c.tps_status
             || '}|{status:colour=' || CASE WHEN c.duration_status = 'Amber' THEN 'Yellow' ELSE c.duration_status END
                || '|title=' || c.duration_status
             || '}|[details|https://confluence.bskyb.com/display/nonfuntst/4. Sql Analysis - '
                || l_desc || '#SqlAnalysis'
                || REPLACE ( REPLACE ( l_desc , ' ' ) , '-' )
                || '-SQLHistoryfor' || c.sql_id || 'from' || c.database_name || ']|' AS col1
        FROM c
       ORDER BY c.rnum
   )
   LOOP
      PIPE ROW ( r1.col1 ) ;
   END LOOP ;

END Get_top25_comparison_brief ;


FUNCTION Get_top25_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED
AS
   l_metric_name g_tvc2 ;
   l_idx VARCHAR2(100) ;
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;

   l_avg_1   number;
   l_max_1   number;
   l_avg_2   number;
   l_max_2   number;
   l_avg_pct number;
   l_max_pct number;   
   l_row     varchar2(4000);
BEGIN
   -- 1) Set default parameters
   l_testId1 := i_testId1 ;
   l_testId2 := i_testId2 ;

   -- Load Comparison (Brief)
   -- PIPE ROW ( 'h3. SQL Comparison' ) ;
   -- Driven by the calling script
   PIPE ROW ( 'h5. Top 25 SQL Comparison Across Database by Elapsed Time (Brief)' ) ;
   -- 17-Apr-2023 Andrew Fraser add RAG status list
   PIPE ROW ( '||Overall Status||Sql Id||Database||TPS Status||Duration Status||' ) ;
   FOR r1 IN (
      WITH cur AS (
         SELECT ROWNUM AS rnum , a.database_name , a.sql_id , a.tps , a.elapsed_time_per_exec_seconds AS els , a.test_description
           FROM (
                  SELECT s.*
                    FROM hp_diag.test_result_sql s
                   WHERE s.test_id = l_testId1
                     AND lower(s.module) not in('dbms_scheduler')
                  ORDER BY s.elapsed_time_seconds DESC
                ) a
          WHERE ROWNUM <= 25
      ) , prev AS (
         SELECT s.database_name , s.sql_id , s.tps , s.elapsed_time_per_exec_seconds AS els
           FROM hp_diag.test_result_sql s
          WHERE s.test_id = l_testId2
            AND lower(s.module) not in('dbms_scheduler')
      ) , b AS (
         SELECT cur.rnum
              , cur.sql_id
              , cur.database_name
              , CASE WHEN cur.tps IS NULL AND prev.tps IS NULL THEN 'Green'
                     WHEN cur.tps IS NULL THEN 'Red'
                     WHEN prev.tps IS NULL THEN 'Red'
                     WHEN cur.tps >= 10 * prev.tps THEN 'Red'
                     WHEN prev.tps >= 10 * cur.tps THEN 'Red'
                     WHEN cur.tps >= 2 * prev.tps THEN 'Amber'
                     WHEN prev.tps >= 2 * cur.tps THEN 'Amber'
                     ELSE 'Green' END AS tps_status
              , CASE WHEN cur.els IS NULL AND prev.els IS NULL THEN 'Green'
                     WHEN prev.els IS NULL AND cur.els >= 1/1000 THEN 'Red'
                     WHEN prev.els IS NULL AND cur.els >= 1/10/1000 THEN 'Amber'
                     WHEN prev.els IS NULL THEN 'Green'
                     WHEN cur.els >= 10 * prev.els THEN 'Red'
                     WHEN cur.els >= 2 * prev.els THEN 'Amber'
                     ELSE 'Green' END AS duration_status
              , cur.test_description
           FROM cur
           LEFT OUTER JOIN prev ON cur.database_name = prev.database_name AND cur.sql_id = prev.sql_id
      ) , c AS (
         SELECT CASE WHEN tps_status = 'Red' THEN 'Red'
                     WHEN duration_status = 'Red' THEN 'Red'
                     WHEN tps_status = 'Amber' THEN 'Amber'
                     WHEN duration_status = 'Amber' THEN 'Amber'
                     ELSE 'Green' END AS overall_status
              , b.sql_id
              , LOWER ( b.database_name ) AS database_name
              , b.tps_status
              , b.duration_status
              , b.test_description
              , b.rnum
           FROM b
      )
      SELECT '|{status:colour=' || CASE WHEN c.overall_status = 'Amber' THEN 'Yellow' ELSE c.overall_status END
                || '|title=' || c.overall_status || '}|'
             || c.sql_id || '|'
             || c.database_name
             || '|{status:colour=' || CASE WHEN c.tps_status = 'Amber' THEN 'Yellow' ELSE c.tps_status END
                || '|title=' || c.tps_status
             || '}|{status:colour=' || CASE WHEN c.duration_status = 'Amber' THEN 'Yellow' ELSE c.duration_status END
                || '|title=' || c.duration_status 
             || '}|' AS col1
        FROM c
       ORDER BY c.rnum
   )
   LOOP
      PIPE ROW ( r1.col1 ) ;
   END LOOP ;


   PIPE ROW ( 'h5. Top 25 SQL Comparison Across Database by Elapsed Time (with values)' ) ;
   -- end of 17-Apr-2023 RAG additions
   PIPE ROW ( '{csv:allowExport=true|columnTypes=s,i,i,s,f,f,f,f,s,s|id=Top25Comparison}' ) ;
   FOR rh IN (
--      SELECT MIN ( CASE m.test_id WHEN l_testId1 THEN m.test_description END ) AS test_description1
--           , MIN ( CASE m.test_id WHEN l_testId2 THEN m.test_description END ) AS test_description2
      SELECT MIN ( CASE m.test_id WHEN l_testId1 THEN 'CURRENT Test' END ) AS test_description1
           , MIN ( CASE m.test_id WHEN l_testId2 THEN 'PREVIOUS Test' END ) AS test_description2

        FROM hp_diag.test_result_master m
       WHERE m.test_id IN ( l_testId1 , l_testId2 )
   )
   LOOP
      l_row :=  '"SQL ID","'|| 
                NVL ( rh.test_description1, l_testid1 ) || ' Position","'|| 
                NVL ( rh.test_description2, l_testid2 ) || ' Position","Database Name","'|| 
                NVL ( rh.test_description1, l_testid1 ) || ' TPS","'|| 
                NVL ( rh.test_description2, l_testid2 ) || ' TPS","'|| 
                NVL ( rh.test_description1, l_testid1 ) || ' ms Per Exec","'|| 
                NVL ( rh.test_description2, l_testid2 ) || ' ms Per Exec","SQL Text","Module"' ;
      PIPE ROW ( l_row ) ;
   END LOOP ;

   --PIPE ROW ( '"SQL ID","Position","' || l_testId2 || ' Position","Database Name","TPS","' || l_testId2 || ' TPS","ms Per Exec","' || l_testId2 || ' ms Per Exec","SQL Text","Module"');
   for r1 in (
      with prev as (
               SELECT ROWNUM rnum, a.database_name, a.sql_id, a.tps, a.ms_pe, a.sql_text, a.module
                 FROM (
                           SELECT LOWER ( s.database_name ) AS database_name, s.sql_id, 
                                  TO_CHAR ( ROUND ( s.tps , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS tps,
                                  TO_CHAR ( ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS ms_pe, 
                                  LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text, 
                                  LOWER ( s.module ) AS module
                             FROM hp_diag.test_result_sql s
                             LEFT OUTER JOIN test_result_sqltext st ON st.sql_id = s.sql_id
                            WHERE test_id = l_testId2
                              AND lower(s.module) not in('dbms_scheduler')
                           ORDER BY elapsed_time_seconds DESC
                      ) a
         ) ,  cur as (
               SELECT ROWNUM rnum, a.database_name, a.sql_id, a.tps, a.ms_pe, a.sql_text, a.module
                 FROM (
                           SELECT LOWER ( s.database_name ) AS database_name, s.sql_id, 
                                  TO_CHAR ( ROUND ( s.tps , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS tps,
                                  TO_CHAR ( ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS ms_pe,
                                  LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text, 
                                  LOWER ( s.module ) AS module
                             FROM hp_diag.test_result_sql s
                             LEFT OUTER JOIN test_result_sqltext st ON st.sql_id = s.sql_id
                            WHERE test_id = l_testId1
                              AND lower(s.module) not in('dbms_scheduler')
                           ORDER BY elapsed_time_seconds DESC
                      ) a
                WHERE ROWNUM <= 25
           )
      select '"' || cur.sql_id
             || '","' || cur.rnum 
             || '","' || nvl(to_char(prev.rnum), 'N/A') 
             || '","' || cur.database_name
             || '","' || cur.tps
             || '","' || nvl(to_char(prev.tps), 'N/A')
             || '","' || cur.ms_pe
             || '","' || nvl(to_char(prev.ms_pe), 'N/A')
             || '","' || cur.sql_text
             || '","' || cur.module
             || '"' as col1
        from cur, prev
       where cur.database_name = prev.database_name (+)
        and cur.sql_id = prev.sql_id (+)
      order by cur.rnum
   )
   LOOP
      PIPE ROW ( r1.col1 ) ;
   END LOOP ;
   PIPE ROW ( '{csv}' ) ;

END Get_top25_comparison ;


FUNCTION Get_top25_metrics (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED
AS
   l_metric_name g_tvc2 ;
   l_idx VARCHAR2(100) ;
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;

   l_avg_1   number;
   l_max_1   number;
   l_avg_2   number;
   l_max_2   number;
   l_avg_pct number;
   l_max_pct number;   
   l_row     varchar2(4000);
BEGIN
   -- 1) Set default parameters
   l_testId1 := i_testId1 ;
   l_testId2 := i_testId2 ;

   -- Future Enhancement - maybe add ||Network Latency||Disk Latency|| to the below?
   --PIPE ROW ( 'h3. Database Metrics' ) ;
   PIPE ROW ( '||Database||Aas||Cpu Aas||Host Cpu||Host Run Queue||Commit ms||' ) ;
   FOR r1 IN (
      WITH cur AS (
         SELECT t.database_name
              , AVG ( CASE WHEN t.metric_name = 'Average Active Sessions' THEN t.average END ) AS aas
              , AVG ( CASE WHEN t.metric_name = 'CPU Usage Per Sec' THEN t.average / 100 END ) AS cpuAas
              , AVG ( CASE WHEN t.metric_name = 'Host CPU Utilization (%)' THEN t.average END ) AS hostCpu
              , AVG ( CASE WHEN t.metric_name = 'Current OS Load' THEN t.average END ) AS runQueue
              , se.wait_avg_ms
           FROM test_result_metrics_detail t
           LEFT OUTER JOIN test_result_system_event se ON se.test_id = t.test_id AND se.database_name = t.database_name
          WHERE t.test_id = l_testId1
            AND t.metric_name IN ( 'Average Active Sessions' , 'CPU Usage Per Sec' , 'Host CPU Utilization (%)' , 'Current OS Load' )
            AND t.database_name IN ( select distinct database_name from TEST_RESULT_DB_STATS where TEST_ID = l_testId1 )
          GROUP BY t.database_name , se.wait_avg_ms
      ) , prev AS (
         SELECT t.database_name
              , AVG ( CASE WHEN t.metric_name = 'Average Active Sessions' THEN t.average END ) AS aas
              , AVG ( CASE WHEN t.metric_name = 'CPU Usage Per Sec' THEN t.average / 100 END ) AS cpuAas
              , AVG ( CASE WHEN t.metric_name = 'Host CPU Utilization (%)' THEN t.average END ) AS hostCpu
              , AVG ( CASE WHEN t.metric_name = 'Current OS Load' THEN t.average END ) AS runQueue
              , se.wait_avg_ms
           FROM test_result_metrics_detail t
           LEFT OUTER JOIN test_result_system_event se ON se.test_id = t.test_id AND se.database_name = t.database_name
          WHERE t.test_id = l_testId2
            AND t.metric_name IN ( 'Average Active Sessions' , 'CPU Usage Per Sec' , 'Host CPU Utilization (%)' , 'Current OS Load' )
            AND t.database_name IN ( select distinct database_name from TEST_RESULT_DB_STATS where TEST_ID = l_testId2 )
          GROUP BY t.database_name , se.wait_avg_ms
      ) , b AS (
         SELECT cur.database_name
              , cur.aas
              , cur.cpuAas
              , cur.hostCpu
              , cur.runQueue
              , cur.wait_avg_ms
              , CASE WHEN prev.aas IS NULL THEN 'Green'
                     WHEN cur.aas >= 1.5 * prev.aas THEN 'Red'
                     WHEN prev.aas >= 1.5 * cur.aas THEN 'Red'
                     WHEN cur.aas >= 1.25 * prev.aas THEN 'Amber'
                     WHEN prev.aas >= 1.25 * cur.aas THEN 'Amber'
                     ELSE 'Green' END AS cAas
              , CASE WHEN prev.cpuAas IS NULL THEN 'Green'
                     WHEN cur.cpuAas >= 1.5 * prev.cpuAas THEN 'Red'
                     WHEN prev.cpuAas >= 1.5 * cur.cpuAas THEN 'Red'
                     WHEN cur.cpuAas >= 1.25 * prev.cpuAas THEN 'Amber'
                     WHEN prev.cpuAas >= 1.25 * cur.cpuAas THEN 'Amber'
                     ELSE 'Green' END AS cCpuAas
              , CASE WHEN prev.hostCpu IS NULL THEN 'Green'
                     WHEN cur.hostCpu >= 1.5 * prev.hostCpu THEN 'Red'
                     WHEN prev.hostCpu >= 1.5 * cur.hostCpu THEN 'Red'
                     WHEN cur.hostCpu >= 1.25 * prev.hostCpu THEN 'Amber'
                     WHEN prev.hostCpu >= 1.25 * cur.hostCpu THEN 'Amber'
                     ELSE 'Green' END AS cHostCpu
              , CASE WHEN prev.runQueue IS NULL THEN 'Green'
                     WHEN cur.runQueue >= 1.5 * prev.runQueue THEN 'Red'
                     WHEN prev.runQueue >= 1.5 * cur.runQueue THEN 'Red'
                     WHEN cur.runQueue >= 1.25 * prev.runQueue THEN 'Amber'
                     WHEN prev.runQueue >= 1.25 * cur.runQueue THEN 'Amber'
                     ELSE 'Green' END AS cRunQueue
              , CASE WHEN prev.wait_avg_ms IS NULL THEN 'Green'
                     WHEN cur.wait_avg_ms >= 1.5 * prev.wait_avg_ms THEN 'Red'
                     WHEN prev.wait_avg_ms >= 1.5 * cur.wait_avg_ms THEN 'Red'
                     WHEN cur.wait_avg_ms >= 1.25 * prev.wait_avg_ms THEN 'Amber'
                     WHEN prev.wait_avg_ms >= 1.25 * cur.wait_avg_ms THEN 'Amber'
                     ELSE 'Green' END AS cWait_avg_ms
           FROM cur
           LEFT OUTER JOIN prev ON prev.database_name = cur.database_name
      )
      SELECT '|' || LOWER ( b.database_name )
             || '|{status:colour=' || REPLACE ( b.cAas , 'Amber' , 'Yellow' ) || '|title=' || b.cAas || '} ' || TO_CHAR ( ROUND ( b.aas ) )
             || '|{status:colour=' || REPLACE ( b.cCpuAas , 'Amber' , 'Yellow' ) || '|title=' || b.cCpuAas || '} ' || TO_CHAR ( ROUND ( b.cpuAas ) )
             || '|{status:colour=' || REPLACE ( b.cHostCpu , 'Amber' , 'Yellow' ) || '|title=' || b.cHostCpu || '} ' || TO_CHAR ( ROUND ( b.hostCpu ) )
             || '%|{status:colour=' || REPLACE ( b.cRunQueue , 'Amber' , 'Yellow' ) || '|title=' || b.cRunQueue || '} ' || TO_CHAR ( ROUND ( b.runQueue ) )
             || CASE WHEN b.wait_avg_ms IS NULL THEN '| '
                     ELSE '|{status:colour=' || REPLACE ( b.cWait_avg_ms , 'Amber' , 'Yellow' ) || '|title=' || b.cWait_avg_ms || '} ' || TO_CHAR ( ROUND ( b.wait_avg_ms ) )
                     END
             || '|' AS col1
        FROM b
       ORDER BY REPLACE ( b.database_name , 'D' , 'Z' )
   )
   LOOP
      PIPE ROW ( r1.col1 ) ;
   END LOOP ;

END Get_top25_metrics ;


FUNCTION Get_top25_long_comp ( i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
                         , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED
AS
   l_metric_name g_tvc2 ;
   l_idx VARCHAR2(100) ;
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;

   l_row     varchar2(4000);
BEGIN
   -- 1) Set default parameters
   l_testId1 := i_testId1 ;
   l_testId2 := i_testId2 ;

   -- 1) Set default parameters
   --PIPE ROW ( 'h3. SQL Comparison' ) ;
   PIPE ROW ( 'h5. Top 25 SQL Comparison Across Database by Elapsed Time (Detail)') ;
   PIPE ROW ( '{csv:allowExport=true|columnTypes=s,s,f,f,f,f,s|id=Top25Comparison}' ) ;

   FOR rh IN (
--      SELECT MIN ( CASE m.test_id WHEN i_testId1 THEN m.test_description END ) AS test_description1
--           , MIN ( CASE m.test_id WHEN i_testId2 THEN m.test_description END ) AS test_description2
      SELECT MIN ( CASE m.test_id WHEN i_testId1 THEN 'Current' END ) AS test_description1
           , MIN ( CASE m.test_id WHEN i_testId2 THEN 'Previous' END ) AS test_description2
        FROM hp_diag.test_result_master m
       WHERE m.test_id IN ( i_testId1 , i_testId2 )
   )
   LOOP
      l_row := '"SQL ID",'||
               '"Database Name",'||
               '"' || rh.test_description1 ||' TPS",'||
               '"' || rh.test_description2 ||' TPS",'||
               '"' || rh.test_description1 ||' ms/Exec",'||
               '"' || rh.test_description2 ||' ms/Exec",'||
               '"SQL Text",'||
               '"Module"';
      PIPE ROW ( l_row );
      --PIPE ROW ( '"SQL ID","Database Name","Current TPS","' || NVL(rh.test_description1, i_testId1) || ' TPS","Current ms/Exec","'||NVL(rh.test_description2, i_testId2)||' ms/Exec","SQL Text","Module"');
   END LOOP ;

   --PIPE ROW ( '"SQL ID","Position","' || i_testId2 || ' Position","Database Name","TPS","' || i_testId2 || ' TPS","ms Per Exec","' || l_testId2 || ' ms Per Exec","SQL Text","Module"');
   for r1 in (
      with prev as (
               SELECT ROWNUM rnum, a.database_name, a.sql_id, a.tps, a.ms_pe, a.sql_text, a.module
                 FROM (
                           SELECT LOWER ( s.database_name ) AS database_name, s.sql_id, 
                                  TO_CHAR ( ROUND ( s.tps , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS tps,
                                  TO_CHAR ( ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS ms_pe, 
                                  LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text, 
                                  LOWER ( s.module ) AS module
                             FROM hp_diag.test_result_sql s
                             LEFT OUTER JOIN test_result_sqltext st ON st.sql_id = s.sql_id
                            WHERE test_id = i_testId2
                              AND lower(s.module) not in('dbms_scheduler')
                           ORDER BY elapsed_time_seconds DESC
                      ) a
      ) , cur as (
               SELECT ROWNUM rnum, a.database_name, a.sql_id, a.tps, a.ms_pe, a.sql_text, a.module
                 FROM (
                           SELECT LOWER ( s.database_name ) AS database_name, s.sql_id, 
                                  TO_CHAR ( ROUND ( s.tps , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS tps,
                                  TO_CHAR ( ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS ms_pe,
                                  LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text, 
                                  LOWER ( s.module ) AS module
                             FROM hp_diag.test_result_sql s
                             LEFT OUTER JOIN test_result_sqltext st ON st.sql_id = s.sql_id
                            WHERE test_id = i_testId1
                              AND lower(s.module) not in('dbms_scheduler')
                           ORDER BY elapsed_time_seconds DESC
                      ) a
                WHERE ROWNUM <= 25
          ) 
      select '"' || cur.sql_id
             || '","' || cur.database_name
             || '","' || cur.tps
             || '","' || nvl(to_char(prev.tps), 'N/A')
             || '","' || cur.ms_pe
             || '","' || nvl(to_char(prev.ms_pe), 'N/A')
             || '","' || cur.sql_text
             || '","' || cur.module
             || '"' as col1
        from cur, prev
       where cur.database_name = prev.database_name (+)
         and cur.sql_id = prev.sql_id (+)
      order by cur.rnum
   )
   LOOP
      PIPE ROW ( r1.col1 ) ;
   END LOOP ;
   PIPE ROW ( '{csv}' ) ;
END Get_top25_long_comp ;


FUNCTION Get_top25_long_comp_db (
        i_dbname  IN varchar2
        , i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
        , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED
AS
   l_metric_name g_tvc2 ;
   l_idx VARCHAR2(100) ;

   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;

   --l_avg_1   number;
   --l_max_1   number;
   --l_avg_2   number;
   --l_max_2   number;
   --l_avg_pct number;
   --l_max_pct number;   
   l_row     varchar2(4000);
BEGIN
   -- 1) Set default parameters
   l_testId1 := i_testId1 ;
   l_testId2 := i_testId2 ;

   l_row := 'h5. Top 15 SQL Comparison for ' || i_dbname || ' by Elapsed Time' ;
   PIPE ROW ( l_row ) ;

   PIPE ROW ( '{csv:allowExport=true|columnTypes=s,s,s,s,s,s,s,s,s,s,s,s|id=Top25Comparison}' ) ;

   FOR rh IN (
      SELECT MIN ( CASE m.test_id WHEN l_testId1 THEN m.test_description END ) AS test_description1
           , MIN ( CASE m.test_id WHEN l_testId2 THEN m.test_description END ) AS test_description2
        FROM hp_diag.test_result_master m
       WHERE m.test_id IN ( l_testId1, l_testId2 )
   )
   LOOP
      PIPE ROW ( '"SQL ID","Database Name","Current TPS","'||NVL(rh.test_description2, l_testid2)||' TPS","Current ms/Exec","'||NVL(rh.test_description2, l_testid2)||' ms/Exec","SQL Text","Module"');
   END LOOP ;

   --PIPE ROW ( '"SQL ID","Position","' || l_testId2 || ' Position","Database Name","TPS","' || l_testId2 || ' TPS","ms Per Exec","' || l_testId2 || ' ms Per Exec","SQL Text","Module"');
   for r1 in (
      with prev as (
               SELECT ROWNUM rnum, a.database_name, a.sql_id, a.tps, a.ms_pe, a.sql_text, a.module
                 FROM (
                           SELECT LOWER ( s.database_name ) AS database_name, s.sql_id, 
                                  TO_CHAR ( ROUND ( s.tps , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS tps,
                                  TO_CHAR ( ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS ms_pe, 
                                  LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text, 
                                  LOWER ( s.module ) AS module
                             FROM hp_diag.test_result_sql s
                             LEFT OUTER JOIN test_result_sqltext st ON st.sql_id = s.sql_id
                            WHERE database_name = upper(i_dbname)
                              and test_id = l_testId2
                           ORDER BY elapsed_time_seconds DESC
                      ) a
      ) , cur as (
               SELECT ROWNUM rnum, a.database_name, a.sql_id, a.tps, a.ms_pe, a.sql_text, a.module
                 FROM (
                           SELECT LOWER ( s.database_name ) AS database_name, s.sql_id, 
                                  TO_CHAR ( ROUND ( s.tps , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS tps,
                                  TO_CHAR ( ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS ms_pe,
                                  LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text, 
                                  LOWER ( s.module ) AS module
                             FROM hp_diag.test_result_sql s
                             LEFT OUTER JOIN test_result_sqltext st ON st.sql_id = s.sql_id
                            WHERE database_name = upper(i_dbname)
                              and test_id = l_testId1
                           ORDER BY elapsed_time_seconds DESC
                      ) a
                WHERE ROWNUM <= 25
             )
      select '"' || cur.sql_id
             || '","' || cur.database_name
             || '","' || cur.tps
             || '","' || nvl(to_char(prev.tps), 'N/A')
             || '","' || cur.ms_pe
             || '","' || nvl(to_char(prev.ms_pe), 'N/A')
             || '","' || cur.sql_text
             || '","' || cur.module
             || '"' as col1
        from cur, prev
       where cur.database_name = prev.database_name (+)
         and cur.sql_id = prev.sql_id (+)
      order by cur.rnum
   )
   LOOP
      PIPE ROW ( r1.col1 ) ;
   END LOOP ;
   PIPE ROW ( '{csv}' ) ;
END Get_top25_long_comp_db ;


FUNCTION Get_top_n_long_comp_db (
     i_dbname IN VARCHAR2
   , i_limit IN NUMBER DEFAULT 25
   , i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE
   , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
   , i_testId3 IN hp_diag.test_result_metrics.test_id%TYPE 
   , i_testId4 IN hp_diag.test_result_metrics.test_id%TYPE 
) RETURN g_tvc2 PIPELINED
AS
   l_metric_name g_tvc2 ;
   l_idx VARCHAR2(100) ;

   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId3 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId4 hp_diag.test_result_metrics.test_id%TYPE ;

   --l_avg_1   number;
   --l_max_1   number;
   --l_avg_2   number;
   --l_max_2   number;
   --l_avg_pct number;
   --l_max_pct number;   
   l_row     varchar2(4000);
BEGIN
   -- 1) Set default parameters
   l_testId1 := i_testId1 ;
   l_testId2 := i_testId2 ;
   l_testId3 := i_testId3 ;
   l_testId4 := i_testId4 ;

   l_row := 'h5. Top ' || TO_CHAR ( i_limit ) || ' SQL Comparison for ' || i_dbname || ' by Elapsed Time' ;
   PIPE ROW ( l_row ) ;

   PIPE ROW ( '{csv:allowExport=true|columnTypes=s,s,s,s,s,s,s,s,s,s,s,s|id=Top25Comparison}' ) ;

   FOR rh IN (
      SELECT MIN ( CASE m.test_id WHEN l_testId1 THEN m.test_description END ) AS test_description1
           , MIN ( CASE m.test_id WHEN l_testId2 THEN m.test_description END ) AS test_description2
           , MIN ( CASE m.test_id WHEN l_testId3 THEN m.test_description END ) AS test_description3
           , MIN ( CASE m.test_id WHEN l_testId4 THEN m.test_description END ) AS test_description4
        FROM hp_diag.test_result_master m
       WHERE m.test_id IN ( l_testId1, l_testId2, l_testId3, l_testId4 )
   )
   LOOP
      PIPE ROW ( '"SQL ID","Database Name","Current TPS","'||NVL(rh.test_description2, l_testid2)||' TPS","'||NVL(rh.test_description3, l_testid3)||' TPS","'||NVL(rh.test_description4, l_testid4)||' TPS","Current ms/Exec","'||NVL(rh.test_description2, l_testid2)||' ms/Exec","'||NVL(rh.test_description3, l_testid3)||' ms/Exec","'||NVL(rh.test_description4, l_testid4)||' ms/Exec","SQL Text","Module"');
   END LOOP ;

   --PIPE ROW ( '"SQL ID","Position","' || l_testId2 || ' Position","Database Name","TPS","' || l_testId2 || ' TPS","ms Per Exec","' || l_testId2 || ' ms Per Exec","SQL Text","Module"');
   for r1 in (
      with prev as (
               SELECT ROWNUM rnum, a.database_name, a.sql_id, a.tps, a.ms_pe, a.sql_text, a.module
                 FROM (
                           SELECT LOWER ( s.database_name ) AS database_name, s.sql_id, 
                                  TO_CHAR ( ROUND ( s.tps , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS tps,
                                  TO_CHAR ( ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS ms_pe, 
                                  LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text, 
                                  LOWER ( s.module ) AS module
                             FROM hp_diag.test_result_sql s
                             LEFT OUTER JOIN test_result_sqltext st ON st.sql_id = s.sql_id
                            WHERE database_name = upper(i_dbname)
                              and test_id = l_testId2
                              AND lower(s.module) not in('dbms_scheduler')
                           ORDER BY elapsed_time_seconds DESC
                      ) a
      ) , prev2 as (
               SELECT ROWNUM rnum, a.database_name, a.sql_id, a.tps, a.ms_pe, a.sql_text, a.module
                 FROM (
                           SELECT LOWER ( s.database_name ) AS database_name, s.sql_id, 
                                  TO_CHAR ( ROUND ( s.tps , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS tps,
                                  TO_CHAR ( ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS ms_pe, 
                                  LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text, 
                                  LOWER ( s.module ) AS module
                             FROM hp_diag.test_result_sql s
                             LEFT OUTER JOIN test_result_sqltext st ON st.sql_id = s.sql_id
                            WHERE database_name = upper(i_dbname)
                              and test_id = l_testId3
                              AND lower(s.module) not in('dbms_scheduler')
                           ORDER BY elapsed_time_seconds DESC
                      ) a
      ) , prev3 as (
               SELECT ROWNUM rnum, a.database_name, a.sql_id, a.tps, a.ms_pe, a.sql_text, a.module
                 FROM (
                           SELECT LOWER ( s.database_name ) AS database_name, s.sql_id, 
                                  TO_CHAR ( ROUND ( s.tps , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS tps,
                                  TO_CHAR ( ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS ms_pe, 
                                  LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text, 
                                  LOWER ( s.module ) AS module
                             FROM hp_diag.test_result_sql s
                             LEFT OUTER JOIN test_result_sqltext st ON st.sql_id = s.sql_id
                            WHERE database_name = upper(i_dbname)
                              and test_id = l_testId4
                              AND lower(s.module) not in('dbms_scheduler')
                           ORDER BY elapsed_time_seconds DESC
                      ) a
          ) , cur as (
               SELECT ROWNUM rnum, a.database_name, a.sql_id, a.tps, a.ms_pe, a.sql_text, a.module
                 FROM (
                           SELECT LOWER ( s.database_name ) AS database_name, s.sql_id, 
                                  TO_CHAR ( ROUND ( s.tps , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS tps,
                                  TO_CHAR ( ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS ms_pe,
                                  LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text, 
                                  LOWER ( s.module ) AS module
                             FROM hp_diag.test_result_sql s
                             LEFT OUTER JOIN test_result_sqltext st ON st.sql_id = s.sql_id
                            WHERE database_name = upper(i_dbname)
                              and test_id = l_testId1
                              AND lower(s.module) not in('dbms_scheduler')
                           ORDER BY elapsed_time_seconds DESC
                      ) a
                WHERE ROWNUM <= i_limit
             )
      select '"' || cur.sql_id
             || '","' || cur.database_name
             || '","' || cur.tps
             || '","' || nvl(to_char(prev.tps), 'N/A')
             || '","' || nvl(to_char(prev2.tps), 'N/A')
             || '","' || nvl(to_char(prev3.tps), 'N/A')
             || '","' || cur.ms_pe
             || '","' || nvl(to_char(prev.ms_pe), 'N/A')
             || '","' || nvl(to_char(prev2.ms_pe), 'N/A')
             || '","' || nvl(to_char(prev3.ms_pe), 'N/A')
             || '","' || cur.sql_text
             || '","' || cur.module
             || '"' as col1
        from cur, prev, prev2, prev3
       where cur.database_name = prev.database_name (+)
         and cur.database_name = prev2.database_name (+)
         and cur.database_name = prev3.database_name (+)
         and cur.sql_id = prev.sql_id (+)
         and cur.sql_id = prev2.sql_id (+)
         and cur.sql_id = prev3.sql_id (+)
      order by cur.rnum
   )
   LOOP
      PIPE ROW ( r1.col1 ) ;
   END LOOP ;
   PIPE ROW ( '{csv}' ) ;
END Get_top_n_long_comp_db ;


FUNCTION Get_load_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE ,
        i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED
AS
   l_metric_name g_tvc2 ;
   l_idx VARCHAR2(100) ;
   l_header_row VARCHAR2(4000) ;

   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;
BEGIN
   -- 1) Set default parameters
   l_testId1 := i_testId1 ;
   l_testId2 := i_testId2 ;

   --PIPE ROW ( 'h3. Load Comparison' ) ;
   PIPE ROW ( 'h5. Load Comparison With Previous Releases - DB Time Per Second' ) ;
   PIPE ROW ( 'The aim of this info is to show how the previous tests compare to each other regarding the overall load put on the individual databases' ) ;
   PIPE ROW ( '' ) ;

   -- 2) return data
   PIPE ROW ( '{chart:type=bar | 3D = true | width=750 | height=750 | orientation = horizontal }' ) ;
      WITH q AS (
           SELECT DISTINCT database_name
            FROM hp_diag.test_result_db_stats
           WHERE test_id IN ( l_testId1 , l_testId2 ) )
      SELECT '|| TEST NAME || ' || LISTAGG ( database_name  , '||' ) WITHIN GROUP ( ORDER BY database_name ) || '||' 
      INTO l_header_row
        FROM q ;
      PIPE ROW ( l_header_row ) ;

   -- 2.2) Chart detail rows.
   FOR r1 IN (
      SELECT '|' || s.test_id || '|' || LISTAGG ( ROUND ( NVL ( s.db_time_per_sec , 0 ) , 2 ) , '|' )
             WITHIN GROUP ( ORDER BY s.database_name ) || '|' AS text_output
        FROM hp_diag.test_result_db_stats s
       WHERE s.test_id IN ( l_testId1 , l_testId2 ) 
       GROUP BY s.test_id
       ORDER BY TO_DATE ( SUBSTR ( s.test_id , 1 , 7 ) , 'DDMONYY' ) DESC
   )
   LOOP
      PIPE ROW ( r1.text_output ) ;
   END LOOP ;

   -- 2.3) Chart footer rows.
   PIPE ROW ( '{chart}' ) ;

   -----------------------------------------------------------------------------------------------------------------------------
   PIPE ROW ( 'h5. Load Comparison With Previous Releases - DB CPU Per Second' ) ;
   PIPE ROW ( '{chart:type=bar | 3D = true | width=750 | height=750 | orientation = horizontal }' ) ;
      WITH q AS (
           SELECT DISTINCT database_name
            FROM hp_diag.test_result_db_stats
           WHERE test_id IN ( l_testId1 , l_testId2 ) )
      SELECT '|| LIST_TESTS || ' || LISTAGG ( database_name  , '||' ) WITHIN GROUP ( ORDER BY database_name ) || '||' 
  INTO l_header_row
        FROM q ;
      PIPE ROW ( l_header_row ) ;

   -- 2.2) Chart detail rows.
   FOR r1 IN (
      SELECT '|' || s.test_id || '|' || LISTAGG ( ROUND ( NVL ( s.db_cpu_per_sec , 0 ) , 2 ) , '|' )
             WITHIN GROUP ( ORDER BY s.database_name ) || '|' AS text_output
        FROM hp_diag.test_result_db_stats s
       WHERE s.test_id IN ( l_testId1 , l_testId2 ) 
       GROUP BY s.test_id
       ORDER BY TO_DATE ( SUBSTR ( s.test_id , 1 , 7 ) , 'DDMONYY' ) DESC
   )
   LOOP
      PIPE ROW ( r1.text_output ) ;
   END LOOP ;

   -- 2.3) Chart footer rows.
   PIPE ROW ( '{chart}' ) ;

   -----------------------------------------------------------------------------------------------------------------------------
   PIPE ROW ( 'h5. Load Comparison With Previous Releases - Execs Per Second' ) ;
   PIPE ROW ( '{chart:type=bar | 3D = true | width=750 | height=750 | orientation = horizontal }' ) ;
      WITH q AS (
           SELECT DISTINCT database_name
            FROM hp_diag.test_result_db_stats
           WHERE test_id IN ( l_testId1 , l_testId2 ) )
      SELECT '|| LIST_TESTS || ' || LISTAGG ( database_name  , '||' ) WITHIN GROUP ( ORDER BY database_name ) || '||' 
  INTO l_header_row
        FROM q ;
      PIPE ROW ( l_header_row ) ;

   -- 2.2) Chart detail rows.
   FOR r1 IN (
      SELECT '|' || s.test_id || '|' || LISTAGG ( ROUND ( NVL ( s.execs_per_sec , 0 ) , 2 ) , '|' )
             WITHIN GROUP ( ORDER BY s.database_name ) || '|' AS text_output
        FROM hp_diag.test_result_db_stats s
       WHERE s.test_id IN ( l_testId1 , l_testId2 ) 
       GROUP BY s.test_id
       ORDER BY TO_DATE ( SUBSTR ( s.test_id , 1 , 7 ) , 'DDMONYY' ) DESC
   )
   LOOP
      PIPE ROW ( r1.text_output ) ;
   END LOOP ;

   -- 2.3) Chart footer rows.
   PIPE ROW ( '{chart}' ) ;
END Get_load_comparison ;


FUNCTION Get_all_db_top25_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Best previous release'
      ) RETURN g_tvc2 PIPELINED
AS
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;

   l_avg_1   number;
   l_max_1   number;
   l_avg_2   number;
   l_max_2   number;
   l_avg_pct number;
   l_max_pct number;   
   l_row     varchar2(4000);

   l_test_description varchar2(100);
   l_start_time       varchar2(100); 
   l_end_time         varchar2(100);
begin
   -- 1) Set default parameters
  l_testId1 := i_testId1 ;
  l_testId2 := i_testId2 ;

  FOR r_dbName IN (
    SELECT distinct t.database_name
      FROM hp_diag.test_result_sql t
      WHERE t.test_id = l_testId1
    ORDER BY 1
  )
  LOOP
    -- pipe header
    l_row := 'h5. SQL Comparison for ' || r_dbName.database_name ;
    PIPE ROW ( l_row ) ;

    PIPE ROW ( '{csv:allowExport=true|sortIcon=true|columnTypes=s,i,i,s,s,s,s,s,s,f,f,f,f,f,f,f,f,f,f,f,f,s,s|rowStyles=,background:lightblue,background:darkgrey}' ) ;    
    FOR rh IN (
--      SELECT MIN ( CASE m.test_id WHEN l_testId1 THEN m.test_description END ) AS test_description1
--           , MIN ( CASE m.test_id WHEN l_testId2 THEN m.test_description END ) AS test_description2
      SELECT MIN ( CASE m.test_id WHEN l_testId1 THEN 'CURR' END ) AS desc1
           , MIN ( CASE m.test_id WHEN l_testId2 THEN 'PREV' END ) AS desc2
        FROM hp_diag.test_result_master m
       WHERE m.test_id IN ( l_testId1 , l_testId2 )
    )
    LOOP
       l_row := '"SQL ID",'||
                '"'||rh.desc1||' Position",'||
                '"'||rh.desc2||' Position",'||
                '"Total Elapsed Diff",'||
                '"TPS Diff",'||
                '"ms Per Exec Diff",'||
                '"Rows Per Exec Diff",'||
                '"CPU Per Exec Diff",'||
                '"BG Per Exec Diff",'||
                '"'||rh.desc1||' Total Elapsed",'||
                '"'||rh.desc2||' Total Elapsed",'||
                '"'||rh.desc1||' TPS",'||
                '"'||rh.desc2||' TPS",'||
                '"'||rh.desc1||' ms Per Exec",'||
                '"'||rh.desc2||' ms Per Exec",'||
                '"'||rh.desc1||' Rows Per Exec",'||
                '"'||rh.desc2||' Rows Per Exec",'||
                '"'||rh.desc1||' CPU Per Exec",'||
                '"'||rh.desc2||' CPU Per Exec",'||
                '"'||rh.desc1||' BG Per Exec",'||
                '"'||rh.desc2||' BG Per Exec",'||
                '"SQL Text",'||
                '"Module"';
       PIPE ROW ( l_row ) ;
       --PIPE ROW ( '"SQL ID","Position","Prev Position","Total Elapsed Diff","TPS Diff","ms Per Exec Diff","Rows Per Exec Diff","CPU Per Exec Diff","BG Per Exec Diff","Total Elapsed","Prev Total Elapsed","TPS","Prev TPS","ms Per Exec","Prev ms Per Exec","Rows Per Exec","Prev Rows Per Exec","CPU Per Exec","Prev CPU Per Exec","BG Per Exec","Prev BG Per Exec","SQL Text","Module"');
    END LOOP ;

    for r1 in (
      with prev as (
        SELECT ROWNUM rnum, a.sql_id, a.tps, a.ms_pe, a.rpe, a.cpu_pe, a.bg_pe, a.ela_sec, a.sql_text, a.module, a.database_name  
          FROM (
                 SELECT s.sql_id, s.database_name,
                        ROUND ( s.tps , 2 ) AS tps,
                        ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 )  AS ms_pe, 
                        ROUND ( s.rows_per_exec, 2 )  AS rpe,
                        ROUND ( s.cpu_time_per_exec_seconds * 1000 , 2 )  AS cpu_pe,
                        ROUND ( s.buffer_gets_per_exec, 2 )  AS bg_pe,
                        round(elapsed_time_seconds,2) ela_sec,
                        LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text, 
                        LOWER ( s.module ) AS module
                   FROM hp_diag.test_result_sql s
                  LEFT OUTER JOIN test_result_sqltext st ON st.sql_id = s.sql_id
                   WHERE test_id = l_testId2
                 AND database_name = r_dbName.database_name
              ORDER BY top_sql_number ) a ),
           cur as ( SELECT ROWNUM rnum, a.sql_id, a.tps, a.ms_pe, a.rpe, a.cpu_pe, a.bg_pe, a.sql_text, a.ela_sec, a.module, a.database_name    
                    FROM (
                             SELECT s.sql_id, s.database_name,
                             ROUND ( s.tps , 2 ) AS tps,
                           ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 )  AS ms_pe,
                             ROUND ( s.rows_per_exec, 2 )  AS rpe,
                                    ROUND ( s.cpu_time_per_exec_seconds * 1000 , 2 )  AS cpu_pe,
                             ROUND ( s.buffer_gets_per_exec, 2 )  AS bg_pe,
                           round(elapsed_time_seconds,2) ela_sec,
                                    LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text, 
                           LOWER ( s.module ) AS module
                        FROM hp_diag.test_result_sql s
                      LEFT OUTER JOIN test_result_sqltext st ON st.sql_id = s.sql_id
                       WHERE test_id = l_testId1
                         AND lower(s.module) not in('dbms_scheduler')
                         AND top_sql_number <= 15
                         AND database_name = r_dbName.database_name
                      ORDER BY top_sql_number
                    ) a
               )
        select '"' || cur.sql_id
               || '","' || cur.rnum 
               || '","' || nvl(to_char(prev.rnum), 'N/A') 
               || '","' || case WHEN prev.ela_sec > 0 THEN round((cur.ela_sec-prev.ela_sec)/prev.ela_sec,4)*100 || '%' ELSE 'N/A' END 
               || '","' || case WHEN prev.tps > 0     THEN round((cur.tps-prev.tps)/prev.tps,4)*100 || '%'             ELSE 'N/A' END 
               || '","' || case WHEN prev.ms_pe > 0   THEN round((cur.ms_pe-prev.ms_pe)/prev.ms_pe,4)*100 || '%'       ELSE 'N/A' END 
               || '","' || case WHEN prev.rpe > 0     THEN round((cur.rpe-prev.rpe)/prev.rpe,4)*100 || '%'             ELSE 'N/A' END 
               || '","' || case WHEN prev.cpu_pe > 0  THEN round((cur.cpu_pe-prev.cpu_pe)/prev.cpu_pe,4)*100 || '%'    ELSE 'N/A' END 
               || '","' || case WHEN prev.bg_pe > 0   THEN round((cur.bg_pe-prev.bg_pe)/prev.bg_pe,4)*100 || '%'       ELSE 'N/A' END 
               || '","' || cur.ela_sec
               || '","' || nvl(to_char(prev.ela_sec), 'N/A')
               || '","' || cur.tps
               || '","' || nvl(to_char(prev.tps), 'N/A')
               || '","' || cur.ms_pe
               || '","' || nvl(to_char(prev.ms_pe), 'N/A')
               || '","' || cur.rpe
               || '","' || nvl(to_char(prev.rpe), 'N/A')
               || '","' || cur.cpu_pe 
               || '","' || nvl(to_char(prev.cpu_pe), 'N/A') 
               || '","' || cur.bg_pe 
               || '","' || nvl(to_char(prev.bg_pe), 'N/A') 
               || '","' || cur.sql_text
               || '","' || cur.module
               || '"' as col1
          from cur, prev
         where cur.database_name = prev.database_name (+)
          and cur.sql_id = prev.sql_id (+)
        order by cur.rnum )
     LOOP
       PIPE ROW ( r1.col1 ) ;
     END LOOP ;
     PIPE ROW ( '{csv}' ) ;
  end loop;
end Get_all_db_top25_comparison;


FUNCTION Get_rlw_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Best previous release'
      ) RETURN g_tvc2 PIPELINED
AS
   l_metric_name g_tvc2 ;
   l_idx VARCHAR2(100) ;
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;

   l_desc1  varchar2(30);
   l_desc2  varchar2(30);

   l_row     varchar2(4000);
BEGIN
   -- 1) Set default parameters
   l_testId1 := i_testId1 ;
   l_testId2 := i_testId2 ;
   l_desc1 :=  'Curr' ;
   l_desc2 :=  'Prev' ;

   l_row := 'h5. Top 25 Row Lock Waits Across Database by Total Waited Time' ;
   PIPE ROW ( l_row ) ;

   PIPE ROW ( '{csv:allowExport=true|columnTypes=s,s,s,i,s,f,f,f,f,f,f,f,f,f,f|id=Top25RLWComparison}' ) ;
/*
   FOR rh IN (
--      SELECT MIN ( CASE m.test_id WHEN l_testId1 THEN m.test_description END ) AS test_description1
--           , MIN ( CASE m.test_id WHEN l_testId2 THEN m.test_description END ) AS test_description2
      SELECT MIN ( CASE m.test_id WHEN l_testId1 THEN 'Curr' END ) AS test_description1
           , MIN ( CASE m.test_id WHEN l_testId2 THEN 'Prev' END ) AS test_description2
        FROM hp_diag.test_result_master m
       WHERE m.test_id IN ( l_testId1 , l_testId2 )
   )
   LOOP
    PIPE ROW ( '"Object Owner",'||
                 '"Object Name",'||
                 '"Object Type",'||
                 '"Position",'||
                 '"Database Name",'||
                 '"' || rh.test_description1 || ' Num Waits",'||
                 '"' || rh.test_description2 || ' Num Waits",'||
                 '"' || rh.test_description1 || ' Min Wait (ms)",'||
                 '"' || rh.test_description2 || ' Min Wait (ms)",'||
                 '"' || rh.test_description1 || ' Max Wait (ms)",'||
                 '"' || rh.test_description2 || ' Max Wait (ms)",'||
                 '"' || rh.test_description1 || ' Avg Wait (ms)",'||
                 '"' || rh.test_description2 || ' Avg Wait (ms)",'||
                 '"' || rh.test_description1 || ' Total Wait (ms)",'||
                 '"' || rh.test_description2 || ' Total Wait (ms)"');
   END LOOP ;
*/
   PIPE ROW ( '"Object Owner",'||
              '"Object Name",'||
              '"Object Type",'||
              '"Position",'||
              '"Database Name",'||
              '"' || l_desc1 || ' Num Waits",'||
              '"' || l_desc2 || ' Num Waits",'||
              '"' || l_desc1 || ' Min Wait (ms)",'||
              '"' || l_desc2 || ' Min Wait (ms)",'||
              '"' || l_desc1 || ' Max Wait (ms)",'||
              '"' || l_desc2 || ' Max Wait (ms)",'||
              '"' || l_desc1 || ' Avg Wait (ms)",'||
              '"' || l_desc2 || ' Avg Wait (ms)",'||
              '"' || l_desc1 || ' Total Wait (ms)",'||
              '"' || l_desc2 || ' Total Wait (ms)"');
   for r1 in (
         with prev as (
                  select rownum rnum, a.database_name, a.object_owner, a.object_name, a.object_type,
                         a.num_waits, a.min_wait_time, a.max_wait_time, a.avg_wait, a.total_wait_time 
                    from ( SELECT rownum rnum, LOWER ( s.database_name ) AS database_name, 
                                  s.object_owner, s.object_name, s.object_type, 
                                  s.num_waits, s.min_wait_time, s.max_wait_time, 
                                  round(s.avg_wait_time, 2) avg_wait, s.total_wait_time
                             FROM hp_diag.test_result_rlw s
                            WHERE test_id = l_testId2
                           ORDER BY total_wait_time DESC
                         ) a
            ) , cur as (
                  select rownum rnum, a.database_name, a.object_owner, a.object_name, a.object_type,
                         a.num_waits, a.min_wait_time, a.max_wait_time, a.avg_wait, a.total_wait_time
                    from ( SELECT LOWER ( s.database_name ) AS database_name, 
                                  s.object_owner, s.object_name, s.object_type, 
                                  s.num_waits, s.min_wait_time, s.max_wait_time, 
                                  round(s.avg_wait_time, 2) avg_wait, s.total_wait_time
                             FROM hp_diag.test_result_rlw s
                            WHERE test_id = l_testId1
                           ORDER BY total_wait_time DESC
                          ) a
                   where rownum <= 25
                )
         select '"' || cur.object_owner
                    || '","' || cur.object_name
                    || '","' || cur.object_type
                    || '","' || cur.rnum
                    || '","' || cur.database_name
                    || '","' || cur.num_waits
                    || '","' || prev.num_waits
                    || '","' || cur.min_wait_time
                    || '","' || prev.min_wait_time
                    || '","' || cur.max_wait_time
                    || '","' || prev.max_wait_time
                    || '","' || cur.avg_wait
                    || '","' || prev.avg_wait
                    || '","' || cur.total_wait_time
                    || '","' || prev.total_wait_time
                    || '"' as col1
           from cur, prev
          where cur.database_name = prev.database_name (+)
            and cur.object_name   = prev.object_name (+)
            and cur.object_type   = prev.object_type (+)
            order by cur.rnum 
   )
   LOOP
      PIPE ROW ( r1.col1 ) ;
   END LOOP ;
   PIPE ROW ( '{csv}' ) ;

END Get_rlw_comparison ;


END REPORT_COMP;
/
