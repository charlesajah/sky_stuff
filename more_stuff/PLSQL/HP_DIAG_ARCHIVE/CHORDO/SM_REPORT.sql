CREATE OR REPLACE PACKAGE         sm_report
/* N01
|| Name : hp_diag.report
|| Database : chordo (N01) and ccs021n (N02)
|| Usage : SELECT * FROM TABLE ( hp_diag.report.chart_comparison )
||    Then in Confluence, CTRL-SHIFT-D and paste in the output.
|| Parameters : optionally specify specific test_ids in one or both input parameters
|| Change History:
||    06-Apr-2017 Andrew Fraser Initial version
*/
AS
   TYPE g_tvc2 IS TABLE OF VARCHAR2(4000) ;
   FUNCTION chart_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Best previous release'
      , i_mslp IN VARCHAR2
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION pct_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Best previous release'
      , i_mslp IN VARCHAR2
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION top15_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Best previous release'
      , i_mslp IN VARCHAR2
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION top15_detail (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_mslp IN VARCHAR2
      ) RETURN g_tvc2 PIPELINED;

   FUNCTION sal_basket_sizes (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      ) RETURN g_tvc2 PIPELINED;

   FUNCTION ccs_prod_sql_comparison  (
        i_testId1      IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_release      IN hp_diag.prod_sql.release_version%TYPE
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION sal_prod_sql_comparison  (
        i_testId1      IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_release      IN hp_diag.prod_sql.release_version%TYPE
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION oms_prod_sql_comparison  (
        i_testId1      IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_release      IN hp_diag.prod_sql.release_version%TYPE
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION top15_long_comp (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_mslp IN VARCHAR2
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION load_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_mslp IN VARCHAR2
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION rlw_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Best previous release'
      , i_mslp IN VARCHAR2
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION top15_long_comp_db (
        i_dbname  IN varchar2,
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      ) RETURN g_tvc2 PIPELINED
;
FUNCTION top_n_long_comp_db (
     i_dbname IN varchar2
   , i_limit IN number DEFAULT 25
   , i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
) RETURN g_tvc2 PIPELINED
;
   FUNCTION load_comparison_4tests (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE ,
        i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE ,
        i_testId3 IN hp_diag.test_result_metrics.test_id%TYPE ,
        i_testId4 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_mslp IN VARCHAR2
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION all_db_top15_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Best previous release'
      , i_mslp IN VARCHAR2
      ) RETURN g_tvc2 PIPELINED ;
END sm_report ;
/


CREATE OR REPLACE PACKAGE BODY         sm_report
/* N01
|| Name : hp_diag.report
|| Database : chordo (N01) and ccs021n (N02)
|| Usage : SELECT * FROM TABLE ( hp_diag.report.chart_comparison )
||    Then in Confluence, CTRL-SHIFT-D and paste in the output.
|| Parameters : optionally specify specific test_ids in one or both input parameters
|| Change History:
||    16-Apr-2021 Andrew Fraser removed 'h5. Database Activity' headers - now in graphs.lst
||    31-Aug-2017 Andrew Fraser added comparison with previous release in numbers.
||    23-May-2017 Andrew Fraser NVLs to cope with null test_descriptions
||    06-Apr-2017 Andrew Fraser Initial version
*/
AS

FUNCTION chart_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Best previous release'
      , i_mslp IN varchar2
      ) RETURN g_tvc2 PIPELINED
AS
   l_metric_name g_tvc2 ;
   l_idx VARCHAR2(100) ;
   l_header_row VARCHAR2(4000) ;
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId3 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId4 hp_diag.test_result_metrics.test_id%TYPE ;
BEGIN
   PIPE ROW ( 'h5. Comparison With Previous Release' ) ;

   -- 1) Set default parameters
   IF i_testId1 = 'Latest test'  -- the default
   THEN
      SELECT MAX ( test_id ) KEEP ( DENSE_RANK FIRST ORDER BY begin_time DESC ) INTO l_testId1
        FROM hp_diag.test_result_master
      ;
   ELSE
      l_testId1 := i_testId1 ;
   END IF ;
   IF i_testId2 = 'Best previous release'  -- the default
   THEN
      SELECT MAX ( test_id ) KEEP ( DENSE_RANK FIRST ORDER BY begin_time DESC ) INTO l_testId2
        FROM hp_diag.test_result_master
       WHERE best_test_for_release IS NOT NULL
      ;
   ELSE
      l_testId2 := i_testId2 ;
   END IF ;
   select test_id 
     into l_testId3
     from (select rownum rnum, test_id
             from (select test_id from test_result_master where best_test_for_release is not null order by begin_time desc))
    where rnum = 2;   
   select test_id 
     into l_testId4
     from (select rownum rnum, test_id
             from (select test_id from test_result_master where best_test_for_release is not null order by begin_time desc))
    where rnum = 3;

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
            AND test_id IN ( l_testId1 , l_testId2, l_testId3, l_testId4 )
            AND  database_name in (SELECT
REGEXP_SUBSTR (
  i_mslp,
  '[^,]+', 1, level) AS string_parts
FROM dual
CONNECT BY REGEXP_SUBSTR (
  i_mslp,
  '[^,]+', 1, level) IS NOT NULL)
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
            AND test_id IN ( l_testId1 , l_testId2, l_testId3, l_testId4 )
            AND  database_name in (SELECT
REGEXP_SUBSTR (
  i_mslp,
  '[^,]+', 1, level) AS string_parts
FROM dual
CONNECT BY REGEXP_SUBSTR (
  i_mslp,
  '[^,]+', 1, level) IS NOT NULL)
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
      with q as (select distinct wait_class from TEST_RESULT_WAIT_CLASS where test_id IN (l_testId1, l_testId2, l_testId3, l_testId4) order by 1)
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
                   where test_id in (l_testId1 , l_testId2, l_testId3, l_testId4)
                     and database_name = rh.database_name
                     AND  database_name in (SELECT
                                              REGEXP_SUBSTR (
                                               i_mslp,
                                               '[^,]+', 1, level) AS string_parts
                                            FROM dual
                                            CONNECT BY REGEXP_SUBSTR (
                                               i_mslp,
                                               '[^,]+', 1, level) IS NOT NULL)
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
      SELECT MIN ( CASE m.test_id WHEN l_testId1 THEN m.test_description END ) AS test_description1
           , MIN ( CASE m.test_id WHEN l_testId2 THEN m.test_description END ) AS test_description2
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
            AND  database_name in (SELECT
                              REGEXP_SUBSTR (
                                 i_mslp,
                                 '[^,]+', 1, level) AS string_parts
                              FROM dual
                              CONNECT BY REGEXP_SUBSTR (
                               i_mslp,
                               '[^,]+', 1, level) IS NOT NULL)
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
      SELECT MIN ( CASE m.test_id WHEN l_testId1 THEN m.test_description END ) AS test_description1
           , MIN ( CASE m.test_id WHEN l_testId2 THEN m.test_description END ) AS test_description2
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
            AND  database_name in (SELECT
                              REGEXP_SUBSTR (
                                 i_mslp,
                                 '[^,]+', 1, level) AS string_parts
                              FROM dual
                              CONNECT BY REGEXP_SUBSTR (
                               i_mslp,
                               '[^,]+', 1, level) IS NOT NULL)
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
      SELECT MIN ( CASE m.test_id WHEN l_testId1 THEN m.test_description END ) AS test_description1
           , MIN ( CASE m.test_id WHEN l_testId2 THEN m.test_description END ) AS test_description2
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
END chart_comparison ;

FUNCTION pct_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Best previous release'
      , i_mslp IN VARCHAR2
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
   -- Wait classes we are intersted in
   l_metric_name := NEW g_tvc2 ( 'Administrative', 'Application', 'CPU', 'Commit', 'Concurrency', 'Configuration', 'Idle', 'Network', 'Other', 'Queueing', 'Scheduler', 'System I/O', 'User I/O' ) ;

   -- 1) Set default parameters
   IF i_testId1 = 'Latest test'  -- the default
   THEN
      SELECT MAX ( test_id ) KEEP ( DENSE_RANK FIRST ORDER BY begin_time DESC ) INTO l_testId1
        FROM hp_diag.test_result_master
      ;
   ELSE
      l_testId1 := i_testId1 ;
   END IF ;
   IF i_testId2 = 'Best previous release'  -- the default
   THEN
      SELECT MAX ( test_id ) KEEP ( DENSE_RANK FIRST ORDER BY begin_time DESC ) INTO l_testId2
        FROM hp_diag.test_result_master
       WHERE best_test_for_release IS NOT NULL
      ;
   ELSE
      l_testId2 := i_testId2 ;
   END IF ;

   -- loop through DBs in the test
   --FOR db_name in (SELECT DISTINCT database_name FROM hp_diag.test_result_metrics WHERE test_id IN ( l_testId1 , l_testId2 ) order by 1)
   FOR r_dbName IN (
      SELECT DISTINCT t.database_name
        FROM hp_diag.test_result_wait_class t
       WHERE t.test_id = l_testId1
         AND t.avg_sessions IS NOT NULL
         AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
       ORDER BY 1
   )
   LOOP
     -- pipe header
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
                           AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
                        group by test_id),
                 t2 as (SELECT nvl(max(decode(wait_class, l_metric_name(l_idx), avg_sessions)),0) avg_val
                          FROM hp_diag.TEST_RESULT_WAIT_CLASS a
                         where test_id = l_testId2
                           and database_name = r_dbName.database_name
                           AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
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
END pct_comparison ;

FUNCTION top15_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Best previous release'
      , i_mslp IN VARCHAR2
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
   IF i_testId1 = 'Latest test'  -- the default
   THEN
      SELECT MAX ( test_id ) KEEP ( DENSE_RANK FIRST ORDER BY begin_time DESC ) INTO l_testId1
        FROM hp_diag.test_result_master
      ;
   ELSE
      l_testId1 := i_testId1 ;
   END IF ;
   IF i_testId2 = 'Best previous release'  -- the default
   THEN
      SELECT MAX ( test_id ) KEEP ( DENSE_RANK FIRST ORDER BY begin_time DESC ) INTO l_testId2
        FROM hp_diag.test_result_master
       WHERE best_test_for_release IS NOT NULL
      ;
   ELSE
      l_testId2 := i_testId2 ;
   END IF ;

   PIPE ROW ( 'h5. Top 25 SQL Comparison Across Database by Elapsed Time' ) ;
   -- 17-Apr-2023 Andrew Fraser add RAG status list
   PIPE ROW ( '||Overall Status||Sql Id||Database||TPS Status||Duration Status||Details||' ) ;
   FOR r1 IN (
      WITH cur AS (
         SELECT ROWNUM AS rnum , a.database_name , a.sql_id , a.tps , a.elapsed_time_per_exec_seconds AS els , a.test_description
           FROM (
                  SELECT s.*
                    FROM hp_diag.test_result_sql s
                   WHERE s.test_id = l_testId1
                   AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
                  ORDER BY s.elapsed_time_seconds DESC
                ) a
          WHERE ROWNUM <= 25
      ) , prev AS (
         SELECT s.database_name , s.sql_id , s.tps , s.elapsed_time_per_exec_seconds AS els
           FROM hp_diag.test_result_sql s
          WHERE s.test_id = l_testId2
          AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
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
             || '}|[details|https://confluence.bskyb.com/display/nonfuntst/Sql Analysis '
                || c.test_description || '#SqlAnalysis'
                || REPLACE ( REPLACE ( c.test_description , ' ' ) , '-' )
                || '-SQLHistoryfor' || c.sql_id || 'from' || c.database_name || ']|' AS col1
        FROM c
       ORDER BY c.rnum
   )
   LOOP
      PIPE ROW ( r1.col1 ) ;
   END LOOP ;
   -- Future Enhancement - maybe add ||Network Latency||Disk Latency|| to the below?
/*
   PIPE ROW ( 'h5. Database Metrics' ) ;
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
            AND t.database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
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
            AND  t.database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
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
*/
   PIPE ROW ( 'h5. Top 25 SQL Comparison Across Database by Elapsed Time (with values)' ) ;
   -- end of 17-Apr-2023 RAG additions
   PIPE ROW ( '{csv:allowExport=true|columnTypes=s,i,i,s,f,f,f,f,f,s,s|id=Top25Comparison}' ) ;
   FOR rh IN (
      SELECT MIN ( CASE m.test_id WHEN l_testId1 THEN m.test_description END ) AS test_description1
           , MIN ( CASE m.test_id WHEN l_testId2 THEN m.test_description END ) AS test_description2
        FROM hp_diag.test_result_master m
       WHERE m.test_id IN ( l_testId1 , l_testId2 )
   )
   LOOP
      PIPE ROW ( '"SQL ID","Position","' || NVL ( rh.test_description2, l_testid2 ) || ' Position","Database Name","TPS Diff","TPS","' || NVL ( rh.test_description2 , l_testid2 ) || ' TPS","ms Per Exec","' || NVL ( rh.test_description2 , l_testid2 ) || ' ms Per Exec","SQL Text","Module"');
   END LOOP ;

   --PIPE ROW ( '"SQL ID","Position","' || l_testId2 || ' Position","Database Name","TPS Diff","TPS","' || l_testId2 || ' TPS","ms Per Exec","' || l_testId2 || ' ms Per Exec","SQL Text","Module"');
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
                            AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
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
                            WHERE test_id = l_testId1
                            AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
                           ORDER BY elapsed_time_seconds DESC
                      ) a
                WHERE ROWNUM <= 25
           )
      SELECT '"' || cur.sql_id
             || '","' || cur.rnum 
             || '","' || nvl(to_char(prev.rnum), 'N/A') 
             || '","' || cur.database_name
             || '","' || case WHEN prev.tps > 0     THEN round((cur.tps-prev.tps)/prev.tps,4)*100 || '%'             ELSE 'N/A' END 
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

END top15_comparison ;

FUNCTION top15_detail (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_mslp IN VARCHAR2
      ) RETURN g_tvc2 PIPELINED
AS
   l_metric_name g_tvc2 ;
   l_idx VARCHAR2(100) ;
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;

   l_avg_1   number;
   l_max_1   number;
   l_avg_2   number;
   l_max_2   number;
   l_avg_pct number;
   l_max_pct number;   
   l_row     varchar2(4000);
BEGIN
   -- 1) Set default parameters
   IF i_testId1 = 'Latest test'  -- the default
   THEN
      SELECT MAX ( test_id ) KEEP ( DENSE_RANK FIRST ORDER BY begin_time DESC ) INTO l_testId1
        FROM hp_diag.test_result_master
      ;
   ELSE
      l_testId1 := i_testId1 ;
   END IF ;

   --l_row := 'h3. Top 25 SQL Comparison Across Database by Previous Releases' ;
   --PIPE ROW ( l_row ) ;

   for r1 in (
         with cur as (
               SELECT ROWNUM rnum, a.database_name, a.sql_id
                 FROM (
                           SELECT LOWER ( s.database_name ) AS database_name, s.sql_id, 
                                  TO_CHAR ( ROUND ( s.tps , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS tps,
                                  TO_CHAR ( ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS ms_pe,
                                  LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text, 
                                  LOWER ( s.module ) AS module
                             FROM hp_diag.test_result_sql s
                             LEFT OUTER JOIN test_result_sqltext st ON st.sql_id = s.sql_id
                            WHERE test_id = l_testId1
                            AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
                           ORDER BY elapsed_time_seconds DESC
                      ) a
                WHERE ROWNUM <= 25
         )
      select sql_id, database_name
        from cur
      order by cur.rnum
   )
   LOOP
      PIPE ROW ( 'h5. SQL History for ' || r1.sql_id || ' from ' || r1.database_name ) ;
      -- 19-Apr-2023 Andrew Fraser added sql text
      PIPE ROW ( '||Module||Sql Text||' ) ;
      FOR r2t IN (
         SELECT '|' || st.module || '|'
                || LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE (
                   st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13) , ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) , '  ' , ' ' )
                 , '  ' , ' ' ) , 1 , 100 ) ) )
                || '|' AS col1
           FROM test_result_sqlText st
          WHERE st.sql_id = r1.sql_id
            AND ROWNUM = 1
      )
      LOOP
         PIPE ROW ( r2t.col1 ) ;
      END LOOP ;
      -- end of changes for 19-Apr-2023 Andrew Fraser added sql text
      PIPE ROW ( '{csv:allowExport=true|columnTypes=s,s,f,f,f,f,f,s|id=' || r1.sql_id || r1.database_name || '}' ) ;
      PIPE ROW ( '"Database","Test Description","TPS","Executions","Avg Elapsed ms","Avg Buffer Gets","Avg CPU ms","Plan Hash Values"' ) ;
      FOR r2 IN (
         SELECT LOWER ( s.database_name ) AS database_name
              , s.test_description
              , s.tps
              , s.executions
              , s.elapsed_time_per_exec_seconds * 1000 AS avg_elapsed_ms
              , s.buffer_gets_per_exec
              , s.cpu_time_per_exec_seconds * 1000 AS avg_cpu_ms
              , s.plan_hash_values
           FROM test_result_sql s
          WHERE s.sql_id = r1.sql_id
            AND UPPER ( s.database_name) = UPPER ( r1.database_name )
            AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
            AND s.test_id IN (
                SELECT v2.test_id
                  FROM (
                        SELECT m.test_id
                          FROM test_result_master m
                         WHERE m.best_test_for_release IS NOT NULL
                         ORDER BY m.begin_time DESC
                       ) v2
                 WHERE ROWNUM <= 10
                )
          UNION
         SELECT LOWER ( s.database_name ) AS database_name
              , s.test_description
              , s.tps
              , s.executions
              , s.elapsed_time_per_exec_seconds * 1000 AS avg_elapsed_ms
              , s.buffer_gets_per_exec
              , s.cpu_time_per_exec_seconds * 1000 AS avg_cpu_ms
              , s.plan_hash_values
           FROM test_result_sql s
          WHERE s.sql_id = r1.sql_id
            AND UPPER ( s.database_name ) = UPPER ( r1.database_name )
            AND s.test_id = l_testId1
            AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
          ORDER BY 1,2,3
      )
      LOOP
         l_row := '"' || r2.database_name || '","'
                  || r2.test_description || '","'
                  || TO_CHAR ( r2.tps , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) || '","'
                  || TO_CHAR ( r2.executions , 'FM9,999,999,999,999,999,999,999,999,999,990' ) || '","'
                  || TO_CHAR ( r2.avg_elapsed_ms , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) || '","'
                  || TO_CHAR ( r2.buffer_gets_per_exec , 'FM9,999,999,999,999,999,999,999,999,999,990' ) || '","'
                  || TO_CHAR ( r2.avg_cpu_ms , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) || '","'
                  || r2.plan_hash_values
                  || '"';
         PIPE ROW ( l_row ) ;
      END LOOP ;
      PIPE ROW ( '{csv}' ) ;
   END LOOP ;
END top15_Detail ;

FUNCTION sal_basket_sizes (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      ) RETURN g_tvc2 PIPELINED
AS
   l_metric_name g_tvc2 ;
   l_idx VARCHAR2(100) ;
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;

   l_avg_1   number;
   l_max_1   number;
   l_avg_2   number;
   l_max_2   number;
   l_avg_pct number;
   l_max_pct number;   
   l_row     varchar2(4000);
BEGIN
   -- 1) Set default parameters
   IF i_testId1 = 'Latest test'  -- the default
   THEN
      SELECT MAX ( test_id ) KEEP ( DENSE_RANK FIRST ORDER BY begin_time DESC ) INTO l_testId1
        FROM hp_diag.test_result_master
      ;
   ELSE
      l_testId1 := i_testId1 ;
   END IF ;

   FOR r1 IN ( SELECT DISTINCT lob_type AS lt FROM sal_basket_sizes WHERE test_id = l_testId1 and lob_type in ('portfolio','total_record') ORDER BY 1 )
   LOOP
     l_row := 'h5. SAL Basket Sizes for ' || r1.lt;
     PIPE ROW ( l_row ) ;
     PIPE ROW ( '{csv:allowExport=true|columnTypes=s,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f|id=SalBasket' || r1.lt || '}' ) ;
     PIPE ROW ( '"Created","0-25kb","26-50kb","51-76kb","76-100kb","101-125","126-150kb","151-175kb","176-200kb","200-250kb","251-500kb","501-750kb","751-1000kb","1001-1500kb","1501-2000kb","2001-3000kb","3001-4000kb","4001-5000","5001-6000kb","6001kb +"');
     for r2 in (select to_char(created, 'dd/mm/yyyy hh24:mi:ss') as created, B0_25KB, B26_50KB, B51_76B, B76_100KB, B101_125KB, B126_150KB, B151_175KB, B176_200KB, 
                       B200_250KB, B251_500KB, B501_750KB, B751_1000KB, B1001_1500KB, B1501_2000KB, B2001_3000KB, B3001_4000KB, 
                       B4001_5000KB, B5001_6000KB, B6001KB
                  from SAL_BASKET_SIZES 
                 where test_id = l_testId1
                   and lob_type = r1.lt
                order by lob_type, created)
     loop
         l_row := '"' || r2.created || '","' || r2.B0_25KB || '","' || r2.B26_50KB || '","' || r2.B51_76B || '","' || r2.B76_100KB || 
                  '","' || r2.B101_125KB || '","' || r2.B126_150KB || '","' || r2.B151_175KB || '","' || r2.B176_200KB || 
                  '","' || r2.B200_250KB || '","' || r2.B251_500KB || '","' || r2.B501_750KB || '","' || r2.B751_1000KB || 
                  '","' || r2.B1001_1500KB || '","' || r2.B1501_2000KB || '","' || r2.B2001_3000KB || '","' || r2.B3001_4000KB || 
                  '","' || r2.B4001_5000KB || '","' || r2.B5001_6000KB || '","' || r2.B6001KB || '"';
         PIPE ROW ( l_row ) ;
     end loop;
     PIPE ROW ( '{csv}' ) ;      
   END LOOP ;
   --PIPE ROW ( '{csv}' ) ;

END sal_basket_sizes ;

FUNCTION ccs_prod_sql_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_release IN hp_diag.prod_sql.release_version%TYPE
      ) RETURN g_tvc2 PIPELINED
AS
   l_prod_db_name hp_diag.prod_sql.database_name%TYPE := 'CHORDP' ;
   l_test_db_name hp_diag.prod_sql.database_name%TYPE := CASE WHEN SYS_CONTEXT ( 'userenv' , 'con_name' ) = 'CHORDO' THEN 'CHORDO' ELSE 'CCS021N' END ;
   l_idx VARCHAR2(100) ;
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_row VARCHAR2(4000) ;
BEGIN
   -- 1) Set default parameters
   IF i_testId1 = 'Latest test'  -- the default
   THEN
      SELECT MAX ( test_id ) KEEP ( DENSE_RANK FIRST ORDER BY begin_time DESC ) INTO l_testId1
        FROM hp_diag.test_result_master
      ;
   ELSE
      l_testId1 := i_testId1 ;
   END IF ;

   l_row := 'h5. SQL Comparison With ' || l_prod_db_name ;
   PIPE ROW ( l_row ) ;

   PIPE ROW ( '{csv:allowExport=true|columnTypes=s,i,s,s,f,s,f,s,s,s|id=CHORDPComparison}' ) ;
   PIPE ROW ( '"SQL ID","Position","CHORDP Position","Database Name","TPS","CHORDP TPS","ms Per Exec","CHORDP ms Per Exec","SQL Text","Module"');

   for r1 in (
      with prev as (
               SELECT ROWNUM AS rnum , a.database_name, a.sql_id, a.tps, a.ms_pe
                 FROM (
                           SELECT LOWER ( s.database_name ) AS database_name, 
                                  s.sql_id, 
                                  TO_CHAR ( ROUND ( s.tps , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS tps,
                                  TO_CHAR ( ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS ms_pe
                             FROM hp_diag.prod_sql s
                            WHERE release_version = i_release
                              AND upper(database_name) = l_prod_db_name
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
                            WHERE test_id = l_testId1
                              AND upper(database_name) = l_test_db_name
                           ORDER BY elapsed_time_seconds DESC) a
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
       where cur.sql_id = prev.sql_id (+)
      order by cur.rnum
   )
   LOOP
      PIPE ROW ( r1.col1 ) ;
   END LOOP ;
   PIPE ROW ( '{csv}' ) ;

END ccs_prod_sql_comparison  ;

FUNCTION sal_prod_sql_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_release IN hp_diag.prod_sql.release_version%TYPE
      ) RETURN g_tvc2 PIPELINED
AS
   l_prod_db_name hp_diag.prod_sql.database_name%TYPE := 'ISSAP1P' ;
   l_test_db_name hp_diag.prod_sql.database_name%TYPE := CASE WHEN SYS_CONTEXT ( 'userenv' , 'con_name' ) = 'CHORDO' THEN 'ISS011N' ELSE 'ISS021N' END ;
   l_idx VARCHAR2(100) ;
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_row varchar2(4000);
BEGIN
   -- 1) Set default parameters
   IF i_testId1 = 'Latest test'  -- the default
   THEN
      SELECT MAX ( test_id ) KEEP ( DENSE_RANK FIRST ORDER BY begin_time DESC ) INTO l_testId1
        FROM hp_diag.test_result_master
      ;
   ELSE
      l_testId1 := i_testId1 ;
   END IF ;

   l_row := 'h5. SQL Comparison With ' || l_prod_db_name ;
   PIPE ROW ( l_row ) ;

   PIPE ROW ( '{csv:allowExport=true|columnTypes=s,i,s,s,f,s,f,s,s,s|id=ISSAP1PComparison}' ) ;
   PIPE ROW ( '"SQL ID","Position","ISSAP1P Position","Database Name","TPS","ISSAP1P TPS","ms Per Exec","ISSAP1P ms Per Exec","SQL Text","Module"');

   for r1 in (
      with prev as (
               SELECT ROWNUM rnum, a.database_name, a.sql_id, a.tps, a.ms_pe
                 FROM (
                           SELECT LOWER ( s.database_name ) AS database_name, 
                                  s.sql_id, 
                                  TO_CHAR ( ROUND ( s.tps , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS tps,
                                  TO_CHAR ( ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS ms_pe
                             FROM hp_diag.prod_sql s
                            WHERE release_version = i_release
                              AND upper(database_name) = l_prod_db_name
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
                            WHERE test_id = l_testId1
                              AND upper(database_name) = l_test_db_name
                           ORDER BY elapsed_time_seconds DESC) a
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
       where cur.sql_id = prev.sql_id (+)
      order by cur.rnum
   )
   LOOP
      PIPE ROW ( r1.col1 ) ;
   END LOOP ;
   PIPE ROW ( '{csv}' ) ;

END sal_prod_sql_comparison  ;

FUNCTION oms_prod_sql_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_release IN hp_diag.prod_sql.release_version%TYPE
      ) RETURN g_tvc2 PIPELINED
AS
   l_prod_db_name hp_diag.prod_sql.database_name%TYPE := 'OMSAP1P' ;
   l_test_db_name hp_diag.prod_sql.database_name%TYPE := CASE WHEN SYS_CONTEXT ( 'userenv' , 'con_name' ) = 'CHORDO' THEN 'OMS011N' ELSE 'OMS021N' END ;
   l_idx VARCHAR2(100) ;
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_row     varchar2(4000);
BEGIN
   -- 1) Set default parameters
   IF i_testId1 = 'Latest test'  -- the default
   THEN
      SELECT MAX ( test_id ) KEEP ( DENSE_RANK FIRST ORDER BY begin_time DESC ) INTO l_testId1
        FROM hp_diag.test_result_master
      ;
   ELSE
      l_testId1 := i_testId1 ;
   END IF ;

   l_row := 'h5. SQL Comparison With ' || l_prod_db_name ;
   PIPE ROW ( l_row ) ;

   PIPE ROW ( '{csv:allowExport=true|columnTypes=s,i,s,s,f,s,f,s,s,s|id=OMSAP1PComparison}' ) ;
   PIPE ROW ( '"SQL ID","Position","OMSAP1P Position","Database Name","TPS","OMSAP1P TPS","ms Per Exec","OMSAP1P ms Per Exec","SQL Text","Module"');

   for r1 in (
      with prev as (
               SELECT ROWNUM rnum, a.database_name, a.sql_id, a.tps, a.ms_pe
                 FROM (
                           SELECT LOWER ( s.database_name ) AS database_name, 
                                  s.sql_id, 
                                  TO_CHAR ( ROUND ( s.tps , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS tps,
                                  TO_CHAR ( ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS ms_pe
                             FROM hp_diag.prod_sql s
                            WHERE release_version = i_release
                              AND upper(database_name) = l_prod_db_name
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
                            WHERE test_id = l_testId1
                              AND upper(database_name) = l_test_db_name
                           ORDER BY elapsed_time_seconds DESC ) a
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
       where cur.sql_id = prev.sql_id (+)
      order by cur.rnum
   )
   LOOP
      PIPE ROW ( r1.col1 ) ;
   END LOOP ;
   PIPE ROW ( '{csv}' ) ;

END oms_prod_sql_comparison  ;

FUNCTION top15_long_comp (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_mslp IN VARCHAR2
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
   IF i_testId1 = 'Latest test'  -- the default
   THEN
      SELECT MAX ( test_id ) KEEP ( DENSE_RANK FIRST ORDER BY begin_time DESC ) INTO l_testId1
        FROM hp_diag.test_result_master;
   ELSE
      l_testId1 := i_testId1 ;
   END IF ;

   select test_id 
     into l_testId2
     from (select rownum rnum, test_id
             from (select test_id from test_result_master where best_test_for_release is not null order by begin_time desc))
    where rnum = 1;

   select test_id 
     into l_testId3
     from (select rownum rnum, test_id
             from (select test_id from test_result_master where best_test_for_release is not null order by begin_time desc))
    where rnum = 2;

   select test_id 
     into l_testId4
     from (select rownum rnum, test_id
             from (select test_id from test_result_master where best_test_for_release is not null order by begin_time desc))
    where rnum = 3;

   l_row := 'h5. Top 25 SQL Comparison Across Database by Elapsed Time' ;
   PIPE ROW ( l_row ) ;
   PIPE ROW ( '{csv:allowExport=true|columnTypes=s,s,f,f,f,f,f,f,f,f,s|id=Top25Comparison}' ) ;

   FOR rh IN (
      SELECT MIN ( CASE m.test_id WHEN l_testId1 THEN m.test_description END ) AS test_description1
           , MIN ( CASE m.test_id WHEN l_testId2 THEN m.test_description END ) AS test_description2
           , MIN ( CASE m.test_id WHEN l_testId3 THEN m.test_description END ) AS test_description3
           , MIN ( CASE m.test_id WHEN l_testId4 THEN m.test_description END ) AS test_description4
        FROM hp_diag.test_result_master m
       WHERE m.test_id IN ( l_testId1 , l_testId2 , l_testId3 , l_testId4 )
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
                            WHERE test_id = l_testId2
                            AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
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
                            WHERE test_id = l_testId3
                            AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
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
                            WHERE test_id = l_testId4
                            AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
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
                            WHERE test_id = l_testId1
                            AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
                           ORDER BY elapsed_time_seconds DESC
                      ) a
                WHERE ROWNUM <= 25
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
END top15_long_comp ;

FUNCTION load_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_mslp IN VARCHAR2
      ) RETURN g_tvc2 PIPELINED
AS
   l_metric_name g_tvc2 ;
   l_idx VARCHAR2(100) ;
   l_header_row VARCHAR2(4000) ;

   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId3 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId4 hp_diag.test_result_metrics.test_id%TYPE ;

BEGIN
   -- 1) Set default parameters
   IF (i_testId1 = 'Latest test' or i_testId1 is null) -- the default
   THEN
      SELECT MAX ( test_id ) KEEP ( DENSE_RANK FIRST ORDER BY begin_time DESC ) 
  INTO l_testId1
        FROM hp_diag.test_result_master ;
   ELSE
      l_testId1 := i_testId1 ;
   END IF ;

   IF i_testId1 = 'Latest test'  -- the default
   THEN
      SELECT MAX ( test_id ) KEEP ( DENSE_RANK FIRST ORDER BY begin_time DESC ) INTO l_testId1
        FROM hp_diag.test_result_master;
   ELSE
      l_testId1 := i_testId1 ;
   END IF ;

   select test_id 
     into l_testId2
     from (select rownum rnum, test_id
             from (select test_id from test_result_master where best_test_for_release is not null order by begin_time desc))
    where rnum = 1;

   select test_id 
     into l_testId3
     from (select rownum rnum, test_id
             from (select test_id from test_result_master where best_test_for_release is not null order by begin_time desc))
    where rnum = 2;

   select test_id 
     into l_testId4
     from (select rownum rnum, test_id
             from (select test_id from test_result_master where best_test_for_release is not null order by begin_time desc))
    where rnum = 3;

   PIPE ROW ( 'The aim of this page is to show how the previous tests compare to each other regarding the overall load put on the individual databases' ) ;
   PIPE ROW ( '' ) ;
   PIPE ROW ( 'h5. Load Comparison With Previous Releases - DB Time Per Second' ) ;

   -- 2) return data
   PIPE ROW ( '{chart:type=bar | 3D = true | width=1000 | height=1000 | orientation = horizontal | dataDisplay = after}' ) ;
      WITH q AS (
           SELECT DISTINCT database_name
            FROM hp_diag.test_result_db_stats
           WHERE test_id IN ( l_testId1 , l_testId2, l_testId3, l_testId4 )
           AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL) )
      SELECT '|| TEST NAME || ' || LISTAGG ( database_name  , '||' ) WITHIN GROUP ( ORDER BY database_name ) || '||' 
  INTO l_header_row
        FROM q ;
      PIPE ROW ( l_header_row ) ;

    -- 2.2) Chart detail rows.
    FOR r1 IN ( SELECT test_id , '|' || test_id || '|' || 
                      LISTAGG ( round(nvl(db_time_per_sec,0),2), '|' ) WITHIN GROUP ( ORDER BY database_name ) || '|' AS text_output
                  FROM hp_diag.test_result_db_stats
             WHERE test_id IN ( l_testId1 , l_testId2, l_testId3, l_testId4 ) 
             AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
            GROUP BY test_id 
            ORDER BY test_id 
      )
      LOOP
         PIPE ROW ( r1.text_output ) ;
      END LOOP ;  -- FOR r1 IN

    -- 2.3) Chart footer rows.
    PIPE ROW ( '{chart}' ) ;

   -----------------------------------------------------------------------------------------------------------------------------
   PIPE ROW ( 'h5. Load Comparison With Previous Releases - DB CPU Per Second' ) ;
   PIPE ROW ( '{chart:type=bar | 3D = true | width=1000 | height=1000 | orientation = horizontal | dataDisplay = after}' ) ;
      WITH q AS (
           SELECT DISTINCT database_name
            FROM hp_diag.test_result_db_stats
           WHERE test_id IN ( l_testId1 , l_testId2, l_testId3, l_testId4 )
           AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL))
      SELECT '|| LIST_TESTS || ' || LISTAGG ( database_name  , '||' ) WITHIN GROUP ( ORDER BY database_name ) || '||' 
  INTO l_header_row
        FROM q ;
      PIPE ROW ( l_header_row ) ;

    -- 2.2) Chart detail rows.
    FOR r1 IN ( SELECT test_id , '|' || test_id || '|' || 
                      LISTAGG ( round(nvl(db_cpu_per_sec,0),2), '|' ) WITHIN GROUP ( ORDER BY database_name ) || '|' AS text_output
                  FROM hp_diag.test_result_db_stats
             WHERE test_id IN ( l_testId1 , l_testId2, l_testId3, l_testId4 ) 
             AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
            GROUP BY test_id 
            ORDER BY test_id 
      )
      LOOP
         PIPE ROW ( r1.text_output ) ;
      END LOOP ;  -- FOR r1 IN

    -- 2.3) Chart footer rows.
    PIPE ROW ( '{chart}' ) ;

   -----------------------------------------------------------------------------------------------------------------------------
   PIPE ROW ( 'h5. Load Comparison With Previous Releases - Execs Per Second' ) ;
   PIPE ROW ( '{chart:type=bar | 3D = true | width=1000 | height=1000 | orientation = horizontal | dataDisplay = after}' ) ;
      WITH q AS (
           SELECT DISTINCT database_name
            FROM hp_diag.test_result_db_stats
           WHERE test_id IN ( l_testId1 , l_testId2, l_testId3, l_testId4 )
           AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL))
      SELECT '|| LIST_TESTS || ' || LISTAGG ( database_name  , '||' ) WITHIN GROUP ( ORDER BY database_name ) || '||' 
  INTO l_header_row
        FROM q ;
      PIPE ROW ( l_header_row ) ;

    -- 2.2) Chart detail rows.
    FOR r1 IN ( SELECT test_id , '|' || test_id || '|' || 
                      LISTAGG ( round(nvl(execs_per_sec,0),2), '|' ) WITHIN GROUP ( ORDER BY database_name ) || '|' AS text_output
                  FROM hp_diag.test_result_db_stats
             WHERE test_id IN ( l_testId1 , l_testId2, l_testId3, l_testId4 ) 
             AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
            GROUP BY test_id 
            ORDER BY test_id 
      )
      LOOP
         PIPE ROW ( r1.text_output ) ;
      END LOOP ;  -- FOR r1 IN

    -- 2.3) Chart footer rows.
    PIPE ROW ( '{chart}' ) ;
END load_comparison ;

FUNCTION rlw_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Best previous release'
      , i_mslp IN VARCHAR2
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
   IF i_testId1 = 'Latest test'  -- the default
   THEN
      SELECT MAX ( test_id ) KEEP ( DENSE_RANK FIRST ORDER BY begin_time DESC ) INTO l_testId1
        FROM hp_diag.test_result_master
      ;
   ELSE
      l_testId1 := i_testId1 ;
   END IF ;
   IF i_testId2 = 'Best previous release'  -- the default
   THEN
      SELECT MAX ( test_id ) KEEP ( DENSE_RANK FIRST ORDER BY begin_time DESC ) INTO l_testId2
        FROM hp_diag.test_result_master
       WHERE best_test_for_release IS NOT NULL
      ;
   ELSE
      l_testId2 := i_testId2 ;
   END IF ;

   l_row := 'h5. Top 25 Row Lock Waits Across Database by Total Waited Time' ;
   PIPE ROW ( l_row ) ;

   PIPE ROW ( '{csv:allowExport=true|columnTypes=s,s,s,i,s,f,f,f,f,f,f,f,f,f,f|id=Top25RLWComparison}' ) ;
   FOR rh IN (
      SELECT MIN ( CASE m.test_id WHEN l_testId1 THEN m.test_description END ) AS test_description1
           , MIN ( CASE m.test_id WHEN l_testId2 THEN m.test_description END ) AS test_description2
        FROM hp_diag.test_result_master m
       WHERE m.test_id IN ( l_testId1 , l_testId2 )
   )
   LOOP
      PIPE ROW ( '"Object Owner","Object Name","Object Type","Position","Database Name","Num Waits","' || NVL ( rh.test_description2 , l_testid2 ) || ' Num Waits","Min Wait (ms)","' || NVL ( rh.test_description2 , l_testid2 ) || ' Min Wait (ms)","Max Wait (ms)","' || NVL ( rh.test_description2 , l_testid2 ) || ' Max Wait (ms)","Avg Wait (ms)","' || NVL ( rh.test_description2 , l_testid2 ) || ' Avg Wait (ms)","Total Wait (ms)","' || NVL ( rh.test_description2 , l_testid2 ) || ' Total Wait (ms)"');
   END LOOP ;

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
                     AND  s.database_name in (SELECT
                                                        REGEXP_SUBSTR (
                                                                       i_mslp,
                                                                       '[^,]+', 1, level) AS string_parts
                                                      FROM dual
                                                      CONNECT BY REGEXP_SUBSTR (
                                                                                i_mslp,
                                                                                '[^,]+', 1, level) IS NOT NULL)
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
                            AND  s.database_name in (SELECT
                                                        REGEXP_SUBSTR (
                                                                       i_mslp,
                                                                       '[^,]+', 1, level) AS string_parts
                                                      FROM dual
                                                      CONNECT BY REGEXP_SUBSTR (
                                                                                i_mslp,
                                                                                '[^,]+', 1, level) IS NOT NULL)
                           ORDER BY total_wait_time DESC
                         ) a
                   WHERE ROWNUM <= 25
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

END rlw_comparison ;

FUNCTION top15_long_comp_db (
        i_dbname  IN varchar2,
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
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
   IF i_testId1 = 'Latest test'  -- the default
   THEN
      SELECT MAX ( test_id ) KEEP ( DENSE_RANK FIRST ORDER BY begin_time DESC ) INTO l_testId1
        FROM hp_diag.test_result_master;
   ELSE
      l_testId1 := i_testId1 ;
   END IF ;

   select test_id 
     into l_testId2
     from (select rownum rnum, test_id
             from (select test_id from test_result_master where best_test_for_release is not null order by begin_time desc))
    where rnum = 1;

   select test_id 
     into l_testId3
     from (select rownum rnum, test_id
             from (select test_id from test_result_master where best_test_for_release is not null order by begin_time desc))
    where rnum = 2;

   select test_id 
     into l_testId4
     from (select rownum rnum, test_id
             from (select test_id from test_result_master where best_test_for_release is not null order by begin_time desc))
    where rnum = 3;

   l_row := 'h5. Top 15 SQL Comparison for ' || i_dbname || ' by Elapsed Time' ;
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
END top15_long_comp_db ;

FUNCTION top_n_long_comp_db (
     i_dbname IN VARCHAR2
   , i_limit IN NUMBER DEFAULT 25
   , i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
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
   IF i_testId1 = 'Latest test'  -- the default
   THEN
      SELECT MAX ( test_id ) KEEP ( DENSE_RANK FIRST ORDER BY begin_time DESC ) INTO l_testId1
        FROM hp_diag.test_result_master;
   ELSE
      l_testId1 := i_testId1 ;
   END IF ;

   select test_id 
     into l_testId2
     from (select rownum rnum, test_id
             from (select test_id from test_result_master where best_test_for_release is not null order by begin_time desc))
    where rnum = 1;

   select test_id 
     into l_testId3
     from (select rownum rnum, test_id
             from (select test_id from test_result_master where best_test_for_release is not null order by begin_time desc))
    where rnum = 2;

   select test_id 
     into l_testId4
     from (select rownum rnum, test_id
             from (select test_id from test_result_master where best_test_for_release is not null order by begin_time desc))
    where rnum = 3;

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
END top_n_long_comp_db ;

FUNCTION load_comparison_4tests (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE ,
        i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE ,
        i_testId3 IN hp_diag.test_result_metrics.test_id%TYPE ,
        i_testId4 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_mslp IN VARCHAR2
      ) RETURN g_tvc2 PIPELINED
AS
   l_metric_name g_tvc2 ;
   l_idx VARCHAR2(100) ;
   l_header_row VARCHAR2(4000) ;

   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId3 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId4 hp_diag.test_result_metrics.test_id%TYPE ;
BEGIN
   -- 1) Set default parameters
   l_testId1 := i_testId1 ;
   l_testId2 := i_testId2 ;
   l_testId3 := i_testId3 ;
   l_testId4 := i_testId4 ;

   PIPE ROW ( 'The aim of this page is to show how the previous tests compare to each other regarding the overall load put on the individual databases' ) ;
   PIPE ROW ( '' ) ;
   PIPE ROW ( 'h5. Load Comparison With Previous Releases - DB Time Per Second' ) ;

   -- 2) return data
   PIPE ROW ( '{chart:type=bar | 3D = true | width=1500 | orientation = vertical | dataDisplay = after}' ) ;
      WITH q AS (
           SELECT DISTINCT database_name
            FROM hp_diag.test_result_db_stats
           WHERE test_id IN ( l_testId1 , l_testId2, l_testId3, l_testId4 )
           AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL))
      SELECT '|| TEST NAME || ' || LISTAGG ( database_name  , '||' ) WITHIN GROUP ( ORDER BY database_name ) || '||' 
  INTO l_header_row
        FROM q ;
      PIPE ROW ( l_header_row ) ;

   -- 2.2) Chart detail rows.
   FOR r1 IN (
      SELECT '|' || s.test_id || '|' || LISTAGG ( ROUND ( NVL ( s.db_time_per_sec , 0 ) , 2 ) , '|' )
             WITHIN GROUP ( ORDER BY s.database_name ) || '|' AS text_output
        FROM hp_diag.test_result_db_stats s
       WHERE s.test_id IN ( l_testId1 , l_testId2 , l_testId3 , l_testId4 ) 
       AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
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
   PIPE ROW ( '{chart:type=bar | 3D = true | width=1500 | orientation = vertical | dataDisplay = after}' ) ;
      WITH q AS (
           SELECT DISTINCT database_name
            FROM hp_diag.test_result_db_stats
           WHERE test_id IN ( l_testId1 , l_testId2, l_testId3, l_testId4 )
           AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL))
      SELECT '|| LIST_TESTS || ' || LISTAGG ( database_name  , '||' ) WITHIN GROUP ( ORDER BY database_name ) || '||' 
  INTO l_header_row
        FROM q ;
      PIPE ROW ( l_header_row ) ;

   -- 2.2) Chart detail rows.
   FOR r1 IN (
      SELECT '|' || s.test_id || '|' || LISTAGG ( ROUND ( NVL ( s.db_cpu_per_sec , 0 ) , 2 ) , '|' )
             WITHIN GROUP ( ORDER BY s.database_name ) || '|' AS text_output
        FROM hp_diag.test_result_db_stats s
       WHERE s.test_id IN ( l_testId1 , l_testId2 , l_testId3 , l_testId4 ) 
       AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
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
   PIPE ROW ( '{chart:type=bar | 3D = true | width=1500 | orientation = vertical | dataDisplay = after}' ) ;
      WITH q AS (
           SELECT DISTINCT database_name
            FROM hp_diag.test_result_db_stats
           WHERE test_id IN ( l_testId1 , l_testId2, l_testId3, l_testId4 ) 
           AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL))
      SELECT '|| LIST_TESTS || ' || LISTAGG ( database_name  , '||' ) WITHIN GROUP ( ORDER BY database_name ) || '||' 
  INTO l_header_row
        FROM q ;
      PIPE ROW ( l_header_row ) ;

   -- 2.2) Chart detail rows.
   FOR r1 IN (
      SELECT '|' || s.test_id || '|' || LISTAGG ( ROUND ( NVL ( s.execs_per_sec , 0 ) , 2 ) , '|' )
             WITHIN GROUP ( ORDER BY s.database_name ) || '|' AS text_output
        FROM hp_diag.test_result_db_stats s
       WHERE s.test_id IN ( l_testId1 , l_testId2 , l_testId3 , l_testId4 ) 
       AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
       GROUP BY s.test_id
       ORDER BY TO_DATE ( SUBSTR ( s.test_id , 1 , 7 ) , 'DDMONYY' ) DESC
   )
   LOOP
      PIPE ROW ( r1.text_output ) ;
   END LOOP ;

   -- 2.3) Chart footer rows.
   PIPE ROW ( '{chart}' ) ;
END load_comparison_4tests ;



FUNCTION all_db_top15_comparison (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Best previous release'
      , i_mslp IN VARCHAR2
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
  IF i_testId1 = 'Latest test'  -- the default
  THEN
    SELECT MAX ( test_id ) KEEP ( DENSE_RANK FIRST ORDER BY begin_time DESC ) INTO l_testId1
      FROM hp_diag.test_result_master;
  ELSE
    l_testId1 := i_testId1 ;
  END IF ;

  IF i_testId2 = 'Best previous release'  -- the default
  THEN
    SELECT MAX ( test_id ) KEEP ( DENSE_RANK FIRST ORDER BY begin_time DESC ) INTO l_testId2
      FROM hp_diag.test_result_master
     WHERE best_test_for_release IS NOT NULL;
  ELSE
    l_testId2 := i_testId2 ;
  END IF ;

  l_row := '{toc:type=list}';
  PIPE ROW ( l_row ) ;

  l_row := 'h3. Test Details';
  PIPE ROW ( l_row ) ;

  SELECT m.test_description, to_char(begin_time, 'Dy dd/mm/yyyy hh24:mi'),to_char(end_time, 'Dy dd/mm/yyyy hh24:mi') 
    into l_test_description, l_start_time, l_end_time
    FROM hp_diag.test_result_master m 
   WHERE m.test_id = l_testId1;

  l_row := 'h5. Current Test ';
  PIPE ROW ( l_row ) ;

  l_row := 'Start Time  : ' || l_start_time;
  PIPE ROW ( l_row ) ;

  l_row := 'End Time    : ' || l_end_time;
  PIPE ROW ( l_row ) ;

  l_row := 'Description : ' || l_test_description;
  PIPE ROW ( l_row ) ;

  SELECT m.test_description, to_char(begin_time, 'Dy dd/mm/yyyy hh24:mi'),to_char(end_time, 'Dy dd/mm/yyyy hh24:mi') 
    into l_test_description, l_start_time, l_end_time
    FROM hp_diag.test_result_master m 
   WHERE m.test_id = l_testId2;

  l_row := 'h5. Previous Test ';
  PIPE ROW ( l_row ) ;

  l_row := 'Start Time  : ' || l_start_time;
  PIPE ROW ( l_row ) ;

  l_row := 'End Time    : ' || l_end_time;
  PIPE ROW ( l_row ) ;

  l_row := 'Description : ' || l_test_description;
  PIPE ROW ( l_row ) ;

  FOR r_dbName IN (
    SELECT distinct t.database_name
      FROM hp_diag.test_result_sql t
      WHERE t.test_id = l_testId1
      AND  database_name in (SELECT
                        REGEXP_SUBSTR (
                        i_mslp,
                        '[^,]+', 1, level) AS string_parts
                       FROM dual
                       CONNECT BY REGEXP_SUBSTR (
                         i_mslp,
                         '[^,]+', 1, level) IS NOT NULL)
    ORDER BY 1
  )
  LOOP
    -- pipe header
    l_row := 'h3. SQL Comparison for ' || r_dbName.database_name ;
    PIPE ROW ( l_row ) ;

    PIPE ROW ( '{csv:allowExport=true|sortIcon=true|columnTypes=s,i,i,s,s,s,s,s,s,f,f,f,f,f,f,f,f,f,f,f,f,s,s|rowStyles=,background:lightblue,background:darkgrey}' ) ;    
    FOR rh IN (
      SELECT MIN ( CASE m.test_id WHEN l_testId1 THEN m.test_description END ) AS test_description1
           , MIN ( CASE m.test_id WHEN l_testId2 THEN m.test_description END ) AS test_description2
        FROM hp_diag.test_result_master m
       WHERE m.test_id IN ( l_testId1 , l_testId2 )
    )
    LOOP
      PIPE ROW ( '"SQL ID","Position","Prev Position","Total Elapsed Diff","TPS Diff","ms Per Exec Diff","Rows Per Exec Diff","CPU Per Exec Diff","BG Per Exec Diff","Total Elapsed","Prev Total Elapsed","TPS","Prev TPS","ms Per Exec","Prev ms Per Exec","Rows Per Exec","Prev Rows Per Exec","CPU Per Exec","Prev CPU Per Exec","BG Per Exec","Prev BG Per Exec","SQL Text","Module"');
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
end all_db_top15_comparison;

END sm_report ;
/
