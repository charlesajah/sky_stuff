CREATE OR REPLACE PACKAGE REPORT_DATA AS 

/* This is the REPOSITORY of Procedures & Functions common for 
   all the DATA reporting within the Confluenece Reports Generation 
*/ 

   TYPE g_tvc2 IS TABLE OF VARCHAR2(4000) ;

   FUNCTION Get_test_info (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION Get_top25_detail (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED;

   FUNCTION Get_PGA_growth (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'None'
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION Get_RedoLogs_Usage (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'None'
      ) RETURN g_tvc2 PIPELINED ;  

   FUNCTION Get_sal_basket_sizes (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'Latest test'
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION Get_Cache_Info (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION Get_DB_Details (
        i_testId1  IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2  IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_metric   IN varchar2 
      , i_filter   IN varchar2 DEFAULT NULL
      ) RETURN g_tvc2 PIPELINED ;

   PROCEDURE Do_AnalyticalData (
         i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
       , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
       , i_dev      IN NUMBER    DEFAULT 0.35
       , i_days     IN NUMBER    DEFAULT 30
       , i_ratio    IN NUMBER    DEFAULT 2
       , i_except   IN VARCHAR2  DEFAULT NULL 
       ) ;

   FUNCTION Get_DB_Summary (
        i_testId1      IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2      IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_filter       IN varchar2 DEFAULT NULL      
      , i_ratio        IN number default 2
      , i_summary      IN varchar2 default NULL
      ) RETURN g_tvc2 PIPELINED ;      

END REPORT_DATA;
/


CREATE OR REPLACE PACKAGE BODY REPORT_DATA AS

/* This is the REPOSITORY of Procedures & Functions common for 
   all the DATA reporting within the Confluenece Reports Generation 
*/ 

FUNCTION Get_test_info (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED
AS
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;

   l_test_description varchar2(100);
   l_start_time       varchar2(100); 
   l_end_time         varchar2(100);
   l_row              varchar2(4000);
begin
   -- 1) Set default parameters
  l_testId1 := i_testId1 ;
  l_testId2 := i_testId2 ;

  l_row := '{toc:type=list}';
  PIPE ROW ( l_row ) ;

  -- The top level label would be driven by the actual calling scripts
  l_row := 'h3. NFT - Analysis Information';
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

end Get_test_info;


FUNCTION Get_top25_detail (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
       ,i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED
AS
   l_metric_name g_tvc2 ;
   l_idx VARCHAR2(100) ;
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;
   l_row     varchar2(4000);
   l_start   varchar2(20);
   l_end     varchar2(20);

BEGIN
   -- 1) Set default parameters
   l_testId1 := i_testId1 ;
   l_testId2 := i_testId2 ;
   l_start := REPORT_ADM.GET_START_DTM(l_testId1);
   l_end   := REPORT_ADM.GET_END_DTM(l_testId1);

   --l_row := 'h3. Top 25 SQL Comparison Across Database by Previous Releases' ;
   l_row := 'h5. Top 25 SQL Detail Comparison' ;
   PIPE ROW ( l_row ) ;

   for r1 in (
         with cur as (
               SELECT ROWNUM rnum, a.database_name, a.sql_id
                 FROM (
                           SELECT LOWER ( s.database_name ) AS database_name, s.sql_id, 
                                  TO_CHAR ( ROUND ( s.tps , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS tps,
                                  TO_CHAR ( ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS ms_pe,
                                  LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text, 
                                  LOWER ( NVL(s.module,'no-module') ) AS module
                             FROM hp_diag.test_result_sql s
                             LEFT OUTER JOIN test_result_sqltext st ON st.sql_id = s.sql_id
                            WHERE test_id = l_testId1
                              AND lower(NVL(s.module,'no-module')) not in ('dbms_scheduler')
                           ORDER BY elapsed_time_seconds DESC
                      ) a
                WHERE ROWNUM <= 25
         )
      select sql_id, database_name
        from cur
      order by cur.rnum
   )
   LOOP
      PIPE ROW ( 'h6. SQL History for ' || r1.sql_id || ' from ' || r1.database_name ) ;
      -- 19-Apr-2023 Andrew Fraser added sql text
      PIPE ROW ( '||Module||Sql Text||' ) ;
      FOR r2t IN (
         SELECT '|' || NVL(st.module,'no-module') || '|'
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
         SELECT * FROM (
            SELECT ROWNUM rnum
              , LOWER( s.database_name ) AS database_name
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
            AND s.begin_time <= to_date(l_end,'DDMONYY-HH24:MI')
            ORDER by s.begin_time desc )
         WHERE ROWNUM < 15   
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
END Get_top25_Detail ;


FUNCTION Get_PGA_growth (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'None'
      ) RETURN g_tvc2 PIPELINED
AS
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;

   l_row     varchar2(4000);
   i         number;
BEGIN
   -- 1) Set default parameters
   l_testId1 := i_testId1 ;
   l_testId2 := i_testId2 ;
   i := 0;

   FOR r1 IN (
      SELECT '"' ||
          LOWER ( m.database_name )
          || '","' ||
          TO_CHAR ( ROUND ( begin_average / 1024 / 1024 / 1024 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' )
          || '","' ||
          TO_CHAR ( ROUND ( end_average / 1024 / 1024 / 1024 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' )
          || '","' ||
          TO_CHAR ( ROUND ( 100 * ( end_average - begin_average ) / begin_average ) , 'FM9,999,999,999,999,999,999,999,999,999,990' )
          || '","' ||
          TO_CHAR ( ROUND ( average / 1024 / 1024 / 1024 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' )
          || '","' ||
          TO_CHAR ( ROUND ( min_average / 1024 / 1024 / 1024 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' )
          || '","' ||
          TO_CHAR ( ROUND ( max_average / 1024 / 1024 / 1024 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' )
          || '"' AS col1
     FROM hp_diag.test_result_metrics m
    WHERE m.test_id = l_testId1
      AND m.metric_name = 'Total PGA Allocated'
      AND m.end_average > m.average  -- progressively growing
      AND m.average > m.begin_average  -- progressively growing
      AND ( 
              m.end_average / m.begin_average >= 1.5 -- grew by at least 50%
           OR ( m.end_average - m.begin_average ) > 2 * 1024 * 1024 * 1024   -- or grew by at least 2gb
          )
     ORDER BY col1
   )
   LOOP
      i := i + 1 ;
      IF i = 1
      THEN
         -- Header line, only output if there is at least one body line to be output.
         PIPE ROW ( 'h5. Pga Growth' ) ;
         PIPE ROW ( '{csv:allowExport=true|columnTypes=s,f,f,f,f,f,f|id=pga}' ) ;
         PIPE ROW ( '"Database","Pga gb Begin","Pga gb End","Growth %","Pga Ave gb","Pga Min gb","Pga Max gb"' ) ;
      END IF ;
      -- Body line
      PIPE ROW ( r1.col1 ) ;
   END LOOP ;
   IF i > 0
   THEN
      -- Footer line, only output if there was at least one body line output.
      PIPE ROW ( '{csv}' ) ;
   END IF ;
END Get_PGA_growth ;


FUNCTION Get_RedoLogs_Usage (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE DEFAULT 'None'
      ) RETURN g_tvc2 PIPELINED
AS
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;

   l_row     varchar2(4000);
   i         number;
BEGIN
   -- 1) Set default parameters
   l_testId1 := i_testId1 ;
   l_testId2 := i_testId2 ;
   i := 0;

   --PIPE ROW ('h3. Archived Redo Log') ;
   PIPE ROW ('h5. Archived Redo Log Usage Comparison') ;
   PIPE ROW ('{csv:autoTotal=true|allowExport=true|columnTypes=s,s,f|id=arcRL}');
   PIPE ROW ('"Database","Current Gb","Previous Gb"');

   FOR r1 IN (
         with prev as (
                  select  m.database_name, ROUND(m.average) average
                    from  test_result_metrics m
                   where  m.test_id = l_testId2 
                     and  m.metric_name = 'Arcvhived Redo Log' 
                order by  m.database_name ) 
             , cur as (
                   select  m.database_name, ROUND(m.average) average
                    from  test_result_metrics m
                   where  m.test_id = l_testId1 
                     and  m.metric_name = 'Arcvhived Redo Log' 
                order by  m.database_name ) 
         select '"' || cur.database_name
                    || '","' || cur.average
                    || '","' || prev.average
                    || '"' as col1
           from cur, prev
          where cur.database_name = prev.database_name 
          order by cur.database_name )
   LOOP
      l_row := r1.col1;
      PIPE ROW (l_row);
   END LOOP;
   PIPE ROW ('{csv}');       

END Get_RedoLogs_Usage ;


FUNCTION Get_sal_basket_sizes (
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
   l_testId1 := i_testId1 ;

   FOR r1 IN ( SELECT DISTINCT lob_type AS lt FROM sal_basket_sizes WHERE test_id = l_testId1 ORDER BY 1 )
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
                order by lob_type, created
     )
     LOOP
         l_row := '"' || r2.created || '","' || r2.B0_25KB || '","' || r2.B26_50KB || '","' || r2.B51_76B || '","' || r2.B76_100KB || 
                  '","' || r2.B101_125KB || '","' || r2.B126_150KB || '","' || r2.B151_175KB || '","' || r2.B176_200KB || 
                  '","' || r2.B200_250KB || '","' || r2.B251_500KB || '","' || r2.B501_750KB || '","' || r2.B751_1000KB || 
                  '","' || r2.B1001_1500KB || '","' || r2.B1501_2000KB || '","' || r2.B2001_3000KB || '","' || r2.B3001_4000KB || 
                  '","' || r2.B4001_5000KB || '","' || r2.B5001_6000KB || '","' || r2.B6001KB || '"';
         PIPE ROW ( l_row ) ;
      END LOOP ;
      PIPE ROW ( '{csv}' ) ;      
   END LOOP ;
END Get_sal_basket_sizes ;


FUNCTION Get_Cache_Info (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED
AS
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;
   l_row                varchar2(4000);
BEGIN
   -- 1) Set default parameters
   l_testId1 := i_testId1 ;
   l_testId2 := i_testId2 ;

   PIPE ROW ( 'h5. Cache vs No Cache Info (Current Test)' ) ;
   PIPE ROW ('{csv:allowExport=true|columnTypes=s,f,f|id=CacheNoCache}') ;
   PIPE ROW ('"Query Type","Executions","Avg Elapsed (ms)"') ;

   FOR r1 IN (
      SELECT '"' || v.query_type || '","'
                 || TRIM ( TO_CHAR ( v.execs , '999,999,999,999,999,999,999,990' ) ) || '","'
                 || TRIM ( TO_CHAR ( v.avg_ms , '999,999,999,999,999,999,999,990.000' ) ) || '"'
              AS line_text
      FROM (
               SELECT CASE WHEN s.sql_id = 'axdsyv3z7aznk' THEN 'No Cache Query (' || s.sql_id || ')'
                           WHEN s.sql_id = '81x3a4w4tznzj' THEN 'Cache Query (' || s.sql_id || ')'
                      END AS query_type
                    , NVL ( SUM ( s.executions_delta ) , 0 ) AS execs 
                    , SUM ( s.elapsed_time_delta ) / NULLIF ( SUM ( s.executions_delta ) , 0 ) / 1000 AS avg_ms
                 FROM dba_hist_sqlstat s
                WHERE s.snap_id BETWEEN REPORT_ADM.Get_StartSnapId(l_testId1) + 1 AND REPORT_ADM.Get_EndSnapId(l_testId1)
                  AND s.sql_id IN ( 'axdsyv3z7aznk' , '81x3a4w4tznzj' )
                GROUP BY s.sql_id ) v
      ORDER BY 1 )
   LOOP
      l_row := r1.line_text ;
      PIPE ROW (l_row) ;
   END LOOP;
   PIPE ROW ( '{csv}' ) ; 


   PIPE ROW ( 'h5. Cache vs No Cache Info (Previous Test)' ) ;
   PIPE ROW ('{csv:allowExport=true|columnTypes=s,f,f|id=CacheNoCache}') ;
   PIPE ROW ('"Query Type","Executions","Avg Elapsed (ms)"') ;

   FOR r1 IN (
      SELECT '"' || v.query_type || '","'
                 || TRIM ( TO_CHAR ( v.execs , '999,999,999,999,999,999,999,990' ) ) || '","'
                 || TRIM ( TO_CHAR ( v.avg_ms , '999,999,999,999,999,999,999,990.000' ) ) || '"'
              AS line_text
      FROM (
               SELECT CASE WHEN s.sql_id = 'axdsyv3z7aznk' THEN 'No Cache Query (' || s.sql_id || ')'
                           WHEN s.sql_id = '81x3a4w4tznzj' THEN 'Cache Query (' || s.sql_id || ')'
                      END AS query_type
                    , NVL ( SUM ( s.executions_delta ) , 0 ) AS execs 
                    , SUM ( s.elapsed_time_delta ) / NULLIF ( SUM ( s.executions_delta ) , 0 ) / 1000 AS avg_ms
                 FROM dba_hist_sqlstat s
                WHERE s.snap_id BETWEEN REPORT_ADM.Get_StartSnapId(l_testId2) + 1 AND REPORT_ADM.Get_EndSnapId(l_testId2)
                  AND s.sql_id IN ( 'axdsyv3z7aznk' , '81x3a4w4tznzj' )
                GROUP BY s.sql_id ) v
      ORDER BY 1 )
   LOOP
      l_row := r1.line_text ;
      PIPE ROW (l_row) ;
   END LOOP;
   PIPE ROW ( '{csv}' ) ;

END Get_Cache_Info ;


/* This one is not yet working  */
FUNCTION Get_DB_Charts (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED
AS
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;
   l_row                varchar2(4000);
BEGIN
   -- 1) Set default parameters
   l_testId1 := i_testId1 ;
   l_testId2 := i_testId2 ;

   PIPE ROW ( 'h5. CPU Utilization (%)' ) ;

   l_row := '{chart:type=timeSeries|dateFormat=M/d H:m|timePeriod=hour|dataOrientation=vertical|rangeAxisLowerBound=0|domainaxisrotateticklabel=true|title=Host CPU Utilization (%)|height=600|width=900}';
   PIPE ROW ( l_row ) ;
   l_row := ' ||KEY || ASU021N|| AUC021N|| CCS021N|| CPP021N|| CSC021N|| DAC021N|| DCU021N|| DCU022N|| DCU023N|| DCU024N|| DCU025N|| DCU026N|| DFS021N|| DPS021N|| EGS021N|| FUL021N|| IFS021N|| IGR021N|| ISS021N|| JBP021N|| OMS021N|| OPR021N|| PGR021N|| PIN021N|| RIS021N|| TCC021N|| OGS021N|| SLT021N|| SMPGP5N|| SMPUK5N|| SMPIT5N|| SMPDE5N|| CAS021N|| PCS021N|| MER021N|| CGS021N||';
   PIPE ROW ( l_row ) ;
   FOR r1 IN (
      select '' na ,  a.* , '' nb
        from ( SELECT * 
           FROM ( SELECT * 
              FROM (SELECT DATABASE_NAME
                         , to_char(trunc(BEGIN_TIME,'hh24')+(ROUND(TO_CHAR(BEGIN_TIME,'mi')/15)*15)/24/60, 'mm/dd hh24')||':00' KEY
                         , ROUND(AVERAGE, 4) VALUE
                      FROM HP_DIAG.TEST_RESULT_METRICS_DETAIL
                     WHERE TEST_ID = l_testId1 
                       AND METRIC_NAME = 'Host CPU Utilization (%)'
                    )
              PIVOT (AVG(VALUE)  FOR (DATABASE_NAME)  IN  
                ('ASU021N','AUC021N','CCS021N','CPP021N','CSC021N','DAC021N','DCU021N','DCU022N','DCU023N','DCU024N','DCU025N','DCU026N','DFS021N','DPS021N','EGS021N','FUL021N','IFS021N','IGR021N','ISS021N','JBP021N','OMS021N','OPR021N','PGR021N','PIN021N','RIS021N','TCC021N', 'OGS021N', 'SLT021N', 'SMPGP5N', 'SMPUK5N', 'SMPIT5N', 'SMPDE5N', 'CAS021N', 'PCS021N', 'MER021N', 'CGS021N')
                    )
           order by 1 )
              ) A ) 
   LOOP
      --l_row := r1.*;  --> Need to think how we can display all these values without having to hardcode the list!
      PIPE ROW ( l_row ) ;
   END LOOP;   

END Get_DB_Charts;

-- #############################  DB Summary Procedures/Functions ##########################################

PROCEDURE Do_TruncateTables AS
BEGIN
    execute immediate 'truncate table TEST_RESULT_SQL_RESULTS' ; 
    execute immediate 'truncate table TEST_RESULT_SQL_SUMMARY' ; 
END Do_TruncateTables; 


PROCEDURE Do_ElapsedTimePerExec (
         i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
       , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
       , i_property IN VARCHAR2
       , i_dev      IN NUMBER    DEFAULT 0.35
       , i_days     IN NUMBER    DEFAULT 30
       , i_ratio    IN NUMBER    DEFAULT 2
       , i_except   IN VARCHAR2  DEFAULT NULL 
       ) as

   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;
   l_begintest   date;          -- Date the comparison Test starts in date format 
   l_endtest     date;          -- Date the comparison Test ends in date format ( we add one minute to include the last date )

   -- PARAMETERS
   l_property    varchar2(30);   -- Property being evaluated
   l_dev         number;        -- deviation degree ( default 0.35 )
   l_days        number;        -- Number of days considered to extract a trend ( defalut would be 30 )
   l_ratio       number;        -- Minimum difference Ratio to qualify al alert as RED
   l_except      varchar2(200); -- Type of MODULEs to be excluded from the result ( APP, DB or NULL (all) )

BEGIN
   l_testId1  := i_testId1 ;
   l_testId2  := i_testId2 ;
   l_property := i_property;
   l_dev      := i_dev;
   l_days     := i_days;
   l_ratio    := i_ratio;
   l_except   := i_except;

   -- The Lowest date of both dates suppplied
   l_begintest := to_date(REPORT_ADM.Get_Start_DTM(l_testId1),'ddMONyy-hh24:mi');
   -- The highest date of both dates supplied
   l_endtest := to_date(REPORT_ADM.Get_End_DTM(l_testId1),'ddMONyy-hh24:mi')+(1/1440*5);


   -- execute immediate 'truncate table TEST_RESULT_SQL_SUM_TMP' ;
   -- Delete all rows gathered for just one perticular PROPERTY
   delete from TEST_RESULT_SQL_RESULTS
   where  PROPERTY = l_property;
   commit;

   -- Retrieve the PROPERTY values for this particular test sample
   FOR r1 IN (
       select distinct DATABASE_NAME
       from   TEST_RESULT_SQL
       where  TEST_ID in ( l_testId1, l_testId2 )
       ) 
   LOOP     
      Insert into TEST_RESULT_SQL_RESULTS t
             ( t.DATABASE_NAME ,t.SQL_ID , t.PROPERTY, t.MODULE, t.CUR_VAL ,t.PRE_VAL ,t.MEDIAN_VAL ,t.DEVIATION_VAL ,t.LOW_MED_VAL ,t.MIN_VAL ,t.HIGH_MED_VAL ,t.MAX_VAL ,t.CUR_STATUS ,t.PRE_STATUS ,t.COMP_STATUS ,t.DIFF_RATIO )
      Select  s.DATABASE_NAME ,s.SQL_ID, s.property, s.module, s.cur_PropValue ,s.pre_PropValue ,s.Med ,s.Dev ,s.Low ,s.Min ,s.High ,s.Max ,s.cur_assess ,s.pre_assess ,s.comp_assess ,s.ratio 
        from (
        with FILTER as ( select TEST_ID  
                               ,DATABASE_NAME
                               ,SQL_ID 
                               ,MODULE
                               ,BEGIN_TIME
                               ,END_TIME
                               ,ELAPSED_TIME_PER_EXEC_SECONDS
                          from TEST_RESULT_SQL
                         where DATABASE_NAME = r1.DATABASE_NAME
                           and ( TEST_ID in (l_testId1, l_testId2) or ( BEGIN_TIME >= l_begintest-l_days and END_TIME <= l_endtest))
                           and (CASE WHEN ( l_except = 'APP' and ( Upper(module) not in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER')
                                                                   and Upper(module) not like 'RMAN%'
                                                                   and module not like 'Oracle Enterprise Manager%'
                                                                  )) THEN 1
                                     WHEN ( l_except = 'DB'  and ( Upper(module) in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER') 
                                                                   or Upper(module) not like 'RMAN%'
                                                                   or module not like 'Oracle Enterprise Manager%'   
                                                                  )) THEN 1
                                     WHEN ( l_except is NULL ) THEN 1
                                     ELSE 0 END ) = 1
                           and ELAPSED_TIME_PER_EXEC_SECONDS is not NULL
                         order by TEST_ID, SQL_ID )
          , CURR as ( select f.SQL_ID 
                             ,f.MODULE
                             ,( CASE WHEN ROUND(f.ELAPSED_TIME_PER_EXEC_SECONDS,10)=0 THEN NULL 
                                     ELSE ROUND(f.ELAPSED_TIME_PER_EXEC_SECONDS,10) END ) as PropValue
                        from FILTER f
                       where f.DATABASE_NAME = r1.DATABASE_NAME
                         and f.TEST_ID = l_testId1
                       order by SQL_ID )
          , PREV as ( select f.SQL_ID 
                            ,f.MODULE
                             ,( CASE WHEN ROUND(f.ELAPSED_TIME_PER_EXEC_SECONDS,10)=0 THEN NULL 
                                     ELSE ROUND(f.ELAPSED_TIME_PER_EXEC_SECONDS,10) END ) as PropValue
                        from FILTER f
                       where f.DATABASE_NAME = r1.DATABASE_NAME
                         and f.TEST_ID = l_testId2
                       order by f.SQL_ID )
          , AVER as ( select distinct f.SQL_ID
                            ,ROUND(Median(f.ELAPSED_TIME_PER_EXEC_SECONDS) over (partition by f.SQL_ID),10) as Med
                            ,ROUND(Stddev(f.ELAPSED_TIME_PER_EXEC_SECONDS) over (partition by f.SQL_ID),10) as dev
                            ,GREATEST(ROUND(Median(f.ELAPSED_TIME_PER_EXEC_SECONDS) over (partition by f.SQL_ID) - l_dev *Stddev(f.ELAPSED_TIME_PER_EXEC_SECONDS) over (partition by f.SQL_ID),10),0.0000000001) as Low
                            ,ROUND(Median(f.ELAPSED_TIME_PER_EXEC_SECONDS) over (partition by f.SQL_ID) + l_dev *Stddev(f.ELAPSED_TIME_PER_EXEC_SECONDS) over (partition by f.SQL_ID),10) as High
                        from FILTER f
                       where f.DATABASE_NAME = r1.DATABASE_NAME
                         and f.BEGIN_TIME BETWEEN l_begintest-l_days AND l_endtest-1
                       order by f.SQL_ID )
           , MAXS as ( select f.SQL_ID
                              ,ROUND(MAX(f.ELAPSED_TIME_PER_EXEC_SECONDS),10) as Max
                              ,ROUND(MIN(f.ELAPSED_TIME_PER_EXEC_SECONDS),10) as Min
                         from FILTER f
                        where f.DATABASE_NAME = r1.DATABASE_NAME
                         and f.BEGIN_TIME BETWEEN l_begintest-l_days AND l_endtest-1
                        group by SQL_ID ) 
          , RATIO as ( select c.SQL_ID
                         ,CASE WHEN p.PropValue is NULL and a.Med is not NULL THEN ROUND(c.PropValue/a.Med,2)
                               WHEN p.PropValue is NULL THEN 0
                               WHEN c.PropValue is NULL THEN 0
                               WHEN c.PropValue >= p.PropValue THEN ROUND(c.PropValue/p.PropValue,2) 
                               ELSE ROUND(p.PropValue/c.PropValue,2)*(-1) END as ratio 
                          from CURR c
                          left outer join PREV p on p.SQL_ID = c.SQL_ID 
                          left outer join AVER a on a.SQL_ID = c.SQL_ID
                      )
         select r1.DATABASE_NAME
               ,c.SQL_ID
               ,l_property as property
               ,c.MODULE
               ,c.PropValue as cur_PropValue
               ,p.PropValue as pre_PropValue
               ,NVL(a.Med,0) as Med
               ,NVL(a.dev,0) as dev
               ,NVL(a.Low,0) as Low
               ,NVL(m.Min,0) as Min
               ,NVL(a.High,0) as High
               ,NVL(m.Max,0) as Max
               ,CASE WHEN a.High is NULL or a.Low is NULL                   THEN 'NEW'
                     WHEN c.PropValue >= a.High and r.ratio >= l_ratio      THEN 'DEGRADED' 
                     WHEN c.PropValue >= a.High                             THEN 'SLOWER'
                     WHEN c.PropValue <= a.Low  and ABS(r.ratio) >= l_ratio THEN 'IMPROVED'
                     WHEN c.PropValue <= a.Low                              THEN 'FASTER'
                     ELSE '-' END as cur_assess
               ,CASE WHEN p.PropValue is NULL                               THEN 'NEW'
                     WHEN p.PropValue >= a.High and r.ratio >= l_ratio      THEN 'DEGRADED' 
                     WHEN p.PropValue >= a.High                             THEN 'SLOWER'
                     WHEN p.PropValue <= a.Low  and ABS(r.ratio) >= l_ratio THEN 'IMPROVED'
                     WHEN p.PropValue <= a.Low                              THEN 'FASTER'
                     ELSE '-' END as pre_assess
               ,CASE WHEN p.PropValue is NULL                                                        THEN 'NEW'
                     WHEN ( c.PropValue/p.PropValue ) > 1 and ( c.PropValue/p.PropValue ) >= l_ratio THEN 'DEGRADED'
                     WHEN ( c.PropValue/p.PropValue ) > 1                                            THEN 'SLOWER'
                     WHEN ( c.PropValue/p.PropValue ) < 1 and ( p.PropValue/c.PropValue ) >= l_ratio THEN 'IMPROVED'
                     WHEN ( c.PropValue/p.PropValue ) < 1                                            THEN 'FASTER'
                     ELSE '-' END as comp_assess                 
                ,r.ratio
        from CURR c
        LEFT OUTER JOIN PREV p on p.SQL_ID = c.SQL_ID
        LEFT OUTER JOIN AVER a on a.SQL_ID = c.SQL_ID
        LEFT OUTER JOIN MAXS m on m.SQL_ID = c.SQL_ID
        LEFT OUTER JOIN RATIO r on r.SQL_ID = c.SQL_ID
       ) s  ;
       COMMIT;

       -- Merge summary info into the summary temp table

       MERGE into TEST_RESULT_SQL_SUMMARY s
       USING ( select tr.DATABASE_NAME, tr.SQL_ID, tr.MODULE, tr.CUR_STATUS, tr.COMP_STATUS, tr.CUR_VAL, tr.DIFF_RATIO
                 from TEST_RESULT_SQL_RESULTS tr
                where tr.PROPERTY = 'ELAPSED_TIME_PER_EXEC_SECONDS' 
              ) r  ON (s.DATABASE_NAME = r.DATABASE_NAME and s.SQL_ID = r.SQL_ID)
       WHEN MATCHED THEN
           update SET s.ELA_ST_TST = r.COMP_STATUS
                     ,s.ELA_ST_GEN = r.CUR_STATUS
                     ,s.ELA_VAL    = r.CUR_VAL
                     ,s.ELA_RATIO  = r.DIFF_RATIO
       WHEN NOT MATCHED THEN
          insert ( s.DATABASE_NAME, s.SQL_ID, s.MODULE, s.ELA_ST_TST, s.ELA_ST_GEN, s.ELA_VAL, s.ELA_RATIO )
          values ( r.DATABASE_NAME, r.SQL_ID, r.MODULE, r.COMP_STATUS, r.CUR_STATUS, r.CUR_VAL, r.DIFF_RATIO )
       ;
       COMMIT;

    END LOOP ;

END Do_ElapsedTimePerExec;


PROCEDURE Do_TPS (
         i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
       , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
       , i_property IN VARCHAR2
       , i_dev      IN NUMBER    DEFAULT 0.35
       , i_days     IN NUMBER    DEFAULT 30
       , i_ratio    IN NUMBER    DEFAULT 2
       , i_except   IN VARCHAR2  DEFAULT NULL 
       ) as

   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;
   l_begintest   date;          -- Date the comparison Test starts in date format 
   l_endtest     date;          -- Date the comparison Test ends in date format ( we add one minute to include the last date )

   -- PARAMETERS
   l_property    varchar2(30);   -- Property being evaluated
   l_dev         number;        -- deviation degree ( default 0.35 )
   l_days        number;        -- Number of days considered to extract a trend ( defalut would be 30 )
   l_ratio       number;        -- Minimum difference Ratio to qualify al alert as RED
   l_except      varchar2(200); -- Type of MODULEs to be excluded from the result ( APP, DB or NULL (all) )

BEGIN
   l_testId1 := i_testId1 ;
   l_testId2 := i_testId2 ;
   l_property := 'TPS';
   l_property := i_property;
   l_dev      := i_dev;
   l_days     := i_days;
   l_ratio    := i_ratio;
   l_except   := i_except;

   -- The Lowest date of both dates suppplied
   l_begintest := to_date(REPORT_ADM.Get_Start_DTM(l_testId1),'ddMONyy-hh24:mi');
   -- The highest date of both dates supplied
   l_endtest := to_date(REPORT_ADM.Get_End_DTM(l_testId1),'ddMONyy-hh24:mi')+(1/1440*5);

   -- execute immediate 'truncate table TEST_RESULT_SQL_SUM_TMP' ;
   -- Delete all rows gathered for just one perticular PROPERTY
   delete from TEST_RESULT_SQL_RESULTS
   where  PROPERTY = l_property;
   commit;

   -- Retrieve the PROPERTY values for this particular test sample
   For r1 IN (
       select distinct DATABASE_NAME
       from   TEST_RESULT_SQL
       where  TEST_ID in ( l_testId1, l_testId2 )
       ) 
   Loop     
      Insert into TEST_RESULT_SQL_RESULTS t
             ( t.DATABASE_NAME ,t.SQL_ID, t.PROPERTY, t.MODULE, t.CUR_VAL ,t.PRE_VAL ,t.MEDIAN_VAL ,t.DEVIATION_VAL ,t.LOW_MED_VAL ,t.MIN_VAL ,t.HIGH_MED_VAL ,t.MAX_VAL ,t.CUR_STATUS ,t.PRE_STATUS ,t.COMP_STATUS ,t.DIFF_RATIO )
      Select  s.DATABASE_NAME ,s.SQL_ID, s.property, s.MODULE, s.cur_PropValue ,s.pre_PropValue ,s.Med ,s.Dev ,s.Low ,s.Min ,s.High ,s.Max ,s.cur_assess ,s.pre_assess ,s.comp_assess ,s.ratio 
        from (
        with FILTER as ( select TEST_ID  
                               ,DATABASE_NAME
                               ,SQL_ID 
                               ,MODULE
                               ,BEGIN_TIME
                               ,END_TIME
                               ,TPS
                          from TEST_RESULT_SQL
                         where DATABASE_NAME = r1.DATABASE_NAME
                           and ( TEST_ID in (l_testId1, l_testId2) or ( BEGIN_TIME >= l_begintest-l_days and END_TIME <= l_endtest))
                           and (CASE WHEN ( l_except = 'APP' and ( Upper(module) not in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER')
                                                                   and Upper(module) not like 'RMAN%'
                                                                   and module not like 'Oracle Enterprise Manager%'
                                                                  )) THEN 1
                                     WHEN ( l_except = 'DB'  and ( Upper(module) in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER') 
                                                                   or Upper(module) not like 'RMAN%'
                                                                   or module not like 'Oracle Enterprise Manager%'   
                                                                  )) THEN 1
                                     WHEN ( l_except is NULL ) THEN 1
                                     ELSE 0 END ) = 1 
                           and TPS is not NULL
                         order by TEST_ID, SQL_ID )
          , CURR as ( select f.SQL_ID 
                             ,f.MODULE
                             ,( CASE WHEN ROUND(f.TPS,10)=0 THEN NULL 
                                     ELSE ROUND(f.TPS,10) END ) as PropValue		
                        from FILTER f
                       where f.DATABASE_NAME = r1.DATABASE_NAME
                         and f.TEST_ID = l_testId1
                       order by SQL_ID )
          , PREV as ( select f.SQL_ID 
                            ,f.MODULE
                             ,( CASE WHEN ROUND(f.TPS,10)=0 THEN NULL 
                                     ELSE ROUND(f.TPS,10) END ) as PropValue		
                        from FILTER f
                       where f.DATABASE_NAME = r1.DATABASE_NAME
                         and f.TEST_ID = l_testId2
                       order by f.SQL_ID )
          , AVER as ( select distinct f.SQL_ID
                            ,ROUND(Median(f.TPS) over (partition by f.SQL_ID),10) as Med
                            ,ROUND(Stddev(f.TPS) over (partition by f.SQL_ID),10) as dev
                            ,GREATEST(ROUND(Median(f.TPS) over (partition by f.SQL_ID) - l_dev *Stddev(f.TPS) over (partition by f.SQL_ID),10),0.0000000001) as Low
                            ,ROUND(Median(f.TPS) over (partition by f.SQL_ID) + l_dev *Stddev(f.TPS) over (partition by f.SQL_ID),10) as High
                        from FILTER f
                       where f.DATABASE_NAME = r1.DATABASE_NAME
                         and f.BEGIN_TIME BETWEEN l_begintest-l_days AND l_endtest-1
                       order by f.SQL_ID )
           , MAXS as ( select f.SQL_ID
                              ,ROUND(MAX(f.TPS),10) as Max
                              ,ROUND(MIN(f.TPS),10) as Min
                         from FILTER f
                        where f.DATABASE_NAME = r1.DATABASE_NAME
                         and f.BEGIN_TIME BETWEEN l_begintest-l_days AND l_endtest-1
                        group by SQL_ID ) 
          , RATIO as ( select c.SQL_ID
                         ,CASE WHEN p.PropValue is NULL and a.Med is not NULL THEN ROUND(c.PropValue/a.Med,2)
                               WHEN p.PropValue is NULL THEN 0
                               WHEN c.PropValue is NULL THEN 0
                               WHEN c.PropValue >= p.PropValue THEN ROUND(c.PropValue/p.PropValue,2) 
                               ELSE ROUND(p.PropValue/c.PropValue,2)*(-1) END as ratio 
                          from CURR c
                          left outer join PREV p on p.SQL_ID = c.SQL_ID 
                          left outer join AVER a on a.SQL_ID = c.SQL_ID
                     )     
         select r1.DATABASE_NAME
               ,c.SQL_ID
               ,l_property as property
               ,c.MODULE
               ,c.PropValue as cur_PropValue
               ,p.PropValue as pre_PropValue
               ,NVL(a.Med,0) as Med
               ,NVL(a.dev,0) as dev
               ,NVL(a.Low,0) as Low
               ,NVL(m.Min,0) as Min
               ,NVL(a.High,0) as High
               ,NVL(m.Max,0) as Max
               ,CASE WHEN a.High is NULL or a.Low is NULL                   THEN 'NEW'
                     WHEN c.PropValue >= a.High and r.ratio >= l_ratio      THEN 'IMPROVED' 
                     WHEN c.PropValue >= a.High                             THEN 'MORE'
                     WHEN c.PropValue <= a.Low  and ABS(r.ratio) >= l_ratio THEN 'DEGRADED'
                     WHEN c.PropValue <= a.Low                              THEN 'LESS'
                     ELSE '-' END as cur_assess
               ,CASE WHEN p.PropValue is NULL                               THEN 'NEW'
                     WHEN p.PropValue >= a.High and r.ratio >= l_ratio      THEN 'IMPROVED' 
                     WHEN p.PropValue >= a.High                             THEN 'MORE'
                     WHEN p.PropValue <= a.Low  and ABS(r.ratio) >= l_ratio THEN 'DEGRADED'
                     WHEN p.PropValue <= a.Low                              THEN 'LESS'
                     ELSE '-' END as pre_assess
               ,CASE WHEN p.PropValue is NULL                                                        THEN 'NEW'
                     WHEN ( c.PropValue/p.PropValue ) < 1 and ( c.PropValue/p.PropValue ) >= l_ratio THEN 'DEGRADED'
                     WHEN ( c.PropValue/p.PropValue ) < 1                                            THEN 'LESS'
                     WHEN ( c.PropValue/p.PropValue ) > 1 and ( p.PropValue/c.PropValue ) >= l_ratio THEN 'IMPROVED'
                     WHEN ( c.PropValue/p.PropValue ) > 1                                            THEN 'MORE'
                     ELSE '-' END as comp_assess                 
               ,r.ratio
        from CURR c
        LEFT OUTER JOIN PREV p on p.SQL_ID = c.SQL_ID
        LEFT OUTER JOIN AVER a on a.SQL_ID = c.SQL_ID
        LEFT OUTER JOIN MAXS m on m.SQL_ID = c.SQL_ID
        LEFT OUTER JOIN RATIO r on r.SQL_ID = c.SQL_ID
       ) s  ;
       COMMIT;

          -- Merge summary info into the summary temp table

       MERGE into TEST_RESULT_SQL_SUMMARY s
       USING ( select tr.DATABASE_NAME, tr.SQL_ID, tr.MODULE, tr.CUR_STATUS, tr.COMP_STATUS, tr.CUR_VAL, tr.DIFF_RATIO
                 from TEST_RESULT_SQL_RESULTS tr
                where tr.PROPERTY = 'TPS' 
              ) r  ON (s.DATABASE_NAME = r.DATABASE_NAME and s.SQL_ID = r.SQL_ID)
       WHEN MATCHED THEN
           update SET s.TPS_ST_TST = r.COMP_STATUS
                     ,s.TPS_ST_GEN = r.CUR_STATUS
                     ,s.TPS_VAL    = r.CUR_VAL
                     ,s.TPS_RATIO  = r.DIFF_RATIO
       WHEN NOT MATCHED THEN
          insert ( s.DATABASE_NAME, s.SQL_ID, s.MODULE, s.TPS_ST_TST, s.TPS_ST_GEN, s.TPS_VAL, s.TPS_RATIO )
          values ( r.DATABASE_NAME, r.SQL_ID, r.MODULE, r.COMP_STATUS, r.CUR_STATUS, r.CUR_VAL, r.DIFF_RATIO )
       ;     
       COMMIT;      

    END LOOP ;

END Do_TPS;


PROCEDURE Do_CPUTimePerExec (
         i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
       , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
       , i_property IN VARCHAR2
       , i_dev      IN NUMBER    DEFAULT 0.35
       , i_days     IN NUMBER    DEFAULT 30
       , i_ratio    IN NUMBER    DEFAULT 2
       , i_except   IN VARCHAR2  DEFAULT NULL 
       ) as

   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;
   l_begintest   date;          -- Date the comparison Test starts in date format 
   l_endtest     date;          -- Date the comparison Test ends in date format ( we add one minute to include the last date )

   -- PARAMETERS
   l_property    varchar2(30);  -- Property being evaluated
   l_dev         number;        -- deviation degree ( default 0.35 )
   l_days        number;        -- Number of days considered to extract a trend ( defalut would be 30 )
   l_ratio       number;        -- Minimum difference Ratio to qualify al alert as RED
   l_except      varchar2(10); -- Type of MODULEs to be excluded from the result ( APP, DB or NULL (all) )

BEGIN
   l_testId1 := i_testId1 ;
   l_testId2 := i_testId2 ;
   l_property := 'CPU_TIME_PER_EXEC_SECONDS';
   l_property := i_property;
   l_dev      := i_dev;
   l_days     := i_days;
   l_ratio    := i_ratio;
   l_except   := i_except;

   -- The Lowest date of both dates suppplied
   l_begintest := to_date(REPORT_ADM.Get_Start_DTM(l_testId1),'ddMONyy-hh24:mi');
   -- The highest date of both dates supplied
   l_endtest := to_date(REPORT_ADM.Get_End_DTM(l_testId1),'ddMONyy-hh24:mi')+(1/1440*5);

   -- execute immediate 'truncate table TEST_RESULT_SQL_SUM_TMP' ;
   -- Delete all rows gathered for just one perticular PROPERTY
   delete from TEST_RESULT_SQL_RESULTS
   where  PROPERTY = l_property;
   commit;

   -- Retrieve the PROPERTY values for this particular test sample
   For r1 IN (
       select distinct DATABASE_NAME
       from   TEST_RESULT_SQL
       where  TEST_ID in ( l_testId1, l_testId2 )
       ) 
   Loop     
      Insert into TEST_RESULT_SQL_RESULTS t
             ( t.DATABASE_NAME ,t.SQL_ID , t.PROPERTY, t.MODULE,  t.CUR_VAL ,t.PRE_VAL ,t.MEDIAN_VAL ,t.DEVIATION_VAL ,t.LOW_MED_VAL ,t.MIN_VAL ,t.HIGH_MED_VAL ,t.MAX_VAL ,t.CUR_STATUS ,t.PRE_STATUS ,t.COMP_STATUS ,t.DIFF_RATIO )
      Select  s.DATABASE_NAME ,s.SQL_ID, s.property, s.MODULE, s.cur_PropValue ,s.pre_PropValue ,s.Med ,s.Dev ,s.Low ,s.Min ,s.High ,s.Max ,s.cur_assess ,s.pre_assess ,s.comp_assess ,s.ratio 
        from (
        with FILTER as ( select TEST_ID  
                               ,DATABASE_NAME
                               ,SQL_ID 
                               ,MODULE
                               ,BEGIN_TIME
                               ,END_TIME
                               ,CPU_TIME_PER_EXEC_SECONDS
                          from TEST_RESULT_SQL
                         where DATABASE_NAME = r1.DATABASE_NAME
                           and ( TEST_ID in (l_testId1, l_testId2) or ( BEGIN_TIME >= l_begintest-l_days and END_TIME <= l_endtest))
                           and (CASE WHEN ( l_except = 'APP' and ( Upper(module) not in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER')
                                                                   and Upper(module) not like 'RMAN%'
                                                                   and module not like 'Oracle Enterprise Manager%'
                                                                  )) THEN 1
                                     WHEN ( l_except = 'DB'  and ( Upper(module) in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER') 
                                                                   or Upper(module) not like 'RMAN%'
                                                                   or module not like 'Oracle Enterprise Manager%'   
                                                                  )) THEN 1
                                     WHEN ( l_except is NULL ) THEN 1
                                     ELSE 0 END ) = 1 
                           and CPU_TIME_PER_EXEC_SECONDS is not NULL
                           order by TEST_ID, SQL_ID )
          , CURR as ( select f.SQL_ID 
                             ,f.MODULE
                             ,( CASE WHEN ROUND(f.CPU_TIME_PER_EXEC_SECONDS,10)=0 THEN NULL 
                                     ELSE ROUND(f.CPU_TIME_PER_EXEC_SECONDS,10) END ) as PropValue	
                        from FILTER f
                       where f.DATABASE_NAME = r1.DATABASE_NAME
                         and f.TEST_ID = l_testId1
                       order by SQL_ID )
          , PREV as ( select f.SQL_ID
                            ,f.MODULE
                             ,( CASE WHEN ROUND(f.CPU_TIME_PER_EXEC_SECONDS,10)=0 THEN NULL 
                                     ELSE ROUND(f.CPU_TIME_PER_EXEC_SECONDS,10) END ) as PropValue	
                        from FILTER f
                       where f.DATABASE_NAME = r1.DATABASE_NAME
                         and f.TEST_ID = l_testId2
                       order by f.SQL_ID )
          , AVER as ( select distinct f.SQL_ID
                            ,ROUND(Median(f.CPU_TIME_PER_EXEC_SECONDS) over (partition by f.SQL_ID),10) as Med
                            ,ROUND(Stddev(f.CPU_TIME_PER_EXEC_SECONDS) over (partition by f.SQL_ID),10) as dev
                            ,GREATEST(ROUND(Median(f.CPU_TIME_PER_EXEC_SECONDS) over (partition by f.SQL_ID) - l_dev *Stddev(f.CPU_TIME_PER_EXEC_SECONDS) over (partition by f.SQL_ID),10),0.0000000001) as Low
                            ,ROUND(Median(f.CPU_TIME_PER_EXEC_SECONDS) over (partition by f.SQL_ID) + l_dev *Stddev(f.CPU_TIME_PER_EXEC_SECONDS) over (partition by f.SQL_ID),10) as High
                        from FILTER f
                       where f.DATABASE_NAME = r1.DATABASE_NAME
                         and f.BEGIN_TIME BETWEEN l_begintest-l_days AND l_endtest-1
                       order by f.SQL_ID )
           , MAXS as ( select f.SQL_ID
                              ,ROUND(MAX(f.CPU_TIME_PER_EXEC_SECONDS),10) as Max
                              ,ROUND(MIN(f.CPU_TIME_PER_EXEC_SECONDS),10) as Min
                         from FILTER f
                        where f.DATABASE_NAME = r1.DATABASE_NAME
                         and f.BEGIN_TIME BETWEEN l_begintest-l_days AND l_endtest-1
                        group by SQL_ID ) 
           , RATIO as ( select c.SQL_ID
                         ,CASE WHEN p.PropValue is NULL and a.Med is not NULL THEN ROUND(c.PropValue/a.Med,2)
                               WHEN p.PropValue is NULL THEN 0
                               WHEN c.PropValue is NULL THEN 0
                               WHEN c.PropValue >= p.PropValue THEN ROUND(c.PropValue/p.PropValue,2) 
                               ELSE ROUND(p.PropValue/c.PropValue,2)*(-1) END as ratio 
                          from CURR c
                          left outer join PREV p on p.SQL_ID = c.SQL_ID 
                          left outer join AVER a on a.SQL_ID = c.SQL_ID
                      )
         select r1.DATABASE_NAME
               ,c.SQL_ID
               ,l_property as property
               ,c.MODULE
               ,c.PropValue as cur_PropValue
               ,p.PropValue as pre_PropValue
               ,NVL(a.Med,0) as Med
               ,NVL(a.dev,0) as dev
               ,NVL(a.Low,0) as Low
               ,NVL(m.Min,0) as Min
               ,NVL(a.High,0) as High
               ,NVL(m.Max,0) as Max
               ,CASE WHEN a.High is NULL or a.Low is NULL                   THEN 'NEW'
                     WHEN c.PropValue >= a.High and r.ratio >= l_ratio      THEN 'DEGRADED' 
                     WHEN c.PropValue >= a.High                             THEN 'SLOWER'
                     WHEN c.PropValue <= a.Low  and ABS(r.ratio) >= l_ratio THEN 'IMPROVED'
                     WHEN c.PropValue <= a.Low                              THEN 'FASTER'
                     ELSE '-' END as cur_assess
               ,CASE WHEN p.PropValue is NULL                               THEN 'NEW'
                     WHEN p.PropValue >= a.High and r.ratio >= l_ratio      THEN 'DEGRADED' 
                     WHEN p.PropValue >= a.High                             THEN 'SLOWER'
                     WHEN p.PropValue <= a.Low  and ABS(r.ratio) >= l_ratio THEN 'IMPROVED'
                     WHEN p.PropValue <= a.Low                              THEN 'FASTER'
                     ELSE '-' END as pre_assess
               ,CASE WHEN p.PropValue is NULL                                                        THEN 'NEW'
                     WHEN ( c.PropValue/p.PropValue ) > 1 and ( c.PropValue/p.PropValue ) >= l_ratio THEN 'DEGRADED'
                     WHEN ( c.PropValue/p.PropValue ) > 1                                            THEN 'SLOWER'
                     WHEN ( c.PropValue/p.PropValue ) < 1 and ( p.PropValue/c.PropValue ) >= l_ratio THEN 'IMPROVED'
                     WHEN ( c.PropValue/p.PropValue ) < 1                                            THEN 'FASTER'
                     ELSE '-' END as comp_assess                 
               ,r.ratio
        from CURR c
        LEFT OUTER JOIN PREV p on p.SQL_ID = c.SQL_ID
        LEFT OUTER JOIN AVER a on a.SQL_ID = c.SQL_ID
        LEFT OUTER JOIN MAXS m on m.SQL_ID = c.SQL_ID
        LEFT OUTER JOIN RATIO r on r.SQL_ID = c.SQL_ID
       ) s  ;
       COMMIT;

         -- Merge summary info into the summary temp table

       MERGE into TEST_RESULT_SQL_SUMMARY s
       USING ( select tr.DATABASE_NAME, tr.SQL_ID, tr.MODULE, tr.CUR_STATUS, tr.COMP_STATUS, tr.CUR_VAL, tr.DIFF_RATIO
                 from TEST_RESULT_SQL_RESULTS tr
                where tr.PROPERTY = 'CPU_TIME_PER_EXEC_SECONDS' 
              ) r  ON (s.DATABASE_NAME = r.DATABASE_NAME and s.SQL_ID = r.SQL_ID)
       WHEN MATCHED THEN
           update SET s.CPU_ST_TST = r.COMP_STATUS
                     ,s.CPU_ST_GEN = r.CUR_STATUS
                     ,s.CPU_VAL    = r.CUR_VAL
                     ,s.CPU_RATIO  = r.DIFF_RATIO
       WHEN NOT MATCHED THEN
          insert ( s.DATABASE_NAME, s.SQL_ID, s.MODULE, s.CPU_ST_TST, s.CPU_ST_GEN, s.CPU_VAL, s.CPU_RATIO )
          values ( r.DATABASE_NAME, r.SQL_ID, r.MODULE, r.COMP_STATUS, r.CUR_STATUS, r.CUR_VAL, r.DIFF_RATIO )
       ;     
       COMMIT;

    END LOOP ;

END Do_CPUTimePerExec;


PROCEDURE Do_FillTheGaps 
AS 
BEGIN

       -- Once all data has been updated into TEST_RESULT_SQL_RESULTS there'll be some NULLS for those Properties that did not fall into the selected categries 
       -- Foor completeness, we will retrieve those status and add them in.
       -- This can only be run when the whole TEST_RESULT_SQL_RESULTS has already been populated.

       MERGE into TEST_RESULT_SQL_SUMMARY s
       USING ( select tr.DATABASE_NAME, tr.SQL_ID, tr.CUR_STATUS, tr.COMP_STATUS, tr.DIFF_RATIO
                 from TEST_RESULT_SQL_RESULTS tr
                 join TEST_RESULT_SQL_SUMMARY ts on ( ts.DATABASE_NAME = tr.DATABASE_NAME and ts.SQL_ID = tr.SQL_ID ) 
                where tr.PROPERTY = 'ELAPSED_TIME_PER_EXEC_SECONDS' 
                  and ts.ELA_ST_TST is null 
              ) r  ON (s.DATABASE_NAME = r.DATABASE_NAME and s.SQL_ID = r.SQL_ID)
       WHEN MATCHED THEN
           update SET s.ELA_ST_TST = r.COMP_STATUS
                     ,s.ELA_ST_GEN = r.CUR_STATUS
                     ,s.ELA_RATIO  = r.DIFF_RATIO
       ;
       COMMIT;

       MERGE into TEST_RESULT_SQL_SUMMARY s
       USING ( select tr.DATABASE_NAME, tr.SQL_ID, tr.CUR_STATUS, tr.COMP_STATUS, tr.DIFF_RATIO
                 from TEST_RESULT_SQL_RESULTS tr
                 join TEST_RESULT_SQL_SUMMARY ts on ( ts.DATABASE_NAME = tr.DATABASE_NAME and ts.SQL_ID = tr.SQL_ID ) 
                where tr.PROPERTY = 'TPS' 
                  and ts.TPS_ST_TST is null 
              ) r  ON (s.DATABASE_NAME = r.DATABASE_NAME and s.SQL_ID = r.SQL_ID)
       WHEN MATCHED THEN
           update SET s.TPS_ST_TST = r.COMP_STATUS
                     ,s.TPS_ST_GEN = r.CUR_STATUS
                     ,s.TPS_RATIO  = r.DIFF_RATIO
       ;
       COMMIT;

       MERGE into TEST_RESULT_SQL_SUMMARY s
       USING ( select tr.DATABASE_NAME, tr.SQL_ID, tr.CUR_STATUS, tr.COMP_STATUS, tr.DIFF_RATIO
                 from TEST_RESULT_SQL_RESULTS tr
                 join TEST_RESULT_SQL_SUMMARY ts on ( ts.DATABASE_NAME = tr.DATABASE_NAME and ts.SQL_ID = tr.SQL_ID ) 
                where tr.PROPERTY = 'CPU_TIME_PER_EXEC_SECONDS' 
                  and ts.CPU_ST_TST is null 
              ) r  ON (s.DATABASE_NAME = r.DATABASE_NAME and s.SQL_ID = r.SQL_ID)
       WHEN MATCHED THEN
           update SET s.CPU_ST_TST = r.COMP_STATUS
                     ,s.CPU_ST_GEN = r.CUR_STATUS
                     ,s.CPU_RATIO  = r.DIFF_RATIO
       ;
       COMMIT;


END Do_FillTheGaps ;



PROCEDURE Do_AnalyticalData (
         i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
       , i_testId2 IN hp_diag.test_result_metrics.test_id%TYPE 
       , i_dev      IN NUMBER    DEFAULT 0.35
       , i_days     IN NUMBER    DEFAULT 30
       , i_ratio    IN NUMBER    DEFAULT 2
       , i_except   IN VARCHAR2  DEFAULT NULL        
       ) as

   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;

   -- Parameters driving the QUERIES
   l_dev         number;        -- deviation degree ( default 0.35 )
   l_days        number;        -- Number of days considered to extract a trend ( defalut would be 30 )
   l_ratio       number;        -- Minimum difference Ratio to qualify al alert as RED
   l_except      varchar2(200); -- Type of MODULEs to be excluded from the result ( APP, DB or NULL (all) )

BEGIN
   l_testId1 := i_testId1 ;
   l_testId2 := i_testId2 ;

   -- PARAMETERS
   l_dev    := i_dev ;
   l_days   := i_days ;
   l_ratio  := i_ratio ;
   l_except := i_except ;

   --Do_CreateTmpTables ;
   Do_TruncateTables ;
   Do_ElapsedTimePerExec(l_testId1,l_testId2,'ELAPSED_TIME_PER_EXEC_SECONDS',l_dev,l_days,l_ratio,l_except);
   Do_TPS(l_testId1,l_testId2,'TPS',l_dev,l_days,l_ratio,l_except);
   Do_CPUTimePerExec(l_testId1,l_testId2,'CPU_TIME_PER_EXEC_SECONDS',l_dev,l_days,l_ratio,l_except);
   Do_FillTheGaps ;

END Do_AnalyticalData ;


FUNCTION Get_DB_Details (
        i_testId1  IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2  IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_metric   IN varchar2 
      , i_filter   IN varchar2 DEFAULT NULL
      ) RETURN g_tvc2 PIPELINED
AS
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;
   l_Filter  varchar2(10) ;
   l_metric  varchar2(30) ;
   l_row     varchar2(4000);
   l_title   varchar2(30) ;
BEGIN
   l_testId1  := i_testId1 ;
   l_testId2  := i_testId2 ;
   l_Filter   := i_Filter ;
   l_metric   := i_metric ;

   IF l_metric = 'ELAPSED_TIME_PER_EXEC_SECONDS' THEN
       l_title := 'Elapsed Time(secs)' ;
   ELSIF l_metric = 'CPU_TIME_PER_EXEC_SECONDS' THEN 
       l_title := 'CPU Time(secs)' ;
   ELSIF l_metric = 'TPS' THEN 
       l_title := 'TPS' ;
   ELSE l_metric := 'N/A' ;
   END IF ;

   IF l_metric <> 'N/A' THEN
   PIPE ROW ( 'h3. Database Status Report by Metric' ) ;
   PIPE ROW ( 'Metric        : ' || l_metric ) ;
   PIPE ROW ( 'Current Test  : ' || l_testId1 ) ;  
   PIPE ROW ( 'Previous Test : ' || l_testId2 ) ;     
   PIPE ROW ( 'Filtering by  : ' || NVL(l_Filter,'All') ) ; 

   PIPE ROW ( '||Database||SQL ID||Module||Test Comparison||Overall Performance||'||l_title||'||Previous Time||Median||Low Med||High Med||Min Val||Max Val||Ratio||' ) ;
   FOR r1 IN (
      with data as (
         select DATABASE_NAME, SQL_ID, MODULE, CUR_VAL, PRE_VAL, MEDIAN_VAL, LOW_MED_VAL, HIGH_MED_VAL, MIN_VAL, MAX_VAL, CUR_STATUS, COMP_STATUS, DIFF_RATIO
           from TEST_RESULT_SQL_RESULTS 
          where PROPERTY = l_metric
            and ( CASE WHEN ( l_filter = 'RED'   AND ( ( CUR_STATUS  = 'DEGRADED' or  COMP_STATUS = 'DEGRADED' )
                                                    OR ( CUR_STATUS  = 'DEGRADED' and COMP_STATUS = 'NEW' )
                                                    OR ( COMP_STATUS = 'NEW'      and CUR_VAL > 1 and l_metric <> 'TPS' )
                                                    )) THEN 1
                       WHEN ( l_filter = 'AMBER' AND ( ( CUR_STATUS in ('SLOWER','LESS') and COMP_STATUS in ('SLOWER','LESS','NEW') )
                                                    OR ( CUR_STATUS in ('SLOWER','LESS') and COMP_STATUS = 'NEW' )
                                                    OR ( CUR_STATUS = 'NEW' and COMP_STATUS = 'NEW' and CUR_VAL <= 1 and CUR_VAL >= 0.1 and l_metric <> 'TPS' )
                                                    )) THEN 1
                       WHEN ( l_filter = 'GREEN' AND ( ( CUR_STATUS  NOT IN ('DEGRADED','SLOWER','LESS','NEW') )
                                                    OR ( COMP_STATUS NOT IN ('DEGRADED','SLOWER','LESS','NEW') )
                                                    OR ( CUR_STATUS = 'NEW' and COMP_STATUS = 'NEW' and CUR_VAL < 0.1 and l_metric <> 'TPS' ) 
                                                    OR ( CUR_STATUS = 'NEW' and COMP_STATUS = 'NEW' and l_metric = 'TPS' ) 
                                                    )) THEN 1
                       WHEN ( l_filter is NULL ) THEN 1
                       ELSE 0 END ) = 1
          order by DATABASE_NAME, DIFF_RATIO desc )
     , class as (
         select d.DATABASE_NAME
               ,d.SQL_ID
               ,CASE WHEN d.COMP_STATUS = 'DEGRADED'                                                THEN 'Red'
                     WHEN d.COMP_STATUS = 'NEW' and d.CUR_STATUS = 'DEGRADED'                       THEN 'Red'
                     WHEN d.COMP_STATUS = 'NEW' and d.CUR_VAL > 1 and l_metric <> 'TPS'             THEN 'Red'
                     WHEN d.COMP_STATUS = 'NEW' and d.CUR_VAL > 0.1 and l_metric <> 'TPS'           THEN 'Amber'                  
                     WHEN d.COMP_STATUS in ('SLOWER','LESS') and d.CUR_STATUS  = 'DEGRADED'         THEN 'Amber'
                     WHEN d.COMP_STATUS in ('SLOWER','LESS') and d.CUR_STATUS in ('SLOWER','LESS')  THEN 'Amber'
                     WHEN d.COMP_STATUS = 'NEW' and d.CUR_STATUS in ('SLOWER','LESS')               THEN 'Amber'
                     ELSE 'Green' END as db_status   -- Database Status
               ,CASE WHEN d.COMP_STATUS = 'DEGRADED'                                                THEN 'Red'
                     WHEN d.COMP_STATUS in ('SLOWER','LESS')                                        THEN 'Amber'
                     WHEN d.COMP_STATUS = 'NEW'                                                     THEN 'Blue'
                     ELSE 'Green' END as comp_st     -- Status based of Test Comparison 
               ,CASE WHEN d.CUR_STATUS = 'DEGRADED'                                                 THEN 'Red'
                     WHEN d.CUR_STATUS in ('SLOWER','LESS')                                         THEN 'Amber'
                     WHEN d.CUR_STATUS = 'NEW'                                                      THEN 'Blue'
                     ELSE 'Green' END as cur_st      -- Status over time for this SQLID
          from  data d )
       select  '|{status:colour=' || CASE WHEN c.db_status = 'Amber' THEN 'Yellow' ELSE c.db_status END || '|title=' || d.DATABASE_NAME || '}|'
              || d.SQL_ID || '|'
              || d.MODULE
              || '|{status:colour=' || CASE WHEN c.comp_st = 'Amber' THEN 'Yellow' ELSE c.comp_st END || '|title=' || d.COMP_STATUS || '}|'
              || '|{status:colour=' || CASE WHEN c.cur_st = 'Amber' THEN 'Yellow' ELSE c.cur_st END || '|title=' || d.CUR_STATUS || '}|'
              || d.CUR_VAL || '|'
              || NVL(d.PRE_VAL,0) || '|'
              || d.MEDIAN_VAL || '|'
              || d.LOW_MED_VAL || '|'
              || d.HIGH_MED_VAL || '|'
              || d.MIN_VAL || '|'
              || d.MAX_VAL || '|'
              || ABS(d.DIFF_RATIO) || '|' as col1
         from data d
         join class c on c.DATABASE_NAME = d.DATABASE_NAME and c.SQL_ID = d.SQL_ID
         order by d.DATABASE_NAME, d.SQL_ID, d.DIFF_RATIO DESC
         ) 
   LOOP
      PIPE ROW ( r1.col1 ) ;
   END LOOP ;
   END IF; 

END Get_DB_Details ;


FUNCTION Get_DB_Summary (
        i_testId1      IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2      IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_filter       IN varchar2 DEFAULT NULL
      , i_ratio        IN number default 2
      , i_summary      IN varchar2 default NULL
      ) RETURN g_tvc2 PIPELINED
AS
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;
   l_filter         varchar2(10) ;     -- Alerts being displayed ( RED / AMBER or GREEN )
   l_ratio          number;            -- Level of difference to evaluate the grading
   l_redsecs        number;            -- seconds to reach to raise a RED warning on new SQL
   l_ambersecs      number;            -- seconds to reach to raise an AMBER warning on new SQL
   l_summary        varchar2(3);       -- Flag to display the overall summary of warnings on Confluence ( default NULL would be a NO )

   l_dbName         varchar2(20) ;
   l_found          boolean ;
   l_foundNew       boolean ;
   l_row            varchar2(4000);
   l_maxRatio       number ;
   l_maxElapsed     number ;
   l_sqlId          varchar2(64) ;

   l_dbCount        number := 0 ;
   l_redCount       number := 0 ;
   l_amberCount     number := 0 ;
   l_greenCount     number := 0 ;

   l_redTotal       number := 0 ;
   l_amberTotal     number := 0 ;
   l_greenTotal     number := 0 ;
   l_sqlTotal       number := 0 ;

   l_redDisplay     number := 0 ;   
   l_amberDisplay   number := 0 ;   
   l_greenDisplay   number := 0 ;   

   cursor c_dbs is
      select distinct DATABASE_NAME 
      from TEST_RESULT_SQL_SUMMARY
      order by DATABASE_NAME
      ;

   cursor c_reds ( p_dbName varchar2
                  ,p_ratio  number   ) is
      select DATABASE_NAME
            ,SQL_ID
            ,MODULE
            ,ELA_ST_TST
            ,ELA_ST_GEN
            ,ELA_VAL 
            ,ELA_RATIO
       from TEST_RESULT_SQL_SUMMARY
      where DATABASE_NAME = p_dbName
        and (  ( ELA_ST_TST = 'DEGRADED' and ELA_ST_GEN = 'DEGRADED' and ELA_RATIO >= p_ratio )
            or ( ELA_ST_TST = 'NEW'      and ELA_ST_GEN = 'DEGRADED' and ELA_RATIO >= p_ratio )
            or ( ELA_ST_TST = 'NEW'      and ELA_ST_GEN = 'SLOWER'   and ELA_VAL > l_redsecs )
            or ( ELA_ST_TST = 'NEW'      and ELA_ST_GEN = 'NEW'      and ELA_VAL > l_redsecs )
            ) 
      ;

   cursor c_ambers ( p_dbName varchar2
                    ,p_ratio  number   ) is
      select DATABASE_NAME
            ,SQL_ID
            ,MODULE
            ,ELA_ST_TST
            ,ELA_ST_GEN
            ,ELA_VAL
            ,ELA_RATIO
       from TEST_RESULT_SQL_SUMMARY
      where DATABASE_NAME = p_dbName
        and ( (( ELA_ST_TST = 'DEGRADED' or  ELA_ST_GEN = 'DEGRADED' ) and ELA_RATIO < p_ratio )
         or    ( ELA_ST_TST = 'NEW'      and ELA_ST_GEN = 'DEGRADED'   and ELA_RATIO < p_ratio ) 
 --        or    ( ELA_ST_TST = 'SLOWER'   and ELA_ST_GEN = 'SLOWER' )   
         or    ( ELA_ST_TST = 'NEW'      and ELA_ST_GEN = 'SLOWER'     and ( ELA_VAL >= l_ambersecs and ELA_VAL <= l_redsecs ) )
         or    ( ELA_ST_TST = 'NEW'      and ELA_ST_GEN = 'NEW'        and ( ELA_VAL >= l_ambersecs and ELA_VAL <= l_redsecs ) )
            ) 
      ;   

   cursor c_greens ( p_dbName varchar2
                    ,p_ratio  number   ) is
      select DATABASE_NAME
            ,SQL_ID
            ,MODULE
            ,ELA_ST_TST
            ,ELA_ST_GEN
            ,ELA_VAL
            ,ELA_RATIO
       from TEST_RESULT_SQL_SUMMARY
      where DATABASE_NAME = p_dbName
--        and ( ( ELA_ST_TST not in ('DEGRADED','SLOWER','NEW') and ELA_ST_GEN not in ('DEGRADED','SLOWER','NEW') )
          and ( ( ELA_ST_TST not in ('DEGRADED','NEW') and ELA_ST_GEN not in ('DEGRADED','NEW') )
--          or  ( ELA_ST_TST = 'NEW' and ELA_ST_GEN not in ('DEGRADED','SLOWER','NEW') )       
          or  ( ELA_ST_TST = 'NEW' and ELA_ST_GEN not in ('DEGRADED','NEW') )       
          or  ( ELA_ST_TST = 'NEW' and ELA_ST_GEN = 'SLOWER' and ELA_VAL < l_ambersecs )
          or  ( ELA_ST_TST = 'NEW' and ELA_ST_GEN = 'NEW'    and ELA_VAL < l_ambersecs )
            )
      ;   

    r_reds          c_reds%rowtype ;
    r_redsnew       c_reds%rowtype ;
    r_ambers        c_ambers%rowtype ;
    r_ambersnew     c_ambers%rowtype ;
    r_greens        c_greens%rowtype ;
    r_greensnew     c_greens%rowtype ;
    
BEGIN
    l_testId1     := i_testId1 ;
    l_testId2     := i_testId2 ;
    l_filter      := NVL(i_filter,'ALL') ;
    l_ratio       := i_ratio ;
    l_summary     := NVL(i_summary,'NO') ;
    l_redsecs     := 1 ;  -- seconds to reach to raise a RED warning on new SQL
    l_ambersecs   := 0.1 ;  -- seconds to reach to raise an AMBER warning on new SQL

    PIPE ROW ( 'h3. Database Status Summary Totals' ) ;
    PIPE ROW ( 'Metric : ELAPSED_TIME_PER_EXEC_SECONDS' ) ;  
    PIPE ROW ( 'Current Test  : ' || l_testId1 ) ;  
    PIPE ROW ( 'Previous Test : ' || l_testId2 ) ;  
    PIPE ROW ( 'Filtering by  : ' || l_filter ) ; 

    PIPE ROW ( '||Database||SQL ID||Module||Differential Ratio||Status||Elapsed Time||' ) ;
    for v_dbs in c_dbs 
    loop
        l_redCount   := 0 ;
        l_amberCount := 0 ;   
        l_greenCount := 0 ;
        l_maxRatio   := null ;
        l_maxElapsed := null ;
        l_dbName     := v_dbs.DATABASE_NAME ;
        l_dbCount    := l_dbCount + 1 ;
        l_found      := false ; 
        l_foundNew   := false ; 

        -- Gathering the REDS

        for v_reds in c_reds (l_dbName,l_ratio) 
        loop
            l_found := true;
            l_redCount := l_redCount + 1 ;
            if v_reds.ELA_ST_TST = 'NEW' and v_reds.ELA_VAL > NVL(l_maxElapsed,0) then
                l_foundNew := true ;
                l_maxElapsed := v_reds.ELA_VAL ;
                r_redsnew := v_reds ;
            elsif v_reds.ELA_RATIO >= NVL(l_maxRatio,0) then
                l_maxRatio := v_reds.ELA_RATIO ;
                r_reds := v_reds ;
            end if ;
        end loop ; 
        if l_found and NVL(l_maxRatio,0) <> 0 then
            l_redDisplay := l_redDisplay + 1 ;
            l_row := '|{status:colour=Red|title=' || r_reds.DATABASE_NAME || '}|'
                     || r_reds.SQL_ID || '|' 
                     || r_reds.MODULE || '|' 
                     || ABS(r_reds.ELA_RATIO) || '|'
                     || r_reds.ELA_ST_TST || '|'
                     || ROUND(r_reds.ELA_VAL,5) || '|';
            if l_filter = 'ALL' or l_filter = 'RED' then
                PIPE ROW (l_row) ;   
            end if;    
        elsif l_found and l_foundNew and NVL(l_maxElapsed,0) > 0 and NVL(l_maxRatio,0) > 0 then
            l_row := '|{status:colour=Blue|title=' || r_redsnew.DATABASE_NAME || '}|'
                     || r_redsnew.SQL_ID || '|' 
                     || r_redsnew.MODULE || '|' 
                     || ABS(r_redsnew.ELA_RATIO) || '|'
                     || r_redsnew.ELA_ST_TST || '|'
                     || ROUND(r_redsnew.ELA_VAL,5) || '|';
            if l_filter = 'ALL' or l_filter = 'RED' then
                PIPE ROW (l_row)  ;   
            end if; 
        end if ;  
        l_redTotal := l_redTotal + l_redCount ;

        -- Gathering the AMBERS
        if not l_found then
            for v_ambers in c_ambers (l_dbName,l_ratio) 
            loop
                l_found := true ;
                l_amberCount := l_amberCount + 1 ;
                if v_ambers.ELA_ST_TST = 'NEW' and v_ambers.ELA_VAL > NVL(l_maxElapsed,0) then
                    l_foundNew := true ;
                    l_maxElapsed := v_ambers.ELA_VAL ;
                    r_ambersnew := v_ambers ;
                elsif v_ambers.ELA_RATIO > NVL(l_maxRatio,0) then
                    l_maxRatio := v_ambers.ELA_RATIO ;                        
                    r_ambers := v_ambers ;
                end if ;
            end loop ; 
            if l_found and NVL(l_maxRatio,0) <> 0 then
               l_amberDisplay := l_amberDisplay + 1 ;
               l_row := '|{status:colour=Yellow|title=' || r_ambers.DATABASE_NAME || '}|'
                         || r_ambers.SQL_ID || '|' 
                         || r_ambers.MODULE || '|' 
                         || ABS(r_ambers.ELA_RATIO) || '|'
                         || r_ambers.ELA_ST_TST || '|'
                         || ROUND(r_ambers.ELA_VAL,5) || '|';
                if l_filter = 'ALL' or l_filter = 'AMBER' then
                    PIPE ROW (l_row) ;   
                end if;    
            elsif l_found and l_foundNew and NVL(l_maxElapsed,0) > 0 and NVL(l_maxRatio,0) > 0 then
                l_row := '|{status:colour=Blue|title=' || r_ambersnew.DATABASE_NAME || '}|'
                        || r_ambersnew.SQL_ID || '|' 
                        || r_ambersnew.MODULE || '|' 
                        || ABS(r_ambersnew.ELA_RATIO) || '|'
                        || r_ambersnew.ELA_ST_TST || '|'
                        || ROUND(r_ambersnew.ELA_VAL,5) || '|';
                if l_filter = 'ALL' or l_filter = 'AMBER' then
                    PIPE ROW (l_row)  ;   
                end if; 
            end if ;              
        end if ;
        l_amberTotal := l_amberTotal + l_amberCount ;

        -- Gathering the GREENS
        if not l_found then
            for v_greens in c_greens (l_dbName,l_ratio) 
            loop
                l_found := true ;
                l_greenCount := l_greenCount + 1 ;
                if v_greens.ELA_ST_TST = 'NEW' and v_greens.ELA_VAL > NVL(l_maxElapsed,0) then
                    l_foundNew := true ;
                    l_maxElapsed := v_greens.ELA_VAL ;
                    r_greensnew  := v_greens ;
                elsif ABS(v_greens.ELA_RATIO) > NVL(l_maxRatio,0) then
                    l_maxRatio := v_greens.ELA_RATIO ;                        
                    r_greens := v_greens ;
                end if ;
            end loop ; 
            if l_found and NVL(l_maxRatio,0) <> 0 then
                l_greenDisplay := l_greenDisplay + 1 ;
                l_row := '|{status:colour=Green|title=' || r_greens.DATABASE_NAME || '}|'
                         || r_greens.SQL_ID || '|' 
                         || r_greens.MODULE || '|' 
                         || ABS(r_greens.ELA_RATIO) || '|'
                         || r_greens.ELA_ST_TST || '|'
                         || ROUND(r_greens.ELA_VAL,5) || '|';
                if l_filter = 'ALL' or l_filter = 'GREEN' then
                    PIPE ROW (l_row) ;   
                end if;      
            elsif l_found and l_foundNew and NVL(l_maxElapsed,0) > 0 and NVL(l_maxRatio,0) > 0 then
                l_row := '|{status:colour=Blue|title=' || r_greensnew.DATABASE_NAME || '}|'
                        || r_greensnew.SQL_ID || '|' 
                        || r_greensnew.MODULE || '|' 
                        || ABS(r_greensnew.ELA_RATIO) || '|'
                        || r_greensnew.ELA_ST_TST || '|'
                        || ROUND(r_greensnew.ELA_VAL,5) || '|';
                if l_filter = 'ALL' or l_filter = 'GREEN' then
                    PIPE ROW (l_row)  ;   
                end if; 
            end if ;              
        end if ;
        l_greenTotal := l_greenTotal + l_greenCount ;

    end loop ;

    l_sqlTotal := l_RedTotal+l_AmberTotal+l_GreenTotal ;

    -- Display summary only if requested
    if l_summary <> 'NO' then   
        PIPE ROW ( 'h5. Status Totals Per Database' ) ;
        PIPE ROW ( 'Metric : ELAPSED_TIME_PER_EXEC_SECONDS' ) ;  

        PIPE ROW ( '||Decreased||Slower||Acceptable||' ) ;
        l_row := '|{status:colour=Red|title=' || ROUND(l_RedDisplay*100/l_dbCount,2) || '%}|'
                 || '|{status:colour=Yellow|title=' || ROUND(l_AmberDisplay*100/l_dbCount,2) || '%}|'
                 || '|{status:colour=Green|title=' || ROUND(100-(l_RedDisplay*100/l_dbCount+l_AmberDisplay*100/l_dbCount),2) || '%}|';
 --                || '|{status:colour=Green|title=' || ROUND(l_GreenTotal*100/l_sqlTotal,2)  || '%}|' ;
        PIPE ROW (l_row) ;  

        PIPE ROW ( 'h5. Status Totals Per SQLIDs' ) ;
        PIPE ROW ( 'Metric : ELAPSED_TIME_PER_EXEC_SECONDS' ) ; 

        PIPE ROW ( '||Decreased||Slower||Acceptable||' ) ;
        l_row := '|{status:colour=Red|title=' || ROUND(l_RedTotal*100/l_sqlTotal,2) || '%}|'
                  || '|{status:colour=Yellow|title=' || ROUND(l_AmberTotal*100/l_sqlTotal,2)  || '%}|'
                  || '|{status:colour=Green|title=' || ROUND(100-(l_RedTotal*100/l_sqlTotal+l_AmberTotal*100/l_sqlTotal),2)  || '%}|' ;                  
--                  || '|{status:colour=Green|title=' || ROUND(l_GreenTotal*100/l_sqlTotal,2)  || '%}|' ;
        PIPE ROW (l_row) ;   
    end if ;

END Get_DB_Summary ;

-- #############################  DB Summary Procedures/Functions - END ##########################################

-- #############################  Function replaced by Get_DB_Summary_Totals ##########################################
FUNCTION Get_DB_Summary_Old (
        i_testId1      IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_testId2      IN hp_diag.test_result_metrics.test_id%TYPE 
      , i_filter       IN varchar2 DEFAULT NULL
      , i_ratio        IN number default 3
      , i_occurrences  IN number default 2
      ) RETURN g_tvc2 PIPELINED
AS
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;
   l_testId2 hp_diag.test_result_metrics.test_id%TYPE ;
   l_filter       varchar2(10) ; 
   l_ratio        number;
   l_occurrences  number;

   l_row     varchar2(4000);   
BEGIN
   l_testId1     := i_testId1 ;
   l_testId2     := i_testId2 ;
   l_filter      := i_filter ;
   l_ratio       := i_ratio ;
   l_occurrences := i_occurrences ;

   PIPE ROW ( 'h3. Database Status Summary' ) ;
   PIPE ROW ( 'Metric : ELAPSED_TIME_PER_EXEC_SECONDS' ) ;  
   PIPE ROW ( 'Current Test  : ' || l_testId1 ) ;  
   PIPE ROW ( 'Previous Test : ' || l_testId2 ) ;  
   PIPE ROW ( 'Filtering by  : ' || NVL(i_filter,'All') ) ; 

   PIPE ROW ( '||Database||SQL ID||Module||Differential Ratio||No. SQL Statements||' ) ;
   FOR r1 IN (
      with max as ( select DATABASE_NAME, max(ELA_RATIO) as MaxRatio
                      from TEST_RESULT_SQL_SUMMARY
                     where ( CASE WHEN ( l_filter = 'RED'   and ( ELA_ST_TST = 'DEGRADED' or ELA_ST_GEN = 'DEGRADED' )) THEN 1
                                  WHEN ( l_filter = 'AMBER' and ( ELA_ST_TST = 'SLOWER'  and ELA_ST_GEN = 'SLOWER' )) THEN 1
                                  WHEN ( l_filter = 'GREEN' and ( ELA_ST_TST NOT IN ('DEGRADED','SLOWER') or ELA_ST_GEN NOT IN ('DEGRADED','SLOWER' ))) THEN 1
                                  WHEN ( l_filter is NULL   and ( ELA_ST_TST IN ('DEGRADED','SLOWER') or ELA_ST_GEN IN ('DEGRADED','SLOWER' ))) THEN 1
                                  WHEN ( l_filter is NULL   and ( ELA_ST_TST NOT IN ('DEGRADED','SLOWER') or ELA_ST_GEN NOT IN ('DEGRADED','SLOWER' ))) THEN 1
                                  ELSE 0 END ) = 1 
                      group by DATABASE_NAME )
         , max2 as ( select SQL_ID, max(ELA_RATIO) as MaxRatio
                      from TEST_RESULT_SQL_SUMMARY
                     where ( CASE WHEN ( l_filter = 'RED'   and ( ELA_ST_TST = 'DEGRADED' or ELA_ST_GEN = 'DEGRADED' )) THEN 1
                                  WHEN ( l_filter = 'AMBER' and ( ELA_ST_TST = 'SLOWER'  and ELA_ST_GEN = 'SLOWER' )) THEN 1
                                  WHEN ( l_filter = 'GREEN' and ( ELA_ST_TST NOT IN ('DEGRADED','SLOWER') or ELA_ST_GEN NOT IN ('DEGRADED','SLOWER' ))) THEN 1
                                  WHEN ( l_filter is NULL   and ( ELA_ST_TST IN ('DEGRADED','SLOWER') or ELA_ST_GEN IN ('DEGRADED','SLOWER' ))) THEN 1
                                  WHEN ( l_filter is NULL   and ( ELA_ST_TST NOT IN ('DEGRADED','SLOWER') or ELA_ST_GEN NOT IN ('DEGRADED','SLOWER' ))) THEN 1
                                  ELSE 0 END ) = 1
                      group by SQL_ID )             
         , cnt as ( select DATABASE_NAME, count(DATABASE_NAME) as occurrences
                      from TEST_RESULT_SQL_SUMMARY
                     where ( CASE WHEN ( l_filter = 'RED'   and ( ELA_ST_TST = 'DEGRADED' or ELA_ST_GEN = 'DEGRADED' )) THEN 1
                                  WHEN ( l_filter = 'AMBER' and ( ELA_ST_TST = 'SLOWER'  and ELA_ST_GEN = 'SLOWER' )) THEN 1
                                  WHEN ( l_filter = 'GREEN' and ( ELA_ST_TST NOT IN ('DEGRADED','SLOWER') or ELA_ST_GEN NOT IN ('DEGRADED','SLOWER' ))) THEN 1
                                  WHEN ( l_filter is NULL ) THEN 1
                                  ELSE 0 END ) = 1
                      group by DATABASE_NAME )
         ,data as ( select a.DATABASE_NAME, a.SQL_ID, a.MODULE, a.ELA_ST_TST, a.ELA_ST_GEN, a.ELA_VAL, t.MaxRatio, c.occurrences
                      from TEST_RESULT_SQL_SUMMARY a
                      join max t on ( t.DATABASE_NAME = a.DATABASE_NAME )
                      join cnt c on ( c.DATABASE_NAME = a.DATABASE_NAME )
                      join max2 m on ( m.SQL_ID = a.SQL_ID )
                     where a.ELA_RATIO = t.MaxRatio
                       and a.ELA_RATIO = m.MaxRatio
                     order by t.MaxRatio desc, c.occurrences desc )
         , class as ( select d.DATABASE_NAME
                            ,d.SQL_ID 
                            ,CASE WHEN d.ELA_ST_TST = 'DEGRADED' and d.ELA_ST_GEN = 'DEGRADED' and d.MaxRatio >= l_ratio THEN 'Red'
                                  WHEN ( d.ELA_ST_TST = 'NEW' or d.ELA_ST_GEN = 'NEW' ) and  d.ELA_VAL > 1               THEN 'Red'
                                  WHEN ( d.ELA_ST_TST = 'NEW' or d.ELA_ST_GEN = 'NEW' ) and  d.ELA_VAL > 0.1             THEN 'Amber'
                                  WHEN d.ELA_ST_TST = 'DEGRADED'  or d.ELA_ST_GEN = 'DEGRADED'                           THEN 'Amber'
                                  WHEN d.ELA_ST_TST = 'SLOWER'    or d.ELA_ST_GEN = 'SLOWER'                             THEN 'Green'
                                  ELSE 'Green' END as db_status 
                        from data d )
       select  '|{status:colour=' || CASE WHEN c.db_status = 'Amber' THEN 'Yellow' ELSE c.db_status END || '|title=' || d.DATABASE_NAME || '}|'
              || d.SQL_ID || '|' 
              || d.MODULE || '|' 
              || ABS(d.MaxRatio) || '|'
              || d.occurrences || '|' as col1
         from data d
         join class c on c.DATABASE_NAME = d.DATABASE_NAME and c.SQL_ID = d.SQL_ID
         order by d.DATABASE_NAME
         ) 
   LOOP
      PIPE ROW ( r1.col1 ) ;
   END LOOP ;

END Get_DB_Summary_old ;

-- ########################################################################################################################







END REPORT_DATA;
/
