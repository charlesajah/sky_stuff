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


END REPORT_DATA;
/
