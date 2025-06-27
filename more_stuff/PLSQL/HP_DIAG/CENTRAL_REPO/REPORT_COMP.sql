CREATE OR REPLACE PACKAGE REPORT_COMP AS 

/* This is the REPOSITORY of Procedures & Functions common for 
   all the COMPARISON utilities within the Confluenece Reports Generation 
*/ 

   TYPE g_tvc2      IS TABLE OF VARCHAR2(4000) ;
   TYPE g_wc_comp   IS TABLE OF wc_comp_obj ;
   TYPE g_load_comp IS TABLE OF load_comp_obj ;
   TYPE g_sql_comp  IS TABLE OF sql_comp_obj ;

   FUNCTION Get_chart_comparison (
        i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE default NULL
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION Get_pct_comparison (
        i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE DEFAULT 'Latest test'
      , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE DEFAULT 'Best previous release'
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION Get_top25_comparison_summary (
        i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_desc    IN varchar2 default null
      , i_title   IN varchar2 default 'Y'
      , i_link    IN varchar2 default 'Y'  
      , i_mode    IN varchar2 default NULL
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION Get_top25_comparison_full (
        i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_title   IN varchar2 default 'Y'   
      , i_mode    IN varchar2 default NULL
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION Get_top25_metrics (
        i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED ;

/*   FUNCTION Get_top25_long_comp (
        i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED ;
*/
   FUNCTION Get_load_comparison_full (
        i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_title   IN varchar2 default 'Y'
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION Get_load_comparison_summary (
        i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_title   IN varchar2 default 'Y' 
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION Get_top25_detail (
        i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_title   IN varchar2 default 'N'
      , i_mode    IN varchar2 default NULL           
      ) RETURN g_tvc2 PIPELINED;

/*   FUNCTION Get_top25_long_comp_db (
        i_dbname  IN varchar2
       ,i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
       ,i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED ;
*/
/*   FUNCTION Get_top_n_long_comp_db ( i_dbname IN varchar2 
        ,i_limit IN number DEFAULT 25 
        ,i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE
        ,i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
        ,i_testId3 IN TEST_RESULT_MASTER.test_id%TYPE 
        ,i_testId4 IN TEST_RESULT_MASTER.test_id%TYPE 
       ) RETURN g_tvc2 PIPELINED;
*/

    FUNCTION Get_all_db_top25_comparison (
        i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_title   IN varchar2 default 'Y'
      , i_mode    IN varchar2 default NULL
      , i_top_n   IN number  default NULL
      ) RETURN g_tvc2 PIPELINED ;

    FUNCTION Get_rlw_comparison (
        i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE DEFAULT 'Latest test'
      , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE DEFAULT 'Best previous release'
      ) RETURN g_tvc2 PIPELINED ;  

    FUNCTION Get_DB_Activity_Headers (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_dbname  IN varchar2
          ) RETURN g_tvc2 PIPELINED;

    FUNCTION Get_DB_Activity_Body (
            i_testId  IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_dbname  IN varchar2 
          ) RETURN g_tvc2 PIPELINED;       

    FUNCTION Get_DB_Charts (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE default NULL
          ) RETURN g_tvc2 PIPELINED;     

    FUNCTION Get_SQLID_byText (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_title   IN varchar2 default 'Y'
          , i_mode    IN varchar2 default NULL
          , i_top_n   IN number   default NULL
          ) RETURN g_tvc2 PIPELINED;

    FUNCTION Get_SQL_Analysis (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE
          , i_c_sql_id IN TEST_RESULT_SQL.sql_id%TYPE default NULL
          , i_p_sql_id IN TEST_RESULT_SQL.sql_id%TYPE default NULL
          , i_database_name IN TEST_RESULT_SQL.database_name%TYPE
          , i_tps_status IN varchar2 default NULL
          , i_duration_status IN varchar2 default NULL
          , i_db_time_per_sec_status IN varchar2 default NULL
          , i_db_cpu_per_sec_status IN varchar2 default NULL
          , i_execs_per_sec_status IN varchar2 default NULL
          , i_cur_db_time_per_sec IN number default NULL
          , i_prev_db_time_per_sec IN number default NULL
          , i_cur_db_cpu_per_sec IN number default NULL
          , i_prev_db_cpu_per_sec IN number default NULL
          , i_cur_db_execs_per_sec IN number default NULL
          , i_prev_db_execs_per_sec IN number default NULL
          , i_cur_tps IN number default NULL
          , i_prev_tps IN number default NULL
          , i_cur_els IN number default NULL
          , i_prev_els IN number default NULL
          , i_cur_rows_processed IN number default NULL
          , i_prev_rows_processed IN number default NULL
          , i_cur_rows_per_execution IN number default NULL
          , i_prev_rows_per_execution IN number default NULL
          , i_cur_executions IN number default NULL
          , i_prev_executions IN number default NULL
          , i_cur_cpu_time_seconds IN number default NULL
          , i_prev_cpu_time_seconds IN number default NULL
    ) RETURN VARCHAR2;  

    Function GET_BEST_PLAN(
      i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE,
      i_database_name IN TEST_RESULT_SQL.database_name%TYPE,
      i_c_sql_id IN TEST_RESULT_SQL.sql_id%TYPE,
      l_end varchar2
    ) RETURN VARCHAR2 ;
    
   FUNCTION Get_waitclass_comparison_summary (
        i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_title   IN varchar2 default 'Y'
      ) RETURN g_tvc2 PIPELINED ;   


    FUNCTION Get_all_db_top10_comparison (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_title   IN varchar2 default 'Y'
          , i_mode    IN varchar2 default NULL
          , i_top_n   IN number  default NULL
          ) RETURN g_tvc2 PIPELINED;
          
          
END REPORT_COMP;
/


CREATE OR REPLACE PACKAGE BODY REPORT_COMP AS


/*--------------------------------------------------------------------------------- */
    -- Get_chart_comparison 
    -- Returns the Charted information from each database METRICS 
    -- Reads from TEST_RESULT_MASTER, TEST_RESULT_METRICS and TEST_RESULT_WAIT_CLASS
    -- Added a clause that will eliminate databases that are not present in BOTH tests
    -- The new chart reports in ALL databases 
    -- Cross environment ready
/*--------------------------------------------------------------------------------- */  
    FUNCTION Get_chart_comparison (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE default NULL
          ) RETURN g_tvc2 PIPELINED
    AS
        t_test1         TEST_RESULT_MASTER%ROWTYPE ;
        t_test2         TEST_RESULT_MASTER%ROWTYPE ;
        l_row           varchar2(4000);
        l_header_row    varchar2(4000) ;
        l_env           varchar2(30);
        l_grp           varchar2(4);
        l_dbname        varchar2(30);
        l_tmp           varchar2(10);

        -- Local variables
        l_query         varchar2(4000);
        l_header        varchar2(4000);
        l_select        varchar2(4000);
        l_pivot         varchar2(4000);
        l_title         varchar2(50);

        -- Arrays
        type  r_row  is table of varchar2(4000);
        l_return        r_row ;

        cursor c_dbs ( p_env    test_result_master.db_env%TYPE
                      ,p_grp    test_result_master.db_group%TYPE
                      ,p_db     varchar2
           ) is
             select distinct DB_TYPE as DB_NAME
              from V_TEST_RESULT_DBS 
             where DB_GROUP = DECODE ( p_grp, 'DB' , 'FULL', p_grp )
               and DB_ENV = DECODE ( p_env, 'ALL', DB_ENV , p_env )
               and DB_ENV != 'PRD'
               and DB_NAME = NVL(Upper(p_db),DB_NAME) 
             order by DB_NAME ;         

        FUNCTION lget_sql ( p_testid1 IN test_result_master.test_id%TYPE
                          , p_testid2 IN test_result_master.test_id%TYPE
                          , p_metric IN varchar2 ) Return varchar2
        IS
            l_sql       varchar2(4000);
        BEGIN
            l_sql := q'# select '|' || a.LIST_TESTS || #' || l_select || q'#
                           from ( select * 
                                    from ( select *
                                             from ( select db.DB_TYPE as DATABASE_NAME
                                                         , m.TEST_DESCRIPTION LIST_TESTS
                                                         , ROUND ( CASE m.METRIC_NAME WHEN 'Total PGA Allocated' THEN m.average/1024/1024/1024 ELSE average END , 2 ) VALUE 
                                                     from TEST_RESULT_METRICS m
                                                     join TEST_RESULT_DBS db on ( db.DB_NAME = m.DATABASE_NAME )
                                                    where m.TEST_ID IN ('#' || p_testid1 || q'#','#' || p_testid2 || q'#')
                                                      and m.METRIC_NAME = '#' || p_metric || q'#' 
                                                  )
                                             pivot (AVG(VALUE)  for (DATABASE_NAME)  in (#' || l_pivot || q'#))
                                             order by 1
                                         )
                                 ) a #';
            return l_sql ;
        END lget_sql ;


    BEGIN
        -- default parameters
        t_test1 := REPORT_GATHER.Get_Test_Details(i_testId1) ;
        t_test2 := REPORT_GATHER.Get_Test_Details(i_testId2) ;

        l_header := '|| LIST_TESTS ||' ;
        l_select := '''|''' ;
        l_pivot  := null ;

        -- Proces First ID
        l_env    := REPORT_ADM.Parse_TestId(t_test1.TEST_ID,'ENV') ;
        l_grp    := REPORT_ADM.Parse_TestId(t_test1.TEST_ID,'GRP') ;
        l_dbname := REPORT_ADM.Parse_TestId(t_test1.TEST_ID,'DB') ;

        -- Generate lines for SQL Code and Headers from the list of databases involved
        for v_dbs in c_dbs (l_env, l_grp, l_dbname)
        loop
            l_header := l_header || v_dbs.DB_NAME || '||' ;
            l_select := l_select || '|| NVL(a.' || v_dbs.DB_NAME || ',0) ||''|''' ; 
            select DECODE(l_pivot,null,'''',',''') into l_tmp from dual ;
            l_pivot := l_pivot || l_tmp || v_dbs.DB_NAME  || ''' as '||v_dbs.DB_NAME ;
        end loop;

        -- CHART COMPARISON : TEST_RESULT_METRICS

        PIPE ROW ( 'h3. Comparison with Previous Release' ) ;

        l_title :=  'Host CPU Utilization (%)';
        PIPE ROW ( 'h5. '|| l_title ) ;
        l_row := '{chart:type=bar | width=1000 | height=1500 | title = ' || l_title || ' | orientation = horizontal}' ;
        PIPE ROW ( l_row ) ;
        l_row := l_header ;
        PIPE ROW ( l_row ) ;        
        l_query := lget_sql(t_test1.TEST_ID, t_test2.TEST_ID, l_title);
        EXECUTE IMMEDIATE l_query BULK COLLECT INTO l_return;
        for i in 1 .. l_return.COUNT
        loop
            l_row := l_return(i);
            PIPE ROW ( l_row ) ; 
        end loop;
        PIPE ROW ('{chart}') ;    

        l_title :=  'Average Active Sessions';
        PIPE ROW ( 'h5. '|| l_title ) ;
        l_row := '{chart:type=bar | width=1000 | height=1500 | title = ' || l_title || ' | orientation = horizontal}' ;
        PIPE ROW ( l_row ) ;
        l_row := l_header ;
        PIPE ROW ( l_row ) ;        
        l_query := lget_sql(t_test1.TEST_ID, t_test2.TEST_ID, l_title);
        EXECUTE IMMEDIATE l_query BULK COLLECT INTO l_return;
        for i in 1 .. l_return.COUNT
        loop
            l_row := l_return(i);
            PIPE ROW ( l_row ) ; 
        end loop;
        PIPE ROW ('{chart}') ;    

        l_title :=  'Current OS Load';
        PIPE ROW ( 'h5. '|| l_title ) ;
        l_row := '{chart:type=bar | width=1000 | height=1500 | title = ' || l_title || ' | orientation = horizontal}' ;
        PIPE ROW ( l_row ) ;
        l_row := l_header ;
        PIPE ROW ( l_row ) ;        
        l_query := lget_sql(t_test1.TEST_ID, t_test2.TEST_ID, l_title);
        EXECUTE IMMEDIATE l_query BULK COLLECT INTO l_return;
        for i in 1 .. l_return.COUNT
        loop
            l_row := l_return(i);
            PIPE ROW ( l_row ) ; 
        end loop;
        PIPE ROW ('{chart}') ;    


        l_title :=  'User Commits Per Sec';
        PIPE ROW ( 'h5. '|| l_title ) ;
        l_row := '{chart:type=bar | width=1000 | height=1500 | title = ' || l_title || ' | orientation = horizontal}' ;
        PIPE ROW ( l_row ) ;
        l_row := l_header ;
        PIPE ROW ( l_row ) ;        
        l_query := lget_sql(t_test1.TEST_ID, t_test2.TEST_ID, l_title);
        EXECUTE IMMEDIATE l_query BULK COLLECT INTO l_return;
        for i in 1 .. l_return.COUNT
        loop
            l_row := l_return(i);
            PIPE ROW ( l_row ) ; 
        end loop;
        PIPE ROW ('{chart}') ;    

        l_title :=  'Physical Read Total Bytes Per Sec';
        PIPE ROW ( 'h5. '|| l_title ) ;
        l_row := '{chart:type=bar | width=1000 | height=1500 | title = ' || l_title || ' | orientation = horizontal}' ;
        PIPE ROW ( l_row ) ;
        l_row := l_header ;
        PIPE ROW ( l_row ) ;        
        l_query := lget_sql(t_test1.TEST_ID, t_test2.TEST_ID, l_title);
        EXECUTE IMMEDIATE l_query BULK COLLECT INTO l_return;
        for i in 1 .. l_return.COUNT
        loop
            l_row := l_return(i);
            PIPE ROW ( l_row ) ; 
        end loop;
        PIPE ROW ('{chart}') ;         

        -- CHART COMPARISON : TEST_RESULT_WAIT_CLASS
        -- Average Sessions Breakdown

        PIPE ROW ( 'h5. Average Active Session Breakdown' ) ;
        -- 2.5 Get the AAS breakdown per database
        FOR rh IN (
          select distinct db.db_type 
            from TEST_RESULT_WAIT_CLASS t 
            join v_TEST_RESULT_DBS db on ( db.db_name = t.database_name and db.db_env = t_test1.DB_ENV )
           where t.test_id IN ( t_test1.TEST_ID ) 
           order by 1
        )
        LOOP
          PIPE ROW ( '{chart:type=bar | width=500 | height=750 | title = Average Session Breakdown for ' || rh.db_type || ' | orientation = horizontal}' ) ;
          with q as (select distinct wait_class from TEST_RESULT_WAIT_CLASS where test_id IN (t_test1.TEST_ID, t_test2.TEST_ID ) order by 1)
          select '|| LIST_TESTS || ' ||  listagg(wait_class, '||') within group (order by wait_class) || '||' INTO l_header_row
            from q;
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
                        FROM TEST_RESULT_WAIT_CLASS a
                        JOIN TEST_RESULT_DBS db ON ( db.DB_NAME = a.DATABASE_NAME )
                       where test_id in (t_test1.TEST_ID , t_test2.TEST_ID )
                         and db.db_type = rh.db_type
                      group by a.test_id, a.database_name
                      order by 1 ASC)
          )
          loop
             PIPE ROW ( r1.text_output ) ;
          end loop;
          PIPE ROW ( '{chart}' ) ;  
        END LOOP ;  


        -- CHART COMPARISON : TEST_RESULT_METRICS
        -- Chordiant Database in Detail       

        if l_dbname in ('CHORDO','CCS021N') or l_dbname is NULL then

            -- return data in figures as well as in chart - chordiant.
            PIPE ROW ( 'h5.Comparison with previous release in numbers, Chordiant database' ) ;
            PIPE ROW ( '{csv:allowExport=true|columnTypes=s,f,f,f,f,s|id=ComparisonNumbersChordiant}' ) ;

            -- header row 
            --PIPE ROW ( '"Metric","' || t_test1.TEST_DESCRIPTION || '","' || t_test2.TEST_DESCRIPTION || '","Pct Increase","Value Increase","Units"' ) ;
            PIPE ROW ( '"Metric","Current Test","Previous Test","Pct Increase","Value Increase","Units"' ) ;

            -- data rows
            FOR r1 IN (
              WITH base AS (
                 SELECT test_id
                      , CASE metric_name WHEN 'Physical Read Total Bytes Per Sec' THEN 'Physical Read Total MB Per Sec' ELSE metric_name END AS metric_name
                      , CASE WHEN metric_name IN ( 'Physical Read Total Bytes Per Sec' , 'Temp Space Used' , 'Total PGA Allocated' ) THEN average/1024/1024 ELSE average END AS average
                      , CASE metric_unit WHEN 'bytes' THEN 'mb' WHEN 'Bytes Per Second' THEN 'mb per Second' ELSE metric_unit END AS metric_unit
                   FROM TEST_RESULT_METRICS
                   JOIN TEST_RESULT_DBS db ON ( db.db_name = DATABASE_NAME )
                  WHERE db.DB_TYPE = 'CCS'
                    AND test_id IN ( t_test1.TEST_ID , t_test2.TEST_ID )
              ) , t1 AS (
                 SELECT * FROM base WHERE test_id = t_test1.TEST_ID
              ) , t2 AS (
                 SELECT * FROM base WHERE test_id = t_test2.TEST_ID
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
        end if;


        -- CHART COMPARISON : TEST_RESULT_METRICS
        -- SALES Database in Detail   

        if SUBSTR(l_dbname,1,3) = 'ISS' or l_dbname is NULL then 

            --  return data in figures as well as in chart - sal/sis/iss.
            PIPE ROW ( 'h5.Comparison with previous release in numbers, SAL/SIS database' ) ;
            PIPE ROW ( '{csv:allowExport=true|columnTypes=s,f,f,f,f,s|id=ComparisonNumbersSal}' ) ;
            -- header row 
            --PIPE ROW ( '"Metric","' || t_test1.TEST_DESCRIPTION || '","' || t_test2.TEST_DESCRIPTION  || '","Pct Increase","Value Increase","Units"' ) ;
            PIPE ROW ( '"Metric","Current Test","Previous Test","Pct Increase","Value Increase","Units"' ) ;   
            -- data rows
            FOR r1 IN (
              WITH base AS (
                 SELECT test_id
                      , CASE metric_name WHEN 'Physical Read Total Bytes Per Sec' THEN 'Physical Read Total MB Per Sec' ELSE metric_name END AS metric_name
                      , CASE WHEN metric_name IN ( 'Physical Read Total Bytes Per Sec' , 'Temp Space Used' , 'Total PGA Allocated' ) THEN average/1024/1024 ELSE average END AS average
                      , CASE metric_unit WHEN 'bytes' THEN 'mb' WHEN 'Bytes Per Second' THEN 'mb per Second' ELSE metric_unit END AS metric_unit
                   FROM TEST_RESULT_METRICS
                   JOIN TEST_RESULT_DBS db ON ( db.db_name = DATABASE_NAME )
                  WHERE db.DB_TYPE = 'ISS' 
                    AND test_id IN ( t_test1.TEST_ID , t_test2.TEST_ID )
              ) , t1 AS (
                 SELECT * FROM base WHERE test_id = t_test1.TEST_ID
              ) , t2 AS (
                 SELECT * FROM base WHERE test_id = t_test2.TEST_ID
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
        end if;

        -- CHART COMPARISON : TEST_RESULT_METRICS
        -- OMS Database in Detail   

        if SUBSTR(l_dbname,1,3) = 'OMS' or l_dbname is NULL then 

            --  return data in figures as well as in chart - oms.
            PIPE ROW ( 'h5.Comparison with previous release in numbers, OMS database' ) ;
            PIPE ROW ( '{csv:allowExport=true|columnTypes=s,f,f,f,f,s|id=ComparisonNumbersOms}' ) ;
            -- header row 
            --PIPE ROW ( '"Metric","' || t_test1.TEST_DESCRIPTION || '","' || t_test2.TEST_DESCRIPTION || '","Pct Increase","Value Increase","Units"' ) ;
            PIPE ROW ( '"Metric","Current Test","Previous Test","Pct Increase","Value Increase","Units"' ) ;
            -- data rows
            FOR r1 IN (
              WITH base AS (
                 SELECT test_id
                      , CASE metric_name WHEN 'Physical Read Total Bytes Per Sec' THEN 'Physical Read Total MB Per Sec' ELSE metric_name END AS metric_name
                      , CASE WHEN metric_name IN ( 'Physical Read Total Bytes Per Sec' , 'Temp Space Used' , 'Total PGA Allocated' ) THEN average/1024/1024 ELSE average END AS average
                      , CASE metric_unit WHEN 'bytes' THEN 'mb' WHEN 'Bytes Per Second' THEN 'mb per Second' ELSE metric_unit END AS metric_unit
                   FROM TEST_RESULT_METRICS
                   JOIN TEST_RESULT_DBS db ON ( db.db_name = DATABASE_NAME )
                  WHERE db.DB_TYPE = 'OMS' 
                    AND test_id IN ( t_test1.TEST_ID , t_test2.TEST_ID )
              ) , t1 AS (
                 SELECT * FROM base WHERE test_id = t_test1.TEST_ID
              ) , t2 AS (
                 SELECT * FROM base WHERE test_id = t_test2.TEST_ID
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
        end if;

    END Get_chart_comparison;





/*--------------------------------------------------------------------------------- */
    -- Get_pct_comparison
    -- Cross environment ready
/*--------------------------------------------------------------------------------- */      
    FUNCTION Get_pct_comparison (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE
          ) RETURN g_tvc2 PIPELINED
    AS
        l_metric_name g_tvc2 ;
        l_idx VARCHAR2(100) ;
        t_test1         TEST_RESULT_MASTER%ROWTYPE ;
        t_test2         TEST_RESULT_MASTER%ROWTYPE ;

        l_avg_1   number;
        l_max_1   number;
        l_avg_2   number;
        l_max_2   number;
        l_avg_pct number;
        l_max_pct number;   
        l_row     varchar2(4000);
    BEGIN
        -- default parameters
        t_test1 := REPORT_GATHER.Get_Test_Details(i_testId1) ;
        t_test2 := REPORT_GATHER.Get_Test_Details(i_testId2) ;

       -- Wait classes we are intersted in
       l_metric_name := NEW g_tvc2 ( 'Administrative', 'Application', 'CPU', 'Commit', 'Concurrency', 'Configuration', 'Idle', 'Network', 'Other', 'Queueing', 'Scheduler', 'System I/O', 'User I/O' ) ;

       -- loop through DBs in the test
       FOR r_dbName IN (
          SELECT DISTINCT t.database_name, db.db_type
            FROM TEST_RESULT_WAIT_CLASS t
            JOIN TEST_RESULT_DBS db ON ( db.db_name = t.database_name and db.db_env = t_test1.DB_ENV )
           WHERE t.test_id = t_test1.TEST_ID
             AND t.avg_sessions IS NOT NULL
           GROUP BY t.database_name, db.db_type  
           ORDER BY 1
       )
       LOOP
         -- pipe header
         l_row := 'h3. WAIT CLASS COMPARISONS ';
         l_row := ' ';
         l_row := 'h5. Wait Class Comparison for ' || r_dbName.db_type ;
         PIPE ROW ( l_row ) ;
         l_idx := l_metric_name.FIRST ;
         WHILE l_idx IS NOT NULL
         LOOP
            begin
                with t1 as (SELECT nvl(max(decode(wait_class, l_metric_name(l_idx), avg_sessions)),0) avg_val
                              FROM TEST_RESULT_WAIT_CLASS a
                              JOIN TEST_RESULT_DBS db ON ( db.db_name = a.database_name )
                             where test_id = t_test1.TEST_ID
                               and db.db_type = r_dbName.db_type
                            group by test_id),
                     t2 as (SELECT nvl(max(decode(wait_class, l_metric_name(l_idx), avg_sessions)),0) avg_val
                              FROM hp_diag.TEST_RESULT_WAIT_CLASS a
                              JOIN TEST_RESULT_DBS db ON ( db.db_name = a.database_name )
                             where test_id = t_test2.TEST_ID
                               and db.db_type = r_dbName.db_type
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
                  l_row := '"' || l_metric_name(l_idx) || '" event class was not been seen in comparative run ' || t_test2.TEST_ID || '. Event class has increased from 0 to ' || l_avg_1 || '.';
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

/*--------------------------------------------------------------------------------- */
    -- GET_LOAD_COMPARISON_SUMMARY
    -- Reports on TEST_RESULT_DB_STATS for the overall DB performance on 
    -- DB Time, CPU and Executions per second  
    -- Cross environment ready
/*--------------------------------------------------------------------------------- */          
    FUNCTION Get_load_comparison_summary (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_title   IN varchar2 default 'Y'
          ) RETURN g_tvc2 PIPELINED
    AS
        t_test1   TEST_RESULT_MASTER%ROWTYPE ;
        t_test2   TEST_RESULT_MASTER%ROWTYPE ;

        l_row     varchar2(4000);
        l_pred    number ;       -- Ratio to calculate a RED warning 
        l_pamber  number ;       -- Ratio to calculate an AMBER warning 
        
    BEGIN
        -- Set parameters
        t_test1   := REPORT_GATHER.Get_TEST_Details(i_testId1) ;
        t_test2   := REPORT_GATHER.Get_TEST_Details(i_testId2) ;
        
        -- Set controls
        l_pred    := 10 ;
        l_pamber  := 2 ;   

        if Upper(i_title) = 'Y' then 
            PIPE ROW ( 'h3. DB LOAD COMPARISON' ) ;
        end if;          

        PIPE ROW ( 'h5. DB Overall Load Comparison (SUMMARY)' ) ;
        PIPE ROW ( 'Show how the previous tests compare to each other regarding the overall load put on the individual databases' ) ;

        PIPE ROW ( '||Overall Status||Database||DB Time Per Sec||DB CPU Per Sec||Execs Per Sec||Details||' ) ;
        FOR r1 IN (
          WITH cur AS (
             SELECT a.database_name, a.db_name, a.db_time_per_sec, a.db_cpu_per_sec, a.execs_per_sec
               FROM (
                      select db.db_type as database_name, db.db_name as db_name, db_time_per_sec, db_cpu_per_sec, execs_per_sec 
                        from TEST_RESULT_DB_STATS 
                        join TEST_RESULT_DBS db on (db.db_name = database_name and db.db_env = t_test1.DB_ENV )
                       where test_id = t_test1.TEST_ID
                       order by 1
                    ) a
          ) , prev AS (
             SELECT x.database_name, x.db_name, x.db_time_per_sec, x.db_cpu_per_sec, x.execs_per_sec
               FROM (
                      select db.db_type as database_name, db.db_name as db_name, db_time_per_sec, db_cpu_per_sec, execs_per_sec 
                        from TEST_RESULT_DB_STATS 
                        join TEST_RESULT_DBS db on (db.db_name = database_name and db.db_env = t_test2.DB_ENV)
                       where test_id = t_test2.TEST_ID
                       order by 1
                    ) x
          ) , b AS (
             SELECT cur.database_name
                  , cur.db_name
                  , cur.db_time_per_sec as cur_db_time_per_sec
                  , prev.db_time_per_sec as prev_db_time_per_sec
                  , cur.db_cpu_per_sec as cur_db_cpu_per_sec
                  , prev.db_cpu_per_sec as prev_db_cpu_per_sec
                  , cur.execs_per_sec as cur_execs_per_sec
                  , prev.execs_per_sec as prev_execs_per_sec
                  , CASE WHEN ( cur.db_time_per_sec IS NULL or cur.db_time_per_sec = 0 ) 
                               AND ( prev.db_time_per_sec IS NULL or prev.db_time_per_sec = 0 ) THEN 'Blue'
                         WHEN cur.db_time_per_sec  IS NULL or cur.db_time_per_sec = 0           THEN 'Red'
                         WHEN prev.db_time_per_sec IS NULL or prev.db_time_per_sec = 0          THEN 'Red'
                         WHEN cur.db_time_per_sec  >= l_pred * prev.db_time_per_sec             THEN 'Red'
                         WHEN prev.db_time_per_sec >= l_pred * cur.db_time_per_sec              THEN 'Red'
                         WHEN cur.db_time_per_sec  >= l_pamber *  prev.db_time_per_sec          THEN 'Amber'
                         WHEN prev.db_time_per_sec >= l_pamber *  cur.db_time_per_sec           THEN 'Amber'
                         ELSE 'Green' END AS db_time_per_sec_status
                  , CASE WHEN ( cur.db_cpu_per_sec IS NULL or cur.db_cpu_per_sec = 0 ) 
                               AND ( prev.db_time_per_sec IS NULL or prev.db_time_per_sec = 0 ) THEN 'Blue'
                         WHEN cur.db_cpu_per_sec  IS NULL or cur.db_cpu_per_sec = 0             THEN 'Red'
                         WHEN prev.db_cpu_per_sec IS NULL or prev.db_time_per_sec = 0           THEN 'Red'
                         WHEN cur.db_cpu_per_sec  >= l_pred * prev.db_cpu_per_sec               THEN 'Red'
                         WHEN prev.db_cpu_per_sec >= l_pred * cur.db_cpu_per_sec                THEN 'Red'
                         WHEN cur.db_cpu_per_sec  >= l_pamber *  prev.db_cpu_per_sec            THEN 'Amber'
                         WHEN prev.db_cpu_per_sec >= l_pamber *  cur.db_cpu_per_sec             THEN 'Amber'
                         ELSE 'Green' END AS db_cpu_per_sec_status
                  , CASE WHEN ( cur.execs_per_sec IS NULL or cur.execs_per_sec = 0 ) 
                               AND ( prev.execs_per_sec IS NULL or prev.execs_per_sec = 0 )     THEN 'Blue'
                         WHEN cur.execs_per_sec  IS NULL or cur.execs_per_sec = 0               THEN 'Red'
                         WHEN prev.execs_per_sec IS NULL or prev.execs_per_sec = 0              THEN 'Red'
                         WHEN cur.execs_per_sec  >= l_pred * prev.execs_per_sec                 THEN 'Red'
                         WHEN prev.execs_per_sec >= l_pred * cur.execs_per_sec                  THEN 'Red'
                         WHEN cur.execs_per_sec  >= l_pamber *  prev.execs_per_sec              THEN 'Amber'
                         WHEN prev.execs_per_sec >= l_pamber *  cur.execs_per_sec               THEN 'Amber'
                         ELSE 'Green' END AS execs_per_sec_status
               FROM cur
               LEFT OUTER JOIN prev on ( cur.database_name = prev.database_name )
          ) , c AS (
             SELECT CASE WHEN db_time_per_sec_status = 'Red'    THEN 'Red'
                         WHEN db_cpu_per_sec_status  = 'Red'    THEN 'Red'
                         WHEN execs_per_sec_status   = 'Red'    THEN 'Red'
                         WHEN db_time_per_sec_status = 'Amber'  THEN 'Amber'
                         WHEN execs_per_sec_status   = 'Amber'  THEN 'Amber'
                         WHEN db_cpu_per_sec_status  = 'Amber'  THEN 'Amber'
                         WHEN db_time_per_sec_status = 'Blue'   THEN 'Blue'
                         WHEN execs_per_sec_status   = 'Blue'   THEN 'Blue'
                         WHEN db_cpu_per_sec_status  = 'Blue'   THEN 'Blue'
                         ELSE 'Green' END AS overall_status
                  , LOWER ( b.database_name ) AS database_name
                  , b.db_name
                  , b.cur_db_time_per_sec
                  , b.prev_db_time_per_sec
                  , b.cur_db_cpu_per_sec
                  , b.prev_db_cpu_per_sec
                  , b.cur_execs_per_sec
                  , b.prev_execs_per_sec
                  , b.db_time_per_sec_status
                  , b.db_cpu_per_sec_status
                  , b.execs_per_sec_status
               FROM b
          )
          SELECT '|{status:colour=' || CASE WHEN c.overall_status = 'Amber' THEN 'Yellow' ELSE c.overall_status END || '|title=' || c.overall_status || '}|'
                 || c.database_name  || '|'
                 || '{status:colour=' || CASE WHEN c.db_time_per_sec_status = 'Amber' THEN 'Yellow' ELSE c.db_time_per_sec_status END || '|title=' || c.db_time_per_sec_status || '}|'
                 || '{status:colour=' || CASE WHEN c.db_cpu_per_sec_status = 'Amber' THEN 'Yellow' ELSE c.db_cpu_per_sec_status END || '|title=' || c.db_cpu_per_sec_status || '}|'
                 || '{status:colour=' || CASE WHEN c.execs_per_sec_status = 'Amber' THEN 'Yellow' ELSE c.execs_per_sec_status END || '|title=' || c.execs_per_sec_status
                 || '}|'
                 || CASE WHEN c.overall_status in ('Amber','Red') THEN Get_SQL_Analysis( i_testId1, i_testId2, NULL, NULL, c.db_name, NULL, NULL, c.db_time_per_sec_status, c.db_cpu_per_sec_status, c.execs_per_sec_status, c.cur_db_time_per_sec, c.prev_db_time_per_sec, c.cur_db_cpu_per_sec, c.prev_db_cpu_per_sec, c.cur_execs_per_sec, c.prev_execs_per_sec, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL ) ||'|'
                       ELSE '  |' END AS col1
            FROM c
           ORDER BY c.database_name
        )
        LOOP
          PIPE ROW ( r1.col1 ) ;
        END LOOP ;

    END Get_load_comparison_summary ;


/*--------------------------------------------------------------------------------- */
    -- GET_TOP25_COMPARISON_SUMMARY
    -- Uses : TEST_RESULT_SQL to compare the top 25 SQL statement between both tests
    -- The Description is the description supplied for the comparison itself, not the test descrition
    -- The title parameter indicates whether the H3 level label should be used or not.
    -- The link parameter indicates whether the link should be shown or nor
    -- filters the result by module if asked
    -- Cross environment ready
/*--------------------------------------------------------------------------------- */          
    FUNCTION Get_top25_comparison_summary (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_desc    IN varchar2 default null
          , i_title   IN varchar2 default 'Y'
          , i_link    IN varchar2 default 'Y'
          , i_mode    IN varchar2 default NULL
          ) RETURN g_tvc2 PIPELINED
    AS
        t_test1   TEST_RESULT_MASTER%ROWTYPE ;
        t_test2   TEST_RESULT_MASTER%ROWTYPE ;
        l_desc    varchar2(100) ;
        l_row     varchar2(4000) ;
        l_pred    number ;       -- Ratio to calculate a RED warning 
        l_pamber  number ;       -- Ratio to calculate an AMBER warning 
        l_header  varchar2(200) ;
        l_mode    varchar2(5) ;  -- QUERY search mode : NULL = All, 'APP' - Application related only, 'DB' - Oracle related only

    BEGIN
        -- Set parameters
        t_test1   := REPORT_GATHER.Get_TEST_Details(i_testId1) ;
        t_test2   := REPORT_GATHER.Get_TEST_Details(i_testId2) ;
        l_desc    := i_desc ;
        l_mode    := i_mode;

        -- Set Controls
        l_pred    := 10 ;
        l_pamber  := 2 ;

        if Upper(i_title) = 'Y' then 
            PIPE ROW ( 'h3. SQL COMPARISONS' ) ;
        end if;

        if Upper(i_link) = 'Y' then
            l_header := '||Overall Status||Sql Id||Database||TPS Status||Duration Status||Details||' ;
        else
            l_header := '||Overall Status||Sql Id||Database||TPS Status||Duration Status||' ;
        end if;

        PIPE ROW ( 'h5. Top 25 SQL Comparison by Elapsed Time (SUMMARY)' ) ;
        PIPE ROW ( l_header ) ;

        FOR r1 IN (
          WITH cur AS (
             SELECT ROWNUM AS rnum , a.database_name , a.db_name, a.sql_id AS cur_sql_id,a.test_id, a.tps , a.elapsed_time_per_exec_seconds AS els , a.rows_processed AS rp, a.rows_per_exec AS rpe, a.executions AS exec, a.cpu_time_seconds AS cpu,  a.test_description
               FROM (
                      SELECT db.db_type as database_name , s.database_name as db_name, s.sql_id , s.test_id, s.tps , s.elapsed_time_per_exec_seconds , s.rows_processed, s.rows_per_exec, s.executions, s.cpu_time_seconds, s.test_description
                        FROM TEST_RESULT_SQL s
                        JOIN TEST_RESULT_DBS db ON ( db.db_name = s.database_name and db.db_env = t_test1.DB_ENV )
                       WHERE s.test_id = t_test1.TEST_ID
                         AND ( CASE  WHEN ( l_mode = 'APP' and ( Upper(s.module) not in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','SYSTEM','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER')
                                                       and Upper(s.module) not like 'RMAN%'
                                                       and s.module not like 'Oracle Enterprise Manager%'
                                                      )) THEN 1
                                     WHEN ( l_mode = 'DB'  and ( Upper(s.module) in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','SYSTEM','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER') 
                                                                   or Upper(s.module) like 'RMAN%'
                                                                   or s.module like 'Oracle Enterprise Manager%'   
                                                                  )) THEN 1
                                     WHEN ( l_mode is NULL ) THEN 1
                                     ELSE 0 END ) = 1 
                      ORDER BY s.elapsed_time_seconds DESC
                    ) a
              WHERE ROWNUM <= 25
          ) , prev AS (
             SELECT a.database_name , a.db_name, a.sql_id as prev_sql_id , a.test_id, a.tps , a.elapsed_time_per_exec_seconds AS els , a.rows_processed AS rp, a.rows_per_exec AS rpe, a.executions AS exec, a.cpu_time_seconds AS cpu, a.test_description
               FROM (
                      SELECT db.db_type as database_name ,s.database_name as db_name, s.sql_id , s.test_id, s.tps , s.elapsed_time_per_exec_seconds , s.rows_processed, s.rows_per_exec, s.executions, s.cpu_time_seconds, s.test_description
                        FROM TEST_RESULT_SQL s
                        JOIN TEST_RESULT_DBS db ON ( db.db_name = s.database_name and db.db_env = t_test2.DB_ENV )
                       WHERE s.test_id = t_test2.TEST_ID
                         AND ( CASE  WHEN ( l_mode = 'APP' and ( Upper(s.module) not in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','SYSTEM','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER')
                                                       and Upper(s.module) not like 'RMAN%'
                                                       and s.module not like 'Oracle Enterprise Manager%'
                                                      )) THEN 1
                                     WHEN ( l_mode = 'DB'  and ( Upper(s.module) in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','SYSTEM','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER') 
                                                                   or Upper(s.module) like 'RMAN%'
                                                                   or s.module like 'Oracle Enterprise Manager%'   
                                                                  )) THEN 1
                                     WHEN ( l_mode is NULL ) THEN 1
                                     ELSE 0 END ) = 1 
                      ORDER BY s.elapsed_time_seconds DESC
                    ) a
          ) , b AS (
             SELECT cur.rnum
                  , cur.cur_sql_id
                  , prev.prev_sql_id
                  , cur.database_name
                  , cur.db_name
                  , CASE WHEN cur.tps IS NULL AND prev.tps IS NULL      THEN 'Blue'
                         WHEN cur.tps = 0 AND prev.tps = 0              THEN 'Grenn' 
                         WHEN cur.tps  IS NULL                          THEN 'Green'
                         WHEN prev.tps IS NULL                          THEN 'Green'
                         WHEN cur.tps >= l_pred * prev.tps              THEN 'Red'
                         WHEN prev.tps >= l_pred * cur.tps              THEN 'Red'
                         WHEN cur.tps >= l_pamber * prev.tps            THEN 'Amber'
                         WHEN prev.tps >= l_pamber * cur.tps            THEN 'Amber'
                         ELSE 'Green' END AS tps_status
                  , CASE WHEN cur.els IS NULL AND prev.els IS NULL      THEN 'Blue'
                         WHEN cur.els = 0 AND prev.els = 0              THEN 'Green'
                         WHEN prev.els IS NULL AND cur.els >= 1/1000    THEN 'Red'
                         WHEN prev.els IS NULL AND cur.els >= 1/10/1000 THEN 'Amber'
                         WHEN cur.els  IS NULL                          THEN 'Green'
                         WHEN prev.els IS NULL                          THEN 'Green'
                         WHEN cur.els >= l_pred * prev.els              THEN 'Red'
                         WHEN cur.els >= l_pamber * prev.els            THEN 'Amber'
                         ELSE 'Green' END AS duration_status
                  , cur.test_description
                  , cur.test_id AS cur_test_id
                  , prev.test_id AS prev_test_id
                  , cur.tps AS cur_tps
                  , prev.tps AS prev_tps
                  , cur.els AS cur_els
                  , prev.els AS prev_els
                  , cur.rp AS cur_rp
                  , prev.rp AS prev_rp
                  , cur.rpe AS cur_rpe
                  , prev.rpe AS prev_rpe
                  , cur.exec AS cur_exec
                  , prev.exec AS prev_exec
                  , cur.cpu AS cur_cpu
                  , prev.cpu AS prev_cpu
               FROM cur
               LEFT OUTER JOIN prev on ( cur.database_name = prev.database_name and cur.cur_sql_id = prev.prev_sql_id )
          ) , c AS (
             SELECT CASE WHEN duration_status = 'Red'   THEN 'Red' 
                         WHEN duration_status = 'Amber' THEN 'Amber'
                         WHEN duration_status = 'Green' AND tps_status      = 'Red' THEN 'Amber'
                         WHEN tps_status      = 'Green' AND duration_status = 'Red' THEN 'Amber'
                         WHEN duration_status = 'Blue'  AND tps_status      = 'Red' THEN 'Amber'
                         WHEN tps_status      = 'Blue'  AND duration_status = 'Red' THEN 'Amber'
                         ELSE 'Green' END AS overall_status
                  , b.cur_sql_id
                  , b.prev_sql_id
                  , LOWER ( b.database_name ) AS database_name
                  , b.db_name
                  , b.tps_status
                  , b.duration_status
                  , b.test_description
                  , b.cur_test_id
                  , b.prev_test_id
                  , b.cur_tps
                  , b.prev_tps
                  , b.cur_els
                  , b.prev_els
                  , b.cur_rp
                  , b.prev_rp
                  , b.cur_rpe
                  , b.prev_rpe
                  , b.cur_exec
                  , b.prev_exec
                  , b.cur_cpu
                  , b.prev_cpu
                  , b.rnum
               FROM b
          )
          SELECT '|{status:colour=' || CASE WHEN c.overall_status = 'Amber' THEN 'Yellow' ELSE c.overall_status END || '|title=' || c.overall_status || '}|'
                  || c.cur_sql_id || '|'
                  || c.database_name || '|'
                  || '{status:colour=' || CASE WHEN c.tps_status = 'Amber' THEN 'Yellow' ELSE c.tps_status END || '|title=' || c.tps_status || '}|'
                  || '{status:colour=' || CASE WHEN c.duration_status = 'Amber' THEN 'Yellow' ELSE c.duration_status END || '|title=' || c.duration_status || '}|'
                  || CASE WHEN c.overall_status in ('Amber','Red') THEN Get_SQL_Analysis( i_testId1, i_testId2, c.cur_sql_id, c.prev_sql_id, c.db_name, c.tps_status, c.duration_status, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, c.cur_tps, c.prev_tps, cur_els, prev_els, cur_rp, prev_rp, cur_rpe, prev_rpe, cur_exec, prev_exec, c.cur_cpu, prev_cpu ) ||'|'
                       ELSE '  |' END AS col1                  
            FROM c
           ORDER BY c.rnum
        )
        LOOP
          PIPE ROW ( r1.col1 ) ;
        END LOOP ;

    END Get_top25_comparison_summary ;

/*--------------------------------------------------------------------------------- */
    -- GET_TOP25_COMPARISON_FULL
    -- Reads from TEST_RESULT_SQL and TEST_RESULT_SQLTEXT and compares to Top 25 SQL statements between each given TEST_ID
    -- filters the result by module if asked
    -- Cross environment ready
/*--------------------------------------------------------------------------------- */          
    FUNCTION Get_top25_comparison_full (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_title   IN varchar2 default 'Y'
          , i_mode    IN varchar2 default NULL
          ) RETURN g_tvc2 PIPELINED
    AS
        t_test1 TEST_RESULT_MASTER%ROWTYPE ;
        t_test2 TEST_RESULT_MASTER%ROWTYPE ;

        l_row     varchar2(4000);
        l_pred    number ;       -- Ratio to calculate a RED warning 
        l_pamber  number ;       -- Ratio to calculate an AMBER warning 
        l_mode    varchar2(5) ;  -- QUERY search mode : NULL = All, 'APP' - Application related only, 'DB' - Oracle related only
    BEGIN
        -- 1) Set default parameters
        t_test1 := REPORT_GATHER.Get_TEST_Details(i_testId1) ;
        t_test2 := REPORT_GATHER.Get_TEST_Details(i_testId2) ;
        l_mode    := i_mode;
        l_pred    := 10 ;
        l_pamber  := 2 ;   


        if Upper(i_title) = 'Y' then 
            PIPE ROW ( 'h3. SQL COMPARISONS' ) ;
        end if;        

        PIPE ROW ( 'h5. Top 25 SQL Comparison by Elapsed Time' ) ;
        PIPE ROW ( '{csv:allowExport=true|columnTypes=s,i,i,s,f,f,f,f,s,s|id=Top25Comparison}' ) ;
        l_row :=  '"SQL ID","'|| 
                    'Cur Position","'|| 
                    'Pre Position","'||
                    'Database Name","'|| 
                    'Cur TPS","'|| 
                    'Pre TPS","'|| 
                    'Cur ms Per Exec","'|| 
                    'Pre ms Per Exec","SQL Text","Module"' ;
        PIPE ROW ( l_row ) ;

        for r1 in (
          with prev as (
                   SELECT ROWNUM rnum, a.database_name, a.sql_id, a.tps, a.ms_pe, a.sql_text, a.module
                     FROM (
                               SELECT LOWER ( db.db_type ) AS database_name, s.sql_id, 
                                      TO_CHAR ( ROUND ( s.tps , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS tps,
                                      TO_CHAR ( ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS ms_pe, 
                                      LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text, 
                                      LOWER ( s.module ) AS module
                                 FROM TEST_RESULT_SQL s
                                 JOIN TEST_RESULT_DBS db ON (db.db_name = s.database_name and db.db_env = t_test2.DB_ENV )
                                 LEFT OUTER JOIN TEST_RESULT_SQLTEXT st ON st.sql_id = s.sql_id
                                WHERE test_id = t_test2.TEST_ID
                                  AND ( CASE WHEN ( l_mode = 'APP' and ( Upper(s.module) not in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','SYSTEM','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER')
                                                                       and Upper(s.module) not like 'RMAN%'
                                                                       and s.module not like 'Oracle Enterprise Manager%'
                                                                      )) THEN 1
                                         WHEN ( l_mode = 'DB'  and ( Upper(s.module) in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','SYSTEM','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER') 
                                                                       or Upper(s.module) like 'RMAN%'
                                                                       or s.module like 'Oracle Enterprise Manager%'   
                                                                      )) THEN 1
                                         WHEN ( l_mode is NULL ) THEN 1
                                         ELSE 0 END ) = 1 
                               ORDER BY elapsed_time_seconds DESC
                          ) a
             ) ,  cur as (
                   SELECT ROWNUM rnum, a.database_name, a.sql_id, a.tps, a.ms_pe, a.sql_text, a.module
                     FROM (
                               SELECT LOWER ( db.db_type ) AS database_name, s.sql_id, 
                                      TO_CHAR ( ROUND ( s.tps , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS tps,
                                      TO_CHAR ( ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS ms_pe,
                                      LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text, 
                                      LOWER ( s.module ) AS module
                                 FROM TEST_RESULT_SQL s
                                 JOIN TEST_RESULT_DBS db ON (db.db_name = s.database_name and db.db_env = t_test2.DB_ENV )
                                 LEFT OUTER JOIN TEST_RESULT_SQLTEXT st ON st.sql_id = s.sql_id
                                WHERE test_id = t_test1.TEST_ID
                                  AND ( CASE WHEN ( l_mode = 'APP' and ( Upper(s.module) not in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','SYSTEM','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER')
                                                                       and Upper(s.module) not like 'RMAN%'
                                                                       and s.module not like 'Oracle Enterprise Manager%'
                                                                      )) THEN 1
                                         WHEN ( l_mode = 'DB'  and ( Upper(s.module) in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','SYSTEM','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER') 
                                                                       or Upper(s.module) like 'RMAN%'
                                                                       or s.module like 'Oracle Enterprise Manager%'   
                                                                      )) THEN 1
                                         WHEN ( l_mode is NULL ) THEN 1
                                         ELSE 0 END ) = 1 
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
            from cur
            left outer join prev on ( cur.database_name = prev.database_name and cur.sql_id = prev.sql_id )
          order by cur.rnum
        )
        LOOP
          PIPE ROW ( r1.col1 ) ;
        END LOOP ;
        PIPE ROW ( '{csv}' ) ;

    END Get_top25_comparison_full ;

/*--------------------------------------------------------------------------------- */
    -- GET_TOP25_DETAIL 
    -- Moved from REPORT_DATA ( as it's a comparison )
    -- Updated to filter by mudule type
    -- Cross environment ready
/*--------------------------------------------------------------------------------- */ 
    FUNCTION Get_top25_detail (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_title   IN varchar2 default 'N'
          , i_mode    IN varchar2 default NULL           
          ) RETURN g_tvc2 PIPELINED
    AS
        l_metric_name   g_tvc2 ;
        l_idx           VARCHAR2(100) ;
        t_test1         TEST_RESULT_MASTER%ROWTYPE ;
        t_test2         TEST_RESULT_MASTER%ROWTYPE ;
        l_mode          varchar2(5) ;  -- QUERY search mode : NULL = All, 'APP' - Application related only, 'DB' - Oracle related only
        l_row           varchar2(4000);
        l_start         varchar2(20);
        l_end           varchar2(20);

    BEGIN
        -- default parameters
        t_test1 := REPORT_GATHER.Get_Test_Details(i_testId1) ;
        t_test2 := REPORT_GATHER.Get_Test_Details(i_testId2) ;
        l_mode  := i_mode;

        l_end   := REPORT_ADM.GET_DTM(t_test1.TEST_ID,'END');

        if Upper(i_title) = 'Y' then 
            PIPE ROW ( 'h3. Top 25 SQL DETAIL Comparison' ) ;
        end if;

        l_row := 'h5. Top 25 SQL Detail Comparison' ;
        PIPE ROW ( l_row ) ;

        for r1 in (
             with cur as (
                   SELECT ROWNUM rnum, a.db_type, a.database_name, a.sql_id
                     FROM (
                               SELECT db.db_type 
                                    , s.database_name
                                    , s.sql_id 
                                    , TO_CHAR ( ROUND ( s.tps , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS tps
                                    , TO_CHAR ( ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS ms_pe
                                    , LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text
                                    , LOWER ( NVL(s.module,'no-module') ) AS module
                                 FROM TEST_RESULT_SQL s
                                 JOIN TEST_RESULT_DBS db ON ( db.db_name = s.database_name and db.db_env = t_test1.DB_ENV )
                                 LEFT OUTER JOIN TEST_RESULT_SQLTEXT st ON st.sql_id = s.sql_id
                                WHERE test_id = t_test1.TEST_ID
                                  AND ( CASE  WHEN ( l_mode = 'APP' and ( Upper(s.module) not in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','SYSTEM','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER')
                                                                   and Upper(s.module) not like 'RMAN%'
                                                                   and s.module not like 'Oracle Enterprise Manager%'
                                                                  )) THEN 1
                                              WHEN ( l_mode = 'DB'  and ( Upper(s.module) in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','SYSTEM','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER') 
                                                                            or Upper(s.module) like 'RMAN%'
                                                                            or s.module like 'Oracle Enterprise Manager%'   
                                                                           )) THEN 1
                                              WHEN ( l_mode is NULL ) THEN 1
                                              ELSE 0 END ) = 1   
                               ORDER BY s.elapsed_time_seconds DESC
                          ) a
                    WHERE ROWNUM <= 25
             )
           select sql_id, db_type, database_name
             from cur
           order by cur.rnum
        )
        LOOP
          PIPE ROW ( 'h6. SQL History for ' || r1.sql_id || ' from ' || r1.db_type ) ;
          -- 19-Apr-2023 Andrew Fraser added sql text
          PIPE ROW ( '||Module||Sql Text||' ) ;
          FOR r2t IN (
             SELECT '|' || NVL(st.module,'no-module') || '|'
                    || LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE (
                       st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13) , ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) , '  ' , ' ' )
                     , '  ' , ' ' ) , 1 , 100 ) ) )
                    || '|' AS col1
               FROM TEST_RESULT_SQLTEXT st
              WHERE st.sql_id = r1.sql_id
                AND ROWNUM = 1
          )
          LOOP
             PIPE ROW ( r2t.col1 ) ;
          END LOOP ;
          -- end of changes for 19-Apr-2023 Andrew Fraser added sql text

          PIPE ROW ( '{csv:allowExport=true|columnTypes=s,s,f,f,f,f,f,s|id=' || r1.sql_id || r1.db_type || '}' ) ;
          PIPE ROW ( '"Database","Test Description","TPS","Executions","Avg Elapsed ms","Avg Buffer Gets","Avg CPU ms","Plan Hash Values"' ) ;
          FOR r2 IN (
             SELECT * FROM (
                SELECT ROWNUM rnum
                  , db.db_type 
                  , s.database_name
                  , s.test_description
                  , s.tps
                  , s.executions
                  , s.elapsed_time_per_exec_seconds * 1000 AS avg_elapsed_ms
                  , s.buffer_gets_per_exec
                  , s.cpu_time_per_exec_seconds * 1000 AS avg_cpu_ms
                  , s.plan_hash_values
               FROM TEST_RESULT_SQL s
               JOIN TEST_RESULT_DBS db ON ( db.db_name = s.database_name and db.db_env = t_test1.DB_ENV )
              WHERE s.sql_id = r1.sql_id
                AND s.database_name = r1.database_name
                AND s.begin_time <= to_date(l_end,'DDMONYY-HH24:MI')
                ORDER by s.begin_time desc )
             WHERE ROWNUM < 15   
          )
          LOOP
             l_row := '"' || r2.db_type || '","'
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



/*--------------------------------------------------------------------------------- */
    -- Get_top25_metrics ( unchanged )
/*--------------------------------------------------------------------------------- */          
    FUNCTION Get_top25_metrics (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          ) RETURN g_tvc2 PIPELINED
    AS
       l_metric_name g_tvc2 ;
       l_idx VARCHAR2(100) ;
       l_testId1 TEST_RESULT_MASTER.test_id%TYPE ;
       l_testId2 TEST_RESULT_MASTER.test_id%TYPE ;

       l_row     varchar2(4000);
       l_pred    number ;       -- Ratio to calculate a RED warning 
       l_pamber  number ;       -- Ratio to calculate an AMBER warning    

    BEGIN
       -- 1) Set default parameters
       l_testId1 := i_testId1 ;
       l_testId2 := i_testId2 ;

       l_pred    := 1.5 ;
       l_pamber  := 1.25 ;   

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
                         WHEN cur.aas >= l_pred * prev.aas THEN 'Red'
                         WHEN prev.aas >= l_pred * cur.aas THEN 'Red'
                         WHEN cur.aas >= l_pamber * prev.aas THEN 'Amber'
                         WHEN prev.aas >= l_pamber * cur.aas THEN 'Amber'
                         ELSE 'Green' END AS cAas
                  , CASE WHEN prev.cpuAas IS NULL THEN 'Green'
                         WHEN cur.cpuAas >= l_pred * prev.cpuAas THEN 'Red'
                         WHEN prev.cpuAas >= l_pred * cur.cpuAas THEN 'Red'
                         WHEN cur.cpuAas >= l_pamber * prev.cpuAas THEN 'Amber'
                         WHEN prev.cpuAas >= l_pamber * cur.cpuAas THEN 'Amber'
                         ELSE 'Green' END AS cCpuAas
                  , CASE WHEN prev.hostCpu IS NULL THEN 'Green'
                         WHEN cur.hostCpu >= l_pred * prev.hostCpu THEN 'Red'
                         WHEN prev.hostCpu >= l_pred * cur.hostCpu THEN 'Red'
                         WHEN cur.hostCpu >= l_pamber * prev.hostCpu THEN 'Amber'
                         WHEN prev.hostCpu >= l_pamber * cur.hostCpu THEN 'Amber'
                         ELSE 'Green' END AS cHostCpu
                  , CASE WHEN prev.runQueue IS NULL THEN 'Green'
                         WHEN cur.runQueue >= l_pred * prev.runQueue THEN 'Red'
                         WHEN prev.runQueue >= l_pred * cur.runQueue THEN 'Red'
                         WHEN cur.runQueue >= l_pamber * prev.runQueue THEN 'Amber'
                         WHEN prev.runQueue >= l_pamber * cur.runQueue THEN 'Amber'
                         ELSE 'Green' END AS cRunQueue
                  , CASE WHEN prev.wait_avg_ms IS NULL THEN 'Green'
                         WHEN cur.wait_avg_ms >= l_pred * prev.wait_avg_ms THEN 'Red'
                         WHEN prev.wait_avg_ms >= l_pred * cur.wait_avg_ms THEN 'Red'
                         WHEN cur.wait_avg_ms >= l_pamber * prev.wait_avg_ms THEN 'Amber'
                         WHEN prev.wait_avg_ms >= l_pamber * cur.wait_avg_ms THEN 'Amber'
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

/*--------------------------------------------------------------------------------- */
    -- GET_TOP25_LONG_COMP ( unchanged )
    -- It's a duplication of "Get_top25_comparison_full"
/*--------------------------------------------------------------------------------- */          
    FUNCTION Get_top25_long_comp ( i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
                                 , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          ) RETURN g_tvc2 PIPELINED
    AS
       l_metric_name g_tvc2 ;
       l_idx VARCHAR2(100) ;
       l_testId1 TEST_RESULT_MASTER.test_id%TYPE ;
       l_testId2 TEST_RESULT_MASTER.test_id%TYPE ;

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
       END LOOP ;

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

/*--------------------------------------------------------------------------------- */
    -- Get_top25_long_comp_db ( Unchanged )
/*--------------------------------------------------------------------------------- */          
    FUNCTION Get_top25_long_comp_db (
            i_dbname  IN varchar2
            , i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
            , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          ) RETURN g_tvc2 PIPELINED
    AS
       l_metric_name g_tvc2 ;
       l_idx VARCHAR2(100) ;

       l_testId1 TEST_RESULT_MASTER.test_id%TYPE ;
       l_testId2 TEST_RESULT_MASTER.test_id%TYPE ;

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

/*--------------------------------------------------------------------------------- */
    -- Get_top_n_long_comp_db ( unchanged )
/*--------------------------------------------------------------------------------- */          
    FUNCTION Get_top_n_long_comp_db (
         i_dbname IN VARCHAR2
       , i_limit IN NUMBER DEFAULT 25
       , i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE
       , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
       , i_testId3 IN TEST_RESULT_MASTER.test_id%TYPE 
       , i_testId4 IN TEST_RESULT_MASTER.test_id%TYPE 
    ) RETURN g_tvc2 PIPELINED
    AS
       l_metric_name g_tvc2 ;
       l_idx VARCHAR2(100) ;

       l_testId1 TEST_RESULT_MASTER.test_id%TYPE ;
       l_testId2 TEST_RESULT_MASTER.test_id%TYPE ;
       l_testId3 TEST_RESULT_MASTER.test_id%TYPE ;
       l_testId4 TEST_RESULT_MASTER.test_id%TYPE ;

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

/*--------------------------------------------------------------------------------- */
    -- GET_LOAD_COMPARISON_FULL
    -- Compares data from TEST_RESULT_DB_STATS
    -- Cross environment ready
/*--------------------------------------------------------------------------------- */          
        FUNCTION Get_load_comparison_full (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
           ,i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
           ,i_title   IN varchar2 default 'Y'
          ) RETURN g_tvc2 PIPELINED
    AS
       l_header_row VARCHAR2(4000) ;

       l_testId1 TEST_RESULT_MASTER.test_id%TYPE ;
       l_testId2 TEST_RESULT_MASTER.test_id%TYPE ;
    BEGIN
        -- Set default parameters
        l_testId1 := i_testId1 ;
        l_testId2 := i_testId2 ;

        if Upper(i_title) = 'Y' then
            PIPE ROW ( 'h3. DB LOAD COMPARISON' ) ;
        end if; 

        PIPE ROW ( 'h5. DB Load Comparison - DB Time Per Second' ) ;
        PIPE ROW ( 'The aim of this info is to show how the previous tests compare to each other regarding the overall load put on the individual databases' ) ;
        PIPE ROW ( '' ) ;

        -- Get Header
        PIPE ROW ( '{chart:type=bar | 3D = true | width=750 | height=750 | orientation = horizontal }' ) ;
          WITH q AS (
               SELECT DISTINCT db.db_type
                FROM TEST_RESULT_DB_STATS t
                JOIN TEST_RESULT_DBS db ON ( db.db_name = t.database_name )
               WHERE t.test_id IN ( l_testId1 , l_testId2 ) )
          SELECT '|| TEST NAME || ' || LISTAGG ( db_type  , '||' ) WITHIN GROUP ( ORDER BY db_type ) || '||' 
          INTO l_header_row
            FROM q ;
          PIPE ROW ( l_header_row ) ;

        -- 2.2) Chart detail rows.
        FOR r1 IN (
          SELECT '|' || s.test_id || '|' || LISTAGG ( ROUND ( NVL ( s.db_time_per_sec , 0 ) , 2 ) , '|' )
                 WITHIN GROUP ( ORDER BY db.db_type ) || '|' AS text_output
            FROM TEST_RESULT_DB_STATS s
            JOIN TEST_RESULT_DBS db ON ( db.db_name = s.database_name )
           WHERE s.test_id IN ( l_testId1 , l_testId2 ) 
           GROUP BY s.test_id
           ORDER BY TO_DATE ( SUBSTR ( s.test_id , 1 , 7 ) , 'DDMONYY' ) ASC
        )
        LOOP
          PIPE ROW ( r1.text_output ) ;
        END LOOP ;
        PIPE ROW ( '{chart}' ) ;

        -----------------------------------------------------------------------------------------------------------------------------
        PIPE ROW ( 'h5. Load Comparison - DB CPU Per Second' ) ;
        PIPE ROW ( '{chart:type=bar | 3D = true | width=750 | height=750 | orientation = horizontal }' ) ;
          WITH q AS (
               SELECT DISTINCT db.db_type
                FROM TEST_RESULT_DB_STATS t
                JOIN TEST_RESULT_DBS db ON ( db.db_name = t.database_name )
               WHERE t.test_id IN ( l_testId1 , l_testId2 ) )
          SELECT '|| LIST_TESTS || ' || LISTAGG ( db_type  , '||' ) WITHIN GROUP ( ORDER BY db_type ) || '||' 
        INTO l_header_row
            FROM q ;
          PIPE ROW ( l_header_row ) ;

        -- 2.2) Chart detail rows.
        FOR r1 IN (
          SELECT '|' || s.test_id || '|' || LISTAGG ( ROUND ( NVL ( s.db_cpu_per_sec , 0 ) , 2 ) , '|' )
                 WITHIN GROUP ( ORDER BY db.db_type ) || '|' AS text_output
            FROM TEST_RESULT_DB_STATS s
            JOIN TEST_RESULT_DBS db ON ( db.db_name = s.database_name )
           WHERE s.test_id IN ( l_testId1 , l_testId2 ) 
           GROUP BY s.test_id
           ORDER BY TO_DATE ( SUBSTR ( s.test_id , 1 , 7 ) , 'DDMONYY' ) ASC
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
               SELECT DISTINCT db.db_type
                FROM TEST_RESULT_DB_STATS t
                JOIN TEST_RESULT_DBS db ON ( db.db_name = t.database_name )
               WHERE t.test_id IN ( l_testId1 , l_testId2 ) )
          SELECT '|| LIST_TESTS || ' || LISTAGG ( db_type  , '||' ) WITHIN GROUP ( ORDER BY db_type ) || '||' 
        INTO l_header_row
            FROM q ;
          PIPE ROW ( l_header_row ) ;

        -- 2.2) Chart detail rows.
        FOR r1 IN (
          SELECT '|' || s.test_id || '|' || LISTAGG ( ROUND ( NVL ( s.execs_per_sec , 0 ) , 2 ) , '|' )
                 WITHIN GROUP ( ORDER BY db.db_type ) || '|' AS text_output
            FROM TEST_RESULT_DB_STATS s
            JOIN TEST_RESULT_DBS db ON ( db.db_name = s.database_name )
           WHERE s.test_id IN ( l_testId1 , l_testId2 ) 
           GROUP BY s.test_id
           ORDER BY TO_DATE ( SUBSTR ( s.test_id , 1 , 7 ) , 'DDMONYY' ) ASC
        )
        LOOP
          PIPE ROW ( r1.text_output ) ;
        END LOOP ;

        -- 2.3) Chart footer rows.
        PIPE ROW ( '{chart}' ) ;
    END Get_load_comparison_full ;



/*--------------------------------------------------------------------------------- */
    -- Get_all_db_top25_comparison 
    -- Added filter for the odule types
    -- Increased the databases to be retrieved to 25 ( corsscheck between databases as part of both tests )
    -- Cross environment ready
/*--------------------------------------------------------------------------------- */           
    FUNCTION Get_all_db_top25_comparison (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_title   IN varchar2 default 'Y'
          , i_mode    IN varchar2 default NULL
          , i_top_n   IN number  default NULL
          ) RETURN g_tvc2 PIPELINED
    AS
        t_test1   TEST_RESULT_MASTER%ROWTYPE ;
        t_test2   TEST_RESULT_MASTER%ROWTYPE ;
        l_mode    varchar2(5) ;  -- QUERY search mode : NULL = All, 'APP' - Application related only, 'DB' - Oracle related only

        l_row     varchar2(4000);

        l_test_description varchar2(100);
        l_start_time       varchar2(100); 
        l_end_time         varchar2(100);
        l_top_n number;

    begin
       -- Set default parameters
        t_test1   := REPORT_GATHER.Get_TEST_Details(i_testId1) ;
        t_test2   := REPORT_GATHER.Get_TEST_Details(i_testId2) ;
        l_mode    := i_mode;
        l_top_n   := i_top_n;

        if l_top_n is NULL then
          l_top_n := 25;
        end if;

        FOR r_dbName IN (
            SELECT db.db_type, t.database_name 
              FROM TEST_RESULT_SQL t
              JOIN TEST_RESULT_DBS db ON ( db.db_name = t.database_name and db.db_env = t_test1.DB_ENV )
             WHERE t.test_id = t_test1.TEST_ID
             GROUP BY t.database_name, db.db_type
        ORDER BY 1
        )
        LOOP
            -- pipe header
            --if Upper(i_title) = 'Y' then 
            --    PIPE ROW ( 'h3. DETAIL SQL COMPARISON Per Database' ) ;
            --end if;

            l_row := 'h5. SQL Comparison for ' || r_dbName.db_type ;
            PIPE ROW ( l_row ) ;

            PIPE ROW ( '{csv:allowExport=true|sortIcon=true|columnTypes=s,i,i,s,s,s,s,s,s,f,f,f,f,f,f,f,f,f,f,f,f,s,s|rowStyles=,background:lightblue,background:darkgrey}' ) ;    
            l_row := '"SQL ID",'||
                    '"CURR Position",'||
                    '"PREV Position",'||
                    '"Total Elapsed Diff",'||
                    '"TPS Diff",'||
                    '"ms Per Exec Diff",'||
                    '"Rows Per Exec Diff",'||
                    '"CPU Per Exec Diff",'||
                    '"BG Per Exec Diff",'||
                    '"CURR Total Elapsed",'||
                    '"PREV Total Elapsed",'||
                    '"CURR TPS",'||
                    '"PREV TPS",'||
                    '"CURR ms Per Exec",'||
                    '"PREV ms Per Exec",'||
                    '"CURR Rows Per Exec",'||
                    '"PREV Rows Per Exec",'||
                    '"CURR CPU Per Exec",'||
                    '"PREV CPU Per Exec",'||
                    '"CURR BG Per Exec",'||
                    '"PREV BG Per Exec",'||
                    '"SQL Text",'||
                    '"Module"';
            PIPE ROW ( l_row ) ;

            for r1 in (
                with prev as (
                    SELECT ROWNUM rnum, a.sql_id, a.tps, a.ms_pe, a.rpe, a.cpu_pe, a.bg_pe, a.ela_sec, a.sql_text, a.module, a.database_name  
                      FROM (
                             SELECT  s.sql_id
                                   , s.database_name as database_name
                                   , ROUND ( s.tps , 2 ) AS tps
                                   , ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 )  AS ms_pe
                                   , ROUND ( s.rows_per_exec, 2 )  AS rpe
                                   , ROUND ( s.cpu_time_per_exec_seconds * 1000 , 2 )  AS cpu_pe
                                   , ROUND ( s.buffer_gets_per_exec, 2 )  AS bg_pe
                                   , ROUND ( s.elapsed_time_seconds,2) ela_sec
                                   --, LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text
                                   , LOWER ( REPLACE ( REPLACE ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ,'[' ) , ']' ) )  AS sql_text
                                   , LOWER ( s.module ) AS module
                                FROM TEST_RESULT_SQL s
                                --JOIN TEST_RESULT_DBS db ON ( db.db_name = s.database_name )
                                LEFT OUTER JOIN TEST_RESULT_SQLTEXT st ON st.sql_id = s.sql_id
                               WHERE s.test_id = t_test2.TEST_ID
                                 AND s.database_name = r_dbName.database_name
                                 AND ( CASE  WHEN ( l_mode = 'APP' and ( Upper(s.module) not in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','SYSTEM','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER')
                                                                       and Upper(s.module) not like 'RMAN%'
                                                                       and s.module not like 'Oracle Enterprise Manager%'
                                                                      )) THEN 1
                                             WHEN ( l_mode = 'DB'  and ( Upper(s.module) in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','SYSTEM','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER') 
                                                                           or Upper(s.module) like 'RMAN%'
                                                                           or s.module like 'Oracle Enterprise Manager%'   
                                                                          )) THEN 1
                                             WHEN ( l_mode is NULL ) THEN 1
                                             ELSE 0 END ) = 1                          
                               ORDER BY s.top_sql_number 
                               ) a ),
                cur as ( 
                    SELECT ROWNUM rnum, a.sql_id, a.tps, a.ms_pe, a.rpe, a.cpu_pe, a.bg_pe, a.sql_text, a.ela_sec, a.module, a.database_name    
                        FROM (
                             SELECT  s.sql_id
                                   , s.database_name as database_name
                                   , ROUND ( s.tps , 2 ) AS tps
                                   , ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 )  AS ms_pe
                                   , ROUND ( s.rows_per_exec, 2 )  AS rpe
                                   , ROUND ( s.cpu_time_per_exec_seconds * 1000 , 2 )  AS cpu_pe
                                   , ROUND ( s.buffer_gets_per_exec, 2 )  AS bg_pe
                                   , ROUND ( s.elapsed_time_seconds,2) ela_sec 
                                   -- , LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text
                                   , LOWER ( REPLACE ( REPLACE ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ,'[' ) , ']' ) )  AS sql_text
                                   , LOWER ( s.module ) AS module
                                FROM TEST_RESULT_SQL s
                                --JOIN TEST_RESULT_DBS db ON ( db.db_name = s.database_name )
                                LEFT OUTER JOIN TEST_RESULT_SQLTEXT st ON st.sql_id = s.sql_id
                               WHERE s.test_id = t_test1.TEST_ID
                                 AND s.top_sql_number <= l_top_n
                                 AND s.database_name = r_dbName.database_name
                                 AND ( CASE  WHEN ( l_mode = 'APP' and ( Upper(s.module) not in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','SYSTEM','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER')
                                                                       and Upper(s.module) not like 'RMAN%'
                                                                       and s.module not like 'Oracle Enterprise Manager%'
                                                                      )) THEN 1
                                             WHEN ( l_mode = 'DB'  and ( Upper(s.module) in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','SYSTEM','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER') 
                                                                           or Upper(s.module) like 'RMAN%'
                                                                           or s.module like 'Oracle Enterprise Manager%'   
                                                                          )) THEN 1
                                             WHEN ( l_mode is NULL ) THEN 1
                                             ELSE 0 END ) = 1  
                              ORDER BY s.top_sql_number
                             ) a )
            select '"' || cur.sql_id
                   || '","' || cur.rnum 
                   || '","' || nvl(to_char(prev.rnum), 'N/A') 
                   || '","' || case WHEN prev.ela_sec > 0 THEN ROUND(round((cur.ela_sec-prev.ela_sec)/prev.ela_sec,4)*100,0) || '%' ELSE 'N/A' END 
                   || '","' || case WHEN prev.tps > 0     THEN ROUND(round((cur.tps-prev.tps)/prev.tps,4)*100,0)  || '%'             ELSE 'N/A' END 
                   || '","' || case WHEN prev.ms_pe > 0   THEN ROUND(round((cur.ms_pe-prev.ms_pe)/prev.ms_pe,4)*100,0)  || '%'       ELSE 'N/A' END 
                   || '","' || case WHEN prev.rpe > 0     THEN ROUND(round((cur.rpe-prev.rpe)/prev.rpe,4)*100,0)  || '%'             ELSE 'N/A' END 
                   || '","' || case WHEN prev.cpu_pe > 0  THEN ROUND(round((cur.cpu_pe-prev.cpu_pe)/prev.cpu_pe,4)*100,0)  || '%'    ELSE 'N/A' END 
                   || '","' || case WHEN prev.bg_pe > 0   THEN ROUND(round((cur.bg_pe-prev.bg_pe)/prev.bg_pe,4)*100,0)  || '%'       ELSE 'N/A' END 
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
            from cur
            LEFT OUTER JOIN prev on ( cur.database_name = prev.database_name and cur.sql_id = prev.sql_id )
            order by cur.rnum )
            LOOP
                PIPE ROW ( r1.col1 ) ;
            END LOOP ;
            PIPE ROW ( '{csv}' ) ;
        end loop;
    end Get_all_db_top25_comparison;



/*--------------------------------------------------------------------------------- */
    -- Get_rlw_comparison 
    -- cross environment ready
/*--------------------------------------------------------------------------------- */          
    FUNCTION Get_rlw_comparison (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          ) RETURN g_tvc2 PIPELINED
    AS
        l_metric_name   g_tvc2 ;
        l_idx           VARCHAR2(100) ;
        t_test1         TEST_RESULT_MASTER%ROWTYPE ;
        t_test2         TEST_RESULT_MASTER%ROWTYPE ;
        l_desc1         varchar2(30);
        l_desc2         varchar2(30);
        l_row           varchar2(4000);
    BEGIN
        -- default parameters
        t_test1 := REPORT_GATHER.Get_Test_Details(i_testId1) ;
        t_test2 := REPORT_GATHER.Get_Test_Details(i_testId2) ;
        l_desc1 :=  'Curr' ;
        l_desc2 :=  'Prev' ;

        l_row := 'h5. Top 25 Row Lock Waits Across Database by Total Waited Time' ;
        PIPE ROW ( l_row ) ;

        PIPE ROW ( '{csv:allowExport=true|columnTypes=s,s,s,i,s,f,f,f,f,f,f,f,f,f,f|id=Top25RLWComparison}' ) ;

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
                      select rownum rnum, a.db_type, a.database_name, a.object_owner, a.object_name, a.object_type,
                             a.num_waits, a.min_wait_time, a.max_wait_time, a.avg_wait, a.total_wait_time 
                        from ( SELECT rownum rnum, db.db_type, LOWER ( s.database_name ) AS database_name, 
                                      s.object_owner, s.object_name, s.object_type, 
                                      s.num_waits, s.min_wait_time, s.max_wait_time, 
                                      round(s.avg_wait_time, 2) avg_wait, s.total_wait_time
                                 FROM TEST_RESULT_RLW s
                                 JOIN TEST_RESULT_DBS db ON ( db.db_name = s.database_name and db.db_env = t_test2.DB_ENV )
                                WHERE test_id = t_test2.TEST_ID
                               ORDER BY total_wait_time DESC
                             ) a
                ) , cur as (
                      select rownum rnum, a.db_type, a.database_name, a.object_owner, a.object_name, a.object_type,
                             a.num_waits, a.min_wait_time, a.max_wait_time, a.avg_wait, a.total_wait_time
                        from ( SELECT db.db_type, LOWER ( s.database_name ) AS database_name, 
                                      s.object_owner, s.object_name, s.object_type, 
                                      s.num_waits, s.min_wait_time, s.max_wait_time, 
                                      round(s.avg_wait_time, 2) avg_wait, s.total_wait_time
                                 FROM TEST_RESULT_RLW s
                                 JOIN TEST_RESULT_DBS db ON ( db.db_name = s.database_name and db.db_env = t_test1.DB_ENV )
                                WHERE test_id = t_test1.TEST_ID
                               ORDER BY total_wait_time DESC
                              ) a
                       where rownum <= 25
                    )
             select '"' || cur.object_owner
                        || '","' || cur.object_name
                        || '","' || cur.object_type
                        || '","' || cur.rnum
                        || '","' || cur.db_type
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
               from cur
               left outer join prev on ( cur.db_type = prev.db_type
                                        and cur.object_name   = prev.object_name
                                        and cur.object_type   = prev.object_type )
               order by cur.rnum 
        )
        LOOP
            PIPE ROW ( r1.col1 ) ;
        END LOOP ;
        PIPE ROW ( '{csv}' ) ;

    END Get_rlw_comparison ;


/*--------------------------------------------------------------------------------- */
    -- Get_DB_Activity
    -- Creates the CSV fies for the DB activity but within the database servers.
    -- The problem with this approach is that we have no easy way to transfer the files from
    -- the database server to Windows.
    -- *************************
    -- THIS SOLUTION IS NOT LIVE and WOULD NOT WORK creating the chart in confluence 
    -- because of a problem with the charater separation codes!!!
/*--------------------------------------------------------------------------------- */   
    FUNCTION Get_DB_Activity (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_dbname  IN varchar2 default null
          ) RETURN g_tvc2 PIPELINED
    AS
        l_row       varchar2(8000);
        l_env		varchar2(3);
        l_grp		varchar2(4);
        l_dbname	varchar2(40);
        -- Local vars
        t_test1		TEST_RESULT_MASTER%ROWTYPE;
        t_test2		TEST_RESULT_MASTER%ROWTYPE;
        l_header 	varchar2(4000);
        l_sysdate	varchar2(20);
        l_file1		varchar2(100);
        l_file2		varchar2(100);

        -- Cursors
        cursor c_dbs ( p_env    test_result_master.db_env%TYPE
                      ,p_grp    test_result_master.db_group%TYPE
                      ,p_db     varchar2 default null
        ) is
            select distinct DB_NAME
              from V_TEST_RESULT_DBS 
             where DB_GROUP = DECODE ( p_grp, 'DB' , 'FULL', p_grp )
               and DB_ENV = DECODE ( p_env, 'ALL', DB_ENV , p_env )
               and DB_ENV != 'PROD'
               and DB_NAME = NVL(Upper(p_db),DB_NAME) 
             order by DB_NAME ;    

        cursor c_data ( p_testid varchar2, p_dbname varchar2
        ) is
            select TEST_ID,DATABASE_NAME,SAMPLE_TIME,CPU,BCPU
                   ,SCHED,USERIO,SYSTEMIO,CURR,APPL,COMT,CONFIG
                   ,ADMIN,NETW,QUEUE,CLUST,OTHER
             from  TEST_RESULT_ACTIVITY
             where TEST_ID = p_testid
               and DATABASE_NAME = p_dbname
             order by SAMPLE_TIME ;

        -- File management
        f1          utl_file.file_type;

        -- Exceptions
        no_testid_found     EXCEPTION ;

    BEGIN
        l_dbname := i_dbname ;
        -- Check the TESTIDs provided exist within the database 
        t_test1 := REPORT_GATHER.Get_Test_Details (i_testid1);
        t_test2 := REPORT_GATHER.Get_Test_Details (i_testid2);


        if t_test1.TEST_ID is null then
            raise no_testid_found;
        elsif t_test2.TEST_ID is null then
            raise no_testid_found;
        end if;   

        -- Get Details from TESTID
        l_env := REPORT_ADM.Parse_TestId (t_test1.TEST_ID,'ENV');
        l_grp := REPORT_ADM.Parse_TestId (t_test1.TEST_ID,'GRP');

        PIPE ROW ( l_env );
        PIPE ROW ( l_grp );

        dbms_output.put_line ( 'Env : '||l_env) ;
        dbms_output.put_line ( 'Grp : '||l_grp) ;

        -- Get date into text to set the directory location
        select to_char(sysdate,'YYMMDDHH24MISS') into l_sysdate from dual;    

        -- Inititlise report
        PIPE ROW ( 'h3. Database Activity\n' ) ;
        PIPE ROW ( '||Database Name||Database Activity Graph Comparison||\n');

        -- For each database within the report requested, find the information 	
        for v_dbs in c_dbs (l_env, l_grp, l_dbname)
        loop
            -- Get the filenames 
            l_file1 := 'activity_'||v_dbs.DB_NAME||'_'||t_test1.TEST_ID||'.cvs';
            l_file2 := 'activity_'||v_dbs.DB_NAME||'_'||t_test2.TEST_ID||'.cvs';

            PIPE ROW ( l_file1 );
            PIPE ROW ( l_file2 );


            select '|'||v_dbs.DB_NAME||'|'||
            '{table-chart:type=Stacked Area|column=stime|hide=true|aggregation=cpu'||
            ',scheduler,user_io,system_io,concurrency,application,commit,configuration,administrative,network,cluster'||
            ',other|barColoringType=mono|colors=#65a620,#e4a14b,#3572b0,#8cc3e9,#654982,#cc1010,#e98125,#a3acb2,#d8d23a,#6ada6a,#7b6888,#000000,#f691b2|'||
            'datepattern=HH:mm|formatVersion=3|hidecontrols=true|isFirstTimeEnter=false|tfc-height=400|tfc-width=1200|title='||
            v_dbs.DB_NAME||'_'||t_test1.TEST_ID||'|xtitle=Time|ytitle=Sessions|version=3}' into l_header from dual ;

            PIPE ROW (l_header);
            PIPE ROW ('{csv:url=http://wd015506.bskyb.com:9320/reports/tests/'||l_sysdate||'/'||l_file1||'}');
            PIPE ROW ('{csv}');
            PIPE ROW ('{table-chart} |');

            -- First File creation
            f1 := utl_file.fopen('KEEP_INFO', l_file1, 'W');

            -- Write the header
            l_row := 'sample_time,stime,cpu,scheduler,user_io,system_io,concurrency,application,commit,configuration,administrative,network,queueing,cluster,other';
            utl_file.put_line(f1, l_row);

            for r1 in c_data ( t_test1.TEST_ID, v_dbs.DB_NAME)
            loop
                l_row := to_char(r1.SAMPLE_TIME,'dd/mm/yyyy hh24:mi:ss')  ||','||
                       to_char(r1.SAMPLE_TIME,'hh24:mi')  ||','||
                       to_char((r1.CPU+r1.BCPU),'990.099')  ||','||
                       to_char(r1.SCHED,'990.099')       ||','||
                       to_char(r1.USERIO,'990.099')      ||','||
                       to_char(r1.SYSTEMIO,'990.099')    ||','||
                       to_char(r1.CURR,'990.099')        ||','||
                       to_char(r1.APPL,'990.099')        ||','||
                       to_char(r1.COMT,'990.099')        ||','||
                       to_char(r1.CONFIG,'990.099')      ||','||
                       to_char(r1.ADMIN,'990.099')       ||','||
                       to_char(r1.NETW,'990.099')        ||','||
                       to_char(r1.QUEUE,'990.099')       ||','||
                       to_char(r1.CLUST,'990.099')       ||','||
                       to_char(r1.OTHER,'990.099');
                utl_file.put_line(f1, l_row);
            end loop;     
            utl_file.fclose(f1);


            select '|'||v_dbs.DB_NAME||'|'||
            '{table-chart:type=Stacked Area|column=stime|hide=true|aggregation=cpu'||
            ',scheduler,user_io,system_io,concurrency,application,commit,configuration,administrative,network,cluster'||
            ',other|barColoringType=mono|colors=#65a620,#e4a14b,#3572b0,#8cc3e9,#654982,#cc1010,#e98125,#a3acb2,#d8d23a,#6ada6a,#7b6888,#000000,#f691b2|'||
            'datepattern=HH:mm|formatVersion=3|hidecontrols=true|isFirstTimeEnter=false|tfc-height=400|tfc-width=1200|title='||
            v_dbs.DB_NAME||'_'||t_test2.TEST_ID||'|xtitle=Time|ytitle=Sessions|version=3}' into l_header from dual ;

            PIPE ROW (l_header);
            PIPE ROW ('{csv:url=http://wd015506.bskyb.com:9320/reports/tests/'||l_sysdate||'/'||l_file2||'}');
            PIPE ROW ('{csv}');
            PIPE ROW ('{table-chart} |');

            -- Second File creation
            f1 := utl_file.fopen('KEEP_INFO', l_file2, 'W');

            -- Write the header
            l_row := 'sample_time,stime,cpu,scheduler,user_io,system_io,concurrency,application,commit,configuration,administrative,network,queueing,cluster,other';
            utl_file.put_line(f1, l_row);

            for r1 in c_data ( t_test2.TEST_ID, v_dbs.DB_NAME)
            loop
                l_row := to_char(r1.SAMPLE_TIME,'dd/mm/yyyy hh24:mi:ss')  ||','||
                       to_char(r1.SAMPLE_TIME,'hh24:mi')  ||','||
                       to_char((r1.CPU+r1.BCPU),'990.099')  ||','||
                       to_char(r1.SCHED,'990.099')       ||','||
                       to_char(r1.USERIO,'990.099')      ||','||
                       to_char(r1.SYSTEMIO,'990.099')    ||','||
                       to_char(r1.CURR,'990.099')        ||','||
                       to_char(r1.APPL,'990.099')        ||','||
                       to_char(r1.COMT,'990.099')        ||','||
                       to_char(r1.CONFIG,'990.099')      ||','||
                       to_char(r1.ADMIN,'990.099')       ||','||
                       to_char(r1.NETW,'990.099')        ||','||
                       to_char(r1.QUEUE,'990.099')       ||','||
                       to_char(r1.CLUST,'990.099')       ||','||
                       to_char(r1.OTHER,'990.099');
                utl_file.put_line(f1, l_row);
            end loop;     
            utl_file.fclose(f1);
        end loop;

    EXCEPTION
        WHEN no_testid_found THEN
            logger.write('Test ID not found : '||t_test1.TEST_ID) ;
            dbms_output.put_line('Test ID not found : '||t_test1.TEST_ID);
        WHEN OTHERS THEN
            logger.write('Get_DB_Activity - '||l_dbname|| ' : ' ||SQLCODE||' : '||SUBSTR(SQLERRM, 1, 500)) ;
    END Get_DB_Activity;    



/*--------------------------------------------------------------------------------- */
    -- Get_DB_Activity_Headers
    -- Generates the Confluence container for the CVS files to be presented.
    -- This procedure is not currently used as we have a problem with the separation character 
    -- on the "aggregation" part which Confluence does not recognised when coming from PL/SQL
    -- At the moment, this part of the logic is still resolved from the calling SQL Plus script itself!
/*--------------------------------------------------------------------------------- */   
    FUNCTION Get_DB_Activity_Headers (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_dbname  IN varchar2
          ) RETURN g_tvc2 PIPELINED
    AS
        l_row       varchar2(8000);
        l_env		varchar2(3);
        l_grp		varchar2(4);
        l_dbname	varchar2(40);
        -- Local vars
        t_test1		TEST_RESULT_MASTER%ROWTYPE;
        t_test2		TEST_RESULT_MASTER%ROWTYPE;
        l_header 	varchar2(4000);
        l_sysdate	varchar2(20);
        l_file1		varchar2(100);
        l_file2		varchar2(100);
        l_sep       varchar2(3);

        -- Exceptions
        no_testid_found     EXCEPTION ;

    BEGIN
        -- Check the TESTIDs provided exist within the database 
        t_test1 := REPORT_GATHER.Get_Test_Details (i_testid1);
        t_test2 := REPORT_GATHER.Get_Test_Details (i_testid2);
        l_sep   := 'â€š' ;

        if t_test1.TEST_ID is null then
            raise no_testid_found;
        elsif t_test2.TEST_ID is null then
            raise no_testid_found;
        end if;   

        -- Get Details from TESTID
        l_env := REPORT_ADM.Parse_TestId (t_test1.TEST_ID,'ENV');
        l_grp := REPORT_ADM.Parse_TestId (t_test1.TEST_ID,'GRP');

        -- Get the database name for the appropriate enviroment
        -- The calling script passes always the first database being processed and not the right one when performing a cross environment report
        select db_name into l_dbname
        from   TEST_RESULT_DBS
        where  db_type in ( select db_type from TEST_RESULT_DBS where db_name = i_dbname )
        and    db_env = l_env ;   

        -- Get date into text to set the directory location
        select to_char(sysdate,'YYYYMMDDHH24MISS') into l_sysdate from dual;    

        -- Inititlise report
        PIPE ROW ( 'h5. Database Activity for '||l_dbname ) ;
        PIPE ROW ( '||Database Name||Database Activity Graph Comparison||');

        -- Get the filenames 
        l_file1 := 'activity_'||l_dbname||'_'||t_test1.TEST_ID||'.csv';

        select '|'||l_dbname||'|'||
        '{table-chart:type=Stacked Area|column=stime|hide=true|aggregation=cpu'||l_sep||'scheduler'||l_sep||'user_io'
        ||l_sep||'system_io'||l_sep||'concurrency'||l_sep||'application'||l_sep||'commit'
        ||l_sep||'configuration'||l_sep||'administrative'||l_sep||'network'||l_sep||'queueing'
        ||l_sep||'cluster'||l_sep
        ||'other|barColoringType=mono|colors=#65a620,#e4a14b,#3572b0,#8cc3e9,#654982,#cc1010,#e98125,#a3acb2,#d8d23a,#6ada6a,#7b6888,#000000,#f691b2|'
        ||'datepattern=HH:mm|formatVersion=3|hidecontrols=true|isFirstTimeEnter=false|tfc-height=400|tfc-width=1200|title='
        ||l_dbname||'_'||t_test1.TEST_ID||'|xtitle=Time|ytitle=Sessions|version=3}' into l_header from dual ;

        PIPE ROW (l_header);
        PIPE ROW ('{csv:url=http://wd015506.bskyb.com:9320/reports/tests/'||l_sysdate||'/'||l_file1||'}');
        PIPE ROW ('{csv}');
        PIPE ROW ('{table-chart} |');


        -- Get Details from TESTID
        l_env := REPORT_ADM.Parse_TestId (t_test2.TEST_ID,'ENV');
        l_grp := REPORT_ADM.Parse_TestId (t_test2.TEST_ID,'GRP');

        -- Get the database name for the appropriate enviroment
        -- The calling script passes always the first database being processed and not the right one when performing a cross environment report
        select db_name into l_dbname
        from   TEST_RESULT_DBS
        where  db_type in ( select db_type from TEST_RESULT_DBS where db_name = i_dbname )
        and    db_env = l_env ;   

        -- Get the filenames 
        l_file2 := 'activity_'||l_dbname||'_'||t_test2.TEST_ID||'.csv';

        /*
        select '|'||l_dbname||'|'||
        '{table-chart:type=Stacked Area|column=stime|hide=true|aggregation=cpu'||
        ',scheduler,user_io,system_io,concurrency,application,commit,configuration,administrative,network,cluster'||
        ',other|barColoringType=mono|colors=#65a620,#e4a14b,#3572b0,#8cc3e9,#654982,#cc1010,#e98125,#a3acb2,#d8d23a,#6ada6a,#7b6888,#000000,#f691b2|'||
        'datepattern=HH:mm|formatVersion=3|hidecontrols=true|isFirstTimeEnter=false|tfc-height=400|tfc-width=1200|title='||
        l_dbname||'_'||t_test2.TEST_ID||'|xtitle=Time|ytitle=Sessions|version=3}' into l_header from dual ;
        */

        select '|'||l_dbname||'|'||
        '{table-chart:type=Stacked Area|column=stime|hide=true|aggregation=cpu'||l_sep||'scheduler'||l_sep||'user_io'
        ||l_sep||'system_io'||l_sep||'concurrency'||l_sep||'application'||l_sep||'commit'
        ||l_sep||'configuration'||l_sep||'administrative'||l_sep||'network'||l_sep||'queueing'
        ||l_sep||'cluster'||l_sep
        ||'other|barColoringType=mono|colors=#65a620,#e4a14b,#3572b0,#8cc3e9,#654982,#cc1010,#e98125,#a3acb2,#d8d23a,#6ada6a,#7b6888,#000000,#f691b2|'
        ||'datepattern=HH:mm|formatVersion=3|hidecontrols=true|isFirstTimeEnter=false|tfc-height=400|tfc-width=1200|title='
        ||l_dbname||'_'||t_test2.TEST_ID||'|xtitle=Time|ytitle=Sessions|version=3}' into l_header from dual ;       

        PIPE ROW (l_header);
        PIPE ROW ('{csv:url=http://wd015506.bskyb.com:9320/reports/tests/'||l_sysdate||'/'||l_file2||'}');
        PIPE ROW ('{csv}');
        PIPE ROW ('{table-chart} |');

    EXCEPTION
        WHEN no_testid_found THEN
            logger.write('Test ID not found : '||t_test1.TEST_ID) ;
            dbms_output.put_line('Test ID not found : '||t_test1.TEST_ID);
        WHEN OTHERS THEN
            logger.write('Get_DB_Activity_Headers - '||l_dbname|| ' : ' ||SQLCODE||' : '||SUBSTR(SQLERRM, 1, 500)) ;
    END Get_DB_Activity_Headers;  


/*--------------------------------------------------------------------------------- */
    -- Get_DB_Activity_Body
    -- It creates the CVS files content by reading the information collected under the 
    -- TEST_RESULT_ACTIVITY table for each TEST_ID
    -- This avoids having to access the target databases to retrieve this data.
    -- The CSV files will get dumped into external CSV file by the SQL Plus script to be loaded into Confluence
    -- Cross environment ready
/*--------------------------------------------------------------------------------- */   
    FUNCTION Get_DB_Activity_Body (
            i_testId  IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_dbname  IN varchar2 
          ) RETURN g_tvc2 PIPELINED
    AS
        l_row       varchar2(8000);
        l_env		varchar2(3);
        l_grp		varchar2(4);
        l_dbname	varchar2(40);
        -- Local vars
        t_test		TEST_RESULT_MASTER%ROWTYPE;

        cursor c_data ( p_testid varchar2, p_dbname varchar2 ) 
        is
            select TEST_ID,DATABASE_NAME,SAMPLE_TIME,CPU,BCPU
                   ,SCHED,USERIO,SYSTEMIO,CURR,APPL,COMT,CONFIG
                   ,ADMIN,NETW,QUEUE,CLUST,OTHER
             from  TEST_RESULT_ACTIVITY
             where TEST_ID = p_testid
               and DATABASE_NAME = p_dbname
             order by SAMPLE_TIME ;

        -- Exceptions
        no_testid_found     EXCEPTION ;

    BEGIN
        -- Check the TESTID provided exist within the database 
        t_test := REPORT_GATHER.Get_Test_Details (i_testid);
        if t_test.TEST_ID is null then
            raise no_testid_found;
        end if;   

        -- Get Details from TESTID
        l_env := REPORT_ADM.Parse_TestId (t_test.TEST_ID,'ENV');
        l_grp := REPORT_ADM.Parse_TestId (t_test.TEST_ID,'GRP');

        -- Get the database name for the appropriate enviroment
        -- The calling script passes always the first database being processed and not the right one when performing a cross environment report
        select db_name into l_dbname
        from   TEST_RESULT_DBS
        where  db_type in ( select db_type from TEST_RESULT_DBS where db_name = i_dbname )
        and    db_env = l_env ;   

        -- Write the header
        l_row := 'sample_time,stime,cpu,scheduler,user_io,system_io,concurrency,application,commit,configuration,administrative,network,queueing,cluster,other';
        PIPE ROW ( l_row );

        for r1 in c_data ( t_test.TEST_ID, l_dbname )
        loop
            l_row := to_char(r1.SAMPLE_TIME,'dd/mm/yyyy hh24:mi:ss')  ||','||
                     trim(to_char(r1.SAMPLE_TIME,'hh24:mi'))   ||','||
                     trim(to_char((r1.CPU+r1.BCPU),'990.099')) ||','||
                     trim(to_char(r1.SCHED,'990.099'))         ||','||
                     trim(to_char(r1.USERIO,'990.099'))        ||','||
                     trim(to_char(r1.SYSTEMIO,'990.099'))      ||','||
                     trim(to_char(r1.CURR,'990.099'))          ||','||
                     trim(to_char(r1.APPL,'990.099'))          ||','||
                     trim(to_char(r1.COMT,'990.099'))          ||','||
                     trim(to_char(r1.CONFIG,'990.099'))        ||','||
                     trim(to_char(r1.ADMIN,'990.099'))         ||','||
                     trim(to_char(r1.NETW,'990.099'))          ||','||
                     trim(to_char(r1.QUEUE,'990.099'))         ||','||
                     trim(to_char(r1.CLUST,'990.099'))         ||','||
                     trim(to_char(r1.OTHER,'990.099'));
            PIPE ROW ( l_row );
        end loop;     

    EXCEPTION
        WHEN no_testid_found THEN
            logger.write('Test ID not found : '||t_test.TEST_ID) ;
            dbms_output.put_line('Test ID not found : '||t_test.TEST_ID);
        WHEN OTHERS THEN
            logger.write('Get_DB_Activity_Body - '||l_dbname|| ' : ' ||SQLCODE||' : '||SUBSTR(SQLERRM, 1, 500)) ;
    END Get_DB_Activity_Body;  


/*--------------------------------------------------------------------------------- */
    -- GET_DB_CHARTS 
    -- Returns the charts for the databases that are part of the TEST gathered and build the charts dynamically.
    -- It returns the charts for BOTH TESTIDs provided.
/*--------------------------------------------------------------------------------- */       
    FUNCTION Get_DB_Charts (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE default NULL
          ) RETURN g_tvc2 PIPELINED
    AS
        t_test1         TEST_RESULT_MASTER%ROWTYPE ;
        t_test2         TEST_RESULT_MASTER%ROWTYPE ;
        l_row           varchar2(4000);
        l_env           varchar2(30);
        l_grp           varchar2(4);
        l_tmp           varchar2(10);

        -- Local variables
        l_query         varchar2(4000);
        l_header        varchar2(4000);
        l_select        varchar2(4000);
        l_pivot         varchar2(4000);
        l_title         varchar2(100);
        l_metric        varchar2(100);

        -- Arrays
        type  r_row  is table of varchar2(4000);
        l_return        r_row ;

        cursor c_dbs ( p_env    test_result_master.db_env%TYPE
                      ,p_grp    test_result_master.db_group%TYPE
                      ,p_db     varchar2
           ) is
             select distinct DB_NAME
              from V_TEST_RESULT_DBS 
             where DB_GROUP = DECODE ( p_grp, 'DB' , 'FULL', p_grp )
               and DB_ENV = DECODE ( p_env, 'ALL', DB_ENV , p_env )
               and DB_ENV != 'PRD'
               and DB_NAME = NVL(Upper(p_db),DB_NAME) 
             order by DB_NAME ;         

        FUNCTION lget_sql ( p_testid IN test_result_master.test_id%TYPE, p_metric IN varchar2 ) Return varchar2
        IS
            l_sql       varchar2(4000);
        BEGIN
            l_sql := ' select ''|'' || a.KEY || '||l_select || chr(10) ||
                   '   from ( select * ' || chr(10) ||
                   '            from ( select * ' || chr(10) ||
                   '                     from ( select DATABASE_NAME ' || chr(10) ||
                   '                          , to_char(trunc(BEGIN_TIME,''hh24'')+(ROUND(TO_CHAR(BEGIN_TIME,''mi'')/15)*15)/24/60, ''mm/dd hh24'')||'':00'' KEY ' || chr(10) ||
                   '                          , ROUND(AVERAGE, 4) VALUE ' || chr(10) ||
                   '                              from TEST_RESULT_METRICS_DETAIL' || chr(10) ||
                   '                             where TEST_ID = '''|| p_testid || '''' || chr(10) ||
                   '                               and METRIC_NAME = '''|| p_metric || '''' || chr(10) ||
                   '                           )' || chr(10) ||
                   '                     pivot (AVG(VALUE)  for (DATABASE_NAME)  in ' || chr(10) ||           
                   '                            ( '|| l_pivot || ')' || chr(10) || 
                   '                           ) ' || chr(10) || 
                   '                     order by 1 ' || chr(10) || 
                   '                  ) ' || chr(10) || 
                   '        ) a ' ;
            return l_sql ;
        END lget_sql ;

    BEGIN
        -- default parameters
        t_test1 := REPORT_GATHER.Get_Test_Details(i_testId1) ;
        t_test2 := REPORT_GATHER.Get_Test_Details(i_testId2) ;

        l_header := '||KEY ||' ;
        l_select := '''|''' ;
        l_pivot  := null ;

        -- Proces First ID
        l_env  := REPORT_ADM.Parse_TestId(t_test1.TEST_ID,'ENV') ;
        l_grp  := REPORT_ADM.Parse_TestId(t_test1.TEST_ID,'GRP') ;

        -- Generate lines for SQL Code and Headers from the list of databases involved
        for v_dbs in c_dbs (l_env, l_grp, null)
        loop
            l_header := l_header || v_dbs.DB_NAME || '||' ;
            l_select := l_select || '|| NVL(a.' || v_dbs.DB_NAME || ',0) ||''|''' ; 
            select DECODE(l_pivot,null,'''',',''') into l_tmp from dual ;
            l_pivot := l_pivot || l_tmp || v_dbs.DB_NAME  || ''' as '||v_dbs.DB_NAME ;
        end loop;

        PIPE ROW ( 'h3. DATABASE CHARTS' ) ;

        l_metric := 'Host CPU Utilization (%)';
        l_title  :=  l_metric || ' - ' || t_test1.TEST_ID;
        PIPE ROW ( 'h5. '|| l_title ) ;
        --l_row := '{chart:type=timeSeries|dateFormat=M/d H:m|timePeriod=hour|dataOrientation=vertical|rangeAxisLowerBound=0|domainaxisrotateticklabel=true|title='||l_title||'|height=600|width=900}';
        l_row := '{chart:type=timeSeries|dateFormat=M/d H:m|timePeriod=hour|dataOrientation=vertical|rangeAxisLowerBound=0|domainaxisrotateticklabel=true|title='||l_title||'|height=600|width=900}';
        PIPE ROW ( l_row ) ;
        l_row := l_header ;
        PIPE ROW ( l_row ) ;        
        l_query := lget_sql(t_test1.TEST_ID, l_metric);
        EXECUTE IMMEDIATE l_query BULK COLLECT INTO l_return;
        for i in 1 .. l_return.COUNT
        loop
            l_row := l_return(i);
            PIPE ROW ( l_row ) ; 
        end loop;
        PIPE ROW ('{chart}') ;

         if i_testId2 is not NULL then       
            l_title  :=  l_metric || ' - ' || t_test2.TEST_ID;
            PIPE ROW ( 'h5. '|| l_title ) ;
            l_row := '{chart:type=timeSeries|dateFormat=M/d H:m|timePeriod=hour|dataOrientation=vertical|rangeAxisLowerBound=0|domainaxisrotateticklabel=true|title='||l_title||'|height=600|width=900}';
            PIPE ROW ( l_row ) ;
            l_row := l_header ;
            PIPE ROW ( l_row ) ;        
            l_query := lget_sql(t_test2.TEST_ID, l_metric);
            EXECUTE IMMEDIATE l_query BULK COLLECT INTO l_return;
            for i in 1 .. l_return.COUNT
            loop
                l_row := l_return(i);
                PIPE ROW ( l_row ) ; 
            end loop;
            PIPE ROW ('{chart}') ;
        end if;

        l_metric := 'Average Active Sessions';
        l_title  :=  l_metric || ' - ' || t_test1.TEST_ID;
        PIPE ROW ( 'h5. '|| l_title ) ;
        l_row := '{chart:type=timeSeries|dateFormat=M/d H:m|timePeriod=hour|dataOrientation=vertical|rangeAxisLowerBound=0|domainaxisrotateticklabel=true|title='||l_title||'|height=600|width=900}';
        PIPE ROW ( l_row ) ;
        l_row := l_header ;
        PIPE ROW ( l_row ) ;        
        l_query := lget_sql(t_test1.TEST_ID, l_metric);
        EXECUTE IMMEDIATE l_query BULK COLLECT INTO l_return;
        for i in 1 .. l_return.COUNT
        loop
            l_row := l_return(i);
            PIPE ROW ( l_row ) ; 
        end loop;
        PIPE ROW ('{chart}') ;

        if i_testId2 is not NULL then  
            l_title  :=  l_metric || ' - ' || t_test2.TEST_ID;
            PIPE ROW ( 'h5. '|| l_title ) ;
            l_row := '{chart:type=timeSeries|dateFormat=M/d H:m|timePeriod=hour|dataOrientation=vertical|rangeAxisLowerBound=0|domainaxisrotateticklabel=true|title='||l_title||'|height=600|width=900}';
            PIPE ROW ( l_row ) ;
            l_row := l_header ;
            PIPE ROW ( l_row ) ;        
            l_query := lget_sql(t_test2.TEST_ID, l_metric);
            EXECUTE IMMEDIATE l_query BULK COLLECT INTO l_return;
            for i in 1 .. l_return.COUNT
            loop
                l_row := l_return(i);
                PIPE ROW ( l_row ) ; 
            end loop;
            PIPE ROW ('{chart}') ;
        end if;

        l_metric := 'Total PGA Allocated';
        l_title  :=  l_metric || ' - ' || t_test1.TEST_ID;
        PIPE ROW ( 'h5. '|| l_title ) ;
        l_row := '{chart:type=timeSeries|dateFormat=M/d H:m|timePeriod=hour|dataOrientation=vertical|rangeAxisLowerBound=0|domainaxisrotateticklabel=true|title='||l_title||'|height=600|width=900}';
        PIPE ROW ( l_row ) ;
        l_row := l_header ;
        PIPE ROW ( l_row ) ;        
        l_query := lget_sql(t_test1.TEST_ID, l_metric);
        EXECUTE IMMEDIATE l_query BULK COLLECT INTO l_return;
        for i in 1 .. l_return.COUNT
        loop
            l_row := l_return(i);
            PIPE ROW ( l_row ) ; 
        end loop;
        PIPE ROW ('{chart}') ;

        if i_testId2 is not NULL then 
            l_title  :=  l_metric || ' - ' || t_test2.TEST_ID;
            PIPE ROW ( 'h5. '|| l_title ) ;
            l_row := '{chart:type=timeSeries|dateFormat=M/d H:m|timePeriod=hour|dataOrientation=vertical|rangeAxisLowerBound=0|domainaxisrotateticklabel=true|title='||l_title||'|height=600|width=900}';
            PIPE ROW ( l_row ) ;
            l_row := l_header ;
            PIPE ROW ( l_row ) ;        
            l_query := lget_sql(t_test2.TEST_ID, l_metric);
            EXECUTE IMMEDIATE l_query BULK COLLECT INTO l_return;
            for i in 1 .. l_return.COUNT
            loop
                l_row := l_return(i);
                PIPE ROW ( l_row ) ; 
            end loop;
            PIPE ROW ('{chart}') ;
        end if;

        l_metric := 'Buffer Cache Hit Ratio';
        l_title  :=  l_metric || ' - ' || t_test1.TEST_ID;
        PIPE ROW ( 'h5. '|| l_title ) ;
        l_row := '{chart:type=timeSeries|dateFormat=M/d H:m|timePeriod=hour|dataOrientation=vertical|rangeAxisLowerBound=0|domainaxisrotateticklabel=true|title='||l_title||'|height=600|width=900}';
        PIPE ROW ( l_row ) ;
        l_row := l_header ;
        PIPE ROW ( l_row ) ;        
        l_query := lget_sql(t_test1.TEST_ID, l_metric);
        EXECUTE IMMEDIATE l_query BULK COLLECT INTO l_return;
        for i in 1 .. l_return.COUNT
        loop
            l_row := l_return(i);
            PIPE ROW ( l_row ) ; 
        end loop;
        PIPE ROW ('{chart}') ;

        if i_testId2 is not NULL then 
            l_title  :=  l_metric || ' - ' || t_test2.TEST_ID;
            PIPE ROW ( 'h5. '|| l_title ) ;
            l_row := '{chart:type=timeSeries|dateFormat=M/d H:m|timePeriod=hour|dataOrientation=vertical|rangeAxisLowerBound=0|domainaxisrotateticklabel=true|title='||l_title||'|height=600|width=900}';
            PIPE ROW ( l_row ) ;
            l_row := l_header ;
            PIPE ROW ( l_row ) ;        
            l_query := lget_sql(t_test2.TEST_ID, l_metric);
            EXECUTE IMMEDIATE l_query BULK COLLECT INTO l_return;
            for i in 1 .. l_return.COUNT
            loop
                l_row := l_return(i);
                PIPE ROW ( l_row ) ; 
            end loop;
            PIPE ROW ('{chart}') ;
        end if;    

        l_metric := 'Current OS Load';
        l_title  :=  l_metric || ' - ' || t_test1.TEST_ID;
        PIPE ROW ( 'h5. '|| l_title ) ;
        l_row := '{chart:type=timeSeries|dateFormat=M/d H:m|timePeriod=hour|dataOrientation=vertical|rangeAxisLowerBound=0|domainaxisrotateticklabel=true|title='||l_title||'|height=600|width=900}';
        PIPE ROW ( l_row ) ;
        l_row := l_header ;
        PIPE ROW ( l_row ) ;        
        l_query := lget_sql(t_test1.TEST_ID, l_metric);
        EXECUTE IMMEDIATE l_query BULK COLLECT INTO l_return;
        for i in 1 .. l_return.COUNT
        loop
            l_row := l_return(i);
            PIPE ROW ( l_row ) ; 
        end loop;
        PIPE ROW ('{chart}') ;

        if i_testId2 is not NULL then 
            l_title  :=  l_metric || ' - ' || t_test2.TEST_ID;
            PIPE ROW ( 'h5. '|| l_title ) ;
            l_row := '{chart:type=timeSeries|dateFormat=M/d H:m|timePeriod=hour|dataOrientation=vertical|rangeAxisLowerBound=0|domainaxisrotateticklabel=true|title='||l_title||'|height=600|width=900}';
            PIPE ROW ( l_row ) ;
            l_row := l_header ;
            PIPE ROW ( l_row ) ;        
            l_query := lget_sql(t_test2.TEST_ID, l_metric);
            EXECUTE IMMEDIATE l_query BULK COLLECT INTO l_return;
            for i in 1 .. l_return.COUNT
            loop
                l_row := l_return(i);
                PIPE ROW ( l_row ) ; 
            end loop;
            PIPE ROW ('{chart}') ;
        end if;  

        l_metric := 'Soft Parse Ratio';
        l_title  :=  l_metric || ' - ' || t_test1.TEST_ID;
        PIPE ROW ( 'h5. '|| l_title || '(Hard Parse Ratio (100 - Soft Parse Ratio))' ) ;
        l_row := '{chart:type=timeSeries|dateFormat=M/d H:m|timePeriod=hour|dataOrientation=vertical|rangeAxisLowerBound=0|domainaxisrotateticklabel=true|title='||l_title||'|height=600|width=900}';
        PIPE ROW ( l_row ) ;
        l_row := l_header ;
        PIPE ROW ( l_row ) ;        
        l_query := lget_sql(t_test1.TEST_ID, l_metric);
        EXECUTE IMMEDIATE l_query BULK COLLECT INTO l_return;
        for i in 1 .. l_return.COUNT
        loop
            l_row := l_return(i);
            PIPE ROW ( l_row ) ; 
        end loop;
        PIPE ROW ('{chart}') ;

        if i_testId2 is not NULL then 
            l_title  :=  l_metric || ' - ' || t_test2.TEST_ID;
            PIPE ROW ( 'h5. '|| l_title || '(Hard Parse Ratio (100 - Soft Parse Ratio))' ) ;
            l_row := '{chart:type=timeSeries|dateFormat=M/d H:m|timePeriod=hour|dataOrientation=vertical|rangeAxisLowerBound=0|domainaxisrotateticklabel=true|title='||l_title||'|height=600|width=900}';
            PIPE ROW ( l_row ) ;
            l_row := l_header ;
            PIPE ROW ( l_row ) ;        
            l_query := lget_sql(t_test2.TEST_ID, l_metric);
            EXECUTE IMMEDIATE l_query BULK COLLECT INTO l_return;
            for i in 1 .. l_return.COUNT
            loop
                l_row := l_return(i);
                PIPE ROW ( l_row ) ; 
            end loop;
            PIPE ROW ('{chart}') ;
        end if ;    

    END Get_DB_Charts;


  FUNCTION Get_SQLID_byText (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_title   IN varchar2 default 'Y'
          , i_mode    IN varchar2 default NULL
          , i_top_n   IN number   default NULL
          ) RETURN g_tvc2 PIPELINED
    AS
        t_test1   TEST_RESULT_MASTER%ROWTYPE ;
        t_test2   TEST_RESULT_MASTER%ROWTYPE ;
        l_mode    varchar2(5) ;  -- QUERY search mode : NULL = All, 'APP' - Application related only, 'DB' - Oracle related only

        l_row     varchar2(4000);

        l_test_description varchar2(100);
        l_start_time       varchar2(100); 
        l_end_time         varchar2(100);
        l_top_n            number;
        cnt number;

        TYPE SQL_COMP_REC_TYPE IS RECORD (
          sql_id      VARCHAR2(100),
          curr_pos    VARCHAR2(5),
          prev_pos    VARCHAR2(5) );
        TYPE SQL_MATCH_REC_TYPE IS RECORD (
        col VARCHAR2(4000));

        -- Collection type for SQL_COMP_REC_TYPE and match_rec_type
        TYPE SQL_COMP_TABTYPE   IS TABLE OF SQL_COMP_REC_TYPE;
        TYPE MATCH_TAB_TYPE     IS TABLE OF SQL_MATCH_REC_TYPE;
        v_results       SQL_COMP_TABTYPE; 
        v_sql_match     MATCH_TAB_TYPE;
        v_sql           VARCHAR2(4000);
        v_sql_prev      VARCHAR2(4000);
        v_sql_curr      VARCHAR2(4000);
        v_header_piped  BOOLEAN := FALSE; -- Flag to track header output for SQLs to be checked for a match

    begin
       -- Set default parameters
        t_test1   := REPORT_GATHER.Get_TEST_Details(i_testId1) ;
        t_test2   := REPORT_GATHER.Get_TEST_Details(i_testId2) ;
        l_mode    := i_mode;
        l_top_n   := i_top_n;


        if l_top_n is NULL then
          l_top_n := 25;
        end if;
        --DBMS_OUTPUT.PUT_LINE('Testing stuff');
        --DBMS_OUTPUT.PUT_LINE('Testing content of t_test1: '||t_test1.TEST_ID);
        --DBMS_OUTPUT.PUT_LINE('Testing content of t_test2: '||t_test2.TEST_ID);
        --DBMS_OUTPUT.PUT_LINE('Testing content of l_mode: '||l_mode);

        FOR r_dbName IN (
            SELECT db.db_type, t.database_name 
              FROM TEST_RESULT_SQL t
              JOIN TEST_RESULT_DBS db ON ( db.db_name = t.database_name and db.db_env = t_test1.DB_ENV )
             WHERE t.test_id = t_test1.TEST_ID
             GROUP BY t.database_name, db.db_type
        ORDER BY 1
        )
        LOOP
            v_header_piped := FALSE; -- Set bool flag back to FALSE for each database. This is used for piping the SQL ID Match Table headers

            --i broke up the SQL string for the query below into three variables : v_sql_prev,v_sql_curr and v_sql
            --reason is because the string was larger than varchar2(4000) and i could not be bothered with CLOB
            v_sql_prev := 'WITH prev AS (
                SELECT ROWNUM rnum, a.sql_id, a.tps, a.ms_pe, a.rpe, a.cpu_pe, a.bg_pe, a.ela_sec, a.sql_text, a.module, a.database_name  
                FROM (
                       SELECT s.sql_id,
                              db.db_type AS database_name,
                              ROUND(s.tps, 2) AS tps,
                              ROUND(s.elapsed_time_per_exec_seconds * 1000, 2) AS ms_pe,
                              ROUND(s.rows_per_exec, 2) AS rpe,
                              ROUND(s.cpu_time_per_exec_seconds * 1000, 2) AS cpu_pe,
                              ROUND(s.buffer_gets_per_exec, 2) AS bg_pe,
                              ROUND(s.elapsed_time_seconds, 2) AS ela_sec,
                              LOWER(REPLACE(REPLACE(TO_CHAR(SUBSTR(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(st.sql_text, ''"''), CHR(9), '' ''), CHR(10), '' ''), CHR(13), '' ''), ''   '', '' ''), ''   '', '' ''), ''  '', '' ''), ''  '', '' ''), 1, 100)), ''[''), '']'')) AS sql_text,
                              LOWER(s.module) AS module
                         FROM TEST_RESULT_SQL s
                         JOIN TEST_RESULT_DBS db ON (db.db_name = s.database_name)
                         LEFT OUTER JOIN TEST_RESULT_SQLTEXT st ON st.sql_id = s.sql_id
                        WHERE s.test_id = :bind_t_test2
                          AND db.db_type = :bind_db_type_1
                          AND (CASE 
                                WHEN (:bind_l_mode_1 = ''APP'' 
                                      AND (UPPER(s.module) NOT IN (''SQL DEVELOPER'', ''DBMS_SCHEDULER'', ''SYS'', ''SYSTEM'', ''HP_DIAG'', ''EMAGENT_SQL_ORACLE_DATABASE'', ''SKYUTILS'', ''MMON_SLAVE'', ''BACKUP ARCHIVELOG'', ''HORUS_MONITORING'', ''DBSNMP'', ''DBMON_AGENT_USER'') 
                                           AND UPPER(s.module) NOT LIKE ''RMAN%'' 
                                           AND s.module NOT LIKE ''Oracle Enterprise Manager%'')) THEN 1
                                WHEN (:bind_l_mode_2 = ''DB'' 
                                      AND (UPPER(s.module) IN (''SQL DEVELOPER'', ''DBMS_SCHEDULER'', ''SYS'', ''SYSTEM'', ''HP_DIAG'', ''EMAGENT_SQL_ORACLE_DATABASE'', ''SKYUTILS'', ''MMON_SLAVE'', ''BACKUP ARCHIVELOG'', ''HORUS_MONITORING'', ''DBSNMP'', ''DBMON_AGENT_USER'') 
                                           OR UPPER(s.module) LIKE ''RMAN%'' 
                                           OR s.module LIKE ''Oracle Enterprise Manager%'')) THEN 1
                                WHEN (:bind_l_mode_3 IS NULL) THEN 1
                                ELSE 0 END) = 1
                       ORDER BY s.top_sql_number
                     ) a),';
                v_sql_curr := 'cur AS (
                    SELECT ROWNUM rnum, a.sql_id, a.tps, a.ms_pe, a.rpe, a.cpu_pe, a.bg_pe, a.sql_text, a.ela_sec, a.module, a.database_name    
                    FROM (
                            SELECT s.sql_id,
                                db.db_type AS database_name,
                                ROUND(s.tps, 2) AS tps,
                                ROUND(s.elapsed_time_per_exec_seconds * 1000, 2) AS ms_pe,
                                ROUND(s.rows_per_exec, 2) AS rpe,
                                ROUND(s.cpu_time_per_exec_seconds * 1000, 2) AS cpu_pe,
                                ROUND(s.buffer_gets_per_exec, 2) AS bg_pe,
                                ROUND(s.elapsed_time_seconds, 2) AS ela_sec,
                                LOWER(REPLACE(REPLACE(TO_CHAR(SUBSTR(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(st.sql_text, ''"''), CHR(9), '' ''), CHR(10), '' ''), CHR(13), '' ''), ''   '', '' ''), ''   '', '' ''), ''  '', '' ''), ''  '', '' ''), 1, 100)), ''[''), '']'')) AS sql_text,
                                LOWER(s.module) AS module
                            FROM TEST_RESULT_SQL s
                            JOIN TEST_RESULT_DBS db ON (db.db_name = s.database_name)
                            LEFT OUTER JOIN TEST_RESULT_SQLTEXT st ON st.sql_id = s.sql_id
                            WHERE s.test_id = :bind_t_test1
                            AND (CASE 
                                    WHEN (:bind_l_mode_4 = ''APP'' 
                                        AND (UPPER(s.module) NOT IN (''SQL DEVELOPER'', ''DBMS_SCHEDULER'', ''SYS'', ''SYSTEM'', ''HP_DIAG'', ''EMAGENT_SQL_ORACLE_DATABASE'', ''SKYUTILS'', ''MMON_SLAVE'', ''BACKUP ARCHIVELOG'', ''HORUS_MONITORING'', ''DBSNMP'', ''DBMON_AGENT_USER'') 
                                                AND UPPER(s.module) NOT LIKE ''RMAN%'' 
                                                AND s.module NOT LIKE ''Oracle Enterprise Manager%'')) THEN 1
                                    WHEN (:bind_l_mode_5 = ''DB'' 
                                        AND (UPPER(s.module) IN (''SQL DEVELOPER'', ''DBMS_SCHEDULER'', ''SYS'', ''SYSTEM'', ''HP_DIAG'', ''EMAGENT_SQL_ORACLE_DATABASE'', ''SKYUTILS'', ''MMON_SLAVE'', ''BACKUP ARCHIVELOG'', ''HORUS_MONITORING'', ''DBSNMP'', ''DBMON_AGENT_USER'') 
                                                OR UPPER(s.module) LIKE ''RMAN%'' 
                                                OR s.module LIKE ''Oracle Enterprise Manager%'')) THEN 1
                                    WHEN (:bind_l_mode_6 IS NULL) THEN 1
                                    ELSE 0 END) = 1
                            AND s.top_sql_number <= :bind_l_top_n
                            AND s.database_name = :bind_db_name
                            ORDER BY s.top_sql_number
                        ) a)';
                v_sql := 'SELECT cur.sql_id,
                    cur.rnum AS curr_pos,
                    NVL(TO_CHAR(prev.rnum), ''N/A'') AS prev_pos
                FROM cur
                LEFT OUTER JOIN prev ON (cur.database_name = prev.database_name AND cur.sql_id = prev.sql_id)
                WHERE NVL(TO_CHAR(prev.rnum), ''N/A'') = ''N/A''
                ORDER BY cur.rnum';

            --well because dynamic sqls cannot access vars defined outside of its scope we turn to binds which looks very messy honestly, but it works.
            --the order of the binds is very important, otherwise things get messed up. It has to be in the order they appear in the SQL string.
            EXECUTE IMMEDIATE v_sql_prev||' '||v_sql_curr||' '||v_sql BULK COLLECT INTO v_results USING t_test2.TEST_ID, r_dbName.db_type, l_mode, l_mode, l_mode, t_test1.TEST_ID, l_mode, l_mode, l_mode, l_top_n, r_dbName.database_name; 

            IF v_results.COUNT  > 0 THEN -- building SQL matching tables for SQLs with no comparisons with prev testid
              FOR i IN v_results.FIRST .. v_results.LAST LOOP
                --i could use a cursor for this query and just check if cursor fetched a record or not
                --but v_results(i).sql_id will not have been determined and populated at the declaration stage
                --awful having to run the query twice essentially.
                SELECT COUNT(*) INTO cnt
                from (
                        select a1.sql_id sql_a,
                        b2.sql_id sql_b,
                        UTL_MATCH.edit_distance_similarity(
                            DBMS_LOB.substr(a1.sql_text, 4000, 1),
                            DBMS_LOB.substr(b2.sql_text, 4000, 1)
                        ) as similarity_score
                        from TEST_RESULT_SQLTEXT a1, TEST_RESULT_SQLTEXT b2
                        where a1.sql_id = v_results(i).sql_id
                        and b2.sql_id in (select distinct sql_id from TEST_RESULT_SQL where test_id=t_test2.TEST_ID and database_name= r_dbName.database_name)
                        and UTL_MATCH.edit_distance_similarity (DBMS_LOB.substr(a1.sql_text,4000, 1), DBMS_LOB.substr(b2.sql_text, 4000, 1)) between 90 and 99
                      )
                where rownum <=5;

                  IF cnt > 0 then --if we have any match then process SQL_ID
                    -- Output the header only once per database
                    --The header is only piped if v_header_piped = FALSE which is the default value and is also reset back to FALSE for each database iteration
                    --After piping the header, v_header_piped is set to TRUE to prevent repetition.
                    IF NOT v_header_piped THEN
                      PIPE ROW ('h5. SQL Matches for ' || r_dbName.db_type);
                      PIPE ROW ( '{csv:allowExport=true|sortIcon=false|columnTypes=s,s,f|rowStyles=,background:lightblue,background:darkgrey}' ) ;
                      l_row := '"SQL ID",'||
                                '"SQL ID MATCHED",'||
                                '"Score"';
                      PIPE ROW ( l_row ) ;
                      v_header_piped := TRUE; -- Set flag to avoid repeating the header for each SQL ID per database
                    END IF;

                    --we only want to present the top 5 similarity score between between 90% and 99%
                    for matched in (
                        select '"' ||sql_a
                                  || '","' ||sql_b
                                  || '","' ||similarity_score
                                  || '"' as col
                                  from(
                                    select a1.sql_id sql_a,
                                    b2.sql_id sql_b,
                                    UTL_MATCH.edit_distance_similarity(
                                        DBMS_LOB.substr(a1.sql_text, 4000, 1),
                                        DBMS_LOB.substr(b2.sql_text, 4000, 1)
                                    ) as similarity_score
                                    from TEST_RESULT_SQLTEXT a1, TEST_RESULT_SQLTEXT b2
                                    where a1.sql_id = v_results(i).sql_id
                                    and b2.sql_id in (select distinct sql_id from TEST_RESULT_SQL where test_id=t_test2.TEST_ID and database_name= r_dbName.database_name)
                                    and UTL_MATCH.edit_distance_similarity (DBMS_LOB.substr(a1.sql_text,4000, 1), DBMS_LOB.substr(b2.sql_text, 4000, 1)) between 90 and 99
                                    order by similarity_score desc)
                                    where rownum <=5)
                    loop 
                      PIPE ROW ( matched.col ) ;                                         
                    end loop;
                  END IF;
              END LOOP;
              if v_header_piped = TRUE then
                PIPE ROW ( '{csv}' ) ;
              end if;
            END IF;
        end loop;
    end Get_SQLID_byText;

    FUNCTION Get_SQL_Analysis (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE
          , i_c_sql_id IN TEST_RESULT_SQL.sql_id%TYPE
          , i_p_sql_id IN TEST_RESULT_SQL.sql_id%TYPE
          , i_database_name IN TEST_RESULT_SQL.database_name%TYPE
          , i_tps_status IN varchar2 default NULL
          , i_duration_status IN varchar2 default NULL
          , i_db_time_per_sec_status IN varchar2 default NULL
          , i_db_cpu_per_sec_status IN varchar2 default NULL
          , i_execs_per_sec_status IN varchar2 default NULL 
          , i_cur_db_time_per_sec IN number default NULL
          , i_prev_db_time_per_sec IN number default NULL
          , i_cur_db_cpu_per_sec IN number default NULL
          , i_prev_db_cpu_per_sec IN number default NULL
          , i_cur_db_execs_per_sec IN number default NULL
          , i_prev_db_execs_per_sec IN number default NULL
          , i_cur_tps IN number default NULL
          , i_prev_tps IN number default NULL
          , i_cur_els IN number default NULL
          , i_prev_els IN number default NULL
          , i_cur_rows_processed IN number default NULL
          , i_prev_rows_processed IN number default NULL
          , i_cur_rows_per_execution IN number default NULL
          , i_prev_rows_per_execution IN number default NULL
          , i_cur_executions IN number default NULL
          , i_prev_executions IN number default NULL
          , i_cur_cpu_time_seconds IN number default NULL
          , i_prev_cpu_time_seconds IN number default NULL
    ) RETURN VARCHAR2
      AS
        t_test1         TEST_RESULT_MASTER%ROWTYPE ;
        t_test2         TEST_RESULT_MASTER%ROWTYPE ;
        l_dbname        TEST_RESULT_SQL.database_name%TYPE;
        l_c_sql_id        TEST_RESULT_SQL.SQL_ID%TYPE;
        l_p_sql_id        TEST_RESULT_SQL.SQL_ID%TYPE;
        no_testid_found     EXCEPTION ;
        sql_idn TEST_RESULT_SQL.sql_id%TYPE;
        phv_count_test1 number;
        phv_count_test2 number;
        phv_test1  TEST_RESULT_SQL.plan_hash_values%TYPE;
        phv_test2  TEST_RESULT_SQL.plan_hash_values%TYPE;
        TYPE MessageMap IS TABLE OF VARCHAR2(200) INDEX BY VARCHAR2(10); --declare associative arrary type used for simulating a key-pair value behaviour
        phv_messages MessageMap;
        TYPE ZScoreMap IS TABLE OF VARCHAR2(100) INDEX BY VARCHAR2(10); --declare associative arrary type used for simulating a key-pair value behaviour
        dev_els_messages ZScoreMap;
        dev_execs_messages ZScoreMap;
        dev_tps_messages ZScoreMap;
        TYPE SimScoreMap IS TABLE OF VARCHAR2(100) INDEX BY VARCHAR2(10); --declare associative arrary type used for simulating a key-pair value behaviour
        sim_score_messages SimScoreMap;
        test_id_chk1 number;
        test_id_chk2 number;
        l_start         varchar2(20);
        l_end           varchar2(20);
        deviation_els NUMBER;
        deviation_tps NUMBER ;        
        deviation_execs NUMBER ;
        threshold NUMBER := 2; -- Z-score threshold
        avg_els number ;
        std_els number ;
        avg_tps number ;
        std_tps number ;
        avg_execs number ;
        std_execs number ;
        phv_flag varchar2(2);
        dev_execs_flag number;
        dev_els_flag number;
        dev_tps_flag number;
        sql_similarity_flag number;
        stddev_message varchar2(4000);
        stddev_els_message varchar2(4000);
        stddev_tps_message varchar2(4000);
        stddev_execs_message varchar2(4000);
        phv_message varchar2(4000);
        sql_similarity_message varchar2(4000);
        testId2 TEST_RESULT_MASTER.test_id%TYPE ;
        sim_sql_id TEST_RESULT_SQL.sql_id%TYPE;
        score number;
        append_message varchar2(100);
        rows_per_exec_ratio number ;
        total_exec_ratio number ;
        cpu_time_ratio number ;
        els_ratio number ;
        tps_ratio number ;
        rows_per_exec_ratio_message varchar2(200) ;
        total_exec_ratio_message varchar2(200) ;
        els_ratio_message varchar2(200) ;
        tps_ratio_message varchar2(200) ;
        els_ratio_perc number;
        tps_ratio_perc number;
        total_exec_ratio_perc number ;
        rows_per_exec_ratio_perc number ;
        db_time_per_sec_ratio number ;
        db_time_per_sec_ratio_perc number ;
        db_time_per_sec_ratio_message varchar2(200) ;
        db_cpu_per_sec_ratio number ;
        db_cpu_per_sec_ratio_perc number ;
        db_cpu_per_sec_ratio_message varchar2(200) ;
        db_execs_per_sec_ratio number ;
        db_execs_per_sec_ratio_perc number ;
        db_execs_per_sec_ratio_message varchar2(200) ;
        db_load_message varchar2(4000) ;
        idle_db_load_message varchar2(4000) ;
        best_plan_message varchar2(4000);

      BEGIN
        IF i_db_time_per_sec_status is NULL AND  i_db_cpu_per_sec_status is NULL AND i_execs_per_sec_status is NULL THEN --we use this to split processing for db_load_summary and  topsql_summary
          l_dbname := upper(i_database_name) ;
          l_c_sql_id := i_c_sql_id ;
          l_p_sql_id := i_p_sql_id ;

          -- Initialize the key-value pairs for the mapping of PHV messages
          phv_messages('0') := 'No SQL plan flips detected.';
          phv_messages('1') := 'SQL Plan flip detected.';
          phv_messages('2') := 'Multiple SQL plans detected in either current test or baseline and a SQL plan mismatch between the tests detected too.';
          phv_messages('3') := 'No SQL ID in the baseline to compare against.';

          -- Initialize the key-value pairs for the z-score messages
          dev_els_messages('0') := ' But when compared to recent tests, SQL execution time was found within acceptable range.';
          dev_els_messages('1') := ' Significant SQL execution time deviation detected.';
          dev_els_messages('2') := ' SQL execution time very closely matches the average of recent tests.';
          dev_execs_messages('0') := 'When compared to recent tests, SQL execution frequency is within acceptable range.';
          dev_execs_messages('1') := '  SQL execution frequency deviation detected.';
          dev_execs_messages('2') := ' SQL execution frequency very closely matches the average of previous runs.';
          dev_tps_messages('0') := ' SQL Transaction load is within acceptable range.';
          dev_tps_messages('1') := ' Significant SQL Transaction load deviation detected.';
          dev_tps_messages('2') := ' SQL Transaction load very closely matches the average of previous runs.';

          -- Initialize the key-value pairs for the SQL text similarity messages
          sim_score_messages('0') := 'SQL ID has no close matching SQL ID in the previous test.';
          sim_score_messages('1') := 'SQL ID has a close match in the previous test. Closest matching SQL ID was '; 

          -- Check the TESTIDs provided exist within the database 
          t_test1 := REPORT_GATHER.Get_Test_Details (trim(i_testid1));
          t_test2 := REPORT_GATHER.Get_Test_Details (trim(i_testid2));

          if t_test1.TEST_ID is null then
            raise no_testid_found;
          elsif t_test2.TEST_ID is null then
              raise no_testid_found;
          end if;

          l_end := REPORT_ADM.GET_DTM(i_testId1,'END');

          IF l_p_sql_id is not null  THEN --check for plan flips only when there is a sql_id in baseline to compare against

            select count(PLAN_HASH_VALUES) into phv_count_test1 from TEST_RESULT_SQL where 
            test_id=i_testid1 and DATABASE_NAME=l_dbname
            and sql_id= l_c_sql_id ;

            select count(PLAN_HASH_VALUES) into phv_count_test2 from TEST_RESULT_SQL where 
            test_id=i_testid2 and DATABASE_NAME=l_dbname
            and sql_id= l_c_sql_id ;

            IF phv_count_test1 = 1 and phv_count_test2=1 then
              select PLAN_HASH_VALUES into phv_test1 from TEST_RESULT_SQL  where 
              test_id=i_testid1 and DATABASE_NAME=l_dbname
              and sql_id= l_c_sql_id ;

              select PLAN_HASH_VALUES into phv_test2 from TEST_RESULT_SQL  where 
              test_id=i_testid2 and DATABASE_NAME=l_dbname
              and sql_id= l_c_sql_id ;

              IF phv_test1 = phv_test2 then
                  phv_flag := '0' ; --no plan flip
                  best_plan_message := '' ;
              ELSIF phv_test1 != phv_test2 then
                  phv_flag := '1' ; --plan flip detected
                  best_plan_message := GET_BEST_PLAN(i_testId1, l_dbname, i_c_sql_id,  l_end);
              END IF;
              phv_message := phv_messages(phv_flag)|| best_plan_message ;
            ELSIF phv_count_test1 > 1 OR phv_count_test2 > 1 THEN --multiple plans detected
                DECLARE
                    unmatched_phv NUMBER := 0;
                BEGIN
                    FOR rec IN (
                        SELECT DISTINCT PLAN_HASH_VALUES AS phv_test1
                        FROM TEST_RESULT_SQL
                        WHERE test_id = i_testId1 AND DATABASE_NAME = l_dbname AND sql_id = l_c_sql_id
                        MINUS
                        SELECT DISTINCT PLAN_HASH_VALUES AS phv_test2
                        FROM TEST_RESULT_SQL
                        WHERE test_id = i_testId2 AND DATABASE_NAME = l_dbname AND sql_id = l_c_sql_id
                    ) LOOP
                        unmatched_phv := 1; --unmatched plan detected
                        EXIT;
                    END LOOP;  --in a situation where current test has multiple PHVs say 3 and the previous test has 2 PHVs.
                               --i am using this MINUS operator to confirm if there is any mismatch in PHV between both tests. So every PHV in one test_id must be found in the other test_id
                               --if that is not the case then there is a plan flip.

                    FOR rec IN (
                        SELECT DISTINCT PLAN_HASH_VALUES AS phv_test2
                        FROM TEST_RESULT_SQL
                        WHERE test_id = i_testId2 AND DATABASE_NAME = l_dbname AND sql_id = l_c_sql_id
                        MINUS
                        SELECT DISTINCT PLAN_HASH_VALUES AS phv_test1
                        FROM TEST_RESULT_SQL
                        WHERE test_id = i_testId1 AND DATABASE_NAME = l_dbname AND sql_id = l_c_sql_id
                    ) LOOP
                        unmatched_phv := 1; --unmatched plan detected
                        EXIT;
                    END LOOP;

                    IF unmatched_phv = 1 THEN
                        phv_flag := '2'; -- Multiple PHV used and a mismatch detected
                        best_plan_message := GET_BEST_PLAN(i_testId1, l_dbname, i_c_sql_id,  l_end);
                    ELSIF unmatched_phv = 0 THEN
                      phv_flag := '0' ; --no plan flips
                      best_plan_message := '' ;
                    END IF;
                    phv_message := phv_messages(phv_flag) || best_plan_message ;
                END;
            ELSIF phv_count_test1 = 0 OR phv_count_test2 = 0 THEN
              DECLARE
                    unmatched_phv NUMBER := 0;
                BEGIN
                    FOR rec IN (
                        SELECT DISTINCT PLAN_HASH_VALUES AS phv_test1
                        FROM TEST_RESULT_SQL
                        WHERE test_id = i_testId1 AND DATABASE_NAME = l_dbname AND sql_id = l_c_sql_id
                        MINUS
                        SELECT DISTINCT PLAN_HASH_VALUES AS phv_test2
                        FROM TEST_RESULT_SQL
                        WHERE test_id = i_testId2 AND DATABASE_NAME = l_dbname AND sql_id = l_c_sql_id
                    ) LOOP
                        unmatched_phv := 1; --unmatched plan detected
                        EXIT;
                    END LOOP;

                    FOR rec IN (
                        SELECT DISTINCT PLAN_HASH_VALUES AS phv_test2
                        FROM TEST_RESULT_SQL
                        WHERE test_id = i_testId2 AND DATABASE_NAME = l_dbname AND sql_id = l_c_sql_id
                        MINUS
                        SELECT DISTINCT PLAN_HASH_VALUES AS phv_test1
                        FROM TEST_RESULT_SQL
                        WHERE test_id = i_testId1 AND DATABASE_NAME = l_dbname AND sql_id = l_c_sql_id
                    ) LOOP
                        unmatched_phv := 1; --unmatched plan detected
                        EXIT;
                    END LOOP;

                    IF unmatched_phv = 1 THEN
                        phv_flag := '2'; -- PHV mismatch detected
                        best_plan_message := GET_BEST_PLAN(i_testId1, l_dbname, i_c_sql_id,  l_end);
                    ELSIF unmatched_phv = 0 THEN
                      phv_flag := '0' ; --no plan flips
                      best_plan_message :='' ;
                    END IF;
                    phv_message := phv_messages(phv_flag) || best_plan_message;

                END;
            END IF;
          ELSE
            begin
              select sql_b, similarity_score  
              into sim_sql_id, score              
              from(
                select a1.sql_id sql_a,
                b2.sql_id sql_b,
                UTL_MATCH.edit_distance_similarity(
                    DBMS_LOB.substr(a1.sql_text, 4000, 1),
                    DBMS_LOB.substr(b2.sql_text, 4000, 1)
                ) as similarity_score
                from TEST_RESULT_SQLTEXT a1, TEST_RESULT_SQLTEXT b2
                where a1.sql_id = l_c_sql_id
                and b2.sql_id in (select distinct sql_id from TEST_RESULT_SQL where test_id=i_testId2 and database_name= i_database_name)
                and UTL_MATCH.edit_distance_similarity (DBMS_LOB.substr(a1.sql_text,4000, 1), DBMS_LOB.substr(b2.sql_text, 4000, 1)) between 90 and 99
                order by similarity_score desc)
                where rownum <=1;
            exception when no_data_found then
              NULL; --do nothing
            end;

            if sim_sql_id is not null then --we set the sql_similarity flag here
              sql_similarity_flag := '1' ;
              --DBMS_OUTPUT.PUT_LINE('Matched sql_id was: '||sim_sql_id||' and score was: '||score);
            else
              sql_similarity_flag := '0' ;
            end if;
            --DBMS_OUTPUT.PUT_LINE('sql_id searched was: '||l_c_sql_id||' and DBName was: '||database_name||' and test_id was '||i_testId1||'and ' ||i_testId2) ;   
            --we continue with setting appropriate phv flag
            phv_flag := '3'; --no sql_id to compare against in the baseline
            phv_message := phv_messages(phv_flag);
          END IF;

          IF trim(i_duration_status) ='Amber' OR  trim(i_duration_status) = 'Red' THEN
            IF i_prev_els is NULL AND i_cur_els is NOT NULL THEN      
              select avrg_els
              , stdd_els   
              , avrg_execs 
              , stdd_execs INTO avg_els, std_els, avg_execs, std_execs   from (
                select  AVG(elapsed_time_per_exec_seconds) as avrg_els
                , STDDEV(elapsed_time_per_exec_seconds) as stdd_els
                , AVG(executions) as avrg_execs
                , STDDEV(executions) as stdd_execs   
                FROM TEST_RESULT_SQL  
                  where database_name= l_dbname
                  and sql_id = l_c_sql_id
                  and begin_time <= to_date(l_end,'DDMONYY-HH24:MI')
                  ORDER by begin_time desc 
                  )
                  WHERE ROWNUM < 15  ;

              IF std_els > 0 THEN
                deviation_els := ABS((i_cur_els - avg_els) / std_els);
                IF deviation_els > threshold THEN
                  dev_els_flag := '1' ;                
                ELSIF deviation_els < threshold THEN
                  dev_els_flag := '0' ;                
                END IF;
                stddev_els_message := dev_els_messages(dev_els_flag);
              END IF;

              IF std_execs > 0 THEN
                deviation_execs := ABS((i_cur_executions - avg_execs) / std_execs);
                IF deviation_execs > threshold THEN
                  dev_execs_flag := '1' ;                
                ELSIF deviation_execs < threshold THEN
                  dev_execs_flag := '0' ;                
                END IF;
                stddev_execs_message := dev_execs_messages(dev_execs_flag);
              END IF;

              IF std_els = 0 OR std_execs = 0 THEN
                dev_els_flag := '2' ;
                dev_execs_flag := '2';
                stddev_els_message := dev_els_messages(dev_els_flag);
                stddev_execs_message := dev_execs_messages(dev_execs_flag);              
              END IF;
              stddev_els_message := stddev_els_message ||stddev_execs_message ;

            ELSIF i_prev_els is NOT NULL AND i_cur_els is NOT NULL THEN
              --what is the ratio of the current metrics to previous metrics?
              if i_prev_rows_per_execution != 0 then
                rows_per_exec_ratio := i_cur_rows_per_execution / i_prev_rows_per_execution ;
              end if;
              if i_prev_executions != 0 then
                total_exec_ratio := i_cur_executions / i_prev_executions ;
              end if;
              if i_prev_cpu_time_seconds !=0 then
                cpu_time_ratio := i_cur_cpu_time_seconds / i_prev_cpu_time_seconds ;
              end if;
              if i_prev_els !=0 then
                els_ratio := i_cur_els / i_prev_els ;
              end if;

              if rows_per_exec_ratio is not null then
                if round(rows_per_exec_ratio,2) < 1 then 
                  rows_per_exec_ratio_perc := 100 - (round(rows_per_exec_ratio,2) * 100) ; --convert to percentage
                end if;
                rows_per_exec_ratio_message :=' Rows processed per execution is '|| 
                  case when round(rows_per_exec_ratio,2) > 1 then round(rows_per_exec_ratio,2)||' times higher than the baseline test. '
                      when round(rows_per_exec_ratio,2) < 1 then round(rows_per_exec_ratio_perc,2)||'% lower than the baseline test. ' 
                      else 'equal to the baseline test. '
                  end;
              end if;
              if total_exec_ratio is not null then
                if round(total_exec_ratio,2) < 1 then 
                  total_exec_ratio_perc := 100 - (round(total_exec_ratio,2) * 100) ; --convert to percentage
                end if;
                total_exec_ratio_message :='Number of times SQL was executed was '|| 
                  case when round(total_exec_ratio,2) > 1 then round(total_exec_ratio,2)||' times higher than the baseline test. '
                      when round(total_exec_ratio,2) < 1 then round(total_exec_ratio_perc,2)||'% lower than the baseline test. ' 
                      else 'equal to the baseline test. '
                  end;
              end if;
              if els_ratio is not null then
                if round(els_ratio,2) < 1 then 
                  els_ratio_perc := 100 - (round(els_ratio,2) * 100) ; --convert to percentage
                end if;
                els_ratio_message :='SQL execution time was found to be '|| 
                  case when round(els_ratio,2) > 1 then round(els_ratio,2)||' times higher than the baseline test. '
                      when round(els_ratio,2) < 1 then round(els_ratio_perc,2)||'% lower than the baseline test. ' 
                      else 'equal to the baseline test. '
                  end;
              end if;
              --DBMS_OUTPUT.PUT_LINE('els_ratio_perc is: '||els_ratio_perc||' total_exec_ratio_perc is: '||total_exec_ratio_perc||' rows_per_exec_ratio_perc is: '||rows_per_exec_ratio_perc);

              stddev_els_message := els_ratio_message ;
            END IF;
          END IF;

          IF trim(i_tps_status) ='Amber' OR  trim(i_tps_status) = 'Red' THEN
            IF i_prev_tps is NULL AND i_cur_tps is NOT NULL THEN
              select avrg_tps 
              , stdd_tps  
              , avrg_execs 
              , stdd_execs INTO avg_tps, std_tps, avg_execs, std_execs   from (
                select AVG(tps) as avrg_tps
                , STDDEV(tps) as stdd_tps
                , AVG(executions) as avrg_execs
                , STDDEV(executions) as stdd_execs   
                FROM TEST_RESULT_SQL  
                  where database_name = l_dbname
                  and sql_id = l_c_sql_id
                  and begin_time <= to_date(l_end,'DDMONYY-HH24:MI')
                  ORDER by begin_time desc 
                  )
                  WHERE ROWNUM < 15  ;

              IF std_tps > 0 THEN
                deviation_tps := ABS((i_cur_tps - avg_tps) / std_tps);
                IF deviation_tps > threshold THEN
                  dev_tps_flag := '1' ;
                ELSIF deviation_tps < threshold THEN
                  dev_tps_flag := '0' ;
                END IF;
                stddev_tps_message := dev_tps_messages(dev_tps_flag);
              ELSIF std_execs > 0 THEN
                deviation_execs := ABS((i_cur_executions - avg_execs) / std_execs);
                IF deviation_execs > threshold THEN
                  dev_execs_flag := '1' ;
                ELSIF deviation_execs < threshold THEN
                  dev_execs_flag := '0' ;
                END IF;
                stddev_execs_message := dev_execs_messages(dev_execs_flag);
              ELSIF std_tps <= 0 OR std_execs <= 0 THEN
                dev_tps_flag := '2' ;
                dev_execs_flag := '2' ;
                stddev_tps_message := dev_tps_messages(dev_tps_flag);
                stddev_execs_message := dev_execs_messages(dev_execs_flag);

              END IF;
              stddev_tps_message := stddev_tps_message || stddev_execs_message ;
            ELSIF i_prev_tps is NOT NULL AND i_cur_tps is NOT NULL THEN
              if i_prev_tps !=0 then
                tps_ratio := i_cur_tps / i_prev_tps ;
              end if;
              --what is the ratio of the current metrics to previous metrics?
              if rows_per_exec_ratio is NULL OR total_exec_ratio is NULL OR cpu_time_ratio is NULL then --we check if any of the metric ratios is null which means it has yet to be computed.
                if i_prev_rows_per_execution != 0  then
                  rows_per_exec_ratio := i_cur_rows_per_execution / i_prev_rows_per_execution ;
                end if;
                if i_prev_executions != 0 then 
                  total_exec_ratio := i_cur_executions / i_prev_executions ;
                end if;
                if i_prev_cpu_time_seconds !=0 then 
                  cpu_time_ratio := i_cur_cpu_time_seconds / i_prev_cpu_time_seconds ;
                end if;              

                if rows_per_exec_ratio is not null then
                  if round(rows_per_exec_ratio,2) < 1 then 
                    rows_per_exec_ratio_perc := 100 - (round(rows_per_exec_ratio,2) * 100) ; --convert to percentage
                  end if;
                  rows_per_exec_ratio_message :=' Rows processed per execution is '|| 
                    case when round(rows_per_exec_ratio,2) > 1 then round(rows_per_exec_ratio,2)||' times higher than the baseline test. '
                        when round(rows_per_exec_ratio,2) < 1 then round(rows_per_exec_ratio_perc,2)||'% lower than the baseline test. ' 
                        else 'equal to the baseline test. '
                    end;
                end if;
                if total_exec_ratio is not null then
                  if round(total_exec_ratio,2) < 1 then 
                    total_exec_ratio_perc := 100 - (round(total_exec_ratio,2) * 100) ; --convert to percentage
                  end if;
                  total_exec_ratio_message :='Number of times SQL was executed was '|| 
                    case when round(total_exec_ratio,2) > 1 then round(total_exec_ratio,2)||' times higher than the baseline test. '
                        when round(total_exec_ratio,2) < 1 then round(total_exec_ratio_perc,2)||'% lower than the baseline test. ' 
                        else 'equal to the baseline test. '
                    end;
                end if;
              end if;

              if tps_ratio is not null then
                if round(tps_ratio,2) < 1 then 
                  tps_ratio_perc := 100 - (round(tps_ratio,2) * 100) ; --convert to percentage
                end if;
                tps_ratio_message :='SQL transaction load was found to be '|| 
                  case when round(tps_ratio,2) > 1 then round(tps_ratio,2)||' times higher than the baseline test. '
                      when round(tps_ratio,2) < 1 then round(tps_ratio_perc,2)||'% lower than the baseline test. ' 
                      else 'equal to the baseline test. '
                  end;
              end if;
              --DBMS_OUTPUT.PUT_LINE('tps_ratio_perc is: '||tps_ratio_perc||' total_exec_ratio_perc is: '||total_exec_ratio_perc||' rows_per_exec_ratio_perc is: '||rows_per_exec_ratio_perc);

              stddev_tps_message := tps_ratio_message ;

            END IF;
          END IF;


          IF sql_similarity_flag = '1' then
            sql_similarity_message := sim_score_messages(sql_similarity_flag);
            append_message := sim_sql_id||' with a score of '||score||'.'; --here we add the information about the topmost matched SQL and its similarity score
          ELSIF sql_similarity_flag = '0' then
            sql_similarity_message := sim_score_messages(sql_similarity_flag);
          END IF;
          stddev_message := stddev_els_message||stddev_tps_message||rows_per_exec_ratio_message||total_exec_ratio_message||sql_similarity_message||append_message;
          --DBMS_OUTPUT.PUT_LINE('i_testId1 is :'||i_testId1||' l_dbname is :'||l_dbname||' i_c_sql_id is:'||i_c_sql_id||'l_end is:'||  l_end);
          best_plan_message := GET_BEST_PLAN(i_testId1, l_dbname, i_c_sql_id,  l_end);
          --DBMS_OUTPUT.PUT_LINE('best_plan_message is:'||best_plan_message);
          return phv_message|| stddev_message ;

        END IF;

        IF i_db_time_per_sec_status is not NULL OR i_db_cpu_per_sec_status is not NULL OR i_execs_per_sec_status is not NULL THEN  --processing for db_load_summary only
          --next we take care of the messaging in situations where nothing is returned for db_time or db_exec or db_cpu load.
          idle_db_load_message := '';

          -- Check for DB Time
          idle_db_load_message := idle_db_load_message || 
              CASE 
                  WHEN i_prev_db_time_per_sec IS NULL OR i_cur_db_time_per_sec IS NULL THEN 
                      'There probably was no value returned for DB Time in either the baseline or current test. This could be due to an outage or DB was idle. '
              END;

          -- Check for CPU Utilisation
          idle_db_load_message := idle_db_load_message || 
              CASE 
                  WHEN i_prev_db_cpu_per_sec IS NULL OR i_cur_db_cpu_per_sec IS NULL THEN 
                      'There probably was no value returned for CPU utilisation in either the baseline or current test. This could be due to an outage or DB was idle. '
              END;

          -- Check for SQL Execution Load
          idle_db_load_message := idle_db_load_message || 
              CASE 
                  WHEN i_prev_db_execs_per_sec IS NULL OR i_cur_db_execs_per_sec IS NULL THEN 
                      'There probably was no value returned for database SQL execution load in either the baseline or current test. This could be due to an outage or DB was idle.'
              END;


          if i_prev_db_time_per_sec != 0 then
            db_time_per_sec_ratio := i_cur_db_time_per_sec / i_prev_db_time_per_sec ;
          end if;

          if i_prev_db_cpu_per_sec != 0 then
            db_cpu_per_sec_ratio := i_cur_db_cpu_per_sec / i_prev_db_cpu_per_sec ;
          end if;

          if i_prev_db_execs_per_sec != 0 then
            db_execs_per_sec_ratio := i_cur_db_execs_per_sec / i_prev_db_execs_per_sec ;
          end if;


          if db_time_per_sec_ratio is not null then
            if round(db_time_per_sec_ratio,2) < 1 then 
              db_time_per_sec_ratio_perc := 100 - (round(db_time_per_sec_ratio,2) * 100) ; --convert to percentage
            end if;
            db_time_per_sec_ratio_message :=' The database time expended was '|| 
              case when round(db_time_per_sec_ratio,2) > 1 then round(db_time_per_sec_ratio,2)||' times higher than the baseline test. '
                    when round(db_time_per_sec_ratio,2) < 1 then round(db_time_per_sec_ratio_perc,2)||'% lower than the baseline test. ' 
                    else 'equal to the baseline test. '
              end;
          end if;


          if db_cpu_per_sec_ratio is not null then
            if round(db_cpu_per_sec_ratio,2) < 1 then 
              db_cpu_per_sec_ratio_perc := 100 - (round(db_cpu_per_sec_ratio,2) * 100) ; --convert to percentage
            end if;
            db_cpu_per_sec_ratio_message :=' The amount of CPU time expended was '|| 
              case when round(db_cpu_per_sec_ratio,2) > 1 then round(db_cpu_per_sec_ratio,2)||' times higher than the baseline test. '
                    when round(db_cpu_per_sec_ratio,2) < 1 then round(db_cpu_per_sec_ratio_perc,2)||'% lower than the baseline test. ' 
                    else 'equal to the baseline test. '
              end;
          end if;


          if db_execs_per_sec_ratio is not null then
            if round(db_execs_per_sec_ratio,2) < 1 then 
              db_execs_per_sec_ratio_perc := 100 - (round(db_execs_per_sec_ratio,2) * 100) ; --convert to percentage
            end if;
            db_execs_per_sec_ratio_message :=' The total database SQL executions per sec was '|| 
              case when round(db_execs_per_sec_ratio,2) > 1 then round(db_execs_per_sec_ratio,2)||' times higher than the baseline test. '
                    when round(db_execs_per_sec_ratio,2) < 1 then round(db_execs_per_sec_ratio_perc,2)||'% lower than the baseline test. ' 
                    else 'equal to the baseline test. '
              end;
          end if;

          db_load_message := db_time_per_sec_ratio_message || db_cpu_per_sec_ratio_message || db_execs_per_sec_ratio_message || idle_db_load_message ;
          return db_load_message ;

        END IF;


       EXCEPTION
        WHEN no_testid_found THEN
            logger.write('Test ID not found : '||t_test1.TEST_ID) ;
            return 'Test ID not found : '||t_test1.TEST_ID ;
        WHEN OTHERS THEN
            logger.write('Get_SQL_Analysis - '||l_dbname|| ' : ' ||SQLCODE||' : '||SUBSTR(SQLERRM, 1, 500)) ;
            return 'Error encountered :'||SQLCODE ;
    END Get_SQL_Analysis;   

    Function GET_BEST_PLAN(
      i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE,
      i_database_name IN TEST_RESULT_SQL.database_name%TYPE,
      i_c_sql_id IN TEST_RESULT_SQL.sql_id%TYPE,
      l_end varchar2
    ) RETURN VARCHAR2
    AS
    phv_test  TEST_RESULT_SQL.plan_hash_values%TYPE;
    least_els number;
    best_plan_message varchar2(4000);
    begin
      select PLAN_HASH_VALUES,AVG(ELAPSED_TIME_PER_EXEC_SECONDS)  into phv_test, least_els 
        from (SELECT PLAN_HASH_VALUES,ELAPSED_TIME_PER_EXEC_SECONDS
          FROM TEST_RESULT_SQL
              WHERE  DATABASE_NAME = i_database_name AND sql_id = i_c_sql_id
              AND begin_time <= to_date(l_end,'DDMONYY-HH24:MI')
              and TEST_ID like '%FULL'
              and PLAN_HASH_VALUES !=0
              and NOT REGEXP_LIKE(PLAN_HASH_VALUES, '\s') -- Exclude values with single space which represent concatenated PHVs
              order by BEGIN_TIME desc
              FETCH FIRST 20 ROWS ONLY)
              group by PLAN_HASH_VALUES
              ORDER BY AVG(ELAPSED_TIME_PER_EXEC_SECONDS) ASC -- Least elapsed time first
              FETCH FIRST ROW ONLY; -- Return the single row with the least average elapsed time 
      best_plan_message := ' The best SQL plan '||
        case when phv_test is not null then 'detected was '||phv_test||'.'
             when phv_test is null then 'could not be found.'
             --when phv_test = 0 then 'could not be detected'
             else 'could not be detected.' 
    end;
    Return best_plan_message;

    exception 
    when no_data_found then 
      return 'The best SQL plan could not be found.' ;
    when others then
      logger.write('Get_Best_Plan - '||i_database_name|| ' : ' ||SQLCODE||' : '||SUBSTR(SQLERRM, 1, 500)) ;
      return 'Error encountered :'||SQLCODE ;
  END GET_BEST_PLAN; 


FUNCTION Get_waitclass_comparison_summary (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_title   IN varchar2 default 'Y'
          ) RETURN g_tvc2 PIPELINED
    AS
        t_test1   TEST_RESULT_MASTER%ROWTYPE ;
        t_test2   TEST_RESULT_MASTER%ROWTYPE ;

        l_row     varchar2(4000);
        l_pred    number ;       -- Ratio to calculate a RED warning 
        l_pamber  number ;       -- Ratio to calculate an AMBER warning 

    BEGIN
        -- Set parameters
        t_test1   := REPORT_GATHER.Get_TEST_Details(i_testId1) ;
        t_test2   := REPORT_GATHER.Get_TEST_Details(i_testId2) ;
        -- Set controls
        l_pred    := 10 ;
        l_pamber  := 2 ;   

        if Upper(i_title) = 'Y' then 
            PIPE ROW ( 'h3. DB WAIT CLASS COMPARISON' ) ;
        end if;          

        PIPE ROW ( 'h5. DB Wait Class Comparison (SUMMARY)' ) ;
        PIPE ROW ( 'Show how the previous tests compare to each other regarding the wait classes in the individual databases' ) ;
        PIPE ROW ( 'CORE Databases are marked with ''*''' ) ;

        PIPE ROW ( '||Overall Status||Database||DB IO||DB CPU||Application and Commit||Concurrency||Network Waits||Administrative||Details||' ) ;
        FOR r1 IN (
          with so as (select a.database_name, nvl(d.db_group,'NOTCORE') typ, round(avg(a.systemio+a.userio),2) IO, round(avg(a.appl+a.comt),2) AC, round(avg(a.curr),2) curr, round(avg(a.netw),2) net, round(avg(a.admin),2) admn, round(avg(a.cpu+a.bcpu),2) CPU_tot
            from test_result_activity a, V_TEST_RESULT_DBS d
            where a.test_id = t_test1.TEST_ID
            and a.database_name = d.db_name(+)
            and d.db_group(+) = 'CORE'
            group by a.database_name, d.db_group),
bl as (select a.database_name, nvl(d.db_group,'NOTCORE') typ, round(avg(a.systemio+a.userio),2) IO, round(avg(a.appl+a.comt),2) AC, round(avg(a.curr),2) curr, round(avg(a.netw),2) net, round(avg(a.admin),2) admn, round(avg(a.cpu+a.bcpu),2) CPU_tot
            from test_result_activity a, V_TEST_RESULT_DBS d
            where a.test_id = t_test2.TEST_ID
            and a.database_name = d.db_name(+)
            and d.db_group(+) = 'CORE'
            group by a.database_name, d.db_group) ,         
cr as (select bl.database_name||' '||decode(bl.typ,'CORE','*') DB,
                        case when bl.typ='NOTCORE' and (bl.io<2 and so.io<2) then 'Green'
                                                                when bl.io =0 or so.io = 0 then 'Red'
                                                                when abs((so.io-bl.io)/bl.io) between 0.2 and 0.4 then 'Amber'
                                                                when abs((so.io-bl.io)/bl.io) > 0.4 then 'Red'
                                                                 else 'Green' end IO_status,
                        case when bl.typ='NOTCORE' and (bl.cpu_tot<2 and so.cpu_tot<2) then 'Green'
                                                                when bl.cpu_tot =0 or so.cpu_tot = 0 then 'Red'
                                                                when abs((so.cpu_tot-bl.cpu_tot)/bl.cpu_tot) between 0.2 and 0.4 then 'Amber'
                                                                when abs((so.cpu_tot-bl.cpu_tot)/bl.cpu_tot) > 0.4 then 'Red'
                                                                 else 'Green' end as CPU_status,
                        case when bl.typ='NOTCORE' and (bl.ac<2 and so.ac<2) then 'Green'
                                                                when so.ac = 0 and bl.ac >3 then 'Amber'
                                                                when bl.ac = 0 and so.ac >3 then 'Amber'
                                                                when bl.ac=0 and so.ac=0 then 'Green'
                                                                when bl.ac >3 and abs((so.ac-bl.ac)/bl.ac) between 0.2 and 0.4 then 'Amber'
                                                                when bl.ac >3 and abs((so.ac-bl.ac)/bl.ac) > 0.4 then 'Red'
                                                                 else 'Green' end as APP_status,
                        case when bl.typ='NOTCORE' and (bl.curr<2 and so.curr<2) then 'Green'
                                                                when so.curr = 0 and bl.curr >2 then 'Red'
                                                                when bl.curr = 0 and so.curr >2 then 'Red'
                                                                when bl.curr=0 and so.curr=0 then 'Green'
                                                                when bl.curr >2 and abs((so.curr-bl.curr)/bl.curr) between 0.2 and 0.4 then 'Amber'
                                                                when bl.curr >2 and abs((so.curr-bl.curr)/bl.curr) > 0.4 then 'Red'
                                                                 else 'Green' end as CURR_status,
                        case when bl.typ='NOTCORE' and (bl.net<2 and so.net<2) then 'Green'
                                                                when so.net = 0 and bl.net >2 then 'Amber'
                                                                when bl.net = 0 and so.net >2 then 'Amber'
                                                                when bl.net=0 and so.net=0 then 'Green'
                                                                when bl.net >2 and abs((so.net-bl.net)/bl.net) between 0.2 and 0.4 then 'Amber'
                                                                when bl.net >2 and abs((so.net-bl.net)/bl.net) > 0.4 then 'Red'
                                                                 else 'Green' end as NET_status,
                        case when bl.typ='NOTCORE' and (bl.admn<2 and so.admn<2) then 'Green'
                                                                when so.admn = 0 and bl.admn >4 then 'Amber'
                                                                when bl.admn = 0 and so.admn >4 then 'Amber'
                                                                when bl.admn=0 and so.admn=0 then 'Green'
                                                                when bl.admn >4 and abs((so.admn-bl.admn)/bl.admn) between 0.2 and 0.4 then 'Amber'
                                                                when bl.admn >4 and abs((so.admn-bl.admn)/bl.admn) > 0.4 then 'Red'
                                                                 else 'Green' end as ADMN_status,
                        case when bl.typ='NOTCORE' and (bl.io<2 and so.io<2) then ' '
                                                                when bl.io =0 then 'No IO in baseline \\'
                                                                when so.io = 0 then 'No IO in latest test \\'
                                                                when abs((so.io-bl.io)/bl.io) > 0.2 and sign(so.io-bl.io)=1 then 'IO up by '||round(abs((so.io-bl.io)/bl.io)*100,1)||'% \\'
                                                                when abs((so.io-bl.io)/bl.io) > 0.2 and sign(so.io-bl.io)=-1 then 'IO down by '||round(abs((so.io-bl.io)/bl.io)*100,1)||'% \\'
                                                                 else ' ' end ||
                        case when bl.typ='NOTCORE' and (bl.cpu_tot<2 and so.cpu_tot<2) then ' '
                                                                when bl.cpu_tot =0 then 'No CPU in baseline \\'
                                                                when so.cpu_tot = 0 then 'No CPU in latest test \\'
                                                                when abs((so.cpu_tot-bl.cpu_tot)/bl.cpu_tot) > 0.2 and sign(so.cpu_tot-bl.cpu_tot)=1 then 'CPU up by '||round(abs((so.cpu_tot-bl.cpu_tot)/bl.cpu_tot)*100,1)||'% \\'
                                                                when abs((so.cpu_tot-bl.cpu_tot)/bl.cpu_tot) > 0.2 and sign(so.cpu_tot-bl.cpu_tot)=-1 then 'CPU down by '||round(abs((so.cpu_tot-bl.cpu_tot)/bl.cpu_tot)*100,1)||'% \\'
                                                                 else ' ' end ||
                        case when bl.typ='NOTCORE' and (bl.ac<2 and so.ac<2) then ' '
                                                                when so.ac = 0 and bl.ac > 3 then 'Application/Commit wait detected in baseline \\'
                                                                when bl.ac = 0 and so.ac > 3 then 'App/Com wait detected in latest test \\'
                                                                when bl.ac=0 and so.ac=0 then ' '
                                                                when bl.ac > 3 and abs((so.ac-bl.ac)/bl.ac) > 0.2 and sign(so.ac-bl.ac)=1 then 'App/Com up by '||round(abs((so.ac-bl.ac)/bl.ac)*100,1)||'% \\'
                                                                when bl.ac > 3 and abs((so.ac-bl.ac)/bl.ac) > 0.2 and sign(so.ac-bl.ac)=-1 then 'App/Com down by '||round(abs((so.ac-bl.ac)/bl.ac)*100,1)||'% \\'
                                                                 else ' ' end ||
                        case when bl.typ='NOTCORE' and (bl.curr<2 and so.curr<2) then '\\'
                                                                when so.curr = 0 and bl.curr >2 then 'Concurrency wait detected in baseline \\'
                                                                when bl.curr = 0 and so.curr >2 then 'Concurrency wait detected in latest test \\'
                                                                when bl.curr=0 and so.curr=0 then ' '
                                                                when bl.curr >2 and abs((so.curr-bl.curr)/bl.curr) > 0.2 and sign(so.curr-bl.curr)=1 then 'Concurrency up by '||round(abs((so.curr-bl.curr)/bl.curr)*100,1)||'% \\'
                                                                when bl.curr >2 and abs((so.curr-bl.curr)/bl.curr) > 0.2 and sign(so.curr-bl.curr)=-1 then 'Concurrency down by '||round(abs((so.curr-bl.curr)/bl.curr)*100,1)||'% \\'
                                                                 else ' ' end ||
                        case when bl.typ='NOTCORE' and (bl.net<2 and so.net<2) then ' '
                                                                when so.net = 0 and bl.net >2 then 'Network wait detected in baseline \\'
                                                                when bl.net = 0 and so.net >2 then 'Network wait detected in latest test \\'
                                                                when bl.net=0 and so.net=0 then ' '
                                                                when bl.net >2 and abs((so.net-bl.net)/bl.net) > 0.2 and sign(so.net-bl.net)=1 then 'Network up by '||round(abs((so.net-bl.net)/bl.net)*100,1)||'% \\'
                                                                when bl.net >2 and abs((so.net-bl.net)/bl.net) > 0.2 and sign(so.net-bl.net)=-1 then 'Network down by '||round(abs((so.net-bl.net)/bl.net)*100,1)||'% \\'
                                                                 else ' ' end ||
                        case when bl.typ='NOTCORE' and (bl.admn<2 and so.admn<2) then '\\'
                                                                when so.admn = 0 and bl.admn >4 then 'Administrative wait detected in baseline \\'
                                                                when bl.admn = 0 and so.admn >4 then 'Administrative wait detected in latest test \\'
                                                                when bl.admn=0 and so.admn=0 then ' '
                                                                when bl.admn > 4 and abs((so.admn-bl.admn)/bl.admn) > 0.2 and sign(so.admn-bl.admn)=1 then 'Administrative up by '||round(abs((so.admn-bl.admn)/bl.admn)*100,1)||'% \\'
                                                                when bl.admn > 4 and abs((so.admn-bl.admn)/bl.admn) > 0.2 and sign(so.admn-bl.admn)=-1 then 'Administrative down by '||round(abs((so.admn-bl.admn)/bl.admn)*100,1)||'% \\'
                                                                 else ' ' end as Msgs
       from so, bl
       where so.database_name = bl.database_name), 
       os as (select cr.db, case when cr.io_status = 'Red' or cr.cpu_status = 'Red' or cr.app_status = 'Red' or cr.CURR_status = 'Red' or cr.NET_status= 'Red' then 'Red'
                   when cr.io_status = 'Amber' or cr.cpu_status = 'Amber' or cr.app_status = 'Amber' or cr.CURR_status = 'Amber' or cr.NET_status= 'Amber' then 'Amber'
                   else 'Green' end as overall_status
       from cr)
          SELECT '|{status:colour=' || CASE WHEN os.overall_status = 'Amber' THEN 'Yellow' ELSE os.overall_status END || '|title=' || os.overall_status || '}|'
                 || os.db  || '|'
                 || '{status:colour=' || CASE WHEN cr.io_status = 'Amber' THEN 'Yellow' ELSE cr.io_status END || '|title=' || cr.io_status || '}|'
                 || '{status:colour=' || CASE WHEN cr.cpu_status = 'Amber' THEN 'Yellow' ELSE cr.cpu_status END || '|title=' || cr.cpu_status || '}|'
                 || '{status:colour=' || CASE WHEN cr.app_status = 'Amber' THEN 'Yellow' ELSE cr.app_status END || '|title=' || cr.app_status || '}|'
                 || '{status:colour=' || CASE WHEN cr.curr_status = 'Amber' THEN 'Yellow' ELSE cr.curr_status END || '|title=' || cr.curr_status || '}|'
                 || '{status:colour=' || CASE WHEN cr.net_status = 'Amber' THEN 'Yellow' ELSE cr.net_status END || '|title=' || cr.net_status || '}|'
                 || '{status:colour=' || CASE WHEN cr.admn_status = 'Amber' THEN 'Yellow' ELSE cr.admn_status END || '|title=' || cr.admn_status 
                 || '}|'
                 || cr.msgs||'  |' AS col1
            FROM os, cr
            where os.db = cr.db
           ORDER BY os.db
        )
        LOOP
          PIPE ROW ( r1.col1 ) ;
        END LOOP ;

    END Get_waitclass_comparison_summary ;
    

/*--------------------------------------------------------------------------------- */
    -- Get_all_db_top10_comparison 
    -- Based on Get_all_db_top25_comparison
    -- Added filter to bring back top 10 only
    -- Removed columns CPU Per Exec Diff, BG Per Exec Diff, CURR CPU Per Exec, PREV CPU Per Exec, CURR BG Per Exec and PREV BG Per Exec
/*--------------------------------------------------------------------------------- */           
    FUNCTION Get_all_db_top10_comparison (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_title   IN varchar2 default 'Y'
          , i_mode    IN varchar2 default NULL
          , i_top_n   IN number  default NULL
          ) RETURN g_tvc2 PIPELINED
    AS
        t_test1   TEST_RESULT_MASTER%ROWTYPE ;
        t_test2   TEST_RESULT_MASTER%ROWTYPE ;
        l_mode    varchar2(5) ;  -- QUERY search mode : NULL = All, 'APP' - Application related only, 'DB' - Oracle related only

        l_row     varchar2(4000);

        l_test_description varchar2(100);
        l_start_time       varchar2(100); 
        l_end_time         varchar2(100);
        l_top_n number;

    begin
       -- Set default parameters
        t_test1   := REPORT_GATHER.Get_TEST_Details(i_testId1) ;
        t_test2   := REPORT_GATHER.Get_TEST_Details(i_testId2) ;
        l_mode    := i_mode;
        l_top_n   := i_top_n;

        if l_top_n is NULL then
          l_top_n := 25;
        end if;

        FOR r_dbName IN (
            SELECT db.db_type, t.database_name 
              FROM TEST_RESULT_SQL t
              JOIN TEST_RESULT_DBS db ON ( db.db_name = t.database_name and db.db_env = t_test1.DB_ENV )
             WHERE t.test_id = t_test1.TEST_ID
             GROUP BY t.database_name, db.db_type
        ORDER BY 1
        )
        LOOP
            -- pipe header
            --if Upper(i_title) = 'Y' then 
            --    PIPE ROW ( 'h3. DETAIL SQL COMPARISON Per Database' ) ;
            --end if;

            l_row := 'h5. SQL Comparison for ' || r_dbName.db_type ;
            PIPE ROW ( l_row ) ;

            PIPE ROW ( '{csv:allowExport=true|sortIcon=true|columnTypes=s,i,i,s,s,s,s,f,f,f,f,f,f,f,f,s,s|rowStyles=,background:lightblue,background:darkgrey}' ) ;    
            l_row := '"SQL ID",'||
                    '"CURR Position",'||
                    '"PREV Position",'||
                    '"Total Elapsed Diff",'||
                    '"TPS Diff",'||
                    '"ms Per Exec Diff",'||
                    '"Rows Per Exec Diff",'||
                    '"CURR Total Elapsed",'||
                    '"PREV Total Elapsed",'||
                    '"CURR TPS",'||
                    '"PREV TPS",'||
                    '"CURR ms Per Exec",'||
                    '"PREV ms Per Exec",'||
                    '"CURR Rows Per Exec",'||
                    '"PREV Rows Per Exec",'||
                    '"Module",'||
                    '"SQL Text"';
            PIPE ROW ( l_row ) ;

            for r1 in (
                with prev as (
                    SELECT ROWNUM rnum, a.sql_id, a.tps, a.ms_pe, a.rpe, a.ela_sec, a.sql_text, a.module, a.database_name  
                      FROM (
                             SELECT  s.sql_id
                                   , s.database_name as database_name
                                   , ROUND ( s.tps , 2 ) AS tps
                                   , ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 )  AS ms_pe
                                   , ROUND ( s.rows_per_exec, 2 )  AS rpe
                                   , ROUND ( s.elapsed_time_seconds,2) ela_sec
                                   --, LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text
                                   , LOWER ( REPLACE ( REPLACE ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ,'[' ) , ']' ) )  AS sql_text
                                   , LOWER ( s.module ) AS module
                                FROM TEST_RESULT_SQL s
                                --JOIN TEST_RESULT_DBS db ON ( db.db_name = s.database_name )
                                LEFT OUTER JOIN TEST_RESULT_SQLTEXT st ON st.sql_id = s.sql_id
                               WHERE s.test_id = t_test2.TEST_ID
                                 AND s.database_name = r_dbName.database_name
                                 AND ( CASE  WHEN ( l_mode = 'APP' and ( Upper(s.module) not in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','SYSTEM','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER')
                                                                       and Upper(s.module) not like 'RMAN%'
                                                                       and s.module not like 'Oracle Enterprise Manager%'
                                                                      )) THEN 1
                                             WHEN ( l_mode = 'DB'  and ( Upper(s.module) in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','SYSTEM','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER') 
                                                                           or Upper(s.module) like 'RMAN%'
                                                                           or s.module like 'Oracle Enterprise Manager%'   
                                                                          )) THEN 1
                                             WHEN ( l_mode is NULL ) THEN 1
                                             ELSE 0 END ) = 1                          
                               ORDER BY s.top_sql_number 
                               ) a ),
                cur as ( 
                    SELECT ROWNUM rnum, a.sql_id, a.tps, a.ms_pe, a.rpe, a.sql_text, a.ela_sec, a.module, a.database_name    
                        FROM (
                             SELECT  s.sql_id
                                   , s.database_name as database_name
                                   , ROUND ( s.tps , 2 ) AS tps
                                   , ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 )  AS ms_pe
                                   , ROUND ( s.rows_per_exec, 2 )  AS rpe
                                   , ROUND ( s.elapsed_time_seconds,2) ela_sec 
                                   -- , LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text
                                   , LOWER ( REPLACE ( REPLACE ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ,'[' ) , ']' ) )  AS sql_text
                                   , LOWER ( s.module ) AS module
                                FROM TEST_RESULT_SQL s
                                --JOIN TEST_RESULT_DBS db ON ( db.db_name = s.database_name )
                                LEFT OUTER JOIN TEST_RESULT_SQLTEXT st ON st.sql_id = s.sql_id
                               WHERE s.test_id = t_test1.TEST_ID
                                 AND s.top_sql_number <= l_top_n
                                 AND s.database_name = r_dbName.database_name
                                 AND ( CASE  WHEN ( l_mode = 'APP' and ( Upper(s.module) not in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','SYSTEM','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER')
                                                                       and Upper(s.module) not like 'RMAN%'
                                                                       and s.module not like 'Oracle Enterprise Manager%'
                                                                      )) THEN 1
                                             WHEN ( l_mode = 'DB'  and ( Upper(s.module) in ('SQL DEVELOPER','DBMS_SCHEDULER','SYS','SYSTEM','HP_DIAG','EMAGENT_SQL_ORACLE_DATABASE','SKYUTILS','MMON_SLAVE','BACKUP ARCHIVELOG','HORUS_MONITORING','DBSNMP','DBMON_AGENT_USER') 
                                                                           or Upper(s.module) like 'RMAN%'
                                                                           or s.module like 'Oracle Enterprise Manager%'   
                                                                          )) THEN 1
                                             WHEN ( l_mode is NULL ) THEN 1
                                             ELSE 0 END ) = 1  
                              ORDER BY s.top_sql_number
                             ) a )
            select '"' || cur.sql_id
                   || '","' || cur.rnum 
                   || '","' || nvl(to_char(prev.rnum), 'N/A') 
                   || '","' || case WHEN prev.ela_sec > 0 THEN ROUND(round((cur.ela_sec-prev.ela_sec)/prev.ela_sec,4)*100,0) || '%' ELSE 'N/A' END 
                   || '","' || case WHEN prev.tps > 0     THEN ROUND(round((cur.tps-prev.tps)/prev.tps,4)*100,0)  || '%'             ELSE 'N/A' END 
                   || '","' || case WHEN prev.ms_pe > 0   THEN ROUND(round((cur.ms_pe-prev.ms_pe)/prev.ms_pe,4)*100,0)  || '%'       ELSE 'N/A' END 
                   || '","' || case WHEN prev.rpe > 0     THEN ROUND(round((cur.rpe-prev.rpe)/prev.rpe,4)*100,0)  || '%'             ELSE 'N/A' END  
                   || '","' || cur.ela_sec
                   || '","' || nvl(to_char(prev.ela_sec), 'N/A')
                   || '","' || cur.tps
                   || '","' || nvl(to_char(prev.tps), 'N/A')
                   || '","' || cur.ms_pe
                   || '","' || nvl(to_char(prev.ms_pe), 'N/A')
                   || '","' || cur.rpe
                   || '","' || nvl(to_char(prev.rpe), 'N/A') 
                   || '","' || cur.module
                   || '","' || cur.sql_text
                   || '"' as col1
            from cur
            LEFT OUTER JOIN prev on ( cur.database_name = prev.database_name and cur.sql_id = prev.sql_id )
            where cur.rnum <= 10
            order by cur.rnum )
            LOOP
                PIPE ROW ( r1.col1 ) ;
            END LOOP ;
            PIPE ROW ( '{csv}' ) ;
        end loop;
    end Get_all_db_top10_comparison;

END REPORT_COMP;
/
