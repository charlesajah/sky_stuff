CREATE OR REPLACE PACKAGE REPORT_DATA AS 

/* This is the REPOSITORY of Procedures & Functions common for 
   all the DATA reporting within the Confluenece Reports Generation 
*/ 

   TYPE g_tvc2 IS TABLE OF VARCHAR2(4000) ;

   FUNCTION Get_test_info (
        i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED ;

--   FUNCTION Get_top25_detail (
--        i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
--      , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
--      ) RETURN g_tvc2 PIPELINED;

   FUNCTION Get_PGA_growth (
            i_testId IN TEST_RESULT_MASTER.test_id%TYPE 
          ) RETURN g_tvc2 PIPELINED;

   FUNCTION Get_RedoLogs_Usage (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          ) RETURN g_tvc2 PIPELINED;

   FUNCTION Get_sal_basket_sizes (
        i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE DEFAULT 'Latest test'
      ) RETURN g_tvc2 PIPELINED ;

   FUNCTION Get_DB_Details (
        i_testId1  IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_testId2  IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_metric   IN varchar2 
      , i_filter   IN varchar2 DEFAULT NULL
      ) RETURN g_tvc2 PIPELINED ;

   PROCEDURE Do_AnalyticalData (
         i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
       , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
       , i_dev      IN NUMBER    DEFAULT 0.35
       , i_days     IN NUMBER    DEFAULT 30
       , i_ratio    IN NUMBER    DEFAULT 2
       , i_except   IN VARCHAR2  DEFAULT NULL 
       , i_topsql   IN NUMBER    DEFAULT NULL
       ) ;

   FUNCTION Get_DB_Summary (
        i_testId1      IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_testId2      IN TEST_RESULT_MASTER.test_id%TYPE 
      , i_filter       IN varchar2 DEFAULT NULL      
      , i_ratio        IN number default 2
      , i_summary      IN varchar2 default NULL
      ) RETURN g_tvc2 PIPELINED ;  

    FUNCTION Get_Cache_Info (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          ) RETURN g_tvc2 PIPELINED;

    FUNCTION Get_billing_rate (
            i_testId  IN TEST_RESULT_MASTER.test_id%TYPE 
          ) RETURN g_tvc2 PIPELINED;

    FUNCTION Get_ObjectDetails (
         i_testid       in TEST_RESULT_OBJ_DETAILS.TEST_ID%type         default NULL
        ,i_env          in TEST_RESULT_OBJ_DETAILS.DB_ENV%type          default 'ALL'
        ,i_dbname       in TEST_RESULT_OBJ_DETAILS.DATABASE_NAME%type   default NULL
        ,i_objtype      in TEST_RESULT_OBJ_DETAILS.OBJ_TYPE%type        default NULL
        ) RETURN g_tvc2 PIPELINED ;          

    FUNCTION Get_ObjectDetailsReport (
         i_testid       in TEST_RESULT_OBJ_DETAILS.TEST_ID%type         default NULL
        ,i_env          in TEST_RESULT_OBJ_DETAILS.DB_ENV%type          default 'ALL'
        ,i_dbname       in TEST_RESULT_OBJ_DETAILS.DATABASE_NAME%type   default NULL
        ,i_objtype      in TEST_RESULT_OBJ_DETAILS.OBJ_TYPE%type        default NULL
        ,i_mode         in varchar2                                     default 'FULL'        
        ) RETURN g_tvc2 PIPELINED;

    PROCEDURE Do_load_object_parallelism_data ;

END REPORT_DATA;
/


CREATE OR REPLACE PACKAGE BODY REPORT_DATA AS


/*--------------------------------------------------------------------------------- */
    -- This is the REPOSITORY of Procedures & Functions common for  
    --  all the DATA reporting within the Confluenece Reports Generation 
/*--------------------------------------------------------------------------------- */

   
/*--------------------------------------------------------------------------------- */
    -- GET_TEST_INFO
    -- Gets the TEST Information from TEST_RESULT_MASTER for each TEST_ID provided
/*--------------------------------------------------------------------------------- */    
    FUNCTION Get_test_info (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          ) RETURN g_tvc2 PIPELINED
    AS
        t_testId1       TEST_RESULT_MASTER%ROWTYPE ;
        t_testId2       TEST_RESULT_MASTER%ROWTYPE ;
        l_row           varchar2(4000);
        l_mode          varchar2(4000);
        
        testid_not_found        EXCEPTION ;
        l_testid_not_found      TEST_RESULT_MASTER.test_id%TYPE ; 
    BEGIN
        -- Get the information from each of the TEST_ID provided
        t_testId1 := REPORT_GATHER.Get_Test_Details(i_testId1) ;
        t_testId2 := REPORT_GATHER.Get_Test_Details(i_testId2) ;
    
        -- Check the TEST_ID passed are valid
        if t_testId1.TEST_ID is null then
            l_testid_not_found := i_testId1;
            raise testid_not_found;
        elsif t_testId2.TEST_ID is null then 
            l_testid_not_found := i_testId2;
            raise testid_not_found;
        end if;
        
        l_row := '{toc:type=list}';
        PIPE ROW ( l_row ) ;
    
        -- The top level label would be driven by the actual calling scripts
        l_row := 'h3. NFT - Analysis Information';
        PIPE ROW ( l_row ) ;
        
        -- Current Test Info
        l_row := 'h5. Current Test for environment : '||REPORT_ADM.Parse_TestId(t_testid1.TEST_ID,'ENV')||' - Group : '||REPORT_ADM.Parse_TestId(t_testid1.TEST_ID,'GRP');
        PIPE ROW ( l_row ) ;       
        l_row := 'Description : ' || t_testid1.TEST_DESCRIPTION;
        PIPE ROW ( l_row ) ;    
        l_row := 'Start Time : ' || REPORT_ADM.Get_DTM(t_testid1.TEST_ID,'START')||' - End Time : ' || REPORT_ADM.Get_DTM(t_testid1.TEST_ID,'END');
        PIPE ROW ( l_row ) ;
     
        -- Previous Test Info
        l_row := 'h5. Previous Test for environment : '||REPORT_ADM.Parse_TestId(t_testid2.TEST_ID,'ENV')||' - Group : '||REPORT_ADM.Parse_TestId(t_testid2.TEST_ID,'GRP');
        PIPE ROW ( l_row ) ;       
        l_row := 'Description : ' || t_testid2.TEST_DESCRIPTION;
        PIPE ROW ( l_row ) ;    
        
        -- Check if this is a normal comparison or against the average values
        if t_testId2.TEST_ID like 'TEST_RESULT%' then
            l_row := 'Comparing Average values' ;
        else
            l_row := 'Start Time : ' || REPORT_ADM.Get_DTM(t_testid2.TEST_ID,'START')||' - End Time : ' || REPORT_ADM.Get_DTM(t_testid2.TEST_ID,'END');
        end if;    
        PIPE ROW ( l_row ) ;
     
        l_mode := 'Comparing ' || CASE (SUBSTR(t_testId1.TESTMODE,1,1)) when 'S' then 'a Standard'         when 'L' then 'a Extended'         when 'O' then 'a non-Standard' else 'an unclassified' END 
                  || ' '       || CASE (SUBSTR(t_testId1.TESTMODE,2,1)) when 'M' then 'Morning'            when 'E' then 'Evening'            when 'A' then 'Ad-hoc'         else 'undefined' END  
                  || ' test '  || CASE (SUBSTR(t_testId1.TESTMODE,3,1)) when 'N' then 'with Nightly Batch' when 'D' then 'with Daytime Batch' when 'X' then 'without batch'  else 'with unknown batch loads' END
                  || ' vs '    || CASE (SUBSTR(t_testId2.TESTMODE,1,1)) when 'S' then 'a Standard'         when 'L' then 'a Extended'         when 'O' then 'a non-Standard' else 'an unclassified' END
                  || ' '       || CASE (SUBSTR(t_testId2.TESTMODE,2,1)) when 'M' then 'Morning'            when 'E' then 'Evening'            when 'A' then 'Ad-hoc'         else 'undefined' END
                  || ' test '  || CASE (SUBSTR(t_testId2.TESTMODE,3,1)) when 'N' then 'with Nightly Batch' when 'D' then 'with Daytime Batch' when 'X' then 'without batch'  else 'with unknown batch loads' END
                  ;          
                  
        l_mode := 'h5. TEST CONDITIONS : '||l_mode  ;        
        PIPE ROW ( l_mode ) ;
    
    EXCEPTION
        WHEN testid_not_found THEN
            logger.write('Test ID not found : ' ||l_testid_not_found) ;
    END Get_test_info;

    
 
/*--------------------------------------------------------------------------------- */
    -- Get_PGA_growth 
    -- It does process only one testid at a time
/*--------------------------------------------------------------------------------- */       
    FUNCTION Get_PGA_growth (
            i_testId IN TEST_RESULT_MASTER.test_id%TYPE 
          ) RETURN g_tvc2 PIPELINED
    AS
       t_test    TEST_RESULT_MASTER%ROWTYPE ;
    
       l_row     varchar2(4000);
       i         number;
    BEGIN
       -- Set default parameters
       t_test := REPORT_GATHER.Get_Test_Details(i_testId) ;
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
         FROM TEST_RESULT_METRICS m
        WHERE m.test_id = t_test.TEST_ID
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
             PIPE ROW ( 'h5. Pga Growth for test : '|| t_test.TEST_DESCRIPTION ) ;
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
    
/*--------------------------------------------------------------------------------- */
    -- Get_RedoLogs_Usage
    -- The spelling of the metric definition has been corrected going forward
    -- Cross environment ready
/*--------------------------------------------------------------------------------- */       
    FUNCTION Get_RedoLogs_Usage (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          ) RETURN g_tvc2 PIPELINED
    AS
       t_test1   TEST_RESULT_MASTER%ROWTYPE ;
       t_test2   TEST_RESULT_MASTER%ROWTYPE ;
       l_row     varchar2(4000);
       i         number;
    BEGIN
       -- Set default parameters
       t_test1 := REPORT_GATHER.Get_Test_Details(i_testId1) ;
       t_test2 := REPORT_GATHER.Get_Test_Details(i_testId2) ;
       i := 0;
    
       PIPE ROW ('h5. Archived Redo Log Usage Comparison') ;
       PIPE ROW ('{csv:autoTotal=true|allowExport=true|columnTypes=s,s,f|id=arcRL}');
       PIPE ROW ('"Database","Current Gb","Previous Gb"');
    
       FOR r1 IN (
             with prev as (
                      select  m.database_name, ROUND(m.average) average, db.db_type
                        from  TEST_RESULT_METRICS m
                        join  TEST_RESULT_DBS db on ( db.db_name = m.database_name and db.db_env = t_test2.DB_ENV )
                       where  m.test_id = t_test2.TEST_ID
                         and  m.metric_name = 'Archived Redo Log' 
                    order by  db.db_type ) 
                 , cur as (
                       select  m.database_name, ROUND(m.average) average, db.db_type
                        from  TEST_RESULT_METRICS m
                        join  TEST_RESULT_DBS db on ( db.db_name = m.database_name and db.db_env = t_test1.DB_ENV )
                       where  m.test_id = t_test1.TEST_ID 
                         and  m.metric_name = 'Archived Redo Log' 
                    order by  db.db_type ) 
             select '"' || cur.database_name||' ('||cur.db_type||')'
                        || '","' || cur.average
                        || '","' || prev.average
                        || '"' as col1
               from cur
               join prev on ( cur.db_type = prev.db_type )
              order by cur.db_type )
       LOOP
          l_row := r1.col1;
          PIPE ROW (l_row);
       END LOOP;
       PIPE ROW ('{csv}');       
    
    END Get_RedoLogs_Usage ;
    
    
/*--------------------------------------------------------------------------------- */
    -- Get_sal_basket_sizes ( unchanged )
/*--------------------------------------------------------------------------------- */       
    FUNCTION Get_sal_basket_sizes (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE DEFAULT 'Latest test'
          ) RETURN g_tvc2 PIPELINED
    AS
       l_metric_name g_tvc2 ;
       l_idx VARCHAR2(100) ;
       l_testId1 TEST_RESULT_MASTER.test_id%TYPE ;
    
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
    
       FOR r1 IN ( SELECT DISTINCT lob_type AS lt FROM TEST_RESULT_SAL_BASKET WHERE test_id = l_testId1 ORDER BY 1 )
       LOOP
         l_row := 'h5. SAL Basket Sizes for ' || r1.lt;
         PIPE ROW ( l_row ) ;
         PIPE ROW ( '{csv:allowExport=true|columnTypes=s,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f,f|id=SalBasket' || r1.lt || '}' ) ;
         PIPE ROW ( '"Created","0-25kb","26-50kb","51-76kb","76-100kb","101-125","126-150kb","151-175kb","176-200kb","200-250kb","251-500kb","501-750kb","751-1000kb","1001-1500kb","1501-2000kb","2001-3000kb","3001-4000kb","4001-5000","5001-6000kb","6001kb +"');
         for r2 in (select to_char(created, 'dd/mm/yyyy hh24:mi:ss') as created, B0_25KB, B26_50KB, B51_76KB, B76_100KB, B101_125KB, B126_150KB, B151_175KB, B176_200KB, 
                           B200_250KB, B251_500KB, B501_750KB, B751_1000KB, B1001_1500KB, B1501_2000KB, B2001_3000KB, B3001_4000KB, 
                           B4001_5000KB, B5001_6000KB, B6001KB
                      from TEST_RESULT_SAL_BASKET 
                     where test_id = l_testId1
                       and lob_type = r1.lt
                    order by lob_type, created
         )
         LOOP
             l_row := '"' || r2.created || '","' || r2.B0_25KB || '","' || r2.B26_50KB || '","' || r2.B51_76KB || '","' || r2.B76_100KB || 
                      '","' || r2.B101_125KB || '","' || r2.B126_150KB || '","' || r2.B151_175KB || '","' || r2.B176_200KB || 
                      '","' || r2.B200_250KB || '","' || r2.B251_500KB || '","' || r2.B501_750KB || '","' || r2.B751_1000KB || 
                      '","' || r2.B1001_1500KB || '","' || r2.B1501_2000KB || '","' || r2.B2001_3000KB || '","' || r2.B3001_4000KB || 
                      '","' || r2.B4001_5000KB || '","' || r2.B5001_6000KB || '","' || r2.B6001KB || '"';
             PIPE ROW ( l_row ) ;
          END LOOP ;
          PIPE ROW ( '{csv}' ) ;      
       END LOOP ;
    END Get_sal_basket_sizes ;


/*--------------------------------------------------------------------------------- */
    -- GET_BILLING_RATE
    -- As we have a limit with Max DB Links open at once, we could not replicate the query
    -- using PL/SQL, instead we are capturing this data when we gather the data and the report
    -- will rely on the data that has been gathered.
/*--------------------------------------------------------------------------------- */          
    FUNCTION Get_billing_rate (
            i_testId  IN TEST_RESULT_MASTER.test_id%TYPE 
          ) RETURN g_tvc2 PIPELINED
    AS
        t_testId    TEST_RESULT_MASTER%ROWTYPE ;
        l_row       varchar2(4000) := null;
    
    BEGIN
        -- Set parameters
        t_testId  := REPORT_GATHER.Get_Test_Details(i_testId) ;
         
        SELECT distinct TEST_ID 
          INTO l_row
          FROM TEST_RESULT_BILLING_RUN
         WHERE TEST_ID = t_testId.TEST_ID;
        
        PIPE ROW ('h3. Billing Rate' ) ;
        PIPE ROW ('h5. Billing Rate for Test '|| t_testId.TEST_ID ) ;
        PIPE ROW ('{chart:type=Area|stacked=true|width=1200|height=400|dataOrientation=vertical|rangeAxisLowerBound=0}'  ) ;
        PIPE ROW (' || DATE_TIME || CUS01 || CUS02 || CUS03 || CUS04 || CUS05 || CUS06 ||') ;
        
        FOR r1 IN (
            WITH cur AS (
                select  to_char(BILLING_DATE,'dd-mm-yyyy hh24:mi') as date_time
                       ,CUS01, CUS02, CUS03, CUS04, CUS05, CUS06
                  from  TEST_RESULT_BILLING_RUN
                 where  TEST_ID = t_testId.TEST_ID 
                 order by  date_time
                        ) 
            SELECT '|'||cur.date_time||'|'||cur.CUS01||'|'||cur.CUS02||'|'||cur.CUS03||'|'||cur.CUS04||'|'||cur.CUS05||'|'||cur.CUS06||'|' as col1 
              FROM cur  )
        LOOP
            PIPE ROW ( r1.col1 ) ;
        END LOOP ;
              
        PIPE ROW ('{chart}') ;
    EXCEPTION 
        WHEN no_data_found THEN
            PIPE ROW ('h3. Billing Rate' ) ;
            PIPE ROW ('h5. Billing Rate for Test '|| t_testId.TEST_ID ) ;           
            PIPE ROW ('***  No Billing data found  ***') ;
    END Get_billing_rate ;


 
/*--------------------------------------------------------------------------------- */
    -- GET_CACHE_INFO
    -- This information is now collected and stored within the repository
    -- This function will read from whatever was collected at the time the info was gathered.
/*--------------------------------------------------------------------------------- */      
    FUNCTION Get_Cache_Info (
            i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
          ) RETURN g_tvc2 PIPELINED
    AS
       l_testId1 TEST_RESULT_MASTER.test_id%TYPE ;
       l_testId2 TEST_RESULT_MASTER.test_id%TYPE ;
       l_row                varchar2(4000);
    
        cursor c_cache ( p_testid in VARCHAR2 ) is
            SELECT * 
              FROM TEST_RESULT_CACHE
             WHERE test_id = p_testid;
    BEGIN
        -- parameters
        l_testId1 := i_testId1 ;
        l_testId2 := i_testId2 ; 
    
        for v_cache in c_cache (l_testId1)  
        loop
            PIPE ROW ( 'h5. Cache vs No Cache Info (Current Test)' ) ;
            PIPE ROW ('{csv:allowExport=true|columnTypes=s,f,f|id=CacheNoCache}') ;
            PIPE ROW ('"Query Type","Executions","Avg Elapsed (ms)"') ;
        
            l_row := '"' || v_cache.query_type || '","' 
                         || TRIM ( TO_CHAR ( v_cache.executions , '999,999,999,999,999,999,999,990' ) ) || '","'
                         || TRIM ( TO_CHAR ( v_cache.avg_time_ms , '999,999,999,999,999,999,999,990.000' ) ) || '"' ;
            PIPE ROW (l_row) ;
            PIPE ROW ( '{csv}' ) ; 
        end loop;
        
     
        for v_cache in c_cache (l_testId2)  
        loop
            PIPE ROW ( 'h5. Cache vs No Cache Info (Previous Test)' ) ;
            PIPE ROW ('{csv:allowExport=true|columnTypes=s,f,f|id=CacheNoCache}') ;
            PIPE ROW ('"Query Type","Executions","Avg Elapsed (ms)"') ;
        
            l_row := '"' || v_cache.query_type || '","' 
                         || TRIM ( TO_CHAR ( v_cache.executions , '999,999,999,999,999,999,999,990' ) ) || '","'
                         || TRIM ( TO_CHAR ( v_cache.avg_time_ms , '999,999,999,999,999,999,999,990.000' ) ) || '"' ;
            PIPE ROW (l_row) ;
            PIPE ROW ( '{csv}' ) ; 
        end loop;
        
    END Get_Cache_Info ;   

/*------------------------------------------------------------------------------------------------------------------------ */
    -- Get_ObjectDetails
    -- It retrieves the data from TEST_RESULT_OBJ_DETAILS
    -- This table contains the degree parallelsm that was set up when the data was collected
    -- alongside the TEST data collections
    -- The function can retrive the data filtered by other parameters
/*------------------------------------------------------------------------------------------------------------------------ */ 
    FUNCTION Get_ObjectDetails (
         i_testid       in TEST_RESULT_OBJ_DETAILS.TEST_ID%type         default NULL
        ,i_env          in TEST_RESULT_OBJ_DETAILS.DB_ENV%type          default 'ALL'
        ,i_dbname       in TEST_RESULT_OBJ_DETAILS.DATABASE_NAME%type   default NULL
        ,i_objtype      in TEST_RESULT_OBJ_DETAILS.OBJ_TYPE%type        default NULL
        ) RETURN g_tvc2 PIPELINED
    AS
        -- Local variables
        t_test          TEST_RESULT_MASTER%rowtype := null ;
        l_env           varchar2(4) ;
        l_dbname        varchar2(30) ;
        l_objtype       varchar2(30) ;
        l_row           varchar2(2000) ;
        l_created       date ;
        
        cursor c_det is
            select DB_ENV, DATABASE_NAME, CREATED, OBJ_OWNER, OBJ_NAME, OBJ_TYPE, OBJ_DEGREE
              from TEST_RESULT_OBJ_DETAILS
             where TEST_ID = DECODE(NVL(t_test.TEST_ID,'ALL'),'ALL',TEST_ID,t_test.TEST_ID)
               and CREATED >= DECODE(NVL(to_char(l_created),'ALL'),'ALL', CREATED, l_created )
               and DB_ENV = DECODE(NVL(l_env,'ALL'),'ALL',DB_ENV,l_env)
               and DATABASE_NAME  = DECODE(NVL(l_dbname,'ALL'),'ALL',DATABASE_NAME,l_dbname)
               and OBJ_TYPE = DECODE(NVL(l_objtype,'ALL'),'ALL',OBJ_TYPE,l_objtype)
             order by  CREATED desc, TEST_ID, DB_ENV, DATABASE_NAME, OBJ_OWNER, OBJ_TYPE, OBJ_NAME
            ;
    BEGIN
        -- Get parameters formatted
        l_env     := trim(upper(i_env)) ;
        l_dbname  := trim(upper(i_dbname)) ;
        l_objtype := trim(upper(i_objtype)) ;
 
        If i_testid is not null then
            t_test    := REPORT_GATHER.Get_TEST_Details (i_testid) ;
            l_created := null ;
        else
            select Max(CREATED) into l_created
              from TEST_RESULT_OBJ_DETAILS
             where DB_ENV = DECODE(NVL(l_env,'ALL'),'ALL',DB_ENV,l_env)
               and DATABASE_NAME  = DECODE(NVL(l_dbname,'ALL'),'ALL',DATABASE_NAME,l_dbname)
               and OBJ_TYPE = DECODE(NVL(l_objtype,'ALL'),'ALL',OBJ_TYPE,l_objtype)
             group by DB_ENV, DATABASE_NAME, OBJ_TYPE 
             fetch first 1 row only
             ;
        end if ;    

        l_row := 'h5. List of the parallelism set up for objects with a degree greater than 1' ;
        PIPE ROW ( l_row ) ;
        l_row := '||Env||Database||Date||Owner||Object||Object Type||Degree of Parallelism';
        PIPE ROW ( l_row ) ;
        For v_det in c_det 
        Loop
            l_row := '|' || v_det.DB_ENV || 
                     '|' || v_det.DATABASE_NAME ||
                     '|' || v_det.CREATED ||
                     '|' || v_det.OBJ_OWNER ||
                     '|' || v_det.OBJ_NAME ||
                     '|' || v_det.OBJ_TYPE ||
                     '|' || v_det.OBJ_DEGREE ;
            PIPE ROW ( l_row ) ;              
        End Loop;
    END Get_ObjectDetails;

/*------------------------------------------------------------------------------------------------------------------------ */
    -- Get_ObjectDetailsReport
    -- It retrieves the data from TEST_RESULT_OBJ_DETAILS compared against the oldest PRODUCTION captured data.
    -- Reports on the differences on parallelism and whether the parallelism is set to DEFAULT    
/*------------------------------------------------------------------------------------------------------------------------- */      
    FUNCTION Get_ObjectDetailsReport (
         i_testid       in TEST_RESULT_OBJ_DETAILS.TEST_ID%type         default NULL
        ,i_env          in TEST_RESULT_OBJ_DETAILS.DB_ENV%type          default 'ALL'
        ,i_dbname       in TEST_RESULT_OBJ_DETAILS.DATABASE_NAME%type   default NULL
        ,i_objtype      in TEST_RESULT_OBJ_DETAILS.OBJ_TYPE%type        default NULL
        ,i_mode         in varchar2                                     default 'FULL'
        ) RETURN g_tvc2 PIPELINED
    AS
        -- Local variables
        t_test          TEST_RESULT_MASTER%rowtype := null ;
        l_env           varchar2(4) ;
        l_dbname        varchar2(30) ;
        l_objtype       varchar2(30) ;
        l_row           varchar2(2000) ;
        l_created       date ;
        l_mode          varchar2(4) ; -- Values are : FULL (reports all), DIFF (reports different values), SAME (reports equal values)  -- All of them greater than 0/1 
        
        cursor c_det (p_mode varchar2) is
              select nft.DB_ENV DB_ENV
                   , nft.DATABASE_NAME DATABASE_NAME
                   , db.DB_TYPE
                   , max(nft.CREATED) CREATED
                   , nft.OBJ_OWNER OBJ_OWNER
                   , nft.OBJ_NAME OBJ_NAME
                   , nft.OBJ_TYPE OBJ_TYPE
                   , max(nft.OBJ_DEGREE) NFT_DEGREE
                   , max(prd.OBJ_DEGREE) PRD_DEGREE
              from TEST_RESULT_OBJ_DETAILS nft
              join V_TEST_RESULT_DBS db ON ( db.DB_NAME = nft.DATABASE_NAME )   
              join ( select distinct p.DB_ENV, p.DATABASE_NAME, d.DB_TYPE, p.CREATED, p.OBJ_OWNER, p.OBJ_NAME, p.OBJ_TYPE, p.OBJ_DEGREE
                       from TEST_RESULT_OBJ_DETAILS p, V_TEST_RESULT_DBS d
                      where p.DB_ENV = 'PRD'
                        and trunc(p.CREATED) = ( select trunc(MAX(CREATED)) from TEST_RESULT_OBJ_DETAILS where DB_ENV = 'PRD' group by DB_ENV )
                        and d.DB_NAME = p.DATABASE_NAME
                    ) prd ON ( prd.DB_TYPE = db.DB_TYPE 
                           and prd.OBJ_OWNER = nft.OBJ_OWNER
                           and prd.OBJ_NAME = nft.OBJ_NAME
                           and prd.OBJ_TYPE = nft.OBJ_TYPE )
             where nft.TEST_ID = DECODE(NVL(t_test.TEST_ID,'ALL'),'ALL',nft.TEST_ID,t_test.TEST_ID)
               and nft.CREATED >= DECODE(NVL(to_char(l_created),'ALL'),'ALL', nft.CREATED, l_created )
               and nft.DB_ENV = DECODE(NVL(l_env,'ALL'),'ALL',nft.DB_ENV,l_env)
               and nft.DATABASE_NAME  = DECODE(NVL(l_dbname,'ALL'),'ALL',nft.DATABASE_NAME,l_dbname)
               and nft.OBJ_TYPE = DECODE(NVL(l_objtype,'ALL'),'ALL',nft.OBJ_TYPE,l_objtype)
               and ( CASE WHEN ( p_mode = 'FULL' ) THEN 1
                          WHEN ( p_mode = 'DIFF' and nft.OBJ_DEGREE != prd.OBJ_DEGREE ) THEN 1
                          WHEN ( p_mode = 'SAME' and nft.OBJ_DEGREE = prd.OBJ_DEGREE ) THEN 1
                          ELSE 0 END ) = 1
             group by nft.DB_ENV, nft.DATABASE_NAME, db.DB_TYPE, nft.OBJ_OWNER, nft.OBJ_NAME, nft.OBJ_TYPE 
             order by CREATED desc, DB_ENV, DATABASE_NAME, OBJ_OWNER, OBJ_TYPE, OBJ_NAME
            ;
    BEGIN
        -- Get parameters formatted
        l_env     := trim(upper(i_env)) ;
        l_dbname  := trim(upper(i_dbname)) ;
        l_objtype := trim(upper(i_objtype)) ;
        l_mode    := trim(Upper(i_mode)) ;
        
        -- Force any awkward value supply to be the default
        if l_mode not in ('FULL','SAME','DIFF') then l_mode := 'FULL'; end if;
        
        -- If the TESTID is not supplied, then filter by the other parameters for results
        -- This allows for searchs by environment and not by a particluar test
        -- any other condition will default to pick the latest results available
        If i_testid is not null then
            t_test    := REPORT_GATHER.Get_TEST_Details (i_testid) ;
            l_created := null ;
        else
            select Max(CREATED) into l_created
              from TEST_RESULT_OBJ_DETAILS
             where DB_ENV = DECODE(NVL(l_env,'ALL'),'ALL',DB_ENV,l_env)
               and DATABASE_NAME  = DECODE(NVL(l_dbname,'ALL'),'ALL',DATABASE_NAME,l_dbname)
               and OBJ_TYPE = DECODE(NVL(l_objtype,'ALL'),'ALL',OBJ_TYPE,l_objtype)
             group by DB_ENV, DATABASE_NAME, OBJ_TYPE 
             fetch first 1 row only
             ;
        end if ;      
    
        PIPE ROW (' ') ;
        if l_mode = 'DIFF' then
            l_row := 'h5. Objects that DIFFER from Production on their parallel degree settings (>1)' ;
        elsif l_mode = 'SAME' then    
            l_row := 'h5. Objects that EQUAL with Production on their parallel degree settings (>1)' ;
        else
            l_row := 'h5. FULL list of objects parallel degree settings (>1)' ;
        end if ;
        PIPE ROW ( l_row ) ;
        l_row := '||Env||Database||Date||Owner||Object||Object Type||Degree of Parallelism NFT||Degree of Parallelism PRD';
        PIPE ROW ( l_row ) ;
        For v_det in c_det ( l_mode )
        Loop
            l_row := '|' || v_det.DB_ENV || 
                     '|' || v_det.DATABASE_NAME ||
                     '|' || v_det.CREATED ||
                     '|' || v_det.OBJ_OWNER ||
                     '|' || v_det.OBJ_NAME ||
                     '|' || v_det.OBJ_TYPE ||
                     '|' || v_det.NFT_DEGREE ||
                     '|' || v_det.PRD_DEGREE ;
            PIPE ROW ( l_row ) ;              
        End Loop;
    END Get_ObjectDetailsReport;



/*****************************************************************************************************/
-- Do_load_object_parallelism_data
-- Loads data into TEST_RESULT_OBJ_DETAILS that reads from the Production data sent via a CVS file
-- The CSV file is read thru an EXTERNAL TABLE : external_objects_parallelism
-- Location of the CSV file :  /share/dbnth/VOLUME_REFRESH/KEEP_INFO_NFT/N02/DATA/PROD_Object_Parallelism_Info.cvs
/*****************************************************************************************************/
    PROCEDURE Do_load_object_parallelism_data  
    IS
        l_created       date ;
    BEGIN
        -- Update base table with Production data 
        -- Base table : TEST_RESULT_OBJ_DETAILS
        select max(created) into l_created
          from TEST_RESULT_OBJ_DETAILS
         where db_env = 'PRD' ;

        if l_created < trunc(sysdate) then
            insert into TEST_RESULT_OBJ_DETAILS
                ( TEST_ID
                 ,DB_ENV
                 ,DATABASE_NAME
                 ,CREATED
                 ,OBJ_OWNER
                 ,OBJ_NAME
                 ,OBJ_TYPE
                 ,OBJ_DEGREE
                )
            select  'PROD_EXTRACT_'||to_char(sysdate,'ddMONyy') 
                   ,'PRD'
                   ,DATABASE_NAME
                   ,to_date(EXTRACT_DATE,'YYYY-MM-DD HH24:MI:SS')
                   ,OBJECT_OWNER
                   ,OBJECT_NAME
                   ,OBJECT_TYPE
                   ,REGEXP_REPLACE(OBJECT_DEGREE_VALUE, '[[:cntrl:]]', '') 
              from external_objects_parallelism
            ;
            commit;

        end if ;
        
    END Do_load_object_parallelism_data ;


    
    -- #############################  DB Summary Procedures/Functions ##########################################
/*--------------------------------------------------------------------------------- */
    -- DO_TRUNCATE TABLES
    -- Truncate the temporary tables used to display the data : 
    -- TEST_RESULT_SQL_RESULTS
    -- TEST_RESULT_SQL_SUMMARY
/*--------------------------------------------------------------------------------- */       
    PROCEDURE Do_TruncateTables AS
    BEGIN
        execute immediate 'truncate table TEST_RESULT_SQL_RESULTS' ; 
        execute immediate 'truncate table TEST_RESULT_SQL_SUMMARY' ; 
    END Do_TruncateTables; 
    

/*--------------------------------------------------------------------------------- */
    -- Get_PropertyData
    -- Populates TEST_RESULT_SQL_RESULTS and TEST_RESULT_SQL_SUMMARY for the properties : 
    -- ELAPSED_TIME_PER_EXEC_SECONDS
    -- CPU_TIME_PER_EXEC_SECONDS
    -- TPS
/*--------------------------------------------------------------------------------- */    
    PROCEDURE Get_PropertyData (
             i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
           , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
           , i_property IN VARCHAR2
           , i_dev      IN NUMBER    DEFAULT 0.35
           , i_days     IN NUMBER    DEFAULT 30
           , i_ratio    IN NUMBER    DEFAULT 2
           , i_except   IN VARCHAR2  DEFAULT NULL 
           , i_topsql   IN NUMBER    DEFAULT NULL
           ) as
    
       l_testId1     TEST_RESULT_MASTER.test_id%TYPE ;
       l_testId2     TEST_RESULT_MASTER.test_id%TYPE ;
       l_begintest   varchar2(25);          -- Date the comparison Test starts in date format 
       l_endtest     varchar2(25);          -- Date the comparison Test ends in date format ( we add one minute to include the last date )
       l_endtest2    varchar2(25);          -- End Date - 1 for the cals
    
       -- PARAMETERS
       l_property    varchar2(35);    -- Property being evaluated
       l_dev         number;          -- deviation degree ( default 0.35 )
       l_days        number;          -- Number of days considered to extract a trend ( defalut would be 30 )
       l_ratio       number;          -- Minimum difference Ratio to qualify al alert as RED
       l_except      varchar2(200);   -- Type of MODULEs to be excluded from the result ( APP, DB or NULL (all) )
       l_topsql      number ;         -- Restricts the number of SQL returns evaluated by TOP_SQL_NUMBER column 
       l_query       varchar2(16000); -- SQL Query to be run

       -- Labels ( they do change for TPS )
       l_label1      varchar2(12);
       l_label2      varchar2(12);
       l_label3      varchar2(12);
       l_label4      varchar2(12);
       l_label5      varchar2(12);
       l_prefix      varchar2(3);
       l_cond1       varchar2(4000);

       property_not_found EXCEPTION;
       
    BEGIN
       -- initialised variables
       l_testId1  := i_testId1 ;
       l_testId2  := i_testId2 ;
       l_property := i_property;
       l_dev      := i_dev;
       l_days     := i_days;
       l_ratio    := i_ratio;
       l_except   := i_except;
       l_topsql   := nvl(i_topsql,999999);

       -- Initialise variables depending on the Property
       CASE WHEN l_property = 'TPS' THEN
           l_prefix := 'TPS' ;
           l_label1 := 'NEW' ;
           l_label2 := 'IMPROVED' ;
           l_label3 := 'MORE' ;
           l_label4 := 'DEGRADED' ;
           l_label5 := 'LESS' ;
       WHEN l_property = 'ELAPSED_TIME_PER_EXEC_SECONDS' THEN
           l_prefix := 'ELA' ;
           l_label1 := 'NEW' ;
           l_label2 := 'DEGRADED' ;
           l_label3 := 'SLOWER' ;
           l_label4 := 'IMPROVED' ;
           l_label5 := 'FASTER' ;       
       WHEN l_property = 'CPU_TIME_PER_EXEC_SECONDS' THEN
           l_prefix := 'CPU' ;
           l_label1 := 'NEW' ;
           l_label2 := 'DEGRADED' ;
           l_label3 := 'SLOWER' ;
           l_label4 := 'IMPROVED' ;
           l_label5 := 'FASTER' ;
       ELSE
           raise property_not_found ;
       END CASE; 
    
       -- Create clause statements based on conditions
       CASE 
            WHEN l_except = 'APP' THEN
                l_cond1 := q'# and ( Upper(module) not in ('SQL DEVELOPER'
                                                    ,'DBMS_SCHEDULER'
                                                    ,'SYS'
                                                    ,'SYSTEM'
                                                    ,'HP_DIAG'
                                                    ,'EMAGENT_SQL_ORACLE_DATABASE'
                                                    ,'SKYUTILS'
                                                    ,'MMON_SLAVE'
                                                    ,'BACKUP ARCHIVELOG'
                                                    ,'HORUS_MONITORING'
                                                    ,'DBSNMP'
                                                    ,'DBMON_AGENT_USER')
                               and Upper(module) not like 'RMAN%'
                               and module not like 'Oracle Enterprise Manager%' 
                               )#' ;
            WHEN l_except = 'DB' THEN
                l_cond1 := q'# and ( Upper(module) in ('SQL DEVELOPER'
                                                    ,'DBMS_SCHEDULER'
                                                    ,'SYS'
                                                    ,'SYSTEM'
                                                    ,'HP_DIAG'
                                                    ,'EMAGENT_SQL_ORACLE_DATABASE'
                                                    ,'SKYUTILS'
                                                    ,'MMON_SLAVE'
                                                    ,'BACKUP ARCHIVELOG'
                                                    ,'HORUS_MONITORING'
                                                    ,'DBSNMP'
                                                    ,'DBMON_AGENT_USER') 
                                  or Upper(module) like 'RMAN%'
                                  or module like 'Oracle Enterprise Manager%' 
                              )#' ;
       ELSE
            l_cond1 := '' ;
       END CASE ;

       -- The Lowest date of both dates suppplied ( minus the number of days supplied )
       l_begintest := to_char(to_date(REPORT_ADM.Get_DTM(l_testId1,'START'),'ddMONyy-hh24:mi')-l_days,'ddMONyy-hh24:mi');
       -- The highest date of both dates supplied
       l_endtest := to_char(to_date(REPORT_ADM.Get_DTM(l_testId1,'END'),'ddMONyy-hh24:mi')+(1/1440*5),'ddMONyy-hh24:mi');
       -- The higgest date minues one day for the calculations 
       l_endtest2 := to_char(to_date(REPORT_ADM.Get_DTM(l_testId1,'END'),'ddMONyy-hh24:mi')+(1/1440*5)-1,'ddMONyy-hh24:mi');
       
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
          l_query := q'# 
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
                                   , #' || l_property ||q'#
                              from TEST_RESULT_SQL
                             where DATABASE_NAME = '#' || r1.DATABASE_NAME || q'#'
                               and ( TEST_ID in ('#' || l_testid1 ||q'#', '#' || l_testid2 ||q'#') or ( BEGIN_TIME >= to_date('#' || l_begintest || q'#','DDMONYY-hh24:mi') and END_TIME <= to_date('#' || l_endtest || q'#','DDMONYY-hh24:mi')))
                               #' || l_cond1 || q'#
                               and #' || l_property ||q'# is not NULL
                               and TOP_SQL_NUMBER <= #' || l_topsql ||q'#
                             order by TEST_ID, SQL_ID )
              , CURR as ( select f.SQL_ID 
                                 ,f.MODULE
                                 ,( CASE WHEN ROUND(f.#' || l_property ||q'# ,10)=0 THEN NULL 
                                         ELSE ROUND(f.#' || l_property ||q'# ,10) END ) as PropValue
                            from FILTER f
                           where f.DATABASE_NAME = '#' || r1.DATABASE_NAME || q'#'
                             and f.TEST_ID = '#' || l_testid1 ||q'#'
                           order by SQL_ID )
              , PREV as ( select f.SQL_ID 
                                ,f.MODULE
                                 ,( CASE WHEN ROUND(f.#' || l_property ||q'# ,10)=0 THEN NULL 
                                         ELSE ROUND(f.#' || l_property ||q'# ,10) END ) as PropValue
                            from FILTER f
                           where f.DATABASE_NAME = '#' || r1.DATABASE_NAME || q'#'
                             and f.TEST_ID = '#' || l_testid2 ||q'#'
                           order by f.SQL_ID )
              , AVER as ( select distinct f.SQL_ID
                                ,ROUND(Median(f.#' || l_property ||q'# ) over (partition by f.SQL_ID),10) as Med
                                ,ROUND(Stddev(f.#' || l_property ||q'# ) over (partition by f.SQL_ID),10) as dev
                                ,GREATEST(ROUND(Median(f.#' || l_property ||q'# ) over (partition by f.SQL_ID) - #' || l_dev ||q'# *Stddev(f.#' || l_property ||q'# ) over (partition by f.SQL_ID),10),0.0000000001) as Low
                                ,ROUND(Median(f.#' || l_property ||q'# ) over (partition by f.SQL_ID) + #' || l_dev ||q'# *Stddev(f.#' || l_property ||q'# ) over (partition by f.SQL_ID),10) as High
                            from FILTER f
                           where f.DATABASE_NAME = '#' || r1.DATABASE_NAME || q'#'
                             and f.BEGIN_TIME BETWEEN to_date('#' || l_begintest || q'#','DDMONYY-hh24:mi') AND to_date('#' || l_endtest2 || q'#','DDMONYY-hh24:mi')
                           order by f.SQL_ID )
               , MAXS as ( select f.SQL_ID
                                  ,ROUND(MAX(f.#' || l_property ||q'# ),10) as Max
                                  ,ROUND(MIN(f.#' || l_property ||q'# ),10) as Min
                             from FILTER f
                            where f.DATABASE_NAME = '#' || r1.DATABASE_NAME || q'#'
                             and f.BEGIN_TIME BETWEEN to_date('#' || l_begintest || q'#','DDMONYY-hh24:mi') AND to_date('#' || l_endtest2 || q'#','DDMONYY-hh24:mi')
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
             select '#' || r1.DATABASE_NAME || q'#' as database_name
                   ,c.SQL_ID
                   ,'#' || l_property ||q'#' as property
                   ,c.MODULE
                   ,c.PropValue as cur_PropValue
                   ,p.PropValue as pre_PropValue
                   ,NVL(a.Med,0) as Med
                   ,NVL(a.dev,0) as dev
                   ,NVL(a.Low,0) as Low
                   ,NVL(m.Min,0) as Min
                   ,NVL(a.High,0) as High
                   ,NVL(m.Max,0) as Max
                   ,CASE WHEN a.High is NULL or a.Low is NULL                   THEN '#' || l_label1 || q'#' 
                         WHEN c.PropValue >= a.High and r.ratio >= #' || l_ratio ||q'#      THEN '#' || l_label2 || q'#'  
                         WHEN c.PropValue >= a.High                             THEN '#' || l_label3 || q'#' 
                         WHEN c.PropValue <= a.Low  and ABS(r.ratio) >= #' || l_ratio ||q'# THEN '#' || l_label4 || q'#' 
                         WHEN c.PropValue <= a.Low                              THEN '#' || l_label5 || q'#' 
                         ELSE '-' END as cur_assess
                   ,CASE WHEN p.PropValue is NULL                               THEN '#' || l_label1 || q'#'  
                         WHEN p.PropValue >= a.High and r.ratio >= #' || l_ratio ||q'#      THEN '#' || l_label2 || q'#'  
                         WHEN p.PropValue >= a.High                             THEN '#' || l_label3 || q'#'  
                         WHEN p.PropValue <= a.Low  and ABS(r.ratio) >= #' || l_ratio ||q'# THEN '#' || l_label4 || q'#' 
                         WHEN p.PropValue <= a.Low                              THEN '#' || l_label5 || q'#' 
                         ELSE '-' END as pre_assess
                   ,CASE WHEN p.PropValue is NULL                                                        THEN '#' || l_label1 || q'#' 
                         WHEN ( c.PropValue/p.PropValue ) > 1 and ( c.PropValue/p.PropValue ) >= #' || l_ratio ||q'# THEN '#' || l_label2 || q'#' 
                         WHEN ( c.PropValue/p.PropValue ) > 1                                            THEN '#' || l_label3 || q'#' 
                         WHEN ( c.PropValue/p.PropValue ) < 1 and ( p.PropValue/c.PropValue ) >= #' || l_ratio ||q'# THEN '#' || l_label4 || q'#' 
                         WHEN ( c.PropValue/p.PropValue ) < 1                                            THEN '#' || l_label5 || q'#'
                         ELSE '-' END as comp_assess                 
                    ,r.ratio
            from CURR c
            LEFT OUTER JOIN PREV p on p.SQL_ID = c.SQL_ID
            LEFT OUTER JOIN AVER a on a.SQL_ID = c.SQL_ID
            LEFT OUTER JOIN MAXS m on m.SQL_ID = c.SQL_ID
            LEFT OUTER JOIN RATIO r on r.SQL_ID = c.SQL_ID
           ) s  #'
           ;
           
           REPORT_GATHER.exec_query (i_query => l_query);  
           
           -- Merge summary info into the summary temp table
           l_query := q'# 
           MERGE into TEST_RESULT_SQL_SUMMARY s
           USING ( select tr.DATABASE_NAME, tr.SQL_ID, tr.MODULE, tr.CUR_STATUS, tr.COMP_STATUS, tr.CUR_VAL, tr.DIFF_RATIO
                     from TEST_RESULT_SQL_RESULTS tr
                    where tr.PROPERTY = '#' || l_property ||q'#' 
                  ) r  ON (s.DATABASE_NAME = r.DATABASE_NAME and s.SQL_ID = r.SQL_ID)
           WHEN MATCHED THEN
               update SET s.#' || l_prefix ||q'#_ST_TST = r.COMP_STATUS
                         ,s.#' || l_prefix ||q'#_ST_GEN = r.CUR_STATUS
                         ,s.#' || l_prefix ||q'#_VAL    = r.CUR_VAL
                         ,s.#' || l_prefix ||q'#_RATIO  = r.DIFF_RATIO
           WHEN NOT MATCHED THEN
              insert ( s.DATABASE_NAME, s.SQL_ID, s.MODULE, s.ELA_ST_TST, s.ELA_ST_GEN, s.ELA_VAL, s.ELA_RATIO )
              values ( r.DATABASE_NAME, r.SQL_ID, r.MODULE, r.COMP_STATUS, r.CUR_STATUS, r.CUR_VAL, r.DIFF_RATIO ) #'
           ;

           REPORT_GATHER.exec_query (i_query => l_query);  
    
           COMMIT;
           
        END LOOP ;
    EXCEPTION
        WHEN property_not_found THEN
            logger.write('Get_PropertyData - Property ' || l_property || ' not found ');
        WHEN OTHERS THEN
            logger.write('Get_PropertyData : ' ||SQLCODE||' : '||SUBSTR(SQLERRM, 1, 500)) ;  
    END Get_PropertyData;


    
/*--------------------------------------------------------------------------------- */
    -- Do_FillTheGaps
    -- Once all data has been updated into TEST_RESULT_SQL_RESULTS there'll be some NULLS 
    -- for those Properties that did not fall into the selected categories. 
    -- For completeness, we will retrieve those status and add them in.
    -- This can only be run when the whole TEST_RESULT_SQL_RESULTS has already been populated.
/*--------------------------------------------------------------------------------- */       
    PROCEDURE Do_FillTheGaps 
    AS 
    BEGIN
   
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
    
    
/*--------------------------------------------------------------------------------- */
    -- Do_AnalyticalData
    -- Calls the PROCEDURES that will harvest the data for the DB analysis
/*--------------------------------------------------------------------------------- */       
    PROCEDURE Do_AnalyticalData (
             i_testId1 IN TEST_RESULT_MASTER.test_id%TYPE 
           , i_testId2 IN TEST_RESULT_MASTER.test_id%TYPE 
           , i_dev      IN NUMBER    DEFAULT 0.35
           , i_days     IN NUMBER    DEFAULT 30
           , i_ratio    IN NUMBER    DEFAULT 2
           , i_except   IN VARCHAR2  DEFAULT NULL        
           , i_topsql   IN NUMBER    DEFAULT NULL
           ) as
    
       l_testId1 TEST_RESULT_MASTER.test_id%TYPE ;
       l_testId2 TEST_RESULT_MASTER.test_id%TYPE ;
    
       -- Parameters driving the QUERIES
       l_dev         number;        -- deviation degree ( default 0.35 )
       l_days        number;        -- Number of days considered to extract a trend ( defalut would be 30 )
       l_ratio       number;        -- Minimum difference Ratio to qualify al alert as RED
       l_except      varchar2(5);   -- Type of MODULEs to be excluded from the result ( APP, DB or NULL (all) )
       l_topsql      number;        -- Restricts the number of SQL returns evaluated by TOP_SQL_NUMBER column 
    
    BEGIN
       l_testId1 := i_testId1 ;
       l_testId2 := i_testId2 ;
    
       -- PARAMETERS
       l_dev    := i_dev ;
       l_days   := i_days ;
       l_ratio  := i_ratio ;
       l_except := i_except ;
       l_topsql := i_topsql ;
    
       --Do_CreateTmpTables ;
       Do_TruncateTables ;
       Get_PropertyData(l_testId1,l_testId2,'ELAPSED_TIME_PER_EXEC_SECONDS',l_dev,l_days,l_ratio,l_except,l_topsql);
       Get_PropertyData(l_testId1,l_testId2,'CPU_TIME_PER_EXEC_SECONDS',l_dev,l_days,l_ratio,l_except,l_topsql);
       Get_PropertyData(l_testId1,l_testId2,'TPS',l_dev,l_days,l_ratio,l_except,l_topsql);
       Do_FillTheGaps ;
    
    END Do_AnalyticalData ;
    
/*--------------------------------------------------------------------------------- */
    -- Get_DB_Details
    -- Returns the contentfrom TEST_RESULT_SQL_RESULTS that meets the criteria passed 
    -- as parameters for the TESTIDs being evaluated 
/*--------------------------------------------------------------------------------- */       
    FUNCTION Get_DB_Details (
            i_testId1  IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2  IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_metric   IN varchar2 
          , i_filter   IN varchar2 DEFAULT NULL
          ) RETURN g_tvc2 PIPELINED
    AS
       l_testId1 TEST_RESULT_MASTER.test_id%TYPE ;
       l_testId2 TEST_RESULT_MASTER.test_id%TYPE ;
       l_Filter  varchar2(10) ;
       l_metric  varchar2(30) ;
       l_row     varchar2(4000);
       l_title   varchar2(30) ;
       no_metric    EXCEPTION ;
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
       ELSE 
           raise no_metric ;
       END IF ;
    
       IF l_metric <> 'N/A' THEN
       PIPE ROW ( 'h3. Database Status Report by '||l_metric ) ;
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
    EXCEPTION
        WHEN no_metric THEN
            logger.write('Get_DB_Details - No METRIC supplied' );
    END Get_DB_Details ;
    
/*--------------------------------------------------------------------------------- */
    -- GET_DB_SUMMARY
    -- This is the procedure that will generate the data to be displayed in Confluence
/*--------------------------------------------------------------------------------- */       
    FUNCTION Get_DB_Summary (
            i_testId1      IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_testId2      IN TEST_RESULT_MASTER.test_id%TYPE 
          , i_filter       IN varchar2 DEFAULT NULL
          , i_ratio        IN number default 2
          , i_summary      IN varchar2 default NULL
          ) RETURN g_tvc2 PIPELINED
    AS
       l_testId1 TEST_RESULT_MASTER.test_id%TYPE ;
       l_testId2 TEST_RESULT_MASTER.test_id%TYPE ;
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

END REPORT_DATA;
/
