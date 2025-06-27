CREATE OR REPLACE PACKAGE REPORT_GATHER
AS

    -- Where the HP~_DIAG repository is based on 
    g_repo  varchar2(20) := 'TCC021N';

    FUNCTION Get_DB_Ver ( i_dbname IN varchar2 ) RETURN varchar2 ;

    PROCEDURE exec_cursor ( 
            i_query  IN varchar2 
           ,i_dbname IN varchar2 default NULL 
           ,i_cursor OUT SYS_REFCURSOR 
        );
        
    PROCEDURE exec_query ( 
            i_query  IN varchar2 
           ,i_dbname IN varchar2 default NULL 
        ); 

    FUNCTION Get_Test_Details(
             i_testId IN test_result_master.test_id%TYPE 
       ) RETURN TEST_RESULT_MASTER%ROWTYPE;
    
    FUNCTION Get_SnapId (
             i_testId IN test_result_master.test_id%TYPE
            ,i_dbname IN varchar2 default NULL
            ,i_mode   IN varchar2 default 'START' 
        ) RETURN number;

    PROCEDURE Get_TEST_DATA (
             i_start  IN test_result_master.begin_time%TYPE
            ,i_end    IN test_result_master.end_time%TYPE
            ,i_desc   IN test_result_master.test_description%TYPE   
            ,i_label  IN test_result_master.best_test_for_release%TYPE DEFAULT NULL
            ,i_flag   IN test_result_master.daily_run_flag%TYPE DEFAULT 0
            ,i_env    IN test_result_master.db_env%TYPE DEFAULT NULL
            ,i_group  IN test_result_master.db_group%TYPE DEFAULT 'FULL'
            ,i_dbname IN varchar2 DEFAULT NULL
            ,i_retain IN test_result_master.retention%TYPE DEFAULT 'N'
            ,i_valid  IN test_result_master.validtest%TYPE DEFAULT 'N'
            ,i_mode   IN test_result_master.testmode%TYPE DEFAULT NULL            
       ) ;

    PROCEDURE Get_TEST_RESULT_BILLING_RUN (
            i_test   IN test_result_master%ROWTYPE
       );

    PROCEDURE Get_TEST_RESULT_SQLDETAILS (
            i_test   IN test_result_master%ROWTYPE
           ,i_dbname IN varchar2 DEFAULT NULL
       );
       
    PROCEDURE Get_TEST_RESULT_CACHE (
            i_test   IN test_result_master%ROWTYPE 
          );

    PROCEDURE Get_DATA_POOL_ROWCOUNTS;

    PROCEDURE Get_DATA_POOL_LOOPS;

    PROCEDURE Housekeep_DATA_POOL_Tables;

END REPORT_GATHER;
/


CREATE OR REPLACE PACKAGE BODY REPORT_GATHER
AS
/* ################################################################################ */
/* ################################################################################ */
/* Internal utilities to the package */
/* ################################################################################ */
/* ################################################################################ */

    -- Where the HP~_DIAG repository is based on 
    --g_repo  varchar2(20) := 'TCC021N';



/*--------------------------------------------------------------------------------- */
    -- Close DBLink  
/*--------------------------------------------------------------------------------- */
    PROCEDURE close_dblink ( i_link   IN varchar2 )
    AS
        l_dbname    varchar2(20);
        -- define error handling exceptions
        dblink_not_open EXCEPTION;
        PRAGMA EXCEPTION_INIT(dblink_not_open, -2081);
    BEGIN
        -- If the link supplied has the "@", remove it
        if i_link != '' then
            l_dbname := SUBSTR(i_link,INSTR(i_link,'@')+1,length(i_link));
            commit ;
            dbms_session.close_database_link(l_dbname) ; 
          --execute immediate 'alter session close database link '||l_dbname ;
        end if ;  
    EXCEPTION
        WHEN dblink_not_open THEN  
            logger.write('close_dblink - '||l_dbname|| ' Link : '|| i_link || ' : database link is not open');
        WHEN OTHERS THEN
            logger.write('close_dblink - '||l_dbname|| ' Link : '|| i_link || ' : ' ||SQLCODE||' : '||SUBSTR(SQLERRM,1,500));
    END close_dblink ;        


/*--------------------------------------------------------------------------------- */
    -- Execute query via dynamic SQL returning a cursor
    -- Returns an open cursor
    -- COMMIT and CLOSE DBLINK should be handled by the calling procedure
/*--------------------------------------------------------------------------------- */ 
    PROCEDURE exec_cursor ( 
            i_query  IN varchar2 
           ,i_dbname IN varchar2 default NULL 
           ,i_cursor OUT SYS_REFCURSOR 
        ) 
    AS
        -- define error handling exceptions
        dblink_not_working EXCEPTION;
        PRAGMA EXCEPTION_INIT(dblink_not_working, -2019);
    BEGIN 
        open i_cursor for i_query; 
    EXCEPTION
        WHEN dblink_not_working THEN
            logger.write('exec_cursor - connection description for remote database not found for : '||i_dbname ) ;
            rollback;
            close i_cursor ;
        WHEN OTHERS THEN
            logger.write('exec_cursor : '|| SQLCODE ||' : '|| SUBSTR(SQLERRM,1,500) || ' DB : ' || i_dbname|| ' : ' || ' Query : ' || SUBSTR(i_query,1,3400));
            rollback;
            close i_cursor ;
    END exec_cursor; 
/*--------------------------------------------------------------------------------- */
     -- Execute query via dynamic sql. No return
     -- COMMIT and CLOSE DBLINK is handled within this procedure
/*--------------------------------------------------------------------------------- */ 
    PROCEDURE exec_query ( 
            i_query  IN varchar2 
           ,i_dbname IN varchar2 default NULL 
        ) 
    AS
        l_dbname    varchar2(25);
        l_query     varchar2(16000);

        -- define error handling exceptions
        dblink_not_working EXCEPTION;
        no_query EXCEPTION;
        PRAGMA EXCEPTION_INIT(dblink_not_working, -2019);
    BEGIN 
        l_dbname := i_dbname ;
        -- If no query received, do not execurte
        If i_query is null then
            raise no_query;            
        end if ;
        l_query := i_query ;
        -- If the dbname supplied has the "@", remove it
        if l_dbname is not NULL then
             l_dbname := SUBSTR(l_dbname,INSTR(l_dbname,'@')+1,length(l_dbname));
        end if ; 
        execute immediate l_query; 
        commit;
        close_dblink(l_dbname);
    EXCEPTION
        WHEN no_query THEN
            logger.write('exec_query - No query supplied');          
        WHEN dblink_not_working THEN
            logger.write('exec_query - connection description for remote database not found for : '||l_dbname ) ;
            rollback;
            close_dblink(l_dbname);
        WHEN OTHERS THEN
            logger.write('exec_query : '|| SQLCODE ||' : '|| SUBSTR(SQLERRM,1,500) || ' DB : ' || i_dbname|| ' : ' || ' Query : ' || SUBSTR(l_query,1,3400));
            --logger.debug('exec_query : '|| SQLCODE ||' : '|| SUBSTR(SQLERRM,1,500) || ' DB : ' || i_dbname|| ' : ' || ' Query : ' || SUBSTR(l_query,1,3400));
            rollback;
            close_dblink(l_dbname);
    END exec_query; 


/*--------------------------------------------------------------------------------- */
     -- Execute DDL query via dynamic sql. No return
     -- COMMIT and CLOSE DBLINK is handled within this procedure
/*--------------------------------------------------------------------------------- */ 
    PROCEDURE exec_ddl_query ( 
            i_query  IN varchar2 
           ,i_dbname IN varchar2 default NULL 
        ) 
    AS
        l_dbname    varchar2(25);

        -- define error handling exceptions
        dblink_not_working EXCEPTION;
        PRAGMA EXCEPTION_INIT(dblink_not_working, -2019);

        table_exist EXCEPTION;
        PRAGMA EXCEPTION_INIT(table_exist, -955);

    BEGIN 
        l_dbname := i_dbname ;
        -- If the dbname supplied has the "@", remove it
        if l_dbname is not NULL then
             l_dbname := SUBSTR(l_dbname,INSTR(l_dbname,'@')+1,length(l_dbname));
        end if ; 
        execute immediate 'BEGIN DBMS_UTILITY.EXEC_DDL_STATEMENT@'||l_dbname||'(:stmt); END;' using i_query; 
        commit;
        close_dblink(l_dbname);

    EXCEPTION
        WHEN dblink_not_working THEN
            logger.write('exec_ddl_query - connection description for remote database not found for : '||l_dbname|| ' - ' || SUBSTR(i_query,1,3400));
            rollback;
            close_dblink(l_dbname);
        WHEN table_exist THEN
            -- This is a expected error if the table already exist and nothing should happen
            --logger.write('exec_ddl_query - ( expected ) Table exist for '||l_dbname||' - '||i_query);
            rollback;
            close_dblink(l_dbname);           
        WHEN OTHERS THEN
            logger.write('exec_ddl_query : '|| SQLCODE ||' : '|| SUBSTR(SQLERRM,1,500) || ' DB : ' || i_dbname|| ' : ' || ' Query : ' || SUBSTR(i_query,1,3400));
            rollback;
            close_dblink(l_dbname);
    END exec_ddl_query; 


/*--------------------------------------------------------------------------------- */
    -- Gets the DB Version   
    -- Required for some queries which differ depending on the DB version
/*--------------------------------------------------------------------------------- */
    FUNCTION Get_DB_Ver ( i_dbname IN varchar2 ) RETURN varchar2
    AS
        l_ver        varchar2(10);
        l_dbname     varchar2(20);
        l_link       varchar2(20);
        l_query      varchar2(4000);
        v_cursor    sys_refcursor;    

    BEGIN
        -- Initialise vars
        l_dbname := i_dbname;
        -- Exception when the database is where the repository lives or no DBNAME has been provided 
        if i_dbname = g_repo or i_dbname is NULL then l_link := ''; else l_link := '@'||i_dbname ; end if ; 

        l_query  := 'select SUBSTR(banner,INSTR(banner,''Oracle Database'')+16,3) as version from v$version'||l_link||' where rownum = 1' ;

        -- Call the procedure to resolve the SQL
        exec_cursor (i_query => l_query, i_dbname => l_dbname, i_cursor => v_cursor) ;  
        if v_cursor%ISOPEN then
            loop
                fetch v_cursor into l_ver;
                exit when v_cursor%notfound;
            end loop;    
            close v_cursor;
            commit;
            close_dblink(l_link);
        end if;    
        return l_ver ;
    END Get_DB_Ver;


/*--------------------------------------------------------------------------------- */
    -- Get the MINIMUM or MAXIMUM SNAP_ID to the date supplied
/*--------------------------------------------------------------------------------- */    
    FUNCTION Get_SnapId (
             i_testId IN test_result_master.test_id%TYPE
            ,i_dbname IN varchar2 default NULL
            ,i_mode   IN varchar2 default 'START' 
        ) RETURN number
    AS
        l_testId    test_result_master.test_id%TYPE ;
        l_dbname    varchar2(20);
        l_mode      varchar2(10);
        v_cursor    sys_refcursor ;

        -- Local variables
        l_query     varchar2(4000) ; -- Dynamic SQL
        l_link      varchar2(25) ;   -- DB Link to be used as required
        l_dtm       varchar2(30);    -- Date to be passed within the query to return the correct SNAPID ( derived from the TESTID )
        l_snapid    number;          -- snapID returned

    BEGIN
        -- Get parameters initialised
        l_testid := i_testId; 
        l_mode   := i_mode ;
        l_dbname := i_dbname ;
        l_snapid := null ;

        -- Exception when the database is where the repository lives or no DBNAME has been provided 
        if l_dbname = g_repo or l_dbname is NULL then l_link := ''; else l_link := '@'||l_dbname ; end if ;   
        -- Get the dates from the TEST_ID ( START or END )
        l_dtm := REPORT_ADM.Get_DTM(l_testId,l_mode);
        -- Dynamic SQL to be executed
        if l_mode = 'START' then 
            -- Return the previous SNAPID to get reports from the begining of the testing period.
            l_query := 'SELECT MIN ( s.snap_id ) - 1'||
                       ' FROM dba_hist_snapshot'||l_link||' s'||
                       ' JOIN v$database'||l_link||' d ON d.dbid = s.dbid'||
                       ' WHERE s.begin_interval_time >= TO_TIMESTAMP ( '''||l_dtm||''' , ''DDMONYY-HH24:MI'' )' ;
        else
            l_query := 'SELECT MAX ( s.snap_id )'||
                       ' FROM dba_hist_snapshot'||l_link||' s'||
                       ' JOIN v$database'||l_link||' d ON d.dbid = s.dbid'||
                       ' WHERE s.begin_interval_time <= TO_TIMESTAMP ( '''||l_dtm||''' , ''DDMONYY-HH24:MI'' )' ;
        end if;
        -- Call the procedure to resolve the SQL
        exec_cursor (i_query => l_query, i_dbname => l_dbname, i_cursor => v_cursor) ;  
        -- If something goes wrong resolving the query, the error will be trapped within the above procedure and the cursor will be closed.
        -- If so, do nothing so that it doesn't stop further processing
        if v_cursor%ISOPEN then
            loop
                fetch v_cursor into l_snapid;
                exit when v_cursor%notfound;
            end loop;    
            close v_cursor;
            commit;
            close_dblink(l_link);
        end if ;
        return l_snapid;
    END Get_SnapId;




/*--------------------------------------------------------------------------------- */
     -- Retrieves the DBID for a given database
/*--------------------------------------------------------------------------------- */
    FUNCTION Get_DBID ( i_dbname IN varchar2 DEFAULT NULL 
        ) RETURN number
    AS
        l_link      varchar2(20);
        l_dbid      number ;
        l_dbname    varchar2(20);
        l_query     varchar2(4000);
        v_cursor    sys_refcursor ;        
    BEGIN
        -- Initialise parameters
        l_dbname := i_dbname ;
        -- Exception when the database is where the repository lives or no DBNAME has been provided 
        if l_dbname = g_repo or l_dbname is NULL then l_link := ''; else l_link := '@'||l_dbname ; end if ;   
        -- Query to retrieve the DBID
        l_query := 'select dbid from v$database'||l_link ;
        -- Call the procedure to resolve the SQL
        exec_cursor (i_query => l_query, i_dbname => l_dbname, i_cursor => v_cursor) ; 
        if v_cursor%ISOPEN then
            loop
                fetch v_cursor into l_dbid;
                exit when v_cursor%notfound;
            end loop;    
            close v_cursor;
            commit;
            close_dblink(l_link);
        end if ;        
        return l_dbid;
    EXCEPTION    
        WHEN OTHERS THEN
            logger.write('Get_DBID : '|| SQLCODE ||' : '|| SUBSTR(SQLERRM,1,500));        
    END Get_DBID;


/*--------------------------------------------------------------------------------- */
    -- Takes the TEST_ID format (DDMONYY-HH24MI_DDMONYY-HH24MI) and returns the date in the following format : DDMONYY-HH24:MI
/*--------------------------------------------------------------------------------- */   
    FUNCTION Get_Test_Details(
             i_testId IN test_result_master.test_id%TYPE 
       ) RETURN TEST_RESULT_MASTER%ROWTYPE
    AS
        t_test      test_result_master%ROWTYPE := null;
    BEGIN
        -- Get the details to return
        select * into t_test
        from    TEST_RESULT_MASTER
        where   test_id = i_testid ;
        return t_test ;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            return t_test ;
        WHEN OTHERS THEN
            logger.write('Check TESTID : ' ||SQLCODE||' : '||SUBSTR(SQLERRM,1,500)) ;  
    END Get_Test_Details;



/*--------------------------------------------------------------------------------- */
  -- Writes the TEST information into TEST_RESULT_MASTER for the TEST results being gathered
/*--------------------------------------------------------------------------------- */   
    PROCEDURE Get_TEST_RESULT_MASTER (
            i_test   IN test_result_master%ROWTYPE
       ) AS
        l_query             varchar2(4000);
    BEGIN
        INSERT INTO TEST_RESULT_MASTER
            ( TEST_ID
             ,DB_ENV
             ,DB_GROUP
             ,TEST_DESCRIPTION
             ,BEGIN_TIME
             ,END_TIME
             ,BEST_TEST_FOR_RELEASE
             ,DAILY_RUN_FLAG
             ,RETENTION
             ,VALIDTEST
             ,TESTMODE
            )
        VALUES ( i_test.TEST_ID
                ,i_test.DB_ENV
                ,i_test.DB_GROUP
                ,i_test.TEST_DESCRIPTION
                ,i_test.BEGIN_TIME
                ,i_test.END_TIME
                ,i_test.BEST_TEST_FOR_RELEASE
                ,i_test.DAILY_RUN_FLAG
                ,i_test.RETENTION
                ,i_test.VALIDTEST
                ,i_test.TESTMODE
               );                
        COMMIT;
    END Get_TEST_RESULT_MASTER ;


/*--------------------------------------------------------------------------------- */
  -- Writes data extracted from : dba_hist_snapshot, dba_hist_sqlstat
/*--------------------------------------------------------------------------------- */      

    PROCEDURE Get_TEST_RESULT_SQL (
            i_test   IN test_result_master%ROWTYPE
           ,i_dbname IN varchar2 DEFAULT NULL
       ) AS
        t_test              TEST_RESULT_MASTER%ROWTYPE;
        l_start_snapid      number ;
        l_end_snapid        number ;
        l_dbid              number ;
        l_dbname            varchar2(20);
        l_link              varchar2(25);
        l_query             varchar2(4000);

        -- Exceptions
        No_SnapId_Found     EXCEPTION;

    BEGIN
        -- Get parameters initialised
        l_dbname := i_dbname ;
        t_test   := i_test ;

        -- Exception when the database is where the repository lives or no DBNAME has been provided 
        if l_dbname = g_repo or l_dbname is NULL then l_link := ''; else l_link := '@'||l_dbname ; end if ;      

        -- Get SNAP IDs for the time period to be extracted
        l_start_snapid := Get_SNAPID (i_testid => t_test.TEST_ID, i_dbname => l_dbname, i_mode => 'START')+1;
        l_end_snapid   := Get_SNAPID (i_testid => t_test.TEST_ID, i_dbname => l_dbname, i_mode => 'END'); 
        l_dbid := Get_DBID(l_dbname) ;

        if l_start_snapid is NULL or l_end_snapid is null then RAISE No_SnapId_Found; end if;

        -- If the start and end snapshots are the same or consecutive, set the start snapid to something meaningful
        if l_end_snapid <= l_start_snapid then
            l_start_snapid := l_end_snapid-1;
        end if;    
        
        -- Get SQL 
        l_query := q'#insert into TEST_RESULT_SQL i (
                    i.test_id
		            , i.test_description
		            , i.database_name
		            , i.begin_snap
		            , i.end_snap
		            , i.begin_time
		            , i.end_time
		            , i.top_sql_number
		            , i.elapsed_time_seconds
		            , i.executions
		            , i.buffer_gets
		            , i.cpu_time_seconds
		            , i.sql_id
		            , i.plan_hash_values
		            , i.module
		            , i.rows_processed
		            )
	            SELECT '#' || t_test.TEST_ID || q'#'
		            , '#' || t_test.TEST_DESCRIPTION || q'#'
		            , '#' || l_dbname || q'#'
		            , sub.begin_snap
		            , sub.end_snap
		            , sub.begin_time
		            , sub.end_time
		            , ROWNUM AS top_sql_number
		            , sub.elapsed_time_seconds
		            , sub.executions
		            , sub.buffer_gets
		            , sub.cpu_time_seconds
		            , sub.sql_id
		            , sub.plan_hash_values
		            , sub.module
		            , sub.rows_processed
	            FROM ( -- sub to sort before rownum
			            SELECT q.sql_id
				            , CASE WHEN MIN ( q.plan_hash_value ) = MAX ( q.plan_hash_value)
						            THEN TO_CHAR ( MIN ( q.plan_hash_value ) )
						            ELSE TO_CHAR ( MIN ( q.plan_hash_value ) ) || ' ' || TO_CHAR ( MAX ( q.plan_hash_value ) ) END AS plan_hash_values                              
				            , ROUND(SUM(q.elapsed_time_delta)/1000000,5) AS elapsed_time_seconds
				            , ROUND(SUM(q.executions_delta),5) AS executions
				            , ROUND(SUM(q.buffer_gets_delta),5) AS buffer_gets
				            , ROUND(SUM(q.cpu_time_delta)/1000000,5) AS cpu_time_seconds
				            , MIN(s.snap_id) AS begin_snap
				            , MAX(s.snap_id) AS end_snap
				            , MIN(s.begin_interval_time) AS begin_time
				            , MAX(s.end_interval_time) AS end_time
				            , MAX( CASE WHEN q.module IS NULL THEN LOWER ( q.parsing_schema_name )
							            WHEN q.module IN ( 'JDBC Thin Client' , 'perl.exe' , 'SQL*Plus' ) THEN LOWER ( q.parsing_schema_name )
							            WHEN q.module LIKE 'sqlplus%' THEN LOWER ( q.parsing_schema_name )
							            WHEN q.module LIKE 'oracle@%' THEN LOWER ( q.parsing_schema_name )
							            ELSE q.module
						            END ) AS module
				            , SUM(q.rows_processed_delta) AS rows_processed
			            FROM dba_hist_snapshot#' || l_link || q'# s JOIN dba_hist_sqlstat#' || l_link || q'# q ON s.snap_id = q.snap_id AND s.dbid = q.dbid
			            WHERE s.snap_id BETWEEN #' || l_start_snapid || q'# AND #' || l_end_snapid || q'#
			            AND s.dbid = #' || l_dbid || q'#
			            GROUP BY q.sql_id
			            ORDER BY SUM ( q.elapsed_time_delta ) / 1000000 DESC
		            ) sub
	            WHERE ROWNUM <= TO_NUMBER ( 100 )#' 
               ;

        exec_query (i_query => l_query, i_dbname => l_dbname);  

    EXCEPTION
        WHEN No_Snapid_Found THEN
            logger.write('Get_TEST_RESULT_SQL - No SNAPIDs found for database '||l_dbname) ;

    END Get_TEST_RESULT_SQL ;

/*--------------------------------------------------------------------------------- */
  -- Writes data extracted from dba_hist_sqltext
/*--------------------------------------------------------------------------------- */ 
    PROCEDURE Get_TEST_RESULT_SQLTEXT (
            i_test   IN test_result_master%ROWTYPE
           ,i_dbname IN varchar2 DEFAULT NULL
       ) AS
        t_test              TEST_RESULT_MASTER%ROWTYPE;
        l_dbname            varchar2(20);
        l_link              varchar2(25);
        l_query             varchar2(4000);

    BEGIN
        -- Get parameters initialised
        l_dbname := i_dbname ;
        t_test   := i_test ;

        -- Exception when the database is where the repository lives or no DBNAME has been provided 
        if l_dbname = g_repo or l_dbname is NULL then l_link := ''; else l_link := '@'||l_dbname ; end if ;      

        -- Get SQL 
        l_query := q'#BEGIN
                        FOR r1 IN (
                            SELECT DISTINCT a.sql_id, a.module
                                FROM TEST_RESULT_SQL a
                                JOIN dba_hist_sqltext#' || l_link || q'# b ON b.sql_id = a.sql_id
                            WHERE NOT EXISTS ( SELECT NULL FROM TEST_RESULT_SQLTEXT ne WHERE ne.sql_id = a.sql_id )
                                AND a.database_name = '#' || l_dbname || q'#'
                                AND a.test_id = '#' || t_test.TEST_ID || q'#'
                                AND b.sql_text IS NOT NULL
                            ORDER BY a.sql_id
                        )
                        LOOP
                            INSERT INTO TEST_RESULT_SQLTEXT t ( t.sql_id , t.sql_text, t.module )
                            SELECT s.sql_id , s.sql_text, r1.module
                                FROM dba_hist_sqltext#' || l_link || q'# s
                            WHERE s.sql_id = r1.sql_id
                                AND ROWNUM <= 1
                            ;
                        END LOOP ;
                    END ;#'
        ;
        exec_query (i_query => l_query, i_dbname => l_dbname);  

    END Get_TEST_RESULT_SQLTEXT ;


/*--------------------------------------------------------------------------------- */
  -- Writes data extracted from : dba_hist_snapshot, dba_hist_sqlstat
  -- This particular one will trap the whole sequence of data snapshot by snapshot
  -- for the top 25 SQL statements that run between the given date/times
  -- This query should collect the necessary information to build a graph over time for any SQLID collected
/*--------------------------------------------------------------------------------- */      

    PROCEDURE Get_TEST_RESULT_SQLDETAILS (
            i_test   IN test_result_master%ROWTYPE
           ,i_dbname IN varchar2 DEFAULT NULL
       ) AS
        t_test              TEST_RESULT_MASTER%ROWTYPE;
        l_start_snapid      number ;
        l_end_snapid        number ;
        l_dbid              number ;
        l_dbname            varchar2(20);
        l_link              varchar2(25);
        l_query             varchar2(4000);

        -- Exceptions
        No_SnapId_Found     EXCEPTION;

    BEGIN
        -- Get parameters initialised
        l_dbname := i_dbname ;
        t_test   := i_test ;

        -- Exception when the database is where the repository lives or no DBNAME has been provided 
        if l_dbname = g_repo or l_dbname is NULL then l_link := ''; else l_link := '@'||l_dbname ; end if ;      

        -- Get SNAP IDs for the time period to be extracted
        l_start_snapid := Get_SNAPID (i_testid => t_test.TEST_ID, i_dbname => l_dbname, i_mode => 'START')+1;
        l_end_snapid   := Get_SNAPID (i_testid => t_test.TEST_ID, i_dbname => l_dbname, i_mode => 'END');        
        l_dbid := Get_DBID(l_dbname) ;

        if l_start_snapid is NULL or l_end_snapid is null then RAISE No_SnapId_Found; end if;

        -- If the start and end snapshots are the same or consecutive, set the start snapid to something meaningful
        if l_end_snapid <= l_start_snapid then
            l_start_snapid := l_end_snapid-1;
        end if;    
        
        -- Get SQL 
        l_query := q'#INSERT into TEST_RESULT_SQLDETAILS i (
                              i.test_id
                            , i.test_description
                            , i.database_name
                            , i.snapid
                            , i.begin_time
                            , i.end_time
                            , i.elapsed_time_seconds
                            , i.executions
                            , i.buffer_gets
                            , i.cpu_time_seconds
                            , i.sql_id
                            , i.plan_hash_value
                            , i.module
                            , i.rows_processed
                            )
                    WITH top as 
                        ( SELECT  x.SQL_ID 
                            FROM  ( SELECT b.SQL_ID  
                                      FROM dba_hist_sqlstat#' || l_link ||q'# b 
                                     WHERE b.snap_id BETWEEN #' || l_start_snapid || q'# AND #' || l_end_snapid || q'#
                                       AND b.dbid = #' || l_dbid || q'#
                                       AND ( b.module not in ('emagent_SQL_oracle_database','rman@unora2w0 (TNS V1-V3)')
                                         AND  lower(b.parsing_schema_name) not in ('dataprov','hp_diag','dbsnmp','dbmon_agent_user','skyutils','mgw_gateway','perfmon') )
                                     GROUP BY b.sql_id, b.module --,lower(b.parsing_schema_name) 
                                     ORDER BY SUM(b.elapsed_time_delta) DESC
                                   ) x
                            WHERE ROWNUM <= 25    
                         )     
                    SELECT '#' || t_test.TEST_ID || q'#'
                         , '#' || t_test.TEST_DESCRIPTION || q'#'
                         , '#' || l_dbname || q'#'
                         , s.snap_id AS snapid
                         , s.begin_interval_time AS begin_time
                         , s.end_interval_time AS end_time
                         , ROUND(q.elapsed_time_delta/1000000,5) AS elapsed_time_seconds
                         , ROUND(q.executions_delta,5) AS executions
                         , ROUND(q.buffer_gets_delta,5) AS buffer_gets
                         , ROUND(q.cpu_time_delta/1000000,5) AS cpu_time_seconds
                         , q.sql_id
                         , q.plan_hash_value                               
                         , ( CASE WHEN q.module IS NULL THEN LOWER ( q.parsing_schema_name )||' - No Module'
                                     WHEN q.module IN ( 'JDBC Thin Client' , 'perl.exe' , 'SQL*Plus' ) THEN LOWER ( q.parsing_schema_name )||' - '||q.module
                                     WHEN q.module LIKE 'sqlplus%' THEN LOWER ( q.parsing_schema_name )||' - '||q.module
                                     WHEN q.module LIKE 'oracle@%' THEN LOWER ( q.parsing_schema_name )||' - '||q.module
                                     ELSE q.module
                                 END ) AS module
                         , q.rows_processed_delta AS rows_processed
                    FROM dba_hist_snapshot#' || l_link ||q'# s 
                    JOIN dba_hist_sqlstat#' || l_link ||q'# q ON s.snap_id = q.snap_id AND s.dbid = q.dbid
                    JOIN top t ON t.sql_id = q.sql_id
                    WHERE s.snap_id BETWEEN #' || l_start_snapid || q'# AND #' || l_end_snapid || q'#
                    AND s.dbid = #' || l_dbid || q'#
                    ORDER BY q.sql_id, begin_time asc#' 
               ;

        exec_query (i_query => l_query, i_dbname => l_dbname);  

    EXCEPTION
        WHEN No_Snapid_Found THEN
            logger.write('Get_TEST_RESULT_SQLDETAILS - No SNAPIDs found for database '||l_dbname) ;
    END Get_TEST_RESULT_SQLDETAILS ;


/*--------------------------------------------------------------------------------- */
    -- get data from tables : dba_hist_sysmetric_summary
    -- Raplaced table dba_hist_sysmetric_summary with DBA_HIST_CON_SYSMETRIC_SUMM as 
    -- pluggable database 19c and above have stopped access to that dict table for 
    -- the SMP* databases only 
/*--------------------------------------------------------------------------------- */ 
    PROCEDURE Get_TEST_RESULT_METRICS (
            i_test   IN test_result_master%ROWTYPE
           ,i_dbname IN varchar2 DEFAULT NULL
       ) AS
        t_test              TEST_RESULT_MASTER%ROWTYPE;
        l_start_snapid      number ;
        l_end_snapid        number ;
        l_start_dt          varchar2(20);
        l_end_dt            varchar2(20);
        l_dbid              number ;
        l_dbname            varchar2(20);
        l_link              varchar2(25);
        l_query             varchar2(4000);
        l_table             varchar2 (35) ;
        l_ver               number;
        l_cdb               varchar2(10);

        -- Exceptions
        No_SnapId_Found     EXCEPTION;

    BEGIN
        -- Get parameters initialised
        l_dbname := i_dbname ;
        t_test   := i_test ;

        -- Exception when the database is where the repository lives or no DBNAME has been provided 
        if l_dbname = g_repo or l_dbname is NULL then l_link := ''; else l_link := '@'||l_dbname ; end if ;     

        --we fetch DB version only the numeric part
        --execute immediate 'SELECT REGEXP_REPLACE( SUBSTR(banner, INSTR(banner, ''Oracle Database'') + 16, 3), ''[^0-9]'', '''') FROM v$version'|| l_link ||' WHERE ROWNUM = 1' INTO l_ver;
        -- It doesn't need to be a call outside the exting procedure, just a transformation of the data, that way the error handling still works
        l_ver := REGEXP_REPLACE ( Get_DB_Ver ( l_dbname ) , '[^0-9]', '' ); 

        if l_ver > 11 then --container DBs were introduced from 12.1 version
            EXECUTE IMMEDIATE 'select cdb from v$database'||l_link INTO  l_cdb; --value used for checking if db is container database or not.
            --we determine if this is a container database or not. 
            case when l_cdb='YES' then 
                l_table := 'DBA_HIST_CON_SYSMETRIC_SUMM' ;
            else
                l_table := 'DBA_HIST_SYSMETRIC_SUMMARY';
            end case; 
        else 
            l_table := 'DBA_HIST_SYSMETRIC_SUMMARY';
        end if;

        -- Get SNAP IDs for the time period to be extracted
        l_start_snapid := Get_SNAPID (i_testid => t_test.TEST_ID, i_dbname => l_dbname, i_mode => 'START')+1;
        l_end_snapid   := Get_SNAPID (i_testid => t_test.TEST_ID, i_dbname => l_dbname, i_mode => 'END');        
        l_dbid         := Get_DBID(l_dbname) ;
        l_start_dt     := REPORT_ADM.Get_DTM(i_testid => t_test.TEST_ID, i_mode => 'START') ;
        l_end_dt       := REPORT_ADM.Get_DTM(i_testid => t_test.TEST_ID, i_mode => 'END') ;

        if l_start_snapid is NULL or l_end_snapid is null then RAISE No_SnapId_Found; end if;

        -- If the start and end snapshots are the same or consecutive, set the start snapid to something meaningful
        if l_end_snapid <= l_start_snapid then
            l_start_snapid := l_end_snapid-1;
        end if;    

        -- Change the table to be accessed based on the databas name
        --if SUBSTR(l_dbname,1,3) = 'SMP' then
            --l_table := 'DBA_HIST_CON_SYSMETRIC_SUMM' ;
        --else
            --l_table := 'DBA_HIST_SYSMETRIC_SUMMARY';
        --end if;

        -- In 19c (and for any other versions starting with 12.2), for PDBs, it is expected to see no rows from DBA_HIST_SYSMETRIC_* among other system views.
        -- PDB level Metric information can be found using the DBA_HIST_CON_SYSMETRIC_SUMM view.
        -- replacing dba_hist_sysmetric_summary with DBA_HIST_CON_SYSMETRIC_SUMM 

        -- Get SQL 
        l_query := q'#INSERT INTO test_result_metrics (
                    test_id
                    , test_description
                    , database_name
                    , begin_snap
                    , end_snap
                    , begin_time
                    , end_time
                    , metric_name
                    , average
                    , begin_average
                    , end_average
                    , min_average
                    , max_average
                    , metric_unit
                    )
                 SELECT '#' || t_test.TEST_ID || q'#'
                    ,'#' || t_test.TEST_DESCRIPTION || q'#'
                    ,'#' || l_dbname || q'#'
                    , MIN(s.snap_id)    -- begin_snap
                    , MAX(s.snap_id)    -- end_snap
                    , MIN(s.begin_time) -- begin_time
                    , MAX(s.end_time)   -- end_time
                    , s.metric_name
                    , ROUND(AVG(s.average),5)  -- average
                    , ROUND(MAX(s.average) KEEP ( DENSE_RANK FIRST ORDER BY s.snap_id ASC ),5)   -- begin_average
                    , ROUND(MAX(s.average) KEEP ( DENSE_RANK FIRST ORDER BY s.snap_id DESC ),5)  -- end_average
                    , ROUND(MIN(s.average),5)  -- min_average
                    , ROUND(MAX(s.average),5)  -- max_average
                    , s.metric_unit
                 FROM #' || l_table || l_link || q'# s   
                 WHERE metric_name IN (
                     'Average Active Sessions' --1
                    --, 'Background CPU Usage Per Sec'
                    --, 'Buffer Cache Hit Ratio'
                    --, 'CPU Usage Per Sec'
                    , 'Current OS Load'
                    , 'Disk Sort Per Sec'
                    , 'Enqueue Deadlocks Per Sec'
                    , 'Full Index Scans Per Sec'
                    , 'Hard Parse Count Per Sec'
                    , 'Host CPU Utilization (%)'  --2
                    , 'Logons Per Sec'
                    , 'Long Table Scans Per Sec'
                    , 'Physical Read Total Bytes Per Sec'
                    , 'Run Queue Per Sec'  -- this metric is available in 12c and above only.
                    --, 'Total Table Scans Per Sec'
                    , 'User Commits Per Sec'
                    , 'Total PGA Allocated'  --3
                    , 'SQL Service Response Time'
                    , 'Total Table Scans Per Sec'
                    , 'Buffer Cache Hit Ratio'
                    , 'Database Wait Time Ratio'
                    , 'Memory Sorts Ratio'
                    , 'Temp Space Used'
                    )
                 AND s.snap_id BETWEEN #' || l_start_snapid || q'# AND #' || l_end_snapid || q'#
                 AND s.dbid = #' || l_dbid || q'#
                 GROUP BY s.metric_name , s.metric_unit
                 ORDER BY s.metric_name#'
                ;

        exec_query (i_query => l_query, i_dbname => l_dbname);  

        l_query := q'#INSERT INTO test_result_metrics (
                     test_id
                   , test_description
                   , database_name
                   , begin_snap
                   , end_snap
                   , begin_time
                   , end_time
                   , metric_name
                   , average
                   , begin_average
                   , end_average
                   , min_average
                   , max_average
                   , metric_unit
                   )
                WITH a AS (
                SELECT MIN(al.completion_time) AS begin_time
                    , MAX(al.completion_time) AS end_time 
                    , SUM(al.blocks * al.block_size) / 1024 / 1024 / 1024 AS average
                    FROM gv$archived_log#' || l_link || q'# al
                    WHERE al.dest_id = 1
                    AND al.completion_time BETWEEN TO_DATE ( '#' || l_start_dt || q'#','DDMONYY-HH24:MI') AND TO_DATE ('#' || l_end_dt || q'#','DDMONYY-HH24:MI')
                )
                SELECT '#' || t_test.TEST_ID || q'#'
                    ,'#' || t_test.TEST_DESCRIPTION || q'#'
                    ,'#' || l_dbname || q'#'
                    , #' || l_start_snapid || q'# AS begin_snap
                    , #' || l_end_snapid || q'# AS end_snap
                    , NVL(a.begin_time,TO_DATE('#' || l_start_dt || q'#','DDMONYY-HH24:MI')) as begin_time
                    , NVL(a.end_time,TO_DATE('#' || l_end_dt || q'#','DDMONYY-HH24:MI')) as end_time
                    , 'Archived Redo Log' AS metric_name
                    , ROUND(a.average,5) as average
                    , NULL AS begin_average
                    , NULL AS end_average
                    , NULL AS min_average
                    , NULL AS max_average
                    , 'gb' AS metric_unit
                FROM a#'
        ;

        exec_query (i_query => l_query, i_dbname => l_dbname);  

    EXCEPTION
        WHEN No_Snapid_Found THEN
            logger.write('Get_TEST_RESULT_METRICS - No SNAPIDs found for database '||l_dbname) ;
    END Get_TEST_RESULT_METRICS ;


/*--------------------------------------------------------------------------------- */
  -- Get data from tables : dba_hist_sysmetric_summary
/*--------------------------------------------------------------------------------- */ 
    PROCEDURE Get_TEST_RESULT_METRICS_DET (
            i_test   IN test_result_master%ROWTYPE
           ,i_dbname IN varchar2 DEFAULT NULL
       ) AS
        t_test              TEST_RESULT_MASTER%ROWTYPE;
        l_start_snapid      number ;
        l_end_snapid        number ;
        l_dbid              number ;
        l_dbname            varchar2(20);
        l_link              varchar2(25);
        l_query             varchar2(4000);       
        l_table             varchar2 (35) ;
        l_ver               number;
        l_cdb               varchar2(10);

        -- Exceptions
        No_SnapId_Found     EXCEPTION;

    BEGIN
        -- Get parameters initialised
        l_dbname := i_dbname ;
        t_test   := i_test ;

        -- Exception when the database is where the repository lives or no DBNAME has been provided 
        if l_dbname = g_repo or l_dbname is NULL then l_link := ''; else l_link := '@'||l_dbname ; end if ;  

        --we fetch DB version only the numeric part
        --execute immediate 'SELECT REGEXP_REPLACE( SUBSTR(banner, INSTR(banner, ''Oracle Database'') + 16, 3), ''[^0-9]'', '''') FROM v$version'|| l_link ||' WHERE ROWNUM = 1' INTO l_ver;
        -- It doesn't need to be a call outside the exting procedure, just a transformation of the data, that way the error handling still works
        l_ver := REGEXP_REPLACE ( Get_DB_Ver ( l_dbname ) , '[^0-9]', '' ); 
        
        if l_ver > 11 then --container DBs were introduced from 12.1 version
            EXECUTE IMMEDIATE 'select cdb from v$database'||l_link INTO  l_cdb; --value used for checking if db is container database or not.
            --we determine if this is a container database or not. 
            case when l_cdb='YES' then 
                l_table := 'DBA_HIST_CON_SYSMETRIC_SUMM' ;
            else
                l_table := 'DBA_HIST_SYSMETRIC_SUMMARY';
            end case; 
        else 
            l_table := 'DBA_HIST_SYSMETRIC_SUMMARY';
        end if;    

        -- Get SNAP IDs for the time period to be extracted
        l_start_snapid := Get_SNAPID (i_testid => t_test.TEST_ID, i_dbname => l_dbname, i_mode => 'START')+1;
        l_end_snapid   := Get_SNAPID (i_testid => t_test.TEST_ID, i_dbname => l_dbname, i_mode => 'END');        
        l_dbid         := Get_DBID(l_dbname) ;

        if l_start_snapid is NULL or l_end_snapid is null then RAISE No_SnapId_Found; end if;

        -- If the start and end snapshots are the same or consecutive, set the start snapid to something meaningful
        if l_end_snapid <= l_start_snapid then
            l_start_snapid := l_end_snapid-1;
        end if;    
        
         -- Change the table to be accessed based on the databas name
        --if SUBSTR(l_dbname,1,3) = 'SMP' then
            --l_table := 'DBA_HIST_CON_SYSMETRIC_SUMM' ;
        --else
            --l_table := 'DBA_HIST_SYSMETRIC_SUMMARY';
        --end if;

        -- In 19c (and for any other versions starting with 12.2), for PDBs, it is expected to see no rows from DBA_HIST_SYSMETRIC_* among other system views.
        -- PDB level Metric information can be found using the DBA_HIST_CON_SYSMETRIC_SUMM view.
        -- replacing dba_hist_sysmetric_summary with DBA_HIST_CON_SYSMETRIC_SUMM 

        l_query := q'#INSERT INTO test_result_metrics_detail i (
                     i.test_id
                   , i.test_description
                   , i.database_name
                   , i.snap_id
                   , i.begin_time
                   , i.end_time
                   , i.metric_name
                   , i.average
                   , i.metric_unit
                   , i.minval
                   , i.maxval   
                   )
                    SELECT '#' || t_test.TEST_ID || q'#'
                        ,'#' || t_test.TEST_DESCRIPTION || q'#'
                        ,'#' || l_dbname || q'#'
                        , s.snap_id
                        , s.begin_time
                        , s.end_time
                        , s.metric_name
                        , ROUND(s.average,5)
                        , s.metric_unit
                        , ROUND(s.minval,5)
                        , ROUND(s.maxval,5)
                    FROM #' || l_table || l_link || q'# s
                    WHERE s.metric_name IN (
                        'Redo Allocation Hit Ratio'
                        ,'Buffer Cache Hit Ratio'
                        ,'Host CPU Utilization (%)'
                        ,'PGA Cache Hit %'
                        ,'Database CPU Time Ratio'
                        ,'Cursor Cache Hit Ratio'
                        ,'Shared Pool Free %'
                        ,'Row Cache Hit Ratio'
                        ,'Library Cache Hit Ratio'
                        ,'Memory Sorts Ratio'
                        ,'Row Cache Miss Ratio'
                        ,'Library Cache Miss Ratio'
                        ,'Process Limit %'
                        ,'Session Limit %'
                        ,'Soft Parse Ratio'
                        ,'User Calls Ratio'
                        ,'Database Wait Time Ratio'
                        ,'Average Active Sessions'
                        ,'Background Time Per Sec'
                        ,'Temp Space Used'
                        ,'Total PGA Allocated'
                        ,'Total PGA Used by SQL Workareas'
                        ,'Redo Generated Per Sec'
                        ,'Network Traffic Volume Per Sec'
                        ,'Physical Read Total Bytes Per Sec'
                        ,'Physical Write Total Bytes Per Sec'
                        ,'Physical Read Bytes Per Sec'
                        ,'Physical Write Bytes Per Sec'
                        ,'SQL Service Response Time'
                        ,'CPU Usage Per Sec'
                        ,'Database Time Per Sec'
                        ,'Background CPU Usage Per Sec'
                        ,'Host CPU Usage Per Sec'
                        ,'CPU Usage Per Txn'
                        ,'Response Time Per Txn'
                        ,'DBWR Checkpoints Per Sec'
                        ,'Background Checkpoints Per Sec'
                        ,'User Commits Per Sec'
                        ,'Current Open Cursors Count'
                        ,'Enqueue Deadlocks Per Sec'
                        ,'Enqueue Deadlocks Per Txn'
                        ,'Current Logons Count'
                        ,'Current OS Load'
                        ,'Total Table Scans Per Sec'
                        ,'Total Index Scans Per Sec'
                        ,'Enqueue Waits Per Sec'
                        ,'Enqueue Timeouts Per Sec'
                        ,'Session Count'
                        )
                    AND s.snap_id BETWEEN #' || l_start_snapid || q'# AND #' || l_end_snapid || q'#
                    AND s.dbid = #' || l_dbid || q'#
                    ORDER BY snap_id , s.metric_name#'
                ;	
        exec_query (i_query => l_query, i_dbname => l_dbname);  

    EXCEPTION
        WHEN No_Snapid_Found THEN
            logger.write('Get_TEST_RESULT_METRICS_DET - No SNAPIDs found for database '||l_dbname) ;
    END Get_TEST_RESULT_METRICS_DET ;


/*--------------------------------------------------------------------------------- */
  -- Gets data from tables : ba_hist_active_sess_history
  -- Wait class events
/*--------------------------------------------------------------------------------- */ 
    PROCEDURE Get_TEST_RESULT_WAIT_CLASS (
            i_test   IN test_result_master%ROWTYPE
           ,i_dbname IN varchar2 DEFAULT NULL
       ) AS
        t_test              TEST_RESULT_MASTER%ROWTYPE;
        l_start_snapid      number ;
        l_end_snapid        number ;
        l_dbid              number ;
        l_dbname            varchar2(20);
        l_link              varchar2(25);
        l_query             varchar2(4000);       

        -- Exceptions
        No_SnapId_Found     EXCEPTION;

    BEGIN
        -- Get parameters initialised
        l_dbname := i_dbname ;
        t_test   := i_test ;

        -- Exception when the database is where the repository lives or no DBNAME has been provided 
        if l_dbname = g_repo or l_dbname is NULL then l_link := ''; else l_link := '@'||l_dbname ; end if ;      

        -- Get SNAP IDs for the time period to be extracted
        l_start_snapid := Get_SNAPID (i_testid => t_test.TEST_ID, i_dbname => l_dbname, i_mode => 'START');
        l_end_snapid   := Get_SNAPID (i_testid => t_test.TEST_ID, i_dbname => l_dbname, i_mode => 'END');        
        l_dbid         := Get_DBID(l_dbname) ;

        if l_start_snapid is NULL or l_end_snapid is null then RAISE No_SnapId_Found; end if;

        -- If the start and end snapshots are the same or consecutive, set the start snapid to something meaningful
        if l_end_snapid <= l_start_snapid then
            l_start_snapid := l_end_snapid-1;
        end if;    
        
        l_query := q'#INSERT INTO test_result_wait_class (
                      test_id
                    , test_description
                    , database_name
                    , WAIT_CLASS
                    , min_sessions
                    , max_sessions
                    , avg_sessions
                    )
                SELECT '#' || t_test.TEST_ID || q'#'
                        ,'#' || t_test.TEST_DESCRIPTION || q'#'
                        ,'#' || l_dbname || q'#'
                         , wait_class
                         , min_sess
                         , max_sess
                         , avg_sess
                 FROM (select  wait_class
                             , min(sess_cnt) min_sess
                             , max(sess_cnt) max_sess
                             , round(avg(sess_cnt),2) avg_sess
                         from (SELECT to_char(cast(sample_time as date),'dd/mm/yyyy hh24:mi') as sample_time 
                                      , nvl(wait_class, 'CPU') wait_class
                                      , count(*) sess_cnt
                                FROM dba_hist_active_sess_history#' || l_link || q'# a
                               WHERE a.snap_id BETWEEN #' || l_start_snapid || q'# AND #' || l_end_snapid || q'#
                                 AND a.dbid = #' || l_dbid || q'#
                               GROUP BY cast(sample_time as date), wait_class
                              )
                        group by wait_class
                     )#'
        ;

        exec_query (i_query => l_query, i_dbname => l_dbname);  

    EXCEPTION
        WHEN No_Snapid_Found THEN
            logger.write('Get_TEST_RESULT_WAIT_CLASS - No SNAPIDs found for database '||l_dbname) ;
    END Get_TEST_RESULT_WAIT_CLASS ;


/*--------------------------------------------------------------------------------- */
  -- Get data from tables : dba_hist_system_event
  -- We are storing a summary of the system events not within the "IDLE" and "NETWORK" wait_class 
/*--------------------------------------------------------------------------------- */ 
    PROCEDURE Get_TEST_RESULT_SYSTEM_EVENT (
            i_test   IN test_result_master%ROWTYPE
           ,i_dbname IN varchar2 DEFAULT NULL
       ) AS
        t_test              TEST_RESULT_MASTER%ROWTYPE;
        l_start_snapid      number ;
        l_end_snapid        number ;
        l_dbid              number ;
        l_dbname            varchar2(20);
        l_link              varchar2(25);
        l_query             varchar2(4000);       

        -- Exceptions
        No_SnapId_Found     EXCEPTION;

    BEGIN
        -- Get parameters initialised
        l_dbname := i_dbname ;
        t_test   := i_test ;

        -- Exception when the database is where the repository lives or no DBNAME has been provided 
        if l_dbname = g_repo or l_dbname is NULL then l_link := ''; else l_link := '@'||l_dbname ; end if ;            

        -- Get SNAP IDs for the time period to be extracted
        l_start_snapid := Get_SNAPID (i_testid => t_test.TEST_ID, i_dbname => l_dbname, i_mode => 'START')+1;
        l_end_snapid   := Get_SNAPID (i_testid => t_test.TEST_ID, i_dbname => l_dbname, i_mode => 'END');        
        l_dbid         := Get_DBID(l_dbname) ;

        if l_start_snapid is NULL or l_end_snapid is null then RAISE No_SnapId_Found; end if;

        -- If the start and end snapshots are the same or consecutive, set the start snapid to something meaningful
        if l_end_snapid <= l_start_snapid then
            l_start_snapid := l_end_snapid-1;
        end if;    
        
        l_query := q'#INSERT INTO TEST_RESULT_SYSTEM_EVENT t ( t.test_id , t.database_name , t.waits , t.wait_avg_ms, t.wait_class, t.event_name )
                    SELECT '#' || t_test.TEST_ID || q'#' AS test_id
                        , '#' || l_dbname || q'#' AS database_name
                        , MAX ( total_waits_fg ) - MIN ( total_waits_fg ) AS waits
                        , CASE WHEN MAX ( total_waits_fg ) - MIN ( total_waits_fg ) > 0 THEN
                               ( ROUND( ( MAX ( time_waited_micro_fg ) - MIN ( time_waited_micro_fg ) ) / ( MAX ( total_waits_fg ) - MIN ( total_waits_fg ) ) / 1000 , 5) )
                          END AS wait_avg_ms
                        , wait_class
                        , event_name
                    FROM dba_hist_system_event#' || l_link || q'# e
                    WHERE e.snap_id BETWEEN #' || l_start_snapid || q'# AND #' || l_end_snapid || q'#
                    AND e.dbid = #' || l_dbid || q'#
                    --AND e.event_name = 'log file sync'
                    AND e.wait_class not in ('Idle','Network')
                    GROUP BY e.event_name, e.wait_class 
                    HAVING MAX ( total_waits_fg ) - MIN ( total_waits_fg ) > 0
                    ORDER BY e.wait_class, e.event_name#'
        ;

        exec_query (i_query => l_query, i_dbname => l_dbname);  

    EXCEPTION
        WHEN No_Snapid_Found THEN
            logger.write('Get_TEST_RESULT_SYSTEM_EVENT - No SNAPIDs found for database '||l_dbname) ;
    END Get_TEST_RESULT_SYSTEM_EVENT ;


/*--------------------------------------------------------------------------------- */
    -- Get data from tables : dba_hist_snapshot
    --                      , DBA_HIST_CON_SYS_TIME_MODEL
    --                      , DBA_HIST_CON_SYSSTAT
/*--------------------------------------------------------------------------------- */  

    PROCEDURE Get_TEST_RESULT_DB_STATS (
            i_test   IN test_result_master%ROWTYPE
           ,i_dbname IN varchar2 DEFAULT NULL
       ) AS
        t_test              TEST_RESULT_MASTER%ROWTYPE;
        l_start_snapid      number ;
        l_end_snapid        number ;
        l_dbid              number ;
        l_dbname            varchar2(20);
        l_link              varchar2(25);
        l_query             varchar2(4000);
        l_ver               varchar2(3);
        l_tab1              varchar2(30);
        l_tab2              varchar2(30);

        -- Exceptions
        No_SnapId_Found     EXCEPTION;

    BEGIN
        -- Get parameters initialised
        l_dbname := i_dbname ;
        t_test   := i_test ;

        -- Exception when the database is where the repository lives or no DBNAME has been provided 
        if l_dbname = g_repo or l_dbname is NULL then l_link := ''; else l_link := '@'||l_dbname ; end if ;      

        -- Get SNAP IDs for the time period to be extracted
        l_start_snapid := Get_SNAPID (i_testid => t_test.TEST_ID, i_dbname => l_dbname, i_mode => 'START');
        l_end_snapid   := Get_SNAPID (i_testid => t_test.TEST_ID, i_dbname => l_dbname, i_mode => 'END');        
        l_dbid := Get_DBID(l_dbname) ;

        if l_start_snapid is NULL or l_end_snapid is null then RAISE No_SnapId_Found; end if;

        -- If the start and end snapshots are the same or consecutive, set the start snapid to something meaningful
        if l_end_snapid <= l_start_snapid then
            l_start_snapid := l_end_snapid-1;
        end if;    
        
        -- Get the DB Version
        l_ver := Get_DB_Ver(l_dbname);
        CASE WHEN l_ver = '19c' THEN 
            l_tab1 := 'DBA_HIST_CON_SYS_TIME_MODEL';
            l_tab2 := 'DBA_HIST_CON_SYSSTAT';
        ELSE
            l_tab1 := 'DBA_HIST_SYS_TIME_MODEL';
            l_tab2 := 'DBA_HIST_SYSSTAT';
        END CASE;

        -- Get SQL 
        l_query := q'#insert into TEST_RESULT_DB_STATS (
                        TEST_ID
                       ,TEST_DESCRIPTION
                       ,DATABASE_NAME
                       ,BEGIN_SNAP
                       ,END_SNAP
                       ,BEGIN_TIME
                       ,END_TIME
                       ,ELAPSED_TIME_SECS
                       ,DB_TIME_SECS
                       ,DB_CPU_SECS
                       ,TOTAL_EXECS ) 
                   select  '#' || t_test.TEST_ID || q'#'
                       ,'#' || t_test.TEST_DESCRIPTION || q'#'
                       ,'#' || l_dbname || q'#'
                       ,#'  || l_start_snapid || q'#
                       ,#'  || l_end_snapid || q'#
                       ,to_date(aa.awr_start_time, 'dd/mm/yyyy hh24:mi:ss') begin_time 
                       ,to_date(bb.awr_end_time,'dd/mm/yyyy hh24:mi:ss') end_time 
                       ,round(to_number((to_date(bb.awr_end_time, 'dd/mm/yyyy hh24:mi:ss')-to_date(aa.awr_start_time,'dd/mm/yyyy hh24:mi:ss')))*60*60*24,2) elapsed_seconds 
                       ,cc.db_time_secs 
                       ,dd.db_cpu_secs
                       ,ee.total_execs    
                    from ( select to_char(a.end_interval_time,'dd/mm/yyyy hh24:mi:ss') awr_start_time 
                           from dba_hist_snapshot#' || l_link || q'# a
                           where a.snap_id = #' || l_start_snapid || q'#
                             and a.dbid = '#' || l_dbid || q'#'
                         ) aa
                        ,( select to_char(a.end_interval_time,'dd/mm/yyyy hh24:mi:ss') awr_end_time 
                           from dba_hist_snapshot#' || l_link || q'# a
                           where a.snap_id = #' || l_end_snapid || q'# 
                             and a.dbid = '#' || l_dbid || q'#' ) bb
                        ,( select round((b.value-a.value)/1000000,2) db_time_secs
                           from  ( select value from #' || l_tab1 || l_link || q'# 
                                    where snap_id = #' || l_start_snapid || q'# 
                                      and dbid = '#' || l_dbid || q'#'
                                      and stat_name = 'DB time') a
                        ,( select value from #' || l_tab1 || l_link || q'# 
                            where snap_id = #' || l_end_snapid || q'# 
                              and dbid = '#' || l_dbid || q'#'
                              and stat_name = 'DB time') b) cc
                        ,( select round((b.value-a.value)/1000000,2) db_cpu_secs
                           from (select value from #' || l_tab1 || l_link || q'# 
                                  where snap_id = #' || l_start_snapid || q'# 
                                    and dbid = '#' || l_dbid || q'#'
                                    and stat_name = 'DB CPU') a
                                ,(select value from #' || l_tab1 || l_link || q'# 
                                   where snap_id = #' || l_end_snapid || q'# 
                                     and dbid = '#' || l_dbid || q'#'
                                     and stat_name = 'DB CPU') b ) dd
                       ,( select b.value-a.value total_execs
                           from (select value from #' || l_tab2 || l_link || q'# 
                                  where snap_id = #' || l_start_snapid || q'# 
                                    and dbid = '#' || l_dbid || q'#'
                                    and stat_name = 'execute count') a
                                ,(select value from #' || l_tab2 || l_link || q'# 
                                   where snap_id = #' || l_end_snapid || q'# 
                                     and dbid = '#' || l_dbid || q'#'
                                     and stat_name = 'execute count') b ) ee#'
        ;

        exec_query (i_query => l_query, i_dbname => l_dbname);  

    EXCEPTION
        WHEN No_Snapid_Found THEN
            logger.write('Get_TEST_RESULT_DB_STATS - No SNAPIDs found for database '||l_dbname) ;
    END Get_TEST_RESULT_DB_STATS ;

/*--------------------------------------------------------------------------------- */
  -- Row Lock Contention Specific Event being trapped in this table
  -- Get data from tables : dba_hist_active_sess_history
  --                       ,dba_objects
  --                       ,v$active_session_history
/*--------------------------------------------------------------------------------- */ 
    PROCEDURE Get_TEST_RESULT_RLW (
            i_test   IN test_result_master%ROWTYPE
           ,i_dbname IN varchar2 DEFAULT NULL
       ) AS
        t_test              TEST_RESULT_MASTER%ROWTYPE;
        l_start_snapid      number ;
        l_end_snapid        number ;
        l_start_dt          varchar2(20);
        l_end_dt            varchar2(20);
        l_dbid              number ;
        l_dbname            varchar2(20);
        l_link              varchar2(25);
        l_query             varchar2(4000);

        -- Exceptions
        No_SnapId_Found     EXCEPTION;

    BEGIN
        -- Get parameters initialised
        l_dbname := i_dbname ;
        t_test   := i_test ;

        -- Exception when the database is where the repository lives or no DBNAME has been provided 
        if l_dbname = g_repo or l_dbname is NULL then l_link := ''; else l_link := '@'||l_dbname ; end if ;      

        -- Get SNAP IDs for the time period to be extracted
        l_start_snapid := Get_SNAPID (i_testid => t_test.TEST_ID, i_dbname => l_dbname, i_mode => 'START');
        l_end_snapid   := Get_SNAPID (i_testid => t_test.TEST_ID, i_dbname => l_dbname, i_mode => 'END');        
        l_dbid         := Get_DBID(l_dbname) ;
        l_start_dt     := REPORT_ADM.Get_DTM(i_testid => t_test.TEST_ID, i_mode => 'START') ;
        l_end_dt       := REPORT_ADM.Get_DTM(i_testid => t_test.TEST_ID, i_mode => 'END') ;

        if l_start_snapid is NULL or l_end_snapid is null then RAISE No_SnapId_Found; end if;

        -- If the start and end snapshots are the same or consecutive, set the start snapid to something meaningful
        if l_end_snapid <= l_start_snapid then
            l_start_snapid := l_end_snapid-1;
        end if;    
        
        -- Get SQL 
        l_query := q'#INSERT INTO TEST_RESULT_RLW (
                           TEST_ID
                           ,TEST_DESCRIPTION
                           ,DATABASE_NAME
                           ,BEGIN_SNAP
                           ,END_SNAP
                           ,BEGIN_TIME
                           ,END_TIME
                           ,OBJECT_OWNER
                           ,OBJECT_NAME
                           ,OBJECT_TYPE
                           ,TOP_RLW_NUMBER
                           ,NUM_WAITS
                           ,MIN_WAIT_TIME
                           ,MAX_WAIT_TIME
                           ,TOTAL_WAIT_TIME 
                        )
                    SELECT '#' || t_test.TEST_ID || q'#'
                            ,'#' || t_test.TEST_DESCRIPTION || q'#'
                            ,'#' || l_dbname || q'#'
                             , null as begin_snap
                             , null as end_snap
                             , TO_DATE('#' || l_start_dt || q'#','DDMONYY-HH24:MI')
                             , TO_DATE('#' || l_end_dt || q'#','DDMONYY-HH24:MI')
                             , object_owner
                             , object_name
                             , OBJECT_TYPE
                             , ROWNUM AS TOP_RLW_NUMBER
                             , num_waits
                             , min_wait_ms
                             , max_wait_ms
                             , total_wait_ms
                          from ( select owner as object_owner
                                       ,object_name
                                       ,object_type
                                       ,count(*) num_waits
                                       ,min(time_ms) min_wait_ms
                                       ,max(time_ms) max_wait_ms
                                       ,sum(time_ms) total_wait_ms
                                  from ( SELECT ASH.event, ASH.current_obj#, ASH.sample_time, ash.time_waited/1000 time_ms, OBJ.object_name, obj.owner, obj.object_type
                                           FROM dba_hist_active_sess_history#' || l_link || q'# ASH
                                              , dba_objects#' || l_link || q'# OBJ
                                          WHERE ASH.event = 'enq: TX - row lock contention'
                                            AND ASH.sample_time BETWEEN TO_DATE('#' || l_start_dt || q'#','DDMONYY-HH24:MI') AND TO_DATE('#' || l_end_dt || q'#','DDMONYY-HH24:MI')
                                            AND ASH.current_obj# = obj.object_id
                                            AND obj.owner not in ('DATAPROV')
                                         UNION
                                         SELECT ASHS.event, ASHS.current_obj#, ASHS.sample_time, ashs.time_waited/1000 time_ms, OBJ.object_name, obj.owner, obj.object_type
                                           FROM v$active_session_history#' || l_link || q'# ASHS
                                              , dba_objects#' || l_link || q'# OBJ
                                          WHERE ASHS.event = 'enq: TX - row lock contention'
                                            AND ASHS.sample_time BETWEEN TO_DATE ('#' || l_start_dt || q'#','DDMONYY-HH24:MI') AND TO_DATE ('#' || l_end_dt || q'#','DDMONYY-HH24:MI')
                                            AND ASHS.current_obj# = OBJ.object_id
                                            and obj.owner not in ('DATAPROV'))
                                  group by owner, object_name, object_type
                                  order by total_wait_ms desc)
                         where rownum <= 50#'
                    ;
        exec_query (i_query => l_query, i_dbname => l_dbname);  

    EXCEPTION
        WHEN No_Snapid_Found THEN
            logger.write('Get_TEST_RESULT_RLW - No SNAPIDs found for database '||l_dbname) ;
    END Get_TEST_RESULT_RLW ;


/*--------------------------------------------------------------------------------- */
    -- Get the AWR report from the remote database
    -- The report has to be generated remotely on each database and it uses a local procedure and table that are created at running time
    -- The central repository will get updated by querying the remote table created to store this data
    -- This is required to move CLOB data across database links
/*--------------------------------------------------------------------------------- */    
    PROCEDURE Get_TEST_RESULT_AWR (
            i_test   IN test_result_master%ROWTYPE
           ,i_dbname IN varchar2 DEFAULT NULL
       ) AS
        t_test              TEST_RESULT_MASTER%ROWTYPE;
        l_test_id           TEST_RESULT_MASTER.TEST_ID%TYPE ;
        l_start_snapid      number ;
        l_end_snapid        number ;
        l_dbname            varchar2(20);
        l_link              varchar2(25);
        l_query             varchar2(4000);

        -- Exceptions
        No_SnapId_Found     EXCEPTION;

    BEGIN
        -- Get parameters initialised
        l_dbname := i_dbname ;
        --t_test := Get_Test_Details(l_test_id) ;
        t_test := i_test ;
        l_test_id := t_test.TEST_ID ;

        -- Exception when the database is where the repository lives or no DBNAME has been provided 
        if l_dbname = g_repo or l_dbname is NULL then l_link := ''; else l_link := '@'||l_dbname ; end if ;      

        -- Get SNAP IDs for the time period to be extracted
        l_start_snapid := Get_SNAPID (i_testid => l_test_id, i_dbname => l_dbname, i_mode => 'START');
        l_end_snapid   := Get_SNAPID (i_testid => l_test_id, i_dbname => l_dbname, i_mode => 'END');
        
        if l_start_snapid is NULL or l_end_snapid is null then RAISE No_SnapId_Found; end if;

        -- If the start and end snapshots are the same or consecutive, set the start snapid to something meaningful
        if l_end_snapid <= l_start_snapid then
            l_start_snapid := l_end_snapid-1;
        end if;    
        
        -- Create table on remote database (ignore if it fails because it already exists )
        l_query := 'CREATE TABLE hp_diag.awr_clob (tmp_clob CLOB)' ;
        exec_ddl_query (i_query => l_query, i_dbname => l_dbname); 

        -- Create Remote PROC
        l_query := q'#CREATE OR REPLACE PROCEDURE "HP_DIAG"."PROC_GET_AWR_DATA" ( 
                         i_start_snapid     IN number 
                        ,i_end_snapid       IN number 
                        ,i_options          IN number DEFAULT 0 
                        )
                AS 
                    -- Options : 0 --> NO_OPTIONS, 8 --> ENABLE_ADDM 
                    l_clob      clob ; 
                    l_count     number ; 
                    l_dbid      number ; 
                BEGIN 
                    -- Create table if not exit 
                    BEGIN 
                        execute immediate 'CREATE TABLE AWR_CLOB ( tmp_clob clob )'; 
                    EXCEPTION WHEN OTHERS THEN 
                        null; 
                    END; 
                    -- Trunctae table always before start 
                    execute immediate 'truncate table AWR_CLOB'; 
                    -- Get the DBID for this database 
                    SELECT dbid INTO l_dbid FROM V$DATABASE; 
                    -- Set the thresholod for the AWR gathering 
                    DBMS_WORKLOAD_REPOSITORY.AWR_SET_REPORT_THRESHOLDS( top_n_sql => 50 ); 
                    -- Get the AWR report stored into a CLOB object  
                    DBMS_LOB.CREATETEMPORARY ( lob_loc => l_clob , cache => TRUE );  
                    FOR r1 IN (  -- generate the AWR report 
                        SELECT output || CHR(10) AS output_nl FROM TABLE ( DBMS_WORKLOAD_REPOSITORY.AWR_REPORT_HTML  
                                ( l_dbid => l_dbid 
                                , l_inst_num => 1  
                                , l_bid => i_start_snapid 
                                , l_eid => i_end_snapid 
                                , l_options => i_options ) ) 
                         ) 
                    LOOP 
                        DBMS_LOB.WRITEAPPEND ( lob_loc => l_clob , amount => LENGTH ( r1.output_nl ) , buffer => r1.output_nl ) ; 
                    END LOOP ;    
                    -- Insert into the AWR_CLOB table 
                    INSERT INTO AWR_CLOB ( tmp_clob ) VALUES ( l_clob ); 
                    -- Free the CLOB object 
                    DBMS_LOB.FREETEMPORARY ( l_clob ) ; 
                    COMMIT; 
                END PROC_GET_AWR_DATA;#'
                ;
        exec_ddl_query (i_query => l_query, i_dbname => l_dbname); 

        -- Have to change a parameter value to be able to run this command
        l_query := 'alter session set remote_dependencies_mode=signature';
        exec_query (i_query => l_query, i_dbname => l_dbname);  


        -- Get the AWR data calling the localised procedure within the remote database
        -- This procedure will populate a temp table with the AWR report stored in a CLOB column : TEMP_CLOB
        l_query := 'BEGIN PROC_GET_AWR_DATA'||l_link||' ( i_start_snapid => '||l_start_snapid||', i_end_snapid => '||l_end_snapid||' ); END; ' ;
        exec_query (i_query => l_query, i_dbname => l_dbname);  

        -- Insert into TEST_RESULT_AWR table
        l_query := q'#INSERT INTO TEST_RESULT_AWR
                      ( test_id
                       ,test_description
                       ,database_name
                       ,begin_snap
                       ,end_snap
                       ,begin_time
                       ,end_time
                       ,awr_report)
                    SELECT '#' || t_test.test_id ||q'#'
                       ,'#' || t_test.test_description ||q'#'
                       ,'#' || l_dbname ||q'#'
                       ,#' || l_start_snapid ||q'#
                       ,#' || l_end_snapid ||q'#
                       ,TO_DATE('#' || REPORT_ADM.get_DTM(t_test.test_id,'START') || q'#','DDMONYY-HH24:MI')
                       ,TO_DATE('#' || REPORT_ADM.get_DTM(t_test.test_id,'END') || q'#','DDMONYY-HH24:MI')
                       ,tmp_clob 
                    FROM AWR_CLOB#' || l_link ;                 

        exec_query (i_query => l_query, i_dbname => l_dbname);  

        -- Drop remote table
        --l_query := 'DROP TABLE hp_diag.awr_clob' ;
        --exec_ddl_query (i_query => l_query, i_dbname => l_dbname);         
    EXCEPTION
        WHEN No_Snapid_Found THEN
            logger.write('Get_TEST_RESULT_AWR - No SNAPIDs found for database '||l_dbname) ;
    END Get_TEST_RESULT_AWR ;

/*--------------------------------------------------------------------------------- */
    -- Get data from tables : dba_hist_active_sess_history
/*--------------------------------------------------------------------------------- */  
    PROCEDURE Get_TEST_RESULT_ACTIVITY (
            i_test   IN test_result_master%ROWTYPE
           ,i_dbname IN varchar2 DEFAULT NULL
       ) AS
        t_test              TEST_RESULT_MASTER%ROWTYPE;
        l_start_dt          varchar2(20);
        l_end_dt            varchar2(20);
        l_dbname            varchar2(20);
        l_link              varchar2(25);
        l_query             varchar2(4000);

    BEGIN
        -- Get parameters initialised
        l_dbname := i_dbname ;
        t_test   := i_test ;

        -- Exception when the database is where the repository lives or no DBNAME has been provided 
        if l_dbname = g_repo or l_dbname is NULL then l_link := ''; else l_link := '@'||l_dbname ; end if ;      

        -- Get the sample dates
        l_start_dt     := REPORT_ADM.Get_DTM(i_testid => t_test.TEST_ID, i_mode => 'START') ;
        l_end_dt       := REPORT_ADM.Get_DTM(i_testid => t_test.TEST_ID, i_mode => 'END') ;

        -- Get SQL 
        l_query := q'#insert into TEST_RESULT_ACTIVITY (
                            TEST_ID
                            ,DATABASE_NAME
                            ,SAMPLE_TIME
                            ,CPU
                            ,BCPU
                            ,SCHED
                            ,USERIO
                            ,SYSTEMIO
                            ,CURR
                            ,APPL
                            ,COMT
                            ,CONFIG
                            ,ADMIN
                            ,NETW
                            ,QUEUE
                            ,CLUST
                            ,OTHER )
                    select '#' || t_test.TEST_ID || q'#'
                           ,'#' || l_dbname || q'#'
                           ,sample_time
                           ,nvl(round((cpu)/6,3),0) as cpu
                           ,nvl(round((bcpu)/6,3),0) as bcpu
                           ,nvl(round((scheduler)/6,3),0) as sched
                           ,nvl(round((uio)/6,3),0) as userio
                           ,nvl(round((sio)/6,3),0) as systemio
                           ,nvl(round((concurrency)/6,3),0) as curr
                           ,nvl(round((application)/6,3),0) as appl
                           ,nvl(round((commit)/6,3),0) as comt
                           ,nvl(round((configuration)/6,3),0) as config
                           ,nvl(round((administrative)/6,3),0) as admin
                           ,nvl(round((network)/6,3),0) as netw
                           ,nvl(round((queueing)/6,3),0) as queue
                           ,nvl(round((clust)/6,3),0) as clust
                           ,nvl(round((other)/6,3),0) as other
                    from  (select TRUNC(sample_time,'MI') AS sample_time
                                 ,DECODE(session_state,'ON CPU',DECODE(session_type,'BACKGROUND','BCPU','ON CPU'), wait_class) AS wait_class
                            from dba_hist_active_sess_history#' || l_link || q'#
                           where sample_time between to_date('#' || l_start_dt || q'#','DDMONYY-HH24:MI') and to_date('#' || l_end_dt || q'#','DDMONYY-HH24:MI')
                           ) ash
                    PIVOT ( COUNT(*) FOR wait_class IN (
                          'ON CPU' AS cpu
                          ,'BCPU' AS bcpu
                          ,'Scheduler' AS scheduler
                          ,'User I/O' AS uio
                          ,'System I/O' AS sio
                          ,'Concurrency' AS concurrency
                          ,'Application' AS application
                          ,'Commit' AS COMMIT
                          ,'Configuration' AS configuration
                          ,'Administrative' AS administrative
                          ,'Network' AS network
                          ,'Queueing' AS queueing
                          ,'Cluster' AS clust
                          ,'Other' AS other)
                           ) ash
                    order by sample_time#'       
               ;
        exec_query (i_query => l_query, i_dbname => l_dbname);  

    END Get_TEST_RESULT_ACTIVITY ;


/*--------------------------------------------------------------------------------- */
    -- Get data from SAL database only
    -- Tables : sal_owner.salesinteraction
    -- Relies on the target DB (ISS011N and ISS021N) having the following elements created 
    -- TMP_SAL_BASKET table
    -- PROC_GET_SAL_BASKET  procedure
/*--------------------------------------------------------------------------------- */  
    PROCEDURE Get_TEST_RESULT_SAL_BASKET (
            i_test   IN test_result_master%ROWTYPE
           ,i_dbname IN varchar2 DEFAULT NULL
       ) AS
        t_test              TEST_RESULT_MASTER%ROWTYPE;
        l_start_dt          varchar2(20);
        l_end_dt            varchar2(20);
        l_dbname            varchar2(20);
        l_link              varchar2(25);
        l_query             varchar2(16000);

    BEGIN
        -- Get parameters initialised
        l_dbname := i_dbname ;
        t_test   := i_test ;

        -- Exception when the database is where the repository lives or no DBNAME has been provided 
        if l_dbname = g_repo or l_dbname is NULL then l_link := ''; else l_link := '@'||l_dbname ; end if ;      

        -- Get the sample dates
        l_start_dt     := REPORT_ADM.Get_DTM(i_testid => t_test.TEST_ID, i_mode => 'START') ;
        l_end_dt       := REPORT_ADM.Get_DTM(i_testid => t_test.TEST_ID, i_mode => 'END') ;

        l_query := q'#BEGIN PROC_GET_SAL_BASKET#' || l_link || q'#
                            (i_testid => '#' || t_test.TEST_ID || q'#'
                            ,i_desc => '#' || t_test.TEST_DESCRIPTION || q'#'
                            ,i_startdt => '#' || l_start_dt || q'#'
                            ,i_enddt => '#' || l_end_dt || q'#'
                            ); 
                      END; #' ;

        exec_query (i_query => l_query, i_dbname => l_dbname);  

        l_query := q'#insert into TEST_RESULT_SAL_BASKET
                      select * from TMP_SAL_BASKET#' || l_link ;
        exec_query (i_query => l_query, i_dbname => l_dbname); 

    END Get_TEST_RESULT_SAL_BASKET ;


/*--------------------------------------------------------------------------------- */
    -- Get data from the CUSTOMER tables.
    -- This procedure will only access the customer tables, either all of them or 
    -- one specific if supplied
/*--------------------------------------------------------------------------------- */  
    PROCEDURE Get_TEST_RESULT_BILLING_RUN (
            i_test   IN test_result_master%ROWTYPE
       ) AS
        t_test              TEST_RESULT_MASTER%ROWTYPE;
        t_bill              TEST_RESULT_BILLING_RUN%ROWTYPE ;
        l_start_dt          varchar2(20);
        l_end_dt            varchar2(20);
        l_dbname            varchar2(20);
        l_link              varchar2(25);
        l_prefix            varchar2(25);
        l_env               varchar2(3) ;
        l_header            varchar2(10) ;
        l_query             varchar2(4000);
        l_count             number := 0;

        v_cursor            sys_refcursor ;
        l_bname             varchar2(10);
        l_date              varchar2(20);
        l_num               number ;

        no_cust_db  EXCEPTION;

    BEGIN
        -- Get parameters initialised
        t_test   := i_test ;

        -- Get the sample dates
        l_start_dt  := REPORT_ADM.Get_DTM(i_testid => t_test.TEST_ID, i_mode => 'START') ;
        l_end_dt    := REPORT_ADM.Get_DTM(i_testid => t_test.TEST_ID, i_mode => 'END') ;
        l_env       := REPORT_ADM.Parse_TestId(t_test.TEST_ID,'ENV') ;

        -- Checks if an specific CUSTOMER database has been provided
        if l_env = 'N01' then 
            l_prefix := '@DCU01';
        elsif l_env = 'N02' then
            l_prefix := '@DCU02';
        else
            raise no_cust_db;
        end if;

        for i in 1..6
        loop
            l_header := 'CUS0'||i ;
            l_link   := l_prefix||i||'N' ;

            l_query := q'#SELECT '#' || l_header || q'#' database
                                , TO_CHAR(prep_date , 'dd-mm-yyyy hh24:mi' ) date_Time, COUNT(*) num_billed 
                            FROM arbor.bill_invoice#' || l_link || q'#
                           WHERE prep_date between to_date('#' || l_start_dt || q'#','DDMONYY-HH24:MI') AND to_date('#' || l_end_dt || q'#','DDMONYY-HH24:MI')
                             AND prep_status = 1  
                             AND interim_bill_flag = 0
                           GROUP BY TO_CHAR( prep_date, 'dd-mm-yyyy hh24:mi')
                           ORDER BY date_Time #';

            -- Call the procedure to resolve the SQL
            exec_cursor (i_query => l_query, i_dbname => l_link, i_cursor => v_cursor) ;  
            -- If something goes wrong resolving the query, the error will be trapped within the above procedure and the cursor will be closed.
            -- If so, do nothing so that it doesn't stop further processing
            if v_cursor%ISOPEN then
                loop
                    fetch v_cursor into l_bname, l_date, l_num;
                    exit when v_cursor%notfound;
                    if l_date is not NULL then
                        CASE WHEN i = 1 THEN
                                insert into TEST_RESULT_BILLING_RUN 
                                    (TEST_ID,TEST_DESCRIPTION,BILLING_DATE,CUS01)
                                values 
                                    (t_test.TEST_ID
                                    ,t_test.TEST_DESCRIPTION
                                    ,to_date(l_date,'dd-mm-yyyy hh24:mi')
                                    ,l_num);  
                            WHEN i = 2 THEN
                                update TEST_RESULT_BILLING_RUN
                                set CUS02 = l_num                                 
                                where TEST_ID = t_test.TEST_ID
                                  and BILLING_DATE = to_date(l_date,'dd-mm-yyyy hh24:mi');
                            WHEN i = 3 THEN
                                update TEST_RESULT_BILLING_RUN
                                set CUS03 = l_num                                 
                                where TEST_ID = t_test.TEST_ID
                                  and BILLING_DATE = to_date(l_date,'dd-mm-yyyy hh24:mi');
                            WHEN i = 4 THEN
                                update TEST_RESULT_BILLING_RUN
                                set CUS04 = l_num                                 
                                where TEST_ID = t_test.TEST_ID
                                  and BILLING_DATE = to_date(l_date,'dd-mm-yyyy hh24:mi');
                            WHEN i = 5 THEN
                                update TEST_RESULT_BILLING_RUN
                                set CUS05 = l_num                                 
                                where TEST_ID = t_test.TEST_ID
                                  and BILLING_DATE = to_date(l_date,'dd-mm-yyyy hh24:mi');
                            WHEN i = 6 THEN
                                update TEST_RESULT_BILLING_RUN
                                set CUS06 = l_num                                 
                                where TEST_ID = t_test.TEST_ID
                                  and BILLING_DATE = to_date(l_date,'dd-mm-yyyy hh24:mi');
                        END CASE;   
                    end if;    
                end loop;    
                close v_cursor;
                commit;
                close_dblink(l_link);
            end if ;
        end loop ;

    EXCEPTION
        WHEN no_cust_db THEN
            logger.write('No Customer DB applicable');
        WHEN OTHERS THEN
            logger.write('exec_query - '||l_dbname|| ' : ' ||SQLCODE||' : '||SUBSTR(SQLERRM,1,500)) ;  
    END Get_TEST_RESULT_BILLING_RUN ;


/*--------------------------------------------------------------------------------- */
    -- Get_TEST_RESULT_CACHE
    -- Gathers the information for 2 specific SQLIDs to be displayed on the reports
    -- It only works for the Chordiant type of databases.
/*--------------------------------------------------------------------------------- */       
    PROCEDURE Get_TEST_RESULT_CACHE (
           i_test   IN test_result_master%ROWTYPE 
          ) 
    AS
        t_test              TEST_RESULT_MASTER%ROWTYPE ;
        l_row                varchar2(4000) ;
        l_env                varchar2(5) ;
        l_dbname             varchar2(30) ;
        l_link               varchar2(25);
        l_query              varchar2(4000);
        l_start_snapid       number ;
        l_end_snapid         number ;
        l_dbid              number ;

    BEGIN
        -- Initialise vars
        t_test := i_test ;

        -- Get the ENV from the test ID and derive the name of the chordo-like database 
        l_env := REPORT_ADM.Parse_TestID(t_test.TEST_ID,'ENV');
        If l_env = 'N01' then 
            l_dbname := 'CHORDO';
        elsif l_env = 'N02' then
            l_dbname := 'CCS021N' ;
        else
            l_dbname := null;
        end if ;
        l_link := '@'||l_dbname ;

        -- Get SNAP IDs for the time period to be extracted
        l_start_snapid := Get_SNAPID (i_testid => t_test.TEST_ID, i_dbname => l_dbname, i_mode => 'START');
        l_end_snapid   := Get_SNAPID (i_testid => t_test.TEST_ID, i_dbname => l_dbname, i_mode => 'END');

        -- If the start and end snapshots are the same or consecutive, set the start snapid to something meaningful
        if l_end_snapid <= l_start_snapid then
            l_start_snapid := l_end_snapid-1;
        end if;    
        
        -- Get the DBID for the database
        l_dbid := Get_DBID(l_dbname) ;

        l_query  := q'#INSERT into TEST_RESULT_CACHE 
                        (TEST_ID
                       , DATABASE_NAME
                       , SQL_ID
                       , QUERY_TYPE
                       , EXECUTIONS
                       , AVG_TIME_MS )
                      SELECT 
                          '#' || t_test.TEST_ID ||q'#'
                         ,'#' || l_dbname ||q'#'
                         , v.sql_id
                         , v.query_type 
                         , v.execs 
                         , v.avg_ms
                      FROM (
                             SELECT  s.sql_id
                                   , CASE WHEN s.sql_id = 'axdsyv3z7aznk' THEN 'No Cache Query (' || s.sql_id || ')'
                                          WHEN s.sql_id = '81x3a4w4tznzj' THEN 'Cache Query (' || s.sql_id || ')'
                                      END AS query_type
                                   , NVL ( SUM ( s.executions_delta ) , 0 ) AS execs 
                                   , SUM ( s.elapsed_time_delta ) / NULLIF ( SUM ( s.executions_delta ) , 0 ) / 1000 AS avg_ms
                                FROM dba_hist_sqlstat#'|| l_link || q'# s
                               WHERE s.snap_id BETWEEN #' || l_start_snapid || q'# AND #' || l_end_snapid || q'#
                                 AND s.dbid = '#' || l_dbid || q'#'
                                 AND s.sql_id IN ( 'axdsyv3z7aznk' , '81x3a4w4tznzj' )
                               GROUP BY s.sql_id ) v
                     ORDER BY 1#' ;

        -- Call the procedure to resolve the SQL
        exec_query (i_query => l_query, i_dbname => l_dbname );  

    END Get_TEST_RESULT_CACHE ;


/*--------------------------------------------------------------------------------- */
    -- Get data from tables : DBA_TABLES and DBA_INDEXES
    -- Retrieves objects that have a level of parallelism greater than 1 or DEFAULT 
/*--------------------------------------------------------------------------------- */  

    PROCEDURE Get_TEST_RESULT_OBJ_DETAILS (
            i_test   IN test_result_master%ROWTYPE
           ,i_dbname IN varchar2 DEFAULT NULL
       ) AS
        t_test              TEST_RESULT_MASTER%ROWTYPE;

        l_dbname            varchar2(20);
        l_link              varchar2(25);
        l_query             varchar2(4000);
        l_where             varchar2(2000);
        l_create            date ; 

    BEGIN
        -- Get parameters initialised
        l_dbname := i_dbname ;
        t_test   := i_test ;
        -- START date extracted from the TEST_ID
        l_create := to_date(REPORT_ADM.Get_DTM(t_test.TEST_ID),'DDMONYY-HH24:MI') ;

        -- Exception when the database is where the repository lives or no DBNAME has been provided 
        if l_dbname = g_repo or l_dbname is NULL then l_link := ''; else l_link := '@'||l_dbname ; end if ;      

        -- Set the where clause depending on DB version
        If to_number(SUBSTR(REPORT_UTILS.Get_DB_Ver(l_dbname),1,2)) > 12 then
            l_where := q'# ORACLE_MAINTAINED = 'Y' #' ;
        Else
            l_where := q'# USERNAME IN ('ANONYMOUS','APEX_PUBLIC_USER','APPQOSSYS','AUDSYS','DBSFWUSER','DBSNMP','DIP','FLOWS_FILES','GGSYS','GSMADMIN_INTERNAL'
                                            ,'GSMCATUSER','GSMUSER','ORACLE_OCM','OUTLN','REMOTE_SCHEDULER_AGENT','SYS','SYS$UMF','SYSBACKUP','SYSDG','SYSKM'
                                            ,'SYSRAC','SYSTEM','XDB','XS$NULL')
                        or USERNAME like 'APEX%'
                        or USERNAME like 'FLOWS%'
                        or USERNAME like 'OWB%'
                        or USERNAME like 'OLAP%' #' ;
        End if;


        -- Get SQL 
        l_query := q'#insert into TEST_RESULT_OBJ_DETAILS (
                        TEST_ID
                       ,DB_ENV 
                       ,DATABASE_NAME
                       ,CREATED
                       ,OBJ_OWNER
                       ,OBJ_NAME
                       ,OBJ_TYPE
                       ,OBJ_DEGREE
                       ) 
                     select '#' || t_test.TEST_ID || q'#'
                           ,'#' || t_test.DB_ENV  || q'#'
                           ,'#' || l_dbname       || q'#'
                           ,'#' || l_create       || q'#'
                            ,t.OBJ_OWNER
                            ,t.OBJ_NAME
                            ,t.OBJ_TYPE
                            ,t.OBJ_DEGREE 
                     from ( select  TABLE_NAME as OBJ_NAME ,trim(Upper(DEGREE)) as OBJ_DEGREE , OWNER as OBJ_OWNER , 'TABLE' as OBJ_TYPE
                              from  DBA_TABLES#' || l_link || q'#     
                             where  DEGREE is not NULL
                               and  Trim(DEGREE) not in  ('1','0')
                               and  OWNER not in ( select USERNAME from DBA_USERS#' || l_link || q'#  where #' || l_where || q'# )
                               and  OWNER not like 'APEX%' 
                            union
                            select  INDEX_NAME as OBJ_NAME , trim(Upper(DEGREE)) as OBJ_DEGREE , OWNER as OBJ_OWNER , 'INDEX' as OBJ_TYPE
                              from  DBA_INDEXES#' || l_link || q'#  
                             where  DEGREE is not NULL      
                               and  Trim(DEGREE) not in  ('1','0')
                               and  OWNER not in ( select USERNAME from DBA_USERS#' || l_link || q'#  where #' || l_where || q'# )
                               and  OWNER not like 'APEX%' 
                           ) t  #'
        ;

        exec_query (i_query => l_query, i_dbname => l_dbname);  

    END Get_TEST_RESULT_OBJ_DETAILS ;


/*--------------------------------------------------------------------------------- */
  -- Calling Procedure to retrieve ALL the Analytical data from each database 
  -- involved in this test
/*--------------------------------------------------------------------------------- */
    PROCEDURE Get_TEST_DATA (
             i_start  IN test_result_master.begin_time%TYPE
            ,i_end    IN test_result_master.end_time%TYPE
            ,i_desc   IN test_result_master.test_description%TYPE   
            ,i_label  IN test_result_master.best_test_for_release%TYPE DEFAULT NULL
            ,i_flag   IN test_result_master.daily_run_flag%TYPE DEFAULT 0
            ,i_env    IN test_result_master.db_env%TYPE DEFAULT NULL
            ,i_group  IN test_result_master.db_group%TYPE DEFAULT 'FULL'
            ,i_dbname IN varchar2 DEFAULT NULL
            ,i_retain IN test_result_master.retention%TYPE DEFAULT 'N'
            ,i_valid  IN test_result_master.validtest%TYPE DEFAULT 'N'
            ,i_mode   IN test_result_master.testmode%TYPE DEFAULT NULL
       ) AS
       no_env          EXCEPTION;
       t_test          TEST_RESULT_MASTER%ROWTYPE;
       l_new_testid    test_result_master.test_id%TYPE; 
       l_start         varchar2(20) ;
       l_end           varchar2(20) ;
       l_custDone      boolean := false ;

        -- Query to select all DBS for the group requested
        -- If the DB_NAME is supplied, it will retrieve data only for that one database
        cursor c_dbs ( p_env    test_result_master.db_env%TYPE
                      ,p_grp    test_result_master.db_group%TYPE
                      ,p_db     varchar2
           ) is
             select distinct DB_NAME, DB_TYPE
              from V_TEST_RESULT_DBS 
             where DB_GROUP = DECODE ( p_grp, 'DB' , 'FULL', p_grp )
               and DB_ENV = DECODE ( p_env, 'ALL', DB_ENV , p_env )
               and DB_ENV != 'PRD'
               and DB_NAME = NVL(Upper(p_db),DB_NAME) 
             order by DB_NAME ;      

    BEGIN
        -- Check the environment has been supplied
        if i_env is NULL and i_dbname is null then RAISE no_env; end if;

        -- Get the initialisation values 
        l_start := TO_CHAR(i_start,'DDMONYY-HH24:MI');
        l_end := TO_CHAR(i_end,'DDMONYY-HH24:MI');
        l_new_testid := REPORT_ADM.Format_TestID ( l_start, l_end, i_env, i_group, i_dbname );

        -- Fill the record to pass onto the calls
        t_test.TEST_ID := l_new_testid ;
        t_test.TEST_DESCRIPTION := i_desc ;
        t_test.BEGIN_TIME := i_start ;
        t_test.END_TIME := i_end ;
        t_test.BEST_TEST_FOR_RELEASE := i_label ;
        t_test.DAILY_RUN_FLAG := i_flag ;
        t_test.RETENTION := i_retain ;
        t_test.VALIDTEST := i_valid ;
        t_test.TESTMODE := i_mode ;

        -- Whne creating the TestId, a special case for the DBNAME supplied will be considered
        -- If supplied, the ENV and GRP would be reset as appropropriate and added to the TESTID
        t_test.DB_ENV   := REPORT_ADM.Parse_TestId(l_new_testid,'ENV') ;
        t_test.DB_GROUP := REPORT_ADM.Parse_TestId(l_new_testid,'GRP') ; 

        -- Write the MASTER row for this test ( only one row per TEST )
        Get_TEST_RESULT_MASTER (t_test) ;

        for v_dbs in c_dbs ( t_test.DB_ENV, t_test.DB_GROUP, i_dbname)
        loop
            -- Gather data into TEST RESULT tables for each Database included in this test
            get_TEST_RESULT_DB_STATS (t_test, v_dbs.DB_NAME);
            get_TEST_RESULT_SQL (t_test, v_dbs.DB_NAME);
            get_TEST_RESULT_SQLTEXT (t_test, v_dbs.DB_NAME);
            get_TEST_RESULT_SQLDETAILS (t_test, v_dbs.DB_NAME);
            get_TEST_RESULT_METRICS (t_test, v_dbs.DB_NAME);
            get_TEST_RESULT_METRICS_DET (t_test, v_dbs.DB_NAME);
            get_TEST_RESULT_RLW (t_test, v_dbs.DB_NAME);
            get_TEST_RESULT_WAIT_CLASS (t_test, v_dbs.DB_NAME);
            get_TEST_RESULT_SYSTEM_EVENT (t_test, v_dbs.DB_NAME);
            get_TEST_RESULT_AWR (t_test, v_dbs.DB_NAME);
            get_TEST_RESULT_ACTIVITY (t_test, v_dbs.DB_NAME);

            -- Gather Table/Index parallelism details
            Get_TEST_RESULT_OBJ_DETAILS (t_test, v_dbs.DB_NAME);

            -- Only for SAL database
            if SUBSTR(v_dbs.DB_NAME,1,3) = 'ISS' then
                get_TEST_RESULT_SAL_BASKET (t_test, v_dbs.DB_NAME); 
            end if;    

            -- Only for Chordiant DBs
            If v_dbs.DB_TYPE = 'CCS' then
                Get_TEST_RESULT_CACHE (t_test);
            end if;    

            -- This part will get done once for all CUSTOMER databases if part of the group requested
            if l_custDone = false and SUBSTR(v_dbs.DB_NAME,1,4) = 'DCU0' then
                l_custDone := true;
                get_TEST_RESULT_BILLING_RUN (t_test);
            end if;    
        end loop ;
    EXCEPTION 
        WHEN no_env THEN
            logger.write('Get_TEST_DATA - TestID : '|| t_test.TEST_ID || ' No environment supplied');
    END Get_TEST_DATA ;


PROCEDURE Get_DATA_POOL_ROWCOUNTS  AS
/*
  Called after N02 Dataprov which should be the last dataprov environment built
*/  

        l_query             varchar2(4000);

        cursor c_db is select database_name, pool_use_table
                       from HP_DIAG.DATAPROV_DATABASES
                       order by database_name;

    BEGIN

    for r_db in c_db loop
        
        -- Get SQL 
       l_query := 'insert into HP_DIAG.DATAPROV_POOL_ROW_COUNTS (
                      database_name,
                      datapool_name,
                      endpoint_name,
                      max_value,
                      burnable,
                      created)
	            select '''||r_db.database_name||''', substr(sequence_name, 2,30),dp.endpoint_name, max_value,cycle_flag, sysdate
                from dba_sequences@'||r_db.database_name||' us, dba_objects@'||r_db.database_name||' uo, dataprov.dprov_config@'||r_db.database_name||' dp
                where us.sequence_name = uo.object_name
                and ''S'' || dp.datapool_name = us.sequence_name
                and uo.object_type = ''SEQUENCE'' 
                and uo.object_name like ''S%''
                and trunc(uo.created) = trunc(sysdate)
                and us.sequence_owner = ''DATAPROV''
                and uo.owner = ''DATAPROV''' 
               ;
               dbms_output.put_line(l_query);
        hp_diag.report_utils.exec_query (i_query => l_query, i_dbname => r_db.database_name);  
    end loop;



    END Get_DATA_POOL_ROWCOUNTS ;



PROCEDURE Get_DATA_POOL_LOOPS  AS
/*
  Called just after midnight to record the day's pool use.
*/


        l_query             varchar2(4000);

        cursor c_db is select database_name, pool_use_table
                       from HP_DIAG.DATAPROV_DATABASES
                       order by database_name;

    BEGIN

    for r_db in c_db loop
        
        -- Get SQL 
        l_query := 'insert into HP_DIAG.DATAPROV_POOL_CYCLES (
                      database_name,
                      datapool_name,
                      counter,
                      loops,
                      max_value,
                      exceeds_maxvalue,
                      created)
	            select '''||r_db.database_name||''', fl.poolname, fl.counter, fl.loops, fl.max_value, fl.exceeds_maxvalue, fl.created
                    from DATAPROV.'||r_db.pool_use_table||'@'||r_db.database_name||' fl
                    where trunc(fl.created) = trunc(sysdate)-1' 
               ;

        hp_diag.report_utils.exec_query (i_query => l_query, i_dbname => r_db.database_name);  
    end loop;



    END Get_DATA_POOL_LOOPS ;


PROCEDURE Housekeep_DATA_POOL_Tables AS
   p_date date := add_months(sysdate,-3);
 BEGIN
    FOR part IN (
        SELECT table_name, partition_name , high_value, partition_position
        FROM user_tab_partitions
        WHERE table_name IN ('DATAPROV_POOL_ROW_COUNTS', 'DATAPROV_POOL_CYCLES')
    )
    LOOP
      if (to_date (substr (part.high_value, 11, 10), 'YYYY-MM-DD') <=
             trunc(p_date+1)) and part.PARTITION_POSITION != 1
      then
         execute immediate 'alter table '||part.table_name||' drop partition '||part.partition_name|| ' UPDATE INDEXES ';
      end if;

    END LOOP;
end Housekeep_DATA_POOL_Tables;


END REPORT_GATHER;
/
