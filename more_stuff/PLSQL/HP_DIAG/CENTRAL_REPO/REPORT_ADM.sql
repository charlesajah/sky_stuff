CREATE OR REPLACE PACKAGE REPORT_ADM
AS
    TYPE g_tvc2 IS TABLE OF VARCHAR2(4000) ;

    FUNCTION Get_DTM (
             i_testId IN test_result_master.test_id%TYPE
            ,i_mode   IN varchar2 default 'START' 
       ) RETURN varchar2;
       
    FUNCTION Parse_TestId (
             i_testId IN test_result_master.test_id%TYPE
            ,i_val    IN varchar2 default NULL 
       ) RETURN varchar2;

    FUNCTION Format_TestId (
             i_start  IN varchar2
            ,i_end    IN varchar2
            ,i_env    IN varchar2 DEFAULT 'ALL'
            ,i_group  IN varchar2 DEFAULT 'FULL'
            ,i_dbname IN varchar2 DEFAULT NULL
       ) RETURN varchar2;

    PROCEDURE Do_DeleteStats (
              i_testId   IN  TEST_RESULT_MASTER.test_id%TYPE
            );
            
   PROCEDURE Do_UpdateStatsDetails (
       i_testId   IN  TEST_RESULT_MASTER.test_id%TYPE 
      ,i_desc     IN  varchar2 default NULL
      ,i_release  IN  varchar2 default NULL
      ,i_flag     IN  varchar2 default NULL
      ,i_valid    IN  varchar2 default NULL
      ,i_mode     IN  varchar2 default NULL      
      ) ; 

   FUNCTION Get_AWR_Report (
        i_dbname  IN varchar2
      , i_testId  IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED ;

   PROCEDURE Create_AWR_HTML_files (
        i_cwd     IN varchar2
      , i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) ; 

    PROCEDURE Do_Stats_HouseKeeping (
        i_retain number default 60
      );
      
    PROCEDURE Do_Stats_Full_HouseKeeping  (
        i_retain number default 550
      );
      
END REPORT_ADM;
/


CREATE OR REPLACE PACKAGE BODY REPORT_ADM
AS

/*--------------------------------------------------------------------------------- */
-- Amalysis Reporting admin procedure & functions
-- Communly used to support all the other pakages and host communly used functionality
/*--------------------------------------------------------------------------------- */

    -- Where the HP~_DIAG repository is based on 
    g_repo  varchar2(20) := 'TCC021N';

/*--------------------------------------------------------------------------------- */
    -- Takes the TEST_ID format (DDMONYY-HH24MI_DDMONYY-HH24MI) and returns the date 
    -- in the following format : DDMONYY-HH24:MI   
/*--------------------------------------------------------------------------------- */
    FUNCTION Get_DTM (
             i_testId IN test_result_master.test_id%TYPE
            ,i_mode   IN varchar2 default 'START' 
       ) RETURN varchar2
    AS
       l_testId     test_result_master.test_id%TYPE;
       l_mode       varchar2(10);
       l_dtm        varchar2(30);
       l_dtm_tmp    varchar2(30);

    BEGIN
       l_testId := i_testId ;
       l_mode := i_mode ;

       if l_mode = 'START' then 
           select SUBSTR(l_testId,1,INSTR(l_testId,'_')-1) into l_dtm_tmp from dual ;
       else 
           select SUBSTR(l_testId,INSTR(l_testId,'_')+1,INSTR(l_testId,'_',2)-1) into l_dtm_tmp from dual ;
       end if ;    
       select SUBSTR(l_dtm_tmp,1,INSTR(l_dtm_tmp,'-')+2)||':'||substr(l_dtm_tmp,INSTR(l_dtm_tmp,'-')+3) into l_dtm from dual ;

       return l_dtm ;
    END Get_DTM;

/*--------------------------------------------------------------------------------- */
    -- Takes the TEST_ID format (DDMONYY-HH24MI_DDMONYY-HH24MI_DBENV_DBGROUP) 
    -- or the new Average results formant TEST_RESULT_<def>-<DBENV>-<DBGROUP>  
    -- ( nb: it has to be separated by Hyphons (-) !!
    -- and strips data as asked
    -- NULL is the default as returns the whole thing again ( pointless )
    -- TST : the original TEST_ID format as DDMONYY-HH24MI_DDMONYY-HH24MI
    -- ENV : the DB_ENV as XXX 
    -- GRP : the DB_GROUP as XXXX  
    -- DB  : the DATABASE NAME if applicable ( any literal that doesn't match the known groups )
    -- Updated to return GROUP = "DB" if the TESTID is for a single database 
/*--------------------------------------------------------------------------------- */
    FUNCTION Parse_TestId (
             i_testId IN test_result_master.test_id%TYPE
            ,i_val    IN varchar2 default NULL 
       ) RETURN varchar2
    AS
        l_testId     test_result_master.test_id%TYPE := null;
    BEGIN
        CASE WHEN SUBSTR(i_testId,1,11) !=  'TEST_RESULT' THEN
                CASE WHEN i_val = 'TST' THEN 
                         l_testid := REGEXP_SUBSTR(i_testId, '([^_]+_[^_]*)', 1, 1) ;
                     WHEN i_val = 'ENV' THEN 
                         if REGEXP_COUNT(i_testId,'_') > 2 then
                             l_testid := REGEXP_SUBSTR(i_testId, '^[^_]+_[^_]+_([^_]+)', 1, 1, NULL, 1) ;
                         else
                             select distinct db_env into l_testid from V_TEST_RESULT_DBS where DB_NAME = Upper(REGEXP_SUBSTR(i_testId, '^[^_]+_[^_]+_(.*)', 1, 1, NULL, 1));
                         end if ;
                     WHEN i_val = 'GRP' THEN 
                         if REGEXP_COUNT(i_testId,'_') > 2 then
                             l_testid := REGEXP_SUBSTR(i_testId, '^[^_]+_[^_]+_[^_]+_(.*)', 1, 1, NULL, 1) ;
                         else
                             l_testid := 'DB';
                         end if ;        
                     WHEN i_val = 'DB' THEN
                         if REGEXP_COUNT(i_testId,'_') > 2 then
                            l_testid := null;
                         else
                            l_testid := REGEXP_SUBSTR(i_testId, '^[^_]+_[^_]+_(.*)', 1, 1, NULL, 1) ;
                         end if;   
                     ELSE l_testid := null ;
                END CASE ;
             ELSE
                CASE WHEN i_val = 'TST' THEN 
                         l_testid := REGEXP_SUBSTR(i_testId, '^[^-]+') ;
                     WHEN i_val = 'ENV' THEN
                         if REGEXP_COUNT(i_testId,'-') > 1 then
                             l_testid := REGEXP_SUBSTR(i_testId, '-(.*?)-', 1, 1, NULL, 1);
                         else
                             select distinct db_env into l_testid from V_TEST_RESULT_DBS where DB_NAME = Upper(REGEXP_SUBSTR(i_testId, '^[^-]+-(.*)', 1, 1, NULL, 1));
                         end if ;  
                     WHEN i_val = 'GRP' THEN     
                         if REGEXP_COUNT(i_testId,'-') > 1 then
                             l_testid :=  REGEXP_SUBSTR(i_testId, '[^-]+-[^-]+-(.+)', 1, 1, NULL, 1);
                         else
                             l_testid := 'DB';
                         end if ;    
                     WHEN i_val = 'DB' THEN
                          if REGEXP_COUNT(i_testId,'-') > 1 then
                            l_testid := null;
                         else
                            l_testid := REGEXP_SUBSTR(i_testId, '^[^-]+-(.*)', 1, 1, NULL, 1) ;
                         end if;    
                     ELSE l_testid := null ;
                 END CASE ;     
        END CASE ; 
        return l_testid ;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            return 'XXX';
    END Parse_TestId;


/****  old to be deleted 


    -- Takes the TEST_ID format (DDMONYY-HH24MI_DDMONYY-HH24MI_DBENV_DBGROUP) 
    -- and strips data as asked
    -- NULL is the default as returns the whole things again ( poitless )
    -- TST : the original TEST_ID format as DDMONYY-HH24MI_DDMONYY-HH24MI
    -- ENV : the DB_ENV as XXX 
    -- GRP : the DB_GROUP as XXXX  
    -- DB  : the DATABASE NAME if applicable ( any literal that doesn't match the known groups )
    -- Updated to return GROUP = "DB" if the TESTID is for a single database 

    FUNCTION Parse_TestId (
             i_testId IN test_result_master.test_id%TYPE
            ,i_val    IN varchar2 default NULL 
       ) RETURN varchar2
    AS
        l_testId     test_result_master.test_id%TYPE := null;
    BEGIN
        CASE WHEN i_val = 'TST' THEN 
                l_testid := SUBSTR(i_testId,1,25) ;
             WHEN i_val = 'ENV' THEN 
                if REGEXP_COUNT(i_testId,'_') > 2 then
                    l_testid := SUBSTR(i_testId,27,3);
                else
                    select distinct db_env into l_testid from V_TEST_RESULT_DBS where DB_NAME = Upper(SUBSTR(i_testId,27));
                end if ;
             WHEN i_val = 'GRP' THEN 
                if REGEXP_COUNT(i_testId,'_') > 2 then
                    l_testid := SUBSTR(i_testId,31);
                else
                    l_testid := 'DB';
                end if ;        
             WHEN i_val = 'DB' THEN
                 if REGEXP_COUNT(i_testId,'_') > 2 then
                    l_testid := null;
                 else
                    l_testid := Upper(SUBSTR(i_testId,27));
                 end if;   
             ELSE l_testid := null ;
        END CASE ; 
        return l_testid ;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            return 'XXX';
    END Parse_TestId;

*******/

/*--------------------------------------------------------------------------------- */
    -- Takes the START DATE/TIME, END DATE/TIME, ENV and GROUP and returns the 
    -- formatted TEST ID : DDMONYY-HH24MI_DDMONYY-HH24MI_DBENV_DBGROUP
    -- START and END format exect as : DDMONYY-HH24:MI  ( as per Jenkins default )
/*--------------------------------------------------------------------------------- */
    FUNCTION Format_TestId (
             i_start  IN varchar2
            ,i_end    IN varchar2
            ,i_env    IN varchar2 DEFAULT 'ALL'
            ,i_group  IN varchar2 DEFAULT 'FULL'
            ,i_dbname IN varchar2 DEFAULT NULL
       ) RETURN varchar2
    AS
        l_testid    test_result_master.test_id%TYPE;
        l_start     varchar2(20);
        l_end       varchar2(20);
    BEGIN
        l_start := REPLACE(i_start,':') ;
        l_end   := REPLACE(i_end,':') ;
        if i_dbname is null then
            l_testid := l_start||'_'||l_end||'_'||Upper(i_env)||'_'||Upper(i_group);
        else 
            l_testid := l_start||'_'||l_end||'_'||Upper(i_dbname);
        end if ;
        return l_testid ;
    END Format_TestId;

/*--------------------------------------------------------------------------------- */
    -- Selects the AWR LOB column from TEST_RESULT_AWR and presents the results
    -- in a PIPELINE, line by line
/*--------------------------------------------------------------------------------- */
    FUNCTION Get_AWR_Report (
            i_dbname  IN varchar2
          , i_testId IN hp_diag.test_result_metrics.test_id%TYPE 
          ) RETURN g_tvc2 PIPELINED
    AS
       l_testId        hp_diag.test_result_metrics.test_id%TYPE ;

       l_dbname        varchar2(30);
       l_output        clob;

       l_row           varchar2(4000);
       l_offset        number := 1;
       l_amount        number ;
       l_length        number ; 
       l_buffer        varchar2(32767);

    BEGIN
       l_testId := i_testId ;
       l_dbname := i_dbname ;

       SELECT awr_report into l_output FROM hp_diag.test_result_awr WHERE test_id = l_testId AND database_name = l_dbname ;
       l_length := DBMS_LOB.getLength(l_output);
       while l_offset < l_length loop
          l_amount := LEAST ( DBMS_LOB.instr(l_output, chr(10), l_offset)-l_offset, 32767);
          if l_amount > 0 then
             DBMS_LOB.READ (l_output, l_amount, l_offset, l_buffer) ;
             l_offset := l_offset + l_amount + 1 ;
          else
             l_buffer := null;
             l_offset := l_offset + 1;
          end if;
          l_row := l_buffer ;
          PIPE ROW ( l_row ) ;
       end loop;

    END Get_AWR_Report;

/*--------------------------------------------------------------------------------- */
    -- Creates the HTML file for the AWR report from the TEST_RESULT_AWR table
    -- I'm not sure whether this one actually works!!!
/*--------------------------------------------------------------------------------- */
    PROCEDURE Create_AWR_HTML_files (
            i_cwd     IN varchar2
          , i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
          ) 
    AS
       l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;

       l_cdw           varchar2(150);
       l_output        clob;
       l_filename      varchar2(200);
       l_sysdate       varchar2(20);

       l_row           varchar2(4000);
       l_offset        number := 1;
       l_amount        number ;
       l_length        number ; 
       l_buffer        varchar2(32767);

       f1              utl_file.file_type;

    BEGIN
       l_testId1 := i_testId1 ;
       l_cdw := i_cwd ;
       select TO_CHAR ( SYSDATE , 'DDMMYY-HH24MI' ) into l_sysdate from dual; 
       execute immediate 'create or replace directory AWR_REPORT as ''' || l_cdw || '''';

       dbms_output.put_line ('Location : '|| l_cdw);
       dbms_output.put_line ('Date : '|| l_sysdate);


       For r1 in ( SELECT database_name, test_id FROM hp_diag.test_result_awr WHERE test_id = l_testId1 GROUP BY DATABASE_NAME, TEST_ID ) loop
           --l_filename := 'AWRRPT_' || R1.DATABASE_NAME || '_' || Get_Start_DTM(r1.test_id) || '_' || Get_End_DTM(r1.test_id) || '_GENERATED_' || l_sysdate || '.html';
           l_filename := 'AWRRPT_' || R1.DATABASE_NAME || '_' || r1.test_id || '_GENERATED_' || l_sysdate || '.html';
           dbms_output.put_line ('File name  : '|| l_filename);

           f1 := utl_file.fopen('AWR_REPORT', l_filename, 'W');
           SELECT awr_report into l_output FROM hp_diag.test_result_awr WHERE test_id = r1.test_id AND database_name = r1.database_name ;
           l_length := DBMS_LOB.getLength(l_output);
           while l_offset < l_length loop
              l_amount := LEAST ( DBMS_LOB.instr(l_output, chr(10), l_offset)-l_offset, 32767);
               if l_amount > 0 then
                  DBMS_LOB.READ (l_output, l_amount, l_offset, l_buffer) ;
                  l_offset := l_offset + l_amount + 1 ;
               else
                  l_buffer := null;
                  l_offset := l_offset + 1;
               end if;
               l_row := l_buffer ;
               utl_file.put_line(f1, l_row);

            end loop;

            utl_file.fclose(f1);
       End Loop;   

    END Create_AWR_HTML_files;


/*--------------------------------------------------------------------------------- */
    --  Deletes the TEST_ID suppied from the Repository tables ( TEST_RESULTS_xxx )
/*--------------------------------------------------------------------------------- */
    PROCEDURE Do_DeleteStats (
              i_testId   IN  TEST_RESULT_MASTER.test_id%TYPE
            ) AS
    -- It deletes all the rows associated to a given TEST_ID 
    -- The TEST_ID must exist in the TEST_RESULT_MASTER first, in order to be verified.

        l_testid    TEST_RESULT_MASTER%ROWTYPE ;
        l_sql       varchar2(4000) ;
        testid_not_found    exception ;

        -- Exclude all tables that are Global Temporary tables
        cursor c_tabs is 
           (  select t.table_name
                from user_tab_columns t
               where t.column_name = 'TEST_ID'  
                 and t.table_name not in ('GBT','GTT_SAL_BASKET','MYGTT','MYGTT2','RLWGTT')
                 and not exists ( select object_name from user_objects where object_name = t.table_name and object_type in ('VIEW') ) 
            ); 
        v_tabs   c_tabs%rowtype ;    

    BEGIN
        --logger.debug('Test Id received : '|| i_testId );
        l_testid  := REPORT_GATHER.Get_Test_Details(i_testid) ;

        -- Verify TEST_ID existance
        if l_testid.TEST_ID is NULL then
            raise testid_not_found ;
        end if;
        logger.debug('Deleting TestID : '||l_testid.TEST_ID) ;

        -- Delete rows per Table found in cursor
        for v_tabs in c_tabs
        loop
            l_sql := 'delete from ' || v_tabs.table_name || chr(10) ||
                     ' where TEST_ID = ''' || l_testid.TEST_ID || '''' ;
            execute immediate l_sql ; 
            --if v_tabs.table_name = 'TEST_RESULT_MASTER' then
            --   logger.debug('Table ' ||v_tabs.table_name ||' (' || sql%rowcount || ') rows deleted' ) ;
            --end if;    
        end loop ;
        commit ; 

    EXCEPTION 
        WHEN testid_not_found THEN
            dbms_output.put_line ('TEST_ID : '|| l_testid.TEST_ID ||' not found') ;
            logger.write('TEST_ID : '|| l_testid.TEST_ID ||' not found') ;
    END Do_DeleteStats ;



/*--------------------------------------------------------------------------------- 
    -- Updates the following fields within TEST_RESULT_MASTER ( and other TEST_RESULT_XX tables for the description )
    -- TEST_DESCRIPTION - In all tables that have this column and it's applicable
    -- BEST_TEST_FOR_RELEASE - In TEST_RESULT_MASTER
    -- It also sets the followig flags :
        RETENTION - In TEST_RESULT_MASTER - 'N' means it has ( no retention ) / 'Y' will stop the test from beaing deleteable
        VALIDTEST - In TEST_RESULT_MASTER - 'N' default, 'Y' means this test can be considered for the calculation of average values
        TESTMODE  - In TEST_RESULT_MASTER :
        Test Mode	
            Standard Test ( 2.5 hours long )	        S
            Extended Test ( 8 hours long )	            L
            Non-Standrad Test ( any other duration )	O
        Test Duration	
            Morning Test ( 6:30 - 9am )	                M
            Evening Test ( 8:30 - 11pm )	            E
            Ad-Hoc Text	                                A
        Batch Element	
            Nightly Batch Run ( Pre/post BDRF type of runs )	N
            Daytime Batch Run	                                D
            No batch 	                                        X
    -- NB : receiving a "NULL" string for BEST_TEST_FOR_RELEASE or TESTMODE will reset the current value to NULL
    -- Otherwise, receiving NULL values will result in no changes applied to the current value.
--------------------------------------------------------------------------------- */
    PROCEDURE Do_UpdateStatsDetails (
              i_testId   IN  TEST_RESULT_MASTER.test_id%TYPE 
             ,i_desc     IN  varchar2 default NULL
             ,i_release  IN  varchar2 default NULL
             ,i_flag     IN  varchar2 default NULL
             ,i_valid    IN  varchar2 default NULL
             ,i_mode     IN  varchar2 default NULL
            ) AS

        l_testid    TEST_RESULT_MASTER%ROWTYPE ;
        l_desc      varchar2(100) ;  -- New description for the test ID given
        l_release   varchar2(20) ;   -- Release TAG name for the test if applicacable ( updates column BEST_TEST_FOR_RELEASE in TEST_RESULT_MASTER )
        l_retention varchar2(1) ;    -- Flag indicating whether the test has been automatically generated or not ( RETENTION ). 'N' means it has ( no retention ) / 'Y' will stop the test from beaing deleteable
        l_valid     varchar2(1) ;    -- Flag to mark when the STATS collected are considered worthy ( valid )
        l_mode      varchar2(5) ;    -- Flags to state what type of test has been collected
        l_sql       varchar2(4000) ;
        testid_not_found    exception ;

        -- List all TABLES that have the TEST_DESCRIPTION column ( and it's applicable )
        cursor c_tabs is 
           ( select table_name
             from   user_tab_columns 
             where  column_name = 'TEST_DESCRIPTION'
             and    ( table_name like 'TEST_RESULT%' or table_name in ('SAL_BASKET_SIZES') )
             and    table_name not in ('ASU_TEST_RESULT_SQL','GTT_SAL_BASKET','MYGTT','MYGTT2','PGR_TEST_RESULT_SQL','RLWGTT') 
            ); 
        v_tabs   c_tabs%rowtype ;    

    BEGIN
        l_testid    := REPORT_GATHER.Get_Test_Details(i_testid) ;
        l_desc      := i_desc ; 
        l_release   := i_release ;
        l_retention := Upper(i_flag) ;
        l_valid     := Upper(i_valid) ;
        l_mode      := Upper(i_mode) ;

        -- Verify TEST_ID existance
        if l_testid.TEST_ID is NULL then
            raise testid_not_found ;
        end if;

        -- Update TEST_DESCRIPTION value
        if l_desc is not NULL then
            for v_tabs in c_tabs
            loop
                l_sql := 'update ' || v_tabs.table_name || 
                        ' set TEST_DESCRIPTION = ''' || l_desc || '''' ||
                        ' where TEST_ID = ''' || l_testid.TEST_ID || '''' ;

                execute immediate l_sql ; 
                --LOGGER.DEBUG ('Table ' ||v_tabs.table_name ||' (' || sql%rowcount || ') rows updated' ) ;
            end loop ;
        end if ;

        -- Update flags / columns
        update TEST_RESULT_MASTER
           set BEST_TEST_FOR_RELEASE = DECODE(Upper(l_release),NULL,BEST_TEST_FOR_RELEASE,'NULL',NULL,l_release)
              ,RETENTION = DECODE(l_retention,NULL,RETENTION,l_retention)
              ,VALIDTEST = DECODE(l_valid,NULL,VALIDTEST,l_valid)
              ,TESTMODE  = DECODE(l_mode,NULL,TESTMODE,'NULL',NULL,l_mode)
        where TEST_ID = l_testid.TEST_ID ;  

        commit ; 
    EXCEPTION 
        WHEN testid_not_found then
            LOGGER.WRITE ( i_testid ||' not found' ) ;
    END Do_UpdateStatsDetails ;

/*--------------------------------------------------------------------------------- */
    -- Deletes rows within all the TEST_RESULT_XXX tables for a given TESTID
    -- It deletes all the rows in all the STATS tables that are identified by the TEST_ID meeting the conditions 
    -- Anything is not set to be retained, would be deleted ! ( whether it's a daily run or not )
    -- The default retention is 2 months worth but this can be changed via a calling parameter
/*--------------------------------------------------------------------------------- */
    PROCEDURE Do_Stats_HouseKeeping (
              i_retain number default 60
              ) AS

        l_days      number ;       -- Number of days to retain
        l_sql       varchar2(4000) ;
        -- Get all TEST_IDs that meet the criteria    
        cursor c_dels is
            select TEST_ID 
              from TEST_RESULT_MASTER
             where BEGIN_TIME < sysdate - l_days
               and RETENTION = 'N'
               --and DAILY_RUN_FLAG = 1
               --and TEST_DESCRIPTION like '%AUTO AWR Stats Collection%' 
        ;
    BEGIN
        l_days := i_retain ;
        for v_dels in c_dels
        loop
            --logger.debug('Test Id identified : '||v_dels.TEST_ID);
            Do_DeleteStats ( v_dels.TEST_ID ) ;
        end loop ;
        commit ;     
    END Do_Stats_HouseKeeping ;

/*--------------------------------------------------------------------------------- */
    -- Deletes rows within all the TEST_RESULT_XXX tables for a given TESTID
    -- It housekeeps any rows within the repository that are older than 550 days ( 18 months )
    -- It will delete everything regardless the retention
/*--------------------------------------------------------------------------------- */
    PROCEDURE Do_Stats_Full_HouseKeeping (
              i_retain number default 550
              )
    AS

        l_days      number ;       -- Number of days to retain
        l_sql       varchar2(4000) ;

        -- Get all TEST_IDs that meet the criteria    
        cursor c_dels (p_days number)is
            select TEST_ID 
              from TEST_RESULT_MASTER
             where BEGIN_TIME < sysdate - p_days
               --and RETENTION != 'Y'
             order by BEGIN_TIME asc  
             ;
    BEGIN
        l_days := i_retain ; 
        for v_dels in c_dels (l_days)
        loop
            --logger.debug('Test Id identified : '||v_dels.TEST_ID);
            Do_DeleteStats ( v_dels.TEST_ID ) ;
        end loop ;
        commit ;     
    END Do_Stats_Full_HouseKeeping ;

END REPORT_ADM;
/
