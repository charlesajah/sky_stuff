CREATE OR REPLACE PACKAGE  REPORT_UTILS AS

    PROCEDURE exec_select (  
        i_query  IN varchar2  
       ,i_dbname IN varchar2 default NULL  
       ,i_result OUT number 
        ) ;

    PROCEDURE exec_query ( 
            i_query  IN varchar2 
           ,i_dbname IN varchar2 default NULL 
        ) ;
    
    PROCEDURE exec_cursor ( 
        i_query  IN varchar2 
       ,i_dbname IN varchar2 default NULL 
       ,i_cursor OUT SYS_REFCURSOR 
        ) ;
    
    PROCEDURE close_dblink ( i_link   IN varchar2 ) ;
    FUNCTION Get_DB_Ver ( i_dbname IN varchar2 ) RETURN varchar2 ;
    PROCEDURE Changed_Objects ( i_db_env  IN varchar2) ;   
    PROCEDURE CresteMasterTESTIDs ;
    PROCEDURE RefreshMViews ( i_mview   IN varchar2 default NULL ) ;
    FUNCTION Is_avg ( i_testId IN TEST_RESULT_MASTER.test_id%TYPE ) RETURN varchar2 ; 
     
END REPORT_UTILS;
/


CREATE OR REPLACE PACKAGE BODY REPORT_UTILS AS


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
            --logger.debug('close_dblink - '||l_dbname|| ' Link : '|| i_link || ' : database link is not open');
            logger.write('close_dblink - '||'database link is not open : '||l_dbname ) ;
        WHEN OTHERS THEN
            --logger.debug('close_dblink - '||l_dbname|| ' Link : '|| i_link || ' : ' ||SQLCODE||' : '||SUBSTR(SQLERRM, 1, 500));
            logger.write('close_dblink - '||l_dbname|| ' : ' ||SQLCODE||' : '||SUBSTR(SQLERRM, 1, 500)) ;
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
            --logger.debug('exec_cursor - connection description for remote database not found for : '||i_dbname||' Query : '|| i_query ) ;
            logger.write('exec_cursor - connection description for remote database not found for : '||i_dbname ) ;
            rollback;
            close i_cursor ;
            close_dblink(i_dbname);
        WHEN OTHERS THEN
            --logger.debug('exec_cursor - '||i_dbname|| ' : ' ||SQLCODE||' : '||SUBSTR(SQLERRM, 1, 500) || ' Query : '|| i_query);
            logger.write('exec_cursor - '||i_dbname|| ' : ' ||SQLCODE||' : '||SUBSTR(SQLERRM, 1, 500)) ;
            rollback;
            close i_cursor ;
            close_dblink(i_dbname);
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

        -- define error handling exceptions
        dblink_not_working EXCEPTION;
        PRAGMA EXCEPTION_INIT(dblink_not_working, -2019);
    BEGIN 
        l_dbname := i_dbname ;
        -- If the dbname supplied has the "@", remove it
        if l_dbname is not NULL then
             l_dbname := SUBSTR(l_dbname,INSTR(l_dbname,'@')+1,length(l_dbname));
        end if ; 
        execute immediate i_query; 
        commit;
        --logger.debug (l_dbname||' : '||SUBSTR(i_query,1,100)) ;
        close_dblink(l_dbname);

    EXCEPTION
        WHEN dblink_not_working THEN
            --logger.debug('exec_query - connection description for remote database not found for : '||l_dbname||' Query : '|| i_query ) ;
            logger.write('exec_query - connection description for remote database not found for : '||l_dbname ) ;
            rollback;
            close_dblink(l_dbname);
        WHEN OTHERS THEN
            --logger.debug('exec_query - '||l_dbname|| ' : ' ||SQLCODE||' : '||SUBSTR(SQLERRM, 1, 500) || ' Query : '|| i_query);
            logger.write('exec_query - '||l_dbname|| ' : ' ||SQLCODE||' : '||SUBSTR(SQLERRM, 1, 500)) ;  
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
            --logger.debug('exec_ddl_query - connection description for remote database not found for : '||l_dbname||' Query : '|| i_query ) ;
            logger.write('exec_ddl_query - connection description for remote database not found for : '||l_dbname ) ;
            rollback;
            close_dblink(l_dbname);
        WHEN table_exist THEN
            --logger.debug('Table exist for '||l_dbname||' - '||i_query);
            rollback;
            close_dblink(l_dbname);           
        WHEN OTHERS THEN
            --logger.debug('exec_ddl_query - '||l_dbname|| ' : ' ||SQLCODE||' : '||SUBSTR(SQLERRM, 1, 500)|| ' Query : '|| i_query);
            logger.write('exec_ddl_query - '||l_dbname|| ' : ' ||SQLCODE||' : '||SUBSTR(SQLERRM, 1, 500)) ;  
            rollback;
            close_dblink(l_dbname);
    END exec_ddl_query; 

/*--------------------------------------------------------------------------------- */
     -- Execute query via dynamic sql. Return NUMBER Value
     -- COMMIT and CLOSE DBLINK is handled within this procedure
/*--------------------------------------------------------------------------------- */ 
    PROCEDURE exec_select ( 
            i_query  IN varchar2 
           ,i_dbname IN varchar2 default NULL 
           ,i_result OUT number
        ) 
    AS
        l_dbname    varchar2(25);
        l_query     varchar2(16000);
        l_result    number;

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
        execute immediate l_query into l_result; 
        commit;
        close_dblink(l_dbname);
        i_result := l_result;
    EXCEPTION
        WHEN no_query THEN
            logger.write('exec_select - No query supplied');          
        WHEN dblink_not_working THEN
            logger.write('exec_select - connection description for remote database not found for : '||l_dbname ) ;
            rollback;
            close_dblink(l_dbname);
        WHEN OTHERS THEN
            logger.write('exec_select : '|| SQLCODE ||' : '|| SUBSTR(SQLERRM,1,500) || ' DB : ' || i_dbname|| ' : ' || ' Query : ' || SUBSTR(l_query,1,3400));
            --logger.debug('exec_select : '|| SQLCODE ||' : '|| SUBSTR(SQLERRM,1,500) || ' DB : ' || i_dbname|| ' : ' || ' Query : ' || SUBSTR(l_query,1,3400));
            rollback;
            close_dblink(l_dbname);
    END exec_select; 

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
        if i_dbname = REPORT_GATHER.g_repo or i_dbname is NULL then l_link := ''; else l_link := '@'||i_dbname ; end if ; 
                
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
            close_dblink(l_dbname);
        end if;    
        return l_ver ;
    END Get_DB_Ver;


/*****************************************************************************************************/
-- Changed_Objects
-- Reports on the Application Schema based objects that have been modified within the last 24h
/*****************************************************************************************************/
    PROCEDURE Changed_Objects (
        i_db_env  IN varchar2
        ) 
    IS
    
        t_res               dba_objects%rowtype;
        v_cursor            sys_refcursor ;
        l_query             varchar2(4000);
        l_count_query       varchar2(4000); 
        l_link              varchar2(25);
        l_dbname            varchar2(20);
        l_owner             varchar2(128); 
        l_object_name       varchar2(128);
        l_object_type       varchar2(23);
        l_last_ddl_time     date;
        l_stamp             varchar2(29);
        l_created           date; 
        l_count             number;
        
        cursor c_changes (p_db_env varchar2) is
            select db_name 
              from HP_DIAG.TEST_RESULT_DBS
             where db_env = i_db_env
               and db_name not like 'OMS%'
             order by db_name;
    
    BEGIN
        for r_changes in c_changes(i_db_env) 
        loop
            l_link := '@'||r_changes.db_name;
    
            l_count_query := q'#select count(*)
                                  from dba_objects#'||l_link||q'#
                                 where to_date(timestamp, 'YYYY-MM-DD:HH24:MI:SS') >= (trunc(sysdate) -1)
                                   and object_type not in ( 'SYNONYM', 'SEQUENCE', 'JOB')
                                   and object_type not like '%PARTITION'
                                   and owner NOT IN ('ANONYMOUS','APEX_030200','APEX_040200','APEX_200200','APEX_LISTENER','APEX_PUBLIC_USER','APEX_REST_PUBLIC_USER',
                                                     'APPQOSSYS','AUDSYS','CTXSYS','DATAPROV','DBSNMP','DIP','EXFSYS','FLOWS_FILES','FLOWS_030100','FLOWS_020000',
                                                     'FLOWS_020200','HP_DIAG','HOUSEKEEPING','PERFMON','MDDATA','MDSYS','OLAPSYS','ORACLE_OCM','ORDDATA','ORDPLUGINS',
                                                     'ORDSYS','ORDS_PUBLIC_USER','OUTLN','OWBSYS','OWBSYS_AUDIT','PUBLIC','QUEST','SI_INFORMTN_SCHEMA','SPATIAL_CSW_ADMIN_USR',
                                                     'SPATIAL_WFS_ADMIN_USR','SYS','SYSMAN','SYSTEM','WMSYS','XDB','XS$NULL','ORDS_METADATA','IGNITE_M','APEX_050000',
                                                     'APEX_050100','TSMSYS','PERFSTAT','QUANTIX','SYSBACKUP','SYSDG','SYSKM','OJVMSYS','REMOTE_SCHEDULER_AGENT','LBACSYS',
                                                     'DVF','DVFSYS','DBSFWUSER','DVSYS','GSMADMIN_INTERNAL','DMSYS','DBMON_AGENT_USER','DBSFWUSER','DLP_OWNER','GGSYS',
                                                     'GSMADMIN_INTERNAL','GSMCATUSER','GSMUSER','LWS_DLP_USER','LWS_NPS_USER','PDBADMIN','REMOTE_SCHEDULER_AGENT','SYS$UMF',
                                                     'SYSRAC','SMTEST','TOOLS')#';
            
            exec_select(l_count_query, r_changes.db_name, l_count);
            
            if l_count > 0 then 
                dbms_output.put_line('h3. '||r_changes.db_name||' - Number of changed objects '||l_count);
            
                l_query := q'#select owner, object_name, object_type, last_ddl_time, to_char(to_date(timestamp, 'YYYY-MM-DD:HH24:MI:SS'), 'DD-MON-YYYY HH24:MI:SS') stampy , created
                                from dba_objects#'||l_link||q'#
                               where to_date(timestamp, 'YYYY-MM-DD:HH24:MI:SS') >= (trunc(sysdate) -1)
                                 and object_type not in ( 'SYNONYM', 'SEQUENCE', 'JOB')
                                 and object_type not like '%PARTITION'
                                 and owner NOT IN ('ANONYMOUS','APEX_030200','APEX_040200','APEX_200200','APEX_LISTENER','APEX_PUBLIC_USER','APEX_REST_PUBLIC_USER',
                                                     'APPQOSSYS','AUDSYS','CTXSYS','DATAPROV','DBSNMP','DIP','EXFSYS','FLOWS_FILES','FLOWS_030100','FLOWS_020000',
                                                     'FLOWS_020200','HP_DIAG','HOUSEKEEPING','PERFMON','MDDATA','MDSYS','OLAPSYS','ORACLE_OCM','ORDDATA','ORDPLUGINS',
                                                     'ORDSYS','ORDS_PUBLIC_USER','OUTLN','OWBSYS','OWBSYS_AUDIT','PUBLIC','QUEST','SI_INFORMTN_SCHEMA','SPATIAL_CSW_ADMIN_USR',
                                                     'SPATIAL_WFS_ADMIN_USR','SYS','SYSMAN','SYSTEM','WMSYS','XDB','XS$NULL','ORDS_METADATA','IGNITE_M','APEX_050000',
                                                     'APEX_050100','TSMSYS','PERFSTAT','QUANTIX','SYSBACKUP','SYSDG','SYSKM','OJVMSYS','REMOTE_SCHEDULER_AGENT','LBACSYS',
                                                     'DVF','DVFSYS','DBSFWUSER','DVSYS','GSMADMIN_INTERNAL','DMSYS','DBMON_AGENT_USER','DBSFWUSER','DLP_OWNER','GGSYS',
                                                     'GSMADMIN_INTERNAL','GSMCATUSER','GSMUSER','LWS_DLP_USER','LWS_NPS_USER','PDBADMIN','REMOTE_SCHEDULER_AGENT','SYS$UMF',
                                                     'SYSRAC','SMTEST','TOOLS')
                               order by 1, 2#';
            
                exec_cursor (i_query => l_query, i_dbname => l_link, i_cursor => v_cursor) ; 
                
                if v_cursor%ISOPEN then
                    dbms_output.put_line('||Owner||Object Name||Object Type||Last DDL Time||Timestamp||Created Date||{color:blue} Changed {color} or {color:red} New {color}|');
                    loop
                        fetch v_cursor into l_owner, l_object_name, l_object_type, l_last_ddl_time, l_stamp, l_created;
                        exit when v_cursor%notfound;
                        dbms_output.put_line('|'||l_owner||'|'||l_object_name||'|'||l_object_type||'|'||l_last_ddl_time||'|'||l_stamp||'|'||l_created||'|'||case when l_created >= trunc(sysdate)-1 then '{color:red} NEW {color}' else '{color:blue} CHANGED {color}' end||'|');
                    end loop;    
                    close v_cursor;
                    commit;
                    close_dblink(l_link);
                    dbms_output.put_line('\\');
                else
                    dbms_output.put_line('Something wrong with v_cursor. Not open');
                end if;   
            end if;
        end loop;
    end Changed_Objects;



/*----------------------------------------------------------------------------------------------------------------------------- */
 -- This Procedure will update the TEST_RESULT_MASTER table with the MASTER TEST_ID to be used for all the TEST_RESULT views
 -- created for the purpose of get the average values.
 -- It will run after each data collection and when data need refresh and avoids duplications ( it can be run at leisure )
 -- If the master TEST_ID already exists it gets skipped.
/*----------------------------------------------------------------------------------------------------------------------------- */   
    PROCEDURE CresteMasterTESTIDs 
    AS
        l_testid    TEST_RESULT_MASTER.test_id%TYPE ;
        l_env       TEST_RESULT_MASTER.db_env%TYPE ;
        l_grp       TEST_RESULT_MASTER.db_group%TYPE ; 
        l_mode      TEST_RESULT_MASTER.testmode%TYPE ; 
        
        -- This is the STANDARD Name Prefix, followed by the Environment and then the group 
        -- Format : TEST_RESULT_AVG-<env>-<grp> or TEST_RESULT_AVG-<env>-<grp>-<testmode> if the TESTMODE is not NULL       
        l_prefix    varchar2(20) :=  'TEST_RESULT_AVG' ;

        -- Select all VIEWS that exist that have the columnm TEST_ID and starts with the name as TEST_RESULT...
        cursor c_ids is 
           select distinct db_env, db_group, testmode
             from TEST_RESULT_MASTER
            where db_env||'-'||db_group||DECODE(NVL(testmode,'NONE'),'NONE','','-'||testmode) not in ( 
                select t.db_env||'-'||t.db_group||DECODE(NVL(testmode,'NONE'),'NONE','','-'||testmode)
                  from TEST_RESULT_MASTER t 
                 where t.test_id = l_prefix || '-' || db_env || '-' || db_group || DECODE(NVL(testmode,'NONE'),'NONE','','-'||testmode)
                )
           ; 
       
    BEGIN
        for v_ids in c_ids
        loop
            if v_ids.testmode is null then
                l_testid := l_prefix || '-' || v_ids.db_env || '-' || v_ids.db_group ;
            else                
                l_testid := l_prefix || '-' || v_ids.db_env || '-' || v_ids.db_group || '-' || v_ids.testmode ;
            end if;
            logger.debug ('Test Id being added : '|| l_testid );
            insert into TEST_RESULT_MASTER 
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
            values    
                ( l_testid
                 ,v_ids.db_env
                 ,v_ids.db_group
                 ,'Average Values for the last 12 Valid Tests'
                 ,sysdate
                 ,sysdate
                 ,null
                 ,0
                 ,'Y'
                 ,'N'
                 ,v_ids.testmode
                ) ;
        end loop ;
        commit ; 

    EXCEPTION 
        WHEN OTHERS THEN
            logger.debug( SQLCODE ||' : '|| SUBSTR(SQLERRM,1,500) );
    END CresteMasterTESTIDs ;



/*----------------------------------------------------------------------------------------------------------------------------- */
    -- RefreshMViews 
    -- It kicks off the process to refresh all the materialzed views that are linked to the TEST_RESULT Repository.
/*----------------------------------------------------------------------------------------------------------------------------- */   
    PROCEDURE RefreshMViews ( i_mview   IN varchar2 default NULL )
    AS
        -- Get all the MV assocaited to the Repo data to be refresehd
        cursor c_mviews is 
           ( select t.table_name viewname
              from user_tab_columns t
              join user_objects o on ( o.object_name = t.table_name )
             where t.column_name = 'TEST_ID'  
               and t.table_name = NVL ( i_mview , t.table_name )
               and o.object_type = 'MATERIALIZED VIEW'    
            ) 
         ;   
        l_sql   varchar2(1000) ;
    BEGIN
        For v_mviews in c_mviews 
        Loop
            l_sql := q'#DBMS_MVIEW.REFRESH('#' || v_mviews.viewname || q'#', 'C')#' ;
            REPORT_UTILS.exec_query(l_sql) ;
            LOGGER.debug ('Refreshing Materialized View '|| v_mviews.viewname ) ;
        End Loop;   
    EXCEPTION
         WHEN OTHERS THEN
            logger.debug( SQLCODE ||' : '|| SUBSTR(SQLERRM,1,500) );       
    END RefreshMViews ;
 
 
 /*--------------------------------------------------------------------------------- */
    -- Is_avg
    -- Returns 'Y' if the TEST_ID is a generic TEST_ID for Average values
    -- and returns "N" if it's an standard TEST_ID
/*--------------------------------------------------------------------------------- */    

    FUNCTION Is_avg ( i_testId IN TEST_RESULT_MASTER.test_id%TYPE ) RETURN varchar2
    IS
        l_ret   varchar2(1) := 'N' ;
    BEGIN
        if substr(Upper(i_testId),1,15) = 'TEST_RESULT_AVG' then l_ret := 'Y'; end if;
        return l_ret ;
    END Is_avg;    

 
    

end REPORT_UTILS;
/
