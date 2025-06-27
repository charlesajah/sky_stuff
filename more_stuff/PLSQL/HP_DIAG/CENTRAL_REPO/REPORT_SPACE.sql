CREATE OR REPLACE PACKAGE         REPORT_SPACE
/*
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
    -- Where the HP~_DIAG repository is based on 
    g_repo  varchar2(20) := 'TCC021N';

    FUNCTION get_overall_space  
        ( i_env       varchar2 default 'ALL'
         ,i_grpname   varchar2 default 'FULL'
        ) RETURN g_tvc2 PIPELINED ;

    PROCEDURE get_db_schema_space         
        ( i_env       varchar2 default 'ALL'
         ,i_grpname   varchar2 default 'FULL'
        ) ;

    PROCEDURE get_db_ts_space 
        ( i_env       varchar2 default 'ALL'
         ,i_grpname   varchar2 default 'FULL'
        ) ;

    PROCEDURE Do_Gather_Space_Mgmt_Data 
        ( i_env       varchar2 default 'ALL'
         ,i_grpname   varchar2 default 'FULL'
        ) ;

    PROCEDURE single_db_schema_space
        ( p_db_name in varchar2
        , i_env       varchar2 default 'ALL'
        ) ;

END REPORT_SPACE ;
/


CREATE OR REPLACE PACKAGE BODY         REPORT_SPACE
AS


    /************************* NON_PUBLISHED PROCEDURES/FUNCTIONS ************************************/
    
    PROCEDURE single_db_schema_space        
            ( p_db_name in varchar2
            , i_env       varchar2 default 'ALL'
        ) as
        l_head varchar2(8000);
        l_sql  varchar2(8000);
        l_cnt  number;
        l_env  varchar2(4);
        l_tmp varchar2(30);
    
        TYPE info_rt  IS RECORD ( row_data     VARCHAR2 (8000));
        TYPE info_aat IS TABLE OF info_rt INDEX BY PLS_INTEGER;
        l_info   info_aat;  
    
        -- Decalre exceptions
        table_does_not_exist exception;  
        PRAGMA EXCEPTION_INIT(table_does_not_exist, -00942);
    
    BEGIN
        -- We need the env name when concurrent runs of this procedure are running 
        -- different TEMP tables will need to be created
        l_env := i_env;
        l_tmp := 'TMP_SEG_SPACE_INFO_'||l_env ;
    
        dbms_output.put_line('h5. Schema Growth for ' || upper(p_db_name)) ;
        dbms_output.put_line('{chart:dataOrientation=vertical|categoryLabelPosition=down45|rangeAxisLowerBound=0|dateFormat=dd/mm|height=500|orientation=vertical|stacked=true|timePeriod=Day|timeSeries=true|title=' || upper(p_db_name) || '|type=area|width=1800|xLabel=Date|yLabel=Space (GB)}' ) ;
        WITH q AS (
                   select * 
                   from (select schema_name, max(run_date) as run_date, max(used_gb) as used_gb
                           from NFT_SEG_SPACE 
                          where db_name = upper(p_db_name)
                            and used_gb > 1
                            and run_date = (select max(run_date) from NFT_SEG_SPACE where db_name = upper(p_db_name) group by db_name)
                          group by schema_name  
                          order by used_gb desc) 
                  where rownum < 21 )               
        SELECT '|| Run Date || ' || LISTAGG ( schema_name  , ' || ' ) WITHIN GROUP ( ORDER BY schema_name ) ||'||' into l_head FROM q ;
    
        if length(l_head) > 17 then 
            dbms_output.put_line( l_head ) ;  
    
            WITH q AS (
                   select * 
                   from (select schema_name, max(run_date) as run_date, max(used_gb) as used_gb
                           from NFT_SEG_SPACE 
                          where db_name = upper(p_db_name)
                            and used_gb > 1
                            and run_date = (select max(run_date) from NFT_SEG_SPACE where db_name = upper(p_db_name) group by db_name)
                          group by schema_name  
                          order by used_gb desc) 
                  where rownum < 21 )   
            SELECT LISTAGG ( ''''|| schema_name  , ''', ' ) WITHIN GROUP ( ORDER BY schema_name ) || '''' into l_head FROM q ;
    
            -- Drop table before extracting data
            BEGIN
                l_sql := 'drop table '||l_tmp ;
                execute immediate l_sql;
            EXCEPTION WHEN table_does_not_exist THEN null;
            END ;
    
            l_sql := 'create table '||l_tmp||' as'||
                     ' with q as ( select to_char(RUN_DATE,''dd/mm'') RUN_DATE, SCHEMA_NAME, max(USED_GB) USED_GB '||
                     ' from HP_DIAG.NFT_SEG_SPACE'||
                     ' where DB_NAME = ''' || upper(p_db_name) || ''''||
                     ' and RUN_DATE > add_months(sysdate,-3)'||
                     ' group by RUN_DATE, SCHEMA_NAME '||
                     ' order by RUN_DATE) '||
                     ' select * from  q pivot (max(USED_GB) for (SCHEMA_NAME) IN ( ' || l_head || '))';
            execute immediate l_sql;
    
            l_sql := 'with q as (select column_id, column_name from user_Tab_columns where table_name = '''||l_tmp||''' order by column_id)'||
                     ' SELECT LISTAGG ( ''|| "''|| column_name  , ''" || ''''||''''''||'''' ) WITHIN GROUP ( ORDER BY column_id ) || ''"''' ||
                     ' FROM q' ;
            --dbms_output.put_line (l_sql);
            execute immediate l_sql INTO l_head ;        
    
            l_sql := 'select ''||'' ' || l_head || '|| '''||'||'' from '||l_tmp||' order by to_date(RUN_DATE, ''dd/mm'')';
            execute immediate l_sql bulk collect into l_info;
            FOR indx IN 1 .. l_info.COUNT
            LOOP
                DBMS_OUTPUT.put_line (l_info(indx).row_data);
            END LOOP;
    
            -- Drop table after gather has completed
            BEGIN
                l_sql := 'drop table '||l_tmp ;
                execute immediate l_sql;
            EXCEPTION WHEN table_does_not_exist THEN null;
            END ;
        end if;
        dbms_output.put_line('{chart}') ;
    end single_db_schema_space;
    
    
    PROCEDURE single_db_tablespace_space(p_db_name in varchar2) as
        l_head varchar2(8000);
        l_sql  varchar2(8000);
        l_cnt  number;
    
        TYPE info_rt IS RECORD ( row_data     VARCHAR2 (8000));
        TYPE info_aat IS TABLE OF info_rt INDEX BY PLS_INTEGER;
        l_info   info_aat;  
    BEGIN
        dbms_output.put_line('h5. Tablespace Growth for ' || upper(p_db_name)) ;
        dbms_output.put_line(' || Tablespace Name || Minimum Size (GB) || Maximum Size (GB) || Increase (GB) || Growth || Days to Die ||' ) ;
        for r1 in (select '|' || ts_name || 
                          '|' || min_gb || 
                          '|' || max_gb || 
                          '|' || round(max_gb-min_gb,2) ||
                          '|{status:colour=' || growth_stat || '|title=' || pct_growth || '}'||
                          '|{status:colour=' || d2d_stat || '|title=' || d2d  || '}|'as chart_row
                     from ( select ts_name
                                   , min(round(allocated_gb,2)) min_gb
                                   , max(round(allocated_gb,2)) max_gb 
                                   , round(((max(allocated_gb)-min(allocated_gb))/max(allocated_gb)),4)*100 || '%' pct_growth
                                   , case when round(((max(allocated_gb)-min(allocated_gb))/max(allocated_gb)),4)*100 < 5 then 'Green' 
                                          when round(((max(allocated_gb)-min(allocated_gb))/max(allocated_gb)),4)*100 between 5 and 10 then 'Yellow' 
                                          else 'Red' end as growth_stat, max(days_to_die) keep(dense_rank last order by run_date) as d2d
                                   , case when max(days_to_die) keep(dense_rank last order by run_date) <= 2 then 'Red'
                                          when max(days_to_die) keep(dense_rank last order by run_date) between 3 and 6 then 'Yellow'
                                          else 'Green' end as d2d_stat
                              from v_db_ts_alloc
                             where db_name = upper(p_db_name)
                               and ts_name not in ('SYSTEM','SYSAUX','SYSAUX_DATA_AUTO_01','TOOLS','TOOLS_AUTO_01','UNDOTBS','UNDOTBS1','UNDOTBS_RECO','USERS','USERS_AUTO_01')
                               and run_date > add_months(trunc(sysdate),-1)
                             group by ts_name order by ts_name)
                  )
        loop
            dbms_output.put_line ( r1.chart_row ) ;
        end loop;
    end single_db_tablespace_space;
    
    /************************* END OF NON_PUBLISHED PROCEDURES/FUNCTIONS ************************************/
    
    
    
    FUNCTION get_overall_space  
            ( i_env       varchar2 default 'ALL'
             ,i_grpname   varchar2 default 'FULL'
            ) RETURN g_tvc2 PIPELINED
    AS
        l_str   varchar2(100);
        l_env   varchar2(10);
        l_group varchar2(10);
    BEGIN
        -- Initialised values
        l_env   := Upper(i_env) ;
        l_group := Upper(i_grpname) ;
    
        -- Get date 3 months in the past from today's date
        select to_char(min(add_months(sysdate,-3)),'dd/mm/yyyy')
        into l_str
        from hp_diag.v_db_summary;
    
        PIPE ROW ('{toc:type=list}');
        PIPE ROW ('h3. Time Window');
        PIPE ROW ('Start Date: ' || l_str );
        PIPE ROW ('End Date: ' || to_char(sysdate,'dd/mm/yyyy'));
    
        PIPE ROW ('h3. Database Growth');
        for r1 in (select distinct a.DB_NAME 
                     from v_db_summary a
                     join v_test_result_dbs db on (db.DB_NAME = a.DB_NAME)
                    where db.DB_ENV = DECODE ( l_env, 'ALL', db.DB_ENV , l_env )
                      and db.DB_ENV != 'PRD'
                      and db.DB_GROUP = l_group
                    order by a.DB_NAME)
        loop
            PIPE ROW ( 'h5. DB Growth for ' || r1.db_name) ;
            PIPE ROW ( '{chart:dataOrientation=vertical|categoryLabelPosition=down45|rangeAxisLowerBound=0|dateFormat=dd/MM/yyyy HH:mm|height=500|orientation=vertical|stacked=true|timePeriod=Day|timeSeries=true|title=' || r1.db_name || '|type=area|width=1800|xLabel=Date|yLabel=Space (GB)}' ) ;
            PIPE ROW ( ' || DATE_TIME || USED_GB || FREE_GB ||' ) ;
            for r2 in ( select '|' || to_char(run_date, 'dd/mm') || '|' || used_gb || '|' || free_gb || '|'  as chart_row
                          from hp_diag.v_db_summary 
                         where db_name = r1.db_name 
                           and run_date > to_date(l_str,'dd/mm/yyyy')
                         order by run_date)
            loop
                PIPE ROW ( r2.chart_row ) ;
            end loop;
            PIPE ROW ('{chart}') ;
        end loop;
    END get_overall_space ;
    
    
    PROCEDURE get_db_schema_space 
            ( i_env       varchar2 default 'ALL'
             ,i_grpname   varchar2 default 'FULL'
            ) 
    AS
        l_env   varchar2(10);
        l_group varchar2(10);
        l_str  varchar2(8000);
    BEGIN
        -- Initialised values
        l_env   := Upper(i_env) ;
        l_group := Upper(i_grpname) ;
    
    
        select to_char(min(add_months(sysdate,-3)),'dd/mm/yyyy')
          into l_str
          from hp_diag.v_db_summary;
    
        dbms_output.put_line('{toc:type=list}');
        dbms_output.put_line('h3. Time Window');
        dbms_output.put_line('Start Date: ' || l_str );
        dbms_output.put_line('End Date: ' || to_char(sysdate,'dd/mm/yyyy'));
    
        dbms_output.put_line('h3. Schema Growth');
    
        for r1 in (select distinct a.DB_NAME 
                     from v_db_summary a
                     join v_test_result_dbs db on (db.DB_NAME = a.DB_NAME)
                    where db.DB_ENV = DECODE ( l_env, 'ALL', db.DB_ENV , l_env )
                      and db.DB_ENV != 'PRD'
                      and db.DB_GROUP = l_group
                    order by a.DB_NAME)
        loop
            single_db_schema_space(r1.db_name, i_env);
        end loop;
    END get_db_schema_space ;
    
    
    
    PROCEDURE get_db_ts_space 
            ( i_env       varchar2 default 'ALL'
             ,i_grpname   varchar2 default 'FULL'
            )  
    AS
        l_env   varchar2(10);
        l_group varchar2(10);
        l_str  varchar2(8000);
    BEGIN
        -- Initialised values
        l_env   := Upper(i_env) ;
        l_group := Upper(i_grpname) ;
    
    
        select to_char(min(run_date),'dd/mm/yyyy')
          into l_str
          from hp_diag.v_db_summary;
    
        dbms_output.put_line('{toc:type=list}');
        dbms_output.put_line('h3. Overview') ;
        dbms_output.put_line('This will show the growth of the tablespaces in each database for the last 3 months') ;
        dbms_output.put_line('') ;
        dbms_output.put_line('Start Date : ' || to_char(trunc(add_months(sysdate,-3)),'dd/mm/yyyy')) ;
        dbms_output.put_line('End Date   : ' || to_char(trunc(sysdate),'dd/mm/yyyy')) ;
    
        dbms_output.put_line('h3. Tablespace Growth') ;
        for r1 in (select distinct a.DB_NAME 
                     from v_db_summary a
                     join v_test_result_dbs db on (db.DB_NAME = a.DB_NAME)
                    where db.DB_ENV = DECODE ( l_env, 'ALL', db.DB_ENV , l_env )
                      and db.DB_ENV != 'PRD'
                      and db.DB_GROUP = l_group
                    order by a.DB_NAME)
        loop
            single_db_tablespace_space(r1.db_name);
        end loop;
    END get_db_ts_space ;
    
    
    PROCEDURE Do_Gather_Space_Mgmt_Data 
            ( i_env       varchar2 default 'ALL'
             ,i_grpname   varchar2 default 'FULL'
            ) 
    AS
        -- Query to select all DBS for the group requested
        cursor c_dbs is
             select distinct DB_NAME
              from V_TEST_RESULT_DBS 
             where DB_GROUP = i_grpname
               and DB_ENV = DECODE ( i_env, 'ALL', DB_ENV , i_env )
               and DB_ENV != 'PRD'
             order by DB_NAME ;
    
        v_dbs       c_dbs%rowtype ;
        l_query     varchar2(4000) ;
        l_dbname    varchar2(10) ;
        l_link      varchar2(20) ;
    
    BEGIN
        -- This loop will get the necessary data PER database selected by the criteria recorded in the SPACE tables
        -- Each database is accessed via a database link which name matches the database name within TEST_RESULTS_DBS
        -- If the DB Link doesn't work, the script will complete for those working and report on those failing.
        for v_dbs in c_dbs 
        loop
            -- Clear TEMP tables before run    
            execute immediate 'truncate table TMP_NFT_SPACE' ;
            execute immediate 'truncate table TMP_NFT_SEG_SPACE' ;
    
            l_dbname := v_dbs.DB_NAME ;
            l_link := '@'||l_dbname ;        
            if l_dbname = g_repo then
                l_link := '' ;
            end if ;   
    
            -- Store TABLESPACE growth Information
            l_query := q'#insert into TMP_NFT_SPACE
                          select '#' ||l_dbname|| q'#' as db_name
                                  ,full.tablespace_name
                                  ,round(sum(mb_alloc),2) as allocated_mb
                                  ,round(sum(mb_alloc)-sum(mb_free),2) as used_mb
                                  ,round(sum(mb_free),2) as free_mb
                                  ,trunc(sysdate) as run_date
                         from ( select tablespace_name, SUM(bytes)/1024/1024 mb_free
                                  from sys.DBA_FREE_SPACE#' ||l_link|| q'#
                                 group by tablespace_name ) FREE
                           ,( select tablespace_name, SUM(bytes)/1024/1024 mb_alloc
                                 from sys.DBA_DATA_FILES#' ||l_link|| q'#
                                 group by tablespace_name ) FULL
                        where FREE.tablespace_name (+) = FULL.tablespace_name
                        group by FULL.tablespace_name #' ;
            
            REPORT_UTILS.exec_query( i_query => l_query, i_dbname => l_link ) ;
    
            merge into NFT_SPACE a
            using ( select * from TMP_NFT_SPACE ) b
               on ( trunc (a.RUN_DATE) = trunc (b.RUN_DATE)
                    and a.DB_NAME = b.DB_NAME
                    and a.tablespace_name = b.tablespace_name
                  )
             when matched then 
                update set a.FREE_MB      = b.FREE_MB
                          ,a.ALLOCATED_MB = b.ALLOCATED_MB
                          ,a.USED_MB      = b.USED_MB
             when not matched then
                insert (DB_NAME, TABLESPACE_NAME, ALLOCATED_MB, USED_MB, FREE_MB, RUN_DATE) 
                values (b.DB_NAME, b.TABLESPACE_NAME, b.ALLOCATED_MB, b.USED_MB, b.FREE_MB, b.RUN_DATE);
    
            commit;
    
             -- Now add in the days to die for each tablespace.    
            l_query := q'#merge into NFT_SPACE a using ( select '#' || l_dbname || q'#' as db_name, tsname, trunc(to_date(rtime,'mm/dd/yyyy hh24:mi:ss')) snap_day
                          ,decode(max(sum(mb_shift)) over (partition by tsname)
                        ,0
                        ,to_number(null)
                        ,floor((max(tablespace_maxsize)- max(tablespace_usedsize))/((max(sum(mb_shift)) over (partition by tsname))/1024))) days_left_peak
                      from (
                         select t2.tsname tsname, t1.rtime
                           ,round(first_value(t1.tablespace_size) over (partition by trunc(to_date(t1.rtime,'mm/dd/yyyy hh24:mi:ss')),t2.tsname order by to_date(t1.rtime,'mm/dd/yyyy hh24:mi:ss') desc)*8/1024/1024,2) tablespace_size
                           ,round(first_value(t1.tablespace_usedsize) over (partition by trunc(to_date(t1.rtime,'mm/dd/yyyy hh24:mi:ss')),t2.tsname order by to_date(t1.rtime,'mm/dd/yyyy hh24:mi:ss') desc)*8/1024/1024,2) tablespace_usedsize
                           ,round(first_value(t1.tablespace_maxsize) over (partition by trunc(to_date(t1.rtime,'mm/dd/yyyy hh24:mi:ss')),t2.tsname order by to_date(t1.rtime,'mm/dd/yyyy hh24:mi:ss') desc)*8/1024/1024,2) tablespace_maxsize
                           ,round((t1.tablespace_usedsize/1024*8)-(lag(t1.tablespace_usedsize) over (partition by t2.tsname order by t1.snap_id)/1024*8),2) mb_shift
                        from dba_hist_tbspc_space_usage#'||l_link|| q'# t1
                             join dba_hist_tablespace#'||l_link|| q'# t2
                             on t1.tablespace_id=t2.ts#
                             and t1.dbid=t2.dbid)
                      group by tsname,trunc(to_date(rtime,'mm/dd/yyyy hh24:mi:ss'))
                       ) b
                     on ( trunc (a.RUN_DATE) = trunc (b.snap_day)
                    and a.DB_NAME = b.DB_NAME
                    and a.tablespace_name = b.tsname
                           )
                   when matched then 
                   update set a.DAYS_TO_DIE  = b.DAYS_LEFT_PEAK#';  
    
    
            REPORT_UTILS.exec_query( i_query => l_query, i_dbname => l_link ) ;
    
            -- Store SCHEMA growth information
            l_query := q'#insert into TMP_NFT_SEG_SPACE
                            select '#' ||l_dbname|| q'#' as db_name
                                   , ds.owner as schema_name
                                   , round(sum(bytes)/1024/1024/1024, 3) as used_gb
                                   , trunc(sysdate) as run_date
                              from DBA_SEGMENTS#'||l_link||q'# ds
                                 , DBA_USERS#'||l_link||q'# us
                             where ds.owner = us.username
                               and us.created > ( select created+(30/60/24) 
                                                    from DBA_USERS#'||l_link||q'#
                                                   where username = 'SYS')
                               and not REGEXP_LIKE (us.username, '[a-z]{3}[0-9]{2}','i')
                               and us.username not in ('HP_DIAG','BSBDEPLOY','HOUSEKEEPING','DATAPROV','XDB','ANONYMOUS','CAPACITY','FOCUS_PRD')
                             group by ds.owner#';             
            
            REPORT_UTILS.exec_query( i_query => l_query, i_dbname => l_link ) ;
    
            merge into NFT_SEG_SPACE a
            using ( select * from TMP_NFT_SEG_SPACE ) b
               on ( trunc(a.RUN_DATE) = trunc(b.RUN_DATE) 
                    and a.DB_NAME = b.DB_NAME
                    and a.SCHEMA_NAME = b.SCHEMA_NAME 
                  )
            when matched then update 
                set a.USED_GB = b.USED_GB
            when not matched then
                insert (DB_NAME, SCHEMA_NAME, USED_GB, RUN_DATE) 
                values (b.DB_NAME, b.SCHEMA_NAME, b.USED_GB, b.RUN_DATE);
    
            commit ; 
    
        end loop ;
    EXCEPTION
        WHEN OTHERS THEN
            logger.write('Do_Gather_Space_Mgmt_Data - '||l_dbname|| ' : ' ||SQLCODE||' : '||SUBSTR(SQLERRM, 1, 500)) ;  
    END Do_Gather_Space_Mgmt_Data ;
    
END REPORT_SPACE ;
/
