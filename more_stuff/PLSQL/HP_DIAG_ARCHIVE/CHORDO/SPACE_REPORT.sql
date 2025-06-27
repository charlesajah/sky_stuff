CREATE OR REPLACE PACKAGE         space_report
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
   FUNCTION overall_space RETURN g_tvc2 PIPELINED ;
   --FUNCTION schema_space RETURN g_tvc2 PIPELINED ;
   procedure all_db_schema_space;
   procedure single_db_schema_space(p_db_name in varchar2) ;
   procedure single_db_tablespace_space(p_db_name in varchar2) ;
   procedure all_db_ts_space  ;
END space_report ;
/


create or replace PACKAGE BODY         space_report
AS

FUNCTION overall_space  RETURN g_tvc2 PIPELINED
AS
  l_str varchar2(100);
BEGIN
  select to_char(min(add_months(sysdate,-3)),'dd/mm/yyyy')
    into l_str
    from hp_diag.v_db_summary;

  PIPE ROW ('{toc:type=list}');
  PIPE ROW ('h3. Time Window');
  PIPE ROW ('Start Date: ' || l_str );
  PIPE ROW ('End Date: ' || to_char(sysdate,'dd/mm/yyyy'));

  PIPE ROW ('h3. Database Growth');
  for r1 in (select distinct db_name from v_db_summary order by 1)
  loop
    PIPE ROW ( 'h5. DB Growth for ' || r1.db_name) ;
    PIPE ROW ( '{chart:dataOrientation=vertical|categoryLabelPosition=down45|rangeAxisLowerBound=0|dateFormat=dd/mm|height=500|orientation=vertical|stacked=true|timePeriod=Day|timeSeries=true|title=' || r1.db_name || '|type=area|width=1800|xLabel=Date|yLabel=Space (GB)}' ) ;
    PIPE ROW ( ' || DATE_TIME || USED_GB || FREE_GB ||' ) ;
    for r2 in (select '|' || to_char(run_date, 'dd/mm') || '|' || used_gb || '|' || free_gb || '|'  as chart_row
                 from hp_diag.v_db_summary where db_name = r1.db_name and run_date > add_months(sysdate,-3)
               order by run_date)
    loop
      PIPE ROW ( r2.chart_row ) ;
    end loop;
    PIPE ROW ('{chart}') ;
  end loop;
END overall_space ;

--FUNCTION schema_space  RETURN g_tvc2 PIPELINED
procedure all_db_schema_space  
AS
  l_str  varchar2(8000);
BEGIN
  select to_char(min(add_months(sysdate,-3)),'dd/mm/yyyy')
    into l_str
    from hp_diag.v_db_summary;

  dbms_output.put_line('{toc:type=list}');
  dbms_output.put_line('h3. Time Window');
  dbms_output.put_line('Start Date: ' || l_str );
  dbms_output.put_line('End Date: ' || to_char(sysdate,'dd/mm/yyyy'));

  dbms_output.put_line('h3. Schema Growth');

  for r1 in (select distinct db_name from v_db_summary order by 1)
  loop
    single_db_schema_space(r1.db_name);
  end loop;
END all_db_schema_space ;

procedure single_db_schema_space(p_db_name in varchar2) as
  l_head varchar2(8000);
  l_sql  varchar2(8000);
  l_cnt  number;
  
  TYPE info_rt IS RECORD ( row_data     VARCHAR2 (8000));
  TYPE info_aat IS TABLE OF info_rt INDEX BY PLS_INTEGER;
  l_info   info_aat;  
BEGIN
  dbms_output.put_line('h5. Schema Growth for ' || upper(p_db_name)) ;
  dbms_output.put_line('{chart:dataOrientation=vertical|categoryLabelPosition=down45|rangeAxisLowerBound=0|dateFormat=dd/mm|height=500|orientation=vertical|stacked=true|timePeriod=Day|timeSeries=true|title=' || upper(p_db_name) || '|type=area|width=1800|xLabel=Date|yLabel=Space (GB)}' ) ;
  WITH q AS (
           select * 
             from (select distinct schema_name 
                     from NFT_SEG_SPACE 
                    where db_name = upper(p_db_name)
                      and run_date = trunc(sysdate) 
                      and used_gb > 1
                   order by used_gb desc) 
            where rownum < 21 )
    SELECT '|| Run Date || ' || 
           LISTAGG ( schema_name  , ' || ' ) WITHIN GROUP ( ORDER BY schema_name ) ||
           '||'
      into l_head     
      FROM q ;

  if length(l_head) > 17 then 
    dbms_output.put_line( l_head ) ;  
    
    WITH q AS (
             select * 
               from (select distinct schema_name 
                       from NFT_SEG_SPACE 
                      where db_name = upper(p_db_name)
                        and run_date = trunc(sysdate) 
                        and used_gb > 1
                     order by used_gb desc) 
              where rownum < 21 )
    SELECT LISTAGG ( ''''|| schema_name  , ''', ' ) WITHIN GROUP ( ORDER BY schema_name ) || '''' into l_head FROM q ;

    select count(*) into l_cnt from user_tables where table_name = 'SEG_SPACE_INFO';
    if l_cnt = 0 then
      l_sql := 'create table seg_space_info as with q as ( SELECT to_char(run_date,''dd/mm'') run_date, schema_name, max(used_gb) used_gb FROM hp_diag.nft_seg_Space where db_name = ''' || upper(p_db_name) || ''' and run_date > add_months(sysdate,-3) group by run_date, schema_name order by run_date) select * from  q pivot (max(used_gb) for (schema_name) IN ( ' || l_head || '))';
      execute immediate l_sql;
    else
      execute immediate 'drop table seg_space_info';
      l_sql := 'create table seg_space_info as with q as ( SELECT to_char(run_date,''dd/mm'') run_date, schema_name, max(used_gb) used_gb FROM hp_diag.nft_seg_Space where db_name = ''' || upper(p_db_name) || ''' and run_date > add_months(sysdate,-3) group by run_date, schema_name order by run_date) select * from  q pivot (max(used_gb) for (schema_name) IN ( ' || l_head || '))';
      execute immediate l_sql;
    end if;
    
    with q as (select column_id, column_name from user_Tab_columns where table_name = 'SEG_SPACE_INFO' order by column_id)
           SELECT LISTAGG ( '|| "'|| column_name  , '" || ''||''' ||'' ) WITHIN GROUP ( ORDER BY column_id ) || '"'
             INTO l_head
             FROM q ;

    l_sql := 'select ''||'' ' || l_head || '|| '''||'||'' from SEG_SPACE_INFO order by to_date(run_date, ''dd/mm'')';
    execute immediate l_sql bulk collect into l_info;
    FOR indx IN 1 .. l_info.COUNT
    LOOP
      DBMS_OUTPUT.put_line (l_info(indx).row_data);
    END LOOP;
  end if;
  
  dbms_output.put_line('{chart}') ;
end;

procedure single_db_tablespace_space(p_db_name in varchar2) as
  l_head varchar2(8000);
  l_sql  varchar2(8000);
  l_cnt  number;
  
  TYPE info_rt IS RECORD ( row_data     VARCHAR2 (8000));
  TYPE info_aat IS TABLE OF info_rt INDEX BY PLS_INTEGER;
  l_info   info_aat;  
BEGIN
  dbms_output.put_line('h5. Tablespace Growth for ' || upper(p_db_name)) ;
  dbms_output.put_line(' || Tablespace Name || Minimum Size (GB) || Maximum Size (GB) || Increase (GB) || Growth ||' ) ;
  for r1 in (select '|' || ts_name || '|' || min_gb || '|' ||  max_gb || '|' || round(max_gb-min_gb,2) ||
       '|{status:colour=' || growth_stat || '|title=' || pct_growth || '}|' as chart_row
  from (select ts_name, min(round(allocated_gb,2)) min_gb, max(round(allocated_gb,2)) max_gb, 
               round(((max(allocated_gb)-min(allocated_gb))/max(allocated_gb)),4)*100 || '%' pct_growth,
               case when round(((max(allocated_gb)-min(allocated_gb))/max(allocated_gb)),4)*100 < 5 then 'Green' 
                    when round(((max(allocated_gb)-min(allocated_gb))/max(allocated_gb)),4)*100 between 5 and 10 then 'Yellow' 
                    ELSE 'Red' end as growth_stat
               from v_db_ts_alloc
              where db_name = upper(p_db_name)
                and ts_name not in ('SYSTEM','SYSAUX','SYSAUX_DATA_AUTO_01','TOOLS','TOOLS_AUTO_01','UNDOTBS','UNDOTBS1','UNDOTBS_RECO','USERS','USERS_AUTO_01')
                and run_date > add_months(trunc(sysdate),-3)
                group by ts_name order by ts_name))
    loop
      dbms_output.put_line ( r1.chart_row ) ;
    end loop;
end;

procedure all_db_ts_space  
AS
  l_str  varchar2(8000);
BEGIN
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
  for r1 in (select distinct db_name from v_db_ts_alloc order by 1)
  loop
    single_db_tablespace_space(r1.db_name);
  end loop;
END all_db_ts_space ;

END space_report ;
/
