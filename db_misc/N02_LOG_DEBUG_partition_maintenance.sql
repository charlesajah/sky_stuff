set termout off
set pagesize 0
set spool on
set head off
set timing on
set serveroutput on


WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK

SPOOL N02_LOG_DEBUG_partition_maintenance.txt



select segment_name || ' before size : ' || round(sum(bytes)/1024/1024,2) || ' MB' from dba_segments where owner = 'DATAPROV' and segment_name = 'LOG_DEBUG' group by segment_name;
select 'Before partition count : ' || count(*) from dba_tab_partitions where table_name = 'LOG_DEBUG' and table_owner = 'DATAPROV' ;

declare
  l_cnt      integer;
  l_strParts varchar2(4000) := '';
  l_days     integer := 365;
begin
  select max(partition_position) into l_cnt
    from dba_tab_partitions 
   where table_name = 'LOG_DEBUG' 
     and table_owner = 'DATAPROV';

  if l_cnt > l_days+1 then
    for parts in (select *
                    from dba_tab_partitions 
                   where table_name = 'LOG_DEBUG' 
                     and partition_position+l_days < l_cnt
                  order by partition_position desc)
    loop
      l_strParts := parts.partition_name || ',' || l_strParts ;
    end loop;
  
    l_strParts := 'ALTER TABLE DATAPROV.LOG_DEBUG DROP PARTITION ' || rtrim(l_strParts,',') || ' update indexes';
  
    dbms_output.put_line(l_strParts);
    execute immediate l_strParts;
  end if ;
end;
/

exec dbms_stats.gather_schema_stats('DATAPROV');

select 'After partition count : ' || count(*) from dba_tab_partitions where table_name = 'LOG_DEBUG' and table_owner = 'DATAPROV' ;
select segment_name || ' after size : ' || round(sum(bytes)/1024/1024,2) || ' MB' from dba_segments where owner = 'DATAPROV' and segment_name = 'LOG_DEBUG' group by segment_name;



spool off
exit
