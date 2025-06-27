set termout off
set pagesize 0
set spool on
set head off
set timing on
set serveroutput on


WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK

SPOOL ACTION_partition_maintenance.txt

alter table BFUL.ACTIONGATE     disable constraint FK_ACTIONID_ACTIONGATE;
alter table BFUL.ACTIONGATEMSG  disable constraint FK_ACTIONID_ACTIONGATEMSG;
alter table BFUL.ACTIONPROVIDER disable constraint FK_ACTIONID_ACTIONPROVIDER;
alter table BFUL.ACTIONOUTLET   disable constraint FK_ACTID_ACTIONOUTLET;

select segment_name || ' before size : ' || round(sum(bytes)/1024/1024,2) || ' MB' from dba_segments where owner = 'BFUL' and segment_name = 'ACTION' group by segment_name;
select 'Before partition count : ' || count(*) from dba_tab_partitions where table_name = 'ACTION' and table_owner = 'BFUL' ;

declare
  l_cnt      integer;
  l_strParts varchar2(4000) := '';
  l_days     integer := 60;
begin
  select max(partition_position) into l_cnt
    from dba_tab_partitions 
   where table_name = 'ACTION' 
     and table_owner = 'BFUL';

  if l_cnt > l_days+1 then
    for parts in (select *
                    from dba_tab_partitions 
                   where table_name = 'ACTION' 
                     and partition_position+l_days < l_cnt
                  order by partition_position desc)
    loop
      l_strParts := parts.partition_name || ',' || l_strParts ;
    end loop;
  
    l_strParts := 'ALTER TABLE BFUL.ACTION DROP PARTITION ' || rtrim(l_strParts,',') || ' update indexes';
  
    dbms_output.put_line(l_strParts);
    execute immediate l_strParts;
  end if ;
end;
/

exec dbms_stats.gather_schema_stats('BFUL');

select 'After partition count : ' || count(*) from dba_tab_partitions where table_name = 'ACTION' and table_owner = 'BFUL' ;
select segment_name || ' after size : ' || round(sum(bytes)/1024/1024,2) || ' MB' from dba_segments where owner = 'BFUL' and segment_name = 'ACTION' group by segment_name;

alter table BFUL.ACTIONGATE     enable novalidate constraint FK_ACTIONID_ACTIONGATE;
alter table BFUL.ACTIONGATEMSG  enable novalidate constraint FK_ACTIONID_ACTIONGATEMSG;
alter table BFUL.ACTIONPROVIDER enable novalidate constraint FK_ACTIONID_ACTIONPROVIDER;
alter table BFUL.ACTIONOUTLET   enable novalidate constraint FK_ACTID_ACTIONOUTLET;

spool off
exit
