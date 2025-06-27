set termout off
set pagesize 0
set spool on
set head off
set timing on
set serveroutput on

WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK

SPOOL fulfillment_archive.txt

alter session set ddl_lock_timeout = 180 ;

select 'Before bful.commercialorder rows : ' || count(*) from bful.commercialorder a ;
select 'Before bful.action rows : '          || count(*) from bful.action          a ;

insert /*+ append */ into bful.commercialorder_arc select * from bful.commercialorder a where trunc (a.created) < trunc(sysdate)-4.5 ;
insert /*+ append */ into bful.action_arc          select * from bful.action          a where trunc (a.created) < trunc(sysdate)-4.5 ;

commit ;

alter table bful.ACTIONGATE disable constraint FK_ACTIONID_ACTIONGATE ;
alter table BFUL.action disable constraint FK_COID_ACTION ;
alter table bful.ACTIONOUTLET disable constraint FK_ACTID_ACTIONOUTLET ;
DECLARE
   l_count NUMBER ;
BEGIN
   SELECT COUNT(*) INTO l_count FROM dba_indexes WHERE index_name = 'TIXI_ACTION_ACTION_DATA' AND owner = 'BFUL' ;
   IF l_count > 0
   THEN
      dbms_output.put_line ( 'Running drop index bful.tixi_action_action_data...' ) ;
      execute immediate 'drop index bful.tixi_action_action_data' ;
   ELSE
      dbms_output.put_line ( 'Index bful.tixi_action_action_data not found, so no action taken at drop index stage.' ) ;
   END IF ;
END ;
/

delete from bful.commercialorder a where trunc (a.created) < trunc(sysdate)-4.5 ;
delete from bful.action          a where trunc (a.created) < trunc(sysdate)-4.5 ;
commit;

select 'After bful.commercialorder rows : ' || count(*) from bful.commercialorder a ;
select 'After bful.action rows : '          || count(*) from bful.action          a ;

-- steps to re-org exsiting table to keep performance acceptable
select segment_name || ' before size : ' || round(sum(bytes)/1024/1024,2) || ' MB' from dba_segments where owner = 'BFUL' and segment_name in ('COMMERCIALORDER','ACTION') group by segment_name ;

select 'Creating backup table and dropping index' from dual ;
create table BFUL.ZZZ_ACTION_ZZZ
as select * from BFUL.ACTION ;

select 'Backup table row count : ' || count(*) from BFUL.ZZZ_ACTION_ZZZ ;

select 'Truncate and repopulate ACTION table'  from dual ;
truncate table BFUL.ACTION ;

declare
   l_cnt       integer := 0 ;
   l_processed integer := 0 ;
begin
   select count(*) into l_cnt from BFUL.ZZZ_ACTION_ZZZ ;
   while l_cnt > 0 
   loop
      delete from BFUL.ZZZ_ACTION_ZZZ t where t.id in ( select a.id from BFUL.ACTION a ) ;
      insert into BFUL.ACTION select * from BFUL.ZZZ_ACTION_ZZZ where rownum < 151 ;
      delete from BFUL.ZZZ_ACTION_ZZZ where rownum < 151 ;
      commit ;
      select count(*) into l_cnt from BFUL.ZZZ_ACTION_ZZZ ;
   end loop ;
end ;
/

select 'Re-create Index and drop temporary table'  from dual ;

drop table BFUL.zzz_action_zzz purge ;

ALTER TABLE bful.actiongate ENABLE NOVALIDATE CONSTRAINT fk_actionid_actiongate ;
ALTER TABLE bful.action ENABLE NOVALIDATE CONSTRAINT fk_coid_action ;
ALTER TABLE bful.actionoutlet ENABLE NOVALIDATE CONSTRAINT fk_actid_actionoutlet ;
CREATE INDEX bful.tixi_action_action_data ON bful.action ( action_data )
   INDEXTYPE IS CTXSYS.CONTEXT PARAMETERS ('section group CTXSYS.JSON_SECTION_GROUP SYNC (ON COMMIT)')
;

exec dbms_stats.gather_schema_stats('BFUL') ;

select 'Final bful.action rows : '          || count(*) from bful.action ;
select segment_name || ' after size : ' || round(sum(bytes)/1024/1024,2) || ' MB' from dba_segments where owner = 'BFUL' and segment_name in ('COMMERCIALORDER','ACTION') group by segment_name ;

spool off
exit
