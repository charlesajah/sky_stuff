spool jbpm_cleardown.log

alter session set ddl_lock_timeout = 60 ;
set lines 250 pages 99
col contraint_name for a30
col table_name for a30
col osuser for a20
col machine for a20

alter table jbpm_owner.correlationPropertyInfo disable constraint FK_CORR_KEYID_CORRELATIONPROPERTYINFO ;
alter table jbpm_owner.eventTypes disable constraint FK_INSTANCEID_EVENTTYPES ;

TRUNCATE TABLE jbpm_owner.processInstanceInfo ;
TRUNCATE TABLE jbpm_owner.variableInstanceLog ;
TRUNCATE TABLE jbpm_owner.nodeInstanceLog ;  

TRUNCATE TABLE jbpm_owner.correlationPropertyInfo ;
TRUNCATE TABLE jbpm_owner.eventTypes ;
TRUNCATE TABLE jbpm_owner.correlationKeyInfo ;  
TRUNCATE TABLE jbpm_owner.processInstanceLog ;

alter table jbpm_owner.correlationPropertyInfo enable novalidate constraint FK_CORR_KEYID_CORRELATIONPROPERTYINFO;
alter table jbpm_owner.eventTypes enable novalidate constraint FK_INSTANCEID_EVENTTYPES ;

delete from jbpm_owner.correlationPropertyInfo t where t.CORRELATIONKEY_KEYID not in ( select s.keyId from jbpm_owner.correlationKeyInfo s ) ;
delete from jbpm_owner.eventTypes t where t.instanceId not in ( select s.instanceId from jbpm_owner.processInstanceInfo s ) ;

alter table jbpm_owner.correlationPropertyInfo enable validate constraint FK_CORR_KEYID_CORRELATIONPROPERTYINFO ;
alter table jbpm_owner.eventTypes enable validate constraint FK_INSTANCEID_EVENTTYPES ;

select constraint_name , status , validated from dba_constraints where constraint_name in ( 'FK_CORR_KEYID_CORRELATIONPROPERTYINFO' , 'FK_INSTANCEID_EVENTTYPES' ) order by 1 ;
select table_name , num_rows FROM dba_tables where table_name IN ( 'CORRELATIONPROPERTYINFO' , 'EVENTTYPES' , 'CORRELATIONKEYINFO' , 'PROCESSINSTANCEINFO' , 'VARIABLEINSTANCELOG' , 'NODEINSTANCELOG' , 'PROCESSINSTANCELOG' ) ORDER BY 1 ;
select segment_name , bytes/1024 as kb FROM dba_segments where segment_name IN ( 'CORRELATIONPROPERTYINFO' , 'EVENTTYPES' , 'CORRELATIONKEYINFO' , 'PROCESSINSTANCEINFO' , 'VARIABLEINSTANCELOG' , 'NODEINSTANCELOG' , 'PROCESSINSTANCELOG' ) ORDER BY 1 ;

select status , last_call_et , osuser,machine
from v$session
where username != 'SYSTEM'
and ( status = 'ACTIVE' or last_call_et <= 4 )
order by 1,2,3,4
;

alter table jbpm_owner.correlationPropertyInfo enable constraint FK_CORR_KEYID_CORRELATIONPROPERTYINFO ;
alter table jbpm_owner.eventTypes enable constraint FK_INSTANCEID_EVENTTYPES ;

spool off;
exit;