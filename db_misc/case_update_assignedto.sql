set termout off
set pagesize 0
set head off
set timing on

WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK

SPOOL update_assignedto.txt

select 'Assigned to is null before : ' || count(*) 
  from caseManagement.bsbcmCase
 WHERE assignedto is null or assignedto = 'null';

update caseManagement.bsbcmCase
set assignedto = 'testUser'
where (assignedto is null or assignedto = 'null');

commit;

exec dbms_stats.gather_table_stats('CASEMANAGEMENT','BSBCMCASE');

select 'Assigned to is null after : ' || count(*) 
  from caseManagement.bsbcmCase
 WHERE assignedto is null or assignedto = 'null';

spool off
exit
