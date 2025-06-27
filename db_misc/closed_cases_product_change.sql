set termout off
set pagesize 0
set head off
set timing on

WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK

SPOOL close_open_cases.txt

select 'Cases to "Close" before : ' || count(*) 
  from caseManagement.bsbcmCase
 WHERE product is not null
   and status = 'CLOSED' 
   and queueid = 'ASSIGNED_AND_CLOSED';

update caseManagement.bsbcmCase
   set product = null
 WHERE /*product LIKE 'Broadband / Talk'
   and */status = 'CLOSED' 
   and queueid = 'ASSIGNED_AND_CLOSED';

commit;

exec dbms_stats.gather_table_stats('CASEMANAGEMENT','BSBCMCASE');

select 'Cases to "Close" after : ' || count(*) 
  from caseManagement.bsbcmCase
 WHERE product is not null
   and status = 'CLOSED'
   and queueid = 'ASSIGNED_AND_CLOSED';

spool off
exit
