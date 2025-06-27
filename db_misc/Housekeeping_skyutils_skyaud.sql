set termout off
set pagesize 0
set spool on
set head off
set timing on
set serveroutput on

spool Housekeeping_skyutils_skyaud.log append

truncate table SKYUTILS.SKY_AUD;

spool off
exit;
