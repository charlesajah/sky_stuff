set termout off
set linesize 900
set pagesize 200
set heading off
set verify off
set feedback off
set newpage 0
set serveroutput on size unlimited
alter session set nls_date_format = 'DD-MON-YYYY';

spool changed_objects.txt
begin
report_utils.changed_objects('&1');

end;
/
spool off
exit
