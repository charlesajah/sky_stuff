SET trimspool on pagesize 0 head off feed off lines 1000 verify off serverout on
spool space.txt
select * from table(space_report.overall_space);
spool off

spool schema_space.txt
exec space_report.all_db_schema_space;
spool off


spool ts_space.txt
exec space_report.all_db_ts_space;
spool off