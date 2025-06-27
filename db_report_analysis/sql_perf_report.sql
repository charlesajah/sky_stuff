define sql_id='&1'
define days='&2'


alter session set nls_date_format = 'dd/mm/yyyy hh24:mi:ss';

set pages 999
SET MARKUP HTML ON SPOOL ON PREFORMAT OFF ENTMAP ON 

COLUMN env HEADING 'Environment' for a15
COLUMN database_name HEADING 'Database Name' for a20
COLUMN test_desc HEADING 'Test Description' for a80
COLUMN begin_time HEADING 'Test Start Time' 
COLUMN end_time HEADING 'Test End Time' 
COLUMN executions HEADING 'Executions' for 999999999
COLUMN AVG_TPS HEADING 'TPS Rate' for 999999990.0000
COLUMN avg_elapsed_time HEADING 'Avg Elapsed Time (s)' for 999999990.0000
COLUMN avg_buffer_gets HEADING 'Avg Buffer Gets' for 999999990.0000
COLUMN avg_cpu_time HEADING 'Avg CPU Time (s)' for 999999990.0000
COLUMN PLAN_HASH_VALUES HEADING 'Explain Plan Hash Values' 

set feedback off termout off VERIFY OFF set head on

SPOOL sql_perf_report.html
set feedback off termout off VERIFY OFF 

select m.DB_ENV Env, t.database_name, nvl(t.test_description,'----------') test_desc, t.begin_time, t.end_time, t.executions,round(t.TPS,3) AVG_TPS, 
       round(t.elapsed_time_per_exec_seconds, 4) avg_elapsed_time, 
       round(t.BUFFER_GETS_PER_EXEC,4) avg_buffer_gets, round(t.CPU_TIME_PER_EXEC_SECONDS, 4) avg_cpu_time,
       t.PLAN_HASH_VALUES
from TEST_RESULT_SQL t
join TEST_RESULT_MASTER m on ( m.TEST_ID = t.TEST_ID )
where t.sql_id = '&sql_id'
  and t.begin_time > trunc(sysdate)-&days
order by 4 desc;


spool off
exit