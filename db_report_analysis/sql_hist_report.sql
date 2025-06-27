define sql_id='&1'

alter session set nls_date_format = 'dd/mm/yyyy hh24:mi:ss';

set pages 999
SET MARKUP HTML ON SPOOL ON PREFORMAT OFF ENTMAP ON 
COLUMN database_name HEADING 'Database Name' for a20
COLUMN test_desc HEADING 'Test Description' 
COLUMN begin_time HEADING 'Begin Time' 
COLUMN duration_hrs HEADING 'Test Duration (hrs)' 
COLUMN AVG_TPS HEADING 'TPS Rate' 
COLUMN executions HEADING 'Executions' 
COLUMN avg_elapsed_time HEADING 'Avg Elapsed Time (s)' 
COLUMN avg_buffer_gets HEADING 'Avg Buffer Gets' 
COLUMN avg_cpu_time HEADING 'Avg CPU Time (s)' 
COLUMN PLAN_HASH_VALUES HEADING 'Explain Plan Hash Values' 

SPOOL sql_hist_report.html
set feedback off termout off VERIFY OFF 
select '1. Historical SQL report for "' || '&&sql_id' || '"' as "Report Information" from dual
union
select '2. SQL ID "&&sql_id" appeared in ' || count(*) || ' tests' 
  from test_result_sql 
 where sql_id = '&&sql_id'
union
select '3. Earliest date that SQL ID "&&sql_id" appeared in ' || min(begin_time) 
  from test_result_sql 
 where sql_id = '&&sql_id'
union
select '4. Latest date that SQL ID "&&sql_id" appeared in ' || max(begin_time) 
  from test_result_sql 
 where sql_id = '&&sql_id';
 
set head on

select database_name, nvl(test_description,'N/A') test_desc, begin_time, round((end_time-begin_time)*24,2) duration_hrs, round(TPS,4) AVG_TPS, executions, round(elapsed_time_per_exec_seconds, 4) avg_elapsed_time, 
       round(BUFFER_GETS_PER_EXEC,4) avg_buffer_gets, round(CPU_TIME_PER_EXEC_SECONDS, 4) avg_cpu_time,
       PLAN_HASH_VALUES
  from test_result_sql 
 where sql_id = '&&sql_id'
 order by begin_time desc;
spool off
exit