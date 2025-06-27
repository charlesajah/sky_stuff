--How To Run SQL Tuning Advisor For A Sql_id

--Suppose the sql id is – 87s8z2zzpsg88

--1. Create Tuning Task

DECLARE
  l_sql_tune_task_id  VARCHAR2(100);
BEGIN
  l_sql_tune_task_id := DBMS_SQLTUNE.create_tuning_task (
                          sql_id      => '87s8z2zzpsg88',
                          scope       => DBMS_SQLTUNE.scope_comprehensive,
                          time_limit  => 500,
                          task_name   => '87s8z2zzpsg88_tuning_task11',
                          description => 'Tuning task1 for statement 87s8z2zzpsg88');
  DBMS_OUTPUT.put_line('l_sql_tune_task_id: ' || l_sql_tune_task_id);
END;
/

 --2  Execute Tuning task:

 EXEC DBMS_SQLTUNE.execute_tuning_task(task_name => '87s8z2zzpsg88_tuning_task11');

 --3.   Get the Tuning advisor report.
 set long 65536
set longchunksize 65536
set linesize 100
select dbms_sqltune.report_tuning_task('87s8z2zzpsg88_tuning_task11') from dual;

--4.    Get list of tuning task present in database:
--We can get the list of tuning tasks present in database from DBA_ADVISOR_LOG
SELECT TASK_NAME, STATUS FROM DBA_ADVISOR_LOG WHERE TASK_NAME='' ;


--What if the sql_id is not present in the cursor , but present in AWR snap?
--SQL_ID =24pzs2d6a6b13
--First we need to find the begin snap and end snap of the sql_id.
select a.instance_number inst_id, a.snap_id,a.plan_hash_value, to_char(begin_interval_time,'dd-mon-yy hh24:mi') btime, abs(extract(minute from (end_interval_time-begin_interval_time)) + extract(hour from (end_interval_time-begin_interval_time))*60 + extract(day from (end_interval_time-begin_interval_time))*24*60) minutes,
executions_delta executions, round(ELAPSED_TIME_delta/1000000/greatest(executions_delta,1),4) "avg duration (sec)" from dba_hist_SQLSTAT a, dba_hist_snapshot b
where sql_id='&sql_id' and a.snap_id=b.snap_id
and a.instance_number=b.instance_number
order by snap_id desc, a.instance_number;

--From here we can get the begin snap and end snap of the sql_id.

--begin_snap -> 235
--end_snap -> 240

--Create the tuning task using the snap IDs
DECLARE
  l_sql_tune_task_id  VARCHAR2(100);
BEGIN
  l_sql_tune_task_id := DBMS_SQLTUNE.create_tuning_task (
                          begin_snap  => 235,
                          end_snap    => 240,
                          sql_id      => '24pzs2d6a6b13',
                          scope       => DBMS_SQLTUNE.scope_comprehensive,
                          time_limit  => 60,
                          task_name   => '24pzs2d6a6b13_AWR_tuning_task',
                          description => 'Tuning task for statement 24pzs2d6a6b13  in AWR');
  DBMS_OUTPUT.put_line('l_sql_tune_task_id: ' || l_sql_tune_task_id);
END;
/

3. Execute the tuning task
EXEC DBMS_SQLTUNE.execute_tuning_task(task_name => '24pzs2d6a6b13_AWR_tuning_task');





