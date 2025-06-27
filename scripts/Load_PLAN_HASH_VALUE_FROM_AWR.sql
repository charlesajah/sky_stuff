--1. To create SQL tuning set.
exec dbms_sqltune.create_sqlset(sqlset_name => 'f3k2wwggkgb00_sqlset_prod1',description => 'query prod');

--What if the sql_id or plan_hash_value is not present in the cursor , but present in AWR snap?
--SQL_ID =f3k2wwggkgb00
--First we need to find the begin snap and end snap of the sql_id.
select a.instance_number inst_id, a.snap_id,a.plan_hash_value, to_char(begin_interval_time,'dd-mon-yy hh24:mi') btime, abs(extract(minute from (end_interval_time-begin_interval_time)) + extract(hour from (end_interval_time-begin_interval_time))*60 + extract(day from (end_interval_time-begin_interval_time))*24*60) minutes,
executions_delta executions, round(ELAPSED_TIME_delta/1000000/greatest(executions_delta,1),4) "avg duration (sec)" from dba_hist_SQLSTAT a, dba_hist_snapshot b
where sql_id='&sql_id' and a.snap_id=b.snap_id
and a.instance_number=b.instance_number
order by snap_id desc, a.instance_number;

--now we know what plan_hash_value we need and the AWR snap_ids containing them
--here sample begin snap_id => 198670 and sample end snap_id => 198671
--in the procedure DBMS_SQLTUNE.SELECT_WORKLOAD_REPOSITORY 
--we supply both the AWR snap_ids and the plan_hash_value we desire

--2. To load execution plan into STS from AWR

declare
baseline_ref_cur DBMS_SQLTUNE.SQLSET_CURSOR;
begin
open baseline_ref_cur for
select VALUE(p) from table(
DBMS_SQLTUNE.SELECT_WORKLOAD_REPOSITORY(198670, 198671,
'sql_id='||CHR(39)||'f3k2wwggkgb00'||CHR(39)||' and
plan_hash_value=600031374', NULL, NULL, NULL, NULL,
NULL, NULL, 'ALL')) p;
DBMS_SQLTUNE.LOAD_SQLSET('f3k2wwggkgb00_sqlset_prod1', baseline_ref_cur);
end;
/

--how to flush SQL_ID from cache 
select ADDRESS, HASH_VALUE from GV$SQLAREA where SQL_ID= '8rqxguzxz1fxf';
exec DBMS_SHARED_POOL.PURGE ('0000001E5CA100A8, 4226857902', 'C');
exec DBMS_SHARED_POOL.PURGE ('0000001E3D764410, 4102001793', 'C');

--3. To create SQL baseline/Fixed plan.

set serveroutput on
declare
my_int pls_integer;
begin
my_int := dbms_spm.load_plans_from_sqlset (
sqlset_name => 'f3k2wwggkgb00_sqlset_prod1',
sqlset_owner => '',
fixed => 'YES',
enabled => 'YES');
DBMS_OUTPUT.PUT_line(my_int);
end;
/