
SELECT a.*
FROM (SELECT s.snap_id
            ,s.begin_interval_time
            ,ss.module
            ,ss.plan_hash_value
            ,ss.executions_delta
            ,ROUND((ss.elapsed_time_delta/NULLIF(ss.executions_delta, 0))/1000000, 3) avg_elapsed
            --,ss.rows_processed_delta/ss.executions_delta rpe
            ,ROUND(SS.elapsed_time_delta/1000000, 3)  elapsed_time_delta_secs
            ,ROUND(ss.cpu_time_delta/1000000, 3)  cpu_sec
            ,ROUND(ss.iowait_delta/1000000, 3)  io_sec
            ,ss.rows_processed_delta
      FROM   dba_hist_sqlstat SS
            ,dba_hist_snapshot s
      WHERE  s.snap_id = ss.snap_id
      AND    ss.sql_id = 'gvsvnswq8jafs'
      AND    ss.INSTANCE_NUMBER=1) a
where a.executions_delta > 0
ORDER BY a.begin_interval_time desc;



--OEM query to filter targets by LifeCycle and/or line of Business
       
   with LifeCycle as(
        select distinct target_name,property_value from (select target_name,target_type,property_name,property_value FROM sysman.MGMT$TARGET_PROPERTIES
        WHERE property_name in ('orcl_gtp_lifecycle_status'))
        where target_type='oracle_database' )
    ,LOB as(        
    select distinct target_name,property_value from (select target_name,target_type,property_name,property_value FROM sysman.MGMT$TARGET_PROPERTIES
        WHERE property_name in ('orcl_gtp_line_of_bus'))
        where target_type='oracle_database')
    select lc.target_name,lc.property_value LifeCycle,lb.property_value LOB from LifeCycle lc,LOB lb
    where lc.target_name=lb.target_name
    and lc.property_value !='MissionCritical'
    and lb.property_value ='IBM' order by target_name;

--A query to find out all the RAC instances in OEM and the RAC DBs they belong to
select distinct instance_target,db_target from sysman.MGMT$RACDB_INTERCONNECTS;



select host_name,entity_name,entity_type
,decode (MANAGE_STATUS,0,'Ignored',1,'Not yet managed',2,'Managed') managed_status
,decode (broken_reason,0,'Not Broken',Broken_str) broken_reason
,decode(promote_status,0,'Cannot Promote',1,'Can be promoted',2,'Promotion in progress',3,'Promoted') promote_status
,to_char(load_timestamp,'dd-mon-yyyy hh24:mi') load_timestamp
from sysman.MGMT$MANAGEABLE_ENTITIES
where host_name = 'scrbafldk001381.crb.apmoller.net'
and MANAGE_STATUS=2 and promote_status=3;

--Bind variable values
select * from table (dbms_xplan.display_cursor('[mysqlid]',[child], format => 'TYPICAL +PEEKED_BINDS'));
select * from table (dbms_xplan.display_cursor('725v8ds1f67n3',0, format => 'TYPICAL +PEEKED_BINDS'));

--Check device mount points
lsblk --output NAME,KNAME,TYPE,SIZE,MOUNTPOINT

-- check sar file for day 10 for sdc device util%
sar -p -A -f /var/log/sa/sa01 |grep sdc

--How many PX servers(QCs) do we have
--list them with their requested and actual degrees
select qcsid, req_degree, degree, count(*)
from v$px_session group by qcsid, req_degree, degree order by qcsid;





--oswatcher sample command
java -jar oswbba.jar -i /opt/app/oracle/admin/tfa_repository/oracle.ahf/data/repository/suptools/scrbsmddk002229/oswbb/oracle/archive -B Feb 22 11:00:00 2021 -E Feb 22 12:30:00 2021 swingbench -s


--grant select privilege on base tables of a view to user NPU030
select 'grant select on '||d.referenced_owner||'.'||d.referenced_name||' to TGU041;' from dba_tab_privs t,dba_dependencies d where t.grantee='NPU030' and t.table_name=d.name and t.type='VIEW';

--find out DB lock holders
select decode(request,0,'Holder: ','Waiter: ')||sid sess,
id1, id2, lmode, request, type
from gv$lock
where (id1, id2, type) in
(select id1, id2, type from gv$lock where request>0)
order by id1, request;

--find out the explain plan and cost of SQl PLAN BASELINES
SELECT * FROM   TABLE(DBMS_XPLAN.display_sql_plan_baseline(plan_name=>'S38098_2050603226_AWR'));

--dbid,instance_id,snapshot begin,snapshot end
--The information above is supplied for the two compare periods as shown below
select * from TABLE(DBMS_WORKLOAD_REPOSITORY.awr_diff_report_text(1610549073,1,240153,240157,1610549073,1,240249,240253)); 

SELECT * FROM table (DBMS_XPLAN.DISPLAY_CURSOR('9ynjpksv9f39p',1));
SELECT * FROM table (DBMS_XPLAN.DISPLAY_CURSOR('3b2sc1wqmu108'));

--Query SQL_MONITORING to find out real time response time of a sql_id
SELECT DBMS_SQLTUNE.report_sql_monitor(
  sql_id       => '9ynjpksv9f39p',
  report_level => 'ALL') AS report
FROM dual;



select * from DBA_JOBS_RUNNING;
select * from dba_jobs where job=108;

--find out all the plans for the sql_id
SELECT DISTINCT PLAN_HASH_VALUE,SQL_ID  FROM DBA_HIST_SQLSTAT
WHERE SQL_ID='9ynjpksv9f39p';


select * from v$sql where  sql_text like '%MODS_MSG PROCESSED_IND%';
select distinct PROCESSED,count(*) from GETDEV.MODS_MSG group by PROCESSED;


select * from stage;


DECLARE
  l_plans_unpacked  PLS_INTEGER;
BEGIN
  l_plans_unpacked := DBMS_SPM.unpack_stgtab_baseline(
    table_name      => 'STAGE',
    table_owner     => 'CAJ020',
    creator         => 'SYS');
DBMS_OUTPUT.put_line('Plans Unpacked: ' || l_plans_unpacked);
END;
/

select * from dba_sql_plan_baselines where SQL_HANDLE='SQL_73cabb888a25f3d6' ;
--Oracle DataGuard Queries



Primary: SQL> select thread#, max(sequence#) "Last Primary Seq Generated"
from v$archived_log val, v$database vdb
where val.resetlogs_change# = vdb.resetlogs_change#
group by thread# order by 1;

PhyStdby:SQL> select thread#, max(sequence#) "Last Standby Seq Received"
from v$archived_log val, v$database vdb
where val.resetlogs_change# = vdb.resetlogs_change#
group by thread# order by 1;

PhyStdby:SQL>select thread#, max(sequence#) "Last Standby Seq Applied"
from v$archived_log val, v$database vdb
where val.resetlogs_change# = vdb.resetlogs_change#
and val.applied in ('YES','IN-MEMORY')
group by thread# order by 1;

--MOnitoring Redo Transport for each redo destination
SELECT FREQUENCY, DURATION FROM 
V$REDO_DEST_RESP_HISTOGRAM WHERE DEST_ID=2 AND FREQUENCY>1;

--Check for redo Gap run from standby
SELECT * FROM V$ARCHIVE_GAP;
