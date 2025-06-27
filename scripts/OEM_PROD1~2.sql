--list of all databases by patch release in OEM

select * from sysman.mgmt$applied_patches ;
select distinct a.target_name,a.host_name,b.patch_release,b.patch,b.host,b.home_location,b.home_name
from sysman.mgmt$target a,sysman.MGMT$APPLIED_PATCHES b
where a.host_name=b.host
and a.target_type in ('oracle_database') and b.patch_release like '11.1%' ;


SELECT  DISTINCT Lower(Replace(target_name, '.apmoller.net:3872', ''))
                target_name
FROM   (SELECT Replace(target_name, '.crb.apmoller.net:1830', '') target_name
        FROM   (SELECT
        Replace(agent_version.target_name, '.crb.apmoller.net:3872', '')
               target_name
                FROM   sysman.mgmt$target_properties agent_version
                WHERE  agent_version.target_type = 'oracle_emd'
                       AND agent_version.property_name = 'Version'
                       AND agent_version.property_value = '13.4.0.0.0'))
                       where Lower(Replace(target_name, '.apmoller.net:3872', ''))='scrbwlsdk006118';
                       
                       
                       
                    
SELECT DISTINCT Lower(Replace(target_name, '.apmoller.net:3872', ''))
                target_name
FROM   (SELECT Replace(target_name, '.crb.apmoller.net:1830', '') target_name
        FROM   (SELECT
        Replace(agent_version.target_name, '.crb.apmoller.net:3872', '')
               target_name
                FROM   sysman.mgmt$target_properties agent_version
                WHERE  agent_version.target_type = 'oracle_emd'
                       AND agent_version.property_name = 'Version'
                       AND agent_version.property_value = '13.2.0.0.0')
        WHERE  Lower(Replace(target_name, '.apmoller.net:3872', '')) NOT IN
               (SELECT target_name
                FROM  (
               SELECT
                      DISTINCT Lower(Replace(target_name, '.apmoller.net:3872',
                                     ''))
                                      target_name,
                               Length(Replace(Trim(target_name),
                                      '.apmoller.net:3872',
                                      ''))
                                      Length,
                               Instr(Lower(Trim(target_name)), 'a', 5, 1)
                               AS
                                      "first_a_position",
                               Instr(Lower(Trim(target_name)), 'd', 5, 1)
                               AS
                                      "first_d_position"
                      FROM   (
                      SELECT Replace(target_name, '.crb.apmoller.net:1830', '')
                             target_name
                      FROM   (SELECT
        Replace(agent_version.target_name, '.crb.apmoller.net:3872', '')
                target_name
                 FROM   sysman.mgmt$target_properties agent_version
                 WHERE  agent_version.target_type = 'oracle_emd'
                        AND agent_version.property_name = 'Version'
                        AND agent_version.property_value = '13.2.0.0.0'))
                                                               WHERE
        target_name LIKE 'po%'
        AND Instr(Lower(Trim(target_name)), 'd', 5, 1) = 5))
        MINUS
        SELECT h_name
        FROM   (WITH version
                     AS (SELECT Replace(h_name, '.apmoller.net:3872', '') h_name
                         FROM  ((SELECT Replace(target_name,
                                        '.crb.apmoller.net:1830',
                                        '')
                                        h_name
                                 FROM   (SELECT Replace(target_name,
                                                '.crb.apmoller.net:3872',
                                                '')
                                                target_name
                                         FROM   sysman.mgmt$target_properties
                                                agent_version
                                         WHERE
                               agent_version.target_type = 'oracle_emd'
                               AND
                                        agent_version.property_name = 'Version'
                                                AND
agent_version.property_value = '13.2.0.0.0')
))),
db_host
AS (SELECT Replace(server, '.apmoller.net:3872', '') server,
target_type
FROM   (SELECT(
Replace(os.target_name, '.crb.apmoller.net', '') )
server,
db.target_type
target_type
FROM   sysman.mgmt$target db,
sysman.mgmt$target os
WHERE  db.host_name = os.target_name
AND db.target_type = 'oracle_database'
AND os.target_type = 'host'))
SELECT Lower(h_name) h_name
FROM   version,
db_host
WHERE  version.h_name = db_host.server)); 



select created,object_name,object_type from dba_objects where owner='CAJ020' order by created desc;


--desc NON_DB_UPGRADE
select * from dba_directories;

select * from (select x.*
from agent_upgrade ,xmltable('/jobExecution/steps/step' 
passing agent_upgrade.XML_COLUMN
columns
"command" varchar2(200) PATH '@command',
"name" varchar2(200) PATH '@name',
"target" varchar2(100) PATH '@target',
"output" clob PATH '/step/stepOutput/output') x)
where  "name"= 'deployAgentUpgrade' and 
"output"  like '%Finished post install%';

select * from dba_directories;



WITH version as(select replace(target_name,'.crb.apmoller.net:1830','') h_name,agent from 
(select replace(target_name,'.crb.apmoller.net:3872','') target_name,target_name agent from sysman.mgmt$target_properties agent_version
where agent_version.target_type='oracle_emd'
and agent_version.property_name='Version'
and agent_version.property_value='13.2.0.0.0')
),  
db_host as(
select  distinct(replace(os.TARGET_NAME,'.crb.apmoller.net','')) server,db.target_type target_type
from SYSMAN.MGMT$TARGET db, SYSMAN.MGMT$TARGET os
where db.HOST_NAME = os.TARGET_NAME
and db.target_type !='oracle_database'
and os.target_type='host'
)
select * from version,db_host
where version.h_name=db_host.server;


select host_name,entity_name,entity_type
,decode (MANAGE_STATUS,0,'Ignored',1,'Not yet managed',2,'Managed') managed_status
,decode (broken_reason,0,'Not Broken',Broken_str) broken_reason
,decode(promote_status,0,'Cannot Promote',1,'Can be promoted',2,'Promotion in progress',3,'Promoted') promote_status
,to_char(load_timestamp,'dd-mon-yyyy hh24:mi') load_timestamp
from sysman.MGMT$MANAGEABLE_ENTITIES
where host_name = 'scrbgcxdkgls125.crb.apmoller.net' ;
--scrbgcxdkbry126
desc agent_upgrade;
select * from dba_directories;


select * from (select x.*
from DB_to_13_4 ,xmltable('/jobExecution/steps/step' 
passing DB_to_13_4.XML_COLUMN
columns
"command" varchar2(200) PATH '@command',
"name" varchar2(200) PATH '@name',
"target" varchar2(100) PATH '@target',
"output" clob PATH '/step/stepOutput/output') x)
where  "name"= 'unzipAgentImage' order by "target" ;

SELECT * FROM gv$rsrc_plan;

SELECT *
  FROM gv$rsrc_consumer_group order by inst_id, cpu_wait_time desc;
  
  select sid,serial#,inst_id,username,sql_id,round(s.LAST_CALL_ET/60,2) LAST_CALL_ET_MINUTES ,prev_sql_id,prev_exec_start,status,osuser,machine,program,module,logon_time,FINAL_BLOCKING_SESSION,final_blocking_instance,event,wait_class,seconds_in_wait,state
from gv$session s
where type <> 'BACKGROUND' and status='ACTIVE'  order by 6 desc;


alter system kill session '3415,44729,@4' immediate;
select * from dba_sql_profiles;

select s.inst_id, s.sid,s.serial#,p.spid from gv$session s,gv$process p
where s.paddr=p.addr and s.inst_id=4 
and s.sid=3415;
--result of above 
4	3415	44729	30452

select * from DBA_SCHEDULER_RUNNING_JOBS;
select * from dba_scheduler_jobs where owner='SYSMAN' and job_name='EM_REPOS_SEV_EVAL';
select job_name from dba_scheduler_jobs where owner='SYSMAN' and job_name not like '%TASK_WORKER%';
select distinct log_message from sysman.emdw_trace_data where log_timestamp > (sys_extract_utc(systimestamp) - 1) and module='EM.jobs';

select * from dba_scheduler_job_run_details where job_name='EM_REPOS_SEV_EVAL' order by log_date desc;

select rule_name, condition, action from dba_scheduler_chain_rules where chain_name = 'EM_REPO_SEV_CHAIN';
select step_name, completed, state, start_date from dba_scheduler_running_chains;


select inst_id,sid,serial#,username,sql_id,round(s.LAST_CALL_ET/60,2) LAST_CALL_ET_MINUTES ,plsql_entry_object_id,status,osuser,machine,program,module,logon_time,FINAL_BLOCKING_SESSION,FINAL_BLOCKING_instance,event,wait_class,seconds_in_wait,state
from gv$session s where type <> 'BACKGROUND' and s.sid=39;

select 

module=OEM.PbsCacheModeWaitPingPool, object_id=107085;

select * from dba_objects where object_id=106893;

select * from gv$active_session_history where session_id=39 and inst_id=4;


3415	44729

alter system kill session '3415,44729,@4' immediate;

select inst_id,sql_id,plsql_entry_object_id,machine,program
from gv$active_session_history where sql_id='ahuhgwr70qxcm';

select instance_id,log_Date,owner,job_name,job_subname,status,req_start_Date,actual_start_date,run_duration from (
select * from dba_scheduler_job_run_details
where job_name ='EM_SLM_COMP_SCHED_JOB' order by log_date desc)
where rownum <=500;


select count(*),instance_id from dba_scheduler_job_run_details where owner='SYSMAN' and job_name like 'EM%' group by instance_id;
select instance_id,owner,job_name,job_subname,failure_count,LAST_START_DATE,LAST_RUN_DURATION,NEXT_RUN_DATE from dba_scheduler_jobs where job_name  like 'EM%' and job_name not like '%TASK%';

select log_Date,owner,job_name,job_subname,status,operation,additional_info from (
select * from DBA_SCHEDULER_JOB_LOG
where job_name ='EM_SLM_COMP_SCHED_JOB' order by log_date desc)
where rownum <=500;

select owner,object_type,object_name from dba_objects where object_id=106993;
select * from dba_objects where object_name='EM_SYSAVAIL_CHANGE';

select * from gv$sql where sql_id='gb1gwzjf7chgq';

select a.instance_number inst_id, a.snap_id,a.plan_hash_value, to_char(begin_interval_time,'dd-mon-yy hh24:mi') btime, abs(extract(minute from (end_interval_time-begin_interval_time)) + extract(hour from (end_interval_time-begin_interval_time))*60 + extract(day from (end_interval_time-begin_interval_time))*24*60) minutes,
executions_delta executions, round(ELAPSED_TIME_delta/1000000/greatest(executions_delta,1),4) "avg duration (sec)" from dba_hist_SQLSTAT a, dba_hist_snapshot b
where sql_id='gb1gwzjf7chgq' and a.snap_id=b.snap_id
and a.instance_number=b.instance_number
order by snap_id desc, a.instance_number;



select owner,table_name,num_rows,LAST_ANALYZED from dba_tables where table_name like '%SYSAVAIL_CHANGE%';

select * from gv$sql where sql_text like '%EM_GATHER_SYSMAN_STATS%';


select * from sysman.mgmt$applied_patches ;
select distinct a.target_name,a.host_name,b.patch_release,b.patch,b.host,b.home_location,b.home_name
from sysman.mgmt$target a,sysman.MGMT$APPLIED_PATCHES b
where a.host_name=b.host
and a.target_type in ('oracle_database') and b.patch_release like '11.1%' ;

desc sysman.mgmt$applied_patches;
select distinct target_type from sysman.mgmt$target order by target_type;

