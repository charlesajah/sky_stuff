select segment_name,round(bytes/1024/1024/1024,2) "size_gb",partition_name,segment_type,tablespace_name from dba_segments
where tablespace_name='TCC_TDE_LOB_AUTO_01'
order by "size_gb" desc;

alter session set nls_date_format='DD-MON-YYYY HH24:MI:SS';

select * from v$session_longops
where target='TCC_OWNER.STG_TABLE'
and sofar < totalwork;

select * from v$sql
where sql_text like '%TCC_OWNER.STG_TABLE%';

select * from v$session
where sql_id='8262r0c7s7a53';

select sid,serial#,inst_id,username,sql_id,round(s.LAST_CALL_ET/60,2) LAST_CALL_ET_MINUTES ,prev_sql_id,prev_exec_start,status,osuser,machine,program,module,logon_time,FINAL_BLOCKING_SESSION,event,wait_class,seconds_in_wait,state
from gv$session s
where inst_id=1 and sql_id='8262r0c7s7a53'  order by 6 desc;

select sid,serial#,inst_id,username,sql_id,round(s.LAST_CALL_ET/60,2) LAST_CALL_ET_MINUTES ,prev_sql_id,prev_exec_start,status,osuser,machine,program,module,logon_time,FINAL_BLOCKING_SESSION,event,wait_class,seconds_in_wait,state
from gv$session s
where sid=757;

alter system kill session '757,5504' immediate;


select s.sid,s.SERIAL#,s.username,s.PROGRAM,s.MODULE,s.INST_ID,p.SPID,s.SQL_ID,s.WAIT_CLASS,s.EVENT,round(s.LAST_CALL_ET/60,2) LAST_CALL_ET_MINUTES from gv$session s,gv$process p
where s.INST_ID=1 and s.type !='BACKGROUND' and  s.PADDR=p.ADDR and s.sid=836;


select * from dba_sql_profiles;
select host_name from v$instance;
select * from dba_directories;

select username from dba_users
where username='DATAPROV';

select tablespace_name from dba_tablespaces;

select distinct obj_name  from dataprov.STAGE_PROF;
select name from dba_sql_profiles;



grant select on dba_sql_profiles to dataprov;
grant read on directory temp to dataprov;
grant write on directory temp to dataprov;

select * from(
select * from dataprov.datapump_log
order by timestamp desc)
where rownum=1;

select * from dataprov.datapump_log
order by timestamp desc;

select sysdate from dual;
select created from dba_objects where object_name='STAGE_PROF';


select distinct obj_name profile_name from dataprov.stage_prof stg
inner join
dba_sql_profiles dsp
on stg.obj_name=dsp.name;



select distinct obj_name profile_name from dataprov.stage_prof 
    where obj_name not in (select name from  dba_sql_profiles);
    
    select distinct obj_name profile_name from dataprov.stage_prof 
    where obj_name not in (select name from  dba_sql_profiles);
    
    
    select * from dataprov.error_log;
    
    select * from dba_sql_profiles;
    
    EXEC DBMS_SQLTUNE.DROP_SQL_PROFILE('PROFILE_7kkuv2annp7st');
    
    select * from dba_sql_plan_baselines;
    
    select object_name,owner from dba_objects
    where object_name='DBMS_SPM';
    
    
    select * from dataprov.stage_plan ;
    select created from dba_objects
    where object_name='STAGE_PROF';
    
    
    select * from dba_sql_plan_baselines;
    select * from dataprov.datapump_log;
   create table dataprov.datapump_log
(timestamp date,
directory varchar2(250),
dumpfile varchar2(60),
type varchar2(50),
table_name varchar2(30));
    

