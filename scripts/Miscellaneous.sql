
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

--finding segment sizes for different segment types
COLUMN TABLE_NAME FORMAT A32
COLUMN OBJECT_NAME FORMAT A32
COLUMN OWNER FORMAT A10
select * from (
SELECT
owner, table_name,segment_type, TRUNC(sum(bytes)/1024/1024/1024) Size_GB
FROM
(SELECT segment_name table_name, owner, bytes ,segment_type
FROM dba_segments  WHERE segment_type = 'TABLE'
UNION ALL
SELECT i.table_name, i.owner, s.bytes ,segment_type FROM dba_indexes i, dba_segments s
WHERE s.segment_name = i.index_name AND   s.owner = i.owner
AND   s.segment_type = 'INDEX'
UNION ALL
SELECT l.table_name, l.owner, s.bytes ,segment_type
FROM dba_lobs l, dba_segments s
WHERE s.segment_name = l.segment_name
AND   s.owner = l.owner
AND   s.segment_type = 'LOBSEGMENT'
UNION ALL
SELECT l.table_name, l.owner, s.bytes ,segment_type
FROM dba_lobs l, dba_segments s
WHERE s.segment_name = l.index_name
AND   s.owner = l.owner
AND   s.segment_type = 'LOBINDEX')
GROUP BY owner,table_name,segment_type
ORDER BY SUM(bytes) desc
)
where Size_GB > 10
and owner !='CCSOWNER';

--How many CPUs on a server
--i had to add case because of how OEM stores CPU details for s390x architecture which is zLinux
--instance_count is OEM equivalent of socket i.e. number of CPU sockets
--num_cores is number of CPUs per socket
--so basically you multiply num_cores * instance_count to get CPU count
--but in s390x architecture num_cores is stored as zero
--hence the case takes care of that discrepancy

select target_name,instance_count,
    case num_cores
        when 0 then instance_count
        else 
        num_cores * instance_count 
    end num_cpus
        from sysman.MGMT$HW_CPU_DETAILS;



--How many PX servers(QCs) do we have
--list them with their requested and actual degrees
select qcsid, req_degree, degree, count(*)
from v$px_session group by qcsid, req_degree, degree order by qcsid;


--script to find out data files that can be resized in order to reclaim space
set verify off
set pages 1000
column file_name format a50 word_wrapped
column smallest format 999,990 heading "Smallest|Size|Poss."
column currsize format 999,990 heading "Current|Size"
column savings  format 999,990 heading "Poss.|Savings"
break on report
compute sum of savings on report

--To Check Database block size:--
column value new_val blksize
select value from v$parameter where name = 'db_block_size'
/

--find all the files than can be resized in tablespace SNTL_DATA_WORK for example
select file_name,
       ceil( (nvl(hwm,1)*&&blksize)/1024/1024 ) smallest,
       ceil( blocks*&&blksize/1024/1024) currsize,
       ceil( blocks*&&blksize/1024/1024) -
       ceil( (nvl(hwm,1)*&&blksize)/1024/1024 ) savings
from dba_data_files a,
     ( select file_id, max(block_id+blocks-1) hwm
         from dba_extents
        group by file_id ) b
where a.tablespace_name='SNTL_DATA_WORK' and
a.file_id = b.file_id(+)




--REDO Log switch report
set pages 999 lines 400
col h0 format 999
col h1 format 999
col h2 format 999
col h3 format 999
col h4 format 999
col h5 format 999
col h6 format 999
col h7 format 999
col h8 format 999
col h9 format 999
col h10 format 999
col h11 format 999
col h12 format 999
col h13 format 999
col h14 format 999
col h15 format 999
col h16 format 999
col h17 format 999
col h18 format 999
col h19 format 999
col h20 format 999
col h21 format 999
col h22 format 999
col h23 format 999
SELECT TRUNC (first_time) "Date", inst_id, TO_CHAR (first_time, 'Dy') "Day",
 COUNT (1) "Total",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '00', 1, 0)) "h0",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '01', 1, 0)) "h1",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '02', 1, 0)) "h2",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '03', 1, 0)) "h3",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '04', 1, 0)) "h4",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '05', 1, 0)) "h5",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '06', 1, 0)) "h6",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '07', 1, 0)) "h7",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '08', 1, 0)) "h8",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '09', 1, 0)) "h9",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '10', 1, 0)) "h10",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '11', 1, 0)) "h11",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '12', 1, 0)) "h12",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '13', 1, 0)) "h13",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '14', 1, 0)) "h14",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '15', 1, 0)) "h15",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '16', 1, 0)) "h16",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '17', 1, 0)) "h17",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '18', 1, 0)) "h18",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '19', 1, 0)) "h19",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '20', 1, 0)) "h20",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '21', 1, 0)) "h21",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '22', 1, 0)) "h22",
 SUM (DECODE (TO_CHAR (first_time, 'hh24'), '23', 1, 0)) "h23",
 ROUND (COUNT (1) / 24, 2) "Avg"
FROM gv$log_history
WHERE thread# = inst_id
AND first_time > sysdate -7
GROUP BY TRUNC (first_time), inst_id, TO_CHAR (first_time, 'Dy')
ORDER BY 1,2;



--sum todat of SGA Size accross all nodes
  select sum(value)/1024/1024/1024 Total_size_In_GB from gV$sga;
  
  --UNUSED SGA memory accross nodes
   Select POOL, Round(bytes/1024/1024,0) Free_Memory_In_MB
   From gV$sgastat
   Where Name Like '%free memory%';
   
   
     
select sid,serial#,inst_id,username,sql_id,round(s.LAST_CALL_ET/60,2) LAST_CALL_ET_MINUTES ,prev_sql_id,prev_exec_start,status,osuser,machine,program,module,logon_time,FINAL_BLOCKING_SESSION,event,wait_class,seconds_in_wait,state
from gv$session s
where inst_id=1 and  type <> 'BACKGROUND' and status='ACTIVE'  order by 6 desc;

select sid,serial#,inst_id,username,sql_id,round(s.LAST_CALL_ET/60,2) LAST_CALL_ET_MINUTES ,prev_sql_id,prev_exec_start,status,osuser,machine,program,module,logon_time,FINAL_BLOCKING_SESSION,event,wait_class,seconds_in_wait,state
from gv$session s
where inst_id=1 and  type <> 'BACKGROUND'  order by 6 desc;


--RMAN backup status details 
   select to_char(start_time,'DD-MON-YYYY HH24:MI:SS') , to_char(end_time,'DD-MON-YYYY HH24:MI:SS'),status,COMPRESSION_RATIO,TIME_TAKEN_DISPLAY,INPUT_BYTES_DISPLAY,INPUT_BYTES_PER_SEC_DISPLAY,OUTPUT_BYTES_DISPLAY,OUTPUT_BYTES_PER_SEC_DISPLAY,OUTPUT_DEVICE_TYPE  from V$RMAN_BACKUP_JOB_DETAILS where INPUT_TYPE='DB INCR' order by start_time desc;
   
--Monitor RMAN sessions
--usually the number of sessions equals the number of RMAN channels
--If you run the longops script at intervals of two minutes or more and the %_COMPLETE column does not increase, then RMAN is encountering a problem.
SELECT SID, SERIAL#, CONTEXT, SOFAR, TOTALWORK,
       ROUND(SOFAR/TOTALWORK*100,2) "%_COMPLETE"
FROM   V$SESSION_LONGOPS
WHERE  OPNAME LIKE 'RMAN%'
AND    OPNAME NOT LIKE '%aggregate%'
AND    TOTALWORK != 0
AND    SOFAR <> TOTALWORK;



-- CPU Consuming sessions and SQLs
select * from (
select se.INST_ID,to_char(ss.prev_exec_start,'DD-MON-YYYY HH24:MI:SS') prev_exec_start,to_char(ss.sql_exec_start,'DD-MON-YYYY HH24:MI:SS') sql_exec_start,
(se.SID),ss.serial#,ss.SQL_ID,ss.username,substr(ss.program,1,22) "program",ss.module,ss.osuser,ss.MACHINE,ss.status,
se.VALUE/100 cpu_usage_sec
from
gv$session ss,
gv$sesstat se,
gv$statname sn,
gv$process p
where
ss.sql_id='8bat9r81xttu1'
and 
se.STATISTIC# = sn.STATISTIC#
and
NAME like '%CPU used by this session%'
and
se.SID = ss.SID
and ss.username ='AUTORATESHEET' and
ss.status='ACTIVE'
and ss.username is not null
and ss.paddr=p.addr and value > 0
order by se.value desc); 


--- Longest running active sessions in a schema
select m.sql_exec_start
, m.sql_id
, m.sql_plan_hash_value
, substr(m.sql_text,1,60)
, m.error_number
, m.module
, s.sql_profile
, s.sql_plan_baseline
, round( m.elapsed_time /1000000/60,2)              as elapse_time_min
from gv$sql_monitor m, gv$sql s
where s.inst_id=m.inst_id and s.sql_id=m.sql_id and s.plan_hash_value = m.sql_plan_hash_value
and m.username in ('AUTORATESHEET')
and UPPER(m.sql_text) not like 'BEGIN%' AND UPPER(m.sql_text) not like ' BEGIN%' AND UPPER(m.sql_text) not like 'DECLARE%' AND m.sql_text not like '/* SQL Analyze%'
and s.sql_profile is  null
and s.sql_plan_baseline is  null
and m.sql_exec_start > sysdate-1/24
order by elapse_time_min desc;

---SQL MONITOR

select sid,serial#,inst_id,username,sql_id,round(s.LAST_CALL_ET/60,2) LAST_CALL_ET_MINUTES ,prev_sql_id,prev_exec_start,status,osuser,machine,program,module,logon_time,FINAL_BLOCKING_SESSION,event,wait_class,seconds_in_wait,state
from gv$session s
where inst_id=1 and  type <> 'BACKGROUND' and status='ACTIVE'  order by 6 desc;

select sid,serial#,inst_id,username,sql_id,round(s.LAST_CALL_ET/60,2) LAST_CALL_ET_MINUTES ,prev_sql_id,prev_exec_start,status,osuser,machine,program,module,logon_time,FINAL_BLOCKING_SESSION,event,wait_class,seconds_in_wait,state
from gv$session s
where inst_id=1 and  type <> 'BACKGROUND'  order by 6 desc;


--Map Linux OS processes to Oracle foreground processes
select s.sid,s.SERIAL#,s.username,s.PROGRAM,s.MODULE,s.INST_ID,p.SPID,s.SQL_ID,s.WAIT_CLASS,s.EVENT,round(s.LAST_CALL_ET/60,2) LAST_CALL_ET_MINUTES from gv$session s,gv$process p
where s.INST_ID=1 and s.type !='BACKGROUND' and  s.PADDR=p.ADDR and p.spid in (43346,47249,4356,59097,4470,3352,3348,59029,53623,12859,35432,51710,25111,62254,28958,64634) ;

---Long running Operations per SQL_ID
select inst_id,sid,serial#,target,sofar,totalwork,to_char(start_time,'DD-MON-YYYY HH24:MI:SS') start_time,to_char(last_update_time,'DD-MON-YYYY HH24:MI:SS') last_update_time,message,elapsed_seconds,username,sql_id,sql_plan_hash_value,to_char(sql_exec_start,'DD-MON-YYYY HH24:MI:SS') sql_exec_start 
from GV$SESSION_LONGOPS where sid in(select sid from gv$session where sql_id='4k6g7bk95brag');


--############# get the plan hash value , avg execution times for sql_id.########
WITH
p AS (
SELECT plan_hash_value
  FROM gv$sql_plan
WHERE sql_id = TRIM('&&sql_id.')
   AND other_xml IS NOT NULL
UNION
SELECT plan_hash_value
  FROM dba_hist_sql_plan
WHERE sql_id = TRIM('&&sql_id.')
   AND other_xml IS NOT NULL ),
m AS (
SELECT plan_hash_value,
       SUM(elapsed_time)/SUM(executions) avg_et_secs
  FROM gv$sql
WHERE sql_id = TRIM('&&sql_id.')
   AND executions > 0
GROUP BY
       plan_hash_value ),
a AS (
SELECT plan_hash_value,
       SUM(elapsed_time_total)/SUM(executions_total) avg_et_secs
  FROM dba_hist_sqlstat
WHERE sql_id = TRIM('&&sql_id.')
   AND executions_total > 0
GROUP BY
       plan_hash_value )
SELECT p.plan_hash_value,
       ROUND(NVL(m.avg_et_secs, a.avg_et_secs)/1e6, 3) avg_et_secs
  FROM p, m, a
WHERE p.plan_hash_value = m.plan_hash_value(+)
   AND p.plan_hash_value = a.plan_hash_value(+)
ORDER BY
       avg_et_secs NULLS LAST;
       
       
--#######
--Get the elapse time of the SQL for a given SQL_ID: from awr_history.
--##############
set pagesize 0
set lines 200
col execs for 999,999,999
col avg_etime for 999,999.999
col avg_lio for 999,999,999.9
col begin_interval_time for a30
col node for 99999
col sql_profile for a30
col module for a30
col action for a30
break on plan_hash_value on startup_time skip 1

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


--How many versions of a sql_id are running
--WHat plans are each version using
--What is the cost of each version
                                          
select sql_id,VERSION_COUNT,LOADED_VERSIONS,OPEN_VERSIONS,EXECUTIONS,PX_SERVERS_EXECUTIONS,USERS_EXECUTING,OPTIMIZER_COST,PLAN_HASH_VALUE,SQL_PROFILE,SQL_PLAN_BASELINE from gv$sqlarea where  sql_id='8bat9r81xttu1';                             
select sql_id,CHILD_NUMBER,FIRST_LOAD_TIME,LAST_LOAD_TIME,to_char(LAST_ACTIVE_TIME,'DD-MON-YYYY HH24:MI:SS') LAST_ACTIVE_TIME,EXECUTIONS, USERS_EXECUTING,PLAN_HASH_VALUE,SQL_PLAN_BASELINE,SQL_PROFILE,DISK_READS,BUFFER_GETS,USER_IO_WAIT_TIME,OPTIMIZER_COST,ELAPSED_TIME from gv$sql where sql_id='8bat9r81xttu1';
select inst_id,sid,SERIAL#,username,status,sql_id,SQL_CHILD_NUMBER,to_char(prev_exec_start,'DD-MON-YYYY HH24:MI:SS') prev_exec_start,to_char(sql_exec_start,'DD-MON-YYYY HH24:MI:SS') sql_exec_start,event,SERVICE_NAME from gv$session where sql_id='8bat9r81xttu1';

select * from dba_sql_plan_baselines where PARSING_SCHEMA_NAME='AUTORATESHEET' order by LAST_EXECUTED desc;
select * from gV$SQL_PLAN where sql_id='8bat9r81xttu1' and OPERATION='SELECT STATEMENT';

--Bind variable values
select * from table (dbms_xplan.display_cursor('[mysqlid]',[child], format => 'TYPICAL +PEEKED_BINDS'));
select * from table (dbms_xplan.display_cursor('725v8ds1f67n3',0, format => 'TYPICAL +PEEKED_BINDS'));



--
-- List Top events of Session Details for a Given Time Period
--
-- s_time format = '22/OCT/2014 04:00:00.000 PM'  
-- e_time format = '23/OCT/2014 04:00:00.000 PM'  
-- inst_no = Instance Number for RAC.  Use 1 for non RAC
--
SELECT NVL(a.event, 'ON CPU') AS event,
       COUNT(*)*10 AS total_wait_time
FROM   dba_hist_active_sess_history a
WHERE  a.sample_time BETWEEN '&s_time' and '&e_time'
GROUP BY a.event
ORDER BY total_wait_time DESC;


SET PAUSE ON
SET PAUSE 'Press Return To Continue'
SET HEADING ON
SET LINESIZE 300
SET PAGESIZE 60
 
COLUMN Sample_Time FOR A12
COLUMN username FOR A20
COLUMN sql_text FOR A40
COLUMN program FOR A40
COLUMN module FOR A40
 
SELECT
   NVL(h.event, 'ON CPU') AS event,
   u.username,
   h.program,
   h.sql_id,
   count(*)
FROM
   DBA_HIST_ACTIVE_SESS_HISTORY h,
   DBA_USERS u
WHERE  sample_time
BETWEEN '&s_time' and '&e_time'
AND
   INSTANCE_NUMBER=&inst_no
   AND h.user_id=u.user_id
   group by username,sql_id,event,program
   

--RMAN monitoring
 select cast(START_TIME as timestamp) as START_TIME,cast(END_TIME as timestamp) as end_time,INPUT_BYTES/1024/1024 as "input_bytes(MB)" ,INPUT_TYPE,status from v$rman_backup_job_details where START_TIME > trunc(sysdate)


--Create a Read-Only Role for schemas 
CREATE ROLE my_read_only_role;

BEGIN
  FOR x IN (SELECT owner||'.'||table_name tb_name FROM dba_tables WHERE owner in ('MDS','CED','CEGSIS','SFDCSCV','SFDCROUTE','SFDCD','SFDCSALES','ESB_AUD','ESB_BPM_MDS','ACTIVE_OSB','ACTIVE_WLS','PP_MDS','PP_SOAINFRA','SLT_JMS_WLSTORE'))
  LOOP
    EXECUTE IMMEDIATE 'GRANT SELECT ON '||x.tb_name||' TO L3_support_RO';
  END LOOP;
  FOR y IN (SELECT owner||'.'||view_name v_name  FROM dba_views WHERE owner in ('MDS','CED','CEGSIS','SFDCSCV','SFDCROUTE','SFDCD','SFDCSALES','ESB_AUD','ESB_BPM_MDS','ACTIVE_OSB','ACTIVE_WLS','PP_MDS','PP_SOAINFRA','SLT_JMS_WLSTORE'))
  LOOP
    EXECUTE IMMEDIATE 'GRANT SELECT ON '||y.v_name||' TO L3_support_RO';
  END LOOP;
END;


--how to load a sql baseline from cursor cache
DECLARE
  l_plans_loaded  PLS_INTEGER;
BEGIN
  l_plans_loaded := DBMS_SPM.load_plans_from_cursor_cache(sql_id => 'gat6z1bc6nc2d',plan_hash_value =>,fixed =>'YES');    
  DBMS_OUTPUT.put_line('Plans Loaded: ' || l_plans_loaded);
END;



--how to flush SQL_ID from cache 
select ADDRESS, HASH_VALUE from GV$SQLAREA where SQL_ID= '8rqxguzxz1fxf';
exec DBMS_SHARED_POOL.PURGE ('0000001E5CA100A8, 4226857902', 'C');
exec DBMS_SHARED_POOL.PURGE ('0000001E3D764410, 4102001793', 'C');



--DIskgroup and failgroup information

col diskgroup_name format a25 heading "Diskgroup Name"
col type   format a10 heading "Redundancy|Level"
col ocrdg  format a10  heading "OCR/Voting|Diskgroup"
col cnt    format 99  heading "Unique|Failgroups"
col status format a70 heading "Configuration Status"

select g.name diskgroup_name
     , fgs.cnt
     , g.type 
     , nvl(ocr.ocrdg,'NO') ocrdg
     , CASE WHEN ocr.ocrdg = 'YES' AND (fgs.cnt <3 OR  mod(fgs.cnt,2)=0) THEN 'ERROR - OCRVD diskgroups require an odd number of failgroups (min 3)'
            WHEN ocr.ocrdg = 'YES' AND (fgs.cnt >2 AND mod(fgs.cnt,2)=1) THEN 'OK - '||fgs.cnt||' failgroups'
            WHEN g.type='NORMAL' and fgs.cnt=2    THEN 'OK - '||fgs.cnt||' failgroups'
            WHEN g.type='NORMAL' and fgs.cnt<>2   THEN 'ERROR - NORMAL redundancy requires EXACTLY 2 failgroups'
            WHEN g.type='HIGH' and fgs.cnt=3      THEN 'OK - '||fgs.cnt||' failgroups'
            WHEN g.type='HIGH' and fgs.cnt<>3     THEN 'ERROR - HIGH redundancy requires EXACTLY 3 failgroups'
            WHEN g.type='EXTERN' THEN 'OK - external redundancy'
            ELSE 'N/A - Unable to determine status'
       END status       
  from v$asm_diskgroup g
     , (select group_number, 'YES' ocrdg from v$asm_file where type='OCRFILE') ocr
     , (select group_number, count(distinct failgroup) cnt from v$asm_disk group by group_number) fgs
where g.group_number = ocr.group_number (+)
   and g.group_number = fgs.group_number
   and g.state in ('MOUNTED','CONNECTED')
order by 1;

