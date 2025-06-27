define START_DTM ='&1'
define END_DTM   ='&2'

set newpage 0
SET UNDERLINE off
SET RECSEP off
set feed off
set echo off
SET COLSEP '|'
SET trimspool on pagesize 0 head off feed off lines 1000 verify off serverout on

SPOOL PMS_triggers.txt

PROMPT h3. PMS Trigger Information
PROMPT {toc:type=list}
PROMPT h5. Analysis Period
select 'Start Time : ' || to_char(TO_DATE('&&START_DTM', 'DDMONYY-HH24:MI' ) , 'dd/mm/yyyy hh24:mi' ) FROM DUAL;
select 'End Time   : ' || to_char(TO_DATE('&&END_DTM'  , 'DDMONYY-HH24:MI' ) , 'dd/mm/yyyy hh24:mi' ) FROM DUAL;
PROMPT 
PROMPT h5. Database Application Class Waits
select '|Overall Database Activity|<ask DBA to add screenshot from OEM>|' from dual;
select '|Application Wait Class Activity|<ask DBA to add screenshot from OEM>|' from dual;
select '|Application Wait Class SQL|<ask DBA to add screenshot from OEM>|' from dual;

PROMPT h5. Object Summary enq: TX - row lock contention
select '{csv:allowExport=true|columnTypes=s,s,s,i,f,f,f,f|id=ObjectWaitSummary}' from dual;
select '"Owner","Object Name","Object Type","Number of Waits","Min Wait Time (ms)","Max Wait Time (ms)","Average Wait Time (ms)","Total Wait Time (ms)"' from dual;
select '"' || owner
       || '","' || object_name
       || '","' || object_type
       || '","' || count(*) 
       || '","' || round(min(time_ms),2) 
       || '","' || round(max(time_ms),2) 
       || '","' || round(AVG(time_ms),2) 
       || '","' || round(sum(time_ms),2) 
       || '"' 
  from (SELECT ASH.event, ASH.current_obj#, ASH.sample_time, ash.time_waited/1000 time_ms, OBJ.object_name, obj.owner, obj.object_type
          FROM dba_hist_active_sess_history ASH, dba_objects OBJ
         WHERE ASH.event = 'enq: TX - row lock contention'
           AND ASH.sample_time BETWEEN to_date('&&START_DTM', 'DDMONYY-HH24:MI') AND to_date('&&END_DTM', 'DDMONYY-HH24:MI')
           AND ASH.current_obj# = OBJ.object_id
        UNION
        SELECT ASHS.event, ASHS.current_obj#, ASHS.sample_time, ashs.time_waited/1000 time_ms, OBJ.object_name, obj.owner, obj.object_type
          FROM v$active_session_history ASHS, dba_objects OBJ
         WHERE ASHS.event = 'enq: TX - row lock contention'
           AND ASHS.sample_time BETWEEN to_date('&&START_DTM', 'DDMONYY-HH24:MI') AND to_date('&&END_DTM', 'DDMONYY-HH24:MI')
           AND ASHS.current_obj# = OBJ.object_id)
group by owner, object_name, object_type
order by 1; 
select '{csv}' from dual;

PROMPT h5. Object Row Lock Contention Per Hour
select '{csv:allowExport=true|columnTypes=s,s,s,s,i,f,f,f,f|id=ObjectRLWPerHour}' from dual;
select '"Owner","Object Name","Object Type","Period Start (60 Min Duration)","Number of Waits","Min Wait Time (ms)","Max Wait Time (ms)","Average Wait Time (ms)","Total Wait Time (ms)"' from dual;
select  '"' || owner
       || '","' || object_name
       || '","' || object_type
       || '","' || to_char(trunc(sample_time, 'hh24'),'dd/mm/yyyy hh24:mi') 
       || '","' || count(*) 
       || '","' || round(min(time_ms),2)
       || '","' || round(max(time_ms),2)
       || '","' || round(AVG(time_ms),2)
       || '","' || round(sum(time_ms),2) 
       || '"' 
  from (SELECT obj.owner, OBJ.object_name, obj.object_type, ASH.sample_time, ash.time_waited/1000 time_ms
          FROM dba_hist_active_sess_history ASH, dba_objects OBJ
         WHERE ASH.event = 'enq: TX - row lock contention'
           AND ASH.sample_time BETWEEN to_date('&&START_DTM', 'DDMONYY-HH24:MI') AND to_date('&&END_DTM', 'DDMONYY-HH24:MI')
           AND ASH.current_obj# = OBJ.object_id
        UNION
        SELECT obj.owner, OBJ.object_name, obj.object_type, ASHS.sample_time, ashs.time_waited/1000 time_ms
          FROM v$active_session_history ASHS, dba_objects OBJ
         WHERE ASHS.event = 'enq: TX - row lock contention'
           AND ASHS.sample_time BETWEEN to_date('&&START_DTM', 'DDMONYY-HH24:MI') AND to_date('&&END_DTM', 'DDMONYY-HH24:MI')
           AND ASHS.current_obj# = OBJ.object_id)
group by owner, object_name, object_type, to_char(trunc(sample_time, 'hh24'),'dd/mm/yyyy hh24:mi')
order by 1;
select '{csv}' from dual;

select 'h5. Tracker Table Partition Counts' from dual;
select '{csv:allowExport=true|columnTypes=s,s,i|id=PartitionCounts}' from dual;
select '"Owner","Table Name","Partition Count"' from dual;
select '"' || table_owner
       || '","' || table_name
	   || '","' || count(*) 
       || '"' 
  from dba_tab_partitions
 where table_name like '%TRACKER' 
   and table_owner = 'CBSSERVICES'
group by table_owner, table_name
order by 1;
select '{csv}' from dual;

PROMPT h5. Tracker Table Addition by Day
select '{csv:allowExport=true|columnTypes=s,s,i|id=TrackerTableData}' from dual;
select '"Table Name","Day","Num Rows"' from dual;
select '"' || tab_name 
       || '","' || aDAY
       || '","' || num_rows
       || '"' 
  from (select 'BSBVISITTRACKER' as tab_name, trunc(LASTUPDATED_UTC) aday, count(*) as num_rows from CBSSERVICES.BSBVISITTRACKER group by trunc(LASTUPDATED_UTC) 
        union
        select 'BSBSUBSCRIPTIONTRACKER', trunc(LASTUPDATED_UTC) , count(*) from CBSSERVICES.BSBSUBSCRIPTIONTRACKER group by trunc(LASTUPDATED_UTC) 
        union
        select 'BSBSERVICETRACKER', trunc(LASTUPDATED_UTC) , count(*) from CBSSERVICES.BSBSERVICETRACKER group by trunc(LASTUPDATED_UTC) 
        union
        select 'BSBPRODUCTTRACKER', trunc(LASTUPDATED_UTC) , count(*) from CBSSERVICES.BSBPRODUCTTRACKER group by trunc(LASTUPDATED_UTC) 
        union
        select 'BSBPAYMENTPLANTRACKER', trunc(LASTUPDATED_UTC) , count(*) from CBSSERVICES.BSBPAYMENTPLANTRACKER group by trunc(LASTUPDATED_UTC) 
        union
        select 'BSBOFFERTRACKER', trunc(LASTUPDATED_UTC) , count(*) from CBSSERVICES.BSBOFFERTRACKER group by trunc(LASTUPDATED_UTC) 
        union
        select 'BSBCONTRACTTRACKER', trunc(LASTUPDATED_UTC) , count(*) from CBSSERVICES.BSBCONTRACTTRACKER group by trunc(LASTUPDATED_UTC) 
        union
        select 'BSBCHARGETRACKER', trunc(LASTUPDATED_UTC) , count(*) from CBSSERVICES.BSBCHARGETRACKER group by trunc(LASTUPDATED_UTC) 
        union
        select 'BSBACCOUNTTRACKER', trunc(LASTUPDATED_UTC) , count(*) from CBSSERVICES.BSBACCOUNTTRACKER group by trunc(LASTUPDATED_UTC) 
        order by 1, 2);
select '{csv}' from dual;

PROMPT h5. All Instances of "enq: TX - row lock contention"
select '{csv:allowExport=true|columnTypes=s,s,s,s,f|id=AllRowLocks}' from dual;
select '"Owner","Object Name","Object Type","Sample Time","Wait Time (ms)"' from dual;
select '"' || OWNER 
       || '","' || OBJECT_NAME
       || '","' || OBJECT_TYPE
       || '","' || SAMPLE_TIME
       || '","' || time_ms
       || '"' 
  from (SELECT obj.owner, OBJ.object_name, obj.object_type, ASH.sample_time, ash.time_waited/1000 time_ms
          FROM dba_hist_active_sess_history ASH, dba_objects OBJ
         WHERE ASH.event = 'enq: TX - row lock contention'
           AND ASH.sample_time BETWEEN to_date('&&START_DTM', 'DDMONYY-HH24:MI') AND to_date('&&END_DTM', 'DDMONYY-HH24:MI')
           AND ASH.current_obj# = OBJ.object_id
        UNION
        SELECT obj.owner, OBJ.object_name, obj.object_type, ASHS.sample_time, ashs.time_waited/1000 time_ms
          FROM v$active_session_history ASHS, dba_objects OBJ
         WHERE ASHS.event = 'enq: TX - row lock contention'
           AND ASHS.sample_time BETWEEN to_date('&&START_DTM', 'DDMONYY-HH24:MI') AND to_date('&&END_DTM', 'DDMONYY-HH24:MI')
           AND ASHS.current_obj# = OBJ.object_id
        ORDER  BY sample_time ); 
select '{csv}' from dual;

spool off
set head on
SET UNDERLINE on
set feed on
set pages 1000
set verify on