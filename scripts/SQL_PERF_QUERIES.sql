-- QUERY 4 , plan history for a particular sql_id
SELECT q.sql_id , q.snap_id
     , s.end_interval_time
     , q.plan_hash_value
     , ROUND ( q.elapsed_time_delta / 1000000 ) AS secs
     , CASE WHEN q.executions_delta != 0 THEN ROUND ( q.elapsed_time_delta / q.executions_delta / 1000000 , 2 ) END AS secs_per_exec
     , CASE WHEN q.executions_delta != 0 THEN ROUND ( q.elapsed_time_delta / q.executions_delta / 1000 , 2 ) END AS millisecs_per_exec
     , ROUND ( q.executions_delta / 15 / 60 ) AS tps
     , CASE WHEN q.executions_delta != 0 THEN ROUND ( q.rows_processed_delta / q.executions_delta ) END AS rp_per_exec
     , CASE WHEN q.executions_delta != 0 THEN ROUND ( q.fetches_delta / q.executions_delta ) END AS fetches_pe
     , q.executions_delta
     , q.fetches_delta
     , q.rows_processed_delta
     , dbms_sqltune.extract_bind( q.bind_data , 1 ).value_string as b1
     , dbms_sqltune.extract_bind( q.bind_data , 2 ).value_string as b2
     , dbms_sqltune.extract_bind( q.bind_data , 3 ).value_string as b3
     , dbms_sqltune.extract_bind( q.bind_data , 4 ).value_string as b4
     , dbms_sqltune.extract_bind( q.bind_data , 5 ).value_string as b5
     , dbms_sqltune.extract_bind( q.bind_data , 6 ).value_string as b6
  FROM dba_hist_snapshot s
  JOIN dba_hist_sqlstat q ON q.snap_id = s.snap_id AND q.dbid = s.dbid AND q.instance_number = s.instance_number
 WHERE q.sql_id IN ( 'ag0xndt68xb0c' , 'g103pxcug49jf' )
 ORDER BY s.end_interval_time DESC
;
-- same but summary
SELECT q.sql_id
     , q.plan_hash_value , MIN(s.end_interval_time) , MAX( s.end_interval_time)
     , CASE WHEN SUM ( q.executions_delta) != 0 THEN ROUND ( SUM ( q.elapsed_time_delta) / SUM ( q.executions_delta) / 1000 , 2 ) END AS millisecs_per_exec
     , SUM ( q.executions_delta ) AS execs
     , SUM ( q.rows_processed_delta ) AS rows_tot
     , CASE WHEN SUM ( q.executions_delta) != 0 THEN ROUND ( SUM ( q.rows_processed_delta ) / SUM ( q.executions_delta) ) END AS rows_pe
     , ( SELECT t.sql_text FROM dba_hist_sqltext t WHERE t.sql_id = q.sql_id AND ROWNUM <= 1 ) AS sql_text
  FROM dba_hist_snapshot s
  JOIN dba_hist_sqlstat q ON q.snap_id = s.snap_id AND q.dbid = s.dbid AND q.instance_number = s.instance_number
 WHERE q.sql_id IN ( '62p8203hhmthf' , '67t066znyv3gn' )
 GROUP BY q.sql_id , q.plan_hash_value
 ORDER BY MAX ( s.end_interval_time ) DESC
;

--same as above but specifically for evening test times
SELECT q.sql_id
     , q.plan_hash_value , MIN(s.begin_interval_time) , MAX( s.end_interval_time)
     , CASE WHEN SUM ( q.executions_delta) != 0 THEN ROUND ( SUM ( q.elapsed_time_delta) / SUM ( q.executions_delta) / 1000 , 2 ) END AS millisecs_per_exec
     , SUM ( q.executions_delta ) AS execs
     , SUM ( q.rows_processed_delta ) AS rows_tot
     , CASE WHEN SUM ( q.executions_delta) != 0 THEN ROUND ( SUM ( q.rows_processed_delta ) / SUM ( q.executions_delta) ) END AS rows_pe
     , ( SELECT t.sql_text FROM dba_hist_sqltext t WHERE t.sql_id = q.sql_id AND ROWNUM <= 1 ) AS sql_text
  FROM dba_hist_snapshot s
  JOIN dba_hist_sqlstat q ON q.snap_id = s.snap_id AND q.dbid = s.dbid AND q.instance_number = s.instance_number
 WHERE q.sql_id IN ( '7kyudc3zuan0z' )
 and s.begin_interval_time >= to_date('21-AUG-24 20:29:00','DD-MON-YY HH24:MI:SS')
 and s.end_interval_time <= to_date('21-AUG-24 23:01:00','DD-MON-YY HH24:MI:SS')
 GROUP BY q.sql_id , q.plan_hash_value
 ORDER BY MAX ( s.end_interval_time ) DESC;

--same as above but specifically for morning test times
 SELECT q.sql_id
     , q.plan_hash_value , MIN(s.begin_interval_time) , MAX( s.end_interval_time)
     , CASE WHEN SUM ( q.executions_delta) != 0 THEN ROUND ( SUM ( q.elapsed_time_delta) / SUM ( q.executions_delta) / 1000 , 2 ) END AS millisecs_per_exec
     , SUM ( q.executions_delta ) AS execs
     , SUM ( q.rows_processed_delta ) AS rows_tot
     , CASE WHEN SUM ( q.executions_delta) != 0 THEN ROUND ( SUM ( q.rows_processed_delta ) / SUM ( q.executions_delta) ) END AS rows_pe
     , ( SELECT t.sql_text FROM dba_hist_sqltext t WHERE t.sql_id = q.sql_id AND ROWNUM <= 1 ) AS sql_text
  FROM dba_hist_snapshot s
  JOIN dba_hist_sqlstat q ON q.snap_id = s.snap_id AND q.dbid = s.dbid AND q.instance_number = s.instance_number
 WHERE q.sql_id IN ( '7czd2113a2d11' )
 and s.begin_interval_time >= to_date('23-AUG-24 06:29:00','DD-MON-YY HH24:MI:SS')
 and s.end_interval_time <= to_date('23-AUG-24 08:59:00','DD-MON-YY HH24:MI:SS')
 GROUP BY q.sql_id , q.plan_hash_value
 ORDER BY MAX ( s.end_interval_time ) DESC;

 --summary for all morning tests per day for the last 7 days including this morning's test
 SELECT 
  q.sql_id, 
  q.plan_hash_value, 
  MIN(s.begin_interval_time) AS start_time, 
  MAX(s.end_interval_time) AS end_time, 
  CASE WHEN SUM(q.executions_delta) != 0 THEN ROUND(
    SUM(q.elapsed_time_delta) / SUM(q.executions_delta) / 1000, 
    2
  ) END AS millisecs_per_exec, 
  SUM(q.executions_delta) AS execs, 
  SUM(q.rows_processed_delta) AS rows_tot, 
  CASE WHEN SUM(q.executions_delta) != 0 THEN ROUND(
    SUM(q.rows_processed_delta) / SUM(q.executions_delta)
  ) END AS rows_pe, 
  (
    SELECT 
      t.sql_text 
    FROM 
      dba_hist_sqltext t 
    WHERE 
      t.sql_id = q.sql_id 
      AND ROWNUM <= 1
  ) AS sql_text 
FROM 
  dba_hist_snapshot s 
  JOIN dba_hist_sqlstat q ON q.snap_id = s.snap_id 
  AND q.dbid = s.dbid 
  AND q.instance_number = s.instance_number 
WHERE 
  s.begin_interval_time BETWEEN TRUNC(sysdate) - 7 AND sysdate
  AND TO_CHAR(s.begin_interval_time, 'HH24:MI') BETWEEN '06:29' AND '09:01'
  AND TO_CHAR(s.end_interval_time, 'HH24:MI') BETWEEN '06:29' AND '09:01'
  AND q.sql_id IN ('7czd2113a2d11') 
GROUP BY 
  q.sql_id, 
  q.plan_hash_value, 
  TRUNC(s.begin_interval_time)
ORDER BY 
  MAX(s.end_interval_time) DESC;


---same as above but with bind variables for the  last 7 days morning tests
select sql_id,MIN(begin_interval_time) begin_interval_time , MAX( end_interval_time) end_interval_time,PLAN_HASH_VALUE
, SUM ( executions_delta ) AS execs
, SUM ( rows_processed_delta ) AS rows_tot
, CASE WHEN SUM ( executions_delta) != 0 THEN ROUND ( SUM ( rows_processed_delta ) / SUM ( executions_delta) ) END AS rows_pe
,b1,b2,b3,b4,b5,b6
from (
SELECT q.sql_id , q.snap_id
      ,s.begin_interval_time
     , s.end_interval_time
     , q.plan_hash_value
     , ROUND ( q.elapsed_time_delta / 1000000 ) AS secs
     , CASE WHEN q.executions_delta != 0 THEN ROUND ( q.elapsed_time_delta / q.executions_delta / 1000000 , 2 ) END AS secs_per_exec
     , CASE WHEN q.executions_delta != 0 THEN ROUND ( q.elapsed_time_delta / q.executions_delta / 1000 , 2 ) END AS millisecs_per_exec
     , ROUND ( q.executions_delta / 15 / 60 ) AS tps
     , CASE WHEN q.executions_delta != 0 THEN ROUND ( q.rows_processed_delta / q.executions_delta ) END AS rp_per_exec
     , CASE WHEN q.executions_delta != 0 THEN ROUND ( q.fetches_delta / q.executions_delta ) END AS fetches_pe
     , q.executions_delta
     , q.fetches_delta
     , q.rows_processed_delta
     , dbms_sqltune.extract_bind( q.bind_data , 1 ).value_string as b1
     , dbms_sqltune.extract_bind( q.bind_data , 2 ).value_string as b2
     , dbms_sqltune.extract_bind( q.bind_data , 3 ).value_string as b3
     , dbms_sqltune.extract_bind( q.bind_data , 4 ).value_string as b4
     , dbms_sqltune.extract_bind( q.bind_data , 5 ).value_string as b5
     , dbms_sqltune.extract_bind( q.bind_data , 6 ).value_string as b6
  FROM dba_hist_snapshot s
  JOIN dba_hist_sqlstat q ON q.snap_id = s.snap_id AND q.dbid = s.dbid AND q.instance_number = s.instance_number
 WHERE q.sql_id IN ( 'as94yzhy33vac' )
 ORDER BY s.end_interval_time DESC)
 where begin_interval_time BETWEEN TRUNC(sysdate) - 7 AND sysdate
  AND TO_CHAR(begin_interval_time, 'HH24:MI') BETWEEN '06:29' AND '09:01'
  AND TO_CHAR(end_interval_time, 'HH24:MI') BETWEEN '06:29' AND '09:01'
 GROUP BY sql_id , 
 plan_hash_value
 ,b1,b2,b3,b4,b5,b6
 order by begin_interval_time desc
;

  --sumaries showing and comparing # of executions during morning test windows for the past 7 days
   SELECT 
  MIN(s.begin_interval_time) AS start_time, 
  MAX(s.end_interval_time) AS end_time, 
  SUM(q.executions_delta) AS execs
FROM 
  dba_hist_snapshot s 
  JOIN dba_hist_sqlstat q ON q.snap_id = s.snap_id 
  AND q.dbid = s.dbid 
  AND q.instance_number = s.instance_number 
WHERE 
  s.begin_interval_time BETWEEN TRUNC(sysdate) - 7 AND sysdate
  AND TO_CHAR(s.begin_interval_time, 'HH24:MI') BETWEEN '06:29' AND '09:01'
  AND TO_CHAR(s.end_interval_time, 'HH24:MI') BETWEEN '06:29' AND '09:01'
  --AND q.sql_id IN ('7czd2113a2d11') 
GROUP BY 
  TRUNC(s.begin_interval_time)
ORDER BY 
  MAX(s.end_interval_time) DESC;

--same result but this time using the repo test results in Transcom
select  DATABASE_NAME,to_char(MIN(begin_time),'DD/MM/YY HH24:MI') AS start_time, 
  to_char(MAX(end_time),'DD/MM/YY HH24:MI') AS end_time, 
  SUM(total_execs) AS execs,
  SUM(DB_TIME_SECS)
  from TEST_RESULT_DB_STATS
where database_name='CDD011N'
and begin_time BETWEEN TRUNC(sysdate) - 4 AND sysdate
AND TO_CHAR(begin_time, 'HH24:MI') BETWEEN '06:59' AND '10:01'
  AND TO_CHAR(end_time, 'HH24:MI') BETWEEN '06:59' AND '10:01'
group by TRUNC(begin_time), DATABASE_NAME
ORDER BY 1 DESC;


 


SELECT * FROM TABLE ( DBMS_XPLAN.display_awr ( sql_id => '56sy7091t82n0' , plan_hash_value => 372179311 ) ) ;
SELECT * FROM TABLE ( DBMS_XPLAN.display_awr ( sql_id => '56sy7091t82n0' , plan_hash_value => 372179311 , format => '+peeked_binds +outline' ) ) ;
SELECT * FROM TABLE ( DBMS_XPLAN.display_cursor ( sql_id => 'csxujjjktk6wd' , cursor_child_no => NULL , format => '+peeked_binds +outline' ) ) ;
SELECT * FROM TABLE ( DBMS_XPLAN.display ) ;
SELECT * FROM TABLE ( DBMS_XPLAN.display ( format => '+outline' ) ) ;
EXPLAIN PLAN INTO sys.plan_table$ FOR ;
SELECT * FROM TABLE ( DBMS_XPLAN.display ( table_name => 'sys.plan_table$' , format => '+outline' ) ) ;
SELECT * FROM dba_hist_sql_plan WHERE sql_id = '3xgra3tn5z52q' ;
SELECT owner , table_name , num_rows/1000/1000 , statType_locked , stale_stats , last_analyzed , blocks*8/1024 as mb FROM dba_tab_statistics
WHERE table_name IN ( 'EXTN_PAYMENT_PLANS_JOURNAL' , 'EXTN_PAYMENT_PLAN_COHORT' ) ORDER BY 1,2 ;
EXEC dbms_workload_repository.add_colored_sql ( sql_id => 'a5t9rx4xpqd9j' ) ;
SELECT sql_id , executions , round(elapsed_time/1000/1000,2) as secs_tot , rows_processed , first_load_time , last_active_time , parsing_user_id , parsing_schema_name , module , action FROM v$sql WHERE sql_id = '3r4d2mwgacc8w' ORDER BY 1 ;
SELECT TO_CHAR ( h.end_interval_time , 'YYYY-MM-DD' ) AS rdate
     , s.sql_id
     , MIN ( s.action )
     , ROUND ( SUM ( s.elapsed_time_delta ) / 1000000 / 60 ) AS mins
     , SUM ( s.executions_delta )
     , SUM ( s.rows_processed_delta )
  FROM dba_hist_snapshot h
  JOIN dba_hist_sqlstat s ON s.dbid = h.dbid AND s.snap_id = h.snap_id
 WHERE s.module = 'CARD_AND_BANK'
   --AND h.end_interval_time > SYSTIMESTAMP - 8
   --and sql_id not in ( 'gtzyhqhj2jx5c' , 'avc2c4nrn3z5h' )
 GROUP BY TO_CHAR ( h.end_interval_time , 'YYYY-MM-DD' )
     , s.sql_id
 ORDER BY 4 DESC , TO_CHAR ( h.end_interval_time , 'YYYY-MM-DD' ) DESC
     , s.sql_id
;
-- look back to previous tests for top 15
select top_sql_number , round(tps) as tps , round(elapsed_time_per_exec_seconds*1000 ) as mspe , end_time , test_description
from hp_diag.test_result_sql
where sql_id = '5ztg482w645wd'
and test_description like '%R1%'
order by end_time desc
;
select session_id , sample_time , sql_id , sql_exec_start , sql_exec_start - sample_time as tdiff , module
from /* dba_hist_Active_sess_history */ v$active_session_history
where sample_time between to_timestamp ( '24-Oct-2017 15:15:00' , 'DD-Mon-YYYY HH24:MI:SS' ) and to_timestamp ( '24-Oct-2017 15:15:55' , 'DD-Mon-YYYY HH24:MI:SS' )
and machine = 'uncsg09b.bskyb.com'
order by 1 , 2
;
EXEC DBMS_WORKLOAD_REPOSITORY.add_colored_sql ( sql_id => '260s8wf7c2zy7' ) ;  -- coloured
-- QUERY 0) Real time activity
SELECT s.username , s.osuser , s.machine , s.program , s.module , s.status , s.last_call_et , s.logon_time
     , s.sql_id , s.prev_sql_id , s.sid , s.serial# , s.sql_trace , s.sql_exec_start , s.prev_exec_start
     , CASE s.sql_trace WHEN 'ENABLED' THEN p.tracefile END AS trace_file
  FROM v$session s
  LEFT OUTER JOIN v$process p
    ON s.paddr = p.addr
 WHERE s.username = 'SAL_USER'
 ORDER BY s.status , s.last_call_et ;
;
SELECT s.sid , s.serial# , s.osuser , s.status , s.last_call_et , s.logon_time , s.event , s.wait_class
     , s.sql_id , SUBSTR ( t.sql_text , 1, 50 ) AS stext
  FROM v$session s JOIN v$sql t ON s.sql_id = t.sql_id
 WHERE s.username != 'SYSTEM'
   AND ( s.status = 'ACTIVE' OR s.last_call_et < 2 )
 ORDER BY s.status , s.last_call_et ;
;
-- QUERY ONE) Indexes
ALTER SESSION SET nls_date_format = 'Dy DD-Mon-YYYY HH24:MI:SS' ;
SELECT t.owner
     , t.table_name
     , ROUND ( t.blocks * 8 / 1024 ) AS mb  -- assuming each block is default 8kb
     , t.num_rows
     , i.index_name
     , LISTAGG ( ic.column_name || ' , ' ) WITHIN GROUP ( ORDER BY ic.column_position ) AS ind_cols
     , i.index_type
     , i.uniqueness
     , t.partitioned AS table_partitioned
     , i.partitioned AS index_partitioned
     , o.created
  FROM dba_tables t
  LEFT OUTER JOIN dba_indexes i
    ON i.table_owner = t.owner
   AND i.table_name = t.table_name
  LEFT OUTER JOIN dba_ind_columns ic
    ON ic.index_owner = i.owner
   AND ic.index_name = i.index_name
  JOIN dba_objects o
    ON o.object_name = i.index_name
   AND o.owner = i.owner
 --WHERE t.table_name IN ( 'JOB_TRANSACTION_LOG' , 'JOB' , 'JOB_TYPE' , 'ENGINEER_JOB_STATUS' , 'JOB_OPERATION' )
 WHERE t.owner = 'SKYBUSINESS'
 GROUP BY t.owner
     , t.table_name
     , t.blocks
     , t.num_rows
     , i.index_name
     , i.index_type
     , i.uniqueness
     , t.partitioned
     , i.partitioned
     , o.created
 ORDER BY t.blocks DESC , t.owner , t.table_name , i.index_name
;
-- QUERY TWO) Foreign Keys not indexed: https://wiki.gutzmann.com/confluence/display/HowTo/Indexing+All+Foreign+Keys
SELECT 'CREATE INDEX ' || LOWER ( owner ) || '.' || LOWER ( constraint_name )
        || ' ON ' || LOWER ( owner ) || '.' || LOWER ( table_name )
        || ' ( ' || columns || ' ) TABLESPACE tcc_index_auto_01 ;'
from (
    select owner
         , table_name
         , constraint_name
         , LOWER ( cname1 || nvl2(cname2,','||cname2,null) ||
             nvl2(cname3,','||cname3,null) || nvl2(cname4,','||cname4,null) ||
             nvl2(cname5,','||cname5,null) || nvl2(cname6,','||cname6,null) ||
             nvl2(cname7,','||cname7,null) || nvl2(cname8,','||cname8,null)
                    ) AS columns
    from ( select b.table_name,
               b.constraint_name,
               max(decode( position, 1, column_name, null )) cname1,
               max(decode( position, 2, column_name, null )) cname2,
               max(decode( position, 3, column_name, null )) cname3,
               max(decode( position, 4, column_name, null )) cname4,
               max(decode( position, 5, column_name, null )) cname5,
               max(decode( position, 6, column_name, null )) cname6,
               max(decode( position, 7, column_name, null )) cname7,
               max(decode( position, 8, column_name, null )) cname8,
               count(*) col_cnt
                , b.owner
          from (select substr(table_name,1,30) table_name,
                       substr(constraint_name,1,30) constraint_name,
                       substr(column_name,1,30) column_name,
                       position
                       , owner
                  from dba_cons_columns ) a,
               dba_constraints b
         where a.constraint_name = b.constraint_name
           and b.constraint_type = 'R'
             and a.owner = b.owner
             and a.owner NOT IN ( 'SYS' , 'SYSTEM' , 'DBSNMP' , 'DATAPROV' , 'GSMADMIN_INTERNAL' , 'APEX_040100' )
         group by b.table_name, b.constraint_name , b.owner
      ) cons
    where col_cnt > ALL
        ( select count(*)
            from dba_ind_columns i
           where i.table_name = cons.table_name
               and i.table_owner = cons.owner
             and i.column_name in ( cname1, cname2, cname3, cname4,
                                    cname5, cname6, cname7, cname8 )
             and i.column_position <= cons.col_cnt
           group by i.index_name
        )
) order by 1
;
-- stats compare optimizer statistics
select * from dba_tab_stats_history where table_name = 'BSBCUSTOMERREWARD' ;
SELECT * FROM TABLE ( dbms_stats.diff_table_stats_in_history (
  ownname => 'CCSOWNER'
, tabname => 'BSBCUSTOMERREWARD'
, time1 => SYSTIMESTAMP
, time2 => TO_TIMESTAMP ( '19-Feb-2018' , 'DD-Mon-YYYY' )
, pctthreshold => 0
) )
;
-- archived redo logs daily
SELECT TRUNC ( completion_time ) , ROUND ( SUM ( blocks * block_size ) / 1024 / 1024 / 1024 , 2 ) AS gb
  FROM v$archived_log
 WHERE dest_id = 1
 GROUP BY TRUNC ( completion_time )
 ORDER BY 1 DESC
;
-- archived redo logs hourly
SELECT TO_CHAR ( completion_time , 'YYYY-MM-DD HH24' ) AS ctime , COUNT(*) , ROUND ( SUM ( blocks * block_size ) / 1024 / 1024 / 1024 , 2 ) AS gb
  FROM v$archived_log
 WHERE dest_id = 1
 GROUP BY TO_CHAR ( completion_time , 'YYYY-MM-DD HH24' )
 ORDER BY 1 DESC
;
-- for confluence
SELECT TO_CHAR ( completion_time , 'YYYY-MM-DD HH24' ) AS a
     , TO_CHAR ( completion_time , 'HH24' ) || ':nn' AS b
     , ROUND ( SUM ( blocks * block_size ) / 1024 / 1024 / 1024 , 2 ) AS gb
     , count(*)
  FROM v$archived_log
 WHERE completion_time BETWEEN 'Sat 03-Sep-2016 12:00:00' AND 'Sat 03-Sep-2016 21:00:00'
 GROUP BY TO_CHAR ( completion_time , 'YYYY-MM-DD HH24' ) , TO_CHAR ( completion_time , 'HH24' ) || ':nn'
 ORDER BY 1 
;
-- Tkprof by ash
select sql_plan_hash_value , sample_time , sql_plan_line_id , sql_plan_operation , sql_plan_options , session_state , NVL ( event , session_state ) , pga_allocated , delta_read_io_bytes , delta_read_io_requests
from v$active_session_history
where sample_time > sysdate - 1
and session_id = 883
and session_serial# = 26006
order by sample_time desc ;
-- summary count
select NVL ( event , session_state ) , COUNT(*) , min(sample_time) , max(sample_time)
from v$active_session_history  -- dba_hist_active_sess_history
where sample_time BETWEEN TO_DATE ( 'Tue 26-Jul-2016 20:00:00' , 'Dy DD-Mon-YYYY HH24:MI:SS' ) AND TO_DATE ( 'Tue 26-Jul-2016 22:44:15' , 'Dy DD-Mon-YYYY HH24:MI:SS' )
GROUP BY NVL ( event , session_state )
order by COUNT(*) DESC ;
-- detailed count
select COUNT(*) , sql_plan_hash_value , sql_plan_line_id , sql_plan_operation , sql_plan_options , session_state , NVL ( event , session_state ) , MAX(pga_allocated)
from v$active_session_history
where sample_time > sysdate - 1
and session_id = 883
and session_serial# = 26006
group by sql_plan_hash_value , sql_plan_line_id , sql_plan_operation , sql_plan_options , session_state , NVL ( event , session_state )
order by 1 desc ;
select sql_plan_hash_value , sample_time , sql_plan_line_id , sql_plan_operation , sql_plan_options , session_state , NVL ( event , session_state ) , pga_allocated , delta_read_io_bytes , delta_read_io_requests
from v$active_session_history
where sample_time > sysdate - 0.5
and sql_id = '	bjsuspx67wn8v'
order by sample_time desc ;
select sql_plan_hash_value , sample_time , sql_plan_line_id , sql_plan_operation , sql_plan_options , session_state , event , pga_allocated , delta_read_io_bytes , delta_read_io_requests
from dba_hist_active_sess_history
where sample_time > sysdate - 0.5
and sql_id = '0nzcjnzbzr2yv'
order by sample_time desc ;
select * from dba_hist_sql_plan where sql_id = '0nzcjnzbzr2yv' and plan_hash_value = '3246643882' and id in ( 22,19,21,20,18) order by id ;
-- stats diff
SELECT * FROM dba_tab_stats_history WHERE table_name = 'BSBCUSORD' ;
SELECT * FROM TABLE ( DBMS_STATS.DIFF_TABLE_STATS_IN_HISTORY (
     ownname => 'OH'
   , tabname => 'BSBCUSORD'
   , time1 => TO_TIMESTAMP ( '12-MAY-16 15.51.38.737466000 +01:00' )
   , time2 => NULL  -- NULL for comparison against dictionary stats.
   , pctthreshold => 10  -- only report if more than this % difference. defaults to 10%
) ) ;
-- datafix in chordo by snapshots
SELECT a.sql_id
     , ROUND ( SUM ( a.elapsed_time_delta ) / 1000 / 1000 ) AS secs
     , SUM ( a.executions_delta ) AS execs
     , MIN ( LOWER ( REPLACE ( REPLACE ( DBMS_LOB.SUBSTR ( sql_text , 40 , 1 ) , CHR(10) , ' ' ) , CHR(13), ' ' ) ) )  AS sql_text
  FROM dba_hist_sqlstat a
  JOIN dba_hist_sqltext t ON a.sql_id = t.sql_id AND a.dbid = t.dbid
  JOIN dba_hist_snapshot s ON s.snap_id = a.snap_id AND s.dbid = a.dbid
  JOIN dba_users u ON u.user_id = a.parsing_user_id
 WHERE u.username = 'DATAFIX'
   AND s.end_interval_time BETWEEN 'Wed 07-Sep-2016 19:45:00' AND 'Wed 07-Sep-2016 22:15:00'
 GROUP BY a.sql_id
HAVING SUM ( a.executions_delta ) > 0
 ORDER BY 2 DESC
;
-- datafix in chordo by ash
select min(sample_time) from v$active_session_history ;
create table afr20.af_ash as select * from v$active_session_history where user_id = ( select d.user_id from dba_users d where d.username = 'DATAFIX' ) ;
SELECT a.sql_id , COUNT(*)
     , MIN ( LOWER ( REPLACE ( REPLACE ( DBMS_LOB.SUBSTR ( sql_text , 40 , 1 ) , CHR(10) , ' ' ) , CHR(13), ' ' ) ) )  AS sql_text
  FROM afr20.af_ash a
  JOIN dba_hist_sqltext t ON a.sql_id = t.sql_id
 WHERE a.sample_time BETWEEN 'Wed 07-Sep-2016 19:45:00' AND 'Wed 07-Sep-2016 22:15:00'
   AND t.dbid = ( SELECT v.dbid FROM v$database v )
   AND a.sql_exec_id IS NOT NULL
 GROUP BY a.sql_id
 ORDER BY 2 DESC
;
-- QUERY THREE) AWR slow sql - http://www.nocoug.org/download/2008-08/a-tour-of-the-awr-tables.nocoug-Aug-21-2008.abercrombie.html#script-find-expensive BETTER VERSION ordered by secs_per_exec.
SELECT *
  FROM ( -- sub to sort before rownum
        SELECT sql_id
             , ROUND(SUM(elapsed_time_delta)/1000000) AS seconds_total
             , SUM(executions_delta) AS execs_total
             , SUM(buffer_gets_delta) AS gets_total
             , ROUND ( SUM ( elapsed_time_delta ) / GREATEST ( SUM ( executions_delta ) , 1 ) / 1000000 , 2 ) AS secs_per_exec
             , MIN ( parsing_schema_name )
             , MIN ( REPLACE ( REPLACE ( DBMS_LOB.SUBSTR ( sql_text , 100 , 1 ) , CHR(10) , ' ' ) , CHR(13), ' ' )  ) AS sql_text
          FROM dba_hist_snapshot NATURAL JOIN dba_hist_sqlstat NATURAL JOIN dba_hist_sqltext
         WHERE snap_id BETWEEN 118783 AND 118786
           AND parsing_schema_name != 'SYS'
         GROUP BY sql_id
         ORDER BY SUM(elapsed_time_delta) DESC
       )
 WHERE ROWNUM <= 10
;
-- afafaf-tps
SELECT *
  FROM ( -- sub to sort before rownum
        SELECT sql_id
             , ROUND(SUM(elapsed_time_delta)/1000000) AS seconds_total
             , SUM(executions_delta) AS execs_total
             , SUM(buffer_gets_delta) AS gets_total
             , round(executions/ ( ( end_interval_time - begin_interval_time ) * 24 - 0.25 ) / 60 / 60 ) as tps
             , ROUND ( SUM ( elapsed_time_delta ) / GREATEST ( SUM ( executions_delta ) , 1 ) / 1000000 , 2 ) AS secs_per_exec
             , MIN ( parsing_schema_name )
             , MIN ( REPLACE ( REPLACE ( DBMS_LOB.SUBSTR ( sql_text , 100 , 1 ) , CHR(10) , ' ' ) , CHR(13), ' ' )  ) AS sql_text
          FROM dba_hist_snapshot NATURAL JOIN dba_hist_sqlstat NATURAL JOIN dba_hist_sqltext
         WHERE end_interval_time BETWEEN TO_DATE ( '29-JUL-2016 16:47' , 'DD-MON-YYYY HH24:MI' ) AND TO_DATE ( '30-JUL-2016 02:48' , 'DD-MON-YYYY HH24:MI' )  -- current (slow)
         --WHERE end_interval_time BETWEEN TO_DATE ( '27-JUL-2016 15:37' , 'DD-MON-YYYY HH24:MI' ) AND TO_DATE ( '28-JUL-2016 01:38' , 'DD-MON-YYYY HH24:MI' )  -- baseline (fast)
           AND parsing_schema_name != 'SYS'
           AND module = 'cbs-sales-dialogue'
         GROUP BY sql_id
         ORDER BY SUM(elapsed_time_delta) DESC
       )
 WHERE ROWNUM <= 10
;
# easy connect
# username/password@chorddbptt/chordo
# username/password@chorddbn02:1525/ccs021n
-- useful sql: https://confluence.bskyb.com/display/nonfuntst/Database+Check+Customer+SQL+Scripts and https://confluence.bskyb.com/display/CBSTA/Useful+SQL
-- find customer database:
select server_id - 2 , a.* from arbor.external_id_acct_map a where external_id = '623024976754' ;
-- TraceAF 231=/omsrvdbn02/ora/diag/rdbms/oms021n_onx/OMS021N/trace/OMS021N_ora_3630.trc
select s.sid , s.serial# , s.username , s.status , s.last_call_et , s.osuser , s.machine , s.sql_id , s.sql_trace , s.sql_trace_waits , s.sql_trace_binds , p.tracefile
from v$session s
left outer join v$process p on s.paddr = p.addr
where s.sid = 231 ;
-- 12 hour clock time for ASH OEM  00:05 = 12:05am
WITH q AS ( SELECT TO_DATE ( '19-Oct-2017 00:05:00' , 'DD-Mon-YYYY HH24:MI:SS' ) AS d1 FROM DUAL ) SELECT d1 , TO_CHAR ( d1 , 'Dy DD-Mon-YYYY HH:MI:SS am' ) FROM q ;
-- During the 15th and 16th centuries, the 12-hour analog dial and time system gradually became established as standard throughout Northern Europe for general public use.
EXEC DBMS_MONITOR.session_trace_enable ( session_id => 231 , serial_num => 10255 , waits => FALSE , binds => FALSE ) ;
-- AppDynamics https://skydevtemp.saas.appdynamics.com/controller/#/location=APP_BT_SLOW_TRANSACTIONS&timeRange=Custom_Time_Range.BETWEEN_TIMES.1530305400000.1530303600000.30&application=19606&businessTransaction=421786

-- is my index being used, and if so by what:
select * from dba_hist_sql_plan where object_name in ( 'IDX_STG_CANDIDATES_IDENTIFIER' , '-NUQ_CSC_REF_STG_CANDIDATES' )
order by timestamp desc
;
-- optimizer environment
SELECT s.sid , oe.value , s.username , s.logon_time
  FROM v$session s
  LEFT OUTER JOIN v$ses_optimizer_env oe ON oe.sid = s.sid
 WHERE ( oe.name = 'optimizer_features_enable' OR oe.name IS NULL )
   AND s.username = 'TCC_USER'
 ORDER BY oe.value , s.logon_time DESC , s.username , s.sid
;
-- QUERY 6, plans
select top_sql_number , round ( elapsed_time_seconds ) as secs
, executions
, round ( tps , 2 ) as tps
, round ( 1000*elapsed_time_per_exec_seconds ) as ms_pe
, round ( buffer_gets_per_exec )
, begin_time , end_time
, test_description
, plan_hash_values
from hp_diag.test_result_sql where sql_id = '63ns6vty7fvuj'
order by end_time desc
;
-- indexes /*+ index ( p ( person.LoyaltyProgramStateChangeDate ) ) */
SELECT LOWER ( table_name ) , LOWER ( index_name ) , column_position , LOWER ( column_name )
  FROM dba_ind_columns
 WHERE table_owner = 'REFDATAMGR'
   AND table_name IN ( 'BSBOFFER' , 'BSBCOMMERCIALOFFER' , 'BSBOFFERDISCOUNT' , 'BSBOFFERPERCENTAGEDISCOUNT' , 'BSBOFFERVALUEDISCOUNT' )
 ORDER BY table_name , index_name , column_position ;
;
-- omg mq. Log in log unora0l0:/ora/mgw_11.2.0.4a/oramgw-unora0l0-20171002172642-7752.log.
select * from mgw_gateway ;
select * from mgw_subscribers order by 1 ;  -- subscriber_id used below
select * from mgw_foreign_queues order by 1 ;
select * from mgw_jobs order by 1 ;
select * from mgw_links order by 1 ;
select * from mgw_mqseries_links order by 1 ;
select * from mgw_schedules order by 1 ;
exec dbms_mgwadm.reset_job ( job_name => 'BSSADI_PROP' ) ;  -- subscriber_id from mgw_subscribers
exec dbms_mgwadm.reset_job ( job_name => 'EST_PROP' ) ;
exec dbms_mgwadm.shutdown ;
exec dbms_mgwadm.startup ;
exec dbms_mgwadm.cleanup_gateway(1);
-- [Ref MoS 335523.1]
-- drill into ash
select sql_exec_start - sample_time as diff , session_id , sql_id , module
sample_time , sql_exec_start , sql_exec_id , session_state , in_sql_execution , sql_plan_line_id , round(pga_allocated/1024/1024) as pga_mb , round(temp_space_allocated/1024/1024) as temp_mb
  from v$active_session_history
 where sample_time between to_timestamp ( '10-Apr-2017 16:22:23' , 'DD-Mon-YYYY HH24:MI:SS' ) and to_timestamp ( '10-Apr-2017 16:22:30' , 'DD-Mon-YYYY HH24:MI:SS' )
order by 1 nulls last , session_id , sample_time
;
-- pools in use by tomcat
select machine , count(*) , sum ( CASE status WHEN 'ACTIVE' THEN 1 ELSE 0 END ) AS num_active , ROUND ( AVG ( last_call_et ) )
from v$session
where osuser = 'tomcat'
group by machine
order by count(*) desc
;
-- batch
select * from batchprocess.bsb_batch_programs where program_name = 'ccloy030' ;
select * from batchprocess.bsb_batch_runs where batch_program_id in ( 984 , 985 ) order by end_date desc ;

select /* andrew Fraser */ sql_id , sum(executions) , max(last_active_time) , min(sql_text) from v$sql where upper(sql_text) like '%SELECT%OM_OWNER.CUSTOMERORDERS%'
 group by sql_id
 order by 3 desc
;
select * from tcd.inventoryservicestatusmap ;
-- kenan customer mapping
select s.ds_database , m.* from arbor.external_id_acct_map m left outer join arbor.server_definition s ON s.server_id = m.server_id where m.external_id_type = 1 and m.external_id = '621000102338'
;
SELECT /*json*/ TRIM ( s.ds_database ) AS cusdb
     , bpr.partyId
     , ba.accountNumber
  FROM ccsowner.bsbBillingAccount ba
  JOIN ccsowner.bsbCustomerRole bcr ON bcr.portfolioId = ba.portfolioId
  JOIN ccsowner.bsbPartyRole bpr ON bpr.id = bcr.partyRoleId
  JOIN arbor.external_id_acct_map@adm m ON m.external_id = ba.accountNumber
  JOIN arbor.server_definition@adm s ON s.server_id = m.server_id
 WHERE m.external_id_type = 1
   AND ba.created > SYSDATE - 1
 ORDER BY 1 , 2 , 3
;
-- compare.xlsx
select sql_id , executions
     -- , round(executions / 8/60/60) as tps1
     , round(executions/ ( ( end_time - begin_time ) * 24 - 0.25 ) / 60 / 60 ) as tps
     , round(1000*elapsed_time_per_exec_seconds ) as millisecs_pe , test_description , top_sql_number , begin_time , ROUND ( ( end_time - begin_time ) * 24 - 0.25 , 2 ) as hours
from hp_diag.test_result_sql
where sql_id in ( '5dy0wx4q5zqqk' , 'gynd24rm5yz83' )
and test_description is not null
and database_name = 'CHORDO'
order by end_time desc
;
-- get average times and execs for a sql statement over 8 hours in production.
SELECT sql_id
     , SUM ( executions_delta ) AS executions
     , ROUND ( SUM ( executions_delta ) / 8 / 60 / 60 ) as tps
     , ROUND ( SUM ( elapsed_time_delta ) / SUM ( executions_delta ) / 1000 ) AS millis_pe
     , ROUND ( SUM ( rows_processed_delta ) / SUM ( executions_delta ) ) as rp_pe
     , 'Production ' || MAX ( begin_interval_time ) AS descrip
     , '.' as top_sql
     , MAX ( end_interval_time ) AS end_time
     , 8 AS hours        
             , CASE WHEN MIN ( plan_hash_value ) = MAX ( plan_hash_value)
                    THEN TO_CHAR ( MIN ( plan_hash_value ) )
                    ELSE TO_CHAR ( MIN ( plan_hash_value ) ) || ' ' || TO_CHAR ( MAX ( plan_hash_value ) ) END as plan_hash_values
             --, SUBSTR ( LISTAGG ( plan_hash_value , ' ' ) WITHIN GROUP ( ORDER BY 1 ) , 1 , 100 ) OVER ( PARTITION BY sql_id , dbid ) AS plan_hash_values
 FROM dba_hist_snapshot NATURAL JOIN dba_hist_sqlstat
WHERE end_interval_time between TO_DATE ( '01-MAY-2016 10:00' , 'DD-MON-YYYY HH24:MI' )
                 AND ( 8/24 + ( TO_DATE ( '01-MAY-2016 10:00' , 'DD-MON-YYYY HH24:MI' ) ) )
  and sql_id = '5dy0wx4q5zqqk'
GROUP BY sql_id
;
-- what is the top sql
SELECT sql_id
     , ROUND ( SUM ( elapsed_time_delta ) / 1000 / 1000 ) as secs
     , SUM ( executions_delta ) AS executions
     , ROUND ( SUM ( executions_delta ) / 8 / 60 / 60 ) as tps
     , ROUND ( SUM ( elapsed_time_delta ) / SUM ( executions_delta ) / 1000 ) AS millis_pe
     , ROUND ( SUM ( rows_processed_delta ) / SUM ( executions_delta ) ) as rp_pe
     --, 'Production ' || MAX ( begin_interval_time ) AS descrip
     , '.' as top_sql
     , MAX ( end_interval_time ) AS end_time
     , 8 AS hours        
             , CASE WHEN MIN ( plan_hash_value ) = MAX ( plan_hash_value)
                    THEN TO_CHAR ( MIN ( plan_hash_value ) )
                    ELSE TO_CHAR ( MIN ( plan_hash_value ) ) || ' ' || TO_CHAR ( MAX ( plan_hash_value ) ) END as plan_hash_values
             --, SUBSTR ( LISTAGG ( plan_hash_value , ' ' ) WITHIN GROUP ( ORDER BY 1 ) , 1 , 100 ) OVER ( PARTITION BY sql_id , dbid ) AS plan_hash_values
 FROM dba_hist_snapshot NATURAL JOIN dba_hist_sqlstat
WHERE snap_id between 2054 and 2086
  --and sql_id = '5dy0wx4q5zqqk'
GROUP BY sql_id
ORDER BY 2 DESC
;
-- Best good top sql, was used for OMS order management testing and e01.
select /*sub.begin_snap
     , sub.end_snap
     , sub.begin_time
     , sub.end_time
     ,*/ ROWNUM AS top_sql_number
     , sub.elapsed_time_seconds
     , sub.executions
     , ROUND ( 1000 * sub.elapsed_time_seconds / sub.executions ) as millis_per_exec
     , ROUND ( sub.executions/ 60 / 60 ) as tps
     , ROUND ( sub.rows_processed_delta / sub.executions ) as rp_per_exec
     --, sub.buffer_gets
     --, sub.cpu_time_seconds
     , sub.sql_id
     , sub.plan_hash_values , sub.module --, sub.action , sub.module2 , sub.action2
     , sub.parsing_schema_name
     , ( SELECT SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( t.sql_text , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) , '  ' , ' ' ) , '  ' , ' ' )
                , 1 , 100 )
           FROM dba_hist_sqltext t
          WHERE t.sql_id = sub.sql_id
            AND ROWNUM = 1
         ) AS sql_text
     FROM (
        SELECT sql_id , dbid
             --, SUBSTR ( LISTAGG ( plan_hash_value , ' ' ) WITHIN GROUP ( ORDER BY 1 ) , 1 , 100 ) OVER ( PARTITION BY sql_id , dbid ) AS plan_hash_values
             , CASE WHEN MIN ( plan_hash_value ) = MAX ( plan_hash_value)
                    THEN TO_CHAR ( MIN ( plan_hash_value ) )
                    ELSE TO_CHAR ( MIN ( plan_hash_value ) ) || ' ' || TO_CHAR ( MAX ( plan_hash_value ) ) END as plan_hash_values
             , round ( SUM ( elapsed_time_delta ) / 1000000 ) AS elapsed_time_seconds
             , SUM ( executions_delta ) AS executions
             , SUM ( buffer_gets_delta ) AS buffer_gets
             , SUM ( cpu_time_delta ) / 1000000 AS cpu_time_seconds
             , SUM ( rows_processed_delta ) AS rows_processed_delta
             , MIN ( snap_id ) AS begin_snap
             , MAX ( snap_id ) AS end_snap
             , MIN ( begin_interval_time ) AS begin_time
             , MAX ( end_interval_time ) AS end_time
             , MIN ( module ) AS module
             , MIN ( action ) AS action
             , MAX ( module ) AS module2
             , MAX ( action ) AS action2
             , MIN ( parsing_schema_name ) AS parsing_schema_name
          FROM dba_hist_snapshot NATURAL JOIN dba_hist_sqlstat
          /*WHERE end_interval_time between TO_TIMESTAMP ( '09-MAY-2016 10:00' , 'DD-MON-YYYY HH24:MI' )
                          AND ( 8/24 + ( TO_TIMESTAMP ( '09-MAY-2016 10:00' , 'DD-MON-YYYY HH24:MI' ) ) ) */
         WHERE snap_id BETWEEN 54806 AND 54808
           AND parsing_schema_name NOT IN ( 'DATAPROV' , 'SYS' )
         GROUP BY sql_id , dbid  --, plan_hash_value
         ORDER BY SUM ( elapsed_time_delta ) / 1000000 DESC
       ) sub
 WHERE ROWNUM <= 20
;
-- fk_subscrip_subscriphistoryst
SELECT * FROM dba_hist_sql_plan WHERE sql_id = '60p9ud8vg0zup' ORDER BY TIMESTAMP DESC , id ;
-- QUERY 5 , bind variable values
SELECT value_string , last_captured , datatype_string , name , position
  FROM dba_hist_sqlbind
 WHERE sql_id = '9f82p5493hm7v'
   AND last_captured > sysdate - 1
 ORDER BY last_captured DESC , name
;
-- display the timestamps:
SELECT last_captured AS sample_time , anydata.accesstimestamp(value_anydata) AS bind_value
  FROM dba_hist_sqlbind
 WHERE sql_id = 'bh52y21xuv25a'
   AND last_captured > sysdate - 10
   AND name = ':3'
 ORDER BY last_captured DESC , name
;
select * from v$sql_bind_capture where sql_id = '0dw6ng3nxtpvr' ;
-- QUERY 7, SQL PLan Management Baselines - http://www.pythian.com/blog/how-to-improve-sql-statements-performance-using-sql-plan-baselines/
SELECT DBMS_SQLTUNE.REPORT_SQL_MONITOR ( type => 'TEXT' , report_level => 'ALL' , sql_id => '0fh4hgzah7q13' ) AS report FROM DUAL ;
DECLARE
   cur SYS_REFCURSOR ;
BEGIN
   OPEN cur FOR
   SELECT VALUE(p)
   FROM TABLE (
      DBMS_SQLTUNE.SELECT_WORKLOAD_REPOSITORY ( begin_snap =>  , end_snap  => basic_filer => 'sql_id = ''0fh4hgzah7q13''' , populate_cursor => cur )
   )
END ;
/
-- 4113407748 = BAD
-- 667873522  = GOOD
DECLARE
  my_plans PLS_INTEGER ;
BEGIN
  my_plans := DBMS_SPM.load_plans_from_sqlset ( sqlset_name => 'AF_DATAPROV_0FH4HGZAH7Q13' , basic_filter => 'plan_hash_value = ''667873522''' ) ;
END;
/
-- flush one statement from shared pool https://blogs.oracle.com/mandalika/oracle-rdbms-:-flushing-a-single-sql-statement-out-of-the-object-library-cache
select ADDRESS, HASH_VALUE from V$SQLAREA where SQL_ID like 'bh52y21xuv25a' ;
exec sys.dbms_shared_pool.purge ( name => '00000007A1A5CB40, 2074970282' , flag => 'C' ) ;
-- cpu load on server
select test_description , begin_time , metric_name , round(average) , metric_unit
from hp_diag.test_result_metrics
where test_description is not null
and metric_name in ( 'Average Active Sessions' , 'Host CPU Utilization (%)' , 'Current OS Load' )
and database_name = 'CHORDO'
order by end_time desc , metric_name ;
-- QUERY 7, AWR retention
SELECT c.snap_interval , c.retention FROM dba_hist_wr_control c join v$database v on v.dbid = c.dbid ;
BEGIN
   DBMS_WORKLOAD_REPOSITORY.modify_snapshot_settings(
      retention => 86400  -- Minutes (= 60 Days)
    , interval => 15  -- Minutes
    , topnsql => 100
    ) ; 
END ;
/
-- AWR report top 100
EXEC DBMS_WORKLOAD_REPOSITORY.awr_set_report_thresholds ( top_n_sql => 100 ) ;
SELECT snap_id , end_interval_time , dbid FROM dba_hist_snapshot
WHERE end_interval_time BETWEEN TO_TIMESTAMP ( '03-Nov-2018 02:29' , 'DD-Mon-YYYY HH24:MI' ) AND TO_TIMESTAMP ( '03-Nov-2018 09:19' , 'DD-Mon-YYYY HH24:MI' )
ORDER BY end_interval_time DESC ;
SELECT * FROM TABLE ( DBMS_WORKLOAD_REPOSITORY.awr_report_html ( l_dbid => 999 , l_inst_num => 1 , l_bid => 998 , l_eid => 999 ) ) ;
SELECT * FROM TABLE ( DBMS_WORKLOAD_REPOSITORY.awr_diff_report_html ( dbid1 => 999 , inst_num1 => 1 , bid1 => 998 , eid1 => 999
   , dbid2 => 999 , inst_num2 => 1 , bid2 => 998 , eid2 => 999
 ) ) ;
-- QUERY 8, what to do after creating an FBI https://richardfoote.wordpress.com/2008/12/04/function-based-indexes-and-missing-statistics-no-surprises/
EXEC DBMS_STATS.GATHER_TABLE_STATS ( ownname => 'skybusiness' , tabname => 'JOB' , method_opt => 'FOR ALL HIDDEN COLUMNS SIZE 1' ) ;
SELECT column_name , histogram , num_buckets FROM dba_tab_columns WHERE table_name = 'AF_JOB' AND owner = 'DATAPROV'  ORDER BY 1 ;
SELECT MIN ( sample_time ) , MAX ( sample_time ) FROM v$active_session_history ;
-- decode high value and low value https://mwidlake.wordpress.com/2010/01/03/decoding-high_value-and-low_value/
select
to_char(1780+to_number(substr(low_value,1,2),'XX')
         +to_number(substr(low_value,3,2),'XX'))||'-'
       ||to_number(substr(low_value,5,2),'XX')||'-'
       ||to_number(substr(low_value,7,2),'XX')||' '
       ||(to_number(substr(low_value,9,2),'XX')-1)||':'
       ||(to_number(substr(low_value,11,2),'XX')-1)||':'
       ||(to_number(substr(low_value,13,2),'XX')-1) AS low_value
, to_char(1780+to_number(substr(high_value,1,2),'XX')
         +to_number(substr(high_value,3,2),'XX'))||'-'
       ||to_number(substr(high_value,5,2),'XX')||'-'
       ||to_number(substr(high_value,7,2),'XX')||' '
       ||(to_number(substr(high_value,9,2),'XX')-1)||':'
       ||(to_number(substr(high_value,11,2),'XX')-1)||':'
       ||(to_number(substr(high_value,13,2),'XX')-1) AS high_value
, c.*
from dba_tab_columns c WHERE table_name IN ( 'ECMBUSINESSACTHIST' ) and column_name like 'STA%' ORDER BY 1,2,3 ;
select * from v$sql where sql_id = 'bh52y21xuv25a' ;
-- QUERY 9 sql running recently
SELECT end_interval_time , snap_id , executions_delta , buffer_gets_delta , disk_reads_delta , elapsed_time_delta
     , CASE WHEN executions_delta > 0 THEN ROUND ( elapsed_time_delta / executions_delta / 1000 , 2 ) END AS millis_per_exec --, sql_text
  FROM dba_hist_sqlstat NATURAL JOIN dba_hist_snapshot
 WHERE sql_id = 'g12np75u1bryy'
 ORDER BY snap_id DESC
;
-- production optimizer statistics stats 
sys.chord_analyze.analyze_all ( p_retain_days => 60 ) ;  -- not sure about the p_retain_days parameter, but looks to be irrelevant from the code anyway.
-- do a partitioned table
select statType_locked , stale_stats , last_analyzed from dba_tab_statistics where table_name = 'BSBCUSTOMERTENURECACHE' ;
DBMS_STATS.UNLOCK_TABLE_STATS ( ownname => 'ccsowner' , tabname => 'bsbcustomertenurecache' ) ;
DBMS_STATS.SET_TABLE_PREFS ( ownname => 'ccsowner' , tabname => c_parts.table_name , pname => 'INCREMENTAL' , pvalue => 'TRUE' ) ;
DBMS_STATS.GATHER_TABLE_STATS ( options => 'GATHER STALE' , granularity => 'ALL' , FORCE => FALSE , ownname => c_owner.owner , tabname => 'bsbcustomertenurecache' ) ;
DBMS_STATS.LOCK_TABLE_STATS ( ownname => 'ccsowner' , tabname => 'bsbcustomertenurecache' ) ;
-- for non partitioned other tables. Is 10% in logs in /apps/ora/admin/logs/controlm
DBMS_STATS.GATHER_TABLE_STATS ( ownname => 'ccsowner' , tabname => 'bsbcustomerreward' , estimate_percent => 10 , cascade => TRUE , granularity => 'GLOBAL' ) ;
-- But Mark  in order management was doing this:
EXEC DBMS_STATS.GATHER_TABLE_STATS ( ownname => 'oh' , tabname => 'bsbcusord' , degree => 8 , estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE , method_opt => 'FOR ALL COLUMNS SIZE 1' , cascade => TRUE ) ;

-- QUERY 10 , privileges needed to allow dataprov to view http://www.thatjeffsmith.com/archive/2013/09/sql-developer-4-and-the-oracle-diagnostics-pack/
GRANT SELECT_CATALOG_ROLE TO dataprov ;  -- or could limit this further.
GRANT EXECUTE ON sys.dbms_workload_repository TO dataprov ;  -- Just selects from a table function from this, but still needs full execute :(
GRANT SELECT ON sys.wrm$_wr_control TO dataprov ;  -- to prevent ORA-942 on the first banner awr screen
-- QUERY 11, distinct logons connection pool
select osuser , username , program , module , machine , count(*)
from skyutils.sky_user_logins where timestamp between
to_timestamp ( '10-Feb-2016 15:19' , 'DD-Mon-YYYY HH24:MI' ) and to_timestamp ( '10-Feb-2016 17:23' , 'DD-Mon-YYYY HH24:MI' ) 
group by osuser , username , program , module , machine
order by 1,2,3,4 ;
order by timestamp desc ;
-- QUERY 12, MVs:
SELECT mview_name, last_refresh_date, round ( fullrefreshtim/60 ) as minutes , increfreshtim FROM dba_mview_analysis WHERE owner = 'BATCHPROCESS' ;
-- QUERY 13, tablespace space
select * from v$instance ;
select t.tablespace_name , round ( t.bytes/1024/1024/1024 , 2 ) as tot_gb , round ( f.bytes/1024/1024/1024 , 2 ) as free_gb , round(100*f.bytes/t.bytes , 2) as pct_free
from sys.sm$ts_avail t left outer join sys.sm$ts_free f on f.tablespace_name = t.tablespace_name
WHERE t.tablespace_name = 'TCC_LOB_AUTO_02'
order by 4 ;
select tablespace_name , substr ( file_name , -8 ) , file_name , bytes/1024/1024/1024 as gb , bytes/1024/1024 as mb , maxbytes/1024/1024 as max_mb , autoextensible
from dba_data_files
where tablespace_name = 'TCC_LOB_AUTO_02'
order by 1,2 desc ;
sqlplus / as sysdba
set pages 9999 lines 112
select t.tablespace_name , round ( t.bytes/1024/1024 ) as tot_mb , round ( u.bytes/1024/1024 ) as used_mb , round(100*u.bytes/t.bytes) as pct_used
from sys.sm$ts_avail t left outer join sys.sm$ts_used u on u.tablespace_name = t.tablespace_name 
order by 4 desc ;
SELECT tablespace_size/1024/1024/1024 AS gb_size , allocated_space/1024/1024/1024 AS alloc_gb , free_space/1024/1024/1024 AS free_gb FROM dba_temp_free_space ;
select df.file_id , df.bytes/1024/1024/1024 as df_gb ,df.user_bytes/1024/1024/1024 as user_gb , sum(fs.bytes)/1024/1024/1024 as free_gb
from dba_data_files df
join dba_free_space fs
on fs.tablespace_name = df.tablespace_name
and fs.file_id = df.file_id
where df.file_id in ( 916 , 917 , 918 )
group by df.bytes/1024/1024/1024 , df.user_bytes/1024/1024/1024 , df.file_id
order by 1
;
select distinct substr ( file_name , 1 , 26 ) , tablespace_name
from dba_data_files
where tablespace_name = 'CHORD_DATA_0001_01'
order by 1
;
-- History of tablespace space usage
SELECT sn.end_interval_time
     , LOWER ( ts.tsName ) AS ts_name
     , ROUND ( u.tablespace_size * 8/1024/1024 ) AS total_gb
     , ROUND ( u.tablespace_usedSize * 8/1024/1024 ) AS used_gb
  FROM dba_hist_snapshot sn
  JOIN dba_hist_tablespace_stat ts ON ts.dbid = sn.dbid AND ts.snap_id = sn.snap_id
  JOIN dba_hist_tbspc_space_usage u ON u.dbid = ts.dbid AND u.snap_id = ts.snap_id AND u.tablespace_id = ts.ts#
 WHERE ts.tsName IN ( 'CHORD_DATA_AUTO_14' )
   AND sn.end_interval_time > SYSTIMESTAMP - 1
 ORDER BY sn.end_interval_time DESC , ts.tsName
;
-- QUERY 14 AWR export pre environment refresh http://webcache.googleusercontent.com/search?q=cache:4A-NOkrNJzcJ:gavinsoorma.com/2009/07/exporting-and-importing-awr-snapshot-data/+&cd=1&hl=en&ct=clnk&gl=uk
select dbid , min(snap_id) , max(snap_id) , min(begin_interval_time) , max(begin_interval_time) from dba_hist_snapshot group by dbid order by 1 ;
set pages 9999
@awrextr.sql
VOL_REFRESH_EXP = /share/oraexpsol/VOLUME_REFRESH/N02/exports
awrdat_6316_12256.
-- QUERY 15 security
SELECT 'GRANT ' ||
       CASE WHEN object_type IN ( 'TABLE' , 'VIEW' ) THEN 'SELECT , INSERT , UPDATE , DELETE'
            WHEN object_type IN ( 'PACKAGE' , 'PROCEDURE' ) THEN 'EXECUTE'
            WHEN object_type = 'SEQUENCE' THEN 'SELECT'
            END
       || ' ON ' || LOWER ( owner || '.' || object_name ) || ' TO skybusiness_user ;' AS run_grant
  FROM dba_objects
 WHERE owner IN ( 'SB_REFDATAMGR' , 'SKYBUSINESS' )
   AND object_type IN ( 'TABLE' , 'VIEW' , 'PACKAGE' , 'PROCEDURE' , 'SEQUENCE' )
   AND object_name != 'TRUNCATE_SCHEMA'
 ORDER BY 1
;
SELECT 'CREATE SYNONYM skybusiness_user.' ||
       LOWER ( object_name )
       || ' FOR ' || LOWER ( owner || '.' || object_name ) || ' ;' AS run_synonym
  FROM dba_objects
 WHERE owner = 'SKYBUSINESS'
   AND object_type IN ( 'TABLE' , 'VIEW' , 'PROCEDURE' , 'SEQUENCE' )
   AND object_name != 'TRUNCATE_SCHEMA'
 ORDER BY 1
;
-- QUERY 16 identify snap_ids of interest for AWR report e.g.
select snap_id , end_interval_time
from dba_hist_snapshot
where begin_interval_time between to_date ( '07-FEB-2016 14', 'DD-Mon-YYYY HH24') and to_date ( '07-FEB-2016 16', 'DD-Mon-YYYY HH24')
order by begin_interval_time desc
;
select dbid , min ( end_interval_time ) , max ( end_interval_time ) , count(*) from dba_hist_snapshot group by dbid order by 3 ;
-- QUERY 17 tests run for 8 hours 45 minutes inc. 45 mins ramp up and ramp down time.
select to_date ( '25-FEB-2016 17:15' , 'DD-MON-YYYY HH24:MI' ) + 8.75/24 from dual ;
-- QUERY 18 ash = see also areid1.sql
SELECT * -- ash.user_id , ash.sql_id
  FROM dba_hist_active_sess_history ash
 WHERE sample_time BETWEEN TO_DATE ( '20-Mar-2016 01:00' , 'DD-Mon-YYYY HH24:MI' ) AND TO_DATE ( '20-Mar-2016 01:20' , 'DD-Mon-YYYY HH24:MI' )
   AND ash.sql_id IN ( '9d6r2xnvjp418' , '3r2zqwutx8k8g' )
;
-- Query 19 details of snapshots for Andrew Reid
SELECT s.snap_id
     , s.end_interval_time AS snap_time
     , s.startup_time AS database_startup
     , CASE WHEN v.dbid IS NOT NULL THEN 'CurrentDB' ELSE NULL END AS CurrentDB
     , di.dbid
     , di.db_name
  FROM dba_hist_snapshot s
  JOIN dba_hist_database_instance di ON di.dbid = s.dbid AND s.instance_number = di.instance_number AND di.startup_time = s.startup_time
  LEFT OUTER JOIN v$database v ON v.dbid = di.dbid
ORDER BY 2 DESC ;
SELECT CASE WHEN v.dbid IS NOT NULL THEN 'CurrentDB' ELSE NULL END AS CurrentDB
     , di.dbid
     , MIN ( s.end_interval_time ) AS first_snap
     , MAX ( s.end_interval_time ) AS last_snap
     , di.db_name
  FROM dba_hist_database_instance di
  JOIN dba_hist_snapshot s ON di.dbid = s.dbid AND s.instance_number = di.instance_number AND di.startup_time = s.startup_time
  LEFT OUTER JOIN v$database v ON v.dbid = di.dbid
GROUP BY v.dbid , di.dbid , di.db_name
ORDER BY 1 , 4 DESC ;
-- Rowena is select osuser from v$session where machine = 'BSKYB\LIVWS0220823' ;
-- Query 20 top 10 cross database
SELECT *
  FROM ( -- sub to sort before rownum
        SELECT LOWER ( database_name ) AS database
             , sql_id
             , ROUND ( elapsed_time_seconds ) as secs
             , executions
             , ROUND ( elapsed_time_per_exec_seconds , 2 ) AS secs_per_exec
             --, ROUND ( elapsed_time_per_exec_seconds * 1000 ) AS ela_per_exec_millisecs
             --, SUBSTR ( REPLACE ( REPLACE ( sql_text , CHR(10) , ' ' ) , CHR(13), ' ' ) , 1 , 100 ) AS sql_text
             , SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( sql_text , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) , '  ' , ' ' ) , '  ' , ' ' )
                , 1 , 100 ) AS sql_text
          FROM hp_diag.test_result_sql
         WHERE test_description = 'R129 Baseline R128' -- 'R129 Smoke'
         ORDER BY elapsed_time_seconds DESC
       )
 WHERE ROWNUM <= 10
;
-- Query 20a)
ALTER SESSION SET nls_date_format = 'YYYYMMDD HH24:MI' ; 
SELECT ROUND ( SUM ( CASE metric_name WHEN 'Average Active Sessions' THEN average END ) ) AS "Average Active Sessions"
     , ROUND ( SUM ( CASE metric_name WHEN 'Current OS Load' THEN average END ) ) AS "Current OS Load"
     , ROUND ( SUM ( CASE metric_name WHEN 'Host CPU Utilization (%)' THEN average END ) ) AS "Host CPU Utilization (%)"
     , ROUND ( SUM ( CASE metric_name WHEN 'User Commits Per Sec' THEN average END ) ) AS "User Commits Per Sec"
     , begin_time , end_time , test_description
  FROM hp_diag.test_result_metrics
 WHERE database_name = 'CHORDO'
   AND metric_name IN ( 'Average Active Sessions' , 'Current OS Load' , 'Host CPU Utilization (%)' , 'User Commits Per Sec' )
   AND ( test_description LIKE '%R12%' OR test_description LIKE '%R130%' )
 GROUP BY begin_time , end_time , test_description
 ORDER BY end_time DESC
;
SELECT SUM ( CASE sql_id WHEN '4anfwhr357vpd' THEN executions END ) AS "4anfwhr357vpd"
     , SUM ( CASE sql_id WHEN 'f621h8csmsvx1' THEN executions END ) AS "f621h8csmsvx1"
     , SUM ( CASE sql_id WHEN '3r4gu0kc7x7fs' THEN executions END ) AS "3r4gu0kc7x7fs"
     , SUM ( CASE sql_id WHEN '60a1x0pfsxtd8' THEN executions END ) AS "60a1x0pfsxtd8"
     , SUM ( CASE sql_id WHEN 'c6ytzqccd8324' THEN executions END ) AS "c6ytzqccd8324"
     , SUM ( CASE sql_id WHEN 'fthyc9kchz0rt' THEN executions END ) AS "fthyc9kchz0rt"
     , SUM ( CASE sql_id WHEN 'dpcau740v852k' THEN executions END ) AS "dpcau740v852k"
     , SUM ( CASE sql_id WHEN '8v1jb7xth02s0' THEN executions END ) AS "8v1jb7xth02s0"
     , SUM ( CASE sql_id WHEN '6khvtsw70c21s' THEN executions END ) AS "6khvtsw70c21s"
     , MIN ( begin_time ) , MAX ( end_time ) , test_description
  FROM hp_diag.test_result_sql
 WHERE sql_id IN ( '4anfwhr357vpd' , 'f621h8csmsvx1' , '3r4gu0kc7x7fs' , '60a1x0pfsxtd8' , 'c6ytzqccd8324' , 'fthyc9kchz0rt' , 'dpcau740v852k' , '8v1jb7xth02s0' , '6khvtsw70c21s' )
   AND database_name = 'CHORDO'
   AND ( test_description LIKE '%R12%' or test_description LIKE '%R130%' )
 GROUP BY test_description
 ORDER BY MAX ( end_time ) DESC
;
-- Query 21) ash
SELECT TO_CHAR ( sample_time , 'YYYYMMDD_HH24:MI' ) , COUNT(*)
  FROM v$active_session_history h  -- dba_hist_active_sess_history
  --LEFT OUTER JOIN dba_users u ON u.user_id = h.user_id
 WHERE sample_time BETWEEN TO_DATE ( '30-Mar-2016 14:00:00' , 'DD-Mon-YYYY HH24:MI:SS' ) AND TO_DATE ( '30-Mar-2016 16:00:00' , 'DD-Mon-YYYY HH24:MI:SS' )
   and program like 'rman%'
 GROUP BY TO_CHAR ( sample_time , 'YYYYMMDD_HH24:MI' )
 ORDER BY 1
;
-- Query 22) Is a test running now?
SELECT username , machine , module , status , last_call_et FROM v$session WHERE ( status = 'ACTIVE' OR last_call_et < 1 ) AND username IS NOT NULL
AND username = 'CBSSERVICES_USER'
;
-- Query 23) 
EXEC SYS.DBMS_SYSTEM.SET_INT_PARAM_IN_SESSION ( sid => 1234 , serial# => 1234 , parnam => 'max_dump_file_size' , intval => 100*1024*1024 ) ;
EXEC DBMS_MONITOR.session_trace_enable ( session_id => 1234 , serial_num => 1234 , waits => TRUE , binds => FALSE ) ;
-- Query 24) trace
CREATE OR REPLACE TRIGGER af_drop
AFTER LOGON ON DATABASE
WHEN (
       USER = 'OMREL_USER'
   AND SYS_CONTEXT ( 'userenv' , 'module' ) = 'JDBC Thin Client'
   )
BEGIN
   EXECUTE IMMEDIATE 'ALTER SESSION SET tracefile_identifier = ''omrel_user''' ;
   EXECUTE IMMEDIATE 'ALTER SESSION SET max_dump_file_size = unlimited' ;
   --dbms_monitor.session_trace_enable ;  -- need to compile as sys :(
   dbms_session.session_trace_enable ( waits => FALSE ) ;
END ;
/
--
ALTER SESSION SET tracefile_identifier = 'andrew_fraser' ;
ALTER SESSION SET max_dump_file_size = unlimited ;
EXEC dbms_session.session_trace_enable ( waits => TRUE ) ;
@1
EXEC dbms_session.session_trace_disable ;
EXEC DBMS_MONITOR.session_trace_enable ( session_id => 1234 , serial_num => 1234 , waits => TRUE , binds => FALSE ) ;
ALTER SYSTEM SET EVENTS 'sql_trace [sql:av3nyus5dkhb1] bind=false, wait=true' ;
ALTER SYSTEM SET EVENTS 'sql_trace [sql:av3nyus5dkhb1] off' ;
-- 0 rows in v$active_session_history empty no rows
ALTER SYSTEM SET "_ash_enable" = FALSE ;
ALTER SYSTEM SET "_ash_enable" = TRUE ;
-- check is on with: (is also written to alert log)
oradebug setmypid
oradebug eventdump system
-- sql_trace [sql:av3nyus5dkhb1] bind=false, wait=true
CREATE OR REPLACE TRIGGER af_drop
AFTER LOGON ON DATABASE
WHEN (
       USER = 'BATCHPROCESS_USER'
   )
BEGIN
   EXECUTE IMMEDIATE 'ALTER SESSION SET max_dump_file_size = unlimited' ;
END ;
/
-- Query 25) 
-- top 10 before, what happened to them after
  WITH b AS ( SELECT * FROM hp_diag.test_result_sql WHERE database_name = 'CHORDO' AND test_description = 'R129 Baseline - 20160504 Pre-BDR Concurrent Blend' AND top_sql_number <= 10 )
     , a AS ( SELECT * FROM hp_diag.test_result_sql WHERE database_name = 'CHORDO' AND test_description = '19052016 Concurrent Pre-BDR' )
SELECT b.sql_id as before_sql
     , ROUND ( b.elapsed_time_per_exec_seconds * 1000 ) AS ela_per_exec_millisecs_before
     , ROUND ( a.elapsed_time_per_exec_seconds * 1000 ) AS ela_per_exec_millisecs_after
     , b.top_sql_number AS top_sql_number_before
     , a.top_sql_number AS top_sql_number_after
     , ROUND ( b.elapsed_time_seconds ) as ela_secs_before
     , ROUND ( a.elapsed_time_seconds ) as ela_secs_after
     , b.executions AS executions_before
     , a.executions AS executions_after
     , b.sql_text
  FROM b LEFT OUTER JOIN a ON a.sql_id = b.sql_id
 ORDER BY b.top_sql_number
;
-- and other way around
  WITH b AS ( SELECT * FROM hp_diag.test_result_sql WHERE database_name = 'CHORDO' AND test_description = 'R129 Baseline - 20160504 Pre-BDR Concurrent Blend' )
     , a AS ( SELECT * FROM hp_diag.test_result_sql WHERE database_name = 'CHORDO' AND test_description = '19052016 Concurrent Pre-BDR' AND top_sql_number <= 10 )
SELECT a.sql_id as after_sql
     , ROUND ( b.elapsed_time_per_exec_seconds * 1000 ) AS ela_per_exec_millisecs_before
     , ROUND ( a.elapsed_time_per_exec_seconds * 1000 ) AS ela_per_exec_millisecs_after
     , b.top_sql_number AS top_sql_number_before
     , a.top_sql_number AS top_sql_number_after
     , ROUND ( b.elapsed_time_seconds ) as ela_secs_before
     , ROUND ( a.elapsed_time_seconds ) as ela_secs_after
     , b.executions AS executions_before
     , a.executions AS executions_after
     , a.sql_text
  FROM a
  LEFT OUTER JOIN b ON a.sql_id = b.sql_id
 ORDER BY a.top_sql_number
;
-- module grabbing
select sql_id , module , machine from dba_hist_active_sess_history
where sql_id in ( 'dpcau740v852k' , '0yhhh1ghcsugy' )
and sample_time > sysdate - 4
--and module != 'JDBC Thin Client'
and rownum < 21
;
select sql_id , h.module , h.machine , s.module
from dba_hist_active_sess_history h
left outer join skyutils.sky_services s on s.machine = h.machine
where sql_id in ( 'dpcau740v852k' )
and sample_time > sysdate - 4
--and module != 'JDBC Thin Client'
and rownum < 21
;
-- query 26 no bind variables hard parse
select /* Andrew Fraser */ parsing_schema_name , substr(sql_text,1,50) , count(*)
--, min(sql_fulltext)
, min ( last_active_time ) , max ( last_active_time )
, min(sql_id) , max(sql_id) , min(sql_text) , max(sql_text)
from v$sql
where parsing_schema_name not in ( 'HP_DIAG' , 'SKYUTILS' , 'SYS' )
and sql_text not like 'SELECT /* DS_SVC%'
group by substr(sql_text,1,50) , parsing_schema_name
having count(*) > 10
order by count(*) desc
;
select * from v$sql where sql_text like 'select externalop0_.C_MU as col_0_0_, externalop0_%'
and parsing_schema_name = 'CTIEX'
order by last_active_time desc
;
-- query 27 module action chordiant batch
SELECT snap_id
     , end_interval_time , sql_id
     , plan_hash_value
     , ROUND ( elapsed_time_delta / 1000000 ) AS secs
     , CASE WHEN executions_delta != 0 THEN ROUND ( elapsed_time_delta / executions_delta / 1000000 , 2 ) END AS secs_per_exec
     , executions_delta
     , rows_processed_delta , module , action
     , SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( sql_text , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) , '  ' , ' ' ) , '  ' , ' ' )
                , 1 , 100 ) AS sql_text
  FROM dba_hist_snapshot NATURAL JOIN dba_hist_sqlstat NATURAL JOIN dba_hist_sqltext
 WHERE module in ( 'ccpre010' , 'ccpre020' )
 ORDER BY snap_id DESC , sql_id
;
-- session pga memory leak
SELECT a.sid , s.username , s.osuser , s.program , s.logon_time , b.name, ROUND ( a.value / 1024 / 1024 ) AS mb 
  FROM v$sesstat a
  JOIN v$statname b ON a.statistic# = b.statistic#
  JOIN v$session s ON s.sid = a.sid
 WHERE b.name = 'session pga memory'
   AND s.username IS NOT NULL
 ORDER BY a.value DESC
;
SELECT ROUND ( MAX ( pga_allocated ) / 1024 / 1024 ) AS mb FROM dba_hist_active_sess_history WHERE user_id = 53 ;  -- 4090 in n02, 1088 in n01 (recently refreshed)
-- editions
SELECT edition_name FROM dba_editions ORDER BY 1 ;
ALTER SESSION SET EDITION = DB_OMS_7_8 ;
SELECT SYS_CONTEXT ( 'userenv' , 'session_edition_name' ) FROM DUAL ;
SELECT s.machine , s.username , s.osuser , s.session_edition_id , o.object_name , COUNT(*) , MIN(logon_time) , MAX(logon_time)
  FROM v$session s
  LEFT OUTER JOIN dba_objects o ON s.session_edition_id = o.object_id
WHERE s.username IS NOT NULL
   AND ( s.status = 'ACTIVE' or s.last_call_et < 61 )
GROUP BY s.machine , s.username , s.osuser , s.session_edition_id , o.object_name
-- HAVING COUNT(*) > 4
ORDER BY 1 , 2 , 3 , 4 ;
-- christmas tree - msc https://asktom.oracle.com/pls/asktom/f?p=100:11:0::::P11_QUESTION_ID:14039738984108
SELECT CASE SIGN ( FLOOR ( maxWidth / 2 ) - ROWNUM )
            WHEN 1 THEN LPAD ( ' ' , FLOOR ( maxWidth / 2 ) - ROWNUM + 1 ) || RPAD ( '*' , 2 * ( ROWNUM - 1 ) + 1 , ' *' )
            ELSE LPAD ( '* * *' , FLOOR ( maxWidth / 2 ) + 3 )
            END AS text
  FROM all_objects
 CROSS JOIN ( SELECT 40 AS maxWidth FROM DUAL )
 WHERE ROWNUM < FLOOR ( maxWidth / 2 ) + 5
;


-- scrap
   TYPE l_type IS TABLE OF employees%ROWTYPE INDEX BY PLS_INTEGER ;
   l_tab l_type ;
BEGIN
   SELECT * BULK COLLECT INTO l_tab FROM employees LIMIT 1000000 ;
   FOR i IN 1 .. l_tab.COUNT
   LOOP
      <stuff>
   END LOOP ;
   FORALL i IN l_tab.FIRST .. l_tab.LAST  -- best, bulk bind the loop also
      <stuff> ;
END ;
/*
https://asktom.oracle.com/pls/asktom/f?p=100:11:0::::P11_QUESTION_ID:2668391900346844476
Lets compare the space 
Need to use http://www.justskins.com/forums/block-level-row-structure-240203.html
Put the null column somewhere other than the end, to get the null pointer.
http://www.peasland.net/2011/08/01/dumping-data-blocks/
*/
ALTER SESSION SET recyclebin = off ;
CREATE TABLE tab1 ( vcn VARCHAR2(10) , chn CHAR(10) , vc1 VARCHAR2(1) , ch1 CHAR(1) , vc2 VARCHAR2(2) , ch2 CHAR(2) ) ;
INSERT INTO tab1 SELECT NULL , NULL , 'A' , 'A' , 'AA' , 'AA' FROM DUAL CONNECT BY LEVEL <= 10000 ;
COMMIT ;
EXEC DBMS_STATS.GATHER_TABLE_STATS ( ownname => USER , tabname => 'tab1' , estimate_percent => 100 ) ;
SELECT avg_row_len FROM user_tables WHERE table_name = 'TAB1' ;
-- 10 bytes
SELECT VSIZE(vcn) , VSIZE(chn) , VSIZE(vc1) , VSIZE(ch1) , VSIZE(vc2) , VSIZE(ch2 ) FROM tab1 ;
-- 0 0 1 1 2 2 = 6. Where/what are the other 4? Two could be null pointers. 
SELECT header_file , header_block + 1
  FROM dba_segments
 WHERE segment_name = 'TAB1'
   AND owner = USER ;
-- 292   1518444
ALTER SYSTEM dump datafile 292 block min 1518444 block max 1518444 ;
/* This says 15 bytes, including overhead!
tab 0, row 425, @0x6cf
tl: 15 fb: --H-FL-- lb: 0x1  cc: 6
col  0: *NULL*
col  1: *NULL*
col  2: [ 1]  41
col  3: [ 1]  41
col  4: [ 2]  41 41
col  5: [ 2]  41 41
*/


CREATE TABLE af_vc ( vc VARCHAR2(1) ) ;
CREATE TABLE af_ch ( ch CHAR(1) ) ;
INSERT INTO af_vc SELECT 'A' FROM DUAL CONNECT BY LEVEL <= 10000 ;
INSERT INTO af_ch SELECT 'A' FROM DUAL CONNECT BY LEVEL <= 10000 ;
COMMIT ;
EXEC DBMS_STATS.GATHER_TABLE_STATS ( ownname => USER , tabname => 'af_vc' , estimate_percent => 100 ) ;
EXEC DBMS_STATS.GATHER_TABLE_STATS ( ownname => USER , tabname => 'af_ch' , estimate_percent => 100 ) ;
SELECT table_name , avg_row_len FROM user_tables WHERE table_name IN ( 'AF_VC' , 'AF_CH' ) ORDER BY table_name ;
-- shows both are 2 bytes, so no difference.
DROP TABLE af_vc ;
DROP TABLE af_ch ;
CREATE TABLE af_vc ( vc VARCHAR2(2) ) ;
CREATE TABLE af_ch ( ch CHAR(2) ) ;
INSERT INTO af_vc SELECT 'AA' FROM DUAL CONNECT BY LEVEL <= 10000 ;
INSERT INTO af_ch SELECT 'AA' FROM DUAL CONNECT BY LEVEL <= 10000 ;
COMMIT ;
EXEC DBMS_STATS.GATHER_TABLE_STATS ( ownname => USER , tabname => 'af_vc' , estimate_percent => 100 ) ;
EXEC DBMS_STATS.GATHER_TABLE_STATS ( ownname => USER , tabname => 'af_ch' , estimate_percent => 100 ) ;
SELECT table_name , avg_row_len FROM user_tables WHERE table_name IN ( 'AF_VC' , 'AF_CH' ) ORDER BY table_name ;
-- shows both are 3 bytes, so no difference.
TRUNCATE TABLE af_vc ;
TRUNCATE TABLE af_ch ;
INSERT INTO af_vc SELECT NULL FROM DUAL CONNECT BY LEVEL <= 10000 ;
INSERT INTO af_ch SELECT NULL FROM DUAL CONNECT BY LEVEL <= 10000 ;
EXEC DBMS_STATS.GATHER_TABLE_STATS ( ownname => USER , tabname => 'af_vc' , estimate_percent => 100 ) ;
EXEC DBMS_STATS.GATHER_TABLE_STATS ( ownname => USER , tabname => 'af_ch' , estimate_percent => 100 ) ;
SELECT table_name , avg_row_len FROM user_tables WHERE table_name IN ( 'AF_VC' , 'AF_CH' ) ORDER BY table_name ;
-- shows both are 0 bytes, so no difference.
/*
So it is worth using CHAR instead of VARCHAR2 *if*
1) You are absolutely sure the data will always and in every case be precisely that length - think columns containing SYS_GUID or one character M/F type codes.
2) NULLs - 
*/
-- Control-M batch log files
-- chdbatchptt + sudo su - chobtptt + PS1="chdbatchptt> " + cd /batch/batchapps/chobtptt/BATCH/logs/
-- dpsdbptt + sudo su - dpsdbtptt + PS1="dpsdbptt> " + cd /PTT/dpsdb/batch/O01/dpsbtptt/logs/
/apps/batch/chobtptt/BATCH/logs/
for fle in $(find /batch/batchapps/chobtptt/BATCH/logs/ -name '*.log' -mtime -7 )
do
  c1=$(grep -ci ORA-02393 $fle)
  if [ $c1 -ne '0' ]
  then
    echo -n $fle
    echo -n ","
    echo $c1
  fi
done


-- codegroup
SELECT code || ' ' || INITCAP ( codeDesc ) , codeGroup , rdmDeletedFlag FROM refdatamgr.picklist WHERE codeGroup LIKE 'Customer%ProductElementStatus' ORDER BY 1,2 ;


-- create user
begin
   dbms_metadata.set_transform_param (dbms_metadata.session_transform, 'SQLTERMINATOR', true);
   dbms_metadata.set_transform_param (dbms_metadata.session_transform, 'PRETTY', true);
end;
/
select dbms_metadata.get_ddl('USER', u.username) AS ddl
from   dba_users u
where  u.username = 'DATAFIX'
union all
select dbms_metadata.get_granted_ddl('TABLESPACE_QUOTA', tq.username) AS ddl
from   dba_ts_quotas tq
where  tq.username = 'DATAFIX'
and    rownum = 1
union all
select dbms_metadata.get_granted_ddl('ROLE_GRANT', rp.grantee) AS ddl
from   dba_role_privs rp
where  rp.grantee = 'DATAFIX'
and    rownum = 1
union all
select dbms_metadata.get_granted_ddl('SYSTEM_GRANT', sp.grantee) AS ddl
from   dba_sys_privs sp
where  sp.grantee = 'DATAFIX'
and    rownum = 1
union all
select dbms_metadata.get_granted_ddl('OBJECT_GRANT', tp.grantee) AS ddl
from   dba_tab_privs tp
where  tp.grantee = 'DATAFIX'
and    rownum = 1
union all
select dbms_metadata.get_granted_ddl('DEFAULT_ROLE', rp.grantee) AS ddl
from   dba_role_privs rp
where  rp.grantee = 'DATAFIX'
and    rp.default_role = 'YES'
and    rownum = 1
union all
select to_clob('/* Start profile creation script in case they are missing') AS ddl
from   dba_users u
where  u.username = 'DATAFIX'
and    u.profile <> 'DEFAULT'
and    rownum = 1
union all
select dbms_metadata.get_ddl('PROFILE', u.profile) AS ddl
from   dba_users u
where  u.username = 'DATAFIX'
and    u.profile <> 'DEFAULT'
union all
select to_clob('End profile creation script */') AS ddl
from   dba_users u
where  u.username = 'DATAFIX'
and    u.profile <> 'DEFAULT'
and    rownum = 1
;

-- Greg Neave sql fix
WITH service AS (
   SELECT si.id
        , CONNECT_BY_ROOT si.id AS billingserviceinstanceid
        , CONNECT_BY_ROOT ba_si.id AS billingid
     FROM ccsowner.bsbserviceinstance si
        , ( SELECT serviceinstanceid, id FROM ccsowner.bsbbillingaccount WHERE id = :1 ) ba_si
    START WITH si.parentserviceinstanceid IS NULL AND si.id = ba_si.serviceinstanceid
CONNECT BY PRIOR si.id = si.parentserviceinstanceid
)
-- Stuart Anderson sql fix
SELECT COUNT(*)
  FROM ccsowner.bsbBillingAccount ba
  JOIN ccsowner.bsbAddressUsageRole aur ON aur.serviceInstanceId = ba.serviceInstanceId  -- this table has addressId
  JOIN ccsowner.bsbVisitRequirement vr ON vr.installationAddressRoleId = aur.id
 WHERE ba.accountNumber = '623180624164'
   AND SYSDATE BETWEEN TRUNC ( aur.effectiveFrom ) AND NVL ( aur.effectiveTo , SYSDATE + 1 )
   AND vr.statusCode IN ( 'CP' , 'CF' )
   AND vr.visitDate >= TRUNC ( SYSDATE )
   AND vr.visitDate < TRUNC ( SYSDATE ) + 1
;



;
-- good lock code
with base as (
select /*+ materialize */ current_obj# , current_file# , current_block# , current_row# , count(*) as samples , max(sql_exec_start - sample_time) as tdiff , max(sample_time) as maxTime , min(sample_time) as minTime
from v$active_session_history a
where sql_id = '7d54cdzq7dbyb'
and event like 'enq%'
group by current_obj# , current_file# , current_block# , current_row#
)
select current_obj# , current_file# , current_block# , current_row# , samples , tdiff , minTime , maxTime
, dbms_rowid.rowid_create ( rowid_type => 1 , object_number => current_obj# , relative_fno => current_file# , block_number => current_block# , row_number => current_row# ) as af_rowid
from base
order by af_rowid
;
select e.*
from ccsowner.eicommunication as of timestamp TO_TIMESTAMP ( '19-Apr-2018 00:03:00' , 'DD-Mon-YYYY HH24:MI:SS' ) e
where id IN (
'12612520068251785862532'
, '12628665481873018564614'
, '12650319222964715241381'
, '12658263465595617590682'
, '12660770971815845704978'
, '12620304322112352104669'
, '12632056015553291162736'
, '12799713250034059186926'
)
order by 1
;
-- database upgrade versions 12c 12.1
select * from dba_hist_database_instance order by startup_time desc ;
-- START sql plan management baseline copying https://oracle-base.com/articles/11g/sql-plan-management-11gr1 and https://blog.pythian.com/how-to-improve-sql-statements-performance-using-sql-plan-baselines/
SET SERVEROUTPUT ON ;
DECLARE
  l_plans_loaded  PLS_INTEGER;
BEGIN
  l_plans_loaded := DBMS_SPM.load_plans_from_cursor_cache(
    sql_id => 'ccd8cfz1ns6zs');
    
  DBMS_OUTPUT.put_line('Plans Loaded: ' || l_plans_loaded);
END;
/
;
select * from DBA_SQL_PLAN_BASELINES order by created desc ;
;
BEGIN
  DBMS_SPM.CREATE_STGTAB_BASELINE(
    table_name      => 'af_drop9',
    table_owner     => 'SYSTEM',
    tablespace_name => 'tcc_data_auto_01');
END;
/
DECLARE
  l_plans_packed  PLS_INTEGER;
BEGIN
  l_plans_packed := DBMS_SPM.pack_stgtab_baseline(
    table_name      => 'af_drop9',
    table_owner     => 'SYSTEM'
    , sql_handle => 'SQL_f197b31e9e6485c1'
    , accepted => 'YES'
  );
  DBMS_OUTPUT.put_line('Plans Packed: ' || l_plans_packed);
END;
/
commit ;
-- copy the table to E05, then on E05:
SET SERVEROUTPUT ON ;
DECLARE
  l_plans_unpacked  PLS_INTEGER;
BEGIN
  l_plans_unpacked := DBMS_SPM.unpack_stgtab_baseline(
    table_name      => 'af_drop9',
    table_owner     => 'BSBDEPLOY'
) ;
  DBMS_OUTPUT.put_line('Plans Unpacked: ' || l_plans_unpacked);
END;
/
exec sys.dbms_shared_pool.purge ( name => '000000041D279318, 3276545016' , flag => 'C' ) ;
drop table bsbdeploy.af_drop9 ;
-- then back on chordo
SET SERVEROUTPUT ON ;
DECLARE
  l_plans_dropped  PLS_INTEGER;
BEGIN
  l_plans_dropped := DBMS_SPM.drop_sql_plan_baseline (
    sql_handle => NULL,
    plan_name  => 'SQL_PLAN_d55fbdza19y64d61b2f9b');
    
  DBMS_OUTPUT.put_line(l_plans_dropped);
END;
/
select * from DBA_SQL_PLAN_BASELINES order by created desc ;
-- END sql plan management baseline copying

-- join accountnumber partyid person
SELECT ba.accountNumber , per.partyId , per.firstName , per.familyName
  FROM ccsowner.bsbBillingAccount ba
  JOIN ccsowner.bsbCustomerRole cr ON ba.portfolioId = cr.portfolioId
  JOIN ccsowner.bsbPartyRole pr ON pr.id = cr.partyRoleId
  --JOIN ccsowner.party p ON p.id = pr.partyId
  JOIN ccsowner.person per ON per.partyId = pr.partyId
 WHERE ba.accountNumber IN ( '200002301279' , '912000048450' , '200005008111' , '200003616444' , '200003618820' )
 ORDER BY ba.accountNumber
;
-- 12c upgrade
select * from dba_hist_database_instance order by startup_time desc ;

-- connection pools in use.
select s.machine , m.module , count(*) as total , sum ( case status when 'ACTIVE' then 1 else 0 end ) as num_active
SELECT * FROM v$diag_alert_ext ;  -- alert log

, round(avg(  case status when 'ACTIVE' THEN 0 ELSE last_call_et END )) as ave_secs_idle
from v$session s
left outer join skyutils.sky_services m on s.machine = m.machine
where s.osuser = 'tomcat'
--and m.module = 'cbs-sales-interaction-service'
group by s.machine , m.module
order by count(*) desc
;

-- flashback source, as sys
select text
from dba_source
-- as of timestamp to_timestamp ( '03-May-2018 16:48' , 'DD-Mon-YYYY HH24:MI' )
as of timestamp systimestamp - 0.8
where owner = 'DATAPROV' and name = 'DATA_PREP_STATIC'
and line between 5570 and 5620
;
-- telephone number <-> accountNumber
SELECT con.partyId , ba.accountNumber , ba.created , ba.createdBy , ba.lastUpdate , ba.updatedBy , t.combinedTelephoneNumber
  FROM ccsowner.bsbTelephone t
  JOIN ccsowner.bsbContactTelephone ct ON ct.telephoneId = t.id
  JOIN ccsowner.bsbContactor con ON con.id = ct.contactorId
  JOIN ccsowner.bsbPartyRole pr ON pr.partyId = con.partyId
  JOIN ccsowner.bsbCustomerRole cr ON cr.partyRoleId = pr.id
  JOIN ccsowner.bsbBillingAccount ba ON ba.portfolioId = cr.portfolioId
 WHERE t.combinedTelephoneNumber = '07923913578'
;

-- query alert log
select originating_timestamp , message_text
  from x$dbgalertext
 where message_text like '%ORA%'
 order by originating_timestamp desc
;

-- top sql best
{csv:columnTypes=f,s,f,f,f,s}
"Num","Sql Id","Secs","Secs Per Exec","TPS","Sql Text"
SELECT '"' ||
       ROWNUM
       || '","' ||
       a.sql_id
       || '","' ||
       a.elapsed_time_seconds
       || '","' ||
       a.elapsed_time_per_exec_seconds
       || '","' ||
       a.tps
       a.tps
       || '","' ||
       a.sql_text
       || '"' AS col1
  FROM ( -- sub to sort before rownum
        SELECT s.sql_id
             , TO_CHAR ( ROUND ( s.elapsed_time_delta / 1000 / 1000 ) , 'FM9,999,999,999,999,999,999,999,999,999,990' ) AS elapsed_time_seconds
             , TO_CHAR ( ROUND ( s.elapsed_time_delta / s.executions_delta / 1000 / 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS elapsed_time_per_exec_seconds
             , TO_CHAR ( ROUND ( s.executions_delta / 15 / 60 ) , 'FM9,999,999,999,999,999,999,999,999,999,990' ) AS tps
             ,  LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) , '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text
          FROM dba_hist_sqlstat s JOIN dba_hist_sqltext t ON t.sql_id = s.sql_id AND t.dbid = s.dbid
         WHERE snap_id = 10706  -- select snap_id , end_interval_time from dba_hist_snapshot where end_interval_time > sysdate - 1 order by 2 desc ;
         AND s.executions_delta > 0
         ORDER BY s.elapsed_time_delta DESC
       ) a
 WHERE ROWNUM <= 20
;
{csv}
-- same for chordiant, to include module
{csv:columnTypes=f,s,s,f,f,f,s}
"Num","Sql Id","Service","Secs","Secs Per Exec","TPS","Sql Text"
SELECT '"' ||
       ROWNUM
       || '","' ||
       a.sql_id
       || '","' ||
       a.module
       || '","' ||
       a.elapsed_time_seconds
       || '","' ||
       a.elapsed_time_per_exec_seconds
       || '","' ||
       a.executions_delta
       || '","' ||
       a.tps
       || '","' ||
       a.sql_text
       || '"' AS col1
  FROM ( -- sub to sort before rownum
        SELECT s.sql_id
             , CASE WHEN s.module IS NULL OR s.module = 'JDBC Thin Client' THEN LOWER ( s.parsing_schema_name ) ELSE s.module END AS module
             , TO_CHAR ( ROUND ( s.elapsed_time_delta / 1000 / 1000 ) , 'FM9,999,999,999,999,999,999,999,999,999,990' ) AS elapsed_time_seconds
             , TO_CHAR ( ROUND ( s.elapsed_time_delta / s.executions_delta / 1000 / 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS elapsed_time_per_exec_seconds
             , TO_CHAR ( s.executions_delta , 'FM9,999,999,999,999,999,999,999,999,999,990' ) AS executions_delta
             , TO_CHAR ( ROUND ( s.executions_delta / 15 / 60 ) , 'FM9,999,999,999,999,999,999,999,999,999,990' ) AS tps
             ,  LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) , '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text
          FROM dba_hist_sqlstat s JOIN dba_hist_sqltext t ON t.sql_id = s.sql_id AND t.dbid = s.dbid
         WHERE snap_id = 10706  -- select snap_id , end_interval_time from dba_hist_snapshot where end_interval_time > sysdate - 1 order by 2 desc ;
         AND s.executions_delta > 0
         ORDER BY s.elapsed_time_delta DESC
       ) a
 WHERE ROWNUM <= 20
;
{csv}
-- dataprov log - also table _detail 
SELECT * FROM user_scheduler_job_run_details WHERE status != 'SUCCEEDED' ORDER BY log_date DESC NULLS LAST ;
SELECT * FROM dataprov.run_job_parallel_control WHERE start_time > SYSDATE - 1 ORDER BY start_time DESC NULLS LAST ;
SELECT * FROM dataprov.dp_test_refresh_runs WHERE start_time > SYSDATE - 1 ORDER BY start_time DESC NULLS LAST ;
SELECT * FROM dataprov.dp_s_test_refresh_runs WHERE start_time > SYSDATE - 1 ORDER BY start_time DESC NULLS LAST ;
SELECT * FROM dataprov.dp_s_test_refresh_runs WHERE test_name = 'CANCELCONFIRMCOMMUNICATION' AND start_time > SYSDATE - 2 ORDER BY start_time DESC NULLS LAST ;
SELECT * FROM dataprov.dp_s_test_refresh_runs_detail WHERE test_name = 'CANCELCONFIRMCOMMUNICATION' AND start_time > SYSDATE - 2 ORDER BY detail_id DESC , start_time DESC NULLS LAST ;
SELECT start_time , end_time , ROUND ( 24 * 60 * ( end_time - start_time ) ) AS mins , test_name FROM dataprov.dp_s_test_refresh_runs WHERE start_time > SYSDATE - 1 order by 3 desc , 1 , 2 ;
ALTER SESSION SET nls_date_format = 'Dy DD-Mon-YYYY HH24:' ;
SELECT day_hour_used , num_used FROM pools_usage WHERE pool_name = 'ACT_CUST_ACT_VISIT' AND num_used >= 60 ORDER BY day_hour_used DESC ;
select * from dp_static_db_links where test_name = 'PRE_ORDER_DEVICES' order by test_name ;
-- check for memory leaks in real time.
SELECT s.module , a.sid , s.serial# , p.pid , s.username , s.osuser , s.program , s.logon_time , b.name, ROUND ( a.value / 1024 / 1024 ) AS mb , status , last_call_et
  FROM v$sesstat a
  JOIN v$statname b ON a.statistic# = b.statistic#
  JOIN v$session s ON s.sid = a.sid
  join v$process p on p.addr = s.paddr
 WHERE b.name = 'session pga memory'
 ORDER BY a.value DESC
;
-- hard parsing
SELECT /* Andrew Fraser */ SUBSTR ( sql_text , 1 , 50 ) , COUNT(*) , MIN(sql_id), MAX(sql_id) 
     , MIN(module) , MAX(module) , MIN(parsing_user_id) , MIN(parsing_schema_name) , MAX(parsing_schema_name)
     , MIN(sql_text) , MAX(sql_text)
  FROM v$sql
 GROUP BY SUBSTR ( sql_text , 1 , 50 )
HAVING COUNT(*) > 100
 ORDER BY COUNT(*) DESC
;
-- commit time
SELECT a.sid , a.event , a.total_waits , ROUND ( a.time_waited_micro / a.total_waits / 1000 ) AS milliseconds_per_commit
  FROM v$session_event a
  JOIN v$event_name b ON a.event = b.name
 WHERE b.name = 'log file sync'
   AND a.sid IN ( SELECT s.sid FROM v$session s WHERE s.username = 'OH_USER' )
 ORDER BY 1
;
-- Order Manaement oms
SELECT * FROM dba_editions ORDER BY 1 DESC ;
ALTER SESSION SET edition = DB_OMS_17_5 ;  -- substitute latest edition.
-- main bsbCusord
SELECT o.orderPayload.getClobVal() , o.*
  FROM oh.bsbCusOrd O,
       XMLTABLE(' for $r in /customerOrder return $r ' PASSING o.orderPayload COLUMNS 
            accountnumber VARCHAR2(50) PATH 'accountNumber'
       ) AS products
 WHERE XMLEXISTS ( 'customerOrder[accountNumber="623016211491"]' PASSING o.orderPayload )
;
-- in flight
SELECT * FROM om_owner.customerOrders where accountNumber = '623016211491' ;
-- relational
SELECT o.customerOrderId , o.orderpayload.getClobVal() , o.* FROM TABLE ( oh.orderRead.findOrdersByAccountNumber ( '623016211491' ) ) o ;
SELECT o.customerOrderId , o.orderpayload.getClobVal() , o.* FROM TABLE ( oh.orderRead.findOrderByCustomerOrderId ( 'oh2-c507395d-6d1e-4667-9672-ab0f9d34ba5a' ) ) o ;
-- audit = too slow to use like this, can only go with id on that table.
WITH q AS ( SELECT XMLTYPE ( orderpayload ) AS orderpayload FROM oh.order_wh_data ) ;
SELECT o.orderPayload.getClobVal() , o.*
  FROM oh.order_wh_data o,
       XMLTABLE(' for $r in /customerOrder return $r ' PASSING XMLTYPE ( o.orderpayload ) COLUMNS 
            accountnumber VARCHAR2(50) PATH 'accountNumber'
       ) AS products
 WHERE XMLEXISTS ( 'customerOrder[accountNumber="622985026500"]' PASSING o.orderPayload )
;
SELECT * FROM oh.order_wh_data ;