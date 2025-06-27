col module form a16 heading 'Module'
col username form a16 heading 'Username'
col program form a16 heading 'Program'
col machine form a63 heading 'Machine'
col osuser form a7 heading 'OS User'
--alter session set nls_date_format = 'Dy DD-Mon-YYYY HH24:MI:SS' ;
spool open_cursors.txt
set pages 99 lines 980 head off feedback off trimspool on
SELECT 'Report run at : ' || TO_CHAR ( SYSDATE , 'Dy DD-Mon-YYYY HH24:MI:SS' ) AS report_run FROM DUAL ;
set head on
col parameter form a22 heading 'Parameter'
col param_limit form 99,999 heading 'Param Limit'
col session_statistic form a26 heading 'Session Statistic'
col largest_session_value form 99,999 heading 'Largest Session Value'
col pct_of_limit form a10 heading '% of Limit'
col open_cursors form 999,999 heading 'Open Cursors'
col sid heading 'Sid' form 999999
col serial# heading 'Serial#' form 999999
col sql_id heading 'Sql Id'
prompt
prompt 1) How close are we to the parameter limit?
SELECT p.name AS parameter
     , TO_NUMBER ( p.value ) AS param_limit
     , n.name AS session_statistic
     , MAX ( ss.value ) AS largest_session_value
     , '     ' || TO_CHAR ( 100 * MAX ( ss.value ) / p.value , '990' ) || '%' AS pct_of_limit
  FROM v$statname n
  JOIN v$sesstat ss ON ss.statistic# = n.statistic#
  JOIN v$parameter p ON p.name = CASE n.name WHEN 'opened cursors current' THEN 'open_cursors' WHEN 'session cursor cache count' THEN 'session_cached_cursors' END
 WHERE n.name IN ( 'opened cursors current' , 'session cursor cache count' )
   AND p.name IN ( 'open_cursors' , 'session_cached_cursors' )
 GROUP BY p.name , p.value , n.name
 ORDER BY p.name
;
prompt
prompt 2) Sessions with high number of open cursors
SELECT s.sid , s.serial# , s.username , a.value AS open_cursors , s.program , s.machine , s.osuser , s.module
  FROM v$sesstat a
  JOIN v$statname b ON a.statistic# = b.statistic#
  JOIN v$session s ON s.sid = a.sid
 WHERE b.name = 'opened cursors current'
   AND s.username IS NOT NULL
   AND a.value > 750 * 0.3
 ORDER BY a.value DESC
;
prompt
prompt 3) Sql statements with highest number of open cursors in total summed across all sessions
SELECT v.sql_id , v.open_cursors , v.username
  FROM (
        SELECT oc.sql_id , COUNT(*) AS open_cursors , oc.user_name AS username
          FROM v$open_cursor oc
         WHERE oc.user_name != 'SYS' 
         GROUP BY oc.sql_id , oc.sql_text , oc.user_name
        HAVING COUNT(*) > 20
         ORDER BY COUNT(*) DESC
       ) v
 WHERE ROWNUM < 21
;
prompt
prompt 4) Sql statements (if any) with more than 150 cursors open in a single session 
SELECT oc.sid , oc.sql_id , COUNT(*) AS open_cursors , oc.user_name AS username
  FROM v$open_cursor oc
 WHERE oc.sid IN (
       SELECT s.sid
         FROM v$sesstat a
         JOIN v$statname b ON a.statistic# = b.statistic#
         JOIN v$session s ON s.sid = a.sid
        WHERE b.name = 'opened cursors current' 
          AND s.username IS NOT NULL
       )
 GROUP BY oc.sid , oc.sql_id , oc.sql_text , oc.user_name
HAVING COUNT(*) > 150
 ORDER BY COUNT(*) DESC
;
prompt
prompt For info, sql id 22b6c7917kj2p is: SELECT * FROM TABLE ( cbsServices.dal_offerControl.customerJourneyType ( i_accountNumber => :1 ) )
prompt
prompt End of report. Produced by https://ppejenkins.bskyb.com/job/NFT DBA - Open Cursor Report/
spool off
col send_email form a60 heading 'Send Email?'
spool send_email.txt
SELECT 'send_email, largest open cursors in a session is ' || TO_CHAR ( MAX ( ss.value ) ) AS send_email
  FROM v$statname n
  JOIN v$sesstat ss ON ss.statistic# = n.statistic#
  JOIN v$parameter p ON p.name = CASE n.name WHEN 'opened cursors current' THEN 'open_cursors' END
 WHERE n.name = 'opened cursors current'
   AND p.name = 'open_cursors'
 GROUP BY p.value
HAVING MAX ( ss.value ) / p.value >= 0.5  -- 50% or above the parameter limit
;
spool off
exit ;