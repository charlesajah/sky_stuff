set termout off
set pagesize 0
set spool on
set head off
set timing on

spool ulm_mint_sessions.log

insert into hp_diag.mint_sessions
select sysdate, username, machine, status, count(*) session_count, min(logon_time) earliest_logon, 
       max(logon_time) latest_logon, min(last_call_et) shortest_idle_time, max(last_call_et) longest_idle_time
  from v$session where username like 'MINT%' group by username, machine, status ;
  
commit;  

spool off
exit;