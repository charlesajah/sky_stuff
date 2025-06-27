set termout off
set pagesize 0
set spool on
set head off
set timing on

spool lws_pms_users.log

insert into hp_diag.lws_pms_conns
select sysdate, osuser, server, machine, status, logon_time, last_call_Et
  from v$session 
 where username = 'LWS_PMS_USER'
   and osuser != 'oracle';
  
commit;  

spool off
exit;