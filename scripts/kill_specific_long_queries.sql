--grant select on SYS.GV_$SESSION to MDM_INFM_SMDS;


create table MDM_INFM_SMDS.killed_sessions (
    inst_id varchar2(20),
    sid varchar2(20),
    serial# varchar2(20),
    sql_id varchar2(20),
    service_name varchar2(20),
    err_code varchar2(20),
    date_added timestamp default SYSTIMESTAMP );

create or replace package SMDS_LONG_SESSIONS AS
    PROCEDURE kill_specific_long_queries;
    PROCEDURE purge_table;
end SMDS_LONG_SESSIONS;
/

create or replace package body SMDS_LONG_SESSIONS AS
    err_code varchar2(20);
PROCEDURE kill_specific_long_queries AS
BEGIN
      FOR x IN (select inst_id,sid,serial#,sql_id,service_name from gv$session where inst_id=2  and sql_id in ('5a63cnnf75dcd', 'dsxq0gnr75318','fsn18v5ckvhzv') and status='ACTIVE' and round(LAST_CALL_ET/60,2) > 1 )
        LOOP
            BEGIN
                EXECUTE IMMEDIATE 'ALTER SYSTEM KILL SESSION '''||x.SID||','||x.SERIAL#||',@'||x.INST_ID||''' IMMEDIATE';
                Exception when others then
                err_code := SQLCODE;
                --print the sesion details and the sql_id and the error code
                -- ORA-00031: session marked for kill is output when we kill a session
                --so we need to see if error 31 is output which means the session has been killed successfully
                DBMS_OUTPUT.put_line('Session to be killed '||x.SID||','||x.SERIAL#||','||x.SQL_ID||','||err_code||'.');
                insert into MDM_INFM_SMDS.killed_sessions (inst_id,sid,serial#,sql_id,service_name,err_code) values(x.inst_id,x.sid,x.serial#,x.sql_id,x.service_name,err_code);
                commit;
            END;
        END LOOP;
END ;
PROCEDURE purge_table AS
BEGIN
    --delete records older than 60 days
    EXECUTE IMMEDIATE 'DELETE FROM MDM_INFM_SMDS.killed_sessions WHERE date_added <  sysdate - 60';
    COMMIT;
exception when others then 
    err_code := SQLCODE;
    DBMS_OUTPUT.put_line('Unable TO delete older records because of the following ORA Error '||err_code);
END;
END SMDS_LONG_SESSIONS;
/

--create scheduled job to run every 5 minutes to kill sessions running over 1 minute
BEGIN
    DBMS_SCHEDULER.CREATE_JOB (
            job_name => 'KILL_SMDS_SESSIONS',
            job_type => 'PLSQL_BLOCK',
            job_action => 'BEGIN SMDS_LONG_SESSIONS.KILL_SPECIFIC_LONG_QUERIES; END;',
            start_date => SYSDATE,
            repeat_interval => 'FREQ=MINUTELY;INTERVAL=5',
            end_date => NULL,
            enabled => TRUE,
            auto_drop => FALSE,
            comments => 'This is to kill sessions running the SQL_IDs 5a63cnnf75dcd, dsxq0gnr75318, fsn18v5ckvhzv for more than 1 minute');     
END;
/

--create scheduled job to run once every month to purge records older than 60 days
BEGIN
    DBMS_SCHEDULER.CREATE_JOB (
            job_name => 'PURGE_REC',
            job_type => 'PLSQL_BLOCK',
            job_action => 'BEGIN SMDS_LONG_SESSIONS.PURGE_TABLE; END;',
            start_date => SYSDATE,
            repeat_interval => 'FREQ=MONTHLY;BYDAY=SUN;BYHOUR=22;BYMINUTE=0;BYSECOND=0',
            end_date => NULL,
            enabled => TRUE,
            auto_drop => FALSE,
            comments => 'This is to delete records older than 60 days');     
END;
/

--rollback change/script
drop package SMDS_LONG_SESSIONS;
drop table MDM_INFM_SMDS.killed_sessions;
exec DBMS_SCHEDULER.drop_job (job_name => 'KILL_SMDS_SESSIONS');
exec DBMS_SCHEDULER.drop_job (job_name => 'PURGE_REC');