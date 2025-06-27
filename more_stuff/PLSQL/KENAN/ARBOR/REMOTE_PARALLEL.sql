create or replace procedure REMOTE_PARALLEL (i_tables IN VARCHAR2, i_action IN VARCHAR2) as

v_routine_Start date := sysdate; -- if run more than once on the same day we only want to look at the jobs run after the start of this procedure.
v_counter number :=0; -- set the max number of times to keep trying to check for the jobs finishing.
v_remain NUMBER := 6; -- The number of jobs still to finish.

cursor c_dbs is -- Used to build up the commands to create a job for each database.
 select distinct 'begin
   dbms_scheduler.create_job (
        job_name => ''P_'||rtrim(sd.ds_database)||'''
      , job_type => ''PLSQL_BLOCK''
      , job_action => ''begin remote_sync@'||rtrim(sd.ds_database)||'J ('''''||i_tables||''''', '''''||i_action||'''''); end;''
      , enabled => TRUE
      ) ; end;' cmd
 from server_definition sd
 where sd.ds_database like 'DCU%';

cursor c_job_results is
 select d.job_name, d.status, d.additional_info
 from DBA_SCHEDULER_JOB_RUN_DETAILS d
 where job_name like 'P_DCU0%'
 and not exists (select null from dba_scheduler_running_jobs r where r.job_name = d.job_name)
 and actual_start_date >= v_routine_Start;

begin
 for r_dbs in c_dbs loop
  dbms_output.put_line(r_dbs.cmd); -- create a job for each database to run the remote procedure.
  execute immediate(r_dbs.cmd);
 end loop;

 --The jobs should be running. Wait 50 minutes then start checking that they have finished successfully.
 sys.dbms_session.sleep(3000);

 --Now check the status of each job.
 --dbms_output.put_line('Check the status of each job');

 while v_remain > 0 and v_counter < 10 loop  -- Keep checking if some jobs have not finished and only check 10 times
  for r_job_results in c_job_results loop
   --dbms_output.put_line(r_job_results.job_name||' '||r_job_results.status );
   v_remain := v_remain -1; 
   v_counter := v_counter +1;
   if r_job_results.status = 'FAILED' then -- A failed job should return an error immediately.
    --dbms_output.put_line('About to fail the code');
    raise_application_error(-20019, 'REMOTE_SYNC on '||ltrim(r_job_results.job_name, 'J')||' failed with: '||r_job_results.additional_info);
   end if; 
  end loop;

  if v_remain !=0 then -- Still some jobs running so wait.
   --sys.dbms_session.sleep(60); -- for testing wait one minute before trying again to see if all jobs have finished.
   sys.dbms_session.sleep(600); -- for live, wait ten minutes before trying again to see if all jobs have finished.
   v_remain := 6; -- we need to go back around all the jobs to make sure all have finished.
  end if;

 end loop;
end REMOTE_PARALLEL;
/