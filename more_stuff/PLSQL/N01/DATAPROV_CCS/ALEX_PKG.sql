CREATE OR REPLACE PACKAGE          ALEX_PKG IS
   PROCEDURE main ;
END ALEX_PKG ;
/


CREATE OR REPLACE PACKAGE BODY ALEX_PKG IS
	g_proc_date DATE := SYSDATE ;

	PROCEDURE p_runJob( i_jobName in varchar2, i_pack_name in varchar2) IS
	   l_load_type VARCHAR2(30) := 'PARALLEL' ;
	   l_run_id NUMBER ;
	   l_sql VARCHAR2(4000) ;
	begin
	   l_run_id := dataprov.dp_test_refresh_seq.NEXTVAL ;
/*	   l_sql := 'DBMS_SCHEDULER.CREATE_JOB (JOB_NAME => '''||'P'||i_job_name||''',JOB_TYPE => ''PLSQL_BLOCK'', JOB_ACTION => ''BEGIN '||
															  i_pack_name||'.'||i_job_name||'('||''''''||l_load_type||''''''||', '||l_run_id||
															  ', '||g_parent_run_id||') ; END;'' ,ENABLED => TRUE) ;';
	   EXECUTE IMMEDIATE ( 'BEGIN ' || l_sql || ' END ;' ) ;*/
       insert into alex_scheduler_job_run_details (job_name, log_date, actual_start_date) values ('P' || i_jobName, sysdate, sysdate);       
	   UPDATE dataprov.alex_run_job_parallel_control SET start_time = SYSDATE, paused = null  WHERE job_name = i_jobName ;
	   DBMS_LOCK.SLEEP ( seconds => 2 ) ;
	end ;

  PROCEDURE main IS
     l_remain NUMBER ;
	 l_run number;
  BEGIN
     SELECT COUNT(*) INTO l_remain FROM dataprov.alex_run_job_parallel_control WHERE start_time IS NULL AND stat_dyn in ('D','S') ;
	 dbms_output.put_line('l_remain = ' || l_remain);
     WHILE l_remain > 0
     LOOP
       FOR i IN (
          SELECT a.job_name, CASE a.stat_dyn WHEN 'D' THEN 'dataprov.data_prep' ELSE 'dataprov.data_prep_static' END AS pack_name, a.priority, a.job_dependence
            FROM dataprov.alex_run_job_parallel_control a
           WHERE a.start_time IS NULL
             AND a.stat_dyn in ('D','S')
          ORDER BY a.priority
       )
       LOOP
		  if i.job_dependence is null then
	         DBMS_LOCK.SLEEP ( seconds => 2 ) ;
		     DBMS_OUTPUT.PUT_LINE ( i.job_name || ' : ' || i.pack_name || ' : ' || i.priority || ' : ' || i.job_dependence) ;
			 p_runJob( i.job_name, i.pack_name);
		  else
		     SELECT COUNT(*) into l_run from alex_scheduler_job_run_details WHERE job_name = 'P' || i.job_dependence and log_date > g_proc_date;
		     if l_run > 0 then
	            DBMS_LOCK.SLEEP ( seconds => 2 ) ;
			    DBMS_OUTPUT.PUT_LINE ( i.job_name || ' : ' || i.pack_name || ' : ' || i.priority || ' : ' || i.job_dependence) ;
			    p_runJob( i.job_name, i.pack_name);
			 else
			    update dataprov.alex_run_job_parallel_control SET paused = 'Y' WHERE job_name = i.job_name ;
			 end if ;
		  end if;
		  commit ;

		  -- check for paused jobs
          FOR x IN (
             SELECT a.job_name, CASE a.stat_dyn WHEN 'D' THEN 'dataprov.data_prep' ELSE 'dataprov.data_prep_static' END AS pack_name, a.priority, a.job_dependence
               FROM dataprov.alex_run_job_parallel_control a
              WHERE a.start_time IS NULL
                AND a.stat_dyn in ('D','S')
				and PAUSED is not null
             ORDER BY a.priority
          )
		  loop
		     SELECT COUNT(*) into l_run from alex_scheduler_job_run_details WHERE job_name = 'P' || x.job_dependence and log_date > g_proc_date;
		     if l_run > 0 then
	            DBMS_LOCK.SLEEP ( seconds => 2 ) ;
			    DBMS_OUTPUT.PUT_LINE ( x.job_name || ' : ' || x.pack_name || ' : ' || x.priority || ' : ' || x.job_dependence) ;
			    p_runJob( x.job_name, x.pack_name);
			 end if;
		  end loop;
       END LOOP  ;
       SELECT COUNT(*) INTO l_remain FROM dataprov.alex_run_job_parallel_control WHERE start_time is NULL AND stat_dyn in ('D','S');
	   commit ;
	 end loop;
  END main ;

end;
/
