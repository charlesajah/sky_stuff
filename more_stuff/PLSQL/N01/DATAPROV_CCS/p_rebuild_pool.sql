--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure P_REBUILD_POOL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."P_REBUILD_POOL" ( i_job_name in Varchar2)  IS
/*
|| p_rebuild_pool = rebuilds a single pool for the Engineers through Jenkins
||      
*/ 
   v_pack_name varchar2(500);
   v_pack_sql varchar2(500);
   a_job_name varchar2(100) := upper(i_job_name) ;
   l_job_run NUMBER ;
   l_run_id NUMBER ;
   v_sql VARCHAR2(4000) ;
   v_cust_sql VARCHAR2(4000) ;
   l_cnt number :=0;

BEGIN
-- Checking input jobs is present in the  run_job_parallel_control table
select count(1) into l_cnt from dataprov.run_job_parallel_control a where upper(a.job_name) = a_job_name ;

If l_cnt=1 then 

         SELECT 
               CASE a.stat_dyn WHEN 'D' THEN 'dataprov.data_prep.wrapper(v_test_name=> '''||a.job_name||''',v_load_type=> ''FULL'');' 
			    ELSE 'dataprov.data_prep_static.wrapper (v_test_name => ''' || a.job_name || ''', v_load_type => ''MASTER'');' end into  v_pack_name
           FROM dataprov.run_job_parallel_control a
          WHERE a.start_time IS Not NULL
           	and a.job_name = a_job_name 
            ORDER BY a.stat_dyn;

             -- submit the rebuild
     dbms_output.put_line(v_pack_name);		 
    v_sql := ' begin '|| v_pack_name || 'end;';
   -- execute immediate v_sql;
else 

select 'exec customers_pkg.''' || a_job_name || '''' into v_pack_sql from dual;

dbms_output.put_line('v_pack_sql :'||v_pack_sql);

end if;

        EXCEPTION
            WHEN no_data_found THEN
           
                DBMS_OUTPUT.PUT_LINE ( i_job_name || ' ' ||' Job name is not exist. please provide correct job name');
            WHEN OTHERS THEN
                dbms_output.put_line('An error was encountered for table BSBCOMMSRENDER: '
                                     || sqlcode
                                     || ' - '
                                     || sqlerrm);

 --COMMIT ;
END;

/

  GRANT EXECUTE ON "DATAPROV"."P_REBUILD_POOL" TO "BATCHPROCESS_USER";
