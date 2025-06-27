--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure P_RUNJOB
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."P_RUNJOB" ( i_jobName IN varchar2 , i_pack_name IN varchar2 ) IS
   l_load_type VARCHAR2(30) := 'PARALLEL' ;
   l_run_id NUMBER ;
   l_sql VARCHAR2(4000) ;
   g_parent_run_id NUMBER := 1 ;
BEGIN
   l_run_id := dataprov.dp_test_refresh_seq.NEXTVAL ;
   IF i_pack_name NOT IN ( 'dataprov.data_prep' , 'dataprov.data_prep_static' )
   THEN
      dbms_scheduler.create_job (
           job_name => i_jobName
         , job_type => 'STORED_PROCEDURE'
         , job_action => i_pack_name || '.' || i_jobName
         , enabled => TRUE
         ) ;
   ELSE
      l_sql := 'DBMS_SCHEDULER.CREATE_JOB (JOB_NAME => '''||'P'||i_jobName||''',JOB_TYPE => ''PLSQL_BLOCK'', JOB_ACTION => ''BEGIN '||
                                                          i_pack_name||'.'||i_jobName||'('||''''''||l_load_type||''''''||', '||l_run_id||
                                                          ', '||g_parent_run_id||') ; END;'' ,ENABLED => TRUE) ;';
      EXECUTE IMMEDIATE ( 'BEGIN ' || l_sql || ' END ;' ) ;
   END IF ;
   UPDATE dataprov.run_job_parallel_control SET start_time = SYSDATE , paused = NULL WHERE job_name = i_jobName ;
END p_runJob ;

/

  GRANT EXECUTE ON "DATAPROV"."P_RUNJOB" TO "BATCHPROCESS_USER";
