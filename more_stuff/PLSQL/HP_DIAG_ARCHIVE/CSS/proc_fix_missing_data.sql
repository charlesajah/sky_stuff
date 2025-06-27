--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure PROC_FIX_MISSING_DATA
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "HP_DIAG"."PROC_FIX_MISSING_DATA" (i_testid in varchar2) as
  l_cnt          integer := 0;
  l_testdesc     varchar2(255) := '';
  TYPE           g_tvc2 IS TABLE OF VARCHAR2(4000) ;
  l_metric_name  g_tvc2 ;
  l_idx          VARCHAR2(100) ;
begin
  null;
  -- check testid is valid
  select count(*)
    into l_cnt
    from hp_diag.test_result_master
   where test_id = upper(i_testid);

  if l_cnt = 1 then
    select test_description
      into l_testdesc
      from hp_diag.test_result_master
     where test_id = upper(i_testid);
    -- valid test
    FOR r_dbs IN (select db_name
                    from hp_diag.test_result_dbs
                  minus
                  select distinct database_name
                    from TEST_RESULT_WAIT_CLASS t
                   where t.test_id = upper(i_testid))
    LOOP
      insert into hp_diag.TEST_RESULT_WAIT_CLASS
      values (upper(i_testid), l_testdesc, r_dbs.db_name, 'Other', null, null, null);
    END LOOP;
    commit;

    select test_description
      into l_testdesc
      from hp_diag.test_result_master
     where test_id = upper(i_testid);
    -- valid test
    FOR r_dbs IN (select db_name
                    from hp_diag.test_result_dbs
                  minus
                  select distinct database_name
                    from test_result_db_stats t
                   where t.test_id = upper(i_testid))
    LOOP
      insert into hp_diag.test_result_db_stats(TEST_ID, TEST_DESCRIPTION, DATABASE_NAME, ELAPSED_TIME_SECS, DB_TIME_SECS, DB_CPU_SECS, TOTAL_EXECS)
      values (upper(i_testid), l_testdesc, r_dbs.db_name, 0, 0, 0, 0);
    END LOOP;
    commit;

    l_metric_name := NEW g_tvc2 ( 'Host CPU Utilization (%)' , 'Average Active Sessions' , 'Total PGA Allocated' , 'Current OS Load' , 'SQL Service Response Time' , 'Total Table Scans Per Sec', 'Database Wait Time Ratio', 'User Commits Per Sec', 'Physical Read Total Bytes Per Sec' ) ;
    l_idx := l_metric_name.FIRST ;
    WHILE l_idx IS NOT NULL
    LOOP
      FOR r_dbs IN (select db_name from hp_diag.test_result_dbs order by 1)
      loop
        MERGE INTO hp_diag.test_result_metrics trm
        USING (SELECT upper(i_testid) as testid, r_dbs.db_name as db_name, l_metric_name(l_idx) as metname from dual) T1
           ON (    T1.testid  = trm.test_id
               and t1.db_name = trm.database_name
               and t1.metname = trm.metric_name)
        WHEN NOT MATCHED THEN
          insert (test_id, test_description, database_name, metric_name, average, begin_average, end_average, min_average, max_average)
          values (upper(i_testid), l_testdesc, r_dbs.db_name, l_metric_name(l_idx), 0, 0, 0, 0, 0);

      end loop;
      l_idx := l_metric_name.NEXT ( l_idx ) ;
    END LOOP;
    commit;
  else
    dbms_output.put_line('Not a valid test');
  end if;
end proc_fix_missing_data;

/
