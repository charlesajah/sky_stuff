--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure P_REBUILD_POOL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."P_REBUILD_POOL" (
    i_job_name IN VARCHAR2
) IS
/*
|| p_rebuild_pool = rebuilds a single pool for the Engineers through Jenkins
||      
*/

    v_pack_name   VARCHAR2(500);
   --i_job_name varchar2(200)  ;
    l_job_run     NUMBER;
    l_run_id      NUMBER;
    v_sql         VARCHAR2(4000);
    l_cnt         NUMBER := 0;
    l_cnt1        NUMBER := 0;
BEGIN
-- Checking input jobs is present in the  run_job_parallel_control table
    SELECT
        COUNT(1)
    INTO l_cnt
    FROM
        dataprov.run_job_parallel_control a
    WHERE
        upper(a.job_name) = upper(i_job_name);

    IF l_cnt = 1 THEN
        SELECT
            CASE a.stat_dyn
                WHEN 'D' THEN
                    'dataprov.data_prep.wrapper(v_test_name=> '''
                    || a.job_name
                    || ''',v_load_type=> ''FULL'');'
                ELSE
                    'dataprov.data_prep_static.wrapper (v_test_name => '''
                    || a.job_name
                    || ''', v_load_type => ''MASTER'');'
            END
        INTO v_pack_name
        FROM
            dataprov.run_job_parallel_control a
        WHERE
            a.start_time IS NOT NULL
            AND a.job_name = i_job_name
        ORDER BY
            a.stat_dyn;

             -- submit the rebuild

        dbms_output.put_line(v_pack_name);
        v_sql := ' begin '
                 || v_pack_name
                 || 'end;';
   -- execute immediate v_sql;
        dbms_output.put_line(v_sql);
    ELSE   
--Checking input jobs is present is part of customer build
        SELECT
            COUNT(1)
        INTO l_cnt1
        FROM
            all_procedures
        WHERE
            upper(object_name) = 'CUSTOMERS_PKG'
            AND upper(procedure_name) = upper(i_job_name);

        IF l_cnt1 = 1 THEN 
 --exec 
            v_sql := ' begin CUSTOMERS_PKG.'
                     || upper(i_job_name)
                     || '; end;';
   -- execute immediate v_sql;
            dbms_output.put_line(v_sql);
        ELSE
            dbms_output.put_line(i_job_name
                                 || ' '
                                 || ' Job name is not exist in run_job_parallel_control table and part of customer rebuild. please provide correct job name');
        END IF;

    END IF;

EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('An error was encountered for proceduere p_rebuild_pool '
                             || sqlcode
                             || ' - '
                             || sqlerrm);
END;

/

  GRANT EXECUTE ON "DATAPROV"."P_REBUILD_POOL" TO "BATCHPROCESS_USER";
