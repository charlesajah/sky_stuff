--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure PROC_FIX_MISSING_FILE_DATA
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "HP_DIAG"."PROC_FIX_MISSING_FILE_DATA" as 
  l_testdesc     test_result_iostat_tablespace.test_description%type := '';
  l_tspace       test_result_iostat_tablespace.tablespace%type       := '';
  l_test_id      test_result_iostat_tablespace.test_id%type          := '';
begin
  FOR r_dbs IN (select distinct database_name from test_result_iostat_tablespace order by 1)
  LOOP
    for r_new_rows in ( SELECT t.test_id, v.tablespace, v.filename, v.test_description
                          FROM (SELECT DISTINCT tablespace, filename, test_description
                                  FROM test_result_iostat_fileio 
                                 where database_name = r_dbs.database_name) v
                          LEFT OUTER JOIN test_result_iostat_fileio t
                          PARTITION BY (t.test_id) ON (v.filename = t.filename and v.tablespace = t.tablespace)
                         WHERE t.tablespace IS NULL)
    loop
      insert into test_result_iostat_fileio
      values (r_new_rows.test_id, r_new_rows.test_description, r_dbs.database_name, 
              r_new_rows.TABLESPACE, r_new_rows.FILENAME, 0, 0, 0, 0, 0, 0, 0);
    end loop;
    commit;
  END LOOP;    

end proc_fix_missing_file_data;

/
