--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure P_DELETE_TEST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "HP_DIAG"."P_DELETE_TEST" (i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE) as

cursor c1 is
 select 'delete from ' || table_name || ' where test_id = '''||i_testId1||'''' cmd
 from user_tab_columns 
 where column_name = 'TEST_ID';
begin

for r1 in c1 loop
 execute immediate(r1.cmd);
 commit;
end loop;
end p_delete_test;

/
