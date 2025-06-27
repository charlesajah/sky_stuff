--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure GET_POS_DATA
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."GET_POS_DATA" (op_account_number out varchar2)
is
  v_account_number VARCHAR2(12);
BEGIN
  select ACCOUNTNUMBER
  into v_account_number
  from
  ( select ACCOUNTNUMBER from FSP_POS_MIGRATIONS ORDER BY dbms_random.value )
  where rownum = 1;
  op_account_number := v_account_number;
END GET_POS_DATA;

/

  GRANT EXECUTE ON "DATAPROV"."GET_POS_DATA" TO "BATCHPROCESS_USER";
