--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure GET_BASELINE_USING_TEST_STATUS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "HP_DIAG"."GET_BASELINE_USING_TEST_STATUS" 
(
  l_Status IN VARCHAR2 
, l_TestResultId OUT SYS_REFCURSOR
) AS 
BEGIN
    OPEN l_TestResultId FOR 
      SELECT TESTID 
        FROM TESTRESULTS 
        WHERE TESTVALIDITY LIKE l_Status
        ORDER BY TESTENDTIME DESC;
END get_baseline_using_test_status;

/
