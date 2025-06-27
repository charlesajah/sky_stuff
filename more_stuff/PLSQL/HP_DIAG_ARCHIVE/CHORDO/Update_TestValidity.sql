--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure UPDATE_TESTVALIDITY
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "HP_DIAG"."UPDATE_TESTVALIDITY" 
(
  l_TestId IN VARCHAR2 
, l_TestValidity IN VARCHAR2 
) AS 
BEGIN
    UPDATE TESTRESULTS
    SET TESTVALIDITY= l_TestValidity
    WHERE TESTID= l_TestId;
END Update_TestValidity;

/
