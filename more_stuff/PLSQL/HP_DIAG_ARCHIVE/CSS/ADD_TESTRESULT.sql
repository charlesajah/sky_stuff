--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure ADD_TESTRESULT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "HP_DIAG"."ADD_TESTRESULT" 
(
l_TestId IN VARCHAR2, 
l_TestName IN VARCHAR2, 
l_TestStartTime IN VARCHAR2, 
l_TestEndTime IN VARCHAR2, 
l_Environment IN VARCHAR2, 
l_TargetComponent IN VARCHAR2, 
l_ReasonForTest IN VARCHAR2,
l_Tags IN VARCHAR2,
l_Batch IN VARCHAR2,
l_ReleaseVersion IN VARCHAR2, 
l_MetadataName IN VARCHAR2
) AS
BEGIN
	INSERT INTO TESTRESULTS (
        TESTID, 
        TESTNAME, 
        TESTSTARTTIME, 
        TESTENDTIME, 
        ENVIRONMENT, 
        TARGETCOMPONENT, 
        REASONFORTEST, 
        TAGS, 
        BATCH,
        RELEASEVERSION,
        METADATANAME)
	VALUES (l_TestId, l_TestName, TO_DATE(l_TestStartTime, 'YYYY-MM-DD HH24:MI:SS'), TO_DATE(l_TestEndTime, 'YYYY-MM-DD HH24:MI:SS'), l_Environment, l_TargetComponent, l_ReasonForTest, l_Tags, l_Batch, l_ReleaseVersion, l_MetadataName);
END Add_Testresult;

/
