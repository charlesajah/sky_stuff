-- =============================================================
-- Name 			: UpdateStatsDetails.sql
-- Author 			: Rakel Fernandez
-- Date 			: 24/07/2024
-- Purpose  		: Update the details within the TEST_RESULT_MASTER table
--                    
-- Change History 	
-- --------------
-- 31/07/24 	RFA	: Changed the FLAG column to RETENTION
-- 13/01/25     RFA : Added a parameter to mark the testid as VALID for comparative purposes ( average values )
-- 28/04/25     RFA : Added a parameter to mark the testid with TESTMODE for comparative purposes
-- =============================================================

define TESTID='&1'
define TESTDESC='&2'
define RELEASE='&3'
define RETENTION='&4'
define VALID='&5'
define TESTMODE='&6'

set serverout on

spool UpdateStatDetails.log append

-- Call Procedure to update the TEST_ID description and other flags
exec HP_DIAG.REPORT_ADM.Do_UpdateStatsDetails(i_testId => '&TESTID', i_desc => '&TESTDESC', i_release => '&RELEASE', i_flag => '&RETENTION', i_valid => '&VALID', i_mode => '&TESTMODE'); 

spool off

