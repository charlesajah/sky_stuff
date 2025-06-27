-- ==========================================================================================================================
-- Name    : nft_space_usage_v2.sql
-- Author  : Rakel Fernandez
-- Date    : 06-Mar-2024
-- Purpose : This script calls the PL_SQL PROCEDURE that will retrieve the data to be reported upon
--         : It only takes one parameter ( N01 / N02 or ALL ) 
-- Changes :
-- 03/03/25     RFA64   : The procedure has changed internally to handle errors better when DB links are not working and to 
--                        exclude the "PRD" environment
-- ==========================================================================================================================
-- Get the variables on the call
-- Environment to be collected for N01/N02
define ENV='&1'

SET trimspool on pagesize 0 head off feed on lines 1000 serverout on

-- Call PROCEDURE
-- exec REPORT_SPACE.Do_Gather_Space_Mgmt_Data (i_env => '&&ENV') ;
exec HP_DIAG.REPORT_SPACE.Do_Gather_Space_Mgmt_Data (i_env => '&&ENV') ;



