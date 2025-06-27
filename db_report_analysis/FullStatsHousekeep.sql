-- =============================================================
-- Name 			: FullStatsHousekeep.sql
-- Author 			: Rakel Fernandez
-- Date 			: 30/07/2024
-- Purpose  		: It deletes all the Repository data that's older than 3 years.
--                    
-- Change History 	
-- --------------
-- 08/11/2024	RFA : Change retention to 550 ( 18 months )
--
-- =============================================================


set pages 0
set newpage 0
set lines 999
SET UNDERLINE off
set head off
SET RECSEP off
set feed off
set echo off
SET COLSEP '|'
set trims on
set serverout on

spool FullStatsHousekeep.log 

-- Call Procedure to update the TEST_ID description and other flags
exec REPORT_ADM.Do_Stats_Full_HouseKeeping ( i_retain => 550 ); 

spool off

set head on
SET UNDERLINE on
set feed on
set pages 1000
set verify on
