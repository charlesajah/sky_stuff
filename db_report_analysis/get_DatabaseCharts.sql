-- =============================================================
-- Name 			: get_DatabaseCharts.sql
-- Author 			: Rakel Fernandez
-- Date 			: 03/05/2024
-- Purpose  		: Retrieves the Database Charts 
-- =============================================================

-- Set the variable that will be pass to all calls  ( Test ID is in the format DDMONYY-HH24MI_DDMONYY-HH24MI )
define TEST_ID1='&1'
define TEST_ID2='&2'

-- Initiate the dbAnalysis page with the header Date/Time
SET trimspool on pagesize 0 head off feed off lines 1000 verify off serverout on


-- Gets the DB Charts 
spool ShowDatabaseCharts.txt

SELECT * FROM TABLE ( REPORT_DATA.Get_db_charts ( i_testId1 => '&&TEST_ID1' , i_testId2 => '&&TEST_ID2' ) ) ;

spool off

