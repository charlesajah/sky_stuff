-- =============================================================
-- Name 			: get_AnalysisReports.sql
-- Author 			: Rakel Fernandez
-- Date 			: 01/05/2024
-- Purpose  		: Retrieves the Anaylsis information for the given dates
-- =============================================================

-- Set the variable that will be pass to all calls  ( Test ID is in the format DDMONYY-HH24MI_DDMONYY-HH24MI )
define TEST_ID1='&1'
define TEST_ID2='&2'
define TEST_DESC='&3'

-- Initiate the dbAnalysis page with the header Date/Time
SET trimspool on pagesize 0 head off feed off lines 1000 verify off serverout on


-- Initialise the file for the summary page within Confluence
-- DB SUMMARY
spool ShowDBAnalysisSummary.txt

-- It displays the Test Comparison summary information in list format 
-- This call will generated the Index for each page
SELECT * FROM TABLE ( REPORT_DATA.Get_Test_Info ( i_testId1 => '&&TEST_ID1' , i_testId2 => '&&TEST_ID2' ) ) ;

-- Get Summary list of SQL Comparison
SELECT * FROM TABLE ( REPORT_COMP.Get_top25_comparison_summary ( i_testId1 => '&&TEST_ID1', i_testId2 => '&&TEST_ID2', i_desc => '&&TEST_DESC', i_title => 'Y' , i_link => 'N' ) ) ;

-- Get summary for the Top SQL Load Information
SELECT * FROM TABLE ( REPORT_COMP.Get_load_comparison_colour ( i_testId1 => '&&TEST_ID1', i_testId2 => '&&TEST_ID2' ) ) ;

-- Get the billing info 
SELECT * FROM TABLE ( REPORT_COMP.Get_billing_rate ( i_testId1 => '&&TEST_ID1' ) ) ;
SELECT * FROM TABLE ( REPORT_COMP.Get_billing_rate ( i_testId1 => '&&TEST_ID2' ) ) ;

spool off


-- Gets the full details

spool ShowDBAnalysisFull.txt

-- It displays the Test Comparison summary information in list format 
-- This call will generated the Index for each page
SELECT * FROM TABLE ( REPORT_DATA.Get_Test_Info ( i_testId1 => '&&TEST_ID1' , i_testId2 => '&&TEST_ID2' ) ) ;

-- Get Summary list of SQL Comparison
SELECT * FROM TABLE ( REPORT_COMP.Get_top25_comparison_summary ( i_testId1 => '&&TEST_ID1', i_testId2 => '&&TEST_ID2', i_desc => '&&TEST_DESC', i_title => 'Y' , i_link => 'Y' ) ) ;



spool off

-- Gets the DB Charts 
spool ShowDBCharts.txt

-- It displays the Test Comparison summary information in list format 
-- This call will generated the Index for each page
SELECT * FROM TABLE ( REPORT_DATA.Get_Test_Info ( i_testId1 => '&&TEST_ID1' , i_testId2 => '&&TEST_ID2' ) ) ;

-- Generated the Charts for specific METERIC values ( TEST_RESULTS_METRIC_DETAIL )
-- The second TESTID can be set to NULL and it will only display the chats for the current test
SELECT * FROM TABLE ( REPORT_DATA.Get_db_charts ( i_testId1 => '&&TEST_ID1' , i_testId2 => '&&TEST_ID2' ) ) ;



spool off





/*

-- DB ANALYSIS Page
spool dbAnalysis_v2.txt

-- Test INFO
SELECT * FROM TABLE ( hp_diag.report_data.Get_test_info ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 ) ) ;


-- "Top 25 SQL Comparison Across Database by Elapsed Time" is outputted by below piplined function line:
select 'h3. SQL Comparison' from dual;
SELECT * FROM TABLE ( hp_diag.report_comp.Get_top25_comparison ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 ) ) ;


-- "Top 25 DB Metrics
-- ( Disable at the moment as it doesn't add any value )
--select 'h3. Database Metrics' from dual;
--SELECT * FROM TABLE ( hp_diag.report_comp.Get_top25_metrics ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 ) ) ;

spool off


-- ADDITIONAL INFO Page
spool chart_v2.txt

-- Test INFO
SELECT * FROM TABLE ( hp_diag.report_data.Get_test_info ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 ) ) ;

-- Displays the information on the CACHE (sqlId)
select 'h3. Cache Information (If applicable)' from dual;
SELECT * FROM TABLE ( hp_diag.report_data.Get_Cache_Info ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 ) ) ;


spool off


*/
