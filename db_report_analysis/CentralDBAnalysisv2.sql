-- =============================================================
-- Name 			: CentralDBAnalysis.sql
-- Author 			: Rakel Fernandez
-- Date 			: 30/10/2023
-- Purpose  		: Makes calls to retrieve Anaylsis information 
--                    
-- Change History 	:
-- --------------
-- 24/07/24 	RFA	: Modifications made for the new Central Repository solution
-- 14/08/24     RFA : Added a parameter for the SQL related functions to retrieve only APP related information
-- 30/08/2024   RFA : Brings together all the PL/SQL calls to generate the report to one file
--                    This change contains the new way to generate the dynamic CHARTS for the database comparisons 
--                    It combines the PL/SQL calls for ALL the database ( does not deal with single database access )
--                    We have added the DB Summary section to the summary page
-- 16/10/24     RFA : report_data.Get_PGA_growth only process one testID at a time ( no comparison )
-- 
--
-- =============================================================

-- Set the variable that will be pass to all calls  ( Test ID is in the format DDMONYY-HH24MI_DDMONYY-HH24MI )
define TEST_ID1='&1'
define TEST_ID2='&2'
define TEST_DESC='&3'

var l_test_id1 varchar2(40);
var l_test_id2 varchar2(40);
var l_test_desc varchar2(100);


-- Initiate the dbAnalysis page with the header Date/Time
SET trimspool on pagesize 0 head off feed off lines 1000 verify off serverout on

-- Initialise all files so, if anything goes wrong, the Confluence pages still get generated with the RESTRICTED
spool CentralDBSummary.txt
spool off
spool CentralDBAnalysis.txt 
spool off
spool CentralChart.txt
spool off
spool CentralSQLComp.txt 
spool off
spool CentralLoadComp.txt 
spool off
spool CentralAllSQLComp.txt 
spool off
--spool CentralSqlTrend.txt 
--spool off

exec :l_test_id1 := '&&TEST_ID1' ;
exec :l_test_id2 := '&&TEST_ID2' ;
exec :l_test_desc := '&&TEST_DESC' ;


-- Initialise the file for the summary page within Confluence
-- DB SUMMARY
-- Page : DB Analysis Summary -
spool CentralDBSummary.txt append

-- It displays the Test Comparison summary information in list format 
SELECT * FROM TABLE ( report_data_charles.Get_test_info ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 ) ) ;

-- Get brief list of SQL Comparison
SELECT * FROM TABLE ( report_comp_charles.Get_top25_comparison_summary ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2, i_desc => :l_test_desc, i_link => 'Y' , i_mode => 'APP' ) ) ;

-- Get summary for the Top SQL Load Information
select * from TABLE ( report_comp_charles.Get_load_comparison_summary ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 ) ) ;

-- Generate DB summary 
select * from table ( report_data_charles.Get_DB_Summary ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2, i_ratio => 5 , i_filter => 'RED' , i_summary => 'YES' ));

spool off




-- DB ANALYSIS
-- Page : 1. Database Analysis - 
spool CentralDBAnalysis.txt append

-- Test INFO
SELECT * FROM TABLE ( report_data_charles.Get_test_info ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 ) ) ;

-- "Top 25 SQL Comparison Across Database by Elapsed Time" is outputted by below piplined function line:
SELECT * FROM TABLE ( report_comp_charles.Get_top25_comparison_summary ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 , i_mode => 'APP' ) ) ;
SELECT * FROM TABLE ( report_comp_charles.Get_top25_comparison_full ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2, i_title => 'N', i_mode => 'APP' ) ) ;

-- "Top 25 DB Metrics
-- ( Disable at the moment as it doesn't add any value )
--select 'h3. Database Metrics' from dual;
--SELECT * FROM TABLE ( report_comp.Get_top25_metrics ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 ) ) ;

-- Get the Billing rate 
SELECT * FROM TABLE ( report_data_charles.Get_billing_rate ( i_testId => :l_test_id1 )) ;

spool off

--SQL Performance Trend
-- Page : 2 SQL Performance Trend Page
spool CentralSqlTrend.txt append
select * from table(HP_DIAG.PERF_METRICS_CHARLES.get_sql_trend(i_testid1 => :l_test_id1, i_testid2 => :l_test_id2));
spool off


-- ADDITIONAL INFO 
-- Page : 2. Additional Info - 
spool CentralChart.txt append

-- Test INFO
SELECT * FROM TABLE ( report_data_charles.Get_test_info ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 ) ) ;

-- Displays the information on the CACHE (sqlId)
select 'h3. Cache Information (If applicable)' from dual;
SELECT * FROM TABLE ( report_data_charles.Get_Cache_Info ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 ) ) ;

select 'h3. Chart Comparisons' from dual;
-- Populates the Chart Comparisons between Releases
SELECT * FROM TABLE ( report_comp_charles.Get_chart_comparison ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 ) ) ;

select 'h3. SAL Basket Sizes' from dual;
-- SAL Basket Sizes for each database ( H3 level tittle in PL/SQL as it adds the Object in question dynamically
SELECT * FROM TABLE ( report_data_charles.Get_sal_basket_sizes ( i_testId1 => :l_test_id1 ) ) ;

select 'h3. Further Information' from dual;
-- Add the Row Lock Waits Information
SELECT * FROM TABLE ( report_comp_charles.Get_rlw_comparison ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2  )) ;

-- PGA report - this section only output if at least one database has pga growth of 50%+ or 2gb+
SELECT * FROM TABLE ( report_data_charles.Get_PGA_growth ( i_testId => :l_test_id1 ) ) ;
SELECT * FROM TABLE ( report_data_charles.Get_PGA_growth ( i_testId => :l_test_id2 ) ) ;

-- Archived Redo Log report
--select 'h3. Archived Redo Log' from dual;
SELECT * FROM TABLE ( report_data_charles.Get_RedoLogs_Usage ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 )) ;

-- Gets the DB Chart comparisons from the TEST_RESULT_METRICS_DETAIL table
-- RFA 14/11/24 -- Disabling this part of the reports as it's not currently working in Confluence !
--SELECT * FROM TABLE ( report_comp.Get_DB_Charts ( i_testid1 => :l_test_id1, i_testid2 => :l_test_id2 ));

spool off


-- Page : 3. Database Activity -
-- This page gets generated via the CentralDBActGraph.sql script. 
-- It's called per database basis as it needs to export the data one at a time



-- SQL ANALYSIS 
-- Page : 4. Sql Analysis - 
spool CentralSQLComp.txt append
SELECT SYSDATE FROM DUAL;

-- Test INFO
SELECT * FROM TABLE ( report_data_charles.Get_test_info ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 ));

select 'h3. Top 25 SQL Comparison Across Database by Previous Releases' from dual;
-- TOP 25 SQL comparisons ( Elapsed Time )
--select * from table( report_comp.Get_top25_long_comp ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 , i_mode => 'APP' ));
-- This is newer and gives the exactsme information ( there's a lot of duplication! )
SELECT * FROM TABLE ( report_comp_charles.Get_top25_comparison_full ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2, i_title => 'N', i_mode => 'APP' ) ) ;

-- TOP 25 SQL in Single Display Comparison 
select * from table( report_comp_charles.Get_top25_detail ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 , i_mode => 'APP' ));

spool off


-- LOAD COMPARISON 
-- Page : 5. Load Comparison - 
spool CentralLoadComp.txt append

-- Test INFO
SELECT * FROM TABLE ( report_data_charles.Get_test_info ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 ) ) ;

-- Show the oveerall load put of individual databases graphically (chart) against multiple previous releases
select 'h3. Load Comparison Across Databases by Previous Releases' from dual;
select * from table( report_comp_charles.Get_load_comparison_summary ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 ));
select * from table( report_comp_charles.Get_load_comparison_full ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2, i_title => 'N' ));

spool off


-- ALL SQL COMAPRISON 
-- Page : 6. All SQL Comparison -
spool CentralAllSQLComp.txt append

SELECT SYSDATE FROM DUAL;

-- Test INFO
SELECT * FROM TABLE ( report_data_charles.Get_test_info ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 ) ) ;

-- Details SQL comparison for previous text per database
select 'h3. Detail SQL Comparison Per Database' from dual;
select * from table( report_comp_charles.Get_all_db_top25_comparison ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 , i_mode => 'APP' ));

-- Presents the SQLID changes for the sqm SQL text
select 'h3. SQLID mutations' from dual;
select * from table( report_comp_charles.Get_SQLID_byText ( i_testId1 => :l_test_id1, i_testId2 => :l_test_id2 , i_mode => 'APP' ));

spool off


