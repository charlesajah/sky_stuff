define TEST1_ID='&1'
define TEST2_ID='&2'

var B1 Varchar2(40)
var B2 Varchar2(40)
begin
  :b1:='&1';
  :b2:='&2';
end;
/

set pages 0
set define off
set newpage 0
set lines 999
SET UNDERLINE off
set head off
SET RECSEP off
set feed off
set echo off
SET COLSEP '|'
set trims on

spool dbStatusSummary.txt append

-- ############  DB Summary  ###############

-- It displays the Test Comparison summary information in list format 
SELECT * FROM TABLE ( REPORT_DATA.Get_test_info ( i_testId1 => :B1, i_testId2 => :B2 ) ) ;

-- Gather the data into the TEMP table
exec REPORT_DATA.Do_AnalyticalData(i_testId1 => :B1, i_testId2 => :B2, i_ratio => 5, i_except => 'APP' ); 

-- Generate DB summary 
--select * from table(REPORT_DATA.Get_DB_Summary(i_testId1 => :B1, i_testId2 => :B2, i_ratio => 5 , i_filter => 'RED'));
select * from table(REPORT_DATA.Get_DB_Summary(i_testId1 => :B1, i_testId2 => :B2, i_ratio => 5 , i_filter => 'RED' , i_summary => 'YES' ));

spool off

-- ############  DB Summary Details ###############

spool dbStatusDetail.txt append

-- It displays the Test Comparison summary information in list format 
SELECT * FROM TABLE ( REPORT_DATA.Get_test_info ( i_testId1 => :B1, i_testId2 => :B2 ) ) ;

-- Generate the report data to be displayed
select * from table(REPORT_DATA.Get_DB_Details(i_testId1 => :B1, i_testId2 => :B2 , i_metric => 'ELAPSED_TIME_PER_EXEC_SECONDS' , i_filter => 'RED'));
select * from table(REPORT_DATA.Get_DB_Details(i_testId1 => :B1, i_testId2 => :B2 , i_metric => 'CPU_TIME_PER_EXEC_SECONDS' , i_filter => 'RED' ));
select * from table(REPORT_DATA.Get_DB_Details(i_testId1 => :B1, i_testId2 => :B2 , i_metric => 'TPS' , i_filter => 'RED' ));

spool off

-- ############  DB Summary in FULL ###############

spool dbStatusDetailFull.txt append

-- It displays the Test Comparison summary information in list format 
SELECT * FROM TABLE ( REPORT_DATA.Get_test_info ( i_testId1 => :B1, i_testId2 => :B2 ) ) ;

-- Generate DB summary 
select * from table(REPORT_DATA.Get_DB_Summary(i_testId1 => :B1, i_testId2 => :B2, i_ratio => 5 ));

-- Generate the report data to be displayed
select * from table(REPORT_DATA.Get_DB_Details(i_testId1 => :B1, i_testId2 => :B2 , i_metric => 'ELAPSED_TIME_PER_EXEC_SECONDS' ));
select * from table(REPORT_DATA.Get_DB_Details(i_testId1 => :B1, i_testId2 => :B2 , i_metric => 'CPU_TIME_PER_EXEC_SECONDS' ));
select * from table(REPORT_DATA.Get_DB_Details(i_testId1 => :B1, i_testId2 => :B2 , i_metric => 'TPS' ));

spool off


-- ############  DB Summary in FULL for all DB Operations ###############

spool dbStatusSummaryDB.txt append

-- It displays the Test Comparison summary information in list format 
SELECT * FROM TABLE ( REPORT_DATA.Get_test_info ( i_testId1 => :B1, i_testId2 => :B2 ) ) ;

-- Gather the data into the TEMP table
exec REPORT_DATA.Do_AnalyticalData(i_testId1 => :B1, i_testId2 => :B2, i_ratio => 5 ); 

-- Generate DB summary 
--select * from table(REPORT_DATA.Get_DB_Summary(i_testId1 => :B1, i_testId2 => :B2, i_ratio => 5 ));
select * from table(REPORT_DATA.Get_DB_Summary(i_testId1 => :B1, i_testId2 => :B2, i_ratio => 5 , i_summary => 'YES' ));

-- Generate the report data to be displayed
select * from table(REPORT_DATA.Get_DB_Details(i_testId1 => :B1, i_testId2 => :B2 , i_metric => 'ELAPSED_TIME_PER_EXEC_SECONDS' ));
select * from table(REPORT_DATA.Get_DB_Details(i_testId1 => :B1, i_testId2 => :B2 , i_metric => 'CPU_TIME_PER_EXEC_SECONDS' ));
select * from table(REPORT_DATA.Get_DB_Details(i_testId1 => :B1, i_testId2 => :B2 , i_metric => 'TPS' ));

spool off



set head on
SET UNDERLINE on
set feed on
set pages 1000
set verify on
