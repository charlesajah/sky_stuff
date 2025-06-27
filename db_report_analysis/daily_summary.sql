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

-- Gather the data into the TEMP tables to produce the DB summary report
exec REPORT_DATA.Do_AnalyticalData(i_testId1 => :B1, i_testId2 => :B2, i_ratio => 5, i_except => 'APP' ); 

spool daily_summary.txt append

select * from table(report_data.Get_test_info(i_testId1 => :B1, i_testId2 => :B2 ));

-- Top 25 summary
select * from table(report_comp.Get_top25_comparison_summary ( i_testId1 => :B1, i_testId2 => :B2 , i_link => 'Y' , i_mode => 'APP' ));

-- Top 25 full
select * from table(report_comp.Get_top25_comparison_full ( i_testId1 => :B1, i_testId2 => :B2 , i_title => 'N' , i_mode => 'APP' ));

-- load comparison summary
select * from table(report_comp.Get_load_comparison_summary ( i_testId1 => :B1, i_testId2 => :B2 ));

-- Wait class comparison summary 
select * from table(report_comp.Get_waitclass_comparison_summary ( i_testId1 => :B1, i_testId2 => :B2  ));

-- Top 25 SQL Full Info
-- The Top 25 colour coded version also brings a summary with the details on it. 
-- select * from table(report_comp.Get_top25_long_comp(i_testId1 => :B1, i_testId2 => :B2 ));

-- Generate DB summary 
select * from table(REPORT_DATA.Get_DB_Summary(i_testId1 => :B1, i_testId2 => :B2, i_ratio => 5 , i_filter => 'RED' , i_summary => 'YES' ));

-- Dumps the report name for the confluence report page for the daily summary.
-- This name gets picked up within the phyton job itself : daily_sum_pages.py
spool repname.txt
select REPORT_ADM.Get_report_name(i_testId1 => :B1, i_testId2 => :B2) from dual;
spool off


spool off

set head on
SET UNDERLINE on
set feed on
set pages 1000
set verify on
