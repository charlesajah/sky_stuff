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


spool centralAvgSummary.txt append

select * from table(RAKEL.Get_test_info(i_testId1 => :B1, i_testId2 => :B2 ));

-- Top 25 summary 
select * from table(RAKEL.Get_top25_comparison_summary ( i_testId1 => :B1, i_testId2 => :B2 , i_mode => 'APP' , i_title => 'Y' ));

-- Top 25 full 
select * from table(RAKEL.Get_top25_comparison_full ( i_testId1 => :B1, i_testId2 => :B2 , i_mode => 'APP' , i_title => 'N' ));

-- Load Comparison sunmmary
select * from table(RAKEL.Get_load_comparison_summary ( i_testId1 => :B1, i_testId2 => :B2 , i_title => 'Y' ));
 
-- Load Comparison full
select * from table(RAKEL.Get_load_comparison_full ( i_testId1 => :B1, i_testId2 => :B2 , i_title => 'N' ));



spool off

set head on
SET UNDERLINE on
set feed on
set pages 1000
set verify on
