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


-- Generate DB summary 
spool sm_testformat.txt append

select * from table(STUART_PKG.Get_table_data(i_testId1 => :B1, i_testId2 => :B2));

prompt

select * from table(STUART_PKG.Get_table_data_csv(i_testId1 => :B1, i_testId2 => :B2));


spool off

set head on
SET UNDERLINE on
set feed on
set pages 1000
set verify on
