define TESTID='&1'


var l_testid 	varchar2(40)

begin
  :l_testid:='&1';
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
set serverout on

spool DeleteStatsDetails.log append

-- Call Procedure to update the TEST_ID description and other flags
exec HP_DIAG.REPORT_ADM.Do_DeleteStats(i_testId => :l_testid ); 

spool off

set head on
SET UNDERLINE on
set feed on
set pages 1000
set verify on
