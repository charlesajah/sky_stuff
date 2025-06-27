define RETENTION='&1'
define env='&2'

var l_ret 	varchar2(30)

begin
  :l_ret:='&1';
end;
/

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

spool DailyStatsHousekeep_&env..log 

-- Call Procedure to update the TEST_ID description and other flags
exec REPORT_ADM.Do_Stats_HouseKeeping( i_retain => :l_ret ); 

spool off

set head on
SET UNDERLINE on
set feed on
set pages 1000
set verify on
