
var B1 Varchar2(30)
var B2 Varchar2(30)

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
set serveroutput on

spool nfr_change.txt append

begin

update dataprov.dp_tele_nfr
set nfr = :b2
where magic_no = :b1;

dataprov.data_telephone_load.nfr_change(:b1);

end;
/

spool off

set head on
SET UNDERLINE on
set feed on
set pages 1000
set verify on
