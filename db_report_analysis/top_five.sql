-- ==========================================================================================================================
-- Name         : top_five.sql
-- Author       : Charles Ajah
-- Date         : 19-Sep-2024
-- Purpose      : This script calls the PL_SQL OBJECT(S) that writes into a file the information(Top 5 SQLs during a test) to be used by Confluence for rendering charts/tables
-- ==========================================================================================================================
-- Get the variables on the call
-- Environment to be collected for N01/N02
--define ENV='&1'
define g_testid = '&1'
define g_dbname = '&2'
define g_dirname = '&3'

COLUMN filename1  NEW_VALUE filename ;

-- Set up SQL Plus parameter to create the correct output
set pages 0
set newpage 0
set lines 999
SET UNDERLINE off
set head off
SET RECSEP off
set feed off
set echo off
set trims on
SET VERIFY OFF

select 'top5_'||'&&g_dbname'||'_'||'&&g_testid'||'.csv' as filename1 from dual;

--spool top5.txt
--select * from table(HP_DIAG.top_five.get_top5());
--spool off

spool top5.txt
select * from table(HP_DIAG.top_five.get_top5_charts());
--select * from table(HP_DIAG.test_charles.get_top5_graph());
spool off


spool top5_charts.txt append

select 'h5. Top 5 SQLs per snapshot interval for &&g_dbname' from dual;
select '||Top 5 SQLs for &&g_dbname||' from dual;

select * from table(HP_DIAG.test_charles.get_top5_graphv2('&&g_testid', '&&g_dbname'));

prompt {csv:url=http://wd015506.bskyb.com:9320/reports/tests/top5/&&g_dirname/&&filename}
prompt {csv}
prompt {table-chart} |

spool off


spool &filename
select * from table(HP_DIAG.test_charles.get_top5_graph('&&g_testid', '&&g_dbname'));
spool off


