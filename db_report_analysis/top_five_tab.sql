-- ==========================================================================================================================
-- Name         : top_five_tab.sql
-- Author       : Charles Ajah
-- Date         : 04-Nov-2024
-- Purpose      : This script calls the PL_SQL OBJECT(S) that writes into a file the information(Top 5 SQLs during a test) to be used by Confluence for rendering tables
-- ==========================================================================================================================
-- Get the variables on the call
-- Environment to be collected for N01/N02

define g_testid = '&1'
define g_dbname = '&2'


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
SET HEADING OFF
SET FEEDBACK OFF



spool top_five_tab.txt
select * from table(HP_DIAG.top_five.get_top5_table(test_id => '&&g_testid',db_name => '&&g_dbname'));
spool off
