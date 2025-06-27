-- ==========================================================================================================================
-- Name         : nft_space_allocation_v2.sql
-- Author       : Rakel Fernandez
-- Date         : 07-Mar-2024
-- Purpose      : This script calls the PL_SQL PROCEDURES that will generate the information to be posted within Confluence
-- Parameters	: ENV --> Environment to display the report for : N01 / N02
--              : ENVNAME --> Environment group of databases to display the inforamtion for : CORE / FULL_N01 / FULL_N02 / KFX 
-- ==========================================================================================================================
-- Get the variables on the call
-- Environment to be collected for N01/N02
define ENV='&1'
define GRPNAME='&2'

SET trimspool on pagesize 0 head off feed off lines 1000 verify off serverout on


spool space.txt
select * from table(HP_DIAG.REPORT_SPACE.get_overall_space (i_env => '&&ENV', i_grpname => '&&GRPNAME'));
spool off

spool schema_space.txt
exec HP_DIAG.REPORT_SPACE.get_db_schema_space (i_env => '&&ENV', i_grpname => '&&GRPNAME');
spool off


spool ts_space.txt
exec HP_DIAG.REPORT_SPACE.get_db_ts_space (i_env => '&&ENV', i_grpname => '&&GRPNAME');
spool off