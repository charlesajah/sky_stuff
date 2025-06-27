-- ==========================================================================================================================
-- Name         : check_dataprov_invalid_objects.sql
-- Author       : Charles Ajah
-- Date         : 01-Nov-2024
-- Purpose      : This script calls the PL_SQL OBJECT(S) that iterate through all databases with a dataprov schema 
--                checking for any invaid objects.
-- ==========================================================================================================================

define g_env = '&1'

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
SET SERVEROUTPUT ON

spool spool_check_dataprov_invalid_objects.log

declare
begin
    dataprov_invalid_objects.DISPLAY_DATAPROV_OBJECT_STATUS(env => '&&g_env');
end;
/
spool off