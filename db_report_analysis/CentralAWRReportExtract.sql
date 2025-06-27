-- =============================================================
-- Name 			: CentralAWRReportExtract.sql
-- Author 			: Rakel Fernandez
-- Date 			: 29/08/2024
-- Purpose  		: Generates the physical HTML files from the AWR reports stored within the Repository
--                    Gets called once per database
--                    
-- Change History 	
-- --------------
-- 08/10/24  RFA64  : Changed it so that it only generates the reports for the CORE type of databases.
--                    It also controls better the output around the HTML file generation
--
-- =============================================================
define g_testid = '&1'
define g_dbname = '&2'

var l_start_dtm_tmp varchar2(30);
var l_start_dtm varchar2(30);
var l_end_dtm_tmp varchar2(30);
var l_end_dtm varchar2(30);

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
set termout off

-- Get the START and END date/time for the TEST_ID supplied
-- These values are injected into the last query below
exec select SUBSTR('&&g_testid',1,INSTR('&&g_testid','_')-1) into :l_start_dtm_tmp from dual ;
exec select SUBSTR(:l_start_dtm_tmp,1,INSTR(:l_start_dtm_tmp,'-')+2)||':'||substr(:l_start_dtm_tmp,INSTR(:l_start_dtm_tmp,'-')+3) into :l_start_dtm from dual ;
exec select SUBSTR('&&g_testid',INSTR('&&g_testid','_')+1,12) into :l_end_dtm_tmp from dual ;
exec select SUBSTR(:l_end_dtm_tmp,1,INSTR(:l_end_dtm_tmp,'-')+2)||':'||substr(:l_end_dtm_tmp,INSTR(:l_end_dtm_tmp,'-')+3) into :l_end_dtm from dual ;

-- Get the SNAP ID Times linked to the start/End Periods - the format passed fits the expected DDMONYY-HH24:MI
column test1_start_dtm new_value test1_start_dtm ;
column test1_end_dtm new_value test1_end_dtm ;
select :l_start_dtm test1_start_dtm from dual ;
select :l_end_dtm test1_end_dtm from dual ;

-- Retrieve the HTML AWR report from the database for a given test_id
-- This section will create the HTML file, which later on will be read upon and listed within the Confluence pages
column repnameStem new_value repnameStem
SELECT 'AWRRPT_' || '&&g_dbname' || '_' || TO_CHAR ( TO_DATE ( '&&test1_start_dtm' , 'DDMONYY-HH24:MI' ) , 'DDMONYY-HH24MI' ) || '_'
    || TO_CHAR ( TO_DATE ( '&&test1_end_dtm' , 'DDMONYY-HH24:MI' ) , 'DDMONYY-HH24MI' ) || '_GENERATED_' || TO_CHAR ( SYSDATE , 'DDMMYY-HH24MI' ) AS repnameStem
  FROM dual;
;

column  repname new_value repname
select '&&repnameStem' || '.html' repname from dual;

define  report_type  = 'html'
define  report_name  = &&repname

set termout on
whenever sqlerror exit failure

var  l_newrep varchar2(150);
BEGIN
    SELECT '&&report_name' into :l_newrep
          FROM hp_diag.TEST_RESULT_AWR t
          JOIN hp_diag.V_TEST_RESULT_DBS db ON ( db.DB_NAME = t.DATABASE_NAME )
          JOIN hp_diag.TEST_RESULT_MASTER tm ON ( tm.TEST_ID = t.TEST_ID )
         WHERE ( db.DB_GROUP = 'CORE' OR tm.DB_GROUP = 'DB' )
           AND t.TEST_ID = '&&g_testid'
           AND t.DATABASE_NAME = '&&g_dbname'
        GROUP BY t.TEST_ID ;		   
		   
END;
/

prompt -----------------------------------------------------------\n
prompt &&report_name
prompt -----------------------------------------------------------\n

spool &&report_name

SELECT * FROM TABLE ( REPORT_ADM.Get_AWR_Report ( i_dbname => '&&g_dbname', i_testId => '&&g_testid' ) ) ;

spool off;

