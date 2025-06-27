-- =============================================================
-- Name 			: CentralDBActGraph.sql
-- Author 			: Rakel Fernandez
-- Date 			: 24/07/2024
-- Purpose  		: Generates the DB Activity graphs per database
--                    
-- Change History 	
-- --------------
-- 24/07/24 	RFA	: Modifications made for the new Central Repository solution
-- 30/08/24 	RFA : Complete rework to call PL/SQL Procedures to deal with the data
--				      It gets called per dataabse but feeds the data from the stored info within the TEST_RESULT_ACTIVITY table.
--
-- =============================================================

define g_testid1 = '&1'
define g_testid2 = '&2'
define g_dbname = '&3'
define g_dirname = '&4'

COLUMN filename1  NEW_VALUE filename1 ;
COLUMN filename2  NEW_VALUE filename2 ;

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

var l_dbtype varchar2(10);
exec select db_type into :l_dbtype from TEST_RESULT_DBS where db_name = '&&g_dbname';
column dbtype new_value dbtype ;
select :l_dbtype dbtype from dual ;


select 'activity_'||'&&dbtype'||'_'||'&&g_testid1'||'.csv' as filename1 from dual;
select 'activity_'||'&&dbtype'||'_'||'&&g_testid2'||'.csv' as filename2 from dual;

spool CentralDBActivity.txt append

-- This function doesn't work as confluence is not taking the "comma" separator for the aggregation - Seems to be a problem with charaters translation 
--select * from table (REPORT_COMP.Get_DB_Activity_Headers ( i_testid1 => '&&g_testid1', i_testid2 => '&&g_testid2', i_dbname => '&&g_dbname'));

--select 'h5. Database Activity for &&dbtype' from dual;
select '||Database Activity Graph Comparison for &&dbtype||' from dual;

select '| {table-chart:type=Stacked Area|column=stime|hide=true|aggregation=cpu'||unistr('\201A')||'scheduler'||unistr('\201A')||'user_io'
        ||unistr('\201A')||'system_io'||unistr('\201A')||'concurrency'||unistr('\201A')||'application'||unistr('\201A')||'commit'
        ||unistr('\201A')||'configuration'||unistr('\201A')||'administrative'||unistr('\201A')||'network'||unistr('\201A')||'queueing'
        ||unistr('\201A')||'cluster'||unistr('\201A')
        ||'other|barColoringType=mono|colors=#65a620,#e4a14b,#3572b0,#8cc3e9,#654982,#cc1010,#e98125,#a3acb2,#d8d23a,#6ada6a,#7b6888,#000000,#f691b2|'
		||'datepattern=HH:mm|formatVersion=3|hidecontrols=true|isFirstTimeEnter=false|tfc-height=400|tfc-width=1200|title='||'&&dbtype'||'_'||'&&g_testid1|xtitle=Time|ytitle=Sessions|version=3}' from dual ;
prompt {csv:url=http://wd015506.bskyb.com:9320/reports/tests/&&g_dirname/&&filename1}
prompt {csv}
prompt {table-chart} |

select '| {table-chart:type=Stacked Area|column=stime|hide=true|aggregation=cpu'||unistr('\201A')||'scheduler'||unistr('\201A')||'user_io'
        ||unistr('\201A')||'system_io'||unistr('\201A')||'concurrency'||unistr('\201A')||'application'||unistr('\201A')||'commit'
        ||unistr('\201A')||'configuration'||unistr('\201A')||'administrative'||unistr('\201A')||'network'||unistr('\201A')||'queueing'
        ||unistr('\201A')||'cluster'||unistr('\201A')
        ||'other|barColoringType=mono|colors=#65a620,#e4a14b,#3572b0,#8cc3e9,#654982,#cc1010,#e98125,#a3acb2,#d8d23a,#6ada6a,#7b6888,#000000,#f691b2|'
		||'datepattern=HH:mm|formatVersion=3|hidecontrols=true|isFirstTimeEnter=false|tfc-height=400|tfc-width=1200|title='||'&&dbtype'||'_'||'&&g_testid2|xtitle=Time|ytitle=Sessions|version=3}' from dual ;
prompt {csv:url=http://wd015506.bskyb.com:9320/reports/tests/&&g_dirname/&&filename2}
prompt {csv}
prompt {table-chart} |

spool off



-- extract DB Activity into csv files
spool &filename1

select * from table (REPORT_COMP.Get_DB_Activity_Body ( i_testid => '&&g_testid1', i_dbname => '&&g_dbname'));

spool off

spool &filename2

select * from table (REPORT_COMP.Get_DB_Activity_Body ( i_testid => '&&g_testid2', i_dbname => '&&g_dbname'));

spool off

