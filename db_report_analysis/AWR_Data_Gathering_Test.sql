-- =============================================================
-- Name 			: AWR_Data_Gathering_Test.sql
-- Author 			: Charles Ajah
-- Date 			: 11/11/2024
-- Purpose  		: 
--
--
-- =============================================================
-- Exit SQL*Plus on any SQL error
WHENEVER SQLERROR EXIT SQL.SQLCODE

define START_DTM = '&1'
define END_DTM = '&2'
define TDESC = '&3'
define ENV = '&4'
define GRP = '&5'
define DBNAME = '&6'
define LABEL = '&7'
define FLAG = '&8'
define RETAIN = '&9'
define TEST_ID_1 = '&10'
-- Declare a bind variable
VARIABLE l_test_id_1 VARCHAR2(40);
VARIABLE l_test_id_2 VARCHAR2(40);

exec :l_test_id_1 := '&&TEST_ID_1' ;


set serveroutput on
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

DECLARE
    l_start      DATE;
    l_end        DATE;
    l_desc       VARCHAR2(100);
    l_env        VARCHAR2(100);
	--l_env        VARCHAR2(4);
    l_group      VARCHAR2(100);
	--l_group      VARCHAR2(4);
    l_label      VARCHAR2(100);
	--l_label      VARCHAR2(10);
    l_flag       NUMBER;
	l_retain     VARCHAR2(1);
    l_dbname     VARCHAR2(10);
    l_test_id_1     VARCHAR2(40);
    l_test_id_2     VARCHAR2(40);
	t_test		 TEST_RESULT_MASTER%rowtype ;

BEGIN
    l_start     := to_date('&START_DTM','DDMONYY-HH24:MI');
    l_end       := to_date('&END_DTM','DDMONYY-HH24:MI');
    l_desc      := '&TDESC';
    l_env       := '&ENV';
    l_group     := '&GRP';
    l_label     := '&LABEL' ;
    l_flag      := '&FLAG' ;
    l_dbname    := '&DBNAME' ;
	l_retain    := '&RETAIN' ;
	l_test_id_1 := '&TEST_ID_1';

    -- DEBUG 

	dbms_output.put_line('l_start  = ' || l_start );
	dbms_output.put_line('l_end    = ' || l_end );
	dbms_output.put_line('l_desc   = ' || l_desc );
	dbms_output.put_line('l_env    = ' || l_env );
	dbms_output.put_line('l_label  = ' || l_label );
	dbms_output.put_line('l_group  = ' || l_group );
	dbms_output.put_line('l_flag   = ' || l_flag );
	dbms_output.put_line('l_dbname = ' || l_dbname );
	dbms_output.put_line('l_retain = ' || l_retain );
	dbms_output.put_line('l_test_id_1 = ' || l_test_id_1 );

    --RETURN;

	-- Format TestID
    l_test_id_2 := REPORT_ADM.Format_testid ( to_char(l_start,'DDMONYY-HH24:MI'), to_char(l_end,'DDMONYY-HH24:MI'), l_env, l_group, l_dbname);
    --dbms_output.put_line('Test ID = ' || l_test_id_2 );

	-- Assign the PL/SQL variable to the SQL*Plus bind variable
	:l_test_id_2 := l_test_id_2;

	-- Check if test_id already exists
	t_test := REPORT_GATHER.Get_Test_Details (l_test_id_2);
--	dbms_output.put_line('Test Description (null is new) : '||t_test.TEST_DESCRIPTION) ;
	--REPORT_ADM.Do_DeleteStats(l_testid);
	-- If DNNAME is <NONE> the null VALUE
	if l_dbname = '-NONE-' then l_dbname := null; end if ;
--	dbms_output.put_line('Database Name (null if not specified) : ' || l_dbname );
	If t_test.TEST_ID is NULL then
		REPORT_GATHER.Get_TEST_DATA (  i_start => l_start 
						,i_end   => l_end 
						,i_desc  => l_desc    
						,i_label => l_label 
						,i_flag  => l_flag 
						,i_env   => l_env 
						,i_group => l_group
						,i_dbname => l_dbname
						,i_retain => l_retain ); 

	else
		--dbms_output.put_line('TestID already exist. Please delete before re-running. --> '||l_test_id_2);
		RAISE_APPLICATION_ERROR(-20001, 'TestID '||l_test_id_2||' already exists. Exiting script.');
	end if;
END ;
/

-- ALL SQL COMAPRISON 
spool in_flight_sql_comparison.txt 

-- Test INFO
SELECT * FROM TABLE ( report_data.Get_test_info ( i_testId1 => :l_test_id_2, i_testId2 => :l_test_id_1 ) ) ;

-- Details SQL comparison for previous text per database
select 'h3. Detail SQL Comparison Per Database' from dual;
select * from table( report_comp.Get_all_db_top25_comparison ( i_testId1 => :l_test_id_2, i_testId2 => :l_test_id_1 , i_mode => 'APP' , i_top_n => 10 ));

spool off


