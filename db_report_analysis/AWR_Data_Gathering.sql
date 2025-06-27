-- =============================================================
-- Name 			: AWR_Data_Gathering.sql
-- Author 			: Rakel Fernandez
-- Date 			: --/--/----
-- Purpose  		: 
--                    
-- Change History 	
-- --------------
-- 31/07/24 	RFA	: Added parameter RETAIN to be supplied in the call
-- 28/04/25     RFA : Added parameters VALIDTEST and TESTMODE to be supplied to the data gathering
-- =============================================================
define START_DTM='&1'
define END_DTM='&2'
define TDESC='&3'
define ENV='&4'
define GRP='&5'
define DBNAME='&6'
define LABEL='&7'
define FLAG='&8'
define RETAIN='&9'
define VALIDTEST='&10'
define TESTMODE='&11'


set serverout on echo off verify off

DECLARE
    l_start      DATE;
    l_end        DATE;
    l_desc       VARCHAR2(100);
    l_env        VARCHAR2(4);
    l_group      VARCHAR2(4);
    l_label      VARCHAR2(10);
    l_flag       NUMBER;
	l_retain     VARCHAR2(1);
    l_dbname     VARCHAR2(10);
	l_validtest  VARCHAR2(1);
	l_mode	     VARCHAR2(5);
    
    l_testid     VARCHAR2(40);
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
	l_validtest := '&VALIDTEST' ;
	l_mode      := '&TESTMODE' ;

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
	dbms_output.put_line('l_validtest = ' || l_validtest );
	dbms_output.put_line('l_mode   = ' || l_mode );


	-- Format TestID
    l_testid := REPORT_ADM.Format_testid ( to_char(l_start,'DDMONYY-HH24:MI'), to_char(l_end,'DDMONYY-HH24:MI'), l_env, l_group, l_dbname);
--	dbms_output.put_line('Test ID = ' || l_testid );
	-- Check is already exists
	t_test := REPORT_GATHER.Get_Test_Details (l_testid);
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
						,i_retain => l_retain 
						,i_valid  => l_validtest
						,i_mode   => l_mode						
						); 
	else
		dbms_output.put_line('TestID already exist. Please delete before re-running. --> '||l_testid);
	end if;
END ;
/



