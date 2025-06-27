-- =============================================================
-- Name 			: AWR_Daily_Data_Gathering.sql
-- Author 			: Rakel Fernandez
-- Date 			: --/--/----
-- Purpose  		: Gathers the STATS/AWR data from the given ENV/GRP of databases
--                    By default it's supposed to run daily, so gets the "run day" automatically from the day the job is run
--                    
-- Change History 	
-- --------------
-- 31/07/24 	RFA	: Added parameter RETAIN to be supplied in the call
-- 04/10/24     RFA : Changes made calculate if the time given belongs to the previous date for the start time
-- 28/04/25     RFA : Added parameter TESTMODE to be supplied to the data gathering
-- =============================================================

define START_TM='&1'
define END_TM='&2'
define ENV='&3'
define GRP='&4'
define TESTMODE='&5'


set serverout on echo off verify off

DECLARE
    l_start      VARCHAR2(20);
    l_end        VARCHAR2(20);
    l_desc       VARCHAR2(100);
    l_env        VARCHAR2(4);
    l_group      VARCHAR2(4);
    
    l_testid     VARCHAR2(40);
	l_today		 VARCHAR2(20);
	l_yesterday	 VARCHAR2(20);
	l_mode 	     VARCHAR2(5);	

	t_test		 TEST_RESULT_MASTER%rowtype ;

BEGIN
	select to_char(sysdate,'DDMONYY') into l_today from dual ;
	
	if to_date('&START_TM','HH24:Mi') > to_date('&END_TM','HH24:Mi') then
		select to_char(sysdate-1,'DDMONYY') into l_yesterday from dual ;
	else	
		l_yesterday := l_today ;
	end if;
	
	l_start     := l_yesterday||'-'||'&START_TM';
	l_end       := l_today||'-'||'&END_TM';
    l_env       := '&ENV';
    l_group     := '&GRP';
	l_mode      := '&TESTMODE';


    l_desc      := l_group||'_'||l_env||'_TEST AUTO AWR Stats Collection '||l_start||' to '||l_end;
	-- Format TestID
    l_testid := REPORT_ADM.Format_testid ( l_start, l_end, l_env, l_group);
	-- Check is already exists
	t_test := REPORT_GATHER.Get_Test_Details (l_testid);
	dbms_output.put_line('test id returned : '||NVL(t_test.TEST_ID,'not found'));
    --REPORT_ADM.Do_DeleteStats(l_testid);
	If t_test.TEST_ID is NULL then
		REPORT_GATHER.Get_TEST_DATA ( i_start => to_date(l_start,'ddMONyy-hh24:mi')
								  ,i_end   => to_date(l_end,'ddMONyy-hh24:mi') 
							      ,i_desc  => l_desc    
							      ,i_env   => l_env 
							      ,i_group => l_group
								  ,i_flag  => 1 
								  ,i_retain => 'N'
								  ,i_mode  => l_mode
						  	     );
	else
		dbms_output.put_line('TestID already exist. Please delete before re-running. --> '||l_testid);
	end if;

END ;
/



