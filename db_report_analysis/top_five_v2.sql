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

COLUMN filename1  NEW_VALUE filename1 ;
COLUMN aggregation NEW_VALUE aggregation;

-- Create a new variable that combines 'agg_' and the value of g_dirname
COLUMN table_name NEW_VALUE final_table_name
SELECT 'agg_' || '&&g_dirname' AS table_name FROM dual;

--the derived table name is logged in the log file.
PROMPT Table name derived: &&final_table_name

select '&&g_testid', '&&g_dbname', '&&g_dirname' from dual;
select 'top5_'||'&&g_dbname'||'_'||'&&g_testid'||'.csv' as filename1 from dual;

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
SET LINESIZE 4000;
SET LONG 100000;
SET LONGCHUNKSIZE 4000;

ALTER SESSION SET NLS_LANGUAGE = 'AMERICAN';
ALTER SESSION SET NLS_TERRITORY = 'AMERICA';

spool top5.txt
select * from table(HP_DIAG.top_five.get_top5_charts());
select 'Checking stuff works' from dual;
--select * from table(HP_DIAG.test_charles.get_top5_graph());
spool off

--spool test.txt
--select * from table(HP_DIAG.test_charles.get_top5_graphv2(test_id => '&&g_testid',db_name => '&&g_dbname'));
--spool OFF

--create table using the dynamically derived table name
CREATE TABLE &&final_table_name (agg_value VARCHAR2(4000) );

--spool check_stuff.log
DECLARE
    v_aggregation  VARCHAR2(4000) := '';  -- Holds the dynamically generated aggregation string
    agg_value      VARCHAR2(4000);
    v_col_count    NUMBER;      -- Number of columns
    v_col_value    VARCHAR2(4000);  -- Holds the column values
    separator      VARCHAR2(50) := ''' || unistr(''\201A'') || '''; -- Store as a literal string
BEGIN
    -- Get the total number of columns
    SELECT TO_NUMBER(COLUMN_VALUE)
    INTO v_col_count
    FROM TABLE(HP_DIAG.test_charles.get_top5_graphv2(test_id => '&&g_testid', db_name => '&&g_dbname'));

    -- Loop through each column position and append its value with separator
    FOR i IN 1..v_col_count LOOP
        -- Fetch the column value dynamically
        SELECT COLUMN_VALUE
        INTO v_col_value
        FROM TABLE(HP_DIAG.test_charles.get_top5_graphv2(test_id => '&&g_testid', db_name => '&&g_dbname', col_pos => i));

        -- Append column value to aggregation string
        IF i = 1 THEN
            v_aggregation := v_col_value;  -- First column, no separator
        ELSE
            v_aggregation := v_aggregation || separator || v_col_value;  -- Append with literal separator
        END IF;
    END LOOP;

    -- Store value in a table    
    INSERT INTO &&final_table_name VALUES (v_aggregation);
    COMMIT;

    --SELECT agg_value into agg_value FROM aggregation;
    --DBMS_OUTPUT.PUT_LINE('Agrregates are: '||agg_value);


    -- Step 3: Output the dynamically built aggregation string with literal unistr('\201A')
    --DBMS_OUTPUT.PUT_LINE('| {table-chart:type=Stacked Column|column=interval_time|hide=true|aggregation=' || v_aggregation || 
        --'|barColoringType=mono|colors=#65a620,#e4a14b,#3572b0,#8cc3e9,#654982,#cc1010,#e98125,#a3acb2,#d8d23a,#6ada6a,#7b6888,#000000,--#f691b2|'
        --|| 'datepattern=HH:mm|formatVersion=3|hidecontrols=true|isFirstTimeEnter=false|tfc-height=400|tfc-width=1200|title='||'&&--g_testid'||'_'||'&&g_dbname'
        --|| '|xtitle=Interval_Time|ytitle=Elapsed_Time_Per_Exec(ms)|version=3}');
END;
/


select '| {table-chart:type=Stacked Column|column=interval_time|hide=true|aggregation='||(SELECT agg_value FROM &&final_table_name)
        ||'|barColoringType=mono|colors=#65a620,#e4a14b,#3572b0,#8cc3e9,#654982,#cc1010,#e98125,#a3acb2,#d8d23a,#6ada6a,#7b6888,#000000,#f691b2|'
		||'datepattern=HH:mm|formatVersion=3|hidecontrols=true|isFirstTimeEnter=false|tfc-height=400|tfc-width=1200|title='||'&&g_testid'||'|xtitle=Interval_Time|ytitle=Elapsed_Time_Per_Exec(ms)|version=3}' AS aggregation from dual;

--spool CentralSqlTrend.txt append as it has been written to already from calling perl script
spool CentralSqlTrend.txt append
SELECT '||h5. Top 5 SQL trend for database &&g_dbname||' FROM dual ;
--UNION ALL SELECT ''|| FROM dual;

SELECT '&aggregation' FROM dual;
--prompt {csv:url=http://wd015506.bskyb.com:9320/reports/tests/top5/&&g_dirname/&&filename1}
prompt {csv:url=http://wd015506.bskyb.com:9320/reports/tests/&&g_dirname/&&filename1}
prompt {csv}
prompt {table-chart}

spool off

-- Drop the table
DROP TABLE &&final_table_name;


--extract DB Activity into csv files
spool &filename1
select * from table(HP_DIAG.test_charles.get_top5_graph('&&g_testid','&&g_dbname'));

spool off


