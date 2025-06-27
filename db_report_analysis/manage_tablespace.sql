-- ==========================================================================================================================
-- Name         : manage_tablespace.sql
-- Author       : Charles Ajah
-- Date         : 18-Oct-2024
-- Purpose      : This script calls the PL_SQL OBJECT(S) that reclaim(s) requested amount of space from a given tablesspace
-- ==========================================================================================================================
-- Get the variables on the call
-- Database , Tablespace & Reclaim value

define g_dbname = '&1'
define g_tablespace = '&2'
define g_reclaim = '&3'

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

spool manage_tablespace.log;
--initially i'd hoped to manage this from a package(HP_DIAG.manage_tablespace) in TCC021N but unfortunately it was not possible to 
--execute ALTER DATABASE statements using database links
--so i had to rework it into this SQL script;
--which means the perl script instead of just connecting to just one database repo 
--now connects to each required database
--EXEC HP_DIAG.manage_tablespace.reclaim_space('&&g_dbname', '&&g_tablespace', '&&g_reclaim');
DECLARE
    -- Define a record type 
    TYPE reclaim_REC_TYPE IS RECORD (
        adjusted_free_mb   NUMBER,
        adjusted_total_mb  NUMBER,
        adjusted_pct_free  NUMBER,
        final_reclaim      NUMBER
    );

    -- Define a collection type for the record type
    TYPE RECLAIM_TABTYPE IS TABLE OF reclaim_REC_TYPE;

    FUNCTION adjust_space_savings (
        database         VARCHAR2,
        tablespace_name  VARCHAR2,
        reclaim VARCHAR2
    ) RETURN RECLAIM_TABTYPE 
    IS
        v_database       VARCHAR2(100);
        v_reclaim_tab    RECLAIM_TABTYPE := RECLAIM_TABTYPE();  -- Initialize the result set
        tspace           VARCHAR2(50);
        v_reclaim        NUMBER;
        v_clean_reclaim  VARCHAR2(20);
        min_threshold    NUMBER := 2;
        v_adjusted       reclaim_REC_TYPE;
        v_sql            VARCHAR2(4000);   -- SQL statement string
        curr_free_mb     NUMBER;           -- Current free space in MB
        curr_total_mb    NUMBER;           -- Current total space in MB
        curr_pct_free    NUMBER;           -- Current free % space
        adjusted_free_mb NUMBER;           -- Adjusted free space in MB
        adjusted_total_mb NUMBER;          -- Adjusted total space in MB
        adjusted_pct_free NUMBER;          -- Adjusted free % space

    BEGIN
        tspace := tablespace_name;
        v_database := database;

        -- Clean up the reclaim string before converting to a number
        SELECT REGEXP_REPLACE(reclaim, '[^0-9]', '') INTO v_clean_reclaim FROM dual;
        IF v_clean_reclaim IS NULL OR v_clean_reclaim = '' OR v_clean_reclaim=0 THEN
            RAISE_APPLICATION_ERROR(-20002, 'Invalid reclaim number supplied');
        ELSE
            -- Convert the cleaned string to a number
            v_reclaim := TO_NUMBER(v_clean_reclaim);
        END IF;

        -- query string to get free and total MB for the tablespace
        v_sql := 'SELECT 
                    NVL(ceil(tsf.free_mb), 0) AS free_mb,
                    ceil(tsu.used_mb) AS total_mb,
                    ROUND(NVL(tsf.free_mb / tsu.used_mb * 100, 0), 2) AS pct_free
                  FROM (SELECT tablespace_name, SUM(bytes) / 1024 / 1024 AS used_mb
                        FROM dba_data_files 
                        GROUP BY tablespace_name) tsu,
                       (SELECT tablespace_name, SUM(bytes) / 1024 / 1024 AS free_mb
                        FROM dba_free_space 
                        GROUP BY tablespace_name) tsf
                  WHERE tsu.tablespace_name = tsf.tablespace_name(+)
                  AND tsu.tablespace_name = :tspace';

        -- Execute dynamic SQL and retrieve values
        EXECUTE IMMEDIATE v_sql INTO curr_free_mb, curr_total_mb, curr_pct_free USING tspace;

        IF curr_free_mb IS NOT NULL AND curr_total_mb IS NOT NULL THEN
            IF v_database IN ('TCC011N', 'TCC021N') THEN  --1% for Transcomm DBs and 2% for others
                min_threshold := 1;
            ELSE
                min_threshold := 2;
            END IF;

            adjusted_free_mb := curr_free_mb - v_reclaim;
            adjusted_total_mb := curr_total_mb - v_reclaim;
            adjusted_pct_free := ROUND((adjusted_free_mb / adjusted_total_mb) * 100, 2);

            IF adjusted_free_mb < 0 OR adjusted_total_mb < 0 THEN 
                RAISE_APPLICATION_ERROR(-20004, 'You have requested to recover more space than can be recovered from tablespace: '||tspace);
            END IF;

            -- Adjust the reclaim value to satisfy the minimum threshold for tablespaces used in our monitoring
            --1% for Transcomm DBs and 2% for others
            IF curr_pct_free  <= min_threshold THEN
                RAISE_APPLICATION_ERROR(-20008, 'You have requested to recover space from a tablespace already below its minimum freespace threshold.');
            ELSE
                IF adjusted_pct_free <= min_threshold THEN
                    v_reclaim := curr_free_mb - (min_threshold / 100) * curr_total_mb;
                    adjusted_free_mb := curr_free_mb - v_reclaim;
                    adjusted_total_mb := curr_total_mb - v_reclaim;
                    adjusted_pct_free := ROUND((adjusted_free_mb / adjusted_total_mb) * 100, 2);
                    DBMS_OUTPUT.PUT_LINE('Reclaim value adjusted down to ' || v_reclaim || ' to meet min threshold ' || min_threshold);
                END IF;
            END IF;
            

            -- Populate the record with adjusted values
            v_adjusted.adjusted_free_mb := adjusted_free_mb;
            v_adjusted.adjusted_total_mb := adjusted_total_mb;
            v_adjusted.adjusted_pct_free := adjusted_pct_free;
            v_adjusted.final_reclaim := v_reclaim;

            -- Add the record to the collection
            v_reclaim_tab.EXTEND;
            v_reclaim_tab(v_reclaim_tab.LAST) := v_adjusted;

            RETURN v_reclaim_tab;
        ELSE
            RAISE_APPLICATION_ERROR(-20003, 'Null values detected for free_mb or total_mb');
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
            RETURN v_reclaim_tab;
    END adjust_space_savings;

    PROCEDURE reclaim_space (
        database       VARCHAR2,
        tablespace_name VARCHAR2,
        reclaim        VARCHAR2
    ) IS
        v_reclaim NUMBER;
        reclaimed NUMBER;
        reclaimed_total NUMBER;
        v_sql VARCHAR2(4000);
        block_size NUMBER;
        tspace VARCHAR2(60) := tablespace_name;
        v_database VARCHAR2(60) := database;
        v_adjusted_total_mb NUMBER;
        resized NUMBER;
        db VARCHAR2(30);
        username VARCHAR2(50);

        TYPE file_info_rec IS RECORD (
            file_name VARCHAR2(255),
            smallest NUMBER,
            currsize NUMBER,
            savings NUMBER
        );

        TYPE file_info_tab IS TABLE OF file_info_rec;

        file_info file_info_tab;
        v_reclaim_tab RECLAIM_TABTYPE;
        remaining_reclaim NUMBER;

    BEGIN
        -- Call adjust_space_savings function to get reclaim values
        v_reclaim_tab := adjust_space_savings(database, tablespace_name, reclaim);
        v_reclaim := v_reclaim_tab(1).final_reclaim;
        v_adjusted_total_mb := v_reclaim_tab(1).adjusted_total_mb;

        -- Get the block size for calculations
        v_sql := 'SELECT value FROM v$parameter WHERE name = ''db_block_size''';
        EXECUTE IMMEDIATE v_sql INTO block_size;

        -- Retrieve datafile details for savings
        v_sql := 'SELECT file_name,
                         CEIL((NVL(hwm, 1) * ' || block_size || ') / 1024 / 1024) AS smallest,
                         CEIL(blocks * ' || block_size || ' / 1024 / 1024) AS currsize,
                         CEIL(blocks * ' || block_size || ' / 1024 / 1024) - CEIL((NVL(hwm, 1) * ' || block_size || ') / 1024 / 1024) AS savings
                  FROM dba_data_files a
                  LEFT JOIN (
                      SELECT file_id, MAX(block_id + blocks - 1) AS hwm
                      FROM dba_extents
                      GROUP BY file_id
                  ) b ON a.file_id = b.file_id
                  WHERE a.tablespace_name = :tspace
                  ORDER BY savings DESC';

        -- Bulk collect the results
        EXECUTE IMMEDIATE v_sql BULK COLLECT INTO file_info USING tspace;

        -- loop through datafiles to reclaim space
        remaining_reclaim := v_reclaim;
        reclaimed_total := 0 ; --initialised to 0

        FOR i IN 1..file_info.COUNT LOOP
            IF remaining_reclaim < file_info(i).savings THEN
                resized := file_info(i).currsize - remaining_reclaim ;
                resized := trunc(resized) ;
                v_sql := 'ALTER DATABASE DATAFILE ''' || file_info(i).file_name || ''' RESIZE ' || resized || 'M';
                DBMS_OUTPUT.PUT_LINE('SQL Statement is: '||v_sql||' ');
                EXECUTE IMMEDIATE v_sql;
                DBMS_OUTPUT.PUT_LINE('Reclaimed ' || remaining_reclaim || ' MB from ' || file_info(i).file_name);
                reclaimed_total := reclaimed_total + remaining_reclaim ;
                remaining_reclaim := 0;
                EXIT;
            ELSIF remaining_reclaim >= file_info(i).savings THEN
                resized := file_info(i).smallest + 1;  --resizing right up to the acclaimed minimum might throw up error ORA-03214: File Size specified is smaller than minimum required
                resized := trunc(resized) ;
                remaining_reclaim := remaining_reclaim - (file_info(i).savings - 1);  --compensating for the deduction of 1MB above
                reclaimed := file_info(i).savings  - 1 ;
                v_sql := 'ALTER DATABASE DATAFILE ''' || file_info(i).file_name || ''' RESIZE ' || resized || 'M';
                DBMS_OUTPUT.PUT_LINE('SQL Statement is: '||v_sql||'. Reclaim balance is : '||remaining_reclaim||'M');
                EXECUTE IMMEDIATE v_sql;
                DBMS_OUTPUT.PUT_LINE('Reclaimed ' || reclaimed || ' MB from ' || file_info(i).file_name); 
                reclaimed_total := reclaimed_total + reclaimed ;                   
            END IF;
            
        END LOOP;
        IF reclaimed_total > 0 THEN
            DBMS_OUTPUT.PUT_LINE('Total reclaimed from tablespace '||tspace||' is : '||reclaimed_total||'M');
        END IF;

        IF remaining_reclaim > 0 THEN
            RAISE_APPLICATION_ERROR(-20001, 'Unable to reclaim the full requested space for some reason. Balance to be reclaimed in MB  is :'||remaining_reclaim);
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
    END reclaim_space;

BEGIN
    reclaim_space('&&g_dbname', '&&g_tablespace', '&&g_reclaim');
END;
/
spool off



