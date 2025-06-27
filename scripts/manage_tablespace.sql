create or replace PACKAGE BODY manage_tablespace AS

    FUNCTION get_space_savings (
        database         VARCHAR2,
        tablespace_name  VARCHAR2
    ) RETURN TSPACE_TABTYPE 
    IS
        v_database       VARCHAR2(100);
        v_tspace_tab     TSPACE_TABTYPE := TSPACE_TABTYPE();  -- Initialize the result set
        tspace           VARCHAR2(50);
        v_tspace         tspace_OBJ_TYPE;  -- Placeholder for each row
        v_sql            VARCHAR2(4000);   -- For dynamic query construction
        v_tablespace_name VARCHAR2(50);    -- Variable to hold tablespace name from query
        v_total_savings  NUMBER;           -- Variable to hold total savings from query

    BEGIN
        tspace := tablespace_name;
        v_database := database;

        --DBMS_OUTPUT.PUT_LINE('Database supplied is: ' || v_database || ' and tablespace supplied is: ' || tablespace_name);

        --IF v_database != database THEN
            --RAISE_APPLICATION_ERROR(-20002, 'Database name does not match.');
        --END IF;

        -- Construct dynamic query using EXECUTE IMMEDIATE
        v_sql := 'WITH savings AS (
                            SELECT 
                                a.tablespace_name, 
                                file_name,
                                CEIL( (NVL(hwm, 1) * 8192) / 1024 / 1024 ) AS smallest,
                                CEIL( blocks * 8192 / 1024 / 1024 ) AS currsize,
                                CEIL( blocks * 8192 / 1024 / 1024 ) - CEIL( (NVL(hwm, 1) * 8192) / 1024 / 1024 ) AS savings
                            FROM dba_data_files@' || v_database || ' a
                            LEFT JOIN (
                                SELECT file_id, MAX(block_id + blocks - 1) AS hwm
                                FROM dba_extents@' || v_database || '
                                GROUP BY file_id
                            ) b ON a.file_id = b.file_id
                            WHERE a.tablespace_name = :tspace
                            ORDER BY savings DESC
                          )
                          SELECT tablespace_name, SUM(savings) AS total_savings
                          FROM savings
                          GROUP BY tablespace_name';

        -- Execute the dynamic query and fetch the results
        EXECUTE IMMEDIATE v_sql INTO v_tablespace_name, v_total_savings USING tspace;

        -- Populate the object type for the result
        v_tspace := tspace_OBJ_TYPE(
            v_tablespace_name,
            v_total_savings
        );

        -- Add the object to the result table
        v_tspace_tab.EXTEND;
        v_tspace_tab(v_tspace_tab.LAST) := v_tspace;

        RETURN v_tspace_tab;

    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
            RETURN v_tspace_tab;  -- Return empty table in case of error
    END get_space_savings;

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
        min_threshold    NUMBER :=2;
        v_tspace         tspace_OBJ_TYPE;  -- Placeholder for each row
        v_adjusted       reclaim_OBJ_TYPE;
        v_sql            VARCHAR2(4000);   -- For dynamic query construction
        v_tablespace_name VARCHAR2(50);    -- Variable to hold tablespace name from query
        v_total_savings  NUMBER;           -- Variable to hold total savings from query
        curr_free_mb    NUMBER;            -- variable to hold current free space in MB
        curr_total_mb   NUMBER;            -- variable to holf current total space in MB
        curr_pct_free   NUMBER;            -- variable to hold current free %
        adjusted_free_mb NUMBER;           -- variable to adjusted free space in MB
        adjusted_total_mb NUMBER;          -- variable to hold adjusted total space in MB
        adjusted_pct_free NUMBER;          -- variable to hold adjusted free % 
        head_room        NUMBER;
        adjusted_threshold NUMBER;

    BEGIN
        tspace := tablespace_name;
        v_database := database;
        --first we try to clean up the reclaim string before converting into number.
        SELECT REGEXP_REPLACE(reclaim, '[^0-9]', '') into v_clean_reclaim FROM dual;
        IF v_clean_reclaim is null OR v_clean_reclaim = '' THEN
            RAISE_APPLICATION_ERROR(-20002, 'No valid number value supplied');
        ELSE
            -- Convert the cleaned string to a number
            v_reclaim := TO_NUMBER(v_clean_reclaim);
            --DBMS_OUTPUT.PUT_LINE('The value supplied for reclaim is :'||v_reclaim);
        END IF;

        -- Construct dynamic query using EXECUTE IMMEDIATE
        v_sql := 'select 
            decode(ceil(tsf.free_mb), NULL, 0, ceil(tsf.free_mb)) free_mb,
            ceil(tsu.used_mb) total_mb,
            decode(100 - ceil(tsf.free_mb / tsu.used_mb * 100),
                    NULL,
                    100,
                    round(tsf.free_mb / tsu.used_mb * 100,2)) pct_free
                from (select tablespace_name, sum(bytes) / 1024 / 1024 used_mb
                        from dba_data_files@' || v_database || ' group by tablespace_name) tsu,
                    (select tablespace_name, sum(bytes) / 1024 / 1024 free_mb
                        from dba_free_space@' || v_database || ' group by tablespace_name) tsf
                where tsu.tablespace_name = tsf.tablespace_name(+)
                and tsu.tablespace_name= :tspace';

        -- Execute the dynamic query and fetch the results
        EXECUTE IMMEDIATE v_sql INTO curr_free_mb, curr_total_mb, curr_pct_free USING tspace;

        if curr_free_mb is not null AND curr_total_mb is not null AND curr_pct_free is not NULL then
            if v_database = 'TCC011N' OR v_database ='TCC021N' then
                min_threshold := 1;
            else
                min_threshold := 2;
            end if;
            adjusted_free_mb := curr_free_mb - v_reclaim;
            adjusted_total_mb := curr_total_mb - v_reclaim;
            adjusted_pct_free := ROUND((adjusted_free_mb/adjusted_total_mb) * 100,2);  --equation for finding the new free % space
            if adjusted_pct_free <= min_threshold then 
                --then we adjust the reclaim value downwards in order to satisfy the configured minimum threshold.
                v_reclaim := curr_free_mb - (min_threshold / 100) * curr_total_mb;
                -- Recalculate adjusted values with new v_reclaim
                adjusted_free_mb := curr_free_mb - v_reclaim;
                adjusted_total_mb := curr_total_mb - v_reclaim;
                adjusted_pct_free := ROUND((adjusted_free_mb / adjusted_total_mb) * 100, 2);
                DBMS_OUTPUT.PUT_LINE('Reclaim value has been adjusted down to '||v_reclaim||' in order to satisfy the min_threshold of '||min_threshold);
            end if;
            -- Populate the object type for the result
            v_adjusted := reclaim_OBJ_TYPE(
                adjusted_free_mb,
                adjusted_total_mb,
                adjusted_pct_free,
                v_reclaim
            );

            -- Add the object to the result table
            v_reclaim_tab.EXTEND;
            v_reclaim_tab(v_reclaim_tab.LAST) := v_adjusted;

            RETURN v_reclaim_tab;
        ELSE
            RAISE_APPLICATION_ERROR(-20003, 'Null values for either of curr_free_mb, curr_total_mb, curr_pct_free not allowed');
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
            RETURN v_reclaim_tab;  -- Return empty table in case of error
    END adjust_space_savings;
PROCEDURE reclaim_space(
    database       VARCHAR2,
    tablespace_name VARCHAR2,
    reclaim        VARCHAR2
) IS
    -- Variables
    v_reclaim_tab RECLAIM_TABTYPE;
    v_reclaim NUMBER;
    v_clean_reclaim VARCHAR2(20);
    v_sql VARCHAR2(4000);
    block_size NUMBER;
    tspace varchar2(60) := tablespace_name;
    v_database varchar2(60) := database;
    username varchar2(50);
    v_adjusted_total_mb NUMBER;
    resized NUMBER;
    db varchar2(30);

    -- Datafile information variables
    TYPE file_info_rec IS RECORD (
        file_name VARCHAR2(255),
        smallest NUMBER,
        currsize NUMBER,
        savings NUMBER
    );

    TYPE file_info_tab IS TABLE OF file_info_rec;
    file_info file_info_tab;

    total_savings NUMBER := 0;
    remaining_reclaim NUMBER := 0;
    reclaim_to_allocate NUMBER := 0;

BEGIN
    -- Call adjust_space_savings to get the proper value for reclaim
    v_reclaim_tab := adjust_space_savings(database, tablespace_name, reclaim);
    v_reclaim := v_reclaim_tab(1).final_reclaim;
    v_adjusted_total_mb := v_reclaim_tab(1).adjusted_total_mb;


    -- Extract the block size from the database
    v_sql := 'SELECT value FROM v$parameter@' || v_database || ' WHERE name = ''db_block_size''';
    EXECUTE IMMEDIATE v_sql INTO block_size;

    --Retrieve the file_name, smallest, currsize, and savings for each datafile with savings more than 100 MB
    v_sql := 'SELECT file_name,
                     CEIL((NVL(hwm, 1) * ' || block_size || ') / 1024 / 1024) AS smallest,
                     CEIL(blocks * ' || block_size || ' / 1024 / 1024) AS currsize,
                     CEIL(blocks * ' || block_size || ' / 1024 / 1024) - CEIL((NVL(hwm, 1) * ' || block_size || ') / 1024 / 1024) AS savings
              FROM dba_data_files@' || v_database || ' a
              LEFT JOIN (
                  SELECT file_id, MAX(block_id + blocks - 1) AS hwm
                  FROM dba_extents@' || v_database || '
                  GROUP BY file_id
              ) b ON a.file_id = b.file_id
              WHERE a.tablespace_name = :tspace
              AND CEIL(blocks * ' || block_size || ' / 1024 / 1024) - CEIL((NVL(hwm, 1) * ' || block_size || ') / 1024 / 1024) > 100
              ORDER BY savings DESC';

    -- Execute the query and store the results in the collection
    DBMS_OUTPUT.PUT_LINE('Just before the BULK Collect');
    EXECUTE IMMEDIATE v_sql BULK COLLECT INTO file_info USING tspace;
    DBMS_OUTPUT.PUT_LINE('Just after the BULK Collect');

    --Find if a single datafile can handle the reclaim value
    remaining_reclaim := v_reclaim;

    FOR i IN 1..file_info.COUNT LOOP
        resized := file_info(i).currsize - remaining_reclaim;
        IF resized > 0 AND file_info(i).savings > 100 THEN --only interested in datafiles with current sizes greater than the amount we want to reclaim and has at least 100M of savings.
            -- Check if the current datafile can accommodate the entire reclaim value
            IF remaining_reclaim <= file_info(i).savings AND resized >= file_info(i).smallest THEN --logically once remaining_reclaim is less than file_info(i).savings then resized is always > than file_info(i).smallest anyway.
                DBMS_OUTPUT.PUT_LINE('Resized value is '||resized);
                -- Single datafile can handle the reclaim value  
                v_sql := 'ALTER DATABASE DATAFILE ''' || file_info(i).file_name || ''' RESIZE ' || resized || 'M';

                select SYS_CONTEXT ('USERENV', 'DB_NAME') into db from dual;
                DBMS_OUTPUT.PUT_LINE('Database is '||db||' and SQL statement is :'||v_sql);
                EXECUTE IMMEDIATE v_sql;
                DBMS_OUTPUT.PUT_LINE('Reclaimed ' || remaining_reclaim || ' MB from ' || file_info(i).file_name ||' in database '||db);
                remaining_reclaim := 0;
                EXIT;  -- Exit the loop since we found a suitable datafile
            ELSIF remaining_reclaim > file_info(i).savings THEN
                DBMS_OUTPUT.PUT_LINE('Just after the IF remaining_reclaim <= file_info(i).savings ELSE part');
                -- Datafile cannot handle the full reclaim value, allocate part of it
                resized := file_info(i).smallest; --then we resize the file by an amount equal to the maximum savings possible for the file.
                remaining_reclaim := remaining_reclaim - file_info(i).savings;
                v_sql := 'ALTER DATABASE DATAFILE ''' || file_info(i).file_name || ''' RESIZE ' || resized || 'M';
                select SYS_CONTEXT ('USERENV', 'SESSION_USER') into username from dual;
                select SYS_CONTEXT ('USERENV', 'DB_NAME') into db from dual;
                DBMS_OUTPUT.PUT_LINE('User executiing statement is '||username||'.Staementts: '||v_sql);
                DBMS_OUTPUT.PUT_LINE('Database is '||db||' and SQL statement is :'||v_sql);
                EXECUTE IMMEDIATE v_sql;
                DBMS_OUTPUT.PUT_LINE('Reclaimed ' || remaining_reclaim || ' MB from ' || file_info(i).file_name ||' in database '||db);

            END IF;
        END IF;
    END LOOP;

    -- If we exit the loop with remaining reclaim to allocate, it means no single file could handle it all
    IF remaining_reclaim > 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Unable to reclaim the full requested space. Some files did not have enough space.');
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('An error occurred: ' || SQLERRM);
END reclaim_space;


END manage_tablespace;