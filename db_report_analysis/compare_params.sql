-- =============================================================
-- Name 			: compare_params.sql
-- Author 			: Charles Ajah
-- Date 			: 20/11/2024
-- Purpose  		: for comparing database parameters of same type between N01 and N02
----
-- =============================================================
set SERVEROUTPUT ON
set lines 999
SET LINESIZE 200
set pages 0
set newpage 0
set lines 999
SET UNDERLINE off
set head off
SET RECSEP off
set echo off
set trims on
SET VERIFY OFF
spool compare_params.log
DECLARE
    -- Define a record type for DB Group
    TYPE DB_REC_TYPE IS RECORD ( 
        db_name   VARCHAR2(100),
        db_type   VARCHAR2(100),
        db_env    VARCHAR2(100)
    );
    -- Record type for query results
    TYPE RESULT_REC_TYPE IS RECORD (
        n01_dbname   VARCHAR2(100),
        param_n01    VARCHAR2(4000),
        value_n01    VARCHAR2(4000),
        n02_dbname   VARCHAR2(100),
        param_n02    VARCHAR2(4000),
        value_n02    VARCHAR2(4000)
    );
    -- Collection type for query results
    TYPE RESULT_TABTYPE IS TABLE OF RESULT_REC_TYPE;

    -- Define a collection type for the DB Group
    TYPE DB_TABTYPE IS TABLE OF DB_REC_TYPE;
    
    -- Variables
    v_db_tab DB_TABTYPE := DB_TABTYPE();  -- Initialize the collection
    v_sql    VARCHAR2(4000);
    v_results RESULT_TABTYPE := RESULT_TABTYPE();  -- we Initialize the collection and store query results here
    v_dynamic_query VARCHAR2(4000);
    v_db_link_1  VARCHAR2(100);
    v_db_link_2  VARCHAR2(100);
    CURSOR db_group IS
        SELECT DISTINCT db_type
        FROM TEST_RESULT_DBS
        WHERE DB_ENV IN ('N01','N02')
        ORDER BY db_type;
BEGIN
    FOR rec IN db_group LOOP
        -- Dynamically build the query
        v_sql := 'SELECT db_name, db_type, db_env FROM TEST_RESULT_DBS ' ||
                 'WHERE db_type = ''' || rec.db_type || ''' ' ||
                 'AND DB_ENV IN (''N01'',''N02'')' ||
                 'ORDER BY db_type ASC, db_env ASC';
        
        -- Execute the query and bulk collect results
        EXECUTE IMMEDIATE v_sql BULK COLLECT INTO v_db_tab;
        BEGIN
            -- Check if rows are found
            IF v_db_tab.COUNT = 2 THEN
                FOR i IN v_db_tab.FIRST .. v_db_tab.LAST LOOP
                    --DBMS_OUTPUT.PUT_LINE('Environment => ' || v_db_tab(i).db_env ||' | Database_type => ' || v_db_tab(i).db_type ||' | DB_NAME => ' || v_db_tab(i).db_name);
                    if v_db_tab(1).db_name <> v_db_tab(2).db_name then --we only compare DBs where N01 and N02 DB names are not same.
                        v_db_link_1 := v_db_tab(1).db_name;
                        v_db_link_2 := v_db_tab(2).db_name;
                    end if;
                END LOOP;
                DBMS_OUTPUT.PUT_LINE('######################################################################################################################################################################################################');
                DBMS_OUTPUT.PUT_LINE('DB links are : '||v_db_link_1||' and '||v_db_link_2);
                DBMS_OUTPUT.PUT_LINE('######################################################################################################################################################################################################');
                -- Build the dynamic query to compare parameters
                v_dynamic_query := 'SELECT ' ||
                                '(SELECT instance_name FROM v$instance@' || v_db_link_1 || ') AS n01_dbname, ' ||
                                'sp1.name AS param_n01, NVL(TO_CHAR(sp1.value), ''N/A'') AS value_n01, ' ||
                                '(SELECT instance_name FROM v$instance@' || v_db_link_2 || ') AS n02_dbname, ' ||
                                'sp2.name AS param_n02, NVL(TO_CHAR(sp2.value), ''N/A'') AS value_n02 ' ||
                                'FROM V$PARAMETER@' || v_db_link_1 || ' sp1, ' ||
                                'V$PARAMETER@' || v_db_link_2 || ' sp2 ' ||
                                'WHERE sp1.name NOT IN ' ||
                                '(' ||
                                '''audit_file_dest'', ''db_name'', ''control_files'', ''diagnostic_dest'', ' ||
                                '''instance_name'', ''local_listener'', ''spfile'',''background_dump_dest'',''user_dump_dest'',''_diag_adr_trace_dest'', ''log_archive_dest_1'', ' ||
                                '''dispatchers'', ''core_dump_dest'', ''db_unique_name'', ' ||
                                '''dg_broker_config_file1'', ''dg_broker_config_file2'',''fal_server'',''service_names'',''fal_client'',''db_file_name_convert'',''log_file_name_convert'',''log_archive_config'',''wallet_root'',''db_recovery_file_dest'', ''processor_group_name''' ||
                                ') ' ||
                                'AND sp1.name NOT LIKE ''log_archive_dest%'' ' ||
                                'AND sp1.name = sp2.name ' ||
                                'AND upper(sp1.value) != upper(sp2.value)' ||
                                ' ORDER by sp1.value , sp2.value ';
                EXECUTE IMMEDIATE v_dynamic_query BULK COLLECT INTO v_results;
                COMMIT;  --added to mitigate against ORA-02020: too many database links in use
                IF v_results.COUNT  > 0 THEN
                    -- Display the results
                    FOR i IN v_results.FIRST .. v_results.LAST LOOP
                        DBMS_OUTPUT.PUT_LINE('N01 DB: ' || v_results(i).n01_dbname ||
                                            ', Param: ' || v_results(i).param_n01 ||
                                            ', Value: ' || v_results(i).value_n01 ||
                                            ' | N02 DB: ' || v_results(i).n02_dbname ||
                                            ', Param: ' || v_results(i).param_n02 ||
                                            ', Value: ' || v_results(i).value_n02);
                    END LOOP;
                    DBMS_OUTPUT.PUT_LINE('######################################################################################################################################################################################################');
                    DBMS_OUTPUT.PUT_LINE(CHR(160));
                ELSE 
                    DBMS_OUTPUT.PUT_LINE('All Clear for the DB_TYPE: '|| rec.db_type);
                    DBMS_OUTPUT.PUT_LINE('######################################################################################################################################################################################################');
                    DBMS_OUTPUT.PUT_LINE(CHR(160));
                END IF;
            ELSIF v_db_tab.COUNT = 1 THEN
                DBMS_OUTPUT.PUT_LINE('######################################################################################################################################################################################################');
                DBMS_OUTPUT.PUT_LINE('Databases do not exist in both N01 and N02 for DB_TYPE: ' || rec.db_type);
                DBMS_OUTPUT.PUT_LINE('######################################################################################################################################################################################################');
                DBMS_OUTPUT.PUT_LINE(CHR(160));
                --null;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('######################################################################################################################################################################################################');
                DBMS_OUTPUT.PUT_LINE('Error occurred for DB_TYPE: ' || rec.db_type);
                DBMS_OUTPUT.PUT_LINE('Error Message: ' || SQLERRM);
                DBMS_OUTPUT.PUT_LINE('######################################################################################################################################################################################################');
                DBMS_OUTPUT.PUT_LINE(CHR(160));
        END;
    END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('An error has occured');
        DBMS_OUTPUT.PUT_LINE('Error Message: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('######################################################################################################################################################################################################');
        DBMS_OUTPUT.PUT_LINE(CHR(160));
END;
/

spool off;






