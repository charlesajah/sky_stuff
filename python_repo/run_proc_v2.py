
import oracledb
import sys
import os
import platform
import pdb
import re
import functools
import math
print (sys.path)
from datapump import Datapump
from collections import Counter
import logging
import traceback

# Get the logger for the main module
logger = logging.getLogger(__name__)

# Configure logging level
logger.setLevel(logging.DEBUG)

def handle_exception(exc_type, exc_value, exc_traceback): # Define the global exception handler function
    exc_info = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    logger.error("Uncaught Exception: %s", exc_info)

sys.excepthook = handle_exception  #Set the global exception handler

# create file handler which logs debug messages
fh = logging.FileHandler('ExportImport.log')
fh.setLevel(logging.DEBUG)

# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

logger.propagate = False

logger.info(f"Starting export and/or import of SQL Plans and SQL Profiles")

if sys.maxsize > 2147483647:
  logger.info(f"Running in 64-bit mode.")
else:

  logger.info(f"Not running in 64-bit mode.")

d = None  # default suitable for Linux
if platform.system() == "Windows":
    d = r"E:\Oracle\client\product\instant"
    #d =r"C:\oracle\product\instantclient_19_20"

# Initialize the Oracle client
oracledb.init_oracle_client(lib_dir=d)

class RunProcedure(Datapump):
    QUERY_TIMEOUT = 30
    LOGIN_TIMEOUT = 30

    def __init__(self):
        super().__init__()
        #self.min_percent = 3.0
        self.tns=""
        self.dns=""
        self.host_val=""
        self.port_val=0
        self.service_val=""
        self.user_val=""
        self.pword_val=""
        self.db_name=""
        self.connection= None
            

    def process_database(self, db):
        logging.info(f"NOTE: Processing {db.get_name()}, tns={db.get_tns()} ...")
        #print("Service name is :" , service_val)  
        self.db_name=db.get_name()
        dns=db.get_name()
        self.user_val=db.get_username()
        self.pword_val=db.get_password()
        
        try:
            #self.connection = oracledb.connect(user=self.user_val, password=self.pword_val, host=self.host_val, service_name=self.service_val, port=self.port_val)
            #The following Standalone connection works but will fail if i tried to add the timeout parameter
            #connection = oracledb.connect(user=self.user_val, password=self.pword_val, dsn=db.get_name())
            
            skip_db = ['DMSO','INTSTGO','TCC011N','TCC021N']
            if db.get_name().upper() in skip_db:
                logger.info(f"Skipping database {db.get_name()} as it is in skip_db list.")
                return
            #In order to be able to implement connection TIME_OUT i had to to use connection pooling as this is not avaiable with Standalone connections
            pool = oracledb.create_pool(user=self.user_val, password=self.pword_val, dsn=db.get_name(),min=1, max=5, increment=1, timeout=self.LOGIN_TIMEOUT)
            self.connection = pool.acquire()
        except oracledb.DatabaseError as error:
            logger.exception(f"Database connection error encountered:, {error}")
            db_available = 0 
            #print(f"CONN ERROR: {str(error)}")
            #print("Conn Details are:" , self.host_val, self.port_val,self.service_val)
            #logger.info(f"_datapoint:DatabaseAvailability:{db.get_name()} Availability:status:{db_available}")
            return
                   
        cursor = self.connection.cursor()
        cursor.arraysize = 1000
        db_available = 1                    
        self.run_procedure(db, self.datapump_type)
        cursor.close()
        self.connection.close()
        pool.close()
        #logger.info(f"_datapoint:DatabaseAvailability:{db.get_name()} Availability:status:{db_available}")
        
 
    
    def run_procedure(self, db, datapump_type):
        try:
            cursor = self.connection.cursor()
            cursor.arraysize = 1000 

            v_sql =""" 
                DECLARE dumpfile varchar2(60);
                name varchar2(100);
                db_role varchar2(50);
                file_handler UTL_FILE.FILE_TYPE;
                v_directory VARCHAR2(100);
                DIR varchar2(100);
                pump_type varchar2(20);
                PROCEDURE manage_errors IS
                    PRAGMA AUTONOMOUS_TRANSACTION;
                    l_code PLS_INTEGER := SQLCODE;
                    l_mesg VARCHAR2(32767) := SQLERRM;
                    BEGIN 
                    select 
                    directory_name into dir 
                    from 
                    dba_directories 
                    where 
                    DIRECTORY_NAME = 'KEEP_INFO_REFRESH';
                    select name into name from v$database;
                    -- Open file for writing
                    file_handler := UTL_FILE.FOPEN(dir, 'error_log.csv', 'A');
                    -- 'A' for append mode
                    -- Write the log entry
                    UTL_FILE.PUT_LINE(
                    file_handler,name||'|'|| l_code || '|' || l_mesg || '|' || sys.DBMS_UTILITY.format_error_backtrace || '|' || sys.DBMS_UTILITY.format_call_stack || '|' || SYSDATE || '|' || USER
                    );
                    -- Close the file
                    UTL_FILE.FCLOSE(file_handler);
                END manage_errors;

                Procedure exp_stg_table(t_name in varchar2) IS 
                    stg_name  varchar2(25) := t_name;
                    l_dp_handle number;
                    sql_stmt varchar2(500);
                    time_piece varchar2(100);
                    job_name varchar2(50);
                    logfile varchar2(60);
                    directory varchar2(20);
                    show_me date;
                    ind NUMBER;
                    -- Loop index
                    job_state VARCHAR2(30);
                    -- To keep track of job state
                    le ku$_LogEntry;
                    -- For WIP and error messages
                    js ku$_JobStatus;
                    -- The job status from get_status
                    jd ku$_JobDesc;
                    -- The job description from get_status
                    sts ku$_Status;
                    -- The status object returned by get_status
                    begin sql_stmt := 'alter session set nls_date_format=''DD_MON_YYYY_HH24_MI_SS''';
                    execute immediate sql_stmt;
                    select 
                    name into name 
                    from 
                    v$database;
                    select 
                    to_char(sysdate) into time_piece 
                    from 
                    dual;
                    JOB_NAME := 'EXP_STG_TABLE_'||time_piece;
                    dbms_output.put_line('Job name is :'||JOB_NAME);
    
                    -- Open a table export job.
                    l_dp_handle := dbms_datapump.open(
                    operation => 'EXPORT', job_mode => 'TABLE', 
                    remote_link => NULL, job_name => JOB_NAME, 
                    version => 'LATEST'
                    );
                    dumpfile := name||'_'||pump_type||'_'||time_piece || '.dmp';
                    directory := 'KEEP_INFO_REFRESH';
                    -- Specify the dump file name and directory object name.
                    dbms_datapump.add_file(
                    handle => l_dp_handle, filename => dumpfile, 
                    directory => directory
                    );
                    logfile := name||'_'||time_piece || '.log';
                    -- Specify the log file name and directory object name.
                    dbms_datapump.add_file(
                    handle => l_dp_handle, filename => logfile, 
                    directory => directory, filetype => DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE
                    );
                    -- Specify the table to be exported, filtering the schema and table.
                    dbms_datapump.metadata_filter(
                    handle => l_dp_handle, name => 'SCHEMA_EXPR', 
                    value => '= ''HP_DIAG'''
                    );
                    dbms_datapump.metadata_filter(
                    handle => l_dp_handle, name => 'NAME_EXPR', 
                    value => '= ''' || stg_name || ''''
                    );
                    dbms_datapump.start_job(l_dp_handle);
                    IF stg_name = 'STAGE_PLAN' THEN dbms_output.put_line(
                    'Datapump export operation started for SQL PLAN Baselines stored in the Staging table ' || stg_name || '...'
                    );
                    ELSIF stg_name = 'STAGE_PROF' THEN dbms_output.put_line(
                    'Datapump export operation started for SQL Profiles stored in the Staging table ' || stg_name || '...'
                    );
                    END IF;
                    while (job_state != 'COMPLETED') 
                    and (job_state != 'STOPPED') loop dbms_datapump.get_status(
                    l_dp_handle, dbms_datapump.ku$_status_job_error + dbms_datapump.ku$_status_job_status + dbms_datapump.ku$_status_wip, 
                    -1, job_state, sts
                    );
                    js := sts.job_status;
                    -- If any work-in-progress (WIP) or error messages were received for the job, we show them.
                    if (
                    bitand(
                        sts.mask, dbms_datapump.ku$_status_wip
                    ) != 0
                    ) then le := sts.wip;
                    else if (
                    bitand(
                        sts.mask, dbms_datapump.ku$_status_job_error
                    ) != 0
                    ) then le := sts.error;
                    else le := null;
                    end if;
                    end if;
                    if le is not null then ind := le.FIRST;
                    while ind is not null loop dbms_output.put_line(
                    le(ind).LogText
                    );
                    ind := le.NEXT(ind);
                    end loop;
                    end if;
                    end loop;
                    dbms_datapump.detach(l_dp_handle);
                    -- Open file for appending
                    file_handler := UTL_FILE.FOPEN(
                    directory, 'datapump_log.csv', 'A'
                    );
                    -- Write the error log entry to the file (CSV format)
                    UTL_FILE.PUT_LINE(
                    file_handler, name || ',' || to_char(sysdate) || ',' || directory || ',' || dumpfile || ',export,' || stg_name
                    );
                    -- Close the file
                    UTL_FILE.FCLOSE(file_handler);
                    IF stg_name = 'STAGE_PLAN' THEN dbms_output.put_line(
                    'Datapump export operation completed for SQL PLAN Baselines stored in the Staging table ' || stg_name || '...'
                    );
                    ELSIF stg_name = 'STAGE_PROF' THEN dbms_output.put_line(
                    'Datapump export operation completed for SQL Profiles stored in the Staging table ' || stg_name || '...'
                    );
                    END IF;
                    EXCEPTION WHEN OTHERS THEN
                    manage_errors();
                    RAISE;

                end exp_stg_table;
                
                Procedure imp_stg_table(t_name in varchar2) IS 
                    stg_name varchar2(25) := t_name;
                    l_dp_handle number;
                    sql_stmt varchar2(500);
                    time_piece varchar2(100);
                    job_name varchar2(50);
                    logfile varchar2(60);
                    directory varchar2(20);
                    dumpfile varchar2(250);
                    ind NUMBER;
                    check_tab number;
                    -- Loop index
                    job_state VARCHAR2(30);
                    -- To keep track of job state
                    le ku$_LogEntry;
                    -- For WIP and error messages
                    js ku$_JobStatus;
                    -- The job status from get_status
                    jd ku$_JobDesc;
                    -- The job description from get_status
                    sts ku$_Status;
                    -- The status object returned by get_status
                    begin 
                    select 
                    name into name 
                    from 
                    v$database;
                    IF upper(stg_name) not in ('STAGE_PLAN', 'STAGE_PROF') THEN RAISE_APPLICATION_ERROR(
                    -20001, 'Invalid input type for procedure imp_stg_table. You have not input the right table to be imported'
                    );
                    END IF;
                    dbms_output.put_line('Table name received inside the datapump API was '||stg_name);
                    sql_stmt := 'alter session set nls_date_format=''DD_MON_YYYY_HH24_MI_SS''';
                    execute immediate sql_stmt;
                    select 
                    to_char(sysdate) into time_piece 
                    from 
                    dual;
                    JOB_NAME := 'IMP_STG_TABLE_' || time_piece;

                    --we query the datapump log table in order to fetch the latest dumpfile that was exported
                    
                   EXECUTE IMMEDIATE '
                    SELECT 
                        directory, 
                        dumpfile
                    FROM 
                        (SELECT directory, dumpfile
                        FROM HP_DIAG.datapump_log 
                        WHERE database_name = :name 
                        AND table_name = :stg_name 
                        AND type = ''export''
                        ORDER BY TO_DATE(TIMESTAMP, ''DD_MON_YYYY_HH24_MI_SS'') DESC
                        ) 
                    WHERE ROWNUM = 1'
                    INTO directory, dumpfile
                    USING name, stg_name;


                    -- Open a table import job.
                    l_dp_handle := dbms_datapump.open(
                    operation => 'IMPORT', job_mode => 'TABLE', 
                    remote_link => NULL, job_name => JOB_NAME, 
                    version => 'LATEST'
                    );
                    -- Specify the dump file name and directory object name.
                    dbms_datapump.add_file(
                    handle => l_dp_handle, filename => dumpfile, 
                    directory => directory
                    );
                    logfile := name || '_' || time_piece || '.log';
                    -- Specify the log file name and directory object name.
                    dbms_datapump.add_file(
                    handle => l_dp_handle, filename => logfile, 
                    directory => directory, filetype => DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE
                    );
                    --if staging table already exists then replace it
                    DBMS_DATAPUMP.SET_PARAMETER(
                    l_dp_handle, 'TABLE_EXISTS_ACTION', 
                    'REPLACE'
                    );
                    dbms_datapump.start_job(l_dp_handle);
                    while (job_state != 'COMPLETED') 
                    and (job_state != 'STOPPED') loop dbms_datapump.get_status(
                    l_dp_handle, dbms_datapump.ku$_status_job_error + dbms_datapump.ku$_status_job_status + dbms_datapump.ku$_status_wip, 
                    -1, job_state, sts
                    );
                    js := sts.job_status;
                    -- If any work-in-progress (WIP) or error messages were received for the job, we show them.
                    if (
                    bitand(
                        sts.mask, dbms_datapump.ku$_status_wip
                    ) != 0
                    ) then le := sts.wip;
                    else if (
                    bitand(
                        sts.mask, dbms_datapump.ku$_status_job_error
                    ) != 0
                    ) then le := sts.error;
                    else le := null;
                    end if;
                    end if;
                    if le is not null then ind := le.FIRST;
                    while ind is not null loop dbms_output.put_line(
                    le(ind).LogText
                    );
                    ind := le.NEXT(ind);
                    end loop;
                    end if;
                    end loop;
                    dbms_datapump.detach(l_dp_handle);
                    dbms_output.put_line(
                    'Datapump Import job has completed for database ' || name
                    );
                    --we populate the export log xternal table
                    -- Get directory path from DBA_DIRECTORIES
                    --SELECT directory_path INTO dir FROM dba_directories WHERE DIRECTORY_NAME = 'KEEP_INFO_REFRESH';
                    -- Set the full directory path for the error log
                    
                    -- Open file for appending
                    file_handler := UTL_FILE.FOPEN(
                    directory, 'datapump_log.csv', 'A'
                    );
                    UTL_FILE.PUT_LINE(
                    file_handler, name || ',' || to_char(sysdate,'DD_MON_YYYY_HH24_MI_SS') || ',' || directory || ',' || dumpfile || ',import,' || stg_name
                    );
                    -- Close the file
                    UTL_FILE.FCLOSE(file_handler);
                    EXCEPTION WHEN NO_DATA_FOUND THEN 
                    IF stg_name = 'STAGE_PLAN' THEN dbms_output.put_line(
                    'No SQL PLAN baseline imported for database ' || name
                    );
                    ELSIF stg_name = 'STAGE_PROF' THEN dbms_output.put_line(
                    'No SQL Profile imported for database ' || name
                    );
                    END IF;
                    WHEN OTHERS THEN manage_errors();
                    RAISE;
                end imp_stg_table;

                FUNCTION create_stg_tables(p_type VARCHAR2) RETURN VARCHAR2 IS 
                    tab_name VARCHAR2(20);
                    check_tab NUMBER;
                    BEGIN -- Determine table name based on the input type
                    IF upper(p_type) = 'PROFILE' THEN 
                        tab_name := 'STAGE_PROF';
                        pump_type := p_type;
                    ELSIF upper(p_type) = 'PLAN' THEN 
                        tab_name := 'STAGE_PLAN';
                        pump_type := p_type;
                    ELSE RAISE_APPLICATION_ERROR(
                    -20001, 'Invalid input type. Use PROFILE or PLAN as argument to function: create_stg_tables'
                    );
                    END IF;
                    -- Check if the table exists
                    SELECT 
                    COUNT(*) INTO check_tab 
                    FROM 
                    user_tables 
                    WHERE 
                    table_name = tab_name;
                    IF check_tab = 1 THEN -- If the table exists, truncate it
                    EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || tab_name;
                    DBMS_OUTPUT.PUT_LINE(
                    'Staging table ' || tab_name || ' has been truncated.'
                    );
                    ELSIF check_tab = 0 THEN -- If the table does not exist, create it
                    IF p_type = 'PROFILE' THEN DBMS_SQLTUNE.CREATE_STGTAB_SQLPROF(
                    table_name => tab_name, schema_name => 'HP_DIAG'
                    );
                    ELSIF p_type = 'PLAN' THEN 
                    DBMS_SPM.CREATE_STGTAB_BASELINE(
                    table_name => tab_name, table_owner => 'HP_DIAG'
                    );
                    END IF;
                    DBMS_OUTPUT.PUT_LINE(
                    'Staging table ' || tab_name || ' has been created.'
                    );
                    END IF;
                    RETURN tab_name;
                EXCEPTION WHEN OTHERS THEN manage_errors();
                    RAISE;
                END create_stg_tables;

                Procedure export_profiles IS 
                    stag_tab varchar2(20);
                    num_sql_profiles number;
                    time_piece varchar2(35);
                    cnt number;
                    sql_stmt varchar2(1000);
                    --store all the SQL Profiles into the c1 cursor
                    cursor c1 IS 
                    select 
                    name 
                    from 
                    dba_sql_profiles;
                    begin 
                    select 
                    name into name 
                    from 
                    v$database;
                    --Here we make sure script can only be run from a Primary database.
                    select 
                    DATABASE_ROLE into db_role 
                    from 
                    v$database;
                    IF db_role != 'PRIMARY' then raise_application_error(
                    -20002, 'Database ' || name || ' is a standby. Deployment cannot continue.'
                    );
                    return;
                    end if;
                    -- Check if there are any SQL profiles in the database
                    SELECT 
                    COUNT(*) INTO num_sql_profiles 
                    FROM 
                    dba_sql_profiles;
                    -- If no SQL profiles are found, exit the procedure
                    IF num_sql_profiles = 0 THEN dbms_output.put_line(
                    'No SQL profiles found in the database ' || name
                    );
                    RETURN;
                    END IF;
                    --we call the function to configure the staging tables
                    stag_tab := create_stg_tables('PROFILE');
                    sql_stmt := 'select count(*) from HP_DIAG.' || stag_tab;
                    FOR prof_rec in c1 LOOP cnt := c1 % ROWCOUNT;
                    DBMS_SQLTUNE.PACK_STGTAB_SQLPROF(
                    staging_table_name => stag_tab, profile_name => prof_rec.name
                    );
                    END LOOP;
                    execute immediate sql_stmt into num_sql_profiles;
                    --select count(*) into num_profiles from HP_DIAG.stag_tab;
                    IF num_sql_profiles > 0 then dbms_output.put_line(
                    cnt || ' SQL profiles have been stored into the staging table ' || stag_tab || 'for database ' || name
                    );
                    --next, call procedure to create datapump dumpfile for the table backup
                    exp_stg_table(stag_tab);
                    END IF;
                EXCEPTION WHEN OTHERS THEN manage_errors();
                    RAISE;
                END export_profiles;

                Procedure export_plan_baselines IS 
                    stag_tab varchar2(20);
                    num_sql_plans number;
                    time_piece varchar2(35);
                    cnt number;
                    p number;
                    sql_stmt varchar2(1000);
                    --store all the SQL Plan baselines into the c1 cursor
                    cursor c1 IS 
                    select 
                    plan_name 
                    from 
                    dba_sql_plan_baselines;
                    begin 
                    select 
                    name into name 
                    from 
                    v$database;
                    --Here we make sure script can only be run from a Primary database.
                    select 
                    DATABASE_ROLE into db_role 
                    from 
                    v$database;
                    IF db_role != 'PRIMARY' then raise_application_error(
                    -20002, 'Database ' || name || ' is a standby. Deployment cannot continue.'
                    );
                    return;
                    end if;
                    -- Check if there are any SQL Plans in the database
                    SELECT 
                    COUNT(*) INTO num_sql_plans 
                    FROM 
                    dba_sql_plan_baselines;
                    -- If no SQL plans are found, exit the procedure
                    IF num_sql_plans = 0 THEN dbms_output.put_line(
                    'No SQL Plans found in the database ' || name
                    );
                    RETURN;
                    END IF;
                    --we call the function to configure the staging tables
                    stag_tab := create_stg_tables('PLAN');
                    sql_stmt := 'select count(*) from HP_DIAG.' || stag_tab;
                    FOR plan_rec in c1 LOOP cnt := c1 % ROWCOUNT;
                    p := DBMS_SPM.PACK_STGTAB_BASELINE(
                    table_name => stag_tab, plan_name => plan_rec.plan_name
                    );
                    END LOOP;
                    execute immediate sql_stmt into num_sql_plans;
                    IF num_sql_plans > 0 then dbms_output.put_line(
                    cnt || ' SQL plan baselines have been stored into the staging table ' || stag_tab || ' for database ' || name
                    );
                    --next, call procedure to create datapump dumpfile for the table backup
                    exp_stg_table(stag_tab);
                    END IF;
                EXCEPTION WHEN OTHERS THEN manage_errors();
                RAISE;
                END export_plan_baselines;
                Procedure import_profiles IS
                    cnt number := 0;
                    profile_name varchar2(100);
                    stmt varchar2(250);
                    rec SYS_REFCURSOR;
                    begin 
                    select 
                    name into name 
                    from 
                    v$database;
                    --Here we make sure script can only be run from a Primary database.
                    select 
                    DATABASE_ROLE into db_role 
                    from 
                    v$database;
                    IF db_role != 'PRIMARY' then raise_application_error(
                    -20002, 'Database ' || name || ' is a standby. Deployment cannot continue.'
                    );
                    return;
                    end if;
                    --we import the datapump dumpfile for most recently exported SQL profiles
                    imp_stg_table('STAGE_PROF');

                    --we import only the SQL Profiles that are not already in dba_sql_profiles using the c1 cursor
                    --if profile already exists then we skip it using the replace=FALSE parameter
                    --DBMS_SQLTUNE.UNPACK_STGTAB_SQLPROF(replace => FALSE,staging_table_name => 'STAGE_PROF',staging_schema_owner => 'HP_DIAG');
                    stmt := 'SELECT DISTINCT obj_name as profile_name FROM HP_DIAG.stage_prof WHERE obj_name NOT IN (SELECT name FROM dba_sql_profiles)';
                    
                    OPEN rec FOR stmt;
                    LOOP
                        FETCH rec INTO profile_name;
                        EXIT WHEN rec%NOTFOUND;
                        cnt := cnt + 1 ; -- Increments the counter for each row processed  --if nothing is returned we find out here                    
                        --dbms_output.put_line(cnt||' Testing the import_profile For Loop');
                        DBMS_SQLTUNE.UNPACK_STGTAB_SQLPROF(
                        profile_name => profile_name, 
                        replace => FALSE, staging_table_name => 'STAGE_PROF', 
                        staging_schema_owner => 'HP_DIAG'
                        );
                        --we disable each of the profiles that we have imported
                        DBMS_SQLTUNE.ALTER_SQL_PROFILE(
                        name => profile_name, attribute_name => 'STATUS', 
                        value => 'DISABLED'
                        );
                        dbms_output.put_line(
                        'SQL Profile ' || PROFILE_NAME || ' has been successfully imported and disabled for database ' || name
                        );
                    END LOOP;
                    IF cnt = 0 then dbms_output.put_line(
                        'No SQL profile has been imported! for database ' || name ||' probably because Profile already exists in the database or it never had one before.' );
                        dbms_output.put_line('');
                    END IF;
                EXCEPTION WHEN OTHERS THEN manage_errors();
                RAISE;
                END import_profiles;
                Procedure import_plan_baselines IS 
                    cnt number;
                    plan_name varchar2(250);
                    p number;
                    r number;
                    stmt varchar2(1000);
                    rec SYS_REFCURSOR;
                    sql_handle varchar2(250);
                    
                    begin 
                    select 
                    name into name 
                    from 
                    v$database;
                    --Here we make sure script can only be run from a Primary database.
                    select 
                    DATABASE_ROLE into db_role 
                    from 
                    v$database;
                    IF db_role != 'PRIMARY' then raise_application_error(
                    -20002, 'Database ' || name || ' is a standby. Deployment cannot continue.'
                    );
                    return;
                    end if;
                    --we import the datapump dumpfile for most recently exported SQL Plans
                    imp_stg_table('STAGE_PLAN');

                    stmt := 'select 
                    distinct obj_name plan_name, 
                    sql_handle 
                    from 
                    HP_DIAG.stage_plan 
                    where 
                    obj_name not in (
                        select 
                        plan_name 
                        from 
                        dba_sql_plan_baselines
                    )';
                    --we disable each of the plans that we have imported
                    OPEN  rec for stmt;
                      LOOP 
                        FETCH rec INTO plan_name, sql_handle;
                        EXIT WHEN rec%NOTFOUND;
                        cnt := cnt + 1 ; -- Increments the counter for each row processed  --if nothing is returned we find out here   --if nothing is returned we find out here
                        --dbms_output.put_line(cnt||' Testing the import_profile For Loop');
                        p := DBMS_SPM.UNPACK_STGTAB_BASELINE(
                        table_name => 'STAGE_PLAN', table_owner => 'HP_DIAG', 
                        creator => 'HP_DIAG', plan_name => plan_name
                        );
                        r := DBMS_SPM.ALTER_SQL_PLAN_BASELINE(
                        sql_handle => sql_handle, plan_name => plan_name, 
                        attribute_name => 'enabled', attribute_value => 'NO'
                        );
                        dbms_output.put_line(
                        'SQL Plan ' || plan_name || ' has been successfully imported and disabled for database ' || name
                        );
                    END LOOP;
                    IF cnt is NULL then dbms_output.put_line(
                    'No SQL plan has been imported for database ' || name ||' probably because baseline(s) already exists in the database or it never had one before. '
                    );
                    --dbms_output.put_line('Probably because they already exist in the database');
                    END IF;
                EXCEPTION WHEN OTHERS THEN manage_errors();
                    RAISE;
                END import_plan_baselines;
                
                begin 
                    DBMS_OUTPUT.ENABLE(1000000);
                    select name into name from v$database;
                    DBMS_OUTPUT.PUT_LINE(name||' started');
                    
                    IF : datapump_type = 'export' THEN 
                        export_plan_baselines;
                        export_profiles;
                    ELSIF : datapump_type = 'import' THEN 
                        import_plan_baselines;
                        import_profiles;
                    END IF;
                    DBMS_OUTPUT.PUT_LINE(name||' ended');
                    dbms_output.put_line('');
                end;

            """

            # Execute the script, passing in the datapump_type as a bind variable
            # by using a dictionary, where the keys represent the placeholders in the SQL
            
            cursor.execute(v_sql, {"datapump_type": datapump_type})   
            
            # Fetch and print DBMS_OUTPUT messages (if any)
            self.print_dbms_output(cursor)
            #logger.info(f"{db.get_name()} ended...")
        except oracledb.DatabaseError as e:
            logger.error("Database error occurred during PL/SQL execution phase.")
            try:
                error, = e.args
                # Check for ORA-31634 specific error
                if 'ORA-31634' in error.message:
                    logger.error(f"Job already exists : ORA-31634 flagged for database  {db.get_name()} - Please check for any orphaned jobs in DBA_DATAPUMP_JOBS!")
                else:
                    logger.error(f"Oracle error for database {self.db_name}: {error.message}")
            except Exception as inner:
                logger.exception("Failed while unpacking or logging Oracle error for database {self.db_name}.")
        except Exception as e:
            logger.exception(f"General error for database {self.db_name}: {str(e)}")
           
        finally:
            cursor.close()  # Ensure the cursor is closed after operation 

    def print_dbms_output(self, cursor):
        # tune this size for your application
        chunk_size = 100
        # create variables to hold the output
        lines_var = cursor.arrayvar(str, chunk_size)
        num_lines_var = cursor.var(int)
        num_lines_var.setvalue(0, chunk_size)

        #fetch the text that was added by the procedure
        while True:
            cursor.callproc("dbms_output.get_lines", (lines_var, num_lines_var))
            num_lines = num_lines_var.getvalue()
            lines = lines_var.getvalue()[:num_lines]
            for line in lines:
                print(line or "")
            if num_lines < chunk_size:
                break
    

    def check_args(self, args):
        if args is not None:
            try:              
                # Ensure that the value string supplied is either 'export' or 'import', and is not empty
                self.datapump_type = args[2:]
                size = len(self.datapump_type)
                self.datapump_type = self.datapump_type[1]
                if hasattr(self, 'datapump_type') and self.datapump_type:
                    #logger.info(f"-a value supplied is {self.datapump_type}")
                    logger.debug(f"datapump_type before stripping: '{self.datapump_type}'")
                    if not self.datapump_type.strip():  # Check if the string is empty after stripping whitespace
                        raise ValueError("Argument -a cannot be an empty string.")
                    return True  # Return True when parsing is successful                               
                    
            except Exception as e:
                logger.exception(f"Floating error is :, {e}")
                return False
        

    def run(self):
        logger.info(f"NOTE: QUERY TIMEOUT      = {self.QUERY_TIMEOUT} seconds")
        logger.info(f"NOTE: CONNECTION TIMEOUT = {self.LOGIN_TIMEOUT} seconds")
        args = sys.argv[1:]  # Get command-line arguments here
        #print("The arguments:", args)
        logger.info(f"The arguments are: {args}")
        if self.check_args(args):
            if self.populate():
                logger.info(f"NOTE: MINIMUM FREE PERCENT  = {self.min_percent}")
                logger.info(f"DEBUG: Number of databases being checked:, {len(self.databases)}")
                for db in self.databases:  # Loop through the databases    
                    self.process_database(db)       
            else:
                logger.info(f"Failed to extract the command line aruguments into variables")

if __name__ == "__main__":
    data_pump = RunProcedure()
    #print("Entering run() method for main class.")
    data_pump.run()
