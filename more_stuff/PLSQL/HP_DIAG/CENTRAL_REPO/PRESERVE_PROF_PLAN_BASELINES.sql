CREATE OR REPLACE package           preserve_prof_plan_baselines
                    AS
                    dumpfile varchar2(60);
                    name varchar2(100);
                    db_role varchar2(50);
                    Procedure export_profiles;
                    Procedure export_plan_baselines;
                    Procedure import_profiles;
                    Procedure import_plan_baselines;
                    Procedure exp_stg_table(t_name in varchar2);
                    Procedure imp_stg_table(t_name in varchar2);

                    end preserve_prof_plan_baselines;
            
/


create or replace package body         preserve_prof_plan_baselines
                AS
                Procedure export_profiles
                IS
                    stag_tab varchar2(20);
                    num_sql_profiles number;
                    time_piece varchar2(35);
                    cnt number;
                    sql_stmt varchar2(1000);
                --store all the SQL Profiles into the c1 cursor
                cursor c1 IS
                    select name from dba_sql_profiles;
                begin
                    select name into name from v$database;
                    --Here we make sure script can only be run from a Primary database.
                    select DATABASE_ROLE into db_role from v$database;
                    IF db_role !='PRIMARY' then
                        raise_application_error(-20002,'Database '||name||' is a standby. Deployment cannot continue.');
                        return;
                    end if;
                     -- Check if there are any SQL profiles in the database
                    SELECT COUNT(*) INTO num_sql_profiles FROM dba_sql_profiles;

                    -- If no SQL profiles are found, exit the procedure
                    IF num_sql_profiles = 0 THEN
                        dbms_output.put_line('No SQL profiles found in the database '||name);
                        dbms_output.put_line('');
                        RETURN;
                    END IF;
                    --we call the function to configure the staging tables
                    stag_tab := HP_DIAG.create_stg_tables('PROFILE');
                    sql_stmt := 'select count(*) from HP_DIAG.'||stag_tab;
                    FOR prof_rec in c1
                    LOOP
                        cnt := c1%ROWCOUNT;
                        DBMS_SQLTUNE.PACK_STGTAB_SQLPROF(staging_table_name => stag_tab, profile_name=>prof_rec.name);
                    END LOOP;
                    execute immediate sql_stmt into num_sql_profiles;
                    --select count(*) into num_profiles from HP_DIAG.stag_tab;
                    IF num_sql_profiles > 0 then
                        dbms_output.put_line(cnt||' SQL profiles have been stored into the staging table '||stag_tab ||'for database '||name);
                        --next, call procedure to create datapump dumpfile for the table backup
                        exp_stg_table(stag_tab);
                    END IF;

                EXCEPTION
                    WHEN OTHERS THEN
                        manage_errors();
                        RAISE;

                END export_profiles;

                Procedure export_plan_baselines
                IS
                    stag_tab varchar2(20);
                    num_sql_plans number;
                    time_piece varchar2(35);
                    cnt number;
                    p number;
                    sql_stmt varchar2(1000);
                --store all the SQL Plan baselines into the c1 cursor
                cursor c1 IS
                    select plan_name from dba_sql_plan_baselines;
                begin
                    select name into name from v$database;
                    --Here we make sure script can only be run from a Primary database.
                    select DATABASE_ROLE into db_role from v$database;
                    IF db_role !='PRIMARY' then
                        raise_application_error(-20002,'Database '||name||' is a standby. Deployment cannot continue.');
                        return;
                    end if;
                     -- Check if there are any SQL Plans in the database
                    SELECT COUNT(*) INTO num_sql_plans FROM dba_sql_plan_baselines;

                    -- If no SQL plans are found, exit the procedure
                    IF num_sql_plans = 0 THEN
                        dbms_output.put_line('No SQL Plans found in the database '||name);
                        RETURN;
                    END IF;

                    --we call the function to configure the staging tables
                    stag_tab := HP_DIAG.create_stg_tables('PLAN');
                    sql_stmt := 'select count(*) from HP_DIAG.'||stag_tab;
                    FOR plan_rec in c1
                    LOOP
                        cnt := c1%ROWCOUNT;
                        p := DBMS_SPM.PACK_STGTAB_BASELINE(table_name =>stag_tab,plan_name=>plan_rec.plan_name);
                    END LOOP;
                    execute immediate sql_stmt into num_sql_plans;

                    IF num_sql_plans > 0 then
                        dbms_output.put_line(cnt||' SQL plan baselines have been stored into the staging table '||stag_tab||' for database '||name);
                        --next, call procedure to create datapump dumpfile for the table backup
                        exp_stg_table(stag_tab);
                    END IF;

                EXCEPTION
                    WHEN OTHERS THEN
                        manage_errors();
                        RAISE;

                END export_plan_baselines;

                Procedure import_profiles
                IS
                    cnt number;
                    profile_name varchar2(100);
                    --we check for SQL profiles that exist in the staging table but not in dba_sql_profiles
                    --a match implies this was on the database prior to the refresh and must have been set by the NFT DBAs
                cursor c1 IS
                    select distinct obj_name profile_name from HP_DIAG.stage_prof
                    where obj_name not in (select name from  dba_sql_profiles);
                begin
                    select name into name from v$database;
                    --Here we make sure script can only be run from a Primary database.
                    select DATABASE_ROLE into db_role from v$database;
                    IF db_role !='PRIMARY' then
                        raise_application_error(-20002,'Database '||name||' is a standby. Deployment cannot continue.');
                        return;
                    end if;
                    --we import the datapump dumpfile for most recently exported SQL profiles
                    imp_stg_table('STAGE_PROF');
                    --we import only the SQL Profiles that are not already in dba_sql_profiles using the c1 cursor
                    --if profile already exists then we skip it using the replace=FALSE parameter
                    --this(replace=FALSE) is useless anyway as the c1 cursor should take care of that,so we comment it out
                    --DBMS_SQLTUNE.UNPACK_STGTAB_SQLPROF(replace => FALSE,staging_table_name => 'STAGE_PROF',staging_schema_owner => 'HP_DIAG');

                    --we disable each of the profiles that we have imported
                    FOR rec in c1
                    LOOP
                        cnt := c1%ROWCOUNT;   --if nothing is returned we find out here
                        --dbms_output.put_line(cnt||' Testing the import_profile For Loop');
                        DBMS_SQLTUNE.UNPACK_STGTAB_SQLPROF(profile_name => rec.profile_name ,replace => FALSE, staging_table_name => 'STAGE_PROF', staging_schema_owner => 'HP_DIAG');
                        DBMS_SQLTUNE.ALTER_SQL_PROFILE(name => rec.profile_name, attribute_name => 'STATUS', value => 'DISABLED');
                        dbms_output.put_line('SQL Profile '||rec.PROFILE_NAME||' has been successfully imported and disabled for database '||name);
                    END LOOP;

                    IF cnt is NULL then
                        dbms_output.put_line('No SQL profile has been imported! for database '||name);
                        dbms_output.put_line('');
                    END IF;

                EXCEPTION
                    WHEN OTHERS THEN
                        manage_errors();
                        RAISE;

                END import_profiles;

                Procedure import_plan_baselines
                IS
                    cnt number;
                    plan_name varchar2(100);
                    p number;
                    r number;
                    --we check for SQL plans that exist in the staging table but not in dba_sql_profiles
                    --a match implies this was on the database prior to the refresh and must have been set by the NFT DBAs
                    cursor c1 IS
                        select distinct obj_name plan_name, sql_handle from HP_DIAG.stage_plan
                        where obj_name not in (select plan_name from  dba_sql_plan_baselines);
                begin
                    select name into name from v$database;
                    --Here we make sure script can only be run from a Primary database.
                    select DATABASE_ROLE into db_role from v$database;
                    IF db_role !='PRIMARY' then
                        raise_application_error(-20002,'Database '||name||' is a standby. Deployment cannot continue.');
                        return;
                    end if;
                    --we import the datapump dumpfile for most recently exported SQL Plans
                    imp_stg_table('STAGE_PLAN');
                    --we disable each of the plans that we have imported
                    FOR rec in c1
                    LOOP
                        cnt := c1%ROWCOUNT;   --if nothing is returned we find out here
                        --dbms_output.put_line(cnt||' Testing the import_profile For Loop');
                        p := DBMS_SPM.UNPACK_STGTAB_BASELINE(table_name => 'STAGE_PLAN', table_owner => 'HP_DIAG',creator => 'HP_DIAG', plan_name => rec.plan_name);
                        r := DBMS_SPM.ALTER_SQL_PLAN_BASELINE(sql_handle => rec.sql_handle, plan_name => rec.plan_name,  attribute_name=> 'enabled',  attribute_value=>'NO');
                        dbms_output.put_line('SQL Plan '||rec.plan_name||' has been successfully imported and disabled for database '||name);
                    END LOOP;

                    IF cnt is NULL then
                        dbms_output.put_line('No SQL plan has been imported for database '||name);
                        --dbms_output.put_line('Probably because they already exist in the database');
                    END IF;

                EXCEPTION
                    WHEN OTHERS THEN
                        manage_errors();
                        RAISE;

                END import_plan_baselines;


                Procedure exp_stg_table(t_name in varchar2)
                IS
                  stg_name varchar2(25) := t_name;
                  l_dp_handle       number;
                  sql_stmt varchar2(500);
                  time_piece varchar2(100);
                  job_name varchar2(50);
                  logfile varchar2(60);
                  directory varchar2(20);
                  ind NUMBER;              -- Loop index
                  job_state VARCHAR2(30);  -- To keep track of job state
                  le ku$_LogEntry;         -- For WIP and error messages
                  js ku$_JobStatus;        -- The job status from get_status
                  jd ku$_JobDesc;          -- The job description from get_status
                  sts ku$_Status;          -- The status object returned by get_status
                begin
                    sql_stmt :='alter session set nls_date_format=''DD_MON_YYYY_HH24_MI_SS''';
                    execute immediate sql_stmt;
                    select to_char(sysdate) into time_piece from dual;
                    JOB_NAME := 'EXP_STG_TABLE_'||time_piece;
                    -- Open a table export job.
                    l_dp_handle := dbms_datapump.open(
                        operation   => 'EXPORT',
                        job_mode    => 'TABLE',
                        remote_link => NULL,
                        job_name    => JOB_NAME,
                        version     => 'LATEST');

                    dumpfile := time_piece||'.dmp';
                    directory :='TEMP';
                    -- Specify the dump file name and directory object name.
                    dbms_datapump.add_file(
                        handle    => l_dp_handle,
                        filename  => dumpfile,
                        directory => directory);
                    logfile := time_piece||'.log';
                    -- Specify the log file name and directory object name.
                    dbms_datapump.add_file(
                        handle    => l_dp_handle,
                        filename  => logfile,
                        directory => 'TEMP',
                        filetype  => DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE);

                    -- Specify the table to be exported, filtering the schema and table.
                    dbms_datapump.metadata_filter(
                        handle => l_dp_handle,
                        name   => 'SCHEMA_EXPR',
                        value  => '= ''HP_DIAG''');

                    dbms_datapump.metadata_filter(
                        handle => l_dp_handle,
                        name   => 'NAME_EXPR',
                        value  => '= '''||stg_name||'''');

                    dbms_datapump.start_job(l_dp_handle);
                    IF stg_name ='STAGE_PLAN' THEN
                        dbms_output.put_line('Datapump export operation started for SQL PLAN Baselines stored in the Staging table '||stg_name||'...');
                    ELSIF stg_name ='STAGE_PROF' THEN
                        dbms_output.put_line('Datapump export operation started for SQL Profiles stored in the Staging table '||stg_name||'...');
                    END IF;

                    while (job_state != 'COMPLETED') and (job_state != 'STOPPED') loop
                    dbms_datapump.get_status(l_dp_handle,
                           dbms_datapump.ku$_status_job_error +
                           dbms_datapump.ku$_status_job_status +
                           dbms_datapump.ku$_status_wip,-1,job_state,sts);
                    js := sts.job_status;

                    -- If any work-in-progress (WIP) or error messages were received for the job, we show them.

                   if (bitand(sts.mask,dbms_datapump.ku$_status_wip) != 0)
                    then
                      le := sts.wip;
                    else
                      if (bitand(sts.mask,dbms_datapump.ku$_status_job_error) != 0)
                      then
                        le := sts.error;
                      else
                        le := null;
                      end if;
                    end if;
                    if le is not null
                    then
                      ind := le.FIRST;
                      while ind is not null loop
                        dbms_output.put_line(le(ind).LogText);
                        ind := le.NEXT(ind);
                      end loop;
                    end if;
                  end loop;

                    dbms_datapump.detach(l_dp_handle);

                    --we populate the export log table
                    insert into HP_DIAG.datapump_log values(sysdate,directory,dumpfile,'export',stg_name);
                    commit;
                    IF stg_name ='STAGE_PLAN' THEN
                        dbms_output.put_line('Datapump export operation completed for SQL PLAN Baselines stored in the Staging table '||stg_name||'...');
                    ELSIF stg_name ='STAGE_PROF' THEN
                        dbms_output.put_line('Datapump export operation completed for SQL Profiles stored in the Staging table '||stg_name||'...');
                        dbms_output.put_line('');
                    END IF;

                end exp_stg_table;

                Procedure imp_stg_table(t_name in varchar2)
                IS
                  stg_name varchar2(25) := t_name;
                  l_dp_handle       number;
                  sql_stmt varchar2(500);
                  time_piece varchar2(100);
                  job_name varchar2(50);
                  logfile varchar2(60);
                  directory varchar2(20);
                  ind NUMBER;              -- Loop index
                  job_state VARCHAR2(30);  -- To keep track of job state
                  le ku$_LogEntry;         -- For WIP and error messages
                  js ku$_JobStatus;        -- The job status from get_status
                  jd ku$_JobDesc;          -- The job description from get_status
                  sts ku$_Status;          -- The status object returned by get_status
                begin
                    select name into name from v$database;
                    IF upper(stg_name) not in ('STAGE_PLAN','STAGE_PROF') THEN
                        RAISE_APPLICATION_ERROR(-20001, 'Invalid input type for procedure imp_stg_table. You have not input the right table to be imported');
                    END IF;
                    --dbms_output.put_line('Table name recived inside the datapump API was '||stg_name);
                    sql_stmt :='alter session set nls_date_format=''DD_MON_YYYY_HH24_MI_SS''';
                    execute immediate sql_stmt;
                    select to_char(sysdate) into time_piece from dual;
                    JOB_NAME := 'IMP_STG_TABLE_'||time_piece;

                    --we query the datapump log table in order to fetch the latest dumpfile that was exported
                    select directory,dumpfile into directory,dumpfile from(
                        select * from HP_DIAG.datapump_log
                        where table_name=stg_name  --there is one unique staging table per SQL profiles and per SQL plans. We don't want to mixup the dumpfiles.
                        and type='export'
                        order by timestamp desc)
                        where rownum=1;
                    -- Open a table import job.
                    l_dp_handle := dbms_datapump.open(
                        operation   => 'IMPORT',
                        job_mode    => 'TABLE',
                        remote_link => NULL,
                        job_name    => JOB_NAME,
                        version     => 'LATEST');

                    -- Specify the dump file name and directory object name.
                    dbms_datapump.add_file(
                        handle    => l_dp_handle,
                        filename  => dumpfile,
                        directory => directory);

                    logfile := time_piece||'.log';
                    -- Specify the log file name and directory object name.
                    dbms_datapump.add_file(
                        handle    => l_dp_handle,
                        filename  => logfile,
                        directory => directory,
                        filetype  => DBMS_DATAPUMP.KU$_FILE_TYPE_LOG_FILE);

                    --if staging table already exists then replace it
                    DBMS_DATAPUMP.SET_PARAMETER(l_dp_handle,'TABLE_EXISTS_ACTION','REPLACE');

                    dbms_datapump.start_job(l_dp_handle);

                    while (job_state != 'COMPLETED') and (job_state != 'STOPPED') loop
                        dbms_datapump.get_status(l_dp_handle,
                               dbms_datapump.ku$_status_job_error +
                               dbms_datapump.ku$_status_job_status +
                               dbms_datapump.ku$_status_wip,-1,job_state,sts);
                        js := sts.job_status;

                        -- If any work-in-progress (WIP) or error messages were received for the job, we show them.

                       if (bitand(sts.mask,dbms_datapump.ku$_status_wip) != 0)
                        then
                          le := sts.wip;
                        else
                          if (bitand(sts.mask,dbms_datapump.ku$_status_job_error) != 0)
                          then
                            le := sts.error;
                          else
                            le := null;
                          end if;
                        end if;
                        if le is not null
                        then
                          ind := le.FIRST;
                          while ind is not null loop
                            dbms_output.put_line(le(ind).LogText);
                            ind := le.NEXT(ind);
                          end loop;
                        end if;
                    end loop;

                    dbms_datapump.detach(l_dp_handle);
                    dbms_output.put_line('Import job has completed for database '||name);
                    insert into HP_DIAG.datapump_log values(sysdate,directory,dumpfile,'import',stg_name);
                    commit;
                EXCEPTION WHEN NO_DATA_FOUND THEN
                    IF stg_name = 'STAGE_PLAN' THEN
                        dbms_output.put_line('There is no SQL PLAN baseline dumpfile to import for database '||name);
                    ELSIF stg_name = 'STAGE_PROF' THEN
                        dbms_output.put_line('There is no SQL Profile dumpfile to import for database '||name);
                    END IF;


                end imp_stg_table;
                end preserve_prof_plan_baselines;
            /