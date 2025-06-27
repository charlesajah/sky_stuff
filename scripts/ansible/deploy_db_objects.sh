#!/bin/bash
#############################################################################################
# Name : onxsmpgpdbn05:/apps/ora/home/ansible/files/deploy_tactical_tspace_monitoring_n02.sh
# Purpose : for setting up tactical_tspace_monitoring for a database
# Change History :
#    08-May-2025 Charles Ajah initial version
#############################################################################################
export ORACLE_SID=$1
if [ ! -z $2 ]
then
  l_pdbCommand="alter session set container = $2 ;"
fi

export ORAENV_ASK=NO
. oraenv > /dev/null

unset ORAENV_ASK

sqlplus -s /nolog << END_SQL
conn / as sysdba
$l_pdbCommand
set serveroutput on feedback off pages 9999 lines 130
DECLARE
   l_name varchar2(100);
   l_count number;
   l_sqltxt  varchar2(4000);
BEGIN
    select name into l_name from v\$database;
    DBMS_OUTPUT.PUT_LINE('.......................................................................................................');
    DBMS_OUTPUT.PUT_LINE('Executing on database '||l_name);
    IF sys_context ( 'userenv' , 'database_role' ) != 'PRIMARY'
    THEN
            raise_application_error(-20002,'Script is only allowed on Primary databases.');
    ELSE
            SELECT COUNT(*) INTO l_count
            FROM dba_users
            WHERE username = 'HP_DIAG';
            IF l_count = 1 then
                l_sqltxt := 'create or replace directory FILESYSTEM as ''${HOME}'||'/df_h''';
                dbms_output.put_line (l_sqltxt);
                execute immediate l_sqltxt;
                dbms_output.put_line ('Directory successfully created');

                EXECUTE IMMEDIATE 'GRANT READ ON DIRECTORY FILESYSTEM to HP_DIAG';
                EXECUTE IMMEDIATE 'GRANT WRITE ON DIRECTORY FILESYSTEM to HP_DIAG';
                dbms_output.put_line ('Directory privileges successfully granted to HP_DIAG');

                dbms_output.put_line ('Creating external table called FILESYSTEM');
                l_sqltxt := q'[CREATE TABLE HP_DIAG.FILESYSTEM
                            ( FILESYSTEM VARCHAR2(400), 
                                FSIZE VARCHAR2(10), 
                                USED VARCHAR2(10), 
                                AVAIL VARCHAR2(10), 
                                USE_PCENT VARCHAR2(10), 
                                MOUNTED_ON VARCHAR2(400), 
                                TIMESTAMP VARCHAR2(50)
                            ) 
                            ORGANIZATION EXTERNAL 
                                ( TYPE ORACLE_LOADER
                                DEFAULT DIRECTORY "FILESYSTEM"
                                ACCESS PARAMETERS
                                ( RECORDS DELIMITED BY NEWLINE
                                    SKIP 1
                                    FIELDS TERMINATED BY ','
                                    MISSING FIELD VALUES ARE NULL
                                )
                                LOCATION ('df_output.csv')
                                )
                            REJECT LIMIT UNLIMITED ]';
                EXECUTE IMMEDIATE  l_sqltxt ;
                dbms_output.put_line ( 'Table FILESYSTEM successfully created');
            ELSE
                dbms_output.put_line ( 'Info: user hp_diag not found in database '||l_name ) ;
                dbms_output.put_line ( 'Please setup the HP_DIAG schema and then run script again.' ) ;
            END IF;
    END IF;
    DBMS_OUTPUT.PUT_LINE('Execution completed successfully on database '||l_name);
EXCEPTION WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
END ;
/
exit ;
END_SQL