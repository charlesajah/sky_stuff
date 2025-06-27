set termout off
set linesize 900
set pagesize 200
set heading off
set verify off
set feedback off
set newpage 0
set serveroutput on size unlimited

-- Call the procedure that loads the data from the CSV files
begin   
    REPORT_DATA.Do_load_object_parallelism_data;
exception
    when others then
        dbms_output.put_line('Error loading data from CSV files: ' || sqlerrm);    
end;
/

spool objects_parallelism.txt
select * from table ( REPORT_DATA.Get_ObjectDetailsReport ( i_env => '&1', i_mode => 'DIFF' )) ;
select * from table ( REPORT_DATA.Get_ObjectDetailsReport ( i_env => '&1', i_mode => 'SAME' )) ;
spool off

exit