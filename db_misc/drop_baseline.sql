set serveroutput on size 1000000 
set pages 99 lines 180 head off feedback off verify off
/*

 Parameters
 ----------
 
 IN		sql_handle + plan_name to identify baseline to drop
 
 Purpose
 -------
 
 What will this actually do?
 
 Drop a baseline identified by sql_handle + plan_name
 

 Privs
 -----
 GRANT ADMINISTER SQL TUNING SET TO HP_DIAG;
 GRANT ADMINISTER SQL MANAGEMENT OBJECT TO HP_DIAG;

*/

spool drop_baseline.txt

declare
    -- parameters will be passed in to this script eventually! ... my test data as hp_diag user on DMG011N
    
	in_sql_handle dba_sql_plan_baselines.sql_handle%type := '&1';
	in_plan_name dba_sql_plan_baselines.plan_name%type := '&2';
				 
    my_int pls_integer := 0;

	l_parameter exception;
	
	cursor bl_cur (p_sql_handle dba_sql_plan_baselines.sql_handle%type, p_plan_name dba_sql_plan_baselines.plan_name%type)  is
	select 'x'
	  from dba_sql_plan_baselines 
	 where sql_handle=p_sql_handle 
	   and plan_name=p_plan_name;
	   
    bl_rec bl_cur%rowtype;

begin 
    -- sql_handle + plan_name BOTH required
    if (in_sql_handle is NULL) or (in_plan_name is NULL) then
	  dbms_output.put_line ('Null sql_handle or plan_name');
	  raise l_parameter;
	end if;
	
	-- valid baseline?
	open bl_cur (in_sql_handle, in_plan_name);
	fetch bl_cur into bl_rec;
	if bl_cur%notfound then
	  dbms_output.put_line ('missing baseline');
	  close bl_cur;
	  raise l_parameter;
	end if;
	close bl_cur;
	
	-- drop  
	my_int := dbms_spm.drop_sql_plan_baseline (sql_handle => in_sql_handle, plan_name => in_plan_name);
	dbms_output.put_line (my_int||' baseline dropped');	
    
	exception
		when l_parameter then
		    dbms_output.put_line ('Check parameters passed');
		when others then
            dbms_output.put_line (SQLERRM);
	
end;
/
spool off
exit

