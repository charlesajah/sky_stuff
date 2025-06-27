set serveroutput on size 1000000 
set pages 99 lines 180 head off feedback off verify off
/*

 Parameters
 ----------
 
 IN		sql_id + plan_hash_value
 OUT	none currently ... could be rewritten as a function/procedure if required
 
 Purpose
 -------
 
 What will this actually do?
 
 There is no direct way to create a SQL plan baseline for a sql_id/plan_hash_value residing in the AWR.
 This can be done in a few steps ...
 
 0) check the sql_id/plan_hash_value exists before starting
 1) create a SQL tuning set (STS)
 2) load the STS with the sql_id/plan_hash_value from the AWR
 3) load the SQL plan baseline from the STS
 
 Thoughts
 --------
 What happens if baseline exists for sql_id/plan_hash_value? 
  From crated_date on dba_sql_plan_baselines it would appear to be re-created each time
 What happens if baseline exists for sql_id with multiple execution plans
  Don't know yet ... expect it will only replace the row for the same plan_hash_value ... needs tested
  
 Privs
 -----
 GRANT ADMINISTER SQL TUNING SET TO HP_DIAG;
 GRANT ADMINISTER SQL MANAGEMENT OBJECT TO HP_DIAG;

*/

spool add_baseline.txt

declare
    -- parameters will be passed in to this script eventually! ... my test data as system user on DMG011N
    
	in_sql_id v$sql.sql_id%type := '&1';
	in_plan_hash_value v$sql.plan_hash_value%type; 
	l_fixed varchar2(3) := 'YES';
	l_enabled varchar2(3) := 'YES';
	
	cursor chk_in_awr_cur (p_sql_id v$sql.sql_id%type, p_plan_hash_value v$sql.plan_hash_value%type) is
	select COUNT(*) from DBA_HIST_SQL_PLAN
	 where sql_id  = p_sql_id and plan_hash_value = p_plan_hash_value;
     
    cursor chk_in_STS_cur (p_name DBA_SQLSET.name%type) is
	select count(*) from DBA_SQLSET where name = p_name;
	
    -- snapshot's not parameterized ... just look into all AWR for the sql_id 
	cursor awr_snap_cur is
        select min(dhs.snap_id), max(dhs.snap_id) from DBA_HIST_SNAPSHOT dhs, v$database vdb where dhs.dbid = vdb.dbid;
	
	-- baseline's in last 5 seconds
	cursor chk_bl_cur is
	select sql_handle, plan_name
      from dba_sql_plan_baselines spb
     where created >= sysdate-5/(60*60*24);
	 
    my_int pls_integer;
	
	l_tot number;
    l_sql_missing_from_AWR exception;
	l_parameter exception;
    l_min_snap_id DBA_HIST_SNAPSHOT.snap_id%type;
	l_max_snap_id DBA_HIST_SNAPSHOT.snap_id%type;
    l_sts_name DBA_SQLSET.name%type := in_sql_id||'_STS_HP_DIAG';
    l_sts_desc DBA_SQLSET.description%type := 'temp STS for baseline';
	l_sql_handle dba_sql_plan_baselines.sql_handle%type;
	l_plan_name dba_sql_plan_baselines.plan_name%type;
    l_filter varchar2(100);
	
	baseline_ref_cur DBMS_SQLTUNE.SQLSET_CURSOR;
    
begin
    -- check command line parameters passed ... is PHV a number?
	begin
	    in_plan_hash_value := to_number('&2');
		exception 
		    when others then
			    dbms_output.put_line ('Plan_hash_value should be a number!');
				raise l_parameter;
	end;
	
	-- is sql_id/plan_hash_value in AWR?
	open chk_in_awr_cur (in_sql_id,in_plan_hash_value);
	fetch chk_in_awr_cur into l_tot;
	close chk_in_awr_cur;
	if l_tot = 0 then
	  dbms_output.put_line ('SQL_ID missing from AWR');
	  raise l_sql_missing_from_AWR;
	end if;

	-- create STS
    open chk_in_STS_cur (l_sts_name);
    fetch chk_in_STS_cur into l_tot;
    close chk_in_STS_cur;
    -- drop if it already exists ... to allow rerun with same STS
	-- ... you can ONLY drop a STS that you own, so will raise an exepction if owned by someone else!
    if l_tot != 0 then
        dbms_output.put_line ('STS found with same name');
        dbms_sqltune.drop_sqlset(sqlset_name => l_sts_name); 
		dbms_output.put_line ('STS found with same name ... dropped');
    end if;
	dbms_sqltune.create_sqlset(sqlset_name => l_sts_name,description => l_sts_desc);

	open awr_snap_cur;
	fetch awr_snap_cur into l_min_snap_id, l_max_snap_id;
	close awr_snap_cur;
    dbms_output.put_line ('AWR min/max '||l_min_snap_id||'/'||l_max_snap_id);

    -- create sql plan baseline from STS
    l_filter := ' sql_id='''||in_sql_id||''' and plan_hash_value='||in_plan_hash_value||' ';
	dbms_output.put_line (l_filter);
	open baseline_ref_cur for
	select VALUE(p) 
	  from table(DBMS_SQLTUNE.SELECT_WORKLOAD_REPOSITORY (BEGIN_SNAP => l_min_snap_id, END_SNAP => l_max_snap_id, BASIC_FILTER => l_filter, RECURSIVE_SQL => 'ALL')) p;  
	  --from table(DBMS_SQLTUNE.SELECT_WORKLOAD_REPOSITORY (l_min_snap_id,l_max_snap_id,l_filter,NULL,NULL,NULL,NULL,NULL,NULL,'ALL')) p;   
	DBMS_SQLTUNE.LOAD_SQLSET(l_sts_name, baseline_ref_cur);
	close baseline_ref_cur;
	
	-- check STS
    open chk_in_STS_cur (l_sts_name);
    fetch chk_in_STS_cur into l_tot;
	dbms_output.put_line ('STS has ... '||l_tot);
    close chk_in_STS_cur;
    
    my_int := dbms_spm.load_plans_from_sqlset (sqlset_name => l_sts_name, basic_filter => l_filter, fixed => l_fixed, enabled => l_enabled);
	dbms_output.put_line (my_int||' plans loaded');
	
	-- rename the baseline to something nicer!
	if my_int > 0 then
	    open chk_bl_cur;
		fetch chk_bl_cur into l_sql_handle, l_plan_name;
		close chk_bl_cur;
		my_int := dbms_spm.alter_sql_plan_baseline (sql_handle => l_sql_handle, plan_name => l_plan_name, attribute_name => 'PLAN_NAME', 
				                                    attribute_value => 'SQL'||'_'||in_sql_id||'_'||in_plan_hash_value);
	    if my_int > 0 then
            dbms_output.put_line ('SQL'||'_'||in_sql_id||'_'||in_plan_hash_value||' renamed successfully');	
        else			
		    dbms_output.put_line ('Plan created but failed to rename ... please check!');	
		end if;
	end if;
	
	-- tidy up
	dbms_sqltune.drop_sqlset(sqlset_name => l_sts_name); 

	exception
        when l_sql_missing_from_AWR then
            dbms_output.put_line (in_sql_id||'/'||in_plan_hash_value||' SQL_ID/PHV missing from AWR');
		when l_parameter then
		    dbms_output.put_line ('Check parameters passed');
		when others then
            dbms_output.put_line (SQLERRM);
	
end;
/
spool off
exit
