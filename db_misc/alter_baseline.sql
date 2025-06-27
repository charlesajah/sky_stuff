set serveroutput on size 1000000 
set pages 99 lines 180 head off feedback off verify off
/*

 Parameters
 ----------
 
 IN		7 parameters ... 2 to identify the baseline + 5 attributes to change
 
		1+2)	sql_handle + plan_name to identify baseline for change
		3,4,5)	enabled, fixed, autopurge flags
		6)		new plan_name 
		7)		description
 
 OUT	n/a
 
 Purpose
 -------
 
 What will this actually do?
 
 Wrapper around dbms_spm.alter_sql_plan_baseline allowing key attributes to be changed.
 
 What will this not do?
 
 Evolve the plans to make them accepted ... differences across versions for this
 Drop baselines
 
 Thoughts
 --------
 
 Should SQL_ID + PHV be passed in to identify the baseline ?

 Privs
 -----
 GRANT ADMINISTER SQL TUNING SET TO HP_DIAG;
 GRANT ADMINISTER SQL MANAGEMENT OBJECT TO HP_DIAG;

*/

spool alter_baseline.txt

declare
    -- parameters will be passed in to this script eventually! ... my test data as hp_diag user on DMG011N
    
	in_sql_handle dba_sql_plan_baselines.sql_handle%type := '&1';
	in_plan_name dba_sql_plan_baselines.plan_name%type := '&2';
	
	in_attr_enabled varchar2(10) := '&3';
	in_attr_fixed varchar2(10)  := '&4';
	in_attr_autopurge varchar2(10)  := '&5';
	in_attr_plan_name dba_sql_plan_baselines.plan_name%type := '&6';
	in_attr_description dba_sql_plan_baselines.description%type := '&7';
			 
    my_int pls_integer;

	l_parameter exception;
	
	cursor bl_cur (p_sql_handle dba_sql_plan_baselines.sql_handle%type, p_plan_name dba_sql_plan_baselines.plan_name%type)  is
	select *
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
	
	-- check YES/NO flags - degault of changeme in Jenkins ... rather than NULL
	
	if in_attr_enabled NOT in ('YES','NO','changeme') then
      dbms_output.put_line ('Invalid enabled attribute');
	  raise l_parameter;
	end if;
	if in_attr_fixed NOT in ('YES','NO','changeme') then
      dbms_output.put_line ('Invalid fixed attribute');
	  raise l_parameter;
	end if;
	if in_attr_autopurge NOT in ('YES','NO','changeme') then
      dbms_output.put_line ('Invalid autopurge attribute');
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
	
	-- enabled 
	if in_attr_enabled != 'changeme' then
      if in_attr_enabled != bl_rec.enabled then
		my_int := dbms_spm.alter_sql_plan_baseline (sql_handle => in_sql_handle, plan_name => in_plan_name, attribute_name => 'enabled', 
				                                    attribute_value => in_attr_enabled);
		if my_int > 0 then
            dbms_output.put_line ('enabled flag changed');	
        end if;
	  end if;
	end if;
	
	-- fixed 
	if in_attr_fixed != 'changeme' then
      if in_attr_fixed != bl_rec.fixed then
		my_int := dbms_spm.alter_sql_plan_baseline (sql_handle => in_sql_handle, plan_name => in_plan_name, attribute_name => 'fixed', 
				                                    attribute_value => in_attr_fixed);
		if my_int > 0 then
            dbms_output.put_line ('fixed flag changed');	
        end if;
	  end if;
	end if;

	-- autopurge 
	if in_attr_autopurge != 'changeme' then
      if in_attr_autopurge != bl_rec.autopurge then
		my_int := dbms_spm.alter_sql_plan_baseline (sql_handle => in_sql_handle, plan_name => in_plan_name, attribute_name => 'autopurge', 
				                                    attribute_value => in_attr_autopurge);
		if my_int > 0 then
            dbms_output.put_line ('autopurge flag changed');	
        end if;
	  end if;
	end if;
	
	-- plan_name 
	if in_attr_plan_name != 'changeme' then
      if in_attr_plan_name != bl_rec.plan_name then
		my_int := dbms_spm.alter_sql_plan_baseline (sql_handle => in_sql_handle, plan_name => in_plan_name, attribute_name => 'plan_name', 
				                                    attribute_value => in_attr_plan_name);
		if my_int > 0 then
            dbms_output.put_line ('plan_name flag changed');	
        end if;
	  end if;
	end if;

	-- description 
	if in_attr_description != 'changeme' then
      if in_attr_description != nvl(bl_rec.description,'take it to the limit') then
		my_int := dbms_spm.alter_sql_plan_baseline (sql_handle => in_sql_handle, plan_name => in_plan_name, attribute_name => 'description', 
				                                    attribute_value => in_attr_description);
		if my_int > 0 then
            dbms_output.put_line ('description flag changed');	
        end if;
	  end if;
	end if;	
	
	close bl_cur;

	exception
		when l_parameter then
		    dbms_output.put_line ('Check parameters passed');
		when others then
            dbms_output.put_line (SQLERRM);
	
end;
/
spool off
exit

