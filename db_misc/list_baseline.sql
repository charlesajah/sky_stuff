set serveroutput on size 1000000
set pages 99 lines 300 head off feedback off verify off

/*

 Parameters
 ----------
 
 IN		sql_id		default %
 IN		sql_string	default %
 IN		cdate		default 010101_23:59.00 ... eg a very old date in this century!
 
 This will allow to search for specific baselines, using either the SQL_ID, a part of the SQL_TEXT or simply a date parameter since when they were created.
 
 Purpose
 -------
 
 What will this actually do?
 
 Produce a quick report showing the baselines that exist and some key atrributes for these e.g. fixed
  
 Privs
 -----
 grant execute on sys.dbms_crypto to hp_diag;
*/

spool list_baseline.txt

declare
  p_sql_id v$sql.sql_id%type; -- := '&1'; -- pass in % for any sql_id
  p_sql_string varchar(100); -- := '&2'; -- pass in % for any string at all
  p_cdate varchar2(30); -- := '&3'; -- pass in 010101 00:00.00 ... for baselines created since this date
  l_dummy varchar2(100); -- used for validation
  l_parameter exception;
  
  l_sql_id v$sql.sql_id%type;
  
  cursor bl_cur (in_cdate varchar2, in_sql_string varchar2) is
  SELECT SQL_HANDLE, SQL_TEXT, PLAN_NAME, to_char(CREATED,'ddmmyy hh24:mi.ss') cdate, ENABLED, ACCEPTED, FIXED, AUTOPURGE, substr(DESCRIPTION,1,95) DESCRIPTION
    FROM dba_sql_plan_baselines
   WHERE upper(SQL_TEXT) like '%'||upper(in_sql_string)||'%'
     AND CREATED >= to_date(in_cdate,'ddmmyy_hh24:mi.ss')
   ORDER BY CREATED DESC;
   

  FUNCTION f_get_phv (in_sql_handle IN varchar2) RETURN VARCHAR2 IS
	l_phv varchar2(10);
  BEGIN
    SELECT TRIM(substr(t.plan_table_output, instr(t.plan_table_output, ':') + 1)) plan_hash_value
	  INTO l_phv
	  FROM dba_sql_plan_baselines c,
           TABLE ( dbms_xplan.display_sql_plan_baseline(c.sql_handle, c.plan_name) ) t
     WHERE c.sql_handle = in_sql_handle 
	   and t.plan_table_output LIKE 'Plan hash value%';
        
    return (rpad(l_phv, 10));
  END f_get_phv;
  
  FUNCTION f_get_sql_id (in_sql_text IN clob) RETURN VARCHAR2 IS
    l_sqlid  VARCHAR2(13) := '';
    l_num    NUMBER;
  BEGIN  
    l_num := to_number(sys.utl_raw.reverse(sys.utl_raw.substr(dbms_crypto.hash(src=>utl_i18n.string_to_raw(in_sql_text||CHR(0),'AL32UTF8'),typ => 2),9,4))
             ||sys.utl_raw.reverse(sys.utl_raw.substr(dbms_crypto.hash(src => utl_i18n.string_to_raw(in_sql_text||CHR(0),'AL32UTF8'),typ=>2),13,4)),rpad('x', 16, 'x'));

    FOR i IN 0..floor(ln(l_num) / ln(32)) LOOP
      l_sqlid := substr('0123456789abcdfghjkmnpqrstuvwxyz', floor(MOD(l_num / power(32, i), 32)) + 1, 1) || l_sqlid;
    END LOOP;

    return (l_sqlid);
  END f_get_sql_id;

begin
  -- check length of sql_id
  l_dummy := '&1';
  if length (l_dummy) != 13 and l_dummy != '%' then
    dbms_output.put_line ('SQL_ID incorrect!');
  end if;
  -- check date format
  begin
    l_dummy := to_char(to_date('&3','ddmmyy_hh24:mi.ss'),'ddmmyy_hh24:mi.ss');
	exception
	  when others then
	    dbms_output.put_line ('CDate incorrect!');
		raise;
  end;
  p_sql_id := '&1'; 
  p_sql_string := '&2'; 
  p_cdate := '&3';
  
  for bl_rec in bl_cur (p_cdate, p_sql_string) loop
  
    l_sql_id := f_get_sql_id(bl_rec.SQL_TEXT);
	
    if p_sql_id = '%' or p_sql_id = l_sql_id then

      dbms_output.put (rpad('SQL_ID',15)); dbms_output.put (' ');
      dbms_output.put (rpad('PHV',15)); dbms_output.put (' ');
      dbms_output.put (rpad('SQL_HANDLE',30)); dbms_output.put (' ');
      dbms_output.put (rpad('PLAN_NAME',30)); dbms_output.put (' ');
      dbms_output.put (rpad('Created Date',20)); dbms_output.put (' ');
      dbms_output.put (rpad('ENAB',5)); dbms_output.put (' ');
      dbms_output.put (rpad('ACC',5)); dbms_output.put (' ');
      dbms_output.put (rpad('FIX',5)); dbms_output.put (' ');
      dbms_output.put (rpad('PUR',5)); dbms_output.put (' ');
      dbms_output.put (rpad('DESCRIPTION',100)); dbms_output.put (' ');
      dbms_output.put_line ('');

      dbms_output.put (rpad('-',15,'-')); dbms_output.put (' ');
      dbms_output.put (rpad('-',15,'-')); dbms_output.put (' ');
      dbms_output.put (rpad('-',30,'-')); dbms_output.put (' ');
      dbms_output.put (rpad('-',30,'-')); dbms_output.put (' ');
      dbms_output.put (rpad('-',20,'-')); dbms_output.put (' ');
      dbms_output.put (rpad('-',5,'-')); dbms_output.put (' ');
      dbms_output.put (rpad('-',5,'-')); dbms_output.put (' ');
      dbms_output.put (rpad('-',5,'-')); dbms_output.put (' ');
	  dbms_output.put (rpad('-',5,'-')); dbms_output.put (' ');
      dbms_output.put (rpad('-',100,'-')); dbms_output.put (' ');
      dbms_output.put_line ('');
    
      dbms_output.put (rpad(l_sql_id,15)); dbms_output.put (' ');
      dbms_output.put (rpad(f_get_phv(bl_rec.SQL_HANDLE),15)); dbms_output.put (' ');
      dbms_output.put (rpad(bl_rec.SQL_HANDLE,30)); dbms_output.put (' ');
      dbms_output.put (rpad(bl_rec.PLAN_NAME,30)); dbms_output.put (' ');
      dbms_output.put (rpad(bl_rec.cdate,20)); dbms_output.put (' ');
      dbms_output.put (rpad(bl_rec.ENABLED,5)); dbms_output.put (' ');
      dbms_output.put (rpad(bl_rec.ACCEPTED,5)); dbms_output.put (' ');
      dbms_output.put (rpad(bl_rec.FIXED,5)); dbms_output.put (' ');
      dbms_output.put (rpad(bl_rec.AUTOPURGE,5)); dbms_output.put (' ');
      dbms_output.put (rpad(bl_rec.DESCRIPTION,100)); dbms_output.put (' ');
      dbms_output.put_line ('');
	  
      dbms_output.put_line (rpad('-',8,'-')); dbms_output.put (' ');
      dbms_output.put_line (rpad('SQL_TEXT',8)); dbms_output.put (' '); 
      dbms_output.put_line (rpad('-',8,'-')); dbms_output.put (' ');

      for i in 1..5 loop
        dbms_output.put (substr(bl_rec.SQL_TEXT,(i-1)*120+1,120)); 
      end loop;	 

      dbms_output.put_line ('');	  
      dbms_output.put_line ('..');

    end if;
	
  end loop;
  
  exception
    when l_parameter then
      dbms_output.put_line ('Check parameters passed');
    when others then
      dbms_output.put_line (SQLERRM);
  
end;
/
spool off
exit

