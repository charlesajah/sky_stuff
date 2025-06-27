set serveroutput on size 1000000 
set pages 99 lines 180 head off feedback off verify off
/*

 Parameters
 ----------
 IN		role to be added, user to have role added
 
 Purpose
 -------
 Add the requested role to the specified users
 
  Needs run as system or cbsservices
*/
spool add_individual_user_role.log

declare
  strRole       varchar2(50) ;
  strUser       varchar2(32) ;
  intExists     integer;
begin
  strRole := '&1';
  strUser := lower('&2');

  select count(*) into intExists from ccsowner.ccsUser where upperloginname = upper(strUser);
  if intExists = 0 then
    dbms_output.put_line('!!! ERROR !!! User "' || strUser || '" does not exist. Exiting');
    return;
  end if;

  SELECT count(*) into intExists from ccsowner.bsbRole where role = strRole;
  if intExists = 0 then
    dbms_output.put_line('!!! ERROR !!! Role "' || strRole || '" does not exist. Exiting');
    return;
  end if;

  begin
    dbms_output.put_line('Adding ' || strRole || ' to ' || strUser);
    cbsServices.dal_userRoles_api.addRole (i_loginName => upper(strUser), i_roleName => strRole, i_updatedBy => USER) ;
    dbms_output.put_line(strRole || ' added successfully to ' || strUser);
    commit;
  exception
    when others then
      dbms_output.put_line('!!! ERROR !!! ' || SQLERRM);
  end;
end;
/

exit
