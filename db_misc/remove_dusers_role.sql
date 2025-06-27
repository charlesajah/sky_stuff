set serveroutput on size 1000000 
set pages 99 lines 180 head off feedback off verify off
/*

 Parameters
 ----------
 IN		role to be added
 
 Purpose
 -------
 Remove the requested role to the D0001 to d5000 users
 
  Needs run as system or cbsservices
*/

spool remove_dusers_role.log

declare
  strRole       varchar2(50) ;
  strUser       varchar2(32) ;
  intExists     integer;
begin
  strRole := '&1';

  SELECT count(*) into intExists from ccsowner.bsbRole where role = strRole;
  if intExists = 0 then
    dbms_output.put_line('!!! ERROR !!! Role "' || strRole || '" does not exist. Exiting');
    return;
  end if;

  FOR i IN 1..5000
  LOOP
    begin
      dbms_output.put_line('Removing ' || strRole || ' from d' || TO_CHAR ( i , 'FM0000' ) ) ;
      cbsServices.dal_userRoles_api.removeRole (i_loginName => 'd' || TO_CHAR ( i , 'FM0000' ), i_roleName => strRole, i_updatedBy => USER) ;
      dbms_output.put_line(strRole || ' removed successfully from ' || 'd' || TO_CHAR ( i , 'FM0000' ));
      commit;
    exception
      when others then
        dbms_output.put_line('!!! ERROR !!! ' || SQLERRM);
    end;
  end loop;
end;
/

exit
