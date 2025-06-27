CREATE OR REPLACE PACKAGE test_user_creation AS
  /*
  0 : successfully created
  1 : user already exists
  2 : failed to create : template user doesn't exist
  3 : failed to create : can't get ID for user
  4 : Couldn't add roles
  */
  procedure addTestUser(p_TemplateUsername IN VARCHAR2, p_Username IN VARCHAR2, p_Firstname IN VARCHAR2, p_Lastname IN VARCHAR2, r_Success OUT INTEGER) ;
  procedure AddRoles(p_TemplateUsername IN VARCHAR2, p_Username IN VARCHAR2, r_Success OUT INTEGER) ;
  function DoesUserExist(p_Username IN VARCHAR2)                            return integer ;  
END test_user_creation ;
/


CREATE OR REPLACE PACKAGE BODY test_user_creation AS
  procedure CleanUp( p_Username IN VARCHAR2) IS
    l_UserID     number ;
  begin
    select id into l_UserID from ccsowner.ccsuser where upperloginname = upper(p_Username);

    delete from ccsowner.bsbuserrolebridge where userid = l_UserID;
    delete from ccsowner.ccsuser where upperloginname = upper(p_Username);
    commit;
  end;

  function UserExists( p_Username IN VARCHAR2 ) return integer IS
    l_cnt integer;
  begin
    select count(*) into l_cnt from ccsowner.ccsuser where upperloginname = upper(p_Username);
    return l_cnt;
  end ;  

  function GetUserID return number IS
    l_id  number;
    l_cnt integer := 1;
  begin
    while l_cnt > 0 
    LOOP
      select CCSOWNER.CCSUSER_SEQ.nextval into l_id from dual;    
      select count(*) into l_cnt from ccsowner.ccsuser where id = l_id;
    end loop;    
    return l_id;
  end ;

  function GetBridgeID return varchar2 IS
    l_id  number;
    l_cnt integer := 1;
  begin
    while l_cnt > 0 
    LOOP
      select CCSOWNER.BSBUSERROLEBRIDGE_SEQ.nextval into l_id from dual;    
      select count(*) into l_cnt from ccsowner.bsbuserrolebridge where id = to_char(l_id);
    end loop;    
    return to_char(l_id);
  end ;

  function Add_bsbUserRoleBridge(p_TemplateUsername IN VARCHAR2, p_Username IN VARCHAR2) return integer IS
    l_TempUserID number;
    l_UserID     number ;
    l_BridgeID   varchar2(47);
    l_cnt        number;
  begin
    select id into l_TempUserID from ccsowner.ccsuser where upperloginname = upper(p_TemplateUsername);
    select id into l_UserID     from ccsowner.ccsuser where upperloginname = upper(p_Username);

    FOR rec IN (select roleid from ccsowner.bsbuserrolebridge where userid = l_TempUserID)
    LOOP
      l_BridgeID  := GetBridgeID ;
      begin
        select count(*) into l_cnt from ccsowner.bsbuserrolebridge where USERID = l_UserID and roleid = rec.roleid;
        if l_cnt = 0 then
          insert into ccsowner.bsbuserrolebridge (ID, ROLEID, USERID, LOCKTOKENTEXT) values (l_BridgeID, rec.roleid, l_UserID, '1' );
        end if;
      exception
        when others then
        begin
          rollback ;
          return 1;
        end ;
      end ;  
    END LOOP; 
    return 0;
  end ;  

  function Add_CCSUser(p_ID IN NUMBER, p_Username IN VARCHAR2, p_Firstname IN VARCHAR2, p_Lastname IN VARCHAR2) return integer is
  begin
    begin
      insert into ccsowner.ccsuser (ID, FIRSTNAME, LASTNAME, LOGINNAME) values (p_ID, p_Firstname, p_Lastname, p_Username);
      return 0;
    EXCEPTION
      when others then
      begin
        rollback;
        return 1;
      end;
    end;
  end ;  

  procedure addTestUser(p_TemplateUsername IN VARCHAR2, p_Username IN VARCHAR2, p_Firstname IN VARCHAR2, p_Lastname IN VARCHAR2, r_Success OUT INTEGER) IS
    l_id integer := 0;
  begin
    -- 1 : check if template user exists
    if UserExists(p_TemplateUsername) = 0 then
      r_success := 2 ;
      return;
    end if ;

    -- 2 : check if user exists
    if UserExists(p_Username) = 1 then
      r_success := 1 ;
      return ;
    end if ;

    -- 3 : get next unused id
    l_id := GetUserID;

    -- 4 : create user
    if Add_CCSUser(l_id, p_Username, p_Firstname, p_Lastname) = 1 then
      r_success := 3 ;
      return ;
    end if;

    -- 5 : clone entries in bsbUserRoleBridge
    if Add_bsbUserRoleBridge(p_TemplateUsername, p_Username) = 1 then
      CleanUp(p_Username);
      r_success := 4;
      return;
    end if;

    -- 6 : return status
    r_success := 0;
  end;

  function DoesUserExist(p_Username IN VARCHAR2) return integer IS
  begin
    if UserExists(p_Username) = 1 then
      return 1;
    else
      return 0;
    end if ;
  end DoesUserExist;

  procedure AddRoles(p_TemplateUsername IN VARCHAR2, p_Username IN VARCHAR2, r_Success OUT INTEGER) IS
  begin
    if Add_bsbUserRoleBridge(p_TemplateUsername, p_Username) = 1 then
      r_success := 1;
    else
      r_Success := 0 ;
    end if ;
  end AddRoles;

end test_user_creation;
/
