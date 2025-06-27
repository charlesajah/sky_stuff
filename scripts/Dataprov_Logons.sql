 CREATE TABLE "DATAPROV_LOGONS" 
   (	"USERNAME" VARCHAR2(20), 
      "PROXYUSER" VARCHAR2(20), 
	"HOSTNAME" VARCHAR2(15), 
	"PROGRAM" VARCHAR2(20), 
	"LOGONTIME" TIMESTAMP (6)
   );


  GRANT DELETE ON "DATAPROV"."DATAPROV_LOGONS" TO "DATAPROV_FULL";
  GRANT INSERT ON "DATAPROV"."DATAPROV_LOGONS" TO "DATAPROV_FULL";
  GRANT SELECT ON "DATAPROV"."DATAPROV_LOGONS" TO "DATAPROV_FULL";
  GRANT UPDATE ON "DATAPROV"."DATAPROV_LOGONS" TO "DATAPROV_FULL";
  GRANT SELECT ON "DATAPROV"."DATAPROV_LOGONS" TO "DATAPROV_RO";


--this is not updated
--use the one after this below 
CREATE OR REPLACE TRIGGER DATAPROV.DATAPROV_ACCESS 
AFTER 
  LOGON ON DATABASE WHEN (USER = 'DATAPROV')
  DECLARE l_archived TIMESTAMP := systimestamp;
  l_username VARCHAR2(20);
BEGIN
  --osuser name and proxyuser if any
  SELECT 
    sys_context('userenv', 'os_user') INTO l_username 
  FROM 
    dual;
  IF l_username != 'oracle' THEN
    IF sys_context('USERENV', 'PROXY_USER') IS NOT NULL THEN
      INSERT INTO dataprov.dataprov_logons 
      VALUES 
        (
          sys_context('userenv', 'os_user'), 
          sys_context('USERENV', 'PROXY_USER'), 
          SYS_CONTEXT('USERENV', 'HOST', 15), 
          SYS_CONTEXT(
            'USERENV', 'CLIENT_PROGRAM_NAME', 
            20
          ), 
          systimestamp
        );
      COMMIT;
    ELSIF sys_context('USERENV', 'PROXY_USER') IS NULL THEN
      INSERT INTO dataprov.dataprov_logons 
      VALUES 
        (
          sys_context('userenv', 'os_user'), 
          NULL, 
          SYS_CONTEXT('USERENV', 'HOST', 15), 
          SYS_CONTEXT(
            'USERENV', 'CLIENT_PROGRAM_NAME', 
            20
          ), 
          systimestamp
        );
      COMMIT;
    END IF;
  END IF;
END;
/






--This was copied from N01 database
create or replace TRIGGER DATAPROV.DATAPROV_ACCESS
AFTER
  LOGON ON DATABASE WHEN (USER = 'DATAPROV')
  DECLARE l_archived TIMESTAMP := systimestamp;
  l_username VARCHAR2(20);
BEGIN
  --osuser name and proxyuser if any
  SELECT
    sys_context('userenv', 'os_user') INTO l_username
  FROM
    dual;
  IF l_username != 'tomcat' AND SYS_CONTEXT('USERENV', 'HOST', 15) NOT IN ( 'dataprov-n01-58','BSKYB\WD014757', 'BSKYB\WD014758', 'BSKYB\WD014760', 'BSKYB\WD014761', 'BSKYB\WD014762','wd014757', 'wd014758', 'wd014760', 'wd014761', 'wd014762')
    AND SYS_CONTEXT('USERENV', 'CLIENT_PROGRAM_NAME',20) NOT LIKE '%J00%' THEN
    IF sys_context('USERENV', 'PROXY_USER')  IS NOT NULL  THEN
      INSERT INTO dataprov.dataprov_logons VALUES  (sys_context('userenv', 'os_user'),sys_context('USERENV', 'PROXY_USER'),SYS_CONTEXT('USERENV', 'HOST', 15),SYS_CONTEXT('USERENV', 'CLIENT_PROGRAM_NAME',20),systimestamp);
      COMMIT;
    ELSIF sys_context('USERENV', 'PROXY_USER') IS NULL THEN
      INSERT INTO dataprov.dataprov_logons
      VALUES
        (
          sys_context('userenv', 'os_user'),
          NULL,
          SYS_CONTEXT('USERENV', 'HOST', 15),
          SYS_CONTEXT(
            'USERENV', 'CLIENT_PROGRAM_NAME',
            20
          ),
          systimestamp
        );
      COMMIT;
    END IF;
  END IF;
END;