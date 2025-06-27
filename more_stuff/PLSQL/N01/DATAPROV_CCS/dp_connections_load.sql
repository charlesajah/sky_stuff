--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure DP_CONNECTIONS_LOAD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."DP_CONNECTIONS_LOAD" as
begin
  --
  insert /*+ append */
  into dataprov.dp_connections
    SELECT s.username,
           s.MACHINE,
           count(*) as total_con,
           count(decode(s.STATUS, 'ACTIVE', 1)) as active_con,
           ROUND(count(decode(s.STATUS, 'ACTIVE', 1)) / count(*) * 100, 2) as pct_active,
           sysdate dt
      FROM sys.v_$session s
     WHERE username is not null
     GROUP BY username, s.MACHINE;
  --
  COMMIT;
  --
end;

/

  GRANT EXECUTE ON "DATAPROV"."DP_CONNECTIONS_LOAD" TO "BATCHPROCESS_USER";
