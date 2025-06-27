--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure TEMP_SEG_USAGE_INSERT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "HP_DIAG"."TEMP_SEG_USAGE_INSERT" IS
BEGIN
  insert into hp_diag.TEMP_SEG_USAGE
  SELECT sysdate dt, s.sid, s.username, s.module, u.tablespace, u.segtype, u.contents, sum(u.blocks) blocks,  c.sql_id, c.sql_text
    FROM v$session s, v$tempseg_usage u, v$sqlarea c
   WHERE s.saddr=u.session_addr
     AND c.address= s.sql_address
     AND c.hash_value = s.sql_hash_value
  group by s.sid, s.username, s.module, u.tablespace, u.segtype, u.contents, c.sql_id, c.sql_text;
  COMMIT;
END;

/
