--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure DEDUP_PROD_CUS_DATA_STG
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "HP_DIAG"."DEDUP_PROD_CUS_DATA_STG" AS 
  l_cnt number(10);
BEGIN
  select count(*) into l_cnt from prod_cus_data_stg WHERE rowid NOT IN ( SELECT MAX(ROWID) FROM prod_cus_data_stg GROUP BY partyid, tstamp );
  sys.dbms_output.put_line('Number of duplicate records before : ' || l_cnt);
  delete from prod_cus_data_stg WHERE rowid NOT IN ( SELECT MAX(ROWID) FROM prod_cus_data_stg GROUP BY partyid, tstamp );
  sys.dbms_output.put_line('De-duplication of records completed');
  select count(*) into l_cnt from prod_cus_data_stg WHERE rowid NOT IN ( SELECT MAX(ROWID) FROM prod_cus_data_stg GROUP BY partyid, tstamp );
  sys.dbms_output.put_line('Number of duplicate records after : ' || l_cnt);
  commit;
END DEDUP_PROD_CUS_DATA_STG;

/
