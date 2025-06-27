--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure DP_ACTIVE_BBT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."DP_ACTIVE_BBT" (p_volume in number,
  v_rec_out out sys_refcursor) AS 
BEGIN
  open v_rec_out for
  select mcf.accountnumber,mcf.COMBINEDTELEPHONE
  FROM dataprov.act_cust_sky_hd mcf
  where rownum <= p_volume;
END DP_ACTIVE_BBT;

/

  GRANT EXECUTE ON "DATAPROV"."DP_ACTIVE_BBT" TO "BATCHPROCESS_USER";
