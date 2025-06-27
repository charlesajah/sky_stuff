--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure DP_MESSOTOKEN_IN
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."DP_MESSOTOKEN_IN" (
    p_messotoken IN VARCHAR2
   , v_partyId OUT VARCHAR2
   , v_accountNumber OUT VARCHAR2
   , v_username OUT VARCHAR2
   , v_nsprofileid OUT VARCHAR2
   , v_messotoken OUT VARCHAR2
   , v_skycesa01token OUT VARCHAR2
   --, v_ssotoken OUT VARCHAR2
   --, v_firstName OUT VARCHAR2
   --, v_familyName OUT VARCHAR2
   --, v_emailAddress OUT VARCHAR2
) AS
BEGIN
   SELECT partyId , accountNumber , username , nsprofileid , messotoken , skycesa01token --, null --, firstName , familyName , emailAddress
     INTO v_partyId , v_accountNumber , v_username , v_nsprofileid , v_messotoken , v_skycesa01token --, v_ssotoken --, v_firstName , v_familyName , v_emailAddress
     FROM (
         SELECT a.partyId , a.accountNumber , a.username , a.nsprofileid , a.messotoken , a.skycesa01token --, NULL AS firstName , NULL AS familyName , NULL AS emailAddress
           FROM dataprov.loyalty_null a
          WHERE a.messotoken = p_messotoken
          UNION 
         --SELECT b.partyId , b.accountNumber , 'testuser' AS username , NULL AS nsprofileid , b.messotoken , b.skycesa01token --, b.firstName , b.familyName , b.emailAddress
         SELECT /*+ parallel(b, 4) */ b.partyId , b.accountNumber , b.username , b.nsprofileid , b.messotoken , b.skycesa01token --, b.firstName , b.familyName , b.emailAddress
           FROM dataprov.customers b
          WHERE b.messotoken = p_messotoken
          UNION
         SELECT NULL AS partyId , f.accountNumber , NULL AS username , NULL AS nsprofileid , NULL AS messotoken , NULL AS skycesa01token --, NULL AS firstName , NULL AS familyName , NULL AS emailAddress
           FROM dataprov.fsp_pos_basepack f
          WHERE f.skyid_token = p_messotoken
          UNION
          SELECT cc.PARTYID AS partyId , cc.ACCOUNTNUMBER AS accountNumber , 'testuser' AS username , NULL AS nsprofileid , cc.MESSOTOKEN as MESSOTOKEN , cc.SKYCESA01TOKEN as skycesa01token --, NULL AS firstName , NULL AS familyName , NULL AS emailAddress
           FROM dataprov.ceased_customers cc
          WHERE cc.messotoken = p_messotoken
          UNION
           SELECT dpf.PARTYID AS partyId , dpf.ACCOUNTNUMBER AS accountNumber , 'testuser' AS username , NULL AS nsprofileid , dpf.MESSOTOKEN as MESSOTOKEN , dpf.SKYCESA01TOKEN as skycesa01token 
           FROM dataprov.dprov_accounts_fast dpf
          WHERE dpf.messotoken = p_messotoken and pool_name = 'EOCN_NO_DEBT'
          order by 1
          )
     WHERE ROWNUM < 2     
  ;
END dp_messotoken_in ;

/

  GRANT EXECUTE ON "DATAPROV"."DP_MESSOTOKEN_IN" TO "DATAPROV_READONLY_ROLE";
  GRANT EXECUTE ON "DATAPROV"."DP_MESSOTOKEN_IN" TO "BATCHPROCESS_USER";
