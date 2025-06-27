--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure DP_SKYCESA01TOKEN_IN
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."DP_SKYCESA01TOKEN_IN" (
/*
Oogway Stub procedure (Oogway stub also uses procedure dp_messotoken_in)
URL e.g. http://hpsva:8080/SVChordiantUtils/oogway/customerdetails/N01/skycesa01token/7154EC377B7804DEE0546805CA05EAD17154EC377B7904DEE0546805CA05EAD1
*/
     p_skycesa01token IN VARCHAR2
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
     INTO v_partyid , v_accountnumber , v_username , v_nsprofileid , v_messotoken , v_skycesa01token --, v_ssotoken --, v_firstName , v_familyName , v_emailAddress
     FROM (
         SELECT a.partyId , a.accountNumber , a.username , a.nsprofileid , a.messotoken , a.skycesa01token --, NULL AS firstName , NULL AS familyName , NULL AS emailAddress
           FROM dataprov.loyalty_null a
          WHERE a.skycesa01token = p_skycesa01token
          UNION 
         --SELECT b.partyId , b.accountNumber , NULL AS username , NULL AS nsprofileid , b.messotoken , b.skycesa01token --, b.firstName , b.familyName , b.emailAddress 
         SELECT b.partyId , b.accountNumber , b.username , b.nsprofileid , b.messotoken , b.skycesa01token --, b.firstName , b.familyName , b.emailAddress
           FROM dataprov.customers b
          WHERE b.skycesa01token = p_skycesa01token
          order by 1
          )
   where rownum < 2;
END dp_skycesa01token_in ;

/

  GRANT EXECUTE ON "DATAPROV"."DP_SKYCESA01TOKEN_IN" TO "DATAPROV_READONLY_ROLE";
  GRANT EXECUTE ON "DATAPROV"."DP_SKYCESA01TOKEN_IN" TO "BATCHPROCESS_USER";
