--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure DP_SSOTOKEN_IN
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."DP_SSOTOKEN_IN" (
/*
Oogway Stub procedure (Oogway stub also uses procedure dp_skycesa01token_in)
URL eg http://hpsva:8080/SVChordiantUtils/oogway/customerdetails/N01/ssotoken/71557C4EDA9A7433E0546805CA05EAD171557C4EDA9B7433E0546805CA05EAD1
*/
     p_ssotoken IN VARCHAR2
   , v_partyId OUT VARCHAR2
   , v_accountNumber OUT VARCHAR2
   , v_username OUT VARCHAR2
   , v_nsprofileid OUT VARCHAR2
   , v_messotoken OUT VARCHAR2
   , v_skycesa01token OUT VARCHAR2
   , v_ssotoken OUT VARCHAR2
   --, v_firstName OUT VARCHAR2
   --, v_familyName OUT VARCHAR2
   --, v_emailAddress OUT VARCHAR2
) AS
BEGIN

   SELECT partyId , accountNumber , username , nsprofileid , messotoken , skycesa01token, ssotoken --, firstName , familyName , emailAddress
     INTO v_partyId , v_accountNumber , v_username , v_nsprofileid , v_messotoken , v_skycesa01token, v_ssotoken --, v_firstName , v_familyName , v_emailAddress
     FROM (
         SELECT a.partyId , a.accountNumber , a.username , a.nsprofileid , a.messotoken , a.skycesa01token, a.ssotoken --, NULL AS firstName , NULL AS familyName , NULL AS emailAddress
           FROM dataprov.loyalty_null a
          WHERE a.ssotoken = p_ssotoken
          UNION 
         SELECT b.partyId , b.accountNumber , b.username , b.nsprofileid , b.messotoken , b.skycesa01token, b.ssotoken --, b.firstName , b.familyName , b.emailAddress
           FROM dataprov.customers b
          WHERE b.ssotoken = p_ssotoken
          order by 1
          )
   where rownum < 2;
END dp_ssotoken_in ;

/

  GRANT EXECUTE ON "DATAPROV"."DP_SSOTOKEN_IN" TO "DATAPROV_READONLY_ROLE";
  GRANT EXECUTE ON "DATAPROV"."DP_SSOTOKEN_IN" TO "BATCHPROCESS_USER";
