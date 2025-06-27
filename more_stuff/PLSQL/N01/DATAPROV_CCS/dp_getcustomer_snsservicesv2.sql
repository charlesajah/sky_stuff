--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure DP_GETCUSTOMER_SNSSERVICESV2
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."DP_GETCUSTOMER_SNSSERVICESV2" 
     (p_accountnumber in varchar2,
       v_rec_out out sys_refcursor) as
begin

open v_rec_out for select * from(
select distinct bba.accountnumber,
       bpp.status,
       case bpp.catalogueproductid
         WHEN '12721' THEN
          'voice'
         else
          'data'
       end servicetype,
       CASE bpp.catalogueproductid
         WHEN '12721' THEN
          btcp.serviceid
         ELSE
          bbcp.servicenumber
       end serviceidentifier,
       bsi.telephonenumber,
       ba.postcode
  from ccsowner.bsbbillingaccount           bba,
       ccsowner.bsbportfolioproduct         bpp,
       ccsowner.bsbserviceinstance          bsi,
       CCSOWNER.BSBCUSTOMERPRODUCTELEMENT   bcp,
       ccsowner.bsbcustomerrole             bcr,
       ccsowner.bsbpartyrole                bpr,
       ccsowner.bsbcontactor                bc,
       ccsowner.bsbcontactaddress           bca,
       ccsowner.bsbaddress                  ba,
       ccsowner.bsbbroadbandcustprodelement bbcp,
       ccsowner.BSBTELEPHONYCUSTPRODELEMENT btcp
where bba.portfolioid = bpp.portfolioid
   and bpp.serviceinstanceid = bsi.id
   and bpp.id = bcp.portfolioproductid
   and bcp.id = bbcp.lineproductelementid(+)
   and bcp.id = btcp.telephonyproductelementid(+)
   and bba.portfolioid = bcr.portfolioid
   and bcr.partyroleid = bpr.id
   and bpr.partyid = bc.partyid
   and bc.id = bca.contactorid
   and bca.addressid = ba.id
   and bsi.serviceinstancetype in (100, 400)
   and BCA.Effectivetodate is null
   and bba.accountnumber = p_accountnumber);
end dp_getcustomer_snsservicesv2;

/

  GRANT EXECUTE ON "DATAPROV"."DP_GETCUSTOMER_SNSSERVICESV2" TO "DATAPROV_READONLY";
  GRANT EXECUTE ON "DATAPROV"."DP_GETCUSTOMER_SNSSERVICESV2" TO "BATCHPROCESS_USER";
