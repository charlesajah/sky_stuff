--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure DP_GETBBSERVICEID
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."DP_GETBBSERVICEID" 
/*
|| Purpose : get the BB service ID from the voice service id. 
|| Author : Rowena Kennedy
|| Change History :
||    30-Dec-2016 Alan Abernethy try and match the voice to BB id¿s when the BB account is in both AC and PC states. The existing procedure only did AC.
*/
     (p_telephonyserviceID in varchar2,
                  bbserviceID out varchar2,
                  productType out varchar2) as
begin
select gbbs.servicenumber,gbbs.MIDNETCLASSIFICATION into bbserviceID,productType 
from (select * from (SELECT bbcpe.servicenumber,
       bbcpe.MIDNETCLASSIFICATION, 1 rank
FROM CCSOWNER.BSBPORTFOLIOPRODUCT bpp,
     CCSOWNER.BSBCUSTOMERPRODUCTELEMENT bcpe,
     CCSOWNER.BSBBROADBANDCUSTPRODELEMENT bbcpe
WHERE bpp.id = bcpe.portfolioproductid
  AND bcpe.customerproductelementtype='BB'
  AND bcpe.id = bbcpe.LINEPRODUCTELEMENTID
  and (bpp.STATUS = 'AC' or bpp.STATUS= 'PC')
  AND bpp.serviceinstanceid=
    (SELECT bsi.id
     FROM ccsowner.bsbserviceinstance bsi
     WHERE bsi.PARENTSERVICEINSTANCEID =
         (SELECT DISTINCT bsi.parentserviceinstanceid
          FROM CCSOWNER.BSBTELEPHONYCUSTPRODELEMENT btcpe,
               CCSOWNER.BSBCUSTOMERPRODUCTELEMENT bcpe,
               ccsowner.bsbportfolioproduct bpp,
               ccsowner.bsbserviceinstance bsi
          WHERE btcpe.SERVICEID = p_telephonyserviceID
            AND btcpe.TELEPHONYPRODUCTELEMENTID = bcpe.id
            AND bcpe.PORTFOLIOPRODUCTID = bpp.id
            AND bpp.SERVICEINSTANCEID = bsi.id)
       AND bsi.SERVICEINSTANCETYPE = '400')
       union all
       select 'NO_DATA', 'NO_DATA', 2 from dual) order by rank) gbbs
       where rownum < 2;
end dp_getBBServiceId;

/

  GRANT EXECUTE ON "DATAPROV"."DP_GETBBSERVICEID" TO "BATCHPROCESS_USER";
