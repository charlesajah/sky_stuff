--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure DP_GETDNFROMTELSERVICEID
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."DP_GETDNFROMTELSERVICEID" 
     (p_telephonyserviceID in varchar2,
                  p_DN out varchar2 ) as
begin
select gbbs.telephonenumber 
into p_DN 
from 
(select * from 
    (
        select bsi.telephonenumber, 1 rank 
        from ccsowner.bsbcustomerproductelement bcpe,
            ccsowner.bsbtelephonycustprodelement btcpe,
            ccsowner.bsbportfolioproduct bpp,
            ccsowner.bsbserviceinstance bsi
        where btcpe.telephonyproductelementid = bcpe.id 
        and bpp.id = bcpe.portfolioproductid
        and bsi.id = bpp.serviceinstanceid
        and btcpe.serviceid = p_telephonyserviceID
        and bpp.catalogueproductid = '12721'
        union all
            select 'NO_DATA', 2 from dual
    ) order by rank
) gbbs
where rownum < 2;
end dp_getDNFromTelServiceId;

/

  GRANT EXECUTE ON "DATAPROV"."DP_GETDNFROMTELSERVICEID" TO "DATAPROV_READONLY";
  GRANT EXECUTE ON "DATAPROV"."DP_GETDNFROMTELSERVICEID" TO "BATCHPROCESS_USER";
