--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure DP_GETBBSERVICEIDV2
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."DP_GETBBSERVICEIDV2" (
    p_telephonyserviceID IN VARCHAR2,
    bbserviceID OUT VARCHAR2 )
AS
BEGIN
  SELECT gbbs.servicenumber
  INTO bbserviceid
  FROM
    (SELECT * FROM
      (SELECT bbcpe.servicenumber,
        1 rank
      FROM CCSOWNER.BSBPORTFOLIOPRODUCT bpp,
        CCSOWNER.BSBCUSTOMERPRODUCTELEMENT bcpe,
        CCSOWNER.BSBBROADBANDCUSTPRODELEMENT bbcpe
      WHERE bpp.id                       = bcpe.portfolioproductid
      AND bcpe.customerproductelementtype='BB'
      AND bcpe.id                        = bbcpe.LINEPRODUCTELEMENTID
      AND (bpp.STATUS                    = 'AC'
      OR bpp.STATUS                      = 'PC')
      AND bpp.serviceinstanceid          in
        (SELECT bsi.id
        FROM ccsowner.bsbserviceinstance bsi
        WHERE bsi.PARENTSERVICEINSTANCEID =
          (SELECT DISTINCT bsi.parentserviceinstanceid
          FROM CCSOWNER.BSBTELEPHONYCUSTPRODELEMENT btcpe,
            CCSOWNER.BSBCUSTOMERPRODUCTELEMENT bcpe,
            ccsowner.bsbportfolioproduct bpp,
            ccsowner.bsbserviceinstance bsi
          WHERE btcpe.SERVICEID               = p_telephonyserviceID
          AND btcpe.TELEPHONYPRODUCTELEMENTID = bcpe.id
          AND bcpe.PORTFOLIOPRODUCTID         = bpp.id
          AND bpp.SERVICEINSTANCEID           = bsi.id
          )
        AND bsi.SERVICEINSTANCETYPE = '400'
        )
      UNION ALL
      SELECT 'NO_DATA', 2 FROM dual
      ) ORDER BY rank
    ) gbbs
  WHERE rownum < 2;
END dp_getBBServiceIdV2;

/

  GRANT EXECUTE ON "DATAPROV"."DP_GETBBSERVICEIDV2" TO "DATAPROV_READONLY";
  GRANT EXECUTE ON "DATAPROV"."DP_GETBBSERVICEIDV2" TO "BATCHPROCESS_USER";
