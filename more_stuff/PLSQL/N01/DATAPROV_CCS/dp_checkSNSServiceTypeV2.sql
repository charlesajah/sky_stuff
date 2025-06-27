--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure DP_CHECKSNSSERVICETYPEV2
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."DP_CHECKSNSSERVICETYPEV2" (
    p_serviceid IN VARCHAR2,
    bbserviceid OUT VARCHAR2,
    bbservicetype OUT VARCHAR2,
    bbstatus OUT VARCHAR2,
    telserviceid OUT VARCHAR2,
    telservicetype OUT VARCHAR2,
    telstatus OUT VARCHAR2)
AS
BEGIN
  SELECT bbserviceid,
    bbservicetype,
    bbstatus,
    telserviceid,
    telservicetype,
    telstatus
  INTO bbserviceid,
    bbservicetype,
    bbstatus,
    telserviceid,
    telservicetype,
    telstatus
  FROM
    (SELECT bbserviceid,
      bbservicetype,
      bbstatus,
      telserviceid,
      telservicetype,
      telstatus
    FROM
      (SELECT bbcpe.SERVICENUMBER AS bbserviceid,
        bsr.TECHNOLOGYCODE        AS bbservicetype,
        bsr.STATUS                AS bbstatus,
        'NOTTEL' as telserviceid,
        'NOTTEL' as telservicetype,
        'NOTTEL' as telstatus,
        1 rank
      FROM ccsowner.bsbsubscription bsr ,
        ccsowner.bsbportfolioproduct bpp,
        CCSOWNER.BSBCUSTOMERPRODUCTELEMENT bcpe,
        CCSOWNER.BSBBROADBANDCUSTPRODELEMENT bbcpe
      WHERE bbcpe.SERVICENUMBER      = p_serviceid
      AND bbcpe.LINEPRODUCTELEMENTID = bcpe.id
      AND bcpe.PORTFOLIOPRODUCTID    = bpp.id
      AND bpp.SUBSCRIPTIONID         = bsr.id
      UNION
      SELECT 'NOTBB' as bbserviceid,
        'NOTBB' as bbservicetype,
        'NOTBB' as bbstatus,
        btcpe.SERVICEID    AS telserviceid,
        bsr.TECHNOLOGYCODE AS telservicetype,
        bsr.STATUS         AS telstatus,
        1 rank
      FROM ccsowner.bsbsubscription bsr ,
        ccsowner.bsbportfolioproduct bpp,
        CCSOWNER.BSBCUSTOMERPRODUCTELEMENT bcpe,
        CCSOWNER.BSBTELEPHONYCUSTPRODELEMENT btcpe
      WHERE btcpe.SERVICEID               = p_serviceid
      AND bpp.CATALOGUEPRODUCTID          ='12721'
      AND btcpe.TELEPHONYPRODUCTELEMENTID = bcpe.id
      AND bcpe.PORTFOLIOPRODUCTID         = bpp.id
      AND bpp.SUBSCRIPTIONID              = bsr.id
      UNION ALL
      SELECT 'NOTBB' as bbserviceid, 'NOTBB' as bbservicetype, 'NOTBB' as bbstatus, 'NOTTEL' as telserviceid, 'NOTTEL' as telservicetype, 'NOTTEL' as telstatus, 2 FROM dual
      )
    ORDER BY rank
    )
  WHERE rownum < 2;
END dp_checkSNSServiceTypeV2;

/

  GRANT EXECUTE ON "DATAPROV"."DP_CHECKSNSSERVICETYPEV2" TO "DATAPROV_READONLY";
  GRANT EXECUTE ON "DATAPROV"."DP_CHECKSNSSERVICETYPEV2" TO "BATCHPROCESS_USER";
