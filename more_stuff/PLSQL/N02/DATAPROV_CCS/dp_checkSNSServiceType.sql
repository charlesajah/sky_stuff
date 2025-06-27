--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure DP_CHECKSNSSERVICETYPE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."DP_CHECKSNSSERVICETYPE" 
     (p_serviceid in varchar2,
                  bbserviceID out varchar2,
                  bbservicetype out varchar2,
                  telserviceid out varchar2,
                  telservicetype out varchar2) as
begin
select bbserviceid,bbservicetype,telserviceid,telservicetype into bbserviceID,bbservicetype,telserviceid,telservicetype
FROM
  (SELECT *
   FROM
     (SELECT bpe.SERVICENUMBER AS bbserviceid,
             bpe.MIDNETCLASSIFICATION AS bbservicetype,
             'NOTTEL' AS telserviceid,
             'NOTTEL' AS telservicetype,
             1 rank
      FROM CCSOWNER.BSBBROADBANDCUSTPRODELEMENT bpe
      WHERE bpe.SERVICENUMBER = p_serviceid
      UNION SELECT 'NOTBB',
                   'NOTBB',
                   bte.SERVICEID AS telserviceid,
                   'OFFNET_VOICE' AS telservicetype,
                   1 rank
      FROM CCSOWNER.BSBTELEPHONYCUSTPRODELEMENT bte
      WHERE bte.SERVICEID = p_serviceid
      UNION ALL SELECT 'NOTBB',
                       'NOTBB',
                       'NOTTEL',
                       'NOTTEL',
                       2
      FROM dual)
   ORDER BY rank)
WHERE rownum < 2;
end dp_checkSNSServiceType;

/

  GRANT EXECUTE ON "DATAPROV"."DP_CHECKSNSSERVICETYPE" TO "BATCHPROCESS_USER";
