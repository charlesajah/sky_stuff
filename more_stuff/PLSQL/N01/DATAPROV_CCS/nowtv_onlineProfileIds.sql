--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure NOWTV_ONLINEPROFILEIDS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."NOWTV_ONLINEPROFILEIDS" AS
/*
|| Name : dataprov.nowtv_onlineprofileids
|| Database : chordo
|| Author : Andrew Fraser
|| Date : 21-Dec-2016
|| Purpose : Use Barry Perez/Don Eastes (Osterley) list of fake onlineProfileIds to switch chordiant NowTV customers onlineProfileId's for Barry's testing of NowTV2.0.
|| Script to generate zipfile of this data for Barry : unora0za:/home/sitescope/ptt/scripts/nft_nowtv_onlineprofileid.bash
|| Tables used :
||    nowtv_onlineprofileids_ost = list supplied by Barry Perez of fake onlineProfileID's we can make use of.
||    nowtv_onlineprofileids_cbs = staging table.
||    nowtv_onlineprofileids_log = for safety, yet another store of what has been changed and when.
|| Steps to clean up after n01 environment refresh from production are:
CREATE TABLE dataprov.nowtv_onlineprofileids_cbs_old AS SELECT * FROM dataprov.nowtv_onlineprofileids_cbs ;
CREATE TABLE dataprov.nowtv_onlineprofileids_ost_old AS SELECT * FROM dataprov.nowtv_onlineprofileids_ost ;
CREATE TABLE dataprov.nowtv_onlineprofileids_log_old AS SELECT * FROM dataprov.nowtv_onlineprofileids_log ;
TRUNCATE TABLE dataprov.nowtv_onlineprofileids_cbs ;
TRUNCATE TABLE dataprov.nowtv_onlineprofileids_log ;
UPDATE dataprov.nowtv_onlineprofileids_ost SET old_onlineProfileId = NULL , lastUpdate = NULL WHERE old_onlineProfileId IS NOT NULL OR lastUpdate IS NOT NULL ;  -- keep the fake new numbers from Barry's supplied list.
|| Change History:
||   30-Jan-2017 Andrew Fraser switched to merges with new seqno column for performance, replaces slow row-by-row loop with rownum<=1 update.
||   11-Jan-2017 Andrew Fraser rewrote to use slow-by-slow pl/sql loop - original fast merge had functional bug causing duplicate data.
*/
BEGIN
   -- 0) List of duplicates, will exclude these from subsequent processing.
   DELETE nowtv_duplicates
   ;
   INSERT INTO nowtv_duplicates
   SELECT pe.onlineProfileId
        --, min(pa.id) , max(pa.id)
        --, min(ba.accountNumber) , max(ba.accountNumber)
        --, SUM ( CASE WHEN cpe.status = 'AC' AND r.productDescription = 'NOW TV Entertainment Month Pass' THEN 1 ELSE 0 END ) AS entertainment_active
        --, SUM ( CASE WHEN cpe.status = 'AC' AND r.productDescription = 'NOW TV Sky Cinema Month Pass' THEN 1 ELSE 0 END ) AS cinema_active
        --, SUM ( CASE WHEN cpe.status = 'AC' AND r.productDescription = 'NOW TV Sky Sports Month Pass' THEN 1 ELSE 0 END ) AS sports_active
        --, count(distinct(pa.id || ba.accountNumber))
     FROM ccsowner.party pa
     JOIN ccsowner.person pe ON pe.partyId = pa.id
     JOIN ccsowner.bsbPartyRole pr ON pr.partyId = pa.id
     JOIN ccsowner.bsbCustomerRole cr ON cr.partyRoleId = pr.id
     JOIN ccsowner.bsbBillingAccount ba ON ba.portfolioId = cr.portfolioId
     JOIN ccsowner.bsbPortfolioProduct pp ON pp.portfolioId = ba.portfolioId
     JOIN ccsowner.bsbCustomerProductElement cpe ON pp.id = cpe.portfolioProductId
     JOIN refdatamgr.bsbCatalogueProduct r ON r.id = pp.catalogueProductId
     LEFT OUTER JOIN dataprov.nowtv_onlineprofileids_ost d ON d.new_onlineProfileId = pe.onlineProfileId
    WHERE pa.id LIKE 'NM%'  -- for migrated only, could change to *all* unit 20 NowTV?
      AND pe.onlineProfileId NOT LIKE '999%'  -- not yet allocated to one of Barry Perez' fake 999... online profile Ids.
      --AND pe.created > TO_DATE ( '-Dec-2016' , 'DD-Mon-YYYY' )
      --AND cpe.status = 'AC'
    GROUP BY pe.onlineProfileId
   HAVING count(distinct(pa.id || ba.accountNumber)) > 1  -- exclude those with multiple accounts associated with a partyId.
   ;
   -- 1) build up list of not yet allocated
   DELETE dataprov.nowtv_onlineprofileids_cbs  -- 37secs, could replace with truncate
   ;
   INSERT INTO dataprov.nowtv_onlineprofileids_cbs
   SELECT pe.onlineProfileId AS old_onlineProfileId
        , NULL AS new_onlineProfileId
        , pa.id AS partyId
        , ba.accountNumber
        , SUM ( CASE WHEN cpe.status = 'AC' AND r.productDescription = 'NOW TV Entertainment Month Pass' THEN 1 ELSE 0 END ) AS entertainment_active
        , SUM ( CASE WHEN cpe.status = 'AC' AND r.productDescription = 'NOW TV Sky Cinema Month Pass' THEN 1 ELSE 0 END ) AS cinema_active
        , SUM ( CASE WHEN cpe.status = 'AC' AND r.productDescription = 'NOW TV Sky Sports Month Pass' THEN 1 ELSE 0 END ) AS sports_active
        , NULL  -- seqno
     FROM ccsowner.party pa
     JOIN ccsowner.person pe ON pe.partyId = pa.id
     JOIN ccsowner.bsbPartyRole pr ON pr.partyId = pa.id
     JOIN ccsowner.bsbCustomerRole cr ON cr.partyRoleId = pr.id
     JOIN ccsowner.bsbBillingAccount ba ON ba.portfolioId = cr.portfolioId
     JOIN ccsowner.bsbPortfolioProduct pp ON pp.portfolioId = ba.portfolioId
     JOIN ccsowner.bsbCustomerProductElement cpe ON pp.id = cpe.portfolioProductId
     JOIN refdatamgr.bsbCatalogueProduct r ON r.id = pp.catalogueProductId
    WHERE pa.id LIKE 'NM%'  -- for migrated only, could change to *all* unit 20 NowTV?
      AND pe.onlineProfileId NOT LIKE '999%'  -- not yet allocated to one of Barry Perez' fake 999... online profile Ids.
      --AND pe.created > TO_DATE ( '-Dec-2016' , 'DD-Mon-YYYY' )
      AND NOT EXISTS (
          SELECT NULL FROM nowtv_duplicates dup WHERE dup.onlineProfileId = pe.onlineProfileId
          )
    GROUP BY pe.onlineProfileId , pa.id , ba.accountNumber
   ;
   -- 1a check no duplicates in that (shouldn't be)
   -- SELECT old_onlineProfileId , COUNT(*) FROM dataprov.nowtv_onlineprofileids_cbs GROUP BY old_onlineProfileId HAVING COUNT(*) > 1 ORDER BY 1 ;
   -- 2) allocate those out - this step was slow, so changed to use seqno column merge join instead of slow row-by-row loop with rownum<=1 update.
   -- 2a) top priority is sports
   UPDATE dataprov.nowtv_onlineprofileids_ost s SET s.seqno = NULL WHERE s.seqno IS NOT NULL ;
   UPDATE dataprov.nowtv_onlineprofileids_cbs t SET t.seqno = NULL WHERE t.seqno IS NOT NULL ;
   UPDATE dataprov.nowtv_onlineprofileids_ost s SET s.seqno = ROWNUM WHERE s.old_onlineProfileId IS NULL AND s.new_onlineprofileid IS NOT NULL AND s.ent_type = 'sportsmonth' ;
   UPDATE dataprov.nowtv_onlineprofileids_cbs t SET t.seqno = ROWNUM WHERE t.old_onlineProfileId IS NOT NULL AND t.new_onlineprofileid IS NULL AND t.sports_active = 1 ;
   MERGE INTO dataprov.nowtv_onlineprofileids_cbs t
   USING (
      SELECT new_onlineProfileId , seqno
        FROM dataprov.nowtv_onlineprofileids_ost so
       WHERE so.old_onlineProfileId IS NULL AND so.new_onlineprofileid IS NOT NULL AND so.ent_type = 'sportsmonth'
      ) s
      ON ( t.seqno = s.seqno )
    WHEN MATCHED THEN UPDATE
     SET t.new_onlineProfileId = s.new_onlineProfileId
   WHERE t.old_onlineProfileId IS NOT NULL AND t.new_onlineprofileid IS NULL AND t.sports_active = 1
   ;
   -- 2b) second top priority is movies/cinema
   UPDATE dataprov.nowtv_onlineprofileids_ost s SET s.seqno = NULL WHERE s.seqno IS NOT NULL ;
   UPDATE dataprov.nowtv_onlineprofileids_cbs t SET t.seqno = NULL WHERE t.seqno IS NOT NULL ;
   UPDATE dataprov.nowtv_onlineprofileids_ost s SET s.seqno = ROWNUM WHERE s.old_onlineProfileId IS NULL AND s.new_onlineprofileid IS NOT NULL AND s.ent_type = 'movies' ;
   UPDATE dataprov.nowtv_onlineprofileids_cbs t SET t.seqno = ROWNUM WHERE t.old_onlineProfileId IS NOT NULL AND t.new_onlineprofileid IS NULL AND t.cinema_active = 1 ;
   MERGE INTO dataprov.nowtv_onlineprofileids_cbs t
   USING (
      SELECT new_onlineProfileId , seqno
        FROM dataprov.nowtv_onlineprofileids_ost so
       WHERE so.old_onlineProfileId IS NULL AND so.new_onlineprofileid IS NOT NULL AND so.ent_type = 'movies'
      ) s
      ON ( t.seqno = s.seqno )
    WHEN MATCHED THEN UPDATE
     SET t.new_onlineProfileId = s.new_onlineProfileId
   WHERE t.old_onlineProfileId IS NOT NULL AND t.new_onlineprofileid IS NULL AND t.cinema_active = 1
   ;
   -- 2c) third is ents
   UPDATE dataprov.nowtv_onlineprofileids_ost s SET s.seqno = NULL WHERE s.seqno IS NOT NULL ;
   UPDATE dataprov.nowtv_onlineprofileids_cbs t SET t.seqno = NULL WHERE t.seqno IS NOT NULL ;
   UPDATE dataprov.nowtv_onlineprofileids_ost s SET s.seqno = ROWNUM WHERE s.old_onlineProfileId IS NULL AND s.new_onlineprofileid IS NOT NULL AND s.ent_type = 'ents' ;
   UPDATE dataprov.nowtv_onlineprofileids_cbs t SET t.seqno = ROWNUM WHERE t.old_onlineProfileId IS NOT NULL AND t.new_onlineprofileid IS NULL AND t.entertainment_active = 1 ;
   MERGE INTO dataprov.nowtv_onlineprofileids_cbs t
   USING (
      SELECT new_onlineProfileId , seqno
        FROM dataprov.nowtv_onlineprofileids_ost so
       WHERE so.old_onlineProfileId IS NULL AND so.new_onlineprofileid IS NOT NULL AND so.ent_type = 'ents'
      ) s
      ON ( t.seqno = s.seqno )
    WHEN MATCHED THEN UPDATE
     SET t.new_onlineProfileId = s.new_onlineProfileId
   WHERE t.old_onlineProfileId IS NOT NULL AND t.new_onlineprofileid IS NULL AND t.entertainment_active = 1
   ;
   -- 2d) lastly Osterley 'sportsday' = no entitlements at all for CBS, an inactive customer.
   UPDATE dataprov.nowtv_onlineprofileids_ost s SET s.seqno = NULL WHERE s.seqno IS NOT NULL ;
   UPDATE dataprov.nowtv_onlineprofileids_cbs t SET t.seqno = NULL WHERE t.seqno IS NOT NULL ;
   UPDATE dataprov.nowtv_onlineprofileids_ost s SET s.seqno = ROWNUM WHERE s.old_onlineProfileId IS NULL AND s.new_onlineprofileid IS NOT NULL AND s.ent_type = 'sportsday' ;
   UPDATE dataprov.nowtv_onlineprofileids_cbs t SET t.seqno = ROWNUM WHERE t.old_onlineProfileId IS NOT NULL AND t.new_onlineprofileid IS NULL AND t.sports_active = 0 AND t.cinema_active = 0 AND t.entertainment_active = 0 ;
   MERGE INTO dataprov.nowtv_onlineprofileids_cbs t
   USING (
      SELECT new_onlineProfileId , seqno
        FROM dataprov.nowtv_onlineprofileids_ost so
       WHERE so.old_onlineProfileId IS NULL AND so.new_onlineprofileid IS NOT NULL AND so.ent_type = 'sportsday'
      ) s
      ON ( t.seqno = s.seqno )
    WHEN MATCHED THEN UPDATE
     SET t.new_onlineProfileId = s.new_onlineProfileId
   WHERE t.old_onlineProfileId IS NOT NULL AND t.new_onlineprofileid IS NULL AND t.sports_active = 0 AND t.cinema_active = 0 AND t.entertainment_active = 0 
   ;
   -- 3) for all those allocated, update real ccsowner table and osterley table and keep a record of what we have done in log table. This step takes 4 minutes - coluld improve using forall loop.
   -- CREATE TABLE dataprov.nowtv_onlineprofileids_log AS SELECT onlineProfileId AS old_onlineProfileId , onlineProfileId AS new_onlineProfileId , SYSDATE AS when_changed FROM ccsowner.person WHERE 1=2 ;
   FOR d1 IN (
      SELECT d.old_onlineProfileId , d.new_onlineProfileId
        FROM dataprov.nowtv_onlineprofileids_cbs d
        JOIN ccsowner.person pe ON pe.onlineProfileId = d.old_onlineProfileId
       WHERE d.new_onlineProfileId IS NOT NULL
   )
   LOOP
      -- update real ccsowner table
      UPDATE ccsowner.person pe
         SET pe.onlineProfileId = d1.new_onlineProfileId , pe.lastUpdate = SYSDATE , pe.updatedBy = USER
       WHERE pe.onlineProfileId = d1.old_onlineProfileId
      ;
      -- update osterley Barry Perez table
      UPDATE dataprov.nowtv_onlineprofileids_ost ost
         SET ost.old_onlineProfileId = d1.old_onlineProfileId
       WHERE ost.new_onlineProfileId = d1.new_onlineProfileId
      ;
      -- keep a record of what we have done in logging table
      INSERT INTO dataprov.nowtv_onlineprofileids_log l ( old_onlineProfileId , new_onlineProfileId , when_changed )
      VALUES ( d1.old_onlineProfileId , d1.new_onlineProfileId , SYSDATE )
      ;
   END LOOP ;
END nowtv_onlineProfileIds ;

/

  GRANT EXECUTE ON "DATAPROV"."NOWTV_ONLINEPROFILEIDS" TO "BATCHPROCESS_USER";
