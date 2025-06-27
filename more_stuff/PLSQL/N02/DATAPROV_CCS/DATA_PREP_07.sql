CREATE OR REPLACE PACKAGE data_prep_07 AS
PROCEDURE tvRecontractPauseCust ;
PROCEDURE tvRecontractPauseCustNoBurn ;
PROCEDURE soipBillView ;
PROCEDURE soipBillViewInDebt ;
PROCEDURE fpsEventTimelineStoreEventIds ;
PROCEDURE replaceHubOutOfWarranty ;
PROCEDURE replaceHubInWarranty ;
PROCEDURE act_cust_ethan ;
PROCEDURE tccUnread ;
PROCEDURE majorServiceOutages ;
PROCEDURE soipPendingActiveEdwin ;
PROCEDURE dpsBillingAccountId ;
PROCEDURE soipActiveSubDisneyPlus ;
PROCEDURE soipNoBroadBand ;
PROCEDURE soip ;
PROCEDURE soipActiveSubDigitalSales ;
PROCEDURE resubscribeSOIPwithoutPanel ;
PROCEDURE resubscribeSOIPwithPanel ;
PROCEDURE soipActiveSubWithoutAmp ;
PROCEDURE soipLessBbRegradeBurnable ;
PROCEDURE soipLessBbRegradeNonBurnable ;
PROCEDURE soipActiveSubscription_noUlm ;
PROCEDURE cinemaWithoutAmp_e2e ;
PROCEDURE addressesRoi ;
PROCEDURE notificationService ;
END data_prep_07 ;
/


create or replace PACKAGE BODY data_prep_07 AS

PROCEDURE tvRecontractPauseCust IS
   l_pool VARCHAR2(29) := 'TVRECONTRACTPAUSECUST' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO tvRecontractPauseCust t ( t.accountnumber , t.partyid , t.username , t.skycesa01token , t.messotoken )
   SELECT DISTINCT c.accountnumber , c.partyid , c.username , c.skycesa01token , c.messotoken
     FROM dataprov.customers c
     JOIN ccsowner.bsbPortfolioOffer po ON po.portfolioid = c.portfolioid
    WHERE c.dtv = 1
      AND c.sports = 1
      AND c.countryCode = 'GBR'
      AND TRUNC ( po.applicationEndDate ) < TRUNC ( SYSDATE ) - 90
      AND po.offerid IN ( '85769' , '85771' , '85772' , '88911' , '89224' , '89471' , '89477' , '89478' , '89479' , '89486' )
      AND po.status != 'ACT'
      AND NOT EXISTS (
          SELECT /*+ parallel(po2) */ NULL
            FROM ccsowner.bsbPortfolioOffer po2
           WHERE po2.portfolioId = po.portfolioId
             AND po2.offerid IN ( '85769' , '85771' , '85772' , '88911' , '89224' , '89471' , '89477' , '89478' , '89479' , '89486' )
             AND po2.status = 'ACT'
          )
      AND c.pool IS NULL
      AND ROWNUM < 150000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.username , t.skycesa01token , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.username , s.skycesa01token , s.messoToken
     FROM (
   SELECT d.accountNumber , d.partyId , d.username , d.skycesa01token , d.messoToken
     FROM tvRecontractPauseCust d
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 50000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   -- table truncated in below child pool data_prep_07.tvRecontractPauseCustNoBurn
   logger.write ( 'complete' ) ;
END tvRecontractPauseCust ;

PROCEDURE tvRecontractPauseCustNoBurn IS
   l_pool VARCHAR2(29) := 'TVRECONTRACTPAUSECUSTNOBURN' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'alter session set ddl_lock_timeout=60'; 
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.username , t.skycesa01token , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.username , s.skycesa01token , s.messoToken
     FROM (
   SELECT d.accountNumber , d.partyId , d.username , d.skycesa01token , d.messoToken
     FROM dataprov.tvRecontractPauseCust d  -- populated in above parent pool data_prep_07.tvRecontractPauseCust
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 50000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   -- Added by SM for debugging purposes
declare
v_message varchar2(3950);
v_source varchar2(50) := 'TVRECONTRACTPAUSECUSTNOBURN';
begin            
SELECT v_source||' '||
    S.SID||'-'||
    S.SERIAL#||'-'||
    P.SPID||'-'||
    S.PROGRAM||'-'||
    s.username||'-'||
    s.status||'-'|| 
    s.machine||'-'||
    nvl(s.sql_id, 'NoSQL')||'-'||
    nvl(s.prev_sql_id,'NoParentSQL')||'-'||
    s.module||'-'||
    S.LOGON_TIME||'-'||
    s.last_call_et||'-'||
    s.event
INTO v_message    
FROM
    V$LOCKED_OBJECT L,
    V$SESSION S,
    V$PROCESS P
WHERE
    L.SESSION_ID = S.SID
    AND S.PADDR = P.ADDR
    and L.OBJECT_ID = (select object_id
        from dba_objects
        where owner = 'DATAPROV'  
            and object_name = 'TVRECONTRACTPAUSECUST'); 
logger.write( v_message);           

exception
when no_data_found then 
 logger.write(v_source||' No locks found.');
end;
---
   
   execute immediate 'truncate table tvRecontractPauseCust' ;
   logger.write ( 'complete' ) ;
END tvRecontractPauseCustNoBurn ;

PROCEDURE soipBillView IS
   -- 04-Oct-2021 Andrew Fraser for Amit More. A child of soipActiveSubscription but for customers who have been billed at least once.
   l_pool VARCHAR2(29) := 'SOIPBILLVIEW' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId , t.messoToken , t.x1Accountid )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId , s.messoToken , s.x1Accountid
     FROM (
   SELECT d.accountnumber , d.partyId , d.messoToken , d.x1Accountid
     FROM soipActiveSubscription d
    WHERE d.billed = 1
      AND d.serviceType = 'SOIP'
      AND d.ulm = 1  -- 09-Feb-2022 Julian Correa must exist in ulm database.
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END soipBillView ;

PROCEDURE soipBillViewInDebt IS
   -- 10-Jan-2020 Andrew Fraser for Julian Correa. A duplicated of soipBillView but for customers who are in debt.
   l_pool VARCHAR2(29) := 'SOIPBILLVIEWINDEBT' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId , t.messoToken , t.x1Accountid )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId , s.messoToken , s.x1Accountid
     FROM (
   SELECT d.accountnumber , d.partyId , d.messoToken , d.x1Accountid
     FROM soipActiveSubscription d
    WHERE d.billed = 1
      AND d.serviceType = 'SOIP'
      AND d.ulm = 1  -- 09-Feb-2022 Julian Correa must exist in ulm database.
      AND d.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance > 0 )
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END soipBillViewInDebt ;

PROCEDURE fpsEventTimelineStoreEventIds IS
   l_pool VARCHAR2(29) := 'FPSEVENTTIMELINESTOREEVENTIDS' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.eventId1 , t.eventId2 , t.eventId3 )
   SELECT ROWNUM AS pool_seqno
        , l_pool AS pool_name
        , REGEXP_SUBSTR ( s.eventIds , '[^|]+' , 1 , 1 ) AS eventId1
        , REGEXP_SUBSTR ( s.eventIds , '[^|]+' , 1 , 2 ) AS eventId2
        , REGEXP_SUBSTR ( s.eventIds , '[^|]+' , 1 , 3 ) AS eventId3
     FROM (
            SELECT LISTAGG ( e.eventId , '|' ) WITHIN GROUP ( ORDER BY 1 ) AS eventIds
              FROM evtl.eventTimelineStore@fps e
             WHERE ROWNUM <= 10*1000*1000
             GROUP BY CEIL ( ROWNUM / 3 )
            HAVING COUNT(*) = 3
             ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100*1000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END fpsEventTimelineStoreEventIds ;

PROCEDURE replaceHubOutOfWarranty IS
   -- 12-Oct-2021 Andrew Fraser for Terence Burton, split into in-warranty and out-of-warranty pools NFTREL-21316.
   -- Customers with all hubs older than 6 months for replace Hub for Erica, Terence Burton NFTREL-20973
   l_pool VARCHAR2(29) := 'REPLACEHUBOUTOFWARRANTY' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table replaceHub' ;
   execute immediate 'alter session enable parallel dml' ;
   INSERT /*+ append parallel(8) */ INTO replaceHub t ( t.accountNumber , t.bb_serviceId , t.talk_serviceId , t.partyId , t.maxLastUpdate , t.portfolioId )
   SELECT MAX ( a.accountNumber ) AS accountNumber
        , MAX ( a.bband ) AS bb_serviceId
        , MAX ( a.talk ) AS talk_serviceId
        , a.partyId
        , MAX ( bpp.lastUpdate ) AS maxLastUpdate
        , a.portfolioId
     FROM ccsowner.bsbBillingAccount bba
     JOIN ccsowner.bsbCustomerRole bcr ON bba.portfolioId = bcr.portfolioId
     JOIN ccsowner.bsbPartyRole bpr ON bcr.partyroleId = bpr.id
     JOIN ccsowner.bsbPortfolioProduct bpp ON bba.portfolioId = bpp.portfolioId
     JOIN refdatamgr.bsbCatalogueProduct p ON bpp.catalogueProductId = p.id
     JOIN act_bb_talk_assurance a ON a.partyId = bpr.partyId
    WHERE LOWER ( p.productDescription ) LIKE '%hub%'
      AND LENGTH ( p.id ) = 5
    GROUP BY a.partyId , a.portfolioId
   HAVING MAX ( bpp.lastUpdate ) < SYSDATE - 32  -- The replace hub journey requires customers with hubs older than 31 days old before it allows another replacement.
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => 'replaceHub' ) ;
   MERGE /*+ parallel(8) */ INTO replaceHub t
   USING (
      SELECT bpp.portfolioId
           , MAX ( bpe.serviceNumber ) AS serviceNumber
        FROM ccsowner.bsbPortfolioProduct bpp
        JOIN ccsowner.bsbCustomerProductElement bcpe ON bcpe.portfolioProductId = bpp.id
        JOIN ccsowner.bsbBroadbandCustProdElement bpe ON bpe.lineProductElementId = bcpe.id
       GROUP BY bpp.portfolioId
   ) s ON ( s.portfolioId = t.portfolioId )
   WHEN MATCHED THEN UPDATE SET t.serviceNumber = s.serviceNumber
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged serviceNumber' ) ;
   COMMIT ;
   -- 17-Sep-2021 Andrew Fraser for Terence Burton NFTREL-21207, remove customers with debt.
   DELETE FROM replaceHub t WHERE t.accountNumber IN ( SELECT da.accountNumber FROM debt_amount da WHERE da.balance > 0 ) ;
   -- 12-Aug-2021 Andrew Fraser for Terence Burton NFTREL-21121, remove customers without a date of birth.
   DELETE FROM replaceHub t
    WHERE t.partyId NOT IN (
          SELECT p.partyId
            FROM ccsowner.person p
           WHERE p.birthDate IS NOT NULL          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for date of birth' ) ;
   -- 29-Jul-2021 Andrew Fraser for Terence Burton NFTREL-21097, exclude customers with open outages cos stops journey progressing thru assurance.
   -- 22-Oct-2021 Andrew Fraser for Humza Ismail NFTREL-21364, use serviceNumber aka serviceId instead of partyId (which was missing many customers).
   DELETE FROM snsOpenOutages ;
   INSERT INTO snsOpenOutages t ( t.serviceId )
   SELECT DISTINCT so.serviceId
     FROM snsOwner.bsbSnsOutageNotification@ass ou
     JOIN snsOwner.bsbSnsServiceOutage@ass so ON so.outageid = ou.id
    WHERE ( ou.closedDate IS NULL OR ou.closedDate > SYSDATE )
   ;
   COMMIT ;
   DELETE FROM replaceHub t
    WHERE t.serviceNumber IN (
          SELECT o.serviceId
            FROM snsOpenOutages o
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for outages' ) ;
   COMMIT ;
   -- 03-Aug-2021 Andrew Fraser for Terence Burton email address and mobile number checks NFTREL-21101
   DELETE /*+ parallel(8) */ FROM replaceHub d
    WHERE d.partyId NOT IN (
          SELECT bc.partyId
            FROM ccsowner.bsbContactor bc
            JOIN ccsowner.bsbContactEmail bce ON bce.contactorId = bc.id
            JOIN ccsowner.bsbEmail be ON be.id = bce.emailId
           WHERE bce.deletedFlag = 0
             AND SYSDATE BETWEEN bce.effectiveFromDate AND NVL ( bce.effectiveToDate , SYSDATE + 1 )
             AND be.emailAddressStatus = 'VALID'
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for email' ) ;
   COMMIT ;
   DELETE /*+ parallel(8) */ FROM replaceHub d
    WHERE d.partyId NOT IN (
          SELECT bc.partyId
            FROM ccsowner.bsbContactor bc
            JOIN ccsowner.bsbContactTelephone bct ON bct.contactorId = bc.id
            JOIN ccsowner.bsbTelephone bt ON bt.id = bct.telephoneId
           WHERE bct.deletedFlag = 0
             AND SYSDATE BETWEEN bct.effectiveFromDate AND NVL ( bct.effectiveToDate , SYSDATE + 1 )
             AND LENGTH ( bt.combinedTelephoneNumber ) = 11
             AND bct.typeCode = 'M'
             AND bt.telephoneNumberStatus = 'VALID'
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for mobiles' ) ;
   COMMIT ;
   -- 06-Sep-2021 Andrew Fraser for Terence Burton filter out customers who have an open engineer booking NFTREL-21173
   -- 21-Oct-2021 Andrew Fraser for Humza Ismail also exclude customers have had an engineer booked within the last 31 days NFTREL-21364.
   DELETE FROM replaceHub t
    WHERE t.accountNumber IN (
          SELECT ba.accountNumber
            FROM ccsowner.bsbVisitRequirement bvr
            JOIN ccsowner.bsbAddressUsageRole baur ON bvr.installAtionAddressRoleId = baur.id
            JOIN ccsowner.bsbServiceInstance bsi ON baur.serviceInstanceId = bsi.id
            JOIN ccsowner.bsbBillingAccount ba ON ba.portfolioId = bsi.portfolioId
           WHERE bvr.statusCode = 'BK'  -- Booked
             AND ( bvr.visitDate > SYSDATE OR bvr.lastUpdate > SYSDATE - 32 )
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for visits' ) ;
   -- 05-Oct-2021 Andrew Fraser for Terence Burton filter out customers who have received Sky Hub's within the last 31 days. The replace hub journey requires customers with hubs older than 31 days old before it allows another replacement. NFTREL-21280
   DELETE FROM replaceHub t
    WHERE t.accountNumber IN (
          SELECT bba.accountNumber
            FROM ccsowner.bsbBillingAccount bba
            JOIN ccsowner.bsbPortfolioProduct bpp ON bba.portfolioId = bpp.portfolioId
            JOIN refdatamgr.bsbCatalogueProduct p ON bpp.catalogueProductId = p.id
           WHERE LOWER ( p.productDescription ) LIKE '%hub%'
             AND LENGTH ( p.id ) = 5
             AND bpp.lastUpdate >= SYSDATE - 32
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for Sky Hub' ) ;
   COMMIT ;
   -- 09-Mar-2022 Andrew Fraser for Terence Burton NFTREL-21621
   DELETE /*+ parallel(8) */ FROM replaceHub d
    WHERE NOT EXISTS (
          SELECT NULL
            FROM ccsowner.bsbBillingAccount ba
            LEFT OUTER JOIN ccsowner.bsbPaymentMethodRole pmr ON pmr.billingAccountId = ba.id
            LEFT OUTER JOIN ccsowner.bsbPaymentMethod pm ON pm.id = pmr.paymentMethodId
            LEFT OUTER JOIN ccsowner.bsbpaymentCardDetail pcd ON pcd.portfolioId = ba.portfolioId
           WHERE ba.accountNumber = d.accountNumber
             AND (
                      (
                           pm.deletedFlag = 0
                       AND pmr.deletedFlag = 0
                       AND ( pmr.effectiveTo > SYSDATE + 1 OR pmr.effectiveTo IS NULL )
                       AND ( pm.cardExpiryDate > SYSDATE + 1 OR pm.cardExpiryDate IS NULL )
                      )
                   OR (
                           pcd.deletedFlag = 0
                       AND ( pcd.cardExpiryDate > SYSDATE + 1 OR pcd.cardExpiryDate IS NULL )
                      )
                 )
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for payments' ) ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.bb_serviceId , t.talk_serviceId , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.bb_serviceId , s.talk_serviceId , s.partyId
     FROM (
   SELECT d.accountNumber , d.bb_serviceId , d.talk_serviceId , d.partyId
     FROM replaceHub d
    WHERE d.maxLastUpdate < ADD_MONTHS ( SYSDATE , -24 )
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 300000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END replaceHubOutOfWarranty ;

PROCEDURE replaceHubInWarranty IS
   -- 12-Oct-2021 Andrew Fraser for Terence Burton, split into in-warranty and out-of-warranty pools NFTREL-21316.
   -- Customers with all hubs older than 6 months for replace Hub for Erica, Terence Burton NFTREL-20973
   l_pool VARCHAR2(29) := 'REPLACEHUBINWARRANTY' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber, t.talk_serviceId , t.bb_serviceId, t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name ,accountnumber, talk_serviceid, serviceid, partyid
   from(select /*+
                  full(bpp)  parallel(bpp 8)  pq_distribute ( bpp hash hash )
                  full(bcpe) parallel(bcpe 8) pq_distribute ( bcpe hash hash )
                  full(bpe)  parallel(bpe 8)  pq_distribute ( bpe hash hash )
                  full(c)    parallel(8)      pq_distribute (c hash hash)
                  full(r) parallel(r 8) pq_distribute (r hash hash)
                 */
   c.accountnumber accountnumber, c.partyid partyid, d.talk_serviceid, bpe.servicenumber serviceid
   from ccsowner.bsbPortfolioProduct bpp, ccsowner.bsbCustomerProductElement bcpe, ccsowner.bsbBroadbandCustProdElement bpe,
   customers c, replaceHub d
   where bcpe.portfolioProductId (+) = bpp.id
   and bpe.lineProductElementId (+) = bcpe.id
   and c.portfolioid= bpp.portfolioid
   and d.accountnumber=c.accountnumber
   and bcpe.status in ('AC', 'DL', 'DS')
   and bpp.status in ('AC', 'DL', 'DS')
   and bpe.serviceNumber is not null
   and ROWNUM <= 300000);
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END replaceHubInWarranty ;

PROCEDURE act_cust_ethan IS
   l_pool VARCHAR2(29) := 'ACT_CUST_ETHAN' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO act_cust_ethan t ( t.accountNumber , t.partyId , t.catalogueProductId , t.status )
   SELECT /*+ full(bpp) parallel(bpp 16)
              full(bba) parallel(bba 16)
              full(bcr) parallel(bcr 16)
              full(bpr) parallel(bpr 16)
              full(act) parallel(act 16)
          */
          bba.accountNumber
        , bpr.partyId
        , bpp.catalogueProductId
        , bpp.status
     FROM ccsowner.bsbPortfolioProduct bpp
     JOIN ccsowner.bsbBillingAccount bba ON bba.portfolioId = bpp.portfolioId
     JOIN ccsowner.bsbCustomerRole bcr ON bcr.portfolioId = bpp.portfolioId
     JOIN ccsowner.bsbPartyRole bpr ON bpr.id = bcr.partyRoleid
     JOIN dataprov.act_uk_cust act ON act.accountNumber = bba.accountNumber
    WHERE bpp.catalogueProductId IN ( '13947' , '13948' )  -- 13947 Grade X Box, 13948 Grade F Box 
      AND bpp.status = 'IN'
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId
     FROM (
   SELECT d.accountNumber , d.partyId
     FROM act_cust_ethan d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END act_cust_ethan ;

PROCEDURE tccUnread IS
   l_pool VARCHAR2(29) := 'TCCUNREAD' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO tccUnread t ( t.id )
   SELECT a.id
     FROM tcc_owner.bsbCommsArtifact@tcc a
    WHERE a.confirmedViewedDate IS NULL
      AND a.status IN ( 'SENT' , 'WITH_FULFILMENT_HOUSE' , 'GENERATED' )
      AND a.created > SYSDATE - 180
      AND ROWNUM <= 300000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.id )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.id
     FROM (
   SELECT d.id
     FROM tccUnread d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 300000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END tccUnread ;

PROCEDURE majorServiceOutages IS
   -- 12-Nov-2021 Andrew Fraser for Edward Falconer, NFTREL-21379.
   l_pool VARCHAR2(29) := 'MAJORSERVICEOUTAGES' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO majorServiceOutages t ( t.startDate , t.serviceId , t.closedDate , t.accountNumber , t.partyId
        , t.serviceTypeId , t.created )
   SELECT ou.startDate
        , so.serviceId
        , ou.closedDate
        , so.accountNumber
        , so.partyId
        , so.serviceTypeId
        , so.created
     FROM snsowner.bsbSnsOutageNotification@ass ou
     JOIN snsowner.bsbSnsServiceOutage@ass so ON so.outageId = ou.id
    WHERE ou.lossOfService = 'FULL'
      AND ou.closedDate IS NULL
      AND so.accountNumber IS NOT NULL
      AND ROWNUM <= 300000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.startDate , t.serviceId , t.closedDate , t.accountNumber , t.partyId
        , t.serviceTypeId , t.created )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.startDate , s.serviceId , s.closedDate , s.accountNumber , s.partyId
        , s.serviceTypeId , s.created
     FROM (
   SELECT d.startDate , d.serviceId , d.closedDate , d.accountNumber , d.partyId , d.serviceTypeId , d.created
     FROM majorServiceOutages d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END majorServiceOutages ;

PROCEDURE soipPendingActiveEdwin IS
   -- 16-Nov-2021 Andrew Fraser for Edwin Scariachin, copy of soipPendingActive for some specific test script of Edwin's.
   l_pool VARCHAR2(29) := 'SOIPPENDINGACTIVEEDWIN' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'alter session set ddl_lock_timeout=60'; 
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   -- 16-Nov-2021 Andrew Fraser for Edwin Scariachin, restrict to delivered (not awaiting_delivery) products, to avoid test script error "pimm rule id rule_restrict_tv_hardware_return_before_delivery has resulted in a do not allow outcome".
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken , t.emailaddress )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken , s.emailaddress
     FROM (
   SELECT d.accountNumber , d.partyId , d.messoToken , d.emailaddress
     FROM soipPendingActive d
    WHERE d.delivered = 1
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   -- Added by SM for debugging purposes
declare
v_message varchar2(3950);
v_source varchar2(50) := 'SOIPPENDINGACTIVEEDWIN';
begin            
SELECT v_source||' '||
    S.SID||'-'||
    S.SERIAL#||'-'||
    P.SPID||'-'||
    S.PROGRAM||'-'||
    s.username||'-'||
    s.status||'-'|| 
    s.machine||'-'||
    nvl(s.sql_id, 'NoSQL')||'-'||
    nvl(s.prev_sql_id,'NoParentSQL')||'-'||
    s.module||'-'||
    S.LOGON_TIME||'-'||
    s.last_call_et||'-'||
    s.event
INTO v_message    
FROM
    V$LOCKED_OBJECT L,
    V$SESSION S,
    V$PROCESS P
WHERE
    L.SESSION_ID = S.SID
    AND S.PADDR = P.ADDR
    and L.OBJECT_ID = (select object_id
        from dba_objects
        where owner = 'DATAPROV'  
            and object_name = 'SOIPPENDINGACTIVE'); 
logger.write( v_message);           

exception
when no_data_found then 
 logger.write(v_source||' No locks found.');
end;
---   
   execute immediate 'truncate table soipPendingActive' ;
   logger.write ( 'complete' ) ;
END soipPendingActiveEdwin ;

PROCEDURE dpsBillingAccountId IS
/*
18-Nov-2021 Andrew Fraser for Amit More, based on sql from Andy Garden.
Be aware we have different fields sharing same name:
1.1) DPS billingAccountId is an integer of 10 digit length max, example data - 20800740.
1.2) Chordiant CCS billingAccountId is a string of 47 characters length max, example data - 'CH10136160S'.
2.1) DPS accountNumber is a string of 50 characters length max, example data - 'CH60554601S'
2.2) Chordiant CCS accountNumber is a string of 12 characters length max, in practice all digits, example data - '622102502706'
*/
   l_pool VARCHAR2(29) := 'DPSBILLINGACCOUNTID' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO dpsBillingAccountId t ( t.billingAccountId , t.accountNumber )
   SELECT asg.billingAccountId , ba.accountNumber
     FROM dpsSuper.dpsAccountServiceGroup@dps asg
     JOIN dpsSuper.dpsBillingAccount@dps ba ON ba.billingAccountId = asg.billingAccountId
    WHERE asg.activeFlag = 'A'
      AND asg.process_type = 1
      AND ROWNUM <= 500000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.billingAccountId , t.accountNumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.billingAccountId , s.accountNumber
     FROM (
   SELECT TO_CHAR ( d.billingAccountId ) AS billingAccountId
        , d.accountNumber
     FROM dpsBillingAccountId d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   --SOIPPOD-2672--execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END dpsBillingAccountId ;

PROCEDURE soipActiveSubDisneyPlus IS
   -- 23-Nov-2021 Andrew Fraser for Deepa Satam NFTREL-21388
   -- 27/11/23 (RFA) - Use table CUSTOMERTOKENS to retrieve the MessoToken 
   l_pool VARCHAR2(29) := 'SOIPACTIVESUBDISNEYPLUS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipActiveSubDisneyPlus t ( t.accountNumber , t.partyId , t.messoToken )
   SELECT ba.accountNumber
        , ct.partyId
        , ct.messoToken
     FROM ccsowner.bsbBillingAccount ba
     JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
     JOIN rcrm.product p ON s.id = p.serviceId
     LEFT JOIN dataprov.customertokens ct ON ba.accountNumber = ct.accountNumber
    WHERE p.suid = 'AMP_DISNEY'
      AND p.status = 'ACTIVE'
   ;
   logger.write ( 'soipActiveSubDisneyPlus :'||to_char(sql%rowcount)||' rows updated' ) ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId , s.messoToken
     FROM (
   SELECT d.accountnumber , d.partyId , d.messoToken
     FROM soipActiveSubDisneyPlus d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipActiveSubDisneyPlus ;

PROCEDURE soipNoBroadBand IS
-- https://cbsjira.bskyb.com/browse/NFTREL-21397
   l_pool VARCHAR2(29) := 'SOIPNOBROADBAND' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId , t.postcode , t.firstName , t.familyName )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId , s.postcode , s.firstName , s.familyName
     FROM (
           SELECT d.accountNumber , d.partyId , d.postcode , d.firstName , d.familyName
             FROM soipActiveSubscription d
            WHERE d.bband = 0
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END soipNoBroadBand ;

PROCEDURE soip IS
-- https://cbsjira.bskyb.com/browse/NFTREL-21398
   l_pool VARCHAR2(29) := 'SOIP' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId , t.postcode , t.firstName , t.familyName )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId , s.postcode , s.firstName , s.familyName
     FROM (
           SELECT d.accountNumber , d.partyId , d.postcode , d.firstName , d.familyName
             FROM soipActiveSubscription d
            WHERE d.bband = 0
              AND d.talk = 0
              AND d.previousCases IS NULL
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END soip ;

PROCEDURE soipActiveSubDigitalSales IS
-- https://cbsjira.bskyb.com/browse/NFTREL-21407
   l_pool VARCHAR2(29) := 'SOIPACTIVESUBDIGITALSALES' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId , s.messotoken
     FROM (
           SELECT d.accountNumber , d.partyId , d.messotoken
             FROM soipActiveSubscription d
            WHERE d.skykids = 0
              AND d.netflix = 0
              AND d.sports = 0
              AND d.ulm = 0
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END soipActiveSubDigitalSales ;

PROCEDURE resubscribeSoipWithoutPanel IS
   -- 17-Dec-2021 Andrew Fraser for Alex Benetatos SOIPPOD-2633
   l_pool VARCHAR2(29) := 'RESUBSCRIBESOIPWITHOUTPANEL' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO resubscribeSoipWithoutPanel t ( t.accountNumber , t.partyId )
   SELECT DISTINCT ba.accountNumber
        , pr.partyId
     FROM ccsowner.bsbPartyRole pr
     JOIN ccsowner.bsbCustomerRole cr ON pr.id = cr.partyRoleId
     JOIN ccsowner.bsbBillingAccount ba ON ba.portfolioId = cr.portfolioId
     JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
     JOIN rcrm.product p ON s.id = p.serviceId
     JOIN rcrm.productPricingItemLink pp ON pp.productId = p.id
     JOIN rcrm.pricingItem pi ON pp.pricingItemId = pi.id
    WHERE pi.ccaId IS NULL
      AND p.suid = 'SOIP_TV_SKY_SIGNATURE'
      AND p.status = 'CEASED'
      AND p.serviceId IN (
          SELECT p2.serviceId
            FROM rcrm.product p2
           WHERE p2.suid IN ( 'LLAMA_LARGE' , 'LLAMA_MEDIUM' , 'LLAMA_SMALL' )
             AND p2.status = 'DELIVERED'
          )
      AND p.serviceId NOT IN (
          SELECT p3.serviceId
            FROM rcrm.product p3
           WHERE p3.status = 'PENDING_CEASE'
          )
      AND p.serviceId NOT IN (
          SELECT p4.serviceId
            FROM rcrm.product p4
           WHERE p4.suid = 'SOIP_TV_SKY_SIGNATURE'
             AND p4.status = 'ACTIVE'
          )
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId
     FROM (
   SELECT d.accountnumber , d.partyId
     FROM resubscribeSoipWithoutPanel d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END resubscribeSoipWithoutPanel ;

PROCEDURE resubscribeSoipWithPanel IS
   -- 17-Dec-2021 Andrew Fraser for Alex Benetatos SOIPPOD-2634
   l_pool VARCHAR2(29) := 'RESUBSCRIBESOIPWITHPANEL' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO resubscribeSoipWithPanel t ( t.accountNumber , t.partyId )
   SELECT DISTINCT ba.accountNumber
        , pr.partyId
     FROM ccsowner.bsbPartyRole pr
     JOIN ccsowner.bsbCustomerRole cr ON pr.id = cr.partyRoleId
     JOIN ccsowner.bsbBillingAccount ba ON ba.portfolioId = cr.portfolioId
     JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
     JOIN rcrm.product p ON s.id = p.serviceId
     JOIN rcrm.productPricingItemLink pp ON pp.productId = p.id
     JOIN rcrm.pricingItem pi ON pp.pricingItemId = pi.id
    WHERE pi.ccaId IS NULL
      AND p.suid = 'SOIP_TV_SKY_SIGNATURE'
      AND p.status = 'CEASED'
      AND p.serviceId IN (
          SELECT p2.serviceId
            FROM rcrm.product p2
           WHERE p2.suid IN ( 'LLAMA_LARGE' , 'LLAMA_MEDIUM' , 'LLAMA_SMALL' )
             AND p2.status IN ( 'RETURNED' , 'CEASED' , 'LOST' )
          )
      AND p.serviceId NOT IN (
          SELECT p3.serviceId
            FROM rcrm.product p3
           WHERE p3.status = 'PENDING_CEASE'
          )
      AND p.serviceId NOT IN (
          SELECT p4.serviceId
            FROM rcrm.product p4
           WHERE p4.suid = 'SOIP_TV_SKY_SIGNATURE'
             AND p4.status = 'ACTIVE'
          )
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId
     FROM (
   SELECT d.accountnumber , d.partyId
     FROM resubscribeSoipWithPanel d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END resubscribeSOIPwithPanel ;

PROCEDURE soipActiveSubWithoutAmp IS
   -- 02-Feb-2022 Andrew Fraser for Archana Burla NFTREL-21517
   l_pool VARCHAR2(29) := 'SOIPACTIVESUBWITHOUTAMP' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId  , t.messoToken , t.x1Accountid
        , t.ssoToken , t.skyCesa01Token )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId  , s.messoToken , s.x1Accountid , s.ssoToken , s.skyCesa01Token
     FROM (
   SELECT d.accountnumber , d.partyId  , d.messoToken , d.x1Accountid , d.ssoToken , d.skyCesa01Token
     FROM soipActiveSubscription d
    WHERE d.inFlightVisit = 0  -- 19-Jan-2022 Humza Ismail NFTREL-21473 to soipActiveSubscription pool, maybe not needed for Archana's pool?
      AND d.returnInTransit = 0 -- 21-Jan-2022 Humza Ismail NFTREL-21480 to soipActiveSubscription pool, maybe not needed for Archana's pool?
      AND d.customerHasAmp = 0
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END soipActiveSubWithoutAmp ;

PROCEDURE soipLessBbRegradeBurnable IS
   l_pool VARCHAR2(29) := 'SOIPLESSBBREGRADEBURNABLE' ;
BEGIN
   logger.write ( 'begin' ) ;
   UPDATE bbRegrade t SET t.soip = 0 ;
   MERGE INTO bbRegrade t USING (
      SELECT DISTINCT bpr.partyId
        FROM ccsowner.bsbBillingAccount ba
        JOIN ccsowner.bsbCustomerRole bcr ON bcr.portfolioId = ba.portfolioId
        JOIN ccsowner.bsbPartyRole bpr ON bpr.id = bcr.partyRoleId
        JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
       WHERE s.serviceType = 'SOIP'
   ) s ON ( s.partyId = t.partyId )
   WHEN MATCHED THEN UPDATE SET t.soip = 1
   ;
   UPDATE bbRegrade t SET t.debt = 0 ;
   MERGE INTO bbRegrade t USING (
      SELECT bpr.partyId
        FROM ccsowner.bsbBillingAccount ba
        JOIN ccsowner.bsbCustomerRole bcr ON bcr.portfolioId = ba.portfolioId
        JOIN ccsowner.bsbPartyRole bpr ON bpr.id = bcr.partyRoleId
        JOIN dataprov.debt_amount da ON da.accountNumber = ba.accountNumber
       GROUP BY bpr.partyId
      HAVING SUM ( da.balance ) > 0
   ) s ON ( s.partyId = t.partyId )
   WHEN MATCHED THEN UPDATE SET t.debt = 1
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.data , t.skycesa01token , t.messotoken
      , t.ssotoken , t.firstName , t.familyName , t.emailAddress , t.code
      )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.data , s.skycesa01token , s.messotoken
        , s.ssotoken , s.firstName , s.familyName , s.emailAddress , s.code
     FROM (
   SELECT d.accountNumber , d.partyId , d.data , d.skycesa01token , d.messotoken
        , d.ssotoken , d.firstName , d.familyName , d.emailAddress , d.code
     FROM bbRegrade d
    WHERE d.soip = 0
      AND d.debt = 0
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END soipLessBbRegradeBurnable ;

PROCEDURE soipLessBbRegradeNonBurnable IS
   l_pool VARCHAR2(29) := 'SOIPLESSBBREGRADENONBURNABLE' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.data , t.skycesa01token , t.messotoken
      , t.ssotoken , t.firstName , t.familyName , t.emailAddress , t.code
      )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.data , s.skycesa01token , s.messotoken
        , s.ssotoken , s.firstName , s.familyName , s.emailAddress , s.code
     FROM (
   SELECT d.accountNumber , d.partyId , d.data , d.skycesa01token , d.messotoken
        , d.ssotoken , d.firstName , d.familyName , d.emailAddress , d.code
     FROM bbRegrade d
    WHERE d.soip = 0
      AND d.debt = 0
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END soipLessBbRegradeNonBurnable ;

PROCEDURE soipActiveSubscription_noUlm IS
   -- 01-Mar-2022 Andrew Fraser for Julian Correa, copy of soipActiveSubscription except with ulm=0 instead of ulm=1
   l_pool VARCHAR2(29) := 'SOIPACTIVESUBSCRIPTION_NOULM' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId  , t.messoToken , t.x1Accountid
        , t.ssoToken , t.skyCesa01Token )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId  , s.messoToken , s.x1Accountid , s.ssoToken , s.skyCesa01Token
     FROM (
   SELECT d.accountnumber , d.partyId  , d.messoToken , d.x1Accountid , d.ssoToken , d.skyCesa01Token
     FROM soipActiveSubscription d
    WHERE d.inFlightVisit = 0  -- 19-Jan-2022 Humza Ismail NFTREL-21473
      AND d.returnInTransit = 0 -- 21-Jan-2022 Humza Ismail NFTREL-21480
      AND d.ulm = 0  -- 01-Mar-2022 Andrew Fraser for Julian Correa, not in ulm database.
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END soipActiveSubscription_noUlm ;

PROCEDURE cinemaWithoutAmp_e2e IS
   -- 24-Jan-2023 Andrew Fraser for Michael Santos, only include customers with 14504 'Sky Cinema' product. SOIPPOD-2736
   -- 14-Apr-2022 Andrew Fraser for Archana Burla increased volume from 100k to 1m.
   -- 14-Mar-2022 Andrew Fraser initial creation for Archana Burla NFTREL-21633
   -- Future enhancement: add these two fields into main customers table
   l_pool VARCHAR2(29) := 'CINEMAWITHOUTAMP_E2E' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'alter session enable parallel dml' ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO cinemaWithoutAmp_e2e t ( t.accountNumber , t.partyId , t.messoToken , t.ssoToken , t.skyCesa01Token
        , t.customerHasAmp , t.countryCode , t.portfolioId )
   SELECT s.accountnumber , s.partyId  , s.messoToken , s.ssoToken , s.skyCesa01Token , s.customerHasAmp , s.countryCode , s.portfolioId
     FROM (
           SELECT c.accountnumber , c.partyId  , c.messoToken , c.ssoToken , c.skyCesa01Token , 0 AS customerHasAmp , c.countryCode , c.portfolioId
             FROM customers c
            WHERE c.cinema = 1
              and c.skyqbox = 1
              and c.skyhdbox= 0
            ORDER BY dbms_random.value
          ) s
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   execute immediate 'alter session enable parallel dml' ;
   -- 24-Jan-2023 Andrew Fraser for Michael Santos SOIPPOD-2736, exclude customers who get error: "code":"journey.management.customer.not.eligible","description":"The customer is not eligible for intent","data":{"intent":"add-amp-paramount","failedPrecondition":"ACTIVE_CUSTOMER"
   DELETE FROM cinemaWithoutAmp_e2e t
    WHERE t.partyId IN (
          SELECT s.partyId
            FROM act_cust_uk_subs s
           WHERE s.dtv != 'AC'
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for active_customer error' ) ;
   COMMIT ;
   -- 24-Jan-2023 Andrew Fraser for Michael Santos, only include customers with 14504 'Sky Cinema' product. SOIPPOD-2736
   DELETE /*+ parallel(8) */ FROM cinemaWithoutAmp_e2e t
    WHERE NOT EXISTS (
          SELECT NULL
            FROM ccsowner.bsbPortfolioProduct pp
           WHERE pp.portfolioId = t.portfolioId
             AND pp.catalogueProductId = '14504'  -- 'Sky Cinema'
             --AND pp.status NOT IN ( 'CN' , 'FBI' , 'SC' , 'RP' )  -- CN=cancelled FBI=Cancelled Talk SC=System Cancelled RP=Replaced: for all these is as if the customer never had the products.
             AND pp.status = 'EN'  -- alternative check to above for EN=enabled
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted because did not have 14504 Sky Cinema.' ) ;
   COMMIT ;
   DELETE /*+ parallel(8) */ FROM cinemaWithoutAmp_e2e t
    WHERE EXISTS (
          SELECT NULL
            FROM ccsowner.bsbPortfolioProduct pp
           WHERE pp.portfolioId = t.portfolioId
             AND pp.catalogueProductId in ('15246','15247') --TNT Sports
             AND pp.status = 'EN'  -- alternative check to above for EN=enabled
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted because did have 15246 or 15247 TNT Sports.' ) ;
   COMMIT ;
   MERGE /*+ parallel(8) */ INTO cinemaWithoutAmp_e2e t USING (
      SELECT DISTINCT pr.partyId
        FROM ccsowner.bsbPartyRole pr
        JOIN ccsowner.bsbCustomerRole cr ON pr.id = cr.partyRoleId
        JOIN ccsowner.bsbBillingAccount ba ON ba.portfolioId = cr.portfolioId
        JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
       WHERE s.serviceType = 'AMP'
   ) s ON ( t.partyId = s.partyId )
   -- removed by AH 13/12/2022 as column is populated with 0 as default and never null
   --WHEN MATCHED THEN UPDATE SET t.customerHasAmp = 1 WHERE t.customerHasAmp IS NULL
   WHEN MATCHED THEN UPDATE SET t.customerHasAmp = 1
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged for customerHasAmp' ) ;
   COMMIT ;
   -- 19-Jan-2013 Andrew Fraser Delete the rows we don't care about for performance in next set of merges.
   DELETE FROM cinemaWithoutAmp_e2e t
    WHERE t.customerHasAmp = 1
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for customerHasAmp' ) ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   -- 28-Feb-2023 Michael Santos PERFENG-574 hopefully only a temporary exclusion to be removed later in 2023.
   DELETE FROM cinemaWithoutAmp_e2e t
    WHERE EXISTS (
          SELECT NULL
            FROM ccsowner.bsbPortfolioOffer po
           WHERE po.portfolioId = t.portfolioId
             AND po.offerId IN ( 50557 , 80889 , 91809 , 73850 , 50558 , 75112 , 73805 , 51628 )
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for portfolioOffer' ) ;
   COMMIT ;
   MERGE INTO cinemaWithoutAmp_e2e t
   USING (
      SELECT bba.accountNumber , MAX ( pte.x1accountId ) AS x1accountId
        FROM ccsowner.bsbBillingAccount bba
        JOIN ccsowner.portfolioTechElement pte ON pte.portfolioId = bba.portfolioId
       GROUP BY bba.accountNumber
   ) s ON ( s.accountNumber = t.accountNumber )
   WHEN MATCHED THEN UPDATE SET t.x1accountId = s.x1accountId WHERE NVL ( t.x1accountId , 'x' ) !=  NVL ( s.x1accountId , 'x' )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged for x1accountId' ) ;
   COMMIT ;
   -- 25-Oct-2022 Michael Santos add cardNumber
   MERGE /*+ parallel(16) */ INTO cinemaWithoutAmp_e2e t
   USING (
      SELECT ba.accountNumber , MAX ( cpe.cardNumber ) AS cardNumber
        FROM ccsowner.bsbBillingAccount ba
        JOIN ccsowner.bsbPortfolioProduct pp ON pp.portfolioId = ba.portfolioId
        JOIN ccsowner.bsbCustomerProductElement cpe ON pp.id = cpe.portfolioProductId
       WHERE cpe.customerProductElementType = 'VC'
         AND cpe.status = 'A'
       GROUP BY ba.accountNumber
   ) s ON ( s.accountNumber = t.accountNumber )
   WHEN MATCHED THEN UPDATE SET t.cardNumber = s.cardNumber WHERE t.accountNumber = s.accountNumber
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged for cardNumber' ) ;
   COMMIT ;
   -- 27-Jan-2023 allocate half the data to this pool (which burns) and the other half to otherwise identical non-burnable pool "cinemaWithoutAmp_journeyStarts" which is in data_prep_09. Michael Santos SOIPPOD-2738.
   UPDATE cinemaWithoutAmp_e2e t SET t.burnPool = MOD ( ROWNUM , 2 ) ;
   -- but non-burnable pool has to exclude customers with outstanding balance (aka 'debt')
   UPDATE cinemaWithoutAmp_e2e t SET t.burnPool = 1
    WHERE t.burnPool = 0
      AND t.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance > 0 )  -- has an outstanding balance
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' updated for burnPool' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId  , t.messoToken , t.x1Accountid
        , t.ssoToken , t.skyCesa01Token , t.cardNumber , t.location )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId  , s.messoToken , s.x1Accountid
        , s.ssoToken , s.skyCesa01Token , s.cardNumber , s.location
     FROM (
           SELECT d.accountnumber , d.partyId  , d.messoToken , d.x1Accountid , d.ssoToken , d.skyCesa01Token , d.cardNumber
                , d.countryCode AS location
             FROM cinemaWithoutAmp_e2e d
            WHERE d.customerHasAmp = 0
              AND d.burnPool = 1
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 1000000
   ;
   -- 11-Jan-2013 Michael Santos, changed to burn, SOIPPOD-2732
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END cinemaWithoutAmp_e2e ;

PROCEDURE addressesRoi IS
   -- 01-Apr-2022 Andrew Fraser for Alex Benetatos. All 50 addresses point to a stub. SOIPPOD-2690.
   -- The eircodes used are real, are all public buildings like libraries, found by google searches.
   l_pool VARCHAR2(29) := 'ADDRESSESROI' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT ALL
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 1 , l_pool , 'Stub Street 01' , 'Stub County 01' , 'Stub Street 01' , 'A94TX94' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 2 , l_pool , 'Stub Street 02' , 'Stub County 01' , 'Stub Street 02' , 'D01DY80' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 3 , l_pool , 'Stub Street 03' , 'Stub County 01' , 'Stub Street 03' , 'D01F5P2' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 4 , l_pool , 'Stub Street 04' , 'Stub County 01' , 'Stub Street 04' , 'D01K0F1' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 5 , l_pool , 'Stub Street 05' , 'Stub County 01' , 'Stub Street 05' , 'D02AF30' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 6 , l_pool , 'Stub Street 01' , 'Stub County 01' , 'Stub Street 01' , 'D02F627' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 7 , l_pool , 'Stub Street 02' , 'Stub County 01' , 'Stub Street 02' , 'D02FH48' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 8 , l_pool , 'Stub Street 03' , 'Stub County 01' , 'Stub Street 03' , 'D02HE37' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 9 , l_pool , 'Stub Street 04' , 'Stub County 01' , 'Stub Street 04' , 'D02HP38' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 10 , l_pool , 'Stub Street 05' , 'Stub County 01' , 'Stub Street 05' , 'D02PN40' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 11 , l_pool , 'Stub Street 11' , 'Stub County 01' , 'Stub Street 11' , 'D02TN83' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 12 , l_pool , 'Stub Street 12' , 'Stub County 01' , 'Stub Street 12' , 'D02VY53' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 13 , l_pool , 'Stub Street 13' , 'Stub County 01' , 'Stub Street 13' , 'D02W710' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 14 , l_pool , 'Stub Street 14' , 'Stub County 01' , 'Stub Street 14' , 'D04H765' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 15 , l_pool , 'Stub Street 15' , 'Stub County 01' , 'Stub Street 15' , 'D04HF53' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 16 , l_pool , 'Stub Street 16' , 'Stub County 01' , 'Stub Street 16' , 'D04TP03' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 17 , l_pool , 'Stub Street 17' , 'Stub County 01' , 'Stub Street 17' , 'D04V1W8' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 18 , l_pool , 'Stub Street 18' , 'Stub County 01' , 'Stub Street 18' , 'D04Y970' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 19 , l_pool , 'Stub Street 19' , 'Stub County 01' , 'Stub Street 19' , 'D06K2K6' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 20 , l_pool , 'Stub Street 20' , 'Stub County 01' , 'Stub Street 20' , 'D07AYW1' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 21 , l_pool , 'Stub Street 21' , 'Stub County 01' , 'Stub Street 21' , 'D07VX54' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 22 , l_pool , 'Stub Street 22' , 'Stub County 01' , 'Stub Street 22' , 'D07XKV4' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 23 , l_pool , 'Stub Street 23' , 'Stub County 01' , 'Stub Street 23' , 'D08EY79' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 24 , l_pool , 'Stub Street 24' , 'Stub County 01' , 'Stub Street 24' , 'D08HN3X' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 25 , l_pool , 'Stub Street 25' , 'Stub County 02' , 'Stub Street 25' , 'D08VF8H' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 26 , l_pool , 'Stub Street 26' , 'Stub County 02' , 'Stub Street 26' , 'D08YY05' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 27 , l_pool , 'Stub Street 27' , 'Stub County 02' , 'Stub Street 27' , 'D09PT78' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 28 , l_pool , 'Stub Street 28' , 'Stub County 02' , 'Stub Street 28' , 'D11DCR7' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 29 , l_pool , 'Stub Street 29' , 'Stub County 02' , 'Stub Street 29' , 'D11F76T' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 30 , l_pool , 'Stub Street 30' , 'Stub County 02' , 'Stub Street 30' , 'D12ET22' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 31 , l_pool , 'Stub Street 31' , 'Stub County 02' , 'Stub Street 31' , 'D12FK18' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 32 , l_pool , 'Stub Street 32' , 'Stub County 02' , 'Stub Street 32' , 'D6WYC59' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 33 , l_pool , 'Stub Street 33' , 'Stub County 02' , 'Stub Street 33' , 'F23HY31' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 34 , l_pool , 'Stub Street 34' , 'Stub County 02' , 'Stub Street 34' , 'P31H674' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 35 , l_pool , 'Stub Street 35' , 'Stub County 02' , 'Stub Street 35' , 'T12HDY2' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 36 , l_pool , 'Stub Street 36' , 'Stub County 02' , 'Stub Street 36' , 'T12K8AF' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 37 , l_pool , 'Stub Street 37' , 'Stub County 02' , 'Stub Street 37' , 'T12NT99' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 38 , l_pool , 'Stub Street 38' , 'Stub County 02' , 'Stub Street 38' , 'T12P928' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 39 , l_pool , 'Stub Street 39' , 'Stub County 02' , 'Stub Street 39' , 'T12P928' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 40 , l_pool , 'Stub Street 40' , 'Stub County 02' , 'Stub Street 40' , 'T12R2NC' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 41 , l_pool , 'Stub Street 41' , 'Stub County 02' , 'Stub Street 41' , 'T12RR84' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 42 , l_pool , 'Stub Street 42' , 'Stub County 02' , 'Stub Street 42' , 'T12WP57' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 43 , l_pool , 'Stub Street 43' , 'Stub County 02' , 'Stub Street 43' , 'T23AC97' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 44 , l_pool , 'Stub Street 44' , 'Stub County 02' , 'Stub Street 44' , 'T23AP11' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 45 , l_pool , 'Stub Street 45' , 'Stub County 02' , 'Stub Street 45' , 'T23E651' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 46 , l_pool , 'Stub Street 41' , 'Stub County 02' , 'Stub Street 41' , 'T23N250' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 47 , l_pool , 'Stub Street 42' , 'Stub County 02' , 'Stub Street 42' , 'T45E033' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 48 , l_pool , 'Stub Street 43' , 'Stub County 02' , 'Stub Street 43' , 'V94EH90' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 49 , l_pool , 'Stub Street 44' , 'Stub County 02' , 'Stub Street 44' , 'V94R7YE' )
      INTO dprov_accounts_fast ( pool_seqno , pool_name , street , town , county , postcode ) VALUES ( 50 , l_pool , 'Stub Street 45' , 'Stub County 02' , 'Stub Street 45' , 'V94RF63' )
      SELECT * FROM DUAL
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END addressesRoi ;

PROCEDURE notificationService IS
-- 15-Nov-2022 Michael Santos added 'AMP_DISCOVERY_PLUS' NFTREL-22077
   l_pool VARCHAR2(29) := 'NOTIFICATIONSERVICE' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO notificationService t ( t.accountNumber , t.portfolioId , t.accountId , t.subscription , t.skyExtAccountId )
   SELECT DISTINCT ba.accountNumber , pte.portfolioId , pte.ampAccountId AS accountid , rde.externalOrderId AS subscription , pte.skyExtAccountId
     FROM ccsowner.bsbBillingAccount ba
     JOIN ccsowner.portfolioTechElement pte ON ba.portfolioId = pte.portfolioId
     JOIN rcrm.service rs ON rs.billingServiceInstanceId = ba.serviceInstanceId
     JOIN rcrm.product rp ON rp.serviceId = rs.id
     JOIN rcrm.digitalMediaProdTechElement rde ON rp.id = rde.productId
    WHERE pte.ampAccountId IS NOT NULL
      AND rp.suid IN ( 'AMP_PARAMOUNT_PLUS' , 'AMP_DISNEY' , 'AMP_DISCOVERY_PLUS' )
      AND ROWNUM <= 10000000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.portfolioId , t.accountId , t.subscription , t.skyExtAccountId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.portfolioId , s.accountId , s.subscription , s.skyExtAccountId
     FROM (
   SELECT d.accountNumber , d.portfolioId , d.accountId , d.subscription , d.skyExtAccountId
     FROM notificationService d
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END notificationService ;

PROCEDURE DisneyStandard IS
   -- 23-Nov-2021 Andrew Fraser for Deepa Satam NFTREL-21388
   -- 27/11/23 (RFA) - Use table CUSTOMERTOKENS to retrieve the MessoToken 
   l_pool VARCHAR2(29) := 'DISNEYSTANDARD' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO DisneyStandard t ( t.accountNumber , t.partyId , t.messoToken )
   SELECT /*+ parallel(4) */ ba.accountNumber
        , ct.partyId
        , ct.messoToken
     FROM ccsowner.bsbBillingAccount ba
     JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
     JOIN rcrm.product p ON s.id = p.serviceId
     LEFT JOIN dataprov.customertokens ct ON ba.accountNumber = ct.accountNumber
    WHERE p.suid = 'AMP_DISNEY_STANDARD'
      AND p.status = 'ACTIVE'
   ;
   logger.write ( 'soipActiveSubDisneyPlus :'||to_char(sql%rowcount)||' rows updated' ) ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId , s.messoToken
     FROM (
   SELECT d.accountnumber , d.partyId , d.messoToken
     FROM DisneyStandard d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END DisneyStandard ;

END data_prep_07 ;
/