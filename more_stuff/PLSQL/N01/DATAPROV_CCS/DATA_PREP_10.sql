CREATE OR REPLACE PACKAGE data_prep_10 AS
PROCEDURE marketplaceCustNoInteraction ;
PROCEDURE marketplaceSpsInteractionRef ;
PROCEDURE MobileCustomerBuckets ;
PROCEDURE activeAmpWithToken ;
PROCEDURE actCustomer0Recommendations ;
PROCEDURE actCustomer1Recommendations ;
PROCEDURE actCustomer3Recommendations ;
PROCEDURE actCustomer5Recommendations ;
PROCEDURE actCustomer12Recommendations ;
PROCEDURE fspUpgradeSkySiglwsweb ;
PROCEDURE cancelConfirmCommunication ;
PROCEDURE auraHub6activation ;
PROCEDURE skyPilSubscriberId ;
PROCEDURE engineerVisitCurrentDay ;
PROCEDURE emailSharedWithTwoPartyIds ;
PROCEDURE auraActivatedUsers ;
PROCEDURE consumerdutypayholmob ;
PROCEDURE qHarmBurn ;
PROCEDURE soipCcaNoDebt;
PROCEDURE validUrns;
PROCEDURE eSimActive;
PROCEDURE digitalCurrentBbNoDebt;
PROCEDURE redeemedUrns;
END data_prep_10 ;
/


create or replace PACKAGE BODY data_prep_10 AS

PROCEDURE marketplaceCustNoInteraction IS
-- 02-Nov-2022 Andrew Fraser for Antti - removed code filter that excluded customers with an interaction (meaning "NoInteraction" part of data pool name is now a misnomer).
   l_pool VARCHAR2(29) := 'MARKETPLACECUSTNOINTERACTION' ;
   l_count NUMBER ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   COMMIT ;
   INSERT /*+ append */ INTO marketplaceCustNoInteraction t ( t.x1accountId , t.partyId )
   SELECT MAX ( pte.x1accountId ) AS x1accountId , bpr.partyId
     FROM ccsowner.bsbBillingAccount ba
     JOIN ccsowner.bsbCustomerRole bcr ON ba.portfolioId = bcr.portfolioId
     JOIN ccsowner.bsbPartyRole bpr ON bcr.partyroleId = bpr.id
     JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
     JOIN ccsowner.portfolioTechElement pte ON pte.portfolioId = ba.portfolioId
    WHERE s.serviceType = 'SOIP'
      AND pte.x1accountId IS NOT NULL
      --AND ROWNUM <= 300000  -- 06-Sep-2022 Andrew Fraser temporarily allow max data during stress testing.
    GROUP BY bpr.partyId
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   -- 01-Nov-2022 Antti add a comma delimited list of customers previously bought PPV product Ids (use externalId column in dprov_accounts_fast)
   MERGE INTO marketplaceCustNoInteraction t USING (
      WITH /*+ materialize */ q AS (
         SELECT DISTINCT c.partyId , ppv.catalogue_product_id
           FROM portfolio.customer@smpuk c
           JOIN portfolio.account@smpuk a ON a.portfolioId = c.portfolioId  -- despite the name, is NOT related to CCS portfolioId.
           JOIN portfolio.service@smpuk s ON s.accountNumber = a.accountNumber
           JOIN portfolio.product@smpuk p ON p.serviceId = s.id
           JOIN productCatalogue.catalogue_item@smpuk ci ON ci.id = p.productIdentifier
           JOIN productCatalogue.ppv_item@smpuk ppv ON ppv.ppv_id = ci.ppv_id
      )
      SELECT q.partyId
           , SUBSTR ( LISTAGG ( q.catalogue_product_id , ',' ) WITHIN GROUP ( ORDER BY q.catalogue_product_id ) , 1 , 4000 ) AS externalId
        FROM q
       GROUP BY q.partyId
   ) s ON ( s.partyId = t.partyId )
   WHEN MATCHED THEN UPDATE SET t.externalId = s.externalId
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged for externalId' ) ;
   -- at therequest of Archana Burla 10/04/2025
   delete from marketplaceCustNoInteraction where externalId is null;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for null externalId' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.x1accountId , t.externalId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.x1accountId , s.externalId
     FROM (
           SELECT d.x1accountId , d.externalId
             FROM marketplaceCustNoInteraction d
            ORDER BY dbms_random.value
          ) s
    --WHERE ROWNUM <= 100000  -- 06-Sep-2022 Andrew Fraser temporarily allow max data during stress testing.
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END marketplaceCustNoInteraction ;

PROCEDURE marketplaceSpsInteractionRef IS
   l_pool VARCHAR2(29) := 'MARKETPLACESPSINTERACTIONREF' ;
   l_count NUMBER ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO marketplaceSpsInteractionRef t ( t.reference )
   SELECT si.reference
     FROM sps_owner.salesInteraction@smpgp si
    WHERE si.created > TRUNC ( SYSTIMESTAMP ) - 4  -- (was -6) housekeeping job can mean only ?7 days partitions are kept.
      AND ROWNUM <= 300000
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   COMMIT ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.data )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.data
     FROM (
           SELECT d.reference AS data
             FROM marketplaceSpsInteractionRef d
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END marketplaceSpsInteractionRef ;

PROCEDURE MobileCustomerBuckets IS
   l_pool VARCHAR2(29) := 'MOBCUSBKT_' ;
   intBkt    integer := 0 ;
   intBktTop integer := 9;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   
   INSERT /*+ append */ INTO MOBCUSBKT_ t ( t.accountnumber , t.partyid , t.portfolioId , t.messoToken , t.port_cnt, t.NSPROFILEID )
   SELECT cus.accountnumber , cus.partyid , cus.portfolioId , cus.messoToken, acpb.port_cnt, cus.NSPROFILEID
     FROM (SELECT /*+ parallel(16) */ s.portfolioId , COUNT(*) AS port_cnt
            FROM ccsowner.bsbportFolioProduct s
   GROUP BY s.portfolioId) acpb
     JOIN customers cus ON cus.portfolioId = acpb.portfolioId
    WHERE cus.countryCode = 'GBR' 
     AND cus.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )
     AND cus.emailAddress NOT LIKE 'noemail%'
     AND cus.inFlightVisit = 0
     AND cus.inFlightOrders = 0
     AND cus.mobile = 1   ;
   COMMIT ;
   
   while intBkt <= 500
   LOOP
     l_pool := 'MOBCUSBKT_' || intBkt;
     logger.write ( 'Processing : ' || l_pool) ;
    sequence_pkg.seqBefore ( i_pool => l_pool ) ;
    
    if intBkt = 500 then
      INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.portfolioId , t.messoToken, t.NSPROFILEID )
      SELECT ROWNUM AS pool_seqno , l_pool AS pool_name, s.accountNumber, s.partyId, s.portfolioId, s.messoToken, s.NSPROFILEID
        FROM ( select rownum, accountnumber , partyid , portfolioId , messoToken, NSPROFILEID 
               from MOBCUSBKT_ where port_cnt >= intBkt
             FETCH FIRST 500 rows with ties ) s ;
    else
      INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.portfolioId , t.messoToken, t.NSPROFILEID )
      SELECT ROWNUM AS pool_seqno , l_pool AS pool_name, s.accountNumber, s.partyId, s.portfolioId, s.messoToken, s.NSPROFILEID
        FROM ( select rownum, accountnumber , partyid , portfolioId , messoToken, NSPROFILEID 
               from MOBCUSBKT_ where port_cnt between intBkt and intBktTop
             FETCH FIRST 500 rows with ties ) s ;
     end if ;
     sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => FALSE ) ;

     intBkt := intBkt+10;
     intBktTop := intBkt+9;    
   END LOOP ;   
   execute immediate 'truncate table MOBCUSBKT_' ;
   logger.write ( 'complete' ) ;
END MobileCustomerBuckets ;

PROCEDURE activeAmpWithToken IS
   l_pool VARCHAR2(29) := 'ACTIVEAMPWITHTOKEN' ;
   l_count NUMBER ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO activeAmpWithToken t ( t.partyId , t.x1accountId )
   SELECT DISTINCT cp.partyId
        , cp.x1accountId
     FROM mgs_owner.customerProduct@fps cp
     JOIN mgs_owner.activationToken@fps atn ON cp.id = atn.customerProductId
    WHERE ( atn.expirationTimestamp IS NULL OR atn.expirationTimestamp > SYSTIMESTAMP + 2 )
    AND cp.x1accountId is not null -- Added by SM 08AUG2023 for Michael Santos PERFENG-912
      AND ROWNUM <= 300000
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   COMMIT ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.partyId , t.x1accountId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.partyId , s.x1accountId
     FROM (
           SELECT d.partyId , d.x1accountId
             FROM activeAmpWithToken d
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END activeAmpWithToken ;

PROCEDURE actCustomer0Recommendations IS
   l_pool VARCHAR2(29) := 'ACTCUSTOMER0RECOMMENDATIONS' ;
   l_count NUMBER ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId
     FROM (
           SELECT c.accountNumber , c.partyId
             FROM customers c
            WHERE SUBSTR ( c.accountNumber , -3 ) BETWEEN '050' AND '099'
              AND c.countryCode = 'GBR'
              AND c.dtv IS NOT NULL  -- exclude soip/amp only, altho not sure if that exclusion is really needed.
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 1000000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END actCustomer0Recommendations ;

PROCEDURE actCustomer1Recommendations IS
   l_pool VARCHAR2(29) := 'ACTCUSTOMER1RECOMMENDATIONS' ;
   l_count NUMBER ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId
     FROM (
           SELECT c.accountNumber , c.partyId
             FROM customers c
            WHERE SUBSTR ( c.accountNumber , -3 ) BETWEEN '500' AND '517'
              AND c.countryCode = 'GBR'
              AND c.dtv IS NOT NULL  -- exclude soip/amp only, altho not sure if that exclusion is really needed.
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 1000000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END actCustomer1Recommendations ;

PROCEDURE actCustomer3Recommendations IS
   l_pool VARCHAR2(29) := 'ACTCUSTOMER3RECOMMENDATIONS' ;
   l_count NUMBER ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId
     FROM (
           SELECT c.accountNumber , c.partyId
             FROM customers c
            WHERE SUBSTR ( c.accountNumber , -3 ) BETWEEN '518' AND '537'
              AND c.countryCode = 'GBR'
              AND c.dtv IS NOT NULL  -- exclude soip/amp only, altho not sure if that exclusion is really needed.
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 1000000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END actCustomer3Recommendations ;

PROCEDURE actCustomer5Recommendations IS
   l_pool VARCHAR2(29) := 'ACTCUSTOMER5RECOMMENDATIONS' ;
   l_count NUMBER ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId
     FROM (
           SELECT c.accountNumber , c.partyId
             FROM customers c
            WHERE SUBSTR ( c.accountNumber , -3 ) BETWEEN '538' AND '549'
              AND c.countryCode = 'GBR'
              AND c.dtv IS NOT NULL  -- exclude soip/amp only, altho not sure if that exclusion is really needed.
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 1000000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END actCustomer5Recommendations ;

PROCEDURE actCustomer12Recommendations IS
   l_pool VARCHAR2(29) := 'ACTCUSTOMER12RECOMMENDATIONS' ;
   l_count NUMBER ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId
     FROM (
           SELECT c.accountNumber , c.partyId
             FROM customers c
            WHERE SUBSTR ( c.accountNumber , -3 ) BETWEEN '700' AND '749'
              AND c.countryCode = 'GBR'
              AND c.dtv IS NOT NULL  -- exclude soip/amp only, altho not sure if that exclusion is really needed.
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 1000000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END actCustomer12Recommendations ;

procedure fspupgradeskysiglwsweb IS
   l_pool VARCHAR2(29) := 'FSPUPGRADESKYSIGLWSWEB' ;
begin
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.messoToken , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber, s.messotoken, s.partyId
     FROM (select /*+ parallel(8) */ c.accountnumber, c.partyid, c.messotoken
             from dataprov.customers c
            where c.skysignature = 1
              and c.kids = 0
              and c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- No Customers in debt
              and c.pool is null
              and c.countryCode = 'GBR'
           ORDER BY dbms_random.value) s
    WHERE ROWNUM <= 100000
   ;
   
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END fspupgradeskysiglwsweb ;

PROCEDURE cancelConfirmCommunication IS
   l_pool VARCHAR2(29) := 'CANCELCONFIRMCOMMUNICATION' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO cancelConfirmCommunication t ( t.data )
   SELECT a.id AS data
     FROM tcc_owner.bsbCommsArtifact@tcc a
    WHERE a.status = 'PENDING_ISSUE'
      AND ROWNUM <= 400000
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted.' ) ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.data )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.data
     FROM (
           SELECT d.data
             FROM cancelConfirmCommunication d
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END cancelConfirmCommunication ;

PROCEDURE auraHub6activation IS
   -- 19-May-2023 Lee Byrnes partyId added
   l_pool VARCHAR2(29) := 'AURAHUB6ACTIVATION' ;
   l_count NUMBER ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO auraHub6activation t ( t.accountNumber , t.partyId , t.x1accountId , t.serialNumber , t.macAddress )
   SELECT DISTINCT ba.accountNumber
        , bpr.partyId
        , pte.x1accountId
        , hpe.serialNumber
        , br.macAddress
     FROM ccsowner.bsbBillingAccount ba
     JOIN ccsowner.bsbServiceInstance si ON si.portfolioId = ba.portfolioId
     JOIN ccsowner.portfolioTechElement pte ON pte.portfolioId = ba.portfolioId
     JOIN rcrm.service s ON s.billingServiceInstanceId = ba.serviceInstanceId
     JOIN rcrm.product p ON p.serviceId = s.id
     JOIN rcrm.hardwareProdTechelement hpe ON hpe.productId = p.id
     JOIN ccsowner.bsbBroadbandRouter br ON br.serialNumber = hpe.serialNumber
     JOIN ccsowner.bsbCustomerRole bcr ON bcr.portfolioId = ba.portfolioId
     JOIN ccsowner.bsbPartyRole bpr ON bpr.id = bcr.partyRoleId
    WHERE p.suid = 'HUB_6'
      AND p.eventcode = 'DELIVERED'
      AND ROWNUM <= 300000
   ;
   l_count := SQL%ROWCOUNT ;
   logger.write ( TO_CHAR ( l_count ) || ' inserted' ) ;
   COMMIT ;
   IF l_count = 1
   THEN
      logger.write ( 'Padding with two extra rows because only inserted ' || TO_CHAR ( l_count ) || ' rows as workaround to avoid NODATA exception in url endpoint' ) ;
      INSERT INTO auraHub6activation t ( t.accountNumber , t.partyId , t.x1accountId , t.serialNumber , t.macAddress )
      SELECT s.accountNumber , s.partyId , s.x1accountId , s.serialNumber , s.macAddress
        FROM auraHub6activation s
      ;
      INSERT INTO auraHub6activation t ( t.accountNumber , t.partyId , t.x1accountId , t.serialNumber , t.macAddress )
      SELECT s.accountNumber , s.partyId, s.x1accountId , s.serialNumber , s.macAddress
        FROM auraHub6activation s
       WHERE ROWNUM <= 1
      ;
   ELSIF l_count = 2
   THEN
      logger.write ( 'Padding with one extra row because only inserted ' || TO_CHAR ( l_count ) || ' rows as workaround to avoid NODATA exception in url endpoint' ) ;
      INSERT INTO auraHub6activation t ( t.accountNumber , t.partyId , t.x1accountId , t.serialNumber , t.macAddress )
      SELECT s.accountNumber , s.partyId , s.x1accountId , s.serialNumber , s.macAddress
        FROM auraHub6activation s
       WHERE ROWNUM <= 1
      ;
   END IF ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.x1accountId , t.serialNumber , t.data )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.x1accountId , s.serialNumber , s.data
     FROM (
           SELECT d.accountNumber , d.partyId , d.x1accountId , d.serialNumber , d.macAddress AS data
             FROM auraHub6activation d
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END auraHub6activation ;

PROCEDURE skyPilSubscriberId IS
   l_pool VARCHAR2(29) := 'SKYPILSUBSCRIBERID' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO skyPilSubscriberId t ( t.accountNumber )
   SELECT c.accountNumber
     FROM customers c
    WHERE EXISTS (
          SELECT NULL
            FROM ccsowner.portfolioTechElement pte
           WHERE pte.portfolioId = c.portfolioId
             AND pte.skyPilSubscriberId IS NOT NULL
          )
      AND ROWNUM <= 300000
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted.' ) ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber
     FROM (
           SELECT d.accountNumber
             FROM skyPilSubscriberId d
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END skyPilSubscriberId ;

PROCEDURE engineerVisitCurrentDay IS
   l_pool VARCHAR2(29) := 'ENGINEERVISITCURRENTDAY' ;
   l_count NUMBER ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO engineerVisitCurrentDay t ( t.setTopBoxNo , t.cardNumber , t.cardSubscriberId )
   SELECT MAX ( cpe.setTopBoxNdsNumber ) AS setTopBoxNo
        , MAX ( cpe.cardNumber ) AS cardNumber
        , si.cardSubscriberId
     FROM ccsowner.bsbPortfolioProduct pp
     JOIN ccsowner.bsbServiceInstance si ON si.id = pp.serviceInstanceId
     JOIN ccsowner.bsbAddressUsageRole aur ON aur.serviceInstanceId = si.parentServiceInstanceId
     JOIN ccsowner.bsbVisitRequirement vr ON vr.installationAddressRoleId = aur.id
     JOIN ccsowner.bsbPortfolioProduct ppAll ON ppAll.serviceInstanceId = pp.serviceInstanceId
     JOIN ccsowner.bsbCustomerProductElement cpe ON cpe.portfolioProductId = ppAll.id
    WHERE vr.visitDate BETWEEN TRUNC ( SYSDATE )
                           AND TO_DATE ( TO_CHAR ( SYSDATE , 'DD-Mon-YYYY' ) || ' 23:59:59' , 'DD-Mon-YYYY HH24:MI:SS' )
      AND vr.statusCode IN ( 'BK' , 'CF' )  -- Booked / Confirmed
      AND ppAll.status IN ( 'IN' , 'AI' , 'A' , 'T' )
      AND pp.catalogueProductId = '13427'  -- 'Install Visit'
      AND pp.status = 'AV'  -- 'Awaiting Visit'
      AND vr.fmsJobReference LIKE 'VR%'
    GROUP BY si.cardSubscriberId
   HAVING MAX ( cpe.cardNumber ) IS NOT NULL
      AND MAX ( cpe.setTopBoxNdsNumber ) IS NOT NULL
   ;
   l_count := SQL%ROWCOUNT ;
   logger.write ( TO_CHAR ( l_count ) || ' inserted' ) ;
   COMMIT ;
   IF l_count = 1
   THEN
      logger.write ( 'Padding with two extra rows because only inserted ' || TO_CHAR ( l_count ) || ' rows as workaround to avoid NODATA exception in url endpoint' ) ;
      INSERT INTO engineerVisitCurrentDay t ( t.setTopBoxNo , t.cardNumber , t.cardSubscriberId )
      SELECT s.setTopBoxNo , s.cardNumber , s.cardSubscriberId
        FROM engineerVisitCurrentDay s
      ;
      INSERT INTO engineerVisitCurrentDay t ( t.setTopBoxNo , t.cardNumber , t.cardSubscriberId )
      SELECT s.setTopBoxNo , s.cardNumber , s.cardSubscriberId
        FROM engineerVisitCurrentDay s
       WHERE ROWNUM <= 1
      ;
   ELSIF l_count = 2
   THEN
      logger.write ( 'Padding with one extra row because only inserted ' || TO_CHAR ( l_count ) || ' rows as workaround to avoid NODATA exception in url endpoint' ) ;
      INSERT INTO engineerVisitCurrentDay t ( t.setTopBoxNo , t.cardNumber , t.cardSubscriberId )
      SELECT s.setTopBoxNo , s.cardNumber , s.cardSubscriberId
        FROM engineerVisitCurrentDay s
       WHERE ROWNUM <= 1
      ;
   END IF ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.setTopBoxNo , t.cardNumber , t.cardSubscriberId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.setTopBoxNo , s.cardNumber , s.cardSubscriberId
     FROM (
           SELECT d.setTopBoxNo , d.cardNumber , d.cardSubscriberId
             FROM engineerVisitCurrentDay d
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END engineerVisitCurrentDay ;

PROCEDURE emailSharedWithTwoPartyIds IS
   l_pool VARCHAR2(29) := 'EMAILSHAREDWITHTWOPARTYIDS' ;
   l_prevEmail VARCHAR2(128) ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO emailSharedWithTwoPartyIds t ( t.partyId , t.emailAddress )
   SELECT /*+ noparallel */ c.partyId , c.emailAddress
     FROM customers c
    WHERE c.emailAddress LIKE 'EMAIL_%@bskyb.com'
      AND NOT EXISTS (
          SELECT NULL
            FROM dprov_accounts_fast f
           WHERE f.emailAddress = c.emailAddress
          )
    ORDER BY dbms_random.value
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   DELETE FROM emailSharedWithTwoPartyIds t
    WHERE t.emailAddress IN (
          SELECT d.emailAddress
            FROM emailSharedWithTwoPartyIds d
           GROUP BY d.emailAddress
          HAVING COUNT(*) > 1
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted' ) ;
   FOR r1 IN (
      SELECT s.partyId
           , s.emailAddress
           , CASE WHEN MOD ( ROWNUM , 2 ) = 0 THEN 1 ELSE 0 END AS changeEmail
           , s.ROWID AS rid
        FROM emailSharedWithTwoPartyIds s
       WHERE ROWNUM <= 100*1000
   )
   LOOP
      IF r1.changeEmail = 0
      THEN
         l_prevEmail := r1.emailAddress ;
      ELSIF r1.changeEmail = 1
      THEN
         UPDATE emailSharedWithTwoPartyIds t SET t.newEmail = l_prevEmail , t.dateChanged = SYSDATE
          WHERE t.ROWID = r1.rid
         ;
         UPDATE ccsowner.bsbEmail t SET t.emailAddress = l_prevEmail
          WHERE UPPER ( t.emailAddress ) = UPPER ( r1.emailAddress )
         ;
      END IF ;
   END LOOP ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.emailAddress )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.emailAddress
     FROM (
           SELECT d.newEmail AS emailAddress
             FROM emailSharedWithTwoPartyIds d
            WHERE d.newEmail IS NOT NULL
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100*1000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   DELETE FROM emailSharedWithTwoPartyIds t
    WHERE t.newEmail IS NULL
      AND NOT EXISTS (
          SELECT NULL
            FROM emailSharedWithTwoPartyIds d
           WHERE t.emailAddress = d.newEmail
          )
   ;
   logger.write ( 'complete' ) ;
END emailSharedWithTwoPartyIds ;

PROCEDURE auraActivatedUsers IS
-- 18-May-2023 Lee Byrnes add accountNumber and partyId columns.
   l_pool VARCHAR2(29) := 'AURAACTIVATEDUSERS' ;
   l_count NUMBER ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO auraActivatedUsers t ( t.accountNumber , t.partyId , t.messoToken )
   SELECT DISTINCT ba.accountNumber , c.partyId , c.messoToken
     FROM ccsowner.bsbBillingAccount ba
     JOIN rcrm.service s ON s.billingServiceInstanceId = ba.serviceInstanceId
     JOIN rcrm.product p ON p.serviceId = s.id
     JOIN customers c ON c.portfolioId = ba.portfolioId
    WHERE p.suid = 'HUB_6'
      AND p.eventcode = 'XFI_ONBOARDING_COMPLETE'
      AND ROWNUM <= 300000
   ;
   l_count := SQL%ROWCOUNT ;
   logger.write ( TO_CHAR ( l_count ) || ' inserted' ) ;
   COMMIT ;
   IF l_count = 1
   THEN
      logger.write ( 'Padding with two extra rows because only inserted ' || TO_CHAR ( l_count ) || ' rows as workaround to avoid NODATA exception in url endpoint' ) ;
      INSERT INTO auraActivatedUsers t ( t.accountNumber , t.partyId , t.messoToken )
      SELECT s.accountNumber , s.partyId , s.messoToken
        FROM auraActivatedUsers s
      ;
      INSERT INTO auraActivatedUsers t ( t.accountNumber , t.partyId , t.messoToken )
      SELECT s.accountNumber , s.partyId , s.messoToken
        FROM auraActivatedUsers s
       WHERE ROWNUM <= 1
      ;
   ELSIF l_count = 2
   THEN
      logger.write ( 'Padding with one extra row because only inserted ' || TO_CHAR ( l_count ) || ' rows as workaround to avoid NODATA exception in url endpoint' ) ;
      INSERT INTO auraActivatedUsers t ( t.accountNumber , t.partyId , t.messoToken )
      SELECT s.accountNumber , s.partyId , s.messoToken
        FROM auraActivatedUsers s
       WHERE ROWNUM <= 1
      ;
   END IF ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken
     FROM (
           SELECT d.accountNumber , d.partyId , d.messoToken
             FROM auraActivatedUsers d
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END auraActivatedUsers ;

procedure consumerdutypayholmob is
   l_pool VARCHAR2(29) := 'CONSUMERDUTYPAYHOLMOB' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table payment_holiday' ;
   FOR i IN 1..6
   LOOP
      execute immediate '
         INSERT /*+ append */ INTO payment_holiday ( agreement_ref , external_id , partyId )
         select agreement_ref , external_id , partyId
           from EXTN_PAYMENT_PLAN_TRACKING@cus0' || TO_CHAR(i) || ' epps
              , external_id_acct_map@adm eiam
              , ccsowner.bsbBillingAccount bba
              , ccsowner.bsbCustomerRole bcr
              , ccsowner.bsbPartyRole bpr
          where epps.account_no = eiam.account_no
            and eiam.external_id = bba.accountNumber
            and bba.portfolioId = bcr.portfolioId
            and bcr.partyroleid = bpr.id
            and eiam.external_id_type = 1' ;
      COMMIT ;
   END LOOP ;
   
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append*/ INTO consumerdutypayholmob ( accountNumber , partyId , id , combinedTelephoneNumber , messoToken )
   select /*+ parallel(bba 8) parallel(bpp 8) parallel(bsi 8) parallel(acus 8) parallel(subs 8) parallel(bcr 8) parallel(bpr 8) parallel(btur 8) parallel(bt 8)*/
     distinct acus.accountNumber , acus.partyId , bsi.id , bt.combinedTelephoneNumber , NULL AS messoToken from
      ccsowner.bsbBillingAccount bba,
      ccsowner.bsbPortfolioProduct bpp,
      ccsowner.bsbServiceInstance bsi,
      ccsowner.bsbsubscription subs,
      act_cust_uk_subs acus,
      ccsowner.bsbCustomerRole bcr,
      ccsowner.bsbPartyRole bpr,
      ccsowner.bsbTelephoneUsageRole btur,
      ccsowner.bsbTelephone bt
     where bba.portfolioId = bpp.portfolioId
      and bba.serviceInstanceId = bsi.parentServiceInstanceId
      and bsi.id = subs.serviceInstanceId
      and bpp.serviceInstanceId = bsi.id
      and bpr.partyId = acus.partyId
      and bba.portfolioId = bcr.portfolioId
      and bcr.partyroleid = bpr.id
      and btur.serviceInstanceId = bsi.id
      and btur.telephoneId = bt.id
      and bpp.status = 'AC'
      and bsi.serviceInstanceType in ( 610 , 620 )
      and acus.dtv = 'AC' --is not null
      and acus.talk = 'A' --is not null
      and acus.bband = 'AC' --is not null
      and acus.mobile = 'AC' 
      and btur.effectiveToDate is null   -- AF 22-Jun-2021 added in attempt to reduce multiple rows of same customer but different phonenumber.
   ;
   logger.write ( 'inserted ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   COMMIT ;
   
   -- remove non CCA accounts
   DELETE FROM consumerdutypayholmob m WHERE m.partyid not IN ( SELECT distinct cap.partyid FROM cca_active_plans cap) ;
   logger.write ( 'non cca deleted ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   
   -- remove payment holiday accounts
   DELETE FROM consumerdutypayholmob m WHERE m.partyid in (SELECT distinct partyid from payment_holiday ph);
   logger.write ( 'payment holiday deleted ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
     
   
   -- 30-Mar-2017 Andrew Fraser remove any customers in debt, request Nicolas Patte.
   DELETE FROM consumerdutypayholmob m WHERE m.accountNumber IN ( SELECT da.accountNumber FROM debt_amount da WHERE da.balance > 0 ) ;
   logger.write ( 'debt deleted ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;

   MERGE INTO consumerdutypayholmob t USING (
      SELECT c.partyId , c.messoToken , c.nsProfileId
        FROM customers c
   ) s ON ( s.partyId = t.partyId )
   WHEN MATCHED THEN UPDATE SET t.messoToken = s.messoToken , t.nsProfileId = s.nsProfileId
   ;
   
   DELETE FROM consumerdutypayholmob m WHERE m.messoToken IS NULL ;
   logger.write ( 'messotoken deleted ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.id , t.telephoneNumber
        , t.messoToken , t.nsProfileId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.id , s.combinedTelephoneNumber
        , s.messoToken , s.nsProfileId
     FROM (
   SELECT d.accountNumber , d.partyId , d.id , d.combinedTelephoneNumber , d.messoToken , d.nsProfileId
     FROM consumerdutypayholmob d
    WHERE ROWNUM <= 1000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT, i_burn => TRUE ) ;

   logger.write ( 'complete' ) ;
end;

PROCEDURE qHarmBurn IS
-- 27-June-2023 Created for Alex Benetatos
   l_pool VARCHAR2(29) := 'QHARMBURN' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.messotoken)
   select ROWNUM AS pool_seqno, l_pool AS pool_name, accountnumber, partyid, messotoken 
     from ( SELECT /*+ parallel(8) */ c.accountnumber, c.partyid, c.messotoken 
              from customers c
             where c.inflightorders = 0 
               and c.pool is null
               and c.skyqbox = 1
               and c.countrycode = 'GBR' -- added by AH 26/07/2023 PERFENG-895
               and (c.entertainment = 1 or c.SKYSIGNATURE = 1) 
               AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )
               and not exists (select /*+ parallel(8) */ 1
                                 FROM ccsowner.bsbbillingaccount ba, ccsowner.bsbserviceinstance bsi,
                                      ccsowner.bsbserviceinstance si, ccsowner.bsbsubscription sub
                                WHERE ba.serviceinstanceid = bsi.id
                                  AND bsi.id = si.parentserviceinstanceid
                                  AND sub.serviceinstanceid = si.id
                                  AND sub.status ='PC'
                                  and ba.accountnumber = c.accountnumber)
               ORDER BY dbms_random.value
               fetch first 50000 rows only) ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END qHarmBurn ;

PROCEDURE soipCcaNoDebt IS
   -- 27-JUL-2023 Stuart Mason -  Based on SOIPSIGNEDPLANSNOBURN
   -- SOIP customer with a signed CCA
   -- No debt on the account - including those in credit
   -- Burns the account once used

   l_pool VARCHAR2(29) := 'SOIPCCANODEBT' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.data , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.data , s.partyId , s.messoToken
     FROM (
   SELECT d.external_id AS accountNumber , d.agreement_ref AS data , d.partyId , d.messoToken
     FROM dataprov.soipSignedPlans d   -- populated in parent pool data_prep_06.soipSignedPlans
     where d.external_id IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <=0 )
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT, i_burn => TRUE ) ;
   --execute immediate 'truncate table soipSignedPlans' ;
   logger.write ( 'complete' ) ;
END soipCcaNoDebt ;

PROCEDURE validUrns IS
   l_pool VARCHAR2(29) := 'VALIDURNS' ;
   l_count NUMBER ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.urn , t.code )
    select ROWNUM AS pool_seqno , l_pool AS pool_name, urncode, status
      from (SELECT u.urncode, u.status
              from ccsowner.urns u, ccsowner.PARTNERURNDTLS p
             where p.id=u.urnbasisid
               and u.status='ISSUED'
               and p.expirydate between sysdate and sysdate+8
            ORDER BY dbms_random.value) 
    where rownum < 100000;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END validUrns ;

PROCEDURE eSimActive IS
   l_pool VARCHAR2(29) := 'ESIMACTIVE' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.portfolioid, t.messotoken , t.id )
   select ROWNUM, l_pool, accountNumber, partyId, portfolioid, messotoken, iccid
   from (      SELECT /*+ parallel(cu 8)   parallel(bba 8)  parallel(bpp 8) parallel(e 8) */
                     distinct cu.accountNumber, cu.partyid, bba.portfolioid, cu.messotoken, e.iccid
               FROM dataprov.customers cu, ccsowner.bsbbillingaccount bba, ccsowner.bsbportfolioproduct bpp,
                    ccsowner.bsbmobileesim e
               WHERE bba.accountnumber = cu.accountnumber
               AND bba.portfolioid = bpp.portfolioid
               AND bpp.catalogueproductid='15861'
               AND accountnumber2 is null
               AND e.portfolioproductid=bpp.id)
   where ROWNUM <= 20000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END eSimActive ;

PROCEDURE digitalCurrentBbNoDebt IS
   -- https://cbsjira.bskyb.com/browse/SOIPPOD-2498
   l_pool VARCHAR2(29) := 'DIGITALCURRENTBBNODEBT' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO digitalCurrentBbNoDebt t ( t.accountNumber , t.partyId , t.messotoken )
   SELECT c.accountNumber
        , c.partyId
        , c.messotoken
     FROM customers c
    WHERE c.bband = 1
      AND c.talk = 0
      AND c.mobile = 0
      AND c.dtv = 0
      AND c.inflightorders = 0
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND c.countryCode = 'GBR'
      AND c.pool IS NULL  -- optional
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 ) -- no outstanding balance
      AND ROWNUM <= 10000
   ;
   COMMIT ;

   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messotoken
     FROM (
   SELECT d.accountNumber
        , d.partyId
        , d.messotoken
     FROM digitalCurrentBbNoDebt d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 10000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => FALSE) ;
   logger.write ( 'complete' ) ;
END digitalCurrentBbNoDebt ;

PROCEDURE redeemedUrns IS
   l_pool VARCHAR2(29) := 'REDEEMEDURNS' ;
   l_count NUMBER ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.code )
    SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , u.urncode, u.status
    from ccsowner.urns u, ccsowner.PARTNERURNDTLS p
    where p.id=u.urnbasisid
    and u.status='REDEEMED'
    and p.expirydate > sysdate;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END redeemedUrns ;

END data_prep_10 ;
/
