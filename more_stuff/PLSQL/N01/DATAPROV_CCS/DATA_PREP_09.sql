CREATE OR REPLACE PACKAGE data_prep_09 AS
PROCEDURE mft_engineers ;
PROCEDURE mft_orders ;
PROCEDURE mobile_stock ;
PROCEDURE soipCustomersMissedPayment ;
PROCEDURE soipDebtHardFail ;
PROCEDURE marketplaceCatalogueProduct ;
PROCEDURE marketplaceCustomers ;
PROCEDURE soipActiveNetflixStandard ;
PROCEDURE soipActiveNetflixPremium ;
PROCEDURE soipOnlyActiveNetflixStandard ;
PROCEDURE soipOnlyActiveNetflixBasic ;
PROCEDURE skyQbasepack ;
PROCEDURE soipOnlyActiveSkyGlass ;
PROCEDURE bbSuperfastForUpgrade ;
PROCEDURE bbUltrafastForDowngrade ;
PROCEDURE digActCustPortfolioMobileBig ;
PROCEDURE digActCustPortfolioBig_noburn ;
PROCEDURE extCustPortfolio_big ;
PROCEDURE actCustNoDebtForSoip ;
PROCEDURE aura ;
PROCEDURE iqcDeviceAssignment ;
PROCEDURE skyData ;
PROCEDURE activeSoipRoi ;
PROCEDURE coreWithoutSkyCinema ;
PROCEDURE cinemaWithoutAmpJourneyStarts ;
END data_prep_09 ;
/


create or replace PACKAGE BODY data_prep_09 AS

PROCEDURE mft_engineers IS
   l_pool VARCHAR2(29) := 'MFT_ENGINEERS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO mft_engineers t ( id , adUserId , name , team )
   SELECT e.engineerId AS id
        , e.username AS adUserId
        , e.firstname || ' ' || e.lastname AS name
        , e.team
     FROM egs_owner.bsbengineer@egs e
    WHERE e.status = 'ENABLED'
      AND e.engineerType IS NOT NULL
      AND engineerId LIKE 'FS%'
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' rows inserted' ) ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.id , t.adUserId , t.name , t.team , t.message_id )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.id , s.adUserId , s.name , s.team , s.message_id
     FROM (
           SELECT d.id , d.adUserId , d.name , d.team , -1 AS message_id
             FROM mft_engineers d
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END mft_engineers ;

PROCEDURE mft_orders IS
   l_pool VARCHAR2(29) := 'MFT_ORDERS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO mft_orders t ( t.id , t.engineerId , t.partNo , t.orderedQty )
   SELECT co.order_no AS id
        , co.customer_no AS engineerId
        , col.part_no AS partNo
        , col.desired_qty AS orderedQty
     FROM ifsapp.customer_order@ifs co
     JOIN ifsapp.customer_order_line@ifs col ON co.order_no = col.order_no
    WHERE co.customer_no IN ( SELECT e.id FROM dataprov.mft_engineers e )
      AND co.state NOT IN ( 'Cancelled' , 'Delivered' , 'Invoiced/Closed' )
      AND co.wanted_delivery_date < TO_DATE ( '01-Jan-2099' , 'DD-Mon-YYYY' )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' rows inserted' ) ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.id , t.engineerId , t.partNo , t.orderedQty )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.id , s.engineerId , s.partNo , s.orderedQty
     FROM (
           SELECT d.id , d.engineerId , d.partNo , d.orderedQty
             FROM mft_orders d
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   -- 27-Aug-2021 Andrew Fraser for Callum Bulloch, mft_orders needs to burn cos after 250 iterations will trigger OMS constraint violations on chk_resordhistoryitems_number for oh.resourceorderhistoryitems.historyItemNumber <=250
   -- would be ok to cycle data if not for above issue - so could cycle at a low volume, allow 2 iterations per day maybe / rebuild pool mid-day.
   sequence_pkg.seqAfter ( i_pool => l_pool , i_burn => TRUE , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   -- execute immediate 'truncate table mft_engineers' ;  -- 29-Mar-2023 Chelsea Moss do NOT truncate dependent table so that jenkins job can repopulate pool on demand.
   logger.write ( 'complete' ) ;
END mft_orders ;

PROCEDURE mobile_stock IS
   l_pool VARCHAR2(29) := 'MOBILE_STOCK' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO mobile_stock t ( t.part_no )
   SELECT DISTINCT p.part_no
     FROM ifsapp.part_catalog@ifs p
     JOIN ifsapp.inventory_part@ifs ip ON ip.part_no = p.part_no
     JOIN skyfs.inventory_part_stock_threshold@ifs st ON st.part_no = p.part_no  -- fixes 'The inventory part exists but thresholds are not defined'
    WHERE ( p.part_no LIKE 'APP%' OR p.part_no LIKE 'SAM%' )  -- Apple iphones + Samsungs
      AND ip.contract IN ( 'SLC' , 'SLCIE' )
      AND ip.part_status != 'O'
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.id )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.id
     FROM (
           SELECT d.part_no AS id
             FROM mobile_stock d
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END mobile_stock ;

PROCEDURE soipCustomersMissedPayment IS
   l_pool VARCHAR2(29) := 'SOIPCUSTOMERSMISSEDPAYMENT' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipCustomersMissedPayment t ( t.accountNumber , t.partyId )
   SELECT DISTINCT s.accountNumber , s.partyId
     FROM hlght_owner.customerIndicator@slt s
    WHERE s.created > SYSTIMESTAMP - 365
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId
     FROM (
           SELECT d.accountNumber , d.partyId
             FROM soipCustomersMissedPayment d
            ORDER BY dbms_random.value
           ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipCustomersMissedPayment ;

PROCEDURE soipDebtHardFail IS
   l_pool VARCHAR2(29) := 'SOIPDEBTHARDFAIL' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipDebtHardFail t ( t.partyId )
   SELECT DISTINCT s.partyId
     FROM hlght_owner.customerIndicator@slt s
    WHERE s.indicatorName = 'FAILED_PAYMENT'
      AND s.created > SYSTIMESTAMP - 365
      AND ROWNUM <= 30000
   ;
   COMMIT ;
   MERGE INTO soipDebtHardFail t USING (
      SELECT c.partyId , c.messoToken
        FROM customers c
   ) s ON ( s.partyId = t.partyId )
   WHEN MATCHED THEN UPDATE SET t.messoToken = s.messoToken
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.messoToken
     FROM (
           SELECT d.messoToken
             FROM soipDebtHardFail d
            WHERE d.messoToken IS NOT NULL
            ORDER BY dbms_random.value
           ) s
    WHERE ROWNUM <= 10000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipDebtHardFail ;

PROCEDURE marketplaceCatalogueProduct IS
   l_pool VARCHAR2(29) := 'MARKETPLACECATALOGUEPRODUCT' ;
   l_count NUMBER ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO marketplaceCatalogueProduct t ( t.id , t.externalId , t.description )
   SELECT DISTINCT ci.id AS id  -- internal_product_id
        , ppv.catalogue_product_id AS externalId -- external_product_id
        , ci.product_classifier AS description
     FROM productCatalogue.catalogue_item@smpuk ci
     JOIN productCatalogue.ppv_item@smpuk ppv ON ppv.ppv_id = ci.ppv_id
   ;
   l_count := SQL%ROWCOUNT ;
   COMMIT ;
   IF l_count <= 2
   THEN
      logger.write ( 'Padding with extra rows because only inserted ' || TO_CHAR ( l_count ) || ' rows as workaround to avoid NODATA exception in url endpoint.' ) ;
      INSERT INTO marketplaceCatalogueProduct t ( t.id , t.externalId , t.description )
      SELECT s.id , s.externalId , s.description
        FROM marketplaceCatalogueProduct s
      ;
   END IF ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.id , t.externalId , t.description )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.id , s.externalId , s.description
     FROM (
           SELECT SUBSTR ( d.id , 1 , 47 ) AS id , d.externalId , d.description
             FROM marketplaceCatalogueProduct d
            ORDER BY dbms_random.value
           ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END marketplaceCatalogueProduct ;

PROCEDURE marketplaceCustomers IS
   l_pool VARCHAR2(29) := 'MARKETPLACECUSTOMERS' ;
   l_count NUMBER ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO marketplaceCustomers t ( t.partyId )
   SELECT c.partyId  -- list of customerIds that you can call MPS marketplace portfolio services with
     FROM portfolio.customer@smpuk c
    WHERE ROWNUM <= 300000
   ;
   l_count := SQL%ROWCOUNT ;
   COMMIT ;
   IF l_count <= 2
   THEN
      logger.write ( 'Padding with extra rows because only inserted ' || TO_CHAR ( l_count ) || ' rows as workaround to avoid NODATA exception in url endpoint.' ) ;
      INSERT INTO marketplaceCustomers t ( t.partyId )
      SELECT s.partyId
        FROM marketplaceCustomers s
      ;
   END IF ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.partyId
     FROM (
           SELECT d.partyId
             FROM marketplaceCustomers d
            ORDER BY dbms_random.value
           ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END marketplaceCustomers ;

PROCEDURE soipActiveNetflixStandard IS
   l_pool VARCHAR2(29) := 'SOIPACTIVENETFLIXSTANDARD' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipActiveNetflixStandard t ( t.accountNumber , t.partyId , t.messoToken )
   SELECT s.accountNumber , s.partyId , s.messoToken
     FROM soipActiveSubscription s
    WHERE s.netflixStan = 1
      AND ROWNUM <= 300000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken
     FROM (
           SELECT d.accountNumber , d.partyId , d.messoToken
             FROM soipActiveNetflixStandard d
            ORDER BY dbms_random.value
           ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipActiveNetflixStandard ;

PROCEDURE soipActiveNetflixPremium IS
   l_pool VARCHAR2(29) := 'SOIPACTIVENETFLIXPREMIUM' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipActiveNetflixPremium t ( t.accountNumber , t.partyId , t.messoToken )
   SELECT s.accountNumber , s.partyId , s.messoToken
     FROM soipActiveSubscription s
    WHERE s.netflixPrem = 1
      AND ROWNUM <= 300000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken
     FROM (
           SELECT d.accountNumber , d.partyId , d.messoToken
             FROM soipActiveNetflixPremium d
            ORDER BY dbms_random.value
           ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipActiveNetflixPremium ;

PROCEDURE soipOnlyActiveNetflixStandard IS
   l_pool VARCHAR2(29) := 'SOIPONLYACTIVENETFLIXSTANDARD' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipOnlyActiveNetflixStandard t ( t.accountNumber , t.partyId , t.messoToken )
   SELECT s.accountNumber , s.partyId , s.messoToken
     FROM soipActiveSubscription s
    WHERE s.netflixStan = 1
      AND s.customerHasAmp = 0
      AND NOT EXISTS (
          SELECT NULL
            FROM ccsowner.bsbBillingAccount ba
            JOIN ccsowner.bsbPortfolioProduct pp ON pp.portfolioId = ba.portfolioId
           WHERE ba.accountNumber = s.accountNumber
          )
      AND ROWNUM <= 300000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken
     FROM (
           SELECT d.accountNumber , d.partyId , d.messoToken
             FROM soipOnlyActiveNetflixStandard d
            ORDER BY dbms_random.value
           ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipOnlyActiveNetflixStandard ;

PROCEDURE soipOnlyActiveNetflixBasic IS
   l_pool VARCHAR2(29) := 'SOIPONLYACTIVENETFLIXBASIC' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipOnlyActiveNetflixBasic t ( t.accountNumber , t.partyId , t.messoToken )
   SELECT s.accountNumber , s.partyId , s.messoToken
     FROM soipActiveSubscription s
    WHERE s.netflixBasic = 1
      AND s.customerHasAmp = 0
      AND NOT EXISTS (
          SELECT NULL
            FROM ccsowner.bsbBillingAccount ba
            JOIN ccsowner.bsbPortfolioProduct pp ON pp.portfolioId = ba.portfolioId
           WHERE ba.accountNumber = s.accountNumber
          )
      AND ROWNUM <= 300000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken
     FROM (
           SELECT d.accountNumber , d.partyId , d.messoToken
             FROM soipOnlyActiveNetflixBasic d
            ORDER BY dbms_random.value
           ) s
     WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipOnlyActiveNetflixBasic ;

PROCEDURE skyQbasepack IS
   l_pool VARCHAR2(29) := 'SKYQBASEPACK' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO skyQbasepack t ( t.accountNumber , t.partyId , t.messoToken , t.x1accountId , t.skyCesa01token , t.ssoToken )
   SELECT /*+ parallel(16) */ c.accountNumber , c.partyId , c.messoToken , pte.x1accountId , c.skyCesa01token , c.ssoToken
     FROM customers c
     LEFT OUTER JOIN ccsowner.portfolioTechElement pte ON pte.portfolioId = c.portfolioId
    WHERE c.discoveryplus = 0
      and (   c.skySignature = 1 
           or c.ULTIMATETVADDON = 1) ;
--
--  commented out for PoC work Alex & Amit           
/*   SELECT c.accountNumber , c.partyId , c.messoToken , pte.x1accountId , c.skyCesa01token , c.ssoToken
     FROM customers c
     LEFT OUTER JOIN ccsowner.portfolioTechElement pte ON pte.portfolioId = c.portfolioId
    WHERE c.skySignature = 1
      --AND c.entertainment = 1  -- google search says "Sky Signature has replaced the Entertainment bundle..."
      --AND c.boxSets = 1
      --AND c.variety = 1
      --AND c.original = 1
      --AND pte.x1accountId IS NOT NULL  -- only get 10,721 hits if add this clause in N01
      AND ROWNUM <= 3000000
   ;
*/   
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken , t.x1accountId
        , t.skyCesa01token , t.ssoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken , s.x1accountId , s.skyCesa01token , s.ssoToken
     FROM (
           SELECT d.accountNumber , d.partyId , d.messoToken , d.x1accountId , d.skyCesa01token , d.ssoToken
             FROM skyQbasepack d
            ORDER BY dbms_random.value
           ) s
    WHERE ROWNUM <= 500000  -- 500k requested by Archana Burla 20-Jul-2022
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END skyQbasepack ;

PROCEDURE soipOnlyActiveSkyGlass IS
   l_pool VARCHAR2(29) := 'SOIPONLYACTIVESKYGLASS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipOnlyActiveSkyGlass t ( t.accountNumber , t.partyId , t.messoToken )
   SELECT s.accountNumber , s.partyId , s.messoToken
     FROM soipActiveSkyGlass s
     JOIN soipActiveSubscription sas ON sas.partyId = s.partyId
    WHERE sas.customerHasAmp = 0
      AND NOT EXISTS (
          SELECT NULL
            FROM ccsowner.bsbBillingAccount ba
            JOIN ccsowner.bsbPortfolioProduct pp ON pp.portfolioId = ba.portfolioId
           WHERE ba.accountNumber = s.accountNumber
          )
      AND sas.billed = 1  -- 05-Aug-2022 Andrew Fraser for Edwin Scariachin
      AND ROWNUM <= 900000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken
     FROM (
           SELECT d.accountNumber , d.partyId , d.messoToken
             FROM soipOnlyActiveSkyGlass d
            ORDER BY dbms_random.value
           ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   execute immediate 'truncate table soipActiveSkyGlass' ;
   logger.write ( 'complete' ) ;
END soipOnlyActiveSkyGlass ;

PROCEDURE bbSuperfastForUpgrade IS
   l_pool VARCHAR2(29) := 'BBSUPERFASTFORUPGRADE' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO bbSuperfastForUpgrade t ( t.accountNumber , t.partyId , t.messoToken )
   SELECT c.accountNumber , c.partyId , c.messoToken
     FROM customers c
    WHERE c.bbSuperfast = 1
      AND c.bbUltrafast = 0
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- no outstanding balance
      AND ROWNUM <= 300000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken
     FROM (
           SELECT d.accountNumber , d.partyId , d.messoToken
             FROM bbSuperfastForUpgrade d
            ORDER BY dbms_random.value
           ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END bbSuperfastForUpgrade ;

PROCEDURE bbUltrafastForDowngrade IS
   l_pool VARCHAR2(29) := 'BBULTRAFASTFORDOWNGRADE' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO bbUltrafastForDowngrade t ( t.accountNumber , t.partyId , t.messoToken )
   SELECT c.accountNumber , c.partyId , c.messoToken
     FROM customers c
    WHERE c.bbSuperfast = 1
      AND c.bbUltrafast = 0
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- no outstanding balance
      AND ROWNUM <= 300000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken
     FROM (
           SELECT d.accountNumber , d.partyId , d.messoToken
             FROM bbUltrafastForDowngrade d
            ORDER BY dbms_random.value
           ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END bbUltrafastForDowngrade ;

PROCEDURE digActCustPortfolioMobileBig IS
   l_pool VARCHAR2(29) := 'DIGACTCUSTPORTFOLIOMOBILEBIG' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO digActCustPortfolioMobileBig t ( t.accountnumber , t.partyid , t.portfolioId , t.messoToken , t.port_cnt )
   SELECT c.accountnumber , c.partyid , c.portfolioId , c.messoToken , acpb.port_cnt
     FROM actCustPortfolio_big acpb
     JOIN customers c ON c.portfolioId = acpb.portfolioId
    WHERE c.countryCode = 'GBR'
      AND c.mobile = 1
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )
      AND c.emailAddress NOT LIKE 'noemail%'
      and c.messotoken not like '%NO-NSPROFILE' -- ignore customers with no nsprofileid
      AND c.inFlightVisit = 0
      AND c.inFlightOrders = 0
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.portfolioId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.portfolioId , s.messoToken
    FROM (
          SELECT d.accountnumber , d.partyid , d.portfolioId , d.messoToken
            FROM digActCustPortfolioMobileBig d
           ORDER BY d.port_cnt DESC
           FETCH FIRST 150000 ROWS ONLY
         ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END digActCustPortfolioMobileBig ;

PROCEDURE digActCustPortfolioBig_noburn IS
   l_pool VARCHAR2(29) := 'DIGACTCUSTPORTFOLIOBIG_NOBURN' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO digActCustPortfolioBig_noburn t ( t.accountnumber , t.partyid , t.portfolioId , t.messoToken , t.port_cnt )
   SELECT c.accountnumber
        , c.partyid
        , c.portfolioId
        , c.messoToken
        , COUNT(*) AS port_cnt
     FROM ccsowner.bsbPortfolioProduct pp
     JOIN customers c ON c.portfolioId = pp.portfolioId
    WHERE c.countryCode = 'GBR'
      AND c.mobile = 0
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )
      AND c.emailAddress NOT LIKE 'noemail%'
      AND c.inFlightVisit = 0
      AND c.inFlightOrders = 0
    GROUP BY c.accountnumber , c.partyid , c.portfolioId , c.messoToken
   HAVING COUNT(*) >= 150
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.portfolioId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.portfolioId , s.messoToken
    FROM (
          SELECT d.accountnumber , d.partyid , d.portfolioId , d.messoToken
            FROM digActCustPortfolioBig_noburn d
           ORDER BY d.port_cnt DESC
           FETCH FIRST 100000 ROWS ONLY
         ) s
    ORDER BY dbms_random.value
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END digActCustPortfolioBig_noburn ;

PROCEDURE extCustPortfolio_big IS
-- 17-Oct-2022 Edwin Scariachin changed to burn data.
   l_pool VARCHAR2(29) := 'EXTCUSTPORTFOLIO_BIG' ;
BEGIN
   logger.write ( 'begin' ) ;
   MERGE INTO digActCustPortfolioBig_noburn t USING (
      SELECT DISTINCT ba.portfolioId
        FROM ccsowner.bsbBillingAccount ba
        JOIN ccsowner.bsbServiceInstance si ON ba.portfolioId = si.portfolioId
        JOIN rcrm.service serv ON si.id = serv.billingServiceInstanceId
       WHERE serv.serviceType = 'SOIP'
   ) s ON ( s.portfolioId = t.portfolioId )
   WHEN MATCHED THEN UPDATE SET t.soipCustomer = 1
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.portfolioId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.portfolioId , s.messoToken
    FROM (
          SELECT d.accountnumber , d.partyid , d.portfolioId , d.messoToken
            FROM digActCustPortfolioBig_noburn d
           WHERE d.soipCustomer IS NULL
           ORDER BY d.port_cnt DESC
           FETCH FIRST 100000 ROWS ONLY
         ) s
    ORDER BY dbms_random.value
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   execute immediate 'truncate table digActCustPortfolioBig_noburn' ;
   logger.write ( 'complete' ) ;
END extCustPortfolio_big ;

PROCEDURE actCustNoDebtForSoip IS
   l_pool VARCHAR2(29) := 'ACTCUSTNODEBTFORSOIP' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO actCustNoDebtForSoip t ( t.accountNumber , t.partyId )
   SELECT /*+ parallel(8) */ c.accountNumber , c.partyId
     FROM customers c
     --FROM ( SELECT * FROM customers WHERE ROWNUM <= 500000 ) c
    WHERE c.inFlightOrders = 0
      AND c.inFlightVisit = 0
      AND c.emailAddress NOT LIKE 'noemail%'  -- 08-Nov-2022 Alex Benetatos must have a primary email address.
      AND c.countryCode = 'GBR'  -- 09-Mar-2023 Alex Benetatos
      AND NOT EXISTS (  -- no outstanding balance, even if under a different account number.
          SELECT NULL
            FROM ccsowner.bsbBillingAccount ba
            JOIN debt_amount da ON da.accountNumber = ba.accountNumber
           WHERE c.portfolioId = ba.portfolioId
             AND da.balance > 0  -- = has an outstanding balance.
          )
      AND NOT EXISTS (  -- does not have soip or amp, even if under a different account number.
          SELECT NULL
            FROM ccsowner.bsbServiceInstance si
            JOIN rcrm.service serv ON si.id = serv.billingServiceInstanceId
           WHERE c.portfolioId = si.portfolioId
          )
      AND NOT EXISTS (  -- does not have a skeletal account, even if under a different account number.
          SELECT NULL
            FROM ccsowner.bsbBillingAccount ba
           WHERE c.portfolioId = ba.portfolioId
             AND ba.skeletalAccountFlag = 1
          )
      AND ROWNUM <= 300000
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId
     FROM (
           SELECT d.accountNumber , d.partyId
             FROM actCustNoDebtForSoip d
            ORDER BY dbms_random.value
           ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END actCustNoDebtForSoip ;

PROCEDURE aura IS
   -- 10-May-2023 Carla Lawrence added messoToken https://cbsjira.bskyb.com/browse/NFTREL-22204
   -- RFA (23/10/23) - PERFENG-1203 (Bhavani ) - No TV products should be part of the result
   l_pool VARCHAR2(29) := 'AURA' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO aura t ( t.accountNumber , t.messoToken )
   SELECT c.accountNumber , c.messoToken
     FROM customers c
    WHERE c.bband = 1
      AND c.fibre = 1
      AND c.bbSuperfast = 1
      AND c.bbUltrafast = 0
      AND c.SKYQBOX = 0 -- RFA (23/10/23) -- Bhavani Requires customers without a TV package
      AND c.SKYHDBOX =  0   -- RFA (23/10/23) -- Bhavani Requires customers without a TV package
      AND c.DTV = 0 -- RFA (23/10/23) -- Bhavani Requires customers without a TV package      
      AND c.countryCode = 'GBR'
      AND c.accountNumber2 IS NULL
      AND EXISTS (
          SELECT NULL
            FROM ccsowner.bsbPortfolioProduct pp
           WHERE pp.portfolioId = c.portfolioId
             AND pp.catalogueProductId = '15060'  -- "Sky Hub 4.2 Customer Owned (Delivered)"
             AND pp.status = 'DL'  -- Delivered
          )
      AND NOT EXISTS (
          SELECT NULL
            FROM ccsowner.bsbPortfolioProduct pp
           WHERE pp.portfolioId = c.portfolioId
             -- RFA (23/10/23) - Added further exceptinos to eliminate any residual TV products
             AND pp.catalogueProductId IN ( '15196' , '15334' , '15280', '11911','15598')  -- 15334="Sky Broadband Boost" , 15196="Sky Broadband Boost (legacy)" , 15280="SKY TV Essentials", 11911="SKY Essentials", 15598=SKY Basics
             AND pp.status != 'CN'  -- Cancelled. Most are AC=Active
          )
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- no outstanding balance 12-Dec-2022 Bhavani Ragunathan
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.messoToken
     FROM (
           SELECT d.accountNumber , d.messoToken
             FROM aura d
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END aura ;

PROCEDURE iqcDeviceAssignment IS
/*
17-Nov-2022 Alex Benetatos exclude if assignmentEndDate in the past
10-Nov-2022 Weird requirement here from Alex Benetatos -
  genuine x1AccountId's from rcrm.iqcDeviceAssignment
  are each paired with 280 randomly selected deviceIds taken from same table,
  but which are not necessarily associated with that particular x1AccountId in that table.
*/
   l_pool VARCHAR2(29) := 'IQCDEVICEASSIGNMENT' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO iqcDeviceAssignment t ( t.x1AccountId )
   SELECT a.x1AccountId
     FROM rcrm.iqcDeviceAssignment a
    WHERE ( a.assignmentEndDate < SYSTIMESTAMP +1 OR a.assignmentEndDate IS NULL )
    GROUP BY a.x1AccountId
    ORDER BY dbms_random.value
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   FOR r1 IN (
      SELECT d.ROWID AS d_rowid
        FROM iqcDeviceAssignment d
       WHERE ROWNUM <= 4000
   )
   LOOP
      -- Pull out 280 random deviceId's, in a listagg.
      -- The listagg is split into two parts to avoid "ORA-01489: result of string concatenation is too long"
      MERGE INTO iqcDeviceAssignment t USING (
         WITH q AS (
            SELECT ar.deviceId
                 , CASE WHEN MOD ( ROWNUM , 2 ) = 0 THEN 2 ELSE 1 END AS part
             FROM (
                   SELECT a.deviceId
                     FROM rcrm.iqcDeviceAssignment a
                    WHERE ( a.assignmentEndDate < SYSTIMESTAMP +1 OR a.assignmentEndDate IS NULL )
                    ORDER BY dbms_random.value
                  ) ar
            WHERE ROWNUM <= 280
         ) , q2 AS (
            SELECT COUNT(*) AS elements
                 , CASE WHEN q.part = 1 THEN LISTAGG ( q.deviceId , ',' ) WITHIN GROUP ( ORDER BY q.deviceId ) END AS p1_deviceIds
                 , CASE WHEN q.part = 2 THEN LISTAGG ( q.deviceId , ',' ) WITHIN GROUP ( ORDER BY q.deviceId ) END AS p2_deviceIds
              FROM q
             GROUP BY q.part
         )
         SELECT TO_CLOB ( MAX ( q2.p1_deviceIds ) ) || ',' || TO_CLOB ( MAX ( q2.p2_deviceIds ) ) AS deviceIds
           FROM q2
         HAVING SUM ( q2.elements ) = 280
      ) s ON ( t.ROWID = r1.d_rowid )
      WHEN MATCHED THEN UPDATE SET t.deviceIds = s.deviceIds
      ;
   END LOOP ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.x1AccountId , t.data_clob )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.x1AccountId , s.deviceIds AS data_clob
     FROM (
           SELECT d.x1AccountId , d.deviceIds
             FROM iqcDeviceAssignment d
            WHERE d.deviceIds IS NOT NULL
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   --execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END iqcDeviceAssignment ;

PROCEDURE skyData IS
   l_pool VARCHAR2(29) := 'SKYDATA' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO skyData t ( t.accountNumber , t.partyId )
   SELECT d.accountNumber , c.partyId
     FROM skyDataCsv d  -- data supplied in csv file 23-Nov-2022 from Stuart Kerr
     JOIN customers c ON d.accountNumber = c.accountNumber OR d.accountNumber = c.accountNumber2
    WHERE ROWNUM <= 500000
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId
     FROM (
           SELECT d.accountNumber , d.partyId
             FROM skyData d
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END skyData ;

PROCEDURE activeSoipRoi IS
   l_pool VARCHAR2(29) := 'ACTIVESOIPROI' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO activeSoipRoi t ( t.accountNumber , t.partyId , t.postcode )
   SELECT d.accountNumber , d.partyId , d.postcode
     FROM soipActiveSubscription d
    WHERE d.countryCode = 'IRL'
      AND d.serviceType = 'SOIP'
      AND ROWNUM <= 500000
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.postcode )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.postcode
     FROM (
           SELECT d.accountNumber , d.partyId , d.postcode
             FROM activeSoipRoi d
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END activeSoipRoi ;

PROCEDURE coreWithoutSkyCinema IS
   l_pool VARCHAR2(29) := 'COREWITHOUTSKYCINEMA' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'alter session enable parallel dml' ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO coreWithoutSkyCinema t ( t.accountNumber , t.partyId , t.portfolioId )
   SELECT c.accountnumber , c.partyId , c.portfolioId
     FROM customers c
    WHERE c.cinema = 0
      AND c.dtv = 1
      and c.skyqbox = 1
      and c.skyhdbox= 0
      AND c.accountNumber2 IS NULL
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 ) -- no outstanding balance
      AND ROWNUM <= 300000
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   -- 28-Feb-2023 Michael Santos PERFENG-574 hopefully only a temporary exclusion to be removed later in 2023.
   DELETE /*+ parallel(8) */ FROM coreWithoutSkyCinema t
    WHERE EXISTS (
          SELECT NULL
            FROM ccsowner.bsbPortfolioOffer po
           WHERE po.portfolioId = t.portfolioId
             AND po.offerId IN ( 50557 , 80889 , 91809 , 73850 , 50558 , 75112 , 73805 , 51628 )
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for portfolioOffer' ) ;
   COMMIT ;
   DELETE /*+ parallel(8) */ FROM coreWithoutSkyCinema t
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
   MERGE /*+ parallel(8) */ INTO coreWithoutSkyCinema t USING (
      SELECT DISTINCT pr.partyId
        FROM ccsowner.bsbPartyRole pr
        JOIN ccsowner.bsbCustomerRole cr ON pr.id = cr.partyRoleId
        JOIN ccsowner.bsbBillingAccount ba ON ba.portfolioId = cr.portfolioId
        JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
       WHERE s.serviceType IN ( 'AMP' , 'SOIP' )
   ) s ON ( t.partyId = s.partyId )
   WHEN MATCHED THEN UPDATE SET t.customerHasAmpOrSoip = 1
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged for customerHasAmpOrSoip' ) ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber
     FROM (
           SELECT d.accountNumber
             FROM coreWithoutSkyCinema d
            WHERE d.customerHasAmpOrSoip != 1 OR d.customerHasAmpOrSoip IS NULL
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;  -- 27-Jan-2023 Michael Santos changed to burn.
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END coreWithoutSkyCinema ;

PROCEDURE cinemaWithoutAmpJourneyStarts IS
   l_pool VARCHAR2(29) := 'CINEMAWITHOUTAMPJOURNEYSTARTS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'alter session set ddl_lock_timeout=60'; 
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
              AND d.burnPool = 0
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
      -- Added by SM for debugging purposes
declare
v_message varchar2(3950);
v_source varchar2(50) := 'ACTBBTALKASSURANCEROIFTTH';
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
            and object_name = 'CINEMAWITHOUTAMP_E2E'); 
logger.write( v_message);           

exception
when no_data_found then 
 logger.write(v_source||' No locks found.');
end;
---

   execute immediate 'truncate table cinemaWithoutAmp_e2e' ;
   logger.write ( 'complete' ) ;
END cinemaWithoutAmpJourneyStarts ;

END data_prep_09 ;
/