create or replace PACKAGE data_prep_08 AS
PROCEDURE ampCustomerStatus ;
PROCEDURE ampParamountActive ;
PROCEDURE soipbbtpending ;
PROCEDURE ifsOrderNumbers ;
PROCEDURE ifsPartsInStock ;
PROCEDURE delayedProvisioningAutocomp ;
PROCEDURE ifsCustomerReservations ;
PROCEDURE act_mobile_customers_no_debt2 ;
PROCEDURE digitalCurrentBbtNoDebt2 ;
PROCEDURE triplePlay_no_debt2 ;
PROCEDURE soipActiveSubNoNetflix2 ;
PROCEDURE dpsBillingAccountId2 ;
PROCEDURE soipBillView2 ;
PROCEDURE actcustportfolio_big ;
PROCEDURE actCustPortfolioMobile_big ;
PROCEDURE soipWithCinema ;
PROCEDURE existingRoiCustomers ;
PROCEDURE soipCustomers ;
PROCEDURE soipRoiDeliveredProducts ;
PROCEDURE actBbTalkAssuranceRoi ;
PROCEDURE actBbTalkAssuranceRoiFtth ;
PROCEDURE soipCoreAmpMobService ;
PROCEDURE activeGlass ;
PROCEDURE soipLessBbRegradeBurnableRoi ;
PROCEDURE soipLessBbRegradeNonBurnabRoi ;
END data_prep_08 ;
/


create or replace PACKAGE BODY data_prep_08 AS

PROCEDURE ampCustomerStatus IS
/* 29-Nov-2021 Andrew Fraser for Julian Correa. Five types of customer accounts in this pool:
1) Account with chargeback
2) Account with Disney+ cancelled
3) Account with failed payments
4) Account with Disney+ pending cancel (customer has requested cancellation but the still have days left to use on their subscription)
5) Account Renewal in Progress (customer is active but we haven¿t had confirmation yet that the renewal payment was successful)
*/
   -- 23-Nov-2021 Andrew Fraser for Deepa Satam NFTREL-21388
   -- 27/11/23 (RFA) - Use table CUSTOMERTOKENS to retrieve the MessoToken 
   l_pool VARCHAR2(29) := 'AMPCUSTOMERSTATUS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO ampCustomerStatus t ( t.accountNumber , t.partyId , t.messoToken
        , t.disneyCancelled , t.disneyPendingCancel , t.ampNotActive , t.paramountPlusActive )
   SELECT ba.accountNumber
        , ct.partyId
        , MAX(ct.messoToken) as messoToken
        --, MAX ( CASE WHEN p.status = 'ACTIVE' THEN 1 ELSE 0 END ) AS chargeback
        , MAX ( CASE WHEN p.suid = 'AMP_DISNEY' AND p.status = 'CEASED' THEN 1 ELSE 0 END ) AS disneyCancelled
        --, MAX ( CASE WHEN p.status = 'ACTIVE' THEN 1 ELSE 0 END ) AS failedPayments
        , MAX ( CASE WHEN p.suid = 'AMP_DISNEY' AND p.status = 'PENDING_CEASE' THEN 1 ELSE 0 END ) AS disneyPendingCancel
        --, MAX ( CASE WHEN p.status = 'ACTIVE' THEN 1 ELSE 0 END ) AS renewalInProgress
        , MAX ( CASE WHEN p.status != 'ACTIVE' THEN 1 ELSE 0 END ) AS ampNotActive
        , MAX ( CASE WHEN p.suid = 'AMP_PARAMOUNT_PLUS' AND p.status = 'ACTIVE' THEN 1 ELSE 0 END ) AS paramountPlusActive
     FROM ccsowner.bsbBillingAccount ba
     JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
     JOIN rcrm.product p ON s.id = p.serviceId
     LEFT JOIN dataprov.customertokens ct ON ba.accountNumber = ct.accountNumber
    WHERE s.serviceType = 'AMP'
    GROUP BY ba.accountNumber, ct.partyId
   ;
   logger.write ( 'ampCustomerStatus : '||to_char(sql%rowcount)||' rows inserted' ) ;
   COMMIT ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   MERGE INTO ampCustomerStatus t
   USING (
      SELECT DISTINCT eaa.partyId
        FROM mint_platform.ext_account_atr@ulm eaa
   ) s ON ( s.partyId = t.partyId )
   WHEN MATCHED THEN UPDATE SET t.ulm = 1
   ;
   UPDATE ampCustomerStatus t SET t.ulm = 0 WHERE t.ulm IS NULL ;
   logger.write ( 'updated to ulm=0 ' || TO_CHAR ( SQL%ROWCOUNT ) || ' rows' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId , t.messoToken , t.description )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId , s.messoToken , s.description
     FROM (
   SELECT d.accountnumber , d.partyId , d.messoToken
        , CASE WHEN d.disneyPendingCancel = 1 THEN 'Disney+ pending cancel'
               WHEN d.disneyCancelled = 1 THEN 'Disney+ cancelled'
               WHEN d.ampNotActive = 0 THEN 'ampActive'
               END AS description
     FROM ampCustomerStatus d
    WHERE ( d.disneyPendingCancel = 1 OR d.disneyCancelled = 1 OR d.ampNotActive = 0 )
      AND d.ulm = 0  -- is not in ulm database, this exclusion at request Julian Correa 01-Dec-2021
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END ampCustomerStatus ;

PROCEDURE ampParamountActive IS
   -- Carla Lawrence NFTREL-21851
   l_pool VARCHAR2(29) := 'AMPPARAMOUNTACTIVE' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId , s.messoToken
     FROM (
   SELECT d.accountnumber , d.partyId , d.messoToken
     FROM ampCustomerStatus d
    WHERE d.paramountPlusActive = 1
      AND ( d.disneyPendingCancel = 1 OR d.disneyCancelled = 1 OR d.ampNotActive = 0 )  -- copied from ampCustomerStatus, not sure if needed.
      AND d.ulm = 0  -- is not in ulm database, copied from ampCustomerStatus, not sure if needed.
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ampCustomerStatus' ;
   logger.write ( 'complete' ) ;
END ampParamountActive ;

PROCEDURE soipBbtPending IS
   -- Edward Falconer NFTREL-21436
   l_pool VARCHAR2(29) := 'SOIPBBTPENDING' ;
BEGIN
   logger.write ( 'begin' ) ;
   -- populate table in FPS aka FUL0*1N
   data_prep_fps.soipBbtPending@fps ;
   logger.write ( 'soipBbtPending@fps completed' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber, t.partyId, t.created, t.fulfilmentReferenceId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.created , s.fulfilmentReferenceId
     FROM (
           SELECT d.accountNumber , d.partyId , d.created , d.fulfilmentReferenceId
             FROM soipBbtPending@fps d
            ORDER BY dbms_random.value  -- before 03-Sep-2021 was instead "ORDER BY d.created"
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END soipBbtPending ;

PROCEDURE ifsOrderNumbers IS
   -- Alex Benetatos SOIPPOD-2654
   l_pool VARCHAR2(29) := 'IFSORDERNUMBERS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO ifsOrderNumbers t ( t.order_no )
   SELECT co.order_no AS id
        --, co.customer_po_no AS accountNumber
     FROM ifsapp.customer_order_tab@ifs co
    WHERE co.rowState NOT IN ( 'Cancelled' , 'Delivered' , 'Invoiced/Closed' )
      AND co.wanted_delivery_date BETWEEN SYSDATE - 120 AND SYSDATE + 120
      AND ROWNUM <= 200000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.customerOrderId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.customerOrderId
     FROM (
   SELECT d.order_no AS customerOrderId
     FROM ifsOrderNumbers d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END ifsOrderNumbers ;

PROCEDURE ifsPartsInStock IS
   -- Alex Benetatos SOIPPOD-2655
   l_pool VARCHAR2(29) := 'IFSPARTSINSTOCK' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO ifsPartsInStock t ( t.contract , t.part_no , t.location_no , t.serial_no )
   SELECT i.contract , i.part_no , i.location_no , i.serial_no
     FROM ifsapp.inventory_part_in_stock@ifs i
    WHERE i.contract IN ( 'S4002' , 'S4003' , 'S4005' , 'S4006' , 'S4012' , 'S4007' , 'S4001' , 'S4008' , 'S4009' , 'S4004' , 'S4010' , 'S4999' , 'S4011' )
      AND i.location_no IN ( 'S4002' , 'S4003' , 'S4005' , 'S4006' , 'S4012' , 'S4007' , 'S4001' , 'S4008' , 'S4009' , 'S4004' , 'S4010' , 'S4999' ,'S4011' )
      AND i.serial_no NOT LIKE '%*%'
      AND i.qty_onHand > 0
      AND ROWNUM <= 300000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.contract , t.partNumber , t.location , t.serialNumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.contract , s.partNumber , s.location , s.serialNumber
     FROM (
   SELECT d.contract , d.part_no AS partNumber , d.location_no AS location , d.serial_no AS serialNumber
     FROM ifsPartsInStock d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END ifsPartsInStock ;

PROCEDURE delayedProvisioningAutoComp IS
   -- Edward Falconer NFTREL-21503
   l_pool VARCHAR2(29) := 'DELAYEDPROVISIONINGAUTOCOMP' ;
   l_count NUMBER ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO delayedProvisioningAutoComp t ( t.accountNumber , t.partyId , t.created , t.technologyCode )
   SELECT bba.accountNumber
        , prn.partyId
        , MAX ( bs.created ) AS created
        , MAX ( bs.technologyCode ) AS technologyCode
     FROM ccsowner.bsbBillingAccount bba
     JOIN ccsowner.bsbPortfolioProduct bpp ON bba.portfolioid = bpp.portfolioid
     JOIN ccsowner.bsbServiceInstance bsi ON bpp.serviceinstanceid = bsi.id
     JOIN ccsowner.bsbCustomerProductElement bcp ON bpp.id = bcp.portfolioproductid
     JOIN ccsowner.bsbCustomerRole bcr ON bba.portfolioid = bcr.portfolioid
     JOIN ccsowner.bsbPartyRole bpr ON bcr.partyroleid = bpr.id
     JOIN ccsowner.bsbContactor bc ON bpr.partyid = bc.partyid
     JOIN ccsowner.bsbContactAddress bca ON bc.id = bca.contactorid
     JOIN ccsowner.bsbAddress ba ON bca.addressid = ba.id
     JOIN ccsowner.bsbSubscription bs ON bs.id = bpp.subscriptionid
     JOIN ccsowner.person prn ON prn.partyid = bpr.partyid
     LEFT OUTER JOIN ccsowner.bsbbroadbandcustprodelement bbcp ON bcp.id = bbcp.lineproductelementid
     LEFT OUTER JOIN ccsowner.bsbtelephonycustprodelement btcp ON bcp.id = btcp.telephonyproductelementid
     LEFT OUTER JOIN rcrm.service s ON bba.serviceInstanceId = s.billingServiceInstanceId
    WHERE s.serviceType IS NULL
      AND bpp.status = 'AC'
      AND bca.effectiveToDate IS NULL
      AND prn.familyName = 'DELAYED-PROVISIONING'
      AND bs.technologyCode = 'ORFTTP'
      AND ROWNUM <= 300000
    GROUP BY bba.accountNumber , prn.partyId
   ;
   l_count := SQL%ROWCOUNT ;
   logger.write ( TO_CHAR ( l_count ) || ' inserted' ) ;
   COMMIT ;
   IF l_count > 0
   THEN
      dbms_stats.gather_table_stats ( ownName => USER , tabName => l_pool ) ;
      DELETE FROM delayedProvisioningAutoComp t
       WHERE t.accountNumber IN (
             SELECT a.accountNumber
               FROM auc_owner.v_compensation_eligibility@auc a
              WHERE a.source = 'DP'
                AND a.omsOrderId NOT LIKE 'oh%'
                AND a.status_code IN ( 'CREDITED' , 'ELIGIBLE' , 'INELIGIBLE' )
             )
      ;
      l_count := l_count - SQL%ROWCOUNT ;
      logger.write ( TO_CHAR ( l_count ) || ' remaining after delete' ) ;
      IF l_count > 0
      THEN
         dbms_stats.gather_table_stats ( ownName => USER , tabName => l_pool ) ;
         -- View used to workaround "ORA-22804: remote operations not permitted on object tables or user-defined type columns"
         MERGE INTO delayedProvisioningAutoComp t USING (
            SELECT v.accountNumber
                 , v.partyId
                 , v.fpsOrderId
                 , v.providerOrderId
              FROM dataprov.dp_delayedProvisioningAutoComp@fps v
         ) s ON ( s.accountNumber = t.accountNumber AND s.partyId = t.partyId )
         WHEN MATCHED THEN UPDATE SET t.fpsOrderId = s.fpsOrderId , t.providerOrderId = s.providerOrderId
         ;
         logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged' ) ;
      END IF ;
   END IF ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.created , t.technologyCode , t.fpsOrderId , t.providerOrderId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.created , s.technologyCode , s.fpsOrderId , s.providerOrderId
     FROM (
           SELECT d.accountNumber , d.created , d.technologyCode , d.fpsOrderId , d.providerOrderId
             FROM delayedProvisioningAutoComp d
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END delayedProvisioningAutoComp ;

PROCEDURE ifsCustomerReservations IS
   -- Alex Benetatos SOIPPOD-2656
   l_pool VARCHAR2(29) := 'IFSCUSTOMERRESERVATIONS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO ifsCustomerReservations t ( t.quotation_no , t.cust_ref )
   SELECT q.quotation_no , q.cust_ref
     FROM ifsapp.order_quotation@ifs q
    WHERE q.cust_ref IS NOT NULL
      AND q.wanted_delivery_date BETWEEN SYSDATE - 90 AND SYSDATE + 90
      AND ROWNUM <= 300000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.quotation_no , t.cust_ref )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.quotation_no , s.cust_ref
     FROM (
   SELECT d.quotation_no , d.cust_ref
     FROM ifsCustomerReservations d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END ifsCustomerReservations ;

PROCEDURE act_mobile_customers_no_debt2 IS
   -- 09-Feb-2022 Alex Benetatos SOIPPOD-2672, duplicate pool for customers created after environment refresh.
   l_pool VARCHAR2(29) := 'ACT_MOBILE_CUSTOMERS_NO_DEBT2' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.id )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.id
     FROM (
   SELECT d.accountNumber , d.partyId , d.serviceInstanceId AS id
     FROM act_mobile_customers_no_debt d
    WHERE d.partyId IN (
          SELECT p.partyId
            FROM ccsowner.person p
           WHERE p.created >= (
                 SELECT MAX ( TO_DATE ( r.parameter_value , 'DD-MON-YYYY' ) )
                   FROM dp_test_script_params r
                  WHERE r.parameter_name = 'ENV_REFRESH'
                 )
          )
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table act_mobile_customers_no_debt' ;
   logger.write ( 'complete' ) ;
END act_mobile_customers_no_debt2 ;

PROCEDURE digitalCurrentBbtNoDebt2 IS
   -- 09-Feb-2022 Alex Benetatos SOIPPOD-2672, duplicate pool for customers created after environment refresh.
   l_pool VARCHAR2(29) := 'DIGITALCURRENTBBTNODEBT2' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'alter session set ddl_lock_timeout=60'; 
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.data , t.skycesa01token , t.messotoken
      , t.ssotoken , t.firstName , t.familyName , t.emailAddress
      )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.data , s.skycesa01token , s.messotoken
      , s.ssotoken , s.firstName , s.familyName , s.emailAddress
     FROM (
   SELECT d.accountNumber
        , d.partyId
        , d.username AS data
        , d.skycesa01token
        , d.messotoken
        , d.ssotoken
        , d.firstName
        , d.familyName
        , d.emailAddress
     FROM digitalCurrentBbtNoDebt d
    WHERE d.partyId IN (
          SELECT p.partyId
            FROM ccsowner.person p
           WHERE p.created >= (
                 SELECT MAX ( TO_DATE ( r.parameter_value , 'DD-MON-YYYY' ) )
                   FROM dp_test_script_params r
                  WHERE r.parameter_name = 'ENV_REFRESH'
                 )
          )
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 300000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
      -- Added by SM for debugging purposes
declare
v_message varchar2(3950);
v_source varchar2(50) := 'DIGITALCURRENTBBTNODEBT2';
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
            and object_name = 'DIGITALCURRENTBBTNODEBT'); 
logger.write( v_message);           

exception
when no_data_found then 
 logger.write(v_source||' No locks found.');
end;
---
   execute immediate 'truncate table digitalCurrentBbtNoDebt' ;
   logger.write ( 'complete' ) ;
END digitalCurrentBbtNoDebt2 ;

PROCEDURE triplePlay_no_debt2 IS
   l_pool VARCHAR2(29) := 'TRIPLEPLAY_NO_DEBT2' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.messoToken , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.messoToken , s.partyId
     FROM (
   SELECT d.accountNumber
        , NVL ( c.messoToken , 'NO TOKEN' ) AS messoToken
        , d.partyId
     FROM triplePlay_no_debt d
     LEFT OUTER JOIN customers c on c.accountNumber = d.accountNumber
    WHERE d.partyId IN (
          SELECT p.partyId
            FROM ccsowner.person p
           WHERE p.created >= (
                 SELECT MAX ( TO_DATE ( r.parameter_value , 'DD-MON-YYYY' ) )
                   FROM dp_test_script_params r
                  WHERE r.parameter_name = 'ENV_REFRESH'
                 )
          )
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   execute immediate 'truncate table triplePlay_no_debt' ;
   logger.write ( 'complete' ) ;
END triplePlay_no_debt2 ;

PROCEDURE soipActiveSubNoNetflix2 IS
   -- 09-Feb-2022 Alex Benetatos SOIPPOD-2672, duplicate pool for customers created after environment refresh.
   l_pool VARCHAR2(29) := 'SOIPACTIVESUBNONETFLIX2' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId , s.messoToken
     FROM (
   SELECT d.accountnumber , d.partyId , d.messoToken
     FROM soipActiveSubscription d
    WHERE d.netflixStanPrem = 0
      AND d.partyId IN (
          SELECT p.partyId
            FROM ccsowner.person p
           WHERE p.created >= (
                 SELECT MAX ( TO_DATE ( r.parameter_value , 'DD-MON-YYYY' ) )
                   FROM dp_test_script_params r
                  WHERE r.parameter_name = 'ENV_REFRESH'
                 )
          )
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END soipActiveSubNoNetflix2 ;

PROCEDURE dpsBillingAccountId2 IS
   -- 09-Feb-2022 Alex Benetatos SOIPPOD-2672, duplicate pool for customers created after environment refresh.
   l_pool VARCHAR2(29) := 'DPSBILLINGACCOUNTID2' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.billingAccountId , t.accountNumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.billingAccountId , s.accountNumber
     FROM (
   SELECT TO_CHAR ( d.billingAccountId ) AS billingAccountId
        , d.accountNumber
     FROM dpsBillingAccountId d
     WHERE d.accountNumber IN (
          SELECT ba.accountNumber
            FROM ccsowner.bsbBillingAccount ba
            JOIN ccsowner.bsbCustomerRole cr ON ba.portfolioId = cr.portfolioId
            JOIN ccsowner.bsbPartyRole pr ON pr.id = cr.partyRoleId
            JOIN ccsowner.person p ON p.partyId = pr.partyId
           WHERE p.created >= (
                 SELECT MAX ( TO_DATE ( r.parameter_value , 'DD-MON-YYYY' ) )
                   FROM dp_test_script_params r
                  WHERE r.parameter_name = 'ENV_REFRESH'
                 )
          )
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   --SOIPPOD-2672--execute immediate 'truncate table ' || l_pool ;
   execute immediate 'truncate table dpsBillingAccountId' ;
   logger.write ( 'complete' ) ;
END dpsBillingAccountId2 ;

PROCEDURE soipBillView2 IS
   -- 09-Feb-2022 Alex Benetatos SOIPPOD-2672, duplicate pool for customers created after environment refresh.
   l_pool VARCHAR2(29) := 'SOIPBILLVIEW2' ;
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
      AND d.partyId IN (
          SELECT p.partyId
            FROM ccsowner.person p
           WHERE p.created >= (
                 SELECT MAX ( TO_DATE ( r.parameter_value , 'DD-MON-YYYY' ) )
                   FROM dp_test_script_params r
                  WHERE r.parameter_name = 'ENV_REFRESH'
                 )
          )
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END soipBillView2 ;

PROCEDURE actCustPortfolio_big IS
   l_pool VARCHAR2(29) := 'ACTCUSTPORTFOLIO_BIG' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   execute immediate 'alter session enable parallel dml' ;
   INSERT /*+ append */ INTO actCustPortfolio_big t ( t.portfolioId , t.port_cnt )
   SELECT /*+ parallel(8) */ s.portfolioId , COUNT(*) AS port_cnt
     FROM ccsowner.bsbPortfolioProduct s
   HAVING COUNT(*) > 175
    GROUP BY s.portfolioId
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.portfolioId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.portfolioId , s.messoToken
    FROM (
          SELECT cus.accountnumber , cus.partyid , cus.portfolioId , cus.messotoken
            FROM actCustPortfolio_big acpb
            JOIN customers cus ON cus.portfolioId = acpb.portfolioId
           WHERE cus.countryCode = 'GBR'
             AND cus.mobile = 0  -- 21-Jun-2022 Liam Fleming NFTREL-21925
             AND cus.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- no outstanding Julian 25/08/2023
           ORDER BY acpb.port_cnt DESC
           FETCH FIRST 250000 ROWS ONLY
         ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END actCustPortfolio_big ;

PROCEDURE actCustPortfolioMobile_big IS
   l_pool VARCHAR2(29) := 'ACTCUSTPORTFOLIOMOBILE_BIG' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.portfolioId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.portfolioId , s.messoToken
    FROM (
          SELECT cus.accountnumber , cus.partyid , cus.portfolioId , cus.messotoken , acpb.port_cnt
            FROM actCustPortfolio_big acpb
            JOIN customers cus ON cus.portfolioId = acpb.portfolioId
           WHERE cus.countryCode = 'GBR'
             AND cus.mobile = 1
           ORDER BY acpb.port_cnt DESC
           FETCH FIRST 50000 ROWS ONLY
         ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END actCustPortfolioMobile_big ;

PROCEDURE soipWithCinema IS
   l_pool VARCHAR2(29) := 'SOIPWITHCINEMA' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId , s.messoToken
     FROM (
   SELECT d.accountnumber , d.partyId , d.messoToken
     FROM soipActiveSubscription d
    WHERE d.cinema = 1
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END soipWithCinema ;

PROCEDURE existingRoiCustomers IS
   -- 23-Aug-2022 Andrew Fraser for Michael Santos, three extra restriction clauses. SOIPPOD-2715.
   -- 28-Jun-2022 Andrew Fraser for Alex Benetatos restrict to customers with mobile phones. SOIPPOD-2703.
   l_pool VARCHAR2(29) := 'EXISTINGROICUSTOMERS' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO existingRoiCustomers t ( t.accountnumber , t.partyId , t.portfolioId , t.skyGlass , t.inFlight , t.eirCode )
   SELECT /*+ parallel(8) */ DISTINCT c.accountnumber
        , c.partyId
        , c.portfolioId
        , 0 AS skyGlass
        , 0 AS inFlight
        , ba.eirCode
     FROM customers c
     JOIN ccsowner.bsbContactor bc ON c.partyId = bc.partyId
     JOIN ccsowner.bsbContactAddress bca ON bc.id = bca.contactorId
     JOIN ccsowner.bsbAddress ba ON bca.addressId = ba.id
     JOIN ccsowner.bsbContactTelephone bct ON bct.contactorId = bc.id
     JOIN ccsowner.bsbTelephone bt ON bt.id = bct.telephoneId
     JOIN ccsowner.bsbContactEmail bce ON bce.contactorId = bc.id
     JOIN ccsowner.bsbEmail be ON be.id = bce.emailId
    WHERE ba.countryCode = 'IRL'
      AND bca.deletedFlag = 0
      AND SYSDATE BETWEEN bca.effectiveFromDate AND NVL ( bca.effectiveToDate , SYSDATE + 1 )
      AND bca.primaryFlag = 1  -- AF: not sure if we want this condtion or not
      AND bct.deletedFlag = 0
      AND SYSDATE BETWEEN bct.effectiveFromDate AND NVL ( bct.effectiveToDate , SYSDATE + 1 )
      AND bct.primaryFlag = 1  -- AF: not sure if we want this condtion or not
      AND bt.telephoneNumberStatus = 'VALID'
      AND bct.typeCode = 'M'
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- no outstanding balance M Santos
      AND bce.deletedFlag = 0
      AND SYSDATE BETWEEN bce.effectiveFromDate AND NVL ( bce.effectiveToDate , SYSDATE + 1 )
      AND be.emailAddressStatus = 'VALID'
      AND bce.primaryFlag = 1
    ORDER BY dbms_random.value
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   MERGE INTO existingRoiCustomers t USING (
      SELECT DISTINCT si.portfolioId
        FROM rcrm.product p
        JOIN rcrm.service s ON p.serviceId = s.id
        JOIN ccsowner.bsbServiceInstance si ON s.billingServiceInstanceId = si.id
       WHERE p.suid IN ( 'LLAMA_SMALL' , 'LLAMA_MEDIUM' , 'LLAMA_LARGE' , 'SKY_GLASS_SMALL_ROI' , 'SKY_GLASS_MEDIUM_ROI' , 'SKY_GLASS_LARGE_ROI' )
         AND p.status != 'CEASED'
   ) s ON ( t.portfolioId = s.portfolioId )
   WHEN MATCHED THEN UPDATE SET t.skyGlass = 1
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged for skyGlass' ) ;
   -- Below to avoid sps app error "PIMM rule ID RULE_DO_NOT_ALLOW_SOIP_SALE_ON_INFLIGHT_ORDERS has resulted in a Do Not Allow outcome." Michael Santos
   MERGE INTO existingRoiCustomers t USING (
      SELECT DISTINCT pp.portfolioId
        FROM ccsowner.bsbPortfolioProduct pp
       WHERE pp.status = 'PC'  -- Pending Cancel
   ) s ON ( t.portfolioId = s.portfolioId )
   WHEN MATCHED THEN UPDATE SET t.inFlight = 1
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged for inFlight' ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId
     FROM (
           SELECT d.accountnumber , d.partyId
             FROM existingRoiCustomers d
            WHERE d.skyGlass = 0
              AND d.inFlight = 0
              AND d.eirCode IS NOT NULL
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END existingRoiCustomers ;

PROCEDURE soipCustomers IS
   l_pool VARCHAR2(29) := 'SOIPCUSTOMERS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipCustomers t ( t.partyId , t.accountNumber )
   SELECT bpr.partyId
        , MIN ( ba.accountNumber ) AS accountNumber
     FROM ccsowner.bsbBillingAccount ba
     JOIN ccsowner.bsbCustomerRole bcr ON ba.portfolioId = bcr.portfolioId
     JOIN ccsowner.bsbPartyRole bpr ON bcr.partyroleId = bpr.id
     JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
     JOIN rcrm.product p ON s.id = p.serviceId
    WHERE s.serviceType = 'SOIP'
    GROUP BY bpr.partyId
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId
     FROM (
   SELECT d.accountNumber , d.partyId
     FROM soipCustomers d
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   --execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipCustomers ;

PROCEDURE soipRoiDeliveredProducts IS
   l_pool VARCHAR2(29) := 'SOIPROIDELIVEREDPRODUCTS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipRoiDeliveredProducts t ( t.accountNumber )
   SELECT DISTINCT ba.accountNumber
     FROM ccsowner.bsbBillingAccount ba
     JOIN rcrm.service se ON ba.serviceInstanceId = se.billingServiceInstanceId
     JOIN rcrm.product pr ON pr.serviceId = se.id
    WHERE pr.eventCode = 'DELIVERED'
      AND pr.status = 'DELIVERED'
      AND pr.suid IN ( 'LLAMA_LARGE' , 'LLAMA_MEDIUM' , 'LLAMA_SMALL' , 'SKY_GLASS_LARGE_ROI' , 'SKY_GLASS_MEDIUM_ROI' , 'SKY_GLASS_SMALL_ROI' )
      AND ba.currencyCode = 'EURO'
   ;
   COMMIT ;
   MERGE INTO soipRoiDeliveredProducts t USING (
      SELECT DISTINCT ba.accountNumber
        FROM ccsowner.bsbBillingAccount ba
        JOIN rcrm.service se ON ba.serviceInstanceId = se.billingServiceInstanceId
        JOIN rcrm.product pr ON pr.serviceId = se.id
        JOIN rcrm.productSubscriptionLink psl ON pr.id = psl.productId
        JOIN rcrm.subscription sub ON psl.subscriptionId = sub.id
       WHERE sub.status IN ( 'AC' , 'ACTIVE' )
   ) s ON ( s.accountNumber = t.accountNumber )
   WHEN MATCHED THEN UPDATE SET sub_status = 'ACTIVE' WHERE sub_status != 'ACTIVE' OR sub_status IS NULL
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber
     FROM (
           SELECT d.accountNumber
             FROM soipRoiDeliveredProducts d
            WHERE d.sub_status = 'ACTIVE'
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipRoiDeliveredProducts ;

PROCEDURE actBbTalkAssuranceRoi IS
   l_pool VARCHAR2(29) := 'ACTBBTALKASSURANCEROI' ;
   -- column "talk" = the serviceInstanceId of talk
   -- column "bband" = the serviceInstanceId of bband
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO actBbTalkAssuranceRoi t ( t.accountNumber , t.portfolioId , t.talk , t.bband , t.partyId )
   SELECT accountNumber , portfolioId , talk , bband , partyId
     FROM (
           SELECT /*+ full(si)   parallel(si 16)   pq_distribute ( si hash hash )
                      full(subs) parallel(subs 16) pq_distribute ( subs hash hash )
                      full(c)    parallel(c 16)    pq_distribute ( c hash hash )
                  */
                  c.accountNumber
                , subs.subscriptionTypeId
                , si.id
                , si.portfolioId
                , c.partyId
             FROM ccsowner.bsbServiceInstance si
             JOIN ccsowner.bsbSubscription subs ON subs.serviceInstanceId = si.id
             JOIN customers c ON c.portfolioId = si.portfolioId
            WHERE subs.status IN ( 'A' , 'AC' )
              AND c.accountNumber2 IS NULL  -- 11-Dec-2017 Andrew Fraser request Stuart Kerr: exclude customers with multiple billing accounts such as mobile customers.
              AND ( subs.statusChangedDate , subs.subscriptionTypeId ) IN (
                   SELECT MAX ( subs.statusChangedDate )
                        , subs.subscriptionTypeId
                     FROM ccsowner.bsbServiceInstance si210
                     JOIN ccsowner.bsbSubscription subs2 ON subs2.serviceInstanceId = si210.id
                    WHERE si210.parentServiceInstanceId = si.parentServiceInstanceId  -- join
                      AND subs2.subscriptionTypeId IN ( '3' , '7' )
                    GROUP BY subs2.subscriptionTypeId
                   )
              AND c.countryCode = 'IRL'  -- future enhancement: deal with more accurately as done in existingRoiCustomers in this same package.
          ) PIVOT ( MAX ( id ) FOR ( subscriptionTypeId ) IN ( '3' AS talk , '7' AS bband ) )
    WHERE talk IS NOT NULL
      AND bband IS NOT NULL
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   -- 21-Sep-2018 NFTREL-15224 exclude customers who already have a visit booked.
   -- 04-Feb-2022 Andrew Fraser for Dimitrios Koulialis, made stricter NFTREL-21394 (tpoc only 04-Feb-2022)
   DELETE FROM actBbTalkAssuranceRoi t
    WHERE t.portfolioid IN (
          SELECT bsi.portfolioId
            FROM ccsowner.bsbVisitRequirement bvr
            JOIN ccsowner.bsbAddressUsageRole baur ON bvr.installAtionAddressRoleId = baur.id
            JOIN ccsowner.bsbServiceInstance bsi ON baur.serviceInstanceId = bsi.id
           WHERE bvr.statusCode NOT IN ( 'CP' , 'CN' )
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted' ) ;
   -- 04-Feb-2022 Andrew Fraser for Dimitrios Koulialis, remove customers with an open outage NFTREL-21394. Fixes FAVsessionCreate test script errors.
   DELETE FROM actBbTalkAssuranceRoi t
    WHERE t.portfolioId IN (
          SELECT /*+
                  full(bpp)  parallel(bpp 8)  pq_distribute ( bpp hash hash )
                  full(bcpe) parallel(bcpe 8) pq_distribute ( bcpe hash hash )
                  full(bpe)  parallel(bpe 8)  pq_distribute ( bpe hash hash )
                  full(o)    parallel(o 8)    pq_distribute ( o hash hash )
                 */ 
                 bpp.portfolioId
            FROM ccsowner.bsbPortfolioProduct bpp
            JOIN ccsowner.bsbCustomerProductElement bcpe ON bcpe.portfolioProductId = bpp.id
            JOIN ccsowner.bsbBroadbandCustProdElement bpe ON bpe.lineProductElementId = bcpe.id
            JOIN dataprov.snsOpenOutages o ON o.serviceId = bpe.serviceNumber  -- populated by data_prep_07.replaceHubOutOfWarranty
           WHERE bcpe.status = 'AC'
             AND bpp.status = 'AC'
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.bb_serviceid , t.talk_serviceid )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.bband , s.talk
     FROM (
           SELECT d.accountNumber , d.partyId , d.bband , d.talk
             FROM actBbTalkAssuranceRoi d
            WHERE ROWNUM <= 100000
            ORDER BY dbms_random.value
           ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END actBbTalkAssuranceRoi ;

PROCEDURE actBbTalkAssuranceRoiFtth IS
   l_pool VARCHAR2(29) := 'ACTBBTALKASSURANCEROIFTTH' ;
   -- column "talk" = the serviceInstanceId of talk
   -- column "bband" = the serviceInstanceId of bband
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'alter session set ddl_lock_timeout=60'; 
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO actBbTalkAssuranceRoiFtth t ( t.accountNumber , t.portfolioId , t.talk , t.bband , t.partyId )
   SELECT DISTINCT d.accountNumber , d.portfolioId , d.talk , d.bband , d.partyId
     FROM dataprov.actBbTalkAssuranceRoi d
     JOIN ccsowner.bsbPortfolioProduct pp ON pp.portfolioId = d.portfolioId
    WHERE pp.catalogueProductId = '14680'  -- "FTTH Appointed Activation Fee"
      AND pp.status = 'CP'
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   -- 08/11/23 (RFA) - adding a hint to the select statement
   MERGE INTO actBbTalkAssuranceRoiFtth t USING (
      SELECT /*+ PARALLEL(10) */ pp.portfolioId
           , MAX ( tcpe.serviceId ) AS talk_serviceId
           , MAX ( bcpe.serviceNumber ) AS bband_serviceId
        FROM ccsowner.bsbPortfolioProduct pp
        JOIN ccsowner.bsbServiceInstance si ON pp.serviceInstanceId = si.id
        JOIN ccsowner.bsbCustomerProductElement cpe ON pp.id = cpe.portfolioProductId
        LEFT OUTER JOIN ccsowner.bsbBroadbandCustProdElement bcpe ON cpe.id = bcpe.lineProductElementId
        LEFT OUTER JOIN ccsowner.bsbTelephonyCustProdElement tcpe ON cpe.id = tcpe.telephonyProductElementId
       WHERE ( tcpe.serviceId IS NOT NULL OR bcpe.serviceNumber IS NOT NULL )
       GROUP BY pp.portfolioId
   ) s ON ( s.portfolioId = t.portfolioId )
   WHEN MATCHED THEN UPDATE SET t.talk_serviceId = s.talk_serviceId , t.bband_serviceId = s.bband_serviceId
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.bb_serviceid , t.talk_serviceid )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.bb_serviceId , s.talk_serviceId
     FROM (
           SELECT d.accountNumber , d.partyId , d.bband_serviceId AS bb_serviceId , d.talk_serviceId
             FROM actBbTalkAssuranceRoiFtth d
            WHERE d.bband_serviceId IS NOT NULL
              AND d.talk_serviceId IS NOT NULL
              AND ROWNUM <= 100000
            ORDER BY dbms_random.value
           ) s
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
            and object_name = 'ACTBBTALKASSURANCEROIFTTH'); 
logger.write( v_message);           

exception
when no_data_found then 
 logger.write(v_source||' No locks found.');
end;
---
   
   execute immediate 'truncate table ' || l_pool ;
   execute immediate 'truncate table actBbTalkAssuranceRoi' ;
   logger.write ( 'complete' ) ;
END actBbTalkAssuranceRoiFtth ;

PROCEDURE soipCoreAmpMobService IS
   l_pool VARCHAR2(29) := 'SOIPCOREAMPMOBSERVICE' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipCoreAmpMobService t ( t.partyId , t.messoToken
        , t.accountNumber , t.accountNumber2 , t.accountNumberAmp , t.accountNumberGlass )
   SELECT c.partyId
        , c.messoToken
        , c.accountNumber
        , c.accountNumber2
        , MAX ( baa.accountNumber ) AS accountNumberAmp
        , MAX ( bag.accountNumber ) AS accountNumberGlass
     FROM customers c
     JOIN ccsowner.bsbBillingAccount baa ON baa.portfolioId = c.portfolioId
     JOIN rcrm.service sa ON sa.billingServiceInstanceId = baa.serviceInstanceId
     JOIN ccsowner.bsbBillingAccount bag ON bag.portfolioId = c.portfolioId
     JOIN rcrm.service sg ON sg.billingServiceInstanceId = bag.serviceInstanceId
     JOIN rcrm.product pg ON pg.serviceId = sg.id
    WHERE c.mobile = 1
      AND c.accountNumber2 IS NOT NULL  -- two accounts means likely has a core account in addition to mobile.
      AND sa.serviceType = 'AMP'
      AND pg.suid IN ( 'LLAMA_SMALL' , 'LLAMA_MEDIUM' , 'LLAMA_LARGE' , 'SKY_GLASS_SMALL_ROI' , 'SKY_GLASS_MEDIUM_ROI' , 'SKY_GLASS_LARGE_ROI' )
    GROUP BY c.partyId
        , c.messoToken
        , c.accountNumber
        , c.accountNumber2
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken
     FROM (
           SELECT d.accountNumber , d.partyId , d.messoToken
             FROM (
                    SELECT d1.accountNumber AS accountNumber , d1.partyId , d1.messoToken FROM soipCoreAmpMobService d1 UNION ALL
                    SELECT d1.accountNumber2 AS accountNumber , d1.partyId , d1.messoToken FROM soipCoreAmpMobService d1 UNION ALL
                    SELECT d1.accountNumberAmp AS accountNumber , d1.partyId , d1.messoToken FROM soipCoreAmpMobService d1 UNION ALL
                    SELECT d1.accountNumberGlass AS accountNumber , d1.partyId , d1.messoToken FROM soipCoreAmpMobService d1
                  ) d
            WHERE ROWNUM <= 100000
            ORDER BY dbms_random.value
           ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipCoreAmpMobService ;

PROCEDURE activeGlass IS
   l_pool VARCHAR2(29) := 'ACTIVEGLASS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO activeGlass t ( t.x1AccountId , t.accountNumber , t.partyId , t.messoToken )
   SELECT DISTINCT d.x1AccountId , d.accountNumber , d.partyId , d.messoToken
     FROM dataprov.soipActiveSubscription d
     JOIN ccsowner.bsbBillingAccount ba ON ba.accountNumber = d.accountNumber
     JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
     JOIN rcrm.product p ON s.id = p.serviceId
     JOIN ccsowner.bsbCustomerRole bcr ON ba.portfolioId = bcr.portfolioId
     JOIN ccsowner.bsbPartyRole bpr ON bcr.partyroleId = bpr.id
     JOIN ccsowner.person per ON per.partyId = bpr.partyId
    WHERE d.serviceType = 'SOIP'
      AND p.suid IN ( 'LLAMA_SMALL' , 'LLAMA_MEDIUM' , 'LLAMA_LARGE' , 'MULTISCREEN_PUCK' )  -- Llama = Sky Glass , Puck = Sky Stream
      -- AND p.status = 'DELIVERED'
      -- AND p.eventCode = 'DELIVERED'
      AND ba.currencyCode = 'GBP'  -- Archana, exclude RoI customers.
      AND NVL ( per.blockPurchaseSwitch , 0 ) != 1  -- Amit, exclude debt block. 1 = customer is in debt block.
      AND NVL ( bcr.debtIndicator , 0 ) != 1  -- Archana, exclude customer in debt. 1 = customer is in debt arrears.
      AND d.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- Archana exclude customers with debt aka balance due on their account.
      AND ROWNUM <= 300000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.x1AccountId , t.accountNumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.x1AccountId , s.accountNumber , s.partyId , s.messoToken
     FROM (
           SELECT d.x1AccountId , d.accountNumber , d.partyId , d.messoToken
             FROM activeGlass d
            WHERE ROWNUM <= 100000
            ORDER BY dbms_random.value
           ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END activeGlass ;

PROCEDURE soipLessBbRegradeBurnableRoi IS
   l_pool VARCHAR2(29) := 'SOIPLESSBBREGRADEBURNABLEROI' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table bbRegradeRoi' ;
   INSERT /*+ append */ INTO bbRegradeRoi t ( t.accountNumber , t.partyId , t.data , t.skycesa01token , t.messotoken , t.ssotoken
      , t.firstName , t.familyName , t.emailAddress )
   SELECT c.accountNumber
        , c.partyId
        , c.username
        , c.skycesa01token
        , c.messotoken
        , c.ssotoken
        , c.firstName
        , c.familyName
        , c.emailAddress
     FROM dataprov.customers c
     JOIN dataprov.bbRegrade_portfolioId_tmp pp ON c.portfolioId = pp.portfolioId  -- must have line rental
    WHERE c.bband = 1
      AND c.fibre = 0
      AND c.bbsuperfast = 0  --based Ross Benton request added
      AND c.bbultrafast = 0  --based Ross Benton request added
      AND c.talk = 1
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND c.countryCode = 'IRL'
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- no outstanding balance 24/09 Shane Venter
      -- eliminate those with a valid power of attorney
      AND c.portfolioid NOT IN (
          SELECT po.portfolioid
            FROM ccsowner.bsbPowerOfAttorneyRole po
           WHERE effectiveToDate IS NULL OR effectiveToDate > SYSDATE
          )
      AND c.emailAddress NOT LIKE 'noemail%'  -- 13-Dec-2021 Andrew Fraser for Edwin Scariachan: valid email needed for soip.
      AND c.partyId IN (  -- 14-Dec-2021 Andrew Fraser for Edwin Scariachan: mobile number needed for soip.
          SELECT bc.partyId
            FROM ccsowner.bsbContactor bc
            LEFT OUTER JOIN ccsowner.bsbContactTelephone bct ON bct.contactorId = bc.id
            LEFT OUTER JOIN ccsowner.bsbTelephone bt ON bt.id = bct.telephoneId
           WHERE bct.deletedFlag = 0
             AND SYSDATE BETWEEN NVL ( bct.effectiveFromDate , SYSDATE - 1 ) AND NVL ( bct.effectiveToDate , SYSDATE + 1 )
             AND bt.telephoneNumberStatus = 'VALID'
             AND bct.typeCode = 'M'
             AND bct.primaryFlag = 1  -- AF: not sure if we want this condtion or not. Added 11-Jul-2022 for Ross Benton as tpoc.
          )
      AND ROWNUM <= 20000
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => 'bbRegradeRoi' ) ;
   UPDATE bbRegradeRoi t SET t.soip = 0 ;
   MERGE INTO bbRegradeRoi t USING (
      SELECT DISTINCT bpr.partyId
        FROM ccsowner.bsbBillingAccount ba
        JOIN ccsowner.bsbCustomerRole bcr ON bcr.portfolioId = ba.portfolioId
        JOIN ccsowner.bsbPartyRole bpr ON bpr.id = bcr.partyRoleId
        JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
       WHERE s.serviceType = 'SOIP'
   ) s ON ( s.partyId = t.partyId )
   WHEN MATCHED THEN UPDATE SET t.soip = 1
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged for soip' ) ;
   FOR i IN 1..6
   LOOP
      execute immediate '
      MERGE INTO bbRegradeRoi t
      USING (
         SELECT eiam.external_id AS accountNumber
              , SUBSTR ( MAX ( espp.iban ) , -6 ) AS code
           FROM external_id_acct_map@adm eiam
           JOIN sky.extn_sepa_payment_profile@cus0' || TO_CHAR ( i ) || ' espp ON espp.account_no = eiam.account_no
          WHERE eiam.external_id_type = 1
          GROUP BY eiam.external_id
      ) s ON ( s.accountNumber = t.accountNumber )
      WHEN MATCHED THEN UPDATE SET t.code = s.code
      ' ;
      logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged for code for cus0' || TO_CHAR ( i ) ) ;
      COMMIT ;
   END LOOP ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.data , t.skycesa01token , t.messotoken
        , t.ssotoken , t.firstName , t.familyName , t.emailAddress , t.code
          )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.data , s.skycesa01token , s.messotoken
        , s.ssotoken , s.firstName , s.familyName , s.emailAddress , s.code
     FROM (
   SELECT d.accountNumber , d.partyId , d.data , d.skycesa01token , d.messotoken
        , d.ssotoken , d.firstName , d.familyName , d.emailAddress , d.code
     FROM bbRegradeRoi d
    WHERE d.soip = 0
      AND d.code IS NOT NULL
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END soipLessBbRegradeBurnableRoi ;

PROCEDURE soipLessBbRegradeNonBurnabRoi IS
   l_pool VARCHAR2(29) := 'SOIPLESSBBREGRADENONBURNABROI' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'alter session set ddl_lock_timeout=60'; 
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.data , t.skycesa01token , t.messotoken
        , t.ssotoken , t.firstName , t.familyName , t.emailAddress , t.code
          )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.data , s.skycesa01token , s.messotoken
        , s.ssotoken , s.firstName , s.familyName , s.emailAddress , s.code
     FROM (
   SELECT d.accountNumber , d.partyId , d.data , d.skycesa01token , d.messotoken
        , d.ssotoken , d.firstName , d.familyName , d.emailAddress , d.code
     FROM bbRegradeRoi d
    WHERE d.soip = 0
      AND d.code IS NOT NULL
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   -- Added by SM for debugging purposes
declare
v_message varchar2(3950);
v_source varchar2(50) := 'SOIPLESSBBREGRADENONBURNABROI';
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
            and object_name = 'BBREGRADEROI'); 
logger.write( v_message);           

exception
when no_data_found then 
 logger.write(v_source||' No locks found.');
end;
---   
   execute immediate 'truncate table bbRegradeRoi' ;
   logger.write ( 'complete' ) ;
END soipLessBbRegradeNonBurnabRoi ;

END data_prep_08 ;
/
