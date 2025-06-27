CREATE OR REPLACE PACKAGE data_prep_04 AS
PROCEDURE loyalty_cust_no_mobile ;
PROCEDURE mobile_orders ;
PROCEDURE mob_restricted_cust ;
PROCEDURE nsProfileIds ;
PROCEDURE orderManagementOrders ;
PROCEDURE partyIdsForCase ;
PROCEDURE partyIdsForPairCard ;
PROCEDURE pcsCardDetails ;
PROCEDURE port_out_cust ;
PROCEDURE properties ;
PROCEDURE reopen_reassign_case ;
PROCEDURE soipDigiExistLanding ;
PROCEDURE soipPreActiveSignature ;
PROCEDURE mobile_cdr_files ;
PROCEDURE populate_orders ;
PROCEDURE accountsWithRecentCases ;
PROCEDURE userIdForCase ;
PROCEDURE nvn_cease ;
PROCEDURE minimum_term ;
PROCEDURE act_cust_no_bb_st_vis ;
PROCEDURE act_cust_inst_prod ;
PROCEDURE act_cust_sky_plus ;
PROCEDURE act_cust_act_visit ;
PROCEDURE cancelled ;
PROCEDURE act_uk_cust_idnv_triple ;
END data_prep_04 ;
/


CREATE OR REPLACE PACKAGE BODY data_prep_04 AS

PROCEDURE loyalty_cust_no_mobile IS
   l_pool VARCHAR2(29) := 'LOYALTY_CUST_NO_MOBILE' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT /*+ append */ INTO loyalty_cust_no_mobile ( customerPartyId , accountNumber )
   SELECT /*+ parallel(ctc 8) parallel(bsb 8) parallel(bcr 8) parallel(bpr 8) parallel(p 8) */
          ctc.customerPartyId
        , bsb.accountNumber
     FROM ccsowner.bsbcustomertenurecache ctc
        , ccsowner.bsbBillingAccount bsb
        , ccsowner.bsbCustomerRole bcr
        , ccsowner.bsbpartyrole bpr
        , ccsowner.person p
    WHERE p.partyid = ctc.customerPartyid
      and bsb.portfolioId = bcr.portfolioId
      and bcr.partyroleid = bpr.id
      and ctc.customerPartyid = bpr.partyid
      AND p.activeInLoyaltyProgram = 1
      AND p.loyaltyProgramStateChangeDate >= TO_DATE ( '01-Jan-2017' , 'DD-Mon-YYYY' )
      and tenureStartDate <= ADD_MONTHS ( SYSDATE , -36 )
      and ctc.customerPartyid not in ( select ni.partyid from act_mobile_numbers ni )
   ;
   COMMIT ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.ssoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.ssoToken
     FROM (
   SELECT d.accountNumber , d.customerPartyId AS partyId , c.ssoToken
     FROM loyalty_cust_no_mobile d
     LEFT OUTER JOIN customers c ON c.partyId = d.customerPartyId
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 50000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END loyalty_cust_no_mobile ;

PROCEDURE mobile_orders IS
   l_pool VARCHAR2(29) := 'MOBILE_ORDERS' ;
BEGIN
   logger.write ( 'begin' ) ;
   data_prep_static_oms.mobile_orders@oms ;
   logger.write ( 'oms remote procedure completed' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO mobile_orders ( id , accountNumber , instanceId )
   SELECT DISTINCT
          oms.id
        , oms.accountNumber
        , oms.instanceId
     FROM mobile_orders@oms oms
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   -- 12/07/2016 Andrew Fraser added partyId request Karthik BVM. Update took 10secs on 12/07/2016.
   UPDATE dataprov.mobile_orders mo
      SET mo.partyId = (
          SELECT pr.partyid
            FROM ccsowner.bsbPartyRole pr
            JOIN ccsowner.bsbCustomerRole cr ON cr.partyRoleId = pr.id
            JOIN ccsowner.bsbBillingAccount ba ON ba.portfolioId = cr.portfolioId
           WHERE ba.accountNumber = mo.accountNumber
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' updated for partyId' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.id , t.serviceNumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.id , s.serviceNumber
     FROM (
           SELECT d.accountNumber , d.partyId , d.id , d.instanceId AS serviceNumber
             FROM mobile_orders d
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;  -- 18-Jan-2023 Dimitrios changed to cycle (was burn)
   logger.write ( 'complete' ) ;
END mobile_orders ;

PROCEDURE mob_restricted_cust IS
   l_pool VARCHAR2(29) := 'MOB_RESTRICTED_CUST' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO mob_restricted_cust
   SELECT /*+ parallel(bba 8) full (bba)  parallel(bpp 8) full (bpp) parallel(bcr 8) full (bcr) parallel(bpr 8) full (bpr)
            parallel(bsi 8) full (bsi) parallel(bcpe 8) full (bcpe) pq_distribute(bpr hash hash) pq_distribute(bcpe hash hash)
            pq_distribute(bpp hash hash) pq_distribute(bcr hash hash) pq_distribute(bba hash hash) */
     DISTINCT bba.accountNumber , bpr.partyid
     FROM ccsowner.bsbBillingAccount bba
        , ccsowner.bsbPortfolioProduct bpp
        , ccsowner.bsbserviceinstance bsi
        , ccsowner.bsbCustomerRole bcr
        , ccsowner.bsbpartyrole bpr
        , ccsowner.bsbcustomerproductelement bcpe
    where bba.portfolioId = bpp.portfolioId
      and bpp.serviceinstanceid=bsi.id
      and bpp.id=bcpe.portfolioproductid
      and bba.portfolioId=bcr.portfolioId
      and bcr.partyroleid = bpr.id
      and bba.serviceinstanceid=bsi.parentserviceinstanceid
      and bsi.serviceinstancetype = 620
      and bba.createdby != 'sky-mobile-sales'
      and bcpe.status = 'R'
      and bcpe.statusreasoncode = 'LOST'
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId
     FROM (
   SELECT d.accountNumber , d.partyId
     FROM mob_restricted_cust d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END mob_restricted_cust ;

PROCEDURE nsProfileIds IS
   l_pool VARCHAR2(29) := 'NSPROFILEIDS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO nsProfileIds ( profileIds )
   SELECT s.identityId
     FROM ccsowner.bsbPartyToIdentity s
    WHERE ROWNUM <= 100000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.profileId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.profileId
     FROM (
   SELECT d.profileIds AS profileId
     FROM nsProfileIds d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END nsProfileIds ;

PROCEDURE orderManagementOrders IS
   l_pool VARCHAR2(29) := 'ORDERMANAGEMENTORDERS' ;
BEGIN
   logger.write ( 'begin' ) ;
   data_prep_static_oms.orderManagementOrders@oms ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT INTO orderManagementOrders t ( t.accountNumber , t.partyId , t.billingAccountId , t.messotoken )
   SELECT oms.accountNumber , c.partyId , c.billingAccountId , c.messotoken
     FROM orderManagementOrders@oms oms
     JOIN customers c ON oms.accountNumber = c.accountNumber
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken , t.billingAccountId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken , s.billingAccountId
     FROM (
   SELECT d.accountNumber , d.partyId , d.messoToken , d.billingAccountId
     FROM orderManagementOrders d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END orderManagementOrders ;

PROCEDURE partyIdsForCase IS
   l_pool VARCHAR2(29) := 'PARTYIDSFORCASE' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   insert /*+ append */ into partyIdsForCase ( accountNumber , partyId , caseId , lastName , postcode , caseNumber )
   select * from (
      select max ( decode ( ctx.type , 'ACCOUNTNUMBER' , ctx.externalid ) ) AS accountNumber
           , max ( decode ( ctx.type , 'PARTY' , ctx.externalid ) ) AS partyId
           , ctx.caseId
           , con.lastName
           , con.postcode
           , cas.caseNumber
        from casemanagement.bsbcmcontext@cse ctx
           , casemanagement.bsbcmcase@cse cas
           , casemanagement.bsbcmcontact@cse con
       where ctx.caseId = cas.id
         and con.caseId = cas.id
         and cas.is_open_status = '1'
         and con.postcode != '{cPostcode}'
       group by ctx.caseId , con.lastName , con.postcode , cas.caseNumber
      ) d
    where d.accountNumber in ( select i.accountNumber from act_uk_cust i )
      and rownum < 200001
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.surname , t.postcode , t.caseNumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.surname , s.postcode , s.caseNumber
     FROM (
   SELECT d.accountNumber , d.partyId , d.lastName AS surname , d.postcode , d.caseNumber
     FROM partyIdsForCase d
    WHERE d.accountNumber IN (
          SELECT i.accountNumber
            FROM partyIdsForCase i
           GROUP BY i.accountNumber
          HAVING COUNT(*) <= 70
          )
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END partyIdsForCase ;

PROCEDURE partyIdsForPairCard IS
   l_pool VARCHAR2(29) := 'PARTYIDSFORPAIRCARD' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   insert /*+ append */ into partyIdsForPairCard ( partyId )
   select s.partyId
     from (
         SELECT /*+ parallel(c 16) parallel(s 16) parallel(p 16) parallel(cpe 16)
                    full(c) full(s) full(p) full(cpe) pq_distribute(cpe hash hash)
                    pq_distribute(s hash hash) pq_distribute(c hash hash) */
                c.partyid
           FROM act_uk_cust c                 ,
                ccsowner.bsbPortfolioProduct p         ,
                ccsowner.bsbserviceinstance s          ,
                ccsowner.bsbcustomerproductelement cpe
          WHERE p.portfolioId = c.portfolioId
            AND p.id = cpe.portfolioproductid
            AND cpe.status = 'A'
            AND cpe.customerproductelementtype = 'VC'
            AND p.serviceinstanceid = s.id
            AND s.serviceinstancetype IN ( 210 , 220 )
            AND p.catalogueProductId = '10137'
            AND cpe.status NOT IN ( 'RJ' , 'PC' , 'D' , 'DL' )
            AND s.lastupdate < SYSDATE - 30
            AND NOT EXISTS (
                SELECT /*+ full(bpp2) parallel(bpp2 ,8) pq_distribute(bpp2 hash hash) */
                       bpp2.portfolioId
                  FROM ccsowner.bsbPortfolioProduct bpp2
                 WHERE bpp2.portfolioId = p.portfolioId
                   AND status = 'AS'
                 )
            AND c.partyid in ( select i.partyid from act_uk_cust i group by i.partyid having count(*) = 1 )
         GROUP BY c.partyid
         HAVING COUNT(*) > 1
           ) s
     where rownum < 100000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.partyId
     FROM (
   SELECT d.partyId
     FROM partyIdsForPairCard d
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 20000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END partyIdsForPairCard ;

PROCEDURE pcsCardDetails IS
   /*
   If this pool runs low on data, can fake the data ok with below update in PCS database:
   UPDATE pcs.bsbTempCardDetails tcd SET tcd.expiryDate = SYSDATE + 90
    WHERE tcd.securityCode IS NOT NULL
      AND tcd.xid IS NULL
      AND tcd.mdStatus IS NULL
      AND tcd.cavv IS NULL
      AND tcd.expiryDate BETWEEN SYSDATE - 400 AND SYSDATE - 10
      AND tcd.cardnumber IN ( '5555555555554444' , '5454545454545454' , '4111111111111111' )
      AND tcd.nameOnCard IS NOT NULL
      AND ROWNUM <= 400001
   ;
   */
   l_pool VARCHAR2(29) := 'PCSCARDDETAILS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO pcsCardDetails t
   SELECT tcd.id
     FROM pcs.bsbTempCardDetails@pcs tcd
    WHERE tcd.securityCode IS NOT NULL
      AND tcd.xid IS NULL
      AND tcd.mdStatus IS NULL
      AND tcd.cavv IS NULL
      AND tcd.expiryDate > SYSDATE
      AND tcd.cardnumber IN ( '5555555555554444' , '5454545454545454' , '4111111111111111' )
      AND tcd.nameOnCard IS NOT NULL
      AND ROWNUM < 300001
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.id )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.id
     FROM (
   SELECT d.id
     FROM pcsCardDetails d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END pcsCardDetails ;

PROCEDURE port_out_cust IS
   l_pool VARCHAR2(29) := 'PORT_OUT_CUST' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO port_out_cust ( accountNumber , partyId, id, serviceid )
   SELECT /*+ parallel(bba, 8) parallel(bpp, 16) parallel(bsi, 16) parallel(acus, 16) index(ms, ak_mobfeat_servid) */ DISTINCT
          bba.accountNumber
        , acus.partyId
        , bsi.id
        , ms.serviceid
     FROM ccsowner.bsbBillingAccount bba
        , ccsowner.bsbPortfolioProduct bpp
        , ccsowner.bsbserviceinstance bsi
        , ccsowner.bsbSubscription subs
        , act_cust_uk_subs acus
        , ccsowner.mobileservicefeaturesettings ms
    where bba.portfolioId = bpp.portfolioId
      and bpp.serviceinstanceid=bsi.id
      and bsi.id = subs.serviceinstanceid
      and bsi.id = ms.serviceinstanceid
      and bba.accountNumber=acus.accountNumber
      and bpp.status = 'AC'
      and bsi.serviceinstancetype in ( 610 , 620 )
      and acus.dtv is not null
      and acus.talk is not null
      and acus.bband is not null
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.id , t.serviceNumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.id , s.serviceNumber
     FROM (
   SELECT d.accountNumber , d.partyId , d.id , d.serviceId AS serviceNumber
     FROM port_out_cust d
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 50000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END port_out_cust ;

PROCEDURE properties IS
   l_pool VARCHAR2(29) := 'PROPERTIES' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO properties
   SELECT id
     FROM csc.bsbPropertyDetails@csc
    WHERE ROWNUM <= 40000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.id )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.id
     FROM (
   SELECT d.id
     FROM properties d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END properties ;

PROCEDURE reopen_reassign_case IS
   l_pool VARCHAR2(29) := 'REOPEN_REASSIGN_CASE' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO reopen_reassign_case ( caseNumber , assignedTo , queueId , version , newQueueId , newAssignedTo )
   SELECT s.caseNumber , s.assignedTo , s.queueId , s.version , s.newQueueId , s.newAssignedTo
     FROM (
         SELECT ca.caseNumber
              , ca.assignedTo
              , ca.queueId
              , ca.version
              , public_queue@cse AS newQueueId
              , 'd' || LPAD ( ( MOD ( ROWNUM , 5000 ) +1 ) , 4 , '0' ) AS newAssignedTo
           FROM caseManagement.bsbcmCase@cse ca
          WHERE ca.closedOn > sysdate - 30
            AND ca.assignedTo != 'unassigned'
            AND ca.assignedTo LIKE 'd____'
            AND LENGTH ( TRIM ( TRANSLATE ( ca.assignedTo , 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ' , ' ' ) ) ) = 4
            AND ca.queueId = 'ASSIGNED_AND_CLOSED'
          ) s
    WHERE ROWNUM < 200001
   ;
   COMMIT ;
   DELETE FROM reopen_reassign_case t WHERE t.assignedTo = t.newAssignedTo ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.casenumber , t.version , t.data , t.id , t.userId , t.surname )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.casenumber , s.version , s.data , s.id , s.userId , s.surname
     FROM (
   SELECT d.casenumber , d.version , d.newQueueId AS data , d.queueId AS id , d.assignedTo AS userId , d.newAssignedTo AS surname
     FROM reopen_reassign_case d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END reopen_reassign_case ;

PROCEDURE soipDigiExistLanding IS
   l_pool VARCHAR2(29) := 'SOIPDIGIEXISTLANDING' ;
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
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END soipDigiExistLanding ;

PROCEDURE soipPreActiveSignature IS
   l_pool VARCHAR2(29) := 'SOIPPREACTIVESIGNATURE' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipPreActiveSignature t ( t.accountNumber , t.partyId , t.messoToken )
   SELECT DISTINCT ba.accountNumber , c.partyId , c.messoToken
     FROM rcrm.subscription sub
     JOIN rcrm.productSubscriptionLink psl ON psl.subscriptionId = sub.id
     JOIN rcrm.product prod ON prod.id = psl.productId
     JOIN rcrm.service serv ON serv.id = prod.serviceId
     JOIN ccsowner.bsbBillingAccount ba ON ba.serviceInstanceId = serv.billingServiceInstanceId
     JOIN customers c ON c.portfolioId = ba.portfolioId
    WHERE sub.status IN ( 'PA' , 'PREACTIVE' )
      AND c.skySignature = 1
   ;
   COMMIT ;
   MERGE INTO soipPreActiveSignature t USING (
      SELECT bc.partyId
           , MAX ( be.emailAddress ) AS emailAddress
        FROM ccsowner.bsbContactor bc
        JOIN ccsowner.bsbContactEmail bce ON bce.contactorId = bc.id
        JOIN ccsowner.bsbEmail be ON be.id = bce.emailId
       WHERE bce.deletedFlag = 0
         AND SYSDATE BETWEEN bce.effectiveFromDate AND NVL ( bce.effectiveToDate , SYSDATE + 1 )
       GROUP BY bc.partyId
   ) s ON ( t.partyId = s.partyId )
   WHEN MATCHED THEN UPDATE SET t.emailAddress = s.emailAddress WHERE NVL ( t.emailAddress , 'x' ) != NVL ( s.emailAddress , 'x' )
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messotoken , t.emailAddress )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messotoken , s.emailAddress
     FROM (
   SELECT d.accountNumber , d.partyId , d.messotoken , d.emailAddress
     FROM soipPreActiveSignature d
    ORDER BY dbms_random.value
   ) s
     WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipPreActiveSignature ;

PROCEDURE mobile_cdr_files IS
/*
|| Populates table mobile_cdr_files only.
|| Data is read from that table by stand-alone procedure dp_mobile_cdr_data.
|| That procedure is called by shell script:
||    kfxbatchn01:/apps/N01/home/bilbtn01/nft_data/generate_cdr_data.sh
|| which gets run occasionally by John Barclay to generate BIP Billing input flat files for Kenan batch processing.
*/
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table mobile_cdr_files' ;
   INSERT /*+ append */ INTO mobile_cdr_files t ( t.misdn , t.serviceInstance , t.accountNumber )
   SELECT /*+ parallel (ba,8) parallel (si,8) parallel (bt,8) parallel (btu,8) */
          bt.combinedTelephoneNumber AS misdn
        , si.id AS serviceInstance
        , ba.accountNumber
     FROM ccsowner.bsbBillingAccount ba
     JOIN ccsowner.bsbServiceInstance si ON ba.serviceInstanceId = si.parentServiceInstanceId
     JOIN ccsowner.bsbTelephoneusagerole btu ON si.id = btu.serviceInstanceId
     JOIN ccsowner.bsbTelephone bt ON btu.telephoneId = bt.id
    WHERE bt.telephoneNumberUseCode = 'MSISDN'
   ;
   COMMIT ;
   logger.write ( 'complete' ) ;
END mobile_cdr_files ;

PROCEDURE populate_orders IS
-- Used by datapool findOrders
BEGIN
   logger.write ( 'begin' ) ;
   data_prep_static_oms.populate_orders@oms ;
   logger.write ( 'complete' ) ;
END populate_orders ;

PROCEDURE accountsWithRecentCases IS
-- Nic Patte NFTREL-13177
   l_pool VARCHAR2(29) := 'ACCOUNTSWITHRECENTCASES' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO accountsWithRecentCases t ( t.accountNumber , t.product )
   SELECT /*+ noparallel */ DISTINCT ctxt.externalId AS accountNumber
        , c.product
     FROM caseManagement.bsbCmCase@cse c
     JOIN casemanagement.bsbCmContext@cse ctxt ON ctxt.caseId = c.id
    WHERE ctxt.type = 'ACCOUNTNUMBER'
      AND c.product IS NOT NULL
      AND is_open_status = 1
      AND c.created > SYSDATE - 56
      AND ROWNUM <= 50000
   ;
   COMMIT ;
   merge into accountsWithRecentCases aa using (
      select /*+ parallel(c, 8) */ c.accountNumber , c.messotoken from customers c
   ) b on ( b.accountNumber = aa.accountNumber )
   when matched then update set aa.messotoken = b.messotoken
   ;
   DELETE FROM accountsWithRecentCases d WHERE d.messoToken IS NULL ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.productId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.productID , s.messoToken
     FROM (
   SELECT d.accountNumber , d.product AS productId , d.messoToken
     FROM accountsWithRecentCases d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END accountsWithRecentCases ;

PROCEDURE userIdForCase IS
   l_pool VARCHAR2(29) := 'USERIDFORCASE' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO userIdForCase t ( t.userId )
   SELECT DISTINCT c.assignedTo AS userId
     FROM caseManagement.bsbCmCase@cse c
    WHERE c.status = 'OPEN'
      AND c.assignedTo NOT IN ( 'unassigned' , 'online-helpcentre-sauron' )
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.userId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.userId
     FROM (
   SELECT d.userId
     FROM userIdForCase d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END userIdForCase ;

PROCEDURE nvn_cease IS
   l_pool VARCHAR2(29) := 'NVN_CEASE' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO nvn_cease t ( t.accountNumber , t.portfolioId , t.status , t.talk_serviceId , t.bb_serviceId )
   select s.accountNumber , s.portfolioId , s.status , s.talk_serviceId , s.bb_serviceId
     from (
      select /*+ full(bpp) parallel(bpp 8) parallel(bcpe 8)
                 full(bsr) full(bsi) full(bba) full(bcp) full(tpe) full(bpe)
                 parallel(bsr 8) parallel(bsi 8) parallel(bba 8) parallel(bcp 8) parallel(tpe 8) parallel(bpe 8)
                 pq_distribute(bpp hash hash) pq_distribute(bcpe hash hash) pq_distribute(bcpe hash hash)
                 pq_distribute(bsr hash hash)
                 pq_distribute(bsi hash hash) pq_distribute(bba hash hash) pq_distribute(bcp hash hash) pq_distribute(tpe hash hash)
             */
         bba.accountNumber,
          bba.portfolioId,
          'AC' AS status ,
       max ( tpe.serviceid ) as Talk_ServiceID,
        max ( bpe.servicenumber ) as BB_ServiceID
          from ccsowner.bsbserviceinstance          bsi,
               ccsowner.bsbSubscription             bsr,
               ccsowner.bsbBillingAccount           bba,
               ccsowner.bsbPortfolioProduct         bpp,
               refdatamgr.bsbCatalogueProduct       bcp,
               ccsowner.bsbCustomerProductElement   bcpe,
               ccsowner.bsbtelephonycustprodelement tpe,
               ccsowner.bsbbroadbandcustprodelement bpe
         where bsi.id = bsr.serviceinstanceid
           AND bsr.created < SYSDATE - 60 --  13-Apr-2018 Andrew Fraser excluded recently added - Liam and Amit
           and bsi.parentserviceinstanceid = bba.serviceinstanceid
           --and bsi.telephonenumber like '%518'  --  13-Apr-2018 Andrew Fraser excluded recently added - Liam and Amit
           -- start 21-Sep-2019 Andrew Fraser restricted to only be for Sky Broadband Unilmited NFTREL-15244
           -- 21-Sep commented this out temporarily:
           and ( ( bsr.status = 'AC' and bpp.catalogueProductId in ( '14258' , '12673' ) ) or ( bsr.status = 'A' and bpp.catalogueProductId  = '12721' ) )
           --noData-- AND bsr.status = 'AC'  -- Active
           --noData-- AND bcp.productDescription LIKE 'Sky Broadband Unlimited%'
           -- end 21-Sep-2019 Andrew Fraser restricted to only be for Sky Broadband Unilmited NFTREL-15244
           and bsr.technologycode = 'MPF'
           and bba.currencycode = 'GBP'
           and bpp.portfolioId = bba.portfolioId
           and bpp.catalogueProductId = bcp.id
           and bba.serviceinstanceid = bsi.parentserviceinstanceid
           AND bcpe.portfolioproductid = bpp.id
           and tpe.telephonyproductelementid(+) = bcpe.id
           and bpe.lineproductelementid(+) = bcpe.id
           AND bba.accountNumber IN ( SELECT da.accountNumber FROM debt_amount da WHERE da.balance <= 0 )
           -- Andrew Fraser 17-May-2016 extra EXISTS at request Bruce Thomson, Stuart Anderson: "All of them have active primary DTV (subscription type 1 @ state AC) but no products under this subscription at a non cancelled state."
           AND EXISTS ( SELECT NULL FROM ccsowner.bsbPortfolioProduct exbpp where exbpp.subscriptionId = bsr.id AND exbpp.status <> 'CN' )
           AND NOT EXISTS ( SELECT /*+ full(nexbpp) parallel(nexbpp 8) pq_distribute(nexbpp hash hash) */ NULL FROM ccsowner.bsbPortfolioProduct nexbpp where nexbpp.subscriptionId = bsr.id AND nexbpp.status = 'AP' )  -- does not have 'Awaiting Provisioning' on broadband.
           group by bba.accountNumber, bba.portfolioId
           ) s
        where s.Talk_ServiceID is not null
          and s.BB_ServiceID is not null
          AND ROWNUM <= 20000
   ;
   COMMIT ;
   -- 16-Jul-2018 for Amit More add partyId
   MERGE INTO nvn_cease t USING (
      SELECT c.accountNumber
           , c.partyId
        FROM customers c
   ) s ON ( s.accountNumber = t.accountNumber )
   WHEN MATCHED THEN UPDATE SET t.partyId = s.partyId
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId
     FROM (
   SELECT d.accountNumber , d.partyId
     FROM nvn_cease d
    WHERE d.partyId IS NOT NULL
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 10000  -- 13-Apr-2018 Andrew Fraser for Liam, was 4000.
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END nvn_cease ;

PROCEDURE minimum_term IS
-- Staging table only, used by data_prep_04.act_cust_no_bb_st_vis (and other pools)
-- Future Enhancement: correct typo in table name - should be minimUm_term, not minimIm_term.
BEGIN
   execute immediate 'truncate table minimim_term' ;
   INSERT /*+ append*/ INTO minimim_term
   SELECT /*+ leading(auc) full(bpp) parallel(bpp, 8) full(bss) parallel(bss, 8) full(auc) parallel(auc, 4) pq_distribute(bss hash hash) */
          DISTINCT bpp.portfolioId
        , FIRST_VALUE ( bpp.subscriptionId ) OVER ( PARTITION BY bpp.portfolioId ORDER BY bss.id ) AS subscriptionId
     FROM ccsowner.bsbPortfolioProduct bpp
     JOIN ccsowner.bsbSubscription bss ON bpp.subscriptionId = bss.id
     JOIN act_uk_cust auc ON bpp.portfolioId = auc.portfolioId
    WHERE bss.subscriptionStartDate > TRUNC ( SYSDATE ) - 365
   ;
   COMMIT ;
   logger.write ( 'complete' ) ;
END minimum_term ;

PROCEDURE act_cust_no_bb_st_vis IS
   -- 15-Jul-2022 Andrew Fraser for Dimitrios Koulialis add contact number NFTREL-21983
   l_pool VARCHAR2(29) := 'ACT_CUST_NO_BB_ST_VIS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO act_cust_no_bb_st_vis t ( t.portfolioId , accountNumber , t.partyId , t.postcode , t.telephoneNumber )
   select /*+ leading(auc) use_hash(bvr bct) use_hash(bpp2 bba) full(bsr) full(auc) full(bba)
     full(bcr) full(bpr) full(bc) full(bct) full(bvr) full(mt) parallel(bsr 4) parallel(auc 4)
    parallel(bba 16) parallel(bcr 8) parallel(bpr 8) parallel(bc 8) parallel(bct 8) parallel(mt 4)
    pq_distribute(bpp2 hash hash) pq_distribute(bpr hash hash) pq_distribute(bc hash hash) pq_distribute(bct hash hash)
       */ DISTINCT
          bba.portfolioId
        , FIRST_VALUE ( bba.accountNumber ) OVER ( PARTITION BY bba.portfolioId ORDER BY bba.ROWID ) AS accountNumber
        , bc.partyid
        , auc.postcode
        , FIRST_VALUE ( bt.combinedTelephoneNumber ) OVER ( PARTITION BY bba.portfolioId ORDER BY bba.ROWID ) AS telephoneNumber
     FROM dataprov.act_uk_cust auc
     JOIN ccsowner.bsbBillingAccount bba on auc.portfolioId = bba.portfolioId
     JOIN ccsowner.bsbCustomerRole bcr on bcr.portfolioId = bba.portfolioId
     JOIN ccsowner.bsbpartyRole bpr on bcr.partyRoleId = bpr.id
     JOIN ccsowner.bsbContactor bc on bc.partyId = bpr.partyId
     JOIN ccsowner.bsbContactTelephone bct ON bc.id = bct.contactorId
     JOIN ccsowner.bsbTelephone bt ON bt.id = bct.telephoneId 
     LEFT OUTER JOIN (
          SELECT /*+ parallel(bpp 16) full(bpp)*/
                 portfolioId
            from ccsowner.bsbPortfolioProduct bpp
            join refdatamgr.bsbCatalogueProduct bcp ON bpp.catalogueProductId = bcp.id
           WHERE bcp.subscriptionType IN ( '7' , '3' )
           ) bpp2 on bpp2.portfolioId = bba.portfolioId
     LEFT OUTER JOIN (
          SELECT /*+ parallel(bvr2 8)*/
                 id
               , telephoneId
            FROM ccsowner.bsbVisitRequirement bvr2
           WHERE statusCode IN ( 'BK' , 'IC' )
          ) bvr on bvr.telephoneId = bct.telephoneId
      LEFT OUTER JOIN dataprov.minimim_term mt on bba.portfolioId = mt.portfolioId  -- populated by data_prep_04.minimum_term
      LEFT OUTER JOIN ccsowner.bsbSamRegistry bsr on bc.partyId = bsr.partyId
     WHERE bpp2.portfolioId IS NULL
       AND bvr.id IS NULL
       AND mt.portfolioId IS NULL
       AND bsr.partyId IS NULL
       AND bct.primaryFlag = 1
       AND bct.typeCode = 'H'
       AND bct.deletedFlag = 0
       AND bcr.customerStatusCode = 'CRACT'
       -- Added gne02 19/11/13 Only customer who have an active sky+ subscription are allowed
       AND EXISTS (
           SELECT bbpp.portfolioId
             FROM ccsowner.bsbPortfolioProduct bbpp
            WHERE bbpp.catalogueProductId = '10136'
              AND bbpp.status = 'IN'
           )
       -- Added by MGI18 requested by Bruce Thomson (Jira NFTREL-5504)
       -- exclude accounts with "Multi-size SIM card for Sky Mobile"
       AND NOT EXISTS (
           SELECT NULL
             FROM ccsowner.bsbPortfolioProduct bbpp
            WHERE bbpp.portfolioId = bba.portfolioId
              AND bbpp.catalogueProductId = '14206'
           )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted.' ) ;
   COMMIT ;
   -- 31-May-2022 Andrew Fraser for Dimitrios Koulialis, restrict to valid email addresses for RESDEL-1841 https://confluence.bskyb.com/pages/viewpage.action?pageId=235929837
   -- 17-Dec-2022 Andrew Fraser added noparallel hint to workaround "ORA-1652: unable to extend temp segment in tablespace temp" Alternative fix would be hints like e.g. "pq_distribute(t hash hash)".
   MERGE /*+ noparallel */ INTO act_cust_no_bb_st_vis t USING (
      SELECT DISTINCT c.partyId , c.skycesa01Token , c.messoToken , c.emailAddress
        FROM customers c
        JOIN dataprov.debt_amount da ON c.accountNumber = da.accountNumber
       WHERE da.balance <= 0  -- 26-Mar-2018 Andrew Fraser request Nic Patte. Reuses table populated in package customers, procedure customer_debt.
   ) s ON ( s.partyId = t.partyId )
   WHEN MATCHED THEN UPDATE SET t.skycesa01Token = s.skycesa01Token , t.messoToken = s.messoToken , t.emailAddress = s.emailAddress
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged.' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.postcode
        , t.skycesa01Token , t.messoToken , t.telephoneNumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.postcode
        , s.skycesa01Token , s.messoToken , s.telephoneNumber
     FROM (
           SELECT d.accountNumber , d.partyId , d.postcode , d.skycesa01Token , d.messoToken , d.telephoneNumber
             FROM act_cust_no_bb_st_vis d
            WHERE d.messoToken IS NOT NULL
              AND d.emailAddress NOT LIKE 'noemail_%'
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 300000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END act_cust_no_bb_st_vis ;

PROCEDURE act_cust_inst_prod IS
-- brought back 31-Aug-2021 for Owen Thomas - almost certainly still not needed or used, if so can delete again
   l_pool VARCHAR2(29) := 'ACT_CUST_INST_PROD' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO act_cust_inst_prod t ( t.portfolioId )
   select /*+ full(bpp) parallel(bpp 4) full(bs) parallel(bs 4) full(auc) parallel(auc 4) pq_distribute(bpp hash hash) pq_distribute(bs hash hash)  */ 
          distinct bpp.portfolioId
       --replacing delete from below
     from ccsowner.bsbPortfolioProduct bpp,
       ccsowner.bsbSubscription bs,
       dataprov.act_uk_cust auc
       -- test using HDW products also
       -- where bpp.catalogueProductId in ('11090','13522','23540','13425', '13646', '13653','10136', '10116', '10140', '10141', '10142')
       --where bpp.catalogueProductId in ('11090','13522','23540','13425', '13646', '13653','10136', '10116', '10140', '10141', '10142','13787','13788','13791','13970')
       where bpp.catalogueProductId in ('11090','13522','23540','13425', '13646', '13653','10136', '10116', '10140', '10141', '10142','13787','13788','13791','13970'
       ,'11090','13641','13686','13789','13790','13792')
       and bpp.status = 'IN'
       and bpp.serviceinstanceid = bs.serviceinstanceid
       and bs.status = 'AC'
       and bpp.created < sysdate -14
       and bpp.portfolioId = auc.portfolioId
       and auc.accountNumber in (select distinct co.customerAccountNumber
     FROM oh.resourceorders@oms ro, oh.resourceOrderStatusCodes@oms rosc,  oh.customerOrders@oms co
    where ro.roStatusCodeId = rosc.roStatusCodeId
      and co.customerOrderId = ro.customerOrderId
      and rosc.roStatusCode = 'COMPLETED'
   -- Added in for Ryan to eliminate in progress orders
   minus                                         
   select distinct co.customerAccountNumber
     FROM oh.resourceorders@oms ro, oh.resourceOrderStatusCodes@oms rosc,  oh.customerOrders@oms co
    where ro.roStatusCodeId = rosc.roStatusCodeId
      and co.customerOrderId = ro.customerOrderId
      and rosc.roStatusCode = 'IN_PROGRESS')
       and not exists (
         select /*+ full(bpp) parallel(bpp, 4) parallel(bcp, 4) pq_distribute(bpp hash hash) */  bpp.portfolioId
                from ccsowner.bsbPortfolioProduct bpp,
                refdatamgr.bsbCatalogueProduct bcp
                where bpp.catalogueProductId = bcp.id
                and (bcp.productsubtypecode = 'EVENT' OR bcp.subscriptiontype in ('7','3')
                --
                -- GNE02 25/11/2014 Only excluding PPV events that we has sold since refresh
                --
                 and bcp.created > sysdate -14
                 --
                 -- gne02 20/11/13 added condition to exclude SKY+ subscriptions  that are awaiting service
                 --
                 OR (bpp.catalogueProductId = '10136' and  bpp.status = 'AS')
                 --
                 -- gne02 22/11/13 Added condition to exclude Service Call Visit that is Awaiting Visit
                 --
                 OR (bpp.catalogueProductId = '13431' and  bpp.status = 'AV')
                 --
                 -- gne02 03/01/14 added condition to exclude Sky+ Subscription that is pending cancel
                 --
                 OR (bpp.catalogueProductId = '10113' and  bpp.status = 'PC')
                 -- 
                 -- cmc59 01/10/14 - exclusions for TV products which were tripping up CANCELPRODUCT/SUB tests
                 --
                 OR (bpp.catalogueProductId = '13728' and bpp.status = 'RQ')
                 OR (bpp.catalogueProductId = '13712' and bpp.status = 'CN')
                 --
                 -- GNE02 03/12/14 - Removing customer who have "Blocked Cease Requested", "CEASE REQUESTED"
                 -- or "Blocked Cease Requested" protfolio entries or "awaiting visit".
                 --
                 OR (bpp.status in ('BCRQ','FBP','BCRQ','CRQ', 'AV','AD'))
                 )
                and bpp.portfolioId = auc.portfolioId)
       and not exists (select mt.portfolioId
            from dataprov.minimim_term mt
           where bpp.portfolioId = mt.portfolioId)
              and exists (select /*+  full(bpp2) parallel(bpp2 4)   */ 1
                    from ccsowner.bsbPortfolioProduct bpp2
                    where bpp.portfolioId = bpp2.portfolioId
                    and bpp2.status='A' and bpp2.catalogueProductId = '12721')

               and exists (select /*+ full(bs3) parallel(bs3 4) full(bpp3) parallel(bpp3 4) pq_distribute(bpp3 hash hash) pq_distribute(bs3 hash hash) */ 1
                    from ccsowner.bsbSubscription bs3, ccsowner.bsbPortfolioProduct bpp3
                    where bs3.serviceinstanceid = bpp3.serviceinstanceid
                    and bpp.portfolioId = bpp3.portfolioId
                    and bs3.subscriptiontypeid = '7' and bs3.status='AC'
                )
       -- Andrew Fraser 18-Mar-2016 exclude all pending cancelled, request Karthik BVM for Cancel Product test getting Dependent Products Error 'Some of the following dependent products have Pending Cancellations against them'
       AND NOT EXISTS ( SELECT /*+  full(bppne) parallel(bppne 4)  */ NULL FROM ccsowner.bsbPortfolioProduct bppne WHERE bppne.portfolioId = auc.portfolioId AND bppne.status = 'PC' )
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber
     FROM (
   SELECT c.accountNumber
     FROM act_cust_inst_prod d
     JOIN customers c ON c.portfolioId = d.portfolioId
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 600000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END act_cust_inst_prod ;

PROCEDURE act_cust_sky_plus IS
   l_pool VARCHAR2(29) := 'ACT_CUST_SKY_PLUS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO act_cust_sky_plus t ( t.portfolioId , t.accountNumber , t.combinedTelephone )
   select s.portfolioId , s.accountNumber , s.combinedTelephoneNumber AS combinedTelephone
     from (
    with bppt as (
       select  /*+ full(bpp) parallel(bpp 8)*/ /*Parallel reduced from 16 to 8 after 12c upgrade iml02(paralle data load)*/
       bpp.portfolioId
       ,max(case when catalogueProductId in ('10136','10116') and bpp.status='IN' then 1  else 0 end) over (partition by bpp.portfolioId) flag1
       ,max(case when catalogueProductId = '10113' and bpp.status='EN' then 1 else 0 end) over (partition by bpp.portfolioId) flag2
       ,max(case when catalogueProductId in ('11090',/*'13522','23540',*/'13425', '13646', '13653', '13641','13787', '13788','13791','13744') then 1 else 0 end) over (partition by bpp.portfolioId) flag3
       --,max(case when catalogueProductId = '13712' and bpp.status='EN' then 1 else 0 end) over (partition by bpp.portfolioId) flag4 
       ,max(case when catalogueProductId = '13429' and bpp.status != 'AV' then 1 else 0 end) over (partition by bpp.portfolioId) flag5
       --,max(case when catalogueProductId = '13744' and bpp.status = 'AC' then 1  else 0 end) over (partition by bpp.portfolioId) flag6
       from (
          select /*+ full(t) parallel(t 8)*/  /*Parallel reduced from 16 to 8 mgi18 (paralle data load)*/  t.* from ccsowner.bsbPortfolioProduct t
          join dataprov.act_uk_cust t2
          on t.portfolioId = t2.portfolioId
          ) bpp
    ) , telno as (
       select /*+ parallel(bt 8) parallel(bct 8) parallel(bc 8) */
       bc.partyid --switched parallel to 8, 25/10/19 Callum Bulloch
       ,bt.combinedtelephonenumber
       from
       ccsowner.bsbtelephone bt,
       ccsowner.bsbcontacttelephone bct,
       ccsowner.bsbcontactor bc
       where bc.id = bct.contactorid
       and bct.primaryflag = '1'
       and bct.deletedflag = '0'
       and bct.effectiveTodate is null
       and bct.telephoneid = bt.id
       and length ( bt.combinedTelephoneNumber ) <= 11
       group by bc.partyid , bt.combinedTelephoneNumber
       having count ( bc.partyid ) = 1
    ) , subs as (
       SELECT /*+ full(si) parallel(si 8)*/  si.portfolioId, count (*) countt
       FROM ccsowner.bsbserviceinstance si
       WHERE serviceinstancetype in (210,220)
       group by si.portfolioId
    )
    select /*+ leading(acs) parallel(acs 8) parallel(bcr 16) full(bcr) pq_distribute(telno hash hash) pq_distribute(bppt hash hash) */
           distinct acs.portfolioId
         , first_value ( acs.accountNumber ) over ( partition by acs.portfolioId order by acs.rowid ) AS accountNumber
         , first_value ( telno.combinedTelephoneNumber ) over ( partition by telno.partyid ) AS combinedTelephoneNumber
    from dataprov.act_uk_cust acs
    join bppt
    on acs.portfolioId = bppt.portfolioId and flag1=1 and flag2=1 and flag3=0 and flag5 =0 --and flag6=1
    join subs
    on acs.portfolioId = subs.portfolioId and countt <=2
    join telno
    on telno.partyid=acs.partyid
    left join ccsowner.bsbCustomerRole bcr
    on bcr.portfolioId = acs.portfolioId
    and bcr.customerStatusCode != 'CRACT'
    where bcr.partyroleid is null
    ) s
   ;
   COMMIT ;
   dbms_stats.gather_table_stats ( 'DATAPROV' , 'ACT_CUST_SKY_PLUS' ) ;   
   logger.write ( 'line 921 : working table populated' ) ;   
   -- 26/02/2018 Andrew Fraser request Archana Burla "HDSaleExisting: ignore the customers who has Post Active Cancel. This is causing in the shop to sale any TV products."
   -- 08/03/2018 Andrew Fraser request Archana Burla "Modify the pool for customer with active DTV without Sky Q products 14277 , 13948 , 13950."
   /* 24/09/2020 Alex Hyslop Requested by Deepa 24/09/2020
                 Not a legacy customer (Boxsets, Original, Variety) AND
                 Not an escalated customer AND
                 Not in active block AND
                 Not in Debt
                 Does not have skyQ
   */
   DELETE FROM act_cust_sky_plus acsp
    WHERE NOT EXISTS (
          SELECT NULL
            FROM customers cu
               , ccsowner.bsbContactor con
               , ccsowner.person per
           WHERE cu.accountNumber = acsp.accountNumber
             and con.partyid = cu.partyid
             and cu.partyid = per.partyid
             AND cu.dtv = 1  -- with active DTV
             AND cu.skyQBox = 0  -- without Sky Q products
             AND cu.skyQBundle = 0  -- without Sky Q products
             and ( cu.boxsets = 0 or original = 0 or variety = 0 )
             and con.escalationcode is null -- not escalated
             and NVL ( per.blockPurchaseSwitch , 0 ) != 1 -- not active block
         )
   ;
   logger.write ( 'line 948 : delete 1 done ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   DELETE FROM act_cust_sky_plus t
    WHERE EXISTS (
          SELECT NULL
            FROM ccsowner.bsbPortfolioOffer bpo 
           WHERE bpo.portfolioId = t.portfolioId
             AND bpo.status = 'ACT'
             AND bpo.offerId = 78670
          )
   ;
   logger.write ( 'line 958 : delete 2 done ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   -- 26-Apr-2022 Andrew Fraser for Antti Makarainen exclude in-flight orders (strictly Antti's request was to exclude "active home move visits")
   DELETE FROM act_cust_sky_plus t
    WHERE EXISTS (
          SELECT NULL 
            FROM ccsowner.bsbBillingAccount ba
            JOIN ccsowner.bsbAddressUsageRole aur ON aur.serviceInstanceId = ba.serviceInstanceId
            JOIN ccsowner.bsbVisitRequirement vr ON vr.installationAddressRoleId = aur.id
           WHERE ba.portfolioId = t.portfolioId
             AND vr.statusCode NOT IN ( 'CP' , 'CN' )
          )
   ;
   logger.write ( 'deleted for in-flight orders : ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   MERGE INTO act_cust_sky_plus t USING (
      SELECT c.portfolioId , c.accountNumber , c.partyId , c.skycesa01Token , c.messoToken
        FROM customers c
       WHERE c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- 09/04/2018 Andrew Fraser request Archana Burla exclude customers with balance due on their account.
   ) s ON ( s.portfolioId = t.portfolioId AND s.accountNumber = t.accountNumber )
   WHEN MATCHED THEN UPDATE SET t.partyId = s.partyId , t.skycesa01Token = s.skycesa01Token , t.messoToken = s.messoToken
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   logger.write ( 'line 967 : merge done' ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.skycesa01Token , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.skycesa01Token , s.messoToken
     FROM (
   SELECT d.accountNumber , d.partyId , d.skycesa01Token , d.messoToken
     FROM act_cust_sky_plus d
    WHERE d.partyId IS NOT NULL
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 150000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END act_cust_sky_plus ;

PROCEDURE act_cust_act_visit IS
   l_pool VARCHAR2(29) := 'ACT_CUST_ACT_VISIT' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO act_cust_act_visit t ( t.portfolioId , t.accountNumber , t.created , t.customerStatusCode , t.partyId , t.ethan_cust )
   with checko as (
      select /*+ full(bpp) parallel(bpp, 6) */ case when bpp.catalogueProductId in ( '13947' , '13948' )  then 1 else 0 end AS flag1
           , bpp.portfolioId
        from ccsowner.bsbPortfolioProduct bpp     
   )
   select portfolioId
       , accountNumber
       , created
       , customerStatusCode
       , partyid
       , ethan_cust
    from (
    select /*+ full(bba) parallel(bba, 8) full(bcr) parallel(bcr, 6) full(ba) parallel(ba, 6) full(baur) parallel(baur, 6)*/
         bba.portfolioId
       , bba.accountNumber
       , bvr.created
       , bcr.customerStatusCode
       , bpr.partyid
       , row_number() over ( partition by bba.portfolioId order by bba.rowid ) AS rn
       , checko.flag1 AS ethan_cust
    from ccsowner.bsbBillingAccount bba,
       ccsowner.bsbCustomerRole bcr,
       ccsowner.bsbAddress ba,
       ccsowner.bsbAddressusagerole baur,
       ccsowner.bsbvisitrequirement bvr,
       ccsowner.bsbpartyrole bpr,
       checko
    --    ,ccsowner.bsbtelephone bt
    where bvr.statuscode = 'BK'
    and bba.portfolioId = checko.portfolioId
    and bpr.id = bcr.partyroleid
    and bvr.visitdate > trunc(sysdate) + 1
    and bvr.installationaddressroleid = baur.id
    -- Added gne02 07/03/14
    -- also pick up service request visits as well as installation visits.
    and bvr.jobtype IN ('IN', 'SR')
    -- Added gne02 19/11/13
    -- Only to pick up DTV installations that do not have a NULL fmsjobreference
    and bvr.fmsjobreference is not NULL
    -- Added gne02 15/05/14
    -- Only to pick up DTV visits
    and bvr.jobrefidentifier = 'DTV'
    and baur.addressId = ba.id
    and ba.countrycode = 'GBR'
    and baur.serviceinstanceid = bba.serviceinstanceid
    and bcr.portfolioId = bba.portfolioId
    and bcr.customerStatusCode in ( 'CRACT' , 'CRP' )
    )
    where rn = 1
   ;
   COMMIT ;
   DELETE FROM act_cust_act_visit t WHERE t.ethan_cust != 0 OR t.ethan_cust IS NULL OR t.accountNumber IS NULL ;
   DELETE FROM act_cust_act_visit t
    WHERE t.portfolioId IN (
          SELECT bpp.portfolioId
            FROM ccsowner.bsbPortfolioProduct bpp
           WHERE bpp.status = 'BL'  -- Blocked
          )
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId
     FROM (
   SELECT d.accountNumber , d.partyId
     FROM act_cust_act_visit d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END act_cust_act_visit ;

PROCEDURE cancelled IS
   l_pool VARCHAR2(29) := 'CANCELLED' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO cancelled t ( t.portfolioId , t.accountNumber )
   SELECT /*+ parallel(8) */ s.portfolioId , s.accountNumber
     FROM (
         WITH bcr AS (
            SELECT /*+ parallel(bcr2 8) pq_distribute(bcr2 hash hash) */ bcr2.portfolioId
              FROM ccsowner.bsbCustomerRole bcr2
             WHERE bcr2.customerStatusCode = 'CRIC'
               AND bcr2.unableToPurchaseProductsSwitch = 0
         ) , bpp1 AS (
            SELECT /*+ parallel(bcr2 8) pq_distribute(bcr2 hash hash) parallel(t2 8) pq_distribute(t2 hash hash) */
                   t1.portfolioId
                 , t1.subscriptionId
                 , t1.catalogueProductId
              FROM ccsowner.bsbPortfolioProduct t1
              JOIN bcr t2 ON t1.portfolioId = t2.portfolioId
         )
         SELECT /*+ leading(bcr) use_hash(bssel bss)
                    parallel(bcr 8) pq_distribute(bcr hash hash)
                    parallel(bpp1 8) pq_distribute(bpp1 hash hash)
                    parallel(bba 8) pq_distribute(bba hash hash)
                    parallel(baur 8) pq_distribute(baur hash hash)
                    parallel(ba 8) pq_distribute(ba hash hash)
                    parallel(bpmr 8) pq_distribute(bpmr hash hash)
                    parallel(bpm 8) pq_distribute(bpm hash hash)
                    parallel(bss 8) pq_distribute(bss hash hash)
                    parallel(bssel 8) pq_distribute(bssel hash hash)
                */
                DISTINCT bcr.portfolioId
              , FIRST_VALUE ( bba.accountNumber ) OVER ( PARTITION BY bpp1.portfolioId ORDER BY bba.accountNumber ) AS accountNumber
           FROM bcr
           JOIN bpp1 ON bpp1.portfolioId = bcr.portfolioId
           JOIN ccsowner.bsbBillingAccount bba ON bcr.portfolioId = bba.portfolioId
           JOIN ccsowner.bsBbillingAddressRole baur ON baur.billingAccountId = bba.id
           JOIN ccsowner.bsbAddress ba ON ba.id = baur.addressId
           JOIN ccsowner.bsbPaymentMethodRole bpmr ON bba.id = bpmr.billingAccountId
           JOIN ccsowner.bsbPaymentMethod bpm ON bpmr.paymentMethodId = bpm.id
           JOIN ccsowner.bsbSubscription bss ON bpp1.subscriptionId = bss.id
           JOIN ccsowner.bsbSubscriptionEntitlement bssel ON bssel.subscriptionId = bss.id
          WHERE baur.effectiveTo IS NULL
            AND bpmr.effectiveTo IS NULL
            AND bpm.paymentMethodType IN ( '03' , '05' )
            AND ba.countrycode = 'GBR'
            AND bss.status in ( 'PO' , 'CN' )
            AND bss.statusChangedDate > SYSDATE - ( 5 * 366 )
            AND bssel.entitlementId NOT IN (
                SELECT /*+ parallel(a 8) pq_distribute(a hash hash) */ a.entitlementId
                  FROM refdatamgr.bsbCatalogueProduct a
                  JOIN refdatamgr.bsbEntitlement b ON a.entitlementId = b.id
                 WHERE a.salesStatus = 'EXP'
                )
            AND bpp1.catalogueProductId NOT IN (
                SELECT /*+ parallel(bcp1 8) pq_distribute(bcp1 hash hash) */ bcp1.id
                  FROM refdatamgr.bsbCatalogueProduct bcp1
                 WHERE bcp1.salesStatus != 'EXP'
                )
            AND EXISTS (
                SELECT /*+ parallel(bpp2 8) pq_distribute(bpp2 hash hash) */ NULL
                  FROM ccsowner.bsbPortfolioProduct bpp2
                 WHERE bpp2.portfolioId = bba.portfolioId
                   AND bpp2.catalogueProductId = '11090'
                   AND bpp2.status != 'DI'
                )
            AND NOT EXISTS (
                SELECT /*+ parallel(bpp3 8) pq_distribute(bpp3 hash hash) parallel(rcp 8) pq_distribute(rcp hash hash) */ NULL
                  FROM ccsowner.bsbPortfolioProduct bpp3
                  JOIN dataprov.stg_sportsProducts rcp ON rcp.id = bpp3.catalogueProductId  -- populated as in below comments.
                 WHERE bpp3.portfolioId = bba.portfolioId
                )
        ) s
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   -- 16-Aug-2018 Remove customers with legacy Sky Sports HD. Andrew Fraser for Archana Burla.
   /* Initial setup was:
   EXECUTE IMMEDIATE 'CREATE TABLE dataprov.stg_sportsProducts ( id VARCHAR2(5) NOT NULL ENABLE ) NOLOGGING' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.stg_sportsProducts' ;
   INSERT INTO dataprov.stg_sportsProducts t
   SELECT rcp.id
     FROM refdatamgr.bsbCatalogueProduct rcp
    WHERE LENGTH ( rcp.id ) = 5
      AND rcp.productDescription LIKE '%Sports%'
      AND rcp.created < TO_DATE ( '01-Jul-2017' , 'DD-Mon-YYYY' )  -- legacy
    ORDER BY rcp.id
   ;
   DBMS_STATS.GATHER_TABLE_STATS ( 'dataprov' , 'stg_sportsProducts' ) ;
   */
   DELETE FROM cancelled t
    WHERE EXISTS (
          SELECT NULL
            FROM ccsowner.bsbPortfolioOffer bpo 
           WHERE bpo.portfolioId = t.portfolioId
             AND bpo.status = 'ACT'
             AND bpo.offerId = 78670
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber
     FROM (
           SELECT d.accountNumber
             FROM cancelled d
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 150000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END cancelled ;

PROCEDURE act_uk_cust_idnv_triple IS
-- Not a staging table, is called directly by dynamic_data_pkg.data_idnv_triple to service tomcat dataprov requests, so do not truncate this table.
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table act_uk_cust_idnv_triple' ;
   INSERT /*+ append */ INTO act_uk_cust_idnv_triple t ( t.accountNumber , t.billingAccountId , t.partyID , t.postcode
        , t.bad_postcode , t.houseNumber , t.emailAddress , t.combinedTelephoneNumber , t.seqno )
   SELECT a.accountNumber , a.billingAccountId , a.partyID , a.postcode
        , a.bad_postcode , a.houseNumber , a.emailAddress , a.combinedTelephoneNumber , a.seqno
     FROM act_uk_cust_idnv a  -- loaded by chorddbptt:~oracle/ptt/scripts/post_env_refresh_pools.bash after env refresh only.
    WHERE a.accountNumber in (
          SELECT b.accountNumber
            FROM act_cust_uk_subs b  -- loaded by customers_pkg.act_cust_uk_subs nightly.
           WHERE b.dtv = 'AC'
             AND b.talk = 'A'
             AND b.bband = 'AC'
          )
   ;
   COMMIT ;
   logger.write ( 'complete' ) ;
END ;

END data_prep_04 ;
/
