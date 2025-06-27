CREATE OR REPLACE PACKAGE data_prep_05 AS
PROCEDURE triplePlay_no_debt ;
PROCEDURE triplePlayNoDebtNoNetflix ;
PROCEDURE salesInteractionRef_directE ;
PROCEDURE salesInteractionRef_dtv ;
PROCEDURE salesInteractionRef_mobile ;
PROCEDURE salesInteractionRefInv_dtv ;
PROCEDURE getCommunication ;
PROCEDURE getCommunication_con ;
PROCEDURE addresses ;
PROCEDURE bill_ref_account_id ;
PROCEDURE skyQViewingCards ;
PROCEDURE names ;
PROCEDURE customersWithPaymentDetails ;
PROCEDURE customersForPairCardCallback ;
PROCEDURE caseRespTemplate ;
PROCEDURE bb_recontracting ;
PROCEDURE teamIdForCase ;
PROCEDURE onlineProfileId_cust_search ;
PROCEDURE act_mob_no_pac ;
PROCEDURE digitalMyMessages ;
PROCEDURE mobile_credit_balance ;
PROCEDURE act_mobile_customers_no_debt ;
PROCEDURE digitalCurrentBbtNoDebt ;
PROCEDURE soipSignedPlansNoBurn ;
PROCEDURE mobile_device_part_no ;
END data_prep_05 ;
/


CREATE OR REPLACE PACKAGE BODY data_prep_05 AS

PROCEDURE triplePlay_no_debt IS
   -- 21-Sep-2022 Alex Benetatos removed "17-Feb-2022 Andrew Fraser for Dimitrios Koulialis exclude postcodes 7FU, 2AA, 1DJ, 1EA because they are sent to SNS, everything else goes to stubs. NFTREL-21518"
   l_pool VARCHAR2(29) := 'TRIPLEPLAY_NO_DEBT' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   execute immediate 'alter session enable parallel dml' ;
   INSERT /*+ append */ INTO tripleplay_no_debt t ( t.accountNumber , t.partyId )
   SELECT act.accountNumber , act.partyId
     FROM act_cust_dtv_bb_talk act
     JOIN customers c ON c.partyId = act.partyId
     JOIN debt_amount da ON da.accountNumber = act.accountNumber  -- 25-Aug-2022
    WHERE c.inFlightOrders = 0  -- 25-Aug-2022 Alex Benetatos SOIPPOD-2497
      AND c.inFlightVisit = 0  -- 28-Sep-2021 Alex Benetatos
      AND da.balance <= 0  -- no outstanding balance
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   -- 21-Oct-2021 Andrew Fraser for Rizwan Soomra exclude any customer with SOIP (even if under a different accountNumber)
   DELETE FROM tripleplay_no_debt d
    WHERE EXISTS (
          SELECT NULL
            FROM ccsowner.bsbBillingAccount ba
            JOIN ccsowner.bsbServiceInstance si ON ba.portfolioId = si.portfolioId
            JOIN rcrm.service serv ON si.id = serv.billingServiceInstanceId
           WHERE d.accountNumber = ba.accountNumber
             AND serviceType = 'SOIP'  -- 19-Oct-2022 Andrew Fraser attempt to get more data in data pool, hoping AMP customers might be ok.
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for soip' ) ;
   COMMIT ;
   -- 29-Oct-2021 Andrew Fraser for Rizwan Soomra remove accounts that have no primary email address.
   DELETE /*+ parallel(8) */ FROM tripleplay_no_debt d
    WHERE d.partyId NOT IN (
          SELECT bc.partyId
            FROM ccsowner.bsbContactor bc
            JOIN ccsowner.bsbContactEmail bce ON bce.contactorId = bc.id
            JOIN ccsowner.bsbEmail be ON be.id = bce.emailId
           WHERE bce.deletedFlag = 0
             AND SYSDATE BETWEEN bce.effectiveFromDate AND NVL ( bce.effectiveToDate , SYSDATE + 1 )
             AND be.emailAddressStatus = 'VALID'
             AND bce.primaryFlag = 1
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for emails' ) ;
   COMMIT ;
   -- 04-Nov-2021 Andrew Fraser for Rizwan Soomra remove accounts that have no mobile number against their record.
   -- 15-Nov-2021 Andrew Fraser for Rizwan Soomra also remove if international dialing code is not '+44'
   -- 16-Nov-2021 Andrew Fraser for Rizwan Soomra only want accounts that have a mobile number as the primary contact number.
   -- https://cbsjira.bskyb.com/browse/TVC-1769
   DELETE /*+ parallel(8) */ FROM tripleplay_no_debt d
    WHERE d.partyId NOT IN (
          SELECT bc.partyId
            FROM ccsowner.bsbContactor bc
            JOIN ccsowner.bsbContactTelephone bct ON bct.contactorId = bc.id
            JOIN ccsowner.bsbTelephone bt ON bt.id = bct.telephoneId
           WHERE bct.deletedFlag = 0
             AND SYSDATE BETWEEN bct.effectiveFromDate AND NVL ( bct.effectiveToDate , SYSDATE + 1 )
             AND bt.telephoneNumberStatus = 'VALID'
             AND bct.mobileNumberConfirmedDate < SYSDATE
             AND bt.combinedTelephoneNumber LIKE '07%'
             AND LENGTH ( bt.combinedTelephoneNumber ) = 11
             AND bt.internationalDialingCode = '+44'
             AND bct.primaryFlag = 1
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for mobiles' ) ;
   COMMIT ;
   -- 09-Mar-2022 Andrew Fraser for Terence Burton NFTREL-21621
   DELETE /*+ parallel(8) */ FROM tripleplay_no_debt d
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
   -- 03-Sep-2022 Andrew Fraser for Michael Santos from Scott Thompson, delete if has a salesStatus of PREACTIVE
   DELETE /*+ parallel(8) */ FROM tripleplay_no_debt d
    WHERE EXISTS (
          SELECT NULL
            FROM ccsowner.bsbBillingAccount ba
            JOIN ccsowner.bsbPortfolioProduct pp ON pp.portfolioId = ba.portfolioId
           WHERE ba.accountNumber = d.accountNumber
             AND LENGTH ( pp.catalogueProductId ) = 5
             AND pp.status IN ( 'KQ' , 'RQ' )
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for preactive' ) ;
   COMMIT ;
   -- 19-Oct-2022 Michael Santos delete customers with skeletal accounts to fix error "Customer unable to add Sky Glass due to pending portfolio change"
   DELETE FROM tripleplay_no_debt t
    WHERE EXISTS (
          SELECT NULL
            FROM ccsowner.bsbBillingAccount ba
            JOIN ccsowner.bsbBillingAccount ba_del ON ba_del.portfolioId = ba.portfolioId
           WHERE ba.accountNumber = t.accountNumber
             AND ba_del.skeletalAccountFlag = 1
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for skeletal accounts' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.messoToken , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.messoToken , s.partyId
     FROM (
           SELECT d.accountNumber
                , NVL ( c.messoToken , 'NO TOKEN' ) AS messoToken
                , d.partyId
             FROM triplePlay_no_debt d
             LEFT OUTER JOIN customers c on c.accountNumber = d.accountNumber
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   -- 19-Oct-2022 Michael Santos changed to BURN to fix error "Customer unable to add Sky Glass due to pending portfolio change" when trying to re-sell soip again to the same customer in cycle loop.
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END triplePlay_no_debt ;

PROCEDURE triplePlayNoDebtNoNetflix IS
   -- Alex Benetatos https://cbsjira.bskyb.com/browse/SOIPPOD-2207 
   l_pool VARCHAR2(29) := 'TRIPLEPLAYNODEBTNONETFLIX' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.messoToken
     FROM (
   SELECT d.accountNumber
        , NVL ( c.messoToken , 'NO TOKEN' ) AS messoToken
     FROM triplePlay_no_debt d
     JOIN customers c on c.accountNumber = d.accountNumber
    WHERE c.netflix = 0
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END triplePlayNoDebtNoNetflix ;

PROCEDURE salesInteractionRef_directE IS
-- real name would be "salesInteractionRef_directExisting" , but had to be shortened to fit into name of sequence. 
   l_pool VARCHAR2(29) := 'SALESINTERACTIONREF_DIRECTE' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table salesInteractionRef' ;
   data_prep_sal_iss.salesInteractionRef@iss ;
   INSERT /*+ append */ INTO salesInteractionRef t SELECT * FROM salesinteractionref@iss ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.id )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.id
     FROM (
   SELECT d.reference AS id
     FROM salesInteractionRef d
    WHERE d.directexisting = 'DIRECTEXISTING'
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END salesInteractionRef_directE ;

PROCEDURE salesInteractionRef_dtv IS
   l_pool VARCHAR2(29) := 'SALESINTERACTIONREF_DTV' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.id )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , q.reference AS id
     FROM (
          SELECT s.reference
            FROM salesInteractionRef s
           WHERE s.lob_size <= 50
             AND ROWNUM <= 3625
           UNION ALL
          SELECT s.reference
            FROM salesInteractionRef s
           WHERE s.lob_size BETWEEN 51 AND 100
             AND ROWNUM <= 9600
           UNION ALL
          SELECT s.reference
            FROM salesInteractionRef s
           WHERE s.lob_size BETWEEN 101 AND 175
             AND ROWNUM <= 25500
           UNION ALL
          SELECT s.reference
            FROM salesInteractionRef s
           WHERE s.lob_size BETWEEN 176 AND 250
             AND ROWNUM <= 24500
           UNION ALL
          SELECT s.reference
            FROM salesInteractionRef s
           WHERE s.lob_size BETWEEN 251 AND 500
             AND ROWNUM <= 35000
           UNION ALL
          SELECT s.reference
            FROM salesInteractionRef s
           WHERE s.lob_size BETWEEN 501 AND 1000
             AND ROWNUM <= 5500
           UNION ALL
          SELECT s.reference
            FROM salesInteractionRef s
           WHERE s.lob_size > 1000
             AND ROWNUM <= 530
          ) q
    ORDER BY dbms_random.value
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END salesInteractionRef_dtv ;

PROCEDURE salesInteractionRef_mobile IS
   l_pool VARCHAR2(29) := 'SALESINTERACTIONREF_MOBILE' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.id )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.id
     FROM (
   SELECT d.reference AS id
     FROM salesInteractionRef d
    WHERE d.nowtv IS NULL
      AND d.mobile = 'MOBILE'
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END salesInteractionRef_mobile ;

PROCEDURE salesInteractionRefInv_dtv IS
   l_pool VARCHAR2(29) := 'SALESINTERACTIONREFINV_DTV' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table salesInteractionRefInv' ;
   data_prep_sal_iss.salesInteractionRefInv@iss ;
   INSERT /*+ append */ INTO salesInteractionRefInv t SELECT * FROM salesInteractionRefInv@iss ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.id )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.id
     FROM (
   SELECT d.reference AS id
     FROM salesInteractionRefInv d
    WHERE d.dtv = 'DTV'
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END salesInteractionRefInv_dtv ;

PROCEDURE getCommunication IS
   l_pool VARCHAR2(29) := 'GETCOMMUNICATION' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO getCommunication t ( t.externalId , t.partyId , t.accountNumber )
   SELECT art.externalId , art.partyId , art.accountNumber
     FROM tcc_owner.bsbCommsArtifact@tcc art
    WHERE art.status IN ( 'WITH_FULFILMENT_HOUSE' , 'GENERATED' )
      AND art.created >= TO_DATE ( '01-Jan-2019' , 'DD-Mon-YYYY' )  -- 23-Feb-2022 Andrew Fraser for Christian Sallnow cos no data. Previously was ">= SYSDATE - 365"
      AND art.externalId IS NOT NULL
      AND ROWNUM <= 100000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.externalId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.externalId
     FROM (
   SELECT d.accountNumber , d.partyId , d.externalId
     FROM getCommunication d
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 20000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END getCommunication ;

PROCEDURE getCommunication_con IS
-- "art.id AS externalId" looks suspect - should it be art.externalId ? Noted 02-Aug-2021.
   l_pool VARCHAR2(29) := 'GETCOMMUNICATION_CON' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO getCommunication_con t ( t.accountNumber , t.externalId , t.id , t.data )
   SELECT art.accountNumber , art.id AS externalId , ac.id , cont.code AS data
     FROM tcc_owner.bsbCommsArtifact@tcc art
     JOIN tcc_owner.bsbCommsArtifactContracts@tcc ac ON art.id = ac.artifactId
     JOIN tcc_owner.bsbContracts@tcc cont ON ac.contractId = cont.id
    WHERE art.status IN ( 'WITH_FULFILMENT_HOUSE' , 'GENERATED' )
      AND ROWNUM <= 200000
   ;
   COMMIT ;
   -- 29-Nov-2021 Andrew Fraser for Julian Correa, populateMessoToken.
   /*
   MERGE INTO getCommunication_con t
   USING (
      SELECT bba.accountNumber
           , MAX ( bpr.partyId ) AS partyId
           , MAX ( per.firstName ) AS firstName
           , MAX ( per.familyName ) AS familyName
           , MAX ( per.firstName ) || MAX ( per.familyName ) AS userName
           , MIN ( pti.identityId ) AS nsProfileId
        FROM ccsowner.bsbBillingAccount bba
        JOIN ccsowner.bsbCustomerRole bcr ON bba.portfolioId = bcr.portfolioId
        JOIN ccsowner.bsbPartyRole bpr ON bcr.partyroleId = bpr.id
        JOIN ccsowner.person per ON per.partyId = bpr.partyId
        LEFT OUTER JOIN ccsowner.bsbPartyToIdentity pti ON per.partyId = pti.partyId AND pti.identityType = 'NSPROFILEID'
       GROUP BY bba.accountNumber
   ) s ON ( s.accountNumber = t.accountNumber )
   WHEN MATCHED THEN UPDATE SET t.messoToken = 'T-MES-'
        || CASE WHEN sys_context ( 'userenv' , 'con_name' ) = 'CHORDO' THEN 'N01' ELSE 'N02' END
        || '-' || s.accountNumber || '-' || s.partyId || '-' || s.username || '-' || NVL ( s.nsProfileId , 'NO-NSPROFILE' )
   ;
   */
   -- 17/11/23 (RFA) - Querying CustomerTokens support table to retrieve the messoTokens
   logger.write ( 'Merging MessoTokens to data pool' ) ;
   MERGE INTO getCommunication_con t
   USING (
      SELECT /*+ parallel(16) */ ct.accountNumber, ct.messoToken
        FROM CustomerTokens ct
   ) s ON ( s.accountNumber = t.accountNumber )
   WHEN MATCHED THEN UPDATE SET t.messoToken = s.messoToken
   ;
   logger.write ( 'Data merged' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.externalId , t.id , t.data , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.externalId , s.id , s.data , s.messoToken
     FROM (
   SELECT d.accountNumber , d.externalId , d.id , d.data , d.messoToken
     FROM getCommunication_con d
    WHERE d.messoToken IS NOT NULL
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END getCommunication_con ;

PROCEDURE addresses IS
   l_pool VARCHAR2(29) := 'ADDRESSES' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO addresses t ( t.street , t.town , t.postcode , t.county )
   SELECT a.street , a.town , a.postcode , a.county
     FROM ccsowner.bsbAddress a
    WHERE a.street IS NOT NULL
      AND a.town IS NOT NULL
      AND a.postcode IS NOT NULL
      AND a.county IS NOT NULL
      AND ROWNUM <= 48000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.street , t.town , t.postcode , t.county )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.street , s.town , s.postcode , s.county
     FROM (
   SELECT d.street , d.town , d.postcode , d.county
     FROM addresses d
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 12000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END addresses ;

PROCEDURE bill_ref_account_id IS
   l_pool VARCHAR2(29) := 'BILL_REF_ACCOUNT_ID' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   FOR i IN 1..6 LOOP
      execute immediate q'[
      INSERT /*+ append */ INTO bill_ref_account_id t ( t.accountId , t.bill_ref_no , t.amount , t.db , t.seqN )
      SELECT auc.accountId , v.bill_ref_no , v.amount , 'CUS01' AS db , MOD ( ROWNUM , 1000 ) AS seqN
        FROM (
             SELECT DISTINCT m.external_id AS chd_acc_no , p.bill_ref_no , p.amount
               FROM arbor.customer_id_acct_map@cus0]' || TO_CHAR ( i ) || ' m
               JOIN arbor.payment_trans@cus0' || TO_CHAR ( i ) || ' p ON m.account_no = p.account_no
              WHERE m.external_id_type = 1
                AND p.statement_date > SYSDATE - 150
                AND p.trans_status IN ( 0 , 8 )
                AND p.bill_ref_no != 0
             ) v
        JOIN act_uk_cust auc ON auc.accountNumber = v.chd_acc_no
       WHERE ROWNUM <= 1000
      ' ;
      COMMIT ;
   END LOOP ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.billingAccountId , t.id , t.cardNumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.billingAccountId , s.id , s.cardNumber
     FROM (
   SELECT d.accountid AS billingAccountId , d.bill_ref_no AS id , d.amount AS cardNumber
     FROM bill_ref_account_id d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END bill_ref_account_id ;

PROCEDURE skyQViewingCards IS
   l_pool VARCHAR2(29) := 'SKYQVIEWINGCARDS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO skyqviewingcards ( settopboxno , cardNumber , cardSubscriberId )
    SELECT *
      FROM (
        SELECT /*+ index(ppB1, FK_BSBPORTFOLIO_PORTPROD) index(ppB2, FK_BSBPORTFOLIO_PORTPROD) index(ppC1, FK_BSBPORTFOLIO_PORTPROD) index(ppC2, FK_BSBPORTFOLIO_PORTPROD) */
                 cpeB1.settopboxndsnumber AS setTopBoxNo
               , cpeC1.cardNumber
               , bsi.CardSubscriberId
                FROM (
                      SELECT /*+ full(pp) parallel(pp, 8) materialize() */
                             count(DISTINCT pp.status), pp.portfolioId
                        FROM ccsowner.bsbPortfolioProduct pp
                       WHERE (pp.catalogueProductId = '10137' and pp.status = 'A') or (pp.catalogueProductId in ('13948','13947','15640', '15597', '15596', '15595', '15491') and PP.STATUS = 'IN') --'13950','15283'
                      GROUP BY pp.portfolioId
                      HAVING count(DISTINCT pp.status) = 2
                    ) li_port,
                     ccsowner.bsbPortfolioProduct ppB1,
                     ccsowner.bsbCustomerProductElement cpeB1,
                     ccsowner.bsbPortfolioProduct ppC1,
                     ccsowner.bsbCustomerProductElement cpeC1,
                     CCSOWNER.BSBServiceInstance bsi
               WHERE li_port.portfolioId = ppB1.Portfolioid
                 AND li_port.portfolioId = ppC1.Portfolioid
                 AND ppB1.id = cpeB1.Portfolioproductid
                 AND ppC1.id = cpeC1.Portfolioproductid
                 AND ppB1.Catalogueproductid in ('13948','13947','15640', '15597', '15596', '15595', '15491')
                 AND ppB1.Status = 'IN'
                 AND ppC1.Catalogueproductid = '10137'
                 AND ppC1.Status = 'A'
                 AND ppC1.Serviceinstanceid = PPB1.Serviceinstanceid
                 AND ppC1.Serviceinstanceid = bsi.ID
                 AND bsi.serviceInstanceType = 210
          )
    WHERE ROWNUM <= 100000
   ;
   COMMIT ;
   -- 05-May-2021 : Andrew Fraser added messotoken to skyQviewingCards for Julian Correa NFTREL-20951
 --  MERGE /*+ parallel(8) */ INTO skyQviewingCards t
 /*
   USING (
      SELECT bsi.cardSubscriberId
           , MAX ( bba.accountNumber ) KEEP ( DENSE_RANK FIRST ORDER BY bba.created ASC ) AS accountNumber  -- earliest accountNumber
           , MAX ( bpr.partyId ) AS partyId
           , MAX ( per.firstName ) || MAX ( per.familyName ) AS userName
           , MIN ( pti.identityId ) AS nsProfileId
        FROM ccsowner.bsbServiceInstance bsi
        JOIN ccsowner.bsbBillingAccount bba ON bsi.portfolioId = bba.portfolioId
        JOIN ccsowner.bsbCustomerRole bcr ON bsi.portfolioId = bcr.portfolioId
        JOIN ccsowner.bsbPartyRole bpr ON bcr.partyroleId = bpr.id
        JOIN ccsowner.person per ON per.partyId = bpr.partyId
        LEFT OUTER JOIN ccsowner.bsbPartyToIdentity pti ON per.partyId = pti.partyId AND pti.identityType = 'NSPROFILEID'
       GROUP BY bsi.cardSubscriberId
   ) s ON ( s.cardSubscriberId = t.cardSubscriberId )
   WHEN MATCHED THEN UPDATE SET t.messoToken = 'T-MES-' || CASE WHEN sys_context ( 'userenv' , 'con_name' ) = 'CHORDO' THEN 'N01' ELSE 'N02' END
      || '-' || s.accountNumber || '-' || s.partyId || '-' || s.username || '-' || NVL ( s.nsProfileId , 'NO-NSPROFILE' )
   ;
*/
   -- 17/11/23 (RFA) - Get MessoToken from CustomerTokes table
   MERGE /*+ parallel(8) */ INTO skyQviewingCards t
   USING (
      SELECT bsi.cardSubscriberId
           , MAX ( bba.accountNumber ) KEEP ( DENSE_RANK FIRST ORDER BY bba.created ASC ) AS accountNumber  -- earliest accountNumber
           , MAX ( ct.messoToken ) as messoToken
        FROM ccsowner.bsbServiceInstance bsi
        JOIN ccsowner.bsbBillingAccount bba ON bsi.portfolioId = bba.portfolioId
        JOIN dataprov.CustomerTokens ct ON ( bba.accountNumber = ct.accountNumber )
       GROUP BY bsi.cardSubscriberId
   ) s ON ( s.cardSubscriberId = t.cardSubscriberId )
   WHEN MATCHED THEN UPDATE SET t.messoToken = s.messoToken
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.settopBoxNo , t.cardNumber , t.cardSubscriberId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.settopBoxNo , s.cardNumber , s.cardSubscriberId , s.messoToken
     FROM (
   SELECT d.settopBoxNo , d.cardNumber , d.cardSubscriberId , d.messoToken
     FROM skyQViewingCards d
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 12000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END skyQViewingCards ;

PROCEDURE names IS
   l_pool VARCHAR2(29) := 'NAMES' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO names t ( t.forename , t.surname )
   SELECT per.firstName AS forename , per.familyName AS surname
     FROM ccsowner.person per
     JOIN act_uk_cust act ON per.partyId = act.partyId
    WHERE ROWNUM <= 40000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.forename , t.surname )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.forename , s.surname
     FROM (
   SELECT d.forename , d.surname
     FROM names d
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 10000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END names ;

PROCEDURE customersWithPaymentDetails IS
   l_pool VARCHAR2(29) := 'CUSTOMERSWITHPAYMENTDETAILS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO customersWithPaymentDetails t ( t.id )
   SELECT v.id
     FROM (
         SELECT /*+ parallel(p 8) full(p) parallel(ba 8) full(ba) parallel(bpp 8) full(bpp) */ ba.id
           FROM ccsowner.bsbPayment p
           JOIN ccsowner.bsbBillingAccount ba ON p.billingAccountId = ba.id
           JOIN ccsowner.bsbPortfolioProduct bpp ON bpp.portfolioId = ba.portfolioId
          WHERE ba.currencyCode = 'GBP'
            AND bpp.catalogueProductId = '10137'
            AND bpp.status = 'A'
          ORDER BY p.lastUpdate DESC
        ) v
    WHERE ROWNUM <= 200000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.id )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.id
     FROM (
   SELECT d.id
     FROM customersWithPaymentDetails d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END customersWithPaymentDetails ;

PROCEDURE customersForPairCardCallback IS
   l_pool VARCHAR2(29) := 'CUSTOMERSFORPAIRCARDCALLBACK' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO customersForPairCardCallback
      SELECT v.settopBoxNo , v.cardnumber , v.cardSubscriberId
        FROM (
        SELECT /*+ index(ppB1, FK_BSBPORTFOLIO_PORTPROD) index(ppB2, FK_BSBPORTFOLIO_PORTPROD) index(ppC1, FK_BSBPORTFOLIO_PORTPROD) index(ppC2, FK_BSBPORTFOLIO_PORTPROD) */
               cpeB1.settopboxndsnumber AS settopBoxNo
             , cpeC1.cardnumber
             , bsi.cardSubscriberId
                FROM (
                     SELECT /*+ full(pp) parallel(pp, 8) materialize() */ COUNT ( DISTINCT pp.status ) , pp.portfolioId
                        FROM ccsowner.bsbPortfolioProduct pp
                       WHERE pp.catalogueProductId IN ('10137', '11090')
                         and pp.status IN ('A', 'T', 'IN', 'AI')
                       GROUP BY pp.portfolioId
                      HAVING COUNT ( DISTINCT pp.status ) = 4
                    ) li_port,
                     ccsowner.bsbPortfolioProduct ppB1,
                     ccsowner.bsbPortfolioProduct ppB2,
                     ccsowner.bsbCustomerProductElement cpeB1,
                     ccsowner.bsbCustomerProductElement cpeB2,
                     ccsowner.bsbPortfolioProduct ppC1,
                     ccsowner.bsbPortfolioProduct ppC2,
                     ccsowner.bsbCustomerProductElement cpeC1,
                     ccsowner.bsbCustomerProductElement cpeC2,
                     CCSOWNER.BSBServiceInstance bsi
               WHERE li_port.portfolioId = ppB1.Portfolioid
                 AND li_port.portfolioId = ppB2.Portfolioid
                 AND li_port.portfolioId = ppC1.Portfolioid
                 AND li_port.portfolioId = ppC2.Portfolioid
                 AND ppB1.id = cpeB1.Portfolioproductid
                 AND ppB2.id = cpeB2.Portfolioproductid
                 AND ppC1.id = cpeC1.Portfolioproductid
                 AND ppC2.id = cpeC2.Portfolioproductid
                 AND ppB1.Catalogueproductid = '11090'
                 AND ppB1.Status = 'IN'
                 AND ppB2.Catalogueproductid = '11090'
                 AND ppB2.Status = 'AI'
                 AND ppC1.Catalogueproductid = '10137'
                 AND ppC1.Status = 'A'
                 AND ppC2.Catalogueproductid = '10137'
                 AND ppC2.Status = 'T'
                 AND ppC2.Serviceinstanceid = PPB2.Serviceinstanceid
                 AND ppC1.Serviceinstanceid = PPB1.Serviceinstanceid
                 AND ppC1.Serviceinstanceid = bsi.ID
             ) v
       WHERE ROWNUM < 800000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.settopBoxNo , t.cardnumber , t.cardSubscriberId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.settopBoxNo , s.cardnumber , s.cardSubscriberId
     FROM (
   SELECT d.settopBoxNo , d.cardnumber , d.cardSubscriberId
     FROM customersForPairCardCallback d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END customersForPairCardCallback ;

PROCEDURE caseRespTemplate IS
   l_pool VARCHAR2(29) := 'CASERESPTEMPLATE' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO caseRespTemplate ( id , sting )
   SELECT c.id , SUBSTR ( c.templatecontent , 0 , 10 ) AS sting
     FROM casemanagement.cmResponseTemplates@cse c
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.id , t.data )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.id , s.data
     FROM (
   SELECT d.id , d.sting AS data
     FROM caseRespTemplate d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END caseRespTemplate ;

PROCEDURE bb_recontracting IS
   l_pool VARCHAR2(29) := 'BB_RECONTRACTING' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO bb_recontracting t ( t.accountNumber , t.messoToken , t.partyId )
   SELECT s.accountNumber , s.messoToken , s.partyId
     FROM (
      SELECT /*+ full(ba) parallel(ba, 8) full(sa) parallel(sa, 8)
                 full(se) parallel(se, 8) full(si) parallel(si, 8)
                 full(cu) parallel(cu, 8) full(cpe) parallel(cpe, 8) */
             ba.accountNumber , cu.messoToken , cu.partyId
        FROM ccsowner.bsbSubscriptionEntitlement se
        JOIN ccsowner.bsbSubscription s ON se.subscriptionId = s.id
        JOIN ccsowner.bsbServiceInstance si ON s.serviceInstanceId = si.id
        JOIN ccsowner.bsbBillingAccount ba ON si.portfolioId = ba.portfolioId
        JOIN ccsowner.bsbPortfolioProduct pp ON ba.portfolioId = pp.portfolioId
        JOIN ccsowner.bsbCustomerProductElement cpe ON pp.id = cpe.portfolioProductId
        JOIN ccsowner.bsbSubscriptionAgreementItem sa ON ba.accountNumber = sa.agreementNumber
        JOIN customers cu ON ba.accountNumber = cu.accountNumber
       WHERE NOT EXISTS (
             SELECT /*+ full(si1) parallel(si1, 8) */ NULL
               FROM ccsowner.bsbServiceInstance si1
              WHERE si1.portfolioId = ba.portfolioId
                AND si1.serviceInstanceType IN ( 210 , 620 , 500 )
             )
         AND EXISTS (
             SELECT /*+ full(cpp) parallel(cpp, 8) full(ppp) parallel(ppp, 8)
                        full(su)  parallel(su, 8)  full(sai) parallel(sai, 8) */ NULL
               FROM ccsowner.bsbPortfolioProduct ppp
               JOIN ccsowner.bsbBillingAccount bap ON bap.portfolioId = ppp.portfolioId
               LEFT OUTER JOIN ccsowner.bsbSubscription su ON ppp.subscriptionId = su.id
               JOIN ccsowner.bsbSubscriptionAgreementItem sai ON bap.accountNumber = sai.agreementNumber
               JOIN refdatamgr.bsbCatalogueProduct cpp ON ppp.catalogueProductId = cpp.id
              WHERE bap.accountNumber = ba.accountNumber
                AND ppp.status = 'AC'
                --AND cpp.id = '12673'  -- 'Sky Broadband Unlimited'  -- 03-Aug-2022 Andrew Fraser commented out to try to get more data
                AND cpp.productDescription LIKE 'Sky Broadband%'
             )
         /*AND ba.portfolioId NOT IN (  -- 03-Aug-2022 Andrew Fraser commented out to try to get more data
             SELECT /*+ full(bpo) parallel(bpo, 8) * / bpo.portfolioId
               FROM ccsowner.bsbPortfolioOffer bpo
              WHERE bpo.status = 'ACT'
             )*/
         AND cu.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 ) -- no outstanding balance Shane Venter
         AND sa.startDate < ADD_MONTHS ( SYSDATE , -18 )
         AND si.serviceInstanceType = 400
         --AND ba.accountNumber LIKE '6%'  -- 03-Aug-2022 Andrew Fraser commented out to try to get more data
       GROUP BY ba.accountNumber , cu.messoToken , cu.partyId
       ORDER BY MAX ( sa.startDate ) DESC NULLS LAST
    ) s
    WHERE ROWNUM <= 100000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.messoToken , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.messoToken , s.partyId
     FROM (
          SELECT d.accountNumber , d.messoToken , d.partyId
            FROM bb_recontracting d
           ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_flagCustomers => TRUE ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END bb_recontracting ;

PROCEDURE teamIdForCase IS
   l_pool VARCHAR2(29) := 'TEAMIDFORCASE' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO teamIdForCase t ( t.teamName , t.string )
   SELECT c.teamName , SUBSTR ( c.teamName , 0 , 3 ) AS string
     FROM caseManagement.cmTeams@cse c
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.id , t.data )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.id , s.data
     FROM (
   SELECT SUBSTR ( d.teamName , 1 , 47 ) AS id , d.string AS data
     FROM teamIdForCase d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END teamIdForCase ;

PROCEDURE onlineProfileId_cust_search IS
   l_pool VARCHAR2(29) := 'ONLINEPROFILEID_CUST_SEARCH' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO onlineProfileId_cust_search t ( t.onlineProfileId )
   SELECT /*+ parallel(ba 8) parallel(bcr 8) parallel(bpr 8) parallel (p 8) */
          p.onlineProfileId
     from ccsowner.bsbBillingAccount ba , ccsowner.bsbcustomerrole bcr , ccsowner.bsbpartyrole bpr , ccsowner.person p
    where bpr.partyId = p.partyId
      and bpr.id = bcr.partyroleid
      and bcr.portfolioId = ba.portfolioId
      and p.onlineprofileid is not null
      and ba.accountNumber is not null
      and p.familyname not in ( 'test' , 'Test' , 'TEST' )
      and rownum <= 500000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.onlineProfileId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.onlineProfileId
     FROM (
   SELECT d.onlineProfileId
     FROM onlineProfileId_cust_search d
    ORDER BY dbms_random.value
   ) s
     WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END onlineProfileId_cust_search ;

PROCEDURE act_mob_no_pac IS
   l_pool VARCHAR2(29) := 'ACT_MOB_NO_PAC' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO act_mob_no_pac t ( t.accountNumber , t.id , t.customerProductElementId , t.combinedTelephoneNumber )
   SELECT /*+ parallel(bba 8) parallel(bpp 8) parallel(bsi 8) parallel(bcr 8) parallel(bpr 8)  parallel(bcpe 8) parallel(bt 8) parallel(btu 8)*/
        DISTINCT bba.accountNumber ,  bpr.partyId AS id , bcpe.id AS customerProductElementId , bt.combinedtelephonenumber
    FROM
      ccsowner.bsbBillingAccount bba,
      ccsowner.bsbPortfolioProduct bpp,
      ccsowner.bsbServiceInstance bsi,
      ccsowner.bsbcustomerrole bcr,
      ccsowner.bsbpartyrole bpr,
      ccsowner.bsbCustomerProductElement bcpe,
      ccsowner.bsbtelephone bt,
      ccsowner.bsbtelephoneusagerole btu
     where bba.portfolioId = bpp.portfolioId
      and bpp.serviceInstanceId=bsi.id
      and bba.serviceInstanceId=bsi.parentserviceinstanceid
      and bba.portfolioId=bcr.portfolioId
      and bcr.partyroleid = bpr.id
      and bpp.id=bcpe.portfolioProductId
      and bsi.id=btu.serviceInstanceId
      and btu.telephoneid=bt.id
      and bt.telephonenumberusecode='MSISDN'
      and btu.effectivetodate is null
      and bpp.status = 'AC'
      and bsi.serviceInstanceType = 620
      and bba.createdby != 'sky-mobile-sales'
      and bpp.catalogueProductId in ( '14207' , '14208' , '14209' )
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.id , t.productId , t.data )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.id , s.productId , s.data
     FROM (
   SELECT d.accountNumber , d.id , d.customerProductElementId AS productId , d.combinedTelephoneNumber AS data
     FROM act_mob_no_pac d
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END act_mob_no_pac ;

PROCEDURE digitalMyMessages IS
   l_pool VARCHAR2(29) := 'DIGITALMYMESSAGES' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO digitalMyMessages t ( t.accountNumber , t.partyId , t.id )
   SELECT a.accountNumber
        , a.partyId
        , a.id
     FROM tcc_owner.bsbCommsArtifact@tcc a                                                                                         
    WHERE ( a.templateId LIKE 'FU%' OR a.templateId LIKE 'CCA9002' )
      AND a.status IN ( 'SENT' , 'GENERATED' , 'WITH_FULFILMENT_HOUSE' )
      AND a.statusChangeDate >= SYSDATE - 30
      AND ROWNUM <= 100000
   ;
   COMMIT ;
   -- joining to customers does not get a hit for all rows in digitalMyMessages :(
   /*
   MERGE INTO digitalMyMessages t USING (
      SELECT per.partyId
           , MAX ( per.firstName ) || MAX ( per.familyName ) AS userName
           , MIN ( pti.identityId ) AS nsProfileId
        FROM ccsowner.person per
        LEFT OUTER JOIN ccsowner.bsbPartyToIdentity pti ON per.partyId = pti.partyId AND pti.identityType = 'NSPROFILEID'
       GROUP BY per.partyId
   ) s ON ( s.partyId = t.partyId )
   WHEN MATCHED THEN UPDATE SET t.messoToken = 'T-MES-' || CASE WHEN sys_context ( 'userenv' , 'con_name' ) = 'CHORDO' THEN 'N01' ELSE 'N02' END
                || '-' || t.accountNumber || '-' || s.partyId || '-' || s.username || '-' || NVL ( s.nsprofileId , 'NO-NSPROFILE' )
   ;
   */
   -- 17/11/23 (RFA) - Adding merging to the CustomreTokens ttable to retrieve the MessoToken
   MERGE INTO digitalMyMessages t
   USING (
      SELECT /*+ parallel(16) */ ct.accountNumber, ct.messoToken
        FROM CustomerTokens ct
   ) s ON ( s.accountNumber = t.accountNumber )
   WHEN MATCHED THEN UPDATE SET t.messoToken = s.messoToken
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.id , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.id , s.messotoken
     FROM (
   SELECT d.accountNumber , d.partyId , d.id , d.messotoken
     FROM digitalMyMessages d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END digitalMyMessages ;

PROCEDURE mobile_credit_balance IS
   l_pool VARCHAR2(29) := 'MOBILE_CREDIT_BALANCE' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO mobile_credit_balance t ( t.accountNumber , t.partyId , t.balance )
   SELECT act.accountNumber , act.partyId , da.balance
     FROM act_mobile_numbers act  -- populated in data_prep_03
     JOIN debt_amount da ON da.accountNumber = act.accountNumber  -- 25-Aug-2022
    WHERE da.balance <= 0  -- no outstanding balance
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.data )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.data
     FROM (
   SELECT d.accountNumber , d.partyId , d.balance AS data
     FROM mobile_credit_balance d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END mobile_credit_balance ;

PROCEDURE act_mobile_customers_no_debt IS
   -- https://cbsjira.bskyb.com/browse/SOIPPOD-2499
   -- 22-Sep-2022 Alex Benetatos remove restriction on in-flight visits.
   l_pool VARCHAR2(29) := 'ACT_MOBILE_CUSTOMERS_NO_DEBT' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO act_mobile_customers_no_debt t ( t.accountNumber , t.partyId , t.serviceInstanceId )
   SELECT act.accountNumber , act.partyId , act.serviceInstanceId
     FROM act_mobile_numbers act  -- populated in data_prep_03
     JOIN debt_amount da ON da.accountNumber = act.accountNumber  -- 25-Aug-2022
     JOIN customers c ON c.partyId = act.partyId
    WHERE da.balance <= 0  -- no outstanding balance
      AND act.accountNumber NOT IN (
          SELECT bba.accountNumber
            FROM ccsowner.bsbBillingAccount bba
            JOIN ccsowner.bsbMobileBlacklist bmb ON bba.id = bmb.billingAccountId
          )
      AND c.inFlightOrders = 0  -- 22-Sep-2022 Michael Santos
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   -- 30-Nov-2021 1) Andrew Fraser for Rizwan Soomra only want accounts that have a mobile number as the primary contact number SOIPPOD-2589.
   -- 30-Nov-2021 3) Andrew Fraser for Rizwan Soomra also remove if international dialing code is not '+44' SOIPPOD-2589.
   -- 06/11/23 (RFA) - Added hint to SELECT to attempt to improve the performance 
   DELETE FROM act_mobile_customers_no_debt d
    WHERE d.partyId NOT IN (
          SELECT /*+ PARALLEL(10) */ bc.partyId
            FROM ccsowner.bsbContactor bc
            JOIN ccsowner.bsbContactTelephone bct ON bct.contactorId = bc.id
            JOIN ccsowner.bsbTelephone bt ON bt.id = bct.telephoneId
           WHERE bct.deletedFlag = 0
             AND SYSDATE BETWEEN bct.effectiveFromDate AND NVL ( bct.effectiveToDate , SYSDATE + 1 )
             AND bt.telephoneNumberStatus = 'VALID'
             AND bct.mobileNumberConfirmedDate < SYSDATE
             AND bt.combinedTelephoneNumber LIKE '07%'
             AND LENGTH ( bt.combinedTelephoneNumber ) = 11
             AND bt.internationalDialingCode = '+44'
             AND bct.primaryFlag = 1
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for mobiles' ) ;
   -- 30-Nov-2021 4) Andrew Fraser for Rizwan Soomra remove accounts that have no primary email address SOIPPOD-2589.
   DELETE FROM act_mobile_customers_no_debt d
    WHERE d.partyId NOT IN (
          SELECT bc.partyId
            FROM ccsowner.bsbContactor bc
            JOIN ccsowner.bsbContactEmail bce ON bce.contactorId = bc.id
            JOIN ccsowner.bsbEmail be ON be.id = bce.emailId
           WHERE bce.deletedFlag = 0
             AND SYSDATE BETWEEN bce.effectiveFromDate AND NVL ( bce.effectiveToDate , SYSDATE + 1 )
             AND be.emailAddressStatus = 'VALID'
             AND bce.primaryFlag = 1
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for emails' ) ;
   -- 30-Nov-2021 5) Andrew Fraser for Rizwan Soomra removing accounts that have any subscriptions with a saleState that is not active (originally "exclude any customer with SOIP (even if under a different accountNumber)") SOIPPOD-2589.
   DELETE FROM act_mobile_customers_no_debt d
    WHERE EXISTS (
          SELECT NULL
            FROM ccsowner.bsbBillingAccount ba
            JOIN ccsowner.bsbServiceInstance si ON ba.portfolioId = si.portfolioId
            JOIN rcrm.service serv ON si.id = serv.billingServiceInstanceId
           WHERE d.accountNumber = ba.accountNumber
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for saleState not active' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.id )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.id
     FROM (
           SELECT d.accountNumber , d.partyId , d.serviceInstanceId AS id
             FROM act_mobile_customers_no_debt d
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END act_mobile_customers_no_debt ;

PROCEDURE digitalCurrentBbtNoDebt IS
   -- https://cbsjira.bskyb.com/browse/SOIPPOD-2498
   l_pool VARCHAR2(29) := 'DIGITALCURRENTBBTNODEBT' ;
BEGIN
   -- 14/11/23 (RFA) - Adding parallel hints to the SELECT queries for PartyID
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO digitalCurrentBbtNoDebt t ( t.accountNumber , t.partyId , t.username , t.skycesa01token , t.messotoken
      , t.ssotoken , t.firstName , t.familyName , t.emailAddress
      )
   SELECT c.accountNumber
        , c.partyId
        , c.username
        , c.skycesa01token
        , c.messotoken
        , c.ssotoken
        , c.firstName
        , c.familyName
        , c.emailAddress  -- needs added to main sql.
     FROM customers c
    WHERE c.bband = 1
      AND c.talk = 1
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND c.countryCode = 'GBR'
      AND c.pool IS NULL  -- optional
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 ) -- no outstanding balance
      --AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= -80 ) -- Rizwan 07-Dec-2021
      AND ROWNUM <= 400000
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) ) ;
   COMMIT ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   -- Exclude in-flight orders:
   DELETE FROM digitalCurrentBbtNoDebt t
    WHERE EXISTS (
          SELECT NULL 
            FROM ccsowner.bsbBillingAccount ba
            JOIN ccsowner.bsbAddressUsageRole aur ON aur.serviceInstanceId = ba.serviceInstanceId
            JOIN ccsowner.bsbVisitRequirement vr ON vr.installationAddressRoleId = aur.id
           WHERE ba.accountNumber = t.accountNumber
             AND vr.statusCode NOT IN ( 'CP' , 'CN' )  -- Complete/Cancelled are ok. UB is definitely a problem, others might be ok depending on visit_date, list is:
             -- select code,codeDesc from refdatamgr.picklist where codeGroup = 'VisitRequirementStatus' order by 1 ;
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) ) ;
   -- 30-Nov-2021 1) Andrew Fraser for Rizwan Soomra only want accounts that have a mobile number as the primary contact number SOIPPOD-2589.
   -- 30-Nov-2021 3) Andrew Fraser for Rizwan Soomra also remove if international dialing code is not '+44' SOIPPOD-2589.
   DELETE FROM digitalCurrentBbtNoDebt d
    WHERE d.partyId NOT IN (
          SELECT /*+ parallel(8) */ bc.partyId
            FROM ccsowner.bsbContactor bc
            JOIN ccsowner.bsbContactTelephone bct ON bct.contactorId = bc.id
            JOIN ccsowner.bsbTelephone bt ON bt.id = bct.telephoneId
           WHERE bct.deletedFlag = 0
             AND SYSDATE BETWEEN bct.effectiveFromDate AND NVL ( bct.effectiveToDate , SYSDATE + 1 )
             AND bt.telephoneNumberStatus = 'VALID'
             AND bct.mobileNumberConfirmedDate < SYSDATE
             AND bt.combinedTelephoneNumber LIKE '07%'
             AND LENGTH ( bt.combinedTelephoneNumber ) = 11
             AND bt.internationalDialingCode = '+44'
             AND bct.primaryFlag = 1
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) ) ;
   -- 30-Nov-2021 4) Andrew Fraser for Rizwan Soomra remove accounts that have no primary email address SOIPPOD-2589.
   DELETE FROM digitalCurrentBbtNoDebt d
    WHERE d.partyId NOT IN (
          SELECT /*+ parallel(8) */ bc.partyId
            FROM ccsowner.bsbContactor bc
            JOIN ccsowner.bsbContactEmail bce ON bce.contactorId = bc.id
            JOIN ccsowner.bsbEmail be ON be.id = bce.emailId
           WHERE bce.deletedFlag = 0
             AND SYSDATE BETWEEN bce.effectiveFromDate AND NVL ( bce.effectiveToDate , SYSDATE + 1 )
             AND be.emailAddressStatus = 'VALID'
             AND bce.primaryFlag = 1
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) ) ;
   -- 30-Nov-2021 5) Andrew Fraser for Rizwan Soomra removing accounts that have any subscriptions with a saleState that is not active (originally "exclude any customer with SOIP (even if under a different accountNumber)") SOIPPOD-2589.
   DELETE FROM digitalCurrentBbtNoDebt d
    WHERE EXISTS (
          SELECT NULL
            FROM ccsowner.bsbBillingAccount ba
            JOIN ccsowner.bsbServiceInstance si ON ba.portfolioId = si.portfolioId
            JOIN rcrm.service serv ON si.id = serv.billingServiceInstanceId
           WHERE d.accountNumber = ba.accountNumber
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) ) ;
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
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 300000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   --SOIPPOD-2672-- execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END digitalCurrentBbtNoDebt ;

PROCEDURE soipSignedPlansNoBurn IS
   -- 11-May-2021 Alex Hyslop create a copy of soipSignedPlans which cycles instead of burns data, for Deepa.
   l_pool VARCHAR2(29) := 'SOIPSIGNEDPLANSNOBURN' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.data , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.data , s.partyId , s.messoToken
     FROM (
   SELECT d.external_id AS accountNumber , d.agreement_ref AS data , d.partyId , d.messoToken
     FROM dataprov.soipSignedPlans d   -- populated in parent pool data_prep_06.soipSignedPlans
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   --execute immediate 'truncate table soipSignedPlans' ;
   logger.write ( 'complete' ) ;
END soipSignedPlansNoBurn ;

PROCEDURE mobile_device_part_no IS
   l_pool VARCHAR2(29) := 'MOBILE_DEVICE_PART_NO' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO mobile_device_part_no t ( t.id , t.deliveryProductCode )
   SELECT DISTINCT cp.id , pe.deliveryProductCode
     FROM refdatamgr.bsbCatalogueProduct cp
     JOIN refdatamgr.bsbProductElement pe ON cp.productDescription = pe.description
    WHERE pe.productElementType = 'MD'
      AND cp.salesStatus = 'SA'
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.productId , t.deliveryProductCode )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.productId , s.deliveryProductCode
     FROM (
   SELECT d.id AS productId , d.deliveryProductCode
     FROM dataprov.mobile_device_part_no d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END mobile_device_part_no ;

END data_prep_05 ;
/
