CREATE OR REPLACE PACKAGE data_prep_06 AS
PROCEDURE act_cust_av_bb ;
PROCEDURE act_cust_av_talk ;
PROCEDURE act_cust_av_dtv ;
PROCEDURE act_cust_ppv_with_pin ;
PROCEDURE pendingSimActivation ;
PROCEDURE migratedSoipCustomer ;
PROCEDURE existingSoipCcaNotSigned ;
PROCEDURE soipEricaInFlightVisit ;
PROCEDURE soipEricaInFlightVisit_bband;
PROCEDURE unifiedUsers ;
PROCEDURE soipDispatchedProducts ;
PROCEDURE soipUndeliveredProducts ;
PROCEDURE soipDevicesForActivation ;
PROCEDURE soipSignedPlans ;
PROCEDURE soipPendingActive ;
PROCEDURE soipOnBoarding ;
PROCEDURE soipDeliveredProducts ;
PROCEDURE soipActiveSubscription ;
PROCEDURE soipActiveNoUlm ;
PROCEDURE soipActiveSubNetflix ;
PROCEDURE soipActiveSubNoNetflix ;
PROCEDURE soipActiveSubNoSport ;
PROCEDURE soipActiveSubWithSkyKids ;
PROCEDURE soipActiveSubNoSkyKids ;
PROCEDURE soipUlm ;
PROCEDURE soipHomeSetup ;
PROCEDURE SOIPCCAUNSIGNEDPLANS ;
----
PROCEDURE soipDev4ActBen ;
PROCEDURE soipUndeliProdBen ;
PROCEDURE soipDispatchedProductsBen ;

END data_prep_06 ;
/


CREATE OR REPLACE PACKAGE BODY data_prep_06 AS

PROCEDURE act_cust_av_bb IS
-- staging table for act_cust_ppv_with_pin, which is only used every few months by Archana Burla.
   l_pool VARCHAR2(29) := 'ACT_CUST_AV_BB' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO act_cust_av_bb t ( t.partyId , t.accountNumber , t.bb_serviceId ) 
   select /*+ full(bpp) parallel(bpp 16) full(bcpe) parallel(bcpe 16)  full(bsi) parallel(bsi 16)  full(bsr) parallel(bsr 16)
              full(bba) parallel(bba 16) full(bcr) parallel(bcr 16)  full(bpr) parallel(bpr 16)  full(bpe) parallel(bpe 16)
               pq_distribute(bpp hash hash) pq_distribute(bcpe hash hash) pq_distribute(bsi hash hash)
              pq_distribute(bsr hash hash) pq_distribute(bba hash hash)
                pq_distribute(bcr hash hash) pq_distribute(bpr hash hash) pq_distribute(bpe hash hash) */
          distinct bpr.partyid
       , bba.accountnumber
       , BPE.SERVICENUMBER as BB_ServiceID
  from ccsowner.bsbserviceinstance bsi,
     ccsowner.bsbsubscription bsr,
     ccsowner.bsbbillingaccount bba,
     ccsowner.bsbportfolioproduct bpp,
     ccsowner.bsbCustomerProductElement   bcpe,
     ccsowner.bsbcustomerrole bcr,
     ccsowner.bsbpartyrole bpr,
     ccsowner.bsbbroadbandcustprodelement bpe
  where bsi.id = bsr.SERVICEINSTANCEID
  and bba.portfolioid                = bcr.portfolioid
  and bcr.partyroleid                = bpr.id
  and bsi.PARENTSERVICEINSTANCEID = bba.SERVICEINSTANCEID
  and bpp.portfolioid=bba.portfolioid
   AND bcpe.portfolioproductid = bpp.id
   and bpe.lineproductelementid = bcpe.id
   and bsr.STATUS = 'AC'
   and BPE.SERVICENUMBER is NOT NULL
   and bpp.catalogueproductid = '13661'
   and bpp.status = 'AC'
   ;
   COMMIT ;
   logger.write ( 'complete' ) ;
END act_cust_av_bb ;

PROCEDURE act_cust_av_talk IS
-- staging table for act_cust_ppv_with_pin, which is only used every few months by Archana Burla.
   l_pool VARCHAR2(29) := 'ACT_CUST_AV_TALK' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO act_cust_av_talk t ( t.partyId , t.accountNumber , t.talk_serviceId ) 
   select /*+ full(bpp) parallel(bpp 16) full(bcpe) parallel(bcpe 16)  full(bsi) parallel(bsi 16)  full(bsr) parallel(bsr 16)
              full(bba) parallel(bba 16) full(bcp) parallel(bcp 16) full(bcr) parallel(bcr 16) full(bpr) parallel(bpr 16) full(tpe) parallel(tpe 16)
              pq_distribute(bpp hash hash) pq_distribute(bcpe hash hash) pq_distribute(bsi hash hash)
              pq_distribute(bsr hash hash) pq_distribute(bba hash hash)
               pq_distribute(bcp hash hash) pq_distribute(bcr hash hash)
                pq_distribute(bpr hash hash) pq_distribute(tpe hash hash) */
          distinct bpr.partyid
        , bba.accountnumber
        , tpe.serviceid as Talk_ServiceID
     from ccsowner.bsbserviceinstance bsi,
           ccsowner.bsbsubscription bsr,
           ccsowner.bsbbillingaccount bba,
           ccsowner.bsbportfolioproduct bpp,
           ccsowner.bsbCustomerProductElement   bcpe,
           ccsowner.bsbcustomerrole bcr,
           ccsowner.bsbpartyrole bpr,
           ccsowner.bsbtelephonycustprodelement tpe
  where bsi.id = bsr.SERVICEINSTANCEID
   and bba.portfolioid                = bcr.portfolioid
   and bcr.partyroleid                = bpr.id
   and bsi.PARENTSERVICEINSTANCEID = bba.SERVICEINSTANCEID
   and bpp.portfolioid=bba.portfolioid
   AND bcpe.portfolioproductid = bpp.id
   and tpe.telephonyproductelementid = bcpe.id
   and bsr.STATUS = 'AC'
   and TPE.SERVICEID is NOT NULL
   and bpp.catalogueproductid = '12721'
   and bpp.status = 'A'
   ;
   COMMIT ;
   logger.write ( 'complete' ) ;
END act_cust_av_talk ;

PROCEDURE act_cust_av_dtv IS
-- only used every few months by Archana Burla.
   l_pool VARCHAR2(29) := 'ACT_CUST_AV_DTV' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO act_cust_av_dtv t ( t.accountNumber , t.partyId , t.bb_serviceId , t.cardSubscriberId , t.serviceInstanceId )
   select /*+ full(bba) parallel(bba 16) full(bcr) parallel(bcr 16) full(bpr) parallel(bpr 16) full(bsi) parallel(bsi 16)
              full(bpp) parallel(bpp 16) full(abb) parallel(abb 16) full(bsr) parallel(bsr 16) */
      distinct bba.accountnumber , bpr.partyid , abb.bb_serviceid
           , bsi.cardSubscriberId
           , bsi.id AS serviceInstanceId
        from ccsowner.bsbbillingaccount   bba,
             ccsowner.bsbcustomerrole     bcr,
             ccsowner.bsbpartyrole        bpr,
             ccsowner.bsbserviceinstance  bsi,
             ccsowner.bsbportfolioproduct bpp,
             act_cust_av_bb abb  -- populated by separate procedure in this same package
       where bba.portfolioid = bcr.portfolioid
         and bcr.partyroleid = bpr.id
         and bsi.serviceinstancetype = 210
         and bsi.id = bpp.serviceinstanceid
         and bpp.portfolioid = bba.portfolioid
         and bpp.catalogueproductid = '10137'
         and bpp.status = 'A'
         and bba.accountnumber = abb.accountnumber
         and exists (select /*+ full(ata) parallel(ata 16) */
               ata.accountnumber
                from act_cust_av_talk ata  -- populated by separate procedure in this same package
               where ata.accountnumber = bba.accountnumber)
         and not exists (select /*+ full(nonsky) parallel(nonsky 16) */
               nonsky.portfolioid
                from ccsowner.bsbportfolioproduct nonsky
               where nonsky.catalogueproductid in ('13686','10098','10102','13927','13968')
                 and nonsky.portfolioid = bba.portfolioid)
         and not exists (select /*+ full(bsi2) parallel(bsi2 16) full(bpp2) parallel(bpp2 16) */
               bpp2.portfolioid
                from ccsowner.bsbserviceinstance  bsi2,
                     ccsowner.bsbportfolioproduct bpp2
               where bsi2.id = bpp2.serviceinstanceid
                 and bsi.serviceinstancetype = 220
                 and bpp2.portfolioid = bpp.portfolioid)
         and exists (select /*+ full(bpp3) parallel(bpp3 16) */
               bpp3.portfolioid
                from ccsowner.bsbportfolioproduct bpp3
               where bpp3.catalogueproductid in
                     ('11090',
                      '13522',
                      '23540',
                      '13425',
                      '13646',
                      '13653',
                      '13641',
                      '13787',
                      '13788',
                      '13791')
                 and bpp3.portfolioid = bpp.portfolioid)
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.bb_serviceId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.bb_serviceId
     FROM (
   SELECT d.accountNumber , d.partyId , d.bb_serviceId
     FROM act_cust_av_dtv d
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 80000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END act_cust_av_dtv ;

PROCEDURE act_cust_ppv_with_pin IS
-- only used every few months by Archana Burla.
   l_pool VARCHAR2(29) := 'ACT_CUST_PPV_WITH_PIN' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO act_cust_ppv_with_pin t ( t.portfolioId , t.accountNumber , t.cardNumber , t.cicPin , t.partyId )
   SELECT /*+ leading(bci) full(bpp) parallel(bpp, 16) full(bcpe) parallel(bcpe, 16) parallel(bsi 8) parallel(auc 4) */
          auc.portfolioid , auc.accountnumber ,  bcpe.cardnumber , bci.cicpin , auc.partyid
     FROM ccsowner.bsbcicinformation bci
     JOIN ccsowner.bsbserviceinstance bsi ON bsi.id = bci.primaryserviceinstanceid
     JOIN ccsowner.bsbportfolioproduct bpp ON bpp.serviceinstanceid = bsi.id
     JOIN act_uk_cust auc ON bpp.portfolioid = auc.portfolioid
     JOIN ccsowner.bsbcustomerproductelement bcpe ON bpp.id = bcpe.portfolioproductid
    WHERE bcpe.customerproductelementtype = 'VC'
      AND bcpe.status = 'A'
   ;
   COMMIT ;
   -- keep only one record for each portfolioId
   DELETE FROM act_cust_ppv_with_pin t
    WHERE ROWID IN (
            SELECT i.rid
              FROM (
                    SELECT ROWID AS rid
                         , ROW_NUMBER() OVER ( PARTITION BY d.portfolioid ORDER BY ROWID ) AS rn
                      FROM act_cust_ppv_with_pin d  -- place the column with duplicate values
                   ) i
             WHERE i.rn != 1
          )
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.portfolio , t.cardNumber , t.pin )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.portfolio , s.cardNumber , s.pin
     FROM (
   SELECT d.accountNumber , d.partyId , d.portfolioId AS portfolio , d.cardNumber , d.cicPin AS pin
     FROM act_cust_ppv_with_pin d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END act_cust_ppv_with_pin ;

PROCEDURE pendingSimActivation IS
-- 21/11/23 (RFA) - Use table CUSTOMERTOKENS to retrieve the MessoToken
   l_pool VARCHAR2(29) := 'PENDINGSIMACTIVATION' ;
BEGIN
   -- 21/11/23 (RFA) - MessoToken merging
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ||' reuse storage';
   INSERT /*+ append */ INTO pendingSimActivation t ( t.accountNumber , t.portfolioId, t.partyId, t.messoToken )
   SELECT DISTINCT ba.accountNumber , ba.portfolioId, ct.partyID, ct.MessoToken
     FROM ccsowner.bsbBillingAccount ba
     JOIN rcrm.service serv ON ba.serviceInstanceId = serv.billingServiceInstanceId
     JOIN ccsowner.bsbPortfolioProduct pp ON ba.portfolioId = pp.portfolioId
     JOIN refdatamgr.bsbCatalogueProduct cp ON pp.catalogueProductId = cp.id
     LEFT JOIN dataprov.customertokens ct ON ba.accountNumber = ct.accountNumber
    WHERE pp.status = 'OIP'  -- Order In Progress
      AND pp.catalogueProductId = '14210'  -- 'Unlimited Calls and Texts'
   ;
   COMMIT ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;

   execute immediate 'truncate table fps_pending_credit_agreement reuse storage' ;
   INSERT /*+ append */ INTO fps_pending_credit_agreement t
   SELECT DISTINCT a.partyId
     FROM rful.action@fps a
    WHERE a.actionType = 'CREATE_SKY_CREDIT_AGREEMENT'
      AND a.state = 'PENDING'
   ;
   COMMIT ;
   DELETE FROM pendingSimActivation t
    WHERE EXISTS (
          SELECT NULL
            FROM fps_pending_credit_agreement a
           WHERE a.partyId = t.partyId  -- join
          )
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken
     FROM (
   SELECT d.accountNumber , d.partyId , d.messoToken
     FROM pendingSimActivation d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ||' reuse storage';
   logger.write ( 'complete' ) ;
END pendingSimActivation ;

PROCEDURE migratedSoipCustomer IS
-- 21/11/23 (RFA) - Use table CUSTOMERTOKENS to retrieve the MessoToken
   l_pool VARCHAR2(29) := 'MIGRATEDSOIPCUSTOMER' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO migratedSoipCustomer t ( t.accountNumber , t.partyId , t.portfolioId, t.messoToken )
   SELECT s.accountNumber , s.partyId , s.portfolioId, s.messoToken
     FROM (
           SELECT DISTINCT ba.accountNumber , pr.partyId , ba.portfolioId, ct.messoToken
             FROM rcrm.service serv
             JOIN ccsowner.bsbBillingAccount ba ON ba.serviceInstanceId = serv.billingServiceInstanceId
             JOIN ccsowner.bsbCustomerRole cr ON cr.portfolioId = ba.portfolioId
             JOIN ccsowner.bsbPartyRole pr ON pr.id = cr.partyRoleId
             LEFT JOIN dataprov.customertokens ct ON ba.accountNumber = ct.accountNumber
            ORDER BY dbms_random.value
          ) s   
    WHERE ROWNUM <= 25000
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   DELETE FROM migratedSoipCustomer t
    WHERE EXISTS (
          SELECT NULL
            FROM fps_pending_credit_agreement a
           WHERE a.partyId = t.partyId  -- join
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for fps_pending_credit_agreement' ) ;
   -- Check to see they are/were non-soip - LWS test script will prompt them with a reminder to return their e.g. Sky Q box.
   -- An alternative method might be to look for soip product 14223 "Returns Packaging"
   -- Done separately from main insert for faster performance - test gave 129s compared to 697 secs in single statement.
   DELETE FROM migratedSoipCustomer t
    WHERE NOT EXISTS (
          SELECT NULL
            FROM ccsowner.bsbPortfolioProduct pp
            JOIN refdatamgr.bsbCatalogueProduct cp ON pp.catalogueProductId = cp.id
           WHERE pp.portfolioId = t.portfolioId
             AND LENGTH ( cp.id ) = 5  -- exclude pay per view events
             AND cp.productDescription LIKE 'Sky Q%'
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for are/were non-soip' ) ;

   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken
     FROM (
           SELECT d.accountNumber , d.partyId , d.messoToken
             FROM migratedSoipCustomer d
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 20000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END migratedSoipCustomer ;

PROCEDURE existingSoipCcaNotSigned IS
-- 21/11/23 (RFA) - Use table CUSTOMERTOKENS to retrieve the MessoToken
   l_pool VARCHAR2(29) := 'EXISTINGSOIPCCANOTSIGNED' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO existingSoipCcaNotSigned t ( t.accountNumber, t.partyId, t.messoToken )
    SELECT DISTINCT ba.accountNumber, ct.partyID, ct.messoToken
     FROM rcrm.product prod
     JOIN rcrm.productPricingItemLink ppil ON ppil.productId = prod.id
     JOIN rcrm.pricingItem pi ON pi.id = ppil.pricingItemId
     JOIN rcrm.ccaPricingItem cpi ON cpi.ccaId = pi.ccaId
     JOIN rcrm.service serv ON serv.id = prod.serviceId
     JOIN ccsowner.bsbBillingAccount ba ON ba.serviceInstanceId = serv.billingServiceInstanceId
     LEFT JOIN dataprov.customertokens ct ON ba.accountNumber = ct.accountNumber
    WHERE prod.status IN ( 'PA' , 'PREACTIVE' )
      AND prod.suid IN ( 'LLAMA_SMALL' , 'LLAMA_MEDIUM' , 'LLAMA_LARGE' )
      AND cpi.signedDate IS NULL
   ;
   COMMIT ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   DELETE FROM existingSoipCcaNotSigned t
    WHERE EXISTS (
          SELECT NULL
            FROM fps_pending_credit_agreement a
           WHERE a.partyId = t.partyId  -- join
          )
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken
     FROM (
   SELECT d.accountNumber , d.partyId , d.messoToken
     FROM existingSoipCcaNotSigned d
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END existingSoipCcaNotSigned ;

PROCEDURE soipEricaInFlightVisit IS
-- 31-Aug-2021 Andrew Fraser for Amit More - allow all customers, no longer restrict to soip customers only.
-- 21/11/23 (RFA) - Use table CUSTOMERTOKENS to retrieve the MessoToken
   l_pool VARCHAR2(29) := 'SOIPERICAINFLIGHTVISIT' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipEricaInFlightVisit t ( t.accountNumber , t.partyId, t.messoToken )
   SELECT DISTINCT ba.accountNumber , pr.partyId, ct.messoToken
     FROM ccsowner.bsbPartyRole pr
     JOIN ccsowner.bsbCustomerRole cr ON pr.id = cr.partyRoleId
     JOIN ccsowner.bsbBillingAccount ba ON ba.portfolioId = cr.portfolioId
     JOIN ccsowner.bsbAddressUsageRole aur ON aur.serviceInstanceId = ba.serviceInstanceId
     JOIN ccsowner.bsbVisitRequirement vr ON vr.installationAddressRoleId = aur.id
     LEFT JOIN dataprov.customertokens ct ON ba.accountNumber = ct.accountNumber
    WHERE vr.visitDate > TRUNC ( SYSDATE ) + 1
      AND vr.statusCode = 'BK'  -- Booked
   ;
   COMMIT ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   DELETE FROM soipEricaInFlightVisit t
    WHERE EXISTS (
          SELECT NULL
            FROM fps_pending_credit_agreement a
           WHERE a.partyId = t.partyId  -- join
          )
   ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken
     FROM (
   SELECT d.accountNumber , d.partyId , d.messoToken
     FROM soipEricaInFlightVisit d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipEricaInFlightVisit ;

PROCEDURE soipEricaInFlightVisit_bband IS
-- NFTREL-22309 25-Sep-2023 Charles Ajah for Deepa Satam - The data for the visit needs to be booked via source system Erica(as this system calls the new stack like SPS).
-- 21/11/23 (RFA) - Use table CUSTOMERTOKENS to retrieve the MessoToken
   l_pool VARCHAR2(29) := 'SOIPERICAINFLIGHTVISIT_BBAND' ;
BEGIN
   logger.write ( 'begin' ) ;
   logger.write ( 'Truncating table '||l_pool ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'Completed Truncating table '||l_pool ) ;
   logger.write ( 'Loading data into table '||l_pool ) ;
   INSERT /*+ append */ INTO soipEricaInFlightVisit_bband t ( t.accountNumber , t.partyId, t.messotoken )
   SELECT  ba.accountnumber,
            pr.partyid,
            MAX( ct.messoToken ) as messoToken
   FROM ccsowner.bsbPartyRole pr
     JOIN ccsowner.bsbCustomerRole cr ON pr.id = cr.partyRoleId
     JOIN ccsowner.bsbBillingAccount ba ON ba.portfolioId = cr.portfolioId
     JOIN ccsowner.bsbAddressUsageRole aur ON aur.serviceInstanceId = ba.serviceInstanceId
     JOIN ccsowner.bsbServiceInstance si ON si.parentServiceInstanceId = ba.serviceInstanceId
     JOIN ccsowner.bsbVisitRequirement vr ON vr.installationAddressRoleId = aur.id
     JOIN ccsowner.visit v ON  vr.id = v.visitrequirementid       --joining to the visit table, as every visit entry in the table was booked using the new erica app
     LEFT JOIN dataprov.customertokens ct ON ba.accountNumber = ct.accountNumber
   WHERE v.providertype='SKY'
        AND vr.jobtype='SR'
        AND (vr.jobdescription LIKE 'Service Call Visit : On-site - Out of warranty Sky Hub%'
        OR  vr.jobdescription LIKE 'Service Call Visit : On-site - In Warranty Sky Hub%'
        OR  vr.jobdescription LIKE 'Service Call Visit : On-site - Wifi Guarantee Sky Hub%'
        OR  vr.jobdescription='Service Call Visit : On-site - Out of warranty Other Router'
        OR  vr.jobdescription='Service Call Visit : On-site - In Warranty Other Router'
        OR  vr.jobdescription LIKE  'Service Call Visit : On-site - Out of warranty Broadband%'
        OR  vr.jobdescription LIKE 'Service Call Visit : On-site - In Warranty Broadband%'
        OR  vr.jobdescription LIKE 'Service Call Visit (Broadband Optimisation visit)%'
        )
        AND vr.statusCode IN ('BK','CF')
        GROUP BY ba.accountnumber , pr.partyId
        ;
   COMMIT ;
   logger.write ( 'Completed loading data into table '||l_pool ) ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;

   DELETE FROM soipEricaInFlightVisit_bband t
    WHERE EXISTS (
          SELECT NULL
            FROM fps_pending_credit_agreement a
           WHERE a.partyId = t.partyId  -- join
          )
   ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;

   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken
     FROM (
   SELECT d.accountNumber , d.partyId , d.messoToken
     FROM soipEricaInFlightVisit_bband d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipEricaInFlightVisit_bband ;


PROCEDURE unifiedUsers IS
-- 27-May-2021 Andrew Fraser initial creation for Nick Patte SOIPPOD-1871
   l_pool VARCHAR2(29) := 'UNIFIEDUSERS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO unifiedUsers t ( t.user_id , t.group_id , t.nsProfileId , t.profileId )
   SELECT a.user_id , a.group_id , b.nsProfileId , b.profileId
     FROM mint_platform.mt_group2user@ulm a
     JOIN mint_platform.ext_user_atr@ulm b on a.user_id = b.entity_id
    WHERE a.flags = '2076'   -- 2076 is the main user of each (household) group
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.user_id , t.group_id , t.nsProfileId , t.profileId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.user_id , s.group_id , s.nsProfileId , s.profileId
     FROM (
   SELECT d.user_id , d.group_id , d.nsProfileId , d.profileId
     FROM unifiedUsers d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 300000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END unifiedUsers ;

PROCEDURE soipDispatchedProducts IS
-- David Dryburgh SOIPPOD-1930
   l_pool VARCHAR2(29) := 'SOIPDISPATCHEDPRODUCTS' ;
BEGIN
   logger.write ( 'begin' ) ;
   -- staging table hosted in fps for performance during the merge statement, to use index on partyId
   DELETE FROM soipDispatchedProducts@fps ;
   commit;
   INSERT INTO soipDispatchedProducts@fps ( accountNumber , productId , partyId )
   SELECT ba.accountNumber
        , p.id AS productId
        , bpr.partyId
     FROM rcrm.product p
     JOIN rcrm.service s ON p.serviceId = s.id
     JOIN ccsowner.bsbBillingaccount ba ON ba.serviceInstanceId = s.billingServiceInstanceId
     JOIN rcrm.hardwareProdTechElement hp ON hp.productId = p.id
     JOIN ccsowner.bsbCustomerRole bcr ON ba.portfolioId = bcr.portfolioId
     JOIN ccsowner.bsbPartyRole bpr ON bcr.partyRoleId = bpr.id
    WHERE p.eventCode = 'DISPATCHED'
      AND p.suid IN ( 'LLAMA_SMALL' , 'LLAMA_MEDIUM' , 'LLAMA_LARGE' )
      --AND hp.serialNumber IS NOT NULL
      AND hp.serialNumber LIKE 'TV11SKA%'
   ;
   commit;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   -- merge statement held in fps for performance and to workaround "ORA-22992: cannot use LOB locators selected from remote tables"
   data_prep_fps.soipDispatchedProducts@fps ;
   commit;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.fulfilmentReferenceId , t.productId , t.accountNumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.fulfilmentReferenceId , s.productId , s.accountNumber , s.partyId
     FROM (
           SELECT d.fulfilmentReferenceId , d.productId , d.accountNumber , d.partyId
             FROM soipDispatchedProducts@fps d
            WHERE d.fulfilmentReferenceId IS NOT NULL
            ORDER BY dbms_random.value  -- before 03-Sep-2021 was instead "ORDER BY d.created"
          ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END soipDispatchedProducts ;

PROCEDURE soipUndeliveredProducts IS
   -- 21/11/23 (RFA) - Use table CUSTOMERTOKENS to retrieve the MessoToken
   -- 11/12/23 (RFA) - Added hints to query
   l_pool VARCHAR2(29) := 'SOIPUNDELIVEREDPRODUCTS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipUndeliveredProducts t ( t.accountNumber , t.productId , t.fulfilmentReferenceId, t.partyId, t.messoToken )
   SELECT  /*+ leading(pr) use_hash(pr hp)
              parallel(pr 8) pq_distribute(pr hash hash)
              parallel(se 8) pq_distribute(se hash hash)
              parallel(ba 8) pq_distribute(ba hash hash) 
              parallel(hp 8) pq_distribute(hp hash hash) 
              parallel(up 8) pq_distribute(up hash hash) 
              parallel(ct 8) pq_distribute(ct hash hash) 
           */ DISTINCT ba.accountnumber , pr.id AS productId , up.fulfilmentReferenceId, ct.partyId, ct.messoToken
     FROM rcrm.product pr
     JOIN rcrm.service se ON pr.serviceId = se.id
     JOIN ccsowner.bsbBillingAccount ba ON ba.serviceInstanceId = se.billingServiceInstanceId
     LEFT OUTER JOIN rcrm.hardwareProdTechElement hp ON hp.productId = pr.id
     JOIN dp_soipUndeliveredProducts_v@fps up ON ba.accountNumber = up.accountNumber
     LEFT JOIN dataprov.customertokens ct ON ba.accountNumber = ct.accountNumber
    WHERE pr.suid IN ( 'LLAMA_SMALL' , 'LLAMA_MEDIUM' , 'LLAMA_LARGE' )  -- changed for Alex Benatatos 23/02/2022
      AND pr.eventCode IN ( 'ACCEPTED_BY_FULFILMENT_HOUSE' , 'SENT_TO_FULFILMENT_HOUSE' )
      AND hp.serialNumber IS NULL
      AND ROWNUM <= 100000
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;

   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.productId , t.fulfilmentReferenceId
        , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.productId , s.fulfilmentReferenceId
        , s.partyId , s.messoToken
     FROM (
           SELECT d.accountNumber , d.productId , d.fulfilmentReferenceId , d.partyId , d.messoToken
             FROM soipUndeliveredProducts d
            ORDER BY dbms_random.value
          ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipUndeliveredProducts ;

PROCEDURE soipDevicesForActivation IS
   -- 02-Sep-2021 Andrew Fraser for Alex Benetatos must have sourceReference, requires joining to deviceChangeHistory table.
   -- 21-Jul-2021 Andrew Fraser for Alex Benetatos restrict to NFT sold products (TV11SKA serialNumbers)
   -- 21-Jul-2021 Andrew Fraser for Amit More remove any customers already ACtivated
   -- 15-Apr-2021 : Alex Hyslop Initial Creation based on SQL from David Dryburgh under SOIPPOD-1598
   l_pool VARCHAR2(29) := 'SOIPDEVICESFORACTIVATION' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO SOIPDEVICESFORACTIVATION t ( t.accountNumber , t.x1AccountId , t.serialNumber , t.productId )
   SELECT DISTINCT ba.accountNumber , pte.x1AccountId , hp.serialNumber , pr.id AS productId
     FROM rcrm.product pr
     JOIN rcrm.service sr ON pr.serviceid = sr.id
     JOIN ccsowner.bsbBillingAccount ba ON ba.serviceInstanceId = sr.billingServiceInstanceId
     JOIN ccsowner.portfolioTechElement pte ON pte.portfolioId = ba.portfolioId
     JOIN rcrm.hardwareProdTechElement hp ON hp.productId = pr.id
     JOIN rcrm.deviceRegistry dr ON dr.publicDeviceId = hp.serialNumber AND dr.accountNumber = ba.accountNumber
     JOIN rcrm.deviceHistoryDetail dhd ON dr.id = dhd.deviceRegistryId
     JOIN rcrm.deviceChangeHistory dch ON dr.accountNumber = dch.accountNumber AND dhd.deviceChangeId = dch.id
    WHERE pr.eventCode =  'DELIVERED' --'DISPATCHED'
      --AND hp.serialNumber IS NOT NULL   -- superflous if join stays at "dr.publicDeviceId = hp.serialNumber"
      AND dch.sourceReference IS NOT NULL
      and hp.serialNumber LIKE 'TV11SKA%'
   ;
   COMMIT ;
   -- 21-Jul-2021 Andrew Fraser for Alex Benetatos restrict to NFT sold products (TV11SKA serialNumbers)
   -- removed below as code added to main query
   --DELETE FROM soipDevicesForActivation t
   -- WHERE t.serialNumber NOT LIKE 'TV11SKA%';

   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   -- 21-Jul-2021 Andrew Fraser for Amit More remove any customers already ACtivated
   DELETE FROM SOIPDEVICESFORACTIVATION t
    WHERE t.accountNumber IN (
          SELECT ba.accountNumber
            FROM rcrm.product p
            JOIN rcrm.service s ON p.serviceid = s.id
            JOIN ccsowner.bsbbillingaccount ba ON ba.serviceInstanceId = s.billingServiceInstanceId
           WHERE p.suid in ('SOIP_TV_SKY_SIGNATURE', 'SKY_GLASS_ULTIMATE_TV_ROI')
           --WHERE p.suid = 'SOIP_TV_SKY_SIGNATURE'
             AND p.status IN ( 'AC' , 'ACTIVE' )
    )
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.x1AccountId , t.serialNumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.x1AccountId , s.serialNumber
     FROM (
   SELECT d.x1AccountId , d.serialNumber
     FROM SOIPDEVICESFORACTIVATION d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipDevicesForActivation ;

PROCEDURE soipSignedPlans IS
-- 21/11/23 (RFA) - Use table CUSTOMERTOKENS to retrieve the MessoToken
   l_pool VARCHAR2(29) := 'SOIPSIGNEDPLANS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   FOR l_cus IN 1..6
   LOOP
      execute immediate 'insert /*+ append*/ into soipSignedPlans t ( t.agreement_ref , t.external_id )
         SELECT agreement_ref , external_id
           FROM extn_payment_plan_summary@cus0' || TO_CHAR ( l_cus ) || ' epps
           JOIN external_id_acct_map@adm eiam ON epps.account_no = eiam.account_no
           JOIN arbor.cmf@cus0' || TO_CHAR ( l_cus ) || ' cmf ON epps.account_no = cmf.account_no
          WHERE eiam.external_id_type = 1
            AND epps.status = 5
            AND epps.is_current = 1
            AND cmf.account_category = 4'
         ;
      COMMIT ;
   END LOOP ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   -- populateMessoToken
   logger.write ( 'Merge messoToken' ) ;
   MERGE INTO soipSignedPlans t
   USING (
      SELECT ct.accountNumber, ct.partyId, ct.messoToken
        FROM dataprov.customertokens ct
   ) s ON ( s.accountNumber = t.external_id )
   WHEN MATCHED THEN UPDATE SET 
        t.partyId = s.partyId
      , t.messoToken = s.messoToken
   ;
   logger.write ( 'Merge complete' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.data , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.data , s.partyId , s.messoToken
     FROM (
   SELECT d.external_id AS accountNumber , d.agreement_ref AS data , d.partyId , d.messoToken
     FROM soipSignedPlans d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   -- staging table truncated in child pool data_prep_05.soipSignedPlansNoBurn
   logger.write ( 'complete' ) ;
END soipSignedPlans ;

PROCEDURE soipPendingActive IS
-- 21/11/23 (RFA) - Use table CUSTOMERTOKENS to retrieve the MessoToken
   l_pool VARCHAR2(29) := 'SOIPPENDINGACTIVE' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipPendingActive t ( t.accountNumber, t.partyId, t.messoToken, t.emailAddress )
   SELECT DISTINCT ba.accountNumber, ct.partyId, ct.messoToken, ct.emailaddress
     FROM rcrm.subscription sub
     JOIN rcrm.productSubscriptionLink psl ON psl.subscriptionId = sub.id
     JOIN rcrm.product prod ON prod.id = psl.productId
     JOIN rcrm.service serv ON serv.id = prod.serviceId
     JOIN ccsowner.bsbBillingAccount ba ON ba.serviceInstanceId = serv.billingServiceInstanceId
     LEFT JOIN dataprov.customertokens ct ON ba.accountNumber = ct.accountNumber
    WHERE sub.status IN ( 'PA' , 'PREACTIVE' )  
   ;
   COMMIT ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   MERGE INTO soipPendingActive t USING (
      SELECT DISTINCT ba.accountnumber
        FROM rcrm.product pr
        JOIN rcrm.service se ON pr.serviceId = se.id
        JOIN ccsowner.bsbBillingAccount ba ON ba.serviceInstanceId = se.billingServiceInstanceId
       WHERE pr.eventcode = 'DELIVERED'
         AND pr.status = 'DELIVERED'
         AND pr.suid IN ( 'LLAMA_LARGE' , 'LLAMA_MEDIUM' , 'LLAMA_SMALL' )
   ) s ON ( s.accountNumber = t.accountNumber )
   WHEN MATCHED THEN UPDATE SET t.delivered = 1
   ;
   -- reduce size of data to be processed by slow populateMessoToken merge statement
   UPDATE soipPendingActive t
      SET t.delivered = 0
    WHERE t.delivered IS NULL
      AND ROWNUM <= 200000
   ;
   DELETE FROM soipPendingActive t
    WHERE t.delivered IS NULL
   ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;

   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken , t.emailaddress )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken , s.emailaddress
     FROM (
   SELECT d.accountNumber , d.partyId , d.messoToken , d.emailaddress
     FROM soipPendingActive d
    WHERE d.delivered = 0
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   -- table not truncated here, because used by child pool soipPendingActiveEdwin in data_prep_07.
   logger.write ( 'complete' ) ;
END soipPendingActive ;

PROCEDURE soipOnBoarding IS
-- 21/11/23 (RFA) - Use table CUSTOMERTOKENS to retrieve the MessoToken
   l_pool VARCHAR2(29) := 'SOIPONBOARDING' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   -- Local table ulmOnboardToken is populated by data_prep_01.ulmOnboardToken, here we copy to fps database for performance.
   DELETE FROM ulmOnBoardToken_tmp@fps ;
   INSERT INTO ulmOnBoardToken_tmp@fps SELECT * FROM ulmOnBoardToken ;
   logger.write ( 'ulmOnBoardToken_tmp@fps Updated' ) ;
   COMMIT ;
   INSERT /*+ append */ INTO soipOnBoarding t ( t.partyId , t.ulmToken , t.accountNumber , t.actionId , t.actionType , t.postcode )
   SELECT s.partyId , s.ulmToken , s.accountNumber , s.actionId , s.actionType , s.postcode
     FROM dp_soipPendingActive_v@fps s
   ;   -- rownum clause is in that view to restrict data / save temp space.
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   -- 23-Jun-2021 Andrew Fraser for Ross Benton - remove any non-soip tp_core accounts (only a tiny number of those were being picked up anyway tho).
   logger.write ( 'soipOnBoarding - Initial Insert done' ) ;
   DELETE FROM soipOnboarding t
    WHERE NOT EXISTS (
      SELECT NULL
        FROM ccsowner.bsbBillingAccount ba
        JOIN rcrm.service serv ON ba.serviceInstanceId = serv.billingServiceInstanceId
        JOIN rcrm.product prod ON serv.id = prod.serviceId
        JOIN rcrm.productSubscriptionLink psl ON prod.id = psl.productId
        JOIN rcrm.subscription sub ON psl.subscriptionId = sub.id
       WHERE ba.accountNumber = t.accountNumber
      )
   ;
   logger.write ( 'soipOnBoarding - Delete 1 done' ) ;
   -- 14-Jul-2021 Andrew Fraser for Julian Correa - remove any customers who have an ACtive soip subscription.
   DELETE FROM soipOnboarding t
    WHERE EXISTS (
      SELECT NULL
        FROM ccsowner.bsbBillingAccount ba
        JOIN rcrm.service serv ON ba.serviceInstanceId = serv.billingServiceInstanceId
        JOIN rcrm.product prod ON serv.id = prod.serviceId
        JOIN rcrm.productSubscriptionLink psl ON prod.id = psl.productId
        JOIN rcrm.subscription sub ON psl.subscriptionId = sub.id
       WHERE ba.accountNumber = t.accountNumber
         AND sub.status IN ( 'AC' , 'ACTIVE' )
      )
   ;
   logger.write ( 'soipOnBoarding - Delete 2 done' ) ;
   -- populateMessoToken
   logger.write ( 'soipOnBoarding - Merge messoToken' ) ;
   MERGE INTO soipOnBoarding t
   USING (
      SELECT ct.accountNumber, ct.partyId, ct.messoToken, ct.emailAddress
        FROM dataprov.customertokens ct
   ) s ON ( s.accountNumber = t.accountNumber )
   WHEN MATCHED THEN UPDATE SET 
        t.partyId = s.partyId
      , t.messoToken = s.messoToken
      , t.emailAddress = s.emailAddress
   ;
   logger.write ( 'soipOnBoarding - MessoToken done' ) ;

   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.actionId , t.actionType , t.partyId , t.postcode
        , t.messoToken , t.ulmToken , t.emailAddress )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.actionId , s.actionType , s.partyId , s.postcode , s.messoToken
        , s.ulmToken , s.emailAddress
     FROM (
   SELECT d.accountNumber , d.actionId , d.actionType , d.partyId , d.postcode , d.messoToken , d.ulmToken , d.emailAddress
     FROM soipOnBoarding d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipOnBoarding ;

PROCEDURE soipDeliveredProducts IS
   -- 24-Jun-2022 Andrew Fraser for Humza Ismail, remove all of Terence Burton's paymentCardDetail exclusions.
   -- 10-Mar-2022 Andrew Fraser for Humza Ismail, deleted restriction "AND d.inFlightVisit = 0" from first insert, which dated back to December request NFTREL-21424, was found to be irrelevant to test script.
   -- 06-Jan-2022 Andrew Fraser for Humza Ismail, remove customers with an open outage NFTREL-21437
   -- 03-Dec-2021 Andrew Fraser for Edward Falconer, restrict to active accounts NFTREL-21402 - which backs out the 10-Sep-2021 change
   -- 14-Sep-2021 Andrew Fraser for Terence Burton, restrict to delivered (not awaiting_delivery) products NFTREL-21200
   -- 10-Sep-2021 Andrew Fraser for Terence Burton, restrict to pre-active soip customers NFTREL-21187
   -- 07-Sep-2021 Alex Hyslop for Terence Burton, initial creation NFTREL-21172
   l_pool VARCHAR2(29) := 'SOIPDELIVEREDPRODUCTS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   execute immediate 'alter session enable parallel dml' ;
   INSERT /*+ append */ INTO soipDeliveredProducts t ( t.accountNumber , t.partyId )
   SELECT d.accountNumber , d.partyId
     FROM dataprov.soipActiveSubscription d
     JOIN ccsowner.bsbBillingAccount ba ON ba.accountNumber = d.accountNumber
     JOIN rcrm.service se ON ba.serviceInstanceId = se.billingServiceInstanceId
     JOIN rcrm.product pr ON pr.serviceId = se.id
    WHERE pr.eventcode = 'DELIVERED'
      AND pr.status = 'DELIVERED'  -- 14-Sep-2021 restrict to delivered (not awaiting_delivery) products
      AND pr.suid IN ( 'LLAMA_LARGE' , 'LLAMA_MEDIUM' , 'LLAMA_SMALL' )
      AND d.returnInTransit = 0 -- 14-Jan-2022 Andrew Fraser for Humza Ismail, remove customers with any return_in_transit status products NFTREL-21454
      AND ROWNUM <= 500000
   ;
   logger.write ( 'inserted ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   COMMIT ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   DELETE FROM soipDeliveredProducts t WHERE t.accountnumber IN ( SELECT /*+ parallel(8) */ c.accountNumber FROM customers c ) ;
   logger.write ( 'deleted for customers ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   -- 06-Jan-2022 Andrew Fraser for Humza Ismail, remove customers with an open outage NFTREL-21437
   DELETE /*+ parallel(8) */ FROM soipDeliveredProducts t
    WHERE t.accountNumber IN (
          SELECT ba.accountNumber
            FROM ccsowner.bsbBillingAccount ba
            JOIN ccsowner.bsbPortfolioProduct bpp ON bpp.portfolioId = ba.portfolioId
            JOIN ccsowner.bsbCustomerProductElement bcpe ON bcpe.portfolioProductId = bpp.id
            JOIN ccsowner.bsbBroadbandCustProdElement bpe ON bpe.lineProductElementId = bcpe.id
            JOIN dataprov.snsOpenOutages o ON o.serviceId = bpe.serviceNumber  -- populated by data_prep_07.replaceHubOutOfWarranty
           WHERE bcpe.status = 'AC'
             AND bpp.status = 'AC'
          )
   ;
   logger.write ( 'deleted for outages ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   COMMIT ;
   -- 07-Jan-2020 Andrew Fraser for Antti Makarainen, remove any invalid phone numbers (greater than or less than 11 digits are invalid) - was causing errors downstream when trying to submit an order
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   DELETE FROM soipDeliveredProducts d
    WHERE d.partyId IN (
          SELECT bc.partyId
            FROM ccsowner.bsbContactor bc
            JOIN ccsowner.bsbContactTelephone bct ON bct.contactorId = bc.id
            JOIN ccsowner.bsbTelephone bt ON bt.id = bct.telephoneId
           WHERE bct.deletedFlag = 0
             AND SYSDATE BETWEEN bct.effectiveFromDate AND NVL ( bct.effectiveToDate , SYSDATE + 1 )
             AND LENGTH ( bt.combinedTelephoneNumber ) != 11
          )
   ;
   logger.write ( 'deleted for phone numbers ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   -- 13-Jul-2022 Andrew Fraser for Ismail Humza, exclude customers with in-flight-status on their Sky Broadband. Not having Sky Brtoadband at all is ok, those customers are likely using a non-Sky broadband company.
   DELETE FROM soipDeliveredProducts t
    WHERE EXISTS (
          SELECT NULL
            FROM customers c
           WHERE c.partyId = t.partyId
             AND c.bband = 2
          )
   ;
   logger.write ( 'deleted for broadband ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber
     FROM (
   SELECT d.accountNumber
     FROM soipDeliveredProducts d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipDeliveredProducts ;

PROCEDURE soipActiveSubscription IS
   -- 19-Aug-2021 Andrew Fraser set to cycle: Rizwan Soomra using this in too high volumes to be viable to burn as he originally requested.
   -- 17-Aug-2021 Andrew Fraser for Rizwan Soomra soipActiveSubNetflix to be for standard and premium, NOT basic, Netflix.
   -- 11-Aug-2021 Andrew Fraser for Rizwan Soomra status to include 'ACTIVE' as well as 'AC'
   -- 02-Jul-2021 Andrew Fraser added sports flag for Rizwan Soomra SOIPOD-2135
   -- 01-Jul-2021 Andrew Fraser added noNetflix flag for Rizwan Soomra SOIPOD-2101
   -- 12-May-2021 Andrew Fraser added skyKids flag for Stuart Kerr SOIPPOD-1785
   -- 19-Nov-2020 Stephen Grant Initial Creation based on SQL from Amit More
   -- 23/11/23 (RFA) - Use table CUSTOMERTOKENS to retrieve the MessoToken
   l_pool VARCHAR2(29) := 'SOIPACTIVESUBSCRIPTION' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   execute immediate 'alter session enable parallel dml' ;
   INSERT /*+ append */ INTO soipActiveSubscription t ( t.accountNumber )
   SELECT DISTINCT ba.accountNumber
     FROM rcrm.subscription sub
     JOIN rcrm.productSubscriptionLink psl ON psl.subscriptionId = sub.id
     JOIN rcrm.product prod ON prod.id = psl.productId
     JOIN rcrm.service serv ON serv.id = prod.serviceId
     JOIN ccsowner.bsbBillingAccount ba ON ba.serviceInstanceId = serv.billingServiceInstanceId
    WHERE sub.status IN ( 'AC' , 'ACTIVE' )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted.' ) ;
   COMMIT ;
   MERGE INTO soipActiveSubscription t USING (
      SELECT ba.accountNumber
           , MAX ( CASE WHEN prod.suid = 'SOIP_TV_KIDS' AND sub.status IN ( 'AC' , 'ACTIVE' ) AND prod.status IN ( 'AC' , 'ACTIVE' ) THEN 1
                        WHEN prod.suid = 'SOIP_TV_KIDS' THEN 2
                        ELSE 0 END ) AS skyKids
           , MAX ( CASE WHEN prod.suid = 'SOIP_TV_KIDS' THEN prod.id ELSE NULL END ) AS portfolioProductId
           , MAX ( CASE WHEN prod.suid LIKE '%NETFLIX%' AND sub.status IN ( 'AC' , 'ACTIVE' ) AND prod.status IN ( 'AC' , 'ACTIVE' ) THEN 1
                        WHEN prod.suid LIKE '%NETFLIX%' THEN 2
                        ELSE 0 END ) AS netflix
           , MAX ( CASE WHEN prod.suid IN ( 'SOIP_NETFLIX_STANDARD' , 'SOIP_NETFLIX_PREMIUM' )
                             AND sub.status IN ( 'AC' , 'ACTIVE' ) AND prod.status IN ( 'AC' , 'ACTIVE' ) THEN 1
                        WHEN prod.suid IN ( 'SOIP_NETFLIX_STANDARD' , 'SOIP_NETFLIX_PREMIUM' ) THEN 2
                        ELSE 0 END ) AS netflixStanPrem
           , MAX ( CASE WHEN prod.suid = 'SOIP_NETFLIX_STANDARD'
                             AND sub.status IN ( 'AC' , 'ACTIVE' ) AND prod.status IN ( 'AC' , 'ACTIVE' ) THEN 1
                        WHEN prod.suid = 'SOIP_NETFLIX_STANDARD' THEN 2
                        ELSE 0 END ) AS netflixStan
           , MAX ( CASE WHEN prod.suid = 'SOIP_NETFLIX_PREMIUM'
                             AND sub.status IN ( 'AC' , 'ACTIVE' ) AND prod.status IN ( 'AC' , 'ACTIVE' ) THEN 1
                        WHEN prod.suid = 'SOIP_NETFLIX_PREMIUM' THEN 2
                        ELSE 0 END ) AS netflixPrem
           , MAX ( CASE WHEN prod.suid = 'SOIP_NETFLIX_BASIC'
                             AND sub.status IN ( 'AC' , 'ACTIVE' ) AND prod.status IN ( 'AC' , 'ACTIVE' ) THEN 1
                        WHEN prod.suid = 'SOIP_NETFLIX_BASIC' THEN 2
                        ELSE 0 END ) AS netflixBasic
           , MAX ( CASE WHEN prod.suid LIKE '%SPORT%' AND sub.status IN ( 'AC' , 'ACTIVE' ) AND prod.status IN ( 'AC' , 'ACTIVE' ) THEN 1
                        WHEN prod.suid LIKE '%SPORT%' THEN 2
                        ELSE 0 END ) AS sports
           , MAX ( CASE WHEN prod.suid = 'SOIP_MOV_COMP' AND sub.status IN ( 'AC' , 'ACTIVE' ) AND prod.status IN ( 'AC' , 'ACTIVE' ) THEN 1
                        WHEN prod.suid = 'SOIP_MOV_COMP' THEN 2
                        ELSE 0 END ) AS cinema
           , MAX ( serv.serviceType ) AS serviceType  -- 'SOIP' or 'AMP'
        FROM rcrm.subscription sub
        JOIN rcrm.productSubscriptionLink psl ON psl.subscriptionId = sub.id
        JOIN rcrm.product prod ON prod.id = psl.productId
        JOIN rcrm.service serv ON serv.id = prod.serviceId
        JOIN ccsowner.bsbBillingAccount ba ON ba.serviceInstanceId = serv.billingServiceInstanceId
       GROUP BY ba.accountNumber
   ) s ON ( t.accountNumber = s.accountNumber )
   WHEN MATCHED THEN UPDATE SET t.skyKids = s.skyKids
      , t.portfolioProductId = s.portfolioProductId
      , t.netflix = s.netflix
      , t.netflixStanPrem = s.netflixStanPrem
      , t.netflixStan = s.netflixStan
      , t.netflixPrem = s.netflixPrem
      , t.netflixBasic = s.netflixBasic
      , t.sports = s.sports
      , t.serviceType = s.serviceType
      , t.cinema = s.cinema
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged.' ) ;

   -- populateMessoToken. FirstName and familyName are only used by dependent child pool soipActiveNoUlm.
   MERGE INTO soipActiveSubscription t
   USING (
      SELECT bba.accountNumber
           , ct.partyId
           , ct.firstName
           , ct.familyName
           , ct.messoToken 
           , ct.ssoToken
           , ct.cesaToken as skyCesa01Token
        FROM ccsowner.bsbBillingAccount bba
        LEFT JOIN dataprov.customertokens ct ON bba.accountNumber = ct.accountNumber
   ) s ON ( s.accountNumber = t.accountNumber )
   WHEN MATCHED THEN UPDATE SET t.partyId = s.partyId
      , t.firstName = s.firstName
      , t.familyName = s.familyName
      , t.messoToken = s.messoToken
      , t.ssoToken = s.ssoToken
      , t.skyCesa01Token = s.skyCesa01Token
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged for messoToken' ) ;
   COMMIT;

   -- 28-Jan-2023 Andrew Fraser for Michael Santos, exclude deceased customers. SOIPPOD-2736.
   DELETE FROM soipActiveSubscription t
    WHERE t.partyId IN (
          SELECT con.partyId
            FROM ccsowner.bsbContactor con
           WHERE con.mortalityStatus IN ( '02' , '03' )  -- 02 Deceased Notified , 03 Deceased Confirmed.
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted because deceased' ) ;
   COMMIT ;

   -- populatePostCode. Only used by dependent child pools soipActiveNoUlm and activeSoipRoi.
   -- 26-Jun-2021 Andrew Fraser added group by to deal with "ORA-30926: unable to get a stable set of rows in the source tables"
   -- 09-Jan-2023 Antti added countryCode for dependent child pool activeSoipRoi NFTREL-22132.
   MERGE INTO soipActiveSubscription t
   USING (
      SELECT bc.partyId
           , MAX ( CASE WHEN ba.countryCode = 'IRL' THEN ba.eirCode ELSE ba.postcode END ) AS postcode
           , MIN ( ba.countryCode ) AS countryCode  -- IRL or GBR
        FROM ccsowner.bsbContactor bc
        JOIN ccsowner.bsbContactAddress bca ON bca.contactorId = bc.id
        JOIN ccsowner.bsbAddress ba ON ba.id = bca.addressId
       WHERE bca.primaryFlag = 1
         AND bca.deletedFlag = 0
         AND bca.effectiveToDate IS NULL
       GROUP BY bc.partyId
   ) s ON ( s.partyId = t.partyId )
   WHEN MATCHED THEN UPDATE SET t.postcode = s.postcode , t.countryCode = s.countryCode
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged for postcode' ) ;
   COMMIT;

   -- ulm flag used by 3 child pools
   MERGE INTO soipActiveSubscription t
   USING (
      SELECT DISTINCT eaa.partyId
        FROM mint_platform.ext_account_atr@ulm eaa
   ) s ON ( s.partyId = t.partyId )
   WHEN MATCHED THEN UPDATE SET t.ulm = 1
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged for ulm' ) ;
   COMMIT;

   UPDATE soipActiveSubscription t SET t.ulm = 0 WHERE t.ulm IS NULL ;
   COMMIT;

   -- 09-Aug-2021 Andrew Fraser for Alex Brown NFTREL-21113, add x1accountId column. Only needed for soipActiveSubscription, but almost as quick doing for all pools in table.
   MERGE INTO soipActiveSubscription t
   USING (
      SELECT bba.accountNumber , MAX ( pte.x1accountId ) AS x1accountId
        FROM ccsowner.bsbBillingAccount bba
        JOIN ccsowner.portfolioTechElement pte ON pte.portfolioId = bba.portfolioId
       GROUP BY bba.accountNumber
   ) s ON ( s.accountNumber = t.accountNumber )
   WHEN MATCHED THEN UPDATE SET t.x1accountId = s.x1accountId WHERE NVL ( t.x1accountId , 'x' ) !=  NVL ( s.x1accountId , 'x' )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged for x1accountId' ) ;
   COMMIT;

   -- used by two child pools in data_prep_07: soip and soipNoBroadband
   MERGE INTO soipActiveSubscription t USING (
      SELECT c.partyId , c.bband , c.talk
        FROM customers c
   ) s ON ( t.partyId = s.partyId )
   WHEN MATCHED THEN UPDATE SET t.bband = s.bband , t.talk = s.talk
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged for bband talk.' ) ;
   COMMIT;

   MERGE INTO soipActiveSubscription t USING (
      SELECT DISTINCT c.externalId AS partyId
        FROM caseManagement.bsbCmContext@cse c
       WHERE c.type = 'PARTY'
   ) s ON ( t.partyId = s.partyId )
   WHEN MATCHED THEN UPDATE SET t.previousCases = 1
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged for case partyId.' ) ;
   COMMIT;

   MERGE INTO soipActiveSubscription t USING (
      SELECT DISTINCT c.externalId AS accountNumber
        FROM caseManagement.bsbCmContext@cse c
       WHERE c.type = 'ACCOUNTNUMBER'
   ) s ON ( t.accountNumber = s.accountNumber )
   WHEN MATCHED THEN UPDATE SET t.previousCases = 1 WHERE t.previousCases IS NULL
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged for case accountNumber.' ) ;
   COMMIT ;

   -- 19-Jan-2022 Humza Ismail NFTREL-21473
   MERGE /*+ parallel(8) */ INTO soipActiveSubscription t USING (
      SELECT DISTINCT ba.accountNumber
        FROM ccsowner.bsbVisitRequirement bvr
        JOIN ccsowner.bsbAddressUsageRole baur ON bvr.installationAddressRoleId = baur.id
        JOIN ccsowner.bsbServiceInstance bsi ON baur.serviceInstanceId = bsi.id
        JOIN ccsowner.bsbBillingAccount ba ON ba.portfolioId = bsi.portfolioId
       WHERE bvr.statusCode NOT IN ( 'CP' , 'CN' )
          -- Complete/Cancelled are ok. UB is definitely a problem, others might be ok depending on visit_date, list is:
          -- select code,codeDesc from refdatamgr.picklist where codeGroup = 'VisitRequirementStatus' order by 1 ;
   ) s ON ( t.accountNumber = s.accountNumber )
   WHEN MATCHED THEN UPDATE SET t.inFlightVisit = 1 WHERE t.inFlightVisit IS NULL
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged for inFlightVisit.' ) ;
   COMMIT ;

   UPDATE soipActiveSubscription t SET t.inFlightVisit = 0 WHERE t.inFlightVisit IS NULL ;

   -- 21-Jan-2022 Humza Ismail NFTREL-21480
   MERGE INTO soipActiveSubscription t USING (
      SELECT DISTINCT ba.accountNumber
         FROM ccsowner.bsbBillingAccount ba
         JOIN rcrm.service se ON ba.serviceInstanceId = se.billingServiceInstanceId
         JOIN rcrm.product pr ON pr.serviceId = se.id
        WHERE pr.status = 'RETURN_IN_TRANSIT'
   ) s ON ( t.accountNumber = s.accountNumber )
   WHEN MATCHED THEN UPDATE SET t.returnInTransit = 1 WHERE t.returnInTransit IS NULL
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged for returnInTransit.' ) ;
   COMMIT;

   UPDATE soipActiveSubscription t SET t.returnInTransit = 0 WHERE t.returnInTransit IS NULL ;

   -- 02-Feb-2022 Archana Burla NFTREL-21517. This field needs to be at customer (partyId) level for Archana.
   MERGE INTO soipActiveSubscription t USING (
      SELECT DISTINCT pr.partyId
        FROM ccsowner.bsbPartyRole pr
        JOIN ccsowner.bsbCustomerRole cr ON pr.id = cr.partyRoleId
        JOIN ccsowner.bsbBillingAccount ba ON ba.portfolioId = cr.portfolioId
        JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
       WHERE s.serviceType = 'AMP'
   ) s ON ( t.partyId = s.partyId )
   WHEN MATCHED THEN UPDATE SET t.customerHasAmp = 1 WHERE t.customerHasAmp IS NULL
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged for customerHasAmp.' ) ;
   COMMIT;

   UPDATE soipActiveSubscription t SET t.customerHasAmp = 0 WHERE t.customerHasAmp IS NULL ;

   -- 25-Jul-2022 below field used in child pools 07.soipBillView 06.4soipHomeSetup
   FOR i IN 1..6
   LOOP
      execute immediate '
      MERGE INTO soipActiveSubscription t
      USING (
         SELECT eiam.external_id AS accountNumber
           FROM external_id_acct_map@adm eiam
           JOIN cmf@cus0' || TO_CHAR ( i ) || ' c ON c.account_no = eiam.account_no
          WHERE eiam.external_id_type = 1
            AND c.no_bill = 0
      ) s ON ( s.accountNumber = t.accountNumber )
      WHEN MATCHED THEN UPDATE SET t.billed = 1 WHERE ( t.billed != 1 OR t.billed IS NULL )
      ' ;
      logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' merged for code for cus0' || TO_CHAR ( i ) ) ;
      COMMIT ;
   END LOOP ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;

   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId  , t.messoToken , t.x1Accountid
        , t.ssoToken , t.skyCesa01Token )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId  , s.messoToken , s.x1Accountid , s.ssoToken , s.skyCesa01Token
     FROM (
           SELECT d.accountnumber , d.partyId  , d.messoToken , d.x1Accountid , d.ssoToken , d.skyCesa01Token
             FROM soipActiveSubscription d
            WHERE d.inFlightVisit = 0  -- 19-Jan-2022 Humza Ismail NFTREL-21473
              AND d.returnInTransit = 0 -- 21-Jan-2022 Humza Ismail NFTREL-21480
              AND d.ulm = 1  -- 01-Mar-2022 Andrew Fraser for Julian Correa
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   COMMIT;
   logger.write ( 'complete' ) ;
END soipActiveSubscription ;


PROCEDURE soipActiveNoUlm IS
   l_pool VARCHAR2(29) := 'SOIPACTIVENOULM' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId  , t.messoToken , t.postcode , t.firstName
        , t.familyName )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId  , s.messoToken , s.postcode , s.firstName , s.familyName
     FROM (
   SELECT d.accountnumber , d.partyId  , d.messoToken , d.postcode , d.firstName , d.familyName
     FROM soipActiveSubscription d
    WHERE d.ulm = 0  -- is not in ulm database
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END soipActiveNoUlm ;

PROCEDURE soipActiveSubNetflix IS
   -- 15-Feb-2022(2) Andrew Fraser for Humza Ismail, changed to cycle instead of burn data
   -- 15-Feb-2022 Andrew Fraser for Humza Ismail, restrict to status=delivered ERICAPOD-239.
   -- 14-Feb-2022 Alex Hyslop for Humza Ismail remove any accounts with existing inflight SOIP visits NFTREL-21555
   -- 19-Aug-2021 Andrew Fraser temporary set to cycle while Rizwan Soomra is smoke testing, can set back to burn once this pool has reasonable data quantities.
   -- 11-Aug-2021 Andrew Fraser for Rizwan Soomra customers who have netflix SOIPPOD-2268
   -- 13-Aug-2021 Andrew Fraser for Rizwan Soomra excluded customers who already exist in the ULM database (who gave a 500 error in test script).
   l_pool VARCHAR2(29) := 'SOIPACTIVESUBNETFLIX' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipActiveSubNetflix t ( t.accountNumber , t.partyId , t.messoToken )
   SELECT DISTINCT d.accountnumber , d.partyId  , d.messoToken
     FROM rcrm.product prod
     JOIN rcrm.service serv ON serv.id = prod.serviceId
     JOIN ccsowner.bsbBillingAccount ba ON ba.serviceInstanceId = serv.billingServiceInstanceId
     JOIN soipActiveSubscription d ON d.accountNumber = ba.accountNumber
    WHERE d.netflixStanPrem = 1
      AND d.ulm = 0  -- is not in ulm database
      AND d.inFlightVisit = 0  -- 14-Feb-2022 Alex Hyslop for Humza Ismail remove any accounts with existing inflight SOIP visits NFTREL-21555
      AND prod.status = 'DELIVERED'  -- 15-Feb-2022 Andrew Fraser for Humza Ismail ERICAPOD-239.
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId  , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId  , s.messoToken
     FROM (
   SELECT d.accountnumber , d.partyId  , d.messoToken
     FROM soipActiveSubNetflix d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;  -- 15-Feb-2022(2) Andrew Fraser for Humza Ismail, before was i_burn => TRUE
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipActiveSubNetflix ;

PROCEDURE soipActiveSubNoNetflix IS
   -- 15-Oct-2021 Andrew Fraser for Rizwan Soomra, remove ulm exclusion + allow netflix basic.
   -- 20-Aug-2021 Andrew Fraser for Rizwan Soomra excluded customers who already exist in the ULM database.
   -- 01-Jul-2021 Andrew Fraser added noNetflix flag for Rizwan Soomra SOIPOD-2101
   l_pool VARCHAR2(29) := 'SOIPACTIVESUBNONETFLIX' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId  , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId  , s.messoToken
     FROM (
   SELECT d.accountnumber , d.partyId  , d.messoToken
     FROM soipActiveSubscription d
    WHERE d.netflixStanPrem = 0
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END soipActiveSubNoNetflix ;

PROCEDURE soipActiveSubNoSport IS
   -- 02-Jul-2021 Andrew Fraser added sports flag for Rizwan Soomra SOIPOD-2135
   l_pool VARCHAR2(29) := 'SOIPACTIVESUBNOSPORT' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId  , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId  , s.messoToken
     FROM (
   SELECT d.accountnumber , d.partyId  , d.messoToken
     FROM soipActiveSubscription d
    WHERE d.sports = 0
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT, i_burn => TRUE ) ;  -- changed to burn 11-Sep-2021 because is a Rizwan pool.
   logger.write ( 'complete' ) ;
END soipActiveSubNoSport ;

PROCEDURE soipActiveSubWithSkyKids IS
   -- 12-May-2021 Andrew Fraser two child pools for Stuart Kerr, one with Sky Kids and one without. Both burn data. SOIPPOD-1785
   l_pool VARCHAR2(29) := 'SOIPACTIVESUBWITHSKYKIDS' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId  , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId  , s.messoToken
     FROM (
   SELECT d.accountnumber , d.partyId  , d.messoToken
     FROM soipActiveSubscription d
    WHERE d.skyKids = 1
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END soipActiveSubWithSkyKids ;

PROCEDURE soipActiveSubNoSkyKids IS
   -- 12-May-2021 Andrew Fraser two child pools for Stuart Kerr, one with Sky Kids and one without. Both burn data. SOIPPOD-1785
   l_pool VARCHAR2(29) := 'SOIPACTIVESUBNOSKYKIDS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipActiveSubNoSkyKids t ( t.accountnumber , t.partyId  , t.messoToken )  
   SELECT d.accountnumber , d.partyId  , d.messoToken
     FROM soipActiveSubscription d
     JOIN ccsowner.bsbBillingAccount ba ON ba.accountNumber = d.accountNumber
     JOIN rcrm.service se ON ba.serviceInstanceId = se.billingServiceInstanceId
     JOIN rcrm.product pr ON pr.serviceId = se.id
    WHERE d.skyKids = 0
      AND d.countrycode = 'GBR'
      AND pr.suid IN ( 'LLAMA_LARGE' , 'LLAMA_MEDIUM' , 'LLAMA_SMALL' , 'SKY_GLASS_LARGE_ROI' , 'SKY_GLASS_MEDIUM_ROI' , 'SKY_GLASS_SMALL_ROI' )
   ;
   COMMIT ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => 'ulmPartyIds' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId  , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId  , s.messoToken
     FROM (
   SELECT d.accountnumber , d.partyId  , d.messoToken
     FROM soipActiveSubNoSkyKids d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipActiveSubNoSkyKids ;

PROCEDURE soipUlm IS
   -- 23-Sep-2021 Andrew Fraser for Ross Benton, initial creation.
   -- 23/11/23 (RFA) - Use table CUSTOMERTOKENS to retrieve the MessoToken
   l_pool VARCHAR2(29) := 'SOIPULM' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ulmPartyIds' ;
   INSERT /*+ append */ INTO ulmPartyIds t ( t.partyId )  
   SELECT DISTINCT eaa.partyId
     FROM mint_platform.ext_account_atr@ulm eaa
   ;
   logger.write ( 'ulmPartyIds table populated : '||to_char(sql%rowcount) ) ;
   COMMIT ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => 'ulmPartyIds' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipUlm t ( t.partyId , t.accountNumber , t.messoToken )
   SELECT ct.partyId
        , MAX(ba.accountNumber) as accountNumber
        , MAX(ct.messoToken) as messoToken
     FROM rcrm.service serv
     JOIN ccsowner.bsbBillingAccount ba ON ba.serviceInstanceId = serv.billingServiceInstanceId
     LEFT JOIN dataprov.customertokens ct ON ba.accountNumber = ct.accountNumber
     JOIN dataprov.ulmPartyIds u ON u.partyId = ct.partyId  -- populated above in this same procedure.
    GROUP BY ct.partyId 
   ;
   logger.write ( 'soipUlm table populated : '||to_char(sql%rowcount) ) ;
   COMMIT;
   execute immediate 'truncate table ulmPartyIds' ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken
     FROM (
   SELECT d.accountNumber , d.partyId , d.messoToken
     FROM soipUlm d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 300000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipUlm ;

PROCEDURE soipHomeSetup IS
   -- 24-Jun-2022 Andrew Fraser for Humza Ismail, remove all of Terence Burton's paymentCardDetail exclusions.
   -- 10-Mar-2022 Andrew Fraser for Humza Ismail, changed to cycle data (previously burned data).
   -- 01-Mar-2022 Andrew Fraser for Humza Ismail, deleted restriction "AND d.inFlightVisit = 0" from first insert, which dated back to December request NFTREL-21424, was found to be irrelevant to test script.
   -- 15-Feb-2022(2) Andrew Fraser for Humza Ismail, changed to burn data instead of cycle.
   -- 06-Jan-2022 Andrew Fraser for Humza Ismail, remove customers with an open outage NFTREL-21437
   -- 19-Oct-2021 Andrew Fraser for Antti Makarainen. Customers that have had a Sky Glass device for less than 31 days. NFTREL-21351
   l_pool VARCHAR2(29) := 'SOIPHOMESETUP' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipHomeSetup t ( t.accountNumber , t.partyId , t.messoToken , t.x1accountId )
   SELECT DISTINCT d.accountNumber , d.partyId , d.messoToken , d.x1accountId
     FROM rcrm.product prod
     JOIN rcrm.service serv ON serv.id = prod.serviceId
     JOIN ccsowner.bsbBillingAccount ba ON ba.serviceInstanceId = serv.billingServiceInstanceId
     JOIN dataprov.soipActiveSubscription d ON d.accountNumber = ba.accountNumber
    WHERE prod.suid IN ( 'LLAMA_SMALL' , 'LLAMA_MEDIUM' , 'LLAMA_LARGE' )  -- Sky Glass
      AND prod.status = 'DELIVERED'
      AND d.returnInTransit = 0 -- 14-Jan-2022 Andrew Fraser for Humza Ismail, remove customers with any return_in_transit status products NFTREL-21454
      AND d.billed = 1  -- 20-Jul-2022 Andrew Fraser for Humza Ismail
   ;
   logger.write ( 'inserted ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   COMMIT ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   -- 08-Nov-2021 Andrew Fraser for Humza Ismail, exclude customers who have returned their Sky Glass, NFTREL-21377.
   DELETE FROM soipHomeSetup t
    WHERE t.accountNumber IN (
          SELECT ba.accountNumber
            FROM rcrm.product prod
            JOIN rcrm.service serv ON serv.id = prod.serviceId
            JOIN ccsowner.bsbBillingAccount ba ON ba.serviceInstanceId = serv.billingServiceInstanceId
            JOIN soipActiveSubscription d ON d.accountNumber = ba.accountNumber
           WHERE prod.suid IN ( 'LLAMA_SMALL' , 'LLAMA_MEDIUM' , 'LLAMA_LARGE' )
             AND prod.status = 'PENDING_RETURN'
          )
   ;
   commit;
   logger.write ( 'deleted for returned ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   -- 06-Jan-2022 Andrew Fraser for Humza Ismail, remove customers with an open outage NFTREL-21437
   DELETE FROM soipHomeSetup t
    WHERE t.accountNumber IN (
          SELECT /*+ parallel(8) */ ba.accountNumber
            FROM ccsowner.bsbBillingAccount ba
            JOIN ccsowner.bsbPortfolioProduct bpp ON bpp.portfolioId = ba.portfolioId
            JOIN ccsowner.bsbCustomerProductElement bcpe ON bcpe.portfolioProductId = bpp.id
            JOIN ccsowner.bsbBroadbandCustProdElement bpe ON bpe.lineProductElementId = bcpe.id
            JOIN dataprov.snsOpenOutages o ON o.serviceId = bpe.serviceNumber  -- populated by data_prep_07.replaceHubOutOfWarranty
           WHERE bcpe.status = 'AC'
             AND bpp.status = 'AC'
          )
   ;
   commit;
   logger.write ( 'deleted for outages ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   -- 14-Feb-2022 Alex Hyslop for Humza Ismail, Remove any accounts with a return in transit NFTREL-21555
   DELETE FROM soipHomeSetup t
    WHERE t.accountNumber IN (
          SELECT ba.accountNumber
            FROM rcrm.product prod
            JOIN rcrm.service serv ON serv.id = prod.serviceId
            JOIN ccsowner.bsbBillingAccount ba ON ba.serviceInstanceId = serv.billingServiceInstanceId
            JOIN soipActiveSubscription d ON d.accountNumber = ba.accountNumber
           WHERE prod.suid IN ( 'LLAMA_SMALL' , 'LLAMA_MEDIUM' , 'LLAMA_LARGE' )
             AND prod.eventcode = 'RETURN_IN_TRANSIT'
          )
   ;
   commit;
   logger.write ( 'deleted for RETURN_IN_TRANSIT ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId  , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId  , s.messoToken
     FROM (
   SELECT d.accountnumber , d.partyId  , d.messoToken
     FROM soipHomeSetup d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ; -- 10-Mar-2022 Andrew Fraser for Humza Ismail, before was NOcycle
   --execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipHomeSetup ;

PROCEDURE soipCcaUnsignedPlans IS
   l_pool VARCHAR2(29) := 'SOIPCCAUNSIGNEDPLANS' ;
   -- 24/11/23 (RFA) - Use table CUSTOMERTOKENS to retrieve the MessoToken
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   FOR l_cus IN 1..6
   LOOP
      execute immediate 'insert /*+ append */ into soipCcaUnsignedPlans t ( t.agreement_ref , t.external_id )
         SELECT agreement_ref , external_id
           FROM extn_payment_plan_summary@cus0' || TO_CHAR ( l_cus ) || ' epps,
                external_id_acct_map@adm eiam,
            arbor.cmf@cus0' || TO_CHAR ( l_cus ) || ' cmf
        WHERE epps.account_no = eiam.account_no
          AND epps.account_no = cmf.account_no
         AND eiam.external_id_type = 1
         AND epps.status = 0
         AND epps.is_current = 1
         AND cmf.account_category = 4';
      COMMIT ;
   END LOOP ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   -- populateMessoToken
   MERGE INTO soipCcaUnsignedPlans t
   USING (
      SELECT ct.accountNumber, ct.partyId, ct.messoToken
        FROM dataprov.customertokens ct
   ) s ON ( s.accountNumber = t.external_id )
   WHEN MATCHED THEN UPDATE SET 
        t.partyId = s.partyId
      , t.messoToken = s.messoToken
   ;
   logger.write ( 'soipCcaUnsignedPlans tokens merged : '||to_char(sql%rowcount) ) ;
   commit;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.data , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.data , s.partyId , s.messoToken
     FROM (
   SELECT d.external_id AS accountNumber , d.agreement_ref AS data , d.partyId , d.messoToken
     FROM soipCcaUnsignedPlans d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END soipCcaUnsignedPlans ;

PROCEDURE soipDev4ActBen IS
   -- 02-Sep-2021 Andrew Fraser for Alex Benetatos must have sourceReference, requires joining to deviceChangeHistory table.
   -- 21-Jul-2021 Andrew Fraser for Alex Benetatos restrict to NFT sold products (TV11SKA serialNumbers)
   -- 21-Jul-2021 Andrew Fraser for Amit More remove any customers already ACtivated
   -- 15-Apr-2021 : Alex Hyslop Initial Creation based on SQL from David Dryburgh under SOIPPOD-1598
   l_pool VARCHAR2(29) := 'SOIPDEV4ACTBEN' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipdev4actben t ( t.accountNumber , t.x1AccountId , t.serialNumber , t.productId )
   SELECT DISTINCT ba.accountNumber , pte.x1AccountId , hp.serialNumber , pr.id AS productId
     FROM rcrm.product pr
     JOIN rcrm.service sr ON pr.serviceid = sr.id
     JOIN ccsowner.bsbBillingAccount ba ON ba.serviceInstanceId = sr.billingServiceInstanceId
     JOIN ccsowner.portfolioTechElement pte ON pte.portfolioId = ba.portfolioId
     JOIN rcrm.hardwareProdTechElement hp ON hp.productId = pr.id
     JOIN rcrm.deviceRegistry dr ON dr.publicDeviceId = hp.serialNumber AND dr.accountNumber = ba.accountNumber
     JOIN rcrm.deviceHistoryDetail dhd ON dr.id = dhd.deviceRegistryId
     JOIN rcrm.deviceChangeHistory dch ON dr.accountNumber = dch.accountNumber AND dhd.deviceChangeId = dch.id
    WHERE pr.eventCode =  'DELIVERED' --'DISPATCHED'
      --AND hp.serialNumber IS NOT NULL   -- superflous if join stays at "dr.publicDeviceId = hp.serialNumber"
      AND dch.sourceReference IS NOT NULL
      and hp.serialNumber LIKE 'TV11SKA%'
   ;
   COMMIT ;
   -- 21-Jul-2021 Andrew Fraser for Alex Benetatos restrict to NFT sold products (TV11SKA serialNumbers)
   -- removed below as code added to main query
   --DELETE FROM soipDevicesForActivation t
   -- WHERE t.serialNumber NOT LIKE 'TV11SKA%';

   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   -- 21-Jul-2021 Andrew Fraser for Amit More remove any customers already ACtivated
   DELETE FROM soipdev4actben t
    WHERE t.accountNumber IN (
          SELECT ba.accountNumber
            FROM rcrm.product p
            JOIN rcrm.service s ON p.serviceid = s.id
            JOIN ccsowner.bsbbillingaccount ba ON ba.serviceInstanceId = s.billingServiceInstanceId
           WHERE p.suid in ('SOIP_TV_SKY_SIGNATURE', 'SKY_GLASS_ULTIMATE_TV_ROI')
           --WHERE p.suid = 'SOIP_TV_SKY_SIGNATURE'
             AND p.status IN ( 'AC' , 'ACTIVE' )
    )
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.x1AccountId , t.serialNumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.x1AccountId , s.serialNumber
     FROM (
   SELECT d.x1AccountId , d.serialNumber
     FROM soipdev4actben d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipDev4ActBen ;

PROCEDURE soipUndeliProdBen IS
   -- 24/11/23 (RFA) - Use table CUSTOMERTOKENS to retrieve the MessoToken ( pool is not in RUN_JOB_PARALLEL_CONTROL. Not run since 2022 )
   l_pool VARCHAR2(29) := 'SOIPUNDELIPRODBEN' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO SOIPUNDELIPRODBEN t ( t.accountNumber , t.productId , t.fulfilmentReferenceId )
   SELECT DISTINCT ba.accountnumber , pr.id AS productId , up.fulfilmentReferenceId
     FROM rcrm.product pr
     JOIN rcrm.service se ON pr.serviceId = se.id
     JOIN ccsowner.bsbBillingAccount ba ON ba.serviceInstanceId = se.billingServiceInstanceId
     LEFT OUTER JOIN rcrm.hardwareProdTechElement hp ON hp.productId = pr.id
     JOIN dp_soipUndeliveredProducts_v@fps up ON ba.accountNumber = up.accountNumber
    WHERE pr.suid in ('LLAMA_SMALL', 'LLAMA_MEDIUM', 'LLAMA_LARGE') -- changed for Alex Benatatos 23/02/2022
      --AND pr.eventCode = 'ACCEPTED_BY_FULFILMENT_HOUSE'
      AND pr.eventCode in ('ACCEPTED_BY_FULFILMENT_HOUSE','SENT_TO_FULFILMENT_HOUSE')
      AND hp.serialNumber IS NULL
      AND ROWNUM <= 100000
   ;
   logger.write ( 'SOIPUNDELIPRODBEN : '||to_char(sql%rowcount)||' rows inserted') ;
   COMMIT ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   -- populateMessoToken
   MERGE INTO SOIPUNDELIPRODBEN t
   USING (
      SELECT ct.accountNumber, ct.partyId, ct.messoToken
        FROM dataprov.customertokens ct
   ) s ON ( s.accountNumber = t.accountNumber )
   WHEN MATCHED THEN UPDATE SET 
        t.partyId = s.partyId
      , t.messoToken = s.messoToken
   ;
   logger.write ( 'SOIPUNDELIPRODBEN token merge : '||to_char(sql%rowcount)||' rows merged') ;
   commit;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.productId , t.fulfilmentReferenceId , t.partyId
       , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.productId , s.fulfilmentReferenceId , s.partyId , s.messoToken
     FROM (
   SELECT d.accountNumber , d.productId , d.fulfilmentReferenceId , d.partyId , d.messoToken
     FROM SOIPUNDELIPRODBEN d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END soipUndeliProdBen ;

PROCEDURE soipDispatchedProductsBen IS
-- David Dryburgh SOIPPOD-1930
   l_pool VARCHAR2(29) := 'SOIPDISPATCHEDPRODUCTSBEN' ;
BEGIN
   logger.write ( 'begin' ) ;
   -- staging table hosted in fps for performance during the merge statement, to use index on partyId
   DELETE FROM soipDispatchedProductsben@fps ;
   INSERT INTO soipDispatchedProductsben@fps ( accountNumber , productId , partyId )
   SELECT ba.accountNumber
        , p.id AS productId
        , bpr.partyId
     FROM rcrm.product p
     JOIN rcrm.service s ON p.serviceId = s.id
     JOIN ccsowner.bsbBillingaccount ba ON ba.serviceInstanceId = s.billingServiceInstanceId
     JOIN rcrm.hardwareProdTechElement hp ON hp.productId = p.id
     JOIN ccsowner.bsbCustomerRole bcr ON ba.portfolioId = bcr.portfolioId
     JOIN ccsowner.bsbPartyRole bpr ON bcr.partyRoleId = bpr.id
    WHERE p.eventCode = 'DISPATCHED'
      and p.suid in ('LLAMA_SMALL', 'LLAMA_MEDIUM', 'LLAMA_LARGE')      
      AND hp.serialNumber IS NOT NULL
   ;
   -- merge statement held in fps for performance and to workaround "ORA-22992: cannot use LOB locators selected from remote tables"
   data_prep_fps.soipDispatchedProductsben@fps ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.fulfilmentReferenceId , t.productId , t.accountNumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.fulfilmentReferenceId , s.productId , s.accountNumber , s.partyId
     FROM (
   SELECT d.fulfilmentReferenceId , d.productId , d.accountNumber , d.partyId
     FROM soipDispatchedProductsben@fps d
    WHERE d.fulfilmentReferenceId IS NOT NULL
    ORDER BY dbms_random.value  -- before 03-Sep-2021 was instead "ORDER BY d.created"
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END soipDispatchedProductsBen ;

END data_prep_06 ;
/
