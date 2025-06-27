CREATE OR REPLACE PACKAGE data_prep_03 AS
PROCEDURE engineerOpenAppts ;
PROCEDURE accountNumberForCase ;
PROCEDURE accountNumbersWithSnapshots ;
PROCEDURE act_bb_talk_assurance ;
PROCEDURE act_cust_dtv_bb_talk_mob ;
PROCEDURE act_cust_ethan_static ;
PROCEDURE act_cust_techenq ;
PROCEDURE act_mobile_billed ;
PROCEDURE act_mobile_customers ;
PROCEDURE act_mobile_numbers ;
PROCEDURE act_mob_change_subs_downgrade ;
PROCEDURE refCancellationReason ;
PROCEDURE refSponsors ;
PROCEDURE refDiallerOutcome ;
PROCEDURE refVisitCancellationCodes ;
PROCEDURE act_trpl_ply_cust_lt ;
PROCEDURE addOpenUpdateCase ;
PROCEDURE avBroadbandServiceCheckId ;
PROCEDURE book_appointments ;
PROCEDURE cca_active_plans ;
PROCEDURE cca_unsigned_plans ;
PROCEDURE digital_content ;
PROCEDURE findOrders ;
PROCEDURE act_mob_change_subs_upgrade ;
PROCEDURE soipActiveSkyGlass ;
END data_prep_03 ;
/


CREATE OR REPLACE PACKAGE BODY data_prep_03 AS

PROCEDURE engineerOpenAppts IS
  l_pool VARCHAR2(29) := 'ENGINEEROPENAPPTS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT INTO engineerOpenAppts
   select distinct vr.fmsjobreference, vr.visitid,  vr.visitdate,  vr.jobtype, vr.jobdescription, vr.notes, bba.accountNumber,
          bpr.partyId, apt.CustomerOrderID, apt.AppointmentID
     from ccsowner.bsbvisitrequirement vr,  ccsowner.bsbinstallerrole ir,
          ccsowner.bsbpropertydetail   pd,  ccsowner.bsbaddressusagerole au,
          ccsowner.bsbBillingAccount bba, CCSOWNER.bsbServiceInstance bsi,
          ccsowner.bsbCustomerRole bcr, ccsowner.bsbPartyRole bpr,
          oh.customerorders@oms co, oh.appointments@oms apt
    where vr.fulfilmentitemid in  (select fi.id
                                     from ccsowner.bsbBillingAccount  ba,
                                          ccsowner.bsbServiceInstance si,
                                          ccsowner.bsbfulfilmentitem  fi
                                    where ba.portfolioId = si.portfolioId
                                      and si.id = fi.serviceInstanceId)
      and bba.portfolioId = bsi.portfolioId
      and vr.installationaddressroleid = pd.installationaddressroleid
      and pd.installationaddressroleid = au.id
      and au.serviceInstanceId = bsi.id
      and bba.serviceInstanceId = bsi.id
      and vr.installerid = ir.idno(+)
      and bcr.portfolioId = bba.portfolioId
      and bpr.id = bcr.partyroleid
      --and vr.created > TO_DATE('19-AUG-21','DD-MON-YY') --cbh06 Added for OFSC Testing when switched to Oracle from Stub. Adjust date to match switch date. TEMPORARY USAGE ONLY 11/08/21
      and vr.visitdate between sysdate - 7 and sysdate --cbh06 To be commented out during OFSC Testing for above filter line to work. SHOULD NORMALLY BE ACTIVE 11/08/21
      and bsi.SERVICEINSTANCETYPE not in (100,400)
      and VR.STATUSCODE = 'BK'
      and co.CustomeraccountNumber = bba.accountNumber
      and co.CustomerpartyId = bpr.partyId
      and apt.CustomerOrderID = co.CustomerOrderID
      and vr.fmsJobReference LIKE 'VR%'  -- NFTREL-20948
   ;

   commit;
   
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.fmsjobreference , t.visitid , t.visitdate , t.jobtype
        , t.jobdescription , t.accountNumber , t.partyId , t.customerorderid , t.appointmentid )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.fmsjobreference , s.visitid , s.visitdate , s.jobtype
        , s.jobdescription , s.accountNumber , s.partyId , s.customerorderid , s.appointmentid
     FROM (
   SELECT *
     FROM engineerOpenAppts
    WHERE ROWNUM <= 75000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END engineerOpenAppts ;

PROCEDURE accountNumberForCase IS
   l_pool VARCHAR2(29) := 'ACCOUNTNUMBERFORCASE' ;
   v_last_result varchar2(10);
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT INTO accountNumberForCase t ( t.caseNumber , t.accountNumber , t.partyId )
   SELECT caseNumber , accountNumber , partyId
     FROM (
           SELECT MAX ( c.caseNumber) AS caseNumber , ctxt.externalId , ctxt.type
             FROM caseManagement.bsbCmContext@cse ctxt
             JOIN caseManagement.bsbCmCase@cse c ON c.id = ctxt.caseid
            WHERE ctxt.type IN ( 'ACCOUNTNUMBER' , 'PARTY' )
              AND c.status = 'OPEN'
             GROUP BY ctxt.externalId , ctxt.type
           )
    PIVOT ( MAX ( externalId ) FOR ( type ) IN ( 'ACCOUNTNUMBER' AS accountNumber , 'PARTY' AS partyId ) )
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.caseNumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.caseNumber
     FROM (
           SELECT a.accountNumber , a.partyId , a.caseNumber
             FROM accountNumberForCase a
            ORDER BY dbms_random.value
           ) s
     WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END accountNumberForCase ;

PROCEDURE accountNumbersWithSnapshots IS
   l_pool VARCHAR2(29) := 'ACCOUNTNUMBERSWITHSNAPSHOTS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT INTO accountNumbersWithSnapshots t ( t.accountNumber , t.billingAccountId , t.id )
   SELECT v.accountNumber , v.billingAccountId , v.id
     FROM (
           SELECT ba.accountNumber
                , s.billingAccountId
                , s.id
                , ROW_NUMBER() OVER ( PARTITION BY ba.accountNumber , s.billingAccountId ORDER BY ba.accountNumber ) AS rn
             FROM avSnapshot.bsbAvSnapshot s
             JOIN ccsowner.bsbBillingAccount ba ON s.billingAccountId = ba.id
            WHERE s.channel = 'ASSURANCE VIEW'
              AND s.created >= SYSDATE - 60
          ) v
    where rn = 1
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.billingAccountId , t.id )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.billingAccountId , s.id
     FROM (
           SELECT *
             FROM accountNumbersWithSnapshots
            ORDER BY dbms_random.value
           ) s
     WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END accountNumbersWithSnapshots ;

PROCEDURE act_bb_talk_assurance IS
   l_pool VARCHAR2(29) := 'ACT_BB_TALK_ASSURANCE' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT INTO act_bb_talk_assurance t ( t.accountNumber , t.portfolioId , t.talk , t.bband , t.partyId )
   SELECT accountNumber , portfolioId , talk , bband , partyId
     FROM (
           SELECT /*+ full(si)   parallel(si 16)   pq_distribute ( si hash hash )
                      full(subs) parallel(subs 16) pq_distribute ( subs hash hash )
                      full(act)  parallel(act 16)  pq_distribute ( act hash hash )
                      full(c)    parallel(c 16)    pq_distribute ( c hash hash )
                  */
                  act.accountNumber
                , subs.subscriptionTypeId
                , si.id
                , si.portfolioId
                , c.partyId
             FROM ccsowner.bsbServiceInstance si
             JOIN ccsowner.bsbsubscription subs ON subs.serviceInstanceId = si.id
             JOIN act_uk_cust act ON act.portfolioId = si.portfolioId
             JOIN customers c ON c.accountNumber = act.accountNumber
            WHERE subs.status IN ( 'A' , 'AC' )
              AND c.accountNumber2 IS NULL  -- 11-Dec-2017 Andrew Fraser request Stuart Kerr: exclude customers with multiple billing accounts such as mobile customers.
              AND ( subs.statuschangeddate , subs.subscriptionTypeId ) IN (
                   SELECT /*+ full(si210) parallel(si210 16) pq_distribute ( si210 hash hash )
                              full(subs)  parallel(subs 16)  pq_distribute ( subs hash hash )
                          */
                          MAX ( subs.statuschangeddate )
                        , subs.subscriptionTypeId
                     FROM ccsowner.bsbServiceInstance si210
                     JOIN ccsowner.bsbsubscription subs ON subs.serviceInstanceId = si210.id
                    WHERE si210.parentServiceInstanceId = si.parentServiceInstanceId  -- join
                      AND subs.subscriptionTypeId IN ( '3' , '7' )
                    GROUP BY subs.subscriptionTypeId
                   )
           ) PIVOT ( MAX ( id ) FOR ( subscriptionTypeId ) IN ( '3' AS talk , '7' AS bband ) )
    WHERE talk IS NOT NULL
      AND bband IS NOT NULL
   ;
   -- 21-Sep-2018 NFTREL-15224 exclude customers who already have a visit booked.
   -- 04-Feb-2022 Andrew Fraser for Dimitrios Koulialis, made stricter NFTREL-21394 (tpoc only 04-Feb-2022)
   DELETE FROM act_bb_talk_assurance t
    WHERE t.portfolioId IN (
          SELECT /*+
                  full(bvr)  parallel(bvr 8)  pq_distribute ( bvr hash hash )
                  full(baur) parallel(baur 8) pq_distribute ( baur hash hash )
                  full(bsi)  parallel(bsi 8)  pq_distribute ( bsi hash hash )
                 */
                 bsi.portfolioId
            FROM ccsowner.bsbVisitRequirement bvr
            JOIN ccsowner.bsbAddressUsageRole baur ON bvr.installAtionAddressRoleId = baur.id
            JOIN ccsowner.bsbServiceInstance bsi ON baur.serviceInstanceId = bsi.id
           WHERE bvr.statusCode NOT IN ( 'CP' , 'CN' )
          )
   ;
   -- 04-Feb-2022 Andrew Fraser for Dimitrios Koulialis, remove customers with an open outage NFTREL-21394. Fixes FAVsessionCreate test script errors.
   DELETE FROM act_bb_talk_assurance t
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
            JOIN ccsowner.bsbBroadbandCustProdElement bpe ON bpe.lineproductElementId = bcpe.id
            JOIN dataprov.snsOpenOutages o ON o.serviceId = bpe.serviceNumber  -- populated by data_prep_07.replaceHubOutOfWarranty
           WHERE bcpe.status = 'AC'
             AND bpp.status = 'AC'
          )
   ;
   /*
   -- 09-Feb-2022 Andrew Fraser for Dimitrios Koulialis, remove customers with an open case NFTREL-21394 (tpoc only 09-Feb-2022)
   -- 14-Feb-2022 below made no difference to test script issues, so commented out.
   DELETE FROM act_bb_talk_assurance t
    WHERE t.partyId IN (
          SELECT x.externalId AS partyId
            FROM caseManagement.bsbCmContext@cse x
            JOIN caseManagement.bsbCmCase@cse c ON c.id = x.caseId
           WHERE x.type = 'PARTY'
             AND c.is_open_status = '1'
          )
   ;
   */
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.bb_serviceid , t.talk_serviceid )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.bband , s.talk
     FROM (
   SELECT *
     FROM act_bb_talk_assurance
    WHERE ROWNUM <= 100000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END act_bb_talk_assurance ;

PROCEDURE act_cust_dtv_bb_talk_mob IS
   l_pool   VARCHAR2(29) := 'ACT_CUST_DTV_BB_TALK_MOB' ;
   l_count  number;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append*/ INTO act_cust_dtv_bb_talk_mob ( accountNumber , partyId , id , combinedTelephoneNumber , messoToken )
   select /*+ parallel(bba 8) parallel(bpp 8) parallel(bsi 8) parallel(acus 8) parallel(acusb 8) parallel(subs 8) parallel(bcr 8) parallel(bpr 8) parallel(btur 8) parallel(bt 8)*/
     distinct acus.accountNumber , acus.partyId , bsi.id , bt.combinedTelephoneNumber , NULL AS messoToken from
      ccsowner.bsbBillingAccount bba,
      ccsowner.bsbPortfolioProduct bpp,
      ccsowner.bsbServiceInstance bsi,
      ccsowner.bsbsubscription subs,
      act_cust_uk_subs acus,
      act_cust_uk_subs acusb,
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
      and acus.partyId = acusb.partyId
      and acus.dtv = 'AC' and acus.talk = 'A' and acus.bband = 'AC'
      and acusb.mobile = 'AC'
      and btur.effectiveToDate is null   -- AF 22-Jun-2021 added in attempt to reduce multiple rows of same customer but different phonenumber.
   ;
   l_count := SQL%ROWCOUNT ;
   commit;
   logger.write ( 'inserted : ' || l_count ) ;
   -- 30-Mar-2017 Andrew Fraser remove any customers in debt, request Nicolas Patte.
   DELETE FROM act_cust_dtv_bb_talk_mob m WHERE m.accountNumber IN ( SELECT da.accountNumber FROM debt_amount da WHERE da.balance > 0 ) ;
   l_count := SQL%ROWCOUNT;
   commit;
   logger.write ( 'deleted : ' || l_count ) ;   
   MERGE INTO act_cust_dtv_bb_talk_mob t USING (
      SELECT c.partyId , c.messoToken , c.nsProfileId
        FROM customers c
   ) s ON ( s.partyId = t.partyId )
   WHEN MATCHED THEN UPDATE SET t.messoToken = s.messoToken , t.nsProfileId = s.nsProfileId
   ;
   l_count := SQL%ROWCOUNT;
   commit ;
   logger.write ( 'merged from customers : ' || l_count ) ;
   DELETE FROM act_cust_dtv_bb_talk_mob m WHERE m.messoToken IS NULL ;
   l_count := SQL%ROWCOUNT;
   commit ;
   logger.write ( 'deleted : ' || l_count ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.id , t.telephoneNumber
        , t.messoToken , t.nsProfileId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.id , s.combinedTelephoneNumber
        , s.messoToken , s.nsProfileId
     FROM (
   SELECT d.accountNumber , d.partyId , d.id , d.combinedTelephoneNumber , d.messoToken , d.nsProfileId
     FROM act_cust_dtv_bb_talk_mob d
    WHERE ROWNUM <= 100000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END act_cust_dtv_bb_talk_mob ;

PROCEDURE act_cust_ethan_static IS
   l_pool VARCHAR2(29) := 'ACT_CUST_ETHAN_STATIC' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT INTO act_cust_ethan_static  -- takes 134s without hints, runs in parallel without hints.
   SELECT /*+ full(bpp) parallel(bpp 16)
              full(bba) parallel(bba 16)
              full(bcr) parallel(bcr 16)
              full(bpr) parallel(bpr 16)
              full(act) parallel(act 16) */
          bba.accountNumber , bpr.partyId , bpp.catalogueProductId , bpp.status
     FROM ccsowner.bsbPortfolioProduct bpp
     JOIN ccsowner.bsbBillingAccount   bba ON bba.portfolioId = bpp.portfolioId
     JOIN ccsowner.bsbCustomerRole     bcr ON bcr.portfolioId = bpp.portfolioId
     JOIN ccsowner.bsbPartyRole        bpr ON bpr.id = bcr.partyroleid
     JOIN act_uk_cust         act ON act.accountNumber = bba.accountNumber
    WHERE bpp.catalogueProductId IN ( '13947' , '13948' )  -- 13947 Grade X Box, 13948 Grade F Box
      AND bpp.status = 'IN'
      AND ROWNUM <= 500000
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId
     FROM (
   SELECT d.accountNumber , d.partyId
     FROM act_cust_ethan_static d
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END act_cust_ethan_static ;

PROCEDURE act_cust_techenq IS
   l_pool VARCHAR2(29) := 'ACT_CUST_TECHENQ' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   insert into act_cust_techenq
        select accountNumber, partyId
          from (select
                /*+ full(bba) parallel(bba, 8) full(subs) parallel(subs, 6) full(bpp) parallel(bpp, 8) */
                distinct bba.accountNumber,
                         subs.partyId,
                         sum(case
                               when bpp.catalogueProductId in
                                    ('11090',
                                     '13522',
                                     '23540',
                                     '13425',
                                     '13646',
                                     '10136',
                                     '10116',
                                     '10140',
                                     '10142',
                                     '11090',
                                     '13686') and bpp.status = 'IN' then
                                1
                               else
                                0
                             end) over(partition by bpp.portfolioId) flag1
                  from ccsowner.bsbPortfolioProduct bpp,
                       act_cust_uk_subs    subs,
                       ccsowner.bsbBillingAccount   bba
                 where subs.accountNumber = bba.accountNumber
                   and bba.portfolioId = bpp.portfolioId
                   and subs.dtv = 'AC'
                   and subs.talk is null
                   and subs.bband is null
                   and (bpp.catalogueProductId in
                                    ('11090',
                                     '13522',
                                     '23540',
                                     '13425',
                                     '13646',
                                     '10136',
                                     '10116',
                                     '10140',
                                     '10142',
                                     '11090',
                                     '13686') and bpp.status = 'IN')) checko
         where checko.flag1 = 1
         and rownum < 30001
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId
     FROM (
   SELECT d.accountNumber , d.partyId
     FROM act_cust_techenq d
    WHERE ROWNUM < 10001
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END act_cust_techenq ;

PROCEDURE act_mob_change_subs_downgrade IS
   l_pool VARCHAR2(29) := 'ACT_MOB_CHANGE_SUBS_DOWNGRADE' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT INTO act_mob_change_subs_downgrade ( accountNumber , partyId , telephonenumber)
   SELECT /*+ parallel(bba 8) full (bba) parallel(bpo 8) full (bpo) parallel(bpp 8) full (bpp) parallel(bol 8)
              parallel(bsi 8) full (bsi) parallel(acus 8) full (acus) full (bcpe) parallel(bcpe 8)
              pq_distribute(bba hash hash) pq_distribute(bpo hash hash) pq_distribute(bpp hash hash) */
          DISTINCT bba.accountNumber , bpr.partyId, bt.combinedTelephoneNumber
     FROM ccsowner.bsbBillingAccount bba
     JOIN ccsowner.bsbPortfolioProduct bpp ON bpp.portfolioId = bba.portfolioId
     JOIN ccsowner.bsbServiceInstance bsi ON bsi.id = bpp.serviceInstanceId AND bsi.parentServiceInstanceId = bba.serviceInstanceId
     JOIN ccsowner.bsbCustomerRole bcr ON bcr.portfolioId = bba.portfolioId
     JOIN ccsowner.bsbPartyRole bpr ON bpr.id = bcr.partyroleid
     JOIN ccsowner.bsbTelephoneUsageRole btur ON btur.serviceInstanceId = bsi.id
     JOIN ccsowner.bsbTelephone bt ON bt.id = btur.telephoneId
    WHERE bpp.status = 'AC'
      AND bsi.serviceInstanceType = 620
      AND bpp.catalogueProductId IN ( '14610' , '14611' )
      --AND bba.createdby != 'sky-mobile-sales'
      AND btur.effectiveToDate IS NULL
      AND bt.combinedTelephoneNumber != '07999999'
      AND NVL ( bcr.debtIndicator , 0 ) != 1  -- 1 = customer is in debt arrears.
      AND NOT EXISTS (
          SELECT /*+ parallel(po1 8) */ NULL
            FROM ccsowner.bsbPortfolioOffer po1
            LEFT OUTER JOIN refdatamgr.bsbOffer ro ON ro.id = po1.offerId
           WHERE po1.portfolioId = bba.portfolioId  -- join
             AND (
                     ro.id IS NULL  -- non-existent offer, has no match in refdata.
                  OR (
                          po1.status = 'PTM'  -- pending terminate offer, gives stan shop errors 'One or more offers is pending termination' 'Canot perform this action due to pending changes'.
                      AND SYSDATE BETWEEN ro.offerValidFromDate AND ro.offerValidToDate
                     )
                 )
          )
   ;
   DELETE FROM act_mob_change_subs_downgrade t
    WHERE t.accountNumber IN (
          SELECT s.accountNumber
            FROM act_mob_change_subs_downgrade s
           GROUP BY s.accountNumber
          HAVING COUNT(*) > 1
          )
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.telephoneNumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.telephoneNumber
     FROM (
   SELECT *
     FROM act_mob_change_subs_downgrade
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END act_mob_change_subs_downgrade ;

PROCEDURE act_mobile_billed IS
   -- 23-Sep-2022 Andrew Fraser limited to 100k rows to improve performance.
   l_pool VARCHAR2(29) := 'ACT_MOBILE_BILLED' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   FOR i IN 1..6 LOOP
      execute immediate '
      INSERT INTO act_mobile_billed t ( t.accountNumber , t.partyId , t.id , t.mobile_number )
        WITH ac AS (
             SELECT d.accountNumber , d.partyId , d.serviceInstanceId , d.combinedTelephoneNumber
               FROM act_mobile_numbers d
              WHERE ROWNUM <= 100000
             )
      SELECT DISTINCT ac.accountNumber
           , ac.partyId
           , ac.serviceInstanceId AS id
           , ac.combinedTelephoneNumber AS mobile_number
        FROM ac
        JOIN external_id_acct_map@adm eiam ON ac.accountNumber = eiam.external_id
        JOIN bill_invoice@cus0' || TO_CHAR ( i ) || ' b ON eiam.account_no = b.account_no  
       WHERE eiam.external_id_type = 1
         AND b.prep_status = 1
         AND ( b.prep_error_code IS NULL OR b.prep_error_code = 0 )
         AND b.backout_status = 0
         AND NOT ( b.special_code = -1 AND b.format_status != 2 )
      ' ;
      logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted for cus0' || TO_CHAR ( i ) ) ;
      COMMIT ;
   END LOOP ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.id , t.data )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.id , s.data
     FROM (
           SELECT d.accountNumber , d.partyId , d.id , d.mobile_number AS data
             FROM act_mobile_billed d
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 300000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END act_mobile_billed ;

PROCEDURE act_mobile_customers IS
   l_pool VARCHAR2(29) := 'ACT_MOBILE_CUSTOMERS' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.id , t.portfolioId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.serviceInstanceId AS id , s.portfolioId , s.messoToken
     FROM (
   SELECT d.accountNumber , d.partyId , d.serviceInstanceId , d.portfolioId , d.messoToken
     FROM act_mobile_numbers d
    WHERE d.messoToken IS NOT NULL
      and d.messotoken not like '%NO-NSPROFILE' -- ignore customers with no nsprofileid
      AND d.accountNumber NOT IN (
          SELECT bba.accountNumber
            FROM ccsowner.bsbBillingAccount bba
            JOIN ccsowner.bsbMobileBlacklist bmb ON bba.id = bmb.billingAccountId
          )
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END act_mobile_customers ;

PROCEDURE act_mobile_numbers IS
   -- 23-Sep-2022 Andrew Fraser increased dop to 16 (was 8)
   -- 09/10/23 (RFA) joined to CUSTOMERS table to retrieve "mobile" customers and messotoken in one step
   l_pool VARCHAR2(29) := 'ACT_MOBILE_NUMBERS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO act_mobile_numbers t ( t.accountNumber , t.partyId , t.mobile_number , t.productElementId
        , t.combinedTelephoneNumber , t.serviceInstanceId , t.portfolioId, t.messoToken )
   WITH v AS (
   SELECT /*+ parallel(bba 16)  pq_distribute(bba hash hash)  full(bba)
              parallel(bpp 16)  pq_distribute(bpp hash hash)  full(bpp)
              parallel(bsi 16)  pq_distribute(bsi hash hash)  full(bsi)
              parallel(btur 16) pq_distribute(btur hash hash) full(btur)
              parallel(bt 16)   pq_distribute(bt hash hash)   full(bt)
              parallel(bcpe 16) pq_distribute(bcpe hash hash) full(bcpe)
              parallel(cus 16)  pq_distribute(cu hash hash)   full(cus)
          */
          DISTINCT bba.accountNumber 
                 , cus.partyId 
                 , bcpe.id
                 , bt.combinedTelephoneNumber 
                 , bsi.id AS serviceInstanceId
                 , bba.portfolioId
                 , cus.messoToken
     FROM ccsowner.bsbBillingAccount bba
     JOIN ccsowner.bsbPortfolioProduct bpp ON bba.portfolioId = bpp.portfolioId
     JOIN ccsowner.bsbServiceInstance bsi ON bpp.serviceInstanceId = bsi.id AND bba.serviceInstanceId = bsi.parentServiceInstanceId
     JOIN ccsowner.bsbTelephoneUsageRole btur ON btur.serviceInstanceId = bsi.id
     JOIN ccsowner.bsbTelephone bt ON btur.telephoneId = bt.id
     JOIN ccsowner.bsbCustomerProductElement bcpe ON bpp.id = bcpe.portfolioProductId
     JOIN customers cus ON bba.accountNumber = cus.accountnumber
    WHERE bpp.status = 'AC'
      AND bsi.serviceInstanceType = 620
      AND cus.mobile = 1 
      AND btur.effectiveToDate IS NULL
      AND bt.combinedTelephoneNumber != '07999999'
      AND NOT EXISTS (
          SELECT /*+ parallel(bol 16) pq_distribute(bol hash hash) */ NULL
            FROM ccsowner.bsbOrderLine bol
           WHERE bol.serviceInstanceId = bsi.id
             AND bol.action = 'PI'
             AND bol.status = 'SALCON'
          )
      AND bba.created < ADD_MONTHS ( SYSDATE , -24 )
      --AND ROWNUM < 100000  -- commented out 22-Sep-2022 Andrew Fraser to get extra data into child pool 05.act_mobile_customers_no_debt
   )
   SELECT 
          v.accountNumber 
        , v.partyId 
        , '4476' || TO_CHAR ( mob_num_seq.NEXTVAL ) AS mobile_number 
        , v.id AS productElementId
        , v.combinedTelephoneNumber 
        , v.serviceInstanceId  
        , v.portfolioId
        , v.messoToken
     FROM v
   ;
   COMMIT ;

   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.data , t.telephoneNumber , t.id )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId
        , s.mobile_number AS data , s.combinedTelephoneNumber AS telephoneNumber , s.productElementId AS id
     FROM (
           SELECT d.accountNumber , d.partyId , d.mobile_number , d.combinedTelephoneNumber , d.productElementId
             FROM act_mobile_numbers d
            ORDER BY dbms_random.value
          ) s
    --WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END act_mobile_numbers ;

PROCEDURE refCancellationReason IS
   l_pool VARCHAR2(29) := 'REFCANCELLATIONREASON' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.id , t.code , t.description )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.id , s.code , s.description
     FROM (
         SELECT cr.id
              , cr.code
              , cr.description
           FROM refdatamgr.bsbCancellationReason cr
          WHERE cr.rdmDeletedFlag = 'N'
          START WITH cr.id IN (
                SELECT a.id
                  FROM (
                        SELECT cr2.id
                          FROM refdatamgr.bsbCancellationReason cr2
                         WHERE cr2.parentId IS NULL
                           AND cr2.rdmDeletedFlag = 'N'
                         ORDER BY cr2.description
                       ) a
               )
        CONNECT BY cr.parentId = PRIOR cr.id
          ORDER SIBLINGS BY cr.description ASC  -- Order sibling hierarchy alphabetically.
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END refCancellationReason ;

PROCEDURE refSponsors IS
   l_pool VARCHAR2(29) := 'REFSPONSORS' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.code )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.code
     FROM (
         SELECT bs.code
            FROM refdatamgr.bsbSponsors bs
           WHERE bs.active = 'Y'
           ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END refSponsors ;

PROCEDURE refDiallerOutcome IS
   l_pool VARCHAR2(29) := 'REFDIALLEROUTCOME' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.id , t.code , t.description )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.id , s.code , s.description
     FROM (
         SELECT dio.id , dio.code , dio.description
           FROM refdatamgr.bsbDiallerOutcome dio
          WHERE dio.rdmdeletedflag = 'N'
          START WITH dio.id IN (
                SELECT b.id
                  FROM (
                        SELECT a.id
                          FROM refdatamgr.bsbdialleroutcome a
                         WHERE a.parentid IS NULL
                          AND a.rdmdeletedflag = 'N'
                        ORDER BY a.description
                       ) b
                )
        CONNECT BY dio.parentid = PRIOR dio.id
          ORDER SIBLINGS BY dio.description ASC  -- Order sibling hierarchy alphabetically.
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END refDiallerOutcome ;

PROCEDURE refVisitCancellationCodes IS
   l_pool VARCHAR2(29) := 'REFVISITCANCELLATIONCODES' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.id , t.code , t.description )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.id , s.code , s.description
     FROM (
         SELECT cr.id , cr.code , cr.description
           FROM refdatamgr.bsbvisitcancellationcodes cr
          WHERE cr.rdmdeletedflag = 'N'
          START WITH cr.id IN (
                SELECT b.id
                 FROM (
                       SELECT a.id , a.description
                         FROM refdatamgr.bsbvisitcancellationcodes a
                        WHERE a.parentid IS NULL
                          AND a.rdmdeletedflag = 'N'
                        ORDER BY a.description
                      ) b
                )
        CONNECT BY cr.parentid = PRIOR cr.id
          ORDER SIBLINGS BY cr.description ASC  -- Order sibling hierarchy alphabetically.
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END refVisitCancellationCodes ;

PROCEDURE act_trpl_ply_cust_lt IS
   l_pool VARCHAR2(29) := 'ACT_TRPL_PLY_CUST_LT' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO act_trpl_ply_cust_lt ( accountNumber , partyId )
   select /*+ parallel(acus, 8) parallel(p, 8) parallel(t, 8) */ distinct acus.accountNumber , acus.partyId
     from act_cust_uk_subs acus
        , ccsowner.person p
        , ccsowner.bsbcustomertenurecache t
    where dtv = 'AC'
      and talk = 'A'
      and bband = 'AC'
      and acus.partyId = p.partyId
      and acus.partyId = t.customerpartyId
      and t.tenurestartdate < sysdate - 1825
      and rownum <= 20000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId
     FROM (
   SELECT *
     FROM act_trpl_ply_cust_lt d
    WHERE d.accountNumber NOT IN ( SELECT ni.accountNumber FROM act_trpl_ply_cust_iss_lt ni )
      AND ROWNUM <= 200000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END act_trpl_ply_cust_lt ;

PROCEDURE addOpenUpdateCase IS
   l_pool VARCHAR2(29) := 'ADDOPENUPDATECASE' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */INTO addOpenUpdateCase t ( t.accountNumber , t.billingAccountId , t.partyId , t.postcode , t.houseNumber
        , t.emailAddress , t.combinedTelephoneNumber )
   SELECT /*+ parallel(8) */ a.accountNumber , a.billingAccountId , a.partyId , a.postcode , a.houseNumber
        , a.emailAddress , a.combinedTelephoneNumber
     FROM act_uk_cust_idnv a
    WHERE a.accountNumber NOT IN (
          SELECT ctxt.externalId
            FROM caseManagement.bsbCmContext@cse ctxt
           WHERE ctxt.type = 'ACCOUNTNUMBER'
             AND ctxt.created > SYSDATE - 75
         )
     AND ROWNUM <= 100000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.billingAccountId , t.partyId , t.postcode , t.street
        , t.emailAddress , t.telephoneNumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.billingAccountId , s.partyId , s.postcode
        , s.houseNumber AS street , s.emailAddress , s.combinedTelephoneNumber
     FROM (
   SELECT d.accountNumber , d.billingAccountId , d.partyId , d.postcode , d.houseNumber , d.emailAddress , d.combinedTelephoneNumber
     FROM addOpenUpdateCase d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END addOpenUpdateCase ;

PROCEDURE avBroadbandServiceCheckId IS
   l_pool VARCHAR2(29) := 'AVBROADBANDSERVICECHECKID' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   insert /*+ append */ into avBroadbandServiceCheckId
   select /*+ parallel(bas 8) */ bas.id
     from avsnapshot.bsbavservice bas
    where servicetype = 'BROADBAND'
      and rownum <= 20000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.id )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.id
     FROM (
   SELECT d.id
     FROM avBroadbandServiceCheckId d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END avBroadbandServiceCheckId ;

PROCEDURE book_appointments IS
   l_pool VARCHAR2(29) := 'BOOK_APPOINTMENTS' ;
   /*
   cbs-appointments-service test ReST book appointments.
   Based on ActiveCustomer datapool but excluding customers who live in an FRU area that does all day appointments, rather than am or pm.
   For Stuart Kerr.
   Noparallel better if just getting few thousand rows.
   Postcode area + district used to cover for e.g. Campbletown (PA28) which is all day appointments only, compared to Paisley (PA1) which allows AM/PM appointments.
   https://en.wikipedia.org/wiki/Postcodes_in_the_United_Kingdom#Formatting
   Last 3 characters of postcode are always postcode sector (1 char) and postcode unit (2 char).
   So to get postcode area (1-2 char) + postcode district (2-4 char), chop off the last 3 characters of the postcode, using SUBSTR ( d.postcode , 1 , LENGTH ( d.postcode ) - 3 ).
   */
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.postcode , t.prefix )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.postcode , s.prefix
     FROM (
   WITH ampm AS (
      SELECT /*+ noparallel */ pc.postCode AS area_plus_district
        FROM refdatamgr.v_bsbFruPostCodes pc
        JOIN refdatamgr.bsbFru f ON pc.fruid = f.id
        JOIN refdatamgr.bsbFruToAppointBandProfile a ON a.fruId = f.id
        JOIN refdatamgr.bsbAppointmentBandProfile abp on abp.id = a.appointmentBandProfileId
       WHERE f.rdmDeletedFlag = 'N'
         AND a.rdmDeletedFlag = 'N'
         AND f.installerRegionId != 'VIP'  -- vip customers (only) can often get am/pm appointments regardless of area
         AND abp.appointmentBandProfile = 'AM PM'
   ) , dd AS (
      SELECT /*+ noparallel */
             d.accountNumber
           , d.partyId
           , d.postcode
           , d.contactorId
        FROM act_uk_cust d
        JOIN ampm ON ampm.area_plus_district = SUBSTR ( d.postcode , 1 , LENGTH ( d.postcode ) - 3 )
         AND ROWNUM <= 10000
   )
   SELECT dd.accountNumber
        , dd.partyId
        , dd.postcode
        , ad.dpSuffix AS prefix
     FROM dd
     JOIN ccsowner.bsbContactAddress ca ON dd.contactorId = ca.contactorId
     JOIN ccsowner.bsbaddress ad ON ca.addressId = ad.id
    WHERE ca.primaryFlag = 1  -- customers primary address
      AND ca.notCurrent IS NULL  -- customers current address check 1
      AND ca.deletedFlag = 0  -- not logically deleted
      AND ca.effectiveToDate IS NULL  -- customers current address check 2
      -- Added the following to limit the data returned to be SDUs only
      and dd.postcode in ( select postcode from dataprov.postcodes_paf where res_type = 'SDU' )
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END book_appointments ;

PROCEDURE cca_active_plans IS
-- includes datapool cca_active_plans_for_cancel within procedure cca_active_plans
   l_pool VARCHAR2(29) := 'CCA_ACTIVE_PLANS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ||' reuse storage' ;
   FOR i IN 1..6
   LOOP
      execute immediate '
         INSERT /*+ append */ INTO cca_active_plans ( agreement_ref , external_id , partyId )
         select  /*+ parallel(8) */ agreement_ref , external_id , partyId
           from extn_payment_plan_summary@cus0' || TO_CHAR(i) || ' epps
              , external_id_acct_map@adm eiam
              , ccsowner.bsbBillingAccount bba
              , ccsowner.bsbCustomerRole bcr
              , ccsowner.bsbPartyRole bpr
          where epps.account_no = eiam.account_no
            and eiam.external_id = bba.accountNumber
            and bba.portfolioId = bcr.portfolioId
            and bcr.partyroleid = bpr.id
            and eiam.external_id_type = 1
            and status = 10
            and is_current = 1
            and final_repay_date > trunc(sysdate)
      ' ;
      logger.write ( 'cca_active_plans cus0'||to_char(i)||' : '||to_char(sql%rowcount)||' rows inserted' ) ;
      COMMIT ;
   END LOOP ;
   sequence_pkg.seqBefore ( i_pool => 'CCA_ACTIVE_PLANS_FOR_CANCEL' ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.data )
   SELECT ROWNUM AS pool_seqno , 'CCA_ACTIVE_PLANS_FOR_CANCEL' AS pool_name , s.accountNumber , s.partyId , s.data
     FROM (
   SELECT d.external_id AS accountNumber
        , d.partyId
        , d.agreement_ref AS data
     FROM cca_active_plans d
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 1000
   ;
   sequence_pkg.seqAfter ( i_pool => 'CCA_ACTIVE_PLANS_FOR_CANCEL' , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
  
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.data )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.data
     FROM (
   SELECT d.external_id AS accountNumber
        , d.partyId
        , d.agreement_ref AS data
     FROM cca_active_plans d
    WHERE d.external_id NOT IN ( SELECT ni.accountNumber FROM dprov_accounts_fast ni WHERE ni.pool_name = 'CCA_ACTIVE_PLANS_FOR_CANCEL' )
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   --execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END cca_active_plans ;

PROCEDURE cca_unsigned_plans IS
   l_pool VARCHAR2(29) := 'CCA_UNSIGNED_PLANS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   FOR i IN 1..6
   LOOP
      execute immediate '
         INSERT /*+ append */ INTO cca_unsigned_plans t ( t.agreement_ref , t.external_id )
         SELECT epps.agreement_ref , eiam.external_id
           FROM extn_payment_plan_summary@cus0' || TO_CHAR(i) || ' epps
           JOIN external_id_acct_map@adm eiam ON epps.account_no = eiam.account_no
          WHERE eiam.external_id_type = 1
            AND epps.status = 0
            AND epps.is_current = 1
      ' ;
      COMMIT ;
   END LOOP ;
   -- 18-Feb-2022 Andrew Fraser for Michael Santos, restrict to mobile customers only NFTREL-21571.
   DELETE FROM cca_unsigned_plans t
    WHERE NOT EXISTS (
          SELECT NULL
            FROM ccsowner.bsbBillingAccount ba
            JOIN ccsowner.bsbServiceInstance si ON ba.serviceInstanceId = si.parentServiceInstanceId
           WHERE ba.accountNumber = t.external_id  -- join
             AND si.serviceInstanceType IN ( 610 , 620 )  -- mobile
          )
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.data )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.data
     FROM (
   SELECT d.external_id AS accountNumber
        , d.agreement_ref AS data
     FROM cca_unsigned_plans d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END cca_unsigned_plans ;

PROCEDURE digital_content IS
   l_pool VARCHAR2(29) := 'DIGITAL_CONTENT' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append*/ INTO digital_content ( accountNumber , id )
   select bba.accountNumber , ii.id
     from act_cust_uk_subs acuk
        , ccsowner.inventoryitems ii
        , ccsowner.bsbBillingAccount bba
    where ii.billingaccountid = bba.id
      and bba.accountNumber = acuk.accountNumber
      and acuk.dtv = 'AC'
      and ii.inventorytype = 'DIGITALCONTENT'
      and ii.created BETWEEN SYSDATE - 180 AND SYSDATE - 160
      and bba.accountNumber not in ( select ni.accountNumber from dataprov.sky_store_digital_content ni )
      and rownum <= 75000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.id )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.id
     FROM (
   SELECT d.accountNumber , d.id
     FROM digital_content d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END digital_content ;

PROCEDURE findOrders IS
   l_pool VARCHAR2(29) := 'FINDORDERS' ;
   v_last_result varchar2(10);
BEGIN

   logger.write ( 'begin' ) ;      

   data_prep_static_oms.findorders@oms ;
   execute immediate 'truncate table ' || l_pool ;
   insert /*+ append */ into findOrders ( accountNumber , partyId , billingaccountid , sourcesystem )
   SELECT /*+  parallel (ba 16) full (ba)  parallel (bcr 16) full (bcr)  parallel (pr 16) full (pr)  parallel (oms 16) full (oms) */
          oms.accountNumber
        , pr.partyId
        , ba.id
        , oms.sourcesystem
     FROM ccsowner.bsbBillingAccount ba
        , ccsowner.bsbCustomerRole bcr
        , ccsowner.bsbPartyRole pr
        , findorders@oms oms
    where bcr.portfolioId = ba.portfolioId
      and bcr.partyroleid = pr.id
      and ba.accountNumber = oms.accountNumber
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.billingAccountId , t.sourceSystem )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.billingAccountId , s.sourceSystem
     FROM (
   SELECT d.accountNumber , d.partyId , d.billingAccountId , d.sourceSystem
     FROM findOrders d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END findOrders ;

PROCEDURE act_mob_change_subs_upgrade IS
   l_pool VARCHAR2(29) := 'ACT_MOB_CHANGE_SUBS_UPGRADE' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO act_mob_change_subs_upgrade ( accountNumber , partyId )
    SELECT /*+ parallel(amn 8) parallel(bba 8) full (bba) parallel(bpp 8) full (bpp) */
           amn.accountNumber
         , amn.partyId
     from act_mobile_numbers amn,
          ccsowner.bsbBillingAccount bba,
          ccsowner.bsbPortfolioProduct bpp,
          ccsowner.bsbCustomerRole bcr
    where amn.accountNumber = bba.accountNumber
     and bba.portfolioId=bpp.portfolioId
     and bpp.portfolioId=bcr.portfolioId
     -- temp test to see if this will return data
     and bpp.catalogueProductId in ('14894','14997','14954','14998','14610')
     --and bpp.catalogueProductId in ('15510', '15230', '15494', '14997', '14207')
     and bpp.status = 'AC'
     AND NVL ( bcr.debtIndicator , 0 ) != 1
     and amn.accountNumber not in (select /*+  parallel(bba1 8) full (bba1) parallel(bpp1 8) full (bpp1) */
                                   accountNumber from ccsowner.bsbBillingAccount bba1,ccsowner.bsbPortfolioProduct bpp1
                                   where bba.portfolioId=bpp.portfolioId
                                   and bpp.catalogueProductId in ('15230','14255','16154','15391','16084')
                                   --                              100M,    1G,     5G,    10G,     25G
                                   and bpp.status IN ( 'OIP' , 'CRQ' )  -- CRQ=Cease Requested added 27-Nov-2017 Andrew Fraser request Amit More
                                   )
   ;
   COMMIT ;
   delete from act_mob_change_subs_upgrade 
   where rowid IN ( select rid
                    from (select rowid rid, 
                                 row_number() over (partition by 
                         accountnumber, partyid
                                   order by rowid) rn
                            from act_mob_change_subs_upgrade)
                   where rn <> 1);
   logger.write ( 'Deleted ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   FOR i in 1..6
   LOOP
      EXECUTE IMMEDIATE q'[
      MERGE INTO dataprov.act_mob_change_subs_upgrade t
      USING (
         SELECT /*+ parallel(am 8) parallel(bd 8) */ am.external_id , SUM ( bd.balance_due) AS sum_balance_due
           FROM arbor.customer_id_acct_map@cus0]' || TO_CHAR(i) || q'[ am
           JOIN arbor.cmf_balance@cus0]' || TO_CHAR(i) || q'[ bd ON am.account_no = bd.account_no
          WHERE am.external_id_type = 1
            AND bd.closed_date IS NULL
          GROUP BY am.external_id
      ) s ON ( t.accountNumber = s.external_id )
      WHEN MATCHED THEN UPDATE SET t.balance_due = s.sum_balance_due , t.cusdb = 'CUS0]' || TO_CHAR(i) || q'['
      ]' ;
      COMMIT ;
   END LOOP ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId
     FROM (
   SELECT d.accountNumber , d.partyId
     FROM act_mob_change_subs_upgrade d
    WHERE NVL ( d.balance_due , 0 ) <= 0  -- in credit or zero balance due, required to be allowed to upgrade in Shop.
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END act_mob_change_subs_upgrade ;

PROCEDURE soipActiveSkyGlass IS
   -- 22-Feb-2022 Andrew Fraser for Edwin Scariachin, used for cancelling Sky Glass.
   l_pool VARCHAR2(29) := 'SOIPACTIVESKYGLASS' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipActiveSkyGlass t ( t.accountNumber , t.partyId , t.messoToken )
   SELECT DISTINCT d.accountNumber , d.partyId , d.messoToken
     FROM soipActiveSubscription d
     JOIN ccsowner.bsbBillingAccount ba ON ba.accountNumber = d.accountNumber
     JOIN rcrm.service s ON ba.serviceInstanceId = s.billingserviceInstanceId
     JOIN rcrm.product p ON s.id = p.serviceId
    WHERE d.serviceType = 'SOIP'
      AND p.suid IN ( 'LLAMA_SMALL' , 'LLAMA_MEDIUM' , 'LLAMA_LARGE' )
      AND p.status = 'DELIVERED'
      AND p.eventCode = 'DELIVERED'
      AND d.billed = 1  -- 20-Jul-2022 Andrew Fraser for Humza Ismail
      AND ROWNUM <= 900000
   ;
   logger.write ( 'inserted ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   COMMIT ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   -- 24-Jun-2022 Andrew Fraser for Terence Burton, exclude customers without a telephone number.
   DELETE FROM soipActiveSkyGlass t
    WHERE NOT EXISTS (
          SELECT NULL
            FROM ccsowner.bsbContactor bc
            JOIN ccsowner.bsbContactTelephone bct ON bct.contactorId = bc.id
            JOIN ccsowner.bsbTelephone bt ON bt.id = bct.telephoneId
           WHERE bc.partyId = t.partyId
             AND bct.deletedFlag = 0
             AND SYSDATE BETWEEN bct.effectiveFromDate AND NVL ( bct.effectiveToDate , SYSDATE + 1 )
          )
   ;
   logger.write ( 'deleted for telephone numbers ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   -- 27-Jun-2022 Andrew Fraser for Ismail Humza, exclude customers without a billing address.
   DELETE FROM soipActiveSkyGlass t
    WHERE NOT EXISTS (
          SELECT NULL
            FROM ccsowner.bsbBillingAccount ba
            JOIN ccsowner.bsbBillingAddressRole bar ON bar.billingAccountId = ba.id
           WHERE ba.accountNumber = t.accountNumber
             AND bar.effectiveFrom < SYSDATE
             AND ( bar.effectiveTo IS NULL OR bar.effectiveTo > SYSDATE )
             AND bar.deletedFlag = 0
          )
   ;
   logger.write ( 'deleted for billing address ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   -- 13-Jul-2022 Andrew Fraser for Ismail Humza, exclude customers with in-flight-status on their Sky Broadband. Not having Sky Broadband at all is ok, those customers are likely using a non-Sky broadband company.
   DELETE FROM soipActiveSkyGlass t
    WHERE EXISTS (
          SELECT NULL
            FROM customers c
           WHERE c.partyId = t.partyId
             AND c.bband = 2
          )
   ;
   logger.write ( 'deleted for broadband ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken
     FROM (
           SELECT d.accountNumber , d.partyId , d.messoToken
             FROM soipActiveSkyGlass d
            ORDER BY dbms_random.value
         ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   -- truncated by dependent pool data_prep_09.soipOnlyActiveSkyGlass
   logger.write ( 'complete' ) ;
END soipActiveSkyGlass ;

END data_prep_03 ;
/
