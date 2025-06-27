CREATE OR REPLACE PACKAGE data_prep_01 AS
FUNCTION get_install_pcode ( i_accno IN dataprov.dprov_accounts_fast.accountnumber%TYPE ) RETURN VARCHAR2 ;
PROCEDURE noNetflix ;
PROCEDURE aop_recontract ;
PROCEDURE act_cust_dtv_bb_talk_prtid ;
PROCEDURE digitalCurrentMobile ;
PROCEDURE digitalBBregradeNoBurn ;
PROCEDURE digital_bb_upgrade ;
PROCEDURE bbRegrade ;
PROCEDURE digitalCurrentBBT ;
PROCEDURE returningMySkyApp ;
PROCEDURE digitalCurrentTv ;
PROCEDURE dataForCustomersVc ;
PROCEDURE incentiveOrders ;
PROCEDURE ceased_customers ;
PROCEDURE mobileSwap ;
PROCEDURE loyalty_null ;
PROCEDURE pauseSports ;
PROCEDURE activeDtv ;
PROCEDURE noDisneyPlus ;
PROCEDURE disneyPlusActivation ;
PROCEDURE mobile_cust_lt ;
PROCEDURE cachedCustomers ;
PROCEDURE avsVisiStTracking ;
PROCEDURE alan_token_test ;
PROCEDURE ulmOnboardToken ;
END data_prep_01 ;
/


CREATE OR REPLACE PACKAGE BODY data_prep_01 AS

PROCEDURE noNetflix IS
   l_pool VARCHAR2(29) := 'NONETFLIX' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO noNetflix t ( t.accountnumber , t.partyid , t.skycesa01token , t.messotoken )
   SELECT s.accountnumber , s.partyid , s.skycesa01token , s.messotoken
     FROM (
   SELECT *
     FROM customers c
    WHERE c.skyQbox = 1
      AND c.netflix = 0
      AND c.entertainment = 1
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- no outstanding balance
      AND c.countryCode = 'GBR'
      AND c.pool IS NULL
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND c.tntsports = 0 -- added at request of Nic Patte, 29/01/2020 -- moved from BT to TNT
      AND ROWNUM <= 20000  -- need some overhead extra data because some customers will not get the "data" column populated later.
    ORDER BY dbms_random.value
   ) s
   ;
   COMMIT ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   --'data' = first two digits of bank account, sql code taken from cbsservices.dal_paymentMethod.retrievePaymentMethods
   MERGE INTO noNetflix t USING (
      SELECT bba.accountNumber
           , MAX ( SUBSTR ( paymt.accountNumber , 1 , 2 ) ) AS data
        FROM ccsowner.bsbPaymentMethod paymt
        JOIN ccsowner.bsbPaymentMethodRole pmtrl ON paymt.id = pmtrl.paymentMethodId
        JOIN ccsowner.bsbBillingAccount bba ON pmtrl.billingAccountId = bba.id
       WHERE paymt.paymentMethodClassType = 'BSBDirectDebitMethod'
         AND SYSDATE BETWEEN pmtrl.effectiveFrom AND NVL ( pmtrl.effectiveTo , SYSDATE + 1000 )
       GROUP BY bba.accountNumber
   ) s ON ( t.accountNumber = s.accountNumber )
   WHEN MATCHED THEN UPDATE SET t.data = s.data
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.skycesa01token , t.messotoken , t.data )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyid , s.skycesa01token , s.messotoken , s.data
     FROM noNetflix s
    WHERE s.data IS NOT NULL
      AND ROWNUM <= 15000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE , i_flagCustomers => TRUE ) ;
   --execute immediate 'truncate table ' || l_pool ;  --this truncate process has been transfered to the dependent procedure dataprov.data_prep_11.nonetflixnoburn
   logger.write ( 'complete' ) ;
END noNetflix ;

PROCEDURE aop_recontract IS
   l_pool VARCHAR2(29) := 'AOP_RECONTRACT' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.skycesa01token , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name, s.accountnumber , s.partyid , s.skycesa01token , s.messotoken
     FROM (
   SELECT *
      FROM customers c
     where c.accountnumber
           in (select /*+ parallel(cu, 12) parallel(ba, 12) parallel(con, 12) parallel(po, 12) parallel(ro, 12) parallel(si, 12)*/ 
                      distinct cu.accountnumber
             from dataprov.customers cu, ccsowner.bsbbillingaccount ba, ccsowner.bsbContactor con, ccsowner.bsbportfoliooffer po,
                  refdatamgr.bsboffer ro, ccsowner.bsbserviceinstance si
            where ba.accountnumber = cu.accountnumber
              and po.portfolioid = cu.portfolioid
              and po.offerid = ro.ID
              and con.partyid = cu.partyid
              and po.portfolioid = si.portfolioid
              and ba.customersubtypecode not in ('RS','SS','ST','RH') -- Remove Staff & VIP
                  and si.serviceinstancetype in (210, 220, 400)
                  --and si.serviceinstancetype in (210, 220) 
              and (con.escalationcode IS NULL or con.escalationcode = 'NE')
              and ro.AGGREGATETOPRICE = 0
              and po.status = 'ACT'
              --and (po.applicationenddate < sysdate + 89 or po.applicationenddate is null)-- offer ends in next 90 days
                  and (po.applicationenddate between trunc(sysdate) and trunc(sysdate)+89 or po.applicationenddate is null) -- offer ends in next 90 days
              and (po.applicationstartdate < sysdate or po.applicationstartdate is null)
              --and cu.loyalty = 0   -- no longer needed 25/11/2020
              and (    cu.DTV = 1
                        or cu.entertainment = 1 
                  or cu.SKYSIGNATURE = 1 
                  or cu.CINEMA = 1 
                  or ( cu.COMPLETESPORTS = 1 or cu.sports = 1 ) -- sports customer
                  or ( cu.fibre = 1 or cu.bband = 1 or cu.bb12gb = 1 or cu.bbandLite = 1 ) -- BB customer
                      )   
                  and cu.countryCode = 'GBR' --check UK customers
              and ( cu.boxsets = 0 and cu.original = 0 and cu.variety = 0 ) -- legacy customers
              -- not in debt
                AND cu.accountNumber IN (SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0))
      and c.pool is null
      AND rownum < 25000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END aop_recontract ;

PROCEDURE act_cust_dtv_bb_talk_prtid IS
   -- 12-Nov-2021 Andrew Fraser for Liam Fleming/Alex Benetatos changed to NOburn because Liam running stress tests using this pool.
   -- 14-APR-2023 Alex Hyslop added emailadress to columns returned under NFTREL-22184 fro Julian / Amit
   l_pool VARCHAR2(29) := 'ACT_CUST_DTV_BB_TALK_PRTID' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.portfolioId , t.messoToken, t.EMAILADDRESS )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.portfolioId , s.messoToken, s.EMAILADDRESS
     FROM (
   SELECT *
     FROM customers c
    WHERE c.countryCode = 'GBR'
      AND c.pool IS NULL
      AND c.creationDt < TO_DATE ( '01-Jan-2010' , 'DD-Mon-YYYY' )
      AND ROWNUM <= 5000000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END act_cust_dtv_bb_talk_prtid ;

PROCEDURE digitalCurrentMobile IS
   l_pool VARCHAR2(29) := 'DIGITALCURRENTMOBILE' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.data , t.skycesa01token
        , t.messotoken , t.ssotoken , t.firstName , t.familyName , t.emailAddress , t.nsProfileId )
        select ROWNUM, l_pool, accountNumber, partyId, username, skycesa01token, messotoken, 
                ssotoken, firstName, familyName, emailAddress, nsProfileId 
        from (SELECT /*+ parallel(cu 8)  full(cu)  parallel(bba 8) full (bba) parallel(bpp 8) full (bpp) */
                     distinct cu.accountNumber, cu.partyId, cu.username, cu.skycesa01token, cu.messotoken, 
                cu.ssotoken, cu.firstName, cu.familyName, cu.emailAddress, cu.nsProfileId
                FROM dataprov.customers cu, ccsowner.bsbbillingaccount bba, ccsowner.bsbportfolioproduct bpp
            where cu.mobile = 1
              AND cu.nsProfileId IS NOT NULL  -- 20-Jul-2018 Alex Brown
             AND cu.accountNumber2 IS NULL  -- only want customers with a single billing account.
             AND cu.accountNumber IN ( SELECT /*+ parallel(da, 8) */ da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 ) -- added Archana request 20-03-20
             AND cu.countryCode = 'GBR'
             AND cu.pool IS NULL
             AND cu.mob_devices between 1 and 4
             and bba.accountnumber = cu.accountnumber
             and bba.portfolioid = bpp.portfolioid
             and bpp.status = 'DL'
             and bpp.catalogueproductid in (select ab.id
                                                  from refdatamgr.bsbcatalogueproduct ab,refdatamgr.bsbproductelement bc
                                     where ab.productdescription = bc.description
                                       and productelementtype = 'MD'
                                       and salesstatus = 'SA')
                 and bpp.portfolioid not in (select /*+ parallel(bpp2 8) full (bpp2) */ 
                                                portfolioid 
                                    from ccsowner.bsbportfolioproduct bpp2    --test changes for optimizer hints
                                   where bpp2.status = 'PRPR')
             )
    WHERE ROWNUM <= 300000
   ;
   
  update dprov_accounts_fast f
  set (name, eventid1, eventid2, street, town, postcode) = (select p.honorifictitlecode,adr.housenumber, adr.housename, adr.street, adr.town, adr.postcode
                                                            from ccsowner.BSBBILLINGADDRESSROLE a, ccsowner.BSBBILLINGACCOUNT b, ccsowner.bsbaddress  adr, ccsowner.person p
                                                            where a.billingaccountid=b.id
                                                            and a.effectiveto is null
                                                            and b.accountnumber=f.accountnumber
                                                            and a.addressid=adr.id
                                                            and f.partyid=p.partyid)
  where pool_name = 'DIGITALCURRENTMOBILE';
 

   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END digitalCurrentMobile ;

PROCEDURE digitalBBregradeNoBurn IS
-- 25-Jun-2021 Andrew Fraser added i_burn parameter to be used by data_prep_01.bbregrade because that pool was burning up all the data for suffix 909.
   l_pool VARCHAR2(29) := 'DIGITALBBREGRADENOBURN' ;
   l_telout varchar2(32) ;
   l_magicno varchar2(3) := '908'; --'660'; -- 518 supports broadband fibre magic number
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast ( pool_seqno , pool_name , accountNumber , partyId , data , skycesa01token , messotoken, ssotoken
      , firstName , familyName , emailAddress
      )
   SELECT /*+ parallel(c, 8) full(pp) parallel(pp, 8) */ ROWNUM AS pool_seqno
        , l_pool AS pool_name
        , c.accountNumber
        , c.partyId
        , c.username
        , c.skycesa01token
        , c.messotoken
        , c.ssotoken
        , c.firstName
        , c.familyName
        , c.emailAddress
     FROM customers c
     JOIN dataprov.bbregrade_portfolioid_tmp pp ON c.portfolioid = pp.portfolioid  -- must have line rental
    WHERE c.bband = 1
      AND c.fibre = 0
      --AND (c.bb12gb = 1 or c.bbandlite = 1) -- added 13/03/2019
      AND c.talk = 1
      --AND c.limaMigration NOT IN ( '2' , '3' ) ---no lima migration errors, Amit More 07/08/18 - 17/10/2018 Removed for one test by shane
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND c.countryCode = 'GBR'
      AND c.pool IS NULL  -- optional
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- no outstanding balance 24/09 Shane Venter
      -- eliminate those with a valid power of attorney
      AND c.portfolioid NOT IN (
          SELECT po.portfolioid
            FROM ccsowner.bsbpowerofattorneyrole po
           WHERE po.effectivetodate IS NULL OR po.effectivetodate > SYSDATE
          )
      AND ROWNUM <= 10000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_flagCustomers => TRUE ) ;
   FOR telinfo IN (
      SELECT d.accountNumber
        FROM dprov_accounts_fast d
       WHERE d.pool_name = l_pool
   )
   LOOP
      dynamic_data_pkg.update_cust_telno ( v_accountnumber => telinfo.accountNumber , v_suffix => l_magicno , v_telephone_out => l_telout , i_burn => FALSE ) ;
   END LOOP ;
   COMMIT ;
   logger.write ( 'complete' ) ;
END digitalBBregradeNoBurn ;

PROCEDURE digital_bb_upgrade IS
   l_pool VARCHAR2(29) := 'DIGITAL_BB_UPGRADE' ;
   l_telout varchar2(32) ;
   l_magicPcode varchar2(7) := 'NW107FU' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast ( pool_seqno , pool_name , accountNumber , partyId , data , skycesa01token , messotoken, ssotoken
          , firstName , familyName , emailAddress, postcode
          )
   SELECT ROWNUM
        , l_pool
        , c.accountNumber
        , c.partyId
        , c.username
        , c.skycesa01token
        , c.messotoken
        , c.ssotoken
        , c.firstName
        , c.familyName
        , c.emailAddress
        , l_magicPcode
      FROM customers c
     WHERE ( c.boxSets = 1 OR c.cinema = 1 OR c.sports = 1  )
       AND c.entertainment = 1
       AND c.bband = 0
       AND c.talk = 0
       AND c.bb12gb = 0
       AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
       AND ( c.skyQBox = 1 OR c.skyHDBox = 1 )  -- to be upgraded to BoxSets, must have a box capable of downloading.
       -- Removed 10/09/20 at request of Thomas Owen
       --AND c.limaMigration NOT IN ( '2' , '3' )  -- no lima migration errors, Alex Benetatos 06-Feb-2018 nftrel-11886
       AND c.accountNumber IN ( SELECT da.accountNumber FROM debt_amount da WHERE da.balance <= 0 )  -- 26-Mar-2018 Andrew Fraser request Nic Patte.
       -- eliminate those with a valid power of attorney
       AND c.portfolioid NOT IN (
           SELECT po.portfolioid
             FROM ccsowner.bsbpowerofattorneyrole po
            WHERE ( po.effectivetodate IS NULL OR po.effectivetodate > sysdate )
           )
       AND EXISTS (  -- 05-Apr-2018 Andrew Fraser only include customers who have a home landline telephone, request Nic Patte .
                   SELECT NULL
                     FROM ccsowner.bsbContactor con
                     JOIN ccsowner.bsbContactTelephone ct ON ct.contactorId = con.id
                    WHERE con.partyId = c.partyId  -- join
                      AND ct.typeCode = 'H'
                      AND ct.deletedFlag = 0  -- not deleted
                      AND ct.effectiveToDate IS NULL   -- could add: or < SYSDATE + could also check effectiveFromDate
                  )
       AND EXISTS (  -- 07-JUL-2020 Alex Hyslop only include customers who do not have an escalation against the account. For Thomas Owen
                   SELECT NULL
                     FROM ccsowner.bsbContactor con
                    WHERE con.partyId = c.partyId  -- join
                      AND (con.escalationcode IS NULL or con.escalationcode = 'NE')
                  )
       AND c.countryCode = 'GBR'
       AND c.accountNumber IN (
           SELECT a.accountnumber
             FROM dataprov.act_cust_uk_subs a
            WHERE a.bband IS NULL
           )
       AND c.emailAddress NOT LIKE 'noemail%'  -- 19-Aug-2019 only include customers who have a valid email, request Shane Venter.
       AND ROWNUM <= 2000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_flagCustomers => TRUE ) ;
   FOR telinfo IN (
      SELECT daf.accountNumber
        FROM dataprov.dprov_accounts_fast daf
       WHERE daf.pool_name = l_pool
   )
   LOOP
      UPDATE ccsowner.bsbaddress badd
          SET badd.postcode = l_magicpcode
        WHERE badd.id IN (
              SELECT bt.id
                FROM ccsowner.bsbaddress          bt,
                     ccsowner.bsbcontactaddress   bct,
                     ccsowner.bsbcontactor        bc,
                     ccsowner.bsbpartyrole        bpr,
                     ccsowner.bsbcustomerrole     bcr,
                     ccsowner.bsbbillingaccount   ba
               WHERE bt.id = bct.addressid
                 AND bc.id = bct.contactorid
                 AND bpr.partyid = bc.partyid
                 AND bcr.partyroleid = bpr.id
                 AND ba.portfolioid = bcr.portfolioid
                 AND bct.effectivetodate IS NULL
                 AND ba.accountnumber = telinfo.accountNumber
             )
      ;
      UPDATE ccsowner.bsbaddress badd
         SET badd.postcode = l_magicpcode
       WHERE badd.id IN (
             SELECT bad.id
               FROM ccsowner.bsbbillingaccount   ba,
                    ccsowner.bsbserviceinstance  bsi,
                    ccsowner.bsbaddressusagerole bar,
                    ccsowner.bsbaddress          bad
              WHERE ba.accountnumber = telinfo.accountNumber
                AND bar.effectiveto IS NULL
                AND bsi.parentserviceinstanceid = ba.serviceinstanceid
                AND bar.serviceinstanceid = ba.serviceinstanceid
                AND bad.id = bar.addressid
             )
      ;
   END LOOP ;
   logger.write ( 'complete' ) ;
END digital_bb_upgrade ;

PROCEDURE bbRegrade IS
/*
04-Aug-2021 Andrew Fraser for Ross Benton added 'code' = first two digits of bank account, sql code taken from cbsservices.dal_paymentMethod.retrievePaymentMethods
25-Jun-2021 Andrew Fraser added i_burn parameter to be used by data_prep_01.bbregrade because that pool was burning up all the data for suffix 909.
*/
   l_pool VARCHAR2(29) := 'BBREGRADE' ;
   l_telout varchar2(32) ;
   l_magicno varchar2(3) := '909'; --901-- 660 supports broadband fibre magic number
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO bbRegrade t ( t.accountNumber , t.partyId , t.data , t.skycesa01token , t.messotoken , t.ssotoken
      , t.firstName , t.familyName , t.emailAddress , t.code )
   SELECT /*+ parallel(c, 8) full(pp) parallel(pp, 8) */
          c.accountNumber
        , c.partyId
        , c.username
        , c.skycesa01token
        , c.messotoken
        , c.ssotoken
        , c.firstName
        , c.familyName
        , c.emailAddress  -- needs added to main sql.
        , v.code
     FROM dataprov.customers c
     JOIN dataprov.bbRegrade_portfolioId_tmp pp ON c.portfolioId = pp.portfolioId  -- must have line rental
     JOIN (
           SELECT bba.accountNumber
                , MAX (pm.accountNumber) AS code
             FROM ccsowner.bsbPaymentMethod pm
             JOIN ccsowner.bsbPaymentMethodRole pmr ON pm.id = pmr.paymentMethodId
             JOIN ccsowner.bsbBillingAccount bba ON pmr.billingAccountId = bba.id
            WHERE pm.paymentMethodClassType = 'BSBDirectDebitMethod'
              AND pm.deletedFlag = 0  -- 09-Mar-2022 Andrew Fraser for Terence Burton NFTREL-21621 + 3 lines below
              AND pmr.deletedFlag = 0
              AND ( pmr.effectiveTo > SYSDATE + 1 OR pmr.effectiveTo IS NULL )
              AND ( pm.cardExpiryDate > SYSDATE + 1 OR pm.cardExpiryDate IS NULL )
            GROUP BY bba.accountNumber
          ) v ON v.accountNumber = c.accountNumber
    WHERE c.bband = 1
      AND c.fibre = 0
      AND c.bbsuperfast = 0
      AND c.bbultrafast = 0
      AND c.talk = 1
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND c.countryCode = 'GBR'
      AND c.pool IS NULL  -- optional
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )
      AND c.portfolioid NOT IN (
          SELECT po.portfolioid
            FROM ccsowner.bsbPowerOfAttorneyRole po
           WHERE po.effectiveToDate IS NULL OR po.effectiveToDate > SYSDATE
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
             AND bct.primaryFlag = 1
             AND bct.typeCode = 'M'
             AND LENGTH ( bt.combinedTelephoneNumber ) = 11
             AND bt.combinedTelephoneNumber LIKE '07%'
          )
      -- Below to avoid sps app error "PIMM rule ID RULE_DO_NOT_ALLOW_SOIP_SALE_ON_INFLIGHT_ORDERS has resulted in a Do Not Allow outcome."
      AND NOT EXISTS (
          SELECT NULL
            FROM ccsowner.bsbPortfolioProduct pp
           WHERE pp.portfolioId = c.portfolioId
             AND pp.status = 'PC'  -- Pending Cancel
          )
      -- Below to avoid sps app error "PIMM rule ID RULE_DO_NOT_ALLOW_POST_ACTIVE_CANCEL_WITH_IN_FLIGHT_VISIT has resulted in a Do Not Allow outcome."
      AND NOT EXISTS (
          SELECT NULL
            FROM ccsowner.bsbVisitRequirement bvr
            JOIN ccsowner.bsbAddressUsageRole baur ON bvr.installAtionAddressRoleId = baur.id
            JOIN ccsowner.bsbServiceInstance bsi ON baur.serviceInstanceId = bsi.id
           WHERE bsi.portfolioId = c.portfolioId
             AND bvr.statusCode NOT IN ( 'CP' , 'CN' )
             -- Complete/Cancelled are ok. UB and BK are definitely a problem, but could add filters for visit_date. List is:
             -- select code,codeDesc from refdatamgr.picklist where codeGroup = 'VisitRequirementStatus' order by 1 ;
          )
      AND ROWNUM <= 20000
   ;
   logger.write ( 'rows inserted ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   COMMIT ;
   FOR telinfo IN (
      SELECT d.accountNumber
        FROM bbRegrade d
   )
   LOOP
      -- 25-Jun-2021 Andrew Fraser added i_burn parameter to be used by data_prep_01.bbregrade because that pool was burning up all the data for suffix 909.
      dynamic_data_pkg.update_cust_telno ( v_accountnumber => telinfo.accountNumber , v_suffix => l_magicno
          , v_telephone_out => l_telout , i_burn => FALSE
          ) ;
   END LOOP ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
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
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE , i_flagCustomers => TRUE ) ;
   -- If truncating: soipDigitalExistLanding in 04 depends on bbRegrade table.
   logger.write ( 'complete' ) ;
END bbRegrade ;

PROCEDURE digitalCurrentBBT IS
   l_pool VARCHAR2(29) := 'DIGITALCURRENTBBT' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast ( pool_seqno , pool_name , accountNumber , partyId , data , skycesa01token , messotoken, ssotoken
      , firstName , familyName , emailAddress
      )
   SELECT ROWNUM
        , l_pool
        , c.accountNumber
        , c.partyId
        , c.username
        , c.skycesa01token
        , c.messotoken
        , c.ssotoken
        , c.firstName
        , c.familyName
        , c.emailAddress
     FROM customers c
    WHERE c.bband = 1
      AND c.talk = 1
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND c.countryCode = 'GBR'
      AND c.pool IS NULL  -- optional
      AND ROWNUM <= 300000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END digitalCurrentBBT ;

PROCEDURE returningMySkyApp IS
   l_pool VARCHAR2(29) := 'RETURNINGMYSKYAPP' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT /*+ append */ INTO dataprov.dprov_accounts_fast ( pool_seqno , pool_name , accountNumber , partyId , data , skycesa01token
      , messotoken, ssotoken , firstName , familyName , emailAddress
      )
   SELECT ROWNUM
        , l_pool
        , c.accountNumber
        , c.partyId
        , c.username
        , c.skycesa01token
        , c.messotoken
        , c.ssotoken
        , c.firstName
        , c.familyName
        , c.emailAddress
     FROM customers c
    WHERE c.dtv = 1
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND c.countryCode = 'GBR'
      AND c.pool IS NULL  -- optional
      AND ROWNUM <= 300000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END returningMySkyApp ;

FUNCTION get_install_pcode ( i_accno IN dataprov.dprov_accounts_fast.accountnumber%TYPE ) RETURN varchar2
   -- public function called by digitalCurrentTv
   -- needs to be public to avoid "PLS-00231: Function may not be used in SQL"
AS
   l_pcode dataprov.dprov_accounts_fast.postcode%type := '';
BEGIN
  SELECT v.installation_postcode
    INTO l_pcode
    FROM (
          SELECT iaddr.postcode AS installation_postcode
               , DENSE_RANK() OVER ( PARTITION BY iar.serviceinstanceid ORDER BY iar.effectiveto DESC NULLS FIRST , iar.effectivefrom DESC , iar.id DESC ) AS iar_rnk 
            FROM ccsowner.bsbbillingaccount ba
            JOIN ccsowner.bsbserviceinstance si ON ba.serviceinstanceid = si.id
            JOIN ccsowner.bsbaddressusagerole iar ON si.id = iar.serviceinstanceid
            JOIN ccsowner.bsbaddress iaddr ON iar.addressid = iaddr.id
           WHERE ba.accountnumber = i_accno
         ) v
    WHERE v.iar_rnk <= 1
   ;
   RETURN l_pcode ;
END get_install_pcode ;

PROCEDURE digitalCurrentTv IS
   l_pool VARCHAR2(29) := 'DIGITALCURRENTTV' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast ( pool_seqno , pool_name , accountNumber , partyId , data , skycesa01token , messotoken, ssotoken
      , firstName , familyName , emailAddress, postcode
      )
   SELECT ROWNUM
        , l_pool
        , c.accountNumber
        , c.partyId
        , c.username
        , c.skycesa01token
        , c.messotoken
        , c.ssotoken
        , c.firstName
        , c.familyName
        , c.emailAddress
        , get_install_pcode ( i_accno => c.accountNumber )  -- public function in this package
     FROM customers c
    WHERE c.dtv = 1
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND c.countryCode = 'GBR'
      AND c.pool IS NULL  -- optional
      AND ROWNUM <= 300000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END digitalCurrentTv ;

PROCEDURE dataForCustomersVc IS
   l_pool VARCHAR2(29) := 'DATAFORCUSTOMERSVC' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   execute immediate 'truncate table ' || l_pool ;
   insert /*+ append */ into dataforcustomersvc
   select
       /*+ full(ba) parallel(ba, 16)
       full(cr) parallel(cr, 16)
       full(pr) parallel(pr, 16)
       full(co) parallel(co, 16)
       full(ca) parallel(ca, 16)
       full(a) parallel(a, 16)
       full(ct) parallel(ct, 16)
       full(t) parallel(t, 16)
       full(ce) parallel(ce, 16)
       full(e) parallel(e, 16)
       full(person) parallel(person, 16)
        */
       distinct ba.accountnumber , pr.partyid , a.postcode
         FROM ccsowner.bsbbillingaccount   ba,
              ccsowner.bsbcustomerrole     cr,
              ccsowner.bsbpartyrole        pr,
              ccsowner.bsbcontactor        co,
              ccsowner.bsbcontactaddress   ca,
              ccsowner.bsbaddress          a,
              ccsowner.bsbcontacttelephone ct,
              ccsowner.bsbtelephone        t,
              ccsowner.bsbcontactemail     ce,
              ccsowner.bsbemail            e,
              ccsowner.person              person
        WHERE cr.portfolioid = ba.portfolioid
          AND pr.id = cr.partyroleid
          AND co.partyid(+) = pr.partyid
          AND person.partyid = pr.partyid
          AND co.id = ca.contactorid(+)
          AND a.id(+) = ca.addressid
          AND ca.deletedflag(+) = 0
          AND sysdate between ca.effectivefromdate(+) AND nvl(ca.effectivetodate(+), sysdate + 1)
          AND co.id = ct.contactorid(+)
          AND t.id(+) = ct.telephoneid
          AND sysdate between ct.effectivefromdate(+) AND nvl(ct.effectivetodate(+), sysdate + 1)
          AND ct.deletedflag(+) = 0
          AND co.id = ce.contactorid(+)
          AND e.id(+) = ce.emailid
          AND ce.deletedflag(+) = 0
          AND sysdate between ce.effectivefromdate(+) AND nvl(ce.effectivetodate(+), sysdate + 1)
          and a.postcode is not NULL
          and rownum < 800001
   ;
   COMMIT ;
   -- 14-Aug-2018 Andrew Fraser Exclude broadband (strictly cases related to broadband) request Ryan Gilday NFTREL-14777
   DELETE FROM dataForCustomerSvc d
    WHERE d.partyId IN (
          SELECT c.partyId
            FROM customers c
           WHERE c.bband = 0
          )
   ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.postcode )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyid , s.postcode
     FROM (
   SELECT *
     FROM dataForCustomerSvc d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END dataForCustomersVc ;

PROCEDURE incentiveOrders IS
   l_pool VARCHAR2(29) := 'INCENTIVEORDERS' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast ( pool_seqno , pool_name , accountNumber )
     WITH q AS (
          SELECT DISTINCT o.customerAccountNumber
            FROM oh.resourceorders@oms r
            JOIN oh.productOrders@oms p on p.customerOrderId = r.customerOrderId
            JOIN oh.resourceOrderStatusCodes@oms c on r.roStatusCodeId = c.roStatusCodeId
            JOIN oh.productTypeCodes@oms cp on p.productTypeCodeId = cp.productTypeCodeId
            JOIN oh.customerOrders@oms o ON o.customerOrderId = r.customerOrderId
           WHERE c.roStatusCode = 'IN_PROGRESS'
             AND cp.productTypeCode = 'ELECTRONIC_VOUCHER'
          )
   SELECT ROWNUM , 'INCENTIVEORDERS' , q.customerAccountNumber
     FROM q
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END incentiveOrders ;

PROCEDURE ceased_customers IS
   -- 27/11/23 (RFA) - Use table CUSTOMERTOKENS to retrieve the MessoToken 
   l_pool VARCHAR2(29) := 'CEASED_CUSTOMERS' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   execute immediate 'truncate table ceased_customers' ;
   INSERT /* append */ INTO ceased_customers ( accountNumber , messoToken , partyId , skycesa01token )
   WITH c AS (
      SELECT /*+ parallel(8) */ DISTINCT si.portfolioId
        FROM ccsowner.bsbserviceinstance si
        JOIN ccsowner.bsbsubscription su ON su.serviceinstanceid = si.id
       WHERE si.serviceinstancetype IN ( 210 , 100 , 400 )
         AND su.status = 'CN'
         AND si.lastupdate >= SYSDATE - 730 --INTERVAL '24' MONTH
         --AND ROWNUM <= 150000
   )
   SELECT /*+ parallel (16) */ ba.accountnumber
        , ct.messotoken
        , ct.partyId
        , ct.cesatoken
     FROM ccsowner.bsbbillingaccount ba
     JOIN dataprov.customerTokens ct ON ( ct.accountNumber = ba.accountnumber )
     JOIN ccsowner.bsbcustomerrole cr ON ba.portfolioid = cr.portfolioid
     JOIN c ON c.portfolioId = ba.portfolioId
    WHERE ba.accountnumber LIKE '6%'
      AND cr.customerstatuscode = 'CRIC'
      AND cr.cancelledDate >= SYSDATE - 730 --INTERVAL '24' MONTH
      AND NOT EXISTS (
          SELECT NULL
            FROM ccsowner.bsbbillingaccount ba1
           WHERE ba1.portfolioid = ba.portfolioid
             AND ba1.accountnumber != ba.accountnumber
          )
      AND ROWNUM <= 10000
   ;
   logger.write ( 'ceased_customers : '||to_char(sql%rowcount)||' rows inserted' ) ;
   commit;
   INSERT INTO dprov_accounts_fast ( pool_seqno , pool_name , accountnumber , messotoken , partyid , skycesa01token )
   SELECT ROWNUM , l_pool , accountnumber , messotoken , partyid , skycesa01token
     FROM ceased_customers
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END ceased_customers ;


/*************************************************************************************
 -- RFA : Query changed to accommodate new requirements. Changed to use CURTOMERSV2 
*************************************************************************************/
PROCEDURE mobileSwap IS
   l_pool   VARCHAR2(29) := 'MOBILESWAP' ;
   l_count  NUMBER ;
BEGIN
   logger.write ( 'begin mobileSwap' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   execute immediate 'truncate table ' || l_pool ;
   execute immediate 'truncate table mobileswapdebt reuse storage' ;
   Insert /* append */ into mobileswap
    select * from (select /*+  full(cus)  parallel(cus, 8)
                               full(ba)   parallel(ba, 8)
                               full(serv) parallel(serv, 8)
                               full(pp)   parallel(pp, 8)
                               full(cpe)  parallel(cpe, 8)
                               full(ppa)  parallel(ppa, 8)
                               full(bpp)  parallel(bpp, 8)
                               full(bp)   parallel(bp 8) 
                               full(mcpe) parallel(mcpe 8) 
                               full(mci)  parallel(mci 8) 
                            */
           ba.accountnumber, Max(cus.messotoken), Max(cpe.portfolioproductid), Max(cpe.name) as productname, Max(cus.partyid)
           from dataprov.customersv2 cus
           join ccsowner.bsbbillingaccount ba on ( cus.accountnumber = ba.accountnumber )
           join ccsowner.bsbserviceinstance serv on ( serv.portfolioid = ba.portfolioid )
           join ccsowner.bsbportfolioproduct pp on ( pp.serviceinstanceid = serv.id )
           join ccsowner.bsbcustomerproductelement cpe on ( cpe.portfolioproductid = pp.id )
           join ccsowner.bsbpaymentplanagreement ppa on ( ppa.customerproductelementid = cpe.id )
           join refdatamgr.BSBPAYMENTPLANPRODUCT bpp on ( bpp.id = ppa.productpaymentplanid  )
           join refdatamgr.BSBPAYMENTPLANTYPE bp on ( bp.id = bpp.paymentplantypeid )
           join ccsowner.bsbmobilecustprodelement mcpe on ( mcpe.mobilecustprodelementid = cpe.id )
           join ccsowner.bsbmobileconsumerinfo mci on ( mcpe.mobileconsumerinfoid = mci.id )
         where cus.mob_devices between 1 and 4         -- The account has to have a max of 4 devices on its portfolio
           and cus.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )
           and cus.messotoken not like '%NO-NSPROFILE' -- ignore customers with no nsprofileid
           and cus.accounttype = 'SKY_MOBILE'
           and cus.mobile = 1                          -- It has a mobile tariff
           and bpp.paymentplantypeid != '21'
           and cpe.status = 'DL'
           and pp.statuschangeddate < sysdate - 150    -- The product has to be active for more than 150 days
           and bp.paymentplanspecification = 'CCA'
           and mci.swapstartdate <= trunc ( sysdate )  -- NFTREL-18466 added for release 163
           and mci.swapenddate >= trunc ( sysdate )    -- NFTREL-18466 added for release 163
        group by ba.accountnumber, cpe.lastupdate
        order by cpe.lastupdate DESC
      )
    WHERE ROWNUM <= 20000
   ;
   l_count := SQL%ROWCOUNT; 
   logger.write ('MOBILESWAP rows inserted : '|| l_count) ;
 
   --DELETE FROM mobileswap t WHERE t.rowid NOT IN ( SELECT MAX ( s.ROWID ) FROM mobileswap s GROUP BY s.accountnumber );
   --l_count := SQL%ROWCOUNT; 
   --logger.write ('MOBILESWAP rows deleted : '|| l_count) ;   
   
   -- delete from table all those accounts that have partyIds with debt
   -- Use a temp table to get the accountNumbers to be dismissed
   INSERT INTO mobileswapdebt ( partyId, accountNumber ) 
    with debt as  
        ( select cus.partyid
            from dataprov.customersv2 cus
            join dataprov.debt_amount da on ( da.accountNumber = cus.accountNumber ) 
            where da.balance > 0 
            group by partyId
        )
    select mob.partyid, mob.accountNumber    
      from dataprov.mobileswap mob 
      join debt d on ( d.partyId = mob.partyId );
   l_count := SQL%ROWCOUNT; 
   commit;                    
   logger.write ('MOBILESWAPDEBT temp table - rows inserted : '|| l_count) ;   
   
   DELETE FROM mobileswap
    WHERE accountNumber in ( select mob.accountNumber from mobileswapdebt mob );
   l_count := SQL%ROWCOUNT; 
   commit;                    
   logger.write ('MOBILESWAP - rows deleted (accounts) : '|| l_count) ;   
   
   INSERT INTO dprov_accounts_fast ( pool_seqno , pool_name , accountnumber , messotoken , portfolioproductid , productname, partyid )
   SELECT ROWNUM , l_pool , ms.accountnumber, ms.messotoken , ms.portfolioproductid , ms.productname , ms.partyid
     FROM mobileswap ms
    WHERE rownum < 10001
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => FALSE ) ; --changed to noburn for Julian 11/07/2023 (NFTREL-22248)
   logger.write ( 'complete mobileSwap' ) ;
END mobileSwap ;



PROCEDURE loyalty_null IS
   l_pool VARCHAR2(29) := 'LOYALTY_NULL' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO loyalty_null t ( t.partyId , t.accountNumber , t.username , t.nsProfileId
        , t.skyCEsa01Token , t.messoToken , t.ssoToken )
   SELECT c.partyId , c.accountNumber , c.userName , c.nsProfileId
        , c.skycesa01token , c.messoToken , c.ssoToken
     FROM customers c
    WHERE c.loyalty IS NULL  -- not yet responded either way to loyalty program. Other values are: 1 = opted-in , 0 = opted-out.
      AND (
               c.bband = 1
            OR ( c.dtv = 1 AND c.activeViewingCard = 1 )
          )
      AND c.mobile = 0
      AND NOT EXISTS (
          SELECT NULL
            FROM ccsOwner.bsbBillingAccount bba
           WHERE bba.portfolioId = c.portfolioId
             AND bba.customerTypeCode = 'NON'  -- "Non-standard", around 0.3% of customers.
          )
      AND ROWNUM <= 2000000
   ;
   COMMIT ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.data
        , t.skycesa01token , t.messoToken , t.ssoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.data
        , s.skycesa01token , s.messoToken , s.ssoToken
     FROM (
           SELECT d.accountNumber , d.partyId , d.username AS data , d.skycesa01token , d.messoToken , d.ssoToken
             FROM loyalty_null d
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 2000000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END loyalty_null ;

PROCEDURE alan_token_test IS
   l_pool VARCHAR2(29) := 'ALAN_TOKEN_TEST' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast ( pool_seqno , pool_name , accountnumber , partyid
        , skycesa01token , messotoken , ssotoken
        , firstname , familyname , emailaddress
        )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , accountnumber , partyid
        , 'T-CES-' || CASE WHEN sys_context ( 'userenv' , 'con_name' ) = 'CHORDO' THEN 'N01' ELSE 'N02' END
             || '-' || accountnumber || '-' || partyId || '-' || SUBSTR ( username , 1 , 17 ) || '-'
             || NVL ( c.nsprofileid , 'NO-NSPROFILE' ) AS skycesa01token
        , 'T-SSO-' || CASE WHEN sys_context ( 'userenv' , 'con_name' ) = 'CHORDO' THEN 'N01' ELSE 'N02' END
             || '-' || accountnumber || '-' || partyId || '-' || SUBSTR ( username , 1 , 17 ) || '-'
             || NVL ( c.nsprofileid , 'NO-NSPROFILE' ) AS ssotoken
        , 'T-MES-' || CASE WHEN sys_context ( 'userenv' , 'con_name' ) = 'CHORDO' THEN 'N01' ELSE 'N02' END
             || '-' || accountnumber || '-' || partyId || '-' || SUBSTR ( username , 1 , 17 ) || '-'
             || NVL ( c.nsprofileid , 'NO-NSPROFILE' ) AS messotoken
        , firstname , familyname , emailaddress
     FROM customers c
    WHERE c.bband = 1
      and c.fibre = 0
      and c.nsprofileid is not null
      AND c.BBSUPERFAST = 0  
      AND c.BBULTRAFAST = 0  
      AND c.talk = 1
      AND c.accountNumber2 IS NULL  
      AND c.countryCode = 'GBR'
      AND c.pool IS NULL  -- optional
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  
      AND c.portfolioid in (select /*+ full(pp) parallel(pp, 8) */ pp.portfolioid from dataprov.bbregrade_portfolioid_tmp pp)
      AND c.pool IS NULL
      AND ROWNUM <= 5000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END alan_token_test ;

PROCEDURE avsVisiStTracking IS
   l_pool VARCHAR2(29) := 'AVSVISISTTRACKING' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast ( pool_seqno , pool_name , accountNumber , partyid , portfolioId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , accountnumber , partyid , portfolioId
     FROM customers c
    WHERE c.countryCode = 'GBR'
      AND c.pool IS NULL
      AND c.accountnumber IN (
          SELECT co.customerAccountNumber
             FROM oh.resourceorders@oms ro
                 ,oh.resourceorderstatuscodes@oms rosc
                 ,oh.resourceordersubstatuscodes@oms rossc
                 ,oh.resources@oms r
                 ,oh.resourcetypecodes@oms rtc
                 ,oh.appointmentresources@oms ar
                 ,oh.appointments@oms ap
                 ,oh.customerOrders@oms co
            WHERE rosc.rostatuscodeid = ro.rostatuscodeid
              AND rossc.rosubstatuscodeid = ro.rosubstatuscodeid
              AND r.resourceid = ro.resourceid
              AND rtc.resourcetypecodeid = r.resourcetypecodeid
              AND ar.customerorderid = r.customerorderid
              AND ar.resourceid = r.resourceid
              AND ap.appointmentid = ar.appointmentid
              AND rtc.resourcetypecode IN ( 'VISIT','VISITSKY', 'SPM_EVENT', 'OR_APP_VISIT','OR_UNAPP_VISIT', 'BTI_SERV_VISIT')
              AND co.customerOrderId = ro.customerOrderId
          )
      AND ROWNUM <= 200000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END avsVisiStTracking ;

PROCEDURE cachedcustomers IS
   l_pool varchar2(29) := 'CACHEDCUSTOMERS';
   l_custcnt number := 30000;
   l_rptcnt number := 128;
BEGIN
   logger.write ('begin');
   dataprov.sequence_pkg.seqBefore (i_pool => l_pool);
   insert into dataprov.dprov_accounts_fast t (t.pool_seqno,t.pool_name,t.accountnumber,t.partyid)
   select rownum pool_seqno
          ,v.* from (
   select l_pool pool_name
          ,a.accountnumber
          ,a.partyid
   from (select c.accountnumber
               ,c.partyid 
          from dataprov.customers c 
         where pool is null and rownum <=l_custcnt) a
               ,(select level lvl from dual connect by level <=l_rptcnt) b
                order by dbms_random.value()) v;
   dataprov.sequence_pkg.seqafter (i_pool=>l_pool,i_count=>sql%rowcount) ;
   logger.write ('complete') ;
end cachedcustomers ;


PROCEDURE mobile_cust_lt IS
   l_pool VARCHAR2(29) := 'MOBILE_CUST_LT' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO mobile_cust_lt t ( t.partyId , t.accountNumber2 )    
   WITH mob_cus AS (
      SELECT cus.partyid
           , cus.accountNumber2
        FROM customers cus
        JOIN ccsowner.bsbcustomertenurecache ten ON cus.partyid = ten.customerpartyid
       WHERE ten.tenurestartdate < ADD_MONTHS ( SYSDATE , -24 )
         AND cus.mobile = 1
         AND cus.dtv = 1
         AND cus.pool IS NULL
         AND ROWNUM <= 125000
   ) , ref_id AS (
      SELECT DISTINCT ab.id
        FROM refdatamgr.bsbCatalogueProduct ab
        JOIN refdatamgr.bsbProductElement bc ON ab.productdescription = bc.description
       WHERE productElementType = 'MD'
   )
   SELECT v.partyid , v.accountnumber2
     FROM (
           SELECT mob_cus.partyid , mob_cus.accountnumber2 , bsi.id AS si_id , bpp.catalogueProductId
             FROM mob_cus 
             JOIN ccsowner.bsbbillingaccount bba ON bba.accountnumber = mob_cus.accountnumber2
             JOIN ccsowner.bsbServiceInstance bsi ON bsi.parentserviceinstanceid = bba.serviceinstanceid
             JOIN ccsowner.bsbportfolioproduct bpp ON bba.portfolioid = bpp.portfolioid
             JOIN ref_id ON ref_id.id = bpp.catalogueProductId
            WHERE bsi.serviceInstanceType = 620
          ) v
   HAVING COUNT ( DISTINCT v.si_id ) > 1 AND COUNT ( DISTINCT v.catalogueProductId ) > 1
    GROUP BY v.partyid , v.accountnumber2
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.partyId , t.accountNumber2 )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.partyId , s.accountNumber2
     FROM (
   SELECT d.partyId , d.accountNumber2
     FROM mobile_cust_lt d
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 25000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END mobile_cust_lt ;

PROCEDURE disneyPlusActivation IS
    l_pool VARCHAR2(29) := 'DISNEYPLUSACTIVATION' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.skycesa01token , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyid , s.skycesa01token , s.messotoken
     FROM (
   SELECT *
     FROM customers c
    WHERE c.disneyPlus = 1
      AND c.countryCode = 'GBR'
      AND c.pool IS NULL
      AND ROWNUM <= 5000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END disneyPlusActivation ;

PROCEDURE noDisneyPlus IS
    l_pool VARCHAR2(29) := 'NODISNEYPLUS' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.skycesa01token , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyid , s.skycesa01token , s.messotoken
     FROM (
   SELECT *
     FROM customers c
    WHERE c.disneyPlus = 0
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 ) --no outstanding balance
      AND c.countryCode = 'GBR'
      AND c.pool IS NULL
      AND c.skyqbox = 1
      AND c.dtv = 1
      AND ( c.variety = 1 OR c.skyqbundle = 1 OR c.boxsets = 1 ) -- Requirement that customers are not on Sky Essentials (NFTREL-18583)
      AND ROWNUM <= 10000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END noDisneyPlus ;

PROCEDURE activeDtv IS
   l_pool VARCHAR2(29) := 'ACTIVEDTV' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.skycesa01token , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyid , s.skycesa01token , s.messotoken
     FROM (
   SELECT *
     FROM customers c
    WHERE c.dtv = 1
      AND c.sports = 0  -- Yellow button interactive - most/all no sports yet, because this tries to sell them sports
      AND c.activeViewingCard = 1
      AND c.countryCode = 'GBR'
      AND c.pool IS NULL
      AND ROWNUM <= 300000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END activeDtv ;

PROCEDURE pauseSports IS
   l_pool VARCHAR2(29) := 'PAUSESPORTS' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.skycesa01token , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyid , s.skycesa01token , s.messotoken
     FROM (
   SELECT *
     FROM customers c
    --WHERE ( premierleague = 1 OR cricket = 1 OR football = 1 ) --17/10/19 DS not needed for sports
    WHERE c.sports = 1
      AND c.entertainment = 1
      AND completesports = 1
      AND ( c.skyQBox = 1 OR c.skyHDBox = 1 )  -- 17/10/19 DS to be upgraded to BoxSets, must have a box capable of downloading.
      AND c.limaMigration IN ('0','1','3') --17/10/19 DS
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- 09-Apr-2018 Andrew Fraser request Archana Burla.
      AND c.countryCode = 'GBR'
      AND c.pool IS NULL
      AND ROWNUM <= 200000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END pauseSports ;

PROCEDURE ulmOnboardToken IS
  l_pool VARCHAR2(29) := 'ULMONBOARDTOKEN' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'alter session set ddl_lock_timeout=60'; 
   -- Added by SM for debugging purposes
declare
v_message varchar2(3950);
v_source varchar2(50) := 'ULMONBOARDTOKEN';
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
            and object_name = 'ULMONBOARDTOKEN'); 
logger.write( v_message);           

exception
when no_data_found then 
 logger.write(v_source||' No locks found.');
end;
---   
   execute immediate 'truncate table ' || l_pool ;
   EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML' ;
   MERGE INTO ulmOnboardToken t USING (
      SELECT SUBSTR ( a.action_data , INSTR ( a.action_data , 'token?value=' ) + 12
                    , INSTR ( SUBSTR ( a.action_data , INSTR ( a.action_data , 'token?value=' ) + 12 ) , '"' ) - 1 ) AS ulmToken
           , SUBSTR ( a.action_data , INSTR ( a.action_data , 'partyId":"' ) + 10
                    , INSTR ( SUBSTR ( a.action_data , INSTR ( a.action_data , 'partyId":"' ) + 10 ) , '"' ) - 1 ) AS partyId
           , a.expiry_date
        FROM mint_platform.mt_action_token@ulm a
       WHERE a.expiry_date >= 1 + SYSTIMESTAMP
   ) s ON ( s.partyId = t.partyId )
   WHEN NOT MATCHED THEN INSERT ( t.partyId , t.ulmToken , t.expiry_date ) VALUES ( s.partyId , s.ulmToken , s.expiry_date )
   ;
   logger.write ( 'pre merge customers' ) ;
   MERGE INTO customers t USING (
      SELECT a.partyId
           , MAX ( a.ulmToken ) KEEP ( DENSE_RANK FIRST ORDER BY a.expiry_date DESC ) AS ulmToken  -- for latest expiry_date
        FROM ulmOnboardToken a
       GROUP BY a.partyId
   ) s ON ( s.partyId = t.partyId )
   WHEN MATCHED THEN UPDATE SET t.ulmToken = s.ulmToken WHERE NVL ( t.ulmToken , 'x' ) != NVL ( s.ulmToken , 'x' )
   ;
   logger.write ( 'pre insert daf' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.partyid , t.ulmtoken , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.partyid , s.ulmtoken , s.messotoken
     FROM (
   SELECT *
     FROM customers c
    WHERE c.countryCode = 'GBR'
      AND c.pool IS NULL
      AND c.ulmToken IS NOT NULL
      AND ROWNUM <= 10000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE , i_flagCustomers => TRUE ) ;
   logger.write ( 'pre final merge' ) ;
   MERGE /*+ parallel(8) */ INTO dprov_accounts_fast t
   USING (
      SELECT /*+ parallel(bc,8) parallel(bca,8) parallel(ba, 8) */ bc.partyId
           , MAX ( ba.postcode ) AS postcode
        FROM ccsowner.bsbContactor bc
        JOIN ccsowner.bsbContactAddress bca ON bca.contactorId = bc.id
        JOIN ccsowner.bsbAddress ba ON ba.id = bca.addressId
       WHERE bca.primaryFlag = 1 
         --AND bca.notCurrent = 0
         AND bca.deletedFlag = 0
         AND bca.effectiveToDate IS NULL
       GROUP BY bc.partyId
   ) s ON ( s.partyId = t.partyId )
   WHEN MATCHED THEN UPDATE SET t.postcode = s.postcode
   WHERE t.pool_name = l_pool
   ;
   logger.write ( 'complete' ) ;
END ulmOnboardToken ;

END data_prep_01 ;
/
