create or replace PACKAGE DATA_PREP_12 AS 

  PROCEDURE preactivemobileroi;
  PROCEDURE activemobileroiplans;
  PROCEDURE soipaoprecontracting;
  PROCEDURE soiptntsportforamp;
  PROCEDURE activemobileroiplanscr009;
  PROCEDURE legacydisneyforamp;
  PROCEDURE mobileordersesim;  
  PROCEDURE existingroiwithiban;
  PROCEDURE soipgen2pendingactive;
  PROCEDURE digitalbbupgrade_dj;
  PROCEDURE mobileSwapBurn;
  PROCEDURE mobileSwapNoBurn;
  PROCEDURE iotprotectnft;
  PROCEDURE nonfttpbroadband;
  PROCEDURE actCoreCustNoDebtForSoip;
  PROCEDURE soipActiveSkyGlassGen2;
  PROCEDURE soipgen2dispatchedproducts ;
  PROCEDURE soipGen2DevicesForActivation ;
  PROCEDURE EligibleAccForEntmntServer ; 
  PROCEDURE NOCINEMAFORAMP;
  PROCEDURE fairusagedata;
  PROCEDURE mymessagescomms;
  PROCEDURE soipactiveskyglassair;
  PROCEDURE glassairdevicesforactivation;
  PROCEDURE fairusagedatabreach;

END DATA_PREP_12;
/


create or replace PACKAGE BODY DATA_PREP_12 AS 

PROCEDURE preactivemobileroi IS
-- 27th May 2024 Created for NFTREL-22428
-- Updated 03-SEP-2024 for PERFENG-5127
   l_pool VARCHAR2(29) := 'PREACTIVEMOBILEROI' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.servicenumber, t.messotoken)
    select ROWNUM AS pool_seqno, l_pool AS pool_name , s.ACCOUNTNUMBER, s.partyid, s.serviceinstanceid, s.messotoken
    from (
          select /*+ parallel(c, 8) parallel(ba, 8) parallel(paymt, 8) parallel(pmtrl, 8) parallel(adr, 8) parallel(a, 8) parallel(p, 8) parallel(cr, 8), parallel(pr, 8) */ 
           ba.ACCOUNTNUMBER, toks.partyid, ba.serviceInstanceId, toks.messotoken
          FROM CCSOWNER.BSBPORTFOLIOPRODUCT p
          JOIN ccsowner.bsbbillingaccount ba ON ba.PORTFOLIOID = p.PORTFOLIOID
          JOIN ccsowner.bsbServiceInstance si ON si.parentServiceInstanceId = ba.serviceInstanceId
          JOIN dataprov.customertokens toks on toks.accountnumber=ba.accountnumber
          WHERE p.CATALOGUEPRODUCTID = 'MOBILE_TALK_UNLIMITED_TEXT_UNLIMITED_ROI'
          AND p.STATUS = 'OIP'
          AND si.serviceInstanceType IN (610,620)
          AND p.STATUSCHANGEDDATE >= sysdate-7
          group by ba.accountnumber, toks.partyid, ba.serviceInstanceId, toks.messotoken) s;

   update dprov_accounts_fast f
   set subscription = (SELECT min(sub.serviceinstanceid)
                       from ccsowner.bsbsubscriptionagreementitem bsi		
                       inner join refdatamgr.bsbsubscriptionagreement sagg		
                       on sagg.id = bsi.subscriptionagreementid		
                       inner join ccsowner.bsbsubscription sub		
                       on sub.id = bsi.subscriptionid		
                       inner join ccsowner.bsbserviceinstance si		
                      on si.id = sub.serviceinstanceid		
                      inner join ccsowner.bsbserviceinstance bsi2		
                      on bsi2.id = si.parentserviceinstanceid		
                      inner join ccsowner.bsbbillingaccount ba		
                      on ba.serviceinstanceid = bsi2.id		
                      inner join ccsowner.bsbportfolioproduct pp		
                      on pp.subscriptionid = sub.id		
                     inner join refdatamgr.bsbcatalogueproduct catp		
                     on catp.id = pp.catalogueproductid	
                    where ba.accountnumber	= f.accountnumber
                    and sub.status in ('AA','OIP'))
   where pool_name = 'PREACTIVEMOBILEROI';       

   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END preactivemobileroi ;

PROCEDURE activemobileroiplans IS
-- 28th May 2024 Created for PERFENG-3708
   l_pool VARCHAR2(29) := 'ACTIVEMOBILEROIPLANS' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   execute immediate 'truncate table ' || l_pool ;

   insert into ACTIVEMOBILEROIPLANS (ACCOUNTNUMBER , PARTYID, SERVICENUMBER , MESSOTOKEN)
   select /*+ parallel(c, 8) parallel(ba, 8) parallel(paymt, 8) parallel(pmtrl, 8) parallel(adr, 8) parallel(a, 8) parallel(p, 8) parallel(cr, 8), parallel(pr, 8) */ 
           ba.ACCOUNTNUMBER, toks.partyid, ba.serviceInstanceId, toks.messotoken
   FROM CCSOWNER.BSBPORTFOLIOPRODUCT p
        JOIN ccsowner.bsbbillingaccount ba ON ba.PORTFOLIOID = p.PORTFOLIOID
        JOIN ccsowner.bsbServiceInstance si ON si.parentServiceInstanceId = ba.serviceInstanceId
        JOIN dataprov.customertokens toks on toks.accountnumber=ba.accountnumber
  WHERE p.CATALOGUEPRODUCTID = 'MOBILE_TALK_UNLIMITED_TEXT_UNLIMITED_ROI'
  AND p.STATUS = 'AC'
  AND si.serviceInstanceType IN (610,620)
  group by ba.accountnumber, toks.partyid, ba.serviceInstanceId, toks.messotoken;

  MERGE INTO ACTIVEMOBILEROIPLANS t
   USING ( SELECT bc.partyId
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

  INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.servicenumber, t.messotoken, t.postcode, t.code)
  select ROWNUM AS pool_seqno, l_pool AS pool_name , s.ACCOUNTNUMBER, s.partyid, s.servicenumber,s.messotoken, s.postcode, s.countrycode
  from ACTIVEMOBILEROIPLANS s;  

   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END activemobileroiplans ;

PROCEDURE soipaoprecontracting IS
-- 18th June 2024 Created for NFTREL-22440
   l_pool VARCHAR2(29) := 'SOIPAOPRECONTRACTING' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.skycesa01token, t.messotoken)
    select /*+ parallel(cu, 12) parallel(po, 12) parallel(ro, 12) parallel(si, 12)*/ 
                      distinct ROWNUM AS pool_seqno, l_pool AS pool_name ,cu.accountnumber, cu.partyid, cu.skycesa01token,cu.messotoken
             from dataprov.Customers cu, ccsowner.bsbportfoliooffer po,
                  refdatamgr.bsboffer ro, ccsowner.bsbserviceinstance si
            where po.portfolioid = cu.portfolioid
              and po.offerid = ro.ID
              and po.portfolioid = si.portfolioid
              and exists (select null from soipCustomers s where s.accountnumber=cu.accountnumber)
                  and (po.applicationenddate between trunc(sysdate) and trunc(sysdate)+89 or po.applicationenddate is null) -- offer ends in next 90 days
              and (po.applicationstartdate < sysdate or po.applicationstartdate is null);
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END soipaoprecontracting ;

PROCEDURE SOIPTNTSPORTFORAMP IS
   -- 20-06-2024 Charles Ajah for Antti Makarainen NFTREL-22441
   l_pool VARCHAR2(29) := 'SOIPTNTSPORTFORAMP' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId  , t.messoToken , t.x1Accountid )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId  , s.messoToken , s.x1Accountid 
     FROM (
       select 
      d.accountnumber, 
      d.partyId, 
      d.messoToken, 
      d.x1Accountid 
    from 
      dataprov.soipActiveSubscription d 
    where 
      d.inFlightVisit = 0 -- 19-Jan-2022 Humza Ismail NFTREL-21473 to soipActiveSubscription pool, maybe not needed for Archana's pool?
      AND d.returnInTransit = 0 -- 21-Jan-2022 Humza Ismail NFTREL-21480 to soipActiveSubscription pool, maybe not needed for Archana's pool?
      AND d.customerHasAmp = 0 
      AND d.sports = 1 --by definition , TNTSPORTS & SKYSPORTS are all bundled together into the sports column
      AND d.accountnumber IN(
        SELECT 
          DISTINCT ba.accountNumber 
        FROM 
          rcrm.subscription sub 
          JOIN rcrm.productSubscriptionLink psl ON psl.subscriptionId = sub.id 
          JOIN rcrm.product prod ON prod.id = psl.productId 
          JOIN rcrm.service serv ON serv.id = prod.serviceId 
          JOIN ccsowner.bsbBillingAccount ba ON ba.serviceInstanceId = serv.billingServiceInstanceId 
        WHERE 
          sub.status IN ('AC', 'ACTIVE') 
          AND prod.suid like '%BT%' ) --we select accounts that have TNTSports
    ORDER BY 
      dbms_random.value
       ) s
   WHERE ROWNUM <= 100000 ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT, i_burn => FALSE ) ;
   logger.write ( 'complete' ) ;
END SOIPTNTSPORTFORAMP ;


PROCEDURE ActiveMobileROIPlansCR009 IS
-- 05-JUL-2024 PERFENG-4106
   l_pool VARCHAR2(29) := 'ACTIVEMOBILEROIPLANSCR009' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;

   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.servicenumber, t.messotoken, t.postcode, t.code)
    select ROWNUM AS pool_seqno, l_pool AS pool_name , s.ACCOUNTNUMBER, s.partyid, s.servicenumber,s.messotoken, s.postcode, s.countrycode
    from ACTIVEMOBILEROIPLANS s 
    where rownum <100000
    and s.postcode in ('D03R8P3','D03XC62','D04RH96','D05H2F8','D07N773','D09EV10','D22TK64','V42TD85');

   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END ActiveMobileROIPlansCR009 ;


PROCEDURE legacydisneyforamp IS
-- 19th July 2024 by SM. Created for PERFENG-4408
   l_pool VARCHAR2(29) := 'LEGACYDISNEYFORAMP' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.messotoken, t.x1accountId)
      select ROWNUM AS pool_seqno, l_pool AS pool_name , s.ACCOUNTNUMBER, s.partyid, s.messotoken, s.x1accountId
        from (      select /*+ parallel(c, 8) parallel(bpp, 8) parallel(pte, 8)*/ c.accountnumber, c.partyId, c.messotoken, pte.x1accountId
                      from customersv2 c
                         JOIN ccsowner.portfolioTechElement pte ON pte.portfolioId = c.portfolioId
                         JOIN ccsowner.bsbPortfolioProduct bpp ON bpp.portfolioId = c.portfolioId
                         LEFT OUTER JOIN dataprov.Products p ON bpp.catalogueProductId = p.id
                      where c.skyqbox=1
                      and c.disneyplus=1
                      and c.pool is null
                      and c.countrycode = 'GBR'  
                      and p.id = '15550'  -- "Disney+ (legacy)"
                      and not exists ( select c2.partyid 
                                         from customersv2 c2 
                                         join ccsowner.bsbBillingAccount ba on ( ba.accountNumber = c2.accountNumber )
                                         join rcrm.service s ON ( ba.serviceInstanceId = s.billingServiceInstanceId )
                                         join rcrm.product p ON ( s.id = p.serviceId )
                                        where c2.partyId = c.partyId 
                                          and c2.accountType = 'SKY_AMP'
                                          and p.suid = 'AMP_DISNEY'
                                          and p.status = 'ACTIVE'
                                      )
                      and rownum <= 20000
                      ORDER BY  dbms_random.value
             ) s
      ;

   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => FALSE ) ;
   logger.write ( 'complete' ) ;
END legacydisneyforamp ;



PROCEDURE mobileordersesim is
-- 9th August 2024 by SM. Created for PERFENG-4675
   l_pool VARCHAR2(29) := 'MOBILEORDERSESIM';

begin
   logger.write ( 'begin' ) ;
   data_prep_static_oms.esim_orders@oms ;
   logger.write ( 'oms remote procedure to esim_orders completed' ) ;   
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT /*+  parallel (8) enable_parallel_dml */ INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.id,t.partyid,t.accountnumber,t.servicenumber)
    select ROWNUM AS pool_seqno, l_pool AS pool_name , s.id, s.partyid, s.accountnumber, s.servicenumber
    from(
     select /*+ parallel(cu,8)  */ distinct oms.id id, cu.partyid partyid, cu.accountNumber accountnumber,  oms.instanceId AS serviceNumber
     FROM dataprov.customers cu, ccsowner.bsbbillingaccount bba, ccsowner.bsbportfolioproduct bpp,
                    ccsowner.bsbmobileesim e, esim_orders@oms oms
     WHERE bba.accountnumber = cu.accountnumber
     AND bba.portfolioid = bpp.portfolioid
     AND bpp.catalogueproductid='15861'
     AND accountnumber2 is null
     AND e.portfolioproductid=bpp.id
     AND oms.accountNumber = bba.accountnumber
     AND rownum <= 50000) s; 

   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;

end mobileordersesim;

PROCEDURE existingroiwithiban is
-- 9th August 2024 by SM. Created for PERFENG-4675
   l_pool VARCHAR2(29) := 'EXISTINGROIWITHIBAN';

begin
  logger.write ( 'begin' ) ;
  execute immediate 'truncate table ' || l_pool;
   
  INSERT /*+ append */ INTO EXISTINGROIWITHIBAN t ( t.accountNumber , t.partyId , t.data , t.skycesa01token , t.messotoken , t.ssotoken, t.firstName , t.familyName , t.emailAddress )
  SELECT c.accountNumber, c.partyId, c.username, c.skycesa01token, c.messotoken, c.ssotoken, c.firstName, c.familyName, c.emailAddress
    FROM dataprov.customers c
    JOIN dataprov.bbRegrade_portfolioId_tmp pp ON c.portfolioId = pp.portfolioId  -- must have line rental
   WHERE c.accountNumber2 IS NULL  -- only want customers with a single billing account.
     AND c.countryCode = 'IRL'
     AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- no outstanding balance 24/09 Shane Venter
     -- eliminate those with a valid power of attorney
     AND c.portfolioid NOT IN ( SELECT po.portfolioid
                                  FROM ccsowner.bsbPowerOfAttorneyRole po
								 WHERE effectiveToDate IS NULL OR effectiveToDate > SYSDATE )
     AND c.emailAddress NOT LIKE 'noemail%'  -- 13-Dec-2021 Andrew Fraser for Edwin Scariachan: valid email needed for soip.
     AND c.partyId IN ( SELECT bc.partyId
	                       FROM ccsowner.bsbContactor bc
						   LEFT OUTER JOIN ccsowner.bsbContactTelephone bct ON bct.contactorId = bc.id
						   LEFT OUTER JOIN ccsowner.bsbTelephone bt ON bt.id = bct.telephoneId
						  WHERE bct.deletedFlag = 0
						    AND SYSDATE BETWEEN NVL ( bct.effectiveFromDate , SYSDATE - 1 ) AND NVL ( bct.effectiveToDate , SYSDATE + 1 )
							AND bt.telephoneNumberStatus = 'VALID'
							AND bct.typeCode = 'M' ) 
     AND c.partyId IN ( SELECT bc.partyId
	                      FROM ccsowner.bsbContactor bc
						  LEFT OUTER JOIN ccsowner.bsbcontactaddress bca ON bca.contactorId = bc.id
						  LEFT OUTER JOIN ccsowner.bsbAddress ba ON ba.id = bca.addressId
						 where bca.deletedFlag = 0
						   and ba.eircode is not null
						   AND SYSDATE BETWEEN NVL ( bca.effectiveFromDate , SYSDATE - 1 ) AND NVL ( bca.effectiveToDate , SYSDATE + 1 ) ) ;

  commit;
  
  dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
  
  FOR i IN 1..6
  LOOP
    execute immediate '
      MERGE INTO EXISTINGROIWITHIBAN t
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
    COMMIT ;
  END LOOP ;
  
  -- remove customers without an iban
  delete from EXISTINGROIWITHIBAN where code is null;
  commit;
  
  sequence_pkg.seqBefore ( i_pool => l_pool ) ;
  
  INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.data , t.skycesa01token , t.messotoken
                                    , t.ssotoken , t.firstName , t.familyName , t.emailAddress , t.code )
  SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.data , s.skycesa01token , s.messotoken
       , s.ssotoken , s.firstName , s.familyName , s.emailAddress , s.code
    FROM (  SELECT d.accountNumber , d.partyId , d.data , d.skycesa01token , d.messotoken, d.ssotoken , d.firstName , d.familyName , d.emailAddress , d.code
	          FROM EXISTINGROIWITHIBAN d
			ORDER BY dbms_random.value ) s ;
   
  sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => FALSE ) ;
  logger.write ( 'complete' ) ;
  
end existingroiwithiban;


PROCEDURE soipgen2pendingactive IS
-- 9th September 2024 Created for PERFENG-5184
   l_pool VARCHAR2(29) := 'SOIPGEN2PENDINGACTIVE' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipgen2pendingactive t ( t.accountNumber, t.partyId, t.messoToken, t.emailAddress )
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

   MERGE INTO soipgen2pendingactive t USING (
      SELECT DISTINCT ba.accountnumber
        FROM rcrm.product pr
        JOIN rcrm.service se ON pr.serviceId = se.id
        JOIN ccsowner.bsbBillingAccount ba ON ba.serviceInstanceId = se.billingServiceInstanceId
       WHERE pr.eventcode = 'DELIVERED'
         AND pr.suid IN ( 'SKY_GLASS_GEN2_LARGE' , 'SKY_GLASS_GEN2_MEDIUM' , 'SKY_GLASS_GEN2_SMALL' )
   ) s ON ( s.accountNumber = t.accountNumber )
   WHEN MATCHED THEN UPDATE SET t.delivered = 1
   ;
   -- reduce size of data to be processed by slow populateMessoToken merge statement

   UPDATE soipgen2pendingactive t
      SET t.delivered = 0
    WHERE t.delivered is null
   ;

   DELETE FROM soipgen2pendingactive t
    WHERE t.delivered = 0

   ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;

   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken , t.emailaddress )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken , s.emailaddress
     FROM (
   SELECT d.accountNumber , d.partyId , d.messoToken , d.emailaddress
     FROM soipgen2pendingactive d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT, i_burn => FALSE ) ;
   -- table not truncated here, because used by child pool soipgen2pendingactiveEdwin in data_prep_07.
   logger.write ( 'complete' ) ;
END soipgen2pendingactive ;

PROCEDURE digitalbbupgrade_dj IS
   l_pool VARCHAR2(29) := 'DIGITALBBUPGRADE_DJ' ;
   l_telout varchar2(32) ;
   l_count number;
   l_magicPcode varchar2(7) := 'HP41DJ' ;
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
     WHERE c.boxSets = 0
       AND c.cinema = 0
       AND c.sports = 0
       AND c.kids = 0
       AND c.entertainment = 1
       AND c.bband = 0
       AND c.talk = 0
       AND c.bb12gb = 0
       AND c.pool != 'DIGITAL_BB_UPGRADE'
       AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
       -- Removed at request of Liam Flemin 10/04/2025 (PERFENG-8552)
       --AND ( c.skyQBox = 1 OR c.skyHDBox = 1 )  -- to be upgraded to BoxSets, must have a box capable of downloading.
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
   l_count := SQL%ROWCOUNT;
   logger.write ('digitalbbupgrade_dj - rows inserted : '|| l_count) ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => l_count, i_flagCustomers => TRUE ) ;
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
END DIGITALBBUPGRADE_DJ ;

PROCEDURE mobileSwapBurn IS
    l_pool      varchar2(30) := 'MOBILESWAPBURN' ; -- 70%
    l_fullpool  varchar2(30) := 'MOBILESWAP' ; -- 100%
    l_temptbl   varchar2(30) := 'mobileswapdebt' ;
    l_count     NUMBER ;
    l_rows      NUMBER ;
BEGIN
    logger.write ( 'begin mobileSwapBurn - load data into '||l_pool ) ;
    execute immediate 'truncate table '|| l_pool ||' reuse storage' ;
    execute immediate 'truncate table '|| l_fullpool ||' reuse storage' ;
    execute immediate 'truncate table '|| l_temptbl ||' reuse storage' ;
    -- Load the full set of data,, which later on will be split into two pools
    -- 70% will be loaded into MOBILESWAPNOBURN 
    -- 30% will be loaded into MOBILESWAPBURN ( a dependent pool build from this one )
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
           and not exists ( select mbl.imei 
                              from ccsowner.BSBMOBILEBLACKLIST mbl
                             where mbl.billingaccountid = ba.id 
                               and mbl.imei = mcpe.imei )  -- exclude PHONE_NOT_BLACKLISTED           
        group by ba.accountnumber, cpe.lastupdate
        order by cpe.lastupdate DESC
      )
    WHERE ROWNUM <= 30000
    ;
    l_count := SQL%ROWCOUNT; 
    l_rows  := l_count ;  --- Keep track of the total number of records in the table
    logger.write ('mobileSwapBurn - rows inserted into mobileswap : '|| l_count) ;
    
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
    logger.write ('mobileSwapBurn - rows inserted into mobileswapdebt : '|| l_count) ;   
    
    DELETE FROM mobileswap
    WHERE accountNumber in ( select mob.accountNumber from mobileswapdebt mob );
    l_count := SQL%ROWCOUNT; 
    l_rows  := l_rows - l_count ;  --- Keep track of the total number of records in the table
    commit;                    
    logger.write ('mobileSwapBurn - rows deleted from mobileswap : '|| l_count) ;   
    
    -- To build the burnable part of the pool
    insert into mobileswapburn
    select * from  mobileswap
    where rownum < ( l_rows*0.30 )
    order by dbms_random.value;
    l_count := SQL%ROWCOUNT; 
    commit;
    logger.write ('mobileSwapBurn - rows inserted into mobileSwapBurn : '||l_count) ;

    -- Write rows to DPROV_ACCOUNTS_FAST
    sequence_pkg.seqBefore ( i_pool => l_pool ) ;
    INSERT INTO dprov_accounts_fast ( pool_seqno , pool_name , accountnumber , messotoken , portfolioproductid , productname, partyid )
    SELECT ROWNUM , l_pool , ms.accountnumber, ms.messotoken , ms.portfolioproductid , ms.productname , ms.partyid
     FROM mobileswapburn ms
    WHERE rownum < 10001 ;
    l_count := SQL%ROWCOUNT;
    commit;
    sequence_pkg.seqAfter ( i_pool => l_pool , i_count => l_count , i_burn => TRUE ) ; 
    logger.write ( 'complete mobileSwapBurn' ) ;

END mobileSwapBurn ;

PROCEDURE mobileSwapNoBurn IS
    l_pool      varchar2(30) := 'MOBILESWAPNOBURN' ; -- 70%
    l_count  NUMBER ;
BEGIN
    -- This pool is dependent on the results from mobileSwapBurn
    -- The interim tables mobileswap and mobileswapburn are supposed to exist and be already populated
    logger.write ( 'begin mobileSwapNoBurn' ) ;
    execute immediate 'truncate table '|| l_pool ||' reuse storage' ;
    
    -- To build the non burnamble part of the pool
    insert into mobileswapnoburn
    select * from  mobileswap
    where accountnumber not in ( select accountNumber from mobileswapburn )
    order by dbms_random.value;
    l_count := SQL%ROWCOUNT; 
    commit;
    logger.write ('mobileswapnoburn - rows inserted : '||l_count) ;

    -- Write rows to DPROV_ACCOUNTS_FAST
    sequence_pkg.seqBefore ( i_pool => l_pool ) ;
    INSERT INTO dprov_accounts_fast ( pool_seqno , pool_name , accountnumber , messotoken , portfolioproductid , productname, partyid )
    SELECT ROWNUM , l_pool , ms.accountnumber, ms.messotoken , ms.portfolioproductid , ms.productname , ms.partyid
     FROM mobileswapnoburn ms
    WHERE rownum < 10001 ;
    l_count := SQL%ROWCOUNT;
    commit;
    sequence_pkg.seqAfter ( i_pool => l_pool , i_count => l_count , i_burn => FALSE ) ; 
    logger.write ( 'complete mobileSwapNoBurn' ) ;    

END mobileSwapNoBurn ;


PROCEDURE iotprotectnft IS
-- 29th October 2024 Created for PERFENG-6366
   l_pool VARCHAR2(29) := 'IOTPROTECTNFT' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;


  INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.user_uuid, t.home_uuid, t.authorizationtoken)
  select ROWNUM AS pool_seqno, l_pool AS pool_name , s.user_uuid, s.home_uuid, s.authorizationtoken
  from IOTPROTECTNFT s;  

   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END iotprotectnft ;



PROCEDURE nonfttpbroadband IS
-- 06 November 2024 Created for PERFENG-6498
   l_pool VARCHAR2(29) := 'NONFTTPBROADBAND' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;


  INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber, t.talk_serviceId , t.bb_serviceId, t.partyId )
  select ROWNUM AS pool_seqno, l_pool AS pool_name , accountnumber, talk_serviceid, serviceid, partyid
   from(select /*+
                  full(bpp)  parallel(bpp 8)  pq_distribute ( bpp hash hash )
                  full(bcpe) parallel(bcpe 8) pq_distribute ( bcpe hash hash )
                  full(bpe)  parallel(bpe 8)  pq_distribute ( bpe hash hash )
                  full(c)    parallel(8)      pq_distribute (c hash hash)
                  full(r) parallel(r 8) pq_distribute (r hash hash)
                 */
   d.accountnumber accountnumber, d.partyid partyid, d.talk_serviceid, bpe.servicenumber serviceid
   from ccsowner.bsbPortfolioProduct bpp, ccsowner.bsbCustomerProductElement bcpe, ccsowner.bsbBroadbandCustProdElement bpe,
   replaceHub d, refdatamgr.bsbCatalogueProduct p
   where bcpe.portfolioProductId (+) = bpp.id
   and bpe.lineProductElementId (+) = bcpe.id
   and d.portfolioid= bpp.portfolioid
   and bpp.catalogueProductId = p.id
   and ( p.id in ('13573','14931','14932','15040') -- sogea
                 or
            p.id in ('13574','13744','14287','15192','15193'  ))
   and bcpe.status in ('AC', 'DL', 'DS')
   and bpp.status in ('AC', 'DL', 'DS')
   and bpe.serviceNumber is not null
   and ROWNUM <= 300000);

   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => FALSE ) ;
   logger.write ( 'complete' ) ;
END nonfttpbroadband ;

PROCEDURE actCoreCustNoDebtForSoip IS
   l_pool VARCHAR2(29) := 'ACTCORECUSTNODEBTFORSOIP' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO actCoreCustNoDebtForSoip t ( t.accountNumber , t.partyId )
   SELECT /*+ parallel(8) */ c.accountNumber , c.partyId
     FROM customersv2 c
     --FROM ( SELECT * FROM customers WHERE ROWNUM <= 500000 ) c
    WHERE c.inFlightOrders = 0
      AND c.inFlightVisit = 0
      AND c.emailAddress NOT LIKE 'noemail%'  -- 08-Nov-2022 Alex Benetatos must have a primary email address.
      AND c.countryCode = 'GBR'  -- 09-Mar-2023 Alex Benetatos
      AND c.mobile = 0  -- No mobile contract
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
             FROM actCoreCustNoDebtForSoip d
            ORDER BY dbms_random.value
           ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END actCoreCustNoDebtForSoip ;


PROCEDURE soipActiveSkyGlassGen2 IS
   -- 22-Feb-2022 Andrew Fraser for Edwin Scariachin, used for cancelling Sky Glass.
   l_pool VARCHAR2(29) := 'SOIPACTIVESKYGLASSGEN2' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipActiveSkyGlassGen2 t ( t.accountNumber , t.partyId , t.messoToken )
   SELECT DISTINCT d.accountNumber , d.partyId , d.messoToken
     FROM soipActiveSubscription d
     JOIN ccsowner.bsbBillingAccount ba ON ba.accountNumber = d.accountNumber
     JOIN rcrm.service s ON ba.serviceInstanceId = s.billingserviceInstanceId
     JOIN rcrm.product p ON s.id = p.serviceId
    WHERE d.serviceType = 'SOIP'
      AND p.suid IN ('SKY_GLASS_GEN2_SMALL' , 'SKY_GLASS_GEN2_MEDIUM' , 'SKY_GLASS_GEN2_LARGE')
      AND p.status = 'DELIVERED'
      AND p.eventCode = 'DELIVERED'
      AND d.billed = 1  -- 20-Jul-2022 Andrew Fraser for Humza Ismail
      AND ROWNUM <= 900000
   ;
   logger.write ( 'inserted ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   COMMIT ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   -- 24-Jun-2022 Andrew Fraser for Terence Burton, exclude customers without a telephone number.
   DELETE FROM soipActiveSkyGlassGen2 t
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
   DELETE FROM soipActiveSkyGlassGen2 t
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
   DELETE FROM soipActiveSkyGlassGen2 t
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
             FROM soipActiveSkyGlassGen2 d
            ORDER BY dbms_random.value
         ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;

   logger.write ( 'complete' ) ;
END soipActiveSkyGlassGen2 ;

PROCEDURE soipgen2dispatchedproducts IS
    -- PERFENG-7539
    l_pool   VARCHAR2(29) := 'SOIPGEN2DISPATCHEDPRODUCTS' ;
    l_count  NUMBER ;
BEGIN
    logger.write ( 'begin '||l_pool ) ;
   -- staging table hosted in fps for performance during the merge statement, to use index on partyId
    DELETE FROM soipGen2DispatchedProducts@fps ;
    COMMIT ;
    logger.write ( l_pool ||' - data deleted from FPS temp table ') ;
   
    INSERT INTO soipGen2DispatchedProducts@fps ( accountNumber , productId , partyId )
        SELECT ba.accountNumber
            , p.id AS productId
            , bpr.partyId
         FROM rcrm.product p
         JOIN rcrm.service s ON p.serviceId = s.id
         JOIN ccsowner.bsbBillingaccount ba ON ba.serviceInstanceId = s.billingServiceInstanceId
         JOIN ccsowner.bsbCustomerRole bcr ON ba.portfolioId = bcr.portfolioId
         JOIN rcrm.hardwareProdTechElement hp ON hp.productId = p.id  
         JOIN ccsowner.bsbPartyRole bpr ON bcr.partyRoleId = bpr.id
        WHERE p.eventCode = 'DELIVERED'
          AND p.suid LIKE 'SKY_GLASS_GEN2%'
          AND hp.serialNumber is NOT NULL
          --AND hp.serialNumber LIKE 'TV11SKA%'  -- Not applicable for this product    
    ;
    l_count := SQL%ROWCOUNT ;
    COMMIT;
    logger.write ( l_pool || TO_CHAR ( l_count ) || ' rows inserted' ) ;

    -- merge statement held in fps for performance and to workaround "ORA-22992: cannot use LOB locators selected from remote tables"
    data_prep_fps.soipGen2DispatchedProducts@fps ;
    COMMIT;
    logger.write ( l_pool || ' Remote MERGE in FPS process completed ') ;

    sequence_pkg.seqBefore ( i_pool => l_pool ) ;
    INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.fulfilmentReferenceId , t.productId , t.accountNumber , t.partyId )
    SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.fulfilmentReferenceId , s.productId , s.accountNumber , s.partyId
     FROM (
           SELECT d.fulfilmentReferenceId , d.productId , d.accountNumber , d.partyId
             FROM soipGen2DispatchedProducts@fps d
            WHERE d.fulfilmentReferenceId IS NOT NULL
            ORDER BY dbms_random.value  
          ) s
    ;
    sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;

    logger.write ( 'complete '||l_pool  ) ;
END soipgen2dispatchedproducts ;



PROCEDURE soipGen2DevicesForActivation IS
    -- PERFENG-7542
    l_pool   VARCHAR2(29) := 'SOIPGEN2DEVICESFORACTIVATION' ;
    l_count  NUMBER ;
BEGIN
    logger.write ( 'begin '||l_pool ) ;
    execute immediate 'truncate table ' || l_pool ||' reuse storage';
    INSERT /*+ append */ INTO SOIPGEN2DEVICESFORACTIVATION t ( t.accountNumber , t.x1AccountId , t.serialNumber , t.productId )
        SELECT DISTINCT ba.accountNumber , pte.x1AccountId , hp.serialNumber , pr.id AS productId
          FROM rcrm.product pr
          JOIN rcrm.service sr ON pr.serviceid = sr.id
          JOIN ccsowner.bsbBillingAccount ba ON ba.serviceInstanceId = sr.billingServiceInstanceId
          JOIN ccsowner.portfolioTechElement pte ON pte.portfolioId = ba.portfolioId
          JOIN rcrm.hardwareProdTechElement hp ON hp.productId = pr.id
          JOIN rcrm.deviceRegistry dr ON dr.publicDeviceId = hp.serialNumber AND dr.accountNumber = ba.accountNumber
          JOIN rcrm.deviceHistoryDetail dhd ON dr.id = dhd.deviceRegistryId
          JOIN rcrm.deviceChangeHistory dch ON dr.accountNumber = dch.accountNumber AND dhd.deviceChangeId = dch.id
         WHERE pr.suid LIKE 'SKY_GLASS_GEN2%'
           AND pr.eventCode = 'DELIVERED' --'DISPATCHED'
           AND dch.sourceReference IS NOT NULL
    ;
    l_count := SQL%ROWCOUNT ;
    COMMIT ;
    logger.write (l_pool||' - '||l_count||' rows inserted ') ;
    
    dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;

    logger.write (l_pool||' - Stats gathered ') ;
    
    sequence_pkg.seqBefore ( i_pool => l_pool ) ;
    INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.x1AccountId , t.serialNumber )
       SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.x1AccountId , s.serialNumber
         FROM (
       SELECT d.x1AccountId , d.serialNumber
         FROM SOIPGEN2DEVICESFORACTIVATION d
        ORDER BY dbms_random.value
       ) s
       WHERE ROWNUM <= 100000
    ;
    sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
        
    logger.write ( 'complete '||l_pool ) ;
END soipGen2DevicesForActivation ;


PROCEDURE EligibleAccForEntmntServer IS
    l_pool  varchar2(29) := Upper('EligibleAccForEntmntServer') ;
    l_count number ;
BEGIN
    logger.write ( 'begin '||l_pool ) ;
    execute immediate 'truncate table ' || l_pool ;
    INSERT /*+ append */ INTO EligibleAccForEntmntServer t ( t.portfolioid , t.accountnumber , t.combinedtelephonenumber )
        SELECT  /*+ parallel(bt 8) parallel(telu 8) parallel(bsi 8) parallel(ba 8) */
                Max(ba.portfolioid), Max(ba.accountnumber) , tele.combinedtelephonenumber
        FROM ( select DISTINCT bt.combinedtelephonenumber, bt.id
                 from ccsowner.BSBTELEPHONE bt
                where bt.telephonenumberusecode = 'MSISDN' 
                  and bt.combinedtelephonenumber LIKE '07%'  
              ) tele
         JOIN ccsowner.BSBTELEPHONEUSAGEROLE telu ON ( telu.telephoneId = tele.id ) 
         JOIN ccsowner.BSBSERVICEINSTANCE bsi ON ( bsi.id = telu.serviceinstanceId  )                                         
         JOIN ccsowner.BSBBILLINGACCOUNT ba ON ( ba.portfolioId = bsi.portfolioid )
        WHERE trunc(SYSDATE) BETWEEN trunc(telu.effectivefromdate) AND trunc(NVL(telu.effectivetodate, SYSDATE))
          AND telu.roletype = 'MSISDN'
          AND bsi.serviceinstancetype = 620
          AND NVL ( ba.accountStatusCode , '01' ) = '01'  -- ('01' - Sent to Billing, '02' - Not sent to Billing) - NULLS will be considered as '01'
        GROUP BY tele.combinedtelephonenumber  
        HAVING count(ba.portfolioid) = 1   
   ;
   l_count := sql%rowcount;
   COMMIT ;

   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.portfolioid , t.accountnumber , t.combinedtelephonenumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.portfolioid , s.accountnumber , s.combinedtelephonenumber
     FROM (
   SELECT d.portfolioid , d.accountnumber , d.combinedtelephonenumber
     FROM EligibleAccForEntmntServer d
    ORDER BY dbms_random.value
   ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete '|| l_pool || ' - Rows : '||l_count) ;
END EligibleAccForEntmntServer ;

PROCEDURE NOCINEMAFORAMP IS
   -- 
   l_pool VARCHAR2(29) := 'NOCINEMAFORAMP' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'alter session enable parallel dml' ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO NOCINEMAFORAMP t ( t.accountNumber , t.partyId , t.messoToken , t.ssoToken , t.skyCesa01Token
        , t.customerHasAmp , t.countryCode , t.portfolioId )
   SELECT s.accountnumber , s.partyId  , s.messoToken , s.ssoToken , s.skyCesa01Token , s.customerHasAmp , s.countryCode , s.portfolioId
     FROM (
           SELECT c.accountnumber , c.partyId  , c.messoToken , c.ssoToken , c.skyCesa01Token , 0 AS customerHasAmp , c.countryCode , c.portfolioId
             FROM customers c
            WHERE c.cinema = 0
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
   DELETE FROM NOCINEMAFORAMP t
    WHERE t.partyId IN (
          SELECT s.partyId
            FROM act_cust_uk_subs s
           WHERE s.dtv != 'AC'
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for active_customer error' ) ;
   COMMIT ;
   -- 24-Jan-2023 Andrew Fraser for Michael Santos, exclude customers with 14504 'Sky Cinema' product.
   DELETE /*+ parallel(8) */ FROM NOCINEMAFORAMP t
    WHERE EXISTS (
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
   DELETE /*+ parallel(8) */ FROM NOCINEMAFORAMP t
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
   MERGE /*+ parallel(8) */ INTO NOCINEMAFORAMP t USING (
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
   DELETE FROM NOCINEMAFORAMP t
    WHERE t.customerHasAmp = 1
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for customerHasAmp' ) ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   -- 28-Feb-2023 Michael Santos PERFENG-574 hopefully only a temporary exclusion to be removed later in 2023.
   DELETE FROM NOCINEMAFORAMP t
    WHERE EXISTS (
          SELECT NULL
            FROM ccsowner.bsbPortfolioOffer po
           WHERE po.portfolioId = t.portfolioId
             AND po.offerId IN ( 50557 , 80889 , 91809 , 73850 , 50558 , 75112 , 73805 , 51628 )
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for portfolioOffer' ) ;
   COMMIT ;
   MERGE INTO NOCINEMAFORAMP t
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
   MERGE /*+ parallel(16) */ INTO NOCINEMAFORAMP t
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
   UPDATE NOCINEMAFORAMP t SET t.burnPool = MOD ( ROWNUM , 2 ) ;
   -- but non-burnable pool has to exclude customers with outstanding balance (aka 'debt')
   UPDATE NOCINEMAFORAMP t SET t.burnPool = 1
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
             FROM NOCINEMAFORAMP d
            WHERE d.customerHasAmp = 0
              AND d.burnPool = 1
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   -- 11-Jan-2013 Michael Santos, changed to burn, SOIPPOD-2732
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => FALSE ) ;
   logger.write ( 'complete' ) ;
END NOCINEMAFORAMP ;


PROCEDURE fairusagedata IS
   -- 20-MAR-2025 PERFENG-8189 Fair Usage Data.
   l_pool VARCHAR2(29) := 'FAIRUSAGEDATA' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;


   logger.write ( 'inserted ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   COMMIT ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.closeddate,t.accountnumber,t.serviceid,t.partyid,t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.closeddate,s.accountnumber,s.fu_service,
          s.partyid,s.messotoken
     FROM (
           select distinct fu.billingperiodenddate closeddate, ba.accountnumber, fu.serviceinstanceid fu_service, 
                           c.partyid, c.messotoken
           from CCSOWNER.MOBILEFAIRUSAGEDAILYDATA fu, ccsowner.bsbbillingaccount ba, ccsowner.bsbserviceinstance si,            
                dataprov.customersv2 c
           where ba.serviceinstanceid = si.parentserviceinstanceid
           and fu.serviceinstanceid = si.id
           and c.accountnumber=ba.accountnumber
           ORDER BY dbms_random.value
         ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;

   logger.write ( 'complete' ) ;
END fairusagedata;


PROCEDURE mymessagescomms IS
   -- 28-MAR-2025 PERFENG-8365 Mobile accounts with many communications records.
   l_pool VARCHAR2(29) := 'MYMESSAGESCOMMS' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;

   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber,t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , account, messo
    from (select n.accountnumber account, messotoken messo
          from act_mobile_numbers n, tcc_owner.bsbCommsArtifact@tcc a
          where n.accountnumber = a.accountnumber
          group by n.accountnumber, messotoken
          having count(*) between 50 and 2000)
    where rownum <= 10000
    ORDER BY dbms_random.value;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;

   logger.write ( 'complete' ) ;
END mymessagescomms;



PROCEDURE soipactiveskyglassair IS
   l_pool VARCHAR2(29) := 'SOIPACTIVESKYGLASSAIR' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO soipactiveskyglassair t ( t.accountNumber , t.partyId , t.messoToken )
   SELECT DISTINCT d.accountNumber , d.partyId , d.messoToken
     FROM soipActiveSubscription d
     JOIN ccsowner.bsbBillingAccount ba ON ba.accountNumber = d.accountNumber
     JOIN rcrm.service s ON ba.serviceInstanceId = s.billingserviceInstanceId
     JOIN rcrm.product p ON s.id = p.serviceId
    WHERE d.serviceType = 'SOIP'
      AND p.suid like 'SKY_GLASS_AIR%'
      AND p.status = 'DELIVERED'
      AND p.eventCode = 'DELIVERED'
      AND d.billed = 1  -- 20-Jul-2022 Andrew Fraser for Humza Ismail
      AND ROWNUM <= 900000
   ;
   logger.write ( 'inserted ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;
   COMMIT ;
   dbms_stats.gather_table_stats ( ownName => user , tabName => l_pool ) ;
   -- 24-Jun-2022 Andrew Fraser for Terence Burton, exclude customers without a telephone number.
   DELETE FROM soipactiveskyglassair t
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
   DELETE FROM soipactiveskyglassair t
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
   DELETE FROM soipactiveskyglassair t
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
             FROM soipactiveskyglassair d
            ORDER BY dbms_random.value
         ) s
   WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;

   logger.write ( 'complete' ) ;
END soipactiveskyglassair ;


PROCEDURE glassairdevicesforactivation IS
    -- PERFENG-8649
    l_pool   VARCHAR2(29) := 'GLASSAIRDEVICESFORACTIVATION' ;
    l_count  NUMBER ;
BEGIN
    logger.write ( 'begin '||l_pool ) ;

    sequence_pkg.seqBefore ( i_pool => l_pool ) ;
    INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber, t.x1AccountId , t.serialNumber )
       SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber, s.x1AccountId , s.serialNumber
         FROM (
               SELECT DISTINCT ba.accountNumber , pte.x1AccountId , hp.serialNumber 
               FROM rcrm.product pr
               JOIN rcrm.service sr ON pr.serviceid = sr.id
               JOIN ccsowner.bsbBillingAccount ba ON ba.serviceInstanceId = sr.billingServiceInstanceId
               JOIN ccsowner.portfolioTechElement pte ON pte.portfolioId = ba.portfolioId
               JOIN rcrm.hardwareProdTechElement hp ON hp.productId = pr.id
               WHERE pr.suid LIKE 'SKY_GLASS_AIR%'
               AND pr.eventCode = 'DELIVERED'
               ORDER BY dbms_random.value
       ) s
       WHERE ROWNUM <= 100000
    ;
    sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
        
    logger.write ( 'complete '||l_pool ) ;
END glassairdevicesforactivation ;




PROCEDURE fairusagedatabreach IS
   -- 25-APR-2025 PERFENG-8702 Fair Usage Data.
   l_pool VARCHAR2(29) := 'FAIRUSAGEDATABREACH' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;

   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.closeddate,t.accountnumber,t.serviceid,t.partyid,t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.closeddate,s.accountnumber,s.fu_service,
          s.partyid,s.messotoken
     FROM (
           select distinct fu.billingperiodenddate closeddate, ba.accountnumber, fu.serviceinstanceid fu_service, 
                           c.partyid, c.messotoken
           from CCSOWNER.MOBILEFAIRUSAGEDAILYDATA fu, ccsowner.bsbbillingaccount ba, ccsowner.bsbserviceinstance si,            
                dataprov.customersv2 c
           where ba.serviceinstanceid = si.parentserviceinstanceid
           and fu.serviceinstanceid = si.id
           and c.accountnumber=ba.accountnumber
           and usagetype = 'ROAMPASS'
           and usage > 25*(1024*1024*1024)
           and rownum <=50000
           UNION
           select distinct fu.billingperiodenddate closeddate, ba.accountnumber, fu.serviceinstanceid fu_service, 
                           c.partyid, c.messotoken
           from CCSOWNER.MOBILEFAIRUSAGEDAILYDATA fu, ccsowner.bsbbillingaccount ba, ccsowner.bsbserviceinstance si,            
                dataprov.customersv2 c
           where ba.serviceinstanceid = si.parentserviceinstanceid
           and fu.serviceinstanceid = si.id
           and c.accountnumber=ba.accountnumber
           and usagetype != 'ROAMPASS'
           and usage > 550*(1024*1024*1024)
           and rownum <=50000
         ) s
     ORDER BY dbms_random.value;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;

   logger.write ( 'complete' ) ;
END fairusagedatabreach;



END DATA_PREP_12;
/
