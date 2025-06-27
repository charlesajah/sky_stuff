create or replace PACKAGE                   data_prep_11 AS

PROCEDURE engineerOpenApptsDig; 
PROCEDURE soip_only_active_glass_stream;    --This builds a pool for SoIP customers that have no other accounts except SkyGlass or Stream
PROCEDURE actcustportfoliomobile_big2;
PROCEDURE actcustportfolioemobbig;
PROCEDURE actcustmobilemultipletariff;
PROCEDURE digitalBbNoDebtHomeMove;
PROCEDURE digActCustPortMobBigEsim ;
PROCEDURE soipactivestreampuck;
PROCEDURE hub4_devices_dispatched;
PROCEDURE bt_existing_mobile;
PROCEDURE bt_existing_q;
PROCEDURE bt_existing_plus;
PROCEDURE act_bb_talk_assurance_ots;
PROCEDURE digital_bb_upgrade_tventonly;
PROCEDURE q_discoveryplus;
PROCEDURE digitaltvrecontracting_noburn;
PROCEDURE bbRegrade2024;
PROCEDURE incompletesales;
PROCEDURE noNetflixNoBurn;
PROCEDURE creditcardverification;


END data_prep_11 ;

create or replace PACKAGE BODY data_prep_11 AS

PROCEDURE engineerOpenApptsDig is
  l_pool VARCHAR2(29) := 'ENGINEEROPENAPPTSDIG' ;
BEGIN
  logger.write ( 'begin' ) ;
  execute immediate 'truncate table ' || l_pool ;

  insert into ENGINEEROPENAPPTSDIG
  select /*+ parallel(8) */ distinct vr.fmsjobreference, vr.visitid,  vr.visitdate,  vr.jobtype, vr.jobdescription, vr.notes, bba.accountNumber,
            bpr.partyId, apt.CustomerOrderID, apt.AppointmentID, bcr.customerStatusCode, cus.messotoken
       from ccsowner.bsbvisitrequirement vr,  ccsowner.bsbinstallerrole ir,
            ccsowner.bsbpropertydetail   pd,  ccsowner.bsbaddressusagerole au,
            ccsowner.bsbBillingAccount bba, CCSOWNER.bsbServiceInstance bsi,
            ccsowner.bsbCustomerRole bcr, ccsowner.bsbPartyRole bpr,
            oh.customerorders@oms co, oh.appointments@oms apt, customers cus
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
        and cus.partyid = bpr.partyid
        and vr.visitdate between trunc(sysdate)-7 and add_months(sysdate,2)
        and bsi.SERVICEINSTANCETYPE not in (100,400)  --  100 = Telephony (Sky Talk), 400 = Broadband
        and VR.STATUSCODE in ('BK','CN','CP','CF','IC','UB','UN','DC') --for deepa
        and co.CustomeraccountNumber = bba.accountNumber
        and co.CustomerpartyId = bpr.partyId
        and apt.CustomerOrderID = co.CustomerOrderID
        and vr.fmsJobReference LIKE 'VR%' 
        and apt.appointmentreference = vr.fmsJobReference;

   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.fmsjobreference , t.visitid , t.visitdate , t.jobtype
        , t.jobdescription , t.accountNumber , t.partyId , t.customerorderid , t.appointmentid, t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.fmsjobreference , s.visitid , s.visitdate , s.jobtype
        , s.jobdescription , s.accountNumber , s.partyId , s.customerorderid , s.appointmentid, s.messotoken
     FROM ( SELECT * FROM ENGINEEROPENAPPTSDIG WHERE ROWNUM <= 75000 ORDER BY dbms_random.value ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
end engineerOpenApptsDig; 

Procedure customer_account_types IS         --creates a supporting table for procedure soip_customers_other_accounts which is called by the pool SOIP_ONLY_ACTIVE_GLASS_STREAM.
                                            --This procedure loads the CUSTOMER_ACCOUNT_TYPES table with all customers including SoIP customers
TABLE_NAME VARCHAR2(29) := 'CUSTOMER_ACCOUNT_TYPES' ;
BEGIN
   execute immediate 'truncate table ' || TABLE_NAME ;
   INSERT /*+ append */ INTO CUSTOMER_ACCOUNT_TYPES 
   SELECT /* parallel(ba 16) pq_distribute(ba hash hash)
          parallel(cr 16) pq_distribute(cr hash hash)
          parallel(pr 16) pq_distribute(pr hash hash)
          parallel(p 16) pq_distribute(p hash hash)
          parallel(pti 16) pq_distribute(pti hash hash)
          parallel(si 16) pq_distribute(si hash hash)
          parallel(serv 16) pq_distribute(serv hash hash)
       */
       distinct ba.accountNumber
     , ba.created
     , ba.createdBy
     , ba.currencyCode
     , ba.paymentDueDay
     , ba.organisation_unit AS orgunit
     , CASE WHEN ba.organisation_unit = '20' THEN 'NOW TV'
            WHEN si.serviceInstanceType IN ( 210 , 100 , 400 ) THEN 'Core'
            WHEN si.serviceInstanceType IN ( 610 , 620  ) THEN 'Mobile'
            WHEN p.onlineProfileId IS NOT NULL AND si.id IS NULL THEN 'Sky Store'
            WHEN serv.serviceType = 'AMP' THEN 'AMP'
            WHEN serv.id IS NOT NULL THEN 'SoIP'
            ELSE NULL
            END AS accountType
     , p.partyId
     , p.onlineProfileId as profileId
     , pti.identityid as nsProfileId
     , ba.portfolioId
     , cr.debtIndicator
     , p.blockPurchaseSwitch
     , cr.unableToPurchaseProductsSwitch
     , cr.customerStatusCode
     , ba.accountStatusCode
     , ba.customerSubTypeCode
  FROM ccsowner.bsbBillingAccount ba
  JOIN ccsowner.bsbCustomerRole cr ON ba.portfolioId = cr.portfolioId
  JOIN ccsowner.bsbPartyRole pr ON cr.partyRoleId = pr.id
  JOIN ccsowner.person p ON pr.partyId = p.partyId
  LEFT OUTER JOIN ccsowner.bsbPartyToIdentity pti ON p.partyId = pti.partyId AND pti.identityType = 'NSPROFILEID'
  LEFT OUTER JOIN ccsowner.bsbServiceInstance si ON si.parentServiceInstanceId = ba.serviceInstanceId
  LEFT OUTER JOIN rcrm.service serv ON serv.billingServiceInstanceId = ba.serviceInstanceId  ;

   COMMIT ;
END customer_account_types;

Procedure soip_customers_other_accounts IS         --creates a support table for the pool SOIP_ONLY_ACTIVE_GLASS_STREAM , containing SoIP customers that have other accounts
    TABLE_NAME VARCHAR2(29) := 'SOIP_CUSTOMERS_OTHER_ACCOUNTS' ;
BEGIN
      logger.write ( 'begin' ) ;      
       execute immediate 'truncate table '||TABLE_NAME ;
       INSERT /*+ append */ INTO SOIP_CUSTOMERS_OTHER_ACCOUNTS 
            WITH SoIP_Accounts as(select * from customer_account_types where accounttype='SoIP'),
            OTHER_ACCOUNT_TYPES as(select * from customer_account_types where accounttype !='SoIP')
            Select sa.accountnumber ,sa.partyid,sa.portfolioid  from SoIP_Accounts sa, OTHER_ACCOUNT_TYPES oat
            where sa.partyid=oat.partyid;

       COMMIT ;
      logger.write ( 'complete' ) ;
END soip_customers_other_accounts;

Procedure soip_only_active_glass_stream IS      --This is for SoIP customers that have no other accounts except SkyGlass or Stream
    l_pool VARCHAR2(29) := 'SOIP_ONLY_ACTIVE_GLASS_STREAM' ;
    --start_time pls_integer;
BEGIN
       logger.write ( 'begin' ) ;
       execute immediate 'truncate table ' || l_pool ;
       logger.write ( 'Populate customer_account_types table' ) ;
       --start_time := dbms_utility.get_time;
       customer_account_types;
       --dbms_output.put_line( 'procedure customer_account_types completed in ' ||(dbms_utility.get_time - start_time)/100 || ' seconds');
       logger.write ( 'Completed population of customer_account_types table' ) ;
       logger.write ( 'Populate soip_customers_other_accounts' ) ;
       --start_time := dbms_utility.get_time;
       soip_customers_other_accounts;
       --dbms_output.put_line( 'procedure soip_customers_other_accounts completed in ' ||(dbms_utility.get_time - start_time)/100 || ' seconds');
       logger.write ( 'Completed population of soip_customers_other_accounts' ) ;
       --start_time := dbms_utility.get_time;
       INSERT /*+ append */ INTO SOIP_ONLY_ACTIVE_GLASS_STREAM 
        select 
            accountNumber, 
            partyId, 
            messoToken 
        from 
  (
    SELECT 

      /*+ parallel(8) */
      distinct d.accountNumber, 
      d.partyId, 
      d.messoToken, 
      (
        select 
          count(bar.ID) 
        from 
          ccsowner.BSBBillingAccount acc, 
          ccsowner.BSBBILLINGADDRESSROLE bar 
        where 
          acc.accountnumber = ba.accountnumber 
          AND acc.id = bar.BILLINGACCOUNTID
      ) mh -- where number greater than 1 is TRUE(moving house) by Charles for NFTREL-22298
    FROM 
      dataprov.soipActiveSubscription d 
      JOIN ccsowner.bsbBillingAccount ba ON ba.accountNumber = d.accountNumber --JOIN ccsowner.BSBBILLINGADDRESSROLE bar ON ba.id=bar.BILLINGACCOUNTID
      JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId 
      JOIN rcrm.product p ON s.id = p.serviceId 
      JOIN ccsowner.bsbCustomerRole bcr ON ba.portfolioId = bcr.portfolioId 
      JOIN ccsowner.bsbPartyRole bpr ON bcr.partyroleId = bpr.id 
      JOIN ccsowner.person per ON per.partyId = bpr.partyId 
    WHERE 
      d.serviceType = 'SOIP' 
      AND d.inflightvisit = 0 -- remove any SOIP accounts with inflight visits 
      AND ba.customerSubTypeCode !='ST'  --should exclude customers that are staff
      AND per.partyid NOT IN (
        select 
          partyid 
        from 
          SOIP_CUSTOMERS_OTHER_ACCOUNTS
      ) --SOIP_CUSTOMERS_OTHER_ACCOUNTS table contains all SoIP customers that have other accounts
      AND p.suid IN (
        'LLAMA_SMALL', 'LLAMA_MEDIUM', 'LLAMA_LARGE', 
        'MULTISCREEN_PUCK'
      ) -- Llama = Sky Glass , Puck = Sky Stream
      AND p.status = 'DELIVERED' 
      AND ba.accountnumber NOT IN (
        select 
          acc.accountnumber 
        from 
          dataprov.soipActiveSubscription sas 
          JOIN ccsowner.bsbBillingAccount acc ON acc.accountNumber = sas.accountNumber --JOIN ccsowner.BSBBILLINGADDRESSROLE bar ON ba.id=bar.BILLINGACCOUNTID
          JOIN rcrm.service sv ON acc.serviceInstanceId = sv.billingServiceInstanceId 
          JOIN rcrm.product pr ON sv.id = pr.serviceId 
        WHERE 
          sas.serviceType = 'SOIP' 
          AND pr.status IN (
            'PREACTIVE', 'DISPATCHED', 'PENDING_RETURN', 
            'RETURN_IN_TRANSIT', 'AWAITING_DELIVERY'
          )
      ) 
      AND p.eventCode = 'DELIVERED' --AND ba.currencyCode = 'GBP'  -- Charles, commenting this out includes RoI customers.
      AND NVL (per.blockPurchaseSwitch, 0) != 1 -- Amit, exclude debt block. 1 = customer is in debt block.
      AND NVL (bcr.debtIndicator, 0) != 1 -- Archana, exclude customer in debt. 1 = customer is in debt arrears.
      AND d.accountNumber IN (
        SELECT 
          da.accountNumber 
        FROM 
          dataprov.debt_amount da 
        WHERE 
          da.balance <= 0
      ) -- Charles exclude customers with debt/balance due on their account. 
      ) 
where 
  mh < 2 --check if customer is moving house; where number greater than 1 is TRUE(moving house). NFTREL-22298 does not want customers who are moving house.
  ;

    COMMIT ;
    --dbms_output.put_line( 'Inserts into table SOIP_ONLY_ACTIVE_GLASS_STREAM completed in ' ||(dbms_utility.get_time - start_time)/100 || ' seconds');
       sequence_pkg.seqBefore ( i_pool => l_pool ) ;
       INSERT INTO dprov_accounts_fast daf ( daf.pool_seqno , daf.pool_name , daf.accountNumber , daf.partyId, daf.messoToken )
       SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken 
        FROM (
              SELECT ss.accountnumber , ss.partyid , ss.messoToken 
                FROM soip_only_active_glass_stream ss
                FETCH FIRST 10000 ROWS ONLY
             ) s
             ORDER BY dbms_random.value;

       sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT, i_burn => TRUE  ) ;
       logger.write ( 'complete' ) ;
END soip_only_active_glass_stream;

PROCEDURE actcustportfoliomobile_big2 is
  l_pool VARCHAR2(29) := 'ACTCUSTPORTFOLIOMOBILE_BIG2' ;
BEGIN
  logger.write ( 'begin' ) ;
  execute immediate 'truncate table ' || l_pool ;

  INSERT INTO actcustportfoliomobile_big2 t ( t.accountNumber , t.partyid , t.portfolioId , t.messotoken , t.portfolioproductid )     
    SELECT /*+ parallel(8) */ DISTINCT ba.accountNumber , cus.partyid, ba.portfolioId , cus.messotoken , pp.id   
      FROM ccsowner.bsbBillingAccount ba  
      JOIN ccsowner.bsbPortfolioProduct pp ON ba.portfolioId = pp.portfolioId
      JOIN customers cus ON ba.accountnumber = cus.accountnumber and ba.portfolioId = cus.portfolioId
      JOIN actCustPortfolio_big acpb ON ba.portfolioId = acpb.portfolioId
      WHERE pp.status = 'AC'  -- ACTIVE
        AND pp.catalogueProductId = '14210'  -- 'Unlimited Calls and Texts'  
        AND cus.countryCode = 'GBR'
        AND cus.mobile = 1
        AND cus.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )
    ORDER BY acpb.port_cnt DESC
    FETCH FIRST 50000 ROWS ONLY;

    COMMIT;

  sequence_pkg.seqBefore ( i_pool => l_pool ) ;
  INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyid , t.portfolioId , t.messotoken , t.portfolioproductid )
    SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyid , s.portfolioId , s.messotoken , s.portfolioproductid
       FROM actcustportfoliomobile_big2 s ;

   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT  , i_burn => FALSE) ;
   logger.write ( 'complete' ) ;

end actcustportfoliomobile_big2; 

PROCEDURE actcustportfolioemobbig is
  l_pool VARCHAR2(29) := 'ACTCUSTPORTFOLIOEMOBBIG' ;
BEGIN
  logger.write ( 'begin' ) ;
  execute immediate 'truncate table ' || l_pool ;

  INSERT INTO actcustportfolioemobbig t ( t.accountNumber , t.partyid , t.portfolioId , t.messotoken , t.portfolioproductid )     
    SELECT /*+ parallel(8) */ DISTINCT ba.accountNumber , cus.partyid, ba.portfolioId , cus.messotoken , pp.id 
      FROM ccsowner.bsbBillingAccount ba  
      JOIN ccsowner.bsbPortfolioProduct pp ON ba.portfolioId = pp.portfolioId
      JOIN customers cus ON ba.accountnumber = cus.accountnumber and ba.portfolioId = cus.portfolioId
      JOIN actCustPortfolio_big acpb ON ba.portfolioId = acpb.portfolioId
      WHERE pp.catalogueProductId = '15861'  -- 'MOB_ESIM'
        AND pp.status IN ('DS','AD')  -- DELIVERED / AWAITING DELIVERY
        AND cus.countryCode = 'GBR'
        AND cus.mobeSim = 1  -- > eSIM customers  RFA 25/09/23 Added as it speeds up the query ( new flag within the CUSTOMERS table )
        AND cus.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )
    ORDER BY acpb.port_cnt DESC
    FETCH FIRST 50000 ROWS ONLY;

    COMMIT;

  sequence_pkg.seqBefore ( i_pool => l_pool ) ;
  INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyid , t.portfolioId , t.messotoken , t.portfolioproductid )
    SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyid , s.portfolioId , s.messotoken , s.portfolioproductid
       FROM actcustportfolioemobbig s ;

   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT  , i_burn => FALSE) ;
   logger.write ( 'complete' ) ;

end actcustportfolioemobbig; 

PROCEDURE actcustmobilemultipletariff is
  l_pool VARCHAR2(29) := 'ACTCUSTMOBILEMULTIPLETARIFF' ;
BEGIN
  logger.write ( 'begin' ) ;
  execute immediate 'truncate table ' || l_pool ;

  INSERT INTO actcustmobilemultipletariff t ( t.accountNumber , t.partyid , t.portfolioId , t.messotoken )     
    SELECT /*+ parallel(8) */ 
             MAX (ba.accountNumber)  
            ,MAX (cus.partyid)   
            ,MAX (ba.portfolioId)  
            ,MAX (cus.messotoken)  
        FROM ccsowner.bsbBillingAccount ba  
        JOIN ccsowner.bsbPortfolioProduct pp ON ba.portfolioId = pp.portfolioId
        JOIN customers cus ON ba.accountnumber = cus.accountnumber and ba.portfolioId = cus.portfolioId
        WHERE pp.catalogueProductId = '14210'  -- 'MOBILE TALK UNLIMITED TEXT UNLIMITED'
        AND   pp.status IN ('AC')  -- ACTIVE
        AND   cus.countryCode = 'GBR'
        AND   cus.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- No debt
        GROUP by ba.accountNumber
           HAVING COUNT(ba.accountNumber) > 2
    FETCH FIRST 50000 ROWS ONLY;

    COMMIT;

  sequence_pkg.seqBefore ( i_pool => l_pool ) ;
  INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyid , t.portfolioId , t.messotoken )
    SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyid , s.portfolioId , s.messotoken 
       FROM actcustmobilemultipletariff s ;

   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT  , i_burn => FALSE) ;
   logger.write ( 'complete' ) ;

end actcustmobilemultipletariff; 

PROCEDURE digitalBbNoDebtHomeMove IS
   -- https://cbsjira.bskyb.com/browse/NFTREL-22340
   l_pool VARCHAR2(29) := 'DIGITALBBNODEBTHOMEMOVE' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messotoken )
   Select ROWNUM AS pool_seqno , l_pool AS pool_name , c.accountNumber , c.partyId, c.messotoken /*+ parallel(c 8)  */
   FROM customers c
   JOIN (
           SELECT bba.accountNumber
                , MAX ( SUBSTR ( pm.accountNumber , 1 , 2 ) ) AS code
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
    WHERE c.entertainment = 0
    AND c.cinema = 0
    AND c.dtv = 0
    AND c.mobile = 0
    AND c.completesports = 0
    AND c.skysignature = 0 
    AND c.ultimatetvaddon = 0
    AND c.skyQBox = 0
    AND c.skyHDBox = 0
    AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
    AND c.countryCode = 'GBR'
    AND c.pool IS NULL
    AND c.inflightorders=0
    AND EXISTS ( SELECT null FROM debt_amount da WHERE da.balance <= 0 and c.accountNumber = da.accountNumber )
    AND c.portfolioid NOT IN (
        SELECT po.portfolioid
        FROM ccsowner.bsbpowerofattorneyrole po
        WHERE ( po.effectivetodate IS NULL OR po.effectivetodate > SYSDATE )
          ) 
    AND c.portfolioid IN ( SELECT i.portfolioid from bbregrade_portfolioid_tmp i )  -- from customers_pkg.buildSupportingTables
    AND EXISTS ( select null
                   from ccsowner.bsbportfolioproduct bpp
                   where c.portfolioid=bpp.portfolioid
                      -- changed for Anish Vora, 17/10/2023 JIRA : NFTREL-22348
                      --AND bpp.catalogueproductid in ('15193', '15192', '14960', '14281' ,'14938', '14061', '13831', '13487','14064','14062' ,'11946', '13575', '13574', '13573','13636','11945' , '15334', '15196'     ))
                      and bpp.catalogueproductid in ('15193', '15192', '14960' ,'14938', '14061', '13487','13575', '13574','13636','11945'))
    AND ROWNUM <= 10000;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE) ;
   logger.write ( 'complete' ) ;
END digitalBbNoDebtHomeMove ;

PROCEDURE digActCustPortMobBigEsim IS
   l_pool VARCHAR2(29) := 'DIGACTCUSTPORTMOBBIGESIM' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO digActCustPortMobBigEsim t ( t.accountnumber , t.partyid , t.portfolioId , t.messoToken , t.port_cnt )
SELECT /*+ parallel(c, 8) */ c.accountnumber , c.partyid , c.portfolioId , c.messoToken , acpb.port_cnt
     FROM actCustPortfolio_big acpb
     JOIN customers c ON c.portfolioId = acpb.portfolioId
    WHERE c.countryCode = 'GBR'
      AND c.mobile = 1
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )
      AND c.emailAddress NOT LIKE 'noemail%'
      AND c.inFlightVisit = 0
      AND c.inFlightOrders = 0
      and c.messotoken not like '%NO-NSPROFILE' -- ignore customers with no nsprofileid
      -- customers must have home telephone number
      and exists ( select /*+ parallel(8) */ 1 
                     from ccsowner.bsbcontactor bc, ccsowner.bsbcontacttelephone bct
                    where bc.partyid = c.partyid
                      and bc.id = bct.contactorid
                      and bct.typecode = 'H'
                      and (bct.effectivetodate is null or bct.effectivetodate > sysdate)
                 ) 
      and not exists ( select /*+ parallel(8) */ 1 
                     from ccsowner.bsbportfolioproduct bpp
                    where bpp.portfolioid = c.portfolioid
                      and bpp.status = 'R'
                 ) 
   ;

   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.portfolioId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.portfolioId , s.messoToken
    FROM (
          SELECT d.accountnumber , d.partyid , d.portfolioId , d.messoToken
            FROM digActCustPortMobBigEsim d
           ORDER BY d.port_cnt DESC
           FETCH FIRST 100000 ROWS ONLY
         ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END digActCustPortMobBigEsim ;

PROCEDURE soipactivestreampuck IS
   l_pool VARCHAR2(29) := 'SOIPACTIVESTREAMPUCK' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken, t.partnumber, t.serialnumber )
   SELECT /*+ parallel (d 8) parallel (ba 8) parallel (s 8) parallel (p 8) parallel (bba 8) parallel (bpp 8) parallel (hpe 8) parallel (h 8)*/ 
          distinct ROWNUM AS pool_seqno , l_pool AS pool_name , d.accountNumber , d.partyId , d.messoToken, h.sku, hpe.serialnumber
     FROM soipActivesubscription d
     JOIN ccsowner.bsbBillingAccount ba ON ba.accountNumber = d.accountNumber
     JOIN rcrm.service s ON ba.serviceInstanceId = s.billingserviceInstanceId
     JOIN rcrm.product p ON s.id = p.serviceId
     JOIN ccsowner.bsbBillingAccount bba ON bba.accountnumber=d.accountnumber
     JOIN ccsowner.bsbPortfolioProduct bpp ON bba.portfolioId = bpp.portfolioId
     JOIN rcrm.hardwareProdTechelement hpe ON hpe.productId = p.id
     JOIN rcrm.HARDWAREPRODUCTFULFILMENT h ON h.productid = p.id
   WHERE p.suid IN ( 'MULTISCREEN_PUCK' )
   AND p.status = 'DELIVERED'
   AND p.eventCode = 'DELIVERED'
   AND h.SKU='PUIP061ANTP' -- At the request of Edwin Scariachan
   AND rownum <100000;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT, i_burn => FALSE  ) ;
   logger.write ( 'complete' ) ;
END soipactivestreampuck ;

PROCEDURE hub4_devices_dispatched IS
   l_pool VARCHAR2(29) := 'HUB4_DEVICES_DISPATCHED' ;
BEGIN
   logger.write ( 'begin' ) ;
    execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO hub4_devices_dispatched t ( t.accountnumber , t.partyid , t.messoToken ) 
   SELECT /*parallel (c 8) parallel (ba 8) parallel (serv 8) parallel (pp 8) parallel (cp 8) */  
          DISTINCT ba.accountNumber , c.partyid, c.messotoken
     FROM ccsowner.bsbBillingAccount ba
     JOIN rcrm.service serv ON ba.serviceInstanceId = serv.billingServiceInstanceId
     JOIN ccsowner.bsbPortfolioProduct pp ON ba.portfolioId = pp.portfolioId
     JOIN refdatamgr.bsbCatalogueProduct cp ON pp.catalogueProductId = cp.id
     JOIN customers c ON c.accountnumber=ba.accountnumber
    WHERE pp.catalogueProductId = '15059'
    AND pp.status in ( 'DS');
   COMMIT ;

   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken
    FROM (
          SELECT d.accountnumber , d.partyid , d.messoToken
            FROM hub4_devices_dispatched d
         ) s
    ORDER BY dbms_random.value
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT, i_burn => FALSE  ) ;
   logger.write ( 'complete' ) ;
END hub4_devices_dispatched ;

PROCEDURE BT_EXISTING_MOBILE IS
   -- https://cbsjira.bskyb.com/browse/NFTREL-22360
   -- 22-Sep-2022 Antti - Existing Mobile customers with UPRN '72237879' without BBT or SOIP for BT Dist testing.
   l_pool VARCHAR2(29) := 'BT_EXISTING_MOBILE' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO BT_EXISTING_MOBILE  (partyid , accountnumber , uprn , messotoken)
    SELECT /*+ parallel(8) */ bpr.partyid,ba.accountnumber,adr.uprn,ctk.messotoken
      FROM ccsowner.bsbcontactaddress   bct,
           ccsowner.bsbcontactor        bc,
           ccsowner.bsbpartyrole        bpr,
           ccsowner.bsbcustomerrole     bcr,
           ccsowner.bsbbillingaccount   ba,
           ccsowner.bsbaddress          adr,
           dataprov.customers           cus,
           ccsowner.bsbServiceInstance si,
           dataprov.customertokens ctk
     WHERE bc.id = bct.contactorid
       AND bpr.partyid = bc.partyid
       AND bcr.partyroleid = bpr.id
       AND ba.portfolioid = bcr.portfolioid 
       --AND ba.portfolioId = bpp.portfolioId
       AND bct.addressid=adr.id
       AND si.parentServiceInstanceId = ba.serviceInstanceId
       and adr.uprn =72237879
       --and bpp.id=bcpe.portfolioproductid
       --and bcpe.status != 'R'
       AND bpr.partyid = cus.partyid 
       and cus.mobile=1
       and cus.bband=0
       and cus.talk=0
       and cus.dtv=0
       and cus.skyQBox=0
       and cus.skyHDBox=0
       and cus.accountnumber2 is null
       and cus.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )
       and bpr.partyid NOT in( SELECT bpr.partyId               --exclude SOIP customers
                                 FROM ccsowner.bsbBillingAccount ba
                                 JOIN ccsowner.bsbCustomerRole bcr ON ba.portfolioId = bcr.portfolioId
                                 JOIN ccsowner.bsbPartyRole bpr ON bcr.partyroleId = bpr.id
                                 JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
                                 JOIN rcrm.product p ON s.id = p.serviceId
                                 WHERE s.serviceType = 'SOIP')
        and cus.emailAddress NOT LIKE 'noemail%'  --  must have a primary email address.
        and cus.accountNumber not in (select accountnumber from dataprov.BBAND_TMP) --filter out any accounts that have ever had bband or Talk active or not
        --and bpr.partyid not in (select partyid from dataprov.MULT_UPRN_M)  --filter out any partyid with multiple UPRN/addresses
        and ctk.accountnumber (+)= ba.accountnumber
     group by bpr.partyid,ba.accountnumber,adr.uprn,ctk.messotoken
                   ORDER BY dbms_random.value
                   fetch first 10000 rows only;
   
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   -- only want accounts that have a mobile number as the primary contact number .
   --  also remove if international dialing code is not '+44' . 
   DELETE FROM BT_EXISTING_MOBILE d
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
   COMMIT;
   -- remove accounts that have no primary email address.
   DELETE FROM BT_EXISTING_MOBILE d
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
   COMMIT;
   --  removing accounts that have any subscriptions with a saleState that is not active (originally "exclude any customer with SOIP (even if under a different accountNumber)").
   DELETE FROM BT_EXISTING_MOBILE d
    WHERE EXISTS (
          SELECT NULL
            FROM ccsowner.bsbBillingAccount ba
            JOIN ccsowner.bsbServiceInstance si ON ba.portfolioId = si.portfolioId
            JOIN rcrm.service serv ON si.id = serv.billingServiceInstanceId
           WHERE d.accountNumber = ba.accountNumber
          )
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for saleState not active' ) ;
   COMMIT;
   
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId ,t.uprn, t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId ,s.uprn, s.messotoken
     FROM (
           select accountnumber,partyid,uprn,messotoken from 
           bt_existing_mobile
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END BT_EXISTING_MOBILE ;

PROCEDURE BT_EXISTING_Q IS
-- 24 November-2023 Created for Antti by Charles
--https://cbsjira.bskyb.com/browse/NFTREL-22361

   l_pool VARCHAR2(29) := 'BT_EXISTING_Q' ;
BEGIN
    logger.write ( 'begin' ) ;
    execute immediate 'truncate table dataprov.MULT_UPRN_Q'; --mult_uprn_q is used to stage all partyids with multiple UPRNs for all SKYQ customers
    execute immediate 'truncate table dataprov.MULT_UPRN_M'; --mult_uprn_q is used to stage all partyids with multiple UPRNs for all SKY Mobile customers
    execute immediate 'truncate table dataprov.BBAND_TMP'; --staging table to hold customers with serviceinstancetype 400 or 100 
    
    insert into dataprov.bband_tmp
    select ba.accountnumber,serviceInstanceType from  ccsowner.bsbBillingAccount ba join ccsowner.bsbServiceInstance si
    ON si.parentServiceInstanceId = ba.serviceInstanceId
    and serviceInstanceType  in (400,100);
    logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
    commit;
    
    insert into dataprov.MULT_UPRN_Q
    SELECT count(*) uprn_count,bpr.partyid
FROM ccsowner.bsbcontactaddress   bct,
       ccsowner.bsbcontactor        bc,
       ccsowner.bsbpartyrole        bpr,
       ccsowner.bsbcustomerrole     bcr,
       ccsowner.bsbbillingaccount   ba,
       ccsowner.bsbaddress          adr,
       dataprov.customers           cus,
       ccsowner.bsbServiceInstance si
 WHERE bc.id = bct.contactorid
   AND bpr.partyid = bc.partyid
   AND bcr.partyroleid = bpr.id
   AND ba.portfolioid = bcr.portfolioid 
   --AND ba.portfolioId = bpp.portfolioId
   AND bct.addressid=adr.id
   AND si.parentServiceInstanceId = ba.serviceInstanceId
   AND adr.uprn is not null
   AND bpr.partyid = cus.partyid 
   and cus.mobile=0
   and cus.bband=0
   and cus.talk=0
   and cus.dtv=1
   and cus.skyQBox=1
   and cus.skyHDBox=0
   and cus.accountnumber2 is null
   and cus.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )
   and bpr.partyid NOT in( SELECT bpr.partyId
                             FROM ccsowner.bsbBillingAccount ba
                             JOIN ccsowner.bsbCustomerRole bcr ON ba.portfolioId = bcr.portfolioId
                             JOIN ccsowner.bsbPartyRole bpr ON bcr.partyroleId = bpr.id
                             JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
                             JOIN rcrm.product p ON s.id = p.serviceId
                             WHERE s.serviceType = 'SOIP')
    and cus.accountNumber not in (select accountnumber from dataprov.BBAND_TMP) --filter out any accounts that have ever had bband or Talk whether active or not
    group by bpr.partyid
    having count(*) > 1;
    logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
    commit;

insert into dataprov.mult_uprn_m
SELECT count(*) uprn_count,bpr.partyid
FROM ccsowner.bsbcontactaddress   bct,
       ccsowner.bsbcontactor        bc,
       ccsowner.bsbpartyrole        bpr,
       ccsowner.bsbcustomerrole     bcr,
       ccsowner.bsbbillingaccount   ba,
       ccsowner.bsbaddress          adr,
       dataprov.customers           cus,
       ccsowner.bsbServiceInstance si
 WHERE bc.id = bct.contactorid
   AND bpr.partyid = bc.partyid
   AND bcr.partyroleid = bpr.id
   AND ba.portfolioid = bcr.portfolioid 
   --AND ba.portfolioId = bpp.portfolioId
   AND bct.addressid=adr.id
   AND si.parentServiceInstanceId = ba.serviceInstanceId
   AND adr.uprn is not null
   AND bpr.partyid = cus.partyid 
   and cus.mobile=1
   and cus.bband=0
   and cus.talk=0
   and cus.dtv=0
   and cus.skyQBox=0
   and cus.skyHDBox=0
   and cus.accountnumber2 is null
   and cus.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )
   and bpr.partyid NOT in( SELECT bpr.partyId
                             FROM ccsowner.bsbBillingAccount ba
                             JOIN ccsowner.bsbCustomerRole bcr ON ba.portfolioId = bcr.portfolioId
                             JOIN ccsowner.bsbPartyRole bpr ON bcr.partyroleId = bpr.id
                             JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
                             JOIN rcrm.product p ON s.id = p.serviceId
                             WHERE s.serviceType = 'SOIP')
    and cus.accountNumber not in (select accountnumber from dataprov.BBAND_TMP) --filter out any accounts that have ever had bband or Talk whether active or not
    group by bpr.partyid
    having count(*) > 1;
    logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
    commit;
    
    

   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name ,t.partyid , t.accountnumber , t.uprn, t.messotoken)
   select ROWNUM AS pool_seqno, l_pool AS pool_name, partyid, accountnumber,  uprn, messotoken 
     from (SELECT /*+ parallel(8) */ bpr.partyid,ba.accountnumber,adr.uprn,ctk.messotoken
  FROM ccsowner.bsbcontactaddress   bct,
       ccsowner.bsbcontactor        bc,
       ccsowner.bsbpartyrole        bpr,
       ccsowner.bsbcustomerrole     bcr,
       ccsowner.bsbbillingaccount   ba,
       ccsowner.bsbaddress          adr,
       dataprov.customers           cus,
       ccsowner.bsbServiceInstance si,
       dataprov.customertokens ctk
 WHERE bc.id = bct.contactorid
   AND bpr.partyid = bc.partyid
   AND bcr.partyroleid = bpr.id
   AND ba.portfolioid = bcr.portfolioid 
   --AND ba.portfolioId = bpp.portfolioId
   AND bct.addressid=adr.id
   AND si.parentServiceInstanceId = ba.serviceInstanceId
   and adr.uprn=72237879
   --and bpp.id=bcpe.portfolioproductid
   --and bcpe.status != 'R'
   AND bpr.partyid = cus.partyid 
   and cus.mobile=0
   and cus.bband=0
   and cus.talk=0
   and cus.dtv=1
   and cus.skyQBox=1
   and cus.skyHDBox=0 
   and cus.accountnumber2 is null
   and cus.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )
   and bpr.partyid NOT in( SELECT bpr.partyId
                             FROM ccsowner.bsbBillingAccount ba
                             JOIN ccsowner.bsbCustomerRole bcr ON ba.portfolioId = bcr.portfolioId
                             JOIN ccsowner.bsbPartyRole bpr ON bcr.partyroleId = bpr.id
                             JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
                             JOIN rcrm.product p ON s.id = p.serviceId
                             WHERE s.serviceType = 'SOIP')
    and bpr.partyid IN (  ----- remove accounts that have no UK mobile contact number.
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
             AND bct.typecode='M'   --must have mobile phone contact with type=M
          )
    and cus.emailAddress NOT LIKE 'noemail%'  --  must have a primary email address.
    and cus.accountNumber not in (select accountnumber from dataprov.BBAND_TMP) --filter out any accounts that have ever had bband or Talk active or not
    and bpr.partyid not in (select partyid from dataprov.MULT_UPRN_Q)  --filter out any partyid with multiple UPRN
    and ctk.accountnumber (+)= ba.accountnumber
 group by bpr.partyid,ba.accountnumber,adr.uprn,ctk.messotoken
               ORDER BY dbms_random.value
               fetch first 10000 rows only) ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => FALSE ) ;
   logger.write ( 'complete' ) ;
END BT_EXISTING_Q ; 

PROCEDURE BT_EXISTING_PLUS IS
-- 27 November-2023 Created for Antti by Charles
--Existing Sky+ customers without BBT or SOIP for BT Dist testing
--https://cbsjira.bskyb.com/browse/NFTREL-22364

   l_pool VARCHAR2(29) := 'BT_EXISTING_PLUS' ;
BEGIN
    logger.write ( 'begin' ) ;
    execute immediate 'truncate table dataprov.MULT_UPRN_SKYHD'; --mult_uprn is used to stage all partyids with multiple UPRNs
    insert into dataprov.MULT_UPRN_SKYHD
SELECT count(*) uprn_count,bpr.partyid
FROM ccsowner.bsbcontactaddress   bct,
       ccsowner.bsbcontactor        bc,
       ccsowner.bsbpartyrole        bpr,
       ccsowner.bsbcustomerrole     bcr,
       ccsowner.bsbbillingaccount   ba,
       ccsowner.bsbaddress          adr,
       dataprov.customers           cus,
       ccsowner.bsbServiceInstance si
 WHERE bc.id = bct.contactorid
   AND bpr.partyid = bc.partyid
   AND bcr.partyroleid = bpr.id
   AND ba.portfolioid = bcr.portfolioid 
   --AND ba.portfolioId = bpp.portfolioId
   AND bct.addressid=adr.id
   AND si.parentServiceInstanceId = ba.serviceInstanceId
   AND adr.uprn is not null
   AND bpr.partyid = cus.partyid 
   and cus.mobile=0
   and cus.bband=0
   and cus.talk=0
   and cus.dtv=1
   and cus.skyQBox=0
   and cus.skyHDBox=1
   and cus.accountnumber2 is null
   and cus.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )
   and bpr.partyid NOT in( SELECT bpr.partyId
                             FROM ccsowner.bsbBillingAccount ba
                             JOIN ccsowner.bsbCustomerRole bcr ON ba.portfolioId = bcr.portfolioId
                             JOIN ccsowner.bsbPartyRole bpr ON bcr.partyroleId = bpr.id
                             JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
                             JOIN rcrm.product p ON s.id = p.serviceId
                             WHERE s.serviceType = 'SOIP')
    and cus.accountNumber not in (select accountnumber from dataprov.BBAND_TMP) --filter out any accounts that have ever had bband or Talk whether active or not
    group by bpr.partyid
    having count(*) > 1;
    
    logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
    commit;
    
    sequence_pkg.seqBefore ( i_pool => l_pool ) ;
    INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name ,t.partyid , t.accountnumber , t.uprn, t.messotoken)
   select ROWNUM AS pool_seqno, l_pool AS pool_name, partyid, accountnumber,  uprn, messotoken 
     from (SELECT /*+ parallel(8) */ bpr.partyid,ba.accountnumber,adr.uprn,ctk.messotoken
  FROM ccsowner.bsbcontactaddress   bct,
       ccsowner.bsbcontactor        bc,
       ccsowner.bsbpartyrole        bpr,
       ccsowner.bsbcustomerrole     bcr,
       ccsowner.bsbbillingaccount   ba,
       ccsowner.bsbaddress          adr,
       dataprov.customers           cus,
       ccsowner.bsbServiceInstance si,
       dataprov.customertokens ctk
 WHERE bc.id = bct.contactorid
   AND bpr.partyid = bc.partyid
   AND bcr.partyroleid = bpr.id
   AND ba.portfolioid = bcr.portfolioid 
   --AND ba.portfolioId = bpp.portfolioId
   AND bct.addressid=adr.id
   AND si.parentServiceInstanceId = ba.serviceInstanceId
   and adr.uprn=72237879
   --and bpp.id=bcpe.portfolioproductid
   --and bcpe.status != 'R'
   AND bpr.partyid = cus.partyid 
   and cus.mobile=0
   and cus.bband=0
   and cus.talk=0
   and cus.dtv=1
   and cus.skyQBox=0
   and cus.skyHDBox=1 
   and cus.accountnumber2 is null
   and cus.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )
   and bpr.partyid NOT in( SELECT bpr.partyId               --exclude SOIP customers
                             FROM ccsowner.bsbBillingAccount ba
                             JOIN ccsowner.bsbCustomerRole bcr ON ba.portfolioId = bcr.portfolioId
                             JOIN ccsowner.bsbPartyRole bpr ON bcr.partyroleId = bpr.id
                             JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
                             JOIN rcrm.product p ON s.id = p.serviceId
                             WHERE s.serviceType = 'SOIP')
    and bpr.partyid  IN (  ----- remove accounts that have no UK mobile contact number.
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
             AND bct.typecode='M'   --must have mobile phone contact with type=M
          )
    and cus.emailAddress NOT LIKE 'noemail%'  --  must have a primary email address.
    and cus.accountNumber not in (select accountnumber from dataprov.BBAND_TMP) --filter out any accounts that have ever had bband or Talk active or not
    and bpr.partyid not in (select partyid from dataprov.MULT_UPRN_SKYHD)  --filter out any partyid with multiple UPRN/addresses
    and ctk.accountnumber (+)= ba.accountnumber
 group by bpr.partyid,ba.accountnumber,adr.uprn,ctk.messotoken
               ORDER BY dbms_random.value
               fetch first 10000 rows only) ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => FALSE ) ;
   logger.write ( 'complete' ) ;
END BT_EXISTING_PLUS ; 

PROCEDURE act_bb_talk_assurance_ots IS
   l_pool VARCHAR2(29) := 'ACT_BB_TALK_ASSURANCE_OTS' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT INTO act_bb_talk_assurance_ots t ( t.accountNumber , t.portfolioId, t.partyId , t.familyname , t.addressline , t.postcode , t.town , t.talk , t.bband, t.uprn  )
   SELECT accountNumber , portfolioId , partyId , familyname , addressline1 , postcode , town , talk , bband, uprn
     FROM (
          SELECT /*+ full(si)   parallel(si 16)   pq_distribute ( si hash hash )
                     full(subs) parallel(subs 16) pq_distribute ( subs hash hash )
                     full(act)  parallel(act 16)  pq_distribute ( act hash hash )
                     full(c)    parallel(c 16)    pq_distribute ( c hash hash )
                     full(pd)   parallel(pd 16)   pq_distribute ( pd hash hash )
                     full(p)    parallel(p 16)   pq_distribute ( p hash hash )
                   */
                  act.accountNumber
                , subs.subscriptionTypeId
                , si.id
                , si.portfolioId
                , c.partyId
                , per.familyname
                , cd.addressline1
                , cd.postcode
                , cd.town
                , cd.uprn
             FROM ccsowner.bsbServiceInstance si
             JOIN ccsowner.bsbsubscription subs ON subs.serviceInstanceId = si.id
             JOIN act_uk_cust act ON act.portfolioId = si.portfolioId
             JOIN customers c ON c.accountNumber = act.accountNumber
             JOIN ccsowner.person per ON per.partyid = c.partyid
             JOIN ccsowner.bsbcontactor ct ON ct.partyid = per.partyid
             JOIN ccsowner.bsbcontactaddress ca ON ca.contactorid = ct.id
             JOIN ccsowner.bsbaddress cd ON cd.id = ca.addressid 
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
              AND ca.primaryflag = 1 
              AND ca.effectivetodate is NULL      
           ) PIVOT ( MAX ( id ) FOR ( subscriptionTypeId ) IN ( '3' AS talk , '7' AS bband ) )
    WHERE talk  IS NOT NULL
      AND bband IS NOT NULL
   ;
   logger.write ( 'rows inserted ' || TO_CHAR ( SQL%ROWCOUNT ) ) ;

   -- 21-Sep-2018 NFTREL-15224 exclude customers who already have a visit booked.
   -- 04-Feb-2022 Andrew Fraser for Dimitrios Koulialis, made stricter NFTREL-21394 (tpoc only 04-Feb-2022)
   DELETE FROM act_bb_talk_assurance_ots t
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
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for customers who already have a visit booked' ) ;

   -- 04-Feb-2022 Andrew Fraser for Dimitrios Koulialis, remove customers with an open outage NFTREL-21394. Fixes FAVsessionCreate test script errors.
   DELETE FROM act_bb_talk_assurance_ots t
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
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for customers with an open outage' ) ;

   -- remove postcodes which have bad data skew
   -- AH 23/07/2024
   delete from act_bb_talk_assurance_ots where postcode in ('HP197FU','ME207FU');
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' deleted for badly skewed postcode' ) ;

   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.portfolioid, t.partyId , t.bb_serviceid , t.talk_serviceid, t.surname, t.postcode, t.town, t.street, t.uprn )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.portfolioid, s.partyId , s.bband , s.talk , s.familyname, s.postcode, s.town, s.addressline, s.uprn 
     FROM (
   SELECT *
     FROM act_bb_talk_assurance_ots
    WHERE ROWNUM <= 100000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END act_bb_talk_assurance_ots ;

    PROCEDURE digital_bb_upgrade_tventonly IS
   l_pool VARCHAR2(29) := 'DIGITAL_BB_UPGRADE_TVENTONLY' ;
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
     WHERE c.boxSets = 0
       AND c.cinema = 0
       AND c.sports = 0
       AND c.kids = 0
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
                      AND con.escalationcode IS NULL
                  )
       AND c.countryCode = 'GBR'
       AND c.accountNumber IN (
           SELECT a.accountnumber
             FROM dataprov.act_cust_uk_subs a
            WHERE a.bband IS NULL
           )
       AND c.emailAddress NOT LIKE 'noemail%'  -- 19-Aug-2019 only include customers who have a valid email, request Shane Venter.
       AND ROWNUM <= 10000
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
END digital_bb_upgrade_tventonly ;

PROCEDURE q_discoveryplus IS
-- 9th February 2024 Created for NFTREL-22383
   l_pool VARCHAR2(29) := 'Q_DISCOVERYPLUS' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.messotoken)
   select ROWNUM AS pool_seqno, l_pool AS pool_name , accountNumber
        , partyId
        , messoToken 
   from (select /*+ parallel (8) */ ba.accountNumber
        , ct.partyId
        , ct.messoToken 
         FROM ccsowner.bsbBillingAccount ba
         JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
         JOIN rcrm.product p ON s.id = p.serviceId
         LEFT JOIN dataprov.customertokens ct ON ba.accountNumber = ct.accountNumber
         WHERE s.serviceType = 'AMP'
         AND p.suid = 'AMP_DISCOVERY_PLUS' 
         AND p.status = 'ACTIVE'
         AND exists (select /*+ parallel c(8) */null
                     from customers c
                     where c.partyid=ct.partyId
                     and c.skyqbox = 1)
    ORDER BY dbms_random.value
    fetch first 20000 rows only) ; 
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' rows inserted.' ) ;
   logger.write ( 'complete' ) ;
END q_discoveryplus ;


PROCEDURE digitaltvrecontracting_noburn IS
  l_pool VARCHAR2(29) := 'DIGITALTVRECONTRACTING_NOBURN' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.partyid , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.partyid , s.messotoken
     FROM (
   SELECT *
     FROM customers c
        where (c.partyid,c.messotoken) in 
        (select /*+ parallel(cu, 12) parallel(ba, 12) parallel(po, 12) parallel(ro, 12) */
                distinct cu.partyid,cu.messotoken
           from dataprov.customers cu, ccsowner.bsbbillingaccount ba,
                --ccsowner.bsbsubscriptionagreementitem sa,
                ccsowner.bsbportfoliooffer po, refdatamgr.bsboffer ro
          where ba.accountnumber = cu.accountnumber
            and cu.DTV =1   --check
            and cu.countryCode = 'GBR' --check
            -- AND ba.accountnumber = sa.agreementnumber
            and po.offerid = ro.ID
            AND ba.portfolioid = po.portfolioid
            AND po.OFFERID  IN ('85769','85770','85771','85772','85773','90005','85774','88910','88911','89224','89225','89471','89472','89477','89478','89479','89486')
            and po.status = 'ACT'
            AND po.applicationenddate > sysdate
            AND po.applicationenddate < (sysdate + 89) 
            AND cu.ULTIMATETVADDON = 0
            AND cu.ENTERTAINMENT=1
            AND cu.accountnumber in (SELECT da.accountNumber FROM debt_amount da WHERE da.balance <= 0)
         )
      AND ROWNUM <= 200000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END digitaltvrecontracting_noburn ;


PROCEDURE bbRegrade2024 IS
/*
04-Aug-2021 Andrew Fraser for Ross Benton added 'code' = first two digits of bank account, sql code taken from cbsservices.dal_paymentMethod.retrievePaymentMethods
25-Jun-2021 Andrew Fraser added i_burn parameter to be used by data_prep_01.bbregrade because that pool was burning up all the data for suffix 909.
*/
   l_pool VARCHAR2(29) := 'BBREGRADE2024' ;
   l_telout varchar2(32) ;
   l_magicno varchar2(3) := '909'; --901-- 660 supports broadband fibre magic number
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO bbRegrade2024 t ( t.accountNumber , t.partyId , t.data , t.skycesa01token , t.messotoken , t.ssotoken
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
    WHERE  ( c.bbsuperfast = 1 OR c.bbultrafast = 1 )
      --AND  c.fibre = 1
      --AND  c.bband = 1
      AND c.talk = 1
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND c.countryCode = 'GBR'
      --AND c.pool IS NULL  -- optional
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
        FROM bbRegrade2024 d
   )
   LOOP
      -- 25-Jun-2021 Andrew Fraser added i_burn parameter to be used by data_prep_01.bbregrade because that pool was burning up all the data for suffix 909.
      dynamic_data_pkg.update_cust_telno ( v_accountnumber => telinfo.accountNumber , v_suffix => l_magicno
          , v_telephone_out => l_telout , i_burn => FALSE
          ) ;
   END LOOP ;
   logger.write ( 'Update Customer PhoneNumber complete' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.data , t.skycesa01token , t.messotoken
      , t.ssotoken , t.firstName , t.familyName , t.emailAddress , t.code
      )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.data , s.skycesa01token , s.messotoken
        , s.ssotoken , s.firstName , s.familyName , s.emailAddress , s.code
     FROM (
           SELECT d.accountNumber , d.partyId , d.data , d.skycesa01token , d.messotoken
                , d.ssotoken , d.firstName , d.familyName , d.emailAddress , d.code
             FROM bbRegrade2024 d
            ORDER BY dbms_random.value
          ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE , i_flagCustomers => TRUE ) ;
   -- If truncating: soipDigitalExistLanding in 04 depends on bbRegrade table.
   logger.write ( 'complete' ) ;
END bbRegrade2024 ;

PROCEDURE incompletesales IS
-- 7th March 2024 Created for PERFENG-2813
   l_pool VARCHAR2(29) := 'INCOMPLETESALES' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.data, t.partyid , t.version)
    select ROWNUM AS pool_seqno, l_pool AS pool_name , reference,partyid,version
    from sps_owner.salesinteraction@iss
    where status='INCOMPLETE'
    and TYPE='EXISTING'
    and version >=4
    and created >= to_date('15-APR-24 00:00:00','DD-MON-YY HH24:MI:SS'); 
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => FALSE ) ;
   logger.write ( 'complete' ) ;
END incompletesales ;

PROCEDURE noNetflixNoBurn IS --NFTREL-22391 requested by Benton as a replica of NONETFLIX, without burn 
   l_pool VARCHAR2(29) := 'NONETFLIXNOBURN' ;
   cnt_rec number;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'alter session set ddl_lock_timeout=60'; 
   execute immediate 'truncate table ' || l_pool ;
   
   select count(*) into cnt_rec from dataprov.noNetflix;
   
   if cnt_rec > 0 then  --we piggyback on the pool noNetflix building the table so we can reuse it.
	
	   insert into dataprov.noNetflixNoBurn select * from dataprov.noNetflix;
	   commit;
	   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
	   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.skycesa01token , t.messotoken , t.data )
	   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyid , s.skycesa01token , s.messotoken , s.data
		 FROM noNetflixNoBurn s
		WHERE s.data IS NOT NULL
		  AND ROWNUM <= 15000
	   ;
	   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => FALSE , i_flagCustomers => TRUE ) ;
   -- Added by SM for debugging purposes
declare
v_message varchar2(3950);
v_source varchar2(50) := 'NONETFLIXNOBURN';
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
            and object_name = 'NONETFLIXNOBURN'); 
logger.write( v_message);           

exception
when no_data_found then 
 logger.write(v_source||' No locks found.');
end;
---
       
	   execute immediate 'truncate table ' || l_pool ;       
       execute immediate 'truncate table NONETFLIX';
	   logger.write ( 'complete' ) ;
	else
		logger.write ( 'Pool was not built because table NONETFLIX was found empty' ) ;
	end if;	
END noNetflixNoBurn ;

PROCEDURE creditcardverification IS
-- 17th April 2024 Created for PERFENG-3169
   l_pool VARCHAR2(29) := 'CREDITCARDVERIFICATION' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.surname , t.postcode, t.visitdate, t.creditcardnumber)
    select /*+ parallel(c, 8) parallel(ba, 8) parallel(paymt, 8) parallel(pmtrl, 8) parallel(adr, 8) parallel(a, 8) parallel(p, 8) parallel(cr, 8), parallel(pr, 8) */ 
           ROWNUM AS pool_seqno, l_pool AS pool_name , c.accountnumber, c.familyname, adr.postcode, p.birthdate, paymt.cardnumber
    from customers c, ccsowner.bsbBillingAccount ba, ccsowner.bsbPaymentMethod paymt, ccsowner.bsbPaymentMethodRole pmtrl,
         ccsowner.BSBBILLINGADDRESSROLE a, ccsowner.bsbaddress  adr, ccsowner.person p, ccsowner.bsbcustomerrole cr, ccsowner.bsbpartyrole pr
    where c.accountnumber = ba.accountnumber
    and c.accountnumber2  is null
    and pmtrl.billingAccountId = ba.id
    and paymt.id = pmtrl.paymentMethodId
    and paymt.paymentmethodclasstype='BSBPaymentCardMethod'
    and a.billingaccountid=ba.id
    and a.addressid=adr.id
    and SYSDATE BETWEEN pmtrl.effectiveFrom AND NVL ( pmtrl.effectiveTo , SYSDATE + 1000 )
    and SYSDATE BETWEEN a.effectiveFrom AND NVL ( a.effectiveTo , SYSDATE + 1000 )
    and (paymt.cardExpiryDate IS NULL 
         OR last_day(to_date(paymt.cardExpiryDate)) >= sysdate)
    and c.partyid = p.partyid
    and cr.partyroleid = pr.id
    and paymt.deletedflag      = 0                  --CIMDB-6205:Ignore Paymentmethod with deletedflag as true
    and pmtrl.deletedflag      = 0
    and ba.portfolioid = cr.portfolioid
    and c.nsprofileid is null
    and c.mobile=1;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => FALSE ) ;
   logger.write ( 'complete' ) ;
END creditcardverification ;

PROCEDURE actmobpiggybank IS
   -- https://cbsjira.bskyb.com/browse/PERFENG-3563
   l_pool VARCHAR2(29) := 'ACTMOBPIGGYBANK' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO actmobpiggybank t ( t.accountNumber , t.partyId , t.serviceInstanceId )
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
      AND act.accountNumber IN (select  account_no from igor.stg_igor_active_accs@IGR021N 
        where current_balance > 5368709120) --Only to return accounts where the piggybank remaining balance is over 5GB
   ;
   logger.write ( TO_CHAR ( SQL%ROWCOUNT ) || ' inserted' ) ;
   COMMIT ;
   -- 30-Nov-2021 1) Andrew Fraser for Rizwan Soomra only want accounts that have a mobile number as the primary contact number SOIPPOD-2589.
   -- 30-Nov-2021 3) Andrew Fraser for Rizwan Soomra also remove if international dialing code is not '+44' SOIPPOD-2589.
   -- 06/11/23 (RFA) - Added hint to SELECT to attempt to improve the performance 
   DELETE FROM actmobpiggybank d
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
   DELETE FROM actmobpiggybank d
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
   DELETE FROM actmobpiggybank d
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
END actmobpiggybank ;

END data_prep_11 ;