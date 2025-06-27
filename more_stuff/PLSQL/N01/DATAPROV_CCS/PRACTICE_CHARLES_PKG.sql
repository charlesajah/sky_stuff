CREATE OR REPLACE PACKAGE practice_charles_pkg AS
   Procedure dbaTestpool_Charles;
   Procedure customer_account_types;            --this returns all customers describing what servicetypes they have
   Procedure soip_customers_other_accounts;     --this returns the SoIP customers that have other accounts
   Procedure soip_only_active_glass_stream;    --This builds a pool for SoIP customers that have no other accounts except SkyGlass or Stream
END practice_charles_pkg;
/


CREATE OR REPLACE PACKAGE BODY practice_charles_pkg AS
PROCEDURE dbaTestpool_Charles IS
   l_pool VARCHAR2(29) := 'DBATESTPOOL_CHARLES' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO dbatestpool_charles t ( t.accountnumber , t.partyid , t.messoToken , t.loyaltytier  )
   SELECT /*+ parallel(c 8) */
       c.accountnumber
        , c.partyid
        , c.messoToken
		, c.loyaltytier
     FROM customers c
    WHERE c.dtv=1                                                                                          -- Digital TV Customers
      and c.bband=1                                                                                        -- Broadband Customer
      and c.mobile=0                                                                                       -- Not mobile Customers
      and c.loyaltytier='Gold'                                                                             -- Gold Loyalty Tier
      and c.port_cnt >=31                                                                                  -- At least 31 items in portfolio
   ;

   COMMIT ;

   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId , t.messoToken  )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken 
    FROM (
          SELECT d.accountnumber , d.partyid , d.messoToken 
            FROM dbatestpool_rakel d
           FETCH FIRST 10000 ROWS ONLY
         ) s
    ORDER BY dbms_random.value
   ;
   
   COMMIT;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT, i_burn => FALSE  ) ;
   logger.write ( 'complete' ) ;

END dbaTestpool_Charles ;

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
BEGIN
       logger.write ( 'begin' ) ;
       execute immediate 'truncate table ' || l_pool ;
       logger.write ( 'Populate customer_account_types table' ) ;
       customer_account_types;
       logger.write ( 'Completed population of customer_account_types table' ) ;
       logger.write ( 'Populate soip_customers_other_accounts' ) ;
       soip_customers_other_accounts;
       logger.write ( 'Completed population of soip_customers_other_accounts' ) ;
       INSERT /*+ append */ INTO SOIP_ONLY_ACTIVE_GLASS_STREAM 
       SELECT distinct d.accountNumber , d.partyId , d.messoToken
         FROM dataprov.soipActiveSubscription d
         JOIN ccsowner.bsbBillingAccount ba ON ba.accountNumber = d.accountNumber
         JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
         JOIN rcrm.product p ON s.id = p.serviceId
         JOIN ccsowner.bsbCustomerRole bcr ON ba.portfolioId = bcr.portfolioId
         JOIN ccsowner.bsbPartyRole bpr ON bcr.partyroleId = bpr.id
         JOIN ccsowner.person per ON per.partyId = bpr.partyId
        WHERE d.serviceType = 'SOIP'
          AND per.partyid NOT IN (select partyid from SOIP_CUSTOMERS_OTHER_ACCOUNTS )           --SOIP_CUSTOMERS_OTHER_ACCOUNTS table contains all SoIP customers that have other accounts
          AND p.suid IN ( 'LLAMA_SMALL' , 'LLAMA_MEDIUM' , 'LLAMA_LARGE' , 'MULTISCREEN_PUCK' )  -- Llama = Sky Glass , Puck = Sky Stream
          AND p.status = 'DELIVERED'
          AND p.eventCode = 'DELIVERED'
          --AND ba.currencyCode = 'GBP'  -- Charles, commenting this out includes RoI customers.
          AND NVL ( per.blockPurchaseSwitch , 0 ) != 1  -- Amit, exclude debt block. 1 = customer is in debt block.
          AND NVL ( bcr.debtIndicator , 0 ) != 1  -- Archana, exclude customer in debt. 1 = customer is in debt arrears.
          AND d.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- Charles exclude customers with debt/balance due on their account. 
    ;
    COMMIT ;
    
       sequence_pkg.seqBefore ( i_pool => l_pool ) ;
       INSERT INTO dprov_accounts_fast daf ( daf.pool_seqno , daf.pool_name , daf.accountNumber , daf.partyId, daf.messoToken )
       SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId , s.messoToken 
        FROM (
              SELECT ss.accountnumber , ss.partyid , ss.messoToken 
                FROM soip_only_active_glass_stream ss
                FETCH FIRST 10000 ROWS ONLY
             ) s
             ORDER BY dbms_random.value;
       
       COMMIT;
       sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT, i_burn => TRUE  ) ;
       logger.write ( 'complete' ) ;
END soip_only_active_glass_stream;
    
    
END practice_charles_pkg;
/
