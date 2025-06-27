CREATE OR REPLACE PACKAGE customers_pkg AS
    PROCEDURE act_uk_cust ;
    PROCEDURE act_cust_uk_subs ;
    PROCEDURE customers ;
    PROCEDURE customer_debt_new ;
    PROCEDURE customer_debt ;  -- 02-Nov-2021 Andrew Fraser called from customer_debt_new to workaround sequencing problems.
    PROCEDURE customersv2 ;
    PROCEDURE CustSupport;
END customers_pkg ;
/


CREATE OR REPLACE PACKAGE BODY customers_pkg AS

PROCEDURE act_uk_cust IS
   l_count  number ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'alter session set ddl_lock_timeout=60';
   execute immediate 'truncate table act_uk_cust' ;
   INSERT /*+ append */ INTO act_uk_cust
   SELECT DISTINCT
          tmp.accountNumber
        , tmp.portfolioId
        , tmp.partyId
        , tmp.postcode
        , tmp.contactorId
        , tmp.accountId
     FROM (
      SELECT /*+ leading (bba) full(bba) parallel(bba 8) full(bcr) parallel(bcr 8) full(bc) parallel(bc 8) full(ba)
                 parallel(ba 8) full(bpr) parallel(bpr 8) full(bca) parallel(bca 8) pq_distribute(bcr hash hash) use_hash_aggregation() */
             bba.accountNumber
           , bcr.portfolioId
           , bc.partyId
           , ba.postcode
           , bc.id AS contactorId
           , bba.id AS accountId
           , COUNT(*) OVER ( PARTITION BY bba.portfolioId ) AS chk
        FROM ccsowner.bsbCustomerRole bcr
        JOIN ccsowner.bsbBillingAccount bba ON bba.portfolioId = bcr.portfolioId
        JOIN ccsowner.bsbPartyRole bpr ON bcr.partyRoleId = bpr.id
        JOIN ccsowner.bsbContactor bc ON bpr.partyId = bc.partyId
        JOIN ccsowner.bsbContactAddress bca ON bc.id = bca.contactorId
        JOIN ccsowner.bsbAddress ba ON bca.addressId = ba.id
       WHERE bcr.customerStatusCode = 'CRACT'
         AND bba.accountStatusCode = '01'
         AND bca.primaryFlag = '1'
         AND bca.deletedFlag != '1'
         AND bca.effectiveToDate IS NULL
         AND ba.countryCode = 'GBR'
         AND bcr.unableToPurchaseProductsSwitch = 0  -- remove any customers that cannot switch products
         AND SUBSTR ( ba.postcode , 0 , 2 ) NOT IN ( 'JE' , 'IM' , 'GY' , 'BT' )
         AND NOT EXISTS (
             SELECT /*+ parallel(bacr 8) pq_distribute(bacr hash hash) */ NULL
               FROM ccsowner.bsbAuthorizedContactRole bacr
              WHERE bacr.portfolioId = bba.portfolioId
             )
        ) tmp
    WHERE tmp.chk <= 2
      AND NOT EXISTS (
          SELECT NULL
            FROM ccsowner.bsbAuthorizedContactRole bacr
           WHERE bacr.portfolioId = tmp.portfolioId
          )
      AND NOT EXISTS (
          SELECT NULL
            FROM ccsowner.bsbPortfolioProduct bsbp
           WHERE tmp.portfolioId = bsbp.portfolioId
             AND bsbp.catalogueProductId = '13611'  -- remove freesat customers
          )
   ;
   l_count := SQL%ROWCOUNT ;
   COMMIT ;
   logger.write ( TO_CHAR ( l_count ) || ' inserted' ) ;
   -- 28-Jan-2023 Andrew Fraser for Michael Santos, exclude deceased customers. SOIPPOD-2736.
   DELETE FROM act_uk_cust t
    WHERE t.partyId IN (
          SELECT con.partyId
            FROM ccsowner.bsbContactor con
           WHERE con.mortalityStatus IN ( '02' , '03' )  -- 02 Deceased Notified , 03 Deceased Confirmed.
          )
   ;
   l_count := SQL%ROWCOUNT ;
   COMMIT ;
   logger.write ( TO_CHAR ( l_count ) || ' deleted because deceased' ) ;
   logger.write ( 'complete' ) ;
END act_uk_cust ;

PROCEDURE act_cust_uk_subs IS
BEGIN
    logger.write ( 'begin' ) ;
    execute immediate 'alter session set ddl_lock_timeout=60';
    execute immediate 'truncate table act_cust_uk_subs reuse storage' ;
    INSERT /*+ append */ INTO act_cust_uk_subs t ( t.accountNumber , t.partyId , t.dtv , t.talk , t.bband , t.mobile )
    SELECT AccountNumber, PartyID, DTV, Talk, BroadBand, Mobile
      FROM ( SELECT * 
               FROM ( SELECT /*+ parallel(si 16) pq_distribute(si hash hash) 
                                 parallel(act 16) pq_distribute(act hash hash)
                              */
                             ACT.AccountNumber, ACT.PartyID, Su.Status, Su.SubscriptionTypeID
                           , ROW_NUMBER( ) OVER( PARTITION BY ACT.AccountNumber, SI.ServiceInstanceType -- Find the priority of status by the account and service instance within it.
                                                 ORDER BY CASE SSM.SalesStatus            -- Choose the status based on what's the subscription status.
                                                            WHEN 'ACTIVE'         THEN 1  -- If there's any active, then pick that.
                                                            WHEN 'RESTRICTED'     THEN 2  -- If there's any resricted, then that will be the next status to be picked.
                                                            WHEN 'PREACTIVE'      THEN 3  -- If there are pre-active, then that will be next
                                                            WHEN 'PENDING CANCEL' THEN 4  -- If there's any pending cancel, then consider that as the next priority.
                                                            WHEN 'CEASED'         THEN 5  -- Finaly if the only subscription left is ceased, then bring that status.
                                                          END ASC
                                               ) SelectStatusRecord
                        FROM DataProv.Act_UK_Cust        ACT
                           , CCSOwner.BSBBillingAccount  BA
                           , CCSOwner.BSBServiceInstance SI
                           , CCSOwner.BSBSubscription    Su
                           , TCD.SalesStatusMap          SSM
                       WHERE SSM.Status                 = Su.Status
                         AND SSM.ItemType               = 'SUBSCRIPTION'
                         AND SSM.SubscriptionTypeID     = Su.SubscriptionTypeID
                         AND Su.ServiceInstanceID       = SI.ID
                         AND Su.SubscriptionTYpeID IN ( '1', '3', '7', '10') -- Primary DTV, Sky Talk, Broadband and Sky Mobile Tariff.
                         AND SI.PortfolioID             = ACT.PortfolioID
                         AND SI.ServiceInstanceType IN ( 210, 100, 400, 620) -- Primary DTV, Talk, Broadband, Mobile Service. NO NOW TV or NOW Talk or NOW BB;
                         AND SI.ParentServiceInstanceID = BA.ServiceInstanceID
                         AND BA.AccountNumber           = ACT.AccountNumber
                    )
              WHERE SelectStatusRecord = 1
           )
     PIVOT ( MAX( Status) FOR ( SubscriptionTypeID ) IN ( '1' AS DTV , '3' AS Talk , '7' AS BroadBand , '10' AS Mobile))
     ;
    COMMIT ;
    logger.write ( 'complete' ) ;
END act_cust_uk_subs ;

PROCEDURE buildSupportingTables IS
BEGIN
   logger.write ( 'begin' ) ;
   -- used by bbregrade and digitalBBRegradeNoBurn pools builds
   EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML' ;
   execute immediate 'alter session set ddl_lock_timeout=60';
   EXECUTE IMMEDIATE 'truncate table bbRegrade_portfolioId_tmp' ;
   INSERT INTO bbRegrade_portfolioId_tmp
   SELECT /*+ parallel(pp 16) full(pp) */ DISTINCT pp.portfolioId
     FROM ccsowner.bsbPortfolioProduct pp
    WHERE pp.catalogueproductid IN ( '12721' , '13806' , '15195' )
      AND pp.status != 'CN'
   ;
   DBMS_STATS.GATHER_TABLE_STATS ( ownName => 'DATAPROV' , tabName => 'BBREGRADE_PORTFOLIOID_TMP' ) ;
   
   -- used by StandaloneBroadband pool builds  - RFA (13/06/24) - No data pools found using this table.
--   EXECUTE IMMEDIATE 'truncate table standaloneBroadband_tmp' ;
--   INSERT INTO standAloneBroadband_tmp
--   SELECT /*+ parallel(pp 16) full(pp) */ DISTINCT pp.portfolioId
--     FROM ccsowner.bsbPortfolioProduct pp
--    WHERE pp.catalogueproductid = '13640'
--   ;
--   DBMS_STATS.GATHER_TABLE_STATS ( ownName => 'DATAPROV' , tabName => 'STANDALONEBROADBAND_TMP' ) ;
   logger.write ( 'complete' ) ;
END buildSupportingTables ;

    PROCEDURE rebuild_Tokens IS
       l_count  number ;
    BEGIN
       logger.write ( 'Tokens - begin' ) ;
       execute immediate 'alter session set ddl_lock_timeout=60';
       EXECUTE IMMEDIATE 'TRUNCATE TABLE CUSTOMERTOKENS REUSE STORAGE' ;
        -- Disable any Indexes on this table
       logger.write ( 'Tokens - Disabling INDEXES' ) ;
       EXECUTE IMMEDIATE 'ALTER SESSION SET skip_unusable_indexes = TRUE' ;
       FOR r1 IN (
          SELECT i.index_name
            FROM all_indexes i
           WHERE i.owner = 'DATAPROV'
             AND i.table_name = 'CUSTOMERTOKENS'
             AND i.status = 'VALID'
           ORDER BY 1
       )
       LOOP
          EXECUTE IMMEDIATE 'ALTER INDEX dataprov.' || r1.index_name || ' UNUSABLE' ;
       END LOOP ;
       
       logger.write ( 'Tokens - Populate table' ) ;
       INSERT INTO /*+ append PARALLEL(CUSTOMERTOKENS, 16) */ 
            CUSTOMERTOKENS ( accountNumber, partyID, cesaToken, ssoToken, messoToken, emailAddress, firstName, familyName, userName, nsProfileId )
            with act as (
                SELECT  /*+ leading (bba) full(bba) parallel(bba 16) pq_distribute(bba hash hash) use_hash_aggregation()
                           full (bcr) parallel(bcr 16) pq_distribute(bcr hash hash) use_hash_aggregation()
                           full (bpr) parallel(bpr 16) 
                           full (per) parallel(per 16) 
                           full (pti) parallel(pti 16)
                        */ bba.accountNumber
                    , bpr.partyId
                    , MAX ( per.firstName ) AS firstName
                    , MAX ( per.familyName ) AS familyName
                    , MAX ( per.firstName ) || MAX ( per.familyName ) AS userName
                    , MIN ( pti.identityId ) AS nsProfileId
                  FROM ccsowner.bsbBillingAccount bba
                  JOIN ccsowner.bsbCustomerRole bcr ON bba.portfolioId = bcr.portfolioId
                  JOIN ccsowner.bsbPartyRole bpr ON bcr.partyroleId = bpr.id
                  JOIN ccsowner.person per ON per.partyId = bpr.partyId
                  LEFT OUTER JOIN ccsowner.bsbPartyToIdentity pti ON per.partyId = pti.partyId AND pti.identityType = 'NSPROFILEID'
                 WHERE bba.accountNumber is not null
                 GROUP BY bba.accountNumber, bpr.partyId  
                ), 
            mail as (
                SELECT /*+ leading (bc) full(bc) parallel(bc 16) pq_distribute(bc hash hash) use_hash_aggregation()
                           full(bce) parallel(bce 16) 
                           full(be)  parallel(be 16)  
                        */ bc.partyId
                     , MAX ( be.emailAddress ) AS emailAddress
                 FROM ccsowner.bsbContactor bc
                 JOIN ccsowner.bsbContactEmail bce ON bce.contactorId = bc.id
                 JOIN ccsowner.bsbEmail be ON be.id = bce.emailId
                WHERE bce.deletedFlag = 0
                  AND SYSDATE BETWEEN NVL ( bce.effectiveFromDate , SYSDATE - 1 ) AND NVL ( bce.effectiveToDate , SYSDATE + 1 )
                  AND be.emailAddressStatus = 'VALID'
                  AND bce.primaryFlag = 1
                GROUP BY bc.partyId
                ),
            env AS (
                select case 
                    when SYS_CONTEXT('userenv', 'con_name') = 'CHORDO' then 'N01' 
                    else 'N02' 
                    end as name
                from dual
                )
            SELECT  act.accountNumber
                   ,act.partyId
                   ,'T-CES-' || env.name || '-' || act.accountNumber || '-' || act.partyId || '-' || act.username || '-' || NVL ( act.nsProfileId , 'NO-NSPROFILE' ) as cesaToken
                   ,'T-SSO-' || env.name || '-' || act. accountNumber || '-' || act.partyId || '-' || act.username || '-' || NVL ( act.nsProfileId , 'NO-NSPROFILE' ) as ssoToken
                   ,'T-MES-' || env.name || '-' || act.accountNumber || '-' || act.partyId || '-' || act.username || '-' || NVL ( act.nsProfileId , 'NO-NSPROFILE' ) 
                             || '-' || NVL( mail.emailaddress, 'noemail_' || act.accountNumber || '@bskyb.com') as messoToken
                   , NVL( mail.emailaddress, 'noemail_' || act.accountNumber || '@bskyb.com') as emailAddress
                   , act.firstName
                   , act.familyName
                   , act.userName
                   , act.nsProfileId
             FROM act 
             JOIN env on ( 1=1 )
             LEFT JOIN mail ON act.partyId = mail.partyId
             ;
       l_count := SQL%ROWCOUNT ;
       COMMIT ;
       logger.write ( 'Tokens - Rows Inserted : '||to_char(l_count)||' rows created' ) ;      
             
       -- Enable Indexes for this table
       logger.write ( 'Tokens - Enabling INDEXES' ) ;
       FOR r1 IN (
          SELECT i.index_name
            FROM all_indexes i
           WHERE i.owner = 'DATAPROV'
             AND i.table_name = 'CUSTOMERTOKENS'
             AND i.status = 'UNUSABLE'
          ORDER BY 1
       )
       LOOP
          EXECUTE IMMEDIATE 'ALTER INDEX dataprov.' || r1.index_name || ' REBUILD NOLOGGING PARALLEL 8' ;
          EXECUTE IMMEDIATE 'ALTER INDEX dataprov.' || r1.index_name || ' NOPARALLEL' ;
       END LOOP ;
       logger.write ( 'Tokens - complete' ) ;
    END rebuild_Tokens ;

PROCEDURE rebuild_Products IS
   l_count number ; 
BEGIN
   logger.write ( 'begin Products' ) ;
   execute immediate 'alter session set ddl_lock_timeout=60';
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.products' ;
   INSERT /*+ append */ INTO dataprov.products ( id , productDescription )
    SELECT id , productDescription
      FROM refdatamgr.bsbCatalogueProduct
     WHERE LENGTH ( id ) = 5  -- long catalogueProductID's are DVDs and PPVs.
       AND (   productDescription LIKE 'Original%'
            OR productDescription LIKE 'Variety%'
            OR productDescription LIKE 'Box Sets%'
            OR productDescription LIKE 'Sky Q%Bundle%'
            OR productDescription LIKE '%Sports%'
            OR productDescription LIKE '%Cinema%'
            OR productDescription = 'Sky Kids'
            OR productDescription LIKE 'Sky Broadband%'
            OR productDescription LIKE '%Broadband%Superfast%'
            OR productDescription LIKE '%Broadband%Ultrafast%'
            OR productDescription LIKE 'Sky Talk%'
            OR productDescription IN ( 'Multi-size SIM card for Sky Mobile' )  -- mobile, AH added in extra 06/01/2020
            OR productDescription IN (select distinct productdescription 
                                        from refdatamgr.bsbcatalogueproduct 
                                       where servicetypecode in ('2002','2003') 
                                         and productdescription not like '%Roaming Charge%') -- Includes all mobile products      
            OR (productDescription LIKE 'Sky Fibre%' OR productDescription LIKE '%Full Fibre%' and productDescription NOT LIKE '%Activation Fee%')
            OR productDescription IN ( 'Sky Multiscreen' , 'Sky+ Monthly Subscription') -- RGI13 request for RS Reinstate
            OR productDescription IN ( 'Chelsea TV' , 'Liverpool TV' , 'MUTV' )  -- premium
            OR productDescription LIKE 'Sky Q % box'
            OR productDescription LIKE 'Sky Q % UHD'
            OR productDescription LIKE 'Sky+ HD Box%'
            OR productDescription LIKE 'Sky Entertainment%'
            OR productDescription LIKE 'Sky Soundbox'
            OR productDescription = 'Spotify'
            OR productDescription LIKE 'Netflix%'  /* 17-May-2021 AF % to take care of 'Netflix Basic' */
            OR ( productDescription LIKE 'BT Sport%' AND productDescription NOT LIKE '%Downgrade%' )
            OR productdescription = 'Ultimate On Demand'
            OR productDescription LIKE '%Sky Signature%'
            OR productdescription LIKE '%Ultimate%TV%Add%on'
            OR productdescription LIKE 'Disney+%'
            OR productDescription = 'Service Call Visit'  -- cbh06  22/07/20
            OR productDescription = 'Viewing Card' --cbh06 16/10/20
            OR productdescription LIKE 'Discovery+%'
            OR productdescription LIKE 'eSIM%'  -- RFA 18/09/23 Added to include eSIM products. Will feed into "mobEsim" column in CUSTOMERS table
           )
   ;
   l_count := sql%rowcount ;
   COMMIT ;
   logger.write ( 'complete Products : '||l_count  ) ;
END rebuild_Products ;


PROCEDURE rebuild_Customers IS
   l_count NUMBER ;
   l_mobile_products    VARCHAR2(4000) ;
   l_query VARCHAR2(32000) ;         
BEGIN
   logger.write ( 'begin Customers' ) ;
   execute immediate 'alter session set ddl_lock_timeout=60';
   EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE cust_orig' ;
   logger.write ( 'CUSTOMERS - Insert including exclusions' ) ;
   INSERT /*+ append */ INTO cust_orig t (
          t.partyId , t.accountNumber , t.accountNumber2 , t.loyalty , t.firstName , t.middleName , t.familyName , t.limaMigration , t.userName
        , t.portfolioId , t.onlineProfileId , t.nsProfileId , t.countryCode , t.billingAccountId , t.creationDt , t.inFlightVisit , t.inFlightOrders
          )
       SELECT /*+ parallel(bba 16) pq_distribute(bba hash hash)
                  parallel(bcr 16) pq_distribute(bcr hash hash)
                  parallel(bpr 16) pq_distribute(bpr hash hash)
                  parallel(per 16) pq_distribute(per hash hash)
                  parallel(pti 16) pq_distribute(pti hash hash)
                  parallel(lm 16) pq_distribute(lm hash hash)
              */
              bpr.partyId
            , MAX ( bba.accountNumber ) KEEP ( DENSE_RANK FIRST ORDER BY bba.created ASC ) AS accountNumber  -- earliest accountNumber
            , CASE WHEN MAX ( bba.accountNumber ) KEEP ( DENSE_RANK FIRST ORDER BY bba.created DESC ) != MAX ( bba.accountNumber ) KEEP ( DENSE_RANK FIRST ORDER BY bba.created ASC )
                   THEN MAX ( bba.accountNumber ) KEEP ( DENSE_RANK FIRST ORDER BY bba.created DESC )  -- latest accountNumber
                   ELSE NULL END AS accountNumber2
            , MAX ( per.activeInLoyaltyProgram ) AS loyalty  -- 0=opted OUT, 1=opted in, NULL=has not made any decision on loyalty yet.
            , MAX ( per.firstName ) AS firstName
            , MAX ( SUBSTR ( per.middleName , 1 , 2 ) ) AS middleName
            , MAX ( per.familyName ) AS familyName
            , MAX ( CASE WHEN lm.mig_result = 'COMPLETED' THEN 1
                         WHEN lm.mig_result NOT IN ( 'COMPLETED' , 'isBundleMigratable-CURRENT BUNDLE NOT MIGRATABLE not in config' ) THEN 2
                         WHEN lm.mig_result = 'isBundleMigratable-CURRENT BUNDLE NOT MIGRATABLE not in config' THEN 3  -- 16-Jul-2018, not sure if that value is good or bad or in-between, so flagging as a special 3 type.
                         ELSE 0 END ) AS limaMigration
            , MAX ( per.firstName ) || MAX ( per.familyName ) AS userName
            , MAX ( bba.portfolioId ) AS portfolioId
            , MAX ( per.onlineProfileId ) AS onlineProfileId
            , MIN ( pti.identityId ) AS nsProfileId
            , MAX ( ba.countrycode ) AS countryCode
            , MAX ( bba.id ) KEEP ( DENSE_RANK FIRST ORDER BY bba.created ASC ) AS billingAccountId  -- earliest billingAccountId
            , MIN ( bba.created ) AS creationDt
            , 0 AS inFlightVisit
            , 0 AS inFlightOrders
         FROM ccsowner.bsbBillingAccount bba
         JOIN ccsowner.bsbCustomerRole bcr ON bba.portfolioId = bcr.portfolioId
         JOIN ccsowner.bsbPartyRole bpr ON bcr.partyroleId = bpr.id
         JOIN ccsowner.person per ON per.partyId = bpr.partyId
         JOIN ccsowner.bsbContactor con ON con.PartyId = bpr.partyId  
         JOIN ccsowner.bsbContactAddress bca ON bca.contactorId = con.Id
         JOIN ccsowner.bsbAddress ba ON ba.Id = bca.addressId
         LEFT OUTER JOIN ccsowner.bsbPartyToIdentity pti ON per.partyId = pti.partyId AND pti.identityType = 'NSPROFILEID'
         LEFT OUTER JOIN pos.lima_migration lm ON lm.accountNumber = bba.accountNumber 
        WHERE bba.organisation_unit = 10  -- 20 for NowTV
          AND NVL ( bcr.debtIndicator , 0 ) != 1  -- 1 = customer is in debt arrears.
          AND NVL ( per.blockPurchaseSwitch , 0 ) != 1  -- 1 = customer is in debt block.
          AND bba.accountNumber IS NOT NULL  -- datafix issue, some rogue customers with no account number.
          AND NVL ( bcr.unableToPurchaseProductsSwitch , 0 ) != 1  -- 1 = customer cannot purchase products.
          AND bcr.customerStatusCode = 'CRACT'  -- Also CRIS CRIC. Some soip accounts are CRIS, so are excluded by this.
          AND bba.accountStatusCode = '01'  -- soip is null, so excluded by this.
          AND bba.customerSubTypeCode NOT IN ( 'TE' , 'ST' ) -- TE=test ST=staff. soip is null, so excluded by this.
          --AND NVL ( bba.accountStatusCode , '01' ) = '01'  -- 15-Dec-2021 NVL added to cope with soip account
          AND con.mortalityStatus NOT IN ( '02' , '03' )  -- 02 Deceased Notified , 03 Deceased Confirmed.  -- 02/10/23 RFA - exclusion added
        GROUP BY bpr.partyId
       ;
   l_count := SQL%ROWCOUNT ;
   COMMIT ;
   logger.write ( 'CUSTOMERS insert completed : '||TO_CHAR ( l_count ) || ' rows inserted' ) ;
 
   -- Select all mobile products to be evaluated
   select listagg ( ''''||productdescription||'''' ,',') within group (order by productdescription) productdescription
     into l_mobile_products
     from refdatamgr.bsbcatalogueproduct 
    where productDescription = 'Multi-size SIM card for Sky Mobile' 
       or (servicetypecode in ('2002','2003') 
      and productdescription not like '%Roaming Charge%');
   
   logger.write ( 'Portfolio merge starting' ) ;
   -- Moved to dynamic SQL to feed a fix list of mobile products into the query
   l_query := q'#
   MERGE /*+ parallel(16) */ INTO cust_orig t USING (
   SELECT bpp.portfolioId
        , MAX ( CASE WHEN p.productDescription LIKE 'Sky Talk%' AND bpp.status = 'A' THEN 1
                     WHEN p.productDescription LIKE 'Sky Talk%' AND bpp.status != 'A' THEN 2
                     ELSE 0 END ) AS talk
        , MAX ( CASE WHEN ( p.productDescription LIKE 'Sky Fibre%' OR p.productDescription LIKE 'Sky Broadband%' OR p.productDescription LIKE '%Broadband%Superfast%' OR p.productDescription LIKE '%Broadband%Ultrafast%' OR p.productDescription LIKE 'Sky Full Fibre%' OR p.productDescription LIKE 'NOW Full Fibre%') AND bpp.status = 'AC' THEN 1
                     WHEN ( p.productDescription LIKE 'Sky Fibre%' OR p.productDescription LIKE 'Sky Broadband%' OR p.productDescription LIKE '%Broadband%Superfast%' OR p.productDescription LIKE '%Broadband%Ultrafast%' OR p.productDescription LIKE 'Sky Full Fibre%' OR p.productDescription LIKE 'NOW Full Fibre%') AND bpp.status != 'AC' THEN 2
                     ELSE 0 END ) AS bband
        , MAX ( CASE WHEN ( p.productDescription = 'Sky Broadband 12GB' ) AND bpp.status = 'AC' THEN 1
                     WHEN ( p.productDescription = 'Sky Broadband 12GB' ) AND bpp.status != 'AC' THEN 2
                    ELSE 0 END ) AS bb12gb
        , MAX ( CASE WHEN ( p.productDescription LIKE 'Sky Broadband Lite%' ) AND bpp.status = 'AC' THEN 1
                     WHEN ( p.productDescription LIKE 'Sky Broadband Lite%' ) AND bpp.status != 'AC' THEN 2
                     ELSE 0 END ) AS bbandLite
        , MAX ( CASE WHEN p.productDescription IN (#' || l_mobile_products || q'#) AND bpp.status != 'CRQ' THEN 1
                     WHEN p.productDescription IN (#' || l_mobile_products || q'#) AND bpp.status = 'CRQ' THEN 2  -- Cease Requested
                     ELSE 0 END ) AS mobile
        , MAX ( CASE WHEN p.productDescription LIKE '%Sports%' AND bpp.status = 'EN' THEN 1
                     WHEN p.productDescription LIKE '%Sports%' AND bpp.status != 'EN' THEN 2
                     ELSE 0 END ) AS sports
        , MAX ( CASE WHEN p.productDescription LIKE '%Cinema%' AND bpp.status = 'EN' THEN 1
                     WHEN p.productDescription LIKE '%Cinema%' AND bpp.status != 'EN' THEN 2
                     ELSE 0 END ) AS cinema
        , MAX ( CASE WHEN p.productDescription = 'Sky Kids' AND bpp.status = 'EN' THEN 1
                     WHEN p.productDescription = 'Sky Kids' AND bpp.status != 'EN' THEN 2
                     ELSE 0 END ) AS kids
        , MAX ( CASE WHEN p.productDescription IN ( 'Chelsea TV' , 'Liverpool TV' , 'MUTV' ) AND bpp.status = 'EN' THEN 1
                     WHEN p.productDescription IN ( 'Chelsea TV' , 'Liverpool TV' , 'MUTV' ) AND bpp.status != 'EN' THEN 2
                     ELSE 0 END ) AS premium
        , MAX ( CASE WHEN p.productDescription LIKE 'Original%' AND bpp.status = 'EN' THEN 1
                     WHEN p.productDescription LIKE 'Original%' AND bpp.status != 'EN' THEN 2
                     ELSE 0 END ) AS original
        , MAX ( CASE WHEN p.productDescription LIKE 'Variety%' AND bpp.status = 'EN' THEN 1
                     WHEN p.productDescription LIKE 'Variety%' AND bpp.status != 'EN' THEN 2
                     ELSE 0 END ) AS variety
        , MAX ( CASE WHEN p.productDescription LIKE 'Box Sets%' AND bpp.status = 'EN' THEN 1
                     WHEN p.productDescription LIKE 'Box Sets%' AND bpp.status != 'EN' THEN 2
                     ELSE 0 END ) AS boxSets
        , MAX ( CASE WHEN p.productDescription LIKE 'Sky Q%Bundle%' AND bpp.status = 'EN' THEN 1
                     WHEN p.productDescription LIKE 'Sky Q%Bundle%' AND bpp.status != 'EN' THEN 2
                     ELSE 0 END ) AS skyQBundle
        , MAX ( CASE WHEN p.productDescription = 'Sky Sports Cricket' AND bpp.status = 'EN' THEN 1
                     WHEN p.productDescription = 'Sky Sports Cricket' AND bpp.status != 'EN' THEN 2
                     ELSE 0 END ) AS cricket
        , MAX ( CASE WHEN p.productDescription = 'Sky Sports Premier League' AND bpp.status = 'EN' THEN 1
                     WHEN p.productDescription = 'Sky Sports Premier League' AND bpp.status != 'EN' THEN 2
                     ELSE 0 END ) AS premierLeague
        , MAX ( CASE WHEN p.productDescription = 'Sky Sports Football' AND bpp.status = 'EN' THEN 1
                     WHEN p.productDescription = 'Sky Sports Football' AND bpp.status != 'EN' THEN 2
                     ELSE 0 END ) AS football
        , MAX ( CASE WHEN p.productDescription = 'Sky Sports - Complete Pack' AND bpp.status = 'EN' THEN 1
                     WHEN p.productDescription = 'Sky Sports - Complete Pack' AND bpp.status != 'EN' THEN 2
                     ELSE 0 END ) AS completeSports
        , MAX ( CASE WHEN ( p.productDescription LIKE 'Sky Q % box' OR p.productDescription LIKE 'Sky Q % UHD' ) AND bpp.status = 'IN' THEN 1
                     WHEN ( p.productDescription LIKE 'Sky Q % box' OR p.productDescription LIKE 'Sky Q % UHD' ) AND bpp.status != 'IN' THEN 2
                     ELSE 0 END ) AS skyQBox
        , MAX ( CASE WHEN p.productDescription LIKE 'Sky+ HD Box%' AND bpp.status = 'IN' THEN 1
                     WHEN p.productDescription LIKE 'Sky+ HD Box%' AND bpp.status != 'IN' THEN 2
                     ELSE 0 END ) AS skyHDBox
        , MAX ( CASE WHEN p.productDescription LIKE 'Sky Entertainment%' AND bpp.status = 'EN' THEN 1
                     WHEN p.productDescription LIKE 'Sky Entertainment%' AND bpp.status != 'EN' THEN 2
                     ELSE 0 END ) AS entertainment
        , MAX ( CASE WHEN p.productDescription LIKE 'Sky Soundbox' AND bpp.status = 'DL' THEN 1
                     WHEN p.productDescription LIKE 'Sky Soundbox' AND bpp.status != 'DL' THEN 2
                     ELSE 0 END ) AS soundbox
        , MAX ( CASE WHEN p.productDescription = 'Spotify' AND bpp.status IN ( 'AC' , 'EN' ) THEN 1
                     WHEN p.productDescription = 'Spotify' AND bpp.status NOT IN ( 'AC' , 'EN' ) THEN 2
                     ELSE 0 END ) AS spotify
        , MAX ( CASE WHEN p.productDescription LIKE 'Netflix%' AND bpp.status IN ( 'AC' , 'EN' ) THEN 1
                     WHEN p.productDescription LIKE 'Netflix%' AND bpp.status NOT IN ( 'AC' , 'EN' ) THEN 2
                     ELSE 0 END ) AS netflix
        , MAX ( CASE WHEN p.productDescription = 'Ultimate On Demand' AND bpp.status IN ( 'AC' , 'EN' ) THEN 1
                     WHEN p.productDescription = 'Ultimate On Demand' AND bpp.status NOT IN ( 'AC' , 'EN' ) THEN 2
                     ELSE 0 END ) AS ultimateOnDemand
        , MAX ( CASE WHEN ( p.productDescription LIKE 'Sky Fibre%' OR p.productDescription LIKE '%Broadband%Ultrafast%' OR p.productDescription LIKE '%Broadband%Superfast%' OR p.productDescription LIKE 'Sky Full Fibre%' OR p.productDescription LIKE 'NOW Full Fibre%') AND bpp.status = 'AC' THEN 1
                     WHEN ( p.productDescription LIKE 'Sky Fibre%' OR p.productDescription LIKE '%Broadband%Ultrafast%' OR p.productDescription LIKE '%Broadband%Superfast%' OR p.productDescription LIKE 'Sky Full Fibre%' OR p.productDescription LIKE 'NOW Full Fibre%') AND bpp.status != 'AC' THEN 2
                     ELSE 0 END ) AS fibre
        , MAX ( CASE WHEN p.productDescription LIKE 'TNT Sport%' AND bpp.status IN ( 'AC' , 'EN' ) THEN 1
                     WHEN p.productDescription LIKE 'TNT Sport%' AND bpp.status NOT IN ( 'AC' , 'EN' ) THEN 2
                     ELSE 0 END ) AS tntSports
        , MAX ( CASE WHEN ( p.productDescription LIKE '%Broadband%Ultrafast%' ) AND bpp.status = 'AC' THEN 1
                     WHEN ( p.productDescription LIKE '%Broadband%Ultrafast%' ) AND bpp.status != 'AC' THEN 2
                     ELSE 0 END ) AS bbUltraFast
        , MAX ( CASE WHEN ( p.productDescription LIKE '%Broadband%Superfast%' ) AND bpp.status = 'AC' THEN 1
                     WHEN ( p.productDescription LIKE '%Broadband%Superfast%' ) AND bpp.status != 'AC' THEN 2
                     ELSE 0 END ) AS bbSuperFast
        , MAX ( CASE WHEN ( p.productDescription LIKE '%Sky Signature%' ) AND bpp.status IN ( 'AC' , 'EN' ) THEN 1
                     WHEN ( p.productDescription LIKE '%Sky Signature%' ) AND bpp.status NOT IN ( 'AC' , 'EN' ) THEN 2
                     ELSE 0 END ) AS skySignature
        , MAX ( CASE WHEN ( p.productDescription LIKE '%Ultimate TV Add on%' ) AND bpp.status IN ( 'AC' , 'EN' ) THEN 1
                     WHEN ( p.productDescription LIKE '%Ultimate TV Add on%' ) AND bpp.status NOT IN ( 'AC' , 'EN' ) THEN 2
                     ELSE 0 END ) AS ultimateTvAddon
        , MAX ( CASE WHEN p.productDescription LIKE 'Disney+%' AND bpp.status IN ( 'AC' , 'EN' ) THEN 1
                     WHEN p.productDescription LIKE 'Disney+%' AND bpp.status NOT IN ( 'AC' , 'EN' ) THEN 2
                     ELSE 0 END ) AS disneyPlus
        , MAX ( CASE WHEN p.productDescription LIKE 'Service Call Visit%' AND bpp.status = 'AV' THEN 1
                     WHEN p.productDescription LIKE 'Service Call Visit%' AND bpp.status != 'CP' THEN 2  -- CP=Complete
                     ELSE 0 END ) AS visitInProgress
        , MAX ( CASE WHEN p.productDescription = 'Viewing Card' AND bpp.status = 'A' THEN 1
                     WHEN p.productDescription = 'Viewing Card' AND bpp.status NOT IN ( 'DL' ,'REN' ) THEN 2  -- REN="Replaced Enabled" DL=Disabled
                     ELSE 0 END ) AS activeViewingCard
        , MAX ( CASE WHEN p.productDescription LIKE 'Discovery+%' AND bpp.status IN ( 'AC' , 'EN' ) THEN 1
                     WHEN p.productDescription LIKE 'Discovery+%' AND bpp.status NOT IN ( 'AC' , 'EN' ) THEN 2
                     ELSE 0 END ) AS discoveryPlus
        , MAX ( CASE WHEN p.productDescription LIKE '%eSIM%' AND bpp.status IN ( 'DS' ) THEN 1 
                     WHEN p.productDescription LIKE '%eSIM%' AND bpp.status NOT IN ( 'DS' ) THEN 2
                     ELSE 0 END ) AS mobEsim        -- RFA 18/09/23 - added new column to flag customers with eSIM contracts
        /*
        Below for SPS message: "PIMM rule id rule_do_not_allow_soip_sale_on_inflight_orders has resulted in a Do Not Allow outcome."
        PC="Pending Cancel" definitely is a problem.
        KQ=Kqueue and RQ=Reqeusted were added 03-Sep-2022 for Michael Santos getting 412s at submit_order.
        Based on this from Scott Thompson:
        "If an interaction contains a portfolio where a TV subscription product (should be most/all products that have a type of DTV_VIEWING) has a salesStatus of PREACTIVE they are going to get blocked.
        From a pure database perspective, you are looking at any portfolio product with a customer product element type of SC that is in a status of KQ or RQ. That should certainly get most of them if not all of them."
        */
        , MAX ( CASE WHEN bpp.status IN ( 'PC' , 'RQ' , 'KQ' ) THEN 1 ELSE 0 END ) AS inFlightOrders
     FROM ccsowner.bsbPortfolioProduct bpp
     LEFT OUTER JOIN dataprov.Products p ON bpp.catalogueProductId = p.id
    WHERE bpp.status NOT IN ( 'CN' , 'FBI' , 'SC' , 'RP' )  -- CN=cancelled FBI=Cancelled Talk SC=System Cancelled RP=Replaced: for all these is as if the customer never had those products, so counts as a 0=false.
      --AND LENGTH ( bpp.catalogueProductId ) = 5 /* the table PRODUCTS already discriminates what products are valid */
    GROUP BY bpp.portfolioId
   ) s ON ( s.portfolioId = t.portfolioId )
   WHEN MATCHED THEN UPDATE SET
          t.talk = s.talk
        , t.bband = s.bband
        , t.bb12gb = s.bb12gb
        , t.bBandLite = s.bBandLite
        , t.mobile = (CASE WHEN s.mobesim = 1 THEN 1 ELSE s.mobile END) -- 09/10/23 (RFA) - All eSIM customers are Mobile customers
        , t.sports = s.sports
        , t.cinema = s.cinema
        , t.kids = s.kids
        , t.premium = s.premium
        , t.original = s.original
        , t.variety = s.variety
        , t.boxSets = s.boxSets
        , t.skyQbundle = s.skyQbundle
        , t.cricket = s.cricket
        , t.premierLeague = s.premierLeague
        , t.football = s.football
        , t.completeSports = s.completeSports
        , t.skyQBox = s.skyQBox
        , t.skyHdBox = s.skyHdBox
        , t.entertainment = s.entertainment
        , t.soundBox = s.soundBox
        , t.spotify = s.spotify
        , t.netflix = s.netflix
        , t.ultimateOnDemand = s.ultimateOnDemand
        , t.fibre = s.fibre
        , t.tntSports = s.tntSports
        , t.bbUltraFast = s.bbUltraFast
        , t.bbSuperFast = s.bbSuperFast
        , t.skySignature = s.skySignature
        , t.ultimateTvAddOn = s.ultimateTvAddOn
        , t.disneyPlus = s.disneyPlus
        , t.visitInProgress = s.visitInProgress
        , t.activeViewingCard = s.activeViewingCard
        , t.discoveryPlus = s.discoveryPlus
        , t.mobEsim = s.mobEsim
        , t.inFlightOrders = s.inFlightOrders
   #' ;   
   execute immediate l_query ;   
   l_count := sql%rowcount;
   COMMIT ;
   logger.write ( 'Portfolio merge completed : '|| to_char(l_count)) ;

   -- Ringfenced customers for Q Harmonization
   -- AH for Alex Benatatos on 12/06
   logger.write ( 'Q Harmonisation Ringfence Accounts') ;
   update cust_orig set pool = 'Q_HARM_RINGFENCE' where accountnumber in (select accountnumber from Q_HARM_RINGFENCE);
   commit;
      
   logger.write ( 'customers self merge starting' ) ;
   UPDATE /*+ parallel(16) */ cust_orig c
      SET c.dtv = CASE
             WHEN c.sports = 1 OR c.cinema = 1 OR c.original = 1 OR c.variety = 1 OR c.boxsets = 1 OR c.skyQBundle = 1 OR c.entertainment = 1 OR c.skySignature = 1 THEN 1
             ELSE GREATEST ( c.sports , c.cinema , c.original , c.variety , c.boxsets , c.skyQBundle , c.entertainment , c.skySignature )
             END
        , c.loyaltytier =  CASE
                            WHEN c.creationdt BETWEEN ADD_MONTHS(trunc(SYSDATE), -(8*12))  AND ADD_MONTHS(trunc(SYSDATE), -(3*12)) THEN 'Gold' 
                            WHEN c.creationdt BETWEEN ADD_MONTHS(trunc(SYSDATE), -(15*12)) AND ADD_MONTHS(trunc(SYSDATE), -(8*12)) THEN 'Platinum' 
                            WHEN c.creationdt       < ADD_MONTHS(trunc(SYSDATE), -(15*12))                                         THEN 'Diamond' 
                            ELSE 'Silver'  
                          END 
   ;
   l_count := sql%rowcount ;
   COMMIT ;
   logger.write ( 'customers self merge completed : '|| to_char(l_count)) ;
   
   logger.write ( 'customers tokens merge starting' ) ;
   UPDATE /*+ PARALLEL(8) */ cust_orig c 
        SET ( c.skycesa01Token, c.ssoToken, c.messoToken, c.emailAddress ) =
            ( SELECT t.cesaToken, t.ssoToken, t.messoToken, t.emailAddress
                FROM CustomerTokens t
               WHERE c.accountNumber = t.accountNumber
                 AND c.partyID = t.partyID );
   
   l_count := sql%rowcount ;
   COMMIT ;
   logger.write ( 'customers tokens merge completed : '|| to_char(l_count)) ;
   
   logger.write ( 'inFlightVisit starting' ) ;
   -- Below for SPS message: "PIMM rule id rule_do_not_allow_post_active_cancel_with_in_flight_visit has resulted in a Do Not Allow outcome."
   -- Complete/Cancelled are ok. UB and BK are definitely a problem, but maybe could add filters for visit_date.
   -- List is: select code,codeDesc from refdatamgr.picklist where codeGroup = 'VisitRequirementStatus' order by 1 ;
   dbms_stats.gather_table_stats ( ownName => USER , tabName => 'cust_orig' ) ;
   logger.write ( 'customers partofolio merge started' ) ;
   MERGE /*+ parallel(16) */ INTO cust_orig t USING (
      SELECT DISTINCT bsi.portfolioId
        FROM ccsowner.bsbVisitRequirement bvr
        JOIN ccsowner.bsbAddressUsageRole baur ON bvr.installationAddressRoleId = baur.id
        JOIN ccsowner.bsbServiceInstance bsi ON baur.serviceInstanceId = bsi.id
       WHERE bvr.statusCode NOT IN ( 'CP' , 'CN' )  -- CP=Complete CN=Cancelled
   ) s ON ( s.portfolioId = t.portfolioId )
   WHEN MATCHED THEN UPDATE SET t.inFlightVisit = 1
   ;
   l_count := sql%rowcount ;
   COMMIT ;
   logger.write ( 'customers portfolio merge completed : '|| to_char(l_count)) ;
   logger.write ( 'Product count starting' ) ;
   MERGE INTO cust_orig t USING (
      SELECT /*+ parallel(16) */ portfolioId , COUNT(*) AS port_cnt
        FROM ccsowner.bsbportFolioProduct
      GROUP BY portfolioId) s ON ( s.portfolioid = t.portfolioid )
   WHEN MATCHED THEN UPDATE SET t.port_cnt = s.port_cnt
   ;
   l_count := sql%rowcount ;
   COMMIT ;
   logger.write ( 'Product count completed : '|| to_char(l_count)) ;

   logger.write ( 'mobile device count starting' ) ;
   MERGE INTO cust_orig t USING (
      SELECT /*+ parallel(16) */ bfp.portfolioid, count(*) mob_devices
	  FROM ccsowner.bsbportFolioProduct bfp, ccsowner.bsbcustomerproductelement cpe
	 where cpe.portfolioproductid = bfp.id
	   and cpe.customerproductelementtype = 'MD'
	group by bfp.portfolioid) s ON ( s.portfolioid = t.portfolioid )
   WHEN MATCHED THEN UPDATE SET t.MOB_DEVICES = s.MOB_DEVICES
   ;
   l_count := sql%rowcount ;
   COMMIT ;
   logger.write ( 'mobile device count completed : '|| to_char(l_count)) ;
   
   logger.write ( 'CUSTOMERS Copy starting' ) ;
   
   -- Added by SM for debugging purposes
   declare
v_message varchar2(3950);
v_source varchar2(50) := 'CUSTOMERS';
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
            and object_name = 'CUSTOMERS'); 
logger.write( v_message);           

exception
when no_data_found then 
 logger.write(v_source||' No locks found.');
end;
---
   EXECUTE IMMEDIATE 'TRUNCATE TABLE customers' ;
   EXECUTE IMMEDIATE 'ALTER SESSION SET skip_unusable_indexes = TRUE' ;
   FOR r1 IN (
      SELECT i.index_name
        FROM all_indexes i
       WHERE i.owner = 'DATAPROV'
         AND i.table_name = 'CUSTOMERS'
         AND i.status = 'VALID'
       ORDER BY 1
   )
   LOOP
      EXECUTE IMMEDIATE 'ALTER INDEX dataprov.' || r1.index_name || ' UNUSABLE' ;
   END LOOP ;
   INSERT /*+ append parallel(8) */ INTO customers t
   SELECT s.*
     FROM (
           SELECT co.*
             FROM cust_orig co
            ORDER BY dbms_random.value
          ) s
   ;
   COMMIT ;

   EXECUTE IMMEDIATE 'truncate table cust_orig' ;
   FOR r1 IN (
      SELECT i.index_name
        FROM all_indexes i
       WHERE i.owner = 'DATAPROV'
         AND i.table_name = 'CUSTOMERS'
         AND i.status = 'UNUSABLE'
      ORDER BY 1
   )
   LOOP
      EXECUTE IMMEDIATE 'ALTER INDEX dataprov.' || r1.index_name || ' REBUILD NOLOGGING PARALLEL 8' ;
      EXECUTE IMMEDIATE 'ALTER INDEX dataprov.' || r1.index_name || ' NOPARALLEL' ;
   END LOOP ;
   
   logger.write ( 'CUSTOMERS complete' ) ;
END rebuild_Customers ;


    PROCEDURE rebuild_CustomersV2 IS
       l_count NUMBER ;
       l_mobile_products    VARCHAR2(4000) ;
       l_query VARCHAR2(32000) ;       
    BEGIN
       logger.write ( 'CustomersV2 - begin' ) ;
       execute immediate 'alter session set ddl_lock_timeout=60';
       EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML' ;

      -- truncate support table
       EXECUTE IMMEDIATE 'TRUNCATE TABLE cust_origv2' ;

       logger.write ( 'CustomersV2 - Insert including exclusions' ) ;
       INSERT /*+ append */ INTO cust_origv2 t (
          t.partyId , t.accountNumber , t.accounttype , t.loyalty , t.firstName , t.middleName , t.familyName , t.limaMigration , t.userName
        , t.portfolioId , t.onlineProfileId , t.nsProfileId , t.countryCode , t.billingAccountId , t.creationDt , t.inFlightVisit , t.inFlightOrders
              )
            with actype as (
                SELECT    /*  parallel(P 16) pq_distribute(P hash hash)
                              parallel(PR 16) pq_distribute(PR hash hash)
                              parallel(CR 16) pq_distribute(CR hash hash)
                              parallel(BA 16) pq_distribute(BA hash hash)
                              parallel(SIP 16) pq_distribute(SIP hash hash)
                              parallel(SIC 16) pq_distribute(SIC hash hash)
                              parallel(BUR 16) pq_distribute(BUR hash hash)
                              parallel(PI 16) pq_distribute(PI hash hash)
                              parallel(SRV 16) pq_distribute(SRV hash hash)
                           */
                   PR.PartyID       AS PartyID
                 , PI.IdentityID    AS NSProfileID
                 , BA.AccountNumber AS AccountNumber
                 , BA.ID            AS AccountID
                 , BAS.countrycode  AS CountryCode
                 , CASE
                     WHEN BA.Organisation_Unit    = 20                                 THEN 'NOWTV'
                     WHEN SIP.ServiceInstanceType IS NULL                                     -- SKY_STORE accounts do not have a billing account serviceinstance							 
                                        AND P.OnlineProfileID IS NOT NULL                                       -- Sky Store will always have an online Profile ID
                                        AND SIC.ServiceInstanceType IS NULL            THEN 'SKY_STORE'  -- and no child SI.
                     WHEN (sip.serviceinstancetype = 300 AND srv.servicetype = 'SOIP') THEN 'SKY_SOIP'
                     WHEN (sip.serviceinstancetype = 300 AND srv.servicetype = 'AMP')  THEN 'SKY_AMP'
                     WHEN SIP.ServiceInstanceType = 300
                                        AND SIC.ServiceInstanceType IN ( 610, 620)     THEN 'SKY_MOBILE'
                     WHEN BA.Skeletalaccountflag = 1 
                                        AND SIP.SERVICEINSTANCETYPE IS NULL 
                                        AND P.OnlineProfileID IS NULL                  THEN 'SKELETAL'							  
                                                                                       ELSE 'SKY_CORE'   -- Otherwise assume Sky Core account.
                   END  AS AccountType
              FROM CCSOwner.Person             P
              JOIN CCSOwner.BSBPartyRole       PR  ON ( PR.PartyID = P.PartyID )
              JOIN CCSOwner.BSBCustomerRole    CR  ON ( CR.PartyRoleID  = PR.ID )
              JOIN CCSOwner.BSBBillingAccount  BA  ON ( BA.PortfolioID  = CR.PortfolioID )
              LEFT OUTER JOIN CCSOwner.BSBServiceInstance  SIP ON ( SIP.ID = BA.ServiceInstanceID )        -- Parent Service instance
              LEFT OUTER JOIN CCSOwner.BSBServiceInstance  SIC ON ( SIC.ParentServiceInstanceID = SIP.ID ) -- Child service instance, may not have a child SI in case of Sky store.
              LEFT OUTER JOIN CCSOwner.BSBAddressusagerole BUR ON ( BUR.serviceinstanceid  =  SIP.ID )
              LEFT OUTER JOIN CCSOwner.BSBAddress          BAS ON ( BAS.id  =  BUR.addressid )
              LEFT OUTER JOIN CCSOwner.BSBPartyToIdentity  PI  ON ( PI.PartyID      = PR.PartyID          -- Not all customers have a NS Profile ID.
                                                               AND PI.IdenTityType = 'NSPROFILEID' )
              LEFT JOIN rcrm.service           SRV ON ( SRV.billingserviceinstanceid = SIP.ID )
              ) 
                SELECT /*+ parallel(bba 16) pq_distribute(bba hash hash)
                           parallel(bcr 16) pq_distribute(bcr hash hash)
                           parallel(bpr 16) pq_distribute(bpr hash hash)
                           parallel(per 16) pq_distribute(per hash hash)
                           parallel(pti 16) pq_distribute(pti hash hash)
                           parallel(lm 16) pq_distribute(lm hash hash)
                       */
                      bpr.partyId
                    , bba.accountNumber AS accountNumber  
                    , MAX ( atp.AccountType ) AS accountType 
                    , MAX ( per.activeInLoyaltyProgram ) AS loyalty  -- 0=opted OUT, 1=opted in, NULL=has not made any decision on loyalty yet.
                    , MAX ( per.firstName ) AS firstName
                    , MAX ( SUBSTR ( per.middleName , 1 , 2 ) ) AS middleName
                    , MAX ( per.familyName ) AS familyName
                    , MAX ( CASE WHEN lm.mig_result = 'COMPLETED' THEN 1
                                 WHEN lm.mig_result = 'isBundleMigratable-CURRENT BUNDLE NOT MIGRATABLE not in config' THEN 3  -- 16-Jul-2018, not sure if that value is good or bad or in-between, so flagging as a special 3 type.
                                 WHEN lm.mig_result IS NOT NULL THEN 2
                                 ELSE 0 END ) AS limaMigration
                    , MAX ( per.firstName ) || MAX ( per.familyName ) AS userName
                    , MAX ( bba.portfolioId ) AS portfolioId
                    , MAX ( per.onlineProfileId ) AS onlineProfileId
                    , MIN ( pti.identityId ) AS nsProfileId
                    , MAX ( atp.countrycode ) AS countryCode
                    , MAX ( bba.id ) KEEP ( DENSE_RANK FIRST ORDER BY bba.created ASC ) AS billingAccountId  -- earliest billingAccountId
                    , MIN ( bba.created ) AS creationDt
                    , 0 AS inFlightVisit
                    , 0 AS inFlightOrders
                 FROM ccsowner.bsbBillingAccount bba
                 JOIN ccsowner.bsbCustomerRole bcr ON ( bba.portfolioId = bcr.portfolioId )
                 JOIN ccsowner.bsbPartyRole bpr ON ( bcr.partyroleId = bpr.id )
                 JOIN ccsowner.person per ON ( per.partyId = bpr.partyId )
                 JOIN ccsowner.bsbContactor con ON ( con.PartyId = bpr.partyId )
                 JOIN actype atp ON ( atp.PartyID = bpr.partyId AND atp.AccountNumber = bba.accountNumber )
                 LEFT OUTER JOIN ccsowner.bsbPartyToIdentity pti ON ( per.partyId = pti.partyId AND pti.identityType = 'NSPROFILEID' )
                 LEFT OUTER JOIN pos.lima_migration lm ON ( lm.accountNumber = bba.accountNumber )
                WHERE bba.organisation_unit = 10                           -- 20 for NowTV  ( financial entity , only 10 or 20 )
                  AND NVL ( bcr.debtIndicator , 0 ) != 1                   -- 1 = customer is in debt arrears.
                  AND NVL ( per.blockPurchaseSwitch , 0 ) != 1             -- 1 = customer is in debt block.
                  AND bba.accountNumber IS NOT NULL                        -- datafix issue, some rogue customers with no account number.
                  AND NVL ( bcr.unableToPurchaseProductsSwitch , 0 ) != 1  -- 1 = customer cannot purchase products.
                  AND bcr.customerStatusCode = 'CRACT'                     -- ACTIVE 
                  AND bba.customerSubTypeCode NOT IN ( 'TE' , 'ST' )       -- TE=test ST=staff
                  AND NVL ( bba.accountStatusCode , '01' ) = '01'          -- ('01' - Sent to Billing, '02' - Not sent to Billing) - NULLS will be considered as '01'
                  AND con.mortalityStatus NOT IN ( '02' , '03' )           -- 02 Deceased Notified , 03 Deceased Confirmed.  -- 02/10/23 RFA - exclusion added
            GROUP BY bpr.partyId,bba.accountNumber 
       ;     
       l_count := SQL%ROWCOUNT ;
       COMMIT ;
       logger.write ( 'CustomersV2 - insert completed : '||TO_CHAR ( l_count ) || ' rows inserted' ) ;
       
       -- Select all mobile products to be evaluated
       select listagg ( ''''||productdescription||'''' ,',') within group (order by productdescription) productdescription
         into l_mobile_products
         from refdatamgr.bsbcatalogueproduct 
        where productDescription = 'Multi-size SIM card for Sky Mobile' 
           or (servicetypecode in ('2002','2003') 
          and productdescription not like '%Roaming Charge%');
     
     /***********************************************************************************************************************************************************
    The following query is used to update in one command all the flags for each portfolioID found within the CUSTOMERSV2 table
    These flags are "calculated" analysing the STATUS colukn within the "ccsowner.bsbPortfolioProduc" table for each PRODUCTCATAOLOGID that matches 
    within the "dataprov.Products" table
    The CASE statement gives a greater weight to anything that's any code other than "active"
    The WHERE clause will avoid getting the wrong weight on status like the list below for all scenarios :
        CN=cancelled 
        FBI=Cancelled Talk 
        SC=System Cancelled 
        RP=Replaced
    We have also added a clause that will only evaluate the last status change to bring the current data
    ***********************************************************************************************************************************************************/
    
       logger.write ( 'CustomersV2 - Portfolio merge (main) starting' ) ;
       -- Moved to dynamic SQL to feed a fix list of mobile products into the query
       l_query := q'#
            MERGE /*+ parallel(16) */ INTO cust_origv2 t
            USING (
               SELECT * FROM ( 
                   WITH product_flags AS (
                        SELECT /*+ parallel (16) */
                            bpp.portfolioId
                            , CASE WHEN p.productDescription LIKE 'Sky Talk%' AND bpp.status = 'A' THEN 1
                                   WHEN p.productDescription LIKE 'Sky Talk%' AND bpp.status != 'A' THEN 2
                                   ELSE 0 END AS talk
                            , CASE WHEN ( p.productDescription LIKE 'Sky Fibre%' 
                                       OR p.productDescription LIKE 'Sky Broadband%' 
                                       OR p.productDescription LIKE '%Broadband%Superfast%' 
                                       OR p.productDescription LIKE '%Broadband%Ultrafast%' 
                                       OR p.productDescription LIKE 'Sky Full Fibre%' 
                                       OR p.productDescription LIKE 'NOW Full Fibre%') AND bpp.status = 'AC' THEN 1
                                   WHEN ( p.productDescription LIKE 'Sky Fibre%' 
                                       OR p.productDescription LIKE 'Sky Broadband%' 
                                       OR p.productDescription LIKE '%Broadband%Superfast%' 
                                       OR p.productDescription LIKE '%Broadband%Ultrafast%' 
                                       OR p.productDescription LIKE 'Sky Full Fibre%' 
                                       OR p.productDescription LIKE 'NOW Full Fibre%') AND bpp.status != 'AC' THEN 2
                                   ELSE 0 END AS bband
                            , CASE WHEN ( p.productDescription = 'Sky Broadband 12GB' ) AND bpp.status = 'AC' THEN 1
                                   WHEN ( p.productDescription = 'Sky Broadband 12GB' ) AND bpp.status != 'AC' THEN 2
                                   ELSE 0 END AS bb12gb
                            , CASE WHEN ( p.productDescription LIKE 'Sky Broadband Lite%' ) AND bpp.status = 'AC' THEN 1
                                   WHEN ( p.productDescription LIKE 'Sky Broadband Lite%' ) AND bpp.status != 'AC' THEN 2
                                   ELSE 0 END AS bbandLite
                            , CASE WHEN p.productDescription IN (#' || l_mobile_products || q'#) AND bpp.status != 'CRQ' THEN 1
                                   WHEN p.productDescription IN (#' || l_mobile_products || q'#) AND bpp.status = 'CRQ' THEN 2  -- Cease Requested
                                   ELSE 0 END AS mobile
                            , CASE WHEN p.productDescription LIKE '%Sports%' AND bpp.status = 'EN' THEN 1
                                   WHEN p.productDescription LIKE '%Sports%' AND bpp.status != 'EN' THEN 2
                                   ELSE 0 END AS sports
                            , CASE WHEN p.productDescription LIKE '%Cinema%' AND bpp.status = 'EN' THEN 1
                                   WHEN p.productDescription LIKE '%Cinema%' AND bpp.status != 'EN' THEN 2
                                   ELSE 0 END AS cinema
                            , CASE WHEN p.productDescription = 'Sky Kids' AND bpp.status = 'EN' THEN 1
                                   WHEN p.productDescription = 'Sky Kids' AND bpp.status != 'EN' THEN 2
                                   ELSE 0 END AS kids
                            , CASE WHEN p.productDescription IN ( 'Chelsea TV' , 'Liverpool TV' , 'MUTV' ) AND bpp.status = 'EN' THEN 1
                                   WHEN p.productDescription IN ( 'Chelsea TV' , 'Liverpool TV' , 'MUTV' ) AND bpp.status != 'EN' THEN 2
                                   ELSE 0 END AS premium
                            , CASE WHEN p.productDescription LIKE 'Original%' AND bpp.status = 'EN' THEN 1
                                   WHEN p.productDescription LIKE 'Original%' AND bpp.status != 'EN' THEN 2
                                   ELSE 0 END AS original
                            , CASE WHEN p.productDescription LIKE 'Variety%' AND bpp.status = 'EN' THEN 1
                                   WHEN p.productDescription LIKE 'Variety%' AND bpp.status != 'EN' THEN 2
                                   ELSE 0 END AS variety
                            , CASE WHEN p.productDescription LIKE 'Box Sets%' AND bpp.status = 'EN' THEN 1
                                   WHEN p.productDescription LIKE 'Box Sets%' AND bpp.status != 'EN' THEN 2
                                   ELSE 0 END AS boxSets
                            , CASE WHEN p.productDescription LIKE 'Sky Q%Bundle%' AND bpp.status = 'EN' THEN 1
                                   WHEN p.productDescription LIKE 'Sky Q%Bundle%' AND bpp.status != 'EN' THEN 2
                                   ELSE 0 END AS skyQBundle
                            , CASE WHEN p.productDescription = 'Sky Sports Cricket' AND bpp.status = 'EN' THEN 1
                                   WHEN p.productDescription = 'Sky Sports Cricket' AND bpp.status != 'EN' THEN 2
                                   ELSE 0 END AS cricket
                            , CASE WHEN p.productDescription = 'Sky Sports Premier League' AND bpp.status = 'EN' THEN 1
                                   WHEN p.productDescription = 'Sky Sports Premier League' AND bpp.status != 'EN' THEN 2
                                   ELSE 0 END AS premierLeague
                            , CASE WHEN p.productDescription = 'Sky Sports Football' AND bpp.status = 'EN' THEN 1
                                   WHEN p.productDescription = 'Sky Sports Football' AND bpp.status != 'EN' THEN 2
                                   ELSE 0 END AS football
                            , CASE WHEN p.productDescription = 'Sky Sports - Complete Pack' AND bpp.status = 'EN' THEN 1
                                   WHEN p.productDescription = 'Sky Sports - Complete Pack' AND bpp.status != 'EN' THEN 2
                                   ELSE 0 END AS completeSports
                            , CASE WHEN ( p.productDescription LIKE 'Sky Q % box' OR p.productDescription LIKE 'Sky Q % UHD' ) AND bpp.status = 'IN' THEN 1
                                   WHEN ( p.productDescription LIKE 'Sky Q % box' OR p.productDescription LIKE 'Sky Q % UHD' ) AND bpp.status != 'IN' THEN 2
                                   ELSE 0 END AS skyQBox
                            , CASE WHEN p.productDescription LIKE 'Sky+ HD Box%' AND bpp.status = 'IN' THEN 1
                                   WHEN p.productDescription LIKE 'Sky+ HD Box%' AND bpp.status != 'IN' THEN 2
                                   ELSE 0 END AS skyHDBox
                            , CASE WHEN p.productDescription LIKE 'Sky Entertainment%' AND bpp.status = 'EN' THEN 1
                                   WHEN p.productDescription LIKE 'Sky Entertainment%' AND bpp.status != 'EN' THEN 2
                                   ELSE 0 END AS entertainment
                            , CASE WHEN p.productDescription LIKE 'Sky Soundbox' AND bpp.status = 'DL' THEN 1
                                   WHEN p.productDescription LIKE 'Sky Soundbox' AND bpp.status != 'DL' THEN 2
                                   ELSE 0 END AS soundbox
                            , CASE WHEN p.productDescription = 'Spotify' AND bpp.status IN ( 'AC' , 'EN' ) THEN 1
                                   WHEN p.productDescription = 'Spotify' AND bpp.status NOT IN ( 'AC' , 'EN' ) THEN 2
                                   ELSE 0 END AS spotify
                            , CASE WHEN p.productDescription LIKE 'Netflix%' AND bpp.status IN ( 'AC' , 'EN' ) THEN 1
                                   WHEN p.productDescription LIKE 'Netflix%' AND bpp.status NOT IN ( 'AC' , 'EN' ) THEN 2
                                   ELSE 0 END AS netflix
                            , CASE WHEN p.productDescription = 'Ultimate On Demand' AND bpp.status IN ( 'AC' , 'EN' ) THEN 1
                                   WHEN p.productDescription = 'Ultimate On Demand' AND bpp.status NOT IN ( 'AC' , 'EN' ) THEN 2
                                   ELSE 0 END AS ultimateOnDemand
                            , CASE WHEN ( p.productDescription LIKE 'Sky Fibre%' 
                                       OR p.productDescription LIKE '%Broadband%Ultrafast%' 
                                       OR p.productDescription LIKE '%Broadband%Superfast%' 
                                       OR p.productDescription LIKE 'Sky Full Fibre%' 
                                       OR p.productDescription LIKE 'NOW Full Fibre%') AND bpp.status = 'AC' THEN 1
                                   WHEN ( p.productDescription LIKE 'Sky Fibre%' 
                                       OR p.productDescription LIKE '%Broadband%Ultrafast%' 
                                       OR p.productDescription LIKE '%Broadband%Superfast%' 
                                       OR p.productDescription LIKE 'Sky Full Fibre%' 
                                       OR p.productDescription LIKE 'NOW Full Fibre%') AND bpp.status != 'AC' THEN 2
                                   ELSE 0 END AS fibre
                            , CASE WHEN p.productDescription LIKE 'TNT Sport%' AND bpp.status IN ( 'AC' , 'EN' ) THEN 1
                                   WHEN p.productDescription LIKE 'TNT Sport%' AND bpp.status NOT IN ( 'AC' , 'EN' ) THEN 2
                                   ELSE 0 END AS tntSports
                            , CASE WHEN ( p.productDescription LIKE '%Broadband%Ultrafast%' ) AND bpp.status = 'AC' THEN 1
                                   WHEN ( p.productDescription LIKE '%Broadband%Ultrafast%' ) AND bpp.status != 'AC' THEN 2
                                   ELSE 0 END AS bbUltraFast
                            , CASE WHEN ( p.productDescription LIKE '%Broadband%Superfast%' ) AND bpp.status = 'AC' THEN 1
                                   WHEN ( p.productDescription LIKE '%Broadband%Superfast%' ) AND bpp.status != 'AC' THEN 2
                                   ELSE 0 END AS bbSuperFast
                            , CASE WHEN ( p.productDescription LIKE '%Sky Signature%' ) AND bpp.status IN ( 'AC' , 'EN' ) THEN 1
                                   WHEN ( p.productDescription LIKE '%Sky Signature%' ) AND bpp.status NOT IN ( 'AC' , 'EN' ) THEN 2
                                   ELSE 0 END AS skySignature
                            , CASE WHEN ( p.productDescription LIKE '%Ultimate TV Add on%' ) AND bpp.status IN ( 'AC' , 'EN' ) THEN 1
                                   WHEN ( p.productDescription LIKE '%Ultimate TV Add on%' ) AND bpp.status NOT IN ( 'AC' , 'EN' ) THEN 2
                                   ELSE 0 END AS ultimateTvAddon
                            , CASE WHEN p.productDescription LIKE 'Disney+%' AND bpp.status IN ( 'AC' , 'EN' ) THEN 1
                                   WHEN p.productDescription LIKE 'Disney+%' AND bpp.status NOT IN ( 'AC' , 'EN' ) THEN 2
                                   ELSE 0 END AS disneyPlus
                            , CASE WHEN p.productDescription LIKE 'Service Call Visit%' AND bpp.status = 'AV' THEN 1
                                   WHEN p.productDescription LIKE 'Service Call Visit%' AND bpp.status != 'CP' THEN 2  -- CP=Complete
                                   ELSE 0 END AS visitInProgress
                            , CASE WHEN p.productDescription = 'Viewing Card' AND bpp.status = 'A' THEN 1
                                   WHEN p.productDescription = 'Viewing Card' AND bpp.status NOT IN ( 'DL' ,'REN' ) THEN 2  -- REN="Replaced Enabled" DL=Disabled
                                   ELSE 0 END AS activeViewingCard
                            , CASE WHEN p.productDescription LIKE 'Discovery+%' AND bpp.status IN ( 'AC' , 'EN' ) THEN 1
                                   WHEN p.productDescription LIKE 'Discovery+%' AND bpp.status NOT IN ( 'AC' , 'EN' ) THEN 2
                                   ELSE 0 END AS discoveryPlus
                            , CASE WHEN p.productDescription LIKE '%eSIM%' AND bpp.status IN ( 'DS' ) THEN 1 
                                   WHEN p.productDescription LIKE '%eSIM%' AND bpp.status NOT IN ( 'DS' ) THEN 2
                                   ELSE 0 END AS mobEsim        -- RFA 18/09/23 - added new column to flag customers with eSIM contracts
                            , CASE WHEN bpp.status IN ( 'PC' , 'RQ' , 'KQ' ) THEN 1 
                                   ELSE 0 END AS inFlightOrders
                        FROM ccsowner.bsbPortfolioProduct bpp
                        LEFT JOIN dataprov.Products p ON bpp.catalogueProductId = p.id
                        WHERE bpp.status NOT IN ('CN', 'FBI', 'SC', 'RP')
                    )
                    SELECT
                        portfolioId
                       ,MAX(talk) AS talk
                       ,MAX(bband) AS bband
                       ,MAX(bb12gb) AS bb12gb
                       ,MAX(bbandLite) AS bbandLite
                       ,MAX(mobile) AS mobile
                       ,MAX(sports) AS sports
                       ,MAX(cinema) AS cinema
                       ,MAX(kids) AS kids
                       ,MAX(premium) AS premium
                       ,MAX(original) AS original
                       ,MAX(variety) AS variety
                       ,MAX(boxSets) AS boxSets
                       ,MAX(skyQBundle) AS skyQBundle
                       ,MAX(cricket) AS cricket
                       ,MAX(premierLeague) AS premierLeague
                       ,MAX(football) AS football
                       ,MAX(completeSports) AS completeSports
                       ,MAX(skyQBox) AS skyQBox
                       ,MAX(skyHDBox) AS skyHDBox
                       ,MAX(entertainment) AS entertainment
                       ,MAX(soundbox) AS soundbox
                       ,MAX(spotify) AS spotify
                       ,MAX(netflix) AS netflix
                       ,MAX(ultimateOnDemand) AS ultimateOnDemand
                       ,MAX(fibre) AS fibre
                       ,MAX(tntSports) AS tntSports
                       ,MAX(bbUltraFast) AS bbUltraFast
                       ,MAX(bbSuperFast) AS bbSuperFast
                       ,MAX(skySignature) AS skySignature
                       ,MAX(ultimateTvAddon) AS ultimateTvAddon
                       ,MAX(disneyPlus) AS disneyPlus
                       ,MAX(visitInProgress) AS visitInProgress
                       ,MAX(activeViewingCard) AS activeViewingCard
                       ,MAX(discoveryPlus) AS discoveryPlus
                       ,MAX(mobEsim) AS mobEsim        
                       ,MAX(inFlightOrders) AS inFlightOrders
                    FROM product_flags
                    GROUP BY portfolioId
                    )
                ) s
                ON (s.portfolioId = t.portfolioId)
                WHEN MATCHED THEN UPDATE SET
                      t.talk = s.talk
                    , t.bband = s.bband
                    , t.bb12gb = s.bb12gb
                    , t.bBandLite = s.bBandLite
                    , t.mobile = (CASE WHEN s.mobesim = 1 THEN 1 ELSE s.mobile END) 
                    , t.sports = s.sports
                    , t.cinema = s.cinema
                    , t.kids = s.kids
                    , t.premium = s.premium
                    , t.original = s.original
                    , t.variety = s.variety
                    , t.boxSets = s.boxSets
                    , t.skyQbundle = s.skyQbundle
                    , t.cricket = s.cricket
                    , t.premierLeague = s.premierLeague
                    , t.football = s.football
                    , t.completeSports = s.completeSports
                    , t.skyQBox = s.skyQBox
                    , t.skyHdBox = s.skyHdBox
                    , t.entertainment = s.entertainment
                    , t.soundBox = s.soundBox
                    , t.spotify = s.spotify
                    , t.netflix = s.netflix
                    , t.ultimateOnDemand = s.ultimateOnDemand
                    , t.fibre = s.fibre
                    , t.tntSports = s.tntSports
                    , t.bbUltraFast = s.bbUltraFast
                    , t.bbSuperFast = s.bbSuperFast
                    , t.skySignature = s.skySignature
                    , t.ultimateTvAddOn = s.ultimateTvAddOn
                    , t.disneyPlus = s.disneyPlus
                    , t.visitInProgress = s.visitInProgress
                    , t.activeViewingCard = s.activeViewingCard
                    , t.discoveryPlus = s.discoveryPlus
                    , t.mobEsim = s.mobEsim
                    , t.inFlightOrders = s.inFlightOrders    #' 
       ;   
       execute immediate l_query ;
       l_count := sql%rowcount;
       COMMIT ;
       logger.write ( 'CustomersV2 - Portfolio merge (main) completed : '|| to_char(l_count)) ;

       -- Ringfenced customers for Q Harmonization
       -- AH for Alex Benatatos on 12/06
       --logger.write ( 'Q Harmonisation Ringfence Accounts') ;
       --update cust_origv2 set pool = 'Q_HARM_RINGFENCE' where accountnumber in (select accountnumber from Q_HARM_RINGFENCE);
       --commit;
          
       logger.write ( 'CustomersV2 - self merge starting' ) ;
       UPDATE /*+ parallel(16) */ cust_origv2 c
          SET c.dtv = CASE
                 WHEN c.sports = 1 OR c.cinema = 1 OR c.original = 1 OR c.variety = 1 OR c.boxsets = 1 OR c.skyQBundle = 1 OR c.entertainment = 1 OR c.skySignature = 1 THEN 1
                 ELSE GREATEST ( c.sports , c.cinema , c.original , c.variety , c.boxsets , c.skyQBundle , c.entertainment , c.skySignature )
                 END
            , c.loyaltytier =  CASE
                                WHEN c.creationdt BETWEEN ADD_MONTHS(trunc(SYSDATE), -(8*12))  AND ADD_MONTHS(trunc(SYSDATE), -(3*12)) THEN 'Gold' 
                                WHEN c.creationdt BETWEEN ADD_MONTHS(trunc(SYSDATE), -(15*12)) AND ADD_MONTHS(trunc(SYSDATE), -(8*12)) THEN 'Platinum' 
                                WHEN c.creationdt       < ADD_MONTHS(trunc(SYSDATE), -(15*12))                                         THEN 'Diamond' 
                                ELSE 'Silver'  
                              END 
       ;
       l_count := sql%rowcount ;
       COMMIT ;
       logger.write ( 'CustomersV2 - self merge completed : '|| to_char(l_count)) ;
       
       logger.write ( 'CustomersV2 - tokens merge starting' ) ;
--       UPDATE /*+ PARALLEL(8) */ cust_origv2 c 
--            SET ( c.skycesa01Token, c.ssoToken, c.messoToken, c.emailAddress ) =
--                ( SELECT t.cesaToken, t.ssoToken, t.messoToken, t.emailAddress
--                    FROM CustomerTokens t
--                   WHERE c.accountNumber = t.accountNumber
--                     AND c.partyID = t.partyID );

       MERGE /*+ PARALLEL(c 8) PARALLEL(t 8) */ INTO cust_origv2 c
       USING CustomerTokens t
       ON (c.accountNumber = t.accountNumber AND c.partyID = t.partyID)
        WHEN MATCHED THEN
        UPDATE SET 
            c.skycesa01Token = t.cesaToken,
            c.ssoToken       = t.ssoToken,
            c.messoToken     = t.messoToken,
            c.emailAddress   = t.emailAddress
       ;       
            
       l_count := sql%rowcount ;
       COMMIT ;
       logger.write ( 'CustomersV2 - tokens merge completed : '|| to_char(l_count)) ;
       
       logger.write ( 'CustomersV2 - inFlightVisit starting' ) ;
       -- Below for SPS message: "PIMM rule id rule_do_not_allow_post_active_cancel_with_in_flight_visit has resulted in a Do Not Allow outcome."
       -- Complete/Cancelled are ok. UB and BK are definitely a problem, but maybe could add filters for visit_date.
       -- List is: select code,codeDesc from refdatamgr.picklist where codeGroup = 'VisitRequirementStatus' order by 1 ;
       dbms_stats.gather_table_stats ( ownName => USER , tabName => 'cust_origv2' ) ;
       logger.write ( 'CustomersV2 - portfolio merge (inFlightVisit) started' ) ;
       UPDATE /*+ parallel(16) */ cust_origv2 t
       SET t.inFlightVisit = 1
       WHERE EXISTS (
           SELECT 1
           FROM ccsowner.bsbVisitRequirement bvr
           JOIN ccsowner.bsbAddressUsageRole baur ON bvr.installationAddressRoleId = baur.id
           JOIN ccsowner.bsbServiceInstance bsi ON baur.serviceInstanceId = bsi.id
           WHERE bvr.statusCode NOT IN ('CP', 'CN')
             AND bsi.portfolioId = t.portfolioId
       )
       ;
       l_count := sql%rowcount ;
       COMMIT ;
       logger.write ( 'CustomersV2 - portfolio merge (inFlightVisit) completed : '|| to_char(l_count)) ;
    
       logger.write ( 'CustomersV2 - Product count starting' ) ;
       MERGE INTO cust_origv2 t USING (
          SELECT /*+ parallel(16) */ portfolioId , COUNT(*) AS port_cnt
            FROM ccsowner.bsbportFolioProduct
          GROUP BY portfolioId) s ON ( s.portfolioid = t.portfolioid )
       WHEN MATCHED THEN UPDATE SET t.port_cnt = s.port_cnt
       ;
       l_count := sql%rowcount ;
       COMMIT ;
       logger.write ( 'CustomersV2 - Product count completed : '|| to_char(l_count)) ;
    
       logger.write ( 'CustomersV2 - mobile device count starting' ) ;
       MERGE INTO cust_origv2 t USING (
          SELECT /*+ parallel(16) */ bfp.portfolioid, count(*) mob_devices
          FROM ccsowner.bsbportFolioProduct bfp, ccsowner.bsbcustomerproductelement cpe
         where cpe.portfolioproductid = bfp.id
           and cpe.customerproductelementtype = 'MD'
        group by bfp.portfolioid) s ON ( s.portfolioid = t.portfolioid )
       WHEN MATCHED THEN UPDATE SET t.MOB_DEVICES = s.MOB_DEVICES
       ;
       l_count := sql%rowcount ;
       COMMIT ;
       logger.write ( 'CustomersV2 - mobile device count completed : '|| to_char(l_count)) ;
       
       
       logger.write ( 'CustomersV2 - begin UMLTOKEN population' ) ;
       execute immediate 'truncate table ulmOnboardToken' ;
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
       logger.write ( 'CustomersV2 - merge UMLTOKEN' ) ;
       MERGE INTO cust_origv2 t USING (
          SELECT a.partyId
               , MAX ( a.ulmToken ) KEEP ( DENSE_RANK FIRST ORDER BY a.expiry_date DESC ) AS ulmToken  -- for latest expiry_date
            FROM ulmOnboardToken a
           GROUP BY a.partyId
       ) s ON ( s.partyId = t.partyId )
       WHEN MATCHED THEN UPDATE SET t.ulmToken = s.ulmToken WHERE NVL ( t.ulmToken , 'x' ) != NVL ( s.ulmToken , 'x' )
       ;
       l_count := sql%rowcount ;
       COMMIT ;
       logger.write ( 'CustomersV2 - UMLTOKEN merge completed : '||l_count ) ;
       
       logger.write ( 'CustomersV2 - Table Copy starting' ) ;
       EXECUTE IMMEDIATE 'TRUNCATE TABLE CUSTOMERSV2' ;
       EXECUTE IMMEDIATE 'ALTER SESSION SET skip_unusable_indexes = TRUE' ;
       FOR r1 IN (
          SELECT i.index_name
            FROM all_indexes i
           WHERE i.owner = 'DATAPROV'
             AND i.table_name = 'CUSTOMERSV2'
             AND i.status = 'VALID'
           ORDER BY 1
       )
       LOOP
          EXECUTE IMMEDIATE 'ALTER INDEX dataprov.' || r1.index_name || ' UNUSABLE' ;
       END LOOP ;
       INSERT /*+ append parallel(8) */ INTO CUSTOMERSV2 t
       SELECT s.*
         FROM (
               SELECT co.*
                 FROM cust_origv2 co
                ORDER BY dbms_random.value
              ) s
       ;
       COMMIT ;
    
       --EXECUTE IMMEDIATE 'truncate table cust_origv2' ;
       FOR r1 IN (
          SELECT i.index_name
            FROM all_indexes i
           WHERE i.owner = 'DATAPROV'
             AND i.table_name = 'CUSTOMERSV2'
             AND i.status = 'UNUSABLE'
          ORDER BY 1
       )
       LOOP
          EXECUTE IMMEDIATE 'ALTER INDEX dataprov.' || r1.index_name || ' REBUILD NOLOGGING PARALLEL 8' ;
          EXECUTE IMMEDIATE 'ALTER INDEX dataprov.' || r1.index_name || ' NOPARALLEL' ;
       END LOOP ;
       
       logger.write ( 'CustomersV2 - ALL complete' ) ;
    END rebuild_CustomersV2 ;

PROCEDURE customer_debt_new IS
   -- Called by separate Jenkins job, dataprov part 2.
   -- 17-May-2023 Andrew Fraser moved to fully remote procedure for performance following v19 database upgrade.
   l_count  number ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'alter session set ddl_lock_timeout=60';
   execute immediate 'TRUNCATE TABLE da_cus_tmp' ;
   FOR i IN 1..6
   LOOP
      execute immediate 'BEGIN data_prep_kenan_cus.da_cus_tmp@cus0' || TO_CHAR ( i ) || ' ; END ;' ;
      execute immediate '
      INSERT /*+ append */ INTO da_cus_tmp t ( t.account_no , t.debt , t.server_id )
      SELECT s.account_no
           , s.debt
           , ' || TO_CHAR ( i + 2 ) || ' AS server_id
        FROM da_cus_tmp@cus0' || TO_CHAR ( i ) || ' s'
      ;
      l_count := SQL%ROWCOUNT ;
      COMMIT ;
      logger.write ( 'da_cus_tmp cus0' || TO_CHAR ( i ) || ' complete. ' || TO_CHAR ( l_count ) || ' rows inserted.' ) ;
   END LOOP ;
   customer_debt ;  -- 02-Nov-2021 Andrew Fraser called from customer_debt_new to workaround sequencing problems.
   logger.write ( 'complete' ) ;
EXCEPTION   
WHEN OTHERS THEN
-- Create a job that fails to register that this pool has failed.
       DBMS_SCHEDULER.CREATE_JOB (
        job_name                 =>  'PCUSTOMER_DEBT_NEW', 
        job_type                 =>  'PLSQL_BLOCK',
        job_action               =>  'BEGIN raise_application_error(-20110,''CUSTOMER_DEBT_NEW has failed '||sqlcode||':'||sqlerrm||''''||'); END;',
        enabled => TRUE);
   
END customer_debt_new ;

PROCEDURE customer_debt IS
    l_count     number ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'alter session set ddl_lock_timeout=60';
   DELETE FROM debt_amount t ;
   INSERT INTO debt_amount t ( t.accountNumber , t.balance )
   SELECT eiam.external_id AS accountNumber , dct.debt AS balance
     FROM external_id_acct_map@adm eiam
     JOIN da_cus_tmp dct ON eiam.account_no = dct.account_no AND eiam.server_id = dct.server_id
    WHERE eiam.external_id_type = 1
   ;
   l_count := SQL%ROWCOUNT ;
   COMMIT ;
   logger.write ( TO_CHAR ( l_count ) || ' rows inserted.' ) ;
   /*  02-Nov-2021 Andrew Fraser commented out because no longer sure John Barclay runs billing every day, esp. in n02.
   -- Nullify debt_amount for anyone who is about to be billed - their balance will change in the middle of daytime concurrent
   MERGE INTO debt_amount t USING (
      WITH latest_file AS (
         SELECT MAX ( f.file_id ) KEEP ( DENSE_RANK FIRST ORDER BY f.create_dt DESC ) AS latest_file_id
           FROM sky.extn_ddo_file_info@adm f
          WHERE f.file_name like 'CSKYUKD.DAT%'
      )
      SELECT DISTINCT m.external_id AS accountNumber
        FROM sky.extn_ddo_staging@adm d
        JOIN latest_file l ON l.latest_file_id = d.file_id
        JOIN arbor.external_id_acct_map@adm m ON m.account_no = d.account_no
       WHERE m.external_id_type = 1
   ) s ON ( s.accountNumber = t.accountNumber )
   WHEN MATCHED THEN UPDATE SET t.balance = NULL WHERE t.balance IS NOT NULL
   ;
   */
   logger.write ( 'complete' ) ;
END customer_debt ;

/*******************************************************************************************
-- customers_old - Replaced 
********************************************************************************************/
PROCEDURE customers_old IS  -- this runs every night.
BEGIN
   -- 17/11/24 - RFA - Changed the CUSTOMERS table build to populate the token & email columns from a support table called CustomerTokens
   -- CustomerTokens constains a larger number of customers (if not all) to support the tokens in all possible data pools
   rebuild_products ;
   rebuild_tokens ; 
   rebuild_customers ;  
   buildSupportingTables ;
END customers_old ;

/*******************************************************************************************
-- New procedure to create the new version of the CUSTOMERS table
-- This version brings all the account numbers linked to a partyID via its portfolio.
-- New replaced
********************************************************************************************/
PROCEDURE customersv2_old IS  
BEGIN
   rebuild_customersv2 ;  
END customersv2_old ;


/*******************************************************************************************
-- CustSupport  - Build all the Customer Suuport tables before the CUSTOMERS and CUSTOMERV2 
-- tables are build
********************************************************************************************/
PROCEDURE CustSupport IS  -- this runs every night.
BEGIN
   rebuild_products ;
   rebuild_tokens ; 
END CustSupport ;

/*******************************************************************************************
-- customers - Build the CUSTOMERS table and it should be dependent on the CustSupport
-- procedure having been run ahed of this.
-- This should be controlled within the JOBS DEPENDENCIES
********************************************************************************************/
PROCEDURE customers IS  -- this runs every night.
BEGIN
   rebuild_customers ;  
   buildSupportingTables ;
END customers ;

/*******************************************************************************************
-- customersv2 - Build the CUSTOMERSV2 table and it should be dependent on the CustSupport
-- procedure having been run ahed of this.
-- This should be controlled within the JOBS DEPENDENCIES
********************************************************************************************/
PROCEDURE customersv2 IS  
BEGIN
   rebuild_customersv2 ;  
END customersv2 ;


END customers_pkg ;
/
