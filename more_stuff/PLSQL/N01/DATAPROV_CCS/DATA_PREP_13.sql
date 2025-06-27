create or replace PACKAGE DATA_PREP_13 AS 

  PROCEDURE glassairdispatchedproducts;
  PROCEDURE digitalbbupgrade_aa;

END DATA_PREP_13;
/

create or replace PACKAGE BODY DATA_PREP_13 AS 

PROCEDURE glassairdispatchedproducts IS
    -- PERFENG-8670
    l_pool   VARCHAR2(29) := 'GLASSAIRDISPATCHEDPRODUCTS' ;
    l_count  NUMBER ;
BEGIN
    logger.write ( 'begin '||l_pool ) ;
   -- staging table hosted in fps for performance during the merge statement, to use index on partyId
    DELETE FROM SOIPGLASSAIRDISPATCHED@fps ;
    COMMIT ;
    logger.write ( l_pool ||' - data deleted from FPS temp table ') ;

    INSERT INTO SOIPGLASSAIRDISPATCHED@fps ( accountNumber , productId , partyId )
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
          AND p.suid LIKE 'SKY_GLASS_AIR%'
          AND hp.serialNumber is NOT NULL
          --AND hp.serialNumber LIKE 'TV11SKA%'  -- Not applicable for this product    
    ;
    l_count := SQL%ROWCOUNT ;
    COMMIT;
    logger.write ( l_pool || TO_CHAR ( l_count ) || ' rows inserted' ) ;

    -- merge statement held in fps for performance and to workaround "ORA-22992: cannot use LOB locators selected from remote tables"
    data_prep_fps.SOIPGLASSAIRDISPATCHED@fps ;
    COMMIT;
    logger.write ( l_pool || ' Remote MERGE in FPS process completed ') ;

    sequence_pkg.seqBefore ( i_pool => l_pool ) ;
    INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.fulfilmentReferenceId , t.productId , t.accountNumber , t.partyId )
    SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.fulfilmentReferenceId , s.productId , s.accountNumber , s.partyId
     FROM (
           SELECT d.fulfilmentReferenceId , d.productId , d.accountNumber , d.partyId
             FROM SOIPGLASSAIRDISPATCHED@fps d
            WHERE d.fulfilmentReferenceId IS NOT NULL
            ORDER BY dbms_random.value  
          ) s
    ;
    sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;

    logger.write ( 'complete '||l_pool  ) ;
END glassairdispatchedproducts ;


PROCEDURE digitalbbupgrade_aa IS
   -- For PERFENG-9220
   l_pool VARCHAR2(29) := 'DIGITALBBUPGRADE_AA' ;
   l_telout varchar2(32) ;
   l_count number;
   l_magicPcode varchar2(7) := 'AB301AA' ; -- sample City Fibre postcodes include AB301AA , AB565AA, AB130AA, AB130AA,AB453AA
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
       AND c.emailAddress NOT LIKE 'noemail%'
       AND NOT EXISTS (select null
                       from dataprov.dprov_accounts_fast f
                       where f.accountnumber = c.accountnumber
                       and f.pool_name = 'DIGITALBBUPGRADE_DJ')
       AND ROWNUM <= 2000
   ;
   l_count := SQL%ROWCOUNT;
   logger.write ('digitalbbupgrade_aa - rows inserted : '|| l_count) ;
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
   commit;
   logger.write ( 'complete' ) ;
END DIGITALBBUPGRADE_AA ;


END DATA_PREP_13;
/