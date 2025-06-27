CREATE OR REPLACE PACKAGE data_prep_02 AS
PROCEDURE digTvOnlyNoBurn ;
PROCEDURE digMobOnlyRegradeNoBurn ;
PROCEDURE fspMigrate ;
PROCEDURE netflix ;
PROCEDURE fspUpgrade ;
PROCEDURE tvRecontractingNoBurn ;
PROCEDURE tvRecontracting ;
PROCEDURE fspUpgradeLwsWeb ;
PROCEDURE fspDowngradeLwsWeb ;
PROCEDURE fspDowngrade ;
PROCEDURE fspAddPremium ;
PROCEDURE fspSpecificSports ;
PROCEDURE fspEntertainmentNoPrem ;
PROCEDURE fspUpgradeNoSports ;
PROCEDURE fspUpgradeNoSportsNoBurn ;
PROCEDURE fspEntertainmentNoPrem_adobe ;
PROCEDURE digitalDtvUpgradeNoQ ;
PROCEDURE digitalDtvUpgradeNoQNoBurn ;
PROCEDURE digBBOnlyRegradeNoBurn ;
PROCEDURE act_cust_dtv_bb_talk ;
PROCEDURE act_trpl_ply_cust_iss_lt ;
PROCEDURE custForMonthlyVcCallbacks ;
PROCEDURE eocn_basket_ref ;
PROCEDURE eocn_no_debt ;
PROCEDURE eocn_no_debt_mob ;
PROCEDURE digbbonlyregrade ;
END data_prep_02 ;
/


CREATE OR REPLACE PACKAGE BODY data_prep_02 AS

PROCEDURE digTvOnlyNoBurn IS
  l_pool VARCHAR2(29) := 'DIGTVONLYNOBURN' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyid , s.messotoken
     FROM (
   SELECT *
     FROM customers c     
    WHERE ( c.boxSets = 1 OR c.cinema = 1 OR c.sports = 1  )
      AND c.entertainment = 1
     AND c.BBAND = 0
     AND c.talk = 0
     AND c.bb12gb = 0
     AND c.mobile = 0
     AND c.pool is null
     AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
     AND ( c.skyQBox = 1 OR c.skyHDBox = 1 )  -- to be upgraded to BoxSets, must have a box capable of downloading.
     AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- 26-Mar-2018 Andrew Fraser request Nic Patte.
     AND c.countryCode = 'GBR'
     -- eliminate those with a valid power of attorney
     and c.portfolioid not in (select po.portfolioid from ccsowner.BSBPOWEROFATTORNEYROLE po where (EFFECTIVETODATE is null or EFFECTIVETODATE > sysdate))
     AND EXISTS (  -- 05-Apr-2018 Andrew Fraser only include customers who have a home landline telephone, request Nic Patte .
                  SELECT /*+ parallel(con, 4) parallel(ct, 4) */ NULL
                    FROM ccsowner.bsbContactor con
                    JOIN ccsowner.bsbContactTelephone ct ON ct.contactorId = con.id
                   WHERE con.partyId = c.partyId  -- join
                     AND ct.typeCode = 'H'
                     AND ct.deletedFlag = 0  -- not deleted
                     AND ct.effectiveToDate IS NULL   -- could add: or < SYSDATE + could also check effectiveFromDate
                 )
     AND EXISTS (  -- 07-JUL-2020 Alex Hyslop only include customers who do not have an escalation against the account. For Thomas Owen
                  SELECT /*+ parallel(con, 4) */ NULL
                    FROM ccsowner.bsbContactor con
                   WHERE con.partyId = c.partyId  -- join
                     AND con.escalationcode is null
                 )
     AND c.accountNumber in (select /*+ parallel(a, 4) */ a.accountnumber
                               from dataprov.act_cust_uk_subs a
                              where bband is null)
     AND EXISTS (  -- 19-Aug-2019 only include customers who have a valid email, request Shane Venter.
                  SELECT /*+ parallel(con, 4) parallel(ct, 4) */ NULL
                    FROM ccsowner.bsbContactor con
                    JOIN ccsowner.BSBCONTACTEMAIL ct ON ct.contactorId = con.id
                   WHERE con.partyId = c.partyId  -- join
                   --AND ct.typeCode = 'H'
                     AND ct.deletedFlag = 0  -- not deleted
                     AND ct.effectiveToDate IS NULL   -- could add: or < SYSDATE + could also check effectiveFromDate
                 )
      AND ROWNUM <= 10000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END digTvOnlyNoBurn ;

PROCEDURE digMobOnlyRegradeNoBurn IS
  l_pool VARCHAR2(29) := 'DIGMOBONLYREGRADENOBURN' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyid , s.messotoken
     FROM (
   SELECT *
     FROM customers c 
    where c.entertainment = 0
      and c.cinema = 0
      and c.dtv=0
      and c.mobile = 1
      and c.completesports = 0
      and c.skyQBox = 0
      and c.skyHDBox = 0
      and c.talk=0
      and c.bband=0
      and c.fibre=0
      and c.bbandlite=0
      and c.bb12gb=0
      and c.bbultrafast=0
      and c.bbsuperfast=0
      and c.ultimatetvaddon=0
      and c.skysignature=0
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND c.countryCode = 'GBR'
      AND c.pool IS NULL  
      and c.portfolioid not in (select po.portfolioid from ccsowner.BSBPOWEROFATTORNEYROLE po where (EFFECTIVETODATE is null or EFFECTIVETODATE > sysdate))
      AND ROWNUM <= 10000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END digMobOnlyRegradeNoBurn ;

PROCEDURE fspMigrate IS
  l_pool VARCHAR2(29) := 'FSPMIGRATE' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber
     FROM (
   SELECT *
     FROM customers c
     WHERE c.entertainment = 1 -- was 0, but changed following discussion with Lee Morris
      AND c.dtv = 1
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND c.limaMigration NOT IN ( '2' , '3' ) ---no lima migration errors
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- No Customers in debt 09-Apr-2018 Andrew Fraser request Archana Burla.
      AND c.countryCode = 'GBR'
      AND c.pool IS NULL
      AND ROWNUM <= 75000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END fspMigrate ;

PROCEDURE netflix IS
  l_pool VARCHAR2(29) := 'NETFLIX' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.skycesa01token , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyid , s.skycesa01token , s.messotoken
     FROM (
   SELECT *
     FROM customers c
    WHERE c.netflix = 1
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- no outstanding balance
      AND c.countryCode = 'GBR'
      AND c.pool IS NULL
      AND ROWNUM <= 15000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END netflix ;

PROCEDURE fspUpgrade IS
  l_pool VARCHAR2(29) := 'FSPUPGRADE' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.skycesa01token , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyid , s.skycesa01token , s.messotoken
     FROM (
   SELECT *
     FROM customers c
        where c.entertainment = 0
        and c.SKYSIGNATURE =0 -- added for ticket NFTREL-18969 ( to exclude productid 15514)
        and c.dtv = 0         -- added for ticket NFTREL-18969 
        and c.countryCode='GBR' -- added for ticket NFTREL-18969 
   -- WHERE ( c.original = 1 OR c.variety = 1 )  -- original or variety > boxsets + limaCinema + 3LimaSportsChannels
      AND c.cinema = 0
      AND c.kids = 0
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND ( c.skyQBox = 1 OR c.skyHDBox = 1 )  -- to be upgraded to BoxSets, must have a box capable of downloading.
      AND c.limaMigration NOT IN ( '2' , '3' ) ---no lima migration errors,Alex Benetatos 06-Feb-2018 nftrel-11886. '
    -- Not '3' added on Ryan's request  on 03/08/18 to solve an issue with customer shops not loading
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- 02-Apr-2018 Andrew Fraser request Stuart Kerr.
      AND c.countryCode = 'GBR'
      AND c.pool IS NULL
      AND ROWNUM <= 200000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END fspUpgrade ;

PROCEDURE tvRecontractingNoBurn IS
  l_pool VARCHAR2(29) := 'TVRECONTRACTINGNOBURN' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.username , t.skycesa01token , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyid , s.username , s.skycesa01token , s.messotoken
     FROM (
   SELECT *
     FROM customers c
        where (c.accountnumber,c.partyid,c.username,c.skycesa01token,c.messotoken) in 
        (select /*+ parallel(cu, 12) parallel(ba, 12) parallel(po, 12) parallel(ro, 12) */
                distinct cu.accountnumber,cu.partyid,cu.username,cu.skycesa01token,cu.messotoken
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
            and cu.pool is null
         )
         and c.pool is null
      AND ROWNUM <= 200000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END tvRecontractingNoBurn ;

PROCEDURE tvRecontracting IS
  l_pool VARCHAR2(29) := 'TVRECONTRACTING' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.username , t.skycesa01token
        , t.messotoken , t.ssotoken
          )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyid , s.username , s.skycesa01token
        , s.messotoken , s.ssotoken
     FROM (
   SELECT *
     FROM customers c
        where (c.accountnumber,c.partyid,c.username,c.skycesa01token,c.messotoken) in 
        (select /*+ parallel(cu, 12) parallel(ba, 12) parallel(po, 12) parallel(ro, 12) */
                distinct cu.accountnumber,cu.partyid,cu.username,cu.skycesa01token,cu.messotoken
           from dataprov.customers cu, ccsowner.bsbbillingaccount ba,
                --ccsowner.bsbsubscriptionagreementitem sa,
                ccsowner.bsbportfoliooffer po,
                refdatamgr.bsboffer ro
          where ba.accountnumber = cu.accountnumber
            and cu.DTV =1   --check
            and cu.countryCode = 'GBR' --check
            -- AND ba.accountnumber = sa.agreementnumber
            and po.offerid = ro.ID
            AND ba.portfolioid = po.portfolioid
            AND po.OFFERID  IN ('85769','85770','85771','85772','85773','90005','85774','88910','88911','89224','89225','89471','89472','89477','89478','89479','89486')
            and po.status = 'ACT'
            AND po.applicationenddate > sysdate
            AND po.applicationenddate < (sysdate + 89) and cu.pool is null
            --and rownum < 50000
         )
         AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- no outstanding balance
         and c.portfolioid not in (select po.portfolioid from ccsowner.BSBPOWEROFATTORNEYROLE po where (EFFECTIVETODATE is null or EFFECTIVETODATE > sysdate))
         and  c.pool is null
      AND ROWNUM <= 50000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END tvRecontracting ;

PROCEDURE fspUpgradeLwsWeb IS
  l_pool VARCHAR2(29) := 'FSPUPGRADELWSWEB' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber
     FROM (
   SELECT *
     FROM customers c
     WHERE c.entertainment = 1
      AND c.kids = 0 -- This product will be added as part of the journey
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND c.limaMigration NOT IN ( '2' , '3' ) ---no lima migration errors
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- No Customers in debt 09-Apr-2018 Andrew Fraser request Archana Burla.
      AND c.countryCode = 'GBR'
      AND c.pool IS NULL
      AND ROWNUM <= 75000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END fspUpgradeLwsWeb ;

PROCEDURE fspDowngradeLwsWeb IS
  -- 08-Nov-2022 Stuart Kerr, add partyId.
  -- 03-Feb-2022 Dimitrios Koulialis NFTREL-21525. Previously had "limaMigration NOT IN ( '2' , '3' )", and a not in debt exclusion by Archana.
  l_pool VARCHAR2(29) := 'FSPDOWNGRADELWSWEB' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   INSERT /*+ append */ INTO fspDowngradeLwsWeb t ( t.accountNumber , t.partyId )
   SELECT c.accountNumber , c.partyId
     FROM customers c
    WHERE c.kids = 1
      AND c.skyQBox = 1
      AND c.accountNumber2 IS NULL
      AND c.countryCode = 'GBR'
      AND ROWNUM <= 1000000
   ;
   COMMIT ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountNumber , t.partyId )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountNumber , s.partyId
     FROM (
           SELECT d.accountNumber , d.partyId
             FROM fspDowngradeLwsWeb d
            ORDER BY dbms_random.value
          ) s
    WHERE ROWNUM <= 100000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   execute immediate 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END fspDowngradeLwsWeb ;

PROCEDURE fspDowngrade IS
  l_pool VARCHAR2(29) := 'FSPDOWNGRADE' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId , t.skycesa01token , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId , s.skycesa01token , s.messotoken
     FROM (
   SELECT *
     FROM customers c
    WHERE c.cinema = 1
       -- AND c.kids = 1 --TEMP disabled due to data issues identified late in release...
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND c.limaMigration NOT IN ( '2' , '3' ) ---no lima migration errors,Alex Benetatos 06-Feb-2018 nftrel-11886.'
      -- Not '3' added on Ryan's request  on 03/08/18 to solve an issue with customer shops not loading
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- 09-Apr-2018 Andrew Fraser request Archana Burla.
      AND c.countryCode = 'GBR'
      AND c.pool IS NULL
      AND ROWNUM <= 100000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END fspDowngrade ;

PROCEDURE fspAddPremium IS
  l_pool VARCHAR2(29) := 'FSPADDPREMIUM' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId , t.skycesa01token , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId , s.skycesa01token , s.messotoken
     FROM (
   SELECT c.accountnumber , c.partyId , c.skycesa01token , c.messotoken
     FROM customers c
    WHERE c.dtv = 1
      AND c.skyQBox = 1
      AND c.ultimateondemand = 0 --Callum Bulloch 02/10/19 Added column to the table to filter out customers with enabled ultimate on demand product from pool.
      AND c.cinema = 1  -- Alex Benetatos 16-Nov-2017: The script that is using this datapool is looking to remove the customers existing Sky Cinema subscription.
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND c.limaMigration NOT IN ( '2' , '3' ) ---no lima migration errors,Alex Benetatos 06-Feb-2018 nftrel-11886. '
    -- Not '3' added on Ryan's request  on 03/08/18 to solve an issue with customer shops not loading
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- 09-Apr-2018 Andrew Fraser request Archana Burla.
      AND c.countryCode = 'GBR'
      AND c.visitInProgress = 0  -- cbh06 - removes customers with in progress visits from pool 22/7/20
      AND c.pool IS NULL
      AND ROWNUM <= 2250
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END fspAddPremium ;

PROCEDURE fspSpecificSports IS
  l_pool VARCHAR2(29) := 'FSPSPECIFICSPORTS' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId , t.skycesa01token , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId , s.skycesa01token , s.messotoken
     FROM (
   SELECT *
     FROM customers c
    --WHERE ( premierleague = 1 OR cricket = 1 OR football = 1 ) --17/10/19 DS not needed for sports
      WHERE c.sports = 0
      AND c.entertainment = 1
      AND completesports = 0 
      AND ( c.skyQBox = 1 OR c.skyHDBox = 1 )  -- 17/10/19 DS to be upgraded to BoxSets, must have a box capable of downloading.
      -- REMOVED ON 07/05/2020 
      -- SPEAK TO ALEX HYSLOP OR SHANE VENTER BEFORE RE-ADDING THE FOLLOWING CLAUSE
      --AND c.limaMigration IN ('0','1','3') --17/10/19 DS
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
END fspSpecificSports ;

PROCEDURE fspEntertainmentNoPrem IS
  l_pool VARCHAR2(29) := 'FSPENTERTAINMENTNOPREM' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId , t.username , t.skycesa01token , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId , s.username , s.skycesa01token , s.messotoken
     FROM (
   SELECT *
     FROM customers c
    WHERE c.entertainment = 1
      AND c.cinema = 0
      AND c.completesports = 0
      AND c.mobile = 0  -- test training only
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND ( c.skyQBox = 1 OR c.skyHDBox = 1 )  -- to be upgraded to BoxSets, must have a box capable of downloading.
      -- REMOVED ON 07/05/2020 
      -- SPEAK TO ALEX HYSLOP OR SHANE VENTER BEFORE RE-ADDING THE FOLLOWING CLAUSE
      --AND c.limaMigration != 2  -- no lima migration errors, Alex Benetatos 06-Feb-2018 nftrel-11886
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 ) --DS Added for no debt
      AND c.countryCode = 'GBR'
      -- AND c.pool IS NULL  -- removed 22-Jul-2021 Andrew Fraser for Julian Correa, also removed i_flagCustomers=>TRUE at same time.
      AND ROWNUM <= 200 * 1000
    ORDER BY dbms_random.value
   ) s
   ;
   --25-Aug-2021 Andrew Fraser very little data, so temporarily trying noburn -- sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END fspEntertainmentNoPrem ;

PROCEDURE fspUpgradeNoSports IS
  l_pool VARCHAR2(29) := 'FSPUPGRADENOSPORTS' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId , t.skycesa01token , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId , s.skycesa01token , s.messotoken
     FROM (
   SELECT *
     FROM customers c
        where c.entertainment = 1
   --     and c.dtv = 1
   -- WHERE ( c.original = 1 OR c.variety = 1 )  -- original or variety > boxsets + limaCinema + 3LimaSportsChannels
      AND c.cinema = 0
      AND c.kids = 0
      AND c.sports = 0
      AND c.completesports = 0
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND ( c.skyQBox = 1 OR c.skyHDBox = 1 )  -- to be upgraded to BoxSets, must have a box capable of downloading.
      AND c.limaMigration NOT IN ( '2' , '3' ) ---no lima migration errors,Alex Benetatos 06-Feb-2018 nftrel-11886. '
    -- Not '3' added on Ryan's request  on 03/08/18 to solve an issue with customer shops not loading
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- 02-Apr-2018 Andrew Fraser request Stuart Kerr.
      AND c.countryCode = 'GBR'
      --AND c.pool IS NULL
      AND ROWNUM <= 5000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END fspUpgradeNoSports ;

PROCEDURE fspUpgradeNoSportsNoBurn IS
  l_pool VARCHAR2(29) := 'FSPUPGRADENOSPORTSNOBURN' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId , t.skycesa01token , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId , s.skycesa01token , s.messotoken
     FROM (
   SELECT *
     FROM customers c
    where c.entertainment = 1
      -- and c.dtv = 1
      -- WHERE ( c.original = 1 OR c.variety = 1 )  -- original or variety > boxsets + limaCinema + 3LimaSportsChannels
      AND c.cinema = 0
      AND c.kids = 0
      AND c.sports = 0
      AND c.completesports = 0
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND ( c.skyQBox = 1 OR c.skyHDBox = 1 )  -- to be upgraded to BoxSets, must have a box capable of downloading.
      --AND c.limaMigration NOT IN ( '2' , '3' ) ---no lima migration errors,Alex Benetatos 06-Feb-2018 nftrel-11886. '
      --Not '3' added on Ryan's request  on 03/08/18 to solve an issue with customer shops not loading
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- 02-Apr-2018 Andrew Fraser request Stuart Kerr.
      AND c.countryCode = 'GBR'
      AND c.pool IS NULL
      AND ROWNUM <= 20000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END fspUpgradeNoSportsNoBurn ;

PROCEDURE fspEntertainmentNoPrem_adobe IS
  l_pool VARCHAR2(29) := 'FSPENTERTAINMENTNOPREM_ADOBE' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.username , t.skycesa01token , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyid , s.username , s.skycesa01token , s.messotoken
     FROM (
   SELECT *
     FROM customers c
    WHERE c.entertainment = 1
      AND c.cinema = 0
      AND c.completesports = 0
      AND c.mobile = 0  -- test training only
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND ( c.skyQBox = 1 OR c.skyHDBox = 1 )  -- to be upgraded to BoxSets, must have a box capable of downloading.
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 ) --DS Added for no debt
      AND c.countryCode = 'GBR'
      AND c.pool IS NULL
      AND ROWNUM <= 100000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END fspEntertainmentNoPrem_adobe ;

PROCEDURE digitalDtvUpgradeNoQ IS
  l_pool VARCHAR2(29) := 'DIGITALDTVUPGRADENOQ' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.skycesa01token , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyid , s.skycesa01token , s.messotoken
     FROM (
   SELECT *
     FROM customers c
    WHERE c.dtv = 1
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND c.original = 1
      --AND c.boxsets = 0 -- removed these conditions to expand the size of this datapool(doesn't affect the working of the scripts) - Swaraj Thakur 12/03/2019
      --AND c.cinema = 0
      AND c.sports = 0
      AND premium = 0
      AND c.accountNumber IN (SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- no outstanding balance - Swaraj Thakur 25/09/2018
      -- Taken out 24/05 Shane
      -- AND c.accountNumber IN (SELECT ac.accountnumber from dataprov.act_uk_cust ac WHERE ac.postcode NOT in (select pa.postcode from dataprov.postcodes_paf pa where pa.res_type = 'SDU'))  -- added to remove MDU from this datapool - Swaraj Thakur 12/03/2019
      AND c.countryCode = 'GBR'
      AND c.skyhdbox = 1
      AND c.skyqbox = 0
      --AND c.limaMigration NOT IN ( '2' , '3' ) removed by shane 12112018
      AND c.pool IS NULL  -- optional
      -- customer MUST have an email address
      AND c.partyid IN (
          SELECT DISTINCT co.partyid
            FROM ccsowner.bsbcontactor co
            JOIN ccsowner.bsbcontactemail ce ON ce.contactorId = co.id
            JOIN ccsowner.bsbemail em ON em.id = ce.emailId
           WHERE ce.deletedFlag = 0
             AND ce.effectiveToDate IS NULL
          )
      AND ROWNUM <= 5000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END digitalDtvUpgradeNoQ ;

PROCEDURE digitalDtvUpgradeNoQNoBurn IS
  l_pool VARCHAR2(29) := 'DIGITALDTVUPGRADENOQNOBURN' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.skycesa01token , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyid , s.skycesa01token , s.messotoken
     FROM (
   SELECT *
     FROM customers c
    WHERE c.dtv = 1
      AND c.accountNumber2 IS NULL  -- only want customers with a single billing account.
      AND c.original = 1
      --AND c.boxsets = 0    -- removed these conditions to expand the size of this datapool(doesn't affect the working of the scripts) - Swaraj Thakur 12/03/2019
      --AND c.cinema = 0
      --AND c.sports = 0
      AND premium = 0
      AND c.accountNumber IN (SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )  -- no outstanding balance - Swaraj Thakur 25/09/2018
      AND c.accountNumber IN (SELECT ac.accountnumber from dataprov.act_uk_cust ac WHERE ac.postcode NOT in (select pa.postcode from dataprov.postcodes_paf pa where pa.res_type = 'SDU'))  -- added to remove MDU from this datapool - Swaraj Thakur 12/03/2019
      AND c.countryCode = 'GBR'
      AND c.skyhdbox = 1
      AND c.skyqbox = 0
     -- AND c.limaMigration NOT IN ( '2' , '3' ) - removed to increase pool size for Netflix hardware test - shane 12112018
      AND c.pool IS NULL  -- optional
      -- customer MUST have an email address
      AND c.partyid IN (
          SELECT DISTINCT co.partyid
            FROM ccsowner.bsbcontactor co
            JOIN ccsowner.bsbcontactemail ce ON ce.contactorId = co.id
            JOIN ccsowner.bsbemail em ON em.id = ce.emailId
           WHERE ce.deletedFlag = 0
             AND ce.effectiveToDate IS NULL
          )
      AND ROWNUM <= 50000
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;
END digitalDtvUpgradeNoQNoBurn ;

PROCEDURE digBBOnlyRegradeNoBurn IS
   l_pool VARCHAR2(29) := 'DIGBBONLYREGRADENOBURN' ;
   l_telout varchar2(32) ;
   l_magicno varchar2(3) := '909'; -- NFTREL-22387 changed from 902 to 909
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   --DBMS_OUTPUT.PUT_LINE('seqBefore Ended: ' || TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.messotoken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyid , s.messotoken
     FROM (
   SELECT /*+ parallel(c, 8) */ c.*
     FROM customers c
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
      AND c.portfolioid NOT IN (
          SELECT po.portfolioid
            FROM ccsowner.bsbpowerofattorneyrole po
           WHERE ( po.effectivetodate IS NULL OR po.effectivetodate > SYSDATE )
          ) 
      AND c.portfolioid IN ( SELECT i.portfolioid from bbregrade_portfolioid_tmp i )  -- from customers_pkg.buildSupportingTables
      AND c.emailAddress NOT LIKE 'noemail%' -- 13-Dec-2021 Andrew Fraser for Edwin Scariachan- valid email needed for soip. 
      AND c.partyId IN ( -- 14-Dec-2021 Andrew Fraser for Edwin Scariachan mobile number needed for soip. 
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
--Below to avoid sps app error "PIMM rule ID RULE_DO_NOT_ALLOW_SOIP_SALE_ON_INFLIGHT_ORDERS has resulted in a Do Not Allow outcome." 
      AND NOT EXISTS (
           SELECT NULL
           FROM ccsowner.bsbPortfolioProduct pp
           WHERE pp.portfolioId = c.portfolioId
           AND pp.status = 'PC' -- Pending Cancel
           )
-- Below to avoid sps app error "PIMM rule ID RULE_DO_NOT_ALLOW_POST_ACTIVE_CANCEL_WITH_IN_FLIGHT_VISIT has resulted in a Do Not Allow outcome." 
      AND NOT EXISTS (
           SELECT NULL
           FROM ccsowner.bsbVisitRequirement bvr
           JOIN ccsowner.bsbAddressUsageRole baur ON bvr.installAtionAddressRoleId = baur.id
           JOIN ccsowner.bsbServiceInstance bsi ON baur.serviceInstanceId = bsi.id
           WHERE bsi.portfolioId = c.portfolioId
           AND bvr.statusCode NOT IN ( 'CP' , 'CN' ) 
           )
      AND ROWNUM <= 10000
    ORDER BY dbms_random.value
   ) s
   ;
   --DBMS_OUTPUT.PUT_LINE('insert Ended: ' || TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_flagCustomers => TRUE ) ;
   --DBMS_OUTPUT.PUT_LINE('seqAfter Ended: ' || TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
   FOR r_telinfo IN (
      SELECT d.accountNumber
        FROM dprov_accounts_fast d
       WHERE d.pool_name = l_pool
   )
   LOOP
      -- 24-Jul-2021 Andrew Fraser added i_burn parameter because was burning up all the data for suffix 902.
      dynamic_data_pkg.update_cust_telno ( v_accountnumber => r_telinfo.accountNumber , v_suffix => l_magicno , v_telephone_out => l_telout , i_burn => FALSE ) ;
   END LOOP ;
   COMMIT ;
   --DBMS_OUTPUT.PUT_LINE('update_cust_telno Ended: ' || TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
   logger.write ( 'complete' ) ;
   
END digBBOnlyRegradeNoBurn ;

PROCEDURE act_cust_dtv_bb_talk IS
/*
10-Dec-2021 Andrew Fraser removed dependence on customers by getting messoToken direct from ccsowner tables, needs new join to pti.
18-Nov-2021 Andrew Fraser for Amit More removed billingAccountId.
16-Nov-2021 Andrew Fraser for Amit More add billingAccountId.
21/07/2016 Andrew Fraser exclude customers who have mobile at request Alex Benetatos Jira NFT-4102.
26/07/2016 Andrew Fraser bugfix to 21/07/2016, mobile exclusion delete needs to go at end of procedure.
04/06/2021 Andrew Fraser moved to dprov_accounts_fast + removed rownum 200k limiter cos being called high volume by Stuart Kerr Sirius tests.
*/
   l_pool VARCHAR2(29) := 'ACT_CUST_DTV_BB_TALK' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table ' || l_pool ;
   execute immediate 'alter session enable parallel dml' ;
   INSERT /*+ append parallel(8) */ INTO act_cust_dtv_bb_talk t ( t.accountNumber , t.partyId , t.messoToken )
   SELECT /*+ parallel(acus, 8) parallel(p, 8) */
          acus.accountNumber
        , p.partyId
        , 'T-MES-' || CASE WHEN sys_context ( 'userenv' , 'con_name' ) = 'CHORDO' THEN 'N01' ELSE 'N02' END
            || '-' || acus.accountNumber || '-' || p.partyId || '-' || MAX ( p.firstName ) || MAX ( p.familyName )
            || '-' || NVL ( MIN ( pti.identityId ) , 'NO-NSPROFILE' ) AS messoToken
     FROM act_cust_uk_subs acus
     JOIN ccsowner.person p ON acus.partyid = p.partyid
     LEFT OUTER JOIN ccsowner.bsbPartyToIdentity pti ON p.partyId = pti.partyId AND pti.identityType = 'NSPROFILEID'
    WHERE acus.dtv = 'AC'
      AND acus.talk = 'A'
      AND acus.bband = 'AC'
      AND p.birthDate IS NOT NULL
      AND EXISTS (
          SELECT /*+ full(a) parallel(a, 8) full(b) parallel(b, 8) */ NULL
            FROM ccsowner.bsbBillingAccount a
            JOIN ccsowner.bsbPortfolioProduct b ON a.portfolioId = b.portfolioId
           WHERE a.accountNumber = acus.accountNumber
             AND a.created < SYSDATE -365
          )
      AND NOT EXISTS (
          SELECT  /*+ full(a) parallel(a, 8) full(b) parallel(b, 8) full(bsi) parallel(bsi, 8) */ NULL
            FROM ccsowner.bsbBillingAccount a
            JOIN ccsowner.bsbPortfolioProduct b ON a.portfolioId = b.portfolioId
            JOIN ccsowner.bsbServiceInstance bsi ON b.serviceInstanceId = bsi.id
           WHERE a.accountNumber = acus.accountNumber
             AND bsi.serviceInstanceType IN ( 610 , 620 )
          )
      AND EXISTS (
          SELECT /*+ parallel(pos, 8) */ NULL
            FROM pos.lima_migration pos
           WHERE pos.mig_result IN ( 'COMPLETED' , 'isBundleMigratable-CURRENT BUNDLE NOT MIGRATABLE not in config' )
             AND pos.accountNumber || '' = acus.accountNumber || ''
             --and pos.accountNumber = acus.accountNumber
          )
      -- 10-Mar-2022 Andrew Fraser for Dimitrios Koulialis, exclude one specific type of product to see if related to test script failures. tpoc only.
      AND NOT EXISTS (
          SELECT /*+ full(a) parallel(a, 8) full(b) parallel(b, 8) */ NULL
            FROM ccsowner.bsbBillingAccount a
            JOIN ccsowner.bsbPortfolioProduct b ON a.portfolioId = b.portfolioId
           WHERE a.accountNumber = acus.accountNumber
             AND b.catalogueProductId = '15192'  -- "Sky Broadband Superfast (FTTC)(1)"
             AND b.status != 'CN'  -- ignore if cancelled already
          )
    GROUP BY acus.accountNumber , p.partyId
   ;
   COMMIT ;
   EXECUTE IMMEDIATE 'alter session disable parallel dml' ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.messotoken, t.partyid )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.messotoken, s.partyid
     FROM (
   SELECT d.accountNumber , d.messoToken , d.partyid
     FROM act_cust_dtv_bb_talk d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END act_cust_dtv_bb_talk ;

PROCEDURE act_trpl_ply_cust_iss_lt IS
/*
26/07/2016 Andrew Fraser bugfix to 21/07/2016, mobile exclusion delete needs to go at end of procedure.
21/07/2016 Andrew Fraser exclude customers who have mobile at request Alex Benetatos Jira NFT-4102.
*/
  l_pool VARCHAR2(29) := 'ACT_TRPL_PLY_CUST_ISS_LT' ;
BEGIN
   logger.write ( 'begin' ) ;
   execute immediate 'truncate table dataprov.act_trpl_ply_cust_iss_lt' ;
   EXECUTE IMMEDIATE 'ALTER SESSION enable parallel dml' ;
   INSERT /*+ APPEND PARALLEL(8) */ INTO act_trpl_ply_cust_iss_lt
    select * 
     from (select /*+ parallel(acus, 8) parallel(p, 8) parallel(t, 8) */ 
                   distinct acus.accountnumber,acus.partyid
            from act_cust_uk_subs acus, ccsowner.person p,  CCSOWNER.bsbcustomertenurecache t
          where DTV = 'AC'
            and TALK = 'A'
            and BBAND = 'AC'
            and acus.partyid = p.partyid
            and acus.partyid = t.customerpartyid
            and trunc(t.tenurestartdate) < add_months(trunc(sysdate), -168 )
            )
     where rownum <= 25000
   ;
   COMMIT ;
   EXECUTE IMMEDIATE 'ALTER SESSION disable parallel dml' ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyid
     FROM (
   SELECT *
     FROM act_trpl_ply_cust_iss_lt d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END act_trpl_ply_cust_iss_lt ;

PROCEDURE custForMonthlyVcCallbacks IS
  l_pool VARCHAR2(29) := 'CUSTFORMONTHLYVCCALLBACKS' ;
BEGIN
   logger.write ( 'begin' ) ;
   EXECUTE IMMEDIATE 'truncate table ' || l_pool ;
   Insert /* append */ into custForMonthlyVcCallbacks
   select * from (with box_ref as
                    (select /*+ full(bpp2) parallel(bpp2 16) */
                       bpp2.catalogueproductid box_id, serviceinstanceid
                 from ccsowner.bsbportfolioproduct bpp2
                where bpp2.catalogueproductid in ('13947', --Sky Q 1TB
                                                  '13948', --Sky Q 2TB UHD
                                                  '11090', --Sky+HD
                                                  '13425', --Sky+HD (A Grade)
                                                  '13646', --Sky+HD Self Install
                                                  '13653', --Sky+HD 2TB Self Install
                                                  '10136', --Sky+
                                                  '10116', --Sky+ (A Grade)
                                                  '10140', --STB (subsidised)
                                                  '10141', --STB (Unsubsidised)
                                                  '10142', --STB (A Grade)
                                                  '13787', --Sky+HDw
                                                  '13788', --Sky+HDw (A Grade)
                                                  '13791', --Sky+HDw Self Install
                                                  '13970', --Sky+HDw Self Install (A Grade)
                                                  '15491', --Sky Q 1TB UHD
                                                  '15595', --Sky Q 1TB Self Install
                                                  '15596', --Sky Q 1TB UHD Self Install
                                                  '15597') --Sky Q 2TB UHD Self Install
                    and bpp2.status = 'IN') -- cbh06 - Added to filter out non installed boxes causing errors. 20/07/21
                   select /*+ full(bce) parallel(bce 16) full(bce2) parallel(bce2 16) full(bpp) 
                              parallel(bpp 16) full(bba) parallel(bba 16) full(bsi) parallel(bsi 16)
                      pq_distribute(bce hash hash) pq_distribute(bce2 hash hash) 
                      pq_distribute(bsi hash hash) pq_distribute(bpp hash hash) 
                      pq_distribute(bba hash hash)*/
                   distinct bba.accountnumber, bce.cardnumber, bsi.cardsubscriberid, bce2.settopboxndsnumber, bsi.lastcallbackdate
                    from ccsowner.bsbcustomerproductelement  bce, ccsowner.bsbcustomerproductelement bce2,
                    ccsowner.bsbportfolioproduct        bpp, ccsowner.bsbbillingaccount          bba,
                    ccsowner.bsbserviceinstance         bsi, ccsowner.BSBCUSTOMERROLE            bcr,
                    box_ref
               where box_ref.serviceinstanceid = bsi.id
                 and bsi.installationproductelementid = bce2.id
                and bpp.id = bce.portfolioproductid
                and bpp.portfolioid = bba.portfolioid 
                and bcr.portfolioid = bba.portfolioid --cbh06 - Change made to filter out inactive subscribers. 21/07/21
                and bpp.serviceinstanceid = bsi.id
                and bpp.serviceinstanceid = box_ref.serviceinstanceid
                and bcr.customerstatuscode = 'CRACT' --cbh06 - Change made to filter out inactive subscribers. 21/07/21 
                and bpp.catalogueproductid = '10137'
                and bce.status = 'A'
                and bsi.lastcallbackdate < SYSDATE - 30
                and bce.cardnumber is not null)
   -- where rownum <= 100000 --cbh06 - Change made to increase size of pool for concurrent test runs 30/07/21
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.cardnumber , t.cardsubscriberid
        , t.settopboxndsnumber , t.lastcallbackdate )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.cardnumber , s.cardsubscriberid
        , s.settopboxndsnumber , s.lastcallbackdate
     FROM (
   SELECT *
     FROM custForMonthlyVcCallbacks d
    ORDER BY dbms_random.value
   ) s
    WHERE ROWNUM <= 600000
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   EXECUTE IMMEDIATE 'truncate table ' || l_pool ;
   logger.write ( 'complete' ) ;
END custForMonthlyVcCallbacks ;

PROCEDURE eocn_basket_ref IS
  l_pool VARCHAR2(29) := 'EOCN_BASKET_REF' ;
  intCnt integer ;
BEGIN
   logger.write ( 'begin' ) ;
   EXECUTE IMMEDIATE 'truncate table ' || l_pool ;
   insert into eocn_basket_ref
   select *
     from (SELECT al.accountnumber, rd.basketid, cus.messotoken
            FROM sal_owner.accounttarifftimeline@iss al, sal_owner.recommendbasket@iss rd,
                 customers cus
           WHERE al.id = rd.accounttarifftimelineid
             and al.accountnumber = cus.accountnumber
              --and trunc(RD.created) between trunc(sysdate)-12 and trunc(sysdate)-11
             and RD.created between to_date('27/02/2020 00:00:00', 'DD/MM/YYYY HH24:MI:SS')
                 and to_date('28/02/2020 00:00:00', 'DD/MM/YYYY HH24:MI:SS')
           --and RD.created) between :from_date and :to_date
          ORDER BY AL.CREATED DESC)
    where rownum < 100000
   ;
   -- this is a quick and dirty hack to get the right data. It needs revisited !!
   logger.write ( 'loop hack start' ) ;
   for c1_rec in ( select distinct accountnumber from eocn_basket_ref )
   loop
      select /*+ parallel(4) */ count(*)
        into intCnt
        from ccsowner.bsbbillingaccount bba, ccsowner.bsbportfolioproduct bpp, ccsowner.bsbsubscription bsub
       where bba.portfolioid = bpp.portfolioid
         and bpp.subscriptionid = bsub.id
         and bpp.catalogueproductid in ('12721','15195')
         and bsub.technologycode is not null
         and bsub.status = 'A'
         and bba.accountnumber = c1_rec.accountnumber
      ;
      if intCnt = 0 then
         delete from eocn_basket_ref where accountnumber = c1_rec.accountnumber ;
      end if ;
   end loop ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.basketId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.basketId , s.messoToken
     FROM (
   SELECT *
     FROM eocn_basket_ref d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END eocn_basket_ref ;

PROCEDURE eocn_no_debt IS
  l_pool VARCHAR2(29) := 'EOCN_NO_DEBT' ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.basketId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.basketId , s.messoToken
     FROM (
   SELECT *
     FROM eocn_basket_ref d
    WHERE d.accountNumber IN ( SELECT da.accountNumber FROM debt_amount da WHERE da.balance <= 0 )
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END eocn_no_debt ;

---------------------------------------------------------------------------------------------------------
-- eocn_no_debt_mob
-- 23/09/24 (RFA) : Changed query to do the sort before joining the debt_amount table as it was blowing up TEMP
---------------------------------------------------------------------------------------------------------
PROCEDURE eocn_no_debt_mob IS
  l_pool VARCHAR2(29) := 'EOCN_NO_DEBT_MOB' ;
BEGIN
   logger.write ( 'begin' ) ;
   EXECUTE IMMEDIATE 'truncate table ' || l_pool ;
   insert into eocn_no_debt_mob t ( t.accountnumber , t.partyId , t.basketId , t.messoToken )
   select s.accountnumber , s.partyId , s.basketId , s.messoToken
       from (
           with acct as 
                ( SELECT /*+ Parallel(cus,4) */
                           al.accountnumber
                         , cus.partyid
                         , rd.basketid
                         , cus.messotoken
                    FROM sal_owner.accounttarifftimeline@iss al
                    JOIN dataprov.eocn_mob_basket@iss rd on ( al.id = rd.accounttarifftimelineid )
                    JOIN dataprov.customers cus on ( al.accountnumber = cus.accountnumber )
                   WHERE cus.mobile = 1
                     AND rd.created > trunc(sysdate)-28
                   ORDER BY AL.CREATED DESC
                )
            select a.accountnumber
                  ,a.partyid
                  ,a.basketid
                  ,a.messotoken
            from acct a	  
            join debt_amount da on ( da.accountNumber = a.accountnumber )
            where da.balance <= 0
            ) s
    where rownum < 100000
   ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyId , t.basketId , t.messoToken )
   SELECT ROWNUM AS pool_seqno , l_pool AS pool_name , s.accountnumber , s.partyId , s.basketId , s.messoToken
     FROM (
   SELECT *
     FROM eocn_no_debt_mob d
    ORDER BY dbms_random.value
   ) s
   ;
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE ) ;
   logger.write ( 'complete' ) ;
END eocn_no_debt_mob ;

PROCEDURE digbbonlyregrade IS
   l_pool VARCHAR2(29) := 'DIGBBONLYREGRADE' ;
   l_telout varchar2(32) ;
   l_magicno varchar2(3) := '909'; -- NFTREL-22387 changed from 902 to 909
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.accountnumber , t.partyid , t.messotoken, t.firstname, t.familyname, t.emailaddress, t.code )
SELECT ROWNUM AS pool_seqno, l_pool AS pool_name , s.accountnumber , s.partyid , s.messotoken, s.firstname, s.familyname, s.emailaddress, s.code
     FROM (
   SELECT /*+ parallel(c, 8) parallel(v,8) */ c.*, v.code
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
      AND c.portfolioid NOT IN (
            SELECT po.portfolioid
            FROM ccsowner.bsbpowerofattorneyrole po
            WHERE ( po.effectivetodate IS NULL OR po.effectivetodate > SYSDATE )
            ) 
      AND c.portfolioid IN ( SELECT i.portfolioid from bbregrade_portfolioid_tmp i )  -- from customers_pkg.buildSupportingTables
      AND c.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )
      AND c.emailAddress NOT LIKE 'noemail%' -- 13-Dec-2021 Andrew Fraser for Edwin Scariachan- valid email needed for soip. 
      AND c.partyId IN ( -- 14-Dec-2021 Andrew Fraser for Edwin Scariachan mobile number needed for soip. -- 65420  Rows
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
--Below to avoid sps app error "PIMM rule ID RULE_DO_NOT_ALLOW_SOIP_SALE_ON_INFLIGHT_ORDERS has resulted in a Do Not Allow outcome." 
      AND NOT EXISTS (
           SELECT NULL
           FROM ccsowner.bsbPortfolioProduct pp
           WHERE pp.portfolioId = c.portfolioId
           AND pp.status = 'PC' -- Pending Cancel
           )
-- Below to avoid sps app error "PIMM rule ID RULE_DO_NOT_ALLOW_POST_ACTIVE_CANCEL_WITH_IN_FLIGHT_VISIT has resulted in a Do Not Allow outcome." 
      AND NOT EXISTS (
           SELECT NULL
           FROM ccsowner.bsbVisitRequirement bvr
           JOIN ccsowner.bsbAddressUsageRole baur ON bvr.installAtionAddressRoleId = baur.id
           JOIN ccsowner.bsbServiceInstance bsi ON baur.serviceInstanceId = bsi.id
           WHERE bsi.portfolioId = c.portfolioId
           AND bvr.statusCode NOT IN ( 'CP' , 'CN' ) 
           )
      AND ROWNUM <= 10000
    ORDER BY dbms_random.value
   ) s; 
   
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT, i_burn => TRUE ) ;
   FOR r_telinfo IN (
      SELECT d.accountNumber
        FROM dprov_accounts_fast d
       WHERE d.pool_name = l_pool
   )
   LOOP
      -- 24-Jul-2021 Andrew Fraser added i_burn parameter because was burning up all the data for suffix 902.
      dynamic_data_pkg.update_cust_telno ( v_accountnumber => r_telinfo.accountNumber , v_suffix => l_magicno , v_telephone_out => l_telout , i_burn => TRUE ) ;
   END LOOP ;
   COMMIT ;
   logger.write ( 'complete' ) ;
END digbbonlyregrade ;

END data_prep_02 ;
/
