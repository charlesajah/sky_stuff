create or replace PACKAGE          data_prep_static_oms IS
--#############################################################
--Created: 22/08/2014                                         #
--Modified:
-- 10/01/2018 Andrew Fraser added procedure portIn - called from chordiant mobile_cancellations pool
-- 05/09/2017 Andrew Fraser removed unused v_env_refresh and most v_schema_name
-- 12/07/2016 Andrew Fraser oms_engineer_visit + audit commented out - not currently required.
-- 06/05/2016 Andrew Fraser oms_engineer_visit_audit added
--#############################################################
   PROCEDURE populate_orders ;
   PROCEDURE ordermanagementorders ;
   PROCEDURE gco ;
   PROCEDURE findorders ;
   PROCEDURE updateorderdata ;
   PROCEDURE mobile_orders ;
   PROCEDURE portIn ;  -- called from chordiant mobile_cancellations pool
   PROCEDURE esim_orders;
END data_prep_static_oms ;
/

create or replace PACKAGE BODY data_prep_static_oms IS

PROCEDURE populate_orders IS
--#############################################################
--Created: 22/08/2014                                         #
--Modified:                                                   #
--Last modification: modified
--Last modified by: Andrew Fraser
-- 05-May-2018 Andrew Fraser source_system commented out because all new records have source_system='00001'.
-- 05-May-2018 Andrew Fraser removed v_schema_name parameter to simplify sql code, use hard coded 'oh' schema name.
-- 22-Nov-2016 Andrew Fraser removed v_env_refresh condition to allow production orders to be used post environment refresh. Tested with Shane Venter.
-- 21-Oct-2016 regressed 11-Oct-2016 changed request Shane Venter, was not working for e.g. 622854073427 oh2-f16c991c-639f-46e5-94c4-53b3354c0af8
-- 11-Oct-2016 Andrew Fraser removed v_env_refresh condition to allow production orders to be used post environment refresh. Tested with Shane Venter.
--         AND TRUNC(a.lastupdate) > ''' || v_env_refresh || '''
-- 21-Jul-2016 Andrew Fraser added 'AND a.created > SYSDATE - 30' request Alex Benetatos Jira NFT-4113.
--Notes:                                                      #
--                                                            #
--#############################################################
BEGIN
   execute immediate 'truncate table dataprov.populate_orders' ;
   -- 21-Jul-2016 Andrew Fraser added 'AND a.created > SYSDATE - 30' request Alex Benetatos Jira NFT-4113.
   INSERT /*+ APPEND*/ INTO dataprov.populate_orders ( id , accountnumber , profileid , productid , orderid , sourcesystem )
   SELECT a.id
        , EXTRACTVALUE ( orderpayload , '/customerOrder/accountNumber' ) AS accountnumber
        , EXTRACTVALUE ( orderpayload , '/customerOrder/externalData/profileId' ) AS profileid
        , EXTRACTVALUE ( orderpayload , '/customerOrder/productOrders/productOrder[1]/productId' ) AS productId
        , a.source_system_order_id AS orderId
        , a.source_system AS sourcesystem
     FROM oh.bsbcusord a
    WHERE XMLEXISTS ( '$payload/customerOrder/productOrders/productOrder[orderAction=$action]'
                    PASSING orderpayload AS "payload",
                    'ADD' AS "action" )
      AND XMLEXISTS ( '$payload/customerOrder/resourceOrders/resourceOrder[state=$state]'
                    PASSING orderpayload AS "payload",
                    'IN_PROGRESS' AS "state" )
      AND a.created > SYSDATE - 30
    --AND a.source_system IN ( 'CRM' , 'STB' , 'Acetrax' )  -- 05-May-2018 AF commented out because all new records have source_system='00001'.
   ;
   COMMIT ;
   dbms_stats.gather_table_stats ( tabname => 'POPULATE_ORDERS' , ownname => USER ) ;
END populate_orders ;

PROCEDURE gco IS
--#############################################################
-- Used by Focus http://focusjenkins:8080/job/FOCUS_PTT_JTF_LWS_ORS/
--Created: 22/08/2014                                         #
--Modified:                                                   #
--   05-May-2021 AF removed rownum<=60k limit because high volumes 250k records used in 8 hour test
--   04-May-2018 Andrew Fraser exclude customers with 000s of orders, getting too high a mix of those into datapool cos of the SkyStore restriction, causing slower sql times than in prod load.
--   14-May-2018 Andrew Fraser Commented out date+writeVersion condition because too little data in pool, Nic Patte NFTREL-13191
--   15-Feb-2018 Andrew Fraser Exclude negative production appWriteVersion, request Amit More.
--   26-Sep-2017 Andrew Fraser 
--   04-Sep-2017 Andrew Fraser changed from xml Acetrax to relational skyStore.
--#############################################################
BEGIN
   EXECUTE IMMEDIATE 'truncate table dataprov.gco' ;
   INSERT /*+ APPEND */ INTO dataprov.gco
   SELECT DISTINCT co.customerAccountNumber AS accountNumber
     FROM oh.customerOrders co
     JOIN oh.resourceOrders ro ON co.customerOrderId = ro.customerOrderId
     JOIN oh.resourceOrderStatusCodes rosc on rosc.roStatusCodeId = ro.roStatusCodeId
    WHERE co.sourceSystem = 'SKYSTORE'
      AND rosc.roStatusCode = 'IN_PROGRESS'
   ;
   COMMIT ;
   -- below condition added 04-May-2021 - gco pool had too high a mix of skystore customers with 000s of orders
   -- removed at request of Richard Imlach 06/10/2021
   --DELETE FROM dataprov.gco g
   -- WHERE 30 <= ( SELECT COUNT(*) FROM oh.customerOrders co2 WHERE co2.customerAccountNumber = g.accountNumber )
   --;
   --COMMIT ;
END gco ;

PROCEDURE findorders IS
--#############################################################
--Created: 22/08/2014                                         #
--Modified: 11-Oct-2016
--Modified: 23-Mar-2017
--Last modification: modification.
--Last modified by: Andrew Fraser
--   05-May-2021 Andrew Fraser removed ROWNUM limit because high volumes 800k used in 8 hour test.
--   04-May-2018 Andrew Fraser exclude customers with 000s of orders, getting too high a mix of those into gco datapool cos of the SkyStore restriction, causing slower sql times than in prod load - may not be needed in findOrders pool also, to be reviewed.
--  12-May-2017 Andrew Fraser phase4 re-design est added temporarily, in place of sns.
--  23-Mar-2017 Andrew Fraser phase3 re-design sns added temporarily.
--  09-Nov-2016 Andrew Fraser increased rownum limiters from 25k to 100k request Andrew Reid to avoid caching in high TPS tests.
--  11-Oct-2016 Andrew Fraser removed STB/Acetrax condition to allow production orders to be used post environment refresh. Tested with Shane Venter.
--      WHERE p.sourceSystem IN ( ''STB'' , ''Acetrax'' )
--  15-Aug-2016 Andrew Fraser expanded to include equal number of relational orders.
--  19-Sep-2017 Andrew Fraser changed proportions to match production: 20% document + 80% relational.
--Notes:                                                      #
--   This lists completed (aka 'historical') orders as well as in-progress orders.
--   So is used for test which view orders in stan.
--   Tests which modify orders should instead use orderManagementOrders.
--#############################################################
BEGIN
   EXECUTE IMMEDIATE 'truncate table dataprov.findorders' ;
   INSERT /*+ APPEND */ INTO dataprov.findOrders f ( f.accountNumber , f.sourceSystem )
   SELECT DISTINCT p.accountNumber , p.sourceSystem
     FROM dataprov.populate_orders p
   ;
   COMMIT ; 
   INSERT /*+ APPEND */ INTO dataprov.findOrders f ( f.accountNumber , f.sourceSystem )
   SELECT DISTINCT r.customerAccountNumber , 'REL' AS sourceSystem
     FROM oh.customerOrders r
    --WHERE ( r.lastUpdate > SYSDATE - 30 OR r.created > SYSDATE - 30 )
    -- changed to 120 days by ah for OMS-D testing 06/10/2021
    WHERE ( r.lastUpdate > SYSDATE - 120 OR r.created > SYSDATE - 120 )
   ;
   COMMIT ;
   -- below condition added 04-May-2021 - gco pool had too high a mix of skystore customers with 000s of orders
   -- removed at request of Richard Imlach 06/10/2021
   --DELETE FROM dataprov.findOrders f
   -- WHERE 30 <= ( SELECT COUNT(*) FROM oh.customerOrders co2 WHERE co2.customerAccountNumber = f.accountNumber )
   --;
   --COMMIT ;
   dbms_stats.gather_table_stats ( tabname => 'FINDORDERS' , ownname => 'DATAPROV' ) ;
END findorders ;

PROCEDURE orderManagementOrders IS
--#############################################################
--Created: 22/08/2014                                         #
--Modified:                                                   #
--Last modification:
--   05-May-2021 Andrew Fraser 
--   02-Nov-2017 Andrew Fraser changed dependency from popoulate_orders to findOrders, request Amit More. 
--                                                            #
--Notes:                                                      #
--   This is same as findOrders except only order that are in progress (others are classed as 'historical').
--   This is used by tests that modify an order in stan.
--   Tests which only view orders in stan can use findOrders.
--   This pool has a dependency on findOrders - should be run after that.
--#############################################################
BEGIN
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.orderManagementOrders'
   ;
   INSERT /*+ APPEND */ INTO dataprov.orderManagementOrders o ( o.accountNumber )
   SELECT DISTINCT f.accountNumber
     FROM dataprov.findOrders f
     JOIN oh.customerOrders co ON co.customerAccountNumber = f.accountNumber
     JOIN oh.resourceOrders ro ON ro.customerOrderId = co.customerOrderId
     JOIN oh.resourceOrderStatusCodes rosc ON rosc.roStatusCodeId = ro.roStatusCodeId
    WHERE rosc.roStatusCode IN ( 'DELAYED' , 'IN_PROGRESS' , 'PENDING' )
      AND ROWNUM <= 500000
   ;
   COMMIT
   ;
END orderManagementOrders ;

PROCEDURE updateOrderData IS
BEGIN
   execute immediate 'TRUNCATE TABLE updateOrderData' ;
   INSERT /*+ append */ INTO updateOrderData t ( t.id , t.accountNumber , t.profileId , t.productId , t.orderId )
   SELECT s.id , s.accountNumber , s.profileId , s.productId , s.orderId
     FROM dataprov.populate_orders s
    WHERE s.sourceSystem = 'Acetrax'
      AND ROWNUM < 110001
   ;
   COMMIT ;
   dbms_stats.gather_table_stats ( tabName => 'updateOrderData' , ownName => USER ) ;
end updateOrderData ;

PROCEDURE mobile_orders IS
--#############################################################
--Created: 11/07/2016                                         #
--Modified:                                                   #
--   18-Jan-2023 Dimitrios changed to have all SIM card products to get more data (constantly running out of data before). Also group by to pick just one instanceId per order.
--   09-Oct-2017 Andrew Fraser added 3 more products.
--   19/12/2019 Alex Hyslop                                   #
-- Removed following product IDs:                             #
--    where $i/productId = 15230                              #
--       or $i/productId = 14954                              #
--       or $i/productId = 15350                              #
--       or $i/productId = 15351                              #
--       or $i/productId = 15352                              #
--       or $i/productId = 15353                              #
--Notes:                                                      #
--   List of products from chordiant:                         #
--   SELECT * FROM refdatamgr.bsbCatalogueProduct             #
--    WHERE subscriptionType = '10' ORDER BY 1 ;              #
--#############################################################
BEGIN
   EXECUTE IMMEDIATE 'truncate table dataprov.mobile_orders';
   INSERT /*+ append */ INTO dataprov.mobile_orders ( id , accountNumber , instanceId )
   SELECT a.id
        , b.accountNumber
        , MIN ( c.instanceId )
     FROM oh.bsbcusord a
        , XMLTABLE ( '/customerOrder' PASSING a.orderPayload COLUMNS accountNumber VARCHAR2(28) PATH 'accountNumber' ) b
        , XMLTABLE ( 'for $i in /customerOrder/productOrders/productOrder
                      where $i/displayName = "100MB"
                         or $i/displayName = "10GB"
                         or $i/displayName = "12GB"
                         or $i/displayName = "14GB"
                         or $i/displayName = "15GB"
                         or $i/displayName = "1GB"
                         or $i/displayName = "200GB"
                         or $i/displayName = "20GB"
                         or $i/displayName = "25GB"
                         or $i/displayName = "2GB"
                         or $i/displayName = "30GB"
                         or $i/displayName = "3GB"
                         or $i/displayName = "40GB"
                         or $i/displayName = "4GB"
                         or $i/displayName = "500MB"
                         or $i/displayName = "50GB"
                         or $i/displayName = "5GB"
                         or $i/displayName = "60GB"
                         or $i/displayName = "6GB"
                         or $i/displayName = "70GB"
                         or $i/displayName = "7GB"
                         or $i/displayName = "8GB"
                         or $i/displayName = "9GB"
                      return $i' PASSING a.orderPayload COLUMNS instanceId VARCHAR2(40) PATH 'service' ) c 
    WHERE XMLEXISTS ( '$payload/customerOrder/services/service[type=$type]' PASSING a.orderPayload as "payload" , 'MOBILE_SERVICE' AS "type" )
      AND XMLEXISTS ( '$payload/customerOrder/resourceOrders/resourceOrder[state=$state]' PASSING a.orderPayload AS "payload" , 'PENDING' AS "state" )
      AND XMLEXISTS ( '$payload/customerOrder/resourceOrders/resourceOrder[subState=$subState]' PASSING a.orderPayload AS "payload" , 'AWAITING_ACTIVATION' AS "subState" )
      AND XMLEXISTS ( '$payload/customerOrder/resourceOrders/resourceOrder/resource[name=$name]' PASSING a.orderPayload AS "payload" , 'SIM_CARD' AS "name" )
      AND a.created > TO_DATE ( '01/04/2017 00:01' , 'DD/MM/YYYY HH24:MI' )
      AND ROWNUM <= 100000
    GROUP BY a.id , b.accountNumber
   ;
   COMMIT ;
END mobile_orders ;

PROCEDURE portIn IS
--#############################################################
--Created: 10/01/2018                                         #
--Modified:                                                   #
--   10-Jan-2018 Andrew Fraser original version.
--                                                            #
--Notes:                                                      #
--   Called from chordiant mobile_cancellations pool.
--   Request Liam Fleming "Filter out customers with a Port In order in progress from the mobilecancellations MOBILE_CANCELLATIONS pool. The In-Progress order prevents cancelation. The script that uses this runs 72 times per hour."
--#############################################################
BEGIN
   -- Due to unpredicatable performance with xml access paths, for safety probe xml document order management 1 customer at a time instead of using single query with three xmlexists clauses.
   FOR r1 IN (
      SELECT DISTINCT d.accountNumber FROM dataprov.mobile_cancellations d
   )
   LOOP
      FOR r2 IN (
         -- probe xml document order management 1 customer at a time, to be sure xml access path will be be customer account number.
         SELECT s.actionSubType , ro.state
           FROM oh.bsbCusOrd o
              , XMLTABLE ( ' for $r in /customerOrder/resourceOrders/resourceOrder return $r ' PASSING o.orderPayload COLUMNS state VARCHAR2(50) PATH 'state' ) AS ro
              , XMLTABLE ( ' for $r in /customerOrder/services/service return $r ' PASSING o.orderPayload COLUMNS actionSubType VARCHAR2(50) PATH 'actionSubType' ) AS s
          WHERE XMLEXISTS ( '$payload/customerOrder[accountNumber=$accountNumber]' PASSING o.orderPayload AS "payload" , r1.accountNumber AS "accountNumber" )
      )
      LOOP  -- loop through each order for that customer.
         IF r2.actionSubType = 'PORT_IN' AND r2.state = 'IN_PROGRESS'
         THEN
            UPDATE dataprov.mobile_cancellations d SET d.portIn_inProgress = 1 WHERE d.accountNumber = r1.accountNumber ;
         END IF ;
      END LOOP ;  -- r2
   END LOOP ;  -- r1
   COMMIT ;
END portIn ;


PROCEDURE esim_orders IS
--#############################################################
--Created: 26/08/2024                                         #
--Modified:                                                   #
--                                                            #
--Notes:                                                      #
--   List of products from chordiant:                         #
--   SELECT * FROM refdatamgr.bsbCatalogueProduct             #
--    WHERE subscriptionType = '10' ORDER BY 1 ;              #
--#############################################################
BEGIN
   EXECUTE IMMEDIATE 'truncate table dataprov.esim_orders';
   INSERT /*+ append */ INTO dataprov.esim_orders ( id , accountNumber , instanceId )
select  a.id
        , b.accountNumber, c.instanceId
     FROM oh.bsbcusord a
        , XMLTABLE ( '/customerOrder' PASSING a.orderPayload COLUMNS accountNumber VARCHAR2(28) PATH 'accountNumber' ) b
        , XMLTABLE ( 'for $i in /customerOrder/productOrders/productOrder
                      where $i/displayName = "Sky Mobile eSIM"
                      return $i' PASSING a.orderPayload COLUMNS instanceId VARCHAR2(40) PATH 'service' ) c
    WHERE XMLEXISTS ( '$payload/customerOrder/resourceOrders/resourceOrder/resource[displayName=$displayName]' PASSING a.orderPayload AS "payload" , 'Sky Mobile eSIM' AS "displayName" )
AND XMLEXISTS ( '$payload/customerOrder/resourceOrders/resourceOrder[subState=$subState]' PASSING a.orderPayload AS "payload" , 'AWAITING_ACTIVATION' AS "subState" )
    AND  rownum<100000;
   COMMIT ;
END esim_orders ;


END data_prep_static_oms ;
/
