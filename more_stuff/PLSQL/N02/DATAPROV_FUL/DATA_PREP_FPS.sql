CREATE OR REPLACE PACKAGE data_prep_fps AS
    PROCEDURE soipDispatchedProducts ;
    PROCEDURE soipBbtPending ;
    PROCEDURE soipDispatchedProductsBen ;
    PROCEDURE soipGen2DispatchedProducts ;
    PROCEDURE soipGlassAirDispatched;
END data_prep_fps ;
/


CREATE OR REPLACE PACKAGE BODY data_prep_fps AS
PROCEDURE soipDispatchedProducts IS
/*
|| Called from chord dataprov.data_prep_soip.soipDispatchedProducts
|| Workaround for "ORA-22992: cannot use LOB locators selected from remote tables"
|| Staging table hosted in fps for performance during the merge statement.
*/
BEGIN
   MERGE INTO soipDispatchedProducts t USING (
      SELECT JSON_VALUE ( co.commercialOrder_data , '$.accounts[0].accountNumber' ) AS accountNumber
           , MAX ( act.id ) AS fulfilmentReferenceId
           , MAX ( co.created ) AS created
        FROM rful.action act
        JOIN rful.commercialOrder co ON JSON_VALUE ( act.action_data , '$.commercialOrderId' ) = co.id
       WHERE JSON_VALUE ( act.action_data , '$.actionType' ) = 'DELIVERY_PRODUCTS'
         AND JSON_VALUE ( act.action_data , '$.state' ) = 'SENT_TO_PROVIDER'
         AND act.created > TRUNC ( SYSDATE ) - 100  -- 29-Mar-2023 Andrew Fraser to improve performance by partition pruning
       GROUP BY JSON_VALUE ( co.commercialOrder_data , '$.accounts[0].accountNumber' )
   ) s ON ( s.accountNumber = t.accountNumber )
   WHEN MATCHED THEN UPDATE SET t.fulfilmentReferenceId = s.fulfilmentReferenceId , t.created = s.created
   WHERE NVL ( t.fulfilmentReferenceId , 'x' ) != NVL ( s.fulfilmentReferenceId , 'x' ) OR NVL ( t.created , SYSDATE ) != NVL ( s.created , SYSDATE )
   ;
   commit;
END soipDispatchedProducts ;


PROCEDURE soipBbtPending IS
  -- Edward Falconer NFTREL-21436
  -- 20/12/23 (RFA) - Added HINT to enforce execution plan on query, which did not work any other way
  -- 09/01/24 (RFA) - Replacing query with new improved version
BEGIN
   execute immediate 'truncate table soipBbtPending reuse storage' ;

   INSERT /*+ append */ INTO soipBbtPending t ( t.accountNumber , t.partyId , t.created , t.fulfilmentReferenceId )
   SELECT /*+ full(a) parallel(a,8) */ jt.accountNumber
         ,a.partyid
         ,a.created
         ,a.id AS fulfilmentReferenceId
    FROM   rful.action a
          ,rful.commercialOrder co
          ,JSON_TABLE(commercialorder_data,
                  '$'
                   COLUMNS(id VARCHAR2(50) PATH '$.id',
                           NESTED PATH '$.accounts[*]'
                            COLUMNS(TYPE VARCHAR2(10) PATH '$.type',
                                    accountNumber VARCHAR2(20) PATH '$.accountNumber'))) AS jt
   WHERE  co.id = a.commercialorderid
   AND    jt.id = co.id
   AND    a.actiontype = 'SIM2_VOICE_AND_DATA'
   AND    a.state = 'PENDING'
   AND    jt.type = 'CORE'
   AND    jt.accountNumber IS NOT NULL
   ;
   
   COMMIT ;
END soipBbtPending ;


PROCEDURE soipDispatchedProductsBen IS
/*
|| Called from chord dataprov.data_prep_soip.soipDispatchedProductsBen
|| Workaround for "ORA-22992: cannot use LOB locators selected from remote tables"
|| Staging table hosted in fps for performance during the merge statement.
*/
BEGIN
   MERGE INTO soipDispatchedProductsBen t USING (
      SELECT JSON_VALUE ( co.commercialOrder_data , '$.accounts[0].accountNumber' ) AS accountNumber
           , MAX ( act.id ) AS fulfilmentReferenceId
           , MAX ( co.created ) AS created
        FROM rful.action act
        JOIN rful.commercialOrder co ON JSON_VALUE ( act.action_data , '$.commercialOrderId' ) = co.id
       WHERE JSON_VALUE ( act.action_data , '$.actionType' ) = 'DELIVERY_PRODUCTS'
         AND JSON_VALUE ( act.action_data , '$.state' ) = 'SENT_TO_PROVIDER'
       GROUP BY JSON_VALUE ( co.commercialOrder_data , '$.accounts[0].accountNumber' )
   ) s ON ( s.accountNumber = t.accountNumber )
   WHEN MATCHED THEN UPDATE SET t.fulfilmentReferenceId = s.fulfilmentReferenceId , t.created = s.created
   WHERE NVL ( t.fulfilmentReferenceId , 'x' ) != NVL ( s.fulfilmentReferenceId , 'x' ) OR NVL ( t.created , SYSDATE ) != NVL ( s.created , SYSDATE )
   ;
END soipDispatchedProductsBen ;


PROCEDURE soipGen2DispatchedProducts IS
/*
|| Called from chord dataprov.data_prep_soip.soipGen2DispatchedProducts
|| Workaround for "ORA-22992: cannot use LOB locators selected from remote tables"
|| Staging table hosted in fps for performance during the merge statement.
*/
BEGIN
   MERGE INTO soipGen2DispatchedProducts t USING (
      SELECT JSON_VALUE ( co.commercialOrder_data , '$.accounts[0].accountNumber' ) AS accountNumber
           , MAX ( act.id ) AS fulfilmentReferenceId
           , MAX ( co.created ) AS created
           , MAX ( act.actionType ) AS action_Type
           , MAX ( act.state ) AS state
        FROM rful.action act
        JOIN rful.commercialOrder co ON act.commercialOrderId = co.id
       WHERE act.created > TRUNC ( SYSDATE ) - 100  
         AND act.actionType = 'DELIVERY_PRODUCTS'
         AND act.state = 'SENT_TO_PROVIDER'
       GROUP BY JSON_VALUE ( co.commercialOrder_data , '$.accounts[0].accountNumber' )
   ) s ON ( s.accountNumber = t.accountNumber )
   WHEN MATCHED THEN UPDATE 
        SET  t.fulfilmentReferenceId = s.fulfilmentReferenceId 
           , t.created = s.created
           , t.action_type = s.action_Type
           , t.state = s.state
   WHERE NVL ( t.fulfilmentReferenceId , 'x' ) != NVL ( s.fulfilmentReferenceId , 'x' ) OR NVL ( t.created , SYSDATE ) != NVL ( s.created , SYSDATE )
   ;
   commit;
END soipGen2DispatchedProducts ;


PROCEDURE soipGlassAirDispatched IS
/*
|| Called from chord dataprov.data_prep_soip.soipGlassAirDispatched
|| Workaround for "ORA-22992: cannot use LOB locators selected from remote tables"
|| Staging table hosted in fps for performance during the merge statement.
*/
BEGIN
   MERGE INTO soipGlassAirDispatched t USING (
      SELECT JSON_VALUE ( co.commercialOrder_data , '$.accounts[0].accountNumber' ) AS accountNumber
           , MAX ( act.id ) AS fulfilmentReferenceId
           , MAX ( co.created ) AS created
           , MAX ( act.actionType ) AS action_Type
           , MAX ( act.state ) AS state
        FROM rful.action act
        JOIN rful.commercialOrder co ON act.commercialOrderId = co.id
       WHERE act.created > TRUNC ( SYSDATE ) - 100  
         AND act.actionType = 'DELIVERY_PRODUCTS'
         AND act.state = 'SENT_TO_PROVIDER'
       GROUP BY JSON_VALUE ( co.commercialOrder_data , '$.accounts[0].accountNumber' )
   ) s ON ( s.accountNumber = t.accountNumber )
   WHEN MATCHED THEN UPDATE 
        SET  t.fulfilmentReferenceId = s.fulfilmentReferenceId 
           , t.created = s.created
           , t.action_type = s.action_Type
           , t.state = s.state
   WHERE NVL ( t.fulfilmentReferenceId , 'x' ) != NVL ( s.fulfilmentReferenceId , 'x' ) OR NVL ( t.created , SYSDATE ) != NVL ( s.created , SYSDATE )
   ;
   commit;
END soipGlassAirDispatched ;


END data_prep_fps ;
/
