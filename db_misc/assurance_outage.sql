set serveroutput on verify off pages 9999 lines 140 document off
/*
Toggles assurance system wide soip outage on/off.
01-Mar-2022 Andrew Fraser for Humza Ismail, remove this condition from update statement - "AND t.description LIKE 'Row Insert for SOIP MSO%'" so will close all outages.
25-Nov-2021 Andrew Fraser for Edward Falconer https://cbsjira.bskyb.com/browse/NFTREL-21391
*/
BEGIN
   IF LOWER ( TRIM ( '&&1' ) ) = 'create'
   THEN
      dbms_output.put_line ( 'Creating new outage notification with an insert ...' ) ;
      INSERT INTO snsOwner.bsbSoipOutageNotification t ( t.id , t.incidentTaskNumber , t.description , t.startDate , t.endDate
           , t.created , t.createdBy , t.lastUpdate , t.updatedBy , t.schedule , t.divertToIvr , t.lossOfService )
      SELECT SYS_GUID() AS id
           , 'INC' || TO_CHAR ( SYSDATE , 'YYYYMMDDHH24MISS' ) AS incidentTaskNumber
           , 'Row Insert for SOIP MSO - ' || TO_CHAR ( SYSDATE , 'DD/MM/YYYY HH24:MI:SS' ) AS description
           , SYSDATE AS startDate
           , NULL AS endDate
           , SYSDATE AS created
           , USER AS createdby
           , SYSDATE AS lastUpdate
           , USER AS updatedBy
           , 'UNPLANNED' AS schedule
           , 'YES' AS divertToIvr
           , 'FULL' AS lossOfService
        FROM DUAL
      ;
      dbms_output.put_line ( TO_CHAR ( SQL%ROWCOUNT ) || ' rows inserted.' ) ;
   ELSE
      dbms_output.put_line ( 'Closing off outage notification with an update of endDate ...' ) ;
      UPDATE snsOwner.bsbSoipOutageNotification t
         SET t.endDate = SYSDATE
       WHERE ( t.endDate IS NULL OR t.endDate > SYSDATE )
      ;
      dbms_output.put_line ( TO_CHAR ( SQL%ROWCOUNT ) || ' rows updated.' ) ;
   END IF ;
END ;
/