create or replace PACKAGE            "LOGGER" AS
-- 10-Jun-2021 Andrew Fraser
PROCEDURE write ( i_item IN VARCHAR2 DEFAULT NULL ) ;
PROCEDURE debug ( i_item IN VARCHAR2 DEFAULT NULL ) ;
PROCEDURE tomcat_errors ( i_poolName IN VARCHAR2 , i_item IN VARCHAR2 ) ;
END logger ;
/

create or replace PACKAGE BODY            "LOGGER" AS
-- 10-Jun-2021 Andrew Fraser
PROCEDURE write ( i_item IN VARCHAR2 DEFAULT NULL ) IS
   l_line CONSTANT NUMBER := utl_call_stack2.unit_line ( dynamic_depth => 2 ) ;
   l_source CONSTANT VARCHAR2(4000) := utl_call_stack2.concatenate_subprogram ( qualified_name => utl_call_stack2.subprogram ( dynamic_depth => 2 ) ) ;
   PRAGMA AUTONOMOUS_TRANSACTION ;
BEGIN
   dbms_output.put_line ( i_item
      || ' - line '
      || TO_CHAR ( l_line )
      || ' '
      || LOWER ( l_source )
      || ' - '
      || TO_CHAR ( SYSDATE , 'Dy DD-Mon-YYYY HH24:MI:SS' )
      ) ;
   INSERT INTO log_table t ( t.item , t.line , t.source , t.dt )
   VALUES ( SUBSTR ( i_item , 1 , 4000 )
      , l_line
      , l_source
      , SYSDATE
      ) ;
   COMMIT WRITE BATCH NOWAIT ;
EXCEPTION WHEN OTHERS THEN NULL ;  -- don't want any failures to write logging info to break the entire calling pl/sql.
END write ;

-- RFA 13/09/23 - Created to write logs into a table. Used within the REFRESH_PKG but could be anywhere! 
PROCEDURE debug ( i_item IN VARCHAR2 DEFAULT NULL ) IS
   l_line CONSTANT NUMBER := utl_call_stack2.unit_line ( dynamic_depth => 2 ) ;
   l_source CONSTANT VARCHAR2(4000) := utl_call_stack2.concatenate_subprogram ( qualified_name => utl_call_stack2.subprogram ( dynamic_depth => 2 ) ) ;
   PRAGMA AUTONOMOUS_TRANSACTION ;
BEGIN
   INSERT INTO log_debug t ( t.logdate , t.logitem , t.logline , t.logsource )
   VALUES ( SYSDATE
	  , SUBSTR ( i_item , 1 , 4000 )
      , l_line
      , l_source
      ) ;
   COMMIT WRITE BATCH NOWAIT ;
   EXCEPTION WHEN OTHERS THEN NULL ;  
END debug ;

PROCEDURE tomcat_errors ( i_poolName IN VARCHAR2 , i_item IN VARCHAR2 ) IS
   PRAGMA AUTONOMOUS_TRANSACTION ;
   l_dayHour CONSTANT DATE := TO_DATE ( TO_CHAR ( SYSDATE , 'YYYY-MM-DD HH24' ) , 'YYYY-MM-DD HH24' ) ;
   l_poolName CONSTANT VARCHAR2(29) := UPPER ( SUBSTR ( TRIM ( i_poolName ) , 1 , 29 ) ) ;
BEGIN
   MERGE INTO tomcat_errors t USING (
      SELECT NULL FROM dual
   ) s ON ( t.day_hour = l_dayHour AND t.pool_name = l_poolName AND t.item = i_item )
   WHEN NOT MATCHED THEN INSERT ( t.day_hour , t.pool_name , t.item ) VALUES (
        l_dayHour
      , l_poolName
      , i_item
      )
   ;
   COMMIT WRITE BATCH NOWAIT ;
EXCEPTION WHEN OTHERS THEN NULL ;  -- don't want any failures to write logging info to break the entire calling pl/sql.
END tomcat_errors ;

END logger ;
/

