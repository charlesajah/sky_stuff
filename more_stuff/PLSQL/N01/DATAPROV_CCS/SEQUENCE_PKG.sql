CREATE OR REPLACE PACKAGE sequence_pkg AS
-- 10-Jun-2021 Andrew Fraser
PROCEDURE seqBefore ( i_pool IN VARCHAR2 , i_flagCustomers IN BOOLEAN DEFAULT FALSE ) ;
PROCEDURE seqAfter ( i_pool IN VARCHAR2 , i_count IN NUMBER , i_burn IN BOOLEAN DEFAULT FALSE , i_flagCustomers IN BOOLEAN DEFAULT FALSE ) ;
PROCEDURE logSequenceBeforeDrop ( i_pool IN VARCHAR2 ) ;
PROCEDURE DropSequence ( i_pool IN VARCHAR2 ) ;

END sequence_pkg ;
/


CREATE OR REPLACE PACKAGE BODY sequence_pkg AS
-- 10-Jun-2021 Andrew Fraser
-- 12/09/24 - RFA : Adding CUSTOMERSV2 to the procecss updating the POOL column
PROCEDURE seqBefore ( i_pool IN VARCHAR2 , i_flagCustomers IN BOOLEAN DEFAULT FALSE ) IS
   l_count NUMBER ;
BEGIN
   SELECT COUNT(*) INTO l_count FROM user_sequences s WHERE s.sequence_name = 'S' || UPPER ( TRIM ( i_pool ) ) ;
   IF l_count > 0
   THEN
      logSequenceBeforeDrop ( i_pool => i_pool ) ;
      EXECUTE IMMEDIATE 'DROP SEQUENCE s' || i_pool ;
   END IF ;
   DELETE FROM dprov_accounts_fast t WHERE t.pool_name = TRIM ( UPPER ( i_pool ) ) ;
   IF i_flagCustomers
   THEN
      UPDATE customers t SET t.pool = NULL WHERE t.pool = TRIM ( UPPER ( i_pool ) ) ;
      --UPDATE customersv2 t SET t.pool = NULL WHERE t.pool = TRIM ( UPPER ( i_pool ) ) ;
      COMMIT;
   END IF ;
END seqBefore ;

PROCEDURE DropSequence ( i_pool IN VARCHAR2 ) IS
   l_count NUMBER ;
BEGIN
   SELECT COUNT(*) INTO l_count FROM user_sequences s WHERE s.sequence_name = 'S' || UPPER ( TRIM ( i_pool ) ) ;
   IF l_count > 0
   THEN
      logSequenceBeforeDrop ( i_pool => i_pool ) ;
      EXECUTE IMMEDIATE 'DROP SEQUENCE s' || i_pool ;
   END IF ;
END DropSequence ;

PROCEDURE seqAfter ( i_pool IN VARCHAR2 , i_count IN NUMBER , i_burn IN BOOLEAN DEFAULT FALSE , i_flagCustomers IN BOOLEAN DEFAULT FALSE ) IS
BEGIN
   -- deals with ORA-04013: number to CACHE must be less than one cycle
   EXECUTE IMMEDIATE 'CREATE SEQUENCE s' || i_pool
      || CASE WHEN i_burn THEN ' NOCYCLE ' ELSE ' CYCLE ' END
      || ' MAXVALUE ' || TO_CHAR ( GREATEST ( 3 , i_count ) )
      || ' CACHE ' || TO_CHAR ( LEAST ( 10000 , GREATEST ( 2 , i_count - 1 ) ) )
      ;
   IF i_flagCustomers
   THEN
      UPDATE customers t SET t.pool = TRIM ( UPPER ( i_pool ) )
       WHERE t.accountNumber IN ( SELECT f.accountNumber FROM dprov_accounts_fast f WHERE f.pool_name = TRIM ( UPPER ( i_pool ) ) ) ;
      --UPDATE customersv2 t SET t.pool = TRIM ( UPPER ( i_pool ) )
      -- WHERE t.accountNumber IN ( SELECT f.accountNumber FROM dprov_accounts_fast f WHERE f.pool_name = TRIM ( UPPER ( i_pool ) ) ) ; 
      COMMIT;
   END IF ;
END seqAfter ;

PROCEDURE logSequenceBeforeDrop ( i_pool IN VARCHAR2 ) IS
-- Records some usage data so we can check later if the pool is actually being used, and if so by how much.
   l_nextVal NUMBER ;
   l_created DATE ;
   l_maxVal NUMBER ;
   l_exceeds_maxvalue VARCHAR2(200) ;
   e_exceeds_maxvalue EXCEPTION ;
   PRAGMA EXCEPTION_INIT ( e_exceeds_maxvalue , -8004 ) ;
BEGIN
   BEGIN
      EXECUTE IMMEDIATE 'SELECT s' || LOWER ( TRIM ( i_pool ) ) || '.NEXTVAL FROM DUAL' INTO l_nextVal ;
   EXCEPTION
      WHEN e_exceeds_maxvalue THEN
         l_exceeds_maxvalue := 'error exceeds maxValue' ;
      WHEN OTHERS THEN NULL ;  -- dont allow a logging failure to cause entire pool rebuild to crash out.
   END ;
   SELECT o.created , s.max_value INTO l_created , l_maxVal
     FROM user_sequences s
     JOIN user_objects o ON o.object_name = s.sequence_name AND o.object_type = 'SEQUENCE'
    WHERE s.sequence_name = 'S' || UPPER ( TRIM ( i_pool ) )
   ;
   MERGE INTO dprov_accounts_fast_log t USING (
      SELECT l.poolName , l.counter , l.loops , l.created
        FROM dprov_accounts_fast_log l
       WHERE l.poolName = UPPER ( TRIM ( i_pool ) )
         AND l.created = l_created
   ) s ON ( t.poolName = s.poolName AND t.created = s.created )
   WHEN MATCHED THEN UPDATE SET t.counter = s.counter + NVL ( l_nextVal , l_maxVal + 1 ) , t.exceeds_maxValue = l_exceeds_maxvalue
   ;
   MERGE INTO dprov_accounts_fast_log t USING (
      SELECT NULL FROM dual
   ) s ON ( t.poolName = UPPER ( TRIM ( i_pool ) ) AND t.created = l_created )
   WHEN NOT MATCHED THEN INSERT ( t.poolName , t.counter , t.loops , t.created , t.exceeds_maxValue , t.max_value )
      VALUES ( UPPER ( TRIM ( i_pool ) ) , NVL ( l_nextVal , l_maxVal + 1 ) , 1 , l_created ,  l_exceeds_maxvalue , l_maxVal )
   ;
EXCEPTION WHEN OTHERS THEN NULL ;  -- dont allow a logging failure to cause entire pool rebuild to crash out.      
END logSequenceBeforeDrop ;

END sequence_pkg ;
/
