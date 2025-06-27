CREATE OR REPLACE PACKAGE data_prep_tpoc AS
  PROCEDURE idvtest;
END data_prep_tpoc ;

/


CREATE OR REPLACE PACKAGE BODY data_prep_tpoc AS

PROCEDURE idvtest IS
   l_pool VARCHAR2(29) := 'IDVTEST' ;
   l_count NUMBER ;
BEGIN
   logger.write ( 'begin' ) ;
   sequence_pkg.seqBefore ( i_pool => l_pool ) ;
   INSERT INTO dprov_accounts_fast t ( t.pool_seqno , t.pool_name , t.PARTYID )
    select ROWNUM AS pool_seqno , l_pool AS pool_name, PARTYID
      from (select distinct partyid
              from hp_diag.prod_cus_data_stg 
             where partyid != 'null' 
               and journeyflag = 'IDV' 
               and tstamp > trunc(sysdate)-1 
            order by dbms_random.value);
   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT ) ;
   logger.write ( 'complete' ) ;
END idvtest ;

END data_prep_tpoc ;

/
