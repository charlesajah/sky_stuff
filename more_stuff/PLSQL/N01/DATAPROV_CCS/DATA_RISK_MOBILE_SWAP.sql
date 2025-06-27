--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure DATA_RISK_MOBILE_SWAP
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."DATA_RISK_MOBILE_SWAP" AS 
BEGIN
  -- remove data from previous run
  CLEAR_OWN_CUST_DATA_A;
  
  -- get account details to add to table in RIS011N
  for customer in (select /*+ parallel(daf, 8) parallel(cu, 8) */ cu.accountnumber, cu.firstname, cu.familyname  
                     from dprov_Accounts_fast daf, customers cu
                    where cu.accountnumber = daf.accountnumber
                    and daf.pool_name = 'MOBILESWAP'
                   order by 1 )
  loop
    -- call procedure which populates table
    INSERT_CLEAR_OWN_CUST_DATA_A(customer.firstname,
                                 customer.familyname,
                                 customer.accountnumber);
  end loop;  
END DATA_RISK_MOBILE_SWAP;

/

  GRANT EXECUTE ON "DATAPROV"."DATA_RISK_MOBILE_SWAP" TO "BATCHPROCESS_USER";
