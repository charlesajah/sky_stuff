set termout off
set pagesize 0
set spool on
set head off
set timing on
set feed on

-- Remove "NOT EXIST clause for MINUS ) : nd not exists (select TOKEN_ID from MERCURY.card_details where token_id = pm.cardtoken));
-- RFA (04/08/23) : Changed RANDOM generation of CARD TRANSACTION IDs for a sequence number as RANDOM was generating duplicates
-- RFA (05/08/23) : Remove comments from query as they were raised as errors 

SPOOL mercury_token_pop.txt

select count(*)
  from ( select /*+ parallel(8) */ distinct pm.cardtoken ctoken
           from ccsowner.bsbpaymentmethod@CCS pm, ccsowner.bsbpaymentmethodrole@CCS pmr
          where pm.id = pmr.paymentmethodid
            and pm.cardtoken is not null
            and pm.paymentmethodclasstype = 'BSBPaymentCardMethod'
	      MINUS  
         select TOKEN_ID from MERCURY.card_details);

insert into MERCURY.card_details (UPDATE_USR, UPDATE_DT, CREATE_USR, CREATE_DT, CARDHOLDER_NAME, TRANSACTION_CARD_ID, EXPIRY_DATE, TOKEN_ID, CARD_TYPE) 
select 'MERCURY_SERV', sysdate-1, 'MERCURY_SERV', sysdate-4, 'MRXXXXXXXXXXXXX',
       '5555' || to_char(HP_DIAG.MERC_CARD_DETAILS_TRANSID.NEXTVAL),'202412',
       ctoken,'MSC'
  from ( select /*+ parallel(8) */ distinct pm.cardtoken ctoken
           from ccsowner.bsbpaymentmethod@CCS pm, ccsowner.bsbpaymentmethodrole@CCS pmr
          where pm.id = pmr.paymentmethodid
            and pm.cardtoken is not null
            and pm.paymentmethodclasstype = 'BSBPaymentCardMethod'
          MINUS  
         select TOKEN_ID from MERCURY.card_details);
select count(*)
  from ( select /*+ parallel(8) */ distinct pm.cardtoken ctoken
           from ccsowner.bsbpaymentmethod@CCS pm, ccsowner.bsbpaymentmethodrole@CCS pmr
          where pm.id = pmr.paymentmethodid
            and pm.cardtoken is not null
            and pm.paymentmethodclasstype = 'BSBPaymentCardMethod'
          MINUS 
         select TOKEN_ID from MERCURY.card_details);

commit;   

spool off
exit
