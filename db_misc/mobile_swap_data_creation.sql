set termout off
set pagesize 0
set spool on
set head off
set timing on

SPOOL mobile_swap_data_creation.txt

select 'Before count : ' || count(*) from own_cust_data_a where update_user = 'DATAPROV';
exec DATA_RISK_MOBILE_SWAP;
select 'After count : ' || count(*) from own_cust_data_a where update_user = 'DATAPROV';

commit;

spool off
exit