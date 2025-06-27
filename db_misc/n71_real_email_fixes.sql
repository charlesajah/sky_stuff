set termout off
set pagesize 0
set spool on
set head off
set timing on

SPOOL n71_email_fixes.txt

select /*+ parallel(be,4) */ 'Before Count : ' || count(*)
  from BCRM.EMAIL be
 where (     TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%bskyb.com'
         AND TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%sky.ie'
         AND TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%sky.uk'
         AND TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%skygroup.com'
       );

update /*+ parallel(be,4) */ BCRM.EMAIL be
set be.emailaddress = to_char(systimestamp,'ddmmyyyy_hh24miss_ff4') || '@sky.uk'
where (      TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%bskyb.com'
         AND TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%sky.ie'
         AND TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%sky.uk'
         AND TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%skygroup.com'
       );
	
select /*+ parallel(be,4) */ 'After Count : ' || count(*)
  from BCRM.EMAIL be
 where (     TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%bskyb.com'
         AND TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%sky.ie'
         AND TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%sky.uk'
         AND TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%skygroup.com'
       );

commit;

spool off
exit
