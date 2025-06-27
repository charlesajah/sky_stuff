spool bsbtelephone.log

set timing on
alter session force parallel dml;

update /*+ parallel(32) */ ccsowner.bsbtelephone
   set telephonenumber = '999' || substr(telephonenumber, 4,3),
       combinedtelephonenumber = areacode ||  '999' || substr(telephonenumber, 4,3)
 where areacode not like '07%' ;

commit;

update /*+ parallel(32) */ ccsowner.bsbserviceinstance
   set telephonenumber = substr(telephonenumber, 1,5) || '999' || substr(telephonenumber, 9,3)
 where telephonenumber is not null 
   and telephonenumber not like '07%' ;
commit;

spool off
exit;
