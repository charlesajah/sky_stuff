set termout off
set pagesize 0
set spool on
set head off
set timing off

SPOOL N02_ASU_data_cleardown.txt

select /*+ parallel(2) */ 'bsbsnsserviceoutage before      : ' || count(*) from snsowner.bsbsnsserviceoutage       where createdby = 'nft_ars_service' ;
select /*+ parallel(2) */ 'bsbsnsoutagenotification before : ' || count(*) from snsowner.bsbsnsoutagenotification  where createdby = 'nft_ars_service' ;

delete from snsowner.bsbsnsserviceoutage       where createdby = 'nft_ars_service' ;
delete from snsowner.bsbsnsoutagenotification  where createdby = 'nft_ars_service' ;

select /*+ parallel(2) */ 'bsbsnsserviceoutage after      : ' || count(*) from snsowner.bsbsnsserviceoutage       where createdby = 'nft_ars_service' ;
select /*+ parallel(2) */ 'bsbsnsoutagenotification after : ' || count(*) from snsowner.bsbsnsoutagenotification  where createdby = 'nft_ars_service' ;

commit;

spool off
exit
