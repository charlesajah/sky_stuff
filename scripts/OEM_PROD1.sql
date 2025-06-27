--list of all oracle db servers with 13.2 agent version
WITH version as(select replace(target_name,'.crb.apmoller.net:1830','') h_name from 
(select replace(target_name,'.crb.apmoller.net:3872','') target_name from sysman.mgmt$target_properties agent_version
where agent_version.target_type='oracle_emd'
and agent_version.property_name='Version'
and agent_version.property_value='13.2.0.0.0')
),  
db_host as(
select  distinct(replace(os.TARGET_NAME,'.crb.apmoller.net','')) server,db.target_type target_type
from SYSMAN.MGMT$TARGET db, SYSMAN.MGMT$TARGET os
where db.HOST_NAME = os.TARGET_NAME
and db.target_type='oracle_database'
and os.target_type='host'
)
select * from version,db_host
where version.h_name=db_host.server;

--start/stop agent
--emcli start_agent -agent_name="agent.example.com:1234" -credential_name="MyMachineCredential"

--to start/stop multiple agents
for i in `cat agents_list`; do emcli start_agent -agent_name=$i -credential_name="MyMachineCredential"; done

show con_id;
--query for checking agent targets managed status
select host_name,entity_name,entity_type
,decode (MANAGE_STATUS,0,'Ignored',1,'Not yet managed',2,'Managed') managed_status
,decode (broken_reason,0,'Not Broken',Broken_str) broken_reason
,decode(promote_status,0,'Cannot Promote',1,'Can be promoted',2,'Promotion in progress',3,'Promoted') promote_status
,to_char(load_timestamp,'dd-mon-yyyy hh24:mi') load_timestamp
from sysman.MGMT$MANAGEABLE_ENTITIES
where host_name = 'scrbafldk001381.crb.apmoller.net'
and MANAGE_STATUS=2 and promote_status=3;

select * from sysman.MGMT$MANAGEABLE_ENTITIES
where host_name = 'scrbafldk001381.crb.apmoller.net' and 
;


select * from sysman.MGMT$AGENTS_MONITORING_TARGETS
where lower(AGENT_HOST_NAME) like 'scrbampdk001522%' and TARGET_TYPE='oracle_home';

select * from sysman.MGMT$TARGET_COMPONENTS  where HOST_NAME like 'scrbampdk001522%' and target_type='oracle_emd';

select * from sysman.MGMT$TARGET where HOST_NAME='scrbafldk001381.crb.apmoller.net' and TARGET_TYPE='oracle_home';
select * from sysman.MGMT$TARGET_TYPE where target_name='scrbafldk001381.crb.apmoller.net';
select * from sysman.MGMT$TARGET_TYPE_DEF where TARGET_TYPE='oracle_emd';
desc sysman.MGMT$TARGET_TYPE_DEF;
select * from "SYSMAN"."GC_AGENTS_MONITORING_TARGETS" where TARGET_TYPE='oracle_home' and agent_name like 'scrbafldk001381.crb.apmoller.net%';
select * from sysman.MGMT$TARGET_FLAT_MEMBERS where MEMBER_TARGET_TYPE='oracle_home' and  member_target_name like '%scrbampdk001522%';

select * from sysman.MGMT$OH_HOME_INFO where host_name='scrbampdk001522.crb.apmoller.net';
select * from sysman.MGMT$OH_INV_SUMMARY where host_name='scrbafldk004173.crb.apmoller.net';
select inf.target_name, inf.HOME_LOCATION from sysman.MGMT$OH_INV_SUMMARY inv, sysman.MGMT$OH_HOME_INFO inf
where inv.host_name='scrbampdk001522.crb.apmoller.net' and inv.comp_name='oracle.sysman.top.agent' and inv.oh_target_guid=inf.target_guid;




--list of all databases by patch release in OEM

select * from sysman.mgmt$applied_patches ;
select distinct a.target_name,a.host_name,b.patch_release,b.patch,b.host,b.home_location,b.home_name
from sysman.mgmt$target a,sysman.MGMT$APPLIED_PATCHES b
where a.host_name=b.host
and a.target_type in ('oracle_database') and b.patch_release like '11.1%' ;