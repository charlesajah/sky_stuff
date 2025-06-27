--OEM query for oracle DBs providing lifecycle and LOB
 with LifeCycle as(
        select distinct target_name,property_value from (select target_name,target_type,property_name,property_value FROM sysman.MGMT$TARGET_PROPERTIES
        WHERE property_name in ('orcl_gtp_lifecycle_status'))
        where target_type='oracle_database' )
    ,LOB as(        
    select distinct target_name,property_value from (select target_name,target_type,property_name,property_value FROM sysman.MGMT$TARGET_PROPERTIES
        WHERE property_name in ('orcl_gtp_line_of_bus'))
        where target_type='oracle_database')
    select lc.target_name,lc.property_value LifeCycle,lb.property_value LOB from LifeCycle lc,LOB lb
    where lc.target_name=lb.target_name
    and lc.property_value !='MissionCritical'
    and lb.property_value !='IBM' order by target_name;




--agent upgrade of multiple agents
emcli upgrade_agents -input_file="agents_file:/home/oracle/agents_to_upgrade" -override_credential="NC_HOST_OEMAGENT" -additional_parameters="-ignorePrereqs SCRATCHPATH=/u01/app/oemagent/product" -job_name="Upgrade_to_13.4"
--getting job execution details in xml
--using xml output we get the full job details including all the steps
emcli help get_job_execution_detail
emcli get_jobs -noheader -name=MW_AGENT_UPGRADE_TO_13.4 | awk  '{print $4}'

--the command above produces the job_id/execution_id for the job which can be supplied to the next command
emcli get_job_execution_detail -execution=D63D54281573BBDCE053EBF9FF0A2702 -xml -showOutput > upgrade_13_4.xml

--how to load xml into table
INSERT INTO caj020.agent_upgrade (xml_column)VALUES ( xmltype(bfilename('UPGRADE_RES', 'Upgrade_to_13.4.xml')  , nls_charset_id('AL32UTF8')));


--the subject xml document is the upgrade_13_4.xml file
--using extractvalue to query attributes of the target tag
SELECT EXTRACTVALUE (VALUE (a1),
                 '/target/@name')
      attribute,
         EXTRACTVALUE (VALUE (a1),
                 '/target/@type')
      TYPE,
   EXTRACTVALUE (VALUE (a1),
                 '/target/@hostName')
      VALUE
 FROM TABLE_WITH_XML_COLUMN,
   TABLE (
      XMLSEQUENCE (
         EXTRACT (
            XML_DOCUMENT,
            '/jobExecution/TargetList/target'))) a1
 WHERE filename = 'upgrade_13_4.xml';


--using extractvalue to query attributes of the target tag including the /step/stepOutput/output
select * from 
(SELECT EXTRACTVALUE (VALUE (a1),
                 '/step/@command')
      attribute,
         EXTRACTVALUE (VALUE (a1),
                 '/step/@name')
      TYPE,
   EXTRACTVALUE (VALUE (a1),
                 '/step/@target')
      TARGET,
         EXTRACTVALUE (VALUE (a1),
                 '/step/stepOutput/output')
      OUTPUT
 FROM TABLE_WITH_XML_COLUMN,
   TABLE (
      XMLSEQUENCE (
         EXTRACT (
            XML_DOCUMENT,
            '/jobExecution/steps/step'))) a1
 WHERE filename = 'upgrade_13_4.xml')
 where type='blackoutStart'
 and output not like '%Exit Code :0%';
 
 --A different way of doing same thing as the prceeding query
 SELECT EXTRACTVALUE (VALUE (a1),
                 '/step/@command')
      attribute,
         EXTRACTVALUE (VALUE (a1),
                 '/step/@name')
      TYPE,
   EXTRACTVALUE (VALUE (a1),
                 '/step/@target')
      TARGET,
         EXTRACTVALUE (VALUE (a1),
                 '/step/stepOutput/output')
      OUTPUT
 FROM TABLE (
      XMLSEQUENCE (
         EXTRACT (
            xmltype(BFILENAME ('XML_DIR', 'upgrade_13_4.xml'),NLS_CHARSET_ID ('AL32UTF8')),
            '/jobExecution/steps/step'))) a1;
 
 
 
 
--The output column sometimes can contain data bigger than the varchar2(4000) bybte limit.
--This is a workaround to store the column in CLOB using XPATH expressions
--This is to avoid the following error
--ORA-01706: user function result value was too large
select * from (select x.*
from table_with_xml_column ,xmltable('/jobExecution/steps/step' 
passing table_with_xml_column.XML_DOCUMENT
columns
"command" varchar2(200) PATH '@command',
"name" varchar2(200) PATH '@name',
"target" varchar2(100) PATH '@target',
"output" clob PATH '/step/stepOutput/output') x)
where "name" ='blackoutStart'
and "output" like '%handshake has no peer%';





--282 servers failed at the blackoutSTart stage



select "target" from (select x.*
from table_with_xml_column ,xmltable('/jobExecution/steps/step' 
passing table_with_xml_column.XML_DOCUMENT
columns
"command" varchar2(200) PATH '@command',
"name" varchar2(200) PATH '@name',
"target" varchar2(100) PATH '@target',
"output" clob PATH '/step/stepOutput/output') x)
where "name" ='blackoutStart'
and "output" like '%handshake has no peer%';




select x.*
from table_with_xml_column ,xmltable('/jobExecution/TargetList/target' 
passing table_with_xml_column.XML_DOCUMENT
columns
"name" varchar2(200) PATH '@name',
"type" varchar2(20) PATH '@type',
"hostName" varchar2(100) PATH '@hostName') x;


--view of all servers that failed for handshake no peer issue
--during the agent upgrade job MW_AGENT_UPGRADE_TO_13.4
create view upgrade_res_view as
select trim(lower(replace(target,'.crb.apmoller.net:1830',''))) target from (
select replace(target,'.crb.apmoller.net:3872','') target
from (
select "target" target from (select x.*
from table_with_xml_column ,xmltable('/jobExecution/steps/step' 
passing table_with_xml_column.XML_DOCUMENT
columns
"command" varchar2(200) PATH '@command',
"name" varchar2(200) PATH '@name',
"target" varchar2(100) PATH '@target',
"output" clob PATH '/step/stepOutput/output') x)
where "name" ='blackoutStart'
and "output" like '%handshake has no peer%')) ;


--these are the middleware servers that failed to upgrade because of handshake no peer issue from
--the upgrade job name MW_AGENT_UPGRADE_TO_13.4
with mo_servers as (select trim(lower(replace(host_name,'.crb.apmoller.net',''))) host_name from middleware_servers)
,upgrade_job as (select * from UPGRADE_RES_VIEW)
select host_name from mo_servers,upgrade_job
where mo_servers.host_name=upgrade_job.target;






select * from middleware_servers;
create view middleware_servers as 
select host_name from azure_mo union 
select host_name from IBM_MO;

create table azure_mo as
select distinct mt.host_name FROM
sysman.mgmt$target mt,
sysman.mgmt$target_members tm
where mt.target_name=tm.MEMBER_TARGET_NAME 
and  tm.AGGREGATE_TARGET_NAME like '%TCS-MO%' 
and mt.host_name is not null;


--As far as the OEM is concerned
--these are the list of all 13.2 agents on non-oracle db servers
select distinct lower(replace(target_name,'.apmoller.net:3872','')) target_name from
(select replace(target_name,'.crb.apmoller.net:1830','') target_name from 
(select replace(agent_version.target_name,'.crb.apmoller.net:3872','') target_name from sysman.mgmt$target_properties agent_version
where agent_version.target_type='oracle_emd'
and agent_version.property_name='Version'
and agent_version.property_value='13.2.0.0.0')
where lower(replace(target_name,'.apmoller.net:3872','')) not in (
select target_name from(
select distinct lower(replace(target_name,'.apmoller.net:3872','')) target_name,LENGTH(replace(trim(target_name),'.apmoller.net:3872','')) Length,INSTR(lower(trim(target_name)),'a',5,1) as "first_a_position",INSTR(lower(trim(target_name)),'d',5,1)as "first_d_position" from
(select replace(target_name,'.crb.apmoller.net:1830','') target_name from 
(select replace(agent_version.target_name,'.crb.apmoller.net:3872','') target_name from sysman.mgmt$target_properties agent_version
where agent_version.target_type='oracle_emd'
and agent_version.property_name='Version'
and agent_version.property_value='13.2.0.0.0'))
where target_name like 'po%' 
and INSTR(lower(trim(target_name)),'d',5,1 )=5))
minus 
select h_name
from (
with version as(
select replace(h_name,'.apmoller.net:3872','') h_name
        from( (select replace(target_name,'.crb.apmoller.net:1830','') h_name from 
        (select replace(target_name,'.crb.apmoller.net:3872','') target_name from sysman.mgmt$target_properties agent_version
        where agent_version.target_type='oracle_emd'
        and agent_version.property_name='Version'
        and agent_version.property_value='13.2.0.0.0')
        )))
,db_host as(        
select  replace(server,'.apmoller.net:3872','') server,target_type
from (select(replace(os.TARGET_NAME,'.crb.apmoller.net','')) server,db.target_type target_type
from SYSMAN.MGMT$TARGET db, SYSMAN.MGMT$TARGET os
where db.HOST_NAME = os.TARGET_NAME
and db.target_type='oracle_database'
and os.target_type='host'))
select lower(h_name) h_name from version,db_host
where version.h_name=db_host.server));





with agents_on_13_2 as (select lower(trim(replace(target_name,'.apmoller.net:3872',''))) target_name from
(select replace(target_name,'.crb.apmoller.net:1830','') target_name from 
(select replace(agent_version.target_name,'.crb.apmoller.net:3872','') target_name from sysman.mgmt$target_properties agent_version
where agent_version.target_type='oracle_emd'
and agent_version.property_name='Version'
and agent_version.property_value='13.2.0.0.0'))),
mw_servers as (select replace(host_name,'.crb.apmoller.net','') host_name from middleware_servers)
select target_name from 
agents_on_13_2,mw_servers
where agents_on_13_2.target_name=mw_servers.host_name;

--select host_name from v$instance;

with version as(
select replace(h_name,'.apmoller.net:3872','') h_name
        from( (select replace(target_name,'.crb.apmoller.net:1830','') h_name from 
        (select replace(target_name,'.crb.apmoller.net:3872','') target_name from sysman.mgmt$target_properties agent_version
        where agent_version.target_type='oracle_emd'
        and agent_version.property_name='Version'
        and agent_version.property_value='13.2.0.0.0')
        )))
,db_host as(        
select  replace(server,'.apmoller.net:3872','') server,target_type
from (select(replace(os.TARGET_NAME,'.crb.apmoller.net','')) server,db.target_type target_type
from SYSMAN.MGMT$TARGET db, SYSMAN.MGMT$TARGET os
where db.HOST_NAME = os.TARGET_NAME
and db.target_type='oracle_database'
and os.target_type='host'))
select h_name,server from version,db_host
where version.h_name=db_host.server;


select target_name from(
select distinct lower(replace(target_name,'.apmoller.net:3872','')) target_name,LENGTH(replace(trim(target_name),'.apmoller.net:3872','')) Length,INSTR(lower(trim(target_name)),'a',5,1) as "first_a_position",INSTR(lower(trim(target_name)),'d',5,1)as "first_d_position" from
(select replace(target_name,'.crb.apmoller.net:1830','') target_name from 
(select replace(agent_version.target_name,'.crb.apmoller.net:3872','') target_name from sysman.mgmt$target_properties agent_version
where agent_version.target_type='oracle_emd'
and agent_version.property_name='Version'
and agent_version.property_value='13.2.0.0.0'))
where target_name like 'po%' 
and INSTR(lower(trim(target_name)),'d',5,1 )=5);



