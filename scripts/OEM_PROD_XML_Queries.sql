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
select count(*) from (select x.*
from table_with_xml_column ,xmltable('/jobExecution/steps/step' 
passing table_with_xml_column.XML_DOCUMENT
columns
"command" varchar2(200) PATH '@command',
"name" varchar2(200) PATH '@name',
"target" varchar2(100) PATH '@target',
"output" clob PATH '/step/stepOutput/output') x)
where "name" ='blackoutStart'
and "output" like '%handshake has no peer%';




select host_name from v$instance;
select instance_name,host_name from v$instance;

select x.*
from table_with_xml_column ,xmltable('/jobExecution/TargetList/target' 
passing table_with_xml_column.XML_DOCUMENT
columns
"name" varchar2(200) PATH '@name',
"type" varchar2(20) PATH '@type',
"hostName" varchar2(100) PATH '@hostName') x;
