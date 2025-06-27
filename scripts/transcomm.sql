select * from dba_part_indexes
where table_name='BSBCOMMSARTIFACT';

select status,d.* from dba_indexes d
where table_name='BSBCOMMSARTIFACT';

select * from dba_part_tables
where table_name='BSBCOMMSARTIFACT';

select * from dba_tab_partitions
where table_name='BSBCOMMSARTIFACT';

select status from dba_objects
where object_name='BSBCOMMSARTIFACT'
and object_type='TABLE PARTITION';

select * from dev.salestable_stg;

select * from dba_objects 
where object_name='BSBCOMMSARTIFACT'
and subobject_name='SYS_P30773';


select bytes/1024/1024/1024 "size_gb",s.* from dba_Segments s
where segment_name in (select index_name from dba_indexes d
where table_name='BSBCOMMSARTIFACT' and status <> 'VALID')
and segment_type='INDEX';

select status,d.* from dba_indexes d
where table_name='BSBCOMMSARTIFACT';

SELECT * FROM (
   SELECT so.name, so.plan_id, b.sql_handle, b.sql_text, b.creator,
       b.origin, b.parsing_schema_name, b.version, b.created, b.last_modified,
       b.last_executed, b.enabled, b.accepted, b.fixed, b.module,
       extractValue(value(h),'.') AS hint
   FROM sys.sqlobj$data od, sys.sqlobj$ so, dba_sql_plan_baselines b,
       table(xmlsequence(extract(xmltype(od.comp_data),'/outline_data/hint'))) h
   WHERE
       so.signature = od.signature
       AND so.category = od.category
       AND so.obj_type = od.obj_type
       AND so.plan_id = od.plan_id
       AND upper(od.comp_data) like '%FIX_CONTROL%'
       AND (od.comp_data like '%6664361%' or od.comp_data like '%6664361%')
       AND so.signature = b.signature
       AND so.name = b.plan_name)
WHERE upper(hint) like '%FIX_CONTROL%'
 AND (hint like '%6664361%');

select count(*) from dba_sql_plan_baselines da
where da.signature in( select distinct SIGNATURE from SQLOBJ$PLAN where other_xml like '%6664361%') ;

select  * from dba_tables
where table_name='CMP3$1321006';


select owner,object_name,cast(created as timestamp) created,object_type,status  from dba_objects;

where object_name='SYS_P30773';


SELECT s.owner , lp.table_name , lp.partition_position , ROUND ( s.bytes/1024/1024/1024 ) AS gb
     , lp.column_name , lp.lob_name , lp.partition_name AS table_partition_name , lp.lob_partition_name
     ,lp.in_row,lp.chunk,lp.pctversion,lp.retention, lp.tablespace_name , s.segment_type
  FROM dba_segments s
  --JOIN dba_lobs l ON s.owner = l.owner AND s.segment_name = l.segment_name
  JOIN dba_lob_partitions lp ON s.owner = lp.table_owner AND s.segment_name = lp.lob_name AND s.partition_name = lp.lob_partition_name
 WHERE 
   --AND s.segment_type = 'LOB PARTITION'
   lp.table_name='BSBCOMMSARTIFACT'
 ORDER BY 4 desc
;



select segment_name,round(bytes/1024/1024/1024,2) "size_gb",partition_name,segment_type,tablespace_name from dba_segments
where tablespace_name='TCC_LOB_AUTO_10'
order by "size_gb" desc;

select * from DBA_REDEFINITION_status;
select * from DBA_REDEFINITION_errors;
---exec DBMS_REDEFINITION.ABORT_REDEF_TABLE('TCC_OWNER','BSBCOMMSARTIFACT','BSBCOMMSARTIFACT_STG');

select owner,object_name,cast(created as timestamp) created,cast(last_ddl_time as timestamp) last_ddl_time,status from dba_objects
where object_name like 'MLOG$%';

select * from dba_mview_logs;
select * from v$sql where sql_id='f9r78pmw58d0n';
select * from v$sql
where sql_text like '%DBMS_REDEFINITION%';
select status,sql_id from v$session where sid in (483,543,763,807);


---alter system kill session '763,44578' immediate;
select * from v$diag_info;


SELECT OWNER, TABLE_NAME, COLUMN_NAME, SEGMENT_NAME, TABLESPACE_NAME, INDEX_NAME, PARTITIONED
FROM DBA_LOBS
WHERE TABLESPACE_NAME = 'TCC_TDE_LOB_AUTO_01';

select * from dba_tablespaces;

CREATE TABLE dev.salestable
(s_productid NUMBER,
s_saledate DATE,
s_custid NUMBER,
s_totalprice NUMBER)
TABLESPACE USERS_AUTO_01
PARTITION BY RANGE(s_saledate)
(PARTITION sal03q1 VALUES LESS THAN (TO_DATE('01-APR-2024', 'DD-MON-YYYY')),
PARTITION sal03q2 VALUES LESS THAN (TO_DATE('01-JUL-2024', 'DD-MON-YYYY')),
PARTITION sal03q3 VALUES LESS THAN (TO_DATE('01-OCT-2024', 'DD-MON-YYYY')),
PARTITION sal03q4 VALUES LESS THAN (TO_DATE('01-JAN-2025', 'DD-MON-YYYY')));


CREATE TABLE dev.salestable_stg
(s_productid NUMBER,
s_saledate DATE,
s_custid NUMBER,
s_totalprice NUMBER)
TABLESPACE TCC_DATA_AUTO_01;


insert into dev.salestable values(100,to_date('10/DEC/2024','DD/MON/YYYY'),2000125,500);
CREATE INDEX dev.sales_stg_index ON dev.salestable_stg 
   (s_saledate, s_productid, s_custid);
   
   
   
   select * from dev.salestable partition (sal03q2);
   select table_owner,table_name,partition_name,high_value,partition_position,tablespace_name from dba_tab_partitions
   where table_name='BSBCOMMSARTIFACT' and partition_name='SYS_P30773';
   desc dba_objects;
   
   
   select tsu.tablespace_name,
       decode(ceil(tsf.free_gb), NULL, 0, ceil(tsf.free_gb)) free_gb,
       ceil(tsu.used_gb) total_gb,
       decode(100 - ceil(tsf.free_gb / tsu.used_gb * 100),
              NULL,
              100,
              round(tsf.free_gb / tsu.used_gb * 100,2)) pct_free
  from (select tablespace_name, sum(bytes) / 1024 / 1024 /1024 used_gb
          from dba_data_files
         group by tablespace_name) tsu,
       (select tablespace_name, sum(bytes) / 1024 / 1024 /1024 free_gb
          from dba_free_space
         group by tablespace_name) tsf
 where tsu.tablespace_name = tsf.tablespace_name(+)
 --and tsu.tablespace_name not in ('TOOLS_AUTO_01','UNDOTBS','USERS_AUTO_01','SYSTEM','SYSAUX')
 and tsu.tablespace_name ='TCC_LOB_AUTO_10'
 order by free_gb desc;
 
 alter database datafile '/trcomdbn01/ora/data13/TCC011N/tcc_lob_auto_05_0011.dbf' resize 32761M ;
 --/trcomdbn01/ora/data18
 alter tablespace TCC_LOB_AUTO_05 add datafile '/trcomdbn01/ora/data18/TCC011N/tcc_lob_auto_05_00114.dbf' size 15G ;
 
 select file_name,bytes/1024/1024 size_mb from dba_data_files
 where tablespace_name='TCC_LOB_AUTO_10'
 and file_name like '%data20%'
 order by file_name;
 
 alter database datafile '/trcomdbn01/ora/data20/TCC011N/tcc_lob_auto_10_00110.dbf' resize 20G;
 
 
 select * from dev.filesystem;

SELECT tablespace_name,round(tablespace_size/1024/1024/1024,2) "tablespace_size(G)",round(free_space/1024/1024/1024,2) "free_space(G)" FROM DBA_TEMP_FREE_SPACE;

select TABLESPACE_NAME, sum(BYTES_USED/1024/1024),sum(BYTES_FREE/1024/1024) 
from V$TEMP_SPACE_HEADER group by TABLESPACE_NAME;

select * from v$session where sid=621;
select * from v$session where sql_id='fb0jf7s1tu11p';
select * from v$PX_session;
select sql_text,sql_id from v$sql where sql_text like'%FK_ORIGARTIFACT_COMMSARTIFACT%';

select * from v$session where sid in(select sid from v$px_session);


select s.sid,s.serial# from v$session s;


select * from dev.filesystem;
select * from v$session where sid=359 and serial#=53407;