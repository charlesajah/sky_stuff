




set SERVEROUTPUT ON;
select total_savings from table(HP_DIAG.manage_tablespace.get_space_savings(database => 'TCC021N', tablespace_name => 'DEMO2'));
select * from table(HP_DIAG.manage_tablespace.adjust_space_savings(database => 'TCC021N', tablespace_name => 'DEMO2' , reclaim => '100MB'));
   SELECT TO_NUMBER(REGEXP_REPLACE('4RT5MB', '[^0-9]', '')) converted  FROM dual;
   select file_name,bytes/1024/1024 from DBA_DATA_FILES where TABLESPACE_NAME='DEMO2';
   alter database datafile '/trcomdbn02/ora/data01/TCC021N/demo.dbf' resize 750M;

create or replace TYPE reclaim_OBJ_TYPE IS OBJECT
(
adjusted_free_mb number,
adjusted_total_mb number,
adjusted_pct_free number,
final_reclaim number
);
/

 CREATE OR REPLACE TYPE RECLAIM_TABTYPE AS TABLE OF reclaim_OBJ_TYPE;
 /
--drop type RECLAIM_TABTYPE;
 select tsu.tablespace_name,
       decode(ceil(tsf.free_mb), NULL, 0, ceil(tsf.free_mb)) free_mb,
       ceil(tsu.used_mb) total_mb,
       decode(100 - ceil(tsf.free_mb / tsu.used_mb * 100),
              NULL,
              100,
              round(tsf.free_mb / tsu.used_mb * 100,2)) pct_free
  from (select tablespace_name, sum(bytes) / 1024 / 1024 used_mb
          from dba_data_files
         group by tablespace_name) tsu,
       (select tablespace_name, sum(bytes) / 1024 / 1024 free_mb
          from dba_free_space
         group by tablespace_name) tsf
 where tsu.tablespace_name = tsf.tablespace_name(+)
 and tsu.tablespace_name  in ('DEMO2')
 order by 1;

 select value from v$parameter where name = 'db_block_size';
/
select file_name,
       ceil( (nvl(hwm,1)*&&blksize)/1024/1024 ) smallest,
       ceil( blocks*&&blksize/1024/1024) currsize,
       ceil( blocks*&&blksize/1024/1024) -
       ceil( (nvl(hwm,1)*&&blksize)/1024/1024 ) as savings
from dba_data_files a,
     ( select file_id, max(block_id+blocks-1) hwm
         from dba_extents
        group by file_id ) b
where a.file_id = b.file_id(+) 
--and ceil( blocks*&&blksize/1024/1024) -
       --ceil( (nvl(hwm,1)*&&blksize)/1024/1024 ) > 100
and a.TABLESPACE_NAME = 'DEMO' 
order by savings desc;

 select * from user_db_links order by DB_LINK;
 show parameter db_name;
 select * from dev.FILESYSTEM
 where MOUNTED_ON like '%data%';

 select privilege from DBA_SYS_PRIVS
 where GRANTEE='HP_DIAG';