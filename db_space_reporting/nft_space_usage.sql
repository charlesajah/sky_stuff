drop TABLE nft_Space_temp;
CREATE GLOBAL TEMPORARY TABLE nft_Space_temp AS select * from hp_diag.nft_Space@chord where rownum < 1;

insert into nft_Space_temp
SELECT sys_context('USERENV','DB_NAME') db_name, 
       full.tablespace_name,
       round(sum(mb_alloc),2) ALLOCATED_MB, 
       round(sum(mb_alloc)-sum(mb_free),2) USED_MB, 
       round(sum(mb_free),2) FREE_MB,
       trunc(sysdate) as run_date
  FROM ( SELECT tablespace_name, SUM(bytes)/1024/1024 mb_free
		   FROM sys.DBA_FREE_SPACE
		 GROUP BY tablespace_name ) FREE,
	   ( SELECT tablespace_name, SUM(bytes)/1024/1024 mb_alloc
		   FROM sys.DBA_DATA_FILES
		 GROUP BY tablespace_name ) FULL
 WHERE FREE.tablespace_name (+) = FULL.tablespace_name
group by full.tablespace_name;

MERGE INTO nft_Space@chord a
USING ( SELECT * FROM nft_Space_temp ) b
   ON (     trunc(a.run_date) = trunc(b.run_date) 
        and a.db_name = b.db_name 
        and a.tablespace_name = b.tablespace_name )
 WHEN MATCHED THEN UPDATE SET a.FREE_MB      = b.FREE_MB,
                              a.ALLOCATED_MB = b.ALLOCATED_MB,
                              a.USED_MB      = b.USED_MB
 WHEN NOT MATCHED THEN insert (DB_NAME, TABLESPACE_NAME, ALLOCATED_MB, USED_MB, FREE_MB, RUN_DATE) 
                       values (b.DB_NAME, b.TABLESPACE_NAME, b.ALLOCATED_MB, b.USED_MB, b.FREE_MB, b.RUN_DATE);

drop TABLE nft_seg_Space_temp;
CREATE GLOBAL TEMPORARY TABLE nft_seg_Space_temp AS select * from hp_diag.nft_seg_Space@chord where rownum < 1;

insert into nft_seg_Space_temp
select sys_context('USERENV','DB_NAME') as db_name, 
       ds.owner as SCHEMA_NAME,
       round(sum(bytes)/1024/1024/1024, 3) as USED_GB,
       trunc(sysdate) as run_date
  from dba_segments ds, dba_users us
 where ds.owner = us.username 
   and us.created > (select created+(30/60/24) from dba_users where username = 'SYS')
   and not REGEXP_LIKE (us.username, '[a-z]{3}[0-9]{2}','i')
   and us.username not in ('HP_DIAG','BSBDEPLOY','HOUSEKEEPING','DATAPROV','XDB','ANONYMOUS','CAPACITY','FOCUS_PRD')
group by ds.owner;

MERGE INTO nft_seg_Space@chord a
USING ( SELECT * FROM nft_seg_Space_temp ) b
   ON (     trunc(a.run_date) = trunc(b.run_date) 
        and a.db_name = b.db_name
        and a.SCHEMA_NAME = b.SCHEMA_NAME )
 WHEN MATCHED THEN UPDATE SET a.USED_GB      = b.USED_GB
 WHEN NOT MATCHED THEN insert (DB_NAME, SCHEMA_NAME, USED_GB, RUN_DATE) 
                       values (b.DB_NAME, b.SCHEMA_NAME, b.USED_GB, b.RUN_DATE);

commit;
