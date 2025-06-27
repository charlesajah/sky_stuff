select * from dev.FILESYSTEM
where mounted_on like '%data%'
order by MOUNTED_ON;


select * from v$sql
where upper(sql_text) like '%ALTER TABLESPACE TCC_LOB_AUTO_10%';


select dd.file_name,d.creation_time from v$datafile d,DBA_DATA_FILES dd
where dd.file_id=d.FILE#
and dd.TABLESPACE_NAME='TCC_LOB_AUTO_10'
order by d.CREATION_TIME desc;

select * from dev.FILESYSTEM
where MOUNTED_ON like '%data%';
select * from DBA_DATA_FILES
where tablespace_name='TCC_LOB_AUTO_10';


--alter tablespace TCC_LOB_AUTO_10 add datafile '/trcomdbn01/ora/data30/TCC011N/tcc_lob_auto_10_00268.dbf' size 32767M;

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
 and tsu.tablespace_name  in ('TCC_LOB_AUTO_10')
 order by 1;