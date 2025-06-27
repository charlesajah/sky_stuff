select file_id,file_name,bytes/1024/1024 from dba_data_files
where tablespace_name='DEMO2';

drop tablespace demo2 including contents and datafiles;
create tablespace demo2 datafile '/trcomdbn01/ora/data01/TCC011N/demo.dbf' size 250M;

alter database datafile '/trcomdbn01/ora/data01/TCC011N/demo.dbf' resize 100M;

alter user dev default tablespace demo2;

select host_name from v$instance;
                        
select owner,segment_name,bytes/1024/1024 from dba_segments 
where segment_type='TABLE' and bytes/1024/1024 > 50 and bytes/1024/1024 <=100
order by bytes desc;

create table dev.object as select * from SKYUTILS.SKY_USER_LOGINS;


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
 and tsu.tablespace_name not in ('TOOLS_AUTO_01','UNDOTBS','USERS_AUTO_01','SYSTEM','SYSAUX')
 and tsu.tablespace_name not like 'TCC_%'
 order by 1;
 

select 
    distinct mounted_on,
    fsize, 
    used, 
    Avail, 
    use_pcent    
    from(
SELECT 
    mounted_on, 
    fsize, 
    used, 
    Avail, 
    use_pcent, 
    tablespace_name 
FROM (
    SELECT 
        ext.mounted_on, 
        ext.fsize, 
        ext.used, 
        ext.Avail, 
        ext.use_pcent, 
        df.tablespace_name, 
        ROW_NUMBER() OVER (
            PARTITION BY df.file_name 
            ORDER BY LENGTH(ext.mounted_on) DESC
        ) as rn 
    FROM dev.filesystem ext 
    JOIN dba_data_files df ON df.file_name LIKE '%' || ext.mounted_on || '%' 
    WHERE ext.mounted_on != '/'
    --AND df.tablespace_name = 'DEMO'
    AND df.autoextensible = 'NO'
) subquery 
WHERE rn = 1 
GROUP BY tablespace_name, mounted_on, fsize, used, Avail, use_pcent 
ORDER BY tablespace_name);

select * FROM dev.filesystem;

select file_name from dba_data_files;


select username from dba_users where username like 'HP_DI%';

grant alter  tablespace to HP_DIAG;


select file_name from dba_data_files;


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
 order by 1;
 
 alter database datafile '/trcomdbn01/ora/data27/TCC011N/tcc_lob_auto_10_0069.dbf' resize 32767M;
