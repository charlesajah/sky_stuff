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
 --and tsu.tablespace_name not like 'TCC_%'
 order by 1
