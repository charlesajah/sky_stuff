select a.tablespace_name,
       a.free_mb,
       b.total_mb,
       round((a.free_mb / b.total_mb) * 100, 2) pct_free
  from (select tablespace_name, sum(bytes) / 1024 / 1024 free_mb
          from dba_free_space
         group by tablespace_name) a,
       (select tab.NAME tablespace_name, sum(bytes) / 1024 / 1024 total_mb
          from v$datafile dat, v$tablespace tab
         where dat.TS# = tab.TS#
         group by tab.NAME) b
 where a.tablespace_name = b.tablespace_name
 order by 1