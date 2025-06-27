set pages 0
set define on
set newpage 0
set lines 999
set verify off
SET UNDERLINE off
set head off
SET RECSEP off
set feed off
set echo off
SET COLSEP '|'
set trims on
set serveroutput on size unlimited

var B1 Varchar2(30)
var B2 Varchar2(30)

begin
  :b1:='&1'; -- first test - the later of the two tests
  :b2:='&2'; -- second test - the earlier test.
end;
/



spool sql_match.txt append

begin
dbms_output.put_line('h5. Closest Matches for SQL Observed in Latest Test but not in Previous Test. ..' ) ;
dbms_output.put_line( '||Database||Sql Id||Matched SQL_ID||Matching Score||' ) ;
for r1 in (
      with prev as (
               SELECT ROWNUM rnum, a.database_name, a.sql_id, a.tps, a.ms_pe, a.sql_text, a.module
                 FROM (
                           SELECT LOWER ( s.database_name ) AS database_name, s.sql_id, 
                                  TO_CHAR ( ROUND ( s.tps , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS tps,
                                  TO_CHAR ( ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS ms_pe, 
                                  LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text, 
                                  LOWER ( s.module ) AS module
                             FROM hp_diag.test_result_sql s
                             LEFT OUTER JOIN test_result_sqltext st ON st.sql_id = s.sql_id
                            WHERE test_id = :b2
                              AND lower(s.module) not in('dbms_scheduler')
                           ORDER BY elapsed_time_seconds DESC
                      ) a
         ) ,  cur as (
               SELECT ROWNUM rnum, a.database_name, a.sql_id, a.tps, a.ms_pe, a.sql_text, a.module
               FROM (
                           SELECT LOWER ( s.database_name ) AS database_name, s.sql_id, 
                                  TO_CHAR ( ROUND ( s.tps , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS tps,
                                  TO_CHAR ( ROUND ( s.elapsed_time_per_exec_seconds * 1000 , 2 ) , 'FM9,999,999,999,999,999,999,999,999,999,990.00' ) AS ms_pe,
                                  LOWER ( TO_CHAR ( SUBSTR ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( REPLACE ( st.sql_text , '"' ) , CHR(9) , ' ' ) , CHR(10) , ' ' ) , CHR(13), ' ' ) , '   ' , ' ' ) , '   ' , ' ' ) ,    '  ' , ' ' ) , '  ' , ' ' ) , 1 , 100 ) ) ) AS sql_text, 
                                  LOWER ( s.module ) AS module
                             FROM hp_diag.test_result_sql s
                             LEFT OUTER JOIN test_result_sqltext st ON st.sql_id = s.sql_id
                            WHERE test_id = :b1
                              AND lower(s.module) not in('dbms_scheduler')
                           ORDER BY elapsed_time_seconds DESC
                      ) a
               WHERE ROWNUM <= 25
           )
     select *
     from (
      select cur.sql_id c_sql_id,
             cur.rnum c_rnum,
             nvl(to_char(prev.rnum), 'N/A') p_rnum,
             cur.database_name c_db,
             cur.tps c_tps,
             nvl(to_char(prev.tps), 'N/A') p_tps,
             cur.ms_pe c_ms,
             nvl(to_char(prev.ms_pe), 'N/A') p_ms,
             cur.sql_text c_text,
             cur.module c_module
        from cur, prev
       where cur.database_name = prev.database_name (+)
        and cur.sql_id = prev.sql_id (+)
      order by cur.rnum)
     where  p_rnum = 'N/A'
   )
   LOOP
      for r2 in (
        with qs as 
            (select /*+ parallel(8) */  distinct t1.sql_id sql1, t1.module module1, q1.database_name db_t1, t2.sql_id sql2, t2.module module2, 
             UTL_MATCH.edit_distance_similarity (DBMS_LOB.substr(t1.sql_text,4000, 1), DBMS_LOB.substr(t2.sql_text, 4000, 1)) score
             from test_result_sql q1, test_result_sqltext t1, test_result_sql q2, test_result_sqltext t2
             where q1.sql_id = t1.sql_id
             and q2.sql_id = t2.sql_id
             and q1.database_name = q2.database_name
             and q1.database_name = upper(r1.c_db)
             and t1.sql_id = r1.c_sql_id
             and q1.sql_id != q2.sql_id
             and q1.test_id = :b1
             and q2.test_id = :b2)
         select sql1, module1, db_t1, sql2, module2, score
         from qs
         where score  between 85 and 100 
         order by score desc
      ) loop
         dbms_output.put_line('|'||r2.db_t1||'|'||r2.sql1||'|'||r2.sql2||'|'||r2.score||'|');
        end loop;      
   END LOOP ;
dbms_output.put_line('End of Report');   
end; 
/



spool off


