--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure DATA_ISSUE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."DATA_ISSUE" (v_table in varchar2, v_test in varchar2, v_out out sys_refcursor) as
rid rowid;
begin
if upper(v_table)='SHARED' then
  select rowid into rid
   from dataprov.shared_pools t
  where test_alloc=v_test
    and record_state='A'
    and rownum=1
  for update;
update dataprov.shared_pools t
   set record_state='U'
 where rowid=rid;
commit;
open v_out for
select t.*
  from dataprov.shared_pools t
where rowid=rid;
elsif upper(v_table)='EXCLUSIVE' then
  select rowid into rid
   from dataprov.exclusive_pools t
  where test_alloc=v_test
    and record_state='A'
    and rownum=1
  for update;
update dataprov.exclusive_pools t
   set record_state='U'
 where rowid=rid;
commit;
open v_out for
select t.*
  from dataprov.exclusive_pools t
where rowid=rid;
else
 raise_application_error(-20001,'Wrong table type specified. Use EXCLUSIVE or SHARED only');
end if;
end;

/

  GRANT EXECUTE ON "DATAPROV"."DATA_ISSUE" TO "BATCHPROCESS_USER";
