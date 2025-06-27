--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure LOAD_VIEW
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "HP_DIAG"."LOAD_VIEW" 
as
begin
EXECUTE IMMEDIATE 'DROP TABLE search_all_views';
            EXECUTE IMMEDIATE 'CREATE TABLE search_all_views 
             AS (select   av.view_name, to_lob(av.text) as text  from    all_VIEWS av)';
    insert into   metadata_version2 (name,TYPE,text_clob,RUN_TIME)
    select view_name,'view',text,sysdate from search_all_views;

end;

/
