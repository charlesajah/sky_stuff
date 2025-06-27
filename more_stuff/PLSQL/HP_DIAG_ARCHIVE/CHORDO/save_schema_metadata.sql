--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure SAVE_SCHEMA_METADATA
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "HP_DIAG"."SAVE_SCHEMA_METADATA" 
is
  type ott is varray(50) of varchar2(32);
  ots ott := ott('table', 'view', 'package','package body', 'type','sequence', 'sequence_disabled', 'materialized view', 'index', 'procedure', 'function', 'trigger');
  --ots ott := ott('package', 'package body', 'procedure', 'function');
  curDateTime  TIMESTAMP := CURRENT_TIMESTAMP ;
  begin
  DBMS_OUTPUT.PUT_LINE('save_schema_metadata job startd  on '|| curDateTime);
  for ot in ots.first..ots.last
  loop
    for t in 
    /*( select distinct name  
                 from user_source  
                where lower(type) = lower(ots(ot))   
                  --and lower(name) not in ('metadata_version','fetch_schema_metadata')
              order by name )  */

    (select distinct a.name
                from 
                (select  us.name,us.type,us.text
                        from user_source us
                       where lower(us.type) in ('table', 'view', 'package','package body', 'type','sequence', 'sequence_disabled',
                                'materialized view', 'index', 'procedure', 'function', 'trigger')
                       minus
                select  name,type,text  from metadata_version2 )a )  




    loop
    begin
        insert into metadata_version2(NAME,TYPE,LINE,TEXT,RUN_TIME)
        select NAME,TYPE,LINE,TEXT,sysdate
          from user_source
         where lower(type) = lower(ots(ot))
           and upper(name) in (t.name);


      EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('An error was encountered in loop: '
                             || sqlcode
                             || ' - '
                             || sqlerrm);     
     end;      
    end loop;
  end loop;

           --views 
           /*
                insert into metadata_version2(NAME,TYPE,TEXT_LONG ,RUN_TIME)           
           select view_name,'VIEW',TEXT,SYSDATE from user_views; */

           EXECUTE IMMEDIATE 'DROP TABLE search_all_views';
            EXECUTE IMMEDIATE 'CREATE TABLE search_all_views 
             AS (select   av.view_name, to_lob(av.text) as text  from all_VIEWS av)';
            insert into   metadata_version2 (name,TYPE,text_clob,RUN_TIME)
            select view_name,'view',text,sysdate from search_all_views;             
            EXECUTE IMMEDIATE 'truncate table search_all_views';


          --tables 
           insert into metadata_version2(NAME,TYPE,TEXT,RUN_TIME)
           select table_name,'TABLE',column_name ||' '|| data_type ||' ' || data_length as text ,sysdate
            from user_tab_columns
            MINUS
            SELECT NAME,'TABLE',TEXT ,sysdate FROM metadata_version2 WHERE TYPE = 'TABLE';

            --INDEX 
           insert into metadata_version2(NAME,TYPE,TEXT,RUN_TIME)
           select index_name,'INDEX', 'CREATE INDEX ' || i.index_name || ' ON ' || i.table_name 
                   || '(' || get_index_columns(i.index_name) 
                   || ') TABLESPACE INDX;' text,SYSDATE
           from user_indexes i
           MINUS
            SELECT NAME,'INDEX',TEXT ,sysdate FROM metadata_version2 WHERE TYPE = 'INDEX'; 

           --SEQUENCE (create script need to modify)
           insert into metadata_version2(NAME,TYPE,TEXT,RUN_TIME)
           select sequence_name ,'SEQUENCE', 
           'CREATE SEQUENCE '|| sequence_name ||' INCREMENT BY ' || INCREMENT_BY 
           ||' MINVALUE ' || min_value 
           ||' MAXVALUE ' || max_value AS TEXT ,SYSDATE
           from user_sequences
           MINUS
            SELECT NAME,'SEQUENCE',TEXT ,sysdate FROM metadata_version2 WHERE TYPE = 'SEQUENCE';

           --constraints
           insert into metadata_version2(NAME,TYPE,TEXT,RUN_TIME)
           SELECT A.constraint_name,'CONSTRAINT',
                'ALTER TABLE '|| B.TABLE_NAME || ' ADD'|| ' CONSTRAINT '|| A.CONSTRAINT_NAME || ' '|| 
                A.CONSTRAINT_TYPE || ' (' ||B.COLUMN_NAME || ')' AS TEXT,
                SYSDATE
            FROM 
            USER_CONSTRAINTS A,
            USER_CONS_COLUMNS B
            WHERE A.CONSTRAINT_NAME=B.CONSTRAINT_NAME  
             MINUS
            SELECT NAME,'CONSTRAINT',TEXT ,sysdate FROM metadata_version2 WHERE TYPE = 'CONSTRAINT';

           --MATERIALIZED VIEW(create script need to modify)
          /* insert into metadata_version2(NAME,TYPE,text,TEXT_long,RUN_TIME)
           select mview_name,'MATERIALIZED VIEW','check text_long column',query,sysdate from user_mviews;  */


  commit;
  DBMS_OUTPUT.PUT_LINE('save_schema_metadata job completd  on '|| curDateTime);
end;

/
