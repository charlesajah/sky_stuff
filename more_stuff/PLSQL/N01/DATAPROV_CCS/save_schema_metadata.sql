--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure SAVE_SCHEMA_METADATA
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."SAVE_SCHEMA_METADATA" 
is
  type ott is varray(50) of varchar2(32);
  ots ott := ott('package','package body','procedure', 'function', 'trigger');
  l_cnt  number :=0;
  begin

  for ot in ots.first..ots.last
  loop

    for t in 
       (select distinct a.name
                from 
                (select  us.name,us.type,us.text,us.line
                        from user_source us
                       where lower(us.type) in ( 'package','package body', 'procedure', 'function', 'trigger')
                       minus
                select  ms.name,ms.type,ms.text,ms.line  from metadata_source ms
                 where lower(ms.type) in 
                 ( 'package','package body', 'procedure', 'function', 'trigger'))a ) 
    loop
     begin
        insert into metadata_source(NAME,TYPE,LINE,TEXT,RUN_TIME)
        select NAME,TYPE,LINE,TEXT,sysdate
        from user_source
        where lower(type) = lower(ots(ot))
        and upper(name) in (t.name);
        l_cnt :=l_cnt+sql%Rowcount;
     EXCEPTION
        WHEN OTHERS THEN
        dbms_output.put_line('An error was encountered in loop: '
                             || sqlcode
                             || ' - '
                             || sqlerrm);     
     end;      
    end loop;
  end loop;  
  --select count(1)into l_cnt from metadata_source where trunc(run_time) = trunc(sysdate);
  dbms_output.put_line('Rows inserted in table metadata_source = '|| l_cnt);
--Loading tables from user_tab_columns to metadata_tab_columns
    insert into metadata_tab_columns(table_name, column_name,data_type,data_length,data_precision,nullable,column_id,run_time)
    select table_name, column_name,data_type,data_length,data_precision,nullable,column_id,sysdate
    from user_tab_columns
    MINUS
    SELECT table_name, column_name,data_type,data_length,data_precision,nullable,column_id,sysdate 
    FROM metadata_tab_columns ;
    dbms_output.put_line('Rows inserted in table metadata_tab_columns = '||sql%Rowcount);
-- Loading tables from user_indexes,user_ind_columns to metadata_indexes 
    insert into metadata_indexes
    (index_name,table_name,column_name,index_type,
     column_length,column_position,uniqueness,run_time )
    select a.index_name,
    a.table_name,b.column_name,a.index_type,
    b.column_length,b.column_position,a.uniqueness,sysdate 
    from user_indexes a, user_ind_columns b
    where a.index_name= b.index_name
    and a.table_name=b.table_name 
    minus
    select index_name,table_name,column_name,index_type,
     column_length,column_position,uniqueness,sysdate 
    from metadata_indexes;
    dbms_output.put_line('Rows inserted in table metadata_indexes = '||sql%Rowcount);
-- Loading table from user_sequences to metadata_sequences   
    insert into metadata_sequences
    (sequence_name,min_value,max_value,increment_by,cycle_flag,run_time)
    select sequence_name,min_value,max_value,increment_by,cycle_flag,sysdate
    from user_sequences
    minus
    select sequence_name,min_value,max_value,increment_by,cycle_flag,sysdate
    from metadata_sequences;
    dbms_output.put_line('Rows inserted in table metadata_sequences = '||sql%Rowcount);
-- Loading table from user_constraints,user_cons_columns to metadata_constraints       
    insert into metadata_constraints
    (table_name,constraint_name,column_name,constraint_type,status,run_time )
    select b.table_name,b.constraint_name,b.column_name,a.constraint_type,a.status,sysdate 
    from  user_constraints a,user_cons_columns b
    where a.constraint_name=b.constraint_name
    minus
    select table_name,constraint_name,column_name,constraint_type,status,sysdate
    from metadata_constraints;
    dbms_output.put_line('Rows inserted in table metadata_constraints = '||sql%Rowcount);
-- Loading table from user_views to metadata_views    
    insert into metadata_views
    (view_name,text_length,text_vc,run_time )
    select view_name,text_length,text_vc ,sysdate
    from user_views
    minus
    select view_name,text_length,text_vc ,sysdate from metadata_views; 
    dbms_output.put_line('Rows inserted in table metadata_views = '||sql%Rowcount);

  end;


/

  GRANT EXECUTE ON "DATAPROV"."SAVE_SCHEMA_METADATA" TO "BATCHPROCESS_USER";
