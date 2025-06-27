--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Function LONG_COLUMN
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "DATAPROV"."LONG_COLUMN" ( p_query in varchar2,
                          p_owner in varchar2,
                          p_owner_value in varchar2,
                          p_name  in varchar2,
                          p_value in varchar2 )
    return clob
    as
        l_cursor       integer default dbms_sql.open_cursor;
        l_n            number;
        l_long_val     clob;
        l_long_piece  clob;
       l_long_len     number;
       l_buflen       number := 32760;
       l_curpos       number := 0;
       l_return_value number;
   begin

dbms_sql.parse( l_cursor, p_query, dbms_sql.native );

       dbms_sql.bind_variable( l_cursor,p_owner, p_owner_value );
       dbms_sql.bind_variable( l_cursor,p_name, p_value );

       dbms_sql.define_column_long(l_cursor, 1);
       l_n := dbms_sql.execute(l_cursor);

       if (dbms_sql.fetch_rows(l_cursor)>0)
       then
           loop
               dbms_sql.column_value_long(l_cursor, 1, l_buflen, l_curpos ,
                                           l_long_val, l_long_len );
               l_curpos := l_curpos + l_long_len;
               l_return_value := nvl(l_return_value,0) + l_long_len;

               exit when l_long_len = 0;

               l_long_piece := l_long_piece||l_long_val; -- added

         end loop;
      end if;
      dbms_sql.close_cursor(l_cursor);  --  added


     return l_long_piece;
   exception
      when others then
        if dbms_sql.is_open(l_cursor) then
            dbms_sql.close_cursor(l_cursor);
         end if;
         raise;
   end long_column;

/
