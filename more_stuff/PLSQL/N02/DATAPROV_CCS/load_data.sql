--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Function LOAD_DATA
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "DATAPROV"."LOAD_DATA" (p_table     in varchar2,
                                     p_cnames    in varchar2,
                                     p_dir       in varchar2,
                                     p_filename  in varchar2,
                                     p_delimiter in varchar2 default ',')
  return number is
  l_input     utl_file.file_type;
  l_theCursor integer default dbms_sql.open_cursor;
  l_buffer    varchar2(4000);
  l_lastLine  varchar2(4000);
  l_status    integer;
  l_colCnt    number default 0;
  l_cnt       number default 0;
  l_sep       char(1) default NULL;
  l_errmsg    varchar2(4000);
begin
  l_input := utl_file.fopen(p_dir, p_filename, 'r');

  l_buffer := 'insert into ' || p_table || ' values ( ';
  l_colCnt := length(p_cnames) - length(replace(p_cnames, ',', '')) + 1;

  for i in 1 .. l_colCnt loop
    l_buffer := l_buffer || l_sep || ':b' || i;
    l_sep    := ',';
  end loop;
  l_buffer := l_buffer || ')';

  dbms_sql.parse(l_theCursor, l_buffer, dbms_sql.native);

  loop
    begin
      utl_file.get_line(l_input, l_lastLine);
    exception
      when NO_DATA_FOUND then
        exit;
    end;
    l_buffer := l_lastLine || p_delimiter;
    for i in 1 .. l_colCnt loop
      dbms_sql.bind_variable(l_theCursor,
                             ':b' || i,
                             substr(l_buffer,
                                    1,
                                    instr(l_buffer, p_delimiter) - 1));
      l_buffer := substr(l_buffer, instr(l_buffer, p_delimiter) + 1);        
    end loop;
    begin
      l_status := dbms_sql.execute(l_theCursor);
      l_cnt    := l_cnt + 1;
    exception
      when others then
        NULL;
    end;
  end loop;
  utl_file.fclose(l_input);
  commit;

  return l_cnt;
end load_data;

/
