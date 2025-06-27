--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Function GET_INDEX_COLUMNS
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "DATAPROV"."GET_INDEX_COLUMNS" (p_index_name IN VARCHAR2) RETURN VARCHAR2
IS
  v_column_string VARCHAR2(2000) := '';
BEGIN
     FOR p_record IN (SELECT column_name
                        FROM user_ind_columns
                       WHERE index_name = p_index_name
                    ORDER BY column_position) LOOP
         v_column_string := v_column_string || p_record.column_name || ', ';
     END LOOP;
     v_column_string := substr(v_column_string,1, length(v_column_string)-2);
     RETURN v_column_string;
EXCEPTION
         WHEN OTHERS THEN RETURN '' ;
END;

/
