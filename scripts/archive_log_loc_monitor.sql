

create or replace directory temp as '/tmp';
grant read on directory temp to hp_diag;

CREATE OR REPLACE TYPE hp_diag.file_line_row AS OBJECT (
    line_id NUMBER,
    line_content VARCHAR2(4000)
);
/

CREATE OR REPLACE TYPE HP_DIAG.file_line_table AS TABLE OF file_line_row;
/


CREATE OR REPLACE FUNCTION hp_diag.read_file(p_directory VARCHAR2 DEFAULT 'TEMP', p_filename VARCHAR2 DEFAULT '/tmp/ora_arch_mon.out')
RETURN file_line_table PIPELINED IS
    file_handle UTL_FILE.FILE_TYPE;
    line VARCHAR2(32767);
    line_id NUMBER := 1; -- Initialize line counter
BEGIN
    file_handle := UTL_FILE.FOPEN(p_directory, p_filename, 'r');

    LOOP
        BEGIN
            UTL_FILE.GET_LINE(file_handle, line);
            -- Pipe the line back as a row
            PIPE ROW(file_line_row(line_id, line));
            line_id := line_id + 1; -- Increment line counter
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                EXIT; -- Exit the loop when no more data is found
        END;
    END LOOP;

    UTL_FILE.FCLOSE(file_handle);
    RETURN;
EXCEPTION
    WHEN OTHERS THEN
        IF UTL_FILE.IS_OPEN(file_handle) THEN --we close the file just in case it is still open
            UTL_FILE.FCLOSE(file_handle);
        END IF;
        RAISE;
END read_file;
/