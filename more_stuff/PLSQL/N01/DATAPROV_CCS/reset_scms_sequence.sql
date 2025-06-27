--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure RESET_SCMS_SEQUENCE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."RESET_SCMS_SEQUENCE" 
is
    l_value number;
begin

-- Select the next value of the sequence

    execute immediate
    'select dataprov.scms_seq.nextval from dual' INTO l_value;

-- Set a negative increment for the sequence,
-- with value = the current value of the sequence

    execute immediate
    'alter sequence dataprov.scms_seq increment by -' || l_value || ' minvalue 0';

-- Select once from the sequence, to
-- take its current value back to 0

    execute immediate
    'select dataprov.scms_seq.nextval from dual' INTO l_value;

-- Set the increment back to 1

    execute immediate
    'alter sequence dataprov.scms_seq increment by 1 minvalue 0';
end;

/

  GRANT EXECUTE ON "DATAPROV"."RESET_SCMS_SEQUENCE" TO "BATCHPROCESS_USER";
