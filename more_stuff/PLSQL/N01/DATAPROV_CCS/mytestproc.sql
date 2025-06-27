--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure MYTESTPROC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."MYTESTPROC" (i_nfr in number ,i_magic_no in number,i_username varchar2)
as

pragma autonomous_transaction;

begin


UPDATE dp_tele_nfr_test
    SET
        updated_by = 'username',
        last_val = i_nfr
    WHERE
        magic_no = i_magic_no;

    COMMIT;
end;

/

  GRANT EXECUTE ON "DATAPROV"."MYTESTPROC" TO "BATCHPROCESS_USER";
