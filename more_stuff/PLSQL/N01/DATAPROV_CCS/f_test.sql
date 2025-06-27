--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Function F_TEST
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "DATAPROV"."F_TEST" ( p_name IN VARCHAR2) RETURN VARCHAR2
IS
BEGIN
RETURN ( p_name);
END;

/
