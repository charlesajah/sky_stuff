--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure SAVE_SCHEMA_METADATA2
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "HP_DIAG"."SAVE_SCHEMA_METADATA2" (p_array_size IN PLS_INTEGER DEFAULT 100)
IS
TYPE ARRAY IS TABLE OF c%ROWTYPE;
l_data ARRAY;

CURSOR c IS (select name  
                 from user_source  
                where  lower(type) in ('package', 'package body', 'procedure', 'function')
               -- lower(type) in lower(ots(ot))   
                  and lower(name) not in ('metadata_version2','save_schema_metadata'));

BEGIN
    OPEN c;
    LOOP
    FETCH c BULK COLLECT INTO l_data LIMIT p_array_size;

    FORALL i IN 1..l_data.COUNT
    INSERT INTO metadata_version2 VALUES l_data(i);

    EXIT WHEN c%NOTFOUND;
    END LOOP;
    CLOSE c;
END ;

/
