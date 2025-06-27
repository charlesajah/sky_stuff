--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Function FIND_LOCKED_ROWS
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "DATAPROV"."FIND_LOCKED_ROWS" ( v_rowid rowid, table_name varchar2) return rowid is
x number;
pragma autonomous_transaction;
Begin
execute immediate
'Begin
Select 1 into :x from '||table_name||' where rowid =:v_rowid for update nowait;
Exception
When Others Then
:x:=null;
End;' using out x , v_rowid;
Rollback;
If x=1 Then
Return v_rowid;
Elsif x is null Then
Return null;
End if;
End;

/
