CREATE OR REPLACE PACKAGE LOGGER AS
    PROCEDURE write ( i_item IN VARCHAR2 DEFAULT NULL ) ;
    PROCEDURE debug ( i_item IN VARCHAR2 DEFAULT NULL ) ;
END logger ;

/


CREATE OR REPLACE PACKAGE BODY LOGGER AS
    PROCEDURE write ( i_item IN VARCHAR2 DEFAULT NULL ) IS
       l_line CONSTANT NUMBER := utl_call_stack.unit_line ( dynamic_depth => 2 ) ;
       l_source CONSTANT VARCHAR2(4000) := utl_call_stack.concatenate_subprogram ( qualified_name => utl_call_stack.subprogram ( dynamic_depth => 2 ) ) ;
       PRAGMA AUTONOMOUS_TRANSACTION ;
    BEGIN
        dbms_output.put_line ( i_item || ' - line ' || TO_CHAR(l_line) || ' ' || LOWER(l_source) || ' - ' || TO_CHAR(SYSDATE,'Dy DD-Mon-YYYY HH24:MI:SS')) ;
        INSERT INTO REPORT_LOGS t ( t.item , t.line , t.source , t.dt )
        VALUES ( SUBSTR ( i_item , 1 , 4000 )
           , l_line
           , l_source
           , systimestamp
           ) ;
        COMMIT WRITE BATCH NOWAIT ;
    EXCEPTION WHEN OTHERS THEN NULL ;  -- don't want any failures to write logging info to break the entire calling pl/sql.
    END write ;

    PROCEDURE debug ( i_item IN VARCHAR2 DEFAULT NULL ) IS
       l_line CONSTANT NUMBER := utl_call_stack.unit_line ( dynamic_depth => 2 ) ;
       l_source CONSTANT VARCHAR2(4000) := utl_call_stack.concatenate_subprogram ( qualified_name => utl_call_stack.subprogram ( dynamic_depth => 2 ) ) ;
       PRAGMA AUTONOMOUS_TRANSACTION ;
    BEGIN
        dbms_output.put_line ( i_item ) ;

        INSERT INTO REPORT_DEBUG t ( t.item , t.line , t.source , t.dt )
        VALUES  ( SUBSTR ( i_item , 1 , 4000 )
           , l_line
           , l_source
           , systimestamp
           ) ;
        COMMIT WRITE BATCH NOWAIT ;

       EXCEPTION WHEN OTHERS THEN NULL ;  
    END debug ;
END logger ;
/
