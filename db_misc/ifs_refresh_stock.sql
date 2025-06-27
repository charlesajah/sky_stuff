SET SERVEROUTPUT ON
DECLARE
    start_time NUMBER;
    end_time NUMBER;
    elapsed_time NUMBER;
BEGIN
    -- Measure time for refresh_stock_position_high
    DBMS_OUTPUT.PUT_LINE('Calling refresh_stock_position_high..........');
    start_time := DBMS_UTILITY.GET_TIME;
    skyfs.stock_position.refresh_stock_position_high();
    end_time := DBMS_UTILITY.GET_TIME;
    elapsed_time := (end_time - start_time) / 100; -- Converting to seconds from 100th of seconds
    DBMS_OUTPUT.PUT_LINE('Completed refresh_stock_position_high in ' || elapsed_time ||' seconds.') ;   

    -- Measure time for refresh_stock_position_low
    DBMS_OUTPUT.PUT_LINE('Calling refresh_stock_position_low..........');
    start_time := DBMS_UTILITY.GET_TIME;
    skyfs.stock_position.refresh_stock_position_low();
    end_time := DBMS_UTILITY.GET_TIME;
    elapsed_time := (end_time - start_time) / 100; -- Converting to seconds from 100th of seconds
    DBMS_OUTPUT.PUT_LINE('Completed refresh_stock_position_low in ' || elapsed_time ||' seconds.') ;  
    
    EXCEPTION WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error occured: ' || SQLERRM);
END;
/