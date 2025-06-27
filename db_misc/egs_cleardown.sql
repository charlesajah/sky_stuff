-- script developed by Heatlie, Douglas (Support Engineer)
-- will manage volumes in the database

alter session set ddl_lock_timeout = 300 ;
alter session set nls_date_format = 'dd/mm/yyyy hh24:mi:ss';

set serveroutput on head off lines 150 pages 99

select 'Start Time : ' || to_char(sysdate,'dd/mm/yyyy hh24:mi:ss') from dual;

col owner for a15
col table_name for a25
col label for a25
col mind for a30
col maxd for a30

set head on 
select owner, table_name, num_rows, blocks from all_tables where owner = 'CMS_OWNER';

select /*+ parallel(4) */ min(n.created) as MinD, max(n.created) as MaxD, 'Notification' as Label from cms_owner.notification n
union
select /*+ parallel(4) */ min(col.created) as MinD, max(col.created) as MaxD, 'Carrier_Order_Line' as Label  from cms_owner.carrier_order_line col
union
select /*+ parallel(4) */ min(co.created) as MinD, max(co.created)  as MaxD, 'Carrier_Order' as Label from cms_owner.carrier_order co
union
select /*+ parallel(4) */ min(mar.created) as MinD, max(mar.created)  as MaxD, 'Message_Audit_Request' as Label from CMS_OWNER.message_audit_request mar;

select /*+ parallel(4) */ count(*), 'Notification' as Label from cms_owner.notification n
union
select /*+ parallel(4) */ count(*), 'Carrier_Order_Line' as Label  from cms_owner.carrier_order_line col
union
select /*+ parallel(4) */ count(*), 'Carrier_Order' as Label from cms_owner.carrier_order co
union
select /*+ parallel(4) */ count(*), 'Message_Audit_Request' as Label from CMS_OWNER.message_audit_request mar;

declare 
    Max_Rows_Processed NUMBER;
    Time_interval NUMBER;
    Rows_To_Process NUMBER;
    Tot_Rows_To_Process NUMBER;

    START_TIME DATE;
    CURR_TIME DATE;
    END_TIME DATE;
    
    Max_Consignment_Date DATE;
    
    type consignment_no_collection is TABLE Of Varchar(48);
    consignment_no consignment_no_collection;
begin
    Max_Rows_Processed := 500;
    dbms_output.put_line('Number of rows to process per transaction : ' || Max_Rows_Processed);
	
    Time_interval := 30/1440; --5mins 5/1440 --5mins 1/1440 60s 1/8640 --10s 1/8640 30 mins 1/48
    dbms_output.put_line('Time interval restriction, in a fraction of a day, for all transactions: ' || Time_interval); --10s 1/8640 30 mins 1/48
    
    select max(c_no.created) into Max_Consignment_Date
    from
    (
        select co.created 
        from CMS_OWNER.carrier_order co join CMS_OWNER.carrier_order_line col on co.consignmentno = col.consignmentno
        where co.created <= ( systimestamp - 30 )
        Order by co.created
    ) c_no
    where rownum <=
    (
        SELECT ROUND(( count(co2.consignmentno) / 11 ), 0) -- [ 1/11 for 9.09% for weekly run: 1/75 1.33% for daily run ]
        from CMS_OWNER.carrier_order co2 join CMS_OWNER.carrier_order_line col2 on co2.consignmentno = col2.consignmentno
    );
    
    select count(c_no.consignmentno) into Tot_Rows_To_Process
    from
    (
        select co.consignmentno
        from CMS_OWNER.carrier_order co join CMS_OWNER.carrier_order_line col on co.consignmentno = col.consignmentno
        where co.created <= Max_Consignment_Date
        Order by co.created
    ) c_no
    where rownum <=
    (
        SELECT ROUND(( count(co2.consignmentno) / 11 ), 0) -- [ 1/11 for 9.09% for weekly run: 1/75 1.33% for daily run ]
        from CMS_OWNER.carrier_order co2 join CMS_OWNER.carrier_order_line col2 on co2.consignmentno = col2.consignmentno
    );
    
    dbms_output.put_line('Total Consignment_no : ' || Tot_Rows_To_Process); 
    
    select c_no.consignmentno
    bulk collect into consignment_no
    from
    (
        select co.consignmentno
        from CMS_OWNER.carrier_order co join CMS_OWNER.carrier_order_line col on co.consignmentno = col.consignmentno
        where co.created <= Max_Consignment_Date
        Order by co.created
    ) c_no
    where rownum <= Max_Rows_Processed;
    
    Rows_To_Process := consignment_no.Count;
    
    select systimestamp INTO START_TIME from dual;
    dbms_output.put_line('Start time : ' || START_TIME);
    
    select systimestamp INTO CURR_TIME from dual;
    
    While (CURR_TIME < START_TIME + Time_interval) AND Rows_To_Process > 0 Loop

        For i in 1..consignment_no.Count Loop        
            -- Set savepoint            
            SAVEPOINT delete_start;
            --/*
            
            DELETE
            from CMS_OWNER.notification n
            where n.consignmentno = consignment_no(i);
            
            DELETE
            from CMS_OWNER.carrier_order_line col
            where col.consignmentno = consignment_no(i);
            
            DELETE
            from CMS_OWNER.carrier_order co
            where co.consignmentno = consignment_no(i);
            
            --Nominal nos of records matching. Most are null. Action done as single delete later.
            --DELETE
            --FROM CMS_OWNER.MESSAGE_AUDIT_REQUEST MAR
            --WHERE MAR.consignmentno = consignment_no(i);
                        
            --*/
            COMMIT;
        
            if i = 1 then            
                dbms_output.put_line('Consignment_no : ' || consignment_no(Rows_To_Process) || '  ' || Rows_To_Process);                
            end if;
        End Loop;
    
        select c_no.consignmentno
        bulk collect into consignment_no
        from
        (
            select co.consignmentno
            from CMS_OWNER.carrier_order co join CMS_OWNER.carrier_order_line col on co.consignmentno = col.consignmentno
            where co.created <= Max_Consignment_Date
            Order by co.created
        ) c_no
        where rownum <= Max_Rows_Processed;
    
        Rows_To_Process := consignment_no.Count;
        
        select systimestamp INTO CURR_TIME from dual;
    End Loop;
    
    --Coalesce Indexes post deletion of data in main tables.
    --EGS Index coalesce script  
    --execute immediate  'ALTER INDEX CMS_OWNER._';
    execute immediate  'ALTER INDEX CMS_OWNER.CARRIER_ORDER_CONSIGNMENTNOREF COALESCE';
    execute immediate  'ALTER INDEX CMS_OWNER.CARRIER_ORDER_LINE_PK COALESCE';
    execute immediate  'ALTER INDEX CMS_OWNER.CARRIER_ORDER_PK COALESCE';
    --execute immediate  'ALTER INDEX CMS_OWNER.MESSAGE_AUDIT_REQUEST_PK COALESCE';
    --execute immediate  'ALTER INDEX CMS_OWNER.MSG_AUDIT_CONSIGNMENTNO COALESCE';
    execute immediate  'ALTER INDEX CMS_OWNER.NOTIFICATION_CONSIGNMENTNO COALESCE';
    execute immediate  'ALTER INDEX CMS_OWNER.NOTIFICATION_PK COALESCE';
    execute immediate  'ALTER INDEX CMS_OWNER.NOTIFICATION_TRACKINGNO COALESCE';
    execute immediate  'ALTER INDEX CMS_OWNER.ORDER_LINE_CONSIGNMENTNO COALESCE';
    execute immediate  'ALTER INDEX CMS_OWNER.ORDER_LINE_LINEREFERENCE COALESCE';
    --DDL statements, implicit commits for all above.
    
    dbms_output.put_line('');
    
    select count(c_no.consignmentno) into Rows_To_Process
    from
    (
        select co.consignmentno
        from CMS_OWNER.carrier_order co join CMS_OWNER.carrier_order_line col on co.consignmentno = col.consignmentno
        where co.created <= Max_Consignment_Date
        Order by co.created
    )c_no;
    
    if Tot_Rows_To_Process <> 0 then
      
        dbms_output.put_line('Rows to process: ' || Rows_To_Process );
        dbms_output.put_line('Proportion processed: ' || ( ((Tot_Rows_To_Process - Rows_To_Process)*100)/ Tot_Rows_To_Process) || '%.');
    else
    
        dbms_output.put_line('No rows processsed.');
    end if;
    
    dbms_output.put_line('Process completed.');
    
    select systimestamp INTO END_TIME from dual;
    dbms_output.put_line('End time : ' || END_TIME);
    
EXCEPTION
    WHEN others THEN
    --Rollback to savepoint.
    ROLLBACK TO delete_start;
    dbms_output.put_line('Database error, delete rolled back.');
    dbms_output.put_line(SQLERRM);
    --Any uncommitted work, rolled back to the savepoint, is to be committed.
    COMMIT;
    
    dbms_output.put_line('');
     
    dbms_output.put_line('Rows to process: ' || Rows_To_Process);
    
    select systimestamp INTO END_TIME from dual;
    dbms_output.put_line('End time : ' || END_TIME);
 END;
/

declare 
    MAX_MAR_ID NUMBER;

    Max_Rows_Processed NUMBER;
    Time_interval NUMBER;
    MAX_ID NUMBER;
    Tot_Rows_To_Process NUMBER;
    Remaining_Rows NUMBER;
    
    START_TIME DATE;
    CURR_TIME DATE;
    END_TIME DATE;
begin
    Max_Rows_Processed := 500;
    dbms_output.put_line('Number of rows to process per transaction : ' || Max_Rows_Processed);
	
    Time_interval := 30/1440; --10s 5/1440 --5mins 1/1440 60s 1/8640 --10s 1/8640 30 mins 1/48
    dbms_output.put_line('Time interval restriction, in a fraction of a day, for all transactions: ' || Time_interval); --10s 1/8640 30 mins 1/48
    
    SELECT MAX(M_ID.ID) INTO MAX_ID
    FROM
    (
        SELECT MAR.ID
        FROM CMS_OWNER.MESSAGE_AUDIT_REQUEST MAR
        WHERE MAR.CREATED <= ( systimestamp - 30 )
        ORDER BY MAR.ID
    ) M_ID
    WHERE ROWNUM <=
    (
        SELECT ROUND(( count(MAR2.ID) / 11 ), 0) -- [ 1/11 for 9.09% for weekly run: 1/75 1.33% for daily run ]
        FROM CMS_OWNER.MESSAGE_AUDIT_REQUEST MAR2
    );
    
    dbms_output.put_line('MAX_ID:  ' || MAX_ID);
    
    SELECT count(MAR.ID) INTO Remaining_Rows 
    FROM CMS_OWNER.MESSAGE_AUDIT_REQUEST MAR
    WHERE MAR.ID <= MAX_ID;
    
    Tot_Rows_To_Process := Remaining_Rows;
    dbms_output.put_line('Rows to process: ' || Tot_Rows_To_Process);
    
    select systimestamp INTO START_TIME from dual;
    dbms_output.put_line('Start time : ' || START_TIME);
    
    select systimestamp INTO CURR_TIME from dual;
    
    While (CURR_TIME < START_TIME + Time_interval) AND Remaining_Rows > 0 Loop    
            SELECT MAX(M_ID.ID) INTO MAX_MAR_ID
            FROM
            (
                SELECT MAR.ID
                FROM CMS_OWNER.MESSAGE_AUDIT_REQUEST MAR
                WHERE MAR.CREATED <= ( systimestamp - 30 )
                ORDER BY MAR.ID
            ) M_ID
            WHERE ROWNUM <= Max_Rows_Processed;
            
            dbms_output.put_line('');
            dbms_output.put_line('MAX_MAR_ID : ' || MAX_MAR_ID);
            
            -- Set savepoint            
            SAVEPOINT delete_start_2;
            --/*
            
            DELETE
            FROM CMS_OWNER.MESSAGE_AUDIT_REQUEST MAR
            WHERE MAR.ID <= MAX_MAR_ID;
                        
            --*/

            COMMIT;

            SELECT count(MAR.ID) INTO Remaining_Rows 
            FROM CMS_OWNER.MESSAGE_AUDIT_REQUEST MAR
            WHERE MAR.ID <= MAX_ID;
    
            select systimestamp INTO CURR_TIME from dual;    
    End Loop;
    
    --Coalesce Indexes post deletion of data in main tables.
    --EGS Index coalesce script
    execute immediate  'ALTER INDEX CMS_OWNER.MESSAGE_AUDIT_REQUEST_PK COALESCE';
    execute immediate  'ALTER INDEX CMS_OWNER.MSG_AUDIT_CONSIGNMENTNO COALESCE';
    --DDL statements, implicit commits for all above.
    
    dbms_output.put_line('');
    
    if Tot_Rows_To_Process <> 0 then     
        dbms_output.put_line('Remaining rows to process: ' || Remaining_Rows);
        dbms_output.put_line('Proportion processed: ' || ( ((Tot_Rows_To_Process - Remaining_Rows)*100)/ Tot_Rows_To_Process) || '%.');
    else    
        dbms_output.put_line('No rows processsed.');
    end if;
    
    dbms_output.put_line('Process completed.');
    
    select systimestamp INTO END_TIME from dual;
    dbms_output.put_line('End time : ' || END_TIME);
    
EXCEPTION
    WHEN others THEN
    --Rollback to savepoint.
    ROLLBACK TO delete_start_2;
    dbms_output.put_line('Database error, delete rolled back.');
    dbms_output.put_line(SQLERRM);
    --Any uncommitted work, rolled back to the savepoint, is to be committed.
    COMMIT;
    
    dbms_output.put_line('');
     
    dbms_output.put_line('Remaining rows to process: ' || Remaining_Rows);
    
    select systimestamp INTO END_TIME from dual;
    dbms_output.put_line('End time : ' || END_TIME);
 END;
/

exec dbms_stats.gather_table_stats('CMS_OWNER','NOTIFICATION');
exec dbms_stats.gather_table_stats('CMS_OWNER','CARRIER_ORDER_LINE');
exec dbms_stats.gather_table_stats('CMS_OWNER','CARRIER_ORDER');
exec dbms_stats.gather_table_stats('CMS_OWNER','MESSAGE_AUDIT_REQUEST');


select owner, table_name, num_rows, blocks from all_tables where owner = 'CMS_OWNER';

select /*+ parallel(4) */ min(n.created) as MinD, max(n.created) as MaxD, 'Notification' as Label from cms_owner.notification n
union
select /*+ parallel(4) */ min(col.created) as MinD, max(col.created) as MaxD, 'Carrier_Order_Line' as Label  from cms_owner.carrier_order_line col
union
select /*+ parallel(4) */ min(co.created) as MinD, max(co.created)  as MaxD, 'Carrier_Order' as Label from cms_owner.carrier_order co
union
select /*+ parallel(4) */ min(mar.created) as MinD, max(mar.created)  as MaxD, 'Message_Audit_Request' as Label from CMS_OWNER.message_audit_request mar;

select /*+ parallel(4) */ count(*), 'Notification' as Label from cms_owner.notification n
union
select /*+ parallel(4) */ count(*), 'Carrier_Order_Line' as Label  from cms_owner.carrier_order_line col
union
select /*+ parallel(4) */ count(*), 'Carrier_Order' as Label from cms_owner.carrier_order co
union
select /*+ parallel(4) */ count(*), 'Message_Audit_Request' as Label from CMS_OWNER.message_audit_request mar;


set head off
select 'End Time : ' || to_char(sysdate,'dd/mm/yyyy hh24:mi:ss') from dual;
