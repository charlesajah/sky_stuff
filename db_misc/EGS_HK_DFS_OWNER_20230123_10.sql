alter session set nls_date_format = 'dd/mm/yyyy hh24:mi:ss';

SET SERVEROUTPUT ON

declare

    Max_Rows_Processed NUMBER;
    Time_interval NUMBER;
    Rows_To_Process NUMBER;
    Tot_Rows_To_Process NUMBER;

    START_TIME DATE;
    CURR_TIME DATE;
    END_TIME DATE;

    Max_Fulfilment_Request_Date DATE;

    type Fulfilment_Request_ID_Collection is TABLE Of Varchar(48);
    Fulfilment_Request_ID Fulfilment_Request_ID_Collection;

    type Fulfilment_Manifest_ID_Collection is TABLE Of Varchar(48);
    Fulfilment_Manifest_ID Fulfilment_Manifest_ID_Collection;

begin
    Max_Rows_Processed := 1000;
    dbms_output.put_line('Number of rows to process per transaction : ' || Max_Rows_Processed);

    Time_interval := 5/1440; --5mins 5/1440 --5mins 1/1440 60s 1/8640 --10s 1/8640 30 mins 1/48
    dbms_output.put_line('Time interval restriction, in a fraction of a day, for all transactions: ' || Time_interval); --10s 1/8640 30 mins 1/48

    select max(c_no.created) into Max_Fulfilment_Request_Date
    from
    (
        select fr2.created
        from dfs_owner.fulfilment_request fr2 join dfs_owner.fulfilment_manifest fm2 on fr2.ID = fm2.fulfilment_request_id
        where fr2.created <= ( trunc(systimestamp) - 93 )
        Order by fr2.created
    ) c_no
    where rownum <=
    (
        select ROUND(( count(fr.ID) / 11 ), 0) -- [ 1/11 for 9.09% for weekly run: 1/75 1.33% for daily run ]
        from dfs_owner.fulfilment_request fr join dfs_owner.fulfilment_manifest fm on fr.ID = fm.fulfilment_request_id
    );

    dbms_output.put_line('Max Date : ' || Max_Fulfilment_Request_Date);
---

    select count(c_no.ID) into Tot_Rows_To_Process
    from
    (
        select fr2.ID
        from dfs_owner.fulfilment_request fr2 join dfs_owner.fulfilment_manifest fm2 on fr2.ID = fm2.fulfilment_request_id
        where fr2.created <= Max_Fulfilment_Request_Date
        Order by fr2.created
    ) c_no
    where rownum <=
    (
        select ROUND(( count(fr.ID) / 11 ), 0) -- [ 1/11 for 9.09% for weekly run: 1/75 1.33% for daily run ]
        from dfs_owner.fulfilment_request fr join dfs_owner.fulfilment_manifest fm on fr.ID = fm.fulfilment_request_id
    );

    dbms_output.put_line('Total Fulfilment Request IDs : ' || Tot_Rows_To_Process);

    select c_no.ID
    bulk collect into Fulfilment_Request_ID
    from
    (
        select fr2.ID
        from dfs_owner.fulfilment_request fr2 join dfs_owner.fulfilment_manifest fm2 on fr2.ID = fm2.fulfilment_request_id
        where fr2.created <= Max_Fulfilment_Request_Date
        Order by fr2.created
    ) c_no
    where rownum <= Max_Rows_Processed;

    Rows_To_Process := Fulfilment_Request_ID.Count;

    --dbms_output.put_line(Rows_To_Process || ' ' || Fulfilment_Request_ID(1) );

    select systimestamp INTO START_TIME from dual;
    dbms_output.put_line('Start time : ' || START_TIME);

    select systimestamp INTO CURR_TIME from dual;

    While (CURR_TIME < START_TIME + Time_interval) AND Rows_To_Process > 0 Loop

        For i in 1..Fulfilment_Request_ID.Count Loop

            -- Set savepoint

            SAVEPOINT delete_start;

            --Review dfs_owner.fulfilment_manif_item_status
            select c_no.fulfilment_manifest_id
            bulk collect into Fulfilment_Manifest_ID
            from
            (
                select fmis.fulfilment_manifest_id
                from dfs_owner.fulfilment_manifest fm join dfs_owner.fulfilment_manif_item_status fmis on fm.ID = fmis.fulfilment_manifest_id
                where fm.fulfilment_request_id = Fulfilment_Request_ID(i)
                Union All
                select fm2.ID
                from dfs_owner.fulfilment_request fr2 join dfs_owner.fulfilment_manifest fm2 on fr2.ID = fm2.fulfilment_request_id
                where fm2.fulfilment_request_id = Fulfilment_Request_ID(i)
            ) c_no;

            For j in 1..Fulfilment_Manifest_ID.Count Loop

                --Internal process to clear aggregate data.
                --/*

                DELETE
                FROM DFS_OWNER.FULFILMENT_MANIF_ITEM_STATUS fmis
                WHERE fmis.fulfilment_manifest_id = Fulfilment_Manifest_ID(j);

                DELETE
                FROM DFS_OWNER.FULFILMENT_ATTRIBUTE fa
                WHERE fa.fulfilment_manifest_id = Fulfilment_Manifest_ID(j);

                DELETE
                FROM DFS_OWNER.FULFILMENT_MANIFEST fm
                WHERE fm.id = Fulfilment_Manifest_ID(j);

                --*/

            End Loop;

            --External process to clear top level data.
            --/*

            DELETE
            FROM dfs_owner.fulfilment_request fr
            WHERE fr.id = Fulfilment_Request_ID(i);

            --*/

            if i = 1  then

                dbms_output.put_line('Fulfilment_Request_ID : ' || Fulfilment_Request_ID(i) || '  ' || Rows_To_Process);
            end if;

            COMMIT;

        End Loop;

        select c_no.ID
        bulk collect into Fulfilment_Request_ID
        from
        (
            select fr2.ID
            from dfs_owner.fulfilment_request fr2 join dfs_owner.fulfilment_manifest fm2 on fr2.ID = fm2.fulfilment_request_id
            where fr2.created <= Max_Fulfilment_Request_Date
            Order by fr2.created
        ) c_no
        where rownum <= Max_Rows_Processed;

        Rows_To_Process := Fulfilment_Request_ID.Count;

        select systimestamp INTO CURR_TIME from dual;
    End Loop;

    --Coalesce Indexes post deletion of data in main tables.
    --EGS Index coalesce script
    --execute immediate  'ALTER INDEX DFS_OWNER._';

    execute immediate  'ALTER INDEX DFS_OWNER.FUL_MANIF_ITEM_STATUS_MAN_ID COALESCE';
    execute immediate  'ALTER INDEX DFS_OWNER.FUL_MANIF_ITEM_STATUS_ORDNUM COALESCE';
    execute immediate  'ALTER INDEX DFS_OWNER.FUL_MANIF_ITEM_STATUS_PK COALESCE';

    execute immediate  'ALTER INDEX DFS_OWNER.FULFILMENT_ATTRIBUTE_REQ_ID COALESCE';
    execute immediate  'ALTER INDEX DFS_OWNER.PK_FULFILMENT_ATTRIBUTE COALESCE';
    execute immediate  'ALTER INDEX DFS_OWNER.FULFILMENT_ATTRIBUTE_MAN_ID COALESCE';

    execute immediate  'ALTER INDEX DFS_OWNER.FULFILMENT_MANIFEST_LINE_REF COALESCE';
    execute immediate  'ALTER INDEX DFS_OWNER.FULFILMENT_MANIFEST_RETURNREF COALESCE';
    execute immediate  'ALTER INDEX DFS_OWNER.FULFILMENT_MANIFEST_LINEREF COALESCE';
    execute immediate  'ALTER INDEX DFS_OWNER.FULFILMENT_MANIFEST_PK COALESCE';
    execute immediate  'ALTER INDEX DFS_OWNER.FULFILMENT_MANIFEST_REQ_ID COALESCE';

    execute immediate  'ALTER INDEX DFS_OWNER.FULFILMENT_REQUEST_PK COALESCE';
    execute immediate  'ALTER INDEX DFS_OWNER.FULFILMENT_REQUEST_REF_ID COALESCE';
    execute immediate  'ALTER INDEX DFS_OWNER.FULFILMENT_REQUEST_ORI_REF_ID COALESCE';
    --DDL statements, implicit commits for all above.

    dbms_output.put_line('');

    select count(c_no.ID) into Rows_To_Process
    from
    (
        select fr2.ID
        from dfs_owner.fulfilment_request fr2 join dfs_owner.fulfilment_manifest fm2 on fr2.ID = fm2.fulfilment_request_id
        where fr2.created <= Max_Fulfilment_Request_Date
        Order by fr2.created
    ) c_no;

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
end;
/
