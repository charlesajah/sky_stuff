CREATE OR REPLACE PACKAGE DPROV_POC_SFU AS 
  procedure data_account_sql(v_testname in varchar2, v_rec_out  out sys_refcursor);
END DPROV_POC_SFU;
/


CREATE OR REPLACE PACKAGE body DPROV_POC_SFU AS 

 procedure data_account_sql(v_testname in varchar2, v_rec_out  out sys_refcursor) is
    --
    --#############################################################
    --Created: 16/04/2020                                         #
    --Modified: 16/02/2015                                        #
    --Last modification: Added outputted field for pool tracking  #
    --Last modified by:                                           #
    --                                                            #
    --Usage Notes:                                                #
    --                                                            #
    --                                                            #
    --                                                            #
    --#############################################################
    --
    l_columns      varchar2 (1000);
    v_stmt         varchar2 (1000);
    l_code         NUMBER;
    l_msg          VARCHAR2(255);
    l_flg          VARCHAR2(1);
    vrowid         VARCHAR2(255);
    v_stmt1        varchar2 (1000);
    vACCOUNTNUMBER VARCHAR2(255);
    TYPE EmpCurTyp IS REF CURSOR;
  begin
    --get rowid from SQL of the datapool.
    select sql_qry, BURNABLE_FLAG, columns into v_stmt, l_flg, l_columns 
      from ALEX_CUS_NEW_DPROV_CONFIG 
     where datapool_name = v_testname
     for update;

    EXECUTE IMMEDIATE v_stmt into vrowid; 

    --get column which need for datapool
    --select columns into l_columns from ALEX_CUS_NEW_DPROV_CONFIG where DATAPOOL_NAME = v_testname;

    v_stmt1 :=   'select ' || l_columns   || ' from dataprov.alex_cus_new where rowid = ''' ||vrowid || '''';
    --DBMS_OUTPUT.PUT_LINE ('v_stmt1 : '  || v_stmt1);  

    if l_flg = 'Y' then
      insert /*+ APPEND */ into ALEX_CUS_USED 
      select accountnumber, systimestamp, v_testname 
        from alex_cus_new
       where rowid = vrowid ;
      COMMIT;
    end if;  
    open v_rec_out for v_stmt1   ;
  end data_account_sql;

END DPROV_POC_SFU;
/
