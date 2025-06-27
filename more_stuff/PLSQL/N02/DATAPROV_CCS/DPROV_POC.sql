CREATE OR REPLACE PACKAGE DPROV_POC AS 
  procedure data_account_sql(v_testname in varchar2, v_rec_out  out sys_refcursor);
  procedure data_account_seq(v_testname in varchar2, v_rec_out  out sys_refcursor);
  procedure build_pool(v_testname in varchar2);
  procedure top_up_pool(v_testname in varchar2, v_rows in number := null) ;
  function  available_records(v_testname in varchar2) return number;
  function  pool_remaining(v_testname in varchar2) return number ;


END DPROV_POC;
/


CREATE OR REPLACE PACKAGE body DPROV_POC AS 

  g_defaultCols varchar2(100) := 'accountnumber,partyId,username,nsprofileid' ;


  PROCEDURE seqBefore ( i_pool IN VARCHAR2 , i_top boolean, o_count OUT NUMBER ) IS
    l_count NUMBER ;
  BEGIN
    o_count := 0 ;

    if i_top = TRUE then
      select max(seq_no) 
        into o_count 
        from ALEX_CUS_NEW
       where pool = upper(i_pool);
    end if;
    
    SELECT COUNT(*) INTO l_count FROM user_sequences s WHERE s.sequence_name = 'S' || UPPER ( TRIM ( i_pool ) ) ;
    IF l_count > 0 THEN
      EXECUTE IMMEDIATE 'DROP SEQUENCE dataprov.s' || i_pool ;
    END IF ;
  END seqBefore ;

  PROCEDURE seqAfter ( i_pool IN VARCHAR2 , i_burn IN VARCHAR2 , i_count IN NUMBER, i_start IN number := 1) IS
    -- deals with ORA-04013: number to CACHE must be less than one cycle
  BEGIN
    EXECUTE IMMEDIATE 'CREATE SEQUENCE dataprov.s' || i_pool
      || CASE WHEN i_burn = 'Y' THEN ' NOCYCLE ' ELSE ' CYCLE ' END 
      || ' MINVALUE ' || i_start || ' MAXVALUE ' || TO_CHAR ( GREATEST ( 3 , i_count ) )
      || ' CACHE ' || TO_CHAR ( LEAST ( 10000 , GREATEST ( 2 , i_count - 1 ) ) )
      ;
  END seqAfter ;

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
     where datapool_name = upper(v_testname);
    
    EXECUTE IMMEDIATE v_stmt into vrowid; 
   
    --get column which need for datapool
    --select columns into l_columns from ALEX_CUS_NEW_DPROV_CONFIG where DATAPOOL_NAME = v_testname;
   
    v_stmt1 :=   'select ' || l_columns   || ' from dataprov.alex_cus_new where rowid = ''' ||vrowid || '''';
    --DBMS_OUTPUT.PUT_LINE ('v_stmt1 : '  || v_stmt1);  
     
    if l_flg = 'Y' then
      insert /*+ APPEND */ into ALEX_CUS_USED 
      select accountnumber, systimestamp, null 
        from alex_cus_new
       where rowid = vrowid ;
      COMMIT;
    end if;  
    open v_rec_out for v_stmt1   ;
  end data_account_sql;

  procedure data_account_seq(v_testname in varchar2, v_rec_out  out sys_refcursor) is
    l_nextVal NUMBER ;
    l_columns varchar2 (1000) := ' * ';
    l_burn    varchar2(1) := 'Y' ;
    v_stmt1   varchar2(1000) := '';
    v_stmt2   varchar2(1000) := '';
  BEGIN
    select columns, burnable_flag into l_columns, l_burn 
      from ALEX_CUS_NEW_DPROV_CONFIG 
     where datapool_name = upper(v_testname);
     
   if l_columns = 'DEFAULT' then
     l_columns := g_defaultCols;
  else
    l_columns := g_defaultCols ||','||l_columns;
   end if;
  
    EXECUTE IMMEDIATE 'SELECT s' || LOWER ( v_testname ) || '.NEXTVAL FROM DUAL' INTO l_nextVal ;    
    v_stmt1 := 'select ' || l_columns || ' FROM alex_cus_new a WHERE a.seq_no = ' || l_nextVal || ' AND a.pool = upper(''' || v_testname || ''')';
    open v_rec_out for v_stmt1 ;  
    
    if l_burn = 'Y' then
      v_stmt2 := 'select accountnumber FROM alex_cus_new a WHERE a.seq_no = ' || l_nextVal || ' AND a.pool = upper(''' || v_testname || ''')';
      EXECUTE IMMEDIATE v_stmt2 into l_columns;
      insert /*+ append */ into alex_cus_used values (l_columns, systimestamp, upper(v_testname));   
      commit;
    end if;  
  END data_account_seq;
  
  ------------------------------------------------------------------------------------------------------------------------------
  -- Management Of Pools
  -- BUILD_POOL        : Will build a pool based on the criteria in the config table
  -- TOP_UP_POOL       : Will add a number of records to the pool, if 2nd parameters in not specified it will add 25% of the 
  --                     initial total to the pool 
  -- AVAILABLE_RECORDS : Will calculate how many records meet the specified criteria in the config table
  ------------------------------------------------------------------------------------------------------------------------------
  /*
  ###########################################################################################################################
  # This will rebuild a pool from scratch and removed any used records from the usage table
  ###########################################################################################################################
  */
  procedure build_pool(v_testname in varchar2) is
    l_burn    varchar2(1) := 'Y' ;  -- set to Y if you want this pool to burn and return 'no data found', instead of cycling around data.
    l_count   NUMBER ;
    l_numrows number := 500;
    l_query   varchar2(5000) := '';
    l_head    varchar2(1000) := '';
    l_remain  number := 0;
    --l_trail   varchar2(1000) := ' and not exists (select 1 from alex_cus_used au where au.accountnumber = ac.accountnumber)';
  BEGIN
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML' ;
    
    select num_rows, sql_qry, burnable_flag 
      into l_numrows, l_query, l_burn
      from ALEX_CUS_NEW_DPROV_CONFIG 
     where datapool_name = upper(v_testname);      
     
    select pool_remaining(upper(v_testname))
      into l_remain
      from dual;
      
    if l_remain < l_numrows then     
      -- NEED TO CHANGE THIS CODE !!! SEE NOTES IN DIARY !!!
      UPDATE /*+ parallel(ac, 8) */ alex_cus_new ac SET ac.pool = NULL , ac.seq_no = NULL WHERE ac.pool = upper(v_testname) ;
      delete from alex_cus_used where pool = upper(v_testname);
      commit;
    
      seqBefore ( i_pool => upper(v_testname), i_top => FALSE, o_count => l_count ) ;      
      l_head := 'update /*+ parallel(ac, 8) */ alex_cus_new ac set ac.pool = ''' || upper(v_testname) || ''', ac.seq_no = ' || l_count || ' + ROWNUM ';
      l_query := l_head || ' ' || l_query || ' and pool is null and rownum <= ' || l_numrows ;
      EXECUTE IMMEDIATE l_query;
      l_count := l_count + SQL%ROWCOUNT ;
      commit;
      
      seqAfter ( i_pool => upper(v_testname), i_burn => l_burn , i_count => l_count ) ;
    else
      dbms_output.put_line('POOL ' || v_testname || ' DOES NOT NEED REBUILT, STOP BEING LAZY !!!');
    end if;
  end build_pool; 

  /*
  ###########################################################################################################################
  # This will add more records to the pool but not rebuild the pool entirely. It will not re-add used accounts
  ###########################################################################################################################
  */
  procedure top_up_pool(v_testname in varchar2, v_rows in number := null) is
    l_burn    varchar2(1) := 'Y' ;  -- set to TRUE if you want this pool to burn and return 'no data found', instead of cycling around data.
    l_count   NUMBER ;
    l_start   NUMBER := 1;
    l_numrows number := 100;
    l_query   varchar2(5000) := '';
    l_head    varchar2(1000) := '';
    --l_trail   varchar2(1000) := ' and not exists (select 1 from alex_cus_used au where au.accountnumber = ac.accountnumber)';
  begin
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML' ;
    
    if v_rows is null then
      select round(num_rows*0.25), sql_qry, burnable_flag into l_numrows, l_query, l_burn
        from ALEX_CUS_NEW_DPROV_CONFIG 
       where datapool_name = upper(v_testname);
    else
      select sql_qry, burnable_flag into l_query, l_burn
        from ALEX_CUS_NEW_DPROV_CONFIG 
       where datapool_name = upper(v_testname);
       
      l_numrows := v_rows; 
    end if;
     
    seqBefore ( i_pool => upper(v_testname), i_top => TRUE, o_count => l_count ) ; 
    l_start := l_count;
    
    l_head := 'update /*+ parallel(ac, 8) */ alex_cus_new ac set ac.pool = ''' || upper(v_testname) || ''', ac.seq_no = ' || l_count || ' + ROWNUM ';
    l_query := l_head || ' ' || l_query || ' and pool is null and rownum <= ' || l_numrows ;
    EXECUTE IMMEDIATE l_query;
    l_count := l_count + SQL%ROWCOUNT ;
    commit;
    
    select nvl((select aa.seq_no+1
      from alex_cus_new aa, 
           (select oldest 
              from (select first_value(accountnumber) over (partition by pool order by useddate desc) as oldest
                      from alex_cus_used
                     where pool = upper(v_testname))
             where rownum < 2) bb
     where aa.accountnumber = bb.oldest) ,1)
      into l_start
      from dual;

    seqAfter ( i_pool => upper(v_testname), i_burn => l_burn , i_count => l_count, i_start => l_start ) ;    
  end top_up_pool;

  /*
  ###########################################################################################################################
  # This will calculate the total number of records available for the pool in the database that are not currently 
  # allocated to another pool
  ###########################################################################################################################
  */
  function available_records(v_testname in varchar2) return number is
    l_count   NUMBER ;
    l_query   varchar2(5000) := '';
    l_head    varchar2(1000) := '';
  begin
    select sql_qry into l_query
      from ALEX_CUS_NEW_DPROV_CONFIG 
     where datapool_name = v_testname;  

    l_head := 'select /*+ parallel(ac, 8) */ count(*) from alex_cus_new ac ' ;
    
    l_query := l_head || ' ' || l_query || ' and pool is null';
    EXECUTE IMMEDIATE l_query into l_count;
     
    return l_count;
  end;

  /*
  ###########################################################################################################################
  # This will calculate how many records are left in the pool
  ###########################################################################################################################
  */
  function pool_remaining(v_testname in varchar2) return number is
    l_rowsused  number := 0;
    l_totalrows number := 0;
    l_remain    number := 0;
  begin
    select nvl((select aa.seq_no  
                  from alex_cus_new aa, 
                       (select oldest 
                          from (select first_value(accountnumber) over (partition by pool order by useddate desc) as oldest
                                  from alex_cus_used
                                 where pool = upper('TEST2'))
                         where rownum < 2) bb
                 where aa.accountnumber = bb.oldest),0) 
      into l_rowsused
      from dual ;
    
    select nvl(max(seq_no),0)
      into l_totalrows
      from alex_cus_new 
     where pool = upper(v_testname);
     
    l_remain := l_totalrows - l_rowsused;
    return l_remain;      
    
  end pool_remaining;
END DPROV_POC;
/
