CREATE OR REPLACE PACKAGE DPROV_UTILS AS 

/*************************************************************************************************
Package local variables to store list of pools
**************************************************************************************************/   
    
    type rec_pools   is record ( level number , poolname  varchar2(30));
    type tab_pools   is table of rec_pools index by pls_integer;

    PROCEDURE Add_Datapool (
        i_poolname      in varchar2
       ,i_endpoint      in varchar2    DEFAULT null 
       ,i_pooldesc      in varchar2
       ,i_pooltype      in varchar2    DEFAULT 'STATIC'
       ,i_columns       in varchar2
       ,i_database      in varchar2    DEFAULT 'chordiant'
       ,i_statdyn       in varchar2    DEFAULT 'S'
       ,i_depend        in varchar2    DEFAULT 'CUSTOMERS'
       ,i_priority      in number      DEFAULT 7
       ,i_package       in varchar2
       ,i_comments      in varchar2    DEFAULT null    
       );

    PROCEDURE RebuildDataPool (
         i_poolname      in varchar2
        ,i_inclDep       in varchar2    DEFAULT 'N'
        );

    PROCEDURE DisableDatapool (
        i_poolname      in varchar2
       ,i_comments      in varchar2    DEFAULT null 
       ,i_inclDep       in varchar2    DEFAULT 'N'
       );

    PROCEDURE EnableDatapool (
        i_poolname      in varchar2
       ,i_comments      in varchar2    DEFAULT null    
       ,i_rebuild       in varchar2    DEFAULT 'N'
       ,i_inclDep       in varchar2    DEFAULT 'N'
       );

    PROCEDURE Delete_Datapool (
        i_poolname      in varchar2
       );
       
    PROCEDURE DisableUnusedDatapools ;
    
END ;
/


CREATE OR REPLACE PACKAGE BODY DPROV_UTILS AS 

/*************************************************************************************************
DropSeqData
When a datapool is being disabled, we need to make sure the assicated sequence is dropped and 
so the data contained within the DPROV_ACCOUNTS_FAST
This will cleared up data and sequence that are no longer required.
**************************************************************************************************/

    PROCEDURE DropSeqData ( i_poolname IN VARCHAR2 ) IS
       l_count NUMBER ;
       l_error VARCHAR2(1000) ;
    BEGIN
       SELECT COUNT(*) INTO l_count FROM user_sequences s WHERE s.sequence_name = 'S' || UPPER ( TRIM ( i_poolname ) ) ;
       IF l_count > 0
       THEN
          EXECUTE IMMEDIATE 'DROP SEQUENCE s' || i_poolname ;
       END IF ;
       DELETE FROM dprov_accounts_fast t WHERE t.pool_name = TRIM ( UPPER ( i_poolname ) ) ;
    EXCEPTION
        WHEN OTHERS THEN
            l_error := i_poolname ||' : ' || SQLCODE||' : '||SUBSTR(SQLERRM,1,500) ;
            LOGGER.write (l_error) ;  
            raise_application_error(-20003,l_error);       
    END DropSeqData;


/*************************************************************************************************
DependentDataPools 
This function return in a table object a list of all the pools that are depedent on the pool supplied
The first pool ( level 1 ) would be the master pool
The rest would be the direct dependents, with the level number indicating the order of dependencides 
( nested dependencies )
**************************************************************************************************/
    FUNCTION DependentDataPools (
        i_poolname      IN varchar2
        ) Return tab_pools
    IS
        cursor c_dep ( p_pool in varchar2 ) is
            select r.job_name, g.sub_str as dependent
              from RUN_JOB_PARALLEL_CONTROL r
                  , json_table( '["' || replace(to_clob(r.job_dependence),',','","') || '"]','$[*]' columns(  sub_str varchar2(200) path '$' )) g
             where g.sub_str = p_pool
               and r.stat_dyn = 'S';    
        lt_dep              tab_pools := tab_pools();
    
        l_dapool            RUN_JOB_PARALLEL_CONTROL.job_name%TYPE ;
        l_danumber          number ;
        l_idx               number ; 
        l_error             varchar2(1000) ;
    BEGIN
        l_dapool   := Upper(Trim(i_poolname)) ;
        l_danumber := 1 ;
        l_idx      := 1;

        lt_dep(l_idx).level := l_idx;
        lt_dep(l_idx).poolname := l_dapool;
        
        While l_idx <= l_danumber
        Loop
            For v_dep in c_dep(lt_dep(l_idx).poolname) 
            Loop
                l_danumber :=  l_danumber + 1;
                lt_dep(l_danumber).poolname := v_dep.job_name ;
                lt_dep(l_danumber).level := l_idx+1 ;
            End loop ;  
            l_idx := l_idx + 1;
        End Loop;
        
        Return lt_dep;

    END DependentDataPools ;


/*************************************************************************************************
Add_Datapool
Adds a new datapool to the datapool engine by updating 3 tables :
    DPROV_CONFIG --> Which drives the URL build and interfaces with the DATAPROV engine
    RUN_JOB_PARALLEL_CONTROL --> which runs our build process for this datapool
**************************************************************************************************/
    PROCEDURE Add_Datapool (
        i_poolname      in varchar2
       ,i_endpoint      in varchar2    DEFAULT null 
       ,i_pooldesc      in varchar2
       ,i_pooltype      in varchar2    DEFAULT 'STATIC'
       ,i_columns       in varchar2
       ,i_database      in varchar2    DEFAULT 'chordiant'
       ,i_statdyn       in varchar2    DEFAULT 'S'
       ,i_depend        in varchar2    DEFAULT 'CUSTOMERS'
       ,i_priority      in number      DEFAULT 7
       ,i_package       in varchar2
       ,i_comments      in varchar2    DEFAULT null    
       )
    IS
        -- Supporting variables
        l_log           varchar2(1000) ;
        l_error         varchar2(1000) ;
        l_endpoint      DPROV_CONFIG.endpoint_name%TYPE ;   
        l_depend        RUN_JOB_PARALLEL_CONTROL.job_name%TYPE ;

        -- Checks
        cursor c_dep (p_name in varchar2) is
            select  datapool_name
              from  DPROV_CONFIG
             where  datapool_name = p_name;  

        MISSING_DATA exception;     

    BEGIN

        l_log := ' Data pool : ' || Upper(i_poolname) || chr(10) ||
                 ' Desc : ' || i_pooldesc || chr(10) ||
                 ' type : ' || i_pooltype || chr(10) ||
                 ' columns : ' || lower(i_columns) || chr(10) ||
                 ' database : ' || lower(i_database) || chr(10) ||
                 ' statdyn : ' || i_statdyn || chr(10) ||
                 ' depend : ' || Upper(i_depend) || chr(10) ||
                 ' priority : ' || i_priority || chr(10) ||
                 ' package : ' || Upper(i_package) || chr(10) ||
                 ' comments : ' || i_comments ;

        --LOGGER.debug (' Data : ' || l_log );

        -- Check if any of any of the required data is missing
        if i_poolname is null or
           i_columns is null or
           i_package is null then
           raise MISSING_DATA;
        end if;   

        -- If the endPoint is not supplied, it'd be set to the same as the datapool name but in lowercase
        l_endpoint := NVL(i_endpoint,lower(i_poolname)) ;

        -- Check the dependency pool requested actually exists
        if i_depend is not null then
            SELECT  job_name
              INTO  l_depend
              from  RUN_JOB_PARALLEL_CONTROL
             where  Upper(job_name) = Upper(i_depend);
        end if; 

        -- Insert into DPROV_CONFIG    
        INSERT INTO DPROV_CONFIG (
             DATAPOOL_NAME
            ,DESCRIPTION
            ,POOL_TYPE
            ,COLUMNS
            ,DATABASE
            ,ENDPOINT_NAME )
        VALUES (
             Upper(i_poolname)
            ,i_pooldesc
            ,i_pooltype
            ,Lower(replace(i_columns,' ',''))
            ,lower(i_database)
            ,l_endpoint );

        -- Insert into RUN_JOB_PARALLEL_CONTROL   
        INSERT INTO RUN_JOB_PARALLEL_CONTROL (
             JOB_NAME
            ,STAT_DYN
            ,JOB_DEPENDENCE
            ,PRIORITY
            ,COMMENTS
            ,PACKAGE_NAME )  
        VALUES (
             Upper(i_poolname)
            ,i_statdyn
            ,Upper(i_depend)
            ,i_priority
            ,i_comments 
            ,Upper(i_package) );

         commit; 

    EXCEPTION
        WHEN MISSING_DATA THEN
            l_error := 'Missing data '||l_log ; 
            LOGGER.write (l_error) ;
            raise_application_error(-20001,l_error);
        WHEN NO_DATA_FOUND THEN
            l_error := 'Data Pool dependency not found : ' || Upper(i_depend) ;
            LOGGER.write (l_error) ;
            raise_application_error(-20002,l_error);
        WHEN OTHERS THEN
            l_error := i_poolname ||' : ' || SQLCODE||' : '||SUBSTR(SQLERRM,1,500) ;
            LOGGER.write (l_error) ;  
            raise_application_error(-20003,l_error);
    END Add_Datapool ;


/*************************************************************************************************
EnableDatapoolBase
Enables the build of the datapool 
It assumes the pool existed before and a backup exists within the DPROV_CONFIG_CONTROL table
Updates --> RUN_JOB_PARALLEL_CONTROL , column STAT_DYN
A trigger within this table will automatically update DPROV_CONFIG and DPROV_CONFIG_CONTROL appropriately
**************************************************************************************************/
    PROCEDURE EnableDatapoolBase (
        i_poolname      in varchar2
       ,i_comments      in varchar2    DEFAULT null    
       )
    IS
        -- Supporting variables
        l_error         varchar2(1000) ;
        l_poolname      RUN_JOB_PARALLEL_CONTROL.job_name%TYPE ;
        l_comments      RUN_JOB_PARALLEL_CONTROL.comments%TYPE ;
        
        MISSING_DATA exception;    
        EXCLUSION_LIST   exception;

    BEGIN
        l_poolname := Upper(Trim(i_poolname)) ;
        l_comments := i_comments ;
        
        -- Check if any of any of the required data is missing
        -- We will not re-enabled some pools that are excluded from being made disabled
        if l_poolname is null then
            raise MISSING_DATA;
        elsif l_poolname in ('CUSTSUPPORT','CUSTOMERS','CUSTOMERSV2') then
            raise EXCLUSION_LIST;
        end if;  

        -- Update column STAT_DYN in RUN_JOB_PARALLEL_CONTROL to enable the datapool
        UPDATE RUN_JOB_PARALLEL_CONTROL 
           SET STAT_DYN = 'S'
              ,COMMENTS = NVL(l_comments,COMMENTS)
         WHERE JOB_NAME = l_poolname 
           AND STAT_DYN = 'X';
        COMMIT; 

    EXCEPTION
        WHEN MISSING_DATA THEN
            l_error := 'Missing data - No poolName supplied' ; 
            LOGGER.write (l_error) ;
            raise_application_error(-20001,l_error);
        WHEN EXCLUSION_LIST THEN
            l_error := 'This pool cannot be enabled (rebuild) using this functoinality - '||l_poolname ; 
            LOGGER.write (l_error) ;         
            raise_application_error(-20002,l_error);
        WHEN NO_DATA_FOUND THEN
            l_error := 'Data Pool not found : ' || Upper(i_poolname) ;
            LOGGER.write (l_error) ;
            raise_application_error(-20002,l_error);
        WHEN OTHERS THEN
            l_error := i_poolname ||' : ' || SQLCODE||' : '||SUBSTR(SQLERRM,1,500) ;
            LOGGER.write (l_error) ;  
            raise_application_error(-20003,l_error);
    END EnableDatapoolBase ;



/*************************************************************************************************
DisableDatapoolBase
Disables the build of the datapool entirely but perservs the data in case we want to reverse this action
This is useful if pools have to be deferred from the build periodically
Updates --> RUN_JOB_PARALLEL_CONTROL , column STAT_DYN
A trigger within this table will automatically update DPROV_CONFIG and DPROV_CONFIG_CONTROL appropriately
**************************************************************************************************/
    PROCEDURE DisableDatapoolBase (
        i_poolname      in varchar2
       ,i_comments      in varchar2    DEFAULT null    
       )
    IS
        -- Supporting variables
        l_error         varchar2(1000) ;
        l_poolname      RUN_JOB_PARALLEL_CONTROL.job_name%TYPE ;
        l_comments      RUN_JOB_PARALLEL_CONTROL.comments%TYPE ;

       MISSING_DATA     exception;    
       EXCLUSION_LIST   exception;

    BEGIN
        l_poolname := Upper(Trim(i_poolname)) ;
        l_comments := i_comments ;
    
        -- Check if any of any of the required data is missing
        if l_poolname is null then
            raise MISSING_DATA;
        elsif l_poolname in ('CUSTSUPPORT','CUSTOMERS','CUSTOMERSV2') then
            raise EXCLUSION_LIST;
        end if;  

        -- Update column STAT_DYN in RUN_JOB_PARALLEL_CONTROL to disable the datapool
        UPDATE RUN_JOB_PARALLEL_CONTROL 
           SET STAT_DYN = 'X'
              ,COMMENTS = NVL(l_comments,COMMENTS)
         WHERE JOB_NAME = l_poolname ;
        
        -- Drop the associated data and sequences
        DropSeqData ( l_poolname );
        
        COMMIT; 

    EXCEPTION
        WHEN MISSING_DATA THEN
            l_error := 'Missing data - No PoolName supplied' ; 
            LOGGER.write (l_error) ;
            raise_application_error(-20001,l_error);
         WHEN EXCLUSION_LIST THEN
            l_error := 'This pool cannot be disabled - '||l_poolname ; 
            LOGGER.write (l_error) ;
            raise_application_error(-20001,l_error);
        WHEN NO_DATA_FOUND THEN
            l_error := 'Data Pool not found : ' || Upper(l_poolname) ;
            LOGGER.write (l_error) ;
            raise_application_error(-20002,l_error);
        WHEN OTHERS THEN
            l_error := l_poolname ||' : ' || SQLCODE||' : '||SUBSTR(SQLERRM,1,500) ;
            LOGGER.write (l_error) ;  
            raise_application_error(-20003,l_error);
    END DisableDatapoolBase ;


/*************************************************************************************************
RebuildDataPoolBase 
This procedure rebuilds the datapool given
**************************************************************************************************/
    PROCEDURE RebuildDataPoolBase (
        i_poolname      IN varchar2
        )
    IS
        l_poolname      RUN_JOB_PARALLEL_CONTROL.job_name%TYPE ;
        l_jobname       varchar2(100) ;
        l_error         varchar2(1000) ;
    BEGIN
        l_poolname := Upper(Trim(i_poolname)) ;
        
        -- Check the datapool asked to be rebuild is Enabled and returns it's name and the package it runs from
        select PACKAGE_NAME||'.'||JOB_NAME as jobname
          into l_jobname
          from RUN_JOB_PARALLEL_CONTROL
         where job_name = l_poolname
           and stat_dyn = 'S';
    
        -- run the pool execution
        LOGGER.debug ( 'executing ... ' || l_jobname );
        execute immediate 'BEGIN '||l_jobname||'; END;'  ;
    
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            l_error := 'Data Pool not found or not disabled : ' || Upper(l_poolname) ;
            LOGGER.write (l_error) ;
            raise_application_error(-20002,l_error);
        WHEN OTHERS THEN
            l_error := l_jobname ||' : ' || SQLCODE||' : '||SUBSTR(SQLERRM,1,500) ;
            LOGGER.write (l_error) ;  
            raise_application_error(-20003,l_error);   
            
    END RebuildDataPoolBase ;



/*************************************************************************************************
RebuildDataPool 
This procedure rebuilds the datapool supplied
Base on parameters supplied it will rebuild the dependent datapools if requested
**************************************************************************************************/
    PROCEDURE RebuildDataPool (
         i_poolname      in varchar2
        ,i_inclDep       in varchar2    DEFAULT 'N'
        )
    IS
        l_poolname      RUN_JOB_PARALLEL_CONTROL.job_name%TYPE ;
        l_jobname       varchar2(100) ;
        l_error         varchar2(1000) ;
        l_inclDep       varchar2(1) ;
        
        -- List of dependent pools
        t_pools         tab_pools ;
    BEGIN
        l_poolname := Upper(Trim(i_poolname)) ;
        l_inclDep  := trim(upper(i_inclDep)) ;
        
        if l_inclDep = 'Y' then
            -- Get the list of all the pools that are dependent on the one to be build
            t_pools := DependentDataPools ( l_poolname ) ;
        
            -- For each pool found, rebuild.
            -- The order the pools are returns is sorted by level 
            For i in 1 .. t_pools.COUNT
            Loop
                LOGGER.debug ( 'Execute '|| t_pools(i).poolname || ' - Level ' || t_pools(i).level );
                RebuildDataPoolBase ( t_pools(i).poolname ) ;
            End loop;    
        else
            -- Rebuild just this datapool
            RebuildDataPoolBase ( l_poolname ) ;
        end if ;
    
    EXCEPTION
        WHEN OTHERS THEN
            l_error := l_poolname ||' : ' || SQLCODE||' : '||SUBSTR(SQLERRM,1,500) ;
            LOGGER.write (l_error) ;  
            raise_application_error(-20003,l_error);   
            
    END RebuildDataPool ;





/*************************************************************************************************
DisableDatapool
Disables the build of the datapool entirely 
It will disable all dependent datapools if stated on the request parameters
**************************************************************************************************/
    PROCEDURE DisableDatapool (
        i_poolname      in varchar2
       ,i_comments      in varchar2    DEFAULT null 
       ,i_inclDep       in varchar2    DEFAULT 'N'
       )
    IS
        -- Supporting variables
        l_error         varchar2(1000) ;
        l_poolname      RUN_JOB_PARALLEL_CONTROL.job_name%TYPE ;
        l_comments      RUN_JOB_PARALLEL_CONTROL.comments%TYPE ;
        l_inclDep       varchar2(1) ;

        -- List of dependent pools
        t_pools         tab_pools ;
    BEGIN
        l_poolname := Upper(Trim(i_poolname)) ;
        l_comments := i_comments ;
        l_inclDep  := trim(upper(i_inclDep)) ;
        
        if l_inclDep = 'Y' then
            -- Get the list of all the pools that are dependent on the one to be build
            t_pools := DependentDataPools ( l_poolname ) ;
    
            -- Disable the first datapool with the original comment
            LOGGER.debug ( 'Disable datapool '|| t_pools(1).poolname || ' - Level ' || t_pools(1).level ||' - Comments : '|| l_comments);
            DisableDatapoolBase ( t_pools(1).poolname, l_comments ) ;        
        
            l_comments := l_comments || '. Datapool disabled by dependency' ;
            -- For each pool found, disable the pool
            For i in 2 .. t_pools.COUNT
            Loop
                LOGGER.debug ( 'Dsaible datapool '|| t_pools(i).poolname || ' - Level ' || t_pools(i).level ||' - Comments : '|| l_comments );
                DisableDatapoolBase ( t_pools(i).poolname , l_comments ) ;
            End loop;    
        else
            DisableDatapoolBase ( l_poolname ) ;
        end if ;

    EXCEPTION
        WHEN OTHERS THEN
            l_error := l_poolname ||' : ' || SQLCODE||' : '||SUBSTR(SQLERRM,1,500) ;
            LOGGER.write (l_error) ;  
            raise_application_error(-20003,l_error);
    END DisableDatapool ;



/*************************************************************************************************
EnableDatapool
It will enable a datapool 
If requested, it will rebuild the datapool
and if also specified, it will rebuild the dependent pools ( as long as they are enabled )
**************************************************************************************************/
    PROCEDURE EnableDatapool (
        i_poolname      in varchar2
       ,i_comments      in varchar2    DEFAULT null    
       ,i_rebuild       in varchar2    DEFAULT 'N'
       ,i_inclDep       in varchar2    DEFAULT 'N'
       )
    IS
        -- Supporting variables
        l_error         varchar2(1000) ;
        l_poolname      RUN_JOB_PARALLEL_CONTROL.job_name%TYPE ;
        l_comments      RUN_JOB_PARALLEL_CONTROL.comments%TYPE ;
        l_inclDep       varchar2(1) ;
        l_rebuild       varchar2(1) ;

    BEGIN
        l_poolname := Upper(Trim(i_poolname)) ;
        l_comments := i_comments ;
        l_inclDep  := trim(upper(i_inclDep));
        l_rebuild  := trim(upper(i_rebuild));
    
        -- Enable the given datapool
        LOGGER.debug ( 'Enable datapool '|| l_poolname || ' - Comments : '|| l_comments);
        EnableDatapoolBase ( l_poolname, l_comments ) ;        
    
        -- If the datapool has to be rebuild
        if l_rebuild = 'Y' then
            -- If requested, rebuild datapool and all its dependents ( if they are enabled already )
            if l_inclDep = 'Y' then
                LOGGER.debug ('Rebuild datapool with dependencies '|| l_poolname ) ;
                rebuildDataPool ( l_poolname, 'Y' ) ;
            else
                LOGGER.debug ('Rebuild datapool alone '|| l_poolname ) ;
                rebuildDataPool ( l_poolname, 'N' ) ;           
            end if;    
        end if ;

    EXCEPTION
        WHEN OTHERS THEN
            l_error := l_poolname ||' : ' || SQLCODE||' : '||SUBSTR(SQLERRM,1,500) ;
            LOGGER.write (l_error) ;  
            raise_application_error(-20003,l_error);
    END EnableDatapool ;



/*************************************************************************************************
Delete_Datapool
Deletes the datapool from the respective tables 
RUN_JOB_PARALLEL_CONTROL and DPROV_CONFIG_CONTROL
A trigger within these table will automatically update DPROV_CONFIG_CONTROL_LOG and DPROV_CONFIG_LOG appropriately
**************************************************************************************************/
    PROCEDURE Delete_Datapool (
        i_poolname      in varchar2
       )
    IS
        -- Supporting variables
        l_log           varchar2(1000) ;
        l_error         varchar2(1000) ;
        l_poolname      RUN_JOB_PARALLEL_CONTROL.job_name%TYPE ;
        l_stat_dyn      RUN_JOB_PARALLEL_CONTROL.stat_dyn%TYPE ;

       MISSING_DATA exception;    
       
    BEGIN

        l_log := ' Data pool : ' || Upper(i_poolname);

        --LOGGER.debug (' Data : ' || l_log );

        -- Check if any of any of the required data is missing
        if i_poolname is null then
           raise MISSING_DATA;
        end if;  

        -- Check the datapool exists and it's DISABLED 
        -- we will only allow deletes for disabled datapools
        SELECT job_name, stat_dyn
          INTO l_poolname, l_stat_dyn
          FROM RUN_JOB_PARALLEL_CONTROL
         WHERE JOB_NAME = i_poolname 
           AND STAT_DYN = 'X'; 

        -- Delete row from RUN_JOB_PARALLEL_CONTROL
        -- The row will be copied via a trigger into RUN_JOB_PARALLEL_CONTROL_LOG
        DELETE FROM RUN_JOB_PARALLEL_CONTROL 
         WHERE JOB_NAME = l_poolname 
           AND STAT_DYN = l_stat_dyn;
       
        -- Delete row from DPROV_CONFIG_CONTROl 
        -- The row will be copied into DPROV_CONFIG_LOG via a trigger
        DELETE FROM DPROV_CONFIG_CONTROL
         WHERE DATAPOOL_NAME = l_poolname ;
         
        COMMIT; 

    EXCEPTION
        WHEN MISSING_DATA THEN
            l_error := 'Missing data '||l_log ; 
            LOGGER.write (l_error) ;
            raise_application_error(-20001,l_error);
        WHEN NO_DATA_FOUND THEN
            l_error := 'Data Pool not found or not disabled : ' || Upper(l_poolname) || ' - Status : '|| l_stat_dyn  ;
            LOGGER.write (l_error) ;
            raise_application_error(-20002,l_error);
        WHEN OTHERS THEN
            l_error := l_poolname ||' : ' || SQLCODE||' : '||SUBSTR(SQLERRM,1,500) ;
            LOGGER.write (l_error) ;  
            raise_application_error(-20003,l_error);            
    END Delete_Datapool ;


/*************************************************************************************************
DisableUnusedDatapools
Disables all detected datapools that are older than 4 weeks since created and have not been 
used for over 4 weeks.
**************************************************************************************************/
    PROCEDURE DisableUnusedDatapools 
    IS
        -- Flags 
        l_depfound       number ;  -- The datapool has dependents
        l_useddepfound   number ;  -- The datapool's dependent pools are being used
        l_reenabled      number ;  -- The dataabse has been recently re-enabled
        l_error          varchar2(100) ;
        
        -- Returns datapools that are older than 4 weeks and have not been used
        cursor c_datapool is
            select d.poolname
                 , Max(d.counter) as counter
                 , Max(d.loops) as loops
            from DPROV_ACCOUNTS_FAST_LOG d
            join RUN_JOB_PARALLEL_CONTROL r on ( r.job_name = d.poolname )
            where  r.stat_dyn = 'S'
              and  d.created > sysdate-29 
            group by d.poolname
            having max(d.counter) < 4
               and min(d.created) < sysdate-28
            order by  d.poolname ;
    
        -- Returns an specific datapool's usage details within the last 4 weeks 
        cursor c_depdatapool ( p_pool in varchar2 ) is
            select d.poolname
                 , Max(d.counter) as counter
                 , Max(d.loops) as loops
            from DPROV_ACCOUNTS_FAST_LOG d
            join RUN_JOB_PARALLEL_CONTROL r on ( r.job_name = d.poolname )
            where d.poolname = p_pool 
            and  r.stat_dyn = 'S'
            and  d.created > sysdate-29 
            group by d.poolname
            --having max(d.counter) < 4
            order by  d.poolname ;    

        -- Returns all the datapools that are dependent on the datapool passed.
        cursor c_dependent ( p_pool in varchar2 ) is
            select r.job_name, g.sub_str as dependent
              from RUN_JOB_PARALLEL_CONTROL r
                  , json_table( '["' || replace(to_clob(r.job_dependence),',','","') || '"]','$[*]' columns(  sub_str varchar2(200) path '$' )) g
             where g.sub_str = p_pool
               and r.stat_dyn = 'S';

        -- This cursor will return the poolname and most recent Modified date when the pool was made "enabled"
        -- This is because we will not automatically disabled any pool that has been recently re-enabled
        cursor c_reenabled ( p_pool in varchar2 ) is
            select JOB_NAME, MODIFIED_DATE
              from RUN_JOB_PARALLEL_CONTROL_LOG
             where JOB_NAME = p_pool
               and MODIFIED_DATE = ( select Max(b.MODIFIED_DATE) 
                                       from RUN_JOB_PARALLEL_CONTROL_LOG b
                                      where b.JOB_NAME = JOB_NAME )
               and ACTION = 'U' 
               and STAT_DYN = 'S'
               and EXISTS  ( select b.ACTION 
                               from RUN_JOB_PARALLEL_CONTROL_LOG b
                              where b.JOB_NAME = JOB_NAME
                                and b.MODIFIED_DATE = MODIFIED_DATE
                                and b.ACTION = 'o'
                                and b.STAT_DYN = 'X');   

    BEGIN
        -- Loop thru all the datapools that are older than 4 weeks and have not been used
        for v_pool in c_datapool 
        loop
            l_depfound := 0;
            l_useddepfound := 0;
            l_reenabled := 0 ;
            -- For each pool, find the dependent pools
            --LOGGER.debug ('Checking data pool : '||v_pool.poolname) ;
            for v_dep in c_dependent ( v_pool.poolname )
            loop
                l_depfound := 1;
                --LOGGER.debug ('Pool '||v_pool.poolname||' Usage : '||v_pool.counter||' Loops : '||v_pool.loops );
                -- For each depedent pool, check its specific usage to highlight if ANY of them are still in use
                for v_deppool in c_depdatapool ( v_dep.job_name )
                loop
                    If v_deppool.counter >= 4 or v_deppool.loops > 1 then 
                        l_useddepfound := 1;
                        --LOGGER.debug ('Pool '|| v_pool.poolname || ' with dependent pool '||v_dep.job_name||' is used. Dependent Counter = '||v_deppool.counter ) ;
                    else 
                        l_depfound := 2;
                        --LOGGER.debug ('Pool '|| v_pool.poolname || ' with dependent pool '||v_dep.job_name||' also not used. Dependent Counter = '||v_deppool.counter ) ;
                    end if;    
                end loop;
            end loop;    
            -- If the pool has been recently reenabled, then do not disabled it
            for v_ren in c_reenabled ( v_pool.poolname )
            loop
                if v_ren.modified_date > sysdate-28 then
                    l_reenabled := 1 ;
                    LOGGER.debug ('Pool '|| v_ren.job_name || ' was last re-enabled  ' || v_ren.modified_date ) ;
                end if;    
            end loop;    
            -- If the pool doesn't have dependens or it has but none of them are used 
            -- We don't need to process the dependent pools coz they'll be eventually listed by the script anyway.
            if ( l_depfound = 0 and l_useddepfound = 0 and l_reenabled = 0 ) then
                LOGGER.debug ('Disabling Datapool '||v_pool.poolname );
                --Disable_Datapool ( v_pool.poolname , 'Automatically disabled on ' || trunc (sysdate) );
            end if;
        end loop;  

    EXCEPTION
        WHEN OTHERS THEN
            l_error := SQLCODE||' : '||SUBSTR(SQLERRM,1,500) ;
            LOGGER.write (l_error) ;  
            raise_application_error(-20003,l_error);   
    END DisableUnusedDatapools;


END DPROV_UTILS;
/
