CREATE OR REPLACE PACKAGE data_telephone_load AS
PROCEDURE dprov_telephone ;  -- called by run_job_parallel_control, which is in turn called by Jenkins nightly job.
PROCEDURE telephone_report ;
PROCEDURE test_logging ( v_current_run_id in number , v_master_run_id in number , v_suffix in varchar2 ) ;
PROCEDURE test_logging_detail ( v_refresh_id in number , v_detail_id in number , v_suffix in varchar2 , v_stage in varchar2 , v_rows_processed in number ) ;
PROCEDURE nfr_change (I_MAGIC_NO in varchar2);
END data_telephone_load ;
/


CREATE OR REPLACE PACKAGE BODY data_telephone_load as

PROCEDURE dprov_telephone IS
/*
|| Called by run_job_parallel_control, which is in turn called by Jenkins nightly job.
|| Change History:
||    30-Aug-2021 Andrew Fraser initial version
*/
   l_stmt VARCHAR2(4000) ;
BEGIN
   logger.write ( 'begin' ) ;
   -- 1) Populate staging table with potential phone number data
   EXECUTE IMMEDIATE 'truncate table dprov_telephone_stg1' ;
   FOR r1 IN (
      SELECT t.magic_no
        FROM dp_tele_nfr t
       WHERE t.nfr > 0
       ORDER BY 1
   )
   LOOP
      -- Loop through all the uk telephone codes
      FOR r2 IN (
         SELECT t.code
              , CASE t.code_length WHEN 3 THEN 5 WHEN 4 THEN 4 WHEN 5 THEN 3 END AS padding
           FROM telephone_codes t
          WHERE t.code_length IN ( 3 , 4 , 5 )
          ORDER BY t.code_length
      )
      LOOP
         INSERT /*+ append */ INTO dprov_telephone_stg1 t ( t.combinedTelephoneNumber , t.suff_alloc )
         SELECT r2.code || LPAD ( LEVEL , r2.padding , '0' ) || r1.magic_no AS combinedTelephoneNumber
              , r1.magic_no AS suff_alloc
           FROM DUAL
        CONNECT BY LEVEL <= 5100
         ;
         COMMIT ;
      END LOOP ;
   END LOOP ;
   dbms_stats.gather_table_stats ( ownName => USER , tabName => 'dprov_telephone_stg1' ) ;
   logger.write ( 'part 1 complete' ) ;
   -- 2) Remove any phoneNumbers belonging to real customers,
   -- and also randomise the data ordering so that different data will be output to tomcat end users each day.
   -- Testing showed parallel hint is worthwhile, 94 secs instead of 245 secs. 
   EXECUTE IMMEDIATE 'truncate table dprov_telephone_stg2' ;
   INSERT /*+ append */ INTO dprov_telephone_stg2 t ( t.combinedTelephoneNumber , t.suff_alloc )
   SELECT /*+ parallel(8) */ DISTINCT s.combinedTelephoneNumber , s.suff_alloc
     FROM dprov_telephone_stg1 s
    WHERE s.combinedTelephoneNumber NOT IN ( SELECT bt.combinedTelephoneNumber FROM ccsowner.bsbTelephone bt )
    ORDER BY dbms_random.value
   ;
   COMMIT ;
   EXECUTE IMMEDIATE 'truncate table dprov_telephone_stg1' ;
   dbms_stats.gather_table_stats ( ownName => USER , tabName => 'dprov_telephone_stg2' ) ;
   logger.write ( 'part 2 complete' ) ;
   -- 3) Populate the real table, the one which is accessed by tomcat get requests.
   EXECUTE IMMEDIATE 'truncate table dprov_telephone' ;
   FOR r1 IN (
      SELECT t.magic_no , t.nfr
        FROM dp_tele_nfr t
       WHERE t.nfr > 0
       ORDER BY 1
   )
   LOOP
      l_stmt := 'INSERT /*+ append */ INTO dprov_telephone t ( t.combinedTelephoneNumber , t.suff_alloc , t.magic_' || r1.magic_no || q'[ )
SELECT s.combinedTelephoneNumber , s.suff_alloc , 'A'
  FROM dprov_telephone_stg2 s
 WHERE s.suff_alloc = ]' || r1.magic_no || '
   AND ROWNUM <= ' || TO_CHAR ( r1.nfr )
      ;
      EXECUTE IMMEDIATE l_stmt ;
      COMMIT ;  -- only needed if insert append above, but testing showed append is good, barely any increase in number of blocks.
   END LOOP ;
   EXECUTE IMMEDIATE 'truncate table dprov_telephone_stg2' ;
   logger.write ( 'complete' ) ;
END dprov_telephone ;

procedure telephone_report is
    --
    --#############################################################
    --Created: 02/05/2014                                         #
    --Modified: 02/05/2014                                        #
    --Last modification: creation                                 #
    --Last modified by: gne02                                     #
    --                                                            #
    --Usage Notes:                                                #
    --                                                            #
    --                                                            #
    --                                                            #
    --#############################################################
    --
begin
    --
    -- Remove records for the day that have already been loaded
    --
    Delete from dataprov.dprov_telephone_report
     where trunc(dt) = trunc(sysdate);
    --
    COMMIT;
    --
    -- Load the dprov_telephone_report table with the status of the telephone numbers used yesterday
    --
    insert /*+ append */
    into dataprov.dprov_telephone_report
      with pp as
       (select
        /*+ full(bt) parallel(bt 16) full(bct) parallel(bct 16)  full(bc) parallel(bc 16)  full(bpr) parallel(bpr 16)  full(bcr) parallel(bcr 16)
        full(ba) parallel(ba 16) full(bcr) parallel(bcr 16)  full(bsi) parallel(bsi 16)  full(bsu) parallel(bsu 16) 
                      pq_distribute(bt hash hash) pq_distribute(bct hash hash) pq_distribute(bc hash hash)
                      pq_distribute(bpr hash hash) pq_distribute(bcr hash hash)
                       pq_distribute(ba hash hash) pq_distribute(bpp hash hash)
                        pq_distribute(bsi hash hash) pq_distribute(bsu hash hash)*/
        distinct (ba.accountnumber),
                 bt.combinedtelephonenumber telephonenumber,
                 case bpp.catalogueproductid
                   WHEN '12721' THEN
                    'TALK'
                   ELSE
                    'BROADBAND'
                 END PRODUCT,
                 CASE
                   WHEN bpp.status IN ('A', 'AC') THEN
                    'ACTIVE'
                   WHEN bpp.status = 'AP' THEN
                    'AWAITING PROVISION'
                   WHEN bpp.status = 'PAC' THEN
                    'PENDING ACTIVATION'
                   WHEN bpp.status = 'S' THEN
                    'SENT'
                   ELSE
                    bpp.status
                 END status,
                 bsu.technologyactivationdate activationdate,
                 case bsu.technologycode
                   WHEN 'MPF' THEN
                    'SVBN'
                   ELSE
                    'Non-SVBN'
                 END PRODUCT_TYPE
          from ccsowner.bsbtelephone        bt,
               ccsowner.bsbcontacttelephone bct,
               ccsowner.bsbcontactor        bc,
               ccsowner.bsbpartyrole        bpr,
               ccsowner.bsbcustomerrole     bcr,
               ccsowner.bsbbillingaccount   ba,
               ccsowner.bsbportfolioproduct bpp,
               ccsowner.bsbserviceinstance  bsi,
               ccsowner.bsbsubscription     bsu
         where bt.id = bct.telephoneid
           and bc.id = bct.contactorid
           and bpr.partyid = bc.partyid
           and bcr.partyroleid = bpr.id
           and ba.portfolioid = bcr.portfolioid
           and bpp.portfolioid = ba.portfolioid
           and bsi.portfolioid = ba.portfolioid
           and bsi.serviceinstancetype in (100, 400)
           and bsi.id = bpp.serviceinstanceid
           and bsu.id = bpp.subscriptionid
           and bpp.catalogueproductid in (12675, 12721, 13661, 13744))
      select tlog.suff_alloc,
             pp.accountnumber,
             pp.telephonenumber,
             max(decode(pp.PRODUCT, 'TALK', pp.STATUS)) Telephony_State,
             max(decode(pp.PRODUCT, 'TALK', pp.activationdate)) Telephony_Active_Date,
             max(decode(pp.PRODUCT, 'TALK', pp.PRODUCT_TYPE)) Telephony_Type,
             max(decode(pp.PRODUCT, 'BROADBAND', pp.STATUS)) Broadband_State,
             max(decode(pp.PRODUCT, 'BROADBAND', pp.activationdate)) Broadband_Active_Date,
             max(decode(pp.PRODUCT, 'BROADBAND', pp.PRODUCT_TYPE)) Broadband_Type,
             trunc(sysdate)
        from pp, dataprov.dprov_telephone_log tlog
       where pp.telephonenumber = tlog.combinedtelephonenumber
         and trunc(tlog.dt) = trunc(sysdate)
       group by tlog.suff_alloc, pp.accountnumber, pp.telephonenumber;
    --
    COMMIT;
    --
    dbms_stats.gather_Table_stats(tabname => 'DPROV_TELEPHONE_REPORT',
                                  ownname => 'DATAPROV',
                                  cascade => TRUE,
                                  degree  => 4);
    --
end telephone_report ;

procedure test_logging(v_current_run_id in number,
                         v_master_run_id  in number,
                         v_suffix         in varchar2) is
begin
    merge into dataprov.dp_tele_refresh_runs a
    using (select v_current_run_id runid,
                  v_master_run_id  master_runid,
                  v_suffix         suffix,
                  sysdate
             from dual) b
    on (b.runid = a.run_id)
    when matched then
      update set a.end_time = sysdate
    when not matched then
      insert
        (a.run_id, a.master_run_id, a.suffix, a.start_time)
      values
        (b.runid, b.master_runid, b.suffix, sysdate);
end test_logging ;

procedure test_logging_detail(v_refresh_id     in number,
                                v_detail_id      in number,
                                v_suffix         in varchar2,
                                v_stage          in varchar2,
                                v_rows_processed in number) is
begin
    merge into dataprov.dp_tele_refresh_runs_detail a
    using (select v_refresh_id     parent_run_id,
                  v_detail_id      detail_id,
                  v_suffix         suffix,
                  v_stage          stage,
                  sysdate,
                  v_rows_processed rows_processed
             from dual) b
    on (b.detail_id = a.detail_id)
    when matched then
      update set a.end_time = sysdate, a.rows_processed = b.rows_processed
    when not matched then
      insert
        (a.parent_run_id,
         a.detail_id,
         a.suffix,
         a.stage,
         a.start_time,
         a.rows_processed)
      values
        (b.parent_run_id,
         b.detail_id,
         b.suffix,
         upper(b.stage),
         sysdate,
         b.rows_processed);
    --
END test_logging_detail ;



PROCEDURE nfr_change (I_MAGIC_NO in varchar2) IS
/*
|| Based on DATA_TELEPHONE_LOAD.DPROV_TELEPHONE.
|| Allows a value for NFR in DP_TELE_NFR to be changed and telephone numbers regenerated for it.
|| Change History:
||    21-MAR-2024 Stuart Mason
*/
   l_stmt VARCHAR2(4000) ;
   v_new_nfr number;

BEGIN
   logger.write ( 'begin' ) ;
   -- 1) Populate staging table with potential phone number data

   select nfr 
   into v_new_nfr
   from dp_tele_nfr
   where magic_no = I_MAGIC_NO;

   EXECUTE IMMEDIATE 'truncate table dprov_telephone_stg1' ;

      -- Loop through all the uk telephone codes
      FOR r2 IN (
         SELECT t.code
              , CASE t.code_length WHEN 3 THEN 5 WHEN 4 THEN 4 WHEN 5 THEN 3 END AS padding
           FROM telephone_codes t
          WHERE t.code_length IN ( 3 , 4 , 5 )
          ORDER BY t.code_length
      )
      LOOP
         INSERT /*+ append */ INTO dprov_telephone_stg1 t ( t.combinedTelephoneNumber , t.suff_alloc )
         SELECT r2.code || LPAD ( LEVEL , r2.padding , '0' ) || I_MAGIC_NO AS combinedTelephoneNumber
              , I_MAGIC_NO AS suff_alloc
           FROM DUAL
        CONNECT BY LEVEL <= 5100
         ;
         COMMIT ;
      END LOOP ;

   dbms_stats.gather_table_stats ( ownName => USER , tabName => 'dprov_telephone_stg1' ) ;
   logger.write ( 'part 1 complete' ) ;
   -- 2) Remove any phoneNumbers belonging to real customers,
   -- and also randomise the data ordering so that different data will be output to tomcat end users each day.
   -- Testing showed parallel hint is worthwhile, 94 secs instead of 245 secs. 
   EXECUTE IMMEDIATE 'truncate table dprov_telephone_stg2' ;
   INSERT /*+ append */ INTO dprov_telephone_stg2 t ( t.combinedTelephoneNumber , t.suff_alloc )
   SELECT /*+ parallel(8) */ DISTINCT s.combinedTelephoneNumber , s.suff_alloc
     FROM dprov_telephone_stg1 s
    WHERE s.combinedTelephoneNumber NOT IN ( SELECT bt.combinedTelephoneNumber FROM ccsowner.bsbTelephone bt )
    ORDER BY dbms_random.value
   ;
   COMMIT ;
   EXECUTE IMMEDIATE 'truncate table dprov_telephone_stg1' ;
   dbms_stats.gather_table_stats ( ownName => USER , tabName => 'dprov_telephone_stg2' ) ;
   logger.write ( 'part 2 complete' ) ;
   -- 3) Populate the real table, the one which is accessed by tomcat get requests.
   EXECUTE IMMEDIATE 'delete dprov_telephone where substr(combinedtelephonenumber,length(combinedtelephonenumber)-('||length(I_MAGIC_NO)||'-1),'||length(I_MAGIC_NO)||') = '''||I_MAGIC_NO||'''' ;

      l_stmt := 'INSERT /*+ append */ INTO dprov_telephone t ( t.combinedTelephoneNumber , t.suff_alloc , t.magic_' || I_MAGIC_NO || q'[ )
SELECT s.combinedTelephoneNumber , s.suff_alloc , 'A'
  FROM dprov_telephone_stg2 s
 WHERE s.suff_alloc = ]' || I_MAGIC_NO || '
   AND ROWNUM <= ' || TO_CHAR ( v_new_nfr )
      ;
      EXECUTE IMMEDIATE l_stmt ;
      COMMIT ;  -- only needed if insert append above, but testing showed append is good, barely any increase in number of blocks.
   EXECUTE IMMEDIATE 'truncate table dprov_telephone_stg2' ;
   logger.write ( 'complete' ) ;
END nfr_change ;



END data_telephone_load ;
/
