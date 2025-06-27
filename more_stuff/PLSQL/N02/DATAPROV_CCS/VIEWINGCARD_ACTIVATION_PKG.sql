CREATE OR REPLACE package          viewingcard_activation_pkg as
  --
  --#############################################################
  --Created: 05/06/2014                                         #
  --Modified:                                                   #
  --Last modification:                                          #
  --Last modified by:                                           #
  --                                                            #
  --Usage Notes:                                                #
  --                                                            #
  --                                                            #
  --                                                            #
  --#############################################################
  --
  procedure load_viewingcard_activation(v_days in number);
  --
  procedure data_vc(req_recs in number, v_rec_out out sys_refcursor);
  --
  procedure processed(v_pk_value in varchar2);
  --
  procedure update_viewingcard_activation(v_rec_out out sys_refcursor);
  --
  function card_count return number;
  --
end viewingcard_activation_pkg;

/


CREATE OR REPLACE package body          viewingcard_activation_pkg as
  --
  --#############################################################
  --Created: 07/02/2014                                         #
  --Modified:                                                   #
  --Last modification: Modification                             #
  --Last modified by: gne02                                     #
  --                                                            #
  --Usage Notes: Stopping duplicate cardnumbers being loaded    #
  --                                                            #
  --                                                            #
  --                                                            #
  --#############################################################
  --
  procedure load_viewingcard_activation(v_days in number) is
    --
    v_param_return dataprov.dp_test_script_params.parameter_value%type;
    v_test_name constant varchar2(50) := 'act_cust_bb_base';
    v_env_refresh date;
    --
  begin
    --
    --
    --get env refresh param
    dataprov.data_prep_framework.get_test_param(v_test_script_name => upper(v_test_name),
                                                v_param_name       => upper('ENV_REFRESH'),
                                                v_param_out        => v_param_return);
    --convert param to corect type
    v_env_refresh := to_date(v_param_return, 'DD-MON-YYYY');
    --
    -- Load viewingcard_activation based on number of days subtracted from sysdate
    --
    DELETE from dataprov.viewingcard_activation a where a.processed = 'Y';
    --
    COMMIT;
    --
    --
    insert /*+ append */
    into dataprov.viewingcard_activation
      (accountnumber,
       cardnumber,
       subscriberid,
       id,
       lastupdate,
       load_date,
       status,
       serial_prefix,
       subscriberid_old)
      with box_ref as
       (select /*+ full(bpp2) parallel(bpp2 16) */
         bpp2.catalogueproductid box_id, serviceinstanceid
          from ccsowner.bsbportfolioproduct bpp2
         where bpp2.catalogueproductid in
               ('13947',
                '13948',
                '11090',
                '13522',
                '23540',
                '13425',
                '13646',
                '13653',
                '10136',
                '10116',
                '10140',
                '10141',
                '10142',
                '13787',
                '13788',
                '13791',
                '13970',
                '15491',
                '15595',
                '15596',
                '15597'))
      select
      /*+ full(bce) parallel(bce 16) full(bpp) parallel(bpp 16)  full(bba) parallel(bba 16)
      full(bsi) parallel(bsi 16)  pq_distribute(bce hash hash)  pq_distribute(bsi hash hash)
      pq_distribute(bpp hash hash) pq_distribute(bba hash hash)*/
       bba.accountnumber,
       bce.cardnumber,
       bsi.cardsubscriberid,
       bce.id,
       bce.lastupdate,
       sysdate,
       bce.status,
       pre.prefix,
       bsi.cardsubscriberid
        from ccsowner.bsbcustomerproductelement bce,
             ccsowner.bsbportfolioproduct       bpp,
             ccsowner.bsbbillingaccount         bba,
             ccsowner.bsbserviceinstance        bsi,
             box_ref,
             dataprov.dp_box_prefixes           pre
       where box_ref.serviceinstanceid = bsi.id
         and pre.id(+) = box_ref.box_id
         and bpp.id = bce.portfolioproductid
         and bpp.portfolioid = bba.portfolioid
         and bpp.serviceinstanceid = bsi.id
         and bpp.serviceinstanceid = box_ref.serviceinstanceid
         and bpp.catalogueproductid = '10137'
         and bce.status = 'T'
         and bce.cardnumber is not null
         and bce.statuschangeddate > sysdate - v_days
         and bba.created > v_env_refresh
            --
         AND NOT EXISTS (SELECT 1
                FROM dataprov.viewingcard_activation cl
               WHERE cl.cardnumber = bce.cardnumber);
    --
    -- COMMIT transaction
    --
    commit;
    --
    --
    INSERT
    /*+ append */
    INTO dataprov.viewingcard_activation
      (accountnumber,
       cardnumber,
       subscriberid,
       id,
       lastupdate,
       load_date,
       status,
       serial_prefix,
       subscriberid_old)
      WITH box_ref AS
       (SELECT
        /*+ full(bpp2) parallel(bpp2 16) */
         bpp2.catalogueproductid box_id, serviceinstanceid
          FROM ccsowner.bsbportfolioproduct bpp2
         WHERE bpp2.catalogueproductid IN
               ('13947',
                '13948',
                '11090',
                '13425',
                '13646',
                '13653',
                '10136',
                '10116',
                '10140',
                '10141',
                '10142',
                '13787',
                '13788',
                '13791',
                '13970',
                '15491',
                '15595',
                '15596',
                '15597')
           and bpp2.status = 'Z'
           and bpp2.created > sysdate - v_days
           and bpp2.created > v_env_refresh)
      SELECT
      /*+ full(bce) parallel(bce 16) full(bpp) parallel(bpp 16)  full(bba) parallel(bba 16)
      full(bsi) parallel(bsi 16)  pq_distribute(bce hash hash)  pq_distribute(bsi hash hash)
      pq_distribute(bpp hash hash) pq_distribute(bba hash hash)*/
       bba.accountnumber,
       bce.cardnumber,
       (hmap.accid - 6388),
       bce.id,
       bce.lastupdate,
       sysdate,
       bce.status,
       pre.prefix,
       bsi.cardsubscriberid
        FROM ccsowner.bsbcustomerproductelement bce,
             ccsowner.bsbportfolioproduct       bpp,
             ccsowner.bsbbillingaccount         bba,
             ccsowner.bsbserviceinstance        bsi,
             box_ref,
             dataprov.dp_box_prefixes           pre,
             dataprov.hhid_mapping              hmap
       WHERE box_ref.serviceinstanceid = bsi.id
       AND hmap.subscriberid = bsi.cardsubscriberid
         AND pre.id(+) = box_ref.box_id
         AND bpp.id = bce.portfolioproductid
         AND bpp.portfolioid = bba.portfolioid
         AND bpp.serviceinstanceid = bsi.id
         AND bpp.serviceinstanceid = box_ref.serviceinstanceid
         AND bpp.catalogueproductid = '10137'
         AND bce.status = 'A'
         AND bce.cardnumber IS NOT NULL
            --
         AND NOT EXISTS (SELECT 1
                FROM dataprov.viewingcard_activation cl
               WHERE cl.cardnumber = bce.cardnumber);
    --
    -- COMMIT transaction
    --
    commit;
    --
    --
  end load_viewingcard_activation;
  --
  --
procedure data_vc ( req_recs in number , v_rec_out out sys_refcursor ) is
    --
    --#############################################################
    --Created: 05/06/2014                                         #
    --Modified:                                                   #
    --Last modification:                                          #
    --Last modified by:                                           #
    --                                                            #
    --Usage Notes:                                                #
    --                                                            #
    --                                                            #
    --                                                            #
    --#############################################################
    --
    --
begin
   -- logging added 31-Aug-2021 Andrew Fraser
   logger.write ( 'req_recs => ' || TO_CHAR ( req_recs ) ) ;
    --
    -- Populate return cursor will all unprocessed viewing cards
    --
    open v_rec_out for
      select *
        from dataprov.viewingcard_activation vca
       where vca.processed = 'N'
         and rownum <= req_recs;
    --
  exception
    when no_data_found then
      raise_application_error(-20003,
                              'Error: There are no unprocessed Viewing cards');
      --
end data_vc ;
  --
  --
  --
  --#############################################################
  --Created: 07/02/2014                                         #
  --Modified:                                                   #
  --Last modification: creation                                 #
  --Last modified by: gne02                                     #
  --                                                            #
  --Usage Notes:                                                #
  --                                                            #
  --                                                            #
  --                                                            #
  --#############################################################
  --
  procedure update_viewingcard_activation(v_rec_out out sys_refcursor) is
  begin
    --
    -- Update status of viewing card after it has been processed
    --
    update dataprov.viewingcard_activation vca
       set vca.status     =
           (select status
              from ccsowner.bsbcustomerproductelement bce
             where vca.id = bce.id),
           vca.update_date = sysdate
     where vca.processed = 'Y';
    --
    -- COMMIT transaction
    --
    commit;
    --
    -- Populate return cursor will all processed viewing cards
    --
    open v_rec_out for
      select *
        from dataprov.viewingcard_activation vca
       where vca.processed = 'Y';
    --
  end update_viewingcard_activation;
  --
  --
  --
  --#############################################################
  --Created: 10/02/2014                                         #
  --Modified:                                                   #
  --Last modification: creation                                 #
  --Last modified by: gne02                                     #
  --                                                            #
  --Usage Notes:                                                #
  --                                                            #
  --                                                            #
  --                                                            #
  --#############################################################
  --
  function card_count return number is
    --
    v_rc number;
    --
  begin
    --
    select count(*)
      into v_rc
      from dataprov.viewingcard_activation vca
     where vca.status in ('T', 'Z');
    return v_rc;
    --
  end card_count;
  --
  --
  --
  --#############################################################
  --Created: 10/02/2014                                         #
  --Modified:                                                   #
  --Last modification: creation                                 #
  --Last modified by: gne02                                     #
  --                                                            #
  --Usage Notes:                                                #
  --                                                            #
  --                                                            #
  --                                                            #
  --#############################################################
  --
  procedure processed(v_pk_value in varchar2) is
    --
  begin
    --
    --
    update dataprov.viewingcard_activation vca
       set vca.processed = 'Y'
     where vca.pk_value = v_pk_value;
    --
    -- COMMIT transaction
    --
    commit;
    --
    --
  end processed;
  --
end viewingcard_activation_pkg;
/
