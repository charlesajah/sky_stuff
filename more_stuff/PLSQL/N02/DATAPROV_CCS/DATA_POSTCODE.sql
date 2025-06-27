CREATE OR REPLACE PACKAGE data_postcode AS
  --
  --#############################################################
  --Created: 05/02/2015                                         #
  --Modified: 
  --Last modification: 
  --Last modified by: 
  --   23-May-2017 Andrew Fraser separated out mdu part into procedure populate_dprov_postcode_mdu.
  --                                                            #
  --Usage Notes:                                                #
  --                                                            #
  --                                                            #
  --                                                            #
  --#############################################################
  --
   PROCEDURE populate_dprov_postcode ;
   PROCEDURE populate_dprov_postcode_mdu ;
   PROCEDURE get_postcode ( v_suffix IN VARCHAR2 , v_res_type IN VARCHAR2 , v_postcode_out OUT VARCHAR2 , v_dpsuffix_out OUT VARCHAR2 , v_propid_out OUT VARCHAR2 ) ;
END data_postcode ;
/


CREATE OR REPLACE PACKAGE BODY data_postcode AS

PROCEDURE populate_dprov_postcode IS
  --#############################################################
  --Created: 11/03/2015                                         #
  --Modified:                                                   #
  --Last modification: Modified                                 #
  --Last modified by: 
  --   23-May-2017 Andrew Fraser separated out mdu part into procedure populate_dprov_postcode_mdu.
  --Usage Notes:   Uses data from PAF database extract now      #
  --                                                            #
  --                                                            #
  --                                                            #
  --#############################################################
begin
    --
    --clear existing table
    --
    execute immediate 'truncate table dataprov.dprov_postcode';
    --
    -- Insert all the postcodes as ANY prefixes
    --
    insert into dataprov.DPROV_POSTCODE
      (postcode, suff_alloc, dpsuffix, res_type)
     select
      paf.postcode,
      'ANY',
      paf.dpsuffix,
      paf.res_type
      from dataprov.postcodes_paf paf,
           ccsowner.bsbaddress b,
         csc.bsbpropertydetails@csc bpc
      where paf.postcode = b.postcode 
     and paf.dpsuffix = b.dpsuffix
     and b.propertyid = bpc.id
     and paf.res_type = 'SDU'
      and b.propertyid is not null
      order by dbms_random.value;
    --
    --
    commit;
    --
    --
    insert into dataprov.DPROV_POSTCODE
      (postcode, suff_alloc, dpsuffix, res_type)
      select ba.postcode, '1PP', dpsuffix, res_type
         from dataprov.DPROV_POSTCODE ba
        where ba.postcode like '%1PP'
        order by dbms_random.value;
    --
    --
    commit;
    --
    --
    insert into dataprov.DPROV_POSTCODE
      (postcode, suff_alloc, dpsuffix, res_type)
      select ba.postcode, '1PQ', dpsuffix, res_type
         from dataprov.DPROV_POSTCODE ba
        where ba.postcode like '%1PQ'
        order by dbms_random.value;
    --
    --
    commit;
    --
    --
        insert into dataprov.DPROV_POSTCODE
      (postcode, suff_alloc, dpsuffix, res_type)
          select ba.postcode, '1ZZ', dpsuffix, res_type
         from dataprov.DPROV_POSTCODE ba
        where ba.postcode like '%1ZZ'
        order by dbms_random.value;
    --
    --
    commit;
    --
    -- Created for Selling FTTP Broadband Products
        insert into dataprov.DPROV_POSTCODE
      (postcode, suff_alloc, dpsuffix, res_type)
          select ba.postcode, '2AA', dpsuffix, res_type
         from dataprov.DPROV_POSTCODE ba
        where ba.postcode like '%2AA'
        order by dbms_random.value;
    --
    -- 
    commit;
    --
    -- Created for FTTP Upgrade Journeys Broadband Products
         insert into dataprov.DPROV_POSTCODE
      (postcode, suff_alloc, dpsuffix, res_type)
          select ba.postcode, '1EF', dpsuffix, res_type
         from dataprov.DPROV_POSTCODE ba
        where ba.postcode like '%1EF'
        order by dbms_random.value;
    --
    --
    commit;
    --
    --
    insert into dataprov.DPROV_POSTCODE
      (postcode, suff_alloc, dpsuffix, res_type)
      select ba.postcode, '2DJ', dpsuffix, res_type
         from dataprov.DPROV_POSTCODE ba
        where ba.postcode like '%2DJ'
        order by dbms_random.value;

    commit ;

   INSERT INTO dataprov.dprov_postcode t ( t.postcode , t.suff_alloc , t.dpSuffix , t.res_type )
   SELECT p.postcode , '1DJ' AS suff_alloc , p.dpSuffix , p.res_type
     FROM dataprov.dprov_postcode p
    WHERE p.postcode LIKE '%1DJ'
    ORDER BY dbms_random.value
   ;
   COMMIT ;
   -- 10-Mar-2022 Andrew Fraser for Amit More and Edwin Scariachan, added 7FU
   INSERT INTO dataprov.dprov_postcode t ( t.postcode , t.suff_alloc , t.dpSuffix , t.res_type )
   SELECT p.postcode , '7FU' AS suff_alloc , p.dpSuffix , p.res_type
     FROM dataprov.dprov_postcode p
    WHERE p.postcode LIKE '%7FU'
    ORDER BY dbms_random.value
   ;
   COMMIT ;

    insert into dataprov.DPROV_POSTCODE
      (postcode, suff_alloc, dpsuffix, res_type)
      select ba.postcode, '3FU', dpsuffix, res_type
         from dataprov.DPROV_POSTCODE ba
        where ba.postcode like '%3FU'
        order by dbms_random.value;

    commit ;
    --
    --
    insert into dataprov.DPROV_POSTCODE
      (postcode, suff_alloc, dpsuffix, res_type)
      select ba.postcode, '1FU', dpsuffix, res_type
         from dataprov.DPROV_POSTCODE ba
        where ba.postcode like '%1FU'
        order by dbms_random.value;

    commit ;
    dbms_stats.gather_table_stats ( 'DATAPROV' , 'DPROV_POSTCODE') ;
end populate_dprov_postcode ;

PROCEDURE populate_dprov_postcode_mdu IS
   --#############################################################
   -- Rebuild dprov_postcode_mdu (multi domain unit, i.e. flat with more than 4 properties in a block)
   -- Used with http://nftdatan01/dataprov/postcode?resType=MDU from below get_postcode procedure.
   -- Change History:
   --   04-May-2018 Andrew Fraser request Alex Benetatos, restrict to 'am pm' engineer visit areas. NFT-6137.
   --   23-May-2017 Andrew Fraser initial version, separated out from procedure populate_dprov_postcode.
   --#############################################################
BEGIN
   -- Re-build table dprov_postcode_mdu (multi domain unit, i.e. flat with more than 4 properties in a block)
   EXECUTE IMMEDIATE 'truncate table dataprov.dprov_postcode_mdu' ;
   INSERT /*+ APPEND */ INTO dataprov.dprov_postcode_mdu ( pk_value , postcode , outputted , suff_alloc , seqno , dpSuffix , res_type , propertyId )
   WITH ampm AS (
      SELECT /*+ noparallel materialize */ pc.postCode AS area_plus_district
        FROM refdatamgr.v_bsbFruPostCodes pc
        JOIN refdatamgr.bsbFru f ON pc.fruid = f.id
        JOIN refdatamgr.bsbFruToAppointBandProfile a ON a.fruId = f.id
        JOIN refdatamgr.bsbAppointmentBandProfile abp on abp.id = a.appointmentBandProfileId
       WHERE f.rdmDeletedFlag = 'N'
         AND a.rdmDeletedFlag = 'N'
         AND f.installerRegionId != 'VIP'  -- vip customers (only) can often get am/pm appointments regardless of area
         AND abp.appointmentBandProfile = 'AM PM'
         AND LENGTH ( pc.postCode ) = 4  -- required for LIKE join below. Note that despite its name, pc.postcode is NOT a postcode, it is an area+district code, e.g. W4 or G42 or AB12. In contrast m.postcode is a real postcode.
   )
   SELECT SYS_GUID()  -- pk_value
        , mp.postcode  -- postcode
        , NULL  -- outputted
        , 'ANY'  -- mp.suff_alloc
        , 0   -- seqno
        , mp.dpSuffix  -- dpSuffix
        , 'MDU'  -- res_type
        , TO_CHAR ( mp.propertyId )  -- propertyId
     FROM csc.mduProperties@csc mp
     JOIN ampm ON mp.postcode LIKE ampm.area_plus_district || '%'
    WHERE LENGTH ( mp.postcode ) = 7  -- required for above LIKE join
      AND ROWNUM <= 100*1000
    ORDER BY DBMS_RANDOM.VALUE
   ;
   COMMIT ;
   DBMS_STATS.GATHER_TABLE_STATS ( 'dataprov' , 'dprov_postcode_mdu' ) ;
END populate_dprov_postcode_mdu ;

PROCEDURE get_postcode ( v_suffix IN VARCHAR2 , v_res_type IN VARCHAR2 , v_postcode_out OUT VARCHAR2 , v_dpsuffix_out OUT VARCHAR2 , v_propid_out OUT VARCHAR2 ) IS
    --
    --#############################################################
    --Created: 05/02/2015                                         #
    --Modified: 08/05/2015                                        #
    --Last modification:                                          #
    --Last modified by: gne02                                     #
    --                                                            #
    --Usage Notes:                                                #
    --                                                            #
    --                                                            #
    --                                                            #
    --#############################################################
    --
    v_pk varchar2(32);
    --
  begin
    --
    --
    IF v_res_type = 'MDU' then
    --
    -- Selet and lock first telephone number for a given prefix.
    --
    select t1.postcode, t1.dpsuffix, t1.propertyid, t1.pk_value
      into v_postcode_out, v_dpsuffix_out, v_propid_out, v_pk
      from dataprov.dprov_postcode_mdu t1
     where t1.suff_alloc = upper(v_suffix)
       and seqno = (select min(seqno)
                      from dataprov.dprov_postcode_mdu
                     where suff_alloc = upper(v_suffix))
       and rownum = 1
       for update;
    --
    --
    -- increment seqno so usage can be tracked
    --
    --
    update dataprov.dprov_postcode_mdu t2
       set t2.seqno = t2.seqno + 1, t2.outputted = sysdate
     where t2.pk_value = v_pk;
    --
    --
    -- Commit Update.
    --
    COMMIT;
    --
    --
    --
    ELSIF v_res_type = 'SDU' then
    --
    -- Selet and lock first telephone number for a given prefix.
    --
    select t1.postcode, t1.dpsuffix, t1.propertyid,  t1.pk_value
      into v_postcode_out, v_dpsuffix_out, v_propid_out, v_pk
      from dataprov.dprov_postcode_sdu t1
     where t1.suff_alloc = upper(v_suffix)
       and seqno = (select min(seqno)
                      from dataprov.dprov_postcode_sdu
                     where suff_alloc = upper(v_suffix))
       and rownum = 1
       for update;
    --
    --
    -- increment seqno so usage can be tracked
    --
    --
    update dataprov.dprov_postcode_sdu t2
       set t2.seqno = t2.seqno + 1, t2.outputted = sysdate
     where t2.pk_value = v_pk;
    --
    --
    -- Commit Update.
    --
    COMMIT;
    --
    --
    --
    ELSE
    --
    -- Selet and lock first telephone number for a given prefix.
    --
    select t1.postcode, t1.dpsuffix, NULL,  t1.pk_value
      into v_postcode_out, v_dpsuffix_out, v_propid_out, v_pk
      from dataprov.dprov_postcode t1
     where t1.suff_alloc = upper(v_suffix)
       and seqno = (select min(seqno)
                      from dataprov.dprov_postcode
                     where suff_alloc = upper(v_suffix))
       and rownum = 1
       for update;
    --
    --
    -- increment seqno so usage can be tracked
    --
    --
    update dataprov.dprov_postcode t2
       set t2.seqno = t2.seqno + 1, t2.outputted = sysdate
     where t2.pk_value = v_pk;
    --
    --
    -- Commit Update.
    --
    COMMIT;
    --
    END IF;
    --
    --
  exception
    when no_data_found then
      raise_application_error(-20003,
                              'Suffix supplied (' || v_suffix ||
                              ') has no remaining data.');
end get_postcode ;
  
end data_postcode ;
/
