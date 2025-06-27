--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure NOWTV_PROFILE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."NOWTV_PROFILE" (v_segment_id in varchar2,
                         v_rec_out  out sys_refcursor) is
    --
    --#############################################################
    --Created: 24/06/2016                                         #
    --Modified:                                                   #
    --Last modification:                                          #
    --Last modified by: rke06                                     #
    --                                                            #
    --Usage Notes:                                                #
    --                                                            #
    --                                                            #
    --                                                            #
    --#############################################################
    --
    v_nowtv_profile dataprov.NOWTV_MIGRATION_STAGING%rowtype;

  begin
    --
    -- Select and lock first nowtv_profile for a given test.
    --
    select t1.*
      into v_nowtv_profile
      from dataprov.NOWTV_MIGRATION_STAGING t1
     where profile_alloc = 'A'
       and segment_id = v_segment_id
       and rownum = 1
       for update;
    --
    -- Set flag from Available to Used.
    --
    update dataprov.NOWTV_MIGRATION_STAGING 
     set outputted = sysdate, 
     profile_alloc = 'U' 
     where nowtv_profile_id = v_nowtv_profile.nowtv_profile_id;
    --
    -- Commit Update.
    --
    COMMIT;
    --
    -- Populate return cursor
    --
    open v_rec_out for
      select *
        from dataprov.NOWTV_MIGRATION_STAGING t
       where t.nowtv_profile_id = v_nowtv_profile.nowtv_profile_id;
  exception
    when no_data_found then
      raise_application_error(-20003,
                              'Test supplied (' || upper(v_segment_id) ||
                              ') has no remaining data.');
  end nowtv_profile;
  --

/

  GRANT EXECUTE ON "DATAPROV"."NOWTV_PROFILE" TO "BATCHPROCESS_USER";
