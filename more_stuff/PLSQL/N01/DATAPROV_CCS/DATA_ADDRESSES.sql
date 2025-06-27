CREATE OR REPLACE package          data_addresses as
  --
  --#############################################################
  --Created: 22/07/2015                                        #
  --Modified: 22/07/2015                                       #
  --Last modification: creation                                 #
  --Last modified by: rke06                                     #
  --                                                            #
  --Usage Notes:                                                #
  --                                                            #
  --                                                            #
  --                                                            #
  --#############################################################
  --
  procedure get_addresses(v_suffix in varchar2,
                          v_res_type in varchar2, 
                          v_postcode_out out varchar2, 
                          v_dpsuffix_out out varchar2, 
                          v_housenumber_out out varchar2,
                          v_street_out out varchar2, 
                          v_town_out out varchar2, 
                          v_propid_out out varchar2,
                          v_account_out out varchar2);
  --
end data_addresses;
/


CREATE OR REPLACE package body          data_addresses as
  --
  --#############################################################
  --Created: 11/03/2015                                         #
  --Modified:                                                   #
  --Last modification: Modified                                 #
  --Last modified by: gne02                                     #
  --                                                            #
  --Usage Notes:   Uses data from PAF database extract now      #
  --                                                            #
  --                                                            #
  --                                                            #
  --#############################################################

  procedure get_addresses(v_suffix in varchar2,
                          v_res_type in VARCHAR2, 
                          v_postcode_out out varchar2, 
                          v_dpsuffix_out out varchar2, 
                          v_housenumber_out out varchar2, 
                          v_street_out out varchar2, 
                          v_town_out out varchar2, 
                          v_propid_out out varchar2,
                          v_account_out out varchar2) is
    --
    --#############################################################
    --Created: 22/07/2015                                         #
    --Modified: 22/07/2015                                        #
    --Last modification:                                          #
    --Last modified by: rke06                                     #
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
    select t1.postcode, t1.dpsuffix, t1.housenumber, t1.street, t1.town, t1.propertyid, t1.pk_value, t1.accountnumber
      into v_postcode_out, v_dpsuffix_out, v_housenumber_out, v_street_out, v_town_out, v_propid_out, v_pk, v_account_out
      from dataprov.addresses_for_mdu t1
     where t1.suff_alloc = upper(v_suffix)
       and seqno = (select min(seqno)
                      from dataprov.addresses_for_mdu
                     where suff_alloc = upper(v_suffix))
       and rownum = 1
       for update;
    --
    --
    -- increment seqno so usage can be tracked
    --
    --
    update dataprov.addresses_for_mdu t2
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
    -- Select and lock first telephone number for a given prefix.
    --
     select t1.postcode, t1.dpsuffix, t1.housenumber, t1.street, t1.town, t1.propertyid, t1.pk_value,null
      into v_postcode_out, v_dpsuffix_out, v_housenumber_out, v_street_out, v_town_out, v_propid_out, v_pk, v_account_out
      from dataprov.addresses_for_sdu t1
     where t1.suff_alloc = upper(v_suffix)
       and seqno = (select min(seqno)
                      from dataprov.addresses_for_sdu
                     where suff_alloc = upper(v_suffix))
       and rownum = 1
       for update;
    --
    --
    -- increment seqno so usage can be tracked
    --
    --
    update dataprov.addresses_for_sdu t2
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
    END IF;
    --
    --
  exception
    when no_data_found then
      raise_application_error(-20003,
                              'Suffix supplied (' || v_suffix ||
                              ') has no remaining data.');
  end get_addresses;
  --
  --
end data_addresses ;
/
