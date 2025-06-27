CREATE OR REPLACE PACKAGE dynamic_data_pkg AS
PROCEDURE data_account_static ( v_testname in VARCHAR2 , v_rec_out  OUT sys_refcursor ) ;
PROCEDURE data_idnv ( v_rec_out OUT sys_refcursor ) ;
PROCEDURE data_idnv_bp ( v_rec_out OUT sys_refcursor ) ;
PROCEDURE data_idnv_triple ( v_rec_out OUT sys_refcursor ) ;
PROCEDURE get_telephone_number ( v_suffix in VARCHAR2 , v_telephone_out OUT VARCHAR2 ) ;
PROCEDURE update_cust_telno ( v_accountnumber IN VARCHAR2 , v_suffix IN VARCHAR2 , v_telephone_out OUT VARCHAR2 , i_burn IN BOOLEAN DEFAULT TRUE ) ;
PROCEDURE get_urn ( v_urn_out OUT VARCHAR2 ) ;
PROCEDURE get_mobile_urn ( v_urn_out OUT VARCHAR2 ) ;
PROCEDURE ct_telephone_numbers ( v_ct_tele OUT VARCHAR2 ) ;
PROCEDURE get_test_user ( v_prefix in VARCHAR2, v_username OUT VARCHAR2, v_password OUT VARCHAR2 ) ;
PROCEDURE pool_report_static ( v_rec_out OUT sys_refcursor ) ;
PROCEDURE data_idnv_stress ( v_rec_out OUT sys_refcursor ) ;
PROCEDURE update_vc_status ( v_accountnumber in VARCHAR2 , v_message OUT VARCHAR2 ) ;
PROCEDURE get_deviceid_seq ( v_seq_out OUT VARCHAR2 ) ;
PROCEDURE get_stb_seq ( v_seq_out OUT VARCHAR2 ) ;
PROCEDURE pool_report_tele ( v_rec_out OUT sys_refcursor ) ;
END dynamic_data_pkg ;
/


CREATE OR REPLACE PACKAGE BODY dynamic_data_pkg as

FUNCTION CheckPhoneNum (v_accountnumber IN VARCHAR2, v_suffix IN VARCHAR2)
    RETURN varchar2
IS
  l_phonenum ccsowner.bsbtelephone.combinedtelephonenumber%TYPE ;
begin
  l_phonenum := '';
  FOR rec1 IN (select substr(bt.combinedtelephonenumber,9,3) suffix, bt.combinedtelephonenumber
                    from ccsowner.bsbtelephone        bt,
                         ccsowner.bsbcontacttelephone bct,
                         ccsowner.bsbcontactor        bc,
                         ccsowner.bsbpartyrole        bpr,
                         ccsowner.bsbcustomerrole     bcr,
                         ccsowner.bsbbillingaccount   ba
                   where bt.id = bct.telephoneid
                     and bc.id = bct.contactorid
                     and bpr.partyid = bc.partyid
                     and bcr.partyroleid = bpr.id
                     and bct.typecode != 'M'
                     and bct.effectivetodate is null
                     and ba.portfolioid = bcr.portfolioid
                     and ba.accountnumber = v_accountnumber)
  LOOP
    if rec1.suffix = v_suffix then
      l_phonenum := rec1.combinedtelephonenumber;
      return l_phonenum;
    end if;
  END LOOP; 
  
  return l_phonenum;
end;

PROCEDURE data_account_static ( v_testname IN VARCHAR2 , v_rec_out OUT sys_refcursor ) IS
   l_nextVal NUMBER ;
   l_created DATE ;
   l_maxVal NUMBER ;
   e_exceeds_maxvalue EXCEPTION ;
   PRAGMA EXCEPTION_INIT ( e_exceeds_maxvalue , -8004 ) ;
BEGIN
   EXECUTE IMMEDIATE 'SELECT s' || LOWER ( v_testname ) || '.NEXTVAL FROM DUAL' INTO l_nextVal ;
   IF l_nextVal = 1
   THEN
      SELECT o.created , s.max_value INTO l_created , l_maxVal
        FROM user_sequences s
        JOIN user_objects o ON o.object_name = s.sequence_name AND o.object_type = 'SEQUENCE'
       WHERE s.sequence_name = 'S' || UPPER ( v_testname )
      ;
      MERGE INTO dprov_accounts_fast_log t USING (
         SELECT l.poolName , l.counter , l.loops , l.created
           FROM dprov_accounts_fast_log l
          WHERE l.poolName = UPPER ( v_testname )
            AND l.created = l_created
      ) s ON ( t.poolName = s.poolName AND t.created = s.created )
      WHEN MATCHED THEN UPDATE SET t.counter = s.counter + l_maxVal + 1 , t.loops = s.loops + 1
      ;
      MERGE INTO dprov_accounts_fast_log t USING (
         SELECT NULL FROM dual
      ) s ON ( t.poolName = UPPER ( v_testname ) AND t.created = l_created )
      WHEN NOT MATCHED THEN INSERT ( t.poolName , t.counter , t.loops , t.created, t.max_value )
         VALUES ( UPPER ( v_testname ) , 1 , 1 , l_created , l_maxVal )
      ;
   END IF ;
   OPEN v_rec_out FOR
      SELECT *
        FROM dprov_accounts_fast a
       WHERE a.pool_seqno = l_nextVal
         AND a.pool_name = v_testname
   ;
EXCEPTION
   WHEN e_exceeds_maxvalue THEN
      logger.tomcat_errors ( i_poolName => v_testname , i_item => 'exceeds maxvalue' ) ;
      raise_application_error ( -20003 , 'Test supplied (' || v_testname || ') has no remaining data.' ) ;
   WHEN NO_DATA_FOUND THEN
      logger.tomcat_errors ( i_poolName => v_testname , i_item => 'no_data_found - even though sequence' ) ;
      raise_application_error ( -20003 , 'Test supplied (' || v_testname || ') has no remaining data.' ) ;
   WHEN OTHERS THEN
      logger.tomcat_errors ( i_poolName => v_testname , i_item => 'others' ) ;
      RAISE ;
END data_account_static ;

PROCEDURE data_idnv ( v_rec_out OUT sys_refcursor ) is
   v_accno dataprov.act_uk_cust_idnv_bp%rowtype ;
BEGIN
   SELECT * INTO v_accno
     FROM dataprov.act_uk_cust_idnv
    WHERE seqno = (SELECT min(seqno) FROM dataprov.act_uk_cust_idnv)
      AND ROWNUM = 1
      FOR UPDATE
   ;
   UPDATE dataprov.act_uk_cust_idnv t2 set t2.seqno = t2.seqno + 1 WHERE t2.accountnumber = v_accno.accountnumber ;
   COMMIT;
   OPEN v_rec_out FOR
      SELECT *
        FROM dataprov.act_uk_cust_idnv t
       WHERE t.accountnumber = v_accno.accountnumber
      ;
EXCEPTION
   WHEN no_data_found THEN
      raise_application_error ( -20003 , 'Error: not able to get act_uk_cust_idnv data' ) ;
END data_idnv ;

PROCEDURE data_idnv_bp ( v_rec_out OUT sys_refcursor ) is
   v_accno dataprov.act_uk_cust_idnv_bp%rowtype ;
BEGIN
   SELECT * INTO v_accno
     FROM dataprov.act_uk_cust_idnv_bp
    WHERE seqno = ( SELECT min ( seqno ) FROM dataprov.act_uk_cust_idnv_bp )
      AND ROWNUM = 1
      FOR UPDATE
   ;
   UPDATE dataprov.act_uk_cust_idnv_bp t2
      set t2.seqno = t2.seqno + 1
    WHERE t2.accountnumber = v_accno.accountnumber
   ;
   COMMIT ;
   OPEN v_rec_out FOR
      SELECT *
        FROM dataprov.act_uk_cust_idnv_bp t
       WHERE t.accountnumber = v_accno.accountnumber
      ;
EXCEPTION
    WHEN no_data_found THEN
      raise_application_error ( -20003 , 'Error: not able to get act_uk_cust_idnv_bp data' ) ;
END data_idnv_bp ;

PROCEDURE data_idnv_triple ( v_rec_out OUT sys_refcursor ) IS
   v_accno dataprov.act_uk_cust_idnv_bp%rowtype ;
BEGIN
   SELECT * INTO v_accno
     FROM dataprov.act_uk_cust_idnv_triple
    WHERE seqno = ( SELECT min ( seqno ) FROM dataprov.act_uk_cust_idnv_triple )
       AND ROWNUM = 1
       FOR UPDATE
   ;
   UPDATE dataprov.act_uk_cust_idnv_triple t2
      set t2.seqno = t2.seqno + 1
    WHERE t2.accountnumber = v_accno.accountnumber
   ;
   COMMIT ;
   OPEN v_rec_out FOR
      SELECT *
        FROM dataprov.act_uk_cust_idnv_triple t
       WHERE t.accountnumber = v_accno.accountnumber
   ;
EXCEPTION
   WHEN no_data_found THEN
      raise_application_error ( -20003 , 'Error: not able to get act_uk_cust_idnv_triple data' ) ;
END data_idnv_triple ;

PROCEDURE get_telephone_number ( v_suffix in VARCHAR2 , v_telephone_out OUT VARCHAR2 ) is
BEGIN
   -- Select AND lock first telephone number FOR a given prefix.
   SELECT t1.combinedtelephonenumber
     INTO v_telephone_out
     FROM dprov_telephone t1
    WHERE t1.v_num_alloc = 'A'
      AND t1.suff_alloc = v_suffix
      AND ROWNUM = 1
      FOR UPDATE
   ;
   -- Set telephone number flag FROM Avaliable to Used.
   execute immediate ( 'UPDATE dprov_telephone set outputted = sysdate , magic_' || v_suffix || ' = ''U'' WHERE combinedTelephoneNumber = ' || v_telephone_out ) ;
   COMMIT;
EXCEPTION
   WHEN no_data_found THEN
      raise_application_error ( -20003 , 'Suffix supplied (' || v_suffix || ') has no remaining data.' ) ;
END get_telephone_number ;

PROCEDURE update_cust_telno (
---#############################################################
-- Modified:                                                   #
-- 25-Jun-2021 Andrew Fraser added i_burn parameter to be used by data_prep_01.bbregrade because that pool was burning up all the data FOR suffix 909.
-- 07/03/2014 gne02 creation
--##############################################################
     v_accountnumber IN VARCHAR2
   , v_suffix IN VARCHAR2
   , v_telephone_out OUT VARCHAR2
   , i_burn IN BOOLEAN DEFAULT TRUE
) IS
   l_count NUMBER ;
   l_postcode VARCHAR2(10);
BEGIN
    -- Get next avaliable telephone number FOR given prefix.
    IF i_burn = TRUE
    THEN
     --v_telephone_out := CheckPhoneNum (v_accountnumber, v_suffix) ;
     --if v_telephone_out = '' then
       get_telephone_number ( v_suffix => v_suffix , v_telephone_out => v_telephone_out ) ;
     --end if;  
    ELSE
      -- 25-Jun-2021 AF just pick up a random row FROM the pool without burning any data FOR data_prep_01.bbregrade, because that was burning up all the data FOR suffix 909.
      SELECT s.combinedTelephoneNumber INTO v_telephone_out
        FROM (
             SELECT t.combinedTelephoneNumber
               FROM dprov_telephone t
              WHERE t.suff_alloc = v_suffix  -- '909' FOR bbRegrade, '902' FOR digBBOnlyRegradeNoBurn
              ORDER BY dbms_random.value
             ) s
       WHERE ROWNUM = 1
      ;
    END IF ;
    --
    -- Update telephone numbers in bsbtelephone FOR the given account number
    -- There may be more than one as there is a home AND office number.
    --
    UPDATE ccsowner.bsbtelephone btu
       set btu.areacode                = substr(v_telephone_out, 0, 4),
           btu.telephonenumber         = substr(v_telephone_out, 5, 7),
           btu.combinedtelephonenumber = v_telephone_out
     WHERE id in (SELECT bt.id
                    FROM ccsowner.bsbtelephone        bt,
                         ccsowner.bsbcontacttelephone bct,
                         ccsowner.bsbcontactor        bc,
                         ccsowner.bsbpartyrole        bpr,
                         ccsowner.bsbcustomerrole     bcr,
                         ccsowner.bsbbillingaccount   ba
                   WHERE bt.id = bct.telephoneid
                     AND bc.id = bct.contactorid
                     AND bpr.partyid = bc.partyid
                     AND bcr.partyroleid = bpr.id
                     and bct.typecode != 'M'
                     AND ba.portfolioid = bcr.portfolioid
                     AND ba.accountnumber = v_accountnumber);
    
    UPDATE ccsowner.bsbserviceinstance bsi
       set bsi.telephonenumber = v_telephone_out
     WHERE ParentServiceInstanceId in (SELECT ba.ServiceInstanceId
                                         FROM ccsowner.bsbbillingaccount ba
                                        WHERE ba.accountnumber = v_accountnumber);

    -- added at request of Shane Venter 
    -- Different suffixes have different postcodes but the SQL to update tables is identical.
    -- Set a variable and reuse the code.
    
    IF    v_suffix = '660' THEN l_postcode := 'M297FU';
    ELSIF v_suffix = '900' THEN l_postcode := 'NW107FU';
    ELSIF v_suffix = '901' THEN l_postcode := 'SE17FU';
    ELSIF v_suffix = '908' THEN l_postcode := 'ME207FU'; 
    ELSIF v_suffix = '909' THEN l_postcode := 'HP197FU';
    END IF;
    
    UPDATE ccsowner.bsbaddress badd
       set badd.postcode = l_postcode
     WHERE badd.id in ( SELECT bct.addressid
                          FROM ccsowner.bsbcontactaddress   bct,
                               ccsowner.bsbcontactor        bc,
                               ccsowner.bsbpartyrole        bpr,
                               ccsowner.bsbcustomerrole     bcr,
                               ccsowner.bsbbillingaccount   ba
                         WHERE bc.id = bct.contactorid
                           AND bpr.partyid = bc.partyid
                           AND bcr.partyroleid = bpr.id
                           AND ba.portfolioid = bcr.portfolioid
                           AND bct.effectivetodate is null
                           AND ba.accountnumber = v_accountnumber);

    UPDATE ccsowner.bsbaddress
       set postcode = l_postcode
     WHERE id in ( SELECT bar.addressid
                     FROM ccsowner.bsbaddressusagerole bar, ccsowner.bsbbillingaccount bba
                    WHERE bar.serviceinstanceid = bba.serviceinstanceid  
                      AND bar.effectiveto is null
                      AND bba.accountnumber = v_accountnumber);

    UPDATE ccsowner.bsbaddress
       set postcode = l_postcode
     WHERE id in (SELECT bbar.addressid
                    FROM CCSOWNER.bsbbillingaddressrole bbar, ccsowner.bsbbillingaccount bba
                   WHERE bbar.billingaccountid = bba.id
                     AND bbar.effectiveto is null 
                     AND bba.accountnumber = v_accountnumber);
   
   COMMIT;
END update_cust_telno ;

PROCEDURE get_urn ( v_urn_out OUT VARCHAR2 ) IS
BEGIN
   SELECT urn INTO v_urn_out
     FROM dataprov.app_urn
    WHERE used is NULL
      AND ROWNUM = 1
      FOR UPDATE
   ;
   UPDATE dataprov.app_urn au set au.used = 'U' WHERE urn = v_urn_out ;
   COMMIT;
EXCEPTION
   WHEN no_data_found THEN
      raise_application_error ( -20003 , 'Test supplied (get_urn) has no remaining data.' ) ;
END get_urn ;

PROCEDURE get_mobile_urn ( v_urn_out OUT VARCHAR2 ) IS
BEGIN
   SELECT urn INTO v_urn_out
     FROM dataprov.mobile_urn
    WHERE used is NULL
      AND ROWNUM = 1
      FOR UPDATE
   ;
   UPDATE dataprov.mobile_urn au set au.used = 'U' WHERE urn = v_urn_out;
   COMMIT ;
EXCEPTION
   WHEN no_data_found THEN
         raise_application_error ( -20003 , 'Test supplied (get_mobile_urn) has no remaining data.' ) ;
END get_mobile_urn ;

PROCEDURE ct_telephone_numbers (v_ct_tele OUT VARCHAR2) IS
BEGIN
   SELECT t1.telephonenumber INTO v_ct_tele
     FROM dataprov.ct_telephone_numbers t1
    WHERE t1.seqno = ( SELECT MIN ( m.seqno ) FROM dataprov.ct_telephone_numbers m )
      AND ROWNUM = 1
      FOR UPDATE
   ;
   UPDATE dataprov.ct_telephone_numbers t2
      SET t2.seqno = t2.seqno + 1
    WHERE t2.telephonenumber = v_ct_tele -- data is still usable, can be picked up once cycled around.
   ;
   COMMIT ;
EXCEPTION
   WHEN no_data_found THEN
      raise_application_error ( -20003 , 'no data' ) ;
END ct_telephone_numbers ;

PROCEDURE get_test_user ( v_prefix in VARCHAR2 , v_username OUT VARCHAR2 , v_password OUT VARCHAR2 ) is
   -- 17/05/2016 Andrew Fraser exclude four specific wlr usernames request Bruce Thomson. They are arbitrarily replaced with a username 500 digits further along.
   v_sequence_name dataprov.dprov_users_setup.sequence_name%TYPE ;
   v_sequence_no NUMBER ;
BEGIN
   SELECT us.sequence_name , us.user_password INTO v_sequence_name , v_password
     FROM dataprov.dprov_users_setup us
    WHERE LOWER ( us.prefix ) = LOWER ( v_prefix )
   ;
   execute immediate 'SELECT dataprov.' || v_sequence_name || '.nextval FROM dual' INTO v_sequence_no ;
   -- generate username FROM prefix, pad AND sequence number
   if v_prefix = '800' then
     v_username := v_prefix || lpad(v_sequence_no, 3, '0');
   else
     v_username := v_prefix || lpad(v_sequence_no, 4, '0');
   end if;  
   -- Andrew Fraser 17-May-2016 exclude four specific wlr usernames request Bruce Thomson. They are arbitrarily replaced with a username 500 digits further along.
   IF v_username IN ( 'wlr0101' , 'wlr0102' , 'wlr0103' , 'wlr0104' )
   THEN
      v_username := REPLACE ( v_username , 'wlr0' , 'wlr6' ) ;
   END IF ;
EXCEPTION
   WHEN no_data_found THEN
      raise_application_error ( -20003 , 'Prefix supplied (' || v_prefix || ') is not valid.' ) ;
END get_test_user ;

PROCEDURE pool_report_static ( v_rec_out OUT sys_refcursor ) IS
--#############################################################
--Created: 28/04/2014                                         #
--Modified:
--   23-Mar-2018 Andrew Fraser handle nulls, outer join, materialize FOR Liam Fleming NFTREL-12536.
--   08-Dec-2017 Andrew Fraser added addOpenUpdateCase FOR Liam.
--   23-Nov-2017 Added FSP/customers pools FOR Alex Benetatos Focus Alerts.
--                                                            #
--Usage Notes:                                                #
--  This is what is called by Focus alerting.
--#############################################################
BEGIN
   OPEN v_rec_out FOR
      WITH empties AS (
                      SELECT 'DATAFORCUSTOMERSVC' AS test_alloc , 0 AS volume , TO_DATE ( '01/01/1970' , 'DD/MM/YYYY' ) AS load_date FROM DUAL UNION
                      SELECT 'CEASED_CUSTOMERS' AS test_alloc , 0 AS volume , TO_DATE ( '01/01/1970' , 'DD/MM/YYYY' ) AS load_date FROM DUAL UNION
                      SELECT UPPER('accServiceid_parentalcontrols') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('customersforetc') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('pcscarddetails') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('custforvalidateserialnumber') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('getcommunication') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('properties') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('dataforgetinventory') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('broadbandorders') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('cancelconfirmcommunication') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('customersforpaircardcallback') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('customerswithpaymentdetails') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('act_cust_ppv_with_pin') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('reissuecommunication') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('addresses') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('names') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('postcodeprefix') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('ordermanagementorders') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('partyidsforcase') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('addresses_for_dtv') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('dataforinventoryservices') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('delprodcodeforinvservices') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('catproductforinvservices') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('casenum') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('accountsforviewingproducts') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('rentaldigitalcontent') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('partyidsforpaircard') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('caseidforclosecase') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('findorders') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('engineers') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('reopen_reassign_case') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('queueswithnotes') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('caseresptemplate') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('teamidforcase') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('item_call_billing') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('manage_property_info') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('getcommunication_sms') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('getcommunication_email') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('getcommunication_con') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('bill_ref_account_id') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('onlineprofileid_cust_search') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT 'ADDOPENUPDATECASE' AS test_alloc , 0 AS volume , TO_DATE ( '01/01/1970' , 'DD/MM/YYYY' ) AS load_date FROM dual union
                      SELECT UPPER('mobile_change_payment_due_date') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('mob_device_cooling_off') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual union
                      SELECT UPPER('igor_accounts') test_alloc , 0 volume, to_date('01/01/1970','DD/MM/YYYY') load_date FROM dual 
      ) , seq_pools AS (  -- check sequence number maxvalue FOR FSP/customer pools
         SELECT /*+ MATERIALIZE */ SUBSTR ( s.sequence_name , 2 ) AS test_alloc
              , s.max_value - s.last_number AS volume
              , NULL AS load_date
           FROM all_sequences s
          WHERE s.sequence_owner = 'DATAPROV'
            -- AND s.cycle_flag = 'N'  -- uncomment this if just want pools that burn data
      )
   SELECT *
     FROM empties e
    UNION
   SELECT sp.test_alloc
        , NVL ( sp.volume , 0 ) AS volume
        , NULL AS load_date
     FROM seq_pools sp
    ORDER BY 1
   ;
END pool_report_static ;

PROCEDURE data_idnv_stress ( v_rec_out OUT sys_refcursor ) is
   v_accno dataprov.idandvextendedhistory.accountnumber%type ;
BEGIN
   SELECT accountnumber INTO v_accno
     FROM dataprov.idandvextendedhistory
    WHERE seqno = ( SELECT min ( seqno ) FROM dataprov.idandvextendedhistory )
      AND ROWNUM = 1
      FOR UPDATE
   ;
   UPDATE dataprov.idandvextendedhistory t2
      set t2.seqno = t2.seqno + 1
    WHERE t2.accountnumber = v_accno
   ;
   COMMIT ;
   OPEN v_rec_out FOR
      SELECT accountnumber
       FROM dataprov.idandvextendedhistory t
      WHERE t.accountnumber = v_accno
   ;
EXCEPTION
   WHEN no_data_found THEN
      raise_application_error ( -20003 , 'No data was found in dataprov.idandvextendedhistory' ) ;
END data_idnv_stress ;

PROCEDURE update_vc_status ( v_accountnumber in VARCHAR2 , v_message OUT VARCHAR2 ) is
   v_rowcounter number :=0 ;
BEGIN
   -- Update status of viewingcard FOR a given account number to In-Transit to support MFT project
   UPDATE ccsowner.bsbcustomerproductelement bce
      set bce.status = 'T'
    WHERE bce.id in (
          SELECT bce2.id
            FROM ccsowner.bsbcustomerproductelement bce2,
                 ccsowner.bsbportfolioproduct       bpp,
                 ccsowner.bsbbillingaccount         bba
           WHERE bpp.id = bce2.portfolioproductid
             AND bpp.portfolioid = bba.portfolioid
             AND bpp.catalogueproductid = '10137'
             AND bba.accountnumber = v_accountnumber
          )
      AND bce.status = 'RQ'
   ;
   v_rowcounter := v_rowcounter + sql%rowcount;
   UPDATE ccsowner.bsbportfolioproduct bpp
      set bpp.status = 'T'
    WHERE bpp.id in (
          SELECT bpp2.id
            FROM ccsowner.bsbportfolioproduct       bpp2,
                 ccsowner.bsbbillingaccount         bba
           WHERE bpp2.portfolioid = bba.portfolioid
             AND bpp.catalogueproductid = '10137'
             AND bba.accountnumber = v_accountnumber
          )
      AND bpp.status = 'RQ'
   ;
   v_rowcounter := v_rowcounter + SQL%ROWCOUNT ;
   COMMIT ;
   IF v_rowcounter >= 2
   THEN
      v_message :=  'SUCCESS' ;
   ELSE
      v_message := 'FAILURE' ;
   END IF ;
END update_vc_status ;

PROCEDURE get_deviceid_seq ( v_seq_out OUT VARCHAR2 ) is
BEGIN
   SELECT dataprov.deviceid_seq.nextval INTO v_seq_out FROM dual ;
END get_deviceid_seq ;

PROCEDURE get_stb_seq ( v_seq_out OUT VARCHAR2 ) is
BEGIN
   SELECT dataprov.stb_seq.nextval INTO v_seq_out FROM dual ;
END get_stb_seq ;

PROCEDURE pool_report_tele ( v_rec_out OUT sys_refcursor ) is
BEGIN
   OPEN v_rec_out FOR
      WITH populated ( suff_alloc , volume ) AS (
         SELECT suff_alloc , COUNT ( suff_alloc ) AS volume
           FROM dataprov.dprov_telephone
          WHERE v_num_alloc = 'A'
          group by suff_alloc
      ) , empties ( suff_alloc , volume ) AS (
         SELECT '001' , 0 FROM DUAL UNION
         SELECT '005' , 0 FROM DUAL UNION
         SELECT '025' , 0 FROM DUAL UNION
         SELECT '518' , 0 FROM DUAL UNION
         SELECT '537' , 0 FROM DUAL UNION
         SELECT '553' , 0 FROM DUAL UNION
         SELECT '996' , 0 FROM DUAL UNION
         SELECT '900' , 0 FROM DUAL UNION
         SELECT '901' , 0 FROM DUAL UNION
         SELECT '902' , 0 FROM DUAL UNION
         SELECT '903' , 0 FROM DUAL UNION
         SELECT '904' , 0 FROM DUAL UNION
         SELECT '905' , 0 FROM DUAL UNION
         SELECT '906' , 0 FROM DUAL UNION
         SELECT '907' , 0 FROM DUAL UNION
         SELECT '908' , 0 FROM DUAL UNION
         SELECT '909' , 0 FROM DUAL UNION
         SELECT '998' , 0 FROM DUAL UNION
         SELECT '997' , 0 FROM DUAL UNION
         SELECT '660' , 0 FROM DUAL
      )
      SELECT * FROM populated
       UNION
      SELECT *
        FROM empties
       WHERE NOT EXISTS ( SELECT NULL FROM populated WHERE populated.suff_alloc = empties.suff_alloc )
   ;
END pool_report_tele ;

END dynamic_data_pkg ;
/
