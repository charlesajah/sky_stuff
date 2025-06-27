--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure SNS_MAGIC_ADDRESS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."SNS_MAGIC_ADDRESS" 	( v_accountnumber IN VARCHAR2, v_pCodeSuffix IN VARCHAR2 , v_magicNum in varchar2, v_postcode_out OUT VARCHAR2 , v_telephone_out OUT VARCHAR2) IS
  l_dummy  varchar2(255) := null;
  l_dummy2 varchar2(255) := null;
begin
  data_postcode.get_postcode ( v_suffix => v_pCodeSuffix, v_res_type => NULL, v_postcode_out => v_postcode_out , v_dpsuffix_out => l_dummy , v_propid_out => l_dummy2 ) ;
  dynamic_data_pkg.update_cust_telno (v_accountnumber => v_accountnumber, v_suffix => v_magicNum, v_telephone_out => v_telephone_out, i_burn => TRUE) ;
  
  UPDATE ccsowner.bsbaddress badd
     set badd.postcode = v_postcode_out
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
						 --AND bct.effectivetodate is null
						 AND ba.accountnumber = v_accountnumber);

  UPDATE ccsowner.bsbaddress
     set postcode = v_postcode_out
   WHERE id in ( SELECT bar.addressid
                   FROM ccsowner.bsbaddressusagerole bar, ccsowner.bsbbillingaccount bba
				  WHERE bar.serviceinstanceid = bba.serviceinstanceid
				    --AND bar.effectiveto is null
					AND bba.accountnumber = v_accountnumber);

  UPDATE ccsowner.bsbaddress
     set postcode = v_postcode_out
   WHERE id in (SELECT bbar.addressid
                  FROM CCSOWNER.bsbbillingaddressrole bbar, ccsowner.bsbbillingaccount bba
				 WHERE bbar.billingaccountid = bba.id
				   --AND bbar.effectiveto is null 
				   AND bba.accountnumber = v_accountnumber);
   COMMIT;

END SNS_Magic_Address ;

/

  GRANT EXECUTE ON "DATAPROV"."SNS_MAGIC_ADDRESS" TO "BATCHPROCESS_USER";
