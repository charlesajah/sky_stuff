--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure UPDATE_CUST_PCODE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."UPDATE_CUST_PCODE" 	( v_accountnumber IN VARCHAR2, v_pCodeSuffix IN VARCHAR2 , v_postcode_out OUT VARCHAR2) IS
  l_dummy  varchar2(255) := null;
  l_dummy2 varchar2(255) := null;
begin
  data_postcode.get_postcode ( v_suffix => v_pCodeSuffix, v_res_type => NULL, v_postcode_out => v_postcode_out , v_dpsuffix_out => l_dummy , v_propid_out => l_dummy2 ) ;

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

END update_cust_pcode;

/

  GRANT EXECUTE ON "DATAPROV"."UPDATE_CUST_PCODE" TO "BATCHPROCESS_USER";
