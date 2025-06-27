--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure REFRESH_CLEARDOWN_TRUNCATE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."REFRESH_CLEARDOWN_TRUNCATE" AS
/*
|| Name : dataprov.refresh_cleardown_truncate in chordo/ccs021n database n01/n02.
|| Purpose : Before KEEP export job of environment refresh, run this procedure in n01/n02 to clear out non-essential data to make exp/imp faster.
||           Tables that tests make use of throughout the day are left untouched by this procedure.
|| Change History:
||    22-Feb-2017 Andrew Fraser initial creation.
*/
BEGIN
   -- 1) Log tables are large, and not essential.
   --EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.dprov_accounts_log' ;
   --EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.dprov_accounts_static_log' ;
   -- 2) Truncated anyway in package data_prep
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_uk_cust' ;
   --EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.fspAccounts' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_uk_subs' ;  
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.minimim_term' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_act_visit' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_sky_plus' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_no_bb_st_vis' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_no_bb_st_vis_roi' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_no_dtv' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_no_tech_enq' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_inst_prod' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.tm_pc' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_gen_enq' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.cancelled' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.ACT_CUST_BB_ST_NO_VIS' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_ppv_with_pin' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_no_paper_bill' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_for_gvp' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_wlr3' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.nvn_cease' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.bb_new' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.dp_lws_paircard_mi' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.dp_assessforetc' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.dp_thirdpartycease' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.dp_lws_reinstate' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.dp_tay_cancel' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.dp_act_cust_callback' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.restrictedpartyid' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_av_bb' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_av_talk' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_av_dtv' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_no_nowtv' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_vmail' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_smpfponnet' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_nossfive' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_ethan' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_sky_hd' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_est_set_limit' ;
   -- 3) Truncated anyway in package data_prep_static
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.dataforcustomersvc' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.accServiceid_parentalcontrols' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.customersforetc' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.pcscarddetails' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.custforvalidateserialnumber' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.getcommunication' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.properties' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.accountnumberforcase' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.useridforcase' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.accountnumberswithsnapshots' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.dataforgetinventory' ;
   --EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.broadbandorders' ;
   --EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.cancelconfirmcommunication' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.customersforpaircardcallback' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.customerswithpaymentdetails' ;
   --EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.reissuecommunication' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.addresses' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.names' ;
   --EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.postcodeprefix' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.ordermanagementorders' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.partyidsforcase' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.dataforinventoryservices' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.delprodcodeforinvservices' ;
   --EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.catproductforinvservices' ;
   --EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.rentaldigitalcontent' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.partyidsforpaircard' ;
   --EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.caseidforclosecase' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.findorders' ;
   --EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.engineers' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.reopen_reassign_case' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.queueswithnotes' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.caseresptemplate' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.teamidforcase' ;
   --EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.item_call_billing' ;
   --EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.manage_property_info' ;
   --EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.getcommunication_sms' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.getcommunication_email' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.getcommunication_con' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.bill_ref_account_id' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.onlineprofileid_cust_search' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_techenq' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.sourcesystemorderid' ;
   --EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.rescheduledjobs' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.avbroadbandservicecheckid' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.salesinteractionref' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.salesinteractionrefinv' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.nowtv_accounts' ;
   --EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.nowtv_accounts_no_sports_pass' ;
   --EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.nowtv_accounts_sports_pass' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.get_inv_om_preactive' ;
   --EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.ddr_get_cache' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_ethan_static' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_dtv_bb_talk' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_dtv_bb_talk_prtid' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_trpl_ply_cust_lt' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_cust_dtv_bb_talk_mob' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.mobile_orders' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_mobile_numbers' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_mob_no_pac' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_mobile_billed' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.port_out_cust' ;
   --EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.mob_parental_control' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.multi_sim_sales' ;
   --used in stub-- EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.loyalty_null' ;
   --EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.igor_accounts' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.mobile_change_payment_due_date' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.cca_unsigned_plans' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.mobile_device_part_no' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.act_bb_talk_assurance' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.cca_active_plans' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.partyids_open_or_closed_av' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.partyids_open_case_closed_av' ;
   --EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.mobile_cancellations' ;
   --
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.engineeropenappts' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.openreach_nlp_appts' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.bb_recontracting' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.mobileswap' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.CEASED_CUSTOMERS' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.mobile_cdr_files' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.LOYALTY_CUST_NO_MOBILE' ;
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.CUSTFORMONTHLYVCCALLBACKS' ;
   -- 4) Not a truncate, but this is a good opportunity to prevent these smaller log tables growing large.
   DELETE FROM dataprov.dp_test_refresh_runs_detail WHERE start_time < SYSDATE - 365 ;
   DELETE FROM dataprov.dp_test_refresh_runs WHERE start_time < SYSDATE - 365 ;
   DELETE FROM dataprov.dp_s_test_refresh_runs_detail WHERE start_time < SYSDATE - 365 ;
   DELETE FROM dataprov.dp_s_test_refresh_runs WHERE start_time < SYSDATE - 365 ;
   -- 5) customers table is quite large, we can live without the unallocated records short term. Alternatively could set the two guid columns to null.
   --used in stub -- DELETE FROM dataprov.customers WHERE pool IS NULL ;
   --
   -- 6) cleardown of the logon monitoring table
   EXECUTE IMMEDIATE 'TRUNCATE TABLE dataprov.dataprov_logons' ;

   COMMIT ;
END ;

/

  GRANT EXECUTE ON "DATAPROV"."REFRESH_CLEARDOWN_TRUNCATE" TO "BATCHPROCESS_USER";
