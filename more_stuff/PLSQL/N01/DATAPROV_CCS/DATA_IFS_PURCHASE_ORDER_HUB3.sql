--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure DATA_IFS_PURCHASE_ORDER_HUB3
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."DATA_IFS_PURCHASE_ORDER_HUB3" (in_part_no IN VARCHAR2, in_contract IN VARCHAR2, in_quantity IN NUMBER, in_coordinator VARCHAR2, in_supplier VARCHAR2, out_ordernumber OUT VARCHAR2)
IS
v_part_no             VARCHAR2(32000) := in_part_no;--'15303';
v_contract            VARCHAR2(32000) := in_contract;--'ASLOV';
v_qty                 NUMBER := in_quantity;--10;
v_coordinator         VARCHAR2(32000) := in_coordinator;--'LOF03';
v_supplier            VARCHAR2(32000) := in_supplier;--'SLC';

info_                 VARCHAR2(32000);
objid_                VARCHAR2(32000);
objversion_           VARCHAR2(32000);
attr_                 VARCHAR2(32000);
action_               VARCHAR2(32000) := 'DO';

cursor c_get_order_no is
select * from IFSAPP.purchase_order@IFS011N_IFSAPP where objid = objid_ and objversion = objversion_;

v_order_no ifsapp.purchase_order@IFS011N_IFSAPP%rowtype;

cursor c_get_sales_part is
select * from ifsapp.sales_part@IFS011N_IFSAPP
where contract = v_contract
and PARENT_PART= v_part_no;

v_sales_part_rec ifsapp.sales_part@IFS011N_IFSAPP%rowtype;

begin
  
  open c_get_sales_part;
  fetch c_get_sales_part into v_sales_part_rec;
  close c_get_sales_part;
  
  info_        := NULL;
  objid_       := NULL;
  objversion_  := NULL;
  
  Client_SYS.Clear_Attr(attr_);
  Client_SYS.Add_To_Attr('CONTRACT', v_sales_part_rec.contract, attr_);
  Client_SYS.Add_To_Attr('INVOICING_SUPPLIER', v_supplier, attr_);
  Client_SYS.Add_To_Attr('VENDOR_NO', v_supplier, attr_);
  Client_SYS.Add_To_Attr('AUTHORIZE_CODE', v_coordinator, attr_);
  Client_SYS.Add_To_Attr('ORDER_CODE', 1, attr_);
  Client_SYS.Add_To_Attr('ORDER_DATE', sysdate, attr_);
  Client_SYS.Add_To_Attr('WANTED_RECEIPT_DATE', trunc(sysdate+1), attr_);

  PURCHASE_ORDER_API.NEW__( info_, objid_, objversion_, attr_, action_ );
  
  open c_get_order_no;
  fetch c_get_order_no into v_order_no;
  close c_get_order_no;
  
  out_ordernumber:=v_order_no.order_no;
  dbms_output.put_line('Purchase order number = '||v_order_no.order_no);
  
  info_        := NULL;
  objid_       := NULL;
  objversion_  := NULL;
  
  Client_SYS.Clear_Attr(attr_);
  Client_SYS.Add_To_Attr('ORDER_NO', v_order_no.order_no, attr_);
  Client_SYS.Add_To_Attr('LINE_NO', 1, attr_);
  Client_SYS.Add_To_Attr('RELEASE_NO', 1, attr_);
  Client_SYS.Add_To_Attr('BUYER_CODE', v_order_no.BUYER_CODE, attr_);
  Client_SYS.Add_To_Attr('BUY_UNIT_MEAS', v_sales_part_rec.sales_unit_meas, attr_);
  Client_SYS.Add_To_Attr('CONTRACT', v_sales_part_rec.contract, attr_);
  Client_SYS.Add_To_Attr('CURRENCY_CODE', v_order_no.CURRENCY_CODE, attr_);
  Client_SYS.Add_To_Attr('ADDRESS_ID', v_order_no.ADDR_NO, attr_);
  Client_SYS.Add_To_Attr('ENG_CHG_LEVEL', 1, attr_);
  Client_SYS.Add_To_Attr('ORDER_CODE', 1, attr_);
  Client_SYS.Add_To_Attr('PART_NO', v_part_no, attr_);
  Client_SYS.Add_To_Attr('BUY_QTY_DUE', v_qty, attr_);
  Client_SYS.Add_To_Attr('PRICE_UNIT_MEAS', v_sales_part_rec.PRICE_UNIT_MEAS, attr_);
  Client_SYS.Add_To_Attr('ADDITIONAL_COST_AMOUNT', 0, attr_);
  Client_SYS.Add_To_Attr('BUY_UNIT_PRICE', v_sales_part_rec.list_price, attr_);
  Client_SYS.Add_To_Attr('CLOSE_CODE', 'Automatic', attr_);
  Client_SYS.Add_To_Attr('CONV_FACTOR', v_sales_part_rec.CONV_FACTOR, attr_);
  Client_SYS.Add_To_Attr('CURRENCY_RATE', 1, attr_);
  Client_SYS.Add_To_Attr('DATE_ENTERED', sysdate, attr_);
  Client_SYS.Add_To_Attr('DESCRIPTION', v_sales_part_rec.catalog_desc, attr_);
  Client_SYS.Add_To_Attr('FBUY_UNIT_PRICE', 1, attr_);
  Client_SYS.Add_To_Attr('LAST_ACTIVITY_DATE', sysdate, attr_);
  Client_SYS.Add_To_Attr('ORIGINAL_QTY', v_qty, attr_);
  Client_SYS.Add_To_Attr('PLANNED_DELIVERY_DATE', trunc(sysdate+1), attr_);
  Client_SYS.Add_To_Attr('PROMISED_DELIVERY_DATE', trunc(sysdate+1), attr_);
  Client_SYS.Add_To_Attr('WANTED_DELIVERY_DATE', trunc(sysdate+1), attr_);
  Client_SYS.Add_To_Attr('PLANNED_RECEIPT_DATE', trunc(sysdate+1), attr_);
  Client_SYS.Add_To_Attr('PRICE_CONV_FACTOR', v_sales_part_rec.price_conv_factor, attr_);
  Client_SYS.Add_To_Attr('ORD_CONF_REM_NUM', 0, attr_);
  Client_SYS.Add_To_Attr('DELIVERY_REM_NUM', 0, attr_);
  Client_SYS.Add_To_Attr('ORD_CONF_REMINDER', 'No Order Reminder', attr_);
  Client_SYS.Add_To_Attr('DELIVERY_REMINDER', 'No Delivery Reminder', attr_);
  Client_SYS.Add_To_Attr('RECEIVE_CASE', 'Receive into Inventory', attr_);
  Client_SYS.Add_To_Attr('PURCHASE_PAYMENT_TYPE', 'Normal', attr_);
  Client_SYS.Add_To_Attr('DEFAULT_ADDR_FLAG', 'Yes', attr_);
  Client_SYS.Add_To_Attr('FREEZE_FLAG', 'Free', attr_);
  Client_SYS.Add_To_Attr('INVOICING_SUPPLIER', 'SLC', attr_);
  Client_SYS.Add_To_Attr('INTRASTAT_EXEMPT', 'Include', attr_);
  
  
  PURCHASE_ORDER_LINE_PART_API.NEW__( info_, objid_, objversion_, attr_, action_ );
  
  PURCHASE_ORDER_API.Release_Order(v_order_no.order_no);
  
  --Purchase_Order_Transfer_API.Send_Purchase_Order(v_order_no.order_no,'MHS');
  
  --Purchase_Order_Transfer_API.Send_Purchase_Order(v_order_no.order_no,'INET_TRANS');
  
  commit;  

end;

/

  GRANT EXECUTE ON "DATAPROV"."DATA_IFS_PURCHASE_ORDER_HUB3" TO "BATCHPROCESS_USER";
