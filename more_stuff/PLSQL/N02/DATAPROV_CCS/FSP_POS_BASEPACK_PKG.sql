CREATE OR REPLACE PACKAGE fsp_pos_basepack_pkg AS
/*
|| 26-Sept-2017 v0.1 Bruce Thomson
*/
   PROCEDURE get_pos_basepack_data ( v_rec_out OUT sys_refcursor );
   
   PROCEDURE write_screenshot_status ( v_rec_out OUT sys_refcursor );
   
   PROCEDURE write_basepack_status ( i_accountNumber        IN VARCHAR2,
                                    i_test_status          IN VARCHAR2,
                                    i_failure_reason       IN VARCHAR2,
                                    i_sales_types           IN VARCHAR2,
                                    i_sales_types_status    IN VARCHAR2,
                                    i_next_bill_before     IN VARCHAR2,
                                    i_next_bill_after      IN VARCHAR2 );
   PROCEDURE hist_update_one (
      i_accountNumber IN dataprov.fsp_pos_basepack_history.accountNumber%TYPE);
      
   PROCEDURE get_sale_type (i_account_number   IN     VARCHAR2,
                            i_product_name     IN     VARCHAR2,
                            o_sale_type           OUT VARCHAR2);
                            
    PROCEDURE get_data_for_post_mig_shot ( v_rec_out OUT sys_refcursor );
    
    PROCEDURE insert_url ( i_account_number IN VARCHAR2, 
                           i_url_kenansnapshot IN VARCHAR2,  
                           i_url_billview_current IN VARCHAR2, 
                            i_url_billview_future IN VARCHAR2, 
                            i_url_portfolioview IN VARCHAR2, 
                            i_timing IN VARCHAR2);
    
    PROCEDURE write_screenshot_status_skyid ( v_rec_out OUT sys_refcursor );
    
END fsp_pos_basepack_pkg ;
/


CREATE OR REPLACE PACKAGE BODY fsp_pos_basepack_pkg AS
/*
|| 26-Sep-2017 v0.1
*/
-- 1) when selected from URL
PROCEDURE get_pos_basepack_data ( v_rec_out OUT sys_refcursor )
IS
   v_fsp_pos_basepack dataprov.fsp_pos_basepack%ROWTYPE ;
BEGIN
    -- Select and lock first fsp_pos_basepack for a given test.
   SELECT t.*
     INTO v_fsp_pos_basepack
     FROM dataprov.fsp_pos_basepack t
    WHERE t.outputted IS NULL
      and t.screenshotted > TRUNC(SYSDATE-INTERVAL '6' HOUR)
      and t.url_portfolioview_before IS NOT NULL
      AND ROWNUM = 1
      FOR UPDATE
   ;
   -- Set flag from Available to Used.
   UPDATE dataprov.fsp_pos_basepack t
      SET t.outputted = SYSDATE
    WHERE t.accountNumber = v_fsp_pos_basepack.accountNumber
   ;
   COMMIT ;
   -- record into history
   INSERT INTO dataprov.fsp_pos_basepack_history h (h.PORTFOLIOID, h.accountNumber, h.basePack, h.outputted, h.product_List_before, h.offer_List_Before
                                                    ,h.url_kenansnapshot_before,  h.url_billview_current_before, h.url_billview_future_before, h.url_portfolioview_before)
    SELECT t.PORTFOLIOID, t.accountNumber, t.basePack, t.outputted, t.product_List_before, t.offer_List_Before, 
           t.url_kenansnapshot_before,  t.url_billview_current_before, t.url_billview_future_before, t.url_portfolioview_before FROM dataprov.fsp_pos_basepack t 
      WHERE t.accountNumber = v_fsp_pos_basepack.accountNumber ;
   COMMIT ;
   -- Populate return cursor
   OPEN v_rec_out FOR
      SELECT *
        FROM dataprov.fsp_pos_basepack t
       WHERE t.accountNumber = v_fsp_pos_basepack.accountNumber
      ;
   EXCEPTION
      WHEN no_data_found THEN
         raise_application_error ( -20003 , 'Table dataprov.fsp_pos_basepack has no remaining data (with outputted IS NULL)' ) ;
END get_pos_basepack_data ;


-- 2) when selected from URL for screenshot
PROCEDURE write_screenshot_status ( v_rec_out OUT sys_refcursor )
IS
   v_fsp_pos_basepack dataprov.fsp_pos_basepack%ROWTYPE ;
BEGIN
    -- Select and lock first fsp_pos_basepack for a given test.
   SELECT t.*
     INTO v_fsp_pos_basepack
     FROM dataprov.fsp_pos_basepack t
    WHERE t.screenshotted IS NULL
      AND ROWNUM = 1
      FOR UPDATE
   ;
   -- Set flag from Available to Used.
   UPDATE dataprov.fsp_pos_basepack t
      SET t.screenshotted = SYSDATE
    WHERE t.accountNumber = v_fsp_pos_basepack.accountNumber
   ;
   COMMIT ;
   -- Populate return cursor
   OPEN v_rec_out FOR
      SELECT *
        FROM dataprov.fsp_pos_basepack t
       WHERE t.accountNumber = v_fsp_pos_basepack.accountNumber
      ;
   EXCEPTION
      WHEN no_data_found THEN
         raise_application_error ( -20003 , 'Table dataprov.fsp_pos_basepack has no remaining data (with screenshotted IS NULL)' ) ;
END write_screenshot_status ;


 -- 3) Write partial basket validation

   PROCEDURE write_basepack_status (i_accountNumber        IN VARCHAR2,
                                    i_test_status        IN VARCHAR2,
                                    i_failure_reason     IN VARCHAR2,
                                    i_sales_types          IN VARCHAR2,
                                    i_sales_types_status   IN VARCHAR2,
                                    i_next_bill_before     IN VARCHAR2,
                                    i_next_bill_after      IN VARCHAR2)
   IS
   BEGIN
      UPDATE dataprov.fsp_pos_basepack_history h
         SET h.test_status = i_test_status,
             h.failure_reason = i_failure_reason,
             h.sales_types = i_sales_types,
             h.sales_types_status = i_sales_types_status,
             h.next_bill_before = i_next_bill_before,
             h.next_bill_after = i_next_bill_after
       WHERE h.accountNumber = i_accountNumber;
   END write_basepack_status;


    -- 4) Update one row of history afterwards
   PROCEDURE hist_update_one (
      i_accountNumber IN dataprov.fsp_pos_basepack_history.accountNumber%TYPE)
   AS
   BEGIN
      FOR rProduct
         IN (  SELECT bpp_ap.portfolioId,
                      REGEXP_REPLACE (
                         LISTAGG (
                               bpp_ap.catalogueProductId
                            || ','
                            || TO_CHAR (se_ap.effectiveFromDate, 'YYYY-MM-DD')
                            || ','
                            || TO_CHAR (se_ap.effectiveToDate, 'YYYY-MM-DD')
                            || ' '
                            || r_bcp.productname
                            || ';'
                            || CHR (13))
                         WITHIN GROUP (ORDER BY 1),
                         '([^,]+)(,\1)*(,|$)',
                         '\1\3')
                         AS product_list_after
                 -- dates from bsbsubscriptionentitlement
                 FROM ccsowner.bsbPortfolioProduct bpp_ap
                      JOIN refdatamgr.bsbcatalogueproduct r_bcp
                         ON bpp_ap.catalogueProductId = r_bcp.id
                      JOIN ccsowner.bsbsubscriptionentitlement se_ap
                         ON bpp_ap.subscriptionid = se_ap.subscriptionid
                      JOIN dataprov.fsp_pos_basepack_history pos
                         ON pos.portfolioId = bpp_ap.portfolioId
                WHERE     bpp_ap.status NOT IN ('CN',
                                                'IN',
                                                'RP',
                                                'RM',
                                                'REN',
                                                'CP') -- maybe 'A' too - all phone products?
                      AND se_ap.effectiveToDate IS NULL
                      AND LENGTH (bpp_ap.catalogueProductId) = 5
                      AND pos.accountNumber = i_accountNumber
             GROUP BY bpp_ap.portfolioId)
      LOOP
         UPDATE dataprov.fsp_pos_basepack_history pos
            SET pos.product_list_after = rProduct.product_list_after
          WHERE pos.portfolioId = rProduct.portfolioId;
      -- DBMS_OUTPUT.PUT_LINE ( 'at ' || rProduct.portfolioId || ' setting to ' || rProduct.product_list_after ) ;
      END LOOP;

      FOR rOffer
         IN (  SELECT bpo_ao.portfolioId,
                      REGEXP_REPLACE (
                         LISTAGG (
                               bpo_ao.offerId
                            || ' '
                            || TO_CHAR (bpo_ao.applicationStartDate,
                                        'YYYY-MM-DD') -- also ro.offerValidFromDate
                            || ' '
                            || TO_CHAR (bpo_ao.applicationEndDate,
                                        'YYYY-MM-DD') -- also ro.offerValidtoDate
                            || ' '
                            || ro.description,
                            ';' || CHR (13))
                         WITHIN GROUP (ORDER BY 1),
                         '([^,]+)(,\1)*(,|$)',
                         '\1\3')
                         AS offer_list_after
                 FROM ccsowner.bsbPortfolioOffer bpo_ao
                      JOIN refdatamgr.bsbOffer ro ON ro.id = bpo_ao.offerId
                      JOIN dataprov.fsp_pos_basepack_history pos
                         ON pos.portfolioId = bpo_ao.portfolioId
                WHERE     ro.offer_type = 'RC' -- Recurring Charge offers only
                      AND bpo_ao.status = 'ACT'                      -- Active
                      AND (   bpo_ao.applicationStartDate =
                                 bpo_ao.applicationEndDate -- will never expire
                           OR bpo_ao.applicationEndDate > SYSDATE -- or has not expired yet
                                                                 )
                      AND pos.accountNumber = i_accountNumber
             GROUP BY bpo_ao.portfolioId)
      LOOP
         UPDATE dataprov.fsp_pos_basepack_history pos
            SET pos.offer_list_after = rOffer.offer_list_after
          -- , pos.checked_date = SYSDATE
          WHERE pos.portfolioId = rOffer.portfolioId;
      END LOOP;
   END hist_update_one;
   
   -- 5) Get sale type (crossgrade) for each account and product
   PROCEDURE get_sale_type (i_account_number   IN     VARCHAR2,
                            i_product_name     IN     VARCHAR2,
                            o_sale_type           OUT VARCHAR2)
   AS
   BEGIN
      SELECT ol.saletype
        INTO o_sale_type
        FROM ccsowner.bsborderline ol,
             ccsowner.bsbportfolioproduct pp,
             ccsowner.bsbserviceinstance si,
             ccsowner.bsbbillingaccount ba,
             refdatamgr.bsbcatalogueproduct cp
       WHERE     pp.serviceinstanceid = si.id
             AND pp.id = ol.portfolioproductid
             AND cp.ID = pp.catalogueproductid
             AND pp.portfolioid = ba.portfolioid
             AND ba.accountnumber = i_account_number
             AND cp.productname = i_product_name
             AND ol.effectivedate >= TRUNC(SYSDATE-INTERVAL '5' MINUTE);
   END get_sale_type;

-- 6) when selected from URL for screenhot update after migration
PROCEDURE get_data_for_post_mig_shot ( v_rec_out OUT sys_refcursor )
IS
   v_fsp_pos_basepack_history dataprov.fsp_pos_basepack_history%ROWTYPE ;
BEGIN
    -- Select and lock first fsp_pos_basepack_history for a given test.
   SELECT t.*
     INTO v_fsp_pos_basepack_history
     FROM dataprov.fsp_pos_basepack_history t
      WHERE t.outputted > TRUNC(SYSDATE-INTERVAL '16' HOUR)
      and t.screenshotted_after IS NULL
      AND ROWNUM = 1
      FOR UPDATE
   ;
   -- Set flag from Available to Used.
   UPDATE dataprov.fsp_pos_basepack_history t
      SET t.screenshotted_after = SYSDATE
    WHERE t.accountNumber = v_fsp_pos_basepack_history.accountNumber
   ;
   COMMIT ;
   -- Populate return cursor
   OPEN v_rec_out FOR
      SELECT *
        FROM dataprov.fsp_pos_basepack_history t
       WHERE t.accountNumber = v_fsp_pos_basepack_history.accountNumber
      ;
   EXCEPTION
      WHEN no_data_found THEN
         raise_application_error ( -20003 , 'Table dataprov.fsp_pos_basepack_history has no remaining data which has not been screenshotted' ) ;
END get_data_for_post_mig_shot ;


-- 7) updates a URL column name
PROCEDURE insert_url ( i_account_number IN VARCHAR2, i_url_kenansnapshot IN VARCHAR2,  i_url_billview_current IN VARCHAR2, 
                            i_url_billview_future IN VARCHAR2, i_url_portfolioview IN VARCHAR2, i_timing IN VARCHAR2 ) 
IS
BEGIN
   IF LOWER ( TRIM ( i_timing ) ) = 'before'
    THEN 
     UPDATE dataprov.fsp_pos_basepack b
       SET b.url_kenansnapshot_before = i_url_kenansnapshot,
           b.url_billview_current_before = i_url_billview_current,
           b.url_billview_future_before = i_url_billview_future,
           b.url_portfolioview_before = i_url_portfolioview
       WHERE b.accountNumber = i_account_number;
    ELSIF LOWER ( TRIM ( i_timing ) ) = 'after'
     THEN
      UPDATE dataprov.fsp_pos_basepack_history h
       SET h.url_kenansnapshot_after = i_url_kenansnapshot,
           h.url_billview_current_after = i_url_billview_current,
           h.url_billview_future_after = i_url_billview_future,
           h.url_portfolioview_after = i_url_portfolioview
       WHERE h.accountNumber = i_account_number;
   END IF ;
   
   COMMIT ;
   
END insert_url ;

-- 8) when selected from URL for skyId screenshot
PROCEDURE write_screenshot_status_skyid ( v_rec_out OUT sys_refcursor )
IS
   v_fsp_pos_basepack dataprov.fsp_pos_basepack%ROWTYPE ;
BEGIN
    -- Select and lock first fsp_pos_basepack for a given test.
   SELECT t.*
     INTO v_fsp_pos_basepack
     FROM dataprov.fsp_pos_basepack t
    WHERE t.screenshotted IS NOT NULL
    AND t.url_portfolioview_before IS NOT NULL
    AND t.screenshotted_skyid IS NULL
      AND ROWNUM = 1
      FOR UPDATE
   ;
   -- Set flag from Available to Used.
   UPDATE dataprov.fsp_pos_basepack t
      SET t.screenshotted_skyid = SYSDATE
    WHERE t.accountNumber = v_fsp_pos_basepack.accountNumber
   ;
   COMMIT ;
   -- Populate return cursor
   OPEN v_rec_out FOR
      SELECT *
        FROM dataprov.fsp_pos_basepack t
       WHERE t.accountNumber = v_fsp_pos_basepack.accountNumber
      ;
   EXCEPTION
      WHEN no_data_found THEN
         raise_application_error ( -20003 , 'Table dataprov.fsp_pos_basepack has no remaining data (with screenshotted IS NULL)' ) ;
END write_screenshot_status_skyid ;



END fsp_pos_basepack_pkg ;
/
