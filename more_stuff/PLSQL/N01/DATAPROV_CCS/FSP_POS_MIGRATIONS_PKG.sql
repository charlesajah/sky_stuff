CREATE OR REPLACE PACKAGE fsp_pos_migrations_pkg
AS
   /*
   || 15-May-2017 v0.1 Andrew Fraser / Bruce Thomson
   */
   PROCEDURE get_pos_migration_data (v_rec_out OUT SYS_REFCURSOR);

   PROCEDURE write_shadowPortfolio (i_accountNumber        IN VARCHAR2,
                                    i_shadow_status        IN VARCHAR2,
                                    i_shadow_portfolio     IN VARCHAR2,
                                    i_unbundled_status     IN VARCHAR2,
                                    i_sales_types          IN VARCHAR2,
                                    i_sales_types_status   IN VARCHAR2,
                                    i_next_bill_status     IN VARCHAR2,
                                    i_next_bill_before     IN VARCHAR2,
                                    i_next_bill_after      IN VARCHAR2);

   PROCEDURE hist_update_one (
      i_accountNumber   IN dataprov.fsp_pos_migrations_history.accountNumber%TYPE);

   PROCEDURE hist_update_many (
      i_dateFrom   IN dataprov.fsp_pos_migrations_history.outputted%TYPE DEFAULT   SYSDATE
                                                                                 - 1);

   PROCEDURE get_sale_type (i_account_number   IN     VARCHAR2,
                            i_product_name     IN     VARCHAR2,
                            o_sale_type           OUT VARCHAR2);
END fsp_pos_migrations_pkg;
/


CREATE OR REPLACE PACKAGE BODY fsp_pos_migrations_pkg
AS
   /*
   || 15-May-2017 v0.1 Andrew Fraser / Bruce Thomson
   */
   -- 1) when selected from URL
   PROCEDURE get_pos_migration_data (v_rec_out OUT SYS_REFCURSOR)
   IS
      v_fsp_pos_migrations   dataprov.fsp_pos_migrations%ROWTYPE;
   BEGIN
      -- Select and lock first fsp_pos_migrations for a given test.
      SELECT t.*
        INTO v_fsp_pos_migrations
        FROM dataprov.fsp_pos_migrations t
       WHERE t.outputted IS NULL AND ROWNUM = 1
      FOR UPDATE;

      -- Set flag from Available to Used.
      UPDATE dataprov.fsp_pos_migrations t
         SET t.outputted = SYSDATE
       WHERE t.accountNumber = v_fsp_pos_migrations.accountNumber;

      COMMIT;

      -- record into history
      INSERT INTO dataprov.fsp_pos_migrations_history h (
                     h.PORTFOLIOID,
                     h.accountNumber,
                     h.bundleName,
                     h.outputted,
                     h.product_List_before,
                     h.offer_List_Before)
         SELECT t.PORTFOLIOID,
                t.accountNumber,
                t.bundleName,
                t.outputted,
                t.product_List_before,
                t.offer_List_Before
           FROM dataprov.fsp_pos_migrations t
          WHERE t.accountNumber = v_fsp_pos_migrations.accountNumber;

      COMMIT;

      -- Populate return cursor
      OPEN v_rec_out FOR
         SELECT *
           FROM dataprov.fsp_pos_migrations t
          WHERE t.accountNumber = v_fsp_pos_migrations.accountNumber;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         raise_application_error (
            -20003,
            'Table dataprov.fsp_pos_migrations has no remaining data (with outputted IS NULL)');
   END get_pos_migration_data;

   -- 2) Write partial basket validation shadow portfolio
   PROCEDURE write_shadowPortfolio (i_accountNumber        IN VARCHAR2,
                                    i_shadow_status        IN VARCHAR2,
                                    i_shadow_portfolio     IN VARCHAR2,
                                    i_unbundled_status     IN VARCHAR2,
                                    i_sales_types          IN VARCHAR2,
                                    i_sales_types_status   IN VARCHAR2,
                                    i_next_bill_status     IN VARCHAR2,
                                    i_next_bill_before     IN VARCHAR2,
                                    i_next_bill_after      IN VARCHAR2)
   IS
   BEGIN
      UPDATE dataprov.fsp_pos_migrations_history h
         SET h.test_status = i_shadow_status,
             h.shadow_portfolio = i_shadow_portfolio,
             h.unbundled_status = i_unbundled_status,
             h.sales_types = i_sales_types,
             h.sales_types_status = i_sales_types_status,
             h.next_bill_status = i_next_bill_status,
             h.next_bill_before = i_next_bill_before,
             h.next_bill_after = i_next_bill_after
       WHERE h.accountNumber = i_accountNumber;
   END write_shadowPortfolio;

   -- 3) Update one row of history afterwards
   PROCEDURE hist_update_one (
      i_accountNumber   IN dataprov.fsp_pos_migrations_history.accountNumber%TYPE)
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
                      JOIN dataprov.fsp_pos_migrations_history pos
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
         UPDATE dataprov.fsp_pos_migrations_history pos
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
                      JOIN dataprov.fsp_pos_migrations_history pos
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
         UPDATE dataprov.fsp_pos_migrations_history pos
            SET pos.offer_list_after = rOffer.offer_list_after
          -- , pos.checked_date = SYSDATE
          WHERE pos.portfolioId = rOffer.portfolioId;
      END LOOP;
   END hist_update_one;

   -- 4) Update all rows of history afterwards based on date
   PROCEDURE hist_update_many (
      i_dateFrom   IN dataprov.fsp_pos_migrations_history.outputted%TYPE DEFAULT   SYSDATE
                                                                                 - 1)
   AS
   BEGIN
      FOR rMany
         IN (SELECT pos.accountNumber
               FROM dataprov.fsp_pos_migrations_history pos
              WHERE     pos.outputted > i_dateFrom
                    -- AND pos.checked_date IS NULL
                    AND (   offer_list_after IS NULL
                         OR pos.product_list_after IS NULL))
      LOOP
         hist_update_one (i_accountNumber => rMany.accountNumber);
      END LOOP;
   END hist_update_many;


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
END fsp_pos_migrations_pkg;
/
