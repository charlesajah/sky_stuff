create or replace PACKAGE data_prep_01 AS

PROCEDURE BOSMULTILEGALOUTLET;
PROCEDURE BOSMULTIOUTLET;
PROCEDURE BOSSINGLEOUTLET;
PROCEDURE BOSBILLEDACCOUNTS;
PROCEDURE BOSEXTERNALACCOUNTIDS ;

END data_prep_01 ;
/

create or replace PACKAGE BODY data_prep_01 AS

PROCEDURE BOSMULTILEGALOUTLET is

   l_pool VARCHAR2(29) := 'BOSMULTILEGALOUTLET' ;
BEGIN
   logger.write ( 'begin') ;

   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   INSERT INTO dprov_accounts_static t ( t.pk_value, t.seqno , t.test_alloc , t.head_office, t.derivedlegal, t.billing_account , t.outlet_acc_name  )
   SELECT sys_guid(), ROWNUM AS pool_seqno , l_pool AS pool_name ,
   sub.head_office_account_name,
    sub.derivedlegal,
    sub.billing_account_name,
    sub.outlet_account_name
   FROM
       (
        SELECT distinct 
            head_office.account_no   AS head_office_account,
            head_office.bill_company AS head_office_account_name,
            'N/A'                    AS legal_office_account,
            ( substr(
                billing.bill_company, 1,(instr(
                    billing.bill_company, ' ', 1
                ))
            )
              || 'legal' )             AS derivedlegal,
            billing.account_no       AS billing_account,
            billing.bill_company     billing_account_name,
            outlet.account_no        AS outlet_account,
            outlet.bill_company      outlet_account_name,
            outlet.date_created,
            head_office.date_created,
            billing.billing_account_created,
            greatest(
                outlet.date_created, head_office.date_created, billing.billing_account_created
            )                        AS min_created_date
        FROM
            (
                SELECT
                    data.account_no  AS account_no,
                    map.external_id  AS external_id,
                    cmf.parent_id    AS parent_id,
                    cmf.hierarchy_id AS hierarchy_id,
                    cmf.bill_company,
                    length(
                        cmf.bill_company
                    )                AS billing_account_length,
                    date_created     AS billing_account_created
                FROM
                    cmf_ext_data@SB_CMKT_NEW data --billing account is in this table
                    INNER JOIN external_id_acct_map@SB_CMKT_NEW map ON map.account_no = data.account_no
                    INNER JOIN cmf@SB_CMKT_NEW                  cmf ON cmf.account_no = map.account_no
                WHERE
                    lower(
                        substr(
                            cmf.bill_company, - 6, 6
                        )
                    ) = ' other'
            ) billing
            INNER JOIN cmf@SB_CMKT_NEW head_office ON billing.parent_id = head_office.account_no
            LEFT OUTER JOIN cmf@SB_CMKT_NEW outlet ON billing.account_no = outlet.parent_id
       ) sub
   WHERE
    sub.min_created_date > TO_DATE('03/01/2024', 'dd/mm/yyyy');

   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => FALSE , i_flagCustomers => TRUE ) ;
   logger.write ( 'complete' ) ;

END BOSMULTILEGALOUTLET;



PROCEDURE BOSMULTIOUTLET IS
   l_pool VARCHAR2(29) := 'BOSMULTIOUTLET' ;
BEGIN
   logger.write ( 'begin' ) ;

   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;
   execute immediate 'truncate table ' || l_pool ;

   insert into BOSMULTIOUTLET (head_office, legal_office, billing_account, outlet, min_created_date)
   SELECT sub.head_office_account_name AS head_office,
    sub.derivedlegal             AS legal_office,
    sub.billing_account_name     AS billing_account,
    sub.outlet_account_name      AS outlet,
    sub.min_created_date
   FROM
    (
        SELECT distinct 
            head_office.account_no   AS head_office_account,
            head_office.bill_company AS head_office_account_name,
            'N/A'                    AS legal_office_account,
            substr(
                billing.bill_company, 0, billing_account_length - 8
            )
            || ' Legal'              AS derivedlegal,
            billing.account_no       AS billing_account,
            billing.bill_company     billing_account_name,
            outlet.account_no        AS outlet_account,
            outlet.bill_company      outlet_account_name,
            outlet.date_created,
            head_office.date_created,
            billing.billing_account_created,
            greatest(
                outlet.date_created, head_office.date_created, billing.billing_account_created
            )                        AS min_created_date
        FROM
            (
                SELECT
                    data.account_no  AS account_no,
                    map.external_id  AS external_id,
                    cmf.parent_id    AS parent_id,
                    cmf.hierarchy_id AS hierarchy_id,
                    cmf.bill_company,
                    length(
                        cmf.bill_company
                    )                AS billing_account_length,
                    date_created     AS billing_account_created
                FROM
                    cmf_ext_data@SB_CMKT_NEW data --billing account is in this table
                    INNER JOIN external_id_acct_map@SB_CMKT_NEW map ON map.account_no = data.account_no
                    INNER JOIN cmf@SB_CMKT_NEW                  cmf ON cmf.account_no = map.account_no
                WHERE
                    lower(
                        substr(
                            cmf.bill_company, - 8, 8
                        )
                    ) = ' billing'
            ) billing
            INNER JOIN cmf@SB_CMKT_NEW head_office ON billing.parent_id = head_office.account_no
            INNER JOIN cmf@SB_CMKT_NEW outlet ON billing.account_no = outlet.parent_id
       ) sub
   WHERE
    sub.min_created_date > TO_DATE('04/01/2024', 'dd/mm/yyyy');

   update BOSMULTIOUTLET h
   set h.outlet_count = (select count(distinct i.outlet)
                         from    BOSMULTIOUTLET i
                         where i.billing_account =h.billing_account);


   COMMIT;

   INSERT INTO dprov_accounts_static t ( t.pk_value, t.seqno , t.test_alloc , t.head_office, t.legal_office, t.billing_account , t.outlet_count )
   SELECT  sys_guid(), ROWNUM AS pool_seqno , l_pool AS pool_name ,
    sub.head_office AS head_office,
    sub.legal_office             AS legal_office,
    sub.billing_account     AS billing_account,
    sub.outlet_count      AS outlet_count
   FROM
    (select distinct head_office,legal_office, billing_account, outlet_count
     from BOSMULTIOUTLET h
     where exists (select count(distinct i.outlet)
                   from    BOSMULTIOUTLET i
                   where i.billing_account =h.billing_account
                   having count(distinct i.outlet)>1) ) sub;

   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE , i_flagCustomers => TRUE ) ;

   logger.write ( 'complete' ) ;
END BOSMULTIOUTLET ;


PROCEDURE BOSSINGLEOUTLET IS
   l_pool VARCHAR2(29) := 'BOSSINGLEOUTLET' ;
BEGIN
   logger.write ( 'begin' ) ;

   sequence_pkg.seqBefore ( i_pool => l_pool , i_flagCustomers => TRUE ) ;


   INSERT INTO dprov_accounts_static t ( t.pk_value, t.seqno , t.test_alloc , t.head_office, t.legal_office, t.billing_account , t.outlet_acc_name , t.min_created_date )
   SELECT sys_guid(), ROWNUM AS pool_seqno , l_pool AS pool_name ,
    sub.head_office AS head_office,
    sub.legal_office             AS legal_office,
    sub.billing_account     AS billing_account,
    sub.outlet      AS outlet,
    sub.min_created_date
   FROM
    (select head_office, legal_office, billing_account, outlet, min_created_date
     from BOSMULTIOUTLET
     where outlet_count=1) sub;

   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => TRUE , i_flagCustomers => TRUE ) ;

   logger.write ( 'complete' ) ;
END BOSSINGLEOUTLET ;


PROCEDURE BOSBILLEDACCOUNTS IS
   l_pool VARCHAR2(29) := 'BOSBILLEDACCOUNTS' ;
BEGIN
   logger.write ( 'begin' ) ;

   sequence_pkg.seqBefore ( i_pool => l_pool ) ;


   INSERT INTO dprov_accounts_static ( pk_value, seqno, test_alloc, EXTERNAL_ID, INVOICENO, INVOICEPREPARATIONDATE )
   SELECT sys_guid() as pk_value, ROWNUM AS pool_seqno, l_pool AS pool_name,
          EXTERNAL_ID, INVOICENO, INVOICEPREPARATIONDATE
     from (select EIAM.EXTERNAL_ID AS EXTERNAL_ID, 
                  BI.BILL_REF_NO AS INVOICENO, 
                  BI.PREP_DATE INVOICEPREPARATIONDATE
             FROM EXTERNAL_ID_ACCT_MAP@SB_CMKT_NEW EIAM, CMF@SB_CMKT_NEW C, BILL_INVOICE@SB_CMKT_NEW BI, CMF_BALANCE@SB_CMKT_NEW CB,
                  PAYMENT_PROFILE@SB_CMKT_NEW PP, cmf_ext_data@SB_CMKT_NEW ced
            WHERE eiam.external_id_type = 2
              AND eiam.account_no = c.account_no
              AND c.account_no = bi.account_no
              AND bi.prep_date > add_months(SYSDATE, -3)
              AND bi.bill_ref_no = cb.bill_ref_no
              AND bi.bill_ref_resets = cb.bill_ref_resets
              AND c.payment_profile_id = pp.profile_id
              AND cb.net_new_charges > 0
              AND c.account_no = ced.account_no(+)
              and rownum < 5001
              order by dbms_random.value);

   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => FALSE ) ;

   logger.write ( 'complete' ) ;
END BOSBILLEDACCOUNTS ;

PROCEDURE BOSEXTERNALACCOUNTIDS IS
   l_pool VARCHAR2(29) := 'BOSEXTERNALACCOUNTIDS' ;
BEGIN
   logger.write ( 'begin' ) ;

   sequence_pkg.seqBefore ( i_pool => l_pool ) ;

   INSERT INTO dprov_accounts_static ( pk_value, seqno, test_alloc, EXTERNAL_ID, ACCOUNTNUMBER )
   SELECT sys_guid() as pk_value, ROWNUM AS pool_seqno, l_pool AS pool_name,
          EXTERNAL_ID, account_no
     from (SELECT external_id, account_no 
             FROM EXTERNAL_ID_ACCT_MAP@SB_CMKT_NEW 
            WHERE external_id LIKE '30%'
              AND LENGTH(external_id) = 12
              and external_id_type = 3
           order by dbms_random.value)
    where rownum < 5001;

   sequence_pkg.seqAfter ( i_pool => l_pool , i_count => SQL%ROWCOUNT , i_burn => FALSE ) ;

   logger.write ( 'complete' ) ;
END BOSEXTERNALACCOUNTIDS ;

END data_prep_01 ;
/
