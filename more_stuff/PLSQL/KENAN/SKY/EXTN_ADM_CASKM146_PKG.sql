create or replace PACKAGE       EXTN_ADM_CASKM146_PKG AS

   PROCEDURE truncate_staging_tables;
   PROCEDURE populate_guiding_suspense;
   PROCEDURE populate_guiding_daily_totals;
   PROCEDURE update_guiding_daily_totals;

END EXTN_ADM_CASKM146_PKG;

create or replace PACKAGE BODY       EXTN_ADM_CASKM146_PKG AS

  v_open_business_date DATE := report_parameters_pkg.get_open_business_date('');

  PROCEDURE truncate_staging_tables IS
  BEGIN

    EXECUTE IMMEDIATE 'TRUNCATE TABLE sky.EXTN_MOBILE_GUIDING_SUSPENSE';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE sky.EXTN_MOBILE_GUIDING_DAILY';

  EXCEPTION
    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20808,
                              'Error in EXTN_ADM_CASKM146_PKG when truncating staging tables - ' ||
                              sqlerrm);

  END truncate_staging_tables;

  PROCEDURE populate_guiding_suspense IS
  BEGIN

    INSERT INTO SKY.EXTN_MOBILE_GUIDING_SUSPENSE
      SELECT cdw.file_id FS_FILE_ID,
             ciam.external_id EXTERNAL_ID,
             cdw.account_no ACCOUNT_NO,
             TRIM(cdw.customer_tag) MSISDN,
             cdw.point_origin POINT_ORIGIN,
             cdw.point_target POINT_TARGET,
             cdw.element_id ELEMENT_ID,
             ut.type_id_usg TYPE_ID_USG,
             d.description_text USAGE_TYPE_DESC,
             cdw.trans_dt TRANS_DT,
             cdw.primary_units PRIMARY_UNITS,
             cdw.rated_units RATED_UNITS,
             cdw.amount AMOUNT,
             cdw.rate_dt RATE_DT,
             CASE
               WHEN upper(emum.CALL_CATEGORY) = 'VOICE' or
                    upper(emum.CALL_CATEGORY) = 'SMS' or
                    upper(emum.CALL_CATEGORY) = 'MMS' THEN
                substr(ANNOTATION, 1, 50)
               ELSE
                ''
             END DESTINATION,
             CASE
               WHEN upper(emum.CALL_CATEGORY) = 'VOICE' or
                    upper(emum.CALL_CATEGORY) = 'SMS' or
                    upper(emum.CALL_CATEGORY) = 'MMS' THEN
                substr(ANNOTATION, 52, 50)
               WHEN upper(emum.CALL_CATEGORY) = 'DATA' THEN
                substr(ANNOTATION, 1, 50)
               ELSE
                ''
             END ROAMING_COUNTRY,
             CASE
               WHEN upper(emum.CALL_CATEGORY) = 'VOICE' or
                    upper(emum.CALL_CATEGORY) = 'SMS' or
                    upper(emum.CALL_CATEGORY) = 'MMS' THEN
                substr(ANNOTATION, 103, 20)
               WHEN upper(emum.CALL_CATEGORY) = 'DATA' THEN
                substr(ANNOTATION, 51, 20)
               ELSE
                ''
             END OCS_CHARGECODE,
             emum.CALL_DIRECTION,
             cdw.MIU_ERROR_CODE1,
             cdw.PROVIDER_ID
        FROM cdr_data_work             cdw,
             external_id_acct_map      ciam,
             descriptions              d,
             usage_types               ut,
             extn_mobile_usage_mapping emum,
             arbor.file_status         fs
       WHERE emum.usage_type(+) = cdw.type_id_usg
         AND ciam.account_no(+) = cdw.account_no
         AND ciam.external_id_type(+) = 1
         AND cdw.type_id_usg = ut.type_id_usg
         AND ut.description_code = d.description_code
         AND d.language_code = 1
         AND fs.file_id = cdw.file_id
         AND fs.ext_contact_id = 13 -- mobile usage Contact
         AND EXISTS ( SELECT 1
                 FROM extn_mobile_guiding_daily@adm
                WHERE fs_file_id = fs.file_id);

  EXCEPTION
    WHEN no_data_found THEN
      NULL; -- trap the no_data_found that can be raised if the EXTN_MOBILE_GUIDING_SUSPENSE is empty

    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20810,
                              'Error WHEN executing the populate_guiding_suspense proc - trying to insert into EXTN_MOBILE_GUIDING_SUSPENSE - ' ||
                              sqlerrm);
  END;

  PROCEDURE populate_guiding_daily_totals IS
  BEGIN

    INSERT INTO SKY.EXTN_MOBILE_GUIDING_DAILY
      (FS_FILE_ID,
       FS_FILE_PROCESS_DT,
       USAGE,
       TOTAL_CDR_COUNT,
       GUIDED_CDR_COUNT,
       SUSPENSE_CDR_COUNT,
       DUPLICATE_CDR_COUNT,
       PROVIDER_NAME)

       SELECT fs.file_id FS_FILE_ID,
          TRUNC(fs.file_process_dt) FS_FILE_PROCESS_DT,
          DECODE(SUBSTR(file_name, 19, 3),
                 'F01', 'VOICE',
                 'F02', 'DATA',
                 'F03', 'SMS',
                 'F04', 'MMS',
                 'F05', 'MON',
                 'Unknown') USAGE,
          fs.total_records      TOTAL_CDR_COUNT,
          fs.num_good           GUIDED_CDR_COUNT,
          ufc.num_discarded     DUPLICATE_CDR_COUNT,
          ufc.num_cdr_data_work SUSPENSE_CDR_COUNT, ---- note this may not be accurate. use cdr_data_work count by file_id
          DECODE(SUBSTR(file_name, 29, 5),
                 'OCS01', 'OCS01',
                 'OCS02', 'OCS02',
                 'OCS03', 'OCS03',
                 'OCS04', 'OCS04',
                 'OCS05', 'OCS05',
                 'OCS06', 'OCS06',
                 'Unknown') PROVIDER_NAME
     FROM file_status fs, usage_file_counts ufc --usage_file_counts table included to get duplicate suspense CDRs totals
    WHERE fs.file_id = ufc.file_id
      AND fs.file_id_serv = ufc.server_id
      AND fs.ext_contact_id = 13 -- mobile usage Contact
      AND fs.file_name like '%cdr.data.F0%'
      AND fs.file_process_dt >= v_open_business_date;

   EXCEPTION
    WHEN no_data_found THEN
      NULL; -- trap the no_data_found that can be raised if the EXTN_MOBILE_GUIDING_DAILY is empty

    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20810,
                              'Error WHEN executing the populate_guiding_daily_totals proc - trying to insert into EXTN_MOBILE_GUIDING_DAILY - ' ||
                              sqlerrm);

  END;

  PROCEDURE update_guiding_daily_totals IS
  BEGIN

    MERGE INTO extn_mobile_guiding_daily d
    USING ( SELECT emgs.fs_file_id, count(*) cdr_suspended_total
              FROM SKY.extn_mobile_guiding_suspense emgs
             GROUP by emgs.fs_file_id) mobs
    ON (d.fs_file_id = mobs.fs_file_id)
    WHEN matched THEN
      UPDATE SET d.suspense_cdr_count = mobs.cdr_suspended_total;


    EXCEPTION
    WHEN no_data_found THEN
      NULL; -- trap the no_data_found that can be raised if the extn_mobile_guiding_daily is empty

    WHEN OTHERS THEN
      RAISE_APPLICATION_ERROR(-20810,
                              'Error WHEN executing the update_guiding_daily_totals proc - trying to merge into extn_mobile_guiding_daily - ' ||
                              sqlerrm);

  END;

END EXTN_ADM_CASKM146_PKG;



