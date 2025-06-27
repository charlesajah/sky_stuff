--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure DP_MOBILE_CDR_DATA
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."DP_MOBILE_CDR_DATA" (
/*
|| This procedure is called by shell script
||    kfxbatchn01:/apps/N01/home/bilbtn01/nft_data/generate_cdr_data.sh
|| which gets run occasionally by John Barclay to generate BIP Billing input flat files for Kenan batch processing.
|| The table mobile_cdr_files is populated by data_prep_0x.mobile_cdr_files which is run nightly via run_job_parallel_control table.
*/
     p_volume IN NUMBER
   , v_rec_out OUT SYS_REFCURSOR
) AS
BEGIN
   OPEN v_rec_out FOR
      SELECT mcf.misdn , mcf.serviceInstance , mcf.accountNumber , mobileCdrSeq.NEXTVAL AS mobseq
        FROM mobile_cdr_files mcf
       WHERE ROWNUM <= p_volume
      ;
   logger.write ( 'p_volume => ' || TO_CHAR ( p_volume ) ) ;
END dp_mobile_cdr_data ;

/

  GRANT EXECUTE ON "DATAPROV"."DP_MOBILE_CDR_DATA" TO "DATAPROV_READONLY";
  GRANT EXECUTE ON "DATAPROV"."DP_MOBILE_CDR_DATA" TO "DATAPROV_READONLY_ROLE";
  GRANT EXECUTE ON "DATAPROV"."DP_MOBILE_CDR_DATA" TO "BATCHPROCESS_USER";
