-- =============================================================
-- Name 			: CentralFixDataN71.sql
-- Author 			: Rakel Fernandez
-- Date 			: 24/07/2024
-- Purpose  		: It creates missing data into the Repository so that the graphs don't break on display
--                    
-- Change History 	
-- --------------
-- 24/07/24 	RFA	: Modifications made for the new Central Repository solution
--
-- =============================================================


column l_test_id new_value l_test_id
define test1_start_dtm = '&1'
define test1_end_dtm = '&2'
define p_test_description = '&3'
SELECT TO_CHAR ( TO_DATE ( '&&TEST1_START_DTM' , 'DDMONYY-HH24:MI' ) , 'DDMONYY-HH24MI' ) || '_' || TO_CHAR ( TO_DATE ( '&&TEST1_END_DTM' , 'DDMONYY-HH24:MI' ) , 'DDMONYY-HH24MI' ) AS l_test_id
  FROM v$database d
;
exec hp_diag.fix_missing_data_all ( p_testId => '&&l_test_id' , p_testDesc => '&&p_test_description' ) ;
prompt Done