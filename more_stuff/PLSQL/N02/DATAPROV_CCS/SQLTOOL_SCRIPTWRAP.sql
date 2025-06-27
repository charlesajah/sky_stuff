--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure SQLTOOL_SCRIPTWRAP
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."SQLTOOL_SCRIPTWRAP" ( SCRIPT_ID VARCHAR2,
	   	  		  								TARGET_SCRIPT VARCHAR2,
												RESULTS_TABLE VARCHAR2,
												LOG_TABLE VARCHAR2,
			    					debug_flag BOOLEAN)
   IS
   total_recs NUMBER;
   start_time DATE;
   end_time DATE;
   BEGIN
   	  EXECUTE IMMEDIATE 'alter session set nls_date_format = ''DD/MM/YYYY HH24:MI:SS''';
      start_time := SYSDATE;
	  IF debug_flag THEN
	  	 DBMS_OUTPUT.PUT_LINE( 'SCRIPT ID = '||SCRIPT_ID||'. Results table = '||results_table||'. Log Table = '||log_table);
		 DBMS_OUTPUT.PUT_LINE( 'Script is - '||SUBSTR(target_script, 1, 200));
	  END IF;
      EXECUTE IMMEDIATE 'INSERT INTO '||results_table||' '||target_script;
      end_time := SYSDATE;
	  EXECUTE IMMEDIATE 'INSERT INTO '||log_table||' VALUES ('''||script_id||''', 0,''OK'', SYSDATE,'''||start_time||''','''||end_time||''')';
      --DBMS_OUTPUT.PUT_LINE( 'Script '||script_id||':COMPLETED. Records = '||TO_CHAR(total_recs)||'. Time taken = '||TO_CHAR( end_time - start_time));
      COMMIT;
   EXCEPTION
      WHEN OTHERS THEN
	  --DBMS_OUTPUT.PUT_LINE(TO_CHAR(start_time, 'DD/MM/YYYY HH24:MI:SS'));
	 EXECUTE IMMEDIATE 'INSERT INTO '||log_table||' VALUES ('''||script_id||''', '||TO_CHAR(SQLCODE)||', '''||SQLERRM||''', SYSDATE, '''||start_time||''', SYSDATE)';
   --	   DBMS_OUTPUT.PUT_LINE('scriptwrapper:ERROR. Script '||script_id||' Code = '||TO_CHAR(SQLCODE)||' - '||SQLERRM||'. Failed at '||TO_CHAR(SYSDATE, 'HH24:MI:SS'));
	 COMMIT;
   END Sqltool_Scriptwrap;

/

  GRANT EXECUTE ON "DATAPROV"."SQLTOOL_SCRIPTWRAP" TO "BATCHPROCESS_USER";
