CREATE OR REPLACE PACKAGE REPORT_ADM AS 
/* This is the REPOSITORY of Procedures & Functions common for 
   all the Confluenece Reports Generation 
*/ 
   TYPE g_tvc2 IS TABLE OF VARCHAR2(4000) ;
   
   PROCEDURE Write_Log_Report (
        i_testId1 IN hp_diag.test_report_log.rep_testid1%TYPE 
      , i_testId2 IN hp_diag.test_report_log.rep_testid2%TYPE 
      , i_desc IN hp_diag.test_report_log.report_name%TYPE
      ) ;

   FUNCTION Get_StartSnapId (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE  
        ) RETURN number;

   FUNCTION Get_EndSnapId (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE  
        ) RETURN number;      

   FUNCTION Get_AWR_Report (
        i_dbname  IN varchar2
      , i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED ;

   PROCEDURE Create_AWR_HTML_files (
        i_cwd     IN varchar2
      , i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) ;   

   FUNCTION Get_Start_DTM (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE  
        ) RETURN varchar2 ;

   FUNCTION Get_End_DTM (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE  
        ) RETURN varchar2 ;
      
END REPORT_ADM;
/


CREATE OR REPLACE PACKAGE BODY REPORT_ADM AS 
/* This is the REPOSITORY of Procedures & Functions common for 
   all the Confluenece Reports Generation 
*/ 

PROCEDURE Write_Log_Report (
        i_testId1 IN hp_diag.test_report_log.rep_testid1%TYPE 
      , i_testId2 IN hp_diag.test_report_log.rep_testid2%TYPE 
      , i_desc    IN hp_diag.test_report_log.report_name%TYPE
      ) 
AS
   l_testId1 hp_diag.test_report_log.rep_testid1%TYPE  ;
   l_testId2 hp_diag.test_report_log.rep_testid2%TYPE ;
   l_desc    hp_diag.test_report_log.report_name%TYPE ; 

BEGIN
   l_testId1 := i_testId1 ;
   l_testId2 := i_testId2 ;
   l_desc    := i_desc ;

   Insert into TEST_REPORT_LOG 
        ( REPORT_NAME
         ,REPORT_DATE
         ,REP_TESTID1
         ,REP_TESTID2
         ,STATUS )
    Values 
        ( l_desc
        , sysdate
        , l_testid1
        , l_testid2
        ,'Complete'
        );

    commit;

END Write_Log_Report;

FUNCTION Get_AWR_Report (
        i_dbname  IN varchar2
      , i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) RETURN g_tvc2 PIPELINED
AS
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;

   l_dbname           varchar2(30);
   l_output           clob;

   l_row           varchar2(4000);
   l_offset        number := 1;
   l_amount        number ;
   l_length        number ; 
   l_buffer        varchar2(32767);

BEGIN
   l_testId1 := i_testId1 ;
   l_dbname := i_dbname ;

   SELECT awr_report into l_output FROM hp_diag.test_result_awr WHERE test_id = l_testId1 AND database_name = l_dbname ;
   l_length := DBMS_LOB.getLength(l_output);
   while l_offset < l_length loop
      l_amount := LEAST ( DBMS_LOB.instr(l_output, chr(10), l_offset)-l_offset, 32767);
      if l_amount > 0 then
         DBMS_LOB.READ (l_output, l_amount, l_offset, l_buffer) ;
         l_offset := l_offset + l_amount + 1 ;
      else
         l_buffer := null;
         l_offset := l_offset + 1;
      end if;
      l_row := l_buffer ;
      PIPE ROW ( l_row ) ;
   end loop;

END Get_AWR_Report;


PROCEDURE Create_AWR_HTML_files (
        i_cwd     IN varchar2
      , i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE 
      ) 
AS
   l_testId1 hp_diag.test_result_metrics.test_id%TYPE ;

   l_cdw           varchar2(150);
   l_output        clob;
   l_filename      varchar2(200);
   l_sysdate       varchar2(20);

   l_row           varchar2(4000);
   l_offset        number := 1;
   l_amount        number ;
   l_length        number ; 
   l_buffer        varchar2(32767);

   f1              utl_file.file_type;

BEGIN
   l_testId1 := i_testId1 ;
   l_cdw := i_cwd ;
   select TO_CHAR ( SYSDATE , 'DDMMYY-HH24MI' ) into l_sysdate from dual; 
   execute immediate 'create or replace directory AWR_REPORT as ''' || l_cdw || '''';

   dbms_output.put_line ('Location : '|| l_cdw);
   dbms_output.put_line ('Date : '|| l_sysdate);


   For r1 in ( SELECT database_name, test_id FROM hp_diag.test_result_awr WHERE test_id = l_testId1 GROUP BY DATABASE_NAME, TEST_ID ) loop
       --l_filename := 'AWRRPT_' || R1.DATABASE_NAME || '_' || Get_Start_DTM(r1.test_id) || '_' || Get_End_DTM(r1.test_id) || '_GENERATED_' || l_sysdate || '.html';
       l_filename := 'AWRRPT_' || R1.DATABASE_NAME || '_' || r1.test_id || '_GENERATED_' || l_sysdate || '.html';
       dbms_output.put_line ('File name  : '|| l_filename);

       f1 := utl_file.fopen('AWR_REPORT', l_filename, 'W');
       SELECT awr_report into l_output FROM hp_diag.test_result_awr WHERE test_id = r1.test_id AND database_name = r1.database_name ;
       l_length := DBMS_LOB.getLength(l_output);
       while l_offset < l_length loop
          l_amount := LEAST ( DBMS_LOB.instr(l_output, chr(10), l_offset)-l_offset, 32767);
           if l_amount > 0 then
              DBMS_LOB.READ (l_output, l_amount, l_offset, l_buffer) ;
              l_offset := l_offset + l_amount + 1 ;
           else
              l_buffer := null;
              l_offset := l_offset + 1;
           end if;
           l_row := l_buffer ;
           utl_file.put_line(f1, l_row);

        end loop;

        utl_file.fclose(f1);
   End Loop;   

END Create_AWR_HTML_files;

FUNCTION Get_StartSnapId (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE  
        ) RETURN number
AS
   l_testId1       hp_diag.test_result_metrics.test_id%TYPE ;
   l_start_dtm     varchar2(30);
   l_start_snapid  number;
BEGIN
   l_testid1 := i_testId1;
   -- Get the dates from the TEST_ID
   l_start_dtm := Get_Start_DTM(l_testId1);

   -- Get test1 start snap_id
   SELECT MIN ( s.snap_id ) INTO l_start_snapid 
     FROM dba_hist_snapshot s
     JOIN v$database d ON d.dbid = s.dbid 
    WHERE s.end_interval_time >= TO_TIMESTAMP ( l_start_dtm , 'DDMONYY-HH24:MI' );

    return l_start_snapid;
END Get_StartSnapId;

FUNCTION Get_EndSnapId (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE  
        ) RETURN number
AS
   l_testId1       hp_diag.test_result_metrics.test_id%TYPE ;
   l_end_dtm       varchar2(30);
   l_end_snapid    number;
BEGIN
   l_testId1 := i_testId1;
   -- Get the dates from the TEST_ID
   l_end_dtm := Get_End_DTM(l_testId1);

   -- Get test1 end snap_id
   SELECT MAX ( s.snap_id ) INTO l_end_snapid 
     FROM dba_hist_snapshot s
     JOIN v$database d ON d.dbid = s.dbid 
    WHERE s.begin_interval_time <= TO_TIMESTAMP ( l_end_dtm , 'DDMONYY-HH24:MI' );

    return l_end_snapid;
END Get_EndSnapId;


FUNCTION Get_Start_DTM (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE  
        ) RETURN varchar2
AS
   l_testId1       hp_diag.test_result_metrics.test_id%TYPE ;
   l_start_dtm     varchar2(30);
   l_start_dtm_tmp varchar2(30);

BEGIN
   l_testId1 := i_testId1 ;
   select SUBSTR(l_testId1,1,INSTR(l_testId1,'_')-1) into l_start_dtm_tmp from dual ;
   select SUBSTR(l_start_dtm_tmp,1,INSTR(l_start_dtm_tmp,'-')+2)||':'||substr(l_start_dtm_tmp,INSTR(l_start_dtm_tmp,'-')+3) into l_start_dtm from dual ;
   return l_start_dtm ;
END Get_Start_DTM;

FUNCTION Get_End_DTM (
        i_testId1 IN hp_diag.test_result_metrics.test_id%TYPE  
        ) RETURN varchar2
AS
   l_testId1  hp_diag.test_result_metrics.test_id%TYPE ;
   l_end_dtm varchar2(30);
   l_end_dtm_tmp varchar2(30);
BEGIN
   l_testId1 := i_testId1 ;
   select SUBSTR(l_testId1,INSTR(l_testId1,'_')+1) into l_end_dtm_tmp from dual ;
   select SUBSTR(l_end_dtm_tmp,1,INSTR(l_end_dtm_tmp,'-')+2)||':'||substr(l_end_dtm_tmp,INSTR(l_end_dtm_tmp,'-')+3) into l_end_dtm from dual ;
   return l_end_dtm ;
END Get_End_DTM;


END REPORT_ADM;
/
