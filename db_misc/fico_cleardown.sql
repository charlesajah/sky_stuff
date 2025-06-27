alter session set ddl_lock_timeout = 300 ;
alter session set nls_date_format = 'dd/mm/yyyy hh24:mi:ss';

set serveroutput on head off lines 150 pages 99

select 'Start Time : ' || to_char(sysdate,'dd/mm/yyyy hh24:mi:ss') from dual;

set head on

SELECT COUNT(DISTINCT application.id) no_of_applications,
       COUNT(DISTINCT application.subjectid) no_of_subjects,
       COUNT(DISTINCT interfaceaudit.tracecontentid) no_of_traces
FROM   t1_os_admin_s.osusr_nho_applicat application,
       t1_os_admin_s.osusr_nho_decision decisionrequest,
       t1_os_admin_s.osusr_67w_subjecta subjectaudit,
       t1_os_admin_s.osusr_67w_interfac interfaceaudit
WHERE  application.id = decisionrequest.applicationid(+)
AND    application.subjectid = subjectaudit.subjectid(+)
AND    subjectaudit.id = interfaceaudit.subjectauditid(+)
AND    decisionrequest.created < add_months(sysdate, -&1) ;

set serveroutput on
declare
  intMonths       integer := 36;
  
  numTracecontent number;
  numApplication  number;
  numSubject      number;

  strTracecontent varchar2(1000) := 'DELETE from t1_os_admin_s.osusr_2s2_tracecon tracecontent WHERE  tracecontent.id <= ';
  strApplication  varchar2(1000) := 'DELETE from t1_os_admin_s.osusr_nho_applicat application  WHERE  application.id  <= ';
  strSubject      varchar2(1000) := 'DELETE from t1_os_admin_s.osusr_67w_subject subject       WHERE  subject.id      <= ';
  strSQL          varchar2(1000) := '';
begin
  while intMonths > &1 loop
    dbms_output.put_line('Processing older than ' || intMonths || ' Months. Starting at ' || to_char(sysdate, 'hh24:mi:ss dd/mm/yyyy'));
    
    SELECT  MAX(interfaceaudit.tracecontentid) as tracecontent,
            MAX(application.id) as application,
            MAX(application.subjectid) as subject
      into numTracecontent, numApplication, numSubject
      FROM t1_os_admin_s.osusr_nho_applicat application,  t1_os_admin_s.osusr_nho_decision decisionrequest,
           t1_os_admin_s.osusr_67w_subjecta subjectaudit, t1_os_admin_s.osusr_67w_interfac interfaceaudit
    WHERE  application.id = decisionrequest.applicationid(+)
      AND  application.subjectid = subjectaudit.subjectid(+)
      AND  subjectaudit.id = interfaceaudit.subjectauditid(+)
      AND  decisionrequest.created < add_months(sysdate, -intMonths) ;
      
      if numTracecontent is not null then
        begin
          SAVEPOINT delete_start1;
          strSQL := strTracecontent || ' ' || numTracecontent;
          execute immediate strSQL;
          commit;
        exception
          when others then
            ROLLBACK TO delete_start1;
            dbms_output.put_line('Database error, delete rolled back.');
            dbms_output.put_line(SQLERRM);
            exit;
        end;
      end if;
      
      if numApplication is not null then
        begin
          SAVEPOINT delete_start2;
          strSQL := strApplication || ' ' || numApplication;
          execute immediate strSQL;
          commit;
        exception
          when others then
            ROLLBACK TO delete_start2;
            dbms_output.put_line('Database error, delete rolled back.');
            dbms_output.put_line(SQLERRM);
            exit;
        end;
      end if;

      if numSubject is not null then
        begin
          SAVEPOINT delete_start3;
          strSQL := strSubject || ' ' || numSubject;
          execute immediate strSQL;
          commit;
        exception
          when others then
            ROLLBACK TO delete_start3;
            dbms_output.put_line('Database error, delete rolled back.');
            dbms_output.put_line(SQLERRM);
            exit;
        end;
      end if;

    intMonths := intMonths - 0.5;
  end loop;
end;
/

SELECT COUNT(DISTINCT application.id) no_of_applications,
       COUNT(DISTINCT application.subjectid) no_of_subjects,
       COUNT(DISTINCT interfaceaudit.tracecontentid) no_of_traces
FROM   t1_os_admin_s.osusr_nho_applicat application,
       t1_os_admin_s.osusr_nho_decision decisionrequest,
       t1_os_admin_s.osusr_67w_subjecta subjectaudit,
       t1_os_admin_s.osusr_67w_interfac interfaceaudit
WHERE  application.id = decisionrequest.applicationid(+)
AND    application.subjectid = subjectaudit.subjectid(+)
AND    subjectaudit.id = interfaceaudit.subjectauditid(+)
AND    decisionrequest.created < add_months(sysdate, -&1) ;

ALTER INDEX T1_OS_ADMIN_S.OSPRK_OSUSR_NHO_APPLICAT COALESCE;
ALTER INDEX T1_OS_ADMIN_S.OSIDX_OSUSR_NHO_APPLICAT_07408 COALESCE;
ALTER INDEX T1_OS_ADMIN_S.OSIDX_OSUSR_NHO_APPLICAT_73946 COALESCE;
ALTER INDEX T1_OS_ADMIN_S.OSIDX_OSUSR_NHO_APPLICAT_14265 COALESCE;
ALTER INDEX T1_OS_ADMIN_S.OSPRK_OSUSR_2S2_TRACECON COALESCE;

--exec dbms_stats.gather_table_stats('t1_os_admin_s','osusr_2s2_tracecon');
--exec dbms_stats.gather_table_stats('t1_os_admin_s','osusr_nho_applicat');
--exec dbms_stats.gather_table_stats('t1_os_admin_s','osusr_67w_subject');

set head off
select 'End Time : ' || to_char(sysdate,'dd/mm/yyyy hh24:mi:ss') from dual;
