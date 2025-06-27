select * from table(HP_DIAG.test_charles.get_top5_graphv3(test_id => '19SEP24-0630_19SEP24-0900_N01_FULL',db_name => 'CHORDO'));
select * from table (REPORT_COMP.Get_DB_Activity_Body ( testid => '19SEP24-0630_19SEP24-0900_N01_FULL', dbname => '&&g_dbname'));
select * from table(HP_DIAG.top_five.GET_TOP5_CHARTS('19SEP24-0630_19SEP24-0900_N01_FULL','CHORDO'));
select '| {table-chart:type=Stacked Column|column=interval_time|hide=true|aggregation='||(select * from table(HP_DIAG.test_charles.get_top5_graphv2(test_id => '19SEP24-0630_19SEP24-0900_N01_FULL',db_name => 'CHORDO')))
        ||'|barColoringType=mono|colors=#65a620,#e4a14b,#3572b0,#8cc3e9,#654982,#cc1010,#e98125,#a3acb2,#d8d23a,#6ada6a,#7b6888,#000000,#f691b2|'
		||'datepattern=HH:mm|formatVersion=3|hidecontrols=true|isFirstTimeEnter=false|tfc-height=400|tfc-width=1200|title='||'CHORDO'||'_'||'19SEP24-0630_19SEP24-0900_N01_FULL|xtitle=Time|ytitle=Sessions|version=3}' from dual ;


select ''''||sysdate||'''' from dual;
select unistr('\201A') from dual;

select (select * from table(HP_DIAG.test_charles.get_top5_graphv2(test_id => '30SEP24-0630_30SEP24-0900_N01_FULL',db_name => 'CHORDO'))) from dual;


select TO_CHAR(TRUNC(begin_time, 'MI'), 'HH24:MI') begin_time,sql_id||'_'||plan_hash_value sql_phv, elapsed_time_per_exec_seconds * 1000 elapsed_time_per_exec_ms from test_result_sqldetails
where test_id='19SEP24-0630_19SEP24-0900_N01_FULL' and database_name='CHORDO'
and elapsed_time_per_exec_seconds is not null
order by begin_time,sql_id;



SELECT *
FROM (
    SELECT 
        TO_CHAR(TRUNC(begin_time, 'MI'), 'HH24:MI') AS begin_time,
        sql_id || '_' || plan_hash_value AS sql_phv,
        elapsed_time_per_exec_seconds * 1000 AS elapsed_time_per_exec_ms
    FROM test_result_sqldetails
    WHERE test_id = '19SEP24-0630_19SEP24-0900_N01_FULL'
      AND database_name = 'CHORDO'
      AND elapsed_time_per_exec_seconds IS NOT NULL
)
PIVOT (
    MAX(elapsed_time_per_exec_ms) 
    FOR sql_phv IN (
        '06xam4qyqk3aq_1073000046',
        '0j66wx5czjrz3_556275594',
        '0wy6qgvb2xsad_1604527505',
        '0wy6qgvb2xsad_2092391423',
        '0xbw42h2f9r7a_2571918273',
        '0ym6d5ja0ccvc_4190046182',
        '1nd718f1dstnp_4133868977',
        '31ajk3c4g8kxh_2000395894',
        '3dyg29y2jhhf8_3863283634',
        '7mst8tv1rzbvn_943469005',
        '8a14yn8mf3270_2897461441',
        'av4u28bdpcnuc_49097228',
        'g8q5gs6y6kund_1635450226',
        'awa0un61xthuq_3555799821',
        'ckkb9tzdgmarv_3336310017',
        'faba6n118kvvh_3033559130',
        'gfb4389663jjm_2182537318',
        'gvk9xuaw0r90w_103356596'
    )
)
ORDER BY begin_time;





SELECT * 
FROM (
    SELECT 
        TO_CHAR(TRUNC(begin_time, 'MI'), 'HH24:MI') AS interval_time,
        sql_id ||'_'|| plan_hash_value AS sqlid_phv,
        ROUND(elapsed_time_per_exec_seconds, 2) * 1000 AS elapsed_time_per_exec_ms
    FROM hp_diag.test_result_sqldetails
    WHERE test_id = '19SEP24-0630_19SEP24-0900_N01_FULL'
      AND database_name = 'CHORDO'
      AND elapsed_time_per_exec_seconds IS NOT NULL
      AND sql_id IN (
          SELECT DISTINCT sql_id
          FROM (
              SELECT sql_id,
                     RANK() OVER (PARTITION BY TO_CHAR(TRUNC(begin_time, 'MI'), 'HH24:MI')
                                  ORDER BY elapsed_time_seconds DESC) AS rank
              FROM hp_diag.test_result_sqldetails
              WHERE test_id = '19SEP24-0630_19SEP24-0900_N01_FULL'
                AND database_name = 'CHORDO'
          )
          WHERE rank <= 5
      )
)
PIVOT (
    MAX(elapsed_time_per_exec_ms) 
    FOR sqlid_phv IN (
    '0wy6qgvb2xsad_2092391423',
    '0wy6qgvb2xsad_1604527505',
    'ckkb9tzdgmarv_3336310017',
    '4mdn6rqu5jwdr_1462986027',
    '0ym6d5ja0ccvc_4190046182',
    '4mdn6rqu5jwdr_1156215561',
    '5sw41dpt9nffd_2637004617',
    '0j66wx5czjrz3_556275594'
    )
)
ORDER BY interval_time;





SELECT 
  * 
FROM 
  (
    SELECT 
      sql_id || '_' || plan_hash_value AS sql_phv, 
      TO_CHAR(
        TRUNC(begin_time, 'MI'), 
        'HH24:MI'
      ) AS interval_time, 
      ROUND(
        elapsed_time_per_exec_seconds, 2
      ) * 1000 AS elapsed_time_per_exec_ms 
    FROM 
      hp_diag.test_result_sqldetails 
    WHERE 
      test_id = '19SEP24-0630_19SEP24-0900_N01_FULL' 
      AND database_name = 'CHORDO' 
      AND elapsed_time_per_exec_seconds IS NOT NULL 
      AND sql_id IN (
        SELECT 
          DISTINCT sql_id 
        FROM 
          (
            SELECT 
              sql_id, 
              RANK() OVER (
                PARTITION BY TO_CHAR(
                  TRUNC(begin_time, 'MI'), 
                  'HH24:MI'
                ) 
                ORDER BY 
                  elapsed_time_seconds DESC
              ) AS rank 
            FROM 
              hp_diag.test_result_sqldetails 
            WHERE 
              test_id = '19SEP24-0630_19SEP24-0900_N01_FULL' 
              AND database_name = 'CHORDO'
          ) 
        WHERE 
          rank <= 5
      ) 
  ) PIVOT (
    MAX(elapsed_time_per_exec_ms) FOR sql_phv IN (
      '0wy6qgvb2xsad_2092391423', '0wy6qgvb2xsad_1604527505', 
      'ckkb9tzdgmarv_3336310017', '4mdn6rqu5jwdr_1462986027', 
      '0ym6d5ja0ccvc_4190046182', '4mdn6rqu5jwdr_1156215561', 
      '5sw41dpt9nffd_2637004617', '0j66wx5czjrz3_556275594'
    )
    
  )ORDER BY interval_time;
  
  
  
  select to_char(sysdate,'DD_MON_YYYY_HH24_MI_SS') from dual;
  
  
  
select TO_CHAR(TRUNC(begin_time, 'MI'), 'HH24:MI') begin_time,sql_id||'_'||plan_hash_value,elapsed_time_per_exec_seconds from hp_diag.test_result_sqldetails
where test_id='30SEP24-0630_30SEP24-0900_N01_FULL' and database_name='CSC011N'
order by begin_time;

select * from hp_diag.test_result_sqldetails



