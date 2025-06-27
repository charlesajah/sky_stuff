create or replace PACKAGE hp_diag.PERF_METRICS
/*
|| Name : hp_diag.perf_metrics
|| Database : TCC021N
*/
AS
    TYPE g_tvc2 IS TABLE OF VARCHAR2(4000) ;

    FUNCTION get_perf_metrics   
        ( i_env       varchar2 default 'ALL'
         ,i_grpname   varchar2 default 'FULL'
        ) RETURN g_tvc2 PIPELINED ;
    FUNCTION get_db_load_profile   
        ( i_env       varchar2 default 'ALL'
         ,i_grpname   varchar2 default 'FULL'
        ) RETURN g_tvc2 PIPELINED ;
    FUNCTION get_db_waitclass
        ( i_env       varchar2 default 'ALL'
         ,i_grpname   varchar2 default 'FULL'
		 ,i_wait_class varchar2 default NULL
        ) RETURN g_tvc2 PIPELINED;

END PERF_METRICS ;
/

create or replace package body  hp_diag.PERF_METRICS
AS
FUNCTION get_perf_metrics 
        ( i_env       varchar2 default 'ALL'
         ,i_grpname   varchar2 default 'FULL'
        ) RETURN g_tvc2 PIPELINED
AS
    l_str   varchar2(100);
    l_env   varchar2(10);
    l_group varchar2(10);
BEGIN
    -- Initialised values
    l_env   := Upper(i_env) ;
    l_group := Upper(i_grpname) ;
    
    -- Get date 3 months in the past from today's date
    select to_char(min(add_months(sysdate,-6)),'dd/mm/yyyy')
    into l_str
    from dual;

    PIPE ROW ('{toc:type=list}');
    PIPE ROW ('h3. Time Window');
    PIPE ROW ('Start Date: ' || l_str );
    PIPE ROW ('End Date: ' || to_char(sysdate,'dd/mm/yyyy'));
    PIPE ROW ('');
    PIPE ROW ('Note: The results are captured from AWR information and as such are aggregated accordingly');
    PIPE ROW ('');
    PIPE ROW ('Average Active Sessions     : is the average across the entire test window for environment '||l_env);
    PIPE ROW ('Max Average Active Sessions : is the peak average active session observed across the test window');

    PIPE ROW ('h3. Average Active Sessions');
    for r1 in (select distinct database_name 
                 from hp_diag.test_result_metrics_detail trm,hp_diag.test_result_dbs trd
                 where trm.database_name=trd.db_name
                 and trd.db_env=l_env
                 and begin_time > to_date(l_str,'dd/mm/yyyy')
                 order by database_name)
    loop
        PIPE ROW ( 'h5. Average Active Sessions for ' || r1.database_name) ;
        PIPE ROW ( '{chart:dataOrientation=vertical|categoryLabelPosition=down45|rangeAxisLowerBound=0|domainAxisTickUnit=2|datepattern=dd/MM/yyyy HH:mm|formatVersion=3|height=500|orientation=vertical|stacked=false|timePeriod=Day|timeSeries=false|title=' || r1.database_name || '|type=bar|width=1000|xLabel=Date|yLabel=Avg_Act_Sessions}' ) ;
        PIPE ROW ( ' || DATE_TIME || Average Active Session|| Max Average Active Sessions ||' ) ;
        --for r2 in ( select   '|' || to_char(trmd.begin_time, 'dd/mm/yyyy HH24:MI') || '|' || round(average, 2) || '|' as chart_row 
        for r2 in ( select   '|' || to_char(trmd.begin_time, 'dd/mm/yyyy HH24:MI') || '|' || round(average, 2) || '|' || '|' || round(max_average, 2) || '|' as chart_row 
            from 
              --hp_diag.test_result_metrics_detail trmd, 
              hp_diag.test_result_metrics trmd, 
              hp_diag.test_result_master trm 
            where 
              trmd.database_name = r1.database_name
              and trmd.test_id = trm.test_id 
              and upper(trm.best_test_for_release) like 'RM%'
              and trmd.metric_name = 'Average Active Sessions' 
              --and trim(to_char(trmd.begin_time, 'DAY')) = 'TUESDAY' 
              --and to_char(trmd.begin_time,'hh24:mi') in ('06:29','06:30','06:31')
              /*and ( EXTRACT( HOUR FROM cast(trmd.begin_time as timestamp)) * 60 + EXTRACT(MINUTE FROM cast(trmd.begin_time as timestamp)
                )
              ) BETWEEN (6 * 60 + 30) AND (9 * 60) */
              and trmd.begin_time > to_date(l_str,'dd/mm/yyyy')
            order by 
              trmd.begin_time
)
        loop
            PIPE ROW ( r2.chart_row ) ;
        end loop;
        PIPE ROW ('{chart}') ;
    end loop;
end get_perf_metrics;

FUNCTION get_db_load_profile
        ( i_env       varchar2 default 'ALL'
         ,i_grpname   varchar2 default 'FULL'
        ) RETURN g_tvc2 PIPELINED
AS
    l_str   varchar2(100);
    l_env   varchar2(10);
    l_group varchar2(10);
BEGIN
    -- Initialised values
    l_env   := Upper(i_env) ;
    l_group := Upper(i_grpname) ;


    -- Get date 6 months in the past from today's date
    select to_char(min(add_months(sysdate,-6)),'dd/mm/yyyy')
    into l_str
    from dual;

    PIPE ROW ('{toc:type=list}');
    PIPE ROW ('h3. Time Window');
    PIPE ROW ('Start Date: ' || l_str );
    PIPE ROW ('End Date: ' || to_char(sysdate,'dd/mm/yyyy'));
    PIPE ROW ('');
    PIPE ROW ('Note: The results are captured from AWR information and as such are aggregated accordingly');
    PIPE ROW ('');
    PIPE ROW ('DB_TIME_PER_SEC  : is the average DB Time Per Sec across the entire test window');
    PIPE ROW ('DB_CPU_PER_SEC : is the average DB CPU observed across the test window');

    PIPE ROW ('h3. Database load profile');
    for r1 in (select distinct database_name 
                 from hp_diag.test_result_db_stats trds,hp_diag.test_result_dbs trd
                 where trds.database_name=trd.db_name
                 and trd.db_env=l_env
                 and begin_time > to_date(l_str,'dd/mm/yyyy')
                 order by database_name)
    loop
        PIPE ROW ( 'h5. Database load profile for ' || r1.database_name) ;
        PIPE ROW ( '{chart:dataOrientation=vertical|categoryLabelPosition=down45|rangeAxisLowerBound=0|domainAxisTickUnit=2|datepattern=dd/MM/yyyy HH:mm|formatVersion=3|height=500|orientation=vertical|stacked=false|timePeriod=Day|timeSeries=false|title=' || r1.database_name || '|type=bar|width=1000|xLabel=Date|yLabel=DB Load}' ) ;
        PIPE ROW ( ' || DATE_TIME || DB_TIME_PER_SEC || DB_CPU_PER_SEC ||' ) ;
        for r2 in ( select   '|' || to_char(trds.begin_time, 'dd/mm/yyyy HH24:MI') || '|' || trds.DB_TIME_PER_SEC || '|' || '|' || trds.DB_CPU_PER_SEC || '|' as chart_row 
            from hp_diag.test_result_db_stats trds,hp_diag.test_result_master trm 
				where trds.test_id=trm.test_id
                and trds.database_name = r1.database_name
                and upper(trm.best_test_for_release) like 'RM%'
                   --trim(to_char(begin_time, 'DAY')) = 'TUESDAY'
				   --and to_char(begin_time,'hh24:mi') in ('06:29','06:30','06:31')
                and trds.begin_time > add_months(sysdate,-6)
            order by 
              trds.begin_time
)
        loop
            PIPE ROW ( r2.chart_row ) ;
        end loop;
        PIPE ROW ('{chart}') ;
    end loop;
end get_db_load_profile;

FUNCTION get_db_waitclass
        ( i_env       varchar2 default 'ALL'
         ,i_grpname   varchar2 default 'FULL'
		 ,i_wait_class varchar2 default NULL
        ) RETURN g_tvc2 PIPELINED
AS
    l_str   varchar2(100);
    l_env   varchar2(10);
    l_group varchar2(10);
	l_waitclass varchar2(100);
BEGIN
    -- Initialised values
    l_env   := Upper(i_env) ;
    l_group := Upper(i_grpname) ;
	l_waitclass := Upper(i_wait_class) ;

    -- Get date 3 months in the past from today's date
    select to_char(min(add_months(sysdate,-6)),'dd/mm/yyyy')
    into l_str
    from dual;

	--create directory for files
	--DBMS_SESSION.SET_NLS('nls_date_format', 'YYYYMMDDHH24MI');
	--select sysdate into 1_dir from dual;

    PIPE ROW ('{toc:type=list}');
    PIPE ROW ('h3. Time Window');
    PIPE ROW ('Start Date: ' || l_str );
    PIPE ROW ('End Date: ' || to_char(sysdate,'dd/mm/yyyy'));
    PIPE ROW ('');
    PIPE ROW ('Note: The results are captured from AWR information and as such are aggregated accordingly');
    PIPE ROW ('');
    PIPE ROW ('Database Average ( CPU and User I/O )  : is the average wait time across the test window');
    PIPE ROW ('Database Max Average ( CPU and User I/O ) : is the peak average wait time observed across the test window');

    PIPE ROW ('h3. Database Average CPU and User I/O');
    for r1 in (select distinct database_name 
                 from hp_diag.test_result_metrics_detail trm,hp_diag.test_result_dbs trd
                 where trm.database_name=trd.db_name
                 and trd.db_env=l_env
                 and begin_time > to_date(l_str,'dd/mm/yyyy')
                 order by database_name)
    loop
        PIPE ROW ( 'h5. Database waitclass chart for ' || r1.database_name) ;
		PIPE ROW ( '|| CPU || USER I/O|| ' );
        PIPE ROW ('| {chart:dataOrientation=vertical|categoryLabelPosition=down45|rangeAxisLowerBound=0|domainAxisTickUnit=2|datepattern=dd/MM/yyyy HH:mm|formatVersion=3|height=500|orientation=vertical|stacked=false|timePeriod=Day|timeSeries=false|type=bar|width=1000|xLabel=Date|yLabel=DB CPU}' ) ;
        PIPE ROW ( '|| DATE_TIME || Average Sessions|| Max Average Sessions|' );
		--for r2 in ( select   '|' || to_char(trmd.begin_time, 'dd/mm/yyyy HH24:MI') || '|' || round(average, 2) || '|' as chart_row 
        for r2 in ( select   '|' || to_char(trm.begin_time, 'dd/mm/yyyy HH24:MI') || '|' || avg_sessions || '|' || max_sessions || '|' as chart_row 
            from hp_diag.test_result_wait_class trwc, hp_diag.test_result_master trm
			where trwc.database_name = r1.database_name
			   and trwc.test_id = trm.test_id
               and upper(trm.best_test_for_release) like 'RM%'
			   and trwc.wait_class = 'CPU'
			   --and trim(to_char(trm.begin_time, 'DAY')) = 'TUESDAY' 
			   --and to_char(trm.begin_time,'hh24:mi') in ('06:29','06:30','06:31')
			   and trm.begin_time > add_months(trunc(sysdate), -6)
			order by trm.begin_time, trwc.wait_class
)
        loop
            PIPE ROW ( r2.chart_row ) ;
        end loop;
        --PIPE ROW ('{chart} |') ;
		PIPE ROW ('{chart} | {chart:dataOrientation=vertical|categoryLabelPosition=down45|rangeAxisLowerBound=0|domainAxisTickUnit=2|datepattern=dd/MM/yyyy HH:mm|formatVersion=3|height=500|orientation=vertical|stacked=false|timePeriod=Day|timeSeries=false|type=bar|width=1000|xLabel=Date|yLabel=USER I/O}' ) ;
		PIPE ROW ( '|| DATE_TIME || Average Sessions|| Max Average Sessions|' );
		--for r2 in ( select   '|' || to_char(trmd.begin_time, 'dd/mm/yyyy HH24:MI') || '|' || round(average, 2) || '|' as chart_row 
        for r3 in ( select   '|' || to_char(trm.begin_time, 'dd/mm/yyyy HH24:MI') || '|' || avg_sessions || '|' || max_sessions || '|' as chart_row 
            from hp_diag.test_result_wait_class trwc, hp_diag.test_result_master trm
			where trwc.database_name = r1.database_name
			   and trwc.test_id = trm.test_id
               and upper(trm.best_test_for_release) like 'RM%'
			   and trwc.wait_class = 'User I/O'
			   --and trim(to_char(trm.begin_time, 'DAY')) = 'TUESDAY' 
			   --and to_char(trm.begin_time,'hh24:mi') in ('06:29','06:30','06:31')
			   and trm.begin_time > add_months(trunc(sysdate), -6)
			order by trm.begin_time, trwc.wait_class
)
		loop
			PIPE ROW (r3.chart_row) ;
		end loop;
		PIPE ROW ('{chart} |') ;
    end loop;
end get_db_waitclass;

FUNCTION get_perf_metrics_dailies 
        ( i_env       varchar2 default 'ALL'
         ,i_grpname   varchar2 default 'FULL'
        ) RETURN g_tvc2 PIPELINED
AS
    l_str   varchar2(100);
    l_env   varchar2(10);
    l_group varchar2(10);
    l_time_slice1 varchar2(10);
    l_time_slice2 varchar2(10);
    l_time_slice3 varchar2(10);
BEGIN
    -- Initialised values
    l_env   := Upper(i_env) ;
    l_group := Upper(i_grpname) ;
    
    if l_env = 'N01' then
        l_time_slice1 := '''06:29''';
        l_time_slice2 := '''06:30''';
        l_time_slice3 := '''06:31''';
    elsif l_env = 'N02' then
        l_time_slice1 := '''20:29''';
        l_time_slice2 := '''20:30''';
        l_time_slice3 := '''20:31''';
    end if;
    -- Get date 3 months in the past from today's date
    select to_char(min(add_months(sysdate,-1)),'dd/mm/yyyy')
    into l_str
    from dual;

    PIPE ROW ('{toc:type=list}');
    PIPE ROW ('h3. Time Window');
    PIPE ROW ('Start Date: ' || l_str );
    PIPE ROW ('End Date: ' || to_char(sysdate,'dd/mm/yyyy'));
    PIPE ROW ('');
    PIPE ROW ('Note: The results are captured from AWR information and as such are aggregated accordingly');
    PIPE ROW ('');
    PIPE ROW ('Average Active Sessions     : is the average across the entire test window for environment '||l_env);
    PIPE ROW ('Max Average Active Sessions : is the peak average active session observed across the test window');

    PIPE ROW ('h3. Average Active Sessions');
    for r1 in (select distinct database_name 
                 from hp_diag.test_result_metrics_detail trm,hp_diag.test_result_dbs trd
                 where trm.database_name=trd.db_name
                 and trd.db_env=l_env
                 and begin_time > to_date(l_str,'dd/mm/yyyy')
                 order by database_name)
    loop
        PIPE ROW ( 'h5. Average Active Sessions for ' || r1.database_name) ;
        PIPE ROW ( '{chart:dataOrientation=vertical|categoryLabelPosition=down45|rangeAxisLowerBound=0|domainAxisTickUnit=2|datepattern=dd/MM/yyyy HH:mm|formatVersion=3|height=500|orientation=vertical|stacked=false|timePeriod=Day|timeSeries=false|title=' || r1.database_name || '|type=bar|width=1000|xLabel=Date|yLabel=Avg_Act_Sessions}' ) ;
        PIPE ROW ( ' || DATE_TIME || Average Active Session|| Max Average Active Sessions ||' ) ;
        --for r2 in ( select   '|' || to_char(trmd.begin_time, 'dd/mm/yyyy HH24:MI') || '|' || round(average, 2) || '|' as chart_row 
        for r2 in ( select   '|' || to_char(trms.begin_time, 'dd/mm/yyyy HH24:MI') || '|' || round(average, 2) || '|' || '|' || round(max_average, 2) || '|' as chart_row 
            from 
              --hp_diag.test_result_metrics_detail trmd, 
              hp_diag.test_result_metrics trms, 
              hp_diag.test_result_master trm 
            where 
              trms.test_id = trm.test_id 
              and trms.database_name = r1.database_name
              --and upper(trm.best_test_for_release) like 'RM%'
              and trms.metric_name = 'Average Active Sessions' 
              --and trim(to_char(trmd.begin_time, 'DAY')) = 'TUESDAY' 
              --and to_char(trmd.begin_time,'hh24:mi') in (l_time_slice1,l_time_slice2,l_time_slice3)
              --and to_char(trmd.begin_time,'hh24:mi') in ('20:29','20:30','20:31')
              /*and ( EXTRACT( HOUR FROM cast(trmd.begin_time as timestamp)) * 60 + EXTRACT(MINUTE FROM cast(trmd.begin_time as timestamp)
                )
              ) BETWEEN (6 * 60 + 30) AND (9 * 60) */
              and trms.begin_time > to_date(l_str,'dd/mm/yyyy')
            order by 
              trms.begin_time
)
        loop
            PIPE ROW ( r2.chart_row ) ;
        end loop;
        PIPE ROW ('{chart}') ;
    end loop;
end get_perf_metrics_dailies;

FUNCTION get_db_load_profile_dailies
        ( i_env       varchar2 default 'ALL'
         ,i_grpname   varchar2 default 'FULL'
        ) RETURN g_tvc2 PIPELINED
AS
    l_str   varchar2(100);
    l_env   varchar2(10);
    l_group varchar2(10);
BEGIN
    -- Initialised values
    l_env   := Upper(i_env) ;
    l_group := Upper(i_grpname) ;


    -- Get date 6 months in the past from today's date
    select to_char(min(add_months(sysdate,-1)),'dd/mm/yyyy')
    into l_str
    from dual;

    PIPE ROW ('{toc:type=list}');
    PIPE ROW ('h3. Time Window');
    PIPE ROW ('Start Date: ' || l_str );
    PIPE ROW ('End Date: ' || to_char(sysdate,'dd/mm/yyyy'));
    PIPE ROW ('');
    PIPE ROW ('Note: The results are captured from AWR information and as such are aggregated accordingly');
    PIPE ROW ('');
    PIPE ROW ('DB_TIME_PER_SEC  : is the average DB Time Per Sec across the entire test window');
    PIPE ROW ('DB_CPU_PER_SEC : is the average DB CPU observed across the test window');

    PIPE ROW ('h3. Database load profile');
    for r1 in (select distinct database_name 
                 from hp_diag.test_result_db_stats trds,hp_diag.test_result_dbs trd
                 where trds.database_name=trd.db_name
                 and trd.db_env=l_env
                 and begin_time > to_date(l_str,'dd/mm/yyyy')
                 order by database_name)
    loop
        PIPE ROW ( 'h5. Database load profile for ' || r1.database_name) ;
        PIPE ROW ( '{chart:dataOrientation=vertical|categoryLabelPosition=down45|rangeAxisLowerBound=0|domainAxisTickUnit=2|datepattern=dd/MM/yyyy HH:mm|formatVersion=3|height=500|orientation=vertical|stacked=false|timePeriod=Day|timeSeries=false|title=' || r1.database_name || '|type=bar|width=1000|xLabel=Date|yLabel=DB Load}' ) ;
        PIPE ROW ( ' || DATE_TIME || DB_TIME_PER_SEC || DB_CPU_PER_SEC ||' ) ;
        for r2 in ( select   '|' || to_char(trds.begin_time, 'dd/mm/yyyy HH24:MI') || '|' || trds.DB_TIME_PER_SEC || '|' || '|' || trds.DB_CPU_PER_SEC || '|' as chart_row 
            from hp_diag.test_result_db_stats trds,hp_diag.test_result_master trm 
				where trds.test_id=trm.test_id
                and trds.database_name = r1.database_name
                --and to_char(trds.begin_time,'hh24:mi') in ('20:29','20:30','20:31')
                --trim(to_char(begin_time, 'DAY')) = 'TUESDAY'
                and trds.begin_time > add_months(sysdate,-1)
            order by 
              trds.begin_time
)
        loop
            PIPE ROW ( r2.chart_row ) ;
        end loop;
        PIPE ROW ('{chart}') ;
    end loop;
end get_db_load_profile_dailies;

FUNCTION get_db_waitclass_dailies
        ( i_env       varchar2 default 'ALL'
         ,i_grpname   varchar2 default 'FULL'
		 ,i_wait_class varchar2 default NULL
        ) RETURN g_tvc2 PIPELINED
AS
    l_str   varchar2(100);
    l_env   varchar2(10);
    l_group varchar2(10);
	l_waitclass varchar2(100);
BEGIN
    -- Initialised values
    l_env   := Upper(i_env) ;
    l_group := Upper(i_grpname) ;
	l_waitclass := Upper(i_wait_class) ;

    -- Get date 3 months in the past from today's date
    select to_char(min(add_months(sysdate,-6)),'dd/mm/yyyy')
    into l_str
    from dual;

	--create directory for files
	--DBMS_SESSION.SET_NLS('nls_date_format', 'YYYYMMDDHH24MI');
	--select sysdate into 1_dir from dual;

    PIPE ROW ('{toc:type=list}');
    PIPE ROW ('h3. Time Window');
    PIPE ROW ('Start Date: ' || l_str );
    PIPE ROW ('End Date: ' || to_char(sysdate,'dd/mm/yyyy'));
    PIPE ROW ('');
    PIPE ROW ('Note: The results are captured from AWR information and as such are aggregated accordingly');
    PIPE ROW ('');
    PIPE ROW ('Database Average ( CPU and User I/O )  : is the average wait time across the test window');
    PIPE ROW ('Database Max Average ( CPU and User I/O ) : is the peak average wait time observed across the test window');

    PIPE ROW ('h3. Database Average CPU and User I/O');
    for r1 in (select distinct database_name 
                 from hp_diag.test_result_metrics_detail trm,hp_diag.test_result_dbs trd
                 where trm.database_name=trd.db_name
                 and trd.db_env=l_env
                 and begin_time > to_date(l_str,'dd/mm/yyyy')
                 order by database_name)
    loop
        PIPE ROW ( 'h5. Database waitclass chart for ' || r1.database_name) ;
		PIPE ROW ( '|| CPU || USER I/O|| ' );
        PIPE ROW ('| {chart:dataOrientation=vertical|categoryLabelPosition=down45|rangeAxisLowerBound=0|domainAxisTickUnit=2|datepattern=dd/MM/yyyy HH:mm|formatVersion=3|height=500|orientation=vertical|stacked=false|timePeriod=Day|timeSeries=false|type=bar|width=1000|xLabel=Date|yLabel=DB CPU}' ) ;
        PIPE ROW ( '|| DATE_TIME || Average Sessions|| Max Average Sessions|' );
		--for r2 in ( select   '|' || to_char(trmd.begin_time, 'dd/mm/yyyy HH24:MI') || '|' || round(average, 2) || '|' as chart_row 
        for r2 in ( select   '|' || to_char(trm.begin_time, 'dd/mm/yyyy HH24:MI') || '|' || avg_sessions || '|' || max_sessions || '|' as chart_row 
            from hp_diag.test_result_wait_class trwc, hp_diag.test_result_master trm
			where trwc.database_name = r1.database_name
			   and trwc.test_id = trm.test_id
               --and upper(trm.best_test_for_release) like 'RM%'
			   and trwc.wait_class = 'CPU'
			   --and trim(to_char(trm.begin_time, 'DAY')) = 'TUESDAY' 
			   --and to_char(trm.begin_time,'hh24:mi') in ('20:29','20:30','20:31')
			   and trm.begin_time > add_months(trunc(sysdate), -1)
			order by trm.begin_time, trwc.wait_class
)
        loop
            PIPE ROW ( r2.chart_row ) ;
        end loop;
        --PIPE ROW ('{chart} |') ;
		PIPE ROW ('{chart} | {chart:dataOrientation=vertical|categoryLabelPosition=down45|rangeAxisLowerBound=0|domainAxisTickUnit=2|datepattern=dd/MM/yyyy HH:mm|formatVersion=3|height=500|orientation=vertical|stacked=false|timePeriod=Day|timeSeries=false|type=bar|width=1000|xLabel=Date|yLabel=USER I/O}' ) ;
		PIPE ROW ( '|| DATE_TIME || Average Sessions|| Max Average Sessions|' );
		--for r2 in ( select   '|' || to_char(trmd.begin_time, 'dd/mm/yyyy HH24:MI') || '|' || round(average, 2) || '|' as chart_row 
        for r3 in ( select   '|' || to_char(trm.begin_time, 'dd/mm/yyyy HH24:MI') || '|' || avg_sessions || '|' || max_sessions || '|' as chart_row 
            from hp_diag.test_result_wait_class trwc, hp_diag.test_result_master trm
			where trwc.database_name = r1.database_name
			   and trwc.test_id = trm.test_id
			   and trwc.wait_class = 'User I/O'
			   --and trim(to_char(trm.begin_time, 'DAY')) = 'TUESDAY' 
			   --and to_char(trm.begin_time,'hh24:mi') in ('20:29','20:30','20:31')
			   and trm.begin_time > add_months(trunc(sysdate), -1)
			order by trm.begin_time, trwc.wait_class
)
		loop
			PIPE ROW (r3.chart_row) ;
		end loop;
		PIPE ROW ('{chart} |') ;
    end loop;
end get_db_waitclass_dailies;

END perf_metrics ;
/