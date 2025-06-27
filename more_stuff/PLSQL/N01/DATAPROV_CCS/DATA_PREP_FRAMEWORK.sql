CREATE OR REPLACE package          data_prep_framework
is
procedure test_logging (v_test_script_id in number
                        ,v_test_name in varchar2
                        ,v_test_load_type in varchar2
                        ,v_current_run_id in number
                        ,v_master_run_id in number);

procedure test_logging_detail (v_test_script_id in number
                               ,v_refresh_id in number
                               ,v_detail_id in number
                               ,v_test_name in varchar2
                               ,v_stage in varchar2
                               ,v_rows_processed in number);

procedure get_test_param (v_test_script_name in varchar2
                          ,v_param_name in varchar2
                          ,v_param_out out varchar2);

procedure add_test_param (v_test_script_name in varchar2
                          ,v_param_name in varchar2
                          ,v_param_value in varchar2
                          ,v_param_note in varchar2
                          );

procedure add_new_test (v_test_name in varchar2
                        ,v_test_script_name in varchar2
                          );

procedure add_new_test_script (v_test_script_name in varchar2
                               );

procedure add_new_test_type (v_test_type_name in varchar2
                             ,v_test_type_notes in varchar2
                               );


procedure add_new_test_vols (v_test_name_id in number
                             ,v_test_type_id in number
                             ,v_hourly_vol in number
                               );

procedure modify_test_vols (v_test_name_id in number
                             ,v_test_type_id in number
                             ,v_hourly_vol in number
                               );

procedure get_test_script_id (v_test_script_name in varchar2
                             ,v_test_script_id out number
                               );

end;
/


CREATE OR REPLACE package body          data_prep_framework
is
procedure test_logging (v_test_script_id in number
                                 ,v_test_name in varchar2
                                 ,v_test_load_type in varchar2
                                 ,v_current_run_id in number
                                 ,v_master_run_id in number) is
begin
merge into dataprov.dp_test_refresh_runs a
 using (select v_current_run_id runid
               ,case when v_master_run_id = -1 then null else v_master_run_id end master_runid
               ,upper(v_test_name) test_name
               ,v_test_script_id test_script_id
               ,upper(v_test_load_type) test_load_type
               ,sysdate from dual) b
 on (b.runid=a.run_id)
 when matched then update set a.end_time=sysdate
 when not matched then insert (a.run_id
                               ,a.master_run_id
                               ,a.test_name
                               ,a.test_script_id
                               ,a.refresh_type
                               ,a.start_time)
                        values (b.runid
                                ,master_runid
                                ,b.test_name
                                ,b.test_script_id
                                ,b.test_load_type
                                ,sysdate);
end;


procedure test_logging_detail (v_test_script_id in number
                               ,v_refresh_id in number
                               ,v_detail_id in number
                               ,v_test_name in varchar2
                               ,v_stage in varchar2
                               ,v_rows_processed in number) is
begin
merge into dataprov.dp_test_refresh_runs_detail a
 using (select v_refresh_id parent_run_id
               ,v_detail_id detail_id
               ,v_test_name test_name
               ,v_test_script_id test_script_id
               ,v_stage stage
               ,sysdate
               ,v_rows_processed rows_processed
               from dual) b
 on (b.detail_id=a.detail_id)
 when matched then update set a.end_time=sysdate, a.rows_processed=b.rows_processed
 when not matched then insert (a.parent_run_id
                               ,a.detail_id
                               ,a.test_name
                               ,a.test_script_id
                               ,a.stage
                               ,a.start_time
                               ,a.rows_processed)
                        values (b.parent_run_id
                                ,b.detail_id
                                ,upper(b.test_name)
                                ,b.test_script_id
                                ,upper(b.stage)
                                ,sysdate
                                ,b.rows_processed);


end;

procedure get_test_param (v_test_script_name in varchar2
                          ,v_param_name in varchar2
                          ,v_param_out out varchar2)
is
begin
select parameter_value into v_param_out
from dataprov.v_dp_test_script_params
where test_script_name=v_test_script_name
and parameter_name=v_param_name;
end;


procedure add_test_param (v_test_script_name in varchar2
                          ,v_param_name in varchar2
                          ,v_param_value in varchar2
                          ,v_param_note in varchar2
                          )
is
v_script_id number := null;
begin
select id into v_script_id
from dataprov.dp_test_scripts ts
where ts.test_script_name = v_test_script_name;

merge into dataprov.dp_test_script_params p
using (select v_script_id v_script_id
             ,v_param_name v_param_name
             ,v_param_value v_param_value
             ,v_param_note v_param_note
             ,sysdate v_last_updated
         from dual) b
on (p.test_script_id=b.v_script_id and p.parameter_name=b.v_param_name)
when matched then update set p.parameter_value=v_param_value
                             ,p.parameter_note=nvl(b.v_param_note,p.parameter_note)
                             ,p.last_updated=b.v_last_updated
when not matched then insert (p.id
                              ,p.test_script_id
                              ,p.parameter_name
                              ,p.parameter_value
                              ,p.parameter_note
                              ,p.last_updated)
                       values (dataprov.dp_test_params_seq.nextval
                               ,b.v_script_id
                               ,b.v_param_name
                               ,b.v_param_value
                               ,b.v_param_note
                               ,b.v_last_updated
                                );
if sql%rowcount < 1 then
raise_application_error(-20002,'Test SCRIPT supplied is not in table. Check dataprov.v_dp_test_lookup for correct script_name or add it first if new');
end if;
commit;
exception when no_data_found then
raise_application_error(-20002,'Test SCRIPT supplied is not in table. Check dataprov.v_dp_test_lookup for correct script_name');
end;

procedure add_new_test_script(v_test_script_name in varchar2
                              )
is
begin
insert into dataprov.dp_test_scripts ts
(id
,test_script_name
,used)
values
(dataprov.dp_test_script_seq.nextval
,upper(v_test_script_name)
,upper('Y'));
commit;
end;


procedure add_new_test(v_test_name in varchar2
                        ,v_test_script_name in varchar2
                          )
is
begin
insert into dataprov.dp_test_names tn
(id
,test_name
,test_script_id
,used)
select
dataprov.dp_test_names_seq.nextval
,upper(v_test_name)
,ts.id
,upper('Y')
from dataprov.dp_test_scripts ts
where upper(ts.test_script_name)=upper(v_test_script_name);
if sql%rowcount < 1 then
raise_application_error(-20002,'Test SCRIPT supplied is not in table. Check dataprov.v_dp_test_lookup for correct script_name or add it first if new');
end if;
commit;
exception when no_data_found then
raise_application_error(-20002,'Test SCRIPT supplied is not in table. Check dataprov.v_dp_test_lookup for correct script_name or add it first if new');
end;

procedure add_new_test_type(v_test_type_name in varchar2
                           ,v_test_type_notes in varchar2
                          )
is
begin
insert into dataprov.dp_test_types tp
(id
,test_type_name
,test_type_notes)
values
(dataprov.dp_test_types_seq.nextval
,upper(v_test_type_name)
,upper(v_test_type_notes));
commit;
end;


procedure add_new_test_vols(v_test_name_id in number
                             ,v_test_type_id in number
                             ,v_hourly_vol in number
                          )
is
begin
insert into dataprov.dp_test_vols tv
(id
,test_name_id
,test_type_id
,hourly_volume)
values
(dataprov.dp_test_vols_seq.nextval
,v_test_name_id
,v_test_type_id
,v_hourly_vol);
commit;
end;

procedure modify_test_vols(v_test_name_id in number
                             ,v_test_type_id in number
                             ,v_hourly_vol in number
                          )
is
begin
update dataprov.dp_test_vols tv set
hourly_volume=v_hourly_vol
,last_updated=sysdate
where test_name_id=v_test_name_id
and test_type_id=v_test_type_id;
commit;
exception when no_data_found then
raise_application_error(-20002,'Test name or ID supplied is not in table. Check dataprov.dp_test_names and dataprov.dp_test_types for correct IDs or add it first if new');
end;


procedure get_test_script_id (v_test_script_name in varchar2
                             ,v_test_script_id out number
                               )
is
begin
select
id into v_test_script_id
from dataprov.dp_test_scripts t
where UPPER(t.test_script_name) = UPPER(v_test_script_name);
end;


end data_prep_framework;
/
