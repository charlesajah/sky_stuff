spool NFT_pwd_reset.log append

set head off feed off
select name||' - Reset passwords at : ' || to_char(sysdate,'dd/mm/yyyy hh24:mi:ss') from v$database;
set head on 
 
alter USER dataprov profile default ;
alter USER dataprov identified by "H6_vjZDKtrAmkj" ;
alter USER dataprov  profile SECURE_USER_NO_PW_EXPIRY ;
prompt dataprov done

alter USER dataprov_readonly profile default ; 
alter USER dataprov_readonly identified by "u4QDfF07_3xs#" ;
alter USER dataprov_readonly  profile SECURE_USER_NO_PW_EXPIRY ;
prompt dataprov_readonly done

alter USER hp_diag profile default ;
alter USER hp_diag identified by "HP_DIAG_1234" ; 
alter USER hp_diag profile SECURE_USER_NO_PW_EXPIRY ;
prompt hp_diag done

alter USER horus_monitoring profile default ;
alter USER horus_monitoring IDENTIFIED BY VALUES 'S:1107C8710FF0A6AFA4B8654EF4FC75CD1A92CAEB5787A9457154EA616A02;T:26AD2AB3117E1CD95ACEDF449906A58F65BBB6FE248FCAD6C6DEB3C032CD4B6F03D3F166533B8F31C3569E38B0C695E7CF42FDFDDE651F1948D98A4E400F0F03B83ADE66B0FD4C87C7F2149D372299DF;C8CEEAB976CB41D3' ;
alter USER horus_monitoring profile SECURE_USER_NO_PW_EXPIRY ;
prompt horus_monitoring done

spool off
exit;

