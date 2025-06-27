--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure DTV_SERIAL_INSERTS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."DTV_SERIAL_INSERTS" 
     (p_ordernum in number,
                  p_mftnum  in varchar2,
                  p_partnum  in varchar2,
                  p_snvolume  in number,
                  TotalIns out number) as
begin
insert into dtv_serial_number (serial_no,mft_no,order_no,man_serial_no,sup_part_no)
select
trim(to_char(dataprov.dtvseq.nextval,'0XXXXXXXXX'))
,p_mftnum
,p_ordernum
,DATAPROV.msseq.nextval
,p_partnum
from dual
connect by level <= p_snvolume;
TotalIns := sql%rowcount;
end dtv_serial_inserts;

/

  GRANT EXECUTE ON "DATAPROV"."DTV_SERIAL_INSERTS" TO "BATCHPROCESS_USER";
