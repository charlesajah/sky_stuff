--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure EOCN_DATAFIX
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."EOCN_DATAFIX" ( i_days integer) is
  strTmp       varchar2(100);
  strTechCode  varchar2(10); 
  intCnt       integer;
begin
  -- main loop to get accounts to process
  for c1_rec in (select accountnumber, created, status 
                   from ccsowner.bsbcontractnotification
				  where created > sysdate-i_days
				    and status = 'NIP')
  loop
    begin
      -- get tech code
	  select bsub.technologycode
	    into strTechCode
        from ccsowner.bsbbillingaccount bba, ccsowner.bsbportfolioproduct bpp, ccsowner.bsbsubscription bsub
       where bba.portfolioid = bpp.portfolioid
	      and bpp.subscriptionid = bsub.id
		  and bpp.catalogueproductid = '12721'
		  and bsub.technologycode is not null
		  and bsub.status = 'A'
		  and bba.accountnumber = c1_rec.accountnumber; 

      -- check if numbers are already set to 718 or 701
      select /*+ parallel(8) */ count(*)
	    into intCnt
	    from ccsowner.bsbtelephone bt, ccsowner.bsbcontacttelephone bct, 
	         ccsowner.bsbcontactor bc, ccsowner.bsbpartyrole bpr,
		     ccsowner.bsbcustomerrole bcr, ccsowner.bsbbillingaccount ba
       where bt.id = bct.telephoneid
	     and bc.id = bct.contactorid
	     and bpr.partyid = bc.partyid
	     and bcr.partyroleid = bpr.id
	     and ba.portfolioid = bcr.portfolioid
	     and bct.typeCode = 'H'
	     and substr(bt.combinedtelephonenumber, 9,3) in ('701','718')
	     and bct.effectivetodate is null
	     and length(bt.combinedtelephonenumber) = 11
	     and ba.accountnumber = c1_rec.accountnumber;

      if intCnt = 0 then
        if strTechCode = 'MPF' then
          dataprov.dynamic_data_pkg.update_cust_telno(c1_rec.accountnumber, '718', strTmp);
        else
          dataprov.dynamic_data_pkg.update_cust_telno(c1_rec.accountnumber, '701', strTmp);
        end if;
      end if ;	
      
	exception
	  when NO_DATA_FOUND then
	    null;
      when others then  
        null;
	end ;
  end loop ;
end;

/

  GRANT EXECUTE ON "DATAPROV"."EOCN_DATAFIX" TO "BATCHPROCESS_USER";
