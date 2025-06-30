set markup csv on
set numwidth 100
spool skyq.csv
SELECT /*+ parallel(8) */ bpr.partyid,adr.uprn,adr.id,'update ccsowner.bsbaddress set uprn='||adr.uprn||' where id='''||adr.id||''';' cmd,'update ccsowner.bsbaddress set uprn=72237879 where id='''||adr.id||''';' updt
  FROM ccsowner.bsbcontactaddress   bct,
       ccsowner.bsbcontactor        bc,
       ccsowner.bsbpartyrole        bpr,
       ccsowner.bsbcustomerrole     bcr,
       ccsowner.bsbbillingaccount   ba,
       ccsowner.bsbaddress          adr,
       dataprov.customers           cus,
       ccsowner.bsbServiceInstance si
       --dataprov.customertokens ctk
 WHERE bc.id = bct.contactorid
   AND bpr.partyid = bc.partyid
   AND bcr.partyroleid = bpr.id
   AND ba.portfolioid = bcr.portfolioid 
   --AND ba.portfolioId = bpp.portfolioId
   AND bct.addressid=adr.id
   AND si.parentServiceInstanceId = ba.serviceInstanceId
   and adr.uprn is not null
   and adr.uprn !=72237879
   --and bpp.id=bcpe.portfolioproductid
   --and bcpe.status != 'R'
   AND bpr.partyid = cus.partyid 
   and cus.mobile=0
   and cus.bband=0
   and cus.talk=0
   and cus.dtv=1
   and cus.skyQBox=1
   and cus.skyHDBox=0
   and cus.accountnumber2 is null
   and cus.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )
   and bpr.partyid NOT in( SELECT bpr.partyId               --exclude SOIP customers
                             FROM ccsowner.bsbBillingAccount ba
                             JOIN ccsowner.bsbCustomerRole bcr ON ba.portfolioId = bcr.portfolioId
                             JOIN ccsowner.bsbPartyRole bpr ON bcr.partyroleId = bpr.id
                             JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
                             JOIN rcrm.product p ON s.id = p.serviceId
                             WHERE s.serviceType = 'SOIP')
and bpr.partyid  IN (  ----- remove accounts that have no UK mobile contact number.
          SELECT /*+ PARALLEL(10) */ bc.partyId
            FROM ccsowner.bsbContactor bc
            JOIN ccsowner.bsbContactTelephone bct ON bct.contactorId = bc.id
            JOIN ccsowner.bsbTelephone bt ON bt.id = bct.telephoneId
           WHERE bct.deletedFlag = 0
             AND SYSDATE BETWEEN bct.effectiveFromDate AND NVL ( bct.effectiveToDate , SYSDATE + 1 )
             AND bt.telephoneNumberStatus = 'VALID'
             AND bct.mobileNumberConfirmedDate < SYSDATE
             AND bt.combinedTelephoneNumber LIKE '07%'
             AND LENGTH ( bt.combinedTelephoneNumber ) = 11
             AND bt.internationalDialingCode = '+44'
             AND bct.primaryFlag = 1
             AND bct.typecode='M'   --must have mobile phone contact with type=M
          )
    and cus.emailAddress NOT LIKE 'noemail%'  --  must have a primary email address.
    --and cus.accountNumber not in (select accountnumber from dataprov.BBAND_TMP) --filter out any accounts that have ever had bband or Talk active or not
    and bpr.partyid not in (select partyid from dataprov.MULT_UPRN_Q)  --filter out any partyid with multiple UPRN/addresses
    --and ctk.accountnumber = ba.accountnumber
 group by bpr.partyid,adr.uprn,adr.id
               ORDER BY dbms_random.value
               fetch first 10000 rows only;

spool off;