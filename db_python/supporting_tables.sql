--create the four supporting tables
CREATE TABLE "DATAPROV"."BBAND_TMP" 
   (	"ACCOUNTNUMBER" VARCHAR2(12), 
	"SERVICEINSTANCETYPE" NUMBER(5,0)
   );

CREATE TABLE "DATAPROV"."MULT_UPRN_M" 
   (	"UPRN_COUNT" NUMBER, 
	"PARTYID" VARCHAR2(47) NOT NULL ENABLE
   );

CREATE TABLE "DATAPROV"."MULT_UPRN_Q" 
   (	"UPRN_COUNT" NUMBER, 
	"PARTYID" VARCHAR2(47) NOT NULL ENABLE
   );

CREATE TABLE "DATAPROV"."MULT_UPRN_SKYHD" 
    (	"UPRN_COUNT" NUMBER, 
        "PARTYID" VARCHAR2(47) NOT NULL ENABLE
    );


--first we truncate the table bband_tmp
truncate table dataprov.bband_tmp;


--we populate the table
insert into dataprov.bband_tmp
    select ba.accountnumber,serviceInstanceType from  ccsowner.bsbBillingAccount ba join ccsowner.bsbServiceInstance si
    ON si.parentServiceInstanceId = ba.serviceInstanceId
    and serviceInstanceType  in (400,100);

commit;

--we truncate the remaining supporting tables
truncate table dataprov.MULT_UPRN_Q;
truncate table dataprov.MULT_UPRN_M;
truncate table dataprov.MULT_UPRN_SKYHD;


--we populate the tables
insert into dataprov.MULT_UPRN_Q
    SELECT count(*) uprn_count,bpr.partyid
FROM ccsowner.bsbcontactaddress   bct,
       ccsowner.bsbcontactor        bc,
       ccsowner.bsbpartyrole        bpr,
       ccsowner.bsbcustomerrole     bcr,
       ccsowner.bsbbillingaccount   ba,
       ccsowner.bsbaddress          adr,
       dataprov.customers           cus,
       ccsowner.bsbServiceInstance si
 WHERE bc.id = bct.contactorid
   AND bpr.partyid = bc.partyid
   AND bcr.partyroleid = bpr.id
   AND ba.portfolioid = bcr.portfolioid
   --AND ba.portfolioId = bpp.portfolioId
   AND bct.addressid=adr.id
   AND si.parentServiceInstanceId = ba.serviceInstanceId
   AND adr.uprn is not null
   AND bpr.partyid = cus.partyid
   and cus.mobile=0
   and cus.bband=0
   and cus.talk=0
   and cus.dtv=1
   and cus.skyQBox=1
   and cus.skyHDBox=0
   and cus.accountnumber2 is null
   and cus.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )
   and bpr.partyid NOT in( SELECT bpr.partyId
                             FROM ccsowner.bsbBillingAccount ba
                             JOIN ccsowner.bsbCustomerRole bcr ON ba.portfolioId = bcr.portfolioId
                             JOIN ccsowner.bsbPartyRole bpr ON bcr.partyroleId = bpr.id
                             JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
                             JOIN rcrm.product p ON s.id = p.serviceId
                             WHERE s.serviceType = 'SOIP')
    and cus.accountNumber not in (select accountnumber from dataprov.BBAND_TMP) --filter out any accounts that have ever had bband or Talk whether active or not
    group by bpr.partyid
    having count(*) > 1;

commit;
 
insert into dataprov.mult_uprn_m
SELECT count(*) uprn_count,bpr.partyid
FROM ccsowner.bsbcontactaddress   bct,
       ccsowner.bsbcontactor        bc,
       ccsowner.bsbpartyrole        bpr,
       ccsowner.bsbcustomerrole     bcr,
       ccsowner.bsbbillingaccount   ba,
       ccsowner.bsbaddress          adr,
       dataprov.customers           cus,
       ccsowner.bsbServiceInstance si
 WHERE bc.id = bct.contactorid
   AND bpr.partyid = bc.partyid
   AND bcr.partyroleid = bpr.id
   AND ba.portfolioid = bcr.portfolioid
   --AND ba.portfolioId = bpp.portfolioId
   AND bct.addressid=adr.id
   AND si.parentServiceInstanceId = ba.serviceInstanceId
   AND adr.uprn is not null
   AND bpr.partyid = cus.partyid
   and cus.mobile=1
   and cus.bband=0
   and cus.talk=0
   and cus.dtv=0
   and cus.skyQBox=0
   and cus.skyHDBox=0
   and cus.accountnumber2 is null
   and cus.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )
   and bpr.partyid NOT in( SELECT bpr.partyId
                             FROM ccsowner.bsbBillingAccount ba
                             JOIN ccsowner.bsbCustomerRole bcr ON ba.portfolioId = bcr.portfolioId
                             JOIN ccsowner.bsbPartyRole bpr ON bcr.partyroleId = bpr.id
                             JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
                             JOIN rcrm.product p ON s.id = p.serviceId
                             WHERE s.serviceType = 'SOIP')
    and cus.accountNumber not in (select accountnumber from dataprov.BBAND_TMP) --filter out any accounts that have ever had bband or Talk whether active or not
    group by bpr.partyid
    having count(*) > 1;

commit;
 
insert into dataprov.MULT_UPRN_SKYHD
SELECT count(*) uprn_count,bpr.partyid
FROM ccsowner.bsbcontactaddress   bct,
       ccsowner.bsbcontactor        bc,
       ccsowner.bsbpartyrole        bpr,
       ccsowner.bsbcustomerrole     bcr,
       ccsowner.bsbbillingaccount   ba,
       ccsowner.bsbaddress          adr,
       dataprov.customers           cus,
       ccsowner.bsbServiceInstance si
 WHERE bc.id = bct.contactorid
   AND bpr.partyid = bc.partyid
   AND bcr.partyroleid = bpr.id
   AND ba.portfolioid = bcr.portfolioid
   --AND ba.portfolioId = bpp.portfolioId
   AND bct.addressid=adr.id
   AND si.parentServiceInstanceId = ba.serviceInstanceId
   AND adr.uprn is not null
   AND bpr.partyid = cus.partyid
   and cus.mobile=0
   and cus.bband=0
   and cus.talk=0
   and cus.dtv=1
   and cus.skyQBox=0
   and cus.skyHDBox=1
   and cus.accountnumber2 is null
   and cus.accountNumber IN ( SELECT da.accountNumber FROM dataprov.debt_amount da WHERE da.balance <= 0 )
   and bpr.partyid NOT in( SELECT bpr.partyId
                             FROM ccsowner.bsbBillingAccount ba
                             JOIN ccsowner.bsbCustomerRole bcr ON ba.portfolioId = bcr.portfolioId
                             JOIN ccsowner.bsbPartyRole bpr ON bcr.partyroleId = bpr.id
                             JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
                             JOIN rcrm.product p ON s.id = p.serviceId
                             WHERE s.serviceType = 'SOIP')
    and cus.accountNumber not in (select accountnumber from dataprov.BBAND_TMP) --filter out any accounts that have ever had bband or Talk whether active or not
    group by bpr.partyid
    having count(*) > 1;

commit;
