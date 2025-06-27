--------------------------------------------------------
--  File created - Thursday-November-09-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Procedure HPSVBBREMAPPER
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "DATAPROV"."HPSVBBREMAPPER" (g_chdid IN varchar2, g_message OUT varchar2) is

l_oldCombinedTelephoneNumber  varchar2(47);
l_newCombinedTelephoneNumber  varchar2(47);
l_oldTelephoneNumber          varchar2(47);
l_newTelephoneNumber          varchar2(47);
l_oldTelephoneId              varchar2(47);
l_newTelephoneId              varchar2(47);
l_oldTelephoneUsageRoleId     varchar2(47);
l_newTelephoneUsageRoleId     varchar2(47);
l_stdCode                     varchar2(47);
l_bsbtelephoneKey             varchar2(47);
l_servicenumber               varchar2(47);
l_message                     varchar2(1000);

begin
-- Get the customers active BB service ID from their chordiant customer ID (will fail if the have multiple BB subscriptions but we really don't care in that case
select servicenumber into l_servicenumber from
(
select bbcp.servicenumber as servicenumber
  from ccsowner.bsbbillingaccount           bba,
       ccsowner.bsbportfolioproduct         bpp,
       ccsowner.bsbserviceinstance          bsi,
       CCSOWNER.BSBCUSTOMERPRODUCTELEMENT   bcp,
       ccsowner.bsbcustomerrole             bcr,
       ccsowner.bsbpartyrole                bpr,
       ccsowner.bsbcontactor                bc,
       ccsowner.bsbcontactaddress           bca,
       ccsowner.bsbaddress                  ba,
       ccsowner.bsbbroadbandcustprodelement bbcp,
       ccsowner.BSBTELEPHONYCUSTPRODELEMENT btcp
where bba.portfolioid = bpp.portfolioid
   and bpp.serviceinstanceid = bsi.id
   and bpp.id = bcp.portfolioproductid
   and bcp.id = bbcp.lineproductelementid(+)
   and bcp.id = btcp.telephonyproductelementid(+)
   and bba.portfolioid = bcr.portfolioid
   and bcr.partyroleid = bpr.id
   and bpr.partyid = bc.partyid
   and bc.id = bca.contactorid
   and bca.addressid = ba.id
   and bsi.serviceinstancetype = 400
   and BCA.Effectivetodate is null
   and bpp.status = 'AC'
   and bbcp.servicenumber is not null
   and bba.accountnumber = g_chdid
);

-- Get the telephone usage role id for the BB service - need this to re-map later
select max(TELEPHONEUSAGEROLEID) into l_oldTelephoneUsageRoleId from CCSOWNER.BSBBROADBANDCUSTPRODELEMENT where SERVICENUMBER = l_servicenumber;

-- Get the customers phone details based on the current BB service to phone mapping
select AREACODE, TELEPHONENUMBER,COMBINEDTELEPHONENUMBER, ID into l_stdCode, l_oldTelephoneNumber, l_oldCombinedTelephoneNumber, l_oldTelephoneId from (
select * from CCSOWNER.BSBTELEPHONE where ID =
(select TELEPHONEID from CCSOWNER.BSBTELEPHONEUSAGEROLE where ID = l_oldTelephoneUsageRoleId));

-- Remap the phone numbers
l_newTelephoneNumber := l_oldTelephoneNumber;
-- The mappings for magic number changes
l_newTelephoneNumber := regexp_replace(l_newTelephoneNumber,'650$','653');
l_newTelephoneNumber := regexp_replace(l_newTelephoneNumber,'651$','652');
l_newTelephoneNumber := regexp_replace(l_newTelephoneNumber,'655$','657');
l_newTelephoneNumber := regexp_replace(l_newTelephoneNumber,'656$','658');

-- Create the new compined telephone number
l_newCombinedTelephonenumber := l_stdCode || l_newTelephoneNumber;
-- Create a new unique telephone id for bsbtelephone
l_newTelephoneId := sys_guid();

-- Insert a new bsbtelephone row for the re-mapped number
insert into ccsowner.bsbtelephone (ID,
INTERNATIONALDIALINGCODE,
AREACODE,
TELEPHONENUMBER,
EXTENSIONNUMBER,
TELEPHONENUMBERUSECODE,
CREATED,
COMBINEDTELEPHONENUMBER,
CREATEDBY,
LASTUPDATE,
UPDATEDBY,
LINETYPECODE,
LOCKTOKENTEXT,
TELEPHONENUMBERSTATUS) values
(l_newTelephoneId,'+44',l_stdCode,l_newTelephoneNumber,NULL,'V',sysdate,l_newCombinedTelephoneNumber,'hpsv',sysdate,'hpsv','STD','1','VALID');

-- Mark the current telephone usage role as no longer active
update ccsowner.bsbtelephoneusagerole set effectivetodate = sysdate, updatedby = 'hpsva', lastupdate = sysdate where telephoneid = l_oldTelephoneId;

-- Link the new telephone number to the telephone usage role
insert into ccsowner.bsbtelephoneusagerole (
ID,
TELEPHONEID,
SERVICEINSTANCEID,
EFFECTIVEFROMDATE,
EFFECTIVETODATE,
CREATED,
CREATEDBY,
LASTUPDATE,
UPDATEDBY,
ROLETYPE,
LOCKTOKENTEXT) values  (sys_guid(),l_newTelephoneId,NULL,sysdate,NULL,sysdate,'hpsv',sysdate,'hpsv','BB','1');

-- Get the new telephone usage role from the insert above
select id into l_newTelephoneUsageRoleId from (select * from ccsowner.bsbtelephoneusagerole where telephoneid = l_newTelephoneId);

-- Link that role to the customers broadband product element
update ccsowner.bsbbroadbandcustprodelement set TELEPHONEUSAGEROLEID = l_newTelephoneUsageRoleId where TELEPHONEUSAGEROLEID = l_oldTelephoneUsageRoleId;

l_message := l_message || 'STD code is ' || l_stdCode;
l_message := l_message || '__NL__Service number ' || l_servicenumber;
l_message := l_message || '__NL__Old telephone number is ' || l_oldTelephoneNumber;
l_message := l_message || '__NL__Old combined telephone number is ' || l_oldCombinedTelephoneNumber;
l_message := l_message || '__NL__Old telephone id is ' || l_oldTelephoneId;
l_message := l_message || '__NL__Old telephone usage role id is   ' || l_oldTelephoneUsageRoleId;
l_message := l_message || '__NL__New telephone number is ' || l_newTelephoneNumber;
l_message := l_message || '__NL__New combined telephone number is ' || l_newCombinedTelephoneNumber;
l_message := l_message || '__NL__New telephone id is ' || l_newTelephoneId;
l_message := l_message || '__NL__New telephone usage role id is   ' || l_newTelephoneUsageRoleId;

g_message := l_message;

end;

/

  GRANT EXECUTE ON "DATAPROV"."HPSVBBREMAPPER" TO "DATAPROV_READONLY";
  GRANT EXECUTE ON "DATAPROV"."HPSVBBREMAPPER" TO "BATCHPROCESS_USER";
