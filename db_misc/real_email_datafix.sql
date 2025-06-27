set termout off
set pagesize 0
set spool on
set head off
set timing on

SPOOL email_fixes.txt

select /*+ parallel(be,4) */ 'Before Count : ' || count(*)
  from ccsowner.bsbemail be
 where (     TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%bskyb.com'
         AND TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%sky.ie'
         AND TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%sky.uk'
         AND TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%skygroup.com'
       );

update /*+ parallel(be,4) */ ccsowner.bsbemail be
set be.emailaddress = to_char(systimestamp,'ddmmyyyy_hh24miss_ff4') || '@sky.uk'
where (      TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%bskyb.com'
         AND TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%sky.ie'
         AND TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%sky.uk'
         AND TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%skygroup.com'
       );
	
select /*+ parallel(be,4) */ 'After Count : ' || count(*)
  from ccsowner.bsbemail be
 where (     TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%bskyb.com'
         AND TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%sky.ie'
         AND TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%sky.uk'
         AND TRIM ( LOWER ( be.emailAddress ) ) NOT LIKE '%skygroup.com'
       );
commit;

alter session force parallel dml;
set timing on

UPDATE /*+ parallel(32) */ ccsOwner.bsbAddress t 
SET t.houseNumber  = CASE WHEN t.houseNumber  IS NOT NULL THEN dbms_random.string ( opt => 'a' , len => 4 ) END, 
    t.houseName    = CASE WHEN t.houseName    IS NOT NULL THEN dbms_random.string ( opt => 'L' , len => 5 ) END, 
	t.street       = CASE WHEN t.street       IS NOT NULL THEN dbms_random.string ( opt => 'a' , len => 15 ) END, 
	t.town         = CASE WHEN t.town         IS NOT NULL THEN dbms_random.string ( opt => 'u' , len => 11 ) END, 
	t.locality     = CASE WHEN t.locality     IS NOT NULL THEN dbms_random.string ( opt => 'a' , len => 6 ) END, 
	t.county       = CASE WHEN t.county       IS NOT NULL THEN dbms_random.string ( opt => 'L' , len => 8 ) END, 
	t.AddressLine1 = CASE WHEN t.AddressLine1 IS NOT NULL THEN dbms_random.string ( opt => 'L' , len => 18 ) END, 
	t.AddressLine2 = CASE WHEN t.AddressLine2 IS NOT NULL THEN dbms_random.string ( opt => 'L' , len => 10 ) END, 
	t.AddressLine3 = CASE WHEN t.AddressLine3 IS NOT NULL THEN dbms_random.string ( opt => 'L' , len => 6 ) END, 
	t.AddressLine4 = CASE WHEN t.AddressLine4 IS NOT NULL THEN dbms_random.string ( opt => 'L' , len => 6 ) END, 
	t.subBuildingName   = CASE WHEN t.subBuildingName   IS NOT NULL THEN dbms_random.string ( opt => 'a' , len => 4 ) END, 
	t.subBuildingNumber = CASE WHEN t.subBuildingNumber IS NOT NULL THEN dbms_random.string ( opt => 'a' , len => 4 ) END
where created > trunc(sysdate)-7;
commit;

UPDATE /*+ parallel(32) */ ccsOwner.bsbNewAddressNotification t 
   SET t.scmsAddressLine1 = CASE WHEN t.scmsAddressLine1 IS NOT NULL THEN dbms_random.string ( opt => 'a' , len => 17 ) END,
       t.scmsAddressLine2 = CASE WHEN t.scmsAddressLine2 IS NOT NULL THEN dbms_random.string ( opt => 'a' , len => 11 ) END, 
       t.scmsAddressLine3 = CASE WHEN t.scmsAddressLine3 IS NOT NULL THEN dbms_random.string ( opt => 'a' , len => 6 ) END,
       t.scmsAddressLine4 = CASE WHEN t.scmsAddressLine4 IS NOT NULL THEN dbms_random.string ( opt => 'a' , len => 3 ) END, 
       t.scmsAddressLine5 = CASE WHEN t.scmsAddressLine5 IS NOT NULL THEN dbms_random.string ( opt => 'a' , len => 2 ) END,
       t.bsbHouseNumber = CASE WHEN t.bsbHouseNumber IS NOT NULL THEN dbms_random.string ( opt => 'a' , len => 3 ) END, 
       t.bsbHouseName = CASE WHEN t.bsbHouseName IS NOT NULL THEN dbms_random.string ( opt => 'u' , len => 5 ) END,
       t.bsbStreet = CASE WHEN t.bsbStreet IS NOT NULL THEN dbms_random.string ( opt => 'a' , len => 15 ) END, 
       t.bsbTown = CASE WHEN t.bsbTown IS NOT NULL THEN dbms_random.string ( opt => 'L' , len => 11 ) END, 
       t.bsbLocality = CASE WHEN t.bsbLocality IS NOT NULL THEN dbms_random.string ( opt => 'L' , len => 8 ) END, 
       t.bsbCounty = CASE WHEN t.bsbCounty IS NOT NULL THEN dbms_random.string ( opt => 'L' , len => 5 ) END, 
       t.bsbAddressLine1 = CASE WHEN t.bsbAddressLine1 IS NOT NULL THEN dbms_random.string ( opt => 'L' , len => 17 ) END, 
       t.bsbAddressLine2 = CASE WHEN t.bsbAddressLine2 IS NOT NULL THEN dbms_random.string ( opt => 'L' , len => 8 ) END,
       t.bsbAddressLine3 = CASE WHEN t.bsbAddressLine3 IS NOT NULL THEN dbms_random.string ( opt => 'L' , len => 5 ) END, 
       t.bsbAddressLine4 = CASE WHEN t.bsbAddressLine4 IS NOT NULL THEN dbms_random.string ( opt => 'L' , len => 5 ) END,
       t.bsbSubBuildingName = CASE WHEN t.bsbSubBuildingName IS NOT NULL THEN dbms_random.string ( opt => 'a' , len => 4 ) END, 
       t.bsbSubBuildingNumber = CASE WHEN t.bsbSubBuildingNumber IS NOT NULL THEN dbms_random.string ( opt => 'a' , len => 3 ) END
where created > trunc(sysdate)-7;
commit;

UPDATE /*+ parallel(32) */ ccsOwner.bsbpaperagreement t 
   SET t.initials = CASE WHEN t.initials IS NOT NULL THEN dbms_random.string ( opt => 'U' , len => 3 ) END, 
   t.HouseNumber = CASE WHEN t.HouseNumber IS NOT NULL THEN dbms_random.string ( opt => 'a' , len => 4 ) END, 
   t.HouseName = CASE WHEN t.HouseName IS NOT NULL THEN dbms_random.string ( opt => 'u' , len => 8 ) END, 
   t.Street = CASE WHEN t.Street IS NOT NULL THEN dbms_random.string ( opt => 'a' , len => 18 ) END, 
   t.Town = CASE WHEN t.Town IS NOT NULL THEN dbms_random.string ( opt => 'L' , len => 10 ) END, 
   t.Locality = CASE WHEN t.Locality IS NOT NULL THEN dbms_random.string ( opt => 'L' , len => 6 ) END, 
   t.County = CASE WHEN t.County IS NOT NULL THEN dbms_random.string ( opt => 'L' , len => 8 ) END, 
   t.hometelephone = CASE WHEN t.hometelephone like '07%' THEN '076' || to_char(trunc(dbms_random.value(10000, 99999))) || '000' ELSE to_char(trunc(dbms_random.value(80000000, 99999999))) || '000' END, 
   t.worktelephone = CASE WHEN t.worktelephone like '07%' THEN '076' || to_char(trunc(dbms_random.value(10000, 99999))) || '000' ELSE to_char(trunc(dbms_random.value(80000000, 99999999))) || '000' END
where created > trunc(sysdate)-7;
commit;

UPDATE /*+ parallel(32) */ ccsOwner.bsbpaperagrerrors t 
   SET t.initials = CASE WHEN t.initials IS NOT NULL THEN dbms_random.string ( opt => 'U' , len => 3 ) END, 
   t.HouseNumber = CASE WHEN t.HouseNumber IS NOT NULL THEN dbms_random.string ( opt => 'a' , len => 3 ) END, 
   t.HouseName = CASE WHEN t.HouseName IS NOT NULL THEN dbms_random.string ( opt => 'u' , len => 8 ) END,
   t.Street = CASE WHEN t.Street IS NOT NULL THEN dbms_random.string ( opt => 'a' , len => 16 ) END,
   t.Town = CASE WHEN t.Town IS NOT NULL THEN dbms_random.string ( opt => 'L' , len => 10 ) END, 
   t.Locality = CASE WHEN t.Locality IS NOT NULL THEN dbms_random.string ( opt => 'L' , len => 7 ) END, 
   t.County = CASE WHEN t.County IS NOT NULL THEN dbms_random.string ( opt => 'L' , len => 8 ) END
where created > trunc(sysdate)-7;
commit;

spool off
exit
