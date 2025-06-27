create or replace PACKAGE retail_agent_creation AS
/*
Purpose: Creates test-script retail agents and assigns them agentRoles.
   Creates 6,001 retail agents, numbered 800000 .. 806000, named AGENT800000 .. AGENT806000.
   That number of retail agents is likely overkill, but shouldn't do any harm having unused retail agents, and should stop testers submitting ad-hoc requests to create more.
Usage: One public procedure only, no parameters: "exec retail_agent_creation.addRetailUsers ;"
   Is called by refresh_pkg so will run automatically at every environment refresh.
   Can be re-run multiple times without doing any harm, won't create duplicate agents or agentRoles if they have already been created, only adds the missing ones.
Privileges: It needs one extra grant to compile:
      GRANT EXECUTE ON cbsServices.dal_userRoles_api TO dataprov ;
   That grant has been added into the two .bash ptt scriptsm so will be re-granted after every environment refresh.
   But risk that this package could go invalid after a database build deployment, depending on what/how the database build is doing, until above privilege is re-granted.
Useful URLs:
   https://ssp.bskyb.com/retailerData/retailAgentViewer
   https://ssp.bskyb.com/retailerData/retailerViewer
   https://confluence.bskyb.com/display/nonfuntst/Create+Retail+Users
   https://confluence.bskyb.com/display/nonfuntst/Retail+User+Maintenance
*/
PROCEDURE addRetailUsers ;
PROCEDURE addAgent ( i_agentId IN VARCHAR2 ) ;
END retail_agent_creation ;
/


create or replace PACKAGE BODY retail_agent_creation AS
/*
Purpose: Creates test-script retail agents and assigns them agentRoles.
   Creates 6,001 retail agents, numbered 800000 .. 806000, named AGENT800000 .. AGENT806000.
   That number of retail agents is likely overkill, but shouldn't do any harm having unused retail agents, and should stop testers submitting ad-hoc requests to create more.
Usage: One public procedure only, no parameters: "exec retail_agent_creation.addRetailUsers ;"
   Is called by refresh_pkg so will run automatically at every environment refresh.
   Can be re-run multiple times without doing any harm, won't create duplicate agents or agentRoles if they have already been created, only adds the missing ones.
Privileges: It needs one extra grant to compile:
      GRANT EXECUTE ON cbsServices.dal_userRoles_api TO dataprov ;
   That grant has been added into the two .bash ptt scriptsm so will be re-granted after every environment refresh.
   But risk that this package could go invalid after a database build deployment, depending on what/how the database build is doing, until above privilege is re-granted.
Useful URLs:
   https://ssp.bskyb.com/retailerData/retailAgentViewer
   https://ssp.bskyb.com/retailerData/retailerViewer
   https://confluence.bskyb.com/display/nonfuntst/Create+Retail+Users
   https://confluence.bskyb.com/display/nonfuntst/Retail+User+Maintenance
*/

PROCEDURE addAgentRole ( i_roleName IN VARCHAR2 , i_agentId IN VARCHAR2 ) IS
   l_count NUMBER ;
BEGIN
   SELECT COUNT(*) INTO l_count
     FROM ccsowner.bsbAgentRole ar
     JOIN ccsowner.bsbAgentRoleBridge arb ON ar.userId = arb.userId
     JOIN refdatamgr.bsbrole r ON arb.roleId = r.id
    WHERE ar.userId = i_agentId
      AND r.role = i_roleName
      AND r.rdmDeletedFlag = 'N'
      AND ar.deletedFlag = 0
   ;
   IF l_count = 0
   THEN
      cbsServices.dal_userRoles_api.addAgentRole ( i_roleName => i_roleName , i_userId => i_agentId , i_updatedBy => USER ) ;
   END IF ;
END addAgentRole ;

PROCEDURE addAgent ( i_agentId IN VARCHAR2 ) IS
   l_asaGroup VARCHAR2(5) := '11142' ;
   l_asaBranch VARCHAR2(4) := '2452' ;
   l_sellAllBranches number(1) := 0 ;
   l_partyId VARCHAR2(47) := 'AGENT' || i_agentId ;
   l_homeRelationId VARCHAR2(47) := 'AGENT' || i_agentId || '-' || l_asaGroup || l_asaBranch ;
   l_saleRelationId VARCHAR2(47) := 'AGENT' || i_agentId || '-S-' || l_asaGroup || l_asaBranch ;
   l_retailerPartyRole VARCHAR2(47) ;
BEGIN
   SELECT pr.id INTO l_retailerPartyRole  -- '5327138'
     FROM ccsowner.bsbRetailBusiness rb
     JOIN ccsowner.bsbPartyRole pr ON rb.partyId = pr.partyId
    WHERE rb.asaGroupNumber = l_asaGroup   -- '11142'
      AND rb.asaBranchNumber = l_asaBranch  -- '2452'
   ;
   -- Activate that retailBusiness if it is not already activated
   UPDATE ccsowner.bsbRetailBusiness t
      SET t.agentMigrated = 1 , t.retailerStatusCode = 'A'
    WHERE ( t.agentMigrated != 1 OR t.retailerStatusCode != 'A' )  -- both columns are NOT NULL
      AND t.id = l_retailerPartyRole
   ;
   MERGE INTO ccsowner.party t USING (
      SELECT l_partyId AS id FROM DUAL
   ) s ON ( s.id = t.id )
   WHEN NOT MATCHED THEN INSERT (  t.id , t.partyTypeCode , t.locktokentext , t.created , t.createdBy , t.lastupdate , t.updatedby )
   VALUES ( l_partyId , 'PERSON' , '1' , SYSDATE , USER , SYSDATE , USER )
   ;
   MERGE INTO ccsowner.person t USING (
      SELECT l_partyId AS partyId FROM DUAL
   ) s ON ( s.partyId = t.partyId )
   WHEN NOT MATCHED THEN INSERT ( t.partyid , t.firstname , t.familyname , t.emailallowedswitch , t.postalmailallowedswitch
   , t.telephonecontactallowedswitch ,t. blockpurchaseswitch , t.smsallowedswitch , t.created , t.createdby , t.lastUpdate , t.updatedby
   , t.locktokentext , t.gdprsourcesystem , t.gdprchangedate , t.gdprchangedby ) VALUES (
   l_partyId , 'Independent' , 'Agent' || i_agentId , 0 , 0 , 0 , 0 , 0 , SYSDATE , USER , SYSDATE , USER , '1' , 'RETAILID' , SYSDATE , USER )
   ;
   MERGE INTO ccsowner.bsbPartyRole t USING (
      SELECT l_partyId AS id FROM DUAL
   ) s ON ( s.id = t.id )
   WHEN NOT MATCHED THEN INSERT ( t.id , t.created , t.createdby , t.lastUpdate , t.updatedby , t.partyId , t.effectiveFrom
   , t.partyroletypecode , t.deletedflag , t.locktokentext ) VALUES (
   l_partyId , SYSDATE , USER , SYSDATE , USER , l_partyId , SYSDATE , 'Agent' , 0 , '1' )
   ;
   MERGE INTO ccsowner.bsbAgentRole t USING (
      SELECT l_partyId AS partyRoleId FROM DUAL
   ) s ON ( s.partyRoleId = t.partyRoleId )
   WHEN NOT MATCHED THEN INSERT ( t.partyRoleId , t.userId , t.sellAllBranches , t.enabled , t.deletedFlag , t.created , t.createdBy
   , t.lastUpdate , t.updatedby , t.locktokentext ) VALUES ( l_partyId , i_agentId , l_sellAllBranches , 1 , 0 , SYSDATE , USER
   , SYSDATE , USER , '1' )
   ;
   MERGE INTO ccsowner.bsbPartyRelatnshipRol t USING (
      SELECT l_homeRelationId AS id FROM DUAL
   ) s ON ( s.id = t.id )
   WHEN NOT MATCHED THEN INSERT ( t.id , t.partyrelatnshipcode , t.primepartyroleid , t.relatedpartyroleid
   , t.effectivefromdate , t.created , t.createdby , t.lastUpdate , t.updatedby , t.locktokentext ) VALUES (
   l_homeRelationId , 'HOMEBRANCH' , l_partyId , l_retailerPartyRole , SYSDATE , SYSDATE , USER , SYSDATE , USER , '1' )
   ;
   MERGE INTO ccsowner.bsbPartyRelatnshipRol t USING (
      SELECT l_saleRelationId AS id FROM DUAL
   ) s ON ( s.id = t.id )
   WHEN NOT MATCHED THEN INSERT ( t.id , t.partyrelatnshipcode , t.primepartyroleid , t.relatedpartyroleid
   , t.effectivefromdate , t.created , t.createdby , t.lastUpdate , t.updatedby , t.locktokentext ) VALUES (
   l_saleRelationId , 'SELLABLEBRANCH' , l_partyId , l_retailerPartyRole , SYSDATE , SYSDATE , USER , SYSDATE , USER , '1' )
   ;
   addAgentRole ( i_roleName => 'Accessibility Team Administrator' , i_agentId => i_agentId ) ;
   addAgentRole ( i_roleName => 'ICT CSR' , i_agentId => i_agentId ) ;
   addAgentRole ( i_roleName => 'MOBILERETAIL' , i_agentId => i_agentId) ;
   addAgentRole ( i_roleName => 'Retail Support' , i_agentId => i_agentId ) ;
   addAgentRole ( i_roleName => 'SHOPRETAIL' , i_agentId => i_agentId ) ;
   addAgentRole ( i_roleName => 'TRIALFSPCLASSIC' , i_agentId => i_agentId ) ;
   addAgentRole ( i_roleName => 'TRIALFSPMOBILE' , i_agentId => i_agentId ) ;
   addAgentRole ( i_roleName => 'TRIALFSPPROSUK' , i_agentId => i_agentId ) ;
   addAgentRole ( i_roleName => 'Project Trials 1' , i_agentId => i_agentId ) ;
   addAgentRole ( i_roleName => 'Project Trials 3' , i_agentId => i_agentId ) ;
   addAgentRole ( i_roleName => 'Project Trials 4' , i_agentId => i_agentId ) ;
   addAgentRole ( i_roleName => 'Project Trials 7' , i_agentId => i_agentId ) ;
   addAgentRole ( i_roleName => 'Project Trials 8' , i_agentId => i_agentId ) ;
   addAgentRole ( i_roleName => 'Project Trials 10' , i_agentId => i_agentId ) ;
   addAgentRole ( i_roleName => 'Project Trials 11' , i_agentId => i_agentId ) ;
   addAgentRole ( i_roleName => 'Project Trials 12' , i_agentId => i_agentId ) ;
END addAgent ;

PROCEDURE addRetailUsers IS
BEGIN
   FOR i IN 800000 .. 806000
   LOOP
      addAgent ( i_agentId => TO_CHAR ( i , 'FM000000' ) ) ;
   END LOOP ;
END addRetailUsers ;

END retail_agent_creation ;
/
