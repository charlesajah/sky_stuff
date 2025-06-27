--this is an example of how to generate a MessoToken derived column
SELECT ba.accountNumber
        , pr.partyId
        , 'T-MES-' || CASE WHEN sys_context ( 'userenv' , 'con_name' ) = 'CHORDO' THEN 'N01' ELSE 'N02' END
            || '-' || ba.accountNumber || '-' || pr.partyId || '-' || MAX ( per.firstName ) || MAX ( per.familyName )
            || '-' || NVL ( MIN ( pti.identityId ) , 'NO-NSPROFILE' ) AS messoToken
        --, MAX ( CASE WHEN p.status = 'ACTIVE' THEN 1 ELSE 0 END ) AS chargeback
        --, MAX ( CASE WHEN p.suid = 'AMP_DISNEY' AND p.status = 'CEASED' THEN 1 ELSE 0 END ) AS disneyCancelled
        --, MAX ( CASE WHEN p.status = 'ACTIVE' THEN 1 ELSE 0 END ) AS failedPayments
        --, MAX ( CASE WHEN p.suid = 'AMP_DISNEY' AND p.status = 'PENDING_CEASE' THEN 1 ELSE 0 END ) AS disneyPendingCancel
        --, MAX ( CASE WHEN p.status = 'ACTIVE' THEN 1 ELSE 0 END ) AS renewalInProgress
       --, MAX ( CASE WHEN p.status != 'ACTIVE' THEN 1 ELSE 0 END ) AS ampNotActive
        --, MAX ( CASE WHEN p.suid = 'AMP_PARAMOUNT_PLUS' AND p.status = 'ACTIVE' THEN 1 ELSE 0 END ) AS paramountPlusActive
     FROM ccsowner.bsbPartyRole pr
     JOIN ccsowner.bsbCustomerRole cr ON pr.id = cr.partyRoleId
     JOIN ccsowner.bsbBillingAccount ba ON ba.portfolioId = cr.portfolioId
     JOIN ccsowner.person per ON per.partyId = pr.partyId
     LEFT OUTER JOIN ccsowner.bsbPartyToIdentity pti ON per.partyId = pti.partyId AND pti.identityType = 'NSPROFILEID'
     --JOIN rcrm.service s ON ba.serviceInstanceId = s.billingServiceInstanceId
     --JOIN rcrm.product p ON s.id = p.serviceId
   -- WHERE s.serviceType = 'AMP'
    GROUP BY ba.accountnumber , pr.partyId
   ;
