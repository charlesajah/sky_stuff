CREATE OR REPLACE PACKAGE          "ETL_UTILTRAIN" IS

  PROCEDURE UTILTRAIN_ET (
    I_JOBINSTANCEID IN varchar2);

END;

/


CREATE OR REPLACE PACKAGE BODY          "ETL_UTILTRAIN" IS

   g_result           NUMBER(1, 0);
   g_message          VARCHAR2(400);
   g_rawerror         VARCHAR2(600);
   g_logid            varchar2(100);
   g_jobname          VARCHAR2(30) := 'UTILTRAIN_ET';
   g_count_nr_inserts NUMBER;
   C_LIMIT CONSTANT INTEGER := 1000;

   /***********************************************/
   PROCEDURE REMOVE_UNUSED_RECORDS(I_JOBINSTANCEID IN varchar2) IS
   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : remove unused records.', g_logid, g_result, g_message, g_rawerror);
      --DBMS_STATS.GATHER_TABLE_STATS ('OPR','UTILTRAIN_DATA');

      DELETE FROM DATAPROV.UTILTRAIN_ALLOCATION a
      WHERE  a.lockeddate < SYSDATE - 60;

      DELETE FROM DATAPROV.UTILTRAIN_DATA u
      WHERE  u.accountnumber NOT IN
             (SELECT a.accountnumber
              FROM   DATAPROV.UTILTRAIN_ALLOCATION a);
      DELETE FROM DATAPROV.UTILTRAIN_PACKAGES;
      DELETE FROM DATAPROV.UTILTRAIN_STATUSES;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END REMOVE_UNUSED_RECORDS;

   /***********************************************/
   PROCEDURE RESET_LOCKED_RECORDS(I_JOBINSTANCEID IN varchar2) IS
   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : reset locked records.',g_logid, g_result, g_message, g_rawerror);

      UPDATE DATAPROV.UTILTRAIN_DATA u
      SET    dtv                 = 'N'
            ,bb                  = 'N'
            ,talk                = 'N'
            ,hdbox               = 'N'
            ,sports              = 'N'
            ,movies              = 'N'
            ,alacarte            = 'N'
            ,hdpack              = 'N'
            ,freesat             = 'N'
            ,processhomemove     = 'N'
            ,cancelledhomemove   = 'N'
            ,original            = 'N'
            ,variety             = 'N'
            ,family              = 'N'
            ,bbpackage           = 'N'
            ,bbstatus            = 'N'
            ,talkpackage         = 'N'
            ,talkstatus          = 'N'
            ,engineervisitbooked = 'N'
            ,u.buytokeep         = 'N'
            ,u.dtvstatus         = 'N'
            ,u.hdpackstatus      = 'N'
            ,u.skygoextra        = 'N'
            ,u.multiscreen       = 'N'
            ,u.no_multiscreen    = 0
            ,u.tvstatus          = 'N'
            ,u.hdstatus          = 'N';

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END RESET_LOCKED_RECORDS;

   /***********************************************/
   PROCEDURE INITIAL_LOAD(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_init IS
         SELECT --/*+DRIVING_SITE(bsbba) */
          bsbba.ACCOUNTNUMBER
         ,bsbba.CURRENCYCODE
         ,bsbba.PORTFOLIOID
         ,bsbba.SERVICEINSTANCEID
         FROM   ccsowner.BSBBILLINGACCOUNT bsbba
               ,DATAPROV.UTILTRAIN_ALLOCATION                    a
         WHERE  bsbba.accountnumber = a.accountnumber(+)
         AND    bsbba.accountnumber IS NOT NULL
         AND    a.accountnumber IS NULL;

      TYPE accnt_tab IS TABLE OF c_init%ROWTYPE;

      l_accnt_tab accnt_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : initial load.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_init;
      LOOP
         FETCH c_init BULK COLLECT
            INTO l_accnt_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_accnt_tab.count
            INSERT INTO DATAPROV.UTILTRAIN_DATA
               (accountnumber, currencycode, portfolioid, serviceinstanceid)
            VALUES
               (l_accnt_tab(i).accountnumber
               ,l_accnt_tab(i).currencycode
               ,l_accnt_tab(i).portfolioid
               ,l_accnt_tab(i).serviceinstanceid);

         EXIT WHEN c_init%NOTFOUND;
      END LOOP;
      CLOSE c_init;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END INITIAL_LOAD;

   /***********************************************/

   PROCEDURE DTV(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_si IS
         SELECT /*+ LEADING(u1)*/ --DRIVING_SITE(bsbsi)
          u1.serviceinstanceid
         ,bsbsub.status
         FROM   ccsowner.bsbserviceinstance bsbsi
               ,ccsowner.bsbsubscription    bsbsub
               ,DATAPROV.UTILTRAIN_DATA                           u1
         WHERE  u1.serviceinstanceid = bsbsi.parentserviceinstanceid
         AND    bsbsi.serviceinstancetype = '210'
         AND    bsbsi.id = bsbsub.serviceinstanceid
         AND    bsbsub.subscriptiontypeid = '1'
         AND    bsbsub.status IN ('EN', 'AC', 'PC');

      TYPE si_tab IS TABLE OF c_si%ROWTYPE;

      l_si_tab si_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : DTV.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_si;
      LOOP
         FETCH c_si BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.dtv      = 'Y'
                  ,u.tvstatus = l_si_tab(i).status
            WHERE  u.serviceinstanceid = l_si_tab(i).serviceinstanceid;

         EXIT WHEN c_si%NOTFOUND;
      END LOOP;
      CLOSE c_si;

      INSERT INTO DATAPROV.UTILTRAIN_STATUSES
         (STATUSCODE, STATUSDESC, PACKAGE_TYPE)
         SELECT code
               ,p.codedesc
               ,'TV'
         FROM   refdatamgr.picklist p
         WHERE  lower(p.codegroup) LIKE 'subscriberstatus'
         AND    p.code IN ('EN', 'AC', 'PC');

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END DTV;

   /***********************************************/

   PROCEDURE BB(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_si IS
         SELECT /*+ LEADING(u1)*/ --DRIVING_SITE(bsbsi)
          u1.serviceinstanceid
         ,bsbcp.productdescription
         ,bsbpk.code
         FROM   ccsowner.bsbserviceinstance           bsbsi
               ,ccsowner.bsbsubscription              bsbsub
               ,ccsowner.bsbportfolioproduct          bsbpp
               ,refdatamgr.bsbcatalogueproduct        bsbcp
               ,refdatamgr.picklist                   bsbpk
               ,ccsowner.bsbsubscriptionhistorystatus bsbsh
               ,DATAPROV.utiltrain_data                                     u1
         WHERE  u1.serviceinstanceid = bsbsi.parentserviceinstanceid
         AND    bsbsi.serviceinstancetype = '400'
         AND    bsbsi.id = bsbsub.serviceinstanceid
         AND    bsbsub.subscriptiontypeid = '7'
               --and bsbsub.status not in ('CN', 'PO','PA','SC')
         AND    bsbsub.status IN ('AC', 'PC', 'CN', 'PO', 'PA', 'SC')
         AND    bsbpp.subscriptionid = bsbsub.id
         AND    bsbpp.catalogueproductid = bsbcp.id
         AND    bsbcp.salesstatus != 'EXP'
         AND    bsbpk.code = bsbsub.status
         AND    bsbpk.codegroup = 'CustomerBroadbandProductElementStatus'
         AND    bsbsh.subscriptionid = bsbsub.id
         AND    bsbsh.statusenddate IS NULL;

      TYPE si_tab IS TABLE OF c_si%ROWTYPE;

      l_si_tab si_tab;
   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : BB.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_si;
      LOOP
         FETCH c_si BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.bbpackage = l_si_tab(i).productdescription
                  ,u.bbstatus  = l_si_tab(i).code
                  ,u.bb        = DECODE(l_si_tab(i).code, 'AC', 'Y',
                                                          'PC', 'Y',
                                                          'CN', 'Y',
                                                          'PO', 'Y',
                                                          'PA', 'Y',
                                                          'SC', 'Y',
                                                          'AP', 'Y',
                                                          'N')
            WHERE  u.serviceinstanceid = l_si_tab(i).serviceinstanceid;

         EXIT WHEN c_si%NOTFOUND;
      END LOOP;
      CLOSE c_si;

      INSERT INTO DATAPROV.UTILTRAIN_PACKAGES
         (PACKAGE_NAME, PACKAGE_COUNTRY, PACKAGE_TYPE)
         SELECT --/*+DRIVING_SITE(bsbcp)*/
         DISTINCT bsbcp.productdescription
                 ,bsbpr.currencycode
                 ,'BB'
         FROM   refdatamgr.bsbcatalogueproduct      bsbcp
               ,refdatamgr.bsbcatalogueproductprice bsbpr
         WHERE  bsbcp.salesstatus != 'EXP'
         AND    bsbcp.subscriptiontype = '7'
         AND    bsbcp.id = bsbpr.catalogueproductid
         AND    bsbpr.enddate IS NULL;

      INSERT INTO DATAPROV.UTILTRAIN_STATUSES
         (STATUSCODE, STATUSDESC, PACKAGE_TYPE)
         SELECT code
               ,p.codedesc
               ,'BB'
         FROM   refdatamgr.picklist p
         WHERE  codegroup = 'CustomerBroadbandProductElementStatus'
         AND    code IN ('AC', 'PC', 'CN', 'PO', 'PA', 'AP', 'SC');
      --and code not in ('CN', 'PO','PA','SC');

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END BB;

   /***********************************************/

   PROCEDURE TALK(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_si IS
         SELECT /*+ LEADING(u1) */ --DRIVING_SITE(bsbsi) 
          u1.serviceinstanceid
         FROM   ccsowner.bsbserviceinstance          bsbsi
               ,ccsowner.bsbsubscription             bsbsub
               ,CCSOWNER.BSBPORTFOLIOPRODUCT         bsbpp
               ,refdatamgr.v_bsbcatalogueproducttype cpt
               ,refdatamgr.v_bsbproducttoproducttype ptpt
               ,refdatamgr.bsbcatalogueproduct       cp
               ,DATAPROV.UTILTRAIN_DATA                                    u1
         WHERE  u1.serviceinstanceid = bsbsi.parentserviceinstanceid
         AND    bsbsi.serviceinstancetype = '100'
         AND    bsbsi.id = bsbsub.serviceinstanceid
         AND    bsbsub.subscriptiontypeid = '3'
         AND    bsbsub.status IN ('AC', 'PC', 'CN')
         AND    bsbpp.subscriptionid = bsbsub.id
         AND    bsbpp.catalogueproductid = cp.id
         AND    ptpt.catalogueproductid = cp.id
         AND    ptpt.catalogueproducttypeid = cpt.id
         AND    cpt.code = 'SKTLKLR';

      TYPE si_tab IS TABLE OF DATAPROV.utiltrain_data.serviceinstanceid%TYPE;

      l_si_tab si_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : talk.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_si;
      LOOP
         FETCH c_si BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.talk = 'Y'
            WHERE  u.serviceinstanceid = l_si_tab(i);

         EXIT WHEN c_si%NOTFOUND;
      END LOOP;
      CLOSE c_si;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END TALK;
   /***********************************************/

   PROCEDURE HDBOX(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_si IS
         SELECT --/*+DRIVING_SITE(pp) */
          a.billingsi
         ,pp.status
         FROM   (WITH pdtv AS (SELECT /*+ leading(u1)*/ --DRIVING_SITE(si)
                                si.id
                               ,u1.serviceinstanceid AS billingsi
                               FROM   ccsowner.BSBServiceInstance si
                                     ,DATAPROV.UTILTRAIN_DATA                           u1
                               WHERE  si.serviceinstancetype = 210
                               AND    u1.serviceinstanceid = si.parentserviceinstanceid) -- Primary DTV
                   SELECT p1.id
                         ,p1.billingsi
                   FROM   pdtv p1
                   UNION ALL
                   SELECT si_sdtv.id
                         ,p.billingsi
                   FROM   pdtv                                         p
                         ,ccsowner.BSBServiceInstance si_sdtv
                   WHERE  si_sdtv.parentserviceinstanceid = p.id
                   AND    si_sdtv.serviceinstancetype = 220 -- Secondary DTV
                    ) a, ccsowner.BSBPortfolioProduct pp, ccsowner.BSBCustomerProductElement cpe
                   WHERE  pp.ServiceInstanceId = a.id
                   AND    pp.status IN ('IN', 'AI')
                   AND    pp.id = cpe.portfolioproductid
                   AND    NVL(cpe.isnonskyproduct, 0) = 0
                   AND    SUBSTR(cpe.settopboxndsnumber, 1, 4) IN
                          (SELECT nctm.ManufacturerCode || nctm.ModelCode
                            FROM   RefDataMgr.BSBNDSCapability nc
                                  ,RefDataMgr.BSBNDSCapToModel nctm
                            WHERE  nc.RDMDeletedFlag = 'N'
                            AND    nc.CapCode = 'HDTV'
                            AND    nctm.CapCode = nc.CapCode
                            AND    nctm.RDMDeletedFlag = 'N');


      TYPE si_tab_t IS TABLE OF c_si%ROWTYPE;

      l_si_tab si_tab_t;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : HDBOX.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_si;
      LOOP
         FETCH c_si BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.hdbox       = 'Y'
                  ,u.hdboxstatus = CASE
                                      WHEN u.hdboxstatus != 'N' AND u.hdboxstatus != l_si_tab(i).status THEN
                                       'ANY'
                                      ELSE
                                       l_si_tab(i).status
                                   END
            WHERE  u.serviceinstanceid = l_si_tab(i).billingsi;

         EXIT WHEN c_si%NOTFOUND;
      END LOOP;
      CLOSE c_si;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END HDBOX;
   /***********************************************/

   PROCEDURE INDIRECTHDBOX(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_si IS
         SELECT --/*+DRIVING_SITE(pp) */
          a.billingsi
         ,pp.status
         FROM   (WITH pdtv AS (SELECT /*+ leading(u1)*/ --DRIVING_SITE(si)
                                si.id
                               ,u1.serviceinstanceid AS billingsi
                               FROM   ccsowner.BSBServiceInstance si
                                     ,DATAPROV.UTILTRAIN_DATA                           u1
                               WHERE  si.serviceinstancetype = 210
                               AND    u1.serviceinstanceid = si.parentserviceinstanceid) -- Primary DTV
                   SELECT p1.id
                         ,p1.billingsi
                   FROM   pdtv p1
                   UNION ALL
                   SELECT si_sdtv.id
                         ,p.billingsi
                   FROM   pdtv                                         p
                         ,ccsowner.BSBServiceInstance si_sdtv
                   WHERE  si_sdtv.parentserviceinstanceid = p.id
                   AND    si_sdtv.serviceinstancetype = 220 -- Secondary DTV
                    ) a, ccsowner.BSBPortfolioProduct pp, ccsowner.BSBCustomerProductElement cpe
                   WHERE  pp.ServiceInstanceId = a.id
                   AND    pp.status IN ('IN', 'AI')
                   AND    pp.id = cpe.portfolioproductid
                   AND    NVL(cpe.isnonskyproduct, 0) = 1
                   AND    SUBSTR(cpe.settopboxndsnumber, 1, 4) IN
                          (SELECT nctm.ManufacturerCode || nctm.ModelCode
                            FROM   RefDataMgr.BSBNDSCapability nc
                                  ,RefDataMgr.BSBNDSCapToModel nctm
                            WHERE  nc.RDMDeletedFlag = 'N'
                            AND    nc.CapCode = 'HDTV'
                            AND    nctm.CapCode = nc.CapCode
                            AND    nctm.RDMDeletedFlag = 'N');


      TYPE si_tab_t IS TABLE OF c_si%ROWTYPE;

      l_si_tab si_tab_t;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : HDBOX.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_si;
      LOOP
         FETCH c_si BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.indirecthdbox       = 'Y'
                  ,u.indirecthdboxstatus = CASE
                                              WHEN u.indirecthdboxstatus != 'N' AND u.indirecthdboxstatus != l_si_tab(i).status THEN
                                               'ANY'
                                              ELSE
                                               l_si_tab(i).status
                                           END
            WHERE  u.serviceinstanceid = l_si_tab(i).billingsi;

         EXIT WHEN c_si%NOTFOUND;
      END LOOP;
      CLOSE c_si;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END INDIRECTHDBOX;

   /***********************************************/
   PROCEDURE HDPACK(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_si IS
         SELECT /*+ LEADING(u1) */ --DRIVING_SITE(si)
          u1.SERVICEINSTANCEID
         ,sub.status
         FROM   ccsowner.bsbserviceinstance         SI
               ,ccsowner.BSBSubscription            sub
               ,ccsowner.BSBSubscriptionEntitlement se
               ,DATAPROV.UTILTRAIN_DATA                                   u1
         WHERE  u1.serviceinstanceid = si.parentserviceinstanceid
         AND    si.serviceinstancetype = 210 -- PDTV
         AND    sub.serviceinstanceid = si.id
         AND    sub.subscriptiontypeid = '4' -- Sky Enhanced Cap Subs
         AND    sub.Status IN ('AC', 'PC', 'EN')
         AND    se.subscriptionid = sub.id
         AND    se.Effectivefromdate <= SYSDATE
         AND    (se.effectivetodate IS NULL OR se.effectivetodate >= TRUNC(SYSDATE))
         AND    se.EntitlementId IN (SELECT cp.EntitlementId
                                     FROM   refdatamgr.bsbcatalogueproducttype cpt
                                           ,refdatamgr.bsbproducttoproducttype ptpt
                                           ,refdatamgr.BSBCatalogueProduct     cp
                                     WHERE  cpt.code = 'HDSUBS'
                                     AND    cpt.rdmdeletedflag = 'N'
                                     AND    ptpt.catalogueproducttypeid = cpt.id
                                     AND    ptpt.rdmdeletedflag = 'N'
                                     AND    cp.id = ptpt.catalogueproductid
                                     AND    cp.RDMDeletedFlag = 'N');

      TYPE si_tab IS TABLE OF c_si%ROWTYPE;

      l_si_tab si_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : HD pack.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_si;
      LOOP
         FETCH c_si BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.hdpack   = 'Y'
                  ,u.hdstatus = l_si_tab(i).status
            WHERE  u.serviceinstanceid = l_si_tab(i).serviceinstanceid;

         EXIT WHEN c_si%NOTFOUND;
      END LOOP;
      CLOSE c_si;

      INSERT INTO DATAPROV.UTILTRAIN_STATUSES
         (STATUSCODE, STATUSDESC, PACKAGE_TYPE)
         SELECT code
               ,p.codedesc
               ,'HD'
         FROM   refdatamgr.picklist p
         WHERE  lower(p.codegroup) LIKE 'subscriberstatus'
         AND    p.code IN ('EN', 'AC', 'PC');

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END HDPACK;
   /***********************************************/
   PROCEDURE SPORTS(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_si IS
         SELECT /*+ LEADING(u1) */ --DRIVING_SITE(bsbsi)
          u1.serviceinstanceid
         FROM   ccsowner.bsbserviceinstance        bsbsi
               ,ccsowner.bsbsubscription           bsbsub
               ,CCSOWNER.BSBPORTFOLIOPRODUCT       bsbpp
               ,ccsowner.bsbcustomerproductelement bsbcpe
               ,refdatamgr.bsbproductelement       rpe
               ,DATAPROV.UTILTRAIN_DATA                                  u1
         WHERE  u1.serviceinstanceid = bsbsi.parentserviceinstanceid
         AND    bsbsi.serviceinstancetype = '210'
         AND    bsbsi.id = bsbsub.serviceinstanceid
         AND    bsbsub.subscriptiontypeid = '1'
         AND    bsbsub.status IN ('EN', 'AC', 'PC')
         AND    bsbsub.id = bsbpp.subscriptionid
         AND    bsbpp.status = 'EN'
         AND    bsbpp.id = bsbcpe.portfolioproductid
         AND    bsbcpe.productelementid = rpe.id
         AND    rpe.productcode LIKE 'SKY_SP%';

      TYPE si_tab IS TABLE OF DATAPROV.utiltrain_data.serviceinstanceid%TYPE;

      l_si_tab si_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : sports.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_si;
      LOOP
         FETCH c_si BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.sports = 'Y'
            WHERE  u.serviceinstanceid = l_si_tab(i);

         EXIT WHEN c_si%NOTFOUND;
      END LOOP;
      CLOSE c_si;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END SPORTS;
   /***********************************************/
   PROCEDURE LIMA_SPORTS(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_si IS
         SELECT /*+ LEADING(u1) */  --DRIVING_SITE(bsbba)
          u1.accountnumber
         ,LISTAGG(' ' || cp.productname || ' ', ',') WITHIN GROUP(ORDER BY cp.id) AS sportspackage
         FROM   ccsowner.bsbbillingaccount     bsbba
               ,ccsowner.bsbserviceinstance    bsbsi
               ,CCSOWNER.BSBPORTFOLIOPRODUCT   bsbpp
               ,refdatamgr.bsbcatalogueproduct cp
               ,DATAPROV.UTILTRAIN_DATA                              u1
         WHERE  u1.accountnumber = bsbba.accountnumber
         AND    bsbba.serviceinstanceid = bsbsi.parentserviceinstanceid
         AND    bsbsi.serviceinstancetype = '210'
         AND    bsbpp.serviceinstanceid = bsbsi.id
         AND    bsbpp.status = 'EN'
         AND    bsbpp.catalogueproductid = cp.id
         AND    bsbpp.catalogueproductid IN
                (SELECT p.id
                  FROM   refdatamgr.bsbcatalogueproduct p
                        ,refdatamgr.bsbproductflag      pf
                  WHERE  pf.catalogueproductid = p.id
                  AND    pf.flagcode = 'POSMIGRATION_LIMA_TRIGGER'
                  AND    p.subscriptiontype = '5'
                  AND    p.subscriptionsubtypeid = '1')
         GROUP  BY u1.accountnumber;

      TYPE si_tab IS TABLE OF c_si%ROWTYPE;

      l_si_tab si_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : lima sports.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_si;
      LOOP
         FETCH c_si BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.sports        = 'Y'
                  ,u.sportspackage = l_si_tab(i).sportspackage
            WHERE  u.accountnumber = l_si_tab(i).accountnumber;

         EXIT WHEN c_si%NOTFOUND;
      END LOOP;
      CLOSE c_si;

      INSERT INTO DATAPROV.UTILTRAIN_STATUSES
         (STATUSCODE, STATUSDESC, PACKAGE_TYPE)
         SELECT bsbcp.salesstatus
               ,bsbcp.productdescription
               ,'SPORTS'
         FROM   refdatamgr.bsbcatalogueproduct bsbcp
         WHERE  bsbcp.subscriptionsubtypeid = '1'
         AND    bsbcp.subscriptiontype = '5'
         AND    bsbcp.salesstatus = 'SA'
         AND    bsbcp.productdescription LIKE 'Sky Sports%';

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END LIMA_SPORTS;
   /***********************************************/
   PROCEDURE MOVIES(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_si IS
         SELECT /*+ LEADING(u1) */ --DRIVING_SITE(bsbsi)
          u1.serviceinstanceid
         FROM   ccsowner.bsbserviceinstance        bsbsi
               ,ccsowner.bsbsubscription           bsbsub
               ,CCSOWNER.BSBPORTFOLIOPRODUCT       bsbpp
               ,ccsowner.bsbcustomerproductelement bsbcpe
               ,refdatamgr.bsbproductelement       rpe
               ,DATAPROV.UTILTRAIN_DATA                                  u1
         WHERE  u1.serviceinstanceid = bsbsi.parentserviceinstanceid
         AND    bsbsi.serviceinstancetype = '210'
         AND    bsbsi.id = bsbsub.serviceinstanceid
         AND    bsbsub.subscriptiontypeid = '1'
         AND    bsbsub.status IN ('EN', 'AC', 'PC')
         AND    bsbsub.id = bsbpp.subscriptionid
         AND    bsbpp.status = 'EN'
         AND    bsbpp.id = bsbcpe.portfolioproductid
         AND    bsbcpe.productelementid = rpe.id
         AND    rpe.productcode LIKE 'SKY_MM%';

      TYPE si_tab IS TABLE OF DATAPROV.utiltrain_data.serviceinstanceid%TYPE;

      l_si_tab si_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : movies.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_si;
      LOOP
         FETCH c_si BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.movies = 'Y'
            WHERE  u.serviceinstanceid = l_si_tab(i);

         EXIT WHEN c_si%NOTFOUND;
      END LOOP;
      CLOSE c_si;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END MOVIES;
   /***********************************************/
   PROCEDURE LIMA_CINEMA(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_si IS
         SELECT /*+ LEADING(u1) */ --DRIVING_SITE(bsbba)
          u1.accountnumber
         FROM   ccsowner.bsbbillingaccount     bsbba
               ,ccsowner.bsbserviceinstance    bsbsi
               ,CCSOWNER.BSBPORTFOLIOPRODUCT   bsbpp
               ,refdatamgr.bsbcatalogueproduct ctp
               ,DATAPROV.UTILTRAIN_DATA                              u1
         WHERE  u1.accountnumber = bsbba.accountnumber
         AND    bsbba.serviceinstanceid = bsbsi.parentserviceinstanceid
         AND    bsbsi.serviceinstancetype = '210'
         AND    bsbpp.serviceinstanceid = bsbsi.id
         AND    bsbpp.status = 'EN'
         AND    bsbpp.catalogueproductid = ctp.id
         AND    ctp.id IN (SELECT p.id
                           FROM   refdatamgr.bsbcatalogueproduct p
                           WHERE  p.subscriptiontype = '5'
                           AND    p.subscriptionsubtypeid = '2'
                           AND    salesstatus = 'SA');

      TYPE si_tab IS TABLE OF DATAPROV.utiltrain_data.accountnumber%TYPE;

      l_si_tab si_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : LIMA_CINEMA.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_si;
      LOOP
         FETCH c_si BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.movies = 'Y'
            WHERE  u.accountnumber = l_si_tab(i);

         EXIT WHEN c_si%NOTFOUND;
      END LOOP;
      CLOSE c_si;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END LIMA_CINEMA;

   /***********************************************/
   PROCEDURE LIMA_KIDS(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_si IS
         SELECT /*+ LEADING(u1) */ --DRIVING_SITE(bsbba)
          u1.accountnumber
         FROM   ccsowner.bsbbillingaccount     bsbba
               ,ccsowner.bsbserviceinstance    bsbsi
               ,CCSOWNER.BSBPORTFOLIOPRODUCT   bsbpp
               ,refdatamgr.bsbcatalogueproduct ctp
               ,DATAPROV.UTILTRAIN_DATA                              u1
         WHERE  u1.accountnumber = bsbba.accountnumber
         AND    bsbba.serviceinstanceid = bsbsi.parentserviceinstanceid
         AND    bsbsi.serviceinstancetype = '210'
         AND    bsbpp.serviceinstanceid = bsbsi.id
         AND    bsbpp.status = 'EN'
         AND    bsbpp.catalogueproductid = ctp.id
         AND    ctp.Subscriptionsubtypeid = 4;

      TYPE si_tab IS TABLE OF DATAPROV.utiltrain_data.accountnumber%TYPE;

      l_si_tab si_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : LIMA_KIDS.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_si;
      LOOP
         FETCH c_si BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.Kids = 'Y'
            WHERE  u.accountnumber = l_si_tab(i);

         EXIT WHEN c_si%NOTFOUND;
      END LOOP;
      CLOSE c_si;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END LIMA_KIDS;
   /**********************************************/
   PROCEDURE LIMA_BOXSETS(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_si IS
         SELECT /*+ LEADING(u1) */ --DRIVING_SITE(bsbba)
          u1.accountnumber
         FROM   ccsowner.bsbbillingaccount     bsbba
               ,ccsowner.bsbserviceinstance    bsbsi
               ,CCSOWNER.BSBPORTFOLIOPRODUCT   bsbpp
               ,refdatamgr.bsbcatalogueproduct ctp
               ,DATAPROV.UTILTRAIN_DATA                              u1
         WHERE  u1.accountnumber = bsbba.accountnumber
         AND    bsbba.serviceinstanceid = bsbsi.parentserviceinstanceid
         AND    bsbsi.serviceinstancetype = '210'
         AND    bsbpp.serviceinstanceid = bsbsi.id
         AND    bsbpp.status = 'EN'
         AND    bsbpp.catalogueproductid = ctp.id
         AND    ctp.Subscriptionsubtypeid = 3;

      TYPE si_tab IS TABLE OF DATAPROV.utiltrain_data.accountnumber%TYPE;

      l_si_tab si_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : LIMA_BOXSETS.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_si;
      LOOP
         FETCH c_si BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.Boxsets = 'Y'
            WHERE  u.accountnumber = l_si_tab(i);

         EXIT WHEN c_si%NOTFOUND;
      END LOOP;
      CLOSE c_si;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END LIMA_BOXSETS;
   /***************************************************************************/
   PROCEDURE ALACARTE(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_si IS
         SELECT /*+ LEADING(u1) */ --DRIVING_SITE(bsbsi)
          u1.serviceinstanceid
         FROM   ccsowner.bsbserviceinstance bsbsi
               ,ccsowner.bsbsubscription    bsbsub
               ,DATAPROV.UTILTRAIN_DATA                           u1
         WHERE  u1.serviceinstanceid = bsbsi.parentserviceinstanceid
         AND    bsbsi.serviceinstancetype = '210'
         AND    bsbsi.id = bsbsub.serviceinstanceid
         AND    bsbsub.subscriptiontypeid = '5'
         AND    bsbsub.status IN ('EN', 'AC', 'PC');

      TYPE si_tab IS TABLE OF DATAPROV.utiltrain_data.serviceinstanceid%TYPE;

      l_si_tab si_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : A la carte.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_si;
      LOOP
         FETCH c_si BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.alacarte = 'Y'
            WHERE  u.serviceinstanceid = l_si_tab(i);

         EXIT WHEN c_si%NOTFOUND;
      END LOOP;
      CLOSE c_si;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END ALACARTE;

   /***********************************************/
   PROCEDURE FREESAT(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_si IS
         SELECT /*+ LEADING(u1) */ --DRIVING_SITE(si)
          u1.serviceinstanceid
         FROM   ccsowner.bsbserviceinstance         SI
               ,ccsowner.BSBSubscription            sub
               ,ccsowner.BSBSubscriptionEntitlement se
               ,DATAPROV.UTILTRAIN_DATA                                   u1
         WHERE  u1.serviceinstanceid = si.parentserviceinstanceid
         AND    si.serviceinstancetype = 210 -- PDTV
         AND    sub.serviceinstanceid = si.id
         AND    sub.subscriptiontypeid = '4' -- Sky Enhanced Cap Subs
         AND    sub.Status IN ('AC', 'PC', 'EN')
         AND    se.subscriptionid = sub.id
         AND    se.Effectivefromdate <= SYSDATE
         AND    (se.effectivetodate IS NULL OR se.effectivetodate >= TRUNC(SYSDATE))
         AND    se.EntitlementId IN (SELECT cp.EntitlementId
                                     FROM   refdatamgr.bsbcatalogueproducttype cpt
                                           ,refdatamgr.bsbproducttoproducttype ptpt
                                           ,refdatamgr.BSBCatalogueProduct     cp
                                     WHERE  cpt.code = 'FRST'
                                     AND    cpt.rdmdeletedflag = 'N'
                                     AND    ptpt.catalogueproducttypeid = cpt.id
                                     AND    ptpt.rdmdeletedflag = 'N'
                                     AND    cp.id = ptpt.catalogueproductid
                                     AND    cp.RDMDeletedFlag = 'N');

      TYPE si_tab IS TABLE OF DATAPROV.utiltrain_data.serviceinstanceid%TYPE;

      l_si_tab si_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : freesat.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_si;
      LOOP
         FETCH c_si BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.freesat = 'Y'
            WHERE  u.serviceinstanceid = l_si_tab(i);

         EXIT WHEN c_si%NOTFOUND;
      END LOOP;
      CLOSE c_si;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END FREESAT;

   /***********************************************/
   PROCEDURE HOMEMOVE(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_po IS
         SELECT /*+ LEADING(u1) */ --DRIVING_SITE(mh)
          u1.portfolioid
         FROM   CCSOWNER.V_BSBMOVEHOMESTATUS mh
               ,DATAPROV.UTILTRAIN_DATA                            u1
         WHERE  u1.portfolioid = mh.portfolioid
         AND    mh.mh_statuscode = 'BK'
         AND    mh.effectivefrom > SYSDATE;

      TYPE po_tab IS TABLE OF DATAPROV.utiltrain_data.portfolioid%TYPE;

      l_po_tab po_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : home move.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_po;
      LOOP
         FETCH c_po BULK COLLECT
            INTO l_po_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_po_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.processhomemove = 'Y'
            WHERE  u.portfolioid = l_po_tab(i);

         EXIT WHEN c_po%NOTFOUND;
      END LOOP;
      CLOSE c_po;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END HOMEMOVE;

   /***********************************************/
   PROCEDURE CANCELHOMEMOVE(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_po IS
         SELECT portfolioid
         FROM   (SELECT /*+ LEADING(u1) */ --DRIVING_SITE(mh)
                  u1.portfolioid
                 ,mh.mh_statuscode
                 ,ROW_NUMBER() OVER(PARTITION BY mh.portfolioid ORDER BY mh.effectivefrom DESC NULLS LAST) srlno
                 FROM   CCSOWNER.V_BSBMOVEHOMESTATUS mh
                       ,DATAPROV.UTILTRAIN_DATA                            u1
                 WHERE  u1.portfolioid = mh.portfolioid)
         WHERE  srlno = '1'
         AND    mh_statuscode = 'CN';

      TYPE po_tab IS TABLE OF DATAPROV.utiltrain_data.portfolioid%TYPE;

      l_po_tab po_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : cancel home move.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_po;
      LOOP
         FETCH c_po BULK COLLECT
            INTO l_po_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_po_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.cancelledhomemove = 'Y'
            WHERE  u.portfolioid = l_po_tab(i);

         EXIT WHEN c_po%NOTFOUND;
      END LOOP;
      CLOSE c_po;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END CANCELHOMEMOVE;
   /***********************************************/
   PROCEDURE ORIGINAL(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_po IS
         SELECT /*+ LEADING(u) */ --DRIVING_SITE(p)
          u.portfolioid
         FROM   refdatamgr.bsbcatalogueproduct c
               ,ccsowner.bsbportfolioproduct   p
               ,DATAPROV.utiltrain_data                              u
         WHERE  u.portfolioid = p.portfolioid
         AND    c.productname = 'Original'
         AND    c.id = p.catalogueproductid
         AND    p.status = 'EN';

      TYPE po_tab IS TABLE OF DATAPROV.utiltrain_data.portfolioid%TYPE;

      l_po_tab po_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : original.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_po;
      LOOP
         FETCH c_po BULK COLLECT
            INTO l_po_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_po_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.original = 'Y'
            WHERE  u.portfolioid = l_po_tab(i);

         EXIT WHEN c_po%NOTFOUND;
      END LOOP;
      CLOSE c_po;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END ORIGINAL;
   /***********************************************/
   PROCEDURE VARIETY(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_po IS
         SELECT /*+ LEADING(u) */ --DRIVING_SITE(p)
          u.portfolioid
         FROM   refdatamgr.bsbcatalogueproduct c
               ,ccsowner.bsbportfolioproduct   p
               ,DATAPROV.utiltrain_data                              u
         WHERE  u.portfolioid = p.portfolioid
         AND    c.productname = 'Variety'
         AND    c.id = p.catalogueproductid
         AND    p.status = 'EN';

      TYPE po_tab IS TABLE OF DATAPROV.utiltrain_data.portfolioid%TYPE;

      l_po_tab po_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : variety.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_po;
      LOOP
         FETCH c_po BULK COLLECT
            INTO l_po_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_po_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.variety = 'Y'
            WHERE  u.portfolioid = l_po_tab(i);

         EXIT WHEN c_po%NOTFOUND;
      END LOOP;
      CLOSE c_po;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END VARIETY;
   /***********************************************/
   PROCEDURE FAMILY(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_po IS
         SELECT /*+ LEADING(u) */ --DRIVING_SITE(p)
          u.portfolioid
         FROM   refdatamgr.bsbcatalogueproduct c
               ,ccsowner.bsbportfolioproduct   p
               ,DATAPROV.utiltrain_data                              u
         WHERE  u.portfolioid = p.portfolioid
         AND    c.productname = 'Family'
         AND    c.id = p.catalogueproductid
         AND    p.status = 'EN';

      TYPE po_tab IS TABLE OF DATAPROV.utiltrain_data.portfolioid%TYPE;

      l_po_tab po_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : family.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_po;
      LOOP
         FETCH c_po BULK COLLECT
            INTO l_po_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_po_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.family = 'Y'
            WHERE  u.portfolioid = l_po_tab(i);

         EXIT WHEN c_po%NOTFOUND;
      END LOOP;
      CLOSE c_po;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END FAMILY;
   /***********************************************/
   PROCEDURE TALKPACKAGE(I_JOBINSTANCEID IN varchar2) IS

      CURSOR c_po IS
         SELECT /*+ LEADING(u1) */ --DRIVING_SITE(bsbsi)
          u1.serviceinstanceid
         ,cp.productdescription
         ,bsbpk.code
         FROM   refdatamgr.bsbcatalogueproduct        cp
               ,CCSOWNER.BSBPORTFOLIOPRODUCT          bsbpp
               ,DATAPROV.UTILTRAIN_DATA                                     u1
               ,ccsowner.bsbserviceinstance           bsbsi
               ,ccsowner.bsbsubscription              bsbsub
               ,refdatamgr.picklist                   bsbpk
               ,ccsowner.bsbsubscriptionhistorystatus bsbsh
         WHERE  u1.serviceinstanceid = bsbsi.parentserviceinstanceid
         AND    bsbsi.id = bsbsub.serviceinstanceid
         AND    cp.subscriptiontype = '3'
         AND    cp.servicetypecode = '3'
         AND    cp.salesstatus = 'SA'
         AND    bsbpp.subscriptionid = bsbsub.id
         AND    bsbpp.catalogueproductid = cp.id
         AND    bsbsub.status IN ('A', 'PC', 'CN')
         AND    bsbpk.code = bsbsub.status
         AND    bsbpk.codegroup = 'CustomerTelephonyServiceProductElementStatus'
         AND    bsbsh.subscriptionid = bsbsub.id
         AND    bsbsh.statusenddate IS NULL
         ORDER  BY u1.serviceinstanceid
                  ,bsbsh.statusstartdate;

      TYPE po_tab IS TABLE OF c_po%ROWTYPE;

      l_po_tab po_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : fibre.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_po;
      LOOP
         FETCH c_po BULK COLLECT
            INTO l_po_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_po_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.talkpackage = l_po_tab(i).productdescription
                  ,u.talkstatus  = l_po_tab(i).code
                  ,u.talk        = DECODE(l_po_tab(i).code, 'A', 'Y',
                                                            'PC', 'Y',
                                                            'CN', 'Y',
                                                            'L',  'Y',
                                                            'N')
            WHERE  u.serviceinstanceid = l_po_tab(i).serviceinstanceid;

         EXIT WHEN c_po%NOTFOUND;
      END LOOP;
      CLOSE c_po;

      INSERT INTO DATAPROV.UTILTRAIN_PACKAGES
         (PACKAGE_NAME, PACKAGE_COUNTRY, PACKAGE_TYPE)
         SELECT cp.productdescription
               ,pr.currencycode
               ,'TALK'
         FROM   refdatamgr.bsbcatalogueproduct      cp
               ,refdatamgr.bsbcatalogueproductprice pr
         WHERE  cp.subscriptiontype = '3'
         AND    cp.servicetypecode = '3'
         AND    salesstatus = 'SA'
         AND    cp.id = pr.catalogueproductid
         AND    pr.enddate IS NULL;

      INSERT INTO DATAPROV.UTILTRAIN_STATUSES
         (STATUSCODE, STATUSDESC, PACKAGE_TYPE)
         SELECT code
               ,p.codedesc
               ,'TALK'
         FROM   refdatamgr.picklist p
         WHERE  codegroup = 'CustomerTelephonyServiceProductElementStatus'
         AND    code IN ('A', 'PC', 'L');

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END TALKPACKAGE;

   /***********************************************/

   PROCEDURE engineervisitbooked(I_JOBINSTANCEID IN varchar2) IS
      l_today DATE := trunc(SYSDATE);

      CURSOR c_si IS
         SELECT /*+ LEADING(u) */ --DRIVING_SITE(f)
          u.serviceinstanceid
         FROM   ccsowner.bsbvisitrequirement v
               ,ccsowner.bsbfulfilmentitem   f
               ,DATAPROV.utiltrain_data                            u
         WHERE  u.serviceinstanceid = f.serviceinstanceid
         AND    f.id = v.fulfilmentitemid
         AND    v.visitdate >= l_today;

      TYPE si_tab IS TABLE OF DATAPROV.utiltrain_data.serviceinstanceid%TYPE;

      l_si_tab si_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : engineer visit booked.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_si;
      LOOP
         FETCH c_si BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.engineervisitbooked = 'Y'
            WHERE  u.serviceinstanceid = l_si_tab(i);

         EXIT WHEN c_si%NOTFOUND;
      END LOOP;
      CLOSE c_si;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END engineervisitbooked;

   /***********************************************/
   PROCEDURE accountactive(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_po IS
         SELECT /*+ LEADING(u) */ --DRIVING_SITE(cr)
          u.portfolioid
         FROM   ccsowner.bsbcustomerrole cr
               ,DATAPROV.utiltrain_data                        u
         WHERE  cr.portfolioid = u.portfolioid
         AND    cr.customerstatuscode = 'CRACT';

      TYPE po_tab IS TABLE OF DATAPROV.utiltrain_data.portfolioid%TYPE;

      l_po_tab po_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : account active.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_po;
      LOOP
         FETCH c_po BULK COLLECT
            INTO l_po_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_po_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.accountactive = 'Y'
            WHERE  u.portfolioid = l_po_tab(i);

         EXIT WHEN c_po%NOTFOUND;
      END LOOP;
      CLOSE c_po;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END accountactive;
   /***********************************************/
   PROCEDURE accountblocked(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_accnts IS
         SELECT /*+ LEADING(u) */ --DRIVING_SITE(ba)
          u.accountnumber
         FROM   ccsowner.bsbsubscription    su
               ,ccsowner.bsbbillingaccount  ba
               ,ccsowner.bsbserviceinstance si
               ,DATAPROV.utiltrain_data                           u
         WHERE  ba.accountnumber = u.accountnumber
         AND    su.status = 'AB'
         AND    su.subscriptiontypeid = '1'
         AND    si.id = su.serviceinstanceid
         AND    si.portfolioid = ba.portfolioid;

      TYPE accnt_tab IS TABLE OF DATAPROV.utiltrain_data.accountnumber%TYPE;

      l_accnt_tab accnt_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : account blocked.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_accnts;
      LOOP
         FETCH c_accnts BULK COLLECT
            INTO l_accnt_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_accnt_tab.count
            UPDATE DATAPROV.UTILTRAIN_DATA
            SET    accountblocked = 'Y'
            WHERE  accountnumber = l_accnt_tab(i);

         EXIT WHEN c_accnts%NOTFOUND;
      END LOOP;
      CLOSE c_accnts;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END accountblocked;

   /***********************************************/
   PROCEDURE BUYTOKEEP(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_buytokeep IS
         SELECT /*+ LEADING(u1) */ --DRIVING_SITE(pp)
         DISTINCT u1.serviceinstanceid
         FROM   refdatamgr.bsbproductflag      f
               ,refdatamgr.bsbcatalogueproduct cp
               ,ccsowner.bsbportfolioproduct   pp
               ,DATAPROV.utiltrain_data                              u1
         WHERE  f.flagcode = 'ESTPRODUCT'
         AND    cp.id = f.catalogueproductid
         AND    f.isbundle IS NULL
         AND    pp.catalogueproductid = cp.id
         AND    u1.portfolioid = pp.portfolioid;

      TYPE si_tab IS TABLE OF DATAPROV.utiltrain_data.serviceinstanceid%TYPE;

      l_si_tab si_tab;
   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : DTV.', g_logid, g_result, g_message, g_rawerror);
      OPEN c_buytokeep;
      LOOP
         FETCH c_buytokeep BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;
         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.UTILTRAIN_DATA
            SET    buytokeep = 'Y'
            WHERE  serviceinstanceid = l_si_tab(i);

         EXIT WHEN c_buytokeep%NOTFOUND;
      END LOOP;
      CLOSE c_buytokeep;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END BUYTOKEEP;

   /***********************************************/
   PROCEDURE SKYGOEXTRA(I_JOBINSTANCEID IN varchar2) IS
      TYPE si_tab IS TABLE OF DATAPROV.utiltrain_data.serviceinstanceid%TYPE;
      l_si_tab si_tab;

      CURSOR c_skygo IS
         SELECT /*+ LEADING(u1) */ --DRIVING_SITE(bsbsi)
          u1.serviceinstanceid
         FROM   ccsowner.bsbserviceinstance        bsbsi
               ,ccsowner.bsbsubscription           bsbsub
               ,CCSOWNER.BSBPORTFOLIOPRODUCT       bsbpp
               ,ccsowner.bsbcustomerproductelement bsbcpe
               ,refdatamgr.bsbproductelement       rpe
               ,DATAPROV.utiltrain_data                                  u1
         WHERE  u1.serviceinstanceid = bsbsi.parentserviceinstanceid
         AND    bsbsi.id = bsbsub.serviceinstanceid
         AND    bsbsub.subscriptiontypeid = '5'
         AND    bsbsi.serviceinstancetype = '210'
         AND    bsbsub.status IN ('EN', 'AC', 'PC')
         AND    bsbsub.id = bsbpp.subscriptionid
         AND    bsbpp.status = 'EN'
         AND    bsbpp.id = bsbcpe.portfolioproductid
         AND    bsbcpe.productelementid = rpe.id
         AND    bsbcpe.name LIKE 'Sky Go Extra';

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : DTV.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_skygo;
      LOOP
         FETCH c_skygo BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.SKYGOEXTRA = 'Y'
            WHERE  u.serviceinstanceid = l_si_tab(i);

         EXIT WHEN c_skygo%NOTFOUND;
      END LOOP;
      CLOSE c_skygo;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END SKYGOEXTRA;
   /***********************************************/
   PROCEDURE MULTISCREEN(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_mr IS
         SELECT /*+ LEADING(u1) */ --DRIVING_SITE(bsbpp)
          u1.serviceinstanceid
         ,COUNT(bsbpp.portfolioid) no_mr
         FROM   ccsowner.bsbserviceinstance        bsbsi
               ,ccsowner.bsbserviceinstance        bsbsi1
               ,ccsowner.bsbsubscription           bsbsub
               ,CCSOWNER.BSBPORTFOLIOPRODUCT       bsbpp
               ,ccsowner.bsbcustomerproductelement bsbcpe
               ,refdatamgr.bsbproductelement       rpe
               ,refdatamgr.bsbproductflag          pf
               ,DATAPROV.utiltrain_data                                  u1
         WHERE  bsbsi1.parentserviceinstanceid = u1.serviceinstanceid
         AND    bsbsi.parentserviceinstanceid = bsbsi1.id
         AND    bsbsi.id = bsbsub.serviceinstanceid
         AND    bsbsub.subscriptiontypeid = '2'
         AND    bsbsi.serviceinstancetype = '220'
         AND    bsbsub.status IN ('EN', 'AC', 'PC')
         AND    bsbsub.id = bsbpp.subscriptionid
         AND    bsbpp.status = 'EN'
         AND    bsbpp.id = bsbcpe.portfolioproductid
         AND    bsbcpe.productelementid = rpe.id
         AND    bsbpp.catalogueproductid = pf.catalogueproductid
         AND    pf.flagcode = 'MULTIROOMALLSUBS'
         AND    pf.rdmdeletedflag != 'Y'
         GROUP  BY u1.serviceinstanceid;

      TYPE si_tab IS TABLE OF c_mr%ROWTYPE;
      l_si_tab si_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : DTV.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_mr;
      LOOP
         FETCH c_mr BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.multiscreen    = 'Y'
                  ,u.no_multiscreen = l_si_tab(i).no_mr
            WHERE  u.serviceinstanceid = l_si_tab(i).serviceinstanceid;

         EXIT WHEN c_mr%NOTFOUND;
      END LOOP;
      CLOSE c_mr;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END MULTISCREEN;

   /***********************************************/

   PROCEDURE skyq(I_JOBINSTANCEID IN varchar2) IS
      --type :1221
      CURSOR c_skyq IS
         SELECT /*+ LEADING(u) */ --DRIVING_SITE(p)
          u.portfolioid
         FROM   refdatamgr.bsbcatalogueproduct     c
               ,refdatamgr.bsbproducttoproducttype t
               ,ccsowner.bsbportfolioproduct       p
               ,DATAPROV.utiltrain_data                                  u
         WHERE  u.portfolioid = p.portfolioid
         AND    t.catalogueproductid = p.catalogueproductid
         AND    t.catalogueproducttypeid = '1221'
         AND    c.id = p.catalogueproductid
         AND    p.status = 'IN';

      TYPE si_tab IS TABLE OF c_skyq%ROWTYPE;
      l_si_tab si_tab;
   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : SKYQ.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_skyq;
      LOOP
         FETCH c_skyq BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.skyq = 'Y'
            WHERE  u.portfolioid = l_si_tab(i).portfolioid;

         EXIT WHEN c_skyq%NOTFOUND;
      END LOOP;
      CLOSE c_skyq;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END skyq;
   /***********************************************/

   PROCEDURE skyqmini(I_JOBINSTANCEID IN varchar2) IS
      --type :1229
      CURSOR c_skyqmini IS
         SELECT /*+ LEADING(u) */ --DRIVING_SITE(p)
          u.portfolioid
         FROM   refdatamgr.bsbcatalogueproduct     c
               ,refdatamgr.bsbproducttoproducttype t
               ,ccsowner.bsbportfolioproduct       p
               ,DATAPROV.utiltrain_data                                  u
         WHERE  u.portfolioid = p.portfolioid
         AND    t.catalogueproductid = p.catalogueproductid
         AND    t.catalogueproducttypeid = '1229'
         AND    c.id = p.catalogueproductid
         AND    p.status = 'IN';

      TYPE si_tab IS TABLE OF c_skyqmini%ROWTYPE;
      l_si_tab si_tab;
   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : SKYQMINI.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_skyqmini;
      LOOP
         FETCH c_skyqmini BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.skyqmini = 'Y'
            WHERE  u.portfolioid = l_si_tab(i).portfolioid;

         EXIT WHEN c_skyqmini%NOTFOUND;
      END LOOP;
      CLOSE c_skyqmini;

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END skyqmini;

   /***********************************************/

   PROCEDURE MOBILE(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_si IS
         SELECT /*+ LEADING(u1)*/ --DRIVING_SITE(bsbsi)
          u1.serviceinstanceid
         FROM   ccsowner.bsbserviceinstance bsbsi
               ,DATAPROV.UTILTRAIN_DATA                           u1
         WHERE  u1.serviceinstanceid = bsbsi.parentserviceinstanceid
         AND    bsbsi.serviceinstancetype IN ('610', '620');

      TYPE si_tab IS TABLE OF c_si%ROWTYPE;

      l_si_tab si_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : MOBILE.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_si;
      LOOP
         FETCH c_si BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.mobile = 'Y'
            WHERE  u.serviceinstanceid = l_si_tab(i).serviceinstanceid;

         EXIT WHEN c_si%NOTFOUND;
      END LOOP;
      CLOSE c_si;

      INSERT INTO DATAPROV.UTILTRAIN_STATUSES
         (STATUSCODE, STATUSDESC, PACKAGE_TYPE)
         SELECT code
               ,p.codedesc
               ,'MOBILE'
         FROM   refdatamgr.picklist p
         WHERE  lower(p.codegroup) LIKE 'subscriberstatus'
         AND    p.code IN ('EN', 'AC', 'PC');

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END MOBILE;

   /***********************************************/

   PROCEDURE MOBILE_TARIFF(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_si IS
         SELECT /*+ LEADING(u1)*/ --DRIVING_SITE(bsbsi)
          cpi.productdescription
         ,cpe.status
         ,u1.serviceinstanceid
         FROM   ccsowner.bsbserviceinstance        bsbsi
               ,ccsowner.bsbportfolioproduct       pp
               ,ccsowner.bsbcustomerproductelement cpe
               ,refdatamgr.bsbcatalogueproduct     cpi
               ,ccsowner.bsbsubscription           sub
               ,DATAPROV.utiltrain_data                                  u1
         WHERE  u1.serviceinstanceid = bsbsi.parentserviceinstanceid
         AND    bsbsi.serviceinstancetype IN ('610', '620')
         AND    pp.serviceinstanceid = bsbsi.id
         AND    cpe.portfolioproductid = pp.id
         AND    pp.catalogueproductid = cpi.id
         AND    pp.subscriptionid = sub.id
         AND    sub.subscriptiontypeid = '10';

      TYPE si_tab IS TABLE OF c_si%ROWTYPE;

      l_si_tab si_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : MOBILETARIFF.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_si;
      LOOP
         FETCH c_si BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.mobtariff       = l_si_tab(i).productdescription
                  ,u.mobtariffstatus = l_si_tab(i).status
            WHERE  u.serviceinstanceid = l_si_tab(i).serviceinstanceid;

         EXIT WHEN c_si%NOTFOUND;
      END LOOP;
      CLOSE c_si;

      INSERT INTO DATAPROV.UTILTRAIN_PACKAGES
         (PACKAGE_NAME, PACKAGE_COUNTRY, PACKAGE_TYPE)
         SELECT --/*+DRIVING_SITE(bsbcp)*/
         DISTINCT bsbcp.productdescription
                 ,bsbpr.currencycode
                 ,'MOBILETARIFF'
         FROM   refdatamgr.bsbcatalogueproduct      bsbcp
               ,refdatamgr.bsbcatalogueproductprice bsbpr
         WHERE  bsbcp.salesstatus != 'EXP'
         AND    bsbcp.subscriptiontype = '10'
         AND    bsbcp.id = bsbpr.catalogueproductid
         AND    bsbpr.enddate IS NULL;

      INSERT INTO DATAPROV.UTILTRAIN_STATUSES
         (STATUSCODE, STATUSDESC, PACKAGE_TYPE)
         SELECT code
               ,p.codedesc
               ,'MOBILETARIFF'
         FROM   refdatamgr.picklist p
         WHERE  codegroup = 'CustomerMobileTariffProductElementStatus';

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END MOBILE_TARIFF;

   /***********************************************/

   PROCEDURE MOBILE_ADDON(I_JOBINSTANCEID IN varchar2) IS
      CURSOR c_si IS
         SELECT /*+ LEADING(u1)*/ --DRIVING_SITE(bsbsi)
          cpi.productdescription
         ,cpe.status
         ,u1.serviceinstanceid
         FROM   ccsowner.bsbserviceinstance        bsbsi
               ,ccsowner.bsbportfolioproduct       pp
               ,ccsowner.bsbcustomerproductelement cpe
               ,refdatamgr.bsbcatalogueproduct     cpi
               ,ccsowner.bsbsubscription           sub
               ,DATAPROV.utiltrain_data                                  u1
         WHERE  u1.serviceinstanceid = bsbsi.parentserviceinstanceid
         AND    bsbsi.serviceinstancetype IN ('610', '620')
         AND    pp.serviceinstanceid = bsbsi.id
         AND    cpe.portfolioproductid = pp.id
         AND    pp.catalogueproductid = cpi.id
         AND    pp.subscriptionid = sub.id
         AND    sub.subscriptiontypeid = '11';

      TYPE si_tab IS TABLE OF c_si%ROWTYPE;

      l_si_tab si_tab;

   BEGIN
      --DATAPROV.JOBUTILS.OPENJOBLOG(I_JOBINSTANCEID, g_jobname || ' : MOBILEADDON.', g_logid, g_result, g_message, g_rawerror);

      OPEN c_si;
      LOOP
         FETCH c_si BULK COLLECT
            INTO l_si_tab LIMIT C_LIMIT;

         FORALL i IN 1 .. l_si_tab.count
            UPDATE DATAPROV.utiltrain_data u
            SET    u.mobaddon       = l_si_tab(i).productdescription
                  ,u.mobaddonstatus = l_si_tab(i).status
            WHERE  u.serviceinstanceid = l_si_tab(i).serviceinstanceid;

         EXIT WHEN c_si%NOTFOUND;
      END LOOP;
      CLOSE c_si;

      INSERT INTO DATAPROV.UTILTRAIN_PACKAGES
         (PACKAGE_NAME, PACKAGE_COUNTRY, PACKAGE_TYPE)
         SELECT --/*+DRIVING_SITE(bsbcp)*/
         DISTINCT bsbcp.productdescription
                 ,bsbpr.currencycode
                 ,'MOBILEADDON'
         FROM   refdatamgr.bsbcatalogueproduct      bsbcp
               ,refdatamgr.bsbcatalogueproductprice bsbpr
         WHERE  bsbcp.salesstatus != 'EXP'
         AND    bsbcp.subscriptiontype = '11'
         AND    bsbcp.id = bsbpr.catalogueproductid
         AND    bsbpr.enddate IS NULL;

      INSERT INTO DATAPROV.UTILTRAIN_STATUSES
         (STATUSCODE, STATUSDESC, PACKAGE_TYPE)
         SELECT code
               ,p.codedesc
               ,'MOBILEADDON'
         FROM   refdatamgr.picklist p
         WHERE  codegroup = 'CustomerMobileRecurringAddOnProductElementStatus';

      --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 1, NULL, g_result, g_message, g_rawerror);
   END MOBILE_ADDON;

   /***********************************************/

   PROCEDURE UTILTRAIN_ET(I_JOBINSTANCEID IN varchar2) IS

   BEGIN

      REMOVE_UNUSED_RECORDS(I_JOBINSTANCEID);
      RESET_LOCKED_RECORDS(I_JOBINSTANCEID);
      INITIAL_LOAD(I_JOBINSTANCEID);
      DTV(I_JOBINSTANCEID);
      BB(I_JOBINSTANCEID);
      TALK(I_JOBINSTANCEID);
/*      HDBOX(I_JOBINSTANCEID);
      INDIRECTHDBOX(I_JOBINSTANCEID);
      HDPACK(I_JOBINSTANCEID);
      SPORTS(I_JOBINSTANCEID);
      LIMA_SPORTS(I_JOBINSTANCEID);
      MOVIES(I_JOBINSTANCEID);
      LIMA_CINEMA(I_JOBINSTANCEID);
      LIMA_KIDS(I_JOBINSTANCEID);
      LIMA_BOXSETS(I_JOBINSTANCEID);
      ALACARTE(I_JOBINSTANCEID);
      FREESAT(I_JOBINSTANCEID);
      HOMEMOVE(I_JOBINSTANCEID);
      CANCELHOMEMOVE(I_JOBINSTANCEID);
      ORIGINAL(I_JOBINSTANCEID);
      VARIETY(I_JOBINSTANCEID);
      FAMILY(I_JOBINSTANCEID);
      TALKPACKAGE(I_JOBINSTANCEID);
      ENGINEERVISITBOOKED(I_JOBINSTANCEID);
      ACCOUNTACTIVE(I_JOBINSTANCEID);
      ACCOUNTBLOCKED(I_JOBINSTANCEID);
      BUYTOKEEP(I_JOBINSTANCEID);
      SKYGOEXTRA(I_JOBINSTANCEID);
      MULTISCREEN(I_JOBINSTANCEID);
      SKYQ(I_JOBINSTANCEID);
      SKYQMINI(I_JOBINSTANCEID);
      MOBILE(I_JOBINSTANCEID);
      MOBILE_TARIFF(I_JOBINSTANCEID);
      MOBILE_ADDON(I_JOBINSTANCEID);*/
   EXCEPTION
      WHEN OTHERS THEN
         g_message := SQLERRM;
         --DATAPROV.JOBUTILS.CLOSEJOBLOG(g_logid, 0, g_message, g_result, g_message, g_rawerror);
         RAISE_APPLICATION_ERROR(-20999, 'UTIL training Dash ET Failed : ' || g_message);

   END UTILTRAIN_ET;

END etl_utiltrain;
/
