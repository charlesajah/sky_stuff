CREATE OR REPLACE PACKAGE            BSB_SCHEMAINFO IS
   /******************************************************************************************
     Author:  David Wilkie
     $Revision: 429 $
     $Date: 2010-01-20 22:31:58 +0000 (Wed, 20 Jan 2010) $

     Usage:
     A generic package to hold schema build information, for calling via view (V_BSBSchemaInfo).
     The package is installed into every schema.

     Change History:
     Date      Initials  Coments
     --------  --------  -------------------------------------------------------------------
     19/01/10  DWI52     Initial Version

   ******************************************************************************************/

   FUNCTION f_schemaname RETURN VARCHAR2;

   FUNCTION f_componentname RETURN VARCHAR2;

   FUNCTION f_buildnumber RETURN VARCHAR2;

   FUNCTION f_builddate RETURN DATE;

   FUNCTION f_releaseversion RETURN VARCHAR2;

   FUNCTION f_scriptname RETURN VARCHAR2;

END;
/


CREATE OR REPLACE PACKAGE BODY            BSB_SCHEMAINFO IS
   /******************************************************************************************
     Author:  David Wilkie
     $Revision: 429 $
     $Date: 2010-01-20 22:31:58 +0000 (Wed, 20 Jan 2010) $

     Usage:
     A generic package to hold schema build information, for calling via view (V_BSBSchemaInfo).
     The build information is held as placeholders, and will be substituted by the ant build
     script during the build process.  The data is returned via functions into a view for easy
     querying by monitoring software/support staff.

     Change History:
     Date      Initials  Coments
     --------  --------  -------------------------------------------------------------------
     19/01/10  DWI52     Initial Version

   ******************************************************************************************/

   l_schemaname     VARCHAR2(255) := 'DATAPROV';
   l_componentname  VARCHAR2(255) := 'DB_CCS';
   l_buildnumber    VARCHAR2(255) := '117.3.80-SNAPSHOT';
   l_builddate      VARCHAR2(255) := '20141126-161757';
   l_releaseversion VARCHAR2(255) := '30257';
   l_scriptname     VARCHAR2(255) := '30257';

   l_dateformat VARCHAR2(255) := 'DD-MM-YYYY HH24:MI:SS';

   FUNCTION is_number(x IN VARCHAR2) RETURN BOOLEAN IS
   BEGIN
      RETURN to_number(x) IS NOT NULL;
   EXCEPTION
      WHEN OTHERS THEN
         RETURN FALSE;
   END is_number;

   FUNCTION is_date(x IN VARCHAR2) RETURN BOOLEAN IS
   BEGIN
      RETURN TO_DATE(x, l_dateformat) IS NOT NULL;
   EXCEPTION
      WHEN OTHERS THEN
         RETURN FALSE;
   END is_date;

   FUNCTION f_schemaname RETURN VARCHAR2 IS
   BEGIN
      RETURN l_schemaname;
   END f_schemaname;

   FUNCTION f_componentname RETURN VARCHAR2 IS
   BEGIN
      RETURN l_componentname;
   END f_componentname;

   FUNCTION f_buildnumber RETURN VARCHAR2 IS
   BEGIN
      RETURN l_buildnumber;
   END f_buildnumber;

   FUNCTION f_builddate RETURN DATE IS
   BEGIN
      IF is_date(l_builddate) THEN
         RETURN to_date(l_builddate,l_dateformat);
      ELSE
         RETURN TO_DATE('01011900', 'DDMMYYYY');
      END IF;
   END f_builddate;

   FUNCTION f_releaseversion RETURN VARCHAR2 IS
   BEGIN
      RETURN l_releaseversion;
   END f_releaseversion;

   FUNCTION f_scriptname RETURN VARCHAR2 IS
   BEGIN
      RETURN l_scriptname;
   END f_scriptname;

END;
/
