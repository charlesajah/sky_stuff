###################################################################################################################
## Script			: ConfluenceReports.pl
## Author			: RFA
## Creation Date	: 10/01/2023
## Description		: This scripts will generate the Confluence Reports comparing 2 given Test IDs
##					  The environment list to be done would be driven by TEST_REUSLTS_DB_STATS
##                    The logic of what DBs to compare will be driven within the respective PL-SQL procedures 
##
## Changes 
## ----------------------
## 24/07/2024	RFA : Adding modification to adapt the cs ript for the new Central Repositiry
## 21/08/2024   RFA : Adding parameter $DBNAME to allow for single database report 
##                    Commented out doLogReport and doLargeObjects as we're not using any of them
## 30/08/24     RFA : Remove the need to access individual databases to produce the DB Activity graphs.
##                    Change the old coded names used for connectivity to DBNAMES instead as we now need that parameter
##                    The only use of the DB connectivity thru the etc/database file is to the actual Central Repository
##                    doLogReport and doLargeObjects have now been removed.
## 02/09/24     RFA : Replaces all the SQL calls into just one SQL file to generate most of the information that do not require 
##                    an specific database as a parameter.
##                    It does not require to append lst or txt files anymore.
##
###################################################################################################################

use strict;
use Data::Dumper;
use Getopt::Long;
use File::Basename;
use POSIX qw(strftime);
use Cwd qw(cwd);

use vars qw($TEST_ID1 $TEST_ID2 $ENV $GRP $DBNAME $DESC $DEBUG $HELP);
my $DBFILE = "etc/databases.txt";
my %DBCONFIG = ();
my $DEBUG=0;
my $g_dirname = strftime ( '%Y%m%d%H%M%S' , localtime ) ;
my $dir = cwd;
my $connHPDIAG ; 	
 
print "$dir\n";

my $PROGNAME = basename($0);
my $usage = qq(
SUMMARY
  Program to generate core Oracle AWR reports.
  
  -test_id1         - First Test ID to compare - format DDMONYY-HH24MI_DDMONYY-HH24MI e.g. 16OCT23-0630_16OCT23-0900 
  -test_id2         - Second Test ID to compare - format DDMONYY-HH24MI_DDMONYY-HH24MI e.g. 17OCT23-0630_17OCT23-0900 
  -dbenv            - Environment ( N01 , N02 , N61 , N71 )
  -dbgrp            - Group of databases to report upon ( FULL, CORE , KNFX , SINGLE-DB)
  -dbname           - Database Name ( if provided )
  -desc             - Description of this report
  -debug            - additional debug output
  -help             - This usage message
);

GetOptions(
   'test_id1=s'  => \$TEST_ID1,
   'test_id2=s'  => \$TEST_ID2,
   'dbenv=s'     => \$ENV,  
   'dbgrp=s'     => \$GRP,  
   'dbname=s'    => \$DBNAME,
   'desc=s'      => \$DESC,   
   'debug=s'     => \$DEBUG,
   'help'        => \$HELP
) or exit 2;

if ($HELP)
{
  warn $usage;
  exit 0;
} 
elsif ($ENV eq "")
{
  print "\nERROR: Missing parameters .....\n\n";
  warn $usage;
  exit 1;
}
elsif ($GRP eq "")
{
  print "\nERROR: Missing parameters .....\n\n";
  warn $usage;
  exit 1;
}
elsif ($TEST_ID1 eq "")
{
  print "\nERROR: Missing parameters .....\n\n";
  warn $usage;
  exit 1;
}
elsif ($TEST_ID2 eq "")
{
  print "\nERROR: Missing parameters .....\n\n";
  warn $usage;
  exit 1;
}

if($DEBUG)
{
  print "\nInbound params ...\n\n";
  print "$TEST_ID1\n";
  print "$TEST_ID2\n";
  print "$ENV\n";
  print "$GRP\n";
  print "$DBNAME\n";
  print "$DESC\n";
  print "$DEBUG\n";
}


## Write first two lines into CentralDBActivity.txt  - other lines spool append from CentralDBActGraph.sql individually for each database.
open my $out , '>>' , 'CentralDBActivity.txt' or die "Can't write CentralDBActivity.txt: $!" ;
print $out "h3. Database Activity Charts\n" ;
close $out ;

##Write the confluence list to be used for Centralsqltrend report page
open my $out , '>>' , 'CentralSqlTrend.txt' or die "Can't write CentralSqlTrend.txt: $!" ;
print $out "{toc:type=list}\n";
print $out "h3. Top Five SQL.\n";
#print $out "Start Date: $l_start\n";
#print $out "End Date: $l_end\n";
print $out "\n";
print $out "This section displays the top 5 SQLs per AWR interval for the current test.\n" ;
print $out "The legend(key) value is represented as a concatenation of SQL_ID and the PLAN HASH VALUE separated by an underscore character.\n" ;
print $out "This information is only produced for relevant databases and not all databases.\n" ;
print $out "\n";
close $out ;



## Get Main Connection details to the HP_DIAG repository - Stores it in $connHPDIAG
doGetHPDIAG_Conn() ;


## make sure the environment name is in upper case for comparisons
$ENV=~s/[a-z]/[A-Z]/g;
$GRP=~s/[a-z]/[A-Z]/g;
$DBNAME=~s/[a-z]/[A-Z]/g;
if ($ENV eq "N01") 
{
  if ($GRP eq "FULL")
  {  
	  print "\nRunning FULL N01 report dump\n\n";
	  doRunReport('ASU011N');
	  doRunReport('AUC011N');
	  doRunReport('CAS011N');
	  doRunReport('CGS011N');
	  doRunReport('CHORDO');
	  doRunReport('CDD011N');
	  doRunReport('CPP011N'); # CPP
	  doRunReport('CSC011N');
	  doRunReport('DAC011N');
	  doRunReport('DCU011N');
	  doRunReport('DCU012N');
	  doRunReport('DCU013N');
	  doRunReport('DCU014N');
	  doRunReport('DCU015N');
	  doRunReport('DCU016N');
	  doRunReport('DFS011N');
	  doRunReport('DPSO');
	  doRunReport('EGS011N');
	  doRunReport('FUL011N');   # FUL	  
	  doRunReport('IFS011N');
	  doRunReport('IGR011N');
	  doRunReport('ISS011N');
	  doRunReport('JBP011N');
	  doRunReport('MER011N');
	  doRunReport('OGS011N');
	  doRunReport('OMS011N');
	  doRunReport('OPR011N');
	  doRunReport('PCS011N');  
	  doRunReport('PGR011N');  # PGR	  
	  doRunReport('PIN011N');	  
	  doRunReport('RIS011N');
	  doRunReport('SLT011N');
	  doRunReport('TCC011N');
	  doRunReport('FIC091N');
	  doRunReport('ULM091N');	  
	  doRunReport('DRP041G');
	  doRunReport('DRP042G');	  
	  ##doRunReport('N01-CDR');
	  ##doRunReport('N01-DDR');

    print "\nRunning CORE N01 Top Five SQL Trend routine\n\n";
    doTopFive('CHORDO');
    doTopFive('AUC011N');
    doTopFive('DPSO');
    doTopFive('ISS011N');
    doTopFive('PGR011N');  # PGR
    doTopFive('OMS011N');
    doTopFive('FUL011N');   # FUL
    doTopFive('IFS011N');
    doTopFive('DAC011N');
    doTopFive('DCU011N');
    doTopFive('DCU012N');
    doTopFive('DCU013N');
    doTopFive('DCU014N');
    doTopFive('DCU015N');
    doTopFive('DCU016N');
  }
  elsif ($GRP eq "CORE") 
  {
	  print "\nRunning CORE N01 report dump\n\n";
	  doRunReport('CHORDO');
	  doRunReport('AUC011N');
	  doRunReport('DPSO');
	  doRunReport('ISS011N');
	  doRunReport('PGR011N');  # PGR
	  doRunReport('OMS011N');
	  doRunReport('FUL011N');   # FUL
	  doRunReport('IFS011N');
	  doRunReport('DAC011N');
	  doRunReport('DCU011N');
	  doRunReport('DCU012N');
	  doRunReport('DCU013N');
	  doRunReport('DCU014N');
	  doRunReport('DCU015N');
	  doRunReport('DCU016N');

    print "\nRunning CORE N01 Top Five SQL Trend routine\n\n";
    doTopFive('CHORDO');
    doTopFive('AUC011N');
    doTopFive('DPSO');
    doTopFive('ISS011N');
    doTopFive('PGR011N');  # PGR
    doTopFive('OMS011N');
    doTopFive('FUL011N');   # FUL
    doTopFive('IFS011N');
    doTopFive('DAC011N');
    doTopFive('DCU011N');
    doTopFive('DCU012N');
    doTopFive('DCU013N');
    doTopFive('DCU014N');
    doTopFive('DCU015N');
    doTopFive('DCU016N');
  }
  elsif ($GRP eq "KNFX") 
  {
	  print "\nRunning KENAN N01 report dump\n\n";
	  doRunReport('DAC011N');
	  doRunReport('DCU011N');
	  doRunReport('DCU012N');
	  doRunReport('DCU013N');
	  doRunReport('DCU014N');
	  doRunReport('DCU015N');
	  doRunReport('DCU016N');

    print "\nRunning KENAN N01 SQL Trend routine\n\n";
	  doTopFive('DAC011N');
	  doTopFive('DCU011N');
	  doTopFive('DCU012N');
	  doTopFive('DCU013N');
	  doTopFive('DCU014N');
	  doTopFive('DCU015N');
	  doTopFive('DCU016N');
  }  
  elsif ($GRP eq "SINGLE-DB") 
  {
	  print "\nRunning SINGLE N01 report dump\n\n";
	  doRunReport($DBNAME);

    print "\nRunning SINGLE N01 SQL Trend routine\n\n";
	  doTopFive($DBNAME);
  }  
}
elsif ($ENV eq "N02")
{
  if ($GRP eq "FULL")
  {  
	  print "\nRunning FULL N02 report dump\n\n";
	  doRunReport('ASU021N');
	  doRunReport('AUC021N');
	  doRunReport('CAS021N');
	  doRunReport('CGS021N');
	  doRunReport('CCS021N');
	  doRunReport('CDD021N');
	  doRunReport('CPP021N'); # CPP
	  doRunReport('CSC021N');
	  doRunReport('DAC021N');
	  doRunReport('DCU021N');
	  doRunReport('DCU022N');
	  doRunReport('DCU023N');
	  doRunReport('DCU024N');
	  doRunReport('DCU025N');
	  doRunReport('DCU026N');
	  doRunReport('DFS021N');
	  doRunReport('DPS021N');
	  doRunReport('EGS021N');
	  doRunReport('FUL021N');   # FUL	  
	  doRunReport('IFS021N');
	  doRunReport('IGR021N');
	  doRunReport('ISS021N');
	  doRunReport('JBP021N');
	  doRunReport('MER021N');
	  doRunReport('OGS021N');
	  doRunReport('OMS021N');
	  doRunReport('OPR021N');
	  doRunReport('PCS021N');  
	  doRunReport('PGR021N');  # PGR	  
	  doRunReport('PIN021N');	  
	  doRunReport('RIS021N');
	  doRunReport('SLT021N');
	  doRunReport('TCC021N');
  }
  elsif ($GRP eq "CORE") 
  {
	  print "\nRunning CORE N02 report dump\n\n";
	  doRunReport('CCS021N');
	  doRunReport('AUC021N');
	  doRunReport('DPS021N');
	  doRunReport('ISS021N');
	  doRunReport('PGR021N');  # PGR
	  doRunReport('OMS021N');
	  doRunReport('FUL021N');   # FUL
	  doRunReport('IFS021N');
	  doRunReport('DAC021N');
	  doRunReport('DCU021N');
	  doRunReport('DCU022N');
	  doRunReport('DCU023N');
	  doRunReport('DCU024N');
	  doRunReport('DCU025N');
	  doRunReport('DCU026N');
  }
  elsif ($GRP eq "KNFX") 
  {
	  print "\nRunning KENAN N02 report dump\n\n";
	  doRunReport('DAC021N');
	  doRunReport('DCU021N');
	  doRunReport('DCU022N');
	  doRunReport('DCU023N');
	  doRunReport('DCU024N');
	  doRunReport('DCU025N');
	  doRunReport('DCU026N');
  }  
  elsif ($GRP eq "SINGLE-DB") 
  {
	  print "\nRunning SINGLE N01 report dump\n\n";
	  doRunReport($DBNAME);
  }   
}
elsif ($ENV eq "N61")
{
  print "\nRunning full N61 report dump\n\n";
  doRunReport('N61-SBP');
  doRunReport('N61-ALPHA');
  doRunReport('N61-CHORDIANT');
  doRunReport('N61-KENAN');
}
elsif ($ENV eq "N05")
{
  print "\nRunning full N05 report dump\n\n";
  doRunReport('N05-SMP-GP');
  doRunReport('N05-SMP-UK');
  doRunReport('N05-SMP-IT');
  doRunReport('N05-SMP-DE');
}
elsif ($ENV eq "N71")
{
  print "\nRunning full N71 report dump\n\n";
  doFixDataN71();
  doRunReport('N71-CAS');
  doRunReport('N71-CMA');
  doRunReport('N71-CUS');
  doRunReport('N71-DPS');
  doRunReport('N71-FUL');
  doRunReport('N71-ISS');
  doRunReport('N71-KAC-KENAN-ADMIN');
  doRunReport('N71-KCU-KENAN-CUS');
  doRunReport('N71-TCC');
}
else 
{
  doRunReport($DBNAME);
}

doTopSql() ;
doCopyFiles() ;

## *******************************************************************************************************************************
## Sub name    : doRunReport
## Params      : Database Environment  - now receiving the DATABASE NAME as parameter 
## Description : Generates the Database Activity report and the HTML files per database.
##               The TXT files will then be posted within the Confluence pages
## *******************************************************************************************************************************
sub doRunReport
{
  my ($dbname) = @_ ;
  print "\n$dbname Report\n\n" ;
  ## Generates file : CentralDBActivity.txt which will include all the Activity Graps per dataabse 
  ## Produces all the CVS files into the work area and then copied to the Web Server
  my ($cmd) = "sqlplus -s $connHPDIAG \@CentralDBActGraph.sql $TEST_ID1 $TEST_ID2 $dbname $g_dirname " ;
  system("${cmd}");
  ## Generates all the HTML files with the AWR reports per database
  ## Procudes all the HTML files into the work area and then copied to the Web Server
  my ($cmd) = "sqlplus -s $connHPDIAG \@CentralAWRReportExtract.sql $TEST_ID1 $dbname " ;
  system("${cmd}");

  print "Done\n";
}


## *******************************************************************************************************************************
## Sub name    : doFixDataN71
## Params      : 
## Description : Fixes missing data due to snap range being missing in a database to ensure graphs are ok
## *******************************************************************************************************************************
sub doFixDataN71
{
  print "Start doFixDataN71\n";
  %DBCONFIG = getDBConfig($DBFILE,'N71-CUS');
  my ($cmd) = "sqlplus -s $DBCONFIG{USER}/$DBCONFIG{PASSWORD}\@$DBCONFIG{SID} \@CentralFixDataN71.sql $TEST_ID1 $TEST_ID2" ;
  system("${cmd}");
  print "Done\n";
}

## *******************************************************************************************************************************
## Sub name    : doCopyFiles
## Params      : 
## Description : Copy html files to wd015506 web server, add hyperlink for each html file to end of chart.txt
## *******************************************************************************************************************************
sub doCopyFiles {
  my @files = <*.html> ;
  if ( @files ) {
    open my $out , '>>' , 'CentralDBAnalysis.txt' or die "Can't write CentralDBAnalysis.txt: $!" ;
    print $out "\n" . "h3. AWR Reports\n" ;
    foreach my $file ( @files ) {
      print $out "* [$file|http://wd015506.bskyb.com:9320/reports/tests/$g_dirname/$file#400]\n" ;
    }
    close $out ;
    # Adding AWR reports to Summary --> AWR do not display in the summary page but the detail pages
	#open my $out , '>>' , 'CentralDBSummary.txt' or die "Can't write CentralDBSummary.txt: $!" ;
    #print $out "\n" . "h3. AWR Reports\n" ;
    #foreach my $file ( @files ) {
    #  print $out "* [$file|http://wd015506.bskyb.com:9320/reports/tests/$g_dirname/$file#400]\n" ;
    #}
    #close $out ;
  }
  print "Copying files to http://wd015506.bskyb.com:9320/reports/tests/$g_dirname/ \n" ;
  system ( "mkdir \\\\wd015506.bskyb.com\\d\$\\TelNum_Reports\\appdata\\reports\\tests\\$g_dirname" ) ;
  system ( "copy /y *.html \\\\wd015506.bskyb.com\\d\$\\TelNum_Reports\\appdata\\reports\\tests\\$g_dirname\\." ) ;
  system ( "copy /y *.csv \\\\wd015506.bskyb.com\\d\$\\TelNum_Reports\\appdata\\reports\\tests\\$g_dirname\\." ) ;
  system ( "copy /y *.txt \\\\wd015506.bskyb.com\\d\$\\TelNum_Reports\\appdata\\reports\\tests\\$g_dirname\\." ) ;
  print "Output at http://wd015506.bskyb.com:9320/reports/tests/$g_dirname/ \n" ;
}


## *******************************************************************************************************************************
## Sub name    : doTopSql
## Params      : 
## Description : Calls the SQL script that generates most of the Information to be displayed within the Confluence Pages
## 
## *******************************************************************************************************************************
sub doTopSql
{
  ## Generates all the information to be displayed on most of the Confluence Report
  ## Includes the Kenan Billing reporting 
  my ($cmd) = "sqlplus -s $connHPDIAG \@CentralDBAnalysisv2.sql $TEST_ID1 $TEST_ID2 \"$DESC\" " ;
  system("${cmd}");
  print "Done\n";
}


## *******************************************************************************************************************************
## Sub name    : doGetHPDIAG_Conn
## Params      : none
## Description : trap the DB conection to the main HP_DIAG dataabase
## 
## *******************************************************************************************************************************
sub doGetHPDIAG_Conn
{
  %DBCONFIG = getDBConfig($DBFILE,'N02-TCC');    
  $connHPDIAG = "$DBCONFIG{USER}/$DBCONFIG{PASSWORD}\@$DBCONFIG{SID}" ;	
  print "HP_DIAG Connection string : $connHPDIAG/n " if($DEBUG); ;
}


## *******************************************************************************************************************************
## Sub name    : getDBConfig
## Params      : $dbfile  : File containing database connection details
##             : $env     : Environment we want a connection for
## Description : Look in the supplied file for a connection for the requested enviroment. Return a hash of connections details
##               or abort if none found
## *******************************************************************************************************************************
sub getDBConfig
{
  my ($dbfile,$env) = @_;
  my %dbconfig=();
  open(INFILE,$dbfile) or die "ERROR  : Failed to open database connection file '$dbfile'";
  my @data=<INFILE>;
  close(INFILE);
  #print "\nAbout to grep for $env in @data\n" if($DEBUG);
  @data=grep(/^$env,/,@data);
  if (scalar @data > 0)
  {
     my $data = $data[0];
     chomp($data);
     my @line=split(/,/,$data);
     $dbconfig{SID}=$line[1];
     $dbconfig{CONN}=$line[2];
     $dbconfig{USER}=$line[3];
     $dbconfig{PASSWORD}=$line[4];
     $dbconfig{DBVERSION}=$line[5];
  }
  else
  {
    die "ERROR: Failed to identify a connection string for environment '$env'";
  } 
  return %dbconfig;
}

## *******************************************************************************************************************************
## Sub name    : doTopFive
## Params      : Takes current TestID , DBNAME and directory name as parameters
## Description : Generates the Top 5 SQLs per snapshot interval per database.
##               
## *******************************************************************************************************************************
sub doTopFive
{
  my ($dbname) = @_ ;
  print "\n$dbname Top5 SQL Report\n\n" ;
  ##print "\n$dbname Checking if this routine was entered\n\n" ;
  ## Generates file : CentralSqlTrend.txt  which will include all the Top5 SQLs per snapshot interval.
  ## Produces all the CVS files into the work area and then copied to the Web Server
  my ($cmd) = "sqlplus -s $connHPDIAG \@top_five_v2.sql $TEST_ID1 $dbname $g_dirname " ; 
  system("${cmd}");
  print "Done\n";
}
### (end of file) ###