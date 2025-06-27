###################################################################################################################
## Script			: manage_tablespace.pl
## Author			: Charles 
## Creation Date	: 18/10/2024
## Description		: This scripts will take in 3 inputs as arguments which are: database, tablespace and reclaim amount.
##					  This is used to reclaim space from a tablespace that has more than it needs so filesystem space can be 
##                    allocated to another tablespace that neeeds it most.
##
##
###################################################################################################################

use strict;
use Data::Dumper;
use Getopt::Long;
use File::Basename;

use vars qw($DBENV $DATABASE $TABLESPACE $RECLAIM $DEBUG $HELP);
my $DBFILE = "etc/database_conn.txt";
my %DBCONFIG = ();
my $DEBUG=0;
my $conn = "TCC021N";
my $connHPDIAG ; 	

my $PROGNAME = basename($0);
my $usage = qq(
SUMMARY
  Program to connect just to the TCC021N database.
  -database         - Database Name 
  -tablespace       - Tablespace name
  -reclaim          - amount in MB to be reclaimed
  -debug            - additional debug output
  -help             - This usage message
);

GetOptions(
   'database=s'     => \$DATABASE,  
   'tablespace=s'     => \$TABLESPACE,  
   'reclaim=s'    => \$RECLAIM,
   'debug=s'     => \$DEBUG,
   'help'        => \$HELP
) or exit 2;

if ($HELP)
{
  warn $usage;
  exit 0;
} 
elsif ($DATABASE eq "")
{
  print "\nERROR: Missing parameters .....\n\n";
  warn $usage;
  exit 1;
}
elsif ($TABLESPACE eq "")
{
  print "\nERROR: Missing parameters .....\n\n";
  warn $usage;
  exit 1;
}
elsif ($RECLAIM eq "")
{
  print "\nERROR: Missing parameters .....\n\n";
  warn $usage;
  exit 1;
}

print "\n Database is $DATABASE and Tablespace is $TABLESPACE and RECLAIM value is $RECLAIM\n\n";
if($DEBUG)
{
  print "\nInbound params ...\n\n";
  print "$DATABASE\n";
  print "$TABLESPACE\n";
  print "$RECLAIM\n";
  print "$DEBUG\n";
}


## Write first two lines into top5_charts.txt - other lines spool append from top_five.sql individually for each database.
#open my $out , '>>' , 'top5_charts.txt' or die "Can't write top5_charts.txt: $!" ;
#print $out "h3. Database Top 5 SQLs per snapshot interval\n" ;
#close $out ;


## Get Main Connection details to the HP_DIAG repository - Stores it in $connHPDIAG
doGetHPDIAG_Conn() ;


## make sure the environment name is in upper case for comparisons
$DATABASE=~s/[a-z]/[A-Z]/g;
$TABLESPACE=~s/[a-z]/[A-Z]/g;
$RECLAIM=~s/[a-z]/[A-Z]/g;

if (defined($RECLAIM) && $RECLAIM ne '')
{
	  print "\nConnecting to TCC021N\n\n";
	  callProc();
	  
  }
else 
{
  print "\RECLAIM parameter is empty or not defined\n\n";;
}

## *******************************************************************************************************************************
## Sub name    : doRunReport
## Params      : Database Environment  - takes in the DATABASE NAME as parameter  just for connection purposes
##               
## *******************************************************************************************************************************
sub callProc
{
  #my ($conn) = @_ ;
  print "\nConnecting to the central repo in TCC021N\n\n" ;
  my ($cmd) = "sqlplus -s $connHPDIAG \@manage_tablespace.sql $DATABASE $TABLESPACE $RECLAIM " ;
  system("${cmd}");
  print "Done\n";
}

## *******************************************************************************************************************************
## Sub name    : doGetHPDIAG_Conn
## Params      : none
## Description : get the DB conection to the main HP_DIAG dataabase in TCC021N
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
##             : $dbenv     : Environment we want a connection for
## Description : Look in the supplied file for a connection for the requested enviroment. Return a hash of connections details
##               or abort if none found
## *******************************************************************************************************************************
sub getDBConfig
{
  my ($dbfile,$dbenv) = @_;
  my %dbconfig=();
  open(INFILE,$dbfile) or die "ERROR  : Failed to open database connection file '$dbfile'";
  my @data=<INFILE>;
  close(INFILE);
  #print "\nAbout to grep for $dbenv in @data\n" if($DEBUG);
  @data=grep(/^$dbenv,/,@data);
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
    die "ERROR: Failed to identify a connection string for TCC021N";
  } 
  return %dbconfig;
}
### (end of file) ###