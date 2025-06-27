###################################################################################################################
## Script			: ConfluenceReports.pl
## Author			: RFA
## Creation Date	: 10/01/2023
## Description		: This scripts will generate the Confluence Reports comparing 2 given Test IDs
##					  The environment list to be done would be driven by TEST_REUSLTS_DB_STATS
##                    The logic of what DBs to compare will be driven within the respective PL-SQL procedures 
##
##
###################################################################################################################

use strict;
use Data::Dumper;
use Getopt::Long;
use File::Basename;
use POSIX qw(strftime);
use Cwd qw(cwd);

use vars qw($ENV $DEBUG $HELP);
my $DBFILE = "etc/databases.txt";
my %DBCONFIG = ();
my $DEBUG=0;
my $connHPDIAG ; 	
 

my $PROGNAME = basename($0);
my $usage = qq(
SUMMARY
  Program to generate core Oracle AWR reports.
  -dbenv            - Environment ( N01 , N02 , N61 , N71 )
  -debug            - additional debug output
  -help             - This usage message
);

GetOptions(
   'dbenv=s'     => \$ENV,  
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


if($DEBUG)
{
  print "\nInbound params ...\n\n";
  print "$ENV\n";
  print "$DEBUG\n";
}



open my $out , '>>' , 'check_dataprov_invalid_objects.txt' or die "Can't write check_dataprov_invalid_objects.txt: $!" ;

close $out ;


## Get Main Connection details to the HP_DIAG repository - Stores it in $connHPDIAG
doGetHPDIAG_Conn() ;


## make sure the environment name is in upper case for comparisons
$ENV=~s/[a-z]/[A-Z]/g;

if (defined($ENV) && $ENV ne '')
{
    print "\nConnecting to HP_DIAG repository \n\n";
    callProc();
}
else
{
  print "\ENV parameter is empty or not defined\n\n";;
}


## *******************************************************************************************************************************
## Sub name    : callProc
## *******************************************************************************************************************************
sub callProc
{
  ##my ($dbname) = @_ ;
  my ($cmd) = "sqlplus -s $connHPDIAG \@check_dataprov_invalid_objects.sql $ENV  " ;
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
### (end of file) ###