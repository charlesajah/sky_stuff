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
use Scalar::Util qw(looks_like_number);

use vars qw($ENV1 $TEST_ID1 $ENV2 $START $END $DESC $DEBUG $HELP);
my $DBFILE = "etc/databases.txt";
my %DBCONFIG = ();
my $GRP ="FULL";
my $DBNAME ="''";
my $LABEL = "in_flight";
my $FLAG = 0 ; 
my $RETAIN = "N";
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
   'dbenv1=s'     => \$ENV1,  
   'test_id_1=s'  => \$TEST_ID1,
   'dbenv2=s'     => \$ENV2,
   'start_time=s' => \$START,
   'end_time=s'   => \$END,
   'desc=s' => \$DESC,
   'debug=s'     => \$DEBUG,
   'help'        => \$HELP
) or exit 2;

if ($HELP)
{
  warn $usage;
  exit 0;
} 
elsif ($ENV1 eq "")
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
elsif ($ENV2 eq "")
{
  print "\nERROR: Missing parameters .....\n\n";
  warn $usage;
  exit 1;
}
elsif ($START eq "")
{
  print "\nERROR: Missing parameters .....\n\n";
  warn $usage;
  exit 1;
}
elsif ($END eq "")
{
  print "\nERROR: Missing parameters .....\n\n";
  warn $usage;
  exit 1;
}



if($DEBUG)
{
    print "\nInbound params ...\n\n";
    print "$ENV1\n";
    print "$TEST_ID1\n";
    print "$ENV2\n";
    print "$START\n";
    print "$END\n";
    print "$DESC\n";
    print "$DEBUG\n";
}

#we trim both ends of the strings for each var
for my $var (\$ENV1, \$TEST_ID1, \$ENV2, \$START, \$END, \$DESC) {
  $$var =~ s/^\s+|\s+$//g;
}

if (!looks_like_number($FLAG)) {
  die "Error: FLAG must be a numeric value, but received '$FLAG'";
}

#open my $out , '>>' , 'in_flight_sql_per.out' or die "Can't write in_flight_sql_per.out: $!" ;

#close $out ;


## Get Main Connection details to the HP_DIAG repository - Stores it in $connHPDIAG
doGetHPDIAG_Conn() ;


## make sure the environment name is in upper case for comparisons
$ENV1=~s/[a-z]/[A-Z]/g;
$ENV2=~s/[a-z]/[A-Z]/g;

##make sure the TEST_ID is in upper case
$TEST_ID1=~s/[a-z]/[A-Z]/g;

##make sure start and end time are in upper case
$START=~s/[a-z]/[A-Z]/g;
$END=~s/[a-z]/[A-Z]/g;

if (defined($ENV1) && $ENV1 ne '' && defined($ENV2) && $ENV2 ne '')
{
    print "\nConnecting to HP_DIAG repository \n\n";
    callProc();
}
else
{
  print "\ENV parameter is empty or not defined\n\n";;
}

#print "\nSTART: $START END: $END DESC: $DESC ENV2: $ENV2 GRP: $GRP DBNAME: $DBNAME LABEL: $LABEL FLAG: $FLAG RETAIN: $RETAIN \n\n" ;
#debug
print "\nSTART: $START (Length: " . length($START) . 
      ") END: $END (Length: " . length($END) . 
      ") DESC: $DESC (Length: " . length($DESC) . 
      ") ENV2: $ENV2 (Length: " . length($ENV2) . 
      ") GRP: $GRP (Length: " . length($GRP) . 
      ") DBNAME: $DBNAME (Length: " . length($DBNAME) . 
      ") LABEL: $LABEL (Length: " . length($LABEL) . 
      ") FLAG: $FLAG (Length: " . length($FLAG) . 
      ") RETAIN: $RETAIN (Length: " . length($RETAIN) . ")\n\n";


## *******************************************************************************************************************************
## Sub name    : callProc
## *******************************************************************************************************************************
sub callProc
{
  ##my ($dbname) = @_ ;
  #we wrap $DESC in quotes and escape them, so as to avoid the processing of individual words in $DESC as separate argument values
  my ($cmd) = "sqlplus -s $connHPDIAG \@AWR_Data_Gathering_Test.sql $START $END \"$DESC\" $ENV2 $GRP $DBNAME $LABEL $FLAG $RETAIN $TEST_ID1" ;
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