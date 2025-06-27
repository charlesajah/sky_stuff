###################################################################################################################
## Script			: manage_tablespace.pl
## Author			: Charles 
## Creation Date	: 18/10/2024
## Description		: This scripts will take in 3 inputs as arguments which are: database, tablespace and reclaim amount.
##					        This is used to reclaim space from a tablespace thereby freeing up space in the data filesystem 
##                  which can then be allocated to another tablespace that needs it most.
##
##
###################################################################################################################

use strict;
use Data::Dumper;
use Getopt::Long;
use File::Basename;

use vars qw($DATABASE $TABLESPACE $RECLAIM $DEBUG $HELP);
my $DBFILE = "etc/databases.txt";
my %DBCONFIG = ();
my $DEBUG=0;
my $connHPDIAG ; 	

my $PROGNAME = basename($0);
my $usage = qq(
SUMMARY
  Program to connect just to the database and modify tablespace sizes.
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

print "\nDatabase => $DATABASE | Tablespace => $TABLESPACE | RECLAIM value => $RECLAIM\n\n";
if($DEBUG)
{
  print "\nInbound params ...\n\n";
  print "$DATABASE\n";
  print "$TABLESPACE\n";
  print "$RECLAIM\n";
  print "$DEBUG\n";
}

#fetch the DB conn name from $DBFILE
my $conn_name = getDBConnName($DBFILE, $DATABASE);
print "The Connection name for $DATABASE is: $conn_name\n";
## Get Main Connection details to HP_DIAG - Stores it in $connHPDIAG
doGetHPDIAG_Conn() ;


## make sure the environment name is in upper case
$DATABASE=~s/[a-z]/[A-Z]/g;
$TABLESPACE=~s/[a-z]/[A-Z]/g;
$RECLAIM=~s/[a-z]/[A-Z]/g;

if (defined($RECLAIM) && $RECLAIM ne '')
{
    print "\nConnecting to $DATABASE\n\n";
    callProc($DATABASE);
}
else
{
  print "\RECLAIM parameter is empty or not defined\n\n";;
}

## *******************************************************************************************************************************
## Sub name    : callProc
## Params      : Database name - takes in the DATABASE NAME as parameter and once connected executes the sql script
##               
## *******************************************************************************************************************************
sub callProc
{
  #my ($database) = @_ ;
  #print "\nConnecting to the central repo in TCC021N\n\n" ;
  my ($cmd) = "sqlplus -s $connHPDIAG \@manage_tablespace.sql $DATABASE $TABLESPACE $RECLAIM " ;
  system("${cmd}");
  print "Done\n";
}

## *******************************************************************************************************************************
## Sub name    : doGetHPDIAG_Conn
## Params      : none
## Description : get the DB conection to $DATABASE
## 
## *******************************************************************************************************************************
sub doGetHPDIAG_Conn
{
  %DBCONFIG = getDBConfig($DBFILE,$conn_name);    
  $connHPDIAG = "$DBCONFIG{USER}/$DBCONFIG{PASSWORD}\@$DBCONFIG{SID}" ;	
  print "HP_DIAG Connection string : $connHPDIAG/n " if($DEBUG); ;
}


## *******************************************************************************************************************************
## Sub name    : getDBConfig
## Params      : $dbfile  : File containing database connection details
##             : $dbenv     : Environment we are connecting to.
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
    die "ERROR: Failed to identify a connection string for $DATABASE";
  } 
  return %dbconfig;
}

sub getDBConnName {
    my ($dbfile, $database) = @_;
    my $conn_name = "";

    open(INFILE, $dbfile) or die "ERROR: Failed to open database connection file '$dbfile'";
    while (my $line = <INFILE>) {
        chomp($line);

        # Skip lines that contain a hash (#) anywhere
        next if $line =~ /#/;

        # Check if $DATABASE appears anywhere on the line
        #if ($line =~ /\b\Q$database\E\b/) { this originally matched whole string for databases

        #now match not just whole string but also partial matches followed by the _RW string
        if ($line =~ /\(SERVICE_NAME=\Q$database\E(_RW)?\)/) {
            # Split the line by commas
            my @fields = split /,/, $line;

            # Get the first column value
            $conn_name = $fields[0];
            last;  # Stop after finding the first match
        }
    }
    close(INFILE);

    # Return the first column value
    return $conn_name;
}
### (end of file) ###