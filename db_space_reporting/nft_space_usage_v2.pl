###################################################################################################################
## Script			: nft_space_usage_v2.pl
## Author			: RFA
## Creation Date	: 06/03/2024
## Description		: Confluence Report on Space usage for all DBs
##
###################################################################################################################
use strict;
use Data::Dumper;
use Getopt::Long;
use File::Basename;
use DBI;
use DBD::Oracle qw(:ora_types);

use vars qw($ENV $GRPNAME);
my $DBFILE = "etc/databases.txt";
my %DBCONFIG = ();
my $DBH;
my $DEBUG=0;

my $PROGNAME = basename($0);
my $usage = qq(
SUMMARY
  Program to generate store space info.
  
  -env              - The environment we wish to report on 
  -grpname          - The name of the DB grouping list
);

GetOptions(
   'env=s'              => \$ENV,
   'grpname=s'          => \$GRPNAME,
) or exit 2;

if ($ENV eq "")
{
  print "\nERROR: Missing parameters .....\n\n";
  warn $usage;
  exit 1;
}
elsif ($GRPNAME eq "")
{
  print "\nERROR: Missing parameters .....\n\n";
  warn $usage;
  exit 1;
}

# make sure the environment name is in upper case for comparisons
$ENV=~s/[a-z]/[A-Z]/g;
$GRPNAME=~s/[a-z]/[A-Z]/g;

# Call subroutines. N01_TCC contains the current HP_DIAG repository for the centralised solution
doReport('N01-TCC');


# *******************************************************************************************************************************
# Sub name    : doReport
# Params      : 
# Description : Creates the TEXT files which will be displayed within the Confluence page
# *******************************************************************************************************************************
sub doReport
{
  my ($dbdesc) = @_ ;
  print "\n$dbdesc Space Report\n\n" ;
  # extract requested environment and connect to the database
  %DBCONFIG = getDBConfig($DBFILE,qq($dbdesc)) ;
  $DBH=doDBConnection(%DBCONFIG) ;
  
  my ($cmd) = "sqlplus -s $DBCONFIG{USER}/$DBCONFIG{PASSWORD}\@$DBCONFIG{SID} \@nft_space_allocation_v2.sql $ENV $GRPNAME ";
  system("${cmd}");

  print "Done\n";
}
# *******************************************************************************************************************************
# Sub name    : doDBConnection
# Params      : %conn : Connection details
# Description : Unused - Connect to the specified database 
# *******************************************************************************************************************************

sub doDBConnection
{
  my (%conn)=@_;

  print "\nAbout to connect $conn{CONN} \n" if($DEBUG);
  
  return DBI->connect("dbi:Oracle:" . $conn{CONN},$conn{USER},$conn{PASSWORD}, {PrintError => 1, RaiseError => 0 }) or die "ERROR : Failed to connect to database";
}

# *******************************************************************************************************************************
# Sub name    : doDBDisconnect
# Params      : $dbh : Database connection
# Description : Disconnect from the DPS database 
# *******************************************************************************************************************************

sub doDBDisconnect
{
  my ($dbh)=@_;

  print "\nAbout to disconnect $dbh \n" if($DEBUG);
  
  $dbh->disconnect();
}

# *******************************************************************************************************************************
# Sub name    : getDBConfig
# Params      : $dbfile  : File containing database connection details
#             : $env     : Environment we want a connection for
# Description : Look in the supplied file for a connection for the requested enviroment. Return a hash of connections details
#               or abort if none found
# *******************************************************************************************************************************

sub getDBConfig
{
  my ($dbfile,$env)=@_;
  
  my %dbconfig=();
  
  open(INFILE,$dbfile) or die "ERROR  : Failed to open database connection file '$dbfile'";
  my @data=<INFILE>;
  close(INFILE);
  
  print "\nAbout to grep for $env in @data\n" if($DEBUG);
  
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
  }
  else
  {
    die "ERROR: Failed to identify a connection string for environment '$env'";
  }
  
  return %dbconfig;
}