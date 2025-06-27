use strict;
use Data::Dumper;
use Getopt::Long;
use File::Basename;
use DBI;
use DBD::Oracle qw(:ora_types);

use vars qw($HELP $SQLID $DAYS);
my $DBFILE = "etc/database_conn.txt";
my %DBCONFIG = ();
my $DBH;
my $DEBUG=0;

my $PROGNAME = basename($0);
my $usage = qq(
SUMMARY
  Program to generate core Oracle AWR reports.
  
  -sqlid            - SQL ID that report is required for
  -days             - Number of days to report on
  -help             - This usage message
);

GetOptions(
   'sqlid=s'            => \$SQLID,
   'days=s'             => \$DAYS,
   'help'               => \$HELP
) or exit 2;

if ($HELP)
{
  warn $usage;
  exit 0;
} 

if($DEBUG)
{
  print "\nInbound params ...\n\n";
  print "$SQLID\n";
  print "$DAYS\n";
}

doSqlHist() ;

# *******************************************************************************************************************************
# Sub name    : doTopSql
# Params      : 
# Description : Generate a csv list of top 10 sql statements cross database
# Changed by MGI18: add chart generation
# *******************************************************************************************************************************
sub doSqlHist
{
  %DBCONFIG = getDBConfig($DBFILE,'N02-TCC');
  $DBH=doDBConnection(%DBCONFIG);
  my ($cmd) = "sqlplus -s $DBCONFIG{USER}/$DBCONFIG{PASSWORD}\@$DBCONFIG{SID} \@sql_perf_report.sql $SQLID $DAYS" ;
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