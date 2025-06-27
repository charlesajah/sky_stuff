use strict;
use Data::Dumper;
use Getopt::Long;
use File::Basename;
use POSIX qw(strftime);

my $g_dirname = strftime ( '%Y%m%d%H%M%S' , localtime ) ;

print "$g_dirname\n";