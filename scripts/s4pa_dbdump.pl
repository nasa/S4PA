#!/usr/bin/perl

=head1 NAME

s4pa_dbdump.pl - a script to dump contents of an MLDBM file containing a two 
level hash to standard output.

=head1 SYNOPSIS

s4pa_dbdump.pl -f <MLDBM filename> [-d delimiter]

=head1 ABSTRACT

B<Pseudo code:>
    Tie a hash to MLDBM file OR exit with a warning.
    For each key of the hash
        print
    Endfor
    End

=head1 DESCRIPTION

s4pa_dbdump.pl is the script for dumping granule.db content into 
an ascii format. It takes the db filename and print out each record
in the MLDBM in a readable format separated by the delimiter.
For a granule.db, it prints:
file:xxx|cksum:xxx|date:xxx|fs:xxx|mode:xxx
For an associate.db, it prints:
file:xxx|associate:xxx,yyy,zzz

=head1 SEE ALSO

L<S4PA::Storage>

=cut

################################################################################
# $Id: s4pa_dbdump.pl,v 1.6 2009/03/24 17:55:22 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use DB_File;
use File::Basename;
use File::stat;
use Getopt::Std;
use S4PA::Storage;

use vars qw($opt_f $opt_d);
getopts('f:d:');
usage() if (!$opt_f);

my $dbm_file = $opt_f;
my $delimiter = $opt_d ? $opt_d : '|';

my ( $granuleHash, $fileHandle ) = S4PA::Storage::OpenGranuleDB( $dbm_file, "r" );
die "Failed to open $dbm_file ($!)" unless defined $granuleHash;

while (my ($key,$value) = each %$granuleHash) {
    my $line = "file:$key";
    if ( ref($value) eq "ARRAY" ) {
        $line .= $delimiter."associate:";
        $line .= "$_," foreach (@$value);
        $line =~ s/\,$//;
    } else {
        $line .= $delimiter."$_:".$value->{$_} foreach (sort keys %$value);
    }
    print "$line\n";
}

S4PA::Storage::CloseGranuleDB( $granuleHash, $dbm_file );
exit;

# Subroutine usage:  print usage and die
sub usage {
    print STDERR "Use: $0 -f DBM_file_name  [-d delimiter]\n";
    exit( 1 );
}
