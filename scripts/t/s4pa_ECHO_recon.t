#!/usr/bin/perl

=head1 NAME

s4pa_ECHO_recon.t - test script to ECHO reconciliation.

=head1 ARGUMENTS

=over 4

=item B<-d>

Debug flag: keep output file.

=back

=head1 AUTHOR

Dr. C. Wrandle Barth, ADNET, NASA/GSFC, Code 610.2, Greenbelt, MD  20771.

=head1 CHANGE LOG

8/30/06 Initial version

=cut

use strict;
use POSIX;
use S4PA::Storage;
use vars qw($opt_d);
use Getopt::Std;
use Test::More 'no_plan';

getopts('d');
my $debug = $opt_d;
# Define path for test files.
my $temppath = '/var/tmp/s4pa_ECHO_recon';
my $datapath = 'data';

#Clean up from previous run.
`rm -fr $temppath` if -d $temppath;

mkdir $temppath or die "Can't make $temppath.";
mkdir "$temppath/storage" or die "Can't make $temppath/storage.";
mkdir "$temppath/storage/TRMM_L1" or die "Can't make $temppath/storage/TRMM_L1.";
mkdir "$temppath/storage/TRMM_L1A" or die "Can't make $temppath/storage/TRMM_L1A.";
mkdir "$temppath/storage/TRMM_L3A" or die "Can't make $temppath/storage/TRMM_L3A.";

# Create data file
open FH, ">$temppath/storage/dataset.cfg" or
		die "Can't create $temppath/storage/dataset.cfg.";
print FH <<'cfg_end';
%data_class  = (
                "TRMM_1A11" => "TRMM_L1A",
                "TRMM_1B11" => "TRMM_L1",
                "TRMM_1B21" => "TRMM_L1",
                "TRMM_1B31" => "TRMM_L1"
              );
cfg_end
close FH;

my $files = <<'myfiles_end';
1B11.060410.47867.6.HDF
1B11.060410.47867.6.HDF.xml
1B11.060410.47869.6.HDF
1B11.060410.47869.6.HDF.xml
1B11.060410.47870.6.HDF
1B11.060410.47870.6.HDF.xml
1B11.060410.47871.6.HDF
1B11.060410.47871.6.HDF.xml
myfiles_end

build_grandb('TRMM_L1', 'TRMM_1B11', $files);
$files =~ s/1B11/1B21/g;
build_grandb('TRMM_L1', 'TRMM_1B21', $files);
$files =~ s/1B21/1B31/g;
build_grandb('TRMM_L1', 'TRMM_1B31', $files);
$files =~ s/1B31/1A11/g;
build_grandb('TRMM_L1A', 'TRMM_1A11', $files);
$files =~ s/1A11/3A11/g;
build_grandb('TRMM_L3A', 'TRMM_3A11', $files);

# Create data file; include lines that begin with UR, end with, and only have.
open FH, ">$temppath/echo.txt" or
		die "Can't create $temppath/echo.txt.";
print FH <<'echo_end';
19971207 23:57:17 TRMM_1B11.6:1B11.060410.47867.6.HDF TRMM_1B11 6
19971207 23:57:17 TRMM_1B11.6:1B11.060410.47869.6.HDF TRMM_1B11 6
19971207 23:57:17 TRMM_1B11.6:1B11.060410.47871.6.HDF TRMM_1B11 6
19971207 23:57:17 TRMM_1B21.6:1B21.060410.47867.6.HDF TRMM_1B21 6
19971207 23:57:17 TRMM_1B21.6:1B21.060410.47868.6.HDF TRMM_1B21 6
19971207 23:57:17 TRMM_1B21.6:1B21.060410.47869.6.HDF TRMM_1B21 6
19971207 23:57:17 TRMM_1B21.6:1B21.060410.47872.6.HDF TRMM_1B21 6
TRMM_1B31.6:1B31.060410.47867.6.HDF TRMM_1B31 6
19971207 23:57:17 TRMM_1B31.6:1B31.060410.47868.6.HDF
TRMM_1B31.6:1B31.060410.47869.6.HDF
19971207 23:57:17 TRMM_1B31.6:1B31.060410.47870.6.HDF TRMM_1B31 6
19971207 23:57:17 TRMM_1B41.6:1B41.060410.47867.6.HDF TRMM_1B41 6
19971207 23:57:17 TRMM_1B41.6:1B41.060410.47868.6.HDF TRMM_1B41 6
19971207 23:57:17 TRMM_1B41.6:1B41.060410.47869.6.HDF TRMM_1B41 6
19971207 23:57:17 TRMM_1B41.6:1B41.060410.47871.6.HDF TRMM_1B41 6
echo_end
close FH;


# Run the script.
print "===Running s4pa_ECHO_recon.pl; Extra DAAC dataset first, extra ECHO at end===\n";
my $stdouttext = `s4pa_ECHO_recon.pl -r $temppath -e $temppath/echo.txt`;
print "$stdouttext\n============\n" if ($stdouttext ne '');

# Check the output.
is($?, 0, 's4pa_ECHO_recon.pl return code.');
like($stdouttext, qr/^\s*PERCENT\s+MATCHES\s+DAACLESS\s+ECHOLESS\s+DATASET\n/, 'Got header');
like($stdouttext, qr/^\s*0.*\%\s+Missing dataset\s+4\s+TRMM_1A11/m, 'ECHOLESS dataset.');
like($stdouttext, qr/^\s*75.*\%\s+3\s+0\s+1\s+TRMM_1B11/m, 'End on match');
like($stdouttext, qr/^\s*50.*\%\s+2\s+2\s+2\s+TRMM_1B21/m, 'End on ECHO file');
like($stdouttext, qr/^\s*75.*\%\s+3\s+1\s+1\s+TRMM_1B31/m, 'End on DAAC file');
like($stdouttext, qr/^\s*50.*\%\s+8\s+3\s+8\s+TOTALS/m, 'Totals line');
unlike($stdouttext, qr/TRMM_1B41/, 'No mention of extra ECHO dataset at end.');

# Create new data file with extra ECHO data set in middle, match at end.
open FH, ">$temppath/echo.txt" or
		die "Can't create $temppath/echo.txt.";
print FH <<'echo_end';
19971207 23:57:17 TRMM_1B11.6:1B11.060410.47867.6.HDF TRMM_1B11 6
19971207 23:57:17 TRMM_1B11.6:1B11.060410.47869.6.HDF TRMM_1B11 6
19971207 23:57:17 TRMM_1B11.6:1B11.060410.47871.6.HDF TRMM_1B11 6
19971207 23:57:17 TRMM_1B21.6:1B21.060410.47867.6.HDF TRMM_1B21 6
19971207 23:57:17 TRMM_1B21.6:1B21.060410.47868.6.HDF TRMM_1B21 6
19971207 23:57:17 TRMM_1B21.6:1B21.060410.47869.6.HDF TRMM_1B21 6
19971207 23:57:17 TRMM_1B21.6:1B21.060410.47872.6.HDF TRMM_1B21 6
19971207 23:57:17 TRMM_1B22.6:1B22.060410.47867.6.HDF TRMM_1B22 6
19971207 23:57:17 TRMM_1B22.6:1B22.060410.47868.6.HDF TRMM_1B22 6
19971207 23:57:17 TRMM_1B22.6:1B22.060410.47869.6.HDF TRMM_1B22 6
19971207 23:57:17 TRMM_1B22.6:1B22.060410.47871.6.HDF TRMM_1B22 6
19971207 23:57:17 TRMM_1B31.6:1B31.060410.47867.6.HDF TRMM_1B31 6
19971207 23:57:17 TRMM_1B31.6:1B31.060410.47868.6.HDF TRMM_1B31 6
19971207 23:57:17 TRMM_1B31.6:1B31.060410.47869.6.HDF TRMM_1B31 6
19971207 23:57:17 TRMM_1B31.6:1B31.060410.47870.6.HDF TRMM_1B31 6

echo_end
close FH;


# Run the script.
print "===Running s4pa_ECHO_recon.pl; Extra DAAC dataset first, match at end===\n";
$stdouttext = `s4pa_ECHO_recon.pl -r $temppath -e $temppath/echo.txt -c $temppath/echorecon.csv`;
print "$stdouttext\n============\n" if ($stdouttext ne '');

# Check the output.
is($?, 0, 's4pa_ECHO_recon.pl return code.');
like($stdouttext, qr/^\s*PERCENT\s+MATCHES\s+DAACLESS\s+ECHOLESS\s+DATASET\n/, 'Got header');
like($stdouttext, qr/^\s*0.*\%\s+Missing dataset\s+4\s+TRMM_1A11/m, 'ECHOLESS dataset.');
like($stdouttext, qr/^\s*75.*\%\s+3\s+0\s+1\s+TRMM_1B11/m, 'End on match');
like($stdouttext, qr/^\s*50.*\%\s+2\s+2\s+2\s+TRMM_1B21/m, 'End on ECHO file');
like($stdouttext, qr/^\s*75.*\%\s+3\s+1\s+1\s+TRMM_1B31/m, 'End on DAAC file');
like($stdouttext, qr/^\s*50.*\%\s+8\s+3\s+8\s+TOTALS/m, 'Totals line');
unlike($stdouttext, qr/TRMM_1B22/, 'No mention of extra ECHO dataset in middle.');
ok(-e "$temppath/echorecon.csv", 'Wrote CSV file');
if (-e "$temppath/echorecon.csv") {
    open FH, "$temppath/echorecon.csv" or die "Can't read CSV file";
    undef $/;
    my $csv = <FH>;
    close FH;
    $/ = "\n";
    print "$csv\n============\n";
    unlike($csv, qr/DAACLESS/, 'No header in CSV');
    like($csv, qr/^\s*75.*\%,3,0,1,TRMM_1B11/m, 'End on match');
    like($csv, qr/^\s*50.*\%,2,2,2,TRMM_1B21/m, 'End on ECHO file');
    like($csv, qr/^\s*75.*\%,3,1,1,TRMM_1B31/m, 'End on DAAC file');
}

# Replace data file, extra DAAC dataset at end of sort.
open FH, ">$temppath/storage/dataset.cfg" or
		die "Can't create $temppath/storage/dataset.cfg.";
print FH <<'cfg_end';
%data_class  = (
                "TRMM_3A11" => "TRMM_L3A",
                "TRMM_1B11" => "TRMM_L1",
                "TRMM_1B21" => "TRMM_L1",
                "TRMM_1B31" => "TRMM_L1"
              );
cfg_end
close FH;

# Run the script.
print "===Running s4pa_ECHO_recon.pl; Extra DAAC dataset at end===\n";
$stdouttext = `s4pa_ECHO_recon.pl -r $temppath -e $temppath/echo.txt -d $temppath/daacless.txt`;
print "$stdouttext\n============\n" if ($stdouttext ne '');

# Check the output.
is($?, 0, 's4pa_ECHO_recon.pl return code.');
like($stdouttext, qr/^\s*PERCENT\s+MATCHES\s+DAACLESS\s+ECHOLESS\s+DATASET\n/, 'Got header');
like($stdouttext, qr/^\s*0.*\%\s+Missing dataset\s+4\s+TRMM_3A11/m, 'ECHOLESS dataset.');
like($stdouttext, qr/^\s*75.*\%\s+3\s+0\s+1\s+TRMM_1B11/m, 'End on match');
like($stdouttext, qr/^\s*50.*\%\s+2\s+2\s+2\s+TRMM_1B21/m, 'End on ECHO file');
like($stdouttext, qr/^\s*75.*\%\s+3\s+1\s+1\s+TRMM_1B31/m, 'End on DAAC file');
like($stdouttext, qr/^\s*50.*\%\s+8\s+3\s+8\s+TOTALS/m, 'Totals line');
unlike($stdouttext, qr/TRMM_1B22/, 'No mention of extra ECHO dataset in middle.');
ok(-e "$temppath/daacless.txt", 'Wrote DAACLESS file');
if (-e "$temppath/daacless.txt") {
    open FH, "$temppath/daacless.txt" or die "Can't read DAACLESS file";
    undef $/;
    my $daacless = <FH>;
    close FH;
    $/ = "\n";
    print "$daacless\n============\n";
    like($daacless, qr/TRMM_1B21.6:1B21.060410.47868.6.HDF/m, 'Found a DAACLESS');
    unlike($daacless, qr/1B11.060410.47867.6.HDF TRMM_1B11/m, 'One that should not be there');
}


#Clean up if not debug.
`rm -fr $temppath` if not $debug;


sub build_grandb {
    my ($class, $ds, $files) = @_;
    mkdir "$temppath/storage/$class/$ds" or die "Can't make $temppath/storage/$class/$ds.";
    my ( $granuleHashRef, $fileHandle ) =
        S4PA::Storage::OpenGranuleDB( "$temppath/storage/$class/$ds/granule.db", "rw" );
    foreach my $f (split /\s+/, $files) {
        $granuleHashRef->{$f} = 1;
    }
    S4PA::Storage::CloseGranuleDB( $granuleHashRef, $fileHandle );
}