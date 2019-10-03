#!/usr/bin/perl

=head1 NAME

s4pa_check_integrity.pl - the station script for Check<data class> stations in
S4PA.

=head1 SYNOPSIS

s4pa_check_integrity.pl [-a] [-l] [-s] [-c] [-v] -f <configuration file> dataset1 ...

=head1 ABSTRACT

B<Pseudo code:>

    Get %cfg_data_version hash
    For each dataset
        For each version
            If S4PA::Storage::CheckCRC() returns false
                Exit with error.
            Endif
        Endfor
    Endfor
    Exit with success.
    End

=head1 DESCRIPTION

s4pa_check_integrity.pl is the station script for Check<data class> stations in
S4PA. It is a wrapper for S4PA::Storage::CheckCRC(). It takes dataset names in
the form of a comma separated list as an argument from command line. It exits
with failure (non-zero) if S4PA::Storage::CheckCRC() return false (0) for any
dataset in any version. The optional 'v' switch stands for verbose and is used
for printing additional log messages.  The 'f' switch is required and names the
configuration file containing the list of versions for each dataset. The 'c' 
switch indicates that the scan should continue even in cases of integrity 
check failures; otherwise, the scan will stop at the first failure. The 's'
switch indicates that the scan should cover storage areas to make sure that 
links in storage area have granule database entries. Optional 'l' switch
indicates the lightweight verifcation options where checksum verification 
is skipped; only link and location verification is performed in that case. The
light weight option has been provided for cases where checksum verfication over
an entire dataset may take couple of days. Optional 'a' switch provides a way to
limiting the scan to a specific file system. Use it to specify the file system
number.

=head1 SEE ALSO

L<S4PA::Storage>

=cut

################################################################################
# $Id: s4pa_check_integrity.pl,v 1.17 2017/05/15 12:17:27 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use S4PA::Storage;
use Getopt::Std;
use File::Copy;
use Fcntl qw(:DEFAULT :flock);

use vars qw( $opt_v $opt_f $opt_c $opt_s $opt_l $opt_a);

getopts( 'vf:csla:' );

# Expect a configuration file passed via -f command line switch. It contains
# a hash named %cfg_data_version whose keys are dataset names. The hash values
# are arrays of the versions to be appended.  An undef (or null string) will
# map to a directory with no version (and no period).
S4P::perish(1, "Failed to find config file $opt_f") unless ( defined $opt_f );

# Try to read %cfg_data_version from the configuration file and on failure stop.
my $cpt = new Safe('CFG');
$cpt->share( '%cfg_data_version' );
$cpt->rdo($opt_f) or S4P::perish( 1, "Failed to read configuration file $opt_f ($@)");

# By default, don't skip on error
$opt_c = 0 unless defined $opt_c;
$opt_v = 0 unless defined $opt_v;
$opt_s = 0 unless defined $opt_s;

# Specify the dataset name(s) as comma-separated list
# die "Specify dataset name(s) as comma-separated list" unless @ARGV;
my @datasetList;
my $fileSystem;
my $jobStatus = 0;
my $checkEntireFsJob = 0;

if ( scalar(@ARGV) > 1 ) {
    # regular check integrity job by passing dataset via command line arguments
    @datasetList = split( /,/, $ARGV[0] );
    $fileSystem = $opt_a if ( defined $opt_a );
} else {
    # priority job to scan all datasets in a particular file system volume
    # get the file system number from command line option or work order name
    @datasetList = ( keys %CFG::cfg_data_version );
    if ( defined $opt_a ) {
        $fileSystem = $opt_a;
    } elsif ( $ARGV[0] =~ /CHECK_ACTIVE_FS\.(\d+)/ ) {
        $fileSystem = $1;
        $checkEntireFsJob = 1;
    } else {
        S4P::perish( 2, "Failed to get a FS volume for priority job" );
    } 
}

# For each dataset specified, check the CRC checksum database.
# my @datasetList = split( /,/, $ARGV[0] );
foreach my $dataset ( @datasetList ) {
    # Remove leading or trailing spaces
    $dataset =~ s/^\s+|\s+$//g;
    # Find all versions of this dataset.
    my $datavers = $CFG::cfg_data_version{$dataset};
    my $checkFlag = 1;
    foreach my $version (@$datavers) {
        # Append each version to build directory name for checking.
        # Null/undef version uses just dataset name.
        my $dirname = $version eq '' ? $dataset : "$dataset.$version";
        # If the the checksum verification fails, exit with failure.
        unless ( S4PA::Storage::CheckCRC( DATASET => $dirname,
            CONTINUE_ON_ERROR => $opt_c, VERBOSE => $opt_v, 
            SCAN_STORAGE => $opt_s, VERIFY_CKSUM => (defined $opt_l) ? 0 : 1, 
            FILE_SYSTEM => $fileSystem, ENTIRE_FS_JOB => $checkEntireFsJob ) ) {
            $checkFlag = 0;
            S4P::logger( 'WARNING', "Integrity check failed for $dirname" );
            $jobStatus = 1;
            exit( $jobStatus ) unless ( $checkEntireFsJob );
        }
        S4P::logger( 'INFO', "$dirname ok" ) if ( $opt_v );
    }

    # only log successful if this was not a check active_fs job
    if ( $checkFlag && !$fileSystem ) {
        # save a copy of the cksum log
        my $logfile = $ARGV[1];
        $logfile =~ s/^DO\.(CHECK_.+)\.(.+)$/$1.$2.log/;
        my $saveLog = "../CHECKED_$dataset.log";
        my $status = `grep cksum $logfile | awk '{print \$1"T"\$2"Z KEY:"\$7" CKSUM:"\$9}' > $saveLog`;

        my ($sec, $min, $hr, $day, $mo, $yr) = (localtime())[0,1,2,3,4,5];
        $yr += 1900;
        $mo += 1;
        my $checkTime = sprintf("%d-%02d-%02d %02d:%02d:%02d", $yr, $mo, $day, $hr, $min, $sec);
        
        # define a check integrity log
        my $checkLog = "check_integrity.log";
        
        # obtain a lock to check integrity log
        if (open(LOCKFH, ">../$checkLog.lock")) {
            unless(flock( LOCKFH, 2)) {
                close(LOCKFH);
                S4P::logger('ERROR', "Failed to get a file lock");
            }
            # open check integrity log for appending
            if (open(CHECKLOG, ">>../$checkLog")) {
                print CHECKLOG "dataset $dataset: last successful round of integrity check completed $checkTime\n";
                close CHECKLOG;
            } else {
                S4P::logger('ERROR', "Failed to get a file lock");
            }
            # Remove lock
            close(LOCKFH);
            flock(LOCKFH, 8);
        } else {
            S4P::logger('ERROR', "Failed to open lock file");
        }
    }
}

exit( $jobStatus );
