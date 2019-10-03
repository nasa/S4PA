#!/usr/bin/perl

=head1 NAME

s4pa_recycle_volumes.pl - script to reconcile volumes under rolling archive partition.

=head1 SYNOPSIS

s4pa_recycle_volumes.pl
[B<-r> I<instance_root_directory>]
[B<-p> I<provider_name>]
[B<-t> I<recycle_threshold>]

=head1 DESCRIPTION

s4pa_recycle_volumes.pl read s4pa instance root directory, provider name of 
receiving station, and the recycle volume threshold from command line options.
It locates the ActiveFs.list file under receiving station, read the rolling
archive root partition and configured volumes from that list file and perform
volume free space reconciliation if the volume's free space ratio is over the
threshold. This is to be set up as a standard housekeeping job to routinely 
remove the .FS_COMPLETE file under a space freed-up volume for reusage.

=head1 ARGUMENTS

=over 4

=item [B<-r> I<instance_root_directory>]

Root directory of S4PA instance stations.

=item [B<-p> I<provider_name>]

Instance provider's name

=item [B<-t> I<recycle_threshold>]

Ratio of volume's free disk space to allocated disk space for the volume to be recycled.

=back

=head1 AUTHOR

Guang-Dih Lei, ADNET Systems

=cut

################################################################################
# s4pa_recycle_volumes.pl,v 1.0 2019/05/22 17:40:31 glei Exp
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use S4P;
use Getopt::Std;
use vars qw($opt_r $opt_p $opt_t);

getopts('r:p:t:');
usage() if ((not defined $opt_r) || (not defined $opt_p) || (not defined $opt_t));
my $rootDir = $opt_r;
my $provider = $opt_p;
my $threshold = $opt_t;

my $recvDir = "$rootDir/receiving/$provider";
my $listFile = "$recvDir/ActiveFs.list";

# make sure there is a ActiveFs.list file under receiving station
S4P::perish(1, "Active FS listing file '$listFile' does not exist") unless (-f $listFile);

# read active fs list file for partition and configured volumes
my $partition;
my @volumes;
open(LST, "<$listFile") or S4P::perish(2, "Failed to open $listFile.");
while(<LST>) {
    chomp;
    # first line in list file is the partition path
    if (! defined $partition) {
        $partition = $_;
    # rest of the lines are configured volumes
    } else {
        push @volumes, $_;
    }
}

# scan each volume for disk reconcile if necessary 
foreach my $volume (@volumes) {
    my $volDir = "$partition/$volume";
    # no need to reconcile if the volume is still open
    next unless (-f "$volDir/.FS_COMPLETE");
    # format reconcile disk command
    my $cmd = "s4pa_reconcile_disk.pl -a update -r $partition -d $volume -t $threshold";
    # perform disk reconcile
    my ($errstr, $rc) = S4P::exec_system("$cmd");
    if ($rc) {
        S4P::logger('ERROR', "Failed to execute $cmd: $errstr");
    } else {
        S4P::logger('INFO', "Successfully executed disk reconcile for $volDir");
    }
}
exit;

sub usage {
    print STDERR
       "Usage: $0 -r <root_directory> -p <provider> -t <threshold>\n";
    exit;
}

