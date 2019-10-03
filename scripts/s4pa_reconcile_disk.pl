#!/usr/bin/perl

=head1 NAME

s4pa_reconcile_disk.pl - script to reconcile disk space in file systems
managed using directory based space management.

=head1 SYNOPSIS

s4pa_deploy.pl
[B<-r> I<active file system root>]
[B<-d> I<comma separated list of file system names (ex:001,002,..)>]
[B<-a> I<action: update or reset or report>]
[B<-t> I<threshold: fraction of free space to allocated space for update action (ex: 0.8)>]

=head1 DESCRIPTION

=head1 ARGUMENTS

=over 4

=item B<-r> I<active file system root>]

The root directory of the active file system

=item B<-d> I<comma separated list of file system names (ex:001,002,..)>]

A comma separated list of active file system names. Generally, they are of
three digits.

=item B<-a> I<action: update or reset or report>]

Action to be performed. The valids are update and reset. 'update' brings the 
available disk space up to date with the current usage. 'reset' resets the
available disk space to the original allocated value. 'report' reports on
the allocated and available disk space.

=item B<-t> I<threshold: fraction of free space to allocated space for update action>]

If specified, only perform update action if the volume is close with a .FS_COMPLETE
file and the ratio of free space to allocated space is over the specified threshold.

=head1 AUTHOR

M. Hegde, SSAI

=cut

################################################################################
# $Id: s4pa_reconcile_disk.pl,v 1.15 2019/06/19 13:35:40 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use S4PA::Receiving;
use Getopt::Std;
use Math::BigInt;
use S4P;

my ( $opt ) = {};

# Get command line arguments
getopts( "r:d:a:t:h", $opt );
if ((not defined $opt->{a}) || (not defined $opt->{r}) || (not defined $opt->{d})) {
    usage() unless (defined $opt->{h});
}

S4P::perish( 1, "Use $0 -r <file system root> -d"
    . "<comma separated directory labels> -a <action: update or reset or report>" )
    if $opt->{h};
# Check whether the action (valids=update or reset) is supported
S4P::perish( 1, "Specify an action (-a): update or reset" )
    unless defined $opt->{a};
S4P::perish( 1, "'$opt->{a}' is not a supported action" )
    unless ( ($opt->{a} eq 'update') or ($opt->{a} eq 'reset') or
             ($opt->{a} eq 'report') );
# Make sure the root directory of active file systems exist
S4P::perish( 1, "Specify file system root (-r)" )
    unless defined $opt->{r};
S4P::perish( 1, "$opt->{r} is not a directory" ) unless( -d $opt->{r} );

# Get a list of active file system labels being scanned
S4P::perish( 1, "Specify comma separated directory labels (-d)" )
    unless defined $opt->{d};
my @partitionList = split( /,/, $opt->{d} );

# Check if threshold is specified
my $threshold = $opt->{t} || 0;

    # An anonymous sub to get disk utilization
my $getDiskUsage = sub {
   my ( $dir ) = @_;
   my $utilSize = `du --summarize --bytes --dereference-args $dir`;
   $utilSize = ( $utilSize =~ /^(\d+)/ ? $1 : undef );
   S4P::logger( "ERROR", "Failed to find directory size of $dir" )
       unless defined $utilSize;
   return $utilSize;
};
# Loop over each file system/directory
foreach my $partition ( @partitionList ) {
    S4P::logger( "INFO", "Updating $partition" );
    $partition =~ s/^\s*|\s*$//g;
    my $dir = $opt->{r} . "/$partition";
    S4P::perish( 1, "$dir is not a directory" ) unless( -d $dir );
    
    # Read the disk reservation file
    my $diskReservationFile = "$dir/.FS_SIZE";
    unless ( -f $diskReservationFile ) {
        S4P::logger( "WARNING",
            "$dir doesn't use directory based disk space management" );
        next;
    }
    my @diskSizeList =
        S4PA::Receiving::DiskPartitionTracker( $diskReservationFile );
    S4P::perish( 2, "Failed to interpret $diskReservationFile" )
        unless ( @diskSizeList == 2 );
    

    # Compute the update size based on the action
    my $difference;
    if ( $opt->{a} eq 'reset' ) {
        # For reset, the difference = original size - known free space
        $difference = $diskSizeList[0] - $diskSizeList[1];
    } elsif ( $opt->{a} eq 'update' ) {
        # For update, find out the directory usage
        my $utilSize = $getDiskUsage->( $dir );
        S4P::perish( 2, "Failed to get disk utilization for $dir" )
           unless defined $utilSize;
        # For update, the 
        # difference = original size - actual usage - known free space
        $difference = $diskSizeList[0] - $diskSizeList[1] - $utilSize;

        # only check free space fraction if volume is closed and threshold is specified.
        # skip update if fraction is less than the threshold
        if ((-f "$dir/.FS_COMPLETE") && ($threshold > 0)) {
            my $thresholdSize = $diskSizeList[0]->numify() * $threshold;
            my $freeSize = $diskSizeList[1]->numify();
            next if ($freeSize < $thresholdSize);
        }
    } elsif ( $opt->{a} eq 'report' ) {
        # Report
        my $utilSize = $getDiskUsage->( $dir );
        S4P::perish( 2, "Failed to get disk utilization for $dir" )
           unless defined $utilSize;
        print STDERR "Directory=$dir, Allocated=$diskSizeList[0] bytes, ",
            "Free Space (computed)=$diskSizeList[1] (", 
            sprintf( "%d\%", $diskSizeList[1]*100./$diskSizeList[0] ), 
            "), Free Space (actual)="
            , ($diskSizeList[0]-$utilSize), "(",
            sprintf( "%d\%", ($diskSizeList[0]-$utilSize)*100./$diskSizeList[0] ), ")\n";
    }

    unless ( $opt->{a} eq 'report' ) {
        # Update the disk reservation file
        @diskSizeList = S4PA::Receiving::DiskPartitionTracker(
            $diskReservationFile, 'update', $difference );
        S4P::perish( 2, "Failed to update $diskReservationFile" )
            unless ( @diskSizeList == 2 );    
        unlink( "$dir/.FS_COMPLETE" ) if ( -f "$dir/.FS_COMPLETE" );
    }
}

sub usage {
    print STDERR
       "Usage: $0 -a <action:update|reset|report> -r <file_system_root> -d <comma_separated_volumes> [-t <threshold>]\n";
    exit;
}

