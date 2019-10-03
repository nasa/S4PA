#!/usr/bin/perl

=head1 NAME

s4pa_subscribe - station script to invoke S4PA::Subscription::fill_subscriptions

=head1 SYNOPSIS

s4pa_subscribe [B<-f> I<subscription_file>] [B<-v>] [B<-d> I<PDR dir>] [<pdr_work_order>]

=head1 DESCRIPTION

s4pa_subscribe.pl reads a PDR file and passes it to S4PA::Subscription::fill_subscriptions.
Alternatively it can read all the PDRs in a directory and build a single composite PDR with the
collective file groups, thereby sending a single subscription notification to each user (each
"destination" host in the subscription configuration file) covering all matching PDR files.

=head1 ARGUMENTS

=over 4

=item B<-f> I<subscription_file>

Configuration file  with subscriptions.  See Subscription(3) man page for
more details on format.  Default is '../s4pa_subscription.cfg'.

=item B<-d> I<PDR directory>

Directory containing PDRs to be processed. Either the <PDR dir> or the <pdr_work_order> must
be specified.

=item B<-v>

Verbose flag.

=back

=head1 SEE ALSO

Subscription(3)

=head1 AUTHOR

Christopher Lynnes, NASA/GSFC, Code 610.2

=head1 LAST REVISED

2004/08/16, 16:44:52

=cut
########################################################################
# s4pa_subscribe.pl,v 1.9 2007/09/04 14:57:13 glei Exp
# -@@@ S4PA, Version $Name:  $
########################################################################
use strict;
use Getopt::Std;
use S4P::PDR;
use S4P;
use S4PA::Subscription;
use S4PA;
use Safe ;
use Log::Log4perl;
use vars qw($opt_f $opt_v $opt_d);

getopts('d:f:v');
usage() unless (@ARGV || $opt_d);

# (1) Set subscription file
my $subscription_file = $opt_f || '../s4pa_subscription.cfg';
# prevent job failed when instance has no subscription like replication.
# S4P::perish(2, "Cannot find subscription file $subscription_file")
#     unless(-f $subscription_file);
unless(-f $subscription_file) {
    exit;
}

my $cpt = new Safe 'CFG';
$cpt->rdo( $subscription_file ) or
    S4P::perish( 1, "Cannot read config file $subscription_file in safe mode: $!\n");

my $logger = S4PA::CreateLogger( $CFG::cfg_logger{FILE},
    $CFG::cfg_logger{LEVEL} ) if ( %CFG::cfg_logger );

my $pdr;
my $pdrdir = $opt_d;
my @pdrFileList = ();
my %subscriberSpecificPdr;
my $numberPDR = 0;
my $numberMRI = 0;
if ( defined $opt_d ) {
    # (2) Build a single composite PDR containing all file groups of all PDRs in the directory.

    opendir (PDRDIR,"$pdrdir") || S4P::perish(2, "Failed to open $pdrdir ($!)");
    my @files = readdir (PDRDIR);
    close (PDRDIR) ;

    # collecting PDRs
    foreach my $file (@files) {
        chomp($file);
        # remove log files and skip files that are not PDR files
        my $pdrPath = "$pdrdir/$file";
        unlink $pdrPath if ($file =~ /\.log$/);
        push(@pdrFileList, $pdrPath) if ($file =~ /\.(PDR|wo)$/);
    }

    foreach my $pdrFile ( @pdrFileList ) {
        my $dummyPdr = S4P::PDR::read_pdr( $pdrFile )
            || S4P::perish( 2, "Cannot read/parser PDR $pdrFile" );

	if ( defined $dummyPdr->{'subscription_id'} ) {
            my $subID = $dummyPdr->{'subscription_id'};
            if ( defined $subscriberSpecificPdr{"$subID"} ) {
	        # Add this PDR's file groups to existing composite.
                foreach my $fileGroup ( @{$dummyPdr->file_groups()} ) {
                    $subscriberSpecificPdr{"$subID"}->add_file_group( $fileGroup );
                }
            } else {
                # Start composite off with first PRD's contents.
                $subscriberSpecificPdr{"$subID"} = $dummyPdr;
            }
            $logger->info( "Processing $pdrFile for subscription user " .
                "$subID" ) if defined $logger;
            $numberMRI++;
        } elsif ( defined $pdr ) {
	    # Add this PDR's file groups to existing composite.
	    foreach my $fileGroup ( @{$dummyPdr->file_groups()} ) {
		$pdr->add_file_group( $fileGroup );
	    }    
            $logger->info( "Processing $pdrFile" ) if defined $logger;
            $numberPDR++;
        } else {
            # Start composite off with first PRD's contents.
            $pdr = $dummyPdr;
            $logger->info( "Processing $pdrFile" ) if defined $logger;
            $numberPDR++;
        }    
    }
} else {
    # (2) Read single work order PDR.
    my $dummyPdr = S4P::PDR::read_pdr($ARGV[0]) or
        S4P::perish(2, "Cannot read / parse PDR work order $ARGV[0]");
    if ( defined $dummyPdr->{'subscription_id'} ) {
        my $subID = $dummyPdr->{'subscription_id'};
        $subscriberSpecificPdr{"$subID"} = $dummyPdr;
        $logger->info( "Processing $ARGV[0] for subscription user " .
            "$subID" ) if defined $logger;
        $numberMRI++;
    } else {
	$pdr = $dummyPdr;
        $logger->info( "Processed $ARGV[0]" ) if defined $logger;
        $numberPDR++;
    }
}

if ( defined $pdr ) {
    # (3) Fill subscriptions if we didn't just find an empty directory.
    S4PA::Subscription::fill_subscriptions($subscription_file, $pdr, $opt_v) or
        S4P::perish(3, "Failed to fill subscriptions");
    $logger->info( "Filled subscription for $numberPDR PDR" ) if defined $logger;
}

# sleep for one second to prevent the MRI work order overwrite the regular one.
sleep 1;

foreach my $subID ( keys %subscriberSpecificPdr ) {
    my $pdr = $subscriberSpecificPdr{"$subID"};
    S4P::logger( "INFO", "Filling in subscriber specific subscriptions" );
    S4PA::Subscription::fill_subscriptions($subscription_file, $pdr, $opt_v) or
        S4P::perish(3, "Failed to fill subscriber specific subscriptions");
    $logger->info( "Filled subscription for $numberMRI MRI" ) if defined $logger;
}

# Cleanup PDRs picked up from the PDR directory, if any.
foreach my $file ( @pdrFileList ) {
    unlink( $file ) || S4P::perish( 2, "Failed to remove $file ($!)" );
}
exit(0);

sub usage {
    die "Usage: $0 [-f subscription_file] [-d PDR_directory] [-v] pdr_work_order";
}
