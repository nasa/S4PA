#!/usr/bin/perl

=head1 NAME

s4pa_controller.pl - the station script for controlling S4PA stations

=head1 SYNOPSIS

s4pa_controller.pl -a <action> <station list file>

=head1 ABSTRACT

B<Pseudo code:>

=head1 DESCRIPTION

s4pa_controller.pl is the script for controlling S4PA stations. It provides
options to start and stop stations. It is used in conjunction with
-c option of tkstat.pl. <action> can be 'startAll' or 'stopAll' or 
'Shutdown'. It skips starting DeleteData stations as a safety measure.

=head1 AUTHOR

M. Hegde

=cut

################################################################################
# $Id: s4pa_controller.pl,v 1.10 2019/04/05 11:31:58 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################
use S4P;
use S4P::TkJob;
use S4PA;
use S4PA::Receiving;
use Cwd;
use Getopt::Std;

# Expect an action (-a) and a file listing station directories as arguments.
my ( $opt ) = {};
getopts( "a:hi", $opt );
S4P::perish( 1,
    "Usage: $0 -i -a <action> <station list file>\n<action>=startAll, stopAll, or Shutdown" ) 
    if $opt->{h};

# Read the list of station directories.
my @stationList = GetStationList( $ARGV[0] );
if ( $opt->{a} eq 'startAll' ) {
    foreach my $dir ( @stationList ) {
        next if ( $dir =~ /\/delete_*/ && !$opt->{i} );
        chdir( $dir );
        # in order to use s4p_station.pl instead of stationmaster.pl
        # to start each station, we need to pass 'undef' on the first
        # argument to undefine the -C switch on the new TkJob.
        # S4P::TkJob::start_station( $dir );
        S4P::TkJob::start_station( undef, $dir );
    }
} elsif ( $opt->{a} eq 'stopAll' ) {
    foreach my $dir ( @stationList ) {
        chdir( $dir );
        S4P::stop_station( $dir );
    }
} elsif ( $opt->{a} eq 'Shutdown' ) {
    foreach my $dir ( @stationList ) {
        chdir( $dir );
        # stop station before terminate all running jobs
        S4P::stop_station( $dir );

        # terminate all running jobs
        my @running_jobs = glob( 'RUNNING.*' );
        foreach my $job_dir( @running_jobs ) {
            # end the running job
            chdir($job_dir);
            S4P::end_job(cwd());
            chdir("..");

            # only retry the terminated job
            # but wait a few seconds for the FAILED directory get created
            system "sleep 5";
            $job_dir =~ s/RUNNING/FAILED/;
            RestartJob($dir, $job_dir);
        }
    }
}

# Get the station list from the config file
sub GetStationList
{
    my ( $file ) = @_;
    my @list = ();
    if ( open( FH, $file ) ) {
        @list = <FH>;
        close( FH );
        chomp( @list );
    }
    return @list;
}

# retry terminated job
sub RestartJob
{
    my ($sta, $job) = @_;
    my $pwd = cwd();

    opendir(DIR, $pwd) or S4P::perish(10, "$0: Failed to opendir $pwd: $!");
    my $dir;
    while ( defined($dir = readdir(DIR)) ) {
        if ( -d $dir and $dir =~ /^$job/ ) {
            my $path = "$sta/$dir";
            chdir($path);
            if ($sta =~ /\/receiving/ && $sta !~ /polling/) {
                S4PA::Receiving::FailureHandler("auto_restart");
            } else {
                S4P::remove_job() if S4P::restart_job();
            }
            chdir($pwd);

            # terminated check_integrity job tends to fail again 
            # try remove the failed job log to get a clean restart
            # identify the failed job log
            if ($job =~ /FAILED\.CHECK/) {
                $job =~ s/FAILED\.//;
                my $logfile = $job . ".log";
                unlink($logfile);
            }
        }
    }
}
