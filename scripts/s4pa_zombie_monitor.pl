#!/usr/bin/perl

=head1 NAME

s4pa_zombie_monitor.pl - a script to monitor each station's running job for zombie
process.

=head1 SYNOPSIS

s4pa_zombie_monitor.pl -f <Configuration_file>

=head1 DESCRIPTION

s4pa_zombie_monitor.pl is one of housekeeping scripts for monitoring
running jobs and take specified action if job become zombie or defunct.

=head1 ARGUMENT

=over 4

=item B<-f>

configuration file.
    $cfg_root='/vol1/OPS/s4pa'
    $cfg_action='kill,notify'
    $cfg_interval='3600'
    $cfg_notify='gsfc-ops-staff-disc@lists.nasa.gov'

=back

=head1 AUTHORS

Guang-Dih Lei (Guang-Dih.Lei@nasa.gov)

=cut

################################################################################
# $Id: s4pa_zombie_monitor.pl,v 1.1 2019/06/30 21:10:10 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################
#
# name: s4pa_zombie_monitor.pl
# original: s4p_zk.pl Mike Theobald
# revised: 09/29/2006 glei initial release
#

use strict;
use S4P;
use Cwd;
use Safe;
use Getopt::Std;

use vars qw($opt_f);
getopts('f:');
usage() if (!$opt_f);

# Read the script configuration file
my $cpt = new Safe 'CFG';
$cpt->rdo($opt_f) or
    S4P::perish(1, "Cannot read config file $opt_f in safe mode: ($!)");

my $curTime = time();
my $oldTime;
if (-f "../zombie_monitor.time") {
    $oldTime = S4P::read_file("../zombie_monitor.time");
    S4P::perish(2, "Failed to read ../zombie_monitor.time") if ($oldTime eq 0);
} else {
    $oldTime = $curTime - $CFG::cfg_interval - 1;
}
if (($curTime - $oldTime) < $CFG::cfg_interval) {
    S4P::logger( "INFO",
        "Last execution was within the specified idle interval" );
    exit(0);
}

# Construct list of stations to check for zombied processes
my @stations = load_stations();
handle_zombies(\@stations);

S4P::write_file( "../zombie_monitor.time", $curTime );
exit(0);

sub load_stations {
    my @stations;

    # station list file
    my $rootDir = $CFG::cfg_root;
    my $listFile= "$rootDir/station.list";

    open(LST, $listFile) or S4P::perish(3, "Failed to open $listFile: $!");
    my @stns = <LST> ;
    close(LST) ;
    chomp(@stns) ;

    # s4pm station.list files do not have full paths, so must provide
    if ($stns[0] !~ m#$rootDir#) {
        foreach my $stn (@stns) {
            push @stations, "$rootDir/$stn";
        }
    } else {
        foreach my $stn (@stns) {
            # remove trailing '/'
            $stn =~ s/\/$//;
            push @stations, $stn;
        }
    }

   return(@stations);
}


sub handle_zombies {
    my $stations_r = shift;

    my @stations = @{$stations_r};
   
    # Create a linked list of parent/child processes with current run state for each pid
    my %psmap = load_ps_info();
   
    # action flag
    my $action = $CFG::cfg_action;
    my $kill = ($action =~ /kill/) ? 1 : 0;
    my $notify = ($action =~ /notify/) ? 1 : 0;

    my $odir = cwd();
    my $zmsg = '';

    # Look at each job and determine if the child pid is a zombie.
    foreach my $stn (@stations) {
        foreach my $job (glob("$stn/RUNNING*")) {
            my ($status, $pid, $owner, $orig_wo, $comment) = S4P::check_job($job);
            if (check_for_zombie($pid, \%psmap)) {
                $zmsg .= "$job is a zombie!\n";
                if ($kill) {
                    chdir($job);
                    if (S4P::alert_job($job, 'END')) {
                        S4P::logger('INFO', "Job terminated");
                        $zmsg .= "Job terminated.\n";
                    } else {
                        S4P::logger('ERROR', 'Failed to terminate job');
                        $zmsg .= "Failed to terminate job.\n";
                    }  
                    chdir($odir);
                }
            # no process related to current running job directory
            } else {
                # job pid does not exist any more, job was either finished or hung
                if (defined $pid) {
                    $zmsg .= "$job has no running process relate to!\n";
                    $zmsg .= "Waiting for operator intervention.\n";
                # running directory without job.status or empty directory
                } elsif (! -f "$job/job.status") {
                    # too dangerous to remove the whole running directory
                    # alert for operator intervention
                    $zmsg .= "$job has no job.status!\n";
                    $zmsg .= "Waiting for operator intervention.\n";
                }
            }
        }
    }

    if ($notify && $zmsg && $CFG::cfg_notify) {
        my $cmd = "echo '$zmsg' | mail -s 'Zombies detected' $CFG::cfg_notify";
        `$cmd`;
        S4P::perish(4, "Failed to mail:\n$cmd ($!)") if ($? >> 8);
    }

    return();
}


sub load_ps_info {
    my %psmap;

    my @res = readpipe("ps -A -o pid,ppid,s");
    chomp(@res);
    shift @res;
    while (my $pinfo = shift @res) {
        $pinfo =~ s/\s/,/g;
        $pinfo =~ s/,+/,/g;
        $pinfo =~ s/^,//;
        my ($pid, $ppid, $stat) = split /,/,$pinfo;
        push @{$psmap{'children'}{$ppid}}, $pid;
        $psmap{'status'}{$pid} = $stat;
   }

   return(%psmap);
}


# find child of specified process and check it's state
sub check_for_zombie {
    my $pid = shift;
    my $psmap_r = shift;
    my %psmap = %{$psmap_r};

    my $zstat;

    # if no child then all is OK
    return(undef) unless ($psmap{'children'}{$pid});

    foreach my $cpid (@{$psmap{'children'}{$pid}}) {
        $zstat ||= check_for_zombie($cpid, $psmap_r);
        $zstat = 1 if ($psmap{'status'}{$cpid} eq "Z");
    }
    return($zstat);
}


##############################################################################
# Subroutine usage:  print usage and die
##############################################################################
sub usage {
  die << "EOF";
Usage: $0 <-f configuration_file>
    configuration file should have the following variables specified:
    \$cfg_root        -> s4pa root directory
    \$cfg_interval    -> idle interval
    \$cfg_action      -> action if zombie process found
    \$cfg_notify      -> notification email list
EOF
}

