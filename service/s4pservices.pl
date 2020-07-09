#!/usr/bin/perl

=head1 NAME

s4pservices.pl - the system startup/shutdown script for S4P stations

=head1 SYNOPSIS

s4pservices.pl -f <configFile> -a [start|stop|status]

=head1 DESCRIPTION

s4pservices.pl is the script for startup/shutdown all S4P stations during system reboot.
It is usually called by /etc/init.d/s4pd but can be used for status check at any time.

=head1 ARGUMENTS

=over 4

=item B<-f>

<configuration file>

It should contain hash(s) of %s4pa or %s4pm as the following sample:

%s4pa = (
    "<instance_label>" => {
        "rootDir" => "/vol1/OPS/s4pa",
        "startDel" => 1,
        "startRunOnly" => 1
        }
);

=item B<-a>

<action>

[start|stop|status]

=item B<-i>

<list of instance name separated by comma>

Optional on specific instances only. Default to all instances in configuration file.

=back

=head1 AUTHOR

Guang-Dih Lei

=cut

################################################################################
# s4pservices.pl,v 1.6 2019/04/08 15:49:40 glei Exp
# -@@@ S4PA, Version Release-3_43_7
################################################################################

use strict;
use Safe;
use Cwd;
use File::Basename;
use File::Copy;
use S4P;
use S4PA;
use Getopt::Std;
use vars qw($opt_f $opt_a $opt_i);

# Expect both configFile (-f) and  action (-a).
getopts('i:f:a:r');
S4P::perish(1, "Usage: $0 -f <configFile> -a <action> [-i <list_of_instances>]\n<action>=start or stop or status")
    unless ($opt_f && $opt_a);
my $SKIP_DISABLE = 1;

my $action = $opt_a;
S4P::perish(2, "Not a qualified action") unless ($action =~ /start|stop|status/);

# reading configuration
my $cpt = new Safe('CFG');
$cpt->rdo($opt_f) or S4P::perish(3, "Failed to read conf file $opt_f ($@)");
my %instConf = ReadConf();

# perform startup/shutdown on each instance
foreach my $group (keys %instConf) {
    my %instances;
    if ($opt_i) {
        map {$instances{$_} = '1'} split /,/,$opt_i;
    } else {
        map {$instances{$_} = '1'} keys %{$instConf{$group}};
    }
    foreach my $instance (keys %instances) {
        unless (defined($instConf{$group}{$instance}{'rootDir'})) { S4P::logger("INFO","Invalid instance: $instance") ; next ; }
        my $rootDir = $instConf{$group}{$instance}{'rootDir'};
        my $runOnly = $instConf{$group}{$instance}{'startRunOnly'};

        # when only start/stop running stations option is defined,
        # we need to find out which stations are currently up and save
        # those stations in the station.list file
        if ($runOnly and $action eq 'stop') {
           updateList($group, $rootDir);
        }
 
        my $staListFile = $rootDir . "/station.list";
        my $staListBackup = $rootDir . "/station.list.service_bak";
        if ($group eq 's4pa' || $group eq 'opsmon') {
            if ($action eq 'start') {
                if ($instConf{$group}{$instance}{'startDel'}) {
                    S4P::exec_system("s4pa_controller.pl -i -a startAll $staListFile");
                } else {
                    S4P::exec_system("s4pa_controller.pl -a startAll $staListFile");
                }
                print "Instance $instance started.\n";
            } elsif ($action eq 'stop') {
                S4P::exec_system("s4pa_controller.pl -a Shutdown $staListFile");
                print "Instance $instance stop.\n";
            # checking each instance status
            } elsif ($action eq 'status') {
                instStatus($group, $instance, $rootDir);
            }

        } elsif ($group eq 's4pm') {
            # S4PM do it differently, we have to change to that root directly
            chdir($rootDir);
            # taking startAll and killAll interfaces from tkstat.cfg of S4PM stream
            if ($action eq 'start') {
                S4P::exec_system("s4p_start.sh -u 002 station.list");
                print "Stream $instance started.\n";
            } elsif ($action eq 'stop') {
                S4P::exec_system("s4pshutdown.pl -r -g station.list");
                print "Stream $instance stop.\n";
                # retry all failed jobs in each station
                RestartJobs($rootDir);
            # checking each stream status
            } elsif ($action eq 'status') {
                instStatus($group, $instance, $rootDir);
            }
        }

        # restore the original station.list only if runOnly is specified
        if ($runOnly and $action eq 'start') {
            File::Copy::copy($staListBackup, $staListFile) if (-f $staListBackup);
        }
    }
}
exit;

sub ReadConf
{
    my %instConf;
    if (%CFG::s4pa) {
        foreach my $instance (keys %CFG::s4pa) {
            # set root directory
            if (exists $CFG::s4pa{$instance}{'rootDir'}) {
                $instConf{'s4pa'}{$instance}{'rootDir'} = $CFG::s4pa{$instance}{'rootDir'};
            } else {
                S4P::perish(4, "S4PA Instance $instance rootDir is not defined.");
            }

            # read start delete stations flag
            if (exists $CFG::s4pa{$instance}{'startDel'}) {
                $instConf{'s4pa'}{$instance}{'startDel'} = $CFG::s4pa{$instance}{'startDel'};
            } else {
                $instConf{'s4pa'}{$instance}{'startDel'} = 0;
            }

            # read start only stations were running flag
            if (exists $CFG::s4pa{$instance}{'startRunOnly'}) {
                $instConf{'s4pa'}{$instance}{'startRunOnly'} = $CFG::s4pa{$instance}{'startRunOnly'};
            } else {
                $instConf{'s4pa'}{$instance}{'startRunOnly'} = 1;
            }
        }
    }

    if (%CFG::s4pm) {
        foreach my $instance (keys %CFG::s4pm) {
            # set root directory
            if (exists $CFG::s4pm{$instance}{'rootDir'}) {
                $instConf{'s4pm'}{$instance}{'rootDir'} = $CFG::s4pm{$instance}{'rootDir'};
            } else {
                S4P::perish(5, "S4PM Stream $instance rootDir is not defined.");
            }

            # read start only stations were running flag
            if (exists $CFG::s4pm{$instance}{'startRunOnly'}) {
                $instConf{'s4pm'}{$instance}{'startRunOnly'} = $CFG::s4pm{$instance}{'startRunOnly'};
            } else {
                $instConf{'s4pm'}{$instance}{'startRunOnly'} = 1;
            }
        }
    }

    if (%CFG::opsmon) {
        foreach my $instance (keys %CFG::opsmon) {
            # set root directory
            if (exists $CFG::opsmon{$instance}{'rootDir'}) {
                $instConf{'opsmon'}{$instance}{'rootDir'} = $CFG::opsmon{$instance}{'rootDir'};
            } else {
                S4P::perish(6, "OPS Monitor $instance rootDir is not defined.");
            }

            # opsmon has no delete station
            $instConf{'opsmon'}{$instance}{'startDel'} = 0;

            # read start only stations were running flag
            if (exists $CFG::opsmon{$instance}{'startRunOnly'}) {
                $instConf{'opsmon'}{$instance}{'startRunOnly'} = $CFG::opsmon{$instance}{'startRunOnly'};
            } else {
                $instConf{'opsmon'}{$instance}{'startRunOnly'} = 1;
            }
        }
    }

    return %instConf;
}

# retry all terminated jobs
sub RestartJobs
{
    my ($rootDir) = @_;
    chdir($rootDir);
    my $staListFile = $rootDir . "/station.list";
    my $cmd = "s4p_restart_all_jobs.pl";

    open(STA, $staListFile) or S4P::perish(7, "$0: Failed to open station list file: $!");
    foreach my $dirname (<STA>) {
        chomp $dirname;
        # change to station directory
        chdir($dirname);
        # retry all failed jobs
        my ($rs, $rc) = S4P::exec_system($cmd);
        # back to station root directory
        chdir($rootDir);
    }
    close(STA);
}

sub updateList {
    my ($group, $rootDir) = @_;

    my $staListFile = $rootDir . "/station.list";
    my $staListBackup = $rootDir . "/station.list.service_bak";
    # backup current station.list file
    File::Copy::copy($staListFile, $staListBackup);

    my @runningSta;
    open(LST, "<$staListFile") or S4P::preish(8, "$0: Failed to open station list file: $!");
    foreach my $station (<LST>) {
        chomp($station);
        my $staPath;
        # s4pa station.list file has each station's full path
        if ($group eq 's4pa' || $group eq 'opsmon') {
            $staPath = $station;
        # s4pm station.list file only has each station's name
        } elsif ($group eq 's4pm') {
            $staPath = $rootDir . "/$station";
        }

        # check station running status
        my $status = staStatus($staPath);
        push(@runningSta, $station) if ($status);
    }
    close(LST);

    # write out updated station.list
    open(LST, ">$staListFile") or S4P::preish(8, "$0: Failed to open station list file: $!");
    foreach my $station (@runningSta) {
        print LST "$station\n";
    }
    close(LST);
}

sub instStatus {
    my ($group, $instance, $rootDir) = @_;

    $rootDir =~ s/\/$//;
    my $staListFile = $rootDir . "/station.list";
    open(LST, "<$staListFile") or S4P::preish(8, "$0: Failed to open station list file: $!");
    print uc($group) . " $instance:\n";
    my $onCount = 0;
    my $offCount = 0;
    my $staCount = 0;
    foreach my $station (<LST>) {
        chomp($station);
        $station =~ s/\/$//;
        my $staPath;
        # s4pa station.list file has each station's full path
        if ($group eq 's4pa' || $group eq 'opsmon') {
            $staPath = $station;
        # s4pm station.list file only has each station's name
        } elsif ($group eq 's4pm') {
            $staPath = $rootDir . "/$station";
        }

        # skip disabled stations
        if ($SKIP_DISABLE) {
            my $staConf = $staPath . "/station.cfg";
            my $disabled = `grep cfg_disable $staConf`;
            if ($disabled) {
                chomp($disabled);
                next if ($disabled =~ /=\s*1/);
            }
        }

        # check station running status
        my $status = staStatus($staPath);
        if ($status) {
            print "   $station: ON\n";
            $onCount++;
        } else {
            print "   $station: OFF\n";
            $offCount++;
        }
        $staCount++;
    }

    # instance stattions summary
    if ($onCount == 0) {
        print "   All stations are OFF\n";
    } elsif ($offCount == 0) {
        print "   All stations are ON\n";
    } else {
        print "   ON station: $onCount, OFF station: $offCount, Total: $staCount\n";
    }
}

sub staStatus {
    my ($staDir) = @_;

    my $station = basename($staDir);
    # assuming station is off be default
    my $staOn = 0;

    # station lock file
    my $staLock = "$staDir" . '/station.lock';
    # station is off if lock file does not exist
    return $staOn unless (-f $staLock);

    # capture station running pid from lock file
    my $pid = `cat $staLock`;
    chomp($pid);
    $pid = $1 if ( $pid =~ /^pid=(\d+)$/ );
    return $staOn unless ($pid);

    # check if there is a running s4p_station job with that pid
    # There could be multiple same stations from different instances/streams running
    my @pids = `ps -ef | grep s4p_station | grep $station | grep -v 'grep' | awk '{print \$2}'`;
    foreach my $runningPid (@pids) {
        chomp($runningPid);
        if ($pid == $runningPid) {
            $staOn = 1;
            last;
        }
    }

    return $staOn;
}

