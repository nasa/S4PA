#!/usr/bin/perl

=head1 NAME

s4pa_active_fs_monitor.pl - script for monitoring active file systems and
for notifying configured authority when they are full.

=head1 SYNOPSIS

s4pa_active_fs_monitor.pl 
[B<-f> I<config_file>]
[B<-o> I<history file>]

=head1 DESCRIPTION

s4pa_active_fs_monitor.pl accepts a configuration file that lists providers
and their active file system directory and a file to previous active file
system.

Configuration file consists of two items: $cfg_s4pa_root points S4PA station 
root directory and %cfg_provider_contact indicates the e-mail contact for
every provider. Keys of %cfg_provider_contact are the provider names and
its values are the corresponding contacts.

History file keeps track of the file systems that are being currently used
by each provider. 

=over 4

=item Sample configuration file

$cfg_s4pa_root  =  "/var/tmp/s4pa/stations/";

%cfg_provider_contact  = (
"airs" => undef,
"edos" => undef,
"trmm" => "mhegde\@pop600.gsfc.nasa.gov"
);

=back

=head1 AUTHORS

M. Hegde
Adnet Systems Inc

=cut

################################################################################
# $Id: s4pa_active_fs_monitor.pl,v 1.17 2019/05/29 19:25:33 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################
use strict;
use File::stat;
use File::Basename;
use Safe;
use Getopt::Std;
use S4P;
use Data::Dumper;
use S4PA::Receiving;
use Math::BigInt;
use Cwd;

my ( $opt ) = {};

# Get command line arguments
getopts( "f:o:", $opt );

S4P::perish( 1, "Specify configuration file (-f)" ) unless defined $opt->{f};

# Read configuration file
my $cpt = new Safe 'CFG';
$cpt->share( '$cfg_s4pa_root', '%cfg_provider_contact', 
    '%cfg_provider_threshold', '%cfg_recycle_threshold' );
$cpt->rdo($opt->{f}) or 
    S4P::perish(2, "Cannot read config file $opt->{f} in safe mode: ($!)");

# Retrieve history
my ( $oldActiveFs, $notifyStatus );
if ( -f $opt->{o} ) {
    local( $/ ) = undef;
    open( FH, $opt->{o} ) || S4P::perish( 3, "Failed to open $opt->{o} ($!)" );
    my $str = <FH>;
    close( FH );
    eval( $str ) || S4P::perish( 4, "Failed to evaluate $opt->{o} ($@)" );
}

$notifyStatus = {} unless defined $notifyStatus;
# Check whether the active file system has changed
my $newActiveFs = {};
my $host = `hostname`;
chomp $host;

my $stationDir = dirname( cwd() );

# Loop through all providers.
foreach my $provider ( keys %CFG::cfg_provider_contact ) {
    my $recvStaDir = $CFG::cfg_s4pa_root . "/receiving/$provider";
    my $fs = $recvStaDir . "/active_fs";
    my $link = readlink( $fs );

    # check ActiveFs.list file under provider's ReceiveData station directory. 
    # Skip low space alert if file existed (configured volumes setup).
    my $fsListFile = $CFG::cfg_s4pa_root . "/receiving/$provider/ActiveFs.list";
    unless ( -f $fsListFile ) {
        my $threshold = $CFG::cfg_provider_threshold{$provider};
        my $raiseAlert = lowSpaceAlert( $link, $threshold );
        if ( $raiseAlert ) {
            my $subject = "Low_FreeSpace_$provider";
            my $content = "$link is running low on space.";

            # don't send email if contact was not configure or
            # anomaly flag was already raised.
            if ( defined $CFG::cfg_provider_contact{$provider} ) {
                my $anomalyFile = S4P::anomaly_path( $subject, $stationDir );
                unless ( -f $anomalyFile ) {
                    my $cmd = "echo '$content' | mail -s '$subject'"
                        . " '$CFG::cfg_provider_contact{$provider}'"; 
                    `$cmd`;
                    S4P::perish( 8, "Failed to mail:\n$cmd ($!)" ) if ( $? >> 8 );
                }
            }
            S4P::raise_anomaly( $subject, $stationDir, 'WARN', $content, 2);
        }
    }

    # check active_fs link in provider's ReceiveData station directory. 
    # Skip if a contact is not defined
    next unless defined $CFG::cfg_provider_contact{$provider};
    S4P::perish( 5, "Active file system, $fs, is not a link ($!)" )
	unless defined $link;
    if ( defined $oldActiveFs->{$provider} ) {
	my $oldStat = stat( $oldActiveFs->{$provider} );
	my $newStat = stat( $link );
        if ( $notifyStatus->{$oldActiveFs->{$provider}} ) {
            S4P::logger( "INFO",
                "Notification has been sent already for provider=$provider,"
                . " file system=$oldActiveFs->{$provider}" );
            $newActiveFs->{$provider} = $link;
	} elsif ( $oldStat->dev != $newStat->dev 
	    || $oldStat->ino != $newStat->ino ) {
            # Compare inodes to make sure active file system hasn't changed.
            # If new active file system is found, notify; notification content
            # came from system administrators.
            
            # Make sure there are no active downloads
            my @activeJobList = ();
            if ( opendir( DH, $oldActiveFs->{$provider} ) ) {
                @activeJobList = grep( /\.RUNNING/, readdir( DH ) );
                closedir( DH );
            }
            if ( @activeJobList ) {
                S4P::logger( "INFO", "Active jobs found in"
                    . " $oldActiveFs->{$provider}" );
            }            
            S4P::perish( 1,
                "Failed to find .FS_COMPLETE in $oldActiveFs->{$provider}" )
                unless ( -f "$oldActiveFs->{$provider}/.FS_COMPLETE" );
            my $fsCompleteStat = 
                stat( "$oldActiveFs->{$provider}/.FS_COMPLETE" );
            my $notifyFlag = 1;
            my $warning = '';
            foreach my $activeJob ( @activeJobList ) {
                my $jobDir = $oldActiveFs->{$provider} . '/' . $activeJob;
                $jobDir =~ s#/\.RUNNING_#/#;
                if ( -d $jobDir ) {
                    # if job directory exist, log it but do not send 
                    # notification until the job is more than an hour old.
                    S4P::logger( "WARN", 
                        "Active jobs present in the file system: $jobDir ");
                    $notifyFlag = 0;
                    my $dirStat =
                        stat( $oldActiveFs->{$provider} . '/' . $activeJob );
                    # we should not compare the running PDR directory timestamp
                    # with the .FS_COMPLETE timestamp. Instead, it should compare
                    # to the current time.
                    # if ( abs($dirStat->mtime - $fsCompleteStat->mtime)> 3600 ) {
                    if ( abs($dirStat->mtime - time())> 3600 ) {
                        # $notifyFlag = 0;
                        $warning .= "$oldActiveFs->{$provider} has an"
                            . " active job indicator ($activeJob) that is older"
                            . " than an hour; please investigate.\n";
                        S4P::logger( "WARN", $warning );
                        my $subject = "Long_Running_Job_$provider";

                        # don't send email if contact was not configure or
                        # anomaly flag was already raised.
                        if ( defined $CFG::cfg_provider_contact{$provider} ) {
                            my $anomalyFile = S4P::anomaly_path( $subject, $stationDir );
                            unless ( -f $anomalyFile ) {
                                my $cmd = "echo '$warning' | mail -s '$subject'"
                                    . " '$CFG::cfg_provider_contact{$provider}'"; 
                                `$cmd`;
                                S4P::perish( 8, "Failed to mail:\n$cmd ($!)" ) if ( $? >> 8 );
                            }
                        }
                        S4P::raise_anomaly( $subject, $stationDir, 'WARN', $warning, 1);
                    }
                } else {
                    unlink( $oldActiveFs->{$provider} . '/' . $activeJob );
                    S4P::logger( "WARN",
                        "Removing $activeJob in $oldActiveFs->{$provider}" );
                }
            }
            
            if ( $notifyFlag ) {
	        my $subject = "S4PA backup of $host:$oldActiveFs->{$provider}"
		    . " requested";

                # replacing obsolote tape backup email body with current active_fs link
	        my $content = "Current active_fs is: $link";

	        # my $content;
                # my $regExp = qr/\/\.([^\/]+)\/(\d+)\/*$/;
	        # if ( $oldActiveFs->{$provider} =~ $regExp ) {
	        #     $content .=  "/usr/local/adm/bin/data.bck any $2 any $1\n\n";
	        # } else {
                #     S4P::perish( 6,
                #         "Pattrn matching failed for $oldActiveFs->{$provider}" );
                # }
	        # if ( $link =~ $regExp ) {
		#     $content .= "/usr/local/adm/bin/data.bck any $2 incr $1 "
                #         . "incr\n\n";
                #     my $hostname = `hostname`;
                #     chomp( $hostname );
                #     $content .= "echo $2 >| /usr/local/adm/etc/data.bck.$hostname"
                #         . ".$1.current\n\n";
                # } else {
                #     S4P::perish( 7, "Pattrn matching failed for $link" );
                # }

	        my $cmd = "echo '$content' | mail -s '$subject'"
		    . " '$CFG::cfg_provider_contact{$provider}'"; 
	        `$cmd`;
	        S4P::perish( 8, "Failed to mail:\n$cmd ($!)" ) if ( $? >> 8 );
	        $newActiveFs->{$provider} = $link;
                $notifyStatus->{$oldActiveFs->{$provider}} = 1;

                # raise anomaly if the new active_fs is the last volume
                my $raiseAlert = lowSpaceAlert( $link, '1.0' );
                if ( $raiseAlert ) {
                    $subject = "Last_Volume_$provider";
                    $content = "$link is the last configured volume.";
                    S4P::raise_anomaly( $subject, $stationDir, 'WARN', $content, 2);
	            my $cmd = "echo '$content' | mail -s '$subject'"
		        . " '$CFG::cfg_provider_contact{$provider}'"; 
                    `$cmd`;
                    S4P::perish( 8, "Failed to mail:\n$cmd ($!)" ) if ( $? >> 8 );
                }
            }
	}
    } else {
	$newActiveFs->{$provider} = $link;
    }
}

# recycle volumes if specified
if (%CFG::cfg_recycle_threshold) {
    foreach my $provider (keys %CFG::cfg_recycle_threshold) {
        my $threshold = $CFG::cfg_recycle_threshold{$provider};
        my $rootDir = $CFG::cfg_s4pa_root;
        my $recvDir = "$rootDir/receiving/$provider";

        # check ActiveFs.list file under provider's ReceiveData station directory. 
        my $listFile = "$recvDir/ActiveFs.list";
        unless (-f $listFile) {
            S4P::logger("WARN", "Failed to locate $listFile");
            S4P::perish(9, "Failed to locate $listFile");
        }
        my $cmd = "s4pa_recycle_volumes.pl -r $rootDir -p $provider -t $threshold";
        my ($errstr, $rc) = S4P::exec_system("$cmd");
        if ($rc) {
            S4P::logger('ERROR', "Failed to execute $cmd: $errstr");
        } else {
            S4P::logger('INFO', "Successfully executed volume recycle for $recvDir");
        }
    }
}

exit( 0 ) unless ( keys %$newActiveFs );

# Update the history file
foreach my $provider ( keys %$oldActiveFs ) {
    $newActiveFs->{$provider} = $oldActiveFs->{$provider}
	unless defined $newActiveFs->{$provider};
}

open( FH, ">$opt->{o}" )
    || S4P::perish( 9, "Failed to open $opt->{o} for writing ($!)" );
print FH Data::Dumper->Dump( [ $newActiveFs, $notifyStatus ], [ 'oldActiveFs', 'notifyStatus' ] );
close( FH );

sub lowSpaceAlert {
    my ( $activeFs, $threshold ) = @_;

    $activeFs  =~ /(.+)\/(\d+)\/*$/;
    my $nextFs = sprintf( "$1/%" . length($2) . '.' . length($2) . 'd/', $2+1 );
    return 0 if ( -d $nextFs );

    # bypass disk space checking if the current active_fs is the last volume
    # by setting $threshold to be 100%.
    return 1 if ( $threshold >= 1.0 );

    my $fsSizeFile = $activeFs . "/.FS_SIZE";
    if ( -f $fsSizeFile ) {
        # Case of directory based disk management
        # Deduct the reserved size from the file system size
        my @sizeList = S4PA::Receiving::DiskPartitionTracker( $fsSizeFile );
        my $lowSize = $sizeList[0]->numify() * $threshold;
        my $freeSize = $sizeList[1]->numify();
        return 1 if ( $freeSize < $lowSize );
    } else {
        # Case of file system based disk management.
        # Get file system information using UNIX df.
        my $fsinfo = (readpipe("/bin/df -k $activeFs"))[-1];
        # Extract individual columns in file system information.
        my ( undef, $fs_size, $fs_used, $fs_free ) = split( /\s+/, $fsinfo );
        my $fs_left = $fs_free / $fs_size;
        return 1 if ( $fs_left < $threshold );
    }
    return 0;
}

