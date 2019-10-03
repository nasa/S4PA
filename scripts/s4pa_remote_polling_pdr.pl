#!/usr/bin/perl

=head1 NAME

s4pa_remote_polling_pdr.pl - script to poll a remote directory for PDRs

=head1 SYNOPSIS

s4pa_remote_polling_pdr.pl
[B<-h>]
[B<-f> I<configuration file>]

OR

[B<-r> I<remote_host>]
[B<-p> I<remote_directory>]
[B<-d> I<local_directory>]
[B<-o> I<history_file>]
[B<-t> I<protocol: FTP or SFTP>]
[B<-e> I<PDR name pattern>]
[B<-i> I<ignore_history>]
[B<-r>]
I<workorder>

=head1 ARGUMENTS

=over

=item B<-h> 

Prints out synopsis.

=item B<-f> I<configuration file>

File containing parameters for polling: cfg_history_file, cfg_remote_host, 
cfg_remote_dir, cfg_local_dir, cfg_CFG::cfg_protocol, cfg_pdr_pattern, 
cfg_data_version. Command line options, if specified, will over ride these 
values. 

=item B<-r> I<remote_hostanme>

Hostname to ftp to in order to poll. A suitable entry in the .netrc file must 
exist for this host. (=cfg_remote_host in config file)

=item B<-p> I<remote_directory>

Directory on the remote host (as seen in the ftp session) which will be 
examined for new PDRs. (=cfg_remote_dir in config file)

=item B<-d> I<local_directory>

Local directory to which new PDRs will be directed.
(=cfg_local_dir in config file)

=item B<-o> I<history_file>

Local filename containing list of previously encountered PDRs.  Name defaults 
to "../oldlist.txt". (cfg_history_file in config file)

=item B<-t> I<protocol>

Specifies the protocol to be used for polling. Valids are FTP and SFTP. 
(=cfg_protocol in config file)

=item B<-e> I<PDR name pattern>

Optional argument which specifies filename pattern (default is \.PDR$)
(=cfg_pdr_pattern in config file)

=item B<-i> I<ignoer_history>

Optional argument which specifies if ignoring history checking
(=cfg_ignore_history in config file)

=item B<-r>

Optional recursive ftp polling flag.

=head1 DESCRIPTION

=head1 AUTHOR

Mike Theobald, NASA/GSFC, Code 902, Greenbelt, MD  20771.
T. Dorman, SSAI, NASA/GSFC, Greenbelt, MD 20771
M. Hegde, SSAI, NASA/GSFC, Greenbelt, MD 20771

=cut

###############################################################################
# s4pa_remote_polling_pdr.pl,v 1.24 2008/07/31 13:42:56 ffang Exp
# -@@@ S4PA, Version $Name:  $
###############################################################################
use strict;
use Safe;
use Net::FTP;
use Net::Netrc;
use Net::SFTP::Foreign;
use Getopt::Std;
use File::Basename;
use File::Copy;
use File::stat;
use S4P;
use S4P::TimeTools;
use S4P::PDR;
use S4PA;
use S4PA::Receiving;
use Log::Log4perl;
use XML::LibXML;
use Cwd;

use vars qw($opt_h $opt_f $opt_r $opt_p $opt_d $opt_o $opt_t $opt_e $opt_i $opt_m);

#Read and parse command line options
getopts( 'f:r:p:d:o:t:e:m:hi' );

S4P::perish( 1, "Use: $0 -f <config file>\n or"
    . " $0 -o <history file> -r <remote host> -p <remote dir> -d"
    . " <local dir> -t <protocol> -e <PDR pattern>" ) if $opt_h;
if ( $opt_f ) {
    # Read the configuration file if specified
    my $cpt = new Safe 'CFG';
    $cpt->share( '$cfg_history_file', '$cfg_remote_host', '$cfg_remote_dir',
        '$cfg_local_dir', '$cfg_protocol', '$cfg_pdr_pattern',
        '%cfg_data_version', '%cfg_exclude_data', '%cfg_pdr_filter',
        '$cfg_ignore_history', '$cfg_merge_pan' );
    $cpt->rdo($opt_f) or 
        S4P::perish(1, "Cannot read config file $opt_f in safe mode: ($!)");
}

my $logger = S4PA::CreateLogger( $CFG::cfg_logger{FILE},
	$CFG::cfg_logger{LEVEL} ) if ( %CFG::cfg_logger );
 
# Command line options over ride the definitions in configuration file
$CFG::cfg_history_file = $opt_o if defined $opt_o;
$CFG::cfg_remote_host = $opt_r if defined $opt_r;
$CFG::cfg_remote_dir = $opt_p if defined $opt_p;
$CFG::cfg_local_dir = $opt_d if defined $opt_d;
$CFG::cfg_protocol = $opt_t if defined $opt_t;
$CFG::cfg_pdr_pattern = $opt_e if defined $opt_e;
my $RECURSIVE = (defined $opt_r) ? 1 :
    (defined $CFG::cfg_recursive && $CFG::cfg_recursive eq 'true') ? 1 : 0;
my $MAX_DEPTH = (defined $opt_m) ? $opt_m :
    (defined $CFG::cfg_max_depth) ? $CFG::cfg_max_depth : 5;

S4P::perish( 1, "Specify history file (-o)" )
    unless defined $CFG::cfg_history_file;
S4P::perish( 1, "Specify host to be polled (-r)" )
    unless defined $CFG::cfg_remote_host;
S4P::perish( 1, "Specify directory to be polled (-p)" )
    unless defined $CFG::cfg_remote_dir;
S4P::perish( 1, "Specify local directory for downloading PDR (-d)" )
    unless defined $CFG::cfg_local_dir;
S4P::perish( 1, "Specify protocol (-t)" )
    unless defined $CFG::cfg_protocol;
$CFG::cfg_pdr_pattern = '\.PDR$' unless defined $CFG::cfg_pdr_pattern;
$CFG::cfg_local_dir .="/" unless ( $CFG::cfg_local_dir =~ /\/$/ );
    
my $ignore_history = ( defined $opt_i ) ?
    1 : ( defined $CFG::cfg_ignore_history && $CFG::cfg_ignore_history eq 'true' ) ?
    1 : 0;

# get PDR filters if defined
my %pdrFilter = %CFG::cfg_pdr_filter if (%CFG::cfg_pdr_filter);

# A hash to keep track of downloaded PDRs
my %oldlist;

# Lock a dummy file to lock out access old list file.
open( LOCKFH, ">$CFG::cfg_history_file.lock" )
    || S4P::perish( 1, "Failed to open lock file" );
unless( flock( LOCKFH, 2 ) ) {
    close( LOCKFH );
    S4P::perish( 1, "Failed to get a lock" );
}

# Read oldlist (%oldlist hash in external file of remote PDRs already processed)
open ( OLDLIST,"$CFG::cfg_history_file" ) 
    || S4P::logger( "WARN",
	"Failed to open oldlist file $CFG::cfg_history_file; created one" );

while ( <OLDLIST> ) {
    chomp() ;
    $oldlist{$_} = "old";
}
close(OLDLIST);

# Transfer status
my $xferstatus = 0;
if ( $CFG::cfg_protocol eq 'FTP' ) {
    # specify default firewall type
    my $firewallType = $ENV{FTP_FIREWALL_TYPE} ? $ENV{FTP_FIREWALL_TYPE} : 1;
    my $ftpPassive = defined $ENV{FTP_PASSIVE} ? $ENV{FTP_PASSIVE} : 1;
    my $remoteHost = $CFG::cfg_remote_host;
    my $remoteDir = $CFG::cfg_remote_dir;

    # Open FTP connection, login, cd to polldir and ls contents.
    my $ftp;
    if ( $ENV{FTP_FIREWALL} ) {
        # Create an Net::FTP object with Firewall option
        my $firewall = $ENV{FTP_FIREWALL};
        if ( defined $ENV{FTP_PASSIVE} ) {
            $ftp = Net::FTP->new( $remoteHost, Timeout => 900,
                Firewall => $firewall, FirewallType => $firewallType,
                Passive => $ftpPassive );
        } else {
            $ftp = Net::FTP->new( $remoteHost, Timeout => 900,
                Firewall => $firewall, FirewallType => $firewallType );
        }
    } else {
        # No firewall specified, let .libnetrc resolve if firewall is required
        if ( defined $ENV{FTP_PASSIVE} ) {
            $ftp = Net::FTP->new( $remoteHost, Timeout => 900,
                Passive => $ftpPassive );
        } else {
            $ftp = Net::FTP->new( $remoteHost, Timeout => 900 );
        }
    }

    S4P::perish( 1, "Failed to create an FTP object for $remoteHost" )
        unless defined $ftp;
    S4P::perish( 1,
	"Failed to login to $remoteHost (" . $ftp->message() . ")" ) 
        unless $ftp->login();
    S4P::logger( "INFO",
	"Beginning scan of $remoteHost:$remoteDir" );

    # Use binary mode since $ftp->size is not allowed in ASCII mode
    $ftp->binary();

    my @remfiles = ftp_poll($ftp, $remoteHost, $remoteDir);
    my $numFile = scalar ( @remfiles );
    S4P::logger( "INFO", "$numFile files found in $remoteDir" );
	$logger->debug( "Found $numFile files in remote directory" )
        if defined $logger;

    # Check contents against oldlist, transfer any new, and update oldlist
    foreach my $remfile ( @remfiles ) {
        # skip non-specified pattern files
        my $filename = basename($remfile);
        next unless ( $filename =~ m/$CFG::cfg_pdr_pattern/ );

        # for backward compatible, we store filename in history file.
        # but, we do need the full pathname for recursive polling
        my $hisRecord = $remfile;
        $hisRecord = basename($remfile) unless ($RECURSIVE);

        if ( $oldlist{$hisRecord} eq 'old' && $ignore_history == 0 ) {
            S4P::logger( "INFO", "$hisRecord is old: skipping" );
            $logger->debug( "Skipped old $hisRecord" ) if defined $logger;
            next;
        }

        # skip empty file
        my $fs = $ftp->size( $remfile );
        if (defined $fs && $fs == 0) {
            S4P::logger( "INFO", "$remfile has zero size: skipping" );
            $logger->debug( "Skipped zero-size $remfile" ) if defined $logger;
            next;
        }
    
	# Transfer files to local directory
        if ( $ftp->get( $remfile ) ) {
            $oldlist{$hisRecord} = ( $oldlist{$hisRecord} eq 'old' ) ? 'repoll' : 'new';
            $logger->info( "Transferred $remfile via ftp" ) if defined $logger;
        } else {
            S4P::logger( "ERROR",
                "Failure  transfer of $remfile (" . $ftp->message . ")"  );
            $logger->error( "Failed transferring $remfile" )
                if defined $logger;
        }
    }
    # Gracefully, close session.
    $ftp->quit() if (ref($ftp) eq 'Net::FTP');

} elsif ( $CFG::cfg_protocol eq 'SFTP' ) {
    my $remoteHost = $CFG::cfg_remote_host;
    my $remoteDir = $CFG::cfg_remote_dir;
    $remoteDir =~ s/\/+$//;

    # making an SFTP connection to remote host
    my $sftp = S4PA::Receiving::SftpConnect($remoteHost);
    S4P::perish(1, "Failed to SFTP connect to $remoteHost: $sftp->error")
        if ($sftp->error);

    # directory listing without hidden file
    my @listPath;
    if ($RECURSIVE) {
        my @lists = $sftp->find($remoteDir, no_wanted => qr/^\./, follow_links => 1);
        S4P::perish(1, "Failed to poll $remoteDir directory: $sftp->error") if ($sftp->error);
        foreach my $entry (@lists) {
            # directory has no 'longname', skip it
            next unless (exists $entry->{longname});
            push @listPath, $entry;
        }
    } else {
        my $lists = $sftp->ls($remoteDir, no_wanted => qr/^\./, follow_links => 1);
        S4P::perish(1, "Failed to poll $remoteDir directory: $sftp->error") if ($sftp->error);
        foreach my $entry (@{$lists}) {
            # skip directory which has longname start with 'd'
            my $longname = $entry->{longname};
            next if ($longname =~ /^d/);
            push @listPath, $entry;
        }
    }

    foreach my $entry (@listPath) {
        my $pathname = $entry->{filename};
        # filename from single directory list has no path
        my $filename = basename($pathname);
        my $size = $entry->{a}{size};

        # Skip non-pattern extension file (default .PDR)
        next unless ($filename =~ m/$CFG::cfg_pdr_pattern/);

        if ($oldlist{$pathname} eq 'old' && $ignore_history == 0) {
            S4P::logger( "INFO", "$pathname is old: skipping" );
            $logger->debug( "Skipped old $pathname" ) if defined $logger;
            next;
        }

        # Skip empty file
        if (defined $size and $size == 0) {
            S4P::logger("INFO", "$pathname has zero size: skipping");
            $logger->debug("Skipped zero-size $pathname") if defined $logger;
            next;
        }
        S4P::logger("INFO", "Found new PDR: $pathname");
        $logger->info("Found new $pathname") if defined $logger;

        # recursive polling already have full path
        my $remotefile;
        if ($RECURSIVE) {
            $remotefile = "$pathname";
        } else {
            $remotefile = "$remoteDir/$filename";
        }

        my $localfile = "./$filename";
        # transfer file to local directory without the remote file timestamp
        $sftp->get($remotefile, $localfile, perm => 0644, copy_time => 0);
        if ($sftp->error) {
            S4P::logger("ERROR", "PDR polling of $remoteHost:$remoteDir failed.");
            $logger->error("Failed polling of $remoteHost:$remoteDir.") if defined $logger;
            next;
        }

        # add transferred PDR to %oldlist
        $oldlist{$pathname} = ($oldlist{$pathname} eq 'old') ? 'repoll' : 'new';
    }
    $logger->info( "Transferred all new PDRs" ) if defined $logger;
    # disconnect from remote host
    $sftp->disconnect() if (defined $sftp);

} elsif ( $CFG::cfg_protocol eq 'FILE' ) {
    my @remfiles;
    my $remoteDir = $CFG::cfg_remote_dir;
    $remoteDir .= '/' unless ($remoteDir =~ /\/$/);

    my $recursive = ($MAX_DEPTH > 1) ? 1 : 0;
    if ($recursive) {
        @remfiles = file_poll($remoteDir);
    } else {
        @remfiles = glob("$remoteDir*");
    }

    foreach my $remfile ( @remfiles ) {
        # skip non-specified pattern files
        my $filename = basename($remfile);
        next unless ( $filename =~ m/$CFG::cfg_pdr_pattern/ );

        # for backward compatible, we store filename in history file.
        # but, we do need the full pathname for recursive polling
        my $hisRecord = $remfile;
        $hisRecord = basename($remfile) unless ($recursive);

        # skip files already in history
        if ( $oldlist{$hisRecord} eq 'old' && $ignore_history == 0 ) {
            S4P::logger( "INFO", "$hisRecord is old: skipping" );
            $logger->debug( "Skipped old $hisRecord" ) if defined $logger;
            next;
        }

        # skip empty files
        my $fileStats = stat($remfile);
        if ($fileStats->size == 0) {
            S4P::logger( "INFO", "$hisRecord has zero size: skipping" );
            $logger->debug( "Skipped old $hisRecord" ) if defined $logger;
            next;
        }
        
        my $localfile = "./" . basename($remfile);
        if ( File::Copy::copy( $remfile, $localfile ) ) {
            S4P::logger( "INFO", "Success in copy of $remfile" );
            $logger->info( "Copied local $remfile" ) if defined $logger;
            $oldlist{$hisRecord} = ( $oldlist{$hisRecord} eq 'old' ) ? 'repoll' : 'new';
        } else {
            S4P::logger ( "ERROR", "Failure to copy " . $remfile
                         . " to $CFG::cfg_local_dir" );
            $logger->error( "Failed copying $remfile to " .
                "$CFG::cfg_local_dir" ) if defined $logger;
        }
    }
}

my $currentDir = cwd();
# Create new oldlist
open ( OLDLIST, ">>$CFG::cfg_history_file" )
    || S4P::perish( 1,
		"Failed open history file $CFG::cfg_history_file for writing ($!)" );
foreach my $remfile ( sort keys( %oldlist ) ) {
    next if ( $oldlist{$remfile} eq "old" );

    # If successful in transfering file, move the file to local 
    # directory and make the file writable. Save the file name in 
    # history.

    # Read PDR
    my $pdrName = basename( $remfile );
    my $originalPdr = S4P::PDR::read_pdr( $pdrName );
    my $originatingSystem;
    if ( defined $originalPdr ) {
        # save the originating_system before pdr splitting
        $originatingSystem = $originalPdr->originating_system;
    } else {
        # If PDR is not defined complain and continue.
        # ReceiveData will handle PDR related errors
        S4P::logger( "ERROR", "$pdrName is invalid; $S4P::PDR::errstr" );
        $logger->error( "Failed reading PDR $pdrName" ) if defined $logger;
    }

    # $pdrName could be overwritten by pdrFilter, 
    # save the original pdr filename in $pdrFile.
    my $pdrFile = $pdrName;

    # Run filter on PDR if defined pdr filter
    my $action = '';
    my $merge_pan = ( $CFG::cfg_merge_pan eq 'true' ) ? 1 : 0;
    my @pdrFiltered;

    if (%pdrFilter) {
        # First check for filter pattern
        foreach my $filter (keys (%pdrFilter)) {
            next unless ($pdrName =~ m/$filter/);
            $action = $pdrFilter{$filter};
            last;
        }
        if ($action) {
            # pdrFilter script is required to return an list of PDR names.
            # The first member of the list will be the filtered PDR for 
            # this instance and will get passed down to receiving station;
            # For a splitting PDR and merging PAN case, the rest of the 
            # members in the retured list need to be staged on a staging area
            # by the pdrfilter script for other instnace to poll.
            my $filterResponse = `$action $pdrName`;
            S4P::perish(1, "Failed to run PDR filter $action on $pdrName ($?)") if ($?);
            chomp $filterResponse;
            @pdrFiltered = split ( /\s/, $filterResponse );
            $pdrName =  $pdrFiltered[0];
            chomp $pdrName;
        }
    }

    # when pdrFilter split PDR, pan merging will be required. 
    # we need to create a work order for the merge_pan station. 
    if ( $merge_pan && defined $originatingSystem ) {
        # create work order for downstream merge PAN station
        my $wo = "MERGE.$pdrFile.wo";
        my $status = merge_wo( $wo, $pdrFile, $originatingSystem, \@pdrFiltered );
        S4P::perish( 1, "Failed to create MERGE work order: $wo" ) if ( $status );
        $logger->info( "Create merge PAN station work order $wo for $pdrName" )
            if defined $logger;

        if ( move( $pdrFile, $CFG::cfg_local_dir ) ) {
            $logger->info( "Moved $pdrFile to $CFG::cfg_local_dir" )
                if defined $logger;
            chmod( 0644, "$CFG::cfg_local_dir/$pdrFile" );
            print OLDLIST "$remfile\n" if ( $oldlist{$remfile} eq 'new' );
        } else {
            S4P::logger( "ERROR", "Failure to move $pdrFile to"
                . " $CFG::cfg_local_dir ($!); removing file" );
            unlink ( $pdrFile );
            $logger->error( "Failed moving $pdrFile to " .
                "$CFG::cfg_local_dir, deleted file" ) if defined $logger;
        }

        # all done, no need to check datatype or version since on split PDR. 
        unlink ($pdrFile) if (-e $pdrFile);
        next;
    }

    # Read filtered PDR
    my $pdr = S4P::PDR::read_pdr( $pdrName );

    # Check whether there is data type/version to be excluded
    my $supportedDataType = 1;
    if ( defined $pdr ) {
        if ( %CFG::cfg_exclude_data ) {
            # Case of polling with exclusion conditions.
            # By default, all datatypes are not supported.
            $supportedDataType = 0;
            foreach my $fileGroup ( @{$pdr->file_groups()} ) {
                my $dataType = $fileGroup->data_type();
                my $dataVersion = $fileGroup->data_version();
                # Support the dataset if it is not specifically excluded
                $supportedDataType = 1
                    unless ( $CFG::cfg_exclude_data{$dataType}{$dataVersion} 
                    || $CFG::cfg_exclude_data{$dataType}{''} );
            }
        } elsif ( %CFG::cfg_data_version ) {
            # Case of polling with inclusion conditions.
            foreach my $fileGroup ( @{$pdr->file_groups()} ) {
                my $dataType = $fileGroup->data_type();
                my $dataVersion = $fileGroup->data_version();
                if ( defined $CFG::cfg_data_version{$dataType} ) {
                    $supportedDataType = 0
                    unless ( $CFG::cfg_data_version{$dataType}{$dataVersion}
                        || $CFG::cfg_data_version{$dataType}{''} );
                } else {
                    $supportedDataType = 0;
                    S4P::logger( "WARNING", "Data type=$dataType,"
                         . " Data version=$dataVersion is unsupported" );
                    $logger->debug( "Unsupported datatype $dataType " .
                         "version $dataVersion" ) if defined $logger;
                    S4P::raise_anomaly('Unsuported_Datatype', dirname($currentDir),
                         'WARN', "$dataType is not supported, $pdrName skipped.", 0);
                }
            }
        } 
    } else {
        # If PDR is not defined complain and transfer PDR to ReceiveData.
        # ReceiveData will handle PDR related errors
        S4P::logger( "ERROR", "$pdrName is invalid; $S4P::PDR::errstr" );
        $logger->error( "Failed reading PDR $pdrName" ) if defined $logger;
    }

    if ( $supportedDataType ) {
        if ( move( $pdrName, $CFG::cfg_local_dir ) ) {
            $logger->info( "Moved $pdrName to $CFG::cfg_local_dir" )
                if defined $logger;
            chmod( 0644, "$CFG::cfg_local_dir/$remfile" );
            print OLDLIST "$remfile\n" if ( $oldlist{$remfile} eq 'new' );
        } else {
            S4P::logger( "ERROR", "Failure to move $pdrName to"
                . " $CFG::cfg_local_dir ($!); removing file" );
            unlink ( $pdrName );
            $logger->error( "Failed moving $pdrName to " .
                "$CFG::cfg_local_dir, deleted file" ) if defined $logger;
        }
    } else {
        print OLDLIST "$remfile\n" if ( $oldlist{$remfile} eq 'new' );
        S4P::logger("ERROR", 
            "$pdrName has unsupported or excluded data type/version; skipping" );
        unlink( $pdrName ); 
        $logger->error( "Unsupported or excluded datatype in $pdrName, " .
            "deleted it" ) if defined $logger;
    }

    # remove the original pdr if exist
    unlink ($pdrFile) if (-e $pdrFile);
}
close( OLDLIST );

# Remove lock
close( LOCKFH );
flock( LOCKFH, 8 );
exit( 0 );

##############################################################################
# merge_wo:  create merge pan work order
##############################################################################
sub merge_wo {
    my ( $wo, $pdrName, $originatingSystem, $splitPdr ) = @_;
    
    # create work order for downstream merge PAN station
    my $wo_parser = XML::LibXML->new();
    my $wo_dom = $wo_parser->parse_string('<workOrder/>');
    my $wo_doc = $wo_dom->documentElement();

    my $pdrNode = XML::LibXML::Element->new( 'OriginalPdr' );
    $pdrNode->setAttribute( 'NAME', $pdrName );
    $pdrNode->appendTextChild( 'OriginatingSystem', $originatingSystem );
    $pdrNode->setAttribute( 'TIME', S4P::TimeTools::CCSDSa_Now );
    $wo_doc->appendChild( $pdrNode );

    foreach my $split ( @$splitPdr ) {
        chomp $split;
        my $splitPdrNode = XML::LibXML::Element->new( 'SplitPdr' );
        $splitPdrNode->setAttribute( 'NAME', basename($split) );
        $splitPdrNode->setAttribute( 'PATH', dirname($split) );

        my $pdr = S4P::PDR::read_pdr( $split );
        $splitPdrNode->appendTextChild( 'PdrText', $pdr->sprint() );
        $wo_doc->appendChild( $splitPdrNode );
    }

    open (WO, ">$wo") || S4P::perish( 1, "Failed to open workorder file $wo: $!");
    print WO $wo_dom->toString(1);
    close WO;

    return(0) ;
}

sub file_poll {
    my ( $dirName, $depth ) = @_;

    $depth ||= 0;
    my @fileList;

    if ( $dirName !~ /^\Q$dirName/o || $depth >= $MAX_DEPTH ) {
        # return $dirName;
        return;
    }

    $dirName .= '/' unless ( $dirName =~ /\/$/ );
    my @contentList = ();
    if ( opendir( FH, $dirName ) ) {
        @contentList = map{ "$dirName$_" } grep( !/^\.+$/, readdir( FH ) );
        close( FH );
        foreach my $entry ( @contentList ) {
            if ( -d $entry ) {
                my @dirList = file_poll( $entry, $depth+1 );
                push @fileList, @dirList;
            } elsif ( -f $entry ) {
                push @fileList, $entry;
            }
        }
    } else {
        die "Failed to open $dirName for reading ($!)";
    }
    return @fileList;
}

sub ftp_poll
{
    my ($ftp, $hostname, $polldir) = @_;
    S4P::logger( "INFO", "Beginning scan of $hostname: $polldir" );
    $logger->info( "Scanning $hostname:$polldir" ) if defined $logger;

    my @fileList;
    # Make sure the polling directory begins with a '/' and ends with '/'. It is
    # just for easier coding later.
    $polldir .= '/' unless ( $polldir =~ /\/$/ );
    unless ( $polldir =~ /^\// ) {
        my $homeDir = $ftp->pwd();
        $polldir = $homeDir . "/$polldir";
    }

    my @dirStack = ( $polldir );
    # Accumulate remote files, skipping files already in history.
    while ( my $dir = shift @dirStack ) {
        S4P::logger( "INFO", "Scanning $dir" );
        unless ( $ftp->cwd( $dir ) ) {
            S4P::logger( "WARN",
                "Failed to change directory to $dir:" . $ftp->message );
            next;
        }
        # Get a long listing of directory
        my @list = $ftp->dir();
        # Switch to using "ls -lL" if dir() doesn't produce a long listing
        if ( @list > 0 ) {
            # some ftp site's directory listing came back as d--------- as directory
            @list = $ftp->ls( "-lL" ) unless ( $list[-1] =~ /^[d-][r-]/ );
        }
        # Get a listing of files first
        foreach my $item ( @list ) {
            # some ftp site's directory listing came back as d--------- as directory
            if ( $RECURSIVE && $item =~ /^d[r-][w-][x-]/ ) {
                my $subDir = (split( /\s+/, $item))[-1];
                # implement subdirectory pattern
                my $subPath = $dir . $subDir . '/';
                push( @dirStack, $subPath ) unless ( $subDir =~ /^\./ );
            } else {
                # skip directory if not recursive
                next if ( $item =~ /^d/ );
                my $file = (split( /\s+/, $item))[-1];
                if ( defined $file && $file !~ /^\./ ) {
                    my $filePath = $dir . $file;
                    push @fileList, $filePath;
                }
            }
        }
    }
    return @fileList;
}

