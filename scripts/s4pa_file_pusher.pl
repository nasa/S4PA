#!/usr/bin/perl

=head1 NAME

s4pa_file_pusher.pl - script to push files via FTP/SFTP to a remote host.

=head1 SYNOPSIS

s4pa_file_pusher.pl
I<-f config file>
I<-d temp filter working dir>
I<-n>
I<workorder>

=head1 ARGUMENTS

=over
=item I<-f configuration file>
Post office configuration file

=item I<-d temporary directory>
Temporary root working directory for filter

=item I<-n>
Flag to trigger creation of data notification when data transfer is complete.

=item I<workorder>

Work order should be an XML file in the format

<FilePacket status="I|C"
		label="userlabel"
		notify="mailto: xx@yy.zz.com"
		messageFormat="S4PA|LEGACY|PDR"
		[destination="[s]ftp:yy.zz.com/dir|mailto:xx2@aa.bb.edu"]
		[numAttempt="n"]
		[completed="<timestamp>"] >
    <FileGroup>
	<File status="I|C"
        	url="ftp://xxdisc.gsfc.nasa.gov/data/yyyy/zzzz/2006/365/filename"
        	localPath="/ftp/.provider/nnn/zzzz/filename"
        	[filter="filter-program" [filteredFilepath="/some/place/filename2" filteredSize="nnn"]]
                [cksumtype="CKSUM|MD5"]
                [cksumvalue="nnn"]
        	[cleanup="N"]
        	[completed="<timestamp>"] />
    </FileGroup>
</FilePacket>

=head1 DESCRIPTION

This script pushes files specified in the work order that have not yet
successfully been pushed (whose status is not yet "C") to a remote
host/directory using the specified protocol (running "sendmail" for "mailto:",
calling S4PA::Receiving::put for SFTP/FTP/BBFTP push).  Unsuccessful attempts cause
the work order to be rescheduled.  Once fully successful, an EMAIL work order is
created to send out a notification email.

=head1 AUTHOR

M. Hegde, NASA/GSFC, Code 610.2, Greenbelt, MD  20771.
J. Pan, NASA/GSFC, Code 610.2, Greenbelt, MD  20771.

=head1 CHANGELOG

02/15/06 Initial version
09/25/06 J Pan     Added cksum verification if wo provides them

=cut

################################################################################
# $Id: s4pa_file_pusher.pl,v 1.38 2019/05/06 15:48:00 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################
use strict;
use Net::Netrc;
use Net::FTP;
use Net::SFTP::Foreign;
use XML::LibXML;
use File::Basename;
use Getopt::Std;
use Cwd;
use Safe;
use S4P;
use S4PA;
use S4PA::Receiving;
use S4PA::Storage;
use Log::Log4perl;
use vars qw($opt_n $opt_f $opt_d);

# Use Net::SSH2 only if it is available
BEGIN{
    eval 'use Net::SSH2';
}
getopts('f:d:n');
S4P::perish( 1, "Specify a work order" ) unless( @ARGV );

my $dummyName = basename( $ARGV[0] );
my ( $jobType, $jobId ) = ( $2, $3 )
    if ( $dummyName =~ /^(DO\.)?([^.]+)\.(.+)(\.wo)?$/ );

# Read configuration file
if ( -f $opt_f ) {
    my $cpt = new Safe 'CFG';
    $cpt->share( '$max_attempt' );
    $cpt->rdo($opt_f) or
        S4P::perish(2, "Cannot read config file $opt_f in safe mode: ($!)");
    undef $cpt;
}

my $logger = S4PA::CreateLogger( $CFG::cfg_logger{FILE},
    $CFG::cfg_logger{LEVEL} ) if ( %CFG::cfg_logger );

# Read work order
my $xmlParser = XML::LibXML->new();
$xmlParser->keep_blanks(0);
my $dom = $xmlParser->parse_file( $ARGV[0] );
S4P::perish( 1, "Failed to read work order $ARGV[0]" ) unless $dom;
my $doc = $dom->documentElement();
S4P::perish( 1, "Failed to find document element in $ARGV[0]" ) unless $doc;
$logger->info( "Processing work order $ARGV[0]" ) if defined $logger;

if ( defined $CFG::max_attempt ) {
    my $numAttempt = $doc->getAttribute( 'numAttempt' );
    $numAttempt = 1 unless defined $numAttempt;
    if ( $numAttempt > $CFG::max_attempt ) {
        if ( open( FH, ">$ARGV[0]" ) ) {
	    $doc->setAttribute( 'numAttempt', 0 );
	    print FH $dom->toString( 1 );
	    close( FH ) ||
		S4P::logger( "ERROR",
		    "Failed to close $ARGV[0] while writing ($!)" );
	} else {
	    S4P::logger( "ERROR", "Failed to open $ARGV[0] for writing ($!)" );
	}
	S4P::perish( 1, "Number of attempts exceeded $CFG::max_attempt" );
    }
} 
#
# Find FilePacket node and its status.  Should be just one, at the root.
my ( $packetNode ) = $doc->findnodes('/FilePacket');
my $packetStatus = $packetNode->getAttribute('status');
my $destination = $packetNode->getAttribute('destination') or
                      S4P::perish( 1, "Specify destination");
my $verifyFlag = $packetNode->getAttribute( 'verify' );
$verifyFlag = ( $verifyFlag eq 'yes' ) ? 1 : 0;

if ( $packetStatus eq "C" ) {
    # Do nothing, packet complete.  (Should not occur.)
    exit(0);
}

# Parse for protocol and destination information
my ($protocol, $dest_string) = split(':', $destination, 2);
S4P::perish(1, "Invalid destination $destination")
    unless $protocol and $dest_string;
my ($remoteHost, $remoteDir);
($remoteHost, $remoteDir) = ($1,$2)
    if ( $dest_string =~ /([^\/]+)(.+)/ );
S4P::perish(1, "Invalid host/remotedir in $destination")
    unless $remoteHost and $remoteDir;

# Read temporary root working directory for filter
my $filterRootDir = $opt_d;
$filterRootDir .= "/" if ($filterRootDir !~ /\/$/);
if (!(-d $filterRootDir)) {
    S4P::perish(1, "Global directory $filterRootDir does not exist: ($!)");
}

# Construct relative working path for filter
S4P::perish(1, "Job ID not defined for incoming work order") if (!$jobId);
my $relPath = $jobId;
my $subId = $packetNode->getAttribute('id');
if (!$subId) {
    S4P::logger("WARN", "FilePacket has no id");
} else {
    $relPath .= "_$subId";
}

# Record local station directory
my $localDir = getcwd();
S4P::logger("INFO", "Local directory is $localDir");

# Define filter working directory
my $filterWorkingDir = $filterRootDir . $relPath;

# Loop through file nodes and process the transfer protocol on the file
my $filePacketStatus = 'C';
# Used for holding old hostname for persistent FTP connections
my $currentSession;
foreach my $fileNode ( $doc->findnodes( '//File' ) ) {
    # Check status attribute of the file node
    my $status = $fileNode->getAttribute('status') || 'I';
    # Done with this node if marked complete.
    next if ( $status eq "C" );

    # Get other attributes of the file node: localPath, filter, and cleanup.
    # Url attribute is just used for the notification; localPath is used to push the
    # data.
    my $localPath = $fileNode->getAttribute('localPath') or
                     S4P::perish( 1, "Specify local path" );
    my $filter = $fileNode->getAttribute('filter');
    my $cleanup = $fileNode->getAttribute('cleanup');
    $cleanup = 'N' unless defined $cleanup;
    my $cksumtype = $fileNode->getAttribute('cksumtype');
    my $cksumvalue = $fileNode->getAttribute('cksumvalue');

    if ( defined $cksumtype && defined $cksumvalue ) {
	# Perform cksum verification
	my $cksum_recompute = S4PA::Storage::ComputeCRC($localPath, $cksumtype);
	S4P::perish(1, "Verify cksum failed ($cksum_recompute $cksumvalue)")
	     if $cksumvalue ne $cksum_recompute;
    }

    # Execute filter if necessary
    my $localPath_filtered;
    if ( defined $filter ) {
        # Make filter working directory if does not exist
        if (!(-d $filterWorkingDir)) {
            unless (mkdir($filterWorkingDir,0775)) {
                S4P::perish(1, "Cannot make directory $filterWorkingDir: ($!)");
            }
        }
    	# Run filter program to process file and produce filtered result;
    	# filter should send back filtered file pathname on stdout.
        unless (chdir $filterWorkingDir) {
            S4P::logger("ERROR", "Cannot change filter working directory to $filterWorkingDir: ($!)");
        }
        $localPath_filtered = `$filter $localPath`;
        if ( $? ) {
            S4P::logger( "ERROR", "Filter failed: $filter $localPath ($!)" );
            undef $localPath_filtered;
        } else {
            # prepend $filterWorkingDir if not full-path returned by $filter
            $localPath_filtered =~ s/\s//g;
            if ($localPath_filtered && ($localPath_filtered !~ /^\//)) {
                $localPath_filtered = $filterWorkingDir . "/$localPath_filtered";
            }
            chomp $localPath_filtered;
        }
        unless (chdir $localDir) {
            S4P::logger("ERROR", "Cannot change directory to $localDir: ($!)");
        }
    } else {
        $localPath_filtered = $localPath;
    }

    # Check to see if filter produced anything
    if ( not defined $localPath_filtered ) {
        # Case of filter failing
        $status = 'I';
    } elsif ( defined $filter && $localPath_filtered eq '' ) {
        # Case of filter succeeding with no output
        $status = 'C';
        S4P::logger( 'INFO',
            "Filter was successful; didn't produce anything!" );
    } else {
        # Execute the transfer protocol
        if ($protocol eq "mailto") {
    		# Push file itself by sendmail.
            if ( mail_file($dest_string, $localPath_filtered) ) {
                $status = 'C';
                $logger->info( "Pushed file $localPath_filtered via " .
                    "email to $dest_string" ) if defined $logger;
            } else {
                S4P::logger( "ERROR", "Failed to email file to $dest_string"
                    . " (file: $localPath_filtered)");
                $logger->error( "Failed pushing file $localPath_filtered via " .
                    "email to $dest_string" ) if defined $logger;
            }
        } else {
            # Push by [s]ftp, file or bbftp.
            my $rc = S4PA::Receiving::put( file => $localPath_filtered,
                      host => $remoteHost,
                      dir => $remoteDir,
                      protocol => uc($protocol),
                      session => $currentSession,
                      verify => $verifyFlag,
		      logger => $logger );
        
            if ( $rc ) {
                $status = 'C';
                $logger->info( "Pushed file $localPath_filtered via " .
                    "$protocol to $remoteHost:$remoteDir" ) if defined $logger;
                if ((ref($rc) eq 'Net::FTP') || (ref($rc) eq 'Net::SFTP::Foreign')) {
                    $currentSession = $rc;
                }
            } else {
                S4P::logger( "ERROR",
                    "Failed to push $localPath_filtered to $remoteHost:$remoteDir"
                    . " by $protocol");
                $logger->error( "Failed pushing file $localPath_filtered via " .
                    "$protocol to $remoteHost:$remoteDir" ) if defined $logger;
            }
        }
    }   # End of if ( $localPathFiltered eq '' )

    if ( $status eq 'C' ) {
        # Modify attributes of this file node to note time of completion.
        my $tm_stamp = `date "+%Y-%m-%d %H:%M:%S"`;
        chomp $tm_stamp;
        $fileNode->setAttribute("status" => "C");
        $fileNode->setAttribute("completed" => $tm_stamp);
        if ( defined $filter ) {
            if ( $localPath_filtered eq '' ) {
                $fileNode->setAttribute( 'filteredFilepath' => '' );
            } else {
                $fileNode->setAttribute(
                    "filteredFilepath" => $localPath_filtered );
                $fileNode->setAttribute(
                    "filteredSize" => -s $localPath_filtered );
                # Perform cksum calculation for $localPath_filtered
                my $cksum_filtered = S4PA::Storage::ComputeCRC($localPath_filtered, $cksumtype);
                $fileNode->setAttribute(
                    "filteredCksumtype" => $cksumtype );
                $fileNode->setAttribute(
                    "filteredCksumvalue" => $cksum_filtered );
            }
        }
        # Clean up local file if asked to do so
        unlink $localPath if $cleanup eq "Y";
    } else {
    	    # Anything not complete makes the packet incomplete.
        $filePacketStatus = 'I';
    }

    # Clean up filtered version if any.
    unlink $localPath_filtered if defined $filter;

}  # END OF foreach(fileNode)

if (-d $filterWorkingDir) {
    unless (rmdir $filterWorkingDir) {
        S4P::logger( "ERROR", "Failed to remove directory $filterWorkingDir");
    }
}

# close FTP/SFTP connection
if ($protocol eq 'FTP') {
    $currentSession->quit();
} elsif (($protocol eq 'SFTP') && (defined $currentSession)) {
    $currentSession->disconnect();
}

#
# Update status of the PacketNode and write out the new work order.
# Write EMAIL work order if we're complete (and add time stamp) to
# send out notification email, or same type work order as before
# if we need to retry.
#
my $numAttempt = $packetNode->getAttribute( 'numAttempt' ) || 0;
$packetNode->setAttribute( "status" => $filePacketStatus );
$packetNode->setAttribute( "numAttempt" => $numAttempt+1 );
if ( $filePacketStatus eq 'C' ) {
    my $tm_stamp = `date "+%Y-%m-%d %H:%M:%S"`;
    chomp $tm_stamp;
    $packetNode->setAttribute( "completed" => $tm_stamp );
}

my $outWorkOrder;
if ( $opt_n ) {
    # Generate a work order for DN creation if file transfers are complete.
    # Generate a work order for file transfer if all files haven't been 
    # transfered.
    $outWorkOrder = ( $filePacketStatus eq 'C' )
	? "EMAIL.$jobId.wo" : "$jobType.$jobId.wo";
} else {
    # Generate a work order for file transfer if all files haven't been 
    # transfered.
    $outWorkOrder = ( $filePacketStatus eq 'C' )
	? undef : "$jobType.$jobId.wo";
}

if ( defined $outWorkOrder ) {
    local( *FH );
    open( FH, ">$outWorkOrder" )
	|| S4P::perish( 1, "Failed to open $outWorkOrder ($!)" );
    print FH $dom->toString(1);
    close( FH );
    if ( defined $logger ) {
        if ( $filePacketStatus eq 'C' ) {
            $logger->info( "Created DN work order $outWorkOrder " .
                "for $ARGV[0]" );
        } else {
            $logger->info( "Created retry work order $outWorkOrder " .
				"for $ARGV[0]" );
        }
    }
} else {
    $logger->info( "Completed work order $ARGV[0]" ) if defined $logger;
}

#
# Exit
#

exit(0);


sub mail_file
{
    my ($to, $file) = @_;
    `/usr/sbin/sendmail -t '$to' < $file`;
    if ($?) {
        return undef;
    } else {
        return 1;
    }
}
