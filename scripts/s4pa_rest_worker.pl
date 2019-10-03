#!/usr/bin/perl -w

=head1 NAME

s4pa_rest_worker.pl - script to submit RESTful HTTP requests to a remote host.

=head1 SYNOPSIS

s4pa_rest_worker.pl
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

<RestPackets>
  <RestPacket status="I|C"
		label="userlabel"
		notify="mailto: xx@yy.zz.com"
		[destination="http[s]:xx.yy.zz.com/restful/path]
		[numAttempt="n"]
		[completed="<timestamp>"] >
    [<HTTPheader>HeaderAttribute: header value</HTTPheader>]
    [
     <Payload>
       <SomeRootElement>
         <SomeChildElement>content<SomeChildElement>
       </SomeRootElement>
     </Payload>
    ]
  </RestPacket>
</RestPackets>

=head1 DESCRIPTION

This script submits RESTful requests specified in the work order that have not
yet successfully been submitted (whose status is not yet "C") to a
destination URL.  Unsuccessful attempts cause the work order to be rescheduled.
Once fully successful, an EMAIL work order is created to send out a
notification email.

=head1 AUTHOR

E. Seiler, NASA/GSFC, Code 610.2, Greenbelt, MD  20771.

=head1 CHANGELOG

=cut


################################################################################
# $Id: s4pa_rest_worker.pl,v 1.3 2019/05/06 15:48:01 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################
use strict;
use Net::Netrc;
use Net::FTP;
use XML::LibXML;
use File::Basename;
use Getopt::Std;
use Cwd;
use Safe;
use S4P;
use S4PA;
use S4PA::Receiving;
use S4PA::Storage;
use HTTP::Request;
use LWP::UserAgent;
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

my $stationDir = dirname(cwd());

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
        if ( open( FH, "> $ARGV[0]" ) ) {
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

# Find RestPackets node and its status.  Should be just one, at the root.
my ( $packetsNode ) = $doc->findnodes('/RestPackets');
my $packetsStatus = $packetsNode->getAttribute('status');
#my $destination = $packetNode->getAttribute('destination') or
#                      S4P::perish( 1, "Specify destination");
my $verifyFlag = $packetsNode->getAttribute( 'verify' );
$verifyFlag = ( $verifyFlag && $verifyFlag eq 'yes' ) ? 1 : 0;

if ( $packetsStatus eq "C" ) {
    # Do nothing, packet complete.  (Should not occur.)
    exit(0);
}


# Loop through RestPacket nodes and process
my $packetStatus = 'C';
foreach my $packetNode ( $doc->findnodes( '/RestPackets/RestPacket' ) ) {
    # Check status attribute of the file node
    my $status = $packetNode->getAttribute('status') || 'I';
    # Done with this node if marked complete.
    next if ( $status eq "C" );

    # Get other attributes of the file node: localPath, filter, and cleanup.
    # Url attribute is just used for the notification;
    # localPath is used to push the data.
    my $destination = $packetNode->getAttribute('destination') or
        S4P::perish( 1, "RestPacket has no destination");
    my ($protocol, $dest_string) = split(':', $destination, 2);
    S4P::perish(1, "Invalid destination '$destination'")
        unless $protocol and $dest_string;

    my $headers;
    my (@headerNodes) = $packetNode->findnodes( 'HTTPheader' );
    if (@headerNodes) {
        $headers = HTTP::Headers->new;
        foreach my $headerNode (@headerNodes) {
            my ($field, $value) = split(':', $headerNode->textContent);
            $headers->header($field, $value) if ( (defined $field) && (defined $value) );
        }
    }
    my ($payloadNode) = $packetNode->findnodes( 'Payload' );
    my $payload;
    my $cleanup;
    my $localPath;
    my $cksumtype;
    my $cksumvalue;
    if ($payloadNode) {
        $localPath = $payloadNode->getAttribute('localPath');
        if ($localPath) {
            $cksumtype = $payloadNode->getAttribute('cksumtype');
            $cksumvalue = $payloadNode->getAttribute('cksumvalue');
            if ( defined $cksumtype && defined $cksumvalue ) {
                # Perform cksum verification
                my $cksum_recompute = S4PA::Storage::ComputeCRC($localPath, $cksumtype);
                S4P::perish(1, "Verify cksum failed ($cksum_recompute $cksumvalue)")
                      if $cksumvalue ne $cksum_recompute;
            }
            my $dom;
            eval { $dom = $xmlParser->parse_file($localPath); };
            S4P::perish( 1, "Could not parse file '$localPath':  $@\n" ) if $@;
            my $doc = $dom->documentElement();
            $payload = $doc->toString(1);
            $cleanup = $payloadNode->getAttribute('cleanup');
            $cleanup = 'N' unless defined $cleanup;
        } else {
            $payload = $payloadNode->firstChild->toString(1);
        }
    }

    my $request = HTTP::Request->new( $jobType, $destination, $headers, $payload);
    my $ua = LWP::UserAgent->new;
    my $response = $ua->request($request);
    if ($response->is_success) {
        $status = 'C';
        $logger->info( "Successful $jobType request to $destination" ) if defined $logger;
    } else {
        my $xml = $response->content;
        my $dom;
        eval {$dom = $xmlParser->parse_string($xml); };
        if ($@) {
            S4P::logger( "ERROR", "Could not parse response from request $destination: $@\n" );
            $logger->error( "Could not parse response from request $destination: $@\n" ) if defined $logger;
        } else {
            $status = 'I';
            my $doc = $dom->documentElement();
            my $errMsg = $doc->toString(1);
            S4P::logger( "ERROR",
                         "Failed $jobType request to $destination\n$errMsg");
            $logger->error( "Failed $jobType request to $destination\n$errMsg" ) if defined $logger;

            # if the entry 'is already deleted' or 'does not exist'
            # we don't want the job to fail but only raise an anomaly.
            # so, we will reset the status to 'C'
            if ( $jobType eq 'DELETE' && $errMsg =~ /is already deleted|does not exist/i ) {
                S4P::raise_anomaly( "Deletion", $stationDir, 'WARN',
                    "$errMsg", 0 );
                $status = 'C';
            }
        }
    }

    if ( $status eq 'C' ) {
        # Modify attributes of this node to note time of completion.
        my $tm_stamp = `date "+%Y-%m-%d %H:%M:%S"`;
        chomp $tm_stamp;
        $packetNode->setAttribute("status" => "C");
        $packetNode->setAttribute("completed" => $tm_stamp);

        # Clean up local file if asked to do so
        unlink $localPath if (defined $localPath) && ($cleanup eq "Y");
    } else {
        # Anything not complete makes the packet incomplete.
        $packetStatus = 'I';
    }

}  # END OF foreach(node)

#
# Update status of the PacketNode and write out the new work order.
# Write EMAIL work order if we're complete (and add time stamp) to
# send out notification email, or same type work order as before
# if we need to retry.
#
my $numAttempt = $doc->getAttribute( 'numAttempt' ) || 0;
$doc->setAttribute( "status" => $packetStatus );
$doc->setAttribute( "numAttempt" => $numAttempt+1 );
if ( $packetStatus eq 'C' ) {
    my $tm_stamp = `date "+%Y-%m-%d %H:%M:%S"`;
    chomp $tm_stamp;
    $doc->setAttribute( "completed" => $tm_stamp );
}

my $outWorkOrder;
#if ( $opt_n ) {
#    # Generate a work order for DN creation if transfers are complete.
#    $outWorkOrder = ( $packetStatus eq 'C' )
#	? "EMAIL.$jobId.wo" : "$jobType.$jobId.wo";
#} else {
    # Generate a work order for REST transfer if all packets haven't been
    # transferred.
    $outWorkOrder = ( $packetStatus eq 'C' )
	? undef : "$jobType.$jobId.wo";
#}

if ( defined $outWorkOrder ) {
    local( *FH );
    open( FH, ">$outWorkOrder" )
	|| S4P::perish( 1, "Failed to open $outWorkOrder ($!)" );
    print FH $dom->toString(1);
    close( FH );
    if ( defined $logger ) {
        if ( $packetStatus eq 'C' ) {
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
