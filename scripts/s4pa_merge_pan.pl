#!/usr/bin/perl -w

=head1 NAME

s4pa_merge_pan.pl - script for merge PANs return from the
split PDR.

=head1 SYNOPSIS

s4pa_merge_pan.pl
[B<-f> I<configuration_file>]
I<workorder>

OR

s4pa_merge_pan.pl
[B<-c>]
[B<-s>]
[B<-f> I<configuration_file>]
[B<-r> I<remote_host>]
[B<-d> I<remote_directory>]
[B<-p> I<pan_directory>]
[B<-t> I<protocol>]
[B<-e> I<retention_time>]
I<workorder>

=head1 DESCRIPTION

s4pa_merge_pan.pl takes a work order passed down from split_pdr
station and search for associated PAN for the split PDR listed in
the work order. When it found all PANs are successful, it will create
a SHORTPAN and a PUSH work order to transfer the PAN back to its
provider.

=head1 ARGUMENTS

=over

=item B<-f> I<configuration file>

File containing parameters for polling: cfg_pan_destination, cfg_pan_dir, 
cfg_protocol, cfg_retention_time.  Command line options, if specified, 
will over ride these values.

=item B<-r> I<remote_hostanme>

Hostname to ftp to in order to push. A suitable entry in the .netrc file must
exist for this host. 

=item B<-d> I<remote_directory>

Directory on the remote host (as seen in the ftp session) which will be
examined for new PDRs. 

=item B<-p> I<pan_directory>

Local directory to which new PDRs will be directed.
(=cfg_pan_dir in config file)

=item B<-t> I<protocol>

Specifies the protocol to be used for polling. Valids are FTP and SFTP.

=item B<-e> I<retention_time>

Optional argument which specifies filename pattern (default is \.PDR$)
(=cfg_pdr_pattern in config file)

=item B<-c>

Optional flag to continue to create LONGPAN and PUSH work order.
This is a part of the failure handler

=item B<-s>

Optional flag to sweep away split PDRs and PANs after a successful
merged PAN was created and PUSH work order submitted.

=back 

=head1 AUTHORS

Guang-Dih Lei, AdNET, Greenbelt, MD 20771

=cut

################################################################################
# $Id: s4pa_merge_pan.pl,v 1.3 2019/05/06 15:48:00 glei Exp $
# -@@@ S4PA, Version $Name:  $ 
###############################################################################
use strict;
use Getopt::Std;
use S4PA;
use S4P;
use S4P::PAN;
use S4P::PDR;
use S4P::TimeTools;
use File::stat;
use File::Copy;
use File::Basename;
use XML::LibXML;
use Log::Log4perl;

use vars qw( $opt_f $opt_r $opt_p $opt_d $opt_t $opt_e $opt_c $opt_s );

getopts('f:r:p:d:t:e:cs');
my $workOrder = shift( @ARGV ) or usage();

# read configuration file
if ( $opt_f ) {
    # Read the configuration file if specified
    my $cpt = new Safe 'CFG';
    $cpt->share( 'cfg_pan_destination', '$cfg_protocol', 
        '$cfg_pan_dir', '$cfg_retention_time' );
    $cpt->rdo($opt_f) or
        S4P::perish(1, "Cannot read config file $opt_f in safe mode: ($!)");
}

# Command line options over ride the definitions in configuration file
$CFG::cfg_retention_time = $opt_e if defined $opt_e;
my $retentionTime = $CFG::cfg_retention_time ? $CFG::cfg_retention_time : 7200;
my $continueFlag = $opt_c ? 1 : 0;
my $cleanupFlag = 0;

my $logger = S4PA::CreateLogger( $CFG::cfg_logger{FILE},
    $CFG::cfg_logger{LEVEL} ) if ( %CFG::cfg_logger );

# read in work order content for original and split PDRs name
my $xmlParser = XML::LibXML->new();
$xmlParser->keep_blanks(0);
my $dom = $xmlParser->parse_file( $workOrder );
S4P::perish( 1, "Failed to read work order $workOrder" ) unless $dom;
my $doc = $dom->documentElement();
S4P::perish( 1, "Failed to find document element in $workOrder" ) unless $doc;
$logger->info( "Processing work order $workOrder" ) if defined $logger;

# identify original pdr filename
my ( $originalPdrNode ) = $doc->findnodes( '//OriginalPdr' );
my $originalPdr = $originalPdrNode->getAttribute( 'NAME' );
my $pdrTimeStamp = $originalPdrNode->getAttribute( 'TIME' );
my ( $systemNode ) = $originalPdrNode->findnodes( './OriginatingSystem' );
my $originatingSystem = $systemNode->textContent();

# Determine remote host for pushing PAN
my $remote_host = ( defined $opt_r ) ? $opt_r
    : ( exists $CFG::cfg_pan_destination{$originatingSystem}->{host} )
    ? $CFG::cfg_pan_destination{$originatingSystem}->{host}
    : undef;

my $remote_dir = ( defined $opt_d ) ? $opt_d
    : ( exists $CFG::cfg_pan_destination{$originatingSystem}->{dir} )
    ? $CFG::cfg_pan_destination{$originatingSystem}->{dir}
    : undef;

my $protocol = ( defined $opt_t ) ? $opt_t
    : ( exists $CFG::cfg_protocol{$remote_host} )
    ? $CFG::cfg_protocol{$remote_host}
    : 'FTP';

my $provider_notify = ( exists $CFG::cfg_pan_destination{$originatingSystem}->{notify} )
    ? $CFG::cfg_pan_destination{$originatingSystem}->{notify}
    : undef;

my $local_pan_dir = ( defined $opt_p ) ? $opt_p
    : ( exists $CFG::cfg_pan_dir{$originatingSystem} )
    ? $CFG::cfg_pan_dir{$originatingSystem}
    : undef;
S4P::perish( 1, "Specify directory for returning PAN (-p)" )
    unless defined $local_pan_dir;
$local_pan_dir .= "/" unless ( $local_pan_dir =~ /\/$/ );

# check if work order is older than the retention period
my $currentTime = S4P::TimeTools::CCSDSa_Now();
my $pdrExpired = ( S4P::TimeTools::CCSDSa_Diff( $pdrTimeStamp, $currentTime )
     > $retentionTime ) ? 1 : 0;

my @splitPdr;
my @successPan;
my @shortPan;
my @longPan;
my @skipPan;
my %panDisposition;
my $fileDisposition = {};

# loop through each split pdr and search for associated pan
foreach my $splitPdrNode ( $doc->findnodes( '//SplitPdr' ) ) {
    my $pdrName = $splitPdrNode->getAttribute( 'NAME' );
    my $pdrPath = $splitPdrNode->getAttribute( 'PATH' );
    my $pdrFile = "$pdrPath/$pdrName";
    my $pdrText = $splitPdrNode->textContent();
    my $pdr = S4P::PDR->new( 'text' => $pdrText );

    # get each split PDR filename and host
    $pdrName =~ s/^\s+|\s+$//g;
    ( my $panFile = $pdrName ) =~ s/\.PDR$/.PAN/;
    my $panPath = $local_pan_dir . $panFile;

    # check if any split PAN was returned
    if ( -s $panPath ) {
        # found PAN, parse it.
        my $pan = S4P::PAN::read_pan( $panPath );

        # fail job if return pan is a PDRD
        if ( $pan->msg_type() =~ /PDRD/ ) {
            S4P::perish( 1, "Found a PDRD which is currently not supported" );

        } elsif ( $pan->msg_type() eq "SHORTPAN" ) {
            my $disposition = $pan->disposition();
            if ( $pan->is_successful() ) {
                # this is a short successful pan
                push @splitPdr, $pdrFile;
                push @successPan, $panPath;
            } else {
                # this is a failed SHORTPAN
                $panDisposition{$disposition}++;
                push @shortPan, $panPath;
            }

            # update all files disposition hash with short pan disposition 
            foreach my $file ( $pdr->files() ) {
                $fileDisposition->{$file} = $disposition;
            }

        } elsif ( $pan->msg_type() eq "LONGPAN" ) {
            # this is a LONGPAN
            push @longPan, $panPath;
            my @fileList = keys %{$pan->{"DISPOSITION"}};

            # update all files attribute with its own disposition from the long pan
            foreach my $file ( @fileList ) {
                $fileDisposition->{$file} = $pan->{"DISPOSITION"}->{$file};
            }
                
        } else {
            S4P::perish( 1, "Unknown PAN: $panPath" );

        }

    } elsif ( $pdrExpired ) {
        $logger->error( "Missing PAN: $panPath for expired PDR: $originalPdr" )
            if $logger;
        S4P::perish( 1, "Missing PAN: $panPath for expired PDR: $originalPdr" );

    # skip pan processing if PAN was not found and PDR has not expired yet
    } else {
        push @skipPan, $panPath;
        last;
    }
}

( my $pdrID = $originalPdr ) =~ s/\.PDR$//;
my $localPan = $local_pan_dir . $pdrID . ".PAN";
my $msgType;
my $disposition;

# Recycle job if any PAN was skipped
if ( @skipPan > 0 ) {
    my $pan = pop @skipPan;
    # create "RETRY" work order
    ( my $retryWo = $workOrder ) =~ s/^DO\.RETRY|^DO/RETRY/;
    $retryWo .= '.wo';
    copy ( $workOrder, $retryWo ) || S4P::perish( 1, "Failed to create retry wo" );
    S4P::logger( 'INFO', "Missing PAN: $pan, recycle work order" );
    $logger->info( "Missing PAN: $pan, recycle work order" ) if defined $logger;

# Otherwise, all PAN has arrived. Merge pan and create push work order
} else {
    # fail job if any split PAN is a LONGPAN
    # return a long pan when continueFlag was set from failure handler
    if ( @longPan > 0 ) {
        unless ( $continueFlag ) { 
            my $pan = pop @longPan;
            $logger->error( "Found Long PAN: $pan" ) if defined $logger;
            S4P::perish( 1, "Found Long PAN: $pan."
                . " Select <Continue> to create a LONGPAN and PUSH work order to"
                . " return PAN to provider." );
        }
        $msgType = 'LONGPAN';
    
    # fail job if there are no long pan but at least one failed short pan
    # return a long pan when continueFlag was set from failure handler
    } elsif ( @shortPan > 0 ) {
        unless ( $continueFlag ) {
            my $pan = pop @shortPan;
            $logger->error( "Found failed Short PAN: $pan" ) if defined $logger;
            S4P::perish( 1, "Found Failed Short PAN: $pan."
                . " Select <Continue> to create a PAN and PUSH work order to "
                . " return PAN to provider." );
        }
    
        # if there is at least one success short pan, 
        # then we need to convert the returning pan to long pan
        if ( @successPan > 0 ) {
            # there is at least one successful PAN, convert it to LONGPAN
            $msgType = 'LONGPAN';
        # returning a short pan only if all short pan having the same disposition
        # otherwise, returning a long pan
        } else {
            # all short pans, check each disposition
            my @dispositionList = keys %panDisposition;
            if ( scalar( @dispositionList ) == 1 ) {
                $msgType = 'SHORTPAN';
                $disposition = $dispositionList[0];
            } else {
                $msgType = 'LONGPAN';
            }
        }
    
    } elsif ( @successPan > 0 ) {
        $msgType = 'SHORTPAN';
        $disposition = '"SUCCESSFUL"';
        $cleanupFlag = $opt_s;
    }

    # create merged PAN and PUSH work order
    create_pan( $fileDisposition, $localPan, $msgType, $disposition ) or
        S4P::perish( 1, "Failed to create PAN: $localPan" );
    my $wo = "PUSH." . $pdrID . ".1.wo";
    my $destination = lc($protocol) . ":" . $remote_host .  $remote_dir;

    create_wo( $wo, 
        {   destination => $destination,
            localpath   => $localPan,
            cleanup     => "N" } ) or
        S4P::perish( 1, "Failed to create work order: $wo" );
    $logger->info( "Created PUSH work order: $wo for $workOrder" ) 
        if defined $logger;

    if ( defined $provider_notify ) {
        my $emailFile = $localPan . '.email';
        create_email_file( $emailFile, $localPan ) or
            S4P::perish( 101, "Fail to create file, $emailFile, for e-mailing");
        my $emailWo = 'PUSH.email.' .  $pdrID . ".2.wo";
        create_wo( $emailWo,
            {   destination => "mailto:$provider_notify",
                localpath   => $emailFile, 
                cleanup     => "Y" } ) or
            S4P::perish( 1, "Failed to create email work order: $emailWo" );
    }

}

# clean up only after successful pan
if ( $cleanupFlag ) {
    # removing split PDRs and PANs
    foreach my $pdr ( @splitPdr ) {
        unlink $pdr || S4P::perish( 1, "Failed to remove $pdr ($!)" );
        $logger->info( "Removed $pdr" );
    }
    foreach my $pan ( @successPan ) {
        unlink $pan || S4P::perish( 1, "Failed to remove $pan ($!)" );
        $logger->info( "Removed $pan" );
    }
}

exit( 0 );

##############################################################################
# create_pan:  create PAN
##############################################################################
sub create_pan {
    my ( $fileDisposition, $panFile, $msgType, $Disposition ) = @_;

    my $dateStamp = S4P::TimeTools::CCSDSa_Now();
    my $text = "MESSAGE_TYPE=$msgType;\n";
    if ($msgType eq "SHORTPAN") {
        $text .= "DISPOSITION=" . $Disposition .
                 ";\nTIME_STAMP=" . $dateStamp . ";\n";
    } else { # LONGPAN
        my @fileNodeList = $doc->findnodes( '//File' );
        $text .= 'NO_OF_FILES=' . scalar( keys %$fileDisposition ) . ";\n";
        foreach my $file ( keys %$fileDisposition ) {
            my $fileName = basename( $file );
            my $directory = dirname( $file );
            my $disposition = $fileDisposition->{$file};
            $text .= "FILE_DIRECTORY=$directory;\n";
            $text .= "FILE_NAME=$fileName;\n";
            $text .= "DISPOSITION=" . $disposition . ";\n";
            $text .= "TIME_STAMP=" . $dateStamp . ";\n";
        }
    }
    return ( S4P::write_file($panFile, $text) ? 1 : 0 );
}

##############################################################################
# create_wo:  create push work order
##############################################################################
sub create_wo {
    my ( $wo_file, $info ) = @_;
    my ( $pdrID, $panFile, $remote_host, $remote_dir, 
        $protocol, $provider_notify ) = @_;

    # create work order for file pusher
    my $wo_parser = XML::LibXML->new();
    my $wo_dom = $wo_parser->parse_string('<FilePacket/>');
    my $wo_doc = $wo_dom->documentElement();

    my ($filePacketNode) = $wo_doc->findnodes('/FilePacket');
    $filePacketNode->setAttribute('status', "I");

    $filePacketNode->setAttribute( 'destination', $info->{destination} );

    my $filegroupNode = XML::LibXML::Element->new( 'FileGroup' );
    my $fileNode = XML::LibXML::Element->new( 'File' );
    $fileNode->setAttribute( 'localPath', $info->{localpath} );
    $fileNode->setAttribute( 'status', "I" );
    $fileNode->setAttribute( 'cleanup', $info->{cleanup} );
    $filegroupNode->appendChild( $fileNode );
    $wo_doc->appendChild( $filegroupNode );

    open (WO, ">$wo_file") 
        || S4P::perish( 1, "Failed to open workorder file $wo_file: $!");
    print WO $wo_dom->toString(1);
    unless ( close WO ) {
        S4P::logger( "ERROR", "Failed to close $wo_file ($!)" );
        unlink $wo_file;
    }

    return( -f $wo_file ? 1 : 0 ) ;
}

##############################################################################
# create_email_file:  create email notification
##############################################################################
sub create_email_file
{
    my ( $emailFile, $panFile) = @_;

    if ( -z $panFile ) {
        warn("WARNING: File $panFile empty");
        return undef;
    }

    my $pan = `cat $panFile`;

    local( *FH );
    open( FH, ">$emailFile" ) or return undef;
    print FH "Subject: " . basename( $panFile ) . "\n\n";
    print FH $pan;
    unless ( close(FH) ) {
        S4P::logger( "ERROR", "Failed to close $emailFile ($!)" );
        unlink $emailFile;
    }
    return ( -f $emailFile ? 1 : 0 );
}

##############################################################################
# Subroutine usage:  print usage and die
##############################################################################
sub usage {
  die << "EOF";
usage: $0 -f <configuration_file> or [options]
Options are:
          -r <remote_host>    remote host for PAN
          -d <remote_dir>     remote directory for PAN
          -t <protocol>       protocol for PAN transfer
          -p <pan_dir>      directory for returning PAN
          -e <retention_time> PDR retention period (in seconds), default to 7200
          -c                  continue flag after a LONGPAN received
          -s                  Clean up split PDRs and PANs flag
EOF
}

