#!/usr/bin/perl

=head1 NAME

s4pa_create_DN.pl - script to create DN

=head1 SYNOPSIS

s4pa_create_DN.pl [B<-f> I<configuration_file>] <DNPreambleDir> <DNxmlFileName>

=head1 ARGUMENTS

<DNPreambleDir> is directory containing files with text to be prefixed 
to outgoing notification messages. File names start with the protocol used to 
push the files (ftp, sftp, mailto, or null) followed by "Success.txt".

=over 4

=item I<-f configuration file>

Post office configuration file

=back

=head1 DESCRIPTION

This script parses the XML file created by the work order and uses it
to create a DN.

Input file format:

<FilePacket status="I|C"
            label="userlabel"
            notify="mailto: xx@yy.zz.com"
            messageFormat="S4PA|LEGACY|PDR"
            [destination="[s|bb]ftp:yy.zz.com/dir|file:yy.zz.com/dir|mailto:xx2@aa.bb.edu"]
            [numAttempt="n"]
            [completed="<timestamp>"]
            [max_granule_count="n"] >
    <FileGroup>
        <File status="I|C"
                url="http|ftp://xxdisc.gsfc.nasa.gov/data/yyyy/zzzz/2006/365/filename"
                localPath="/ftp/.provider/nnn/zzzz/filename"
		[filter="filter-program" [filteredFilepath="/some/place/filename2" filteredSize="nnn"]]
                [cleanup="N"]
                [completed="<timestamp>"] />
    </FileGroup>
</FilePacket>


=head1 AUTHOR

S Kreisler, SSAI, NASA/GSFC, Code 610.2, Greenbelt, MD  20771.

=head1 CHANGELOG

02/15/06 Initial version
05/03/06 Added capability to produce PDR-formatted DN.
06/19/06 Modified email subject line, added order id, URL added to ftp pull legacy DN.
07/28/06 Bugzilla fix 16: remove UR, starting/ending date, add expiration date
08/01/06 Bugzilla fix 29: for pushed data, report target host/dir in PDRs
08/14/06 Add checksums to legacy DNs.
08/30/06 Checksum dropped on metadata files.
12/13/06 Bugzilla 424: add segmenting on max_granule_count
12/15/06 Checksum dropped on browse files. (we will probably not have browse file in DN at all).
08/20/07 Added master log logging.

=cut

################################################################################
# s4pa_create_DN.pl,v 1.31 2006/10/17 15:39:55 hegde Exp
# -@@@ S4PA, Version $Name:  $
################################################################################

use XML::LibXML;
use File::Basename;
use S4P;
use S4P::PDR;
use S4P::FileGroup;
use S4PA::Receiving;
use Getopt::Std;
use S4PA;
use Log::Log4perl;
use strict;
use vars qw( $opt_f );

MAIN: {
    getopts('f:');
    $#ARGV == 1 or die "Usage: $0 <DNPreambleDir> <DNxmlFileName> \n";

    my $filename = $ARGV[1];
    my $preamDir = $ARGV[0];

    # Read configuration file
    if ( -f $opt_f ) {
        my $cpt = new Safe 'CFG';
        $cpt->rdo($opt_f) or
            S4P::perish(2, "Cannot read config file $opt_f in safe mode: ($!)");
        undef $cpt;
    }

    my $logger = S4PA::CreateLogger( $CFG::cfg_logger{FILE},
        $CFG::cfg_logger{LEVEL} ) if ( %CFG::cfg_logger );

    my $dummyName = basename( $filename );
    my ( $jobType, $jobId ) = ( $2, $3 ) if ( $dummyName =~ /^(DO\.)?([^.]+)\.(.+)(\.wo)?$/ );
    $logger->debug( "Processing $filename" ) if defined $logger;

    my $xmlParser = XML::LibXML->new();
    $xmlParser->keep_blanks( 0 );
    my $dom = $xmlParser->parse_file( $filename );

    my ($filePacket, $fileGroups, $notify, $status, $pullList)
        = getDNComponents($xmlParser, $dom);

    # Check for no-file condition
    my $fileCount=0;
    if ( defined $fileGroups ) {
        foreach my $granFile ( @$fileGroups ) {
            next unless ( defined $granFile->{FILES} && @{$granFile->{FILES}} );
            $fileCount++;
        }
    }
    S4P::perish( 0, "DN not created as files are not found" )
        unless ( (@$pullList > 0) || $fileCount);
   
    # Once upon a time $status mattered and DN was sent only if file transfer
    # was complete.  Now $status is ignored because DN is used to notify
    # subscribers even if files are not pushed to them and they will use ftp pull.

    # How many groups do we have, how many DNs do we need?
    my $totgroups = ( $filePacket->{MESSAGEFMT} eq 'S4PA' ) ?
        scalar(@$pullList) : scalar(@$fileGroups);
    my $maxgroups = $filePacket->{MAXGRNCNT};
    my $totDNs = int( (($totgroups - 1) / $maxgroups) + 1);
    my $lastsize = (($totgroups - 1) % $maxgroups) + 1;
    # Assume all will get succeed.

    # Something to hold the text of one DN.
    my $dn;
    # Generate a unique number based on time and pid.
    my $orderid = 'DN' . time() . '-' . $$;
    my $workOrder;

    # for user-defined DN format
    if ( $filePacket->{MESSAGEFMT} eq 'USER-DEFINED') {
        my $script = $filePacket->{MESSAGEFLT};
        if ( $totDNs == 1 ) {
            $dn = `$script $filename`;
            if ( $dn ) {
                $workOrder = createWO($dn, $filePacket->{MESSAGEFMT}, $notify, 
                    $orderid, $filePacket->{NOTICESUFFIX}, $filePacket->{NOTICESUBJECT} );
                $logger->info( "Created $workOrder for $filename" ) if defined $logger;
            } else {
                $logger->info( "No DN created for $filename" ) if defined $logger;
            }
        } else {
            my @splitedWOs = splitWO( $filename, $maxgroups, $totDNs );
            # make idpost to be an integer
            my ($idpre, $idpost) = ($orderid . '.Seg', "of$totDNs");
            for (my $segno = 1; $segno <= $totDNs; $segno++) {
                my $DNid = $idpre . $segno . $idpost;
                my $wo = $splitedWOs[$segno-1];
                $dn = `$script $wo`;
                if ( $dn ) {
                    $workOrder = createWO($dn, $filePacket->{MESSAGEFMT}, $notify, $DNid,
                        $filePacket->{NOTICESUFFIX});
                    $logger->info( "Created $workOrder for $filename" ) if defined $logger;
                } else {
                    $logger->info( "No DN created for $filename" ) if defined $logger;
                }
                unlink $wo;
            }
        }

    # for traditional DN format (S4PA, LEGACY, PDR)
    } else {
        if ($totDNs == 1) { # Only one DN to send
            if ($filePacket->{MESSAGEFMT} eq 'LEGACY') {
                $dn = getDn($filePacket, $fileGroups, $preamDir, $orderid);
            } elsif ($filePacket->{MESSAGEFMT} eq 'S4PA') {
                $dn = getDNList($pullList);
            } elsif ($filePacket->{MESSAGEFMT} eq 'PDR') {
                $dn = getPDR($filePacket, $fileGroups);
            }
            $workOrder = createWO($dn, $filePacket->{MESSAGEFMT}, $notify, 
                $orderid, $filePacket->{NOTICESUFFIX}, $filePacket->{NOTICESUBJECT} );
            $logger->info( "Created $workOrder for $filename" ) if defined $logger;

        } else { # Segment into DNs of $maxgroups
            # make idpost to be an integer
            my ($idpre, $idpost) = ($orderid . '.Seg', "of$totDNs");
            for (my $segno = 1; $segno <= $totDNs; $segno++) {
                my $DNid = $idpre . $segno . $idpost;
                my $grouplist = [];
                my $thissize = $segno == $totDNs ? $lastsize : $maxgroups;
                # Get the right number of groups (URLs or FileGroups) into 
                # $grouplist.
                for (my $i = 1; $i <= $thissize; $i++) {
                    push @$grouplist, ($filePacket->{MESSAGEFMT} eq 'S4PA' ?
                         shift @$pullList : shift @$fileGroups);
                }
                if ($filePacket->{MESSAGEFMT} eq 'LEGACY') {
                    $dn = getDn($filePacket, $grouplist, $preamDir, $DNid);
                } elsif ($filePacket->{MESSAGEFMT} eq 'S4PA') {
                    $dn = getDNList($grouplist);
                } elsif ($filePacket->{MESSAGEFMT} eq 'PDR') {
                    $dn = getPDR($filePacket, $grouplist);
                }
                # Still sucessful until something fails.
                $workOrder = createWO($dn, $filePacket->{MESSAGEFMT}, $notify, $DNid,
                    $filePacket->{NOTICESUFFIX});
                $logger->info( "Created $workOrder for $filename" ) if defined $logger;
            }
        }
    }

    # Write the work order for tracking
    if ( $CFG::cfg_publish_dotchart && $filePacket->{TRACK} eq 'yes' ) {
        # We write the tracking wo even if the DN failed; we'll write another 
        # when we retry.
        my $wo = "TRACK.$jobId.wo";
        local(*FH);
        open( FH, ">$wo" )
            || S4P::perish( 1, "Failed to create $wo for writing ($!)" );
        print FH $dom->toString(1);
        close( FH );
        $logger->info( "Created $wo for $filename" ) if defined $logger;
    }
}

sub createWO {
    # create work order for postoffice
    my ( $dn, $format, $notify, $orderid, $DN_extension, $DN_subject ) = @_;
    # Pull protocol from first (only) key in hash referenced by $notify.
    my ( $protocol ) = ( keys %$notify );
    my $destination = $protocol . ":" . $notify->{$protocol};

    my $dnFile;
    if ( -f $dn ) {
        $dnFile = $dn;
    } else {
        if ( $protocol eq 'mailto') {
            my $subject = "GES DISC Order Notification Order ID: $orderid";
            if (defined $DN_subject) {
                $subject = $DN_subject . ": $orderid";
            }
            # add subject on the first line of the email.
            $dn = "To: $notify->{$protocol}\nSubject: $subject\n\n" . $dn;
            $dnFile = "/var/tmp/$orderid" . ".email";
            
        } elsif (($protocol eq 'ftp') || ($protocol eq 'sftp') ||
            ($protocol eq 'file') || ($protocol eq 'bbftp')) {
            # Specify DN file extension based on protocol???
            if ( not defined $DN_extension ) {
                if ( $protocol eq 'sftp' && $format eq 'LEGACY' ) {
                    $DN_extension = '.notifysftp';
                } else {
                    $DN_extension = '.notify';
                }
                $DN_extension = '.PDR' if ( $format eq 'PDR' );
            } elsif ( $DN_extension ne '' ) {
                $DN_extension = ".$DN_extension";
            }
    
            # Build file in /var/tmp.
            $dnFile = "/var/tmp/$orderid" . $DN_extension;
        }
    
        $dnFile =~ s/\s|:/_/g;
        local(*FH);
        open( FH, ">$dnFile" )
            || S4P::perish( 1, "Failed to open $dnFile for writing ($!)" );
        print FH $dn;
        close( FH ) || S4P::perish( 1, "Failed to close $dnFile ($!)" );
    }
    
    my $xmlParser = XML::LibXML->new();
    my $dom = $xmlParser->parse_string('<FilePacket/>');
    my $doc = $dom->documentElement();

    my ($filePacketNode) = $doc->findnodes('/FilePacket');
    $filePacketNode->setAttribute('status', "I");
    $filePacketNode->setAttribute('destination', $destination);
    my $filegroupNode = XML::LibXML::Element->new('FileGroup');
    my $fileNode = XML::LibXML::Element->new('File');
    $fileNode->setAttribute('localPath', $dnFile);
    $fileNode->setAttribute('status', "I");
    $fileNode->setAttribute('cleanup', "Y");
    $filegroupNode->appendChild($fileNode);
    $doc->appendChild($filegroupNode);

    my $wo_file = 'PUSH.' . $orderid . '.wo';
    open (WO, ">$wo_file")
        || S4P::perish(2, "Failed to open workorder file $wo_file: $!");
    print WO $dom->toString(1);
    close ( WO ) || S4P::perish( 2, "Failed to close $wo_file ($!)" );
    return $wo_file;
}

sub getDNList {
    my ($pullList) = @_;
    # Join list referenced by each list referenced by $pullList with \n's and 
    # return.
    my $dn;

    foreach my $group (@$pullList) {
        $dn .= (join "\n", @$group) . "\n";
    }

    return $dn;
}

sub getDn {
    my ($filePacket, $fileGroups, $preamDir, $orderid) = @_;

    # make sure host and directory will not be over-written to be 'MULTIPLE'
    # replace MEDIATYPE from Push(HTTP|Ftp)Pull to (HTTP|Ftp)Pull after
    # ftppull was evaluated.
    my $mediaType = $filePacket->{MEDIATYPE};
    my $ftppull = ( $mediaType eq 'FtpPull' );
    $mediaType =~ s/Push(.+)Pull/$1Pull/;
    
    # Prefix DN from preamble in $preamDir's xxxSuccess.txt, where xxx is push 
    # protocol.
    my $preamFile = $preamDir . '/'. $mediaType . 'Success.txt';

    my $dn = ( -f $preamFile ) ? `/bin/cat $preamFile` : '';
    my $subHeadFile = $preamDir . '/' . $filePacket->{ID}
        . '.header';
    $dn .= ( -f $subHeadFile ) ? getHeader( $subHeadFile ) : '';
    $dn .= "\n\n\n++++++++++\n\n";

    # Format DN info.
    $dn .= "ORDERID: $orderid\n";
    $dn .= "REQUESTID: $orderid\n";
    $dn .= "USERSTRING: $filePacket->{USERSTRING}\n";
    $dn .= "FINISHED: $filePacket->{FINISHED}\n\n";
    $dn .= "MEDIATYPE: $mediaType\n";
    # Grab initial take on ftp host (push host, email address, or NONE for ftp 
    # pull) and directory. Eventually we'll report URL host as FTP host for 
    # ftp pull (or 'MULTIPLE' if more than one).
    # Same will be done for the directory path.
    my ($host, $dir) = ($filePacket->{FTPHOST}, $filePacket->{FTPDIR});
    $dn .= "FTPHOST: ~~DROP HOST HERE~~\n";
    $dn .= "FTPDIR: ~~DROP DIR HERE~~\n";
    $dn .= "MEDIA 1 of 1\n";
    $dn .= "MEDIAID:\n\n";

    foreach my $grans (@$fileGroups) {
        next unless ( @{$grans->{FILES}} );
	# Convoluted logic to derive ECS like version IDs; 5 maps to 005, 5.1 maps to 051, 5.1.1 maps to 051
	my ( $shortName, $version ) = split( /\./, $grans->{ESDT}, 2 );
	if ( $version =~ /\./ ) {
	    $version =~ s/\.//g;
	    $version = sprintf( "%3.3d", substr($version,0,2) );
	} elsif ( $version =~ /^\d+$/ ) {
	    $version = sprintf( "%3.3d", $version );
	}
	$grans->{ESDT} = $shortName . '.' . $version;
        $dn .= "\tGRANULE: $grans->{URLPROTO}://$grans->{URLHOST}$grans->{URLPATH}/$grans->{GRANID}\n";
        $dn .= "\tESDT: $grans->{ESDT}\n\n";
        # For ftp pull, grab source host and path.
        if ($ftppull) {
            if ($host eq 'NONE') {
                $host = $grans->{URLHOST};
            } else {
                $host = 'MULTIPLE' if $host ne $grans->{URLHOST};
            }
            if ($dir eq 'NONE') {
                $dir = $grans->{URLPATH};
            } else {
                $dir = 'MULTIPLE' if $host ne $grans->{URLPATH};
            }
        }

        my ($crctype, $crcvalue) = ($grans->{CRCTYPE}, $grans->{CRCVALUE});
        foreach my $gran ($grans->{FILES}) {
            foreach my $file (@$gran) {
                my $fn = $file->{FILENAME};
                $dn .= "\t\tFILENAME: $fn\n";
                $dn .= "\t\tFILESIZE: $file->{FILESIZE}\n";
                # Pull the values from the metadata file.
                my ($t, $v) = ($crctype->{$fn}, $crcvalue->{$fn});
                ($t, $v) = ('CKSUM', $file->{FILTEREDCRC}) if $file->{FILTEREDCRC} ne '';
                unless ( $file->{FILETYPE} =~ /(METADATA|BROWSE|HDF4MAP)/ ) {
                    $dn .= "\t\tFILECKSUMTYPE: $t\n\t\tFILECKSUMVALUE: $v\n";
                }
		$dn .= "\n";
            }
        }
    }
    $dn =~ s/~~DROP HOST HERE~~/$host/s;
    $dn =~ s/~~DROP DIR HERE~~/$dir/s;

    return $dn;
}

sub getPDR {
    my ($filePacket, $fileGroups) = @_;
    # Return a string containing a DN formatted as a PDR.  $filePacket and
    # fileGroups are as described in getDNComponents.

    my $nodename;
    my $pdr = S4P::PDR->new();

    # Bugzilla fix 29: For pushed data, report target host/dir.
    # Grab initial take on nodename (push host, email address, or NONE for ftp pull) and directory.
    # Eventually we'll report nodename as FTP host and dir for ftp pull.
    my ($host, $dir) = ($filePacket->{FTPHOST}, $filePacket->{FTPDIR});
    my $ftppull = ($filePacket->{MEDIATYPE} eq 'FtpPull');

    foreach my $grans (@$fileGroups) {
        next unless ( @{$grans->{FILES}} );
        ($nodename) = $grans->{URLHOST} =~ /^([^.]+)/;
        my $fg = new S4P::FileGroup();
        my ($type, $version) = split( /\./, $grans->{ESDT}, 2 );
        $fg->data_type($type);
        if ( S4PA::IsNumber( $version ) ) {
            $fg->data_version($version);
        } else {
            $fg->data_version($version, "%s" );
        }
        $fg->node_name($ftppull ? $grans->{URLHOST} : $host);

# Bugzilla fix 16 deletes UR, and start and end time.
#       $fg->ur("$grans->{URLPROTO}://$grans->{URLHOST}$grans->{URLPATH}/$grans->{GRANID}");
#       $fg->data_start($grans->{STARTS});
#       $fg->data_end($grans->{ENDS});
        my @allspecs;
        foreach my $gran ($grans->{FILES}) {
            foreach my $file (@$gran) {
                my $fs = new S4P::FileSpec();
                my $path = $ftppull ? $grans->{URLPATH} : $dir;
                $fs->pathname("$path/$file->{FILENAME}");
                $fs->file_type($file->{FILETYPE});
                $fs->file_size($file->{FILESIZE});
                push @allspecs, $fs;
            }
        }
        $fg->file_specs(\@allspecs);
        $pdr->add_file_group($fg);
    }
    $pdr->originating_system("S4PA_$nodename");
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=localtime();
# Bugzilla fix 16.  Give an expiration date roughly a year from now.
    $pdr->expiration_time(sprintf '%4d-%02d-01T00:00:00Z', ($year + 1901), ($mon + 1));
    return $pdr->sprint();
}

sub getDNComponents {
    my ($xmlParser, $dom_dnfile) = @_;

    my $doc_dnfile = $dom_dnfile->documentElement();
    # $filePacket will be reference to hash mapping ftphost and ftpdir (that files
    # were pushed to), mediatype (protocol by which files are transferred),
    # finished (time stamp when push completed or current time), userstring
    # (from subscription configuration), and messagefmt (S4PA, LEGACY, or PDR).
    my $filePacket = {};
    # Presume $status is "packet transfer completed."
    my $status = 0;
    # $fileGroups is reference to list of hash references mapping ESDT, range
    # start and end time stamps and list of hashes mapping filenames and
    # sizes (LEGACY/PDR formats only).
    my $fileGroups = [];
    # $pullFiles is reference to list of granule groups, each of which is
    # a reference to a list of URLs available for that granule (S4PA format only).
    my $pullFiles = [];
    # Map protocols to LEGACY mediaType values; use "ftppull" for null push protocol.
    my %mediaTypeLUT = (
            ftp => 'FtpPush',
            sftp => 'sftp',
            ftppull => 'FtpPull',
            mailto => 'Email',
            file => 'File',
            bbftp => 'bbftp'
    );

    # Point to root node.
    my ($node_fp) = $doc_dnfile->findnodes( '//FilePacket');

    # $status 1 if every file not transferred (should only occur if no destination
    #   for files, i.e., just email notice for ftp pull).
    $status = 1 if $node_fp->getAttribute( 'status' ) ne 'C';

    # $finished is date-time stamp of last push, or current time for pulls.  Format
    # the time like ECS did.
    my $finished = $node_fp->getAttribute( 'completed' );
    unless ( defined $finished ) {
        $finished = `date "+%Y-%m-%d %H:%M:%S"`;
        chomp $finished;
    }
    $finished = formatDateTime($finished);
    my $messageFmt = $node_fp->getAttribute( 'messageFormat' );
    my $messageFlt = $node_fp->getAttribute( 'messageFilter' );
    my $noticeSuffix = $node_fp->getAttribute( 'noticeSuffix' );
    my $noticeSubject = $node_fp->getAttribute( 'noticeSubject' );
    my $userString = $node_fp->getAttribute( 'label' );
    my $trackFlag = $node_fp->getAttribute( 'track' );
    $trackFlag = 'no' unless defined $trackFlag;
    # Maximum number of file groups before segmenting; usually 1 or unlimited.
    my $max_granule_count = $node_fp->getAttribute( 'max_granule_count' ) || 1000000000;
    # Get the subscription ID
    my $id = $node_fp->getAttribute( 'id' );
    
    # Set values, then replace nulls of ftp pull with
    my $dest = $node_fp->getAttribute( 'destination' );
    # $mediaType is [s]ftp, mailto, or null.
    my ($mediaType, $hostdir) = split /:/, $dest;
    my $host = substr($hostdir,0,index($hostdir,'/')) ;
    my $dir = substr($hostdir,index($hostdir,'/'));
    # If destination is "mailto:..." this will be a bit strange, but it's not likely
    # that a subscription really will be fulfilled by pushing files by email.
    ($host, $dir) = ($hostdir, 'NONE') if ($mediaType eq 'mailto');
    # Replace the nulls of ftp pull with these values.
    $mediaType ||= 'ftppull';
    $host ||= 'NONE';
    $dir ||= 'NONE';

    my $address = $node_fp->getAttribute( 'notify' );
    my ($protocol,$destination) = split( /:/, $address, 2 );
    # Protocol of notification can also be [s]ftp or mailto.
    my $notify = { $protocol => $destination,  };

    $filePacket->{FTPHOST} = $host;
    $filePacket->{MEDIATYPE} = $mediaTypeLUT{$mediaType};
    # Watch to see if any URL is http:; if so we'll change FtpPull to HTTPPull.
    my $anyrestricted = 0;
    $filePacket->{FTPDIR} = $dir;
    $filePacket->{FINISHED} = $finished if $finished ne '';
    $filePacket->{USERSTRING} = $userString;
    $filePacket->{MESSAGEFMT} = $messageFmt;
    $filePacket->{MESSAGEFLT} = $messageFlt;
    $filePacket->{NOTICESUFFIX} = $noticeSuffix;
    $filePacket->{NOTICESUBJECT} = $noticeSubject;
    $filePacket->{TRACK} = $trackFlag;
    $filePacket->{MAXGRNCNT} = $max_granule_count;
    $filePacket->{ID} = $id;

    if ($messageFmt eq 'S4PA') {
        # Conventional S4PA notification (of files available or pushed).
        # Put list of URLs available in array referenced by $pullFiles.
        foreach my $node_fg ( $doc_dnfile->findnodes( '//FileGroup') ) {
            # Build URL list for this granule.
            my $granURLs = [];

            if ( defined $node_fp->getAttribute( 'pullPath' ) ) {
                foreach my $node_file ( $node_fg->findnodes( './File') ) {
                    # use pullPath to replace url if this is a
                    # push to pull conversion
                    my $localPath = $node_file->getAttribute( 'filteredFilepath' ) ?
                        $node_file->getAttribute( 'filteredFilepath' ) : 
                        $node_file->getAttribute( 'localPath' );
                    # Skip in the case of filters producing no output
                    next if  ( $localPath eq '' );
                    my $localFile = basename( $localPath );
                    push @$granURLs, $node_fp->getAttribute( 'pullPath' ) 
                        . "/$localFile";
                }
            } else {
                foreach my $node_file ( $node_fg->findnodes( './File') ) {
                    my $localFile = $node_file->getAttribute( 'url' );
                    push @$granURLs, $localFile;
                    $anyrestricted ||= ($localFile =~ /^http/);
                }
            }
            # Push refernece to that list onto total list.
            push @$pullFiles, $granURLs;
        }
    }

    else {
        # ECS-style DN of files pushed (or available for ftp pull).
        # Also handles PDR-format data notifications.
        # Put file names, sizes, and ESDTs into $fileGroups structure.
        foreach my $node_fg ( $doc_dnfile->findnodes( '//FileGroup') ) {
            # $grans is reference to hash mapping the variables following.
            my $grans = {};
            # $gran is reference to list of hash references mapping the
            # file name, size, and type, and, if filtered, the crc.
            my $gran = [];
            # $esdt is shortname.version from last xml metadata file.
            my $esdt;
            # Range time stamps and granule id from last xml file.
            my ($startstamp, $endstamp, $granuleid);
            # URL host and path grabbed from the first file.
            my ($urlproto, $urlhost, $urlpath) = ('N/A', 'N/A', 'N/A');
            # CRCs type and value for each file of granule, hashed from filename.
            my ($crctype, $crcvalue) = ({}, {});

            foreach my $node_file ( $node_fg->findnodes( './File') ) {
                # $file is reference to hash mapping file name and size.
                my $file = {};

                my $localFile = $node_file->getAttribute( 'localPath' );
                $anyrestricted ||= ($localFile =~ /^http/);

                my ($filename) = reverse split /\//, $localFile;
                my $filesize = -s $localFile;
                my $filetype = ( $filename =~ /\.xml$/ ) ? 'METADATA' : 
                               ( $filename =~ /\.(jpg|jpeg)$/i ) ? 'BROWSE' :
                               ( $filename =~ /\.map\.gz$/ ) ? "HDF4MAP" : 'SCIENCE';
                my $filteredcrc;
                my $filteredPath = $node_file->getAttribute('filteredFilepath');
                # Skip if the filtere path is empty; case of filters producing
                # no output.
                if ( defined $filteredPath  ) {
                    next if ( $filteredPath eq '' );
                    # We will ignore the true path of the filtered file--it's been
                    # deleted and can't be pulled anyway.
                    ($filename) = reverse split /\//, $filteredPath;
                    $filesize = $node_file->getAttribute('filteredSize');
                    $filteredcrc = $node_file->getAttribute('filteredCksumvalue');
#                    ($filteredcrc) = split( /\s+/, `cksum $filteredPath`);
                } else {
                    # If we don't have it yet, extract the URL host and path from
                    # first unfiltered file.  If all are filtered, there is nothing
                    # that can be transferred, so this will stay 'N/A'.
                    if ($urlhost eq 'N/A') {
                        my $urlFile = $node_file->getAttribute( 'url' );
                        ($urlproto, $urlhost, $urlpath) = ($urlFile =~ m#^(\w+)://([^/]+)(.+)/[^/]+$#);
                    }
                }
                if ($localFile =~ /.xml$/) {
                    # Parse every metadata file (should be only one).
                    my $dom_gran = $xmlParser->parse_file( $localFile );
                    my $doc_gran = $dom_gran->documentElement();

                    my ($node_gran) = $doc_gran->findnodes( '//CollectionMetaData/ShortName');
                    my $shortName = $node_gran->textContent;
                    ($node_gran) = $doc_gran->findnodes( '//CollectionMetaData/VersionID');
                    my $ver = $node_gran->textContent;
                    # Save the last ESDT.
                    $esdt = sprintf "%s.%s", $shortName, $ver;
                    # Save the time stamps.
                    ($node_gran) = $doc_gran->findnodes( '//RangeBeginningDate');
                    $startstamp = $node_gran->textContent;
                    ($node_gran) = $doc_gran->findnodes( '//RangeBeginningTime');
                    $startstamp .= 'T' . $node_gran->textContent;
                    $startstamp .= 'Z' if $startstamp !~ /Z$/;
                    ($node_gran) = $doc_gran->findnodes( '//RangeEndingDate');
                    $endstamp = $node_gran->textContent;
                    ($node_gran) = $doc_gran->findnodes( '//RangeEndingTime');
                    $endstamp .= 'T' . $node_gran->textContent;
                    $endstamp .= 'Z' if $endstamp !~ /Z$/;
                    ($node_gran) = $doc_gran->findnodes( '//GranuleID');
                    $granuleid = $node_gran->textContent;
                    # Find all the science file CRCs.
                    my (@fn, @cst, @csv);  # List of files, checksumtypes, values.
                    my @granulits = $doc_gran->findnodes('//Granulits');
                    if (scalar(@granulits) > 0) { # Multifile Granule.
                        @fn = textvals($doc_gran, '//FileName');
                        @cst = textvals($doc_gran, '//Granulits//CheckSumType');
                        @csv = textvals($doc_gran, '//Granulits//CheckSumValue');
                    } else { # Single file, each list just one.
                        @fn = ($granuleid);
                        @cst = textvals($doc_gran, '//CheckSumType');
                        @csv = textvals($doc_gran, '//CheckSumValue');
                    }
                    foreach my $f (@fn) { # For each file (usually just 1):
                        my $t = shift @cst;
                        $crctype->{$f} = ($t eq 'CRC32' ? 'CKSUM' : $t);
                        $crcvalue->{$f} = shift @csv;
                    }
                }

                $status = 1 if $node_file->getAttribute( 'status' ) ne 'C';

                $file->{FILENAME} = $filename;
                $file->{FILESIZE} = $filesize;
                $file->{FILETYPE} = $filetype;
                $file->{FILTEREDCRC} = $filteredcrc;

                push @$gran, $file;
            }

            $grans->{ESDT} = $esdt;
            $grans->{URLHOST} = $urlhost;
            $grans->{URLPATH} = $urlpath;
            $grans->{URLPROTO} = $urlproto;
            $grans->{STARTS} = $startstamp;
            $grans->{ENDS} = $endstamp;
            $grans->{GRANID} = $granuleid;
            $grans->{FILES} = $gran;
            $grans->{CRCTYPE} = $crctype;
            $grans->{CRCVALUE} = $crcvalue;

            push @$fileGroups, $grans;
        }
    }
    # Change MEDIATYPE from FtpPull to HTTPPull if any restricted found.
    $filePacket->{MEDIATYPE} = 'HTTPPull' if 
    		$anyrestricted and $mediaType eq 'ftppull';

    # converting a push DN to a pull DN if 'pullPath' exists as an
    # attribute in <FilePacket>, the new MEDIATYPE will be either
    # PushFtpPull or PushHTTPPull depends on the pullPath's protocol
    if ( defined $node_fp->getAttribute( 'pullPath' ) ) {
        my ( $protocol, $pullPath ) = split '://', $node_fp->getAttribute( 'pullPath' );
        $mediaType = ( $protocol eq 'http' ) ? 'PushHTTPPull' : 'PushFtpPull';
        $host = substr($pullPath,0,index($pullPath,'/')) ;
        $dir = substr($pullPath,index($pullPath,'/'));

        $filePacket->{MEDIATYPE} = $mediaType;
        $filePacket->{FTPHOST} = $host;
        $filePacket->{FTPDIR} = $dir;
    }

    return ($filePacket, $fileGroups, $notify, $status, $pullFiles);
}

sub textvals {
    # Return the textContent of each node in the Xpath $xp of doc $d.
    my ($d, $xp) = @_;
    my @nodes = $d->findnodes($xp);
    my @vals;
    foreach my $n (@nodes) {
        push @vals, $n->textContent;
    }
    return @vals;
}


sub formatDateTime {
    my $datetime = shift;
    # If argument is yyyy-mm-dd hh:mm:ss reformat to mm/dd/yyyy hh:mm:ss.

    my ($date,$time) = split " ", $datetime;
    my ($yr,$mon,$day);
    my $newdate;

    if ($date =~ /^\d{4}-\d{2}-\d{2}$/) {
        ($yr,$mon,$day) = split /-/, $date;
        $newdate = join '/', $mon,$day,$yr;

        $datetime = "$newdate $time";
    }

    return $datetime;
}

sub getHeader
{
    my ( $file ) = @_;
    my $xmlParser = XML::LibXML->new();
    $xmlParser->keep_blanks(0);
    
    unless ( open( LOCKFH, ">$file.lock" ) ) {
        S4P::perish( 1, "Failed to open lock file $file.lock" );
    }
    unless( flock( LOCKFH, 2 ) ) {
        S4P::perish( 1, "Failed to lock $file.lock" );
    }
       
    my $dom = $xmlParser->parse_file( $file );
    my $docOrig = $dom->documentElement();
    
    my @nodeList = ();
    foreach my $node ( $docOrig->findnodes( '//sequence' ) ) {
        my $number = $node->getAttribute( 'NUMBER' );
        my $textNode = $dom->createTextNode( $number );
        my $parent = $node->parentNode();
        $node->setAttribute( 'NUMBER' => $number+1 );
        push( @nodeList, $node, $textNode );
    }
    my ( $currentDate, $currentTime ) = split( /\s+/, S4P::timestamp(), 2 );
    foreach my $node ( $docOrig->findnodes( '//currentDate' ) ) {
        my $textNode = $dom->createTextNode( $currentDate );
        my $parent = $node->parentNode();
        push( @nodeList, $node, $textNode );
    }
    
    foreach my $node ( $docOrig->findnodes( '//currentTime' ) ) {
        my $textNode = $dom->createTextNode( $currentTime );
        my $parent = $node->parentNode();
        push( @nodeList, $node, $textNode );
    }
    
    close( LOCKFH );
    flock( LOCKFH, 8 );
    
    my $input = $docOrig->toString(1);
    for ( my $i=0 ; $i<@nodeList ; $i+=2 ) {
        my $parent = $nodeList[$i]->parentNode();
        $parent->insertBefore( $nodeList[$i+1], $nodeList[$i] );
        $parent->removeChild( $nodeList[$i] );
    }
    my $output = '';
    foreach my $child ( $docOrig->getChildNodes() ) {
        $output .= $child->toString(1);
    }
    $output =~ s/^\s+|\s+$//g;
    if ( open( FH, ">$file" ) ) {
        print FH $input, "\n";
        close( FH );
    } else {
        S4P::perish( 1, "Failed to update $file" );
    }
    
    return $output;
}

sub splitWO {
    my ( $woFile, $maxGroups, $totalDNs ) = @_;
    my $xmlParser = XML::LibXML->new();
    $xmlParser->keep_blanks( 0 );
    my $dom = $xmlParser->parse_file( $woFile );
    my $doc = $dom->documentElement();
    my ( $FilePacketNode ) = $doc->findnodes( '//FilePacket');
    my @FileGroupNodes = $doc->findnodes( '//FileGroup' );
    my $totalFGs = scalar( @FileGroupNodes );

    my @splitedWOs;
    for ( my $segno = 1; $segno <= $totalDNs; $segno++ ) {
        ( my $wo = $woFile ) =~ s/\.wo$/S$segno.wo/;
        $FilePacketNode->removeChildNodes();
        my $count = 0;
        while ( $count < $maxGroups ) {
            my $groupIndex = ( $segno - 1 ) * $maxGroups + $count;
            last if ( $groupIndex == $totalFGs );
            my $FileGroupNode = $FileGroupNodes[$groupIndex];
            $FilePacketNode->appendChild( $FileGroupNode );
            $count++;
        }
        open( WO, ">$wo" ) 
            || S4P::perish( 1, "Failed to create $wo for writing ($!)" );
        print WO $dom->toString(1);
        close( WO );
        push @splitedWOs, $wo;
    }
    return @splitedWOs;
}
