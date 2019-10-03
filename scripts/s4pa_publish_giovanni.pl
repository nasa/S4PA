#!/usr/bin/perl

=head1 NAME

s4pa_publish_giovanni.pl - script for Giovanni publication

=head1 SYNOPSIS

s4pa_publish_giovanni.pl 
[B<-c> I<config_file>]
[B<-p> I<pdrdir>]
[B<-x> I<xslfile>]
[B<-s> I<stagingdir>]
[B<-v> I<verbose>]

=head1 DESCRIPTION

s4pa_publish_giovanni.pl can accept a PDR directory containing work
orders or ALL as input, and then transforms every metadata file 
it finds into the proper Giovanni format. Giovanni xml formated file 
are written to a staging directory and work order created for
downstream processing.

=head1 ARGUMENTS

=over 4

=item B<-c>

configuration file. It should contain the following variables:
    $INSTANCE_NAME='goldsfs1u'
    $DESTDIR='/ftp/private/.xxxx/<MODE>/pending_insert';
    $HOST='tads1u.ecs.nasa.gov';
    $UNRESTRICTED_ROOTURL='ftp://goldsfs1u.ecs.nasa.gov/data/s4pa/';
    $RESTRICTED_ROOTURL='http://goldsfs1u.ecs.nasa.gov/data/s4pa/';
    $TYPE='insert';
    $MAX_GRANULE_COUNT='1000';

=item B<-p>

pending PDR and wo directory. For S4PA system, it is usually at
    ../pending_publish, or ../pending_delete

=item B<-x>

XSL file for transform s4pa meatadata file to giovanni xml format.

=item B<s>

staging directory for generated xml files.

=item B<-v>

Verbose.

=back

=head1 AUTHORS

Guang-Dih Lei (guang-dih.lei@gsfc.nasa.gov)

=cut

################################################################################
# $Id: s4pa_publish_giovanni.pl,v 1.4 2019/05/06 15:48:01 glei Exp $
# -@@@ S4PA, Version $Name:  $
###############################################################################
#
# name: s4pa_publish_giovanni.pl
# revised: 09/29/2006 glei initial release
#

use strict ;
use Getopt::Std ;
use S4P::PDR ;
use Safe ;
use XML::LibXSLT ;
use XML::LibXML ;
use S4PA::Storage;
use S4PA;
use Log::Log4perl;

=head1 AUTHORS

Guang-Dih Lei (guang-dih.lei@gsfc.nasa.gov)

=cut

################################################################################
# $Id: s4pa_publish_giovanni.pl,v 1.4 2019/05/06 15:48:01 glei Exp $
# -@@@ S4PA, Version $Name:  $
###############################################################################
#
# name: s4pa_publish_giovanni.pl
# revised: 09/29/2006 glei initial release
#

use strict ;
use Getopt::Std ;
use S4P::PDR ;
use Safe ;
use XML::LibXSLT ;
use XML::LibXML ;
use File::Basename ;
use File::Copy;
use File::stat ;
use vars qw( $opt_c $opt_p $opt_x $opt_s $opt_v );

getopts('c:p:x:s:v');

unless (defined($opt_c)) {
    S4P::logger("ERROR","Failure to specify -c <ConfigFile> on command line.") ;
    usage();
}
my $configFile = $opt_c;

unless (defined($opt_p)) {
    S4P::logger("ERROR","Failure to specify -p <pdrdir> on command line.") ;
    usage();
}
my $pdrdir = $opt_p ;

unless (defined($opt_s)) {
    S4P::logger("ERROR","Failure to specify -s <stagingdir> on command line.") ;
    usage();
}

unless (defined($opt_x)) {
    S4P::logger("ERROR","Failure to specify -x <xslfile> on command line.") ;
    usage();
}


#####################################################################
# Configuration setup
#####################################################################
my $stagingDir = $opt_s;
my $verbose = $opt_v;

# retrieve config values
my $cpt = new Safe 'CFG';
$cpt->share( '$INSTANCE_NAME', '$DESTDIR', '$HOST', '$UNRESTRICTED_ROOTURL', 
    '$RESTRICTED_ROOTURL', '$TYPE', '$MAX_GRANULE_COUNT' ) ;
$cpt->rdo($opt_c) or
    S4P::perish( 1, "Cannot read config file $opt_c in safe mode: $!\n");

my $logger = S4PA::CreateLogger( $CFG::cfg_logger{FILE},
    $CFG::cfg_logger{LEVEL} ) if ( %CFG::cfg_logger );

my @date = localtime(time);
my $dateString = sprintf("T%04d%02d%02d%02d%02d%02d", 
    $date[5]+1900, $date[4]+1, $date[3], $date[2], $date[1], $date[0]);
my $maxCount = $CFG::MAX_GRANULE_COUNT ? $CFG::MAX_GRANULE_COUNT : 1000;
my $opsType = $CFG::TYPE;
my $instance = $CFG::INSTANCE_NAME;

my $xmlPrefix;
if ( $opsType eq 'insert' ) {
    $xmlPrefix = 'GiovanniIns';
} elsif ( $opsType eq 'delete' ) {
    $xmlPrefix = 'GiovanniDel';
} else {
    S4P::perish( 2, "Operation Type: $opsType not supported");
}

# append instance name to prefix if INSTANCE_NAME is in configuration
# if not, append the nodename in UNRESTRICTED_ROOTURL to prefix
if ( ! defined $instance ) {
    $instance = "$1" if ( $CFG::UNRESTRICTED_ROOTURL =~ m#://(\w+)\.# );
}
$xmlPrefix .= ".$instance";

# create staging area if it doesn't exist.
mkdir( $stagingDir ) || S4P::perish( 4, "Failed to create $stagingDir" )
    unless ( -d $stagingDir );

my $xslFile = $opt_x;
S4P::perish( 1, "Cannot find Specified $xslFile") unless ( -f $xslFile );

#####################################################################
# PDR processing
#####################################################################
# examine PDR directory
opendir (PDRDIR,"$pdrdir") || S4P::perish( 3, "Failed to open $pdrdir" );
my @files = readdir (PDRDIR) ;
close (PDRDIR) ;
my $numFiles = scalar(@files);
$logger->debug( "Found $numFiles files under $pdrdir" )
    if defined $logger;

# batch all PDRs
my @processed_pdr;
my @batchPDR;
my @xmlFiles;
my $granuleCount = 0;
my $batchCount = 0;
my $pdrCount = 0;
my @invalidPDR;

# parse each PDR
foreach my $pdrfile ( @files ) {
    chomp( $pdrfile );
    next if ($pdrfile !~ /\.(PDR|wo)$/);
    my $pdrPath = "$pdrdir/$pdrfile";

    # count file_group in the current pdr
    my $fgCount = `grep OBJECT=FILE_GROUP $pdrPath | wc -l` / 2;

    # skip empty PDR or work order
    next if ( $fgCount == 0 );
    $logger->debug( "Found $pdrfile with $fgCount granule(s)." ) 
        if defined $logger;
    $granuleCount += $fgCount;
    push @batchPDR, $pdrPath;

    # convert pdr to xml when granule count exceeded MAX_GRANULE_COUNT
    $pdrCount++;
    if ( $granuleCount >= $maxCount ) {
        S4P::logger("INFO", "Processing $pdrCount PDRs, $granuleCount Granules.");
        $logger->info( "Processing $pdrCount PDR(s) with total $granuleCount " .
            "granule(s)." ) if defined $logger;

        my ( $xmlPath, @badPDR ) = create_xml($batchCount, $instance, @batchPDR);
        if ( -f "$xmlPath" ) {
            S4P::logger( "INFO", "$xmlPath created." );
            $logger->info( "Created $xmlPath for publishing" ) 
                if defined $logger;
        }
        push @xmlFiles, $xmlPath;
        push @invalidPDR, @badPDR;

        # counter/array reset
        $granuleCount = 0;
        $pdrCount = 0;
        @batchPDR = ();
        $batchCount++;
    }
}

if ( $pdrCount > 0 ) {
    S4P::logger("INFO", "Processing $pdrCount PDRs, $granuleCount Granules.");
    $logger->info( "Processing $pdrCount PDR(s) with total $granuleCount " .
        "granule(s)." ) if defined $logger;

    my ( $xmlPath, @badPDR ) = create_xml($batchCount, $instance, @batchPDR); 
    S4P::logger( "INFO", "$xmlPath created." ) if ( -f "$xmlPath" );
    $logger->info( "Created $xmlPath for publishing" ) if defined $logger;
    push @xmlFiles, $xmlPath;
    push @invalidPDR, @badPDR;
}
$pdrCount = scalar( @processed_pdr );


#####################################################################
# Creating work order for postoffice
#####################################################################
# write out work order for postoffice
my $wo_type = 'PUSH';
my $wo_file = sprintf("%s.%s.%s.wo", $wo_type, $xmlPrefix, $dateString);

if ( $pdrCount ) {
    my $status = create_wo( $wo_file, $pdrCount, \@xmlFiles );
    unless ( $status ) {
        foreach my $pdr ( @processed_pdr ) {
            $logger->info( "Created work order $wo_file for postoffice" )
                if defined $logger;
            unlink $pdr;
            S4P::logger( "INFO", "Removed $pdr" ) if ($verbose);
            $logger->debug( "Deleted $pdr" ) if defined $logger;
        }

        S4P::logger("INFO", "Completed: Processed $pdrCount PDRs.");
        $logger->info( "Completed $pdrCount PDR(s)" )
            if defined $logger;
    }
} 
else {
    S4P::logger( "INFO", "No PDRs or WOs found" );
    $logger->debug( "Found no PDR or WO" ) if defined $logger;
    foreach my $xmlfile ( @xmlFiles ) {
        unlink $xmlfile;
    }
}

# Fail the job if there is any invalid PDR encountered, Ticket #5995.
if ( scalar @invalidPDR > 0 ) {
    my $failMessage = "Encountered invalid PDR(s): ";
    foreach my $pdr ( @invalidPDR ) {
        $failMessage .= $pdr . " ";
    }
    $failMessage .= " All valid PDR(s) has been processed.";
    $logger->error( $failMessage ) if defined $logger;
    $failMessage .= " Please check master publish.log for details.";
    S4P::perish( 8, $failMessage );
}

exit( 0 );


#####################################################################
# create_xml:  transform batch of PDRs into one xml file
#####################################################################
sub create_xml {

    my ($batchCount, $instance, @batchPDR) = @_;
    my $skippedGranule = 0;
    my $skippedPDR = 0;
    my @invalidPDR = ();

    # generate Giovanni xml header
    my $xmlParser = XML::LibXML->new();
    my $dom = $xmlParser->parse_string('<GranuleMetaDataFile/>');
    my $doc = $dom->documentElement();

    # parse each PDR
    foreach my $pdrfile ( @batchPDR ) {
        my $pdr = S4P::PDR::read_pdr($pdrfile);
        if (!$pdr) {
            S4P::logger('ERROR', "Cannot read PDR file $pdrfile: $!");
            $logger->error( "Failed reading $pdrfile" ) if defined $logger;
            $skippedPDR++;
            push @invalidPDR, $pdrfile;
            next;
        }
        $logger->info( "Processing $pdrfile" ) if defined $logger;

        # parse through each FILE_GROUP
        my $url ;
        foreach my $fg (@{$pdr->file_groups}) {
            my $datatype = $fg->data_type();
            my $meta_file = $fg->met_file();

            # make sure all metadata and science files exist
            unless ( -f $meta_file ) {
                S4P::logger('ERROR', "Missing metadata file: $meta_file, skipped");
                $logger->error( "Missing metadata file: $meta_file, skipped") 
                    if defined $logger;
                $skippedGranule++;
                next;
            }

            # convert s4pa xml to giovanni xml
            my $g3_string;
            $g3_string = convert_file( $meta_file, $xslFile );
            my $fg_parser = XML::LibXML->new();

            my $fg_dom = $fg_parser->parse_string( $g3_string );
            my $fg_doc = $fg_dom->documentElement();

            if ( $opsType eq 'insert' ) {
                # extract GranuleURMetaData node
                my $fg_nodes = $fg_doc->findnodes('//GranuleURMetaData');
                my $URMetaDataNode = $fg_nodes->get_node(1);
                $doc->appendChild($URMetaDataNode);
                $logger->info( "Added $meta_file for giovanni publishing" )
                    if defined $logger;
            }
            elsif ( $opsType eq 'delete' ) {
                my $DelGranuleNode = XML::LibXML::Element->new('DeleteGranule');

                # append ShortName node
                my $fg_nodes = $fg_doc->findnodes('//ShortName');
                my $ShortNameNode = $fg_nodes->get_node(1);
                $DelGranuleNode->appendChild($ShortNameNode);

                # append VersionID node
                my $fg_nodes = $fg_doc->findnodes('//VersionID');
                my $VersionIDNode = $fg_nodes->get_node(1);
                $DelGranuleNode->appendChild($VersionIDNode);

                # append GranuleID node
                my $fg_nodes = $fg_doc->findnodes('//DataURL');
                my $GranuleIDNode = $fg_nodes->get_node(1);
                $DelGranuleNode->appendChild($GranuleIDNode);

                # append DeleteTime node
                my $delete_time = sprintf("%04d\-%02d\-%02d %02d\:%02d\:%02d",
                    $date[5]+1900, $date[4]+1, $date[3], $date[2], $date[1], $date[0]);
                $DelGranuleNode->appendTextChild( 'DeleteDateTime' , $delete_time );
                $doc->appendChild($DelGranuleNode);
                $logger->info( "Added $meta_file for giovanni deletion" )
                    if defined $logger;
            }
        }
        push @processed_pdr, $pdrfile;
    }

    my $batchString = "";
    if ( $batchCount > 0 ) {
        $batchString = "_" . "$batchCount";
    }
    my $giovanni_xml = sprintf("%s.%s%s.xml", $xmlPrefix, $dateString, $batchString);
    my $g3XML = "$stagingDir/$giovanni_xml";

    open (OUTFILE, "> $g3XML") or S4P::perish( 5, "Can't open $g3XML\n" );
    print OUTFILE $dom->toString(1);
    close (OUTFILE);
    S4P::logger( "INFO", "skipped $skippedPDR PDRs and $skippedGranule Granules");
    if ( $skippedPDR || $skippedGranule ) {
        $logger->error( "Skipped $skippedPDR PDR(s) and $skippedGranule granule(s)." )
            if defined $logger;
    }

    return ( $g3XML, @invalidPDR );
}


#####################################################################
# convert_file:  transform S4PA metadata file to Giovanni xml file
#####################################################################
sub convert_file {
    my ( $meta_file, $granule_xsl_file ) = @_ ;
    my $parser = XML::LibXML->new();
    $parser->keep_blanks(0);
    my $dom = $parser->parse_file( $meta_file );

    my $xslt = XML::LibXSLT->new();
    my $styleSheet = $xslt->parse_stylesheet_file( $granule_xsl_file );
    my $output = $styleSheet->transform( $dom );
    my $storedString = $styleSheet->output_string( $output );

    # replace the metadata file URL before output
    $storedString =~ /<DataURL>(.*)<\/DataURL>/;
    my $granuleID = $1;
    my $dataFileString = "<DataURL>" ;

    my $url = S4PA::Storage::GetRelativeUrl( $meta_file );
    S4P::perish( 6, "Failed to get relative URL for $meta_file" )
        unless defined $url;
    my $fs = stat( $meta_file );
    $url = $url . '/' . $granuleID;

    # remove double // in the path
    $url =~ s/\/+/\//g;

    if ( $fs->mode() & 004 ) {
        $url = $CFG::UNRESTRICTED_ROOTURL . $url;
    } else {
        $url = $CFG::RESTRICTED_ROOTURL . $url;
    }
    $dataFileString .= "$url</DataURL>";
    $storedString =~ s/<DataURL>(.*)<\/DataURL>/$dataFileString/;

    return $storedString;
}

#####################################################################
# create_wo:  create postoffice work order
#####################################################################
sub create_wo {
    my ( $wo, $pdrCount, $xmlFiles ) = @_;

    my $wo_parser = XML::LibXML->new();
    my $wo_dom = $wo_parser->parse_string('<FilePacket/>');
    my $wo_doc = $wo_dom->documentElement();

    my ($filePacketNode) = $wo_doc->findnodes('/FilePacket');
    $filePacketNode->setAttribute('status', "I");

    my $destination = "ftp:" . $CFG::HOST . $CFG::DESTDIR;
    $filePacketNode->setAttribute('destination', $destination);

    # carete filegroup for giovanni xml file
    if ( $pdrCount ) {
        my $filegroupNode = XML::LibXML::Element->new('FileGroup');
        foreach my $xmlFile ( @{$xmlFiles} ) {
            my $fileNode = XML::LibXML::Element->new('File');
            $fileNode->setAttribute('localPath', $xmlFile);
            $fileNode->setAttribute('status', "I");
            $fileNode->setAttribute('cleanup', "Y");
            $filegroupNode->appendChild($fileNode);
        }
        $wo_doc->appendChild($filegroupNode);
    }

    open (WO, ">$wo") || S4P::perish( 7, "Failed to open workorder file $wo: $!");
    print WO $wo_dom->toString(1);
    close WO;

    return(0) ;
}


#####################################################################
# usage:  print usage and die
#####################################################################
sub usage {
  die << "EOF";
usage: $0 <-c config_file> <-x granule_to_giovanni_xls_file>
                         <-p pending_pdr_dir> <-s staging_dir> [options]
Options are:
        -v                  Verbose
EOF
}

################  end of s4pa_publish_giovanni  ################
