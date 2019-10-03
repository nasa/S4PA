#!/usr/bin/perl

=head1 NAME

s4pa_publish_echo.pl - script for ECHO publication

=head1 SYNOPSIS

s4pa_publish_echo.pl
B<-c> I<config_file>
B<-p> I<pdrdir>
B<-x> I<xslfile>
B<-b> I<browse xslfile>
B<-s> I<stagingdir>

=head1 DESCRIPTION

s4pa_publish_echo.pl uses an xsl stylesheet file to transform
every metadata file (PDR) it finds in the
PDR directory that was specified as an option into the ECHO granule metadata
xml format. ECHO xml formatted files are written to a staging directory,
and a work order is created for downstream processing.

=head1 ARGUMENTS

=over 4

=item B<-c> I<config_file>

A configuration file for used for publishing granules to ECHO. It should
contain:

=over 4

=item I<%DATA_ACCESS>

S4PA access hash, keys are dataset shortnames, values
are hashes whose keys are version strings and values are "public",
"private", or "restricted".

=item I<$DESTDIR>

Destination directory published files will be transferred to

=item I<$HOST>:

Host that published files will be transferred to

=item I<$UNRESTRICTED_ROOTURL>

Root URL for access to unrestricted files

=item I<$RESTRICTED_ROOTURL>

Root URL for access to restricted files

=item I<$TYPE>

Publishing type ('insert' or 'delete')

=back

=item B<-p> I<pdrdir>

Directory containing PDR files which specify granule metadata files to be
published

=item B<-x> I<xslfile>

XSL stylesheet file used to convert granule metadate to ECHO metadata

=item B<-b> I<browse xslfile>

XSL stylesheet file used to convert granule metadate to ECHO browse reference

=item B<-s> I<stagingdir>

Directory where ECHO metadata files will be staged before being transferred
to an ECHO host

=back

=head1 AUTHORS

Lou Fenichel (lou.fenichel@gsfc.nasa.gov)
Ed Seiler (Ed.Seiler@nasa.gov)

=cut

################################################################################
# $Id: s4pa_publish_echo.pl,v 1.59 2019/05/06 15:48:01 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################
#
# name: s4pa_publish_echo.pl
# revised: 03/20/2006 lhf integrate s4pa_delete_echo.pl as well as URL creation
# revised: 03/21/2006 lhf make type an entry in the configFile instead of a command
#                         line argument, verify that multi-granule files will construct
#                         <URL> tags correctly
# revised: 05/12/2006 glei added ftp threough firewall option
# revised: 08/12/2006 glei batch all pdr into one xml, create work order
#                         to replace ftp push and -o oldlist option
# revised: 10/27/2006 ejs Now finds matches between %ECHO_ACCESS and
#                         %DATA_ACCESS by checking every %ECHO_ACCESS version
#                         for a numerical match with the %DATA_ACCESS entry
#                         determined by the filegroup version
#                         No longer publishes an "empty" granule metadata
#                         file if no PDRs contain publishable granules
# revised: 12/19/2006 ahe Use instance name in xml file name.
# revised: 12/19/2006 ahe Added browse file xsl file.  Added option b
# revised: 12/26/2006 glei added browse and reference file publishing
# revised: 12/06/2014 glei add publish versionless datasets with different versions

use strict;
use Getopt::Std;
use S4P::PDR;
use Safe;
use XML::LibXSLT;
use XML::LibXML;
use File::Basename;
use File::stat;
use File::Copy;
use S4PA::Storage;
use S4PA;
use Log::Log4perl;
use Cwd;
use vars qw( $opt_c $opt_p $opt_x $opt_b $opt_s $opt_v );

getopts('c:p:x:b:s:v');

unless (defined($opt_c)) {
    S4P::logger("ERROR","Failure to specify -c <ConfigFile> on command line.");
    exit(2);
}
unless (defined($opt_p)) {
    S4P::logger("ERROR","Failure to specify -p <pdrdir> on command line.");
    exit(2);
}
unless (defined($opt_x)) {
    S4P::logger("ERROR","Failure to specify -x <xslfile> on command line.");
    exit(2);
}
unless (defined($opt_s)) {
    S4P::logger("ERROR","Failure to specify -s <stagingdir> on command line.");
    exit(2);
}

my $stationDir = dirname( cwd() );

# Read configuration values
my $cpt = new Safe 'CFG';
$cpt->rdo($opt_c) or
    S4P::perish(1, "Cannot read config file $opt_c in safe mode: $!\n");
my $granMax = $CFG::MAX_GRANULE_COUNT ? $CFG::MAX_GRANULE_COUNT : 1000;

# Determine currently configured ECHO version
$opt_x =~ /S4paGran2ECHO(\d{2})\.xsl/;
my $echoSchemaVersion = ( defined $1 ) ? $1 : 9;

my $logger = S4PA::CreateLogger( $CFG::cfg_logger{FILE},
    $CFG::cfg_logger{LEVEL} ) if ( %CFG::cfg_logger );

my $pdrdir = $opt_p;
my @date = localtime(time);

# Get the names of all files in the PDR directory
opendir (PDRDIR,"$pdrdir") || S4P::perish(2, "Can't open $pdrdir: $!");
my @files = readdir (PDRDIR);
close (PDRDIR);
my $numFiles = scalar(@files);
$logger->debug( "Found $numFiles files under $pdrdir" )
    if defined $logger;

# Create the staging area directory if it doesn't exist.
mkdir( $opt_s ) || S4P::perish(3, "Failed to create $opt_s")
    unless ( -d $opt_s );

# Generate the ECHO xml header

my ( $dom, $doc );
$dom = XML::LibXML::Document->new( '1.0', 'UTF-8' );
my $root = XML::LibXML::Element->new( 'GranuleMetaDataFile' );
$root->setAttribute( 'xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance' );
$root->setAttribute( 'xsi:noNamespaceSchemaLocation',
                     'http://www.echo.nasa.gov/ingest/schemas/operations/Granule.xsd' );
$dom->setDocumentElement( $root );
$doc = $dom->documentElement();

# ECHO xml elements

my $DataSetNode = XML::LibXML::Element->new('GranuleMetaDataSet');
my $GranulesNode = XML::LibXML::Element->new('Granules');
my $DelGranulesNode = XML::LibXML::Element->new('DeleteGranules');
my $BrowseImagesNode = XML::LibXML::Element->new( 'BrowseImages' );
my $GranuleDeletesNode = XML::LibXML::Element->new('GranuleDeletes');
my $BrowseDeletesNode = XML::LibXML::Element->new( 'BrowseImageDeletes' );

# Generate the ECHO browse xml header

my ( $browseDom, $browseDoc, $temporalNode );
$browseDom = XML::LibXML::Document->new( '1.0', 'UTF-8' );
$root = XML::LibXML::Element->new( 'BrowseMetaDataFile' );
$root->setAttribute( 'xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance' );
$root->setAttribute( 'xsi:noNamespaceSchemaLocation',
                     'http://www.echo.nasa.gov/ingest/schemas/operations/Browse.xsd' );
$browseDom->setDocumentElement( $root );
$browseDoc = $browseDom->documentElement();

# Loop for every file in the PDR directory
my @processed_pdr;
my @processed_browse;
my @unpublishable_pdr;
my $processed_browse_count;
my $processed_fg_count;

foreach my $pdrfile ( @files ) {
    chomp( $pdrfile );

    # Skip files that are not PDR files
    next if ($pdrfile !~ /\.PDR$/);

    # Read the PDR file
    my $pdrfile_fullpath = "$pdrdir/$pdrfile";
    my $pdr = S4P::PDR::read_pdr($pdrfile_fullpath);
    if (!$pdr) {
        S4P::logger('ERROR', "Cannot read PDR file $pdrfile: $!");
        $logger->error( "Failed reading $pdrfile" ) if defined $logger;
        next;
    }
    $logger->info( "Processing $pdrfile" ) if defined $logger;

    # Loop for every file group in the PDR
    $processed_fg_count = 0;
    foreach my $fg (@{$pdr->file_groups}) {
        my $datatype = $fg->data_type();
        my $dataversion = $fg->data_version();
        my $meta_file = $fg->met_file();
        my @science_files = $fg->science_files();
        my $browsePath = $fg->browse_file();

        # set versionless dataset flag
        my $versionless = ( $dataversion eq '' ) ? 1 : 0;
        my $s4paAccessType;
        my %echoAccessHash;
        # When inserting, skip file groups whose S4PA access type is hidden or
        # whose S4PA access type does not match the ECHO access type
        if ( $CFG::TYPE eq 'insert' ) {
            # When inserting, %CFG::ECHO_ACCESS will not be defined unless
            # s4pa_get_echo_access.pl has added it to the configuration file.
            # If it is not defined, that indicates that s4pa_get_echo_access.pl
            # has not been run, and therefore we can't tell which file groups
            # are really meant to be skipped, so we will exit with an error.
            S4P::perish( 9, "No %ECHO_ACCESS found in $opt_c" )
                unless %CFG::ECHO_ACCESS;

            if ( defined $CFG::DATA_ACCESS{$datatype}{$dataversion} ) {
                $s4paAccessType = $CFG::DATA_ACCESS{$datatype}{$dataversion};
            } elsif ( defined $CFG::DATA_ACCESS{$datatype}{''} ) {
                $s4paAccessType = $CFG::DATA_ACCESS{$datatype}{''};
                # dataset is versionless if metadata of PDR carry a version ID
                # and there is a versionless version ID defined
                $versionless = 1;
            } else {
                S4P::perish( 4, "S4PA access type (DATA_ACCESS) not found" .
                             " for Datatype $datatype Version $dataversion" );
            }
            if ( $s4paAccessType eq 'hidden' ) {
                S4P::logger('INFO', "Datatype $datatype Version $dataversion" .
                            " is $s4paAccessType in S4PA");
                $logger->debug( "Skipped $s4paAccessType datatype $datatype " .
                    "Version $dataversion" ) if defined $logger;
                next;
            }
            my $echoDataVersion;
            if ($opt_v) {
                # Use the unmodified data version string as the key for
                # obtaining the ECHO access type.
                $echoDataVersion = $dataversion;
            } else {
                # Expect dataset version in ECHO to have been converted
                # to a numerical value.
                # Try to find an ECHO version for the datatype that matches
                # the data version by comparing the values numerically
                # after the non-digits have been removed from the data version.
                (my $dataversion_num = $dataversion) =~ s/\D//g;
                foreach my $version (keys %{$CFG::ECHO_ACCESS{$datatype}}) {
                    if ($version == $dataversion_num) {
                        $echoDataVersion = $version;
                        last;
                    }
                }
            }

            # For an s4pa versionless dataset, do the access type checking
            # until we get the versionID from the metadata file. For now,
            # just collect all versionID's access type in a hash.
            # if ( $dataversion eq '' ) {
            if ( $versionless ) {
                foreach my $version (keys %{$CFG::ECHO_ACCESS{$datatype}}) {
                    $echoAccessHash{$version} = $CFG::ECHO_ACCESS{$datatype}{$version};
                }
            } else {
                my $echoAccessType = defined $CFG::ECHO_ACCESS{$datatype}{$echoDataVersion} ?
                    $CFG::ECHO_ACCESS{$datatype}{$echoDataVersion} : '';
                if ( $s4paAccessType ne $echoAccessType ) {
                    S4P::logger('INFO', "Datatype $datatype Version '$dataversion'" .
                        " is $s4paAccessType in S4PA and Version" .
                        " '$echoDataVersion' is $echoAccessType in ECHO");
                    $logger->debug( "Skipped unmatched access type on datatype $datatype " .
                        "version $dataversion" ) if defined $logger;
                    next;
                }
            }
        }

        # Print out error message for missing metadata files
        # and skip the current fileGroup, ticket #6848.
        unless ( -f $meta_file ) {
            my $message = "Missing metadata file: $meta_file, skipped";
            S4P::logger('ERROR', $message);
            $logger->error( $message ) if defined $logger;
            S4P::raise_anomaly( "MISSING_METADATA_FILE", $stationDir,
                'WARN', $message, 0 );
            next;
        }

        # Skip publishing hidden granule unless it is a delete action
        my $fs = stat( $meta_file );
        if ( ( ( $fs->mode & 07777 ) == 0600 ) && ( $CFG::TYPE eq 'insert' ) ) {
            S4P::logger('ERROR', "Skip publishing hidden granule: $meta_file");
            $logger->error( "Skip publishing hidden granule: $meta_file")
                if defined $logger;
            next;
        }

        # Print out error message for missing browse files
        # and skip the current fileGroup, ticket #14376.
        if ( ( $browsePath ) && !(-f $browsePath) ) {
            my $message = "Missing browse file: $browsePath, skipped";
            S4P::logger('ERROR', $message);
            $logger->error( $message ) if defined $logger;
            S4P::raise_anomaly( "MISSING_BROWSE_FILE", $stationDir,
                'WARN', $message, 0 );
            next;
        }

        my $opendapInfo;
        if ( defined $CFG::ECHO_OPENDAP{$datatype}{$dataversion} ) {
            $opendapInfo = $CFG::ECHO_OPENDAP{$datatype}{$dataversion};
        } elsif ( defined $CFG::ECHO_OPENDAP{$datatype}{''} ) {
            $opendapInfo = $CFG::ECHO_OPENDAP{$datatype}{''};
        }

        # Convert S4PA xml to ECHO xml using the xsl stylesheet specified
        # by $opt_x
        my $echo_string;
        $echo_string = convert_file( $meta_file, $opt_x, $opendapInfo,
                                     @science_files );

        # For a versionless dataset in s4pa, check the ECHO access type with
        # versionID from the metadata file. Skip the filegroup if access type
        # does not match
        if ( $versionless ) {
            $echo_string =~ m#<VersionId>(.+)</VersionId>#;
            # this is the version ID in the metadata file
            my $echoDataVersion = $1;
            my $echoAccessType = defined $echoAccessHash{$echoDataVersion} ?
                $echoAccessHash{$echoDataVersion} : '';

            # can't find a matching version, try matching with digit in version only
            if ( $echoAccessType eq '' ) {
                # Try to find an ECHO version for the datatype that matches
                # the data version by comparing the values numerically
                # after the non-digits have been removed from the data version.
                (my $dataversion_num = $echoDataVersion) =~ s/\D//g;
                foreach my $version (keys %{$CFG::ECHO_ACCESS{$datatype}}) {
                    if ($version == $dataversion_num) {
                        $echoDataVersion = $version;
                        last;
                    }
                }
                $echoAccessType = $echoAccessHash{$echoDataVersion};

                # this versionless dataset probably published to ECHO with
                # a different version ID (probably from DIF) than the one in the metadata file,
                # replace the metadata version ID with the ECHO version ID when publishing
                $echo_string =~ s#<VersionId>(.+)</VersionId>#<VersionId>$echoDataVersion</VersionId>#;
            }

            if ( $s4paAccessType ne $echoAccessType ) {
                S4P::logger('INFO', "Datatype $datatype Version '$dataversion'" .
                    " is $s4paAccessType in S4PA and Version" .
                    " '$echoDataVersion' is $echoAccessType in ECHO");
                $logger->debug( "Skipped unmatched access type on datatype $datatype " .
                    "version $dataversion" ) if defined $logger;
                next;
            }
        }

        # Parse the converted xml file
        my $fg_parser = XML::LibXML->new();
        my $fg_dom = $fg_parser->parse_string( $echo_string );
        my $fg_doc = $fg_dom->documentElement();

        if ( $CFG::TYPE eq 'insert' ) {
            # Extract the Granule node from the converted
            # file and add it to a list of granules to be inserted
            my $fg_nodes;
            $fg_nodes = $fg_doc->findnodes('//Granule');
            my $GranuleNode = $fg_nodes->get_node(1);
            $GranulesNode->appendChild($GranuleNode);
            $logger->info( "Added $meta_file for echo publishing" )
                if defined $logger;
            $processed_fg_count++;
        }
        elsif ( $CFG::TYPE eq 'delete' ) {
            my $delete_time = sprintf("%04d\-%02d\-%02d %02d\:%02d\:%02d",
                              $date[5]+1900, $date[4]+1, $date[3],
                              $date[2], $date[1], $date[0]);
            # Extract the Granule node from the converted
            # file and add a GranuleDelete and DeleteTime node to a list of
            # granules to be deleted
            my $fg_nodes = $fg_doc->findnodes('//GranuleUR');
            my $GranuleURNode = $fg_nodes->get_node(1);
            my $GranuleDeleteNode = XML::LibXML::Element->new('GranuleDelete');
            $GranuleDeleteNode->appendChild($GranuleURNode);
            $GranuleDeletesNode->appendChild($GranuleDeleteNode);
            $logger->info( "Added $meta_file for echo deletion" )
                if defined $logger;
            $processed_fg_count++;
        }
        else {
            S4P::logger('INFO', "Type: $CFG::TYPE not supported");
        }

        # Jump to next filegroup if no browse files exist in the file group
        next unless ( $browsePath );

        # Get the optional BROWSE_DESCRIPTION from the configuration file,
        # leave it blank if not found.
        my $browseDescription = "";
        if ( defined $CFG::BROWSE_DESCRIPTION{$datatype}{$dataversion} ) {
            $browseDescription = $CFG::BROWSE_DESCRIPTION{$datatype}{$dataversion};
        } elsif ( defined $CFG::BROWSE_DESCRIPTION{$datatype}{''} ) {
            $browseDescription = $CFG::BROWSE_DESCRIPTION{$datatype}{''};
        } else {
            S4P::logger( 'INFO', "BrowseDescription not found" .
                " for Datatype $datatype Version $dataversion" )
                if ( $CFG::TYPE eq 'insert' );
        }

        # Convert S4PA xml to browse xml using the xsl stylesheet specified
        # by $opt_b
        my $browse_string;
        $browse_string = transform_browse( $meta_file, $opt_b );

        # Parse the converted xml file
        my $fg_browse_parser = XML::LibXML->new();
        my $fg_browse_dom = $fg_browse_parser->parse_string( $browse_string );
        my $fg_browse_doc = $fg_browse_dom->documentElement();

        my $fg_browseNode;
        ( $fg_browseNode ) = $fg_browse_doc->findnodes( 'BrowseImages/BrowseImage' );

        if ( $CFG::TYPE eq 'insert' ) {
            # Get browse file name and size
            my $browseFile = basename( $browsePath );
            my $fs = stat( $browsePath );

            my ( $sizeNode, $descNode );
            ( $sizeNode ) = $fg_browseNode->findnodes( 'FileSize' );
            ( $descNode ) = $fg_browseNode->findnodes( 'Description' );
            $sizeNode->appendText( $fs->size );

            # ECHO 10 uses FileURL instead of FileName.
            # Replace empty FileURL node with the URL determined
            # by $browsePath.
            ( my $urlNode ) = $fg_browseNode->findnodes( 'FileURL' );
            my $url = S4PA::Storage::GetRelativeUrl( $browsePath );
            S4P::perish( 6, "Failed to get relative URL for $browsePath" )
                  unless defined $url;
            $url = $url . '/' . $browseFile;
            $url =~ s/\/+/\//g;
            if ( $fs->mode() & 004 ) {
                $url =~ s#^/## if ($CFG::UNRESTRICTED_ROOTURL =~ m#/$#);
                $url = $CFG::UNRESTRICTED_ROOTURL . $url;
            } else {
                $url =~ s#^/## if ($CFG::RESTRICTED_ROOTURL =~ m#/$#);
                $url = $CFG::RESTRICTED_ROOTURL . $url;
            }
            $urlNode->appendText( $url );

            # ECHO10 does not allow an empty string for Description,
            # so delete the Description node if there is no text
            $descNode->unbindNode() unless ( $browseDescription );

            # Add browse file name to the list of files to be published.
            push @processed_browse, "$opt_s/$browseFile";
            $BrowseImagesNode->appendChild( $fg_browseNode );
            $processed_browse_count++;
        }

        elsif ( $CFG::TYPE eq 'delete' ) {
            my ( $BrowseidNode ) = $fg_browseNode->findnodes( 'ProviderBrowseId' );
            my $BrowseDeleteNode = XML::LibXML::Element->new( 'BrowseImageDelete' );
            $BrowseDeleteNode->appendChild( $BrowseidNode );
            $BrowseDeletesNode->appendChild( $BrowseDeleteNode );
            $logger->info( "Added $browsePath for echo browse deletion" )
                if defined $logger;
            $processed_browse_count++;
         }
         else {
             S4P::logger('INFO', "Type: $CFG::TYPE not supported");
         }
    }
    # Maintain separate lists of PDRs that contain publishable granules
    # and PDRs that contain unpublishable granules
    if ( $processed_fg_count ) {
        push @processed_pdr, $pdrfile_fullpath;
    } else {
        push @unpublishable_pdr, $pdrfile_fullpath;
    }
}
my $pdrCount = scalar( @processed_pdr );

# Determine elements of work order file names, append the granules node
# to the dataset node, and append the dataset node to the xml output document
my $wo;
my $browseWo;
if ( $CFG::TYPE eq 'insert' ) {
    $wo = 'EchoIns';
    $browseWo = 'EchoBrowseIns';
}
elsif ( $CFG::TYPE eq 'delete' ) {
    $wo = 'EchoDel';
    $browseWo = 'EchoBrowseDel';
}
else {
    S4P::logger('INFO', "Type: $CFG::TYPE not supported, skip PDR");
}

if ( $CFG::TYPE eq 'insert' ) {
    $doc->appendChild( $GranulesNode );
    $browseDoc->appendChild( $BrowseImagesNode );
} elsif ( $CFG::TYPE eq 'delete' ) {
    $doc->appendChild( $GranuleDeletesNode );
    $browseDoc->appendChild( $BrowseDeletesNode );
}

# Create work order for granule files

my $purgeFlag = 1;  # set flag to delete PDR files
my @madeGranuleXML = ();
my @madeBrowseXML = ();
my @madeGranuleWO = ();
my @madeBrowseWO = ();
print "has $pdrCount pdrs\n";
my $xpath;
if ( $pdrCount ) {
    # Find granule nodes
    $xpath = ($CFG::TYPE eq 'insert')
        ? '/GranuleMetaDataFile/Granules/Granule'
            : ($CFG::TYPE eq 'delete') ?
                '/GranuleMetaDataFile/GranuleDeletes/GranuleDelete' : undef;
    my @outGranuleNodeList = defined $xpath ? $doc->findnodes($xpath) : ();

    my $ct = scalar @outGranuleNodeList;
    print "has $ct granules in list\n";

    my $fileCounter = 0;
    $xpath = ($CFG::TYPE eq 'insert')
        ? '/GranuleMetaDataFile/Granules'
            : ($CFG::TYPE eq 'delete') ?
                '/GranuleMetaDataFile/GranuleDeletes': undef;
    while (scalar @outGranuleNodeList > 0) {
        my ($node) = $doc->findnodes($xpath) if defined $xpath;
        $node->removeChildNodes();
        my $length = (scalar @outGranuleNodeList > $granMax)
           ? $granMax : scalar @outGranuleNodeList;
        for (my $i=0; $i<$length; $i++) {
            $node->appendChild($outGranuleNodeList[$i]);
        }
        splice(@outGranuleNodeList, 0, $length);

        # Write ECHO granule metadata file to staging directory $opt_s
        # Include instance name in file name
        @date = localtime(time);
        my $echo_xml = sprintf("%s.%s.T%04d%02d%02d%02d%02d%02d_%d.xml", $wo, $CFG::INSTANCE_NAME,
            $date[5]+1900, $date[4]+1, $date[3], $date[2], $date[1], $date[0], $fileCounter);
        my $echo_path = "$opt_s/$echo_xml";
        unless ( open (OUTFILE, "> $echo_path") ) {
            S4P::logger('ERROR', "Failed to open $echo_path ($!)");
            $purgeFlag = 0;
        }
        print OUTFILE $dom->toString(1);
        unless ( close (OUTFILE) ) {
            S4P::logger('ERROR', "Failed to close $echo_path ($!)");
            $purgeFlag = 0;
        }

        S4P::logger( "INFO", "$echo_path created for $length granules" )
            if ( -f "$echo_path" );

        if ( -f "$echo_path" ) {
            push @madeGranuleXML, $echo_path;
            S4P::logger( "INFO",
                "$echo_path created for publishing $length granules" );
            $logger->info( "Created $echo_path for publishing $length granules" )
                if defined $logger;
        }

        # Write work order for postoffice station for the delivery of
        # granule xml
        @date = localtime(time);
        my $wo_type = 'PUSH';
        my $wo_file = sprintf("%s.%s.T%04d%02d%02d%02d%02d%02d_%d.wo", $wo_type, $wo,
            $date[5]+1900, $date[4]+1, $date[3], $date[2], $date[1], $date[0], $fileCounter);
        my $status = create_wo( $wo_file, $echo_path );
        $purgeFlag = 0 unless $status;
        if ($status) {
            push @madeGranuleWO, $wo_file;
            S4P::logger('INFO', "Work order $wo_file created");
        }
        $fileCounter++;
    }
} else {
    # No PDR files containing publishable granules were found in
    # the PDR directory
    S4P::logger( "INFO", "No PDRs containing publishable granules found" );
    $logger->debug( "No PDRs containing publishable granules found" )
        if defined $logger;
}

# Create work orders for browse files
if ( $processed_browse_count ) {

    # Find browse nodes
    my $xpath;
    $xpath = ($CFG::TYPE eq 'insert')
        ? '/BrowseMetaDataFile/BrowseImages/BrowseImage'
            : ($CFG::TYPE eq 'delete') ?
                '/BrowseMetaDataFile/BrowseImageDeletes/BrowseImageDelete' : undef;
    my @outBrowseNodeList = defined $xpath ? $browseDoc->findnodes($xpath) : ();

    my $fileCounter = 0;
    $xpath = ($CFG::TYPE eq 'insert')
        ? '/BrowseMetaDataFile/BrowseImages'
            : ($CFG::TYPE eq 'delete') ?
                '/BrowseMetaDataFile/BrowseImageDeletes' : undef;

    # Loop through the list of nodes in order to construct one or more work
    # orders, where each work order applies to a maximum of $granMax files.
    while (scalar @outBrowseNodeList > 0) {
        my ($node) = $browseDoc->findnodes($xpath);
        $node->removeChildNodes();
        my $length = (scalar @outBrowseNodeList > $granMax)
           ? $granMax : scalar @outBrowseNodeList;
        my @out_processed_browse = splice(@processed_browse, 0, $length);
        for (my $i=0; $i<$length; $i++) {
            $node->appendChild($outBrowseNodeList[$i]);
        }
        splice(@outBrowseNodeList, 0, $length);

        # Write the ECHO browse xml file to the staging directory $opt_s.
        # Include the instance name in the file name.
        @date = localtime(time);
        my $browse_xml = sprintf("%s.%s.T%04d%02d%02d%02d%02d%02d_%d.xml", $browseWo,
            $CFG::INSTANCE_NAME, $date[5]+1900, $date[4]+1, $date[3], $date[2],
            $date[1], $date[0], $fileCounter);
        my $browse_path = "$opt_s/$browse_xml";
        unless ( open (OUTFILE, "> $browse_path") ) {
            S4P::logger('ERROR', "Can't open $browse_path ($!)");
            $purgeFlag = 0;
        }
        print OUTFILE $browseDom->toString(1);
        unless ( close (OUTFILE) ) {
            S4P::logger('ERROR', "Failed to close $browse_path ($!)");
            $purgeFlag = 0;
        }

        if ( -f "$browse_path" ) {
            push @madeBrowseXML, $browse_path;
            S4P::logger( "INFO", "$browse_path created for $length" .
                " Browse Files" );
            $logger->info( "Created $browse_path for $length " .
                "browse files" ) if defined $logger;
        }

        # For ECHO 10 publishing, we are not publishing the browse files,
        # we are only publishing the browse xml file.
        @out_processed_browse = ($browse_path);

        # Write work order to the postoffice station for the delivery of
        # browse files (if inserting) and browse xml file
        @date = localtime(time);
        my $wo_type = 'PUSH';
        my $wo_file = sprintf("%s.%s.T%04d%02d%02d%02d%02d%02d_%d.wo", $wo_type, $browseWo,
                              $date[5]+1900, $date[4]+1, $date[3], $date[2], $date[1], $date[0], $fileCounter);

        my $status = create_browse_wo( $wo_file, @out_processed_browse );
        $purgeFlag = 0 unless $status;
        if ($status) {
            push @madeBrowseWO, $wo_file;
            S4P::logger('INFO', "Work order $wo_file created");
        }
        if ( $CFG::TYPE eq 'insert' ) {
            $logger->info( "Created work order $wo_file for echo browse publishing" )
                if defined $logger;
        } elsif ( $CFG::TYPE eq 'delete' ) {
            $logger->info( "Created work order $wo_file for echo browse deletion" )
                if defined $logger;
        }
        $fileCounter++;
    }
    if ( $CFG::TYPE eq 'insert' ) {
        S4P::logger('INFO', "Work orders created for " .
            "$processed_browse_count browse file publishing");
    }
    elsif ( $CFG::TYPE eq 'delete' ) {
        S4P::logger('INFO', "Work orders created for " .
            "$processed_browse_count browse file deletion");
    }
}

if ( $purgeFlag ) {
    # If all work orders were created, delete all PDRs containing
    # publishable granules
    foreach my $pdr ( @processed_pdr ) {
        unlink $pdr;
        S4P::logger('INFO', "Processed and removed $pdr.");
        $logger->debug( "Deleted $pdr" ) if defined $logger;
    }
} else {
    # Delete XML and work orders written
    foreach my $deleteFile (@madeGranuleXML) {
        unlink $deleteFile;
    }
    foreach my $deleteFile (@madeGranuleWO) {
        unlink $deleteFile;
    }
    foreach my $deleteFile (@madeBrowseXML) {
        unlink $deleteFile;
    }
    foreach my $deleteFile (@madeBrowseWO) {
        unlink $deleteFile;
    }
}

# Delete all PDRs containing only unpublishable granules
foreach my $pdr ( @unpublishable_pdr ) {
    unlink $pdr;
    S4P::logger('INFO', "No publishable granules, removed $pdr.");
    $logger->debug( "Delete $pdr, no publishable granules" )
        if defined $logger;
}

exit( 0 );


sub convert_file {
    my ( $meta_file, $granule_xsl_file, $opendapInfo, @science_files ) = @_;

    # Transform S4PA xml to ECHO xml using a stylesheet
    my $parser = XML::LibXML->new();
    $parser->keep_blanks(0);
    my $dom = $parser->parse_file( $meta_file );

    my $xslt = XML::LibXSLT->new();
    my $styleSheet = $xslt->parse_stylesheet_file( $granule_xsl_file );
    my $output = $styleSheet->transform( $dom );
    my $storedString = $styleSheet->output_string( $output );

    $storedString =~ /(\ +)<OnlineAccessURLs\/>/;
    my $indent = $1;
    $storedString =~ /(\ +)<OnlineResources\>/;
    my $indent2 = $1;

    # Replace the empty OnlineAccessURLs tag with xml containing a URL
    # for each science file
    my $urlString = "${indent}<OnlineAccessURLs>\n";
    my $openDapResourceUrlString;
    foreach my $sf ( @science_files ) {
        my $url = S4PA::Storage::GetRelativeUrl( $sf );
        S4P::perish( 6, "Failed to get relative URL for $sf" )
            unless defined $url;
        my $fs = stat( $sf );
        $url = $url . '/' . basename( $sf );
        $url =~ s/\/+/\//g;
        if (defined $opendapInfo) {
            if (exists $opendapInfo->{OPENDAP_URL_PREFIX}) {

                # Remove the group string, which extends from the first slash
                # up to, but not including, the second slash.
                (my $grouplessUrl = $url) =~ s#/.+?/##;

                if (exists $opendapInfo->{OPENDAP_URL_SUFFIX}) {
                    my $openDapUrl = $opendapInfo->{OPENDAP_URL_PREFIX} . '/' .
                                     $grouplessUrl .
                                     $opendapInfo->{OPENDAP_URL_SUFFIX};
                    $urlString .=
                        "$indent  <OnlineAccessURL>\n" .
                        "$indent    <URL>$openDapUrl</URL>\n" .
                        "$indent  </OnlineAccessURL>\n";
                }
                if (exists $opendapInfo->{OPENDAP_RESOURCE_URL_SUFFIX}) {
                    my $openDapUrl = $opendapInfo->{OPENDAP_URL_PREFIX} . '/' .
                                     $grouplessUrl .
                                     $opendapInfo->{OPENDAP_RESOURCE_URL_SUFFIX};
                    my $mimeType = 'application/octet-stream';
                    my $urlSuffix = $1 if $openDapUrl =~ /.+(\..+)$/;
                    if ($urlSuffix =~ /\.html$/) {
                        $mimeType = 'text/html';
                    } elsif ($urlSuffix =~ /\.info$/) {
                        $mimeType = 'text/html';
                    } elsif ($urlSuffix =~ /\.ddx$/) {
                        $mimeType = 'application/xml';
                    } elsif ($urlSuffix =~ /\.dds$/) {
                        $mimeType = 'text/plain';
                    } elsif ($urlSuffix =~ /\.das$/) {
                        $mimeType = 'text/plain';
                    } elsif ($urlSuffix =~ /\.rdf$/) {
                        $mimeType = 'application/rdf+xml';
                    } elsif ($urlSuffix =~ /\.nc$/) {
                        $mimeType = 'application/x-netcdf';
                    } elsif ($urlSuffix =~ /\.nc4$/) {
                        $mimeType = 'application/x-netcdf;ver=4';
                    } elsif ($urlSuffix =~ /\.ascii$/) {
                        $mimeType = 'text/plain';
                    } elsif ($urlSuffix =~ /\.txt$/) {
                        $mimeType = 'text/plain';
                    } elsif ($urlSuffix =~ /\.csv$/) {
                        $mimeType = 'text/csv';
                    } elsif ($urlSuffix =~ /\.HDF$/) {
                        $mimeType = 'application/x-hdf';
                    } elsif ($urlSuffix =~ /\.HDF5$/) {
                        $mimeType = 'application/x-hdf';
                    } elsif ($urlSuffix =~ /\.hdf$/) {
                        $mimeType = 'application/x-hdf';
                    } elsif ($urlSuffix =~ /\.hdf5$/) {
                        $mimeType = 'application/x-hdf';
                    } elsif ($urlSuffix =~ /\.h5$/) {
                        $mimeType = 'application/x-hdf';
                    } elsif ($urlSuffix =~ /\.he5$/) {
                        $mimeType = 'application/x-hdf';
                    } elsif ($urlSuffix =~ /\.gif$/) {
                        $mimeType = 'image/gif';
                    } elsif ($urlSuffix =~ /\.jpg$/) {
                        $mimeType = 'image/jpeg';
                    } elsif ($urlSuffix =~ /\.png$/) {
                        $mimeType = 'image/png';
                    } elsif ($urlSuffix =~ /\.gz$/) {
                        $mimeType = 'application/x-gzip';
                    } elsif ($urlSuffix =~ /\.gzip$/) {
                        $mimeType = 'application/x-gzip';
                    } elsif ($urlSuffix =~ /\.bz2$/) {
                        $mimeType = 'application/x-bzip2';
                    } elsif ($urlSuffix =~ /\.z$/) {
                        $mimeType = 'application/x-compress';
                    } elsif ($urlSuffix =~ /\.Z$/) {
                        $mimeType = 'application/x-compress';
                    } elsif ($urlSuffix =~ /\.tar$/) {
                        $mimeType = 'application/x-tar';
                    }
                    $openDapResourceUrlString .=
                        "$indent2  <OnlineResource>\n" .
                        "$indent2    <URL>$openDapUrl</URL>\n" .
                        "$indent2    <Description>The OPENDAP location for the granule.</Description>\n" .
                        "$indent2    <Type>GET DATA : OPENDAP DATA (DODS)</Type>\n" .
                        "$indent2    <MimeType>$mimeType</MimeType>\n" .
                        "$indent2  </OnlineResource>\n";
                }
            }
        }
        if ( $fs->mode() & 004 ) {
            $url =~ s#^/## if ($CFG::UNRESTRICTED_ROOTURL =~ m#/$#);
            $url = $CFG::UNRESTRICTED_ROOTURL . $url;
        } else {
            $url =~ s#^/## if ($CFG::RESTRICTED_ROOTURL =~ m#/$#);
            $url = $CFG::RESTRICTED_ROOTURL . $url;
        }
        $urlString .=
            "$indent  <OnlineAccessURL>\n" .
            "$indent    <URL>$url</URL>\n" .
            "$indent  </OnlineAccessURL>\n";
    }
    $urlString .= "${indent}</OnlineAccessURLs>";
    $storedString =~ s/${indent}<OnlineAccessURLs\/>/$urlString/;

    if (defined $openDapResourceUrlString) {
        $openDapResourceUrlString = "${indent2}<OnlineResources>\n" .
                                    $openDapResourceUrlString .
                                    "${indent2}</OnlineResources>\n";
        $storedString =~ s/${indent2}<OnlineResources\/>/$openDapResourceUrlString/;
    } else {
        $storedString =~ s/${indent2}<OnlineResources\/>//;
    }

    return $storedString;
}


sub transform_browse {
    my ( $meta_file, $browse_xsl_file ) = @_;

    # Transform S4PA xml to ECHO xml using a stylesheet
    my $parser = XML::LibXML->new();
    $parser->keep_blanks(0);
    my $dom = $parser->parse_file( $meta_file );

    my $xslt = XML::LibXSLT->new();
    my $styleSheet = $xslt->parse_stylesheet_file( $browse_xsl_file );
    my $output = $styleSheet->transform( $dom );
    my $storedString = $styleSheet->output_string( $output );

    return $storedString;
}


sub create_wo {
    my ( $wo, $echo_file ) = @_;

    # Write a work order to push a file via ftp
    my $wo_parser = XML::LibXML->new();
    my $wo_dom = $wo_parser->parse_string('<FilePacket/>');
    my $wo_doc = $wo_dom->documentElement();

    my ($filePacketNode) = $wo_doc->findnodes('/FilePacket');
    $filePacketNode->setAttribute('status', "I");

    my $destination = "ftp:" . $CFG::HOST . $CFG::DESTDIR;
    $filePacketNode->setAttribute('destination', $destination);

    my $filegroupNode = XML::LibXML::Element->new('FileGroup');
    my $fileNode = XML::LibXML::Element->new('File');
    $fileNode->setAttribute('localPath', $echo_file);
    $fileNode->setAttribute('status', "I");
    $fileNode->setAttribute('cleanup', "Y");
    $filegroupNode->appendChild($fileNode);

    $wo_doc->appendChild($filegroupNode);

    unless ( open (WO, ">$wo") ) {
        S4P::logger( "ERROR", "Failed to open work order $wo ($!)");
        return 0;
    }
    print WO $wo_dom->toString(1);
    unless ( close WO ) {
        S4P::logger( "ERROR", "Failed to close work order $wo ($!)" );
        return 0;
    }
    return(1);
}

sub create_browse_wo {
    my ( $wo, @browseFileList ) = @_;

    # Write a work order to push a list of files via ftp
    my $wo_parser = XML::LibXML->new();
    my $wo_dom = $wo_parser->parse_string('<FilePacket/>');
    my $wo_doc = $wo_dom->documentElement();

    my ($filePacketNode) = $wo_doc->findnodes('/FilePacket');
    $filePacketNode->setAttribute('status', "I");

    my $destination = "ftp:" . $CFG::HOST . $CFG::BROWSEDIR;
    $filePacketNode->setAttribute('destination', $destination);

    my $browseGroupNode = XML::LibXML::Element->new('FileGroup');
    foreach my $browse ( @browseFileList ) {
        my $browseNode = XML::LibXML::Element->new('File');
        $browseNode->setAttribute('localPath', $browse);
        $browseNode->setAttribute('status', "I");
        $browseNode->setAttribute('cleanup', "Y");
        $browseGroupNode->appendChild($browseNode);
    }
    $wo_doc->appendChild($browseGroupNode);

    unless ( open (WO, ">$wo") ) {
        S4P::logger( "ERROR", "Failed to open workorder file $wo: ($!)");
        return 0;
    }
    print WO $wo_dom->toString(1);
    unless ( close WO ) {
        S4P::logger( "ERROR", "Failed to close work order $wo ($!)" );
        return(0);
    }
    return(1);
}

################  end of s4pa_publish_echo  ################
