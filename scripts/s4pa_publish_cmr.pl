#!/usr/bin/perl

=head1 NAME

s4pa_publish_cmr.pl - script for CMR publication

=head1 SYNOPSIS

s4pa_publish_cmr.pl
B<-c> I<config_file>
B<-p> I<pdrdir>
B<-x> I<xslfile>
B<-b> I<browse xslfile>
B<-s> I<stagingdir>

=head1 DESCRIPTION

s4pa_publish_cmr.pl uses an xsl stylesheet file to transform
every metadata file (PDR) it finds in the
PDR directory that was specified as an option into the CMR granule metadata
xml format. CMR xml formatted files are written to a work order that is
sent to downstream processing.

=head1 ARGUMENTS

=over 4

=item B<-c> I<config_file>

A configuration file for used for publishing granules to CMR. It should
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

XSL stylesheet file used to convert granule metadate to CMR metadata (obsolete)

=item B<-b> I<browse xslfile>

XSL stylesheet file used to convert granule metadate to CMR browse reference

=item B<-s> I<stagingdir>

Directory where CMR metadata files will be staged before being transferred
to an CMR host

=item B<-v>

Use the data version string from the PDR as the key for obtaining the CMR access type in the configuration file

=back

=head1 AUTHORS

Ed Seiler (Ed.Seiler@nasa.gov)

=cut

################################################################################
# $Id: s4pa_publish_cmr.pl,v 1.12 2019/08/28 11:50:17 s4pa Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use Getopt::Std;
use S4P;
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
use Clavis;
use LWP::UserAgent;
use vars qw( $opt_c $opt_p $opt_x $opt_b $opt_s );

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

unless (defined $CFG::cfg_s4pa_root) {
    S4P::perish(1, "cfg_s4pa_root not defined in $opt_c\n");
}
my $ds_cfg_path = "$CFG::cfg_s4pa_root/other/dif_fetcher/s4pa_dif_info.cfg";

my $cpt2 = new Safe 'DS_CFG';
$cpt2->rdo($ds_cfg_path) or
    S4P::perish(1, "Cannot read config file $ds_cfg_path in safe mode: $!\n");

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

# Generate the CMR xml header

my ( $dom, $doc );
$dom = XML::LibXML::Document->new( '1.0', 'UTF-8' );
my $root = XML::LibXML::Element->new( 'GranuleMetaDataFile' );
$root->setAttribute( 'xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance' );
$root->setAttribute( 'xsi:noNamespaceSchemaLocation',
                     'https://git.earthdata.nasa.gov/projects/EMFD/repos/echo-schemas/browse/schemas/10.0/Granule.xsd' );
$dom->setDocumentElement( $root );
$doc = $dom->documentElement();

# CMR xml elements

my $DataSetNode = XML::LibXML::Element->new('GranuleMetaDataSet');
my $GranulesNode = XML::LibXML::Element->new('Granules');
my $DelGranulesNode = XML::LibXML::Element->new('DeleteGranules');
#my $BrowseImagesNode = XML::LibXML::Element->new( 'BrowseImages' );
my $GranuleDeletesNode = XML::LibXML::Element->new('GranuleDeletes');
#my $BrowseDeletesNode = XML::LibXML::Element->new( 'BrowseImageDeletes' );

# Generate the CMR browse xml header

#my ( $browseDom, $browseDoc, $temporalNode );
#$browseDom = XML::LibXML::Document->new( '1.0', 'UTF-8' );
#$root = XML::LibXML::Element->new( 'BrowseMetaDataFile' );
#$root->setAttribute( 'xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance' );
#$root->setAttribute( 'xsi:noNamespaceSchemaLocation',
#                     'http://www.echo.nasa.gov/ingest/schemas/operations/Browse.xsd' );
#$browseDom->setDocumentElement( $root );
#$browseDoc = $browseDom->documentElement();

my $cmrPassword = Clavis::decrypt( $CFG::CMR_PASSWORD )
    if ( defined $CFG::CMR_PASSWORD );
my $cmrToken = login($CFG::CMR_USERNAME, $cmrPassword, $CFG::CMR_PROVIDER);

# Loop for every file in the PDR directory
my @processed_pdr;
#my @processed_browse;
my @unpublishable_pdr;
#my $processed_browse_count;
my $processed_fg_count;

PDR:
foreach my $pdrfile ( @files ) {
    chomp( $pdrfile );

    # remove log files and skip files that are not PDR files
    unlink("$pdrdir/$pdrfile") if ($pdrfile =~ /\.log$/);
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
        my $datatype      = $fg->data_type();
        my $dataversion   = $fg->data_version();
        my $meta_file     = $fg->met_file();
        my @science_files = $fg->science_files();
        my $browsePath    = $fg->browse_file();

        # Set versionless dataset flag
        my $versionless;
        if ( exists $DS_CFG::cmr_collection_id{$datatype}{$dataversion} ) {
            $versionless = ( $dataversion eq '' ) ? 1 : 0;
        } else {
            # no corresponding version in configuration,
            # this fileGroup is probably for the versionless collection
            $versionless = 1;
            # re-assign version to be null to match configuration
            $dataversion = '';
        }

        my $s4paAccessType;
        # my $cmrAccessType;
        my $cmrShortname;
        my $cmrVersion;
        # When inserting, skip file groups whose S4PA access type is hidden or
        # whose S4PA access type does not match the CMR access type
        if ( $CFG::TYPE eq 'insert' ) {
            # When inserting, %CFG::CMR_ACCESS will not be defined unless
            # s4pa_get_cmr_access.pl has added it to the configuration file.
            # If it is not defined, that indicates that s4pa_get_cmr_access.pl
            # has not been run, and therefore we can't tell which file groups
            # are really meant to be skipped, so we will exit with an error.
            #
            # disable CMR access type checking
            # S4P::perish( 9, "No %CMR_ACCESS found in $opt_c" )
            #     unless %CFG::CMR_ACCESS;

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

            # get CMR Shortname and Version from s4pa_dif_info.cfg file
            $cmrShortname = $DS_CFG::cmr_collection_id{$datatype}{$dataversion}{'short_name'};
            $cmrVersion = $DS_CFG::cmr_collection_id{$datatype}{$dataversion}{'version_id'};

            # disable CMR access type checking
            # if ( defined $CFG::CMR_ACCESS{$datatype}{$dataversion} ) {
            #     $cmrAccessType = $CFG::CMR_ACCESS{$datatype}{$dataversion};
            #     if ( $s4paAccessType ne $cmrAccessType ) {
            #         S4P::logger('INFO', "Datatype $datatype Version '$dataversion'" .
            #             " is $s4paAccessType in S4PA and Version" .
            #             " '$cmrVersion' is $cmrAccessType in CMR");
            #         $logger->debug( "Skipped unmatched access type on datatype $datatype " .
            #             "version $dataversion" ) if defined $logger;
            #         next;
            #     }
            # } else {
            #     my $message = "Datatype $datatype Version '$dataversion'" .
            #         " is not defined in CMR. Please execute s4pa_get_cmr_access tool " .
            #         " or check CMR dataset ACL to populate CMR_ACCESS hash";
            #     S4P::logger('ERROR', $message);
            #     $logger->error( $message ) if defined $logger;
            #     S4P::raise_anomaly( "MISSING_CMR_ACCESS", $stationDir,
            #         'WARN', $message, 0 );
            #     next;
            # }
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

        # skip if datatype is not defined or no science files
        # this is probably a collection metadata
        unless (defined $datatype) {
            my $message = "No datatype defined in file: $meta_file, skipped";
            S4P::logger('ERROR', $message);
            $logger->error($message) if defined $logger;
            next;
        }

        unless (@science_files) {
            my $message = "No science file in metadata: $meta_file, skipped";
            S4P::logger('ERROR', $message);
            $logger->error($message) if defined $logger;
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

        # skip publishing on qualified PSA or XPATH setting
        if ( $CFG::TYPE eq 'insert' ) {
            # only continue checking if PSA or XPATH setting exist
            if ((@CFG::cfg_psa_skip) or (%CFG::cfg_xpath_skip)) {
                my $publishMsg = CheckSkipping($meta_file);
                # skip this granule when it has qualified PSA or XPATH
                if ( $publishMsg =~ /^Skip/ ) {
                    $logger->warn( "$publishMsg for $meta_file" ) if defined $logger;
                    next;
                }
            }
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
        if ( defined $CFG::CMR_OPENDAP{$datatype}{$dataversion} ) {
            $opendapInfo = $CFG::CMR_OPENDAP{$datatype}{$dataversion};
        } elsif ( defined $CFG::CMR_OPENDAP{$datatype}{''} ) {
            $opendapInfo = $CFG::CMR_OPENDAP{$datatype}{''};
        }

        # Convert S4PA xml to CMR xml using the xsl stylesheet specified
        # by $opt_x
        my $cmr_string;
        $cmr_string = convert_file( $meta_file, $opt_x, $opendapInfo, @science_files );
        $cmr_string =~ s#<Shortname>(.+)</Shortname>#<Shortname>$cmrShortname</Shortname>#;
        $cmr_string =~ s#<VersionId>(.+)</VersionId>#<VersionId>$cmrVersion</VersionId>#;

        # Parse the converted xml file
        my $fg_parser = XML::LibXML->new();
        my $fg_dom = $fg_parser->parse_string( $cmr_string );
        my $fg_doc = $fg_dom->documentElement();

        if ( $CFG::TYPE eq 'insert' ) {
            # Extract the Granule node from the converted
            # file and add it to a list of granules to be inserted
            my $fg_nodes;
            $fg_nodes = $fg_doc->findnodes( '//Granule' );
            my $GranuleNode = $fg_nodes->get_node(1);

            if ( $browsePath ) {
                # Get browse file URL
                my $url = S4PA::Storage::GetRelativeUrl( $browsePath );
                S4P::perish( 6, "Failed to get relative URL for $browsePath" )
                      unless defined $url;
                my $browseFile = basename( $browsePath );
                $url = $url . '/' . $browseFile;
                $url =~ s/\/+/\//g;
                if ( $fs->mode() & 004 ) {
                    $url =~ s#^/## if ($CFG::UNRESTRICTED_ROOTURL =~ m#/$#);
                    $url = $CFG::UNRESTRICTED_ROOTURL . $url;
                } else {
                    $url =~ s#^/## if ($CFG::RESTRICTED_ROOTURL =~ m#/$#);
                    $url = $CFG::RESTRICTED_ROOTURL . $url;
                }
                my $urlNode = XML::LibXML::Element->new( 'URL' );
                $urlNode->appendText( $url );

                # Get browse file size
                my $fs = stat( $browsePath );
                my $sizeNode = XML::LibXML::Element->new( 'FileSize' );
                $sizeNode->appendText( $fs->size );

                # Get the optional BROWSE_DESCRIPTION from the configuration
                # file, leave it blank if not found.
                # my $descNode;
                # my $browseDescription = "";
                # if ( defined $CFG::BROWSE_DESCRIPTION{$datatype}{$dataversion} ) {
                #     $browseDescription = $CFG::BROWSE_DESCRIPTION{$datatype}{$dataversion};
                # } elsif ( defined $CFG::BROWSE_DESCRIPTION{$datatype}{''} ) {
                #     $browseDescription = $CFG::BROWSE_DESCRIPTION{$datatype}{''};
                # } else {
                #      S4P::logger( 'INFO', "BrowseDescription not found" .
                #                   " for Datatype $datatype Version $dataversion" );
                # }

                # Use current GranuleID for browse image description
                # Get the data granule ID, which can be decoded from current GranuleUR
                # <GranuleUR> is <Shortname>.<Version>:<GranuleID>
                my $descNode;
                my ($id_node) = $fg_doc->findnodes('//Granule/GranuleUR');
                my $granuleID = $id_node->textContent();
                $granuleID =~ s/^(.*):(.*)$/$2/;
                my $browseDescription = "";
                if ($granuleID) {
                    $browseDescription = "Browse image for $granuleID";
                } else {
                    S4P::logger( 'WARN', "BrowseDescription not found" .
                                 " for Datatype $datatype Version $dataversion" );
                }

                if ($browseDescription) {
                    $descNode = XML::LibXML::Element->new( 'Description' );
                    $descNode->appendText( $browseDescription );
                }

                my $abiuNode = XML::LibXML::Element->new( 'AssociatedBrowseImageUrls' );
                my $pbNode = XML::LibXML::Element->new( 'ProviderBrowseUrl' );
                $pbNode->appendChild( $urlNode );
                $pbNode->appendChild( $sizeNode );
                $pbNode->appendChild( $descNode ) if $descNode;
                # If we knew the mime type of the browse file, we would
                # append it to $pbNode here.
                $abiuNode->appendChild( $pbNode );

                # Replace AssociatedBrowseImages node with
                # AssociatedBrowseImageUrls node. We only have to do this
                # because the stylesheet populates the AssociatedBrowseImages
                # node. If it didn't we could append the
                # AssociatedImageBrowseUrls to the end of the granule.
                my ($abiNode) = $GranuleNode->findnodes( 'AssociatedBrowseImages' );
                if ( $abiNode ) {
                    $abiNode->replaceNode( $abiuNode );
                } else {
                    $GranuleNode->appendChild( $abiuNode );
                }
            }

            $GranulesNode->appendChild($GranuleNode);
            $logger->info( "Added $meta_file for cmr publishing" )
                if defined $logger;
            $processed_fg_count++;
        }
        elsif ( $CFG::TYPE eq 'delete' ) {
#            my $delete_time = sprintf("%04d\-%02d\-%02d %02d\:%02d\:%02d",
#                              $date[5]+1900, $date[4]+1, $date[3],
#                              $date[2], $date[1], $date[0]);
            # Extract the Granule node from the converted
            # file and add a GranuleDelete and DeleteTime node to a list of
            # granules to be deleted
            my ($GranuleURNode) = $fg_doc->findnodes('//GranuleUR');
            my $GranuleDeleteNode = XML::LibXML::Element->new('GranuleDelete');
            $GranuleDeleteNode->appendChild($GranuleURNode);
            $GranuleDeletesNode->appendChild($GranuleDeleteNode);
            $logger->info( "Added $meta_file for cmr deletion" )
                if defined $logger;
            $processed_fg_count++;
        }
        else {
            S4P::logger('INFO', "Type: $CFG::TYPE not supported");
        }
    }  # END foreach my $fg (@{$pdr->file_groups})

    # Maintain separate lists of PDRs that contain publishable granules
    # and PDRs that contain unpublishable granules
    if ( $processed_fg_count ) {
        push @processed_pdr, $pdrfile_fullpath;
    } else {
        push @unpublishable_pdr, $pdrfile_fullpath;
    }
}  # END foreach my $pdrfile ( @files )

my $pdrCount = scalar( @processed_pdr );

# Determine elements of work order file names, append the granules node
# to the dataset node
my $wo;
if ( $CFG::TYPE eq 'insert' ) {
    $wo = 'CmrIns';
    $doc->appendChild( $GranulesNode );
}
elsif ( $CFG::TYPE eq 'delete' ) {
    $wo = 'CmrDel';
    $doc->appendChild( $GranuleDeletesNode );
}
else {
    S4P::logger('INFO', "Type: $CFG::TYPE not supported, skip PDR");
}

# Create work order for granule files

my $purgeFlag = 1;  # set flag to delete PDR files
my @madeGranuleXML = ();
#my @madeBrowseXML = ();
my @madeGranuleWO = ();
#my @madeBrowseWO = ();
print "has $pdrCount pdrs\n";
my $xpath;
if ( $pdrCount ) {
    # Find granule nodes
    $xpath = ($CFG::TYPE eq 'insert') ?
             '/GranuleMetaDataFile/Granules/Granule'
             : ($CFG::TYPE eq 'delete') ?
             '/GranuleMetaDataFile/GranuleDeletes/GranuleDelete'
             : undef;
    my @outGranuleNodeList = defined $xpath ? $doc->findnodes($xpath) : ();

    my $ct = scalar @outGranuleNodeList;
    print "has $ct granules in list\n";

    my $fileCounter = 0;
#    $xpath = ($CFG::TYPE eq 'insert')
#        ? '/GranuleMetaDataFile/Granules'
#            : ($CFG::TYPE eq 'delete') ?
#                '/GranuleMetaDataFile/GranuleDeletes': undef;
    while (scalar @outGranuleNodeList > 0) {
#        my ($node) = $doc->findnodes($xpath) if defined $xpath;
#        $node->removeChildNodes();
        my $length = (scalar @outGranuleNodeList > $granMax)
           ? $granMax : scalar @outGranuleNodeList;
#        for (my $i=0; $i<$length; $i++) {
#            $node->appendChild($outGranuleNodeList[$i]);
#        }
#        splice(@outGranuleNodeList, 0, $length);
        my (@gnodes) = splice(@outGranuleNodeList, 0, $length);

        # Write CMR granule metadata file to staging directory $opt_s
        # Include instance name in file name
#        @date = localtime(time);
#        my $cmr_xml = sprintf("%s.%s.T%04d%02d%02d%02d%02d%02d_%d.xml", $wo, $CFG::INSTANCE_NAME,
#            $date[5]+1900, $date[4]+1, $date[3], $date[2], $date[1], $date[0], $fileCounter);
#        my $cmr_path = "$opt_s/$cmr_xml";
#        unless ( open (OUTFILE, "> $cmr_path") ) {
#            S4P::logger('ERROR', "Failed to open $cmr_path ($!)");
#            $purgeFlag = 0;
#        }
#        print OUTFILE $dom->toString(1);
#        unless ( close (OUTFILE) ) {
#            S4P::logger('ERROR', "Failed to close $cmr_path ($!)");
#            $purgeFlag = 0;
#        }
#
#        S4P::logger( "INFO", "$cmr_path created for $length granules" )
#            if ( -f "$cmr_path" );
#
#        if ( -f "$cmr_path" ) {
#            push @madeGranuleXML, $cmr_path;
#            S4P::logger( "INFO",
#                "$cmr_path created for publishing $length granules" );
#            $logger->info( "Created $cmr_path for publishing $length granules" )
#                if defined $logger;
#        }

        # Write work order for postoffice station for the delivery of
        # granule xml
        @date = localtime(time);
#        my $wo_type = 'PUSH';
        my $wo_type = ($CFG::TYPE eq 'insert') ? 'PUT'
                      : ($CFG::TYPE eq 'delete') ? 'DELETE'
                      : undef;
        my $wo_file = sprintf("%s.%s.T%04d%02d%02d%02d%02d%02d_%d.wo",
                              $wo_type, $wo,
                              $date[5]+1900, $date[4]+1, $date[3], $date[2],
                              $date[1], $date[0], $fileCounter);
        my $status = create_rest_wo( $wo_file, $cmrToken, $CFG::TYPE, \@gnodes );
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
#    foreach my $deleteFile (@madeBrowseXML) {
#        unlink $deleteFile;
#    }
#    foreach my $deleteFile (@madeBrowseWO) {
#        unlink $deleteFile;
#    }
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

    # Transform S4PA xml to CMR xml using a stylesheet
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
                        "$indent2    <Type>GET DATA : OPENDAP DATA</Type>\n" .
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

sub create_rest_wo {
    my ($wo, $cmrToken, $type, $nodes) = @_;

    # Write a work order to submit a REST request
    my $parser = XML::LibXML->new();
    my $woDom = $parser->parse_string('<RestPackets/>');
    my $woDoc = $woDom->documentElement();
    $woDoc->setAttribute('status', "I");

    my $destinationBase = $CFG::CMR_ENDPOINT_URI . 'ingest/providers/' .
                          $CFG::CMR_PROVIDER . '/granules/';

    foreach my $node (@$nodes) {
        my ($GranuleURNode) = $node->findnodes('./GranuleUR');
        my $granuleUR = $GranuleURNode->textContent if $GranuleURNode;
        next unless defined $granuleUR;

        my $restPacketNode = XML::LibXML::Element->new('RestPacket');
        $restPacketNode->setAttribute('status', "I");
        my $destination = $destinationBase . $granuleUR;
        $restPacketNode->setAttribute('destination', $destination);
        if ($cmrToken) {
            my $headerNode = XML::LibXML::Element->new('HTTPheader');
            $headerNode->appendText("Echo-Token: $cmrToken");
            $restPacketNode->appendChild($headerNode);
        }

        if ($type eq 'insert') {
            my $headerNode = XML::LibXML::Element->new('HTTPheader');
            if ($CFG::CMR_ENDPOINT_URI =~ /cmr/i) {
                $headerNode->appendText("Content-Type: application/echo10+xml");
            } else {
                $headerNode->appendText("Content-Type: application/xml");
            }
            $restPacketNode->appendChild($headerNode);
            my $payloadNode = XML::LibXML::Element->new('Payload');
            $payloadNode->appendChild($node);
            $restPacketNode->appendChild($payloadNode);
        }
        $woDoc->appendChild($restPacketNode);
    }

    unless ( open (WO, "> $wo") ) {
        S4P::logger( "ERROR", "Failed to open work order $wo ($!)");
        return 0;
    }
    print WO $woDom->toString(1);
    unless ( close WO ) {
        S4P::logger( "ERROR", "Failed to close work order $wo ($!)" );
        return 0;
    }
    return(1);
}

sub login {

    my ($username, $pwd, $provider) = @_;

    my $hostname = Sys::Hostname::hostname();
    my $packed_ip_address = (gethostbyname($hostname))[4];
    my $ip_address = join('.', unpack('C4', $packed_ip_address));

    my $tokenNode = XML::LibXML::Element->new('token');
    $tokenNode->appendTextChild('username', $username);
    $tokenNode->appendTextChild('password', $pwd);
    $tokenNode->appendTextChild('client_id', 'GES_DISC');
    $tokenNode->appendTextChild('user_ip_address', $ip_address);
    $tokenNode->appendTextChild('provider', $provider);

    my $id;
    my $tokenUrl = $CFG::CMR_TOKEN_URI . 'tokens';
    my $request = HTTP::Request->new( 'POST',
                                      $tokenUrl,
                                      [Content_Type => 'application/xml'],
                                      $tokenNode->toString() );
    my $ua = LWP::UserAgent->new;
    my $response = $ua->request($request);
    if ($response->is_success) {
        my $xml = $response->content;
        my $tokenDom;
        my $xmlParser = XML::LibXML->new();
        eval {$tokenDom = $xmlParser->parse_string($xml); };
        S4P::perish( 2, "Could not parse response from CMR token request:  $@\n" ) if $@;
        my $tokenDoc = $tokenDom->documentElement();
        my ($idNode) = $tokenDoc->findnodes('/token/id');
        $id = $idNode->textContent();
    } else {
        S4P::perish( 2, "Could not connect to CMR API for login; check if CMR API is down.\nReason:  $@\n" );
    }

    return $id;
}

sub CheckSkipping {
    my ($met_file) = @_;

    my $parser = XML::LibXML->new();
    $parser->keep_blanks(0);
    my $dom = $parser->parse_file($met_file);
    my $doc = $dom->documentElement();

    my $publishFlag = 1;
    if ( @CFG::cfg_psa_skip ) {
        $publishFlag = VerifyPSA( $doc );
        unless ( $publishFlag ) {
            S4P::logger( "WARN", "Matching skip PSA in $met_file, "
                       . "skipping this granule." );
            return 'Skip publishing on matching PSA';
        }
    }

    # collection specific XPATH skip setting
    if ( %CFG::cfg_xpath_skip ) {
        my $shortname = GetNodeValue($doc, '//CollectionMetaData/ShortName');
        my $versionid = GetNodeValue($doc, '//CollectionMetaData/VersionID');
        my $xpath;
        if ( exists $CFG::cfg_xpath_skip{$shortname}{$versionid} ) {
            $xpath = $CFG::cfg_xpath_skip{$shortname}{$versionid};
        # versionless collection
        } elsif ( exists $CFG::cfg_xpath_skip{$shortname}{''} ) {
            $xpath = $CFG::cfg_xpath_skip{$shortname}{''};
        }
        if ( defined $xpath ) {
            $publishFlag = VerifyXpathValue( $met_file, $doc, $xpath );
            unless ( $publishFlag ) {
                return 'Skip publishing on qualified XPATH';
            }
        }
    }
    return $publishFlag;
}

sub GetNodeValue {
    my ($root, $xpath) = @_;
    my ($node) = ($xpath ? $root->findnodes($xpath) : $root);
    return undef unless defined $node;
    my $val = $node->textContent();
    $val =~ s/^\s+|\s+$//g;
    return $val;
}

sub VerifyPSA {
    my ($doc) = @_;

    my $returnFlag = 1;
    # default PSA xpath should be the following in granule metadata
    my $xpath = '//PSAs/PSA';
    foreach my $psaNode ( $doc->findnodes($xpath) ) {
        my $psaName = GetNodeValue($psaNode, 'PSAName');
        my $psaValue = GetNodeValue($psaNode, 'PSAValue');
        foreach my $psa ( @CFG::cfg_psa_skip ) {
            my $skipName = $psa->{'PSAName'};
            my $skipValue = $psa->{'PSAValue'};

            # both PSAName and PSAValue match with the skip setting,
            # skip this granule
            if ( ($skipName eq $psaName) && ($skipValue eq $psaValue) ) {
                $returnFlag = 0;
                return $returnFlag;
            }
        }
    }
    return $returnFlag;
}

sub VerifyXpathValue {
    my ($met_file, $doc, $xpath_list) = @_;

    my $returnFlag = 1;
    foreach my $operator ( keys %$xpath_list ) {
        foreach my $xpath ( keys %{$xpath_list->{$operator}} ) {
            my ( $xpathNode ) = $doc->findnodes($xpath);
            unless (defined $xpathNode) {
                S4P::logger( "WARN", "XPATH $xpath in $met_file is not defined, "
                    . "skipping this granule." );
                $returnFlag = 0;
                return $returnFlag;
            }

            my $targetVal = $xpath_list->{$operator}{$xpath};
            # only verify the existence of the xpath if target value is empty
            # and make sure the node value is empty too
            if ( $targetVal eq '' ) {
                my $value = GetNodeValue($xpathNode);
                if ($value) {
                    return $returnFlag;
                } else {
                    S4P::logger( "WARN", "XPATH $xpath in $met_file is not defined, "
                        . "skipping this granule." );
                    $returnFlag = 0;
                    return $returnFlag;
                }
            }

            my $granuleVal = $doc->findvalue( $xpath );
            # Make sure the input and the output are of the same type
            my $targetType = S4PA::IsNumber( $targetVal );
            my $granuleType = S4PA::IsNumber( $granuleVal );
            S4P::logger( "WARN",
                "Values of $xpath in $met_file is not the same type "
                 . "as the target value specified in configuration!" )
                unless ( $targetType == $granuleType );

            # Reset replace flag if any of the specified attributes don't match.
            my $cmpFlag = ( $granuleType != $targetType )
                ? $granuleVal cmp $targetVal
                : ( $granuleType
                    ? $granuleVal <=> $targetVal
                    : $granuleVal cmp $targetVal );

            if ( $operator eq 'EQ' ) {
                $returnFlag = 0 if ( $cmpFlag == 0 );
            } elsif ( $operator eq 'NE' ) {
                $returnFlag = 0 if ( $cmpFlag != 0 );
            } elsif ( $operator eq 'LT' ) {
                $returnFlag = 0 if ( $cmpFlag == -1 );
            } elsif ( $operator eq 'LE' ) {
                $returnFlag = 0 if ( $cmpFlag <= 0 );
            } elsif ( $operator eq 'GT' ) {
                $returnFlag = 0 if ( $cmpFlag == 1 );
            } elsif ( $operator eq 'GE' ) {
                $returnFlag = 0 if ( $cmpFlag >= 0 );
            } else {
                S4P::perish( 1,
                    "Operator specified for $xpath, $operator, not"
                    . " supported" );
            }
            unless ( $returnFlag ) {
                S4P::logger( "WARN",
                    "Values of $xpath in $met_file is '$operator' " .
                    "the target value specified in configuration, " .
                    "skipping this granule." );
                return $returnFlag;
            }
        }
    }
    return $returnFlag;
}

################  end of s4pa_publish_cmr  ################
