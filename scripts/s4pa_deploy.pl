#!/usr/bin/perl

=head1 NAME

s4pa_deploy - script to deploy S4PA instance based on a descriptor file.

=head1 SYNOPSIS

s4pa_deploy.pl
[B<-f> I<descriptor_file>]
[B<-s> I<schema_file>]

=head1 DESCRIPTION

s4pa_deploy.pl creates necessary station directories, configuration files and
symbolic links based on a descriptor file.

=head1 ARGUMENTS

=over 4

=item B<-f> I<descriptor_file>

Descriptor file describes datasets handled by S4PA instance and its attributes
such as compression/uncompression methods, access type etc.,

=item B<-s> I<schema_file>

Schema file defines the S4PA instance descriptor's requirement.

=item B<-i> I<instance_name>

Define the instance name for locating descriptor file in cvs 
S4PA_CONFIG repository.  If specified, deployment will be based on the descriptor,
subscription configuration, and metadata templates from the cvs.

=item B<-v> I<s4pa_release_version>

Define the S4PA release label for schema and stylesheets. If not specified,
the release label in the current script will be used if available. 
If no release lable found in current script, current cvs version will be used.

=item B<-p> I<project_name>

Optional cvs project name for metadata template.

=item B<-r> I<project_release>

Optional cvs project release. If not specified, current version of the
project in cvs will be used.

=item B<-x>

Skip all XSL stylesheets installation. If not specified, all available stylesheets
will get copied to its destination.

=head1 AUTHOR

M. Hegde, SSAI

=cut

################################################################################
# $Id: s4pa_deploy.pl,v 1.322 2020/05/26 19:20:26 s4pa Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use Getopt::Std;
use File::Basename;
use File::Temp qw(tempdir);
use File::Copy;
use Data::Dumper;
use XML::LibXML;
use S4PA;
use S4PA::Storage;
use LWP::Simple;
use Safe;
use Log::Log4perl;
use Clavis;
use JSON;
use LWP::UserAgent;
use HTTP::Request;

my ( $opt ) = {};

# Get command line arguments
getopts( "f:s:i:v:p:r:dx", $opt );

if ( $opt->{i} ) {
    my %argConfig;
    $argConfig{INSTANCE} = $opt->{i};
    $argConfig{S4PA_VERSION} = ( $opt->{v} ) ?
        $opt->{v} : '';
    $argConfig{PROJECT} = ( $opt->{p} ) ?
        $opt->{p} : '';
    $argConfig{PROJECT_RELEASE} = ( $opt->{r} ) ?
        $opt->{r} : '';
    $argConfig{SKIP_XSL} = ( $opt->{x} ) ?
        '-x' : '';
    my ( $deployCmd, $updateCmd ) = PrepareConfig( %argConfig );
    print "\n\nDeploying with \'$deployCmd\'\n";
    `$deployCmd`;
    if ( defined $updateCmd ) {
        print "\n\nUpdate subscription with \'$updateCmd\'\n";
        `$updateCmd`;
    } else {
        print "\n\nNo subscription update.\n";
    }
    exit;
}
Usage() unless ( $opt->{f} && $opt->{s} );

# Create an XML DOM parser.
my $xmlParser = XML::LibXML->new();
$xmlParser->keep_blanks(0);

# Parse the descriptor file.
my $dom = $xmlParser->parse_file( $opt->{f} );
my $doc = $dom->documentElement();

# Validate using the specified schema
my $schema = XML::LibXML::Schema->new( location => $opt->{s} );
die "Failed to read XML schema, $opt->{s}" unless $schema;

eval { $schema->validate( $dom ); };
die "Failed to validate $opt->{f} with $opt->{s}\n$@" if $@;

# Set the umask so that all directories/files are writable by S4PA user.
umask 0022;

# Get global parameters
my $global = GetGlobalParameters( $doc );

# Message logging
my $logger;
my $msg;
if ( defined $global->{LOGGER} ) {
    $logger = S4PA::CreateLogger( $global->{LOGGER}{FILE},
        $global->{LOGGER}{LEVEL} );
    $global->{LOGGER}{LOGGING} = $logger;
    $logger->info( "Deployment with $0 -f $opt->{f} -s $opt->{s}" );
}

my ( @stationDirList );

# Create temporary directory under s4paRoot
my $tmpDir = $global->{S4PA_ROOT} . "tmp";
DeployLogging( 'error', "Failed to create $tmpDir" )
    unless S4PA::CreateDir( $tmpDir, $logger );

# Create Polling stations
$global->{MERGE_PAN} = 0;
push( @stationDirList, CreatePollingStation( 'PDR', $doc, $global ) );
push( @stationDirList, CreatePollingStation( 'DATA', $doc, $global ) );
push( @stationDirList, CreateMergePanStation( $doc, $global ) )
    if ( $global->{MERGE_PAN} );

# Remove existing storage/dataset.cfg if exist.
unlink ( "$global->{S4PA_ROOT}/storage/dataset.cfg" )
    if ( -f "$global->{S4PA_ROOT}/storage/dataset.cfg" );

# Create Receiving and Storage station
foreach my $provider ( $doc->findnodes( 'provider' ) ) {
    push( @stationDirList, CreateReceiveDataStation( $provider, $global ) );
    push( @stationDirList, CreateStorageStations( $provider, $global ) );
}

# Create Associate DB file if necessary
CreateAssociateDb( $global );

push( @stationDirList, CreateSubscribeStation( $doc, $global ) );

push( @stationDirList, CreatePublishEchoStations( $doc, $global ) )
    if ( $global->{PUBLISH_ECHO} );
push( @stationDirList, CreatePublishCmrStations( $doc, $global ) )
    if ( $global->{PUBLISH_CMR} );
push( @stationDirList, CreatePublishWhomStations( $doc, $global ) )
    if ( $global->{PUBLISH_WHOM} );
push( @stationDirList, CreatePublishMiradorStations( $doc, $global ) )
    if ( $global->{PUBLISH_MIRADOR} );
push( @stationDirList, CreatePublishGiovanniStations( $doc, $global ) )
    if ( $global->{PUBLISH_GIOVANNI} );
push( @stationDirList, CreatePublishDotchartStations( $doc, $global ) )
    if ( $global->{PUBLISH_DOTCHART} );
push( @stationDirList, CreatePublishUserStations( $doc, $global ) )
    if ( $global->{PUBLISH_USER} );

push( @stationDirList, CreateGiovanniStation( $doc, $global ) );
push( @stationDirList, CreatePostOfficeStation( $doc, $global ) );

if ( $global->{PUBLISH_CMR} || $global->{PUBLISH_DOTCHART} ) {
    my $reconStation = CreateReconciliationStation( $doc, $global );
    push( @stationDirList, $reconStation ) if (defined $reconStation);
}

# Create DIF fetcher and House keeping stations
push( @stationDirList, CreateOtherStations( $doc, $global ) );


# backup curent stattion.list file
my $stationListFile = "$global->{S4PA_ROOT}/station.list";
my $timestamp = `date +%Y%m%d%H%M`;
chomp($timestamp);
if (-f $stationListFile) {
    my $oldStationList = $stationListFile . ".$timestamp";
    if (copy($stationListFile, $oldStationList)) {
        print "Backup current station.list to $oldStationList\n";
    }
}

# Write station list file
if ( open( FH, ">$stationListFile" ) ) {
    foreach my $stationDir ( @stationDirList ) {
        print FH $stationDir, "\n" if defined $stationDir;
    }
    unless ( close( FH ) ) {
        my $msg = "Failed to close $stationListFile ($!)";
        DeployLogging( 'error', $msg );
    }
} else {
    my $msg = "Failed to open $stationListFile ($!)";
    DeployLogging( 'error', $msg );
}

# Write config file for use with tkstat.pl 
my $config = {
    'tkstat_commands' => {
        'Start All Stations' => "s4pa_controller.pl -a startAll "
            . $stationListFile,
        'Stop All Stations' => "s4pa_controller.pl -a stopAll $stationListFile",
#         'Check Instance Integrity' => "s4pa_check_instance_integrity.pl"
#             . " -r $global->{S4PA_ROOT} -d /var/tmp",
        'Delete Granule' => qq( perl -e 'use S4PA; S4PA::DeleteGranule(ROOT => "$global->{S4PA_ROOT}", TITLE => "Delete Granule")' )
        },
    '__TYPE__' => {
        tkstat_commands => 'HASH',
        }
    };    
S4PA::WriteStationConfig( 's4pa_gui.cfg', $global->{S4PA_ROOT}, $config );

# Install defined metadata template and XSL stylesheets.
my $skipXSL = ( defined $opt->{x} ) ? 1 : 0;
CopyConfigFiles( $doc, $global, $skipXSL );

# publish descriptor to dotchart if configured
if ( $global->{PUBLISH_DOTCHART} ) {
    my $workOrder = PublishDescriptor( $doc, $global );
    DeployLogging( 'info', "work order " . basename($workOrder) .
        " created for publishing descriptor." ) if ( defined $workOrder );
} else {
    DeployLogging( 'info', "Skipped publish descriptor: " .
        "No dotChart defined in instance descriptor." );
}

if ( defined $logger ) {
    if ( Log::Log4perl::NDC->get() eq '[undef]' ) {
        DeployLogging( 'info', "Deployment completed without error." );
    } else {
        print STDERR "\n##########################################\n" .
            "Error/Warning Messages recorded during deployment:\n";
        while ( my $stack = Log::Log4perl::NDC->pop() ) {
            print STDERR "$stack\n";
        }
        DeployLogging( 'info', "Deployment completed with error." );
    }
    print STDERR "Please refer to $global->{LOGGER}{FILE} for detailed logging.\n";
}

################################################################################
# =head1 CreateOtherStations
# 
# Description
#   Creates DIF fetcher, House keeper stations.
#
# =cut
################################################################################
sub CreateOtherStations
{
    my ( $doc, $global ) = @_;
    my ( $config, $cmdHash, $downStream, $virtualJobs ) = ( {}, {}, {}, {} );
    my ( @stationDirList ) = ();
    my $logger = $global->{LOGGER}{LOGGING} if defined $global->{LOGGER}{LOGGING};
    # Failure handler to retry a failed job.
    my $failureHandler = { 'Retry Job' => "perl -e 'use S4P; S4P::remove_job()"
                            . " if S4P::restart_job()'",
                           'Remove Job' => "perl -e 'use S4P; S4P::remove_job()'"
                         };
    my $datasetToDifMap = {};
    my $cmrCollectionID = {};
    my $echoVisible = {};
    my $cmrVisible = {};
    my %existCmrCollection;
    my $transientArchive = {}; 
    my $datasetDocUrl = {};

    my $cpt = new Safe 'CLASS';
    DeployLogging( 'fatal', "Failed to read dataset->data class mapping ($!)" )
        unless $cpt->rdo( "$global->{S4PA_ROOT}/storage/dataset.cfg" );

    # some instances does not have CMR/ECHO interface at all.
    # so, only check if instance actually publish to CMR.
    if (-f "$global->{S4PA_ROOT}/other/housekeeper/s4pa_dif_info.cfg") {
        my $cmrInfo = new Safe 'CMRINFO';
        DeployLogging( 'warn', "Failed to read echo visibility mapping ($!)" )
            unless $cmrInfo->rdo( "$global->{S4PA_ROOT}/other/housekeeper/s4pa_dif_info.cfg" );

        # assign current visibility from configuration file
        foreach my $dataset ( keys %CMRINFO::echo_visible ) {
            foreach my $version ( keys %{$CMRINFO::echo_visible{$dataset}} ) {
                $echoVisible->{$dataset}{$version} = $CMRINFO::echo_visible{$dataset}{$version};
            }
        }

        # assign current visibility from configuration file
        foreach my $dataset ( keys %CMRINFO::cmr_visible ) {
            foreach my $version ( keys %{$CMRINFO::cmr_visible{$dataset}} ) {
                $cmrVisible->{$dataset}{$version} = $CMRINFO::cmr_visible{$dataset}{$version};
            }
        }

        # save current CMR collection IDs
        foreach my $dataset ( keys %CMRINFO::cmr_collection_id ) {
            foreach my $version ( keys %{$CMRINFO::cmr_collection_id{$dataset}} ) {
                $existCmrCollection{$dataset}{$version}{'entry_id'} = 
                    $CMRINFO::cmr_collection_id{$dataset}{$version}{'entry_id'}
                    if ( exists $CMRINFO::cmr_collection_id{$dataset}{$version}{'entry_id'} );
                $existCmrCollection{$dataset}{$version}{'native_id'} = 
                    $CMRINFO::cmr_collection_id{$dataset}{$version}{'native_id'}
                    if ( exists $CMRINFO::cmr_collection_id{$dataset}{$version}{'native_id'} );
                $existCmrCollection{$dataset}{$version}{'concept_id'} = 
                    $CMRINFO::cmr_collection_id{$dataset}{$version}{'concept_id'}
                    if ( exists $CMRINFO::cmr_collection_id{$dataset}{$version}{'concept_id'} );
                $existCmrCollection{$dataset}{$version}{'revision_id'} = 
                    $CMRINFO::cmr_collection_id{$dataset}{$version}{'revision_id'}
                    if ( exists $CMRINFO::cmr_collection_id{$dataset}{$version}{'revision_id'} );
            }
        }
    }

    foreach my $dataset ( $doc->findnodes( '//dataClass/dataset' ) ) {
        my $datasetAttr = GetDataAttributes( $dataset,
            GetDataAttributes( $dataset->parentNode() ) );
        $datasetAttr->{PUBLISH_DOTCHART} = 
            ( $global->{PUBLISH_DOTCHART} ) ? 'true' : 'false';
        my $dataClass = $CLASS::data_class{$datasetAttr->{NAME}};
        my $downStreamDeleteDir = "storage/$dataClass/"
            . "delete_$dataClass/intra_version_pending";

        my @dataVersionList = $dataset->getChildrenByTagName( 'dataVersion' );
        
        if ( @dataVersionList ) {
            foreach my $dataVersion ( @dataVersionList ) {
                my $dataVersionAttr
                    = GetDataAttributes( $dataVersion, $datasetAttr );
                $dataVersionAttr->{PUBLISH_DOTCHART} = 
                    ( $global->{PUBLISH_DOTCHART} ) ? 'true' : 'false';
                my ( $dataName, $versionLabel, $docName, $difId ) = (
                    $datasetAttr->{NAME},
                    $dataVersionAttr->{LABEL},
                    $dataVersionAttr->{DOC},
                    $dataVersionAttr->{DIF_ENTRY_ID} );
                $datasetToDifMap->{$dataName}{$versionLabel} = $difId
                    if ( defined $difId && ($dataVersionAttr->{ACCESS} ne 'hidden') );

                # assign CMR collection id map
                my $fetchFlag = 0;
                my ( $collectionShortname, $collectionVersion );

                if ( defined $dataVersionAttr->{COLLECTION_SHORTNAME} ) {
                    # set collection fetching flag if collection_shortname is specified
                    $collectionShortname = $dataVersionAttr->{COLLECTION_SHORTNAME};
                    $fetchFlag = 1;
                } else {
                    # otherwise, use dataset name
                    $collectionShortname = $dataName;
                }

                if ( defined $dataVersionAttr->{COLLECTION_VERSION} ) {
                    # set collection fetching flag if collection_version is specified
                    $collectionVersion = $dataVersionAttr->{COLLECTION_VERSION};
                    $fetchFlag = 1;
                } else {
                    # otherwise, use dataset version label
                    $collectionVersion = $versionLabel;
                }

                if ( $fetchFlag ) {
                    # make sure collection_version is specified for versionless dataset
                    DeployLogging( 'fatal', "COLLECTION_VERSION not specified for versionless dataset: $dataName.")
                        if ( $versionLabel eq '' && $collectionVersion eq '' );
                    $cmrCollectionID->{$dataName}{$versionLabel}{'short_name'} = $collectionShortname;
                    $cmrCollectionID->{$dataName}{$versionLabel}{'version_id'} = $collectionVersion;
                    $cmrCollectionID->{$dataName}{$versionLabel}{'entry_id'} =
                        $existCmrCollection{$dataName}{$versionLabel}{'entry_id'}
                        if ( exists $existCmrCollection{$dataName}{$versionLabel}{'entry_id'} );
                    $cmrCollectionID->{$dataName}{$versionLabel}{'native_id'} =
                        $existCmrCollection{$dataName}{$versionLabel}{'native_id'}
                        if ( exists $existCmrCollection{$dataName}{$versionLabel}{'native_id'} );
                    $cmrCollectionID->{$dataName}{$versionLabel}{'concept_id'} =
                        $existCmrCollection{$dataName}{$versionLabel}{'concept_id'}
                        if ( exists $existCmrCollection{$dataName}{$versionLabel}{'concept_id'} );
                    $cmrCollectionID->{$dataName}{$versionLabel}{'revision_id'} =
                        $existCmrCollection{$dataName}{$versionLabel}{'revision_id'}
                        if ( exists $existCmrCollection{$dataName}{$versionLabel}{'revision_id'} );
                } else {
                    # make sure we need to fetch collection metadata if publishing to CMR
                    DeployLogging( 'error', "No collection metadata fetching flag was set for " .
                        "$dataName.$versionLabel while publishing to CMR" )
                        if ( $dataVersionAttr->{PUBLISH_CMR} eq 'true' );

                    # make sure we need to fetch collection metadata if publishing to Mirador 
                    DeployLogging( 'error', "No collection metadata fetching flag was set for " .
                        "$dataName.$versionLabel while publishing to Mirador" )
                        if ( $dataVersionAttr->{PUBLISH_CMR} eq 'true' && $dataVersionAttr->{ACCESS} eq 'public' );
                }

                # set default echo visibility to be '1' unless it was already
                # set in the s4pa_dif_info.cfg configuration file, ticket #10096.
                if ( $dataVersionAttr->{PUBLISH_ECHO} eq 'true' ) {
                    $echoVisible->{$dataName}{$versionLabel} = 1
                        unless ( defined $echoVisible->{$dataName}{$versionLabel} );
                }
                if ( $dataVersionAttr->{PUBLISH_CMR} eq 'true' ) {
                    $cmrVisible->{$dataName}{$versionLabel} = 1
                        unless ( defined $cmrVisible->{$dataName}{$versionLabel} );
                }

                if ( $dataVersionAttr->{EXPIRY} ) {
                    $transientArchive->{$dataName}{$versionLabel} = {
                        'startTime' => '1900-01-01 00:00:00',
                        'expirationDays' => $dataVersionAttr->{EXPIRY}
                    };
                    my $key = 'INTRA_VERSION_DELETE_'
                        . $dataName;
                    $key .= '_' . $versionLabel if ( $versionLabel ne '' );

                    if ( $dataVersionAttr->{PUBLISH_DOTCHART} eq 'true' ) {
                        $downStream->{$key} = [ 'publish_dotchart/pending_delete' ];
                    } else {
                        $downStream->{$key} = [ $downStreamDeleteDir ];
                    }
                    push( @{$downStream->{$key}},
                        'publish_whom/pending_delete' )
                        if ( $dataVersionAttr->{PUBLISH_WHOM} eq 'true' );
                    push( @{$downStream->{$key}},
                        'publish_echo/pending_delete' )
                        if ( $dataVersionAttr->{PUBLISH_ECHO} eq 'true' );
                    push( @{$downStream->{$key}},
                        'publish_cmr/pending_delete' )
                        if ( $dataVersionAttr->{PUBLISH_CMR} eq 'true' );
                    push( @{$downStream->{$key}},
                        'publish_mirador/pending_delete' )
                        if ( $dataVersionAttr->{PUBLISH_MIRADOR} eq 'true' );
                    push( @{$downStream->{$key}},
                        'publish_giovanni/pending_delete' )
                        if ( $dataVersionAttr->{PUBLISH_GIOVANNI} eq 'true' );
                }
                if ($docName) {
                    my $dataRootUrl = $global->{URL}{HTTP};
                    if ($datasetAttr->{ACCESS} eq 'public') {
                        $dataRootUrl = $global->{URL}{FTP};
                    }
                    $dataRootUrl .= '/' unless ( $dataRootUrl =~ /\/$/ );
                    if ($versionLabel) {
                        $dataRootUrl .= "$datasetAttr->{GROUP}/$dataName\.$versionLabel/doc";
                    } else {
                        $dataRootUrl .= "$datasetAttr->{GROUP}/$dataName/doc";
                    }
                    $datasetDocUrl->{$dataName}{$versionLabel} = $dataRootUrl .
                       "/$docName";
                }
            }
        } else {
            $datasetToDifMap->{$datasetAttr->{NAME}}{""}
                = $datasetAttr->{DIF_ENTRY_ID}
                if ( defined $datasetAttr->{DIF_ENTRY_ID}
                    && ($datasetAttr->{ACCESS} ne 'hidden') );
            if ( $datasetAttr->{PUBLISH_ECHO} eq 'true' ) {
                $echoVisible->{$datasetAttr->{NAME}}{""} = 1
                    unless ( defined $echoVisible->{$datasetAttr->{NAME}}{""} );
            }

            if ( $datasetAttr->{PUBLISH_CMR} eq 'true' ) {
                # we should not end up here since a versionless dataset publishing to CMR
                # need to have collection_version specified under dataVersion
                DeployLogging( 'fatal', "No collection_version was specified for the versionless " .
                    $datasetAttr->{NAME} . ".");
            }

            if ( $datasetAttr->{PUBLISH_MIRADOR} eq 'true' && $datasetAttr->{ACCESS} eq 'public' ) {
                # we should not end up here since a versionless dataset publishing to Mirador
                # need to have collection_version specified under dataVersion
                DeployLogging( 'fatal', "No collection_version was specified for the versionless " .
                    $datasetAttr->{NAME} . ".");
            }

            if ( defined $datasetAttr->{EXPIRY} ) {
                $transientArchive->{$datasetAttr->{NAME}}{''} = {
                    'startTime' => '1900-01-01 00:00:00',
                    'expirationDays' => $datasetAttr->{EXPIRY}
                };
                my $key = 'INTRA_VERSION_DELETE_' . $datasetAttr->{NAME};
                if ( $datasetAttr->{PUBLISH_DOTCHART} eq 'true' ) {
                    $downStream->{$key} = [ 'publish_dotchart/pending_delete' ];
                } else {
                    $downStream->{$key} = [ $downStreamDeleteDir ];
                }
                push( @{$downStream->{$key}}, 'publish_dotchart/pending_delete' )
                    if ( $datasetAttr->{PUBLISH_DOTCHART} eq 'true' );
                push( @{$downStream->{$key}}, 'publish_whom/pending_delete' )
                    if ( $datasetAttr->{PUBLISH_WHOM} eq 'true' );
                push( @{$downStream->{$key}}, 'publish_echo/pending_delete' )
                    if ( $datasetAttr->{PUBLISH_ECHO} eq 'true' );
                push( @{$downStream->{$key}}, 'publish_cmr/pending_delete' )
                    if ( $datasetAttr->{PUBLISH_CMR} eq 'true' );
                push( @{$downStream->{$key}}, 'publish_mirador/pending_delete' )
                    if ( $datasetAttr->{PUBLISH_MIRADOR} eq 'true' );
                push( @{$downStream->{$key}}, 'publish_giovanni/pending_delete' )
                    if ( $datasetAttr->{PUBLISH_GIOVANNI} eq 'true' );
            }
            if (defined $datasetAttr->{DOC}) {
                my $dataRootUrl = $global->{URL}{HTTP};
                if ($datasetAttr->{ACCESS} eq 'public') {
                    $dataRootUrl = $global->{URL}{FTP};
                }
                $dataRootUrl .= '/' unless ( $dataRootUrl =~ /\/$/ );
                $dataRootUrl .= "$datasetAttr->{GROUP}/$datasetAttr->{NAME}/doc";
                $datasetDocUrl->{$datasetAttr->{NAME}}{''} = $dataRootUrl .
                   "/$datasetAttr->{DOC}";
            }
        }
    }
    my $houseKeeperStationDir = $global->{S4PA_ROOT} . "other/housekeeper";
    my $archiveWatcherStationDir = $global->{S4PA_ROOT}
        . "other/transientArchive";
    my $machineSearchStationDir = $global->{S4PA_ROOT} . "other/machine_search";
    
    if ( keys %$transientArchive ) {
        # TransientArchive station
        $cmdHash = {
            SEARCH => 
                "s4pa_transient_archive.pl -f ../s4pa_transient_archive.cfg"
        };
        $virtualJobs = { SEARCH => 1 };
        $config = {
            cfg_station_name => "TransientArchive",
            cfg_root => $global->{S4PA_ROOT},
            cfg_group => $global->{S4PA_GID},
            cfg_max_failures => 1,
            cfg_max_time => 600,
            cfg_polling_interval => 86400,
            cfg_stop_interval => 4,
            cfg_end_job_interval => 2,
            cfg_restart_defunct_jobs => 1,
            cfg_sort_jobs => 'FIFO',
            cfg_failure_handlers => $failureHandler,
            cfg_commands => $cmdHash,
            cfg_downstream => $downStream,
            cfg_ignore_duplicates => 1,
	    cfg_output_work_order_suffix => 'PDR',
            cfg_umask => 022,
            cfg_virtual_jobs => $virtualJobs,
            __TYPE__ => {
                cfg_failure_handlers => 'HASH',
                cfg_commands => 'HASH',
                cfg_downstream => 'HASH',
                cfg_virtual_jobs => 'HASH',
                }
            };
        # Add an interface to edit station config
        $config->{cfg_interfaces}{'Edit Station Config'} =
          qq( perl -e 'use S4PA; S4PA::EditFile( FILE => "$archiveWatcherStationDir/station.cfg", TITLE => "TransientArchive" )' );
        # Add an interface to remove Stale log
        $config->{cfg_interfaces}{'Remove Stale Log'} =
          qq( perl -e 'use S4PA; S4PA::RemoveStaleLog( STATION => "$archiveWatcherStationDir", TITLE => "TransientArchive" )' );
        # Add an interface for retrying all failed jobs
        $config->{cfg_interfaces}{'Retry Failed Jobs'} = qq(s4p_restart_all_jobs.pl);
        $config->{__TYPE__}{cfg_interfaces} = 'HASH';

        S4PA::CreateStation( $archiveWatcherStationDir, $config, $logger );
        push( @stationDirList, $archiveWatcherStationDir);
        
        $config = {
            cfg_s4pa_root => $global->{S4PA_ROOT},
            cfg_max_granule_count => 1000,
            cfg_transient_dataset => $transientArchive,
            cfg_publish_dotchart => $global->{PUBLISH_DOTCHART},
            __TYPE__ => {
                cfg_transient_dataset => 'HASH'
            }
        };
        S4PA::WriteStationConfig( 's4pa_transient_archive.cfg',
            $archiveWatcherStationDir, $config );
    }    
    
    # MachineSearch station
    $cmdHash = {
       SEARCH => "s4pa_m2m_search.pl "
    };
    $downStream = { SEARCH => [ 'subscribe/pending' ] };
    $config = {
        cfg_max_children => 1,
        cfg_station_name => "MachineSearch",
        cfg_root => $global->{S4PA_ROOT},
        cfg_group => $global->{S4PA_GID},
        cfg_max_failures => 1,
        cfg_max_time => 600,
        cfg_polling_interval => 10,
        cfg_stop_interval => 4,
        cfg_end_job_interval => 2,
        cfg_restart_defunct_jobs => 1,
        cfg_sort_jobs => 'FIFO',
        cfg_failure_handlers => $failureHandler,
        cfg_commands => $cmdHash,
        cfg_downstream => $downStream,
        cfg_ignore_duplicates => 1,
	cfg_output_work_order_suffix => 'PDR',
        cfg_umask => 022,
        __TYPE__ => {
            cfg_failure_handlers => 'HASH',
            cfg_commands => 'HASH',
            cfg_downstream => 'HASH',
            }
        };
        # Add an interface to edit station config
        $config->{cfg_interfaces}{'Edit Station Config'} =
          qq( perl -e 'use S4PA; S4PA::EditFile( FILE => "$machineSearchStationDir/station.cfg", TITLE => "MachineSearch" )' );
        # Add an interface to remove stale log
        $config->{cfg_interfaces}{'Remove Stale Log'} =
          qq( perl -e 'use S4PA; S4PA::RemoveStaleLog( STATION => "$machineSearchStationDir", TITLE => "MachineSearch" )' );
        # Add an interface for retrying all failed jobs
        $config->{cfg_interfaces}{'Retry Failed Jobs'} = qq(s4p_restart_all_jobs.pl);
        $config->{__TYPE__}{cfg_interfaces} = 'HASH';


    S4PA::CreateStation( $machineSearchStationDir, $config, $logger );
    chmod 0777, $machineSearchStationDir;
    push( @stationDirList, $machineSearchStationDir);

    # if ( keys %$datasetToDifMap ) {                            
    if ( keys %$cmrCollectionID ) {                            
        # Create DIF fetcher station
        my $difFetcherStationDir = $global->{S4PA_ROOT} . "other/dif_fetcher";
        $cmdHash = {
            FETCH_DIF => "s4pa_fetch_revised_DIFs.pl -c ../s4pa_dif_info.cfg",
            };
        
        # flag fatal message if any required DIF fetching info is missing
        DeployLogging( 'fatal', "DIF Fetching URI is not defined" )
            if (not defined $global->{DIFFETCHER}{ENDPOINT_URI});
        DeployLogging( 'fatal', "DIF Fetching TOKEN URI is not defined" )
            if (not defined $global->{DIFFETCHER}{TOKEN_URI});
        DeployLogging( 'fatal', "DIF Fetching PROVIDER is not defined" )
            if (not defined $global->{DIFFETCHER}{PROVIDER});
        DeployLogging( 'fatal', "DIF Fetching USERNAME or CERT_FILE is not defined" )
            if (not defined $global->{DIFFETCHER}{CERT_FILE} and not defined $global->{DIFFETCHER}{USERNAME});
        DeployLogging( 'fatal', "DIF Fetching encrypted PASSWORD or CERT_PASS is not defined" )
            if (not defined $global->{DIFFETCHER}{CERT_PASS} and not defined $global->{DIFFETCHER}{PASSWORD});

        $virtualJobs = { FETCH_DIF => 1 };
        $downStream = { CONVERT_DIF => [ 'other/housekeeper' ] };
        $config = {
            cfg_max_children => 1,
            cfg_station_name => "CmrDifFetcher",
            cfg_root => $global->{S4PA_ROOT},
            cfg_group => $global->{S4PA_GID},
            cfg_max_failures => 1,
            cfg_max_time => 600,
            cfg_polling_interval => 86400,
            cfg_stop_interval => 4,
            cfg_end_job_interval => 2,
            cfg_restart_defunct_jobs => 1,
	    cfg_sort_jobs => 'FIFO',
            cfg_failure_handlers => $failureHandler,
            cfg_commands => $cmdHash,
            cfg_downstream => $downStream,
            cfg_virtual_jobs => $virtualJobs,
            cfg_virtual_feedback => 1,
            cfg_ignore_duplicates => 1,
	    cfg_umask => 022,
            __TYPE__ => {
                cfg_failure_handlers => 'HASH',
                cfg_commands => 'HASH',
                cfg_downstream => 'HASH',
                cfg_virtual_jobs => 'HASH',
                }
            };

        # Add an interface to manually fetching DIF now, ticket #6580.
        $config->{cfg_interfaces}{'Fetch DIF now'} =
            qq( perl -e 'use S4PA; S4PA::DifFetching( STATION => "$difFetcherStationDir", TITLE => "CmrDifFetcher" )' );
        # Add an interface to edit station config
        $config->{cfg_interfaces}{'Edit Station Config'} =
          qq( perl -e 'use S4PA; S4PA::EditFile( FILE => "$difFetcherStationDir/station.cfg", TITLE => "CmrDifFetcher" )' );
        # Add an interface to remove stale log
        $config->{cfg_interfaces}{'Remove Stale Log'} =
          qq( perl -e 'use S4PA; S4PA::RemoveStaleLog( STATION => "$difFetcherStationDir", TITLE => "CmrDifFetcher" )' );
        # Add an interface for retrying all failed jobs
        $config->{cfg_interfaces}{'Retry Failed Jobs'} = qq(s4p_restart_all_jobs.pl);
        $config->{__TYPE__}{cfg_interfaces} = 'HASH';

        S4PA::CreateStation( $difFetcherStationDir, $config, $logger );
        push( @stationDirList, $difFetcherStationDir );

        my $cmr_rest_base = $global->{DIFFETCHER}{ENDPOINT_URI};
        my $cmr_token_base = $global->{DIFFETCHER}{TOKEN_URI};
        my $cmr_provider = $global->{DIFFETCHER}{PROVIDER};
        my $cmr_username = $global->{DIFFETCHER}{USERNAME} if (defined $global->{DIFFETCHER}{USERNAME});
        my $cmr_password = $global->{DIFFETCHER}{PASSWORD} if (defined $global->{DIFFETCHER}{PASSWORD});
        my $cmr_certfile = $global->{DIFFETCHER}{CERT_FILE} if (defined $global->{DIFFETCHER}{CERT_FILE});
        my $cmr_certpass = $global->{DIFFETCHER}{CERT_PASS} if (defined $global->{DIFFETCHER}{CERT_PASS});

        my $destination = {};
        if ( $global->{PUBLISH_ECHO} ) {
            my $echoHost = $global->{ECHO}{COLLECTION}{HOST};
            my $echoDir = $global->{ECHO}{COLLECTION}{DIR};
            DeployLogging( 'fatal', "Host for ECHO collections not found" )
                unless defined $echoHost;
            DeployLogging( 'fatal', "Directory for ECHO collections not found" )
                unless defined $echoDir;
            unless ( defined $global->{PROTOCOL}{$echoHost} ) {
                $global->{PROTOCOL}{$echoHost} = 'FTP';
                DeployLogging( 'info', "Protocol for $echoHost not found; using FTP" );
            }
            $destination->{ECHO} = lc($global->{PROTOCOL}{$echoHost}) 
                . ":$echoHost$echoDir";
        }

        if ( $global->{PUBLISH_MIRADOR} ) {
            my $miradorHost = $global->{MIRADOR}{DOCUMENT}{HOST};
            my $miradorDir = $global->{MIRADOR}{DOCUMENT}{DIR};
            DeployLogging( 'fatal', "Host for Mirador collections not found" )
                unless defined $miradorHost;
            DeployLogging( 'fatal', "Directory for Mirador collections not found" )
                unless defined $miradorDir;
            unless ( defined $global->{PROTOCOL}{$miradorHost} ) {    
                $global->{PROTOCOL}{$miradorHost} = 'FTP';
                DeployLogging( 'info', "Protocol for $miradorHost not found; using FTP" );
            }
            $destination->{MIRADOR} = lc($global->{PROTOCOL}{$miradorHost})
                . ":$miradorHost$miradorDir";
        }

        if ( $global->{PUBLISH_DOTCHART} ) {
            my $dotChartHost = $global->{DOTCHART}{COLLECTION}{HOST};
            my $dotChartDir = $global->{DOTCHART}{COLLECTION}{DIR} ;
            $destination->{EMS} = lc($global->{PROTOCOL}{$dotChartHost})
                . ":$dotChartHost$dotChartDir";
        }

        $config = {
            dataset_to_dif_entry_id => $datasetToDifMap,
            cmr_collection_id => $cmrCollectionID,
            echo_visible => $echoVisible,
            cmr_visible => $cmrVisible,
            dataset_doc_url => $datasetDocUrl,
            ECHO_XSLFILE => "../S4paDIF2ECHO.xsl",
            S4PA_XSLFILE => "../S4paDIF102Collect.xsl",
            CMR_PROVIDER => $cmr_provider,
            CMR_ENDPOINT_URI => $cmr_rest_base,
            TMPDIR => "$global->{S4PA_ROOT}/tmp",
            destination => $destination,
            max_fetch_attempts => 3,
            sleep_seconds => 1,
            S4PA_ROOT => $global->{S4PA_ROOT},
            cfg_publish_dotchart => $global->{PUBLISH_DOTCHART},
            __TYPE__ => {
                dataset_to_dif_entry_id => 'HASH',
                cmr_collection_id => 'HASH',
                echo_visible => 'HASH',
                cmr_visible => 'HASH',
                dataset_doc_url => 'HASH',
                destination => 'HASH',
                MIRADOR_TARGET_XFORMS => 'HASH',
                }
            };

        $config->{ECHO_XSLFILE} = "../S4paDIF2ECHO10.xsl"
            if ( $global->{ECHO}{VERSION} > 9 );

        # either ECHO token with username/password
        # or Launchpad token with certificate file/password
        if (defined $cmr_certfile) {
            $config->{LAUNCHPAD_URI} = $cmr_token_base;
            $config->{CMR_CERTFILE} = $cmr_certfile;
            $config->{CMR_CERTPASS} = $cmr_certpass;
        } else {
            $config->{CMR_TOKEN_URI} = $cmr_token_base;
            $config->{CMR_USERNAME} = $cmr_username;
            $config->{CMR_PASSWORD} = $cmr_password;
        }

        S4PA::WriteStationConfig( 's4pa_dif_info.cfg', $difFetcherStationDir,
            $config );
        S4PA::WriteStationConfig( 's4pa_dif_info.cfg', $houseKeeperStationDir,
            $config );
        $cmdHash = {
            CONVERT_DIF => "s4pa_convert_DIF.pl -c ../s4pa_dif_info.cfg",
            };
    }
    # Create HouseKeeper station directory if absent
    S4PA::CreateDir( $houseKeeperStationDir, $logger )
        unless ( -d $houseKeeperStationDir );
    # Create a House keeping station for running routine jobs: DIF conversion,
    # active file system monitoring   
    $cmdHash->{ACTIVE_FS_MONITOR} = 
            "s4pa_active_fs_monitor.pl -f ../s4pa_active_fs.cfg -o ../active_fs.history";
    $virtualJobs = { ACTIVE_FS_MONITOR => 1 };
    
    # Add PAN monitors for EDOS pollers
    foreach my $poller (
        $doc->findnodes( '//poller/pdrPoller/job[@TYPE="EDOS"]' ) ) {
        my $pollerName = $poller->getAttribute( 'NAME' );
        my $pdrDir = $poller->getAttribute( 'DIR' );
        my $panDir = GetNodeValue( $poller, '../../../pan/local' );
        next unless ( defined $panDir && defined $pdrDir 
            && defined $pollerName );
        my $jobType = "EDOS_PAN_MONITOR_$pollerName";
        $virtualJobs->{$jobType} = 1;
        $cmdHash->{$jobType} =
            "s4pa_pdr_cleanup.pl -p $pdrDir -a $panDir -r 86400 -e";
    }

    my ( $node ) =  $doc->findnodes( '//publication/dotChart/dbExport' );
    if ( defined $node ) {
        my $host = $node->getAttribute( 'HOST' );
        my $dir = $node->getAttribute( 'DIR' );
        $host = $node->parentNode()->getAttribute( 'HOST' )
            unless ( defined $host );
        DeployLogging( 'fatal', "Failed to find host for DB export" )
            unless defined $host;
        my $interval = $node->getAttribute( 'INTERVAL' ) || 86400;
        my $protocol = lc( $global->{PROTOCOL}{$host} || 'FTP' );
        my $dbExportConfig = {
            cfg_root => $global->{S4PA_ROOT},
            cfg_instance => $global->{S4PA_NAME},
            cfg_interval => $interval,
            cfg_destination => "$protocol:$host/$dir",
        };
        S4PA::WriteStationConfig( 's4pa_db_export.cfg', $houseKeeperStationDir,
            $dbExportConfig );
        $cmdHash->{"DB_EXPORT"} = "s4pa_db_export.pl -f ../s4pa_db_export.cfg";
        $virtualJobs->{"DB_EXPORT"} = 1;
    }
    $downStream = { PUSH => [ 'postoffice' ] }; 

    # Add customized house keeping jobs, ticket #10228.
    my @houseKeeperJobs = $doc->findnodes( '//houseKeeper/job' );
    foreach my $job ( @houseKeeperJobs ) {
        my $jobType = $job->getAttribute( 'NAME' );
        $cmdHash->{$jobType} = GetNodeValue( $job );
        $virtualJobs->{$jobType} = 1;
        my $jobDownStream = $job->getAttribute( 'DOWNSTREAM' );
        $downStream->{$jobType} = [ "$jobDownStream" ] if ( defined $jobDownStream );
    }

    # Add zombies monitoring job
    my ($zombie) = $doc->findnodes('//houseKeeper/zombie');
    if (defined $zombie) {
        my $action = $zombie->getAttribute('ACTION') || 'kill,notify';
        my $interval = $zombie->getAttribute('INTERVAL') || '3600';
        my $zombieConfig = {
            cfg_root => $global->{S4PA_ROOT},
            cfg_action => $action,
            cfg_interval => $interval
        };
        my $notify = $zombie->getAttribute('NOTIFY');
        $zombieConfig->{'cfg_notify'} = $notify if (defined $notify);

        S4PA::WriteStationConfig('s4pa_zombie_monitor.cfg', $houseKeeperStationDir,
            $zombieConfig);
        $cmdHash->{"ZOMBIE_MONITOR"} = "s4pa_zombie_monitor.pl -f ../s4pa_zombie_monitor.cfg";
        $virtualJobs->{"ZOMBIE_MONITOR"} = 1;
    }

    $config = {
        cfg_max_children => 1,
        cfg_station_name => "HouseKeeper",
        cfg_root => $global->{S4PA_ROOT},
        cfg_group => $global->{S4PA_GID},
        cfg_max_time => 60,
	cfg_sort_jobs => 'FIFO',
        cfg_polling_interval => 60,
        cfg_stop_interval => 4,
        cfg_end_job_interval => 2,
        cfg_restart_defunct_jobs => 1,
        cfg_failure_handlers => $failureHandler,
        cfg_commands => $cmdHash,
        cfg_downstream => $downStream,
        cfg_virtual_jobs => $virtualJobs,
        cfg_virtual_feedback => 1,
        cfg_ignore_duplicates => 1,
	cfg_umask => 022,
        __TYPE__ => {
            cfg_failure_handlers => 'HASH',
            cfg_commands => 'HASH',
            cfg_downstream => 'HASH',
            cfg_virtual_jobs => 'HASH',
            }
        };
    # Add an interface to edit station config
    $config->{cfg_interfaces}{'Edit Station Config'} =
      qq( perl -e 'use S4PA; S4PA::EditFile( FILE => "$houseKeeperStationDir/station.cfg", TITLE => "HouseKeeper" )' );
    # Add an interface to remove stale log
    $config->{cfg_interfaces}{'Remove Stale Log'} =
      qq( perl -e 'use S4PA; S4PA::RemoveStaleLog( STATION => "$houseKeeperStationDir", TITLE => "HouseKeeper" )' );
    # Add an interface for retrying all failed jobs
    $config->{cfg_interfaces}{'Retry Failed Jobs'} = qq(s4p_restart_all_jobs.pl);
    $config->{__TYPE__}{cfg_interfaces} = 'HASH';

    S4PA::CreateStation( $houseKeeperStationDir, $config, $logger );
    push( @stationDirList, $houseKeeperStationDir );
    
    my $providerContact = {};
    my $providerThreshold = {};
    my $recycleThreshold = {};
    foreach my $provider ( $doc->findnodes( '/s4pa/provider' ) ) {
        my $providerName = $provider->getAttribute( 'NAME' );
        my ( $activeFsNode )
            = $provider->getChildrenByTagName( 'activeFileSystem' );
        my $notifyOnFull = $activeFsNode->getAttribute( 'NOTIFY_ON_FULL' );
        $providerContact->{$providerName} = $notifyOnFull;

        # default the low volume threshold to be 0.1 (10%).
        my $threshold = $activeFsNode->getAttribute( 'LOW_VOLUME_THRESHOLD' ) || '0.1';
        $providerThreshold->{$providerName} = $threshold;

        my $rvThreshold = $activeFsNode->getAttribute('RECYCLE_VOLUME_THRESHOLD');
        if (defined $rvThreshold) {
            $recycleThreshold->{$providerName} = $rvThreshold;
        }
    }
    $config = {
        cfg_s4pa_root => $global->{S4PA_ROOT},
        cfg_provider_contact => $providerContact,
        cfg_provider_threshold => $providerThreshold,
        __TYPE__ => {
            cfg_provider_contact => 'HASH',
            cfg_provider_threshold => 'HASH',
            } 
        };
    if ($recycleThreshold) {
        $config->{cfg_recycle_threshold} = $recycleThreshold;
        $config->{__TYPE__}{cfg_recycle_threshold} = 'HASH';
    }
    S4PA::WriteStationConfig( 's4pa_active_fs.cfg', $houseKeeperStationDir,
        $config );
    return @stationDirList;
}

################################################################################
# =head1 CreateSubscribeStation
# 
# Description
#   Creates subscription station.
#
# =cut
################################################################################
sub CreateSubscribeStation
{
    my ( $doc, $global ) = @_;
    my ( $config, $cmdHash, $downStream, $virtualJobs ) = ( {}, {}, {}, {} );
    my $logger = $global->{LOGGER}{LOGGING} if defined $global->{LOGGER}{LOGGING};

    # Failure handler to retry a failed job.
    my $failureHandler = { 'Retry Job' => "perl -e 'use S4P; S4P::remove_job()"
                            . " if S4P::restart_job()'" };
                                
    # Create subscribe station
    my $subscribeStationDir = $global->{S4PA_ROOT} . "subscribe/";
    $cmdHash = {
        SUBSCRIBE => "s4pa_subscribe.pl -f ../s4pa_subscription.cfg"
            . " -d ../pending",
        };
    $downStream = { EMAIL => [ 'postoffice/' ] };
    $virtualJobs = { SUBSCRIBE => 1 };    
    $config = {
        cfg_max_children => 1,
        cfg_station_name => "SubscribeData",
        cfg_root => $global->{S4PA_ROOT},
        cfg_group => $global->{S4PA_GID},
	cfg_sort_jobs => 'FIFO',
        cfg_max_failures => 1,
        cfg_max_time => 600,
        cfg_polling_interval => $global->{SUBSCRIPTION}{INTERVAL},
        cfg_stop_interval => 4,
        cfg_end_job_interval => 2,
        cfg_restart_defunct_jobs => 1,
        cfg_failure_handlers => $failureHandler,
        cfg_commands => $cmdHash,
        cfg_downstream => $downStream,
        cfg_virtual_jobs => $virtualJobs,
        cfg_virtual_feedback => 1,
        cfg_ignore_duplicates => 1,
	cfg_umask => 022,
        __TYPE__ => {
            cfg_failure_handlers => 'HASH',
            cfg_commands => 'HASH',
            cfg_downstream => 'HASH',
            cfg_virtual_jobs => 'HASH',
            }
        };
    # Add an interface to edit station config
    $config->{cfg_interfaces}{'Edit Station Config'} =
      qq( perl -e 'use S4PA; S4PA::EditFile( FILE => "$subscribeStationDir/station.cfg", TITLE => "SubscribeData" )' );
    # Add an interface to remove stale log
    $config->{cfg_interfaces}{'Remove Stale Log'} =
      qq( perl -e 'use S4PA; S4PA::RemoveStaleLog( STATION => "$subscribeStationDir", TITLE => "SubscribeData" )' );
    # Add an interface for retrying all failed jobs
    $config->{cfg_interfaces}{'Retry Failed Jobs'} = qq(s4p_restart_all_jobs.pl);
    $config->{__TYPE__}{cfg_interfaces} = 'HASH';

    S4PA::CreateStation( $subscribeStationDir, $config, $logger );
    return $subscribeStationDir;
}
################################################################################
# =head1 CreatePublishWhomStations
# 
# Description
#   Creates WHOM publication stations that are used in 
#   publishing metadata for ingested data to search clients.
#
# =cut
################################################################################
sub CreatePublishWhomStations
{
    my ( $doc, $global ) = @_;
    my ( $config, $cmdHash, $virtualJobs, $downStream ) = ( {}, {}, {}, {} );
    my $logger = $global->{LOGGER}{LOGGING} if defined $global->{LOGGER}{LOGGING};

    # Failure handler to retry a failed job.
    my $failureHandler = { 'Retry Job' => "perl -e 'use S4P; S4P::remove_job()"
                            . " if S4P::restart_job()'" };
                                
    # Holder of station directories
    my @stationDirList = ();

    my $tmpDir = $global->{S4PA_ROOT}. "tmp";   

    # Master log file
    my $logFile = defined $global->{LOGGER}
        ? "$global->{LOGGER}{DIR}/publish.log" : undef;

    # Create WHOM publication    
    my $pubWhomStationDir = $global->{S4PA_ROOT} . "publish_whom/";
    $cmdHash = {
        PUBLISH_WHOM => "s4pa_publish_whom.pl -f ../s4pa_publish_whom.cfg"
            . " -w ../pending_publish",
        DELETE_WHOM => "s4pa_whom_dfa_from_pdr.pl -f ../s4pa_publish_whom.cfg"
            . " -w ../pending_delete"
        };
    $virtualJobs = { 
        DELETE_WHOM => 1,
        PUBLISH_WHOM => 1
        };    
    $config = {
        cfg_max_children => 2,
        cfg_station_name => "PublishWhom",
        cfg_root => $global->{S4PA_ROOT},
        cfg_group => $global->{S4PA_GID},
        cfg_max_failures => 1,
        cfg_max_time => 600,
	cfg_sort_jobs => 'FIFO',
        cfg_polling_interval => 3600,
        cfg_stop_interval => 4,
        cfg_end_job_interval => 2,
        cfg_restart_defunct_jobs => 1,
        cfg_failure_handlers => $failureHandler,
        cfg_commands => $cmdHash,
        cfg_virtual_jobs => $virtualJobs,
        cfg_virtual_feedback => 1,
        cfg_ignore_duplicates => 1,
	cfg_umask => 022,
        cfg_downstream => { 'PUSH' => [ 'postoffice' ] },
        __TYPE__ => {
            cfg_failure_handlers => 'HASH',
            cfg_commands => 'HASH',
            cfg_virtual_jobs => 'HASH',
            cfg_downstream => 'HASH',
            }
        };
    # Add an interface to edit station config
    $config->{cfg_interfaces}{'Edit Station Config'} =
      qq( perl -e 'use S4PA; S4PA::EditFile( FILE => "$pubWhomStationDir/station.cfg", TITLE => "PublishWhom" )' );
    # Add an interface to remove stale log
    $config->{cfg_interfaces}{'Remove Stale Log'} =
      qq( perl -e 'use S4PA; S4PA::RemoveStaleLog( STATION => "$pubWhomStationDir", TITLE => "PublishWhom" )' );
    # Add an interface to republish
    $config->{cfg_interfaces}{'Republish Data'} =
      qq( perl -e 'use S4PA; S4PA::RepublishData( STATION => "$pubWhomStationDir", TITLE => "PublishWhom" )' );
    # Add an interface for retrying all failed jobs
    $config->{cfg_interfaces}{'Retry Failed Jobs'} = qq(s4p_restart_all_jobs.pl);
    $config->{__TYPE__}{cfg_interfaces} = 'HASH';


    S4PA::CreateStation( $pubWhomStationDir, $config, $logger );
    push( @stationDirList, $pubWhomStationDir );
    return @stationDirList;       
}
    
################################################################################
# =head1 CreatePublishEchoStations
# 
# Description
#   Creates ECHO publication stations that are used in 
#   publishing metadata for ingested data to search clients.
#
# =cut
################################################################################
sub CreatePublishEchoStations
{
    my ( $doc, $global ) = @_;
    my ( $config, $cmdHash, $virtualJobs, $downStream ) = ( {}, {}, {}, {} );
    my $logger = $global->{LOGGER}{LOGGING} if defined $global->{LOGGER}{LOGGING};

    # Failure handler to retry a failed job.
    my $failureHandler = { 'Retry Job' => "perl -e 'use S4P; S4P::remove_job()"
                            . " if S4P::restart_job()'" };
                                
    # Holder of station directories
    my @stationDirList = ();

    my $tmpDir = $global->{S4PA_ROOT}. "tmp";   

    # Master log file
    my $logFile = defined $global->{LOGGER}
        ? "$global->{LOGGER}{DIR}/publish.log" : undef;

    # xsl stylesheet depends on echo version
    my ( $xslFile, $xslBrowseFile );
    if ( $global->{ECHO}{VERSION} > 9 ) {
        $xslFile = "S4paGran2ECHO10.xsl";
        $xslBrowseFile = "S4paGran2EchoBrowse10.xsl";
    } else { 
        $xslFile = "S4paGran2ECHO.xsl";
        $xslBrowseFile = "S4paGran2EchoBrowse.xsl";
    }

    # Create ECHO publication 
    my $pubEchoStationDir = $global->{S4PA_ROOT} . "publish_echo/";
    $cmdHash = {
        DELETE_ECHO => "s4pa_publish_echo.pl -c ../s4pa_delete_echo.cfg"
            . " -p ../pending_delete -s $tmpDir -x ../$xslFile"
            . " -b ../$xslBrowseFile",
        PUBLISH_ECHO => "s4pa_publish_echo.pl -c ../s4pa_insert_echo.cfg"
            . " -p ../pending_publish -s $tmpDir -x ../$xslFile"
            . " -b ../$xslBrowseFile -v",
        UPDATE_ECHO_ACCESS => "s4pa_get_echo_access.pl -f ../s4pa_insert_echo.cfg"
        };
    $virtualJobs = { 
        DELETE_ECHO => 1,
        PUBLISH_ECHO => 1,
        };
    $downStream = { 'PUSH' => [ 'postoffice' ] };
    $config = {
        cfg_max_children => 2,
        cfg_station_name => "PublishECHO",
        cfg_root => $global->{S4PA_ROOT},
        cfg_group => $global->{S4PA_GID},
        cfg_max_failures => 1,
        cfg_max_time => 600,
        cfg_polling_interval => 3600,
        cfg_stop_interval => 4,
        cfg_end_job_interval => 2,
        cfg_restart_defunct_jobs => 1,
	cfg_sort_jobs => 'FIFO',
        cfg_failure_handlers => $failureHandler,
        cfg_commands => $cmdHash,
        cfg_virtual_jobs => $virtualJobs,
        cfg_virtual_feedback => 1,
        cfg_ignore_duplicates => 1,
	cfg_umask => 022,
        cfg_downstream => $downStream,
        __TYPE__ => {
            cfg_failure_handlers => 'HASH',
            cfg_commands => 'HASH',
            cfg_virtual_jobs => 'HASH',
            cfg_downstream => 'HASH',
            }
        };
    # Add an interface to edit station config
    $config->{cfg_interfaces}{'Edit Station Config'} =
      qq( perl -e 'use S4PA; S4PA::EditFile( FILE => "$pubEchoStationDir/station.cfg", TITLE => "PublishECHO" )' );
    # Add an interface to remove stale log
    $config->{cfg_interfaces}{'Remove Stale Log'} =
      qq( perl -e 'use S4PA; S4PA::RemoveStaleLog( STATION => "$pubEchoStationDir", TITLE => "PublishECHO" )' );
    # Add an interface to republish
    $config->{cfg_interfaces}{'Republish Data'} =
      qq( perl -e 'use S4PA; S4PA::RepublishData( STATION => "$pubEchoStationDir", TITLE => "PublishECHO" )' );
    # Add an interface to delete dataset, ticket #10096.
    $config->{cfg_interfaces}{'Update Dataset Configuration'} =
      "s4pa_echo_dataset.pl -r $global->{S4PA_ROOT}";
    # Add an interface for retrying all failed jobs
    $config->{cfg_interfaces}{'Retry Failed Jobs'} = qq(s4p_restart_all_jobs.pl);
    $config->{__TYPE__}{cfg_interfaces} = 'HASH';

    S4PA::CreateStation( $pubEchoStationDir, $config, $logger );
    
    # Find out access type for datasets
    my $dataAccess = {};
    foreach my $dataClass ( $doc->findnodes( '//provider/dataClass' ) ) {
        my $classAttr = GetDataAttributes( $dataClass );
        foreach my $dataset ( $dataClass->findnodes( 'dataset' ) ) {
            my $dataAttr = GetDataAttributes( $dataset, $classAttr );
            my $dataName = $dataAttr->{NAME};
            my ( @dataVersionList ) = $dataset->findnodes( 'dataVersion' );
            $dataAccess->{$dataName} = {};
            if ( @dataVersionList ) {
                foreach my $version ( @dataVersionList ) {
                    my $versionId = $version->getAttribute( 'LABEL' );
                    my $versionAttr = GetDataAttributes( $version, $dataAttr );
                    if ( defined $versionAttr->{ACCESS} ) {
                        $dataAccess->{$dataName}{$versionId}
                            = $versionAttr->{ACCESS} || 'public';
                    }
                }
            } else {
                # Case of version-less system.
                $dataAccess->{$dataName}{''} = $dataAttr->{ACCESS} || 'public';
            }
        }
    }
    
    # Obtain OPeNDAP information for datasets
    my $opendapInfo;
    foreach my $dataClass ( $doc->findnodes( '//provider/dataClass' ) ) {
        my $classAttr = GetDataAttributes( $dataClass );
        foreach my $dataset ( $dataClass->findnodes( 'dataset' ) ) {
            my $dataAttr = GetDataAttributes( $dataset, $classAttr );
            my $dataName = $dataAttr->{NAME};
            my ( @dataVersionList ) = $dataset->findnodes( 'dataVersion' );
            if ( @dataVersionList ) {
                foreach my $version ( @dataVersionList ) {
                    my $versionId = $version->getAttribute( 'LABEL' );
                    my $versionAttr = GetDataAttributes( $version, $dataAttr );
                    # skip if this version is not set to publish to ECHO
                    next unless ( $versionAttr->{'PUBLISH_ECHO'} eq 'true' );
                    if ( ( defined $versionAttr->{PUBLISH_ECHO_OPENDAP} ) && ( $versionAttr->{PUBLISH_ECHO_OPENDAP} eq 'true' ) ){
                        if ( defined $versionAttr->{OPENDAP_URL_PREFIX} ) {
                            if ($versionAttr->{OPENDAP_URL_PREFIX}) {
                                $opendapInfo->{$dataName}->{$versionId}->{'OPENDAP_URL_PREFIX'} = $versionAttr->{OPENDAP_URL_PREFIX};
                                $opendapInfo->{$dataName}->{$versionId}->{'OPENDAP_URL_SUFFIX'} = $versionAttr->{OPENDAP_URL_SUFFIX} if (exists $versionAttr->{OPENDAP_URL_SUFFIX});
                                $opendapInfo->{$dataName}->{$versionId}->{'OPENDAP_RESOURCE_URL_SUFFIX'} = $versionAttr->{OPENDAP_RESOURCE_URL_SUFFIX} if (exists $versionAttr->{OPENDAP_RESOURCE_URL_SUFFIX});
                            }
                        }
                    }
                }
            } else {
                # skip if this dataset is not set to publish to ECHO
                next unless ( $dataAttr->{'PUBLISH_ECHO'} eq 'true' );
                # Case of version-less system.
                if ( ( defined $dataAttr->{PUBLISH_ECHO_OPENDAP} ) && ( $dataAttr->{PUBLISH_ECHO_OPENDAP} eq 'true' ) ){
                    if ( defined $dataAttr->{OPENDAP_URL_PREFIX} ) {
                        if ($dataAttr->{OPENDAP_URL_PREFIX}) {
                            $opendapInfo->{$dataName}->{''}->{'OPENDAP_URL_PREFIX'} = $dataAttr->{OPENDAP_URL_PREFIX};
                            $opendapInfo->{$dataName}->{''}->{'OPENDAP_URL_SUFFIX'} = $dataAttr->{OPENDAP_URL_SUFFIX} if (exists $dataAttr->{OPENDAP_URL_SUFFIX});
                            $opendapInfo->{$dataName}->{''}->{'OPENDAP_RESOURCE_URL_SUFFIX'} = $dataAttr->{OPENDAP_RESOURCE_URL_SUFFIX} if (exists $dataAttr->{OPENDAP_RESOURCE_URL_SUFFIX});
                        }
                    }
                }
            }
        }
    }

    # Preserve ECHO_ACCESS hash if exist and consistent with $dataAccess
    my $echoAccess;
    my $insertConfig = $pubEchoStationDir . "s4pa_insert_echo.cfg";
    if (-f $insertConfig) {
        my $cpt = Safe->new( 'CFG' );
        $cpt->share( '%ECHO_ACCESS' );
        if ( $cpt->rdo( "$insertConfig" ) ) {
            if (%CFG::ECHO_ACCESS) {
                foreach my $dataName (keys %CFG::ECHO_ACCESS) {
                    foreach my $versionId (keys %{$CFG::ECHO_ACCESS{$dataName}}) {
                        if ( !($CFG::ECHO_ACCESS{$dataName}{$versionId} eq 'public' &&
                               $dataAccess->{$dataName}{$versionId} ne 'public') ) {
                            $echoAccess->{$dataName}{$versionId} =
                                     $CFG::ECHO_ACCESS{$dataName}{$versionId};
                        }
                    }
                }
            }
        }
    }

    $config = {
        TYPE => 'insert',
        UNRESTRICTED_ROOTURL => $global->{URL}{FTP},
        RESTRICTED_ROOTURL => $global->{URL}{HTTP},
        INSTANCE_NAME => $global->{S4PA_NAME},
        HOST => $global->{ECHO}{GRANULE}{INSERT}{HOST},
        DESTDIR => $global->{ECHO}{GRANULE}{INSERT}{DIR},
        ECHO_USERNAME => $global->{RECON}{ECHO}{USERNAME},
        ECHO_PASSWORD => $global->{RECON}{ECHO}{PASSWORD},
        ECHO_PROVIDER => 'GSFCS4PA',
        ECHO_ENDPOINT_URI => $global->{RECON}{ECHO}{ENDPOINT_URI},
        MAX_GRANULE_COUNT =>
            $global->{ECHO}{GRANULE}{INSERT}{MAX_GRANULE_COUNT},
        DATA_ACCESS => $dataAccess,
        __TYPE__ => {
            DATA_ACCESS => 'HASH'
            }
        };
    $config->{BROWSEDIR} = $global->{ECHO}{BROWSE}{INSERT}{DIR}
        if ( defined $global->{ECHO}{BROWSE}{INSERT}{DIR} );
    if (defined $echoAccess) {
        $config->{ECHO_ACCESS} = $echoAccess;
        $config->{__TYPE__}{ECHO_ACCESS} = 'HASH';
    }
    if (defined $opendapInfo) {
        $config->{ECHO_OPENDAP} = $opendapInfo;
        $config->{__TYPE__}{ECHO_OPENDAP} = 'HASH';
    }
    if ( defined $global->{LOGGER} ) {
        $config->{cfg_logger} = {
            LEVEL => $global->{LOGGER}{LEVEL},
            FILE => $logFile };
        $config->{__TYPE__}{cfg_logger} = 'HASH';
    }
    S4PA::WriteStationConfig( 's4pa_insert_echo.cfg', $pubEchoStationDir,
        $config );
    $config = {
        TYPE => 'delete',
        UNRESTRICTED_ROOTURL => $global->{URL}{FTP},
        RESTRICTED_ROOTURL => $global->{URL}{HTTP},
        INSTANCE_NAME => $global->{S4PA_NAME},
        HOST => $global->{ECHO}{GRANULE}{DELETE}{HOST},
        DESTDIR => $global->{ECHO}{GRANULE}{DELETE}{DIR},
        MAX_GRANULE_COUNT =>
            $global->{ECHO}{GRANULE}{DELETE}{MAX_GRANULE_COUNT},
        }; 
    $config->{BROWSEDIR} = $global->{ECHO}{BROWSE}{DELETE}{DIR}
        if ( defined $global->{ECHO}{BROWSE}{DELETE}{DIR} );
    if ( defined $global->{LOGGER} ) {
        $config->{cfg_logger} = {
            LEVEL => $global->{LOGGER}{LEVEL},
            FILE => $logFile };
        $config->{__TYPE__}{cfg_logger} = 'HASH';
    }
    S4PA::WriteStationConfig( 's4pa_delete_echo.cfg', $pubEchoStationDir,
        $config );
    # create a job for getting ECHO access
    my @items = `ls -a $pubEchoStationDir`;
    my @files = map {/DO\.UPDATE_ECHO_ACCESS/} @items;
    unless (@files) {
        my $time = time;
        `touch $pubEchoStationDir/DO.UPDATE_ECHO_ACCESS.$time.wo`;
    }

    push( @stationDirList, $pubEchoStationDir );
    return @stationDirList;       
}
    
################################################################################
# =head1 CreatePublishCmrStations
# 
# Description
#   Creates CMR publication stations that are used in 
#   publishing metadata for ingested data to search clients.
#
# =cut
################################################################################
sub CreatePublishCmrStations
{
    my ( $doc, $global ) = @_;
    my ( $config, $cmdHash, $virtualJobs, $downStream ) = ( {}, {}, {}, {} );
    my $logger = $global->{LOGGER}{LOGGING} if defined $global->{LOGGER}{LOGGING};

    # Failure handler to retry a failed job.
    my $failureHandler = { 'Retry Job' => "perl -e 'use S4P; S4P::remove_job()"
                            . " if S4P::restart_job()'" };
                                
    # Holder of station directories
    my @stationDirList = ();

    my $tmpDir = $global->{S4PA_ROOT}. "tmp";   

    # Master log file
    my $logFile = defined $global->{LOGGER}
        ? "$global->{LOGGER}{DIR}/publish.log" : undef;

    # xsl stylesheet depends on cmr version
    my ( $xslFile, $xslBrowseFile );
    $xslFile = "S4paGran2CMR.xsl";
    $xslBrowseFile = "S4paGran2CmrBrowse.xsl";

    # Create CMR publication 
    my $pubCmrStationDir = $global->{S4PA_ROOT} . "publish_cmr/";
    $cmdHash = {
        DELETE_CMR => "s4pa_publish_cmr.pl -c ../s4pa_delete_cmr.cfg"
            . " -p ../pending_delete -s $tmpDir -x ../$xslFile",
        PUBLISH_CMR => "s4pa_publish_cmr.pl -c ../s4pa_insert_cmr.cfg"
            . " -p ../pending_publish -s $tmpDir -x ../$xslFile",
        # disable checking CMR for access type
        # UPDATE_CMR_ACCESS => "s4pa_get_cmr_access.pl -f ../s4pa_insert_cmr.cfg"
        };
    $virtualJobs = { 
        DELETE_CMR => 1,
        PUBLISH_CMR => 1,
        };
    $downStream = { 'PUT' => [ 'postoffice' ],
                    'DELETE' => [ 'postoffice' ] };
    $config = {
        cfg_max_children => 2,
        cfg_station_name => "PublishCMR",
        cfg_root => $global->{S4PA_ROOT},
        cfg_group => $global->{S4PA_GID},
        cfg_max_failures => 1,
        cfg_max_time => 600,
        cfg_polling_interval => 3600,
        cfg_stop_interval => 4,
        cfg_end_job_interval => 2,
        cfg_restart_defunct_jobs => 1,
	cfg_sort_jobs => 'FIFO',
        cfg_failure_handlers => $failureHandler,
        cfg_commands => $cmdHash,
        cfg_virtual_jobs => $virtualJobs,
        cfg_virtual_feedback => 1,
        cfg_ignore_duplicates => 1,
	cfg_umask => 022,
        cfg_downstream => $downStream,
        __TYPE__ => {
            cfg_failure_handlers => 'HASH',
            cfg_commands => 'HASH',
            cfg_virtual_jobs => 'HASH',
            cfg_downstream => 'HASH',
            }
        };
    # Add an interface to edit station config
    $config->{cfg_interfaces}{'Edit Station Config'} =
      qq( perl -e 'use S4PA; S4PA::EditFile( FILE => "$pubCmrStationDir/station.cfg", TITLE => "PublishCMR" )' );
    # Add an interface to remove stale log
    $config->{cfg_interfaces}{'Remove Stale Log'} =
      qq( perl -e 'use S4PA; S4PA::RemoveStaleLog( STATION => "$pubCmrStationDir", TITLE => "PublishCMR" )' );
    # Add an interface to republish
    $config->{cfg_interfaces}{'Republish Data'} =
      qq( perl -e 'use S4PA; S4PA::RepublishData( STATION => "$pubCmrStationDir", TITLE => "PublishCMR" )' );
    # Add an interface for retrying all failed jobs
    $config->{cfg_interfaces}{'Retry Failed Jobs'} = qq(s4p_restart_all_jobs.pl);

    # We will be using MMT for collection setting update
    # $config->{cfg_interfaces}{'Update Dataset Configuration'} =
    #   "s4pa_cmr_dataset.pl -r $global->{S4PA_ROOT}";

    $config->{__TYPE__}{cfg_interfaces} = 'HASH';

    S4PA::CreateStation( $pubCmrStationDir, $config, $logger );
    
    # Find out access type and CMR ID for datasets
    my $dataAccess = {};
    foreach my $dataClass ( $doc->findnodes( '//provider/dataClass' ) ) {
        my $classAttr = GetDataAttributes( $dataClass );
        foreach my $dataset ( $dataClass->findnodes( 'dataset' ) ) {
            my $dataAttr = GetDataAttributes( $dataset, $classAttr );
            my $dataName = $dataAttr->{NAME};
            my ( @dataVersionList ) = $dataset->findnodes( 'dataVersion' );
            if ( @dataVersionList ) {
                foreach my $version ( @dataVersionList ) {
                    my $versionId = $version->getAttribute( 'LABEL' );
                    my $versionAttr = GetDataAttributes( $version, $dataAttr );
                    if ( defined $versionAttr->{ACCESS} && defined $versionAttr->{PUBLISH_CMR}
                        && $versionAttr->{PUBLISH_CMR} eq 'true' ) {
                        $dataAccess->{$dataName}{$versionId} = $versionAttr->{ACCESS};
                    }
                }
            } else {
                # we should not end up here since a versionless dataset publishing to CMR
                # need to have collection_version specified under dataVersion
                DeployLogging( 'fatal', "No collection_version was specified for " .
                    "the versionless $dataName." ) if ( defined $dataAttr->{PUBLISH_CMR} &&
                    $dataAttr->{PUBLISH_CMR} eq 'true' );
            }
        }
    }
    
    # Obtain OPeNDAP information for datasets
    my $opendapInfo;
    foreach my $dataClass ( $doc->findnodes( '//provider/dataClass' ) ) {
        my $classAttr = GetDataAttributes( $dataClass );
        foreach my $dataset ( $dataClass->findnodes( 'dataset' ) ) {
            my $dataAttr = GetDataAttributes( $dataset, $classAttr );
            my $dataName = $dataAttr->{NAME};
            my ( @dataVersionList ) = $dataset->findnodes( 'dataVersion' );
            if ( @dataVersionList ) {
                foreach my $version ( @dataVersionList ) {
                    my $versionId = $version->getAttribute( 'LABEL' );
                    my $versionAttr = GetDataAttributes( $version, $dataAttr );
                    # skip if this version is not set to publish to CMR
                    next unless ( $versionAttr->{'PUBLISH_CMR'} eq 'true' );
                    if ( ( defined $versionAttr->{PUBLISH_CMR_OPENDAP} ) && ( $versionAttr->{PUBLISH_CMR_OPENDAP} eq 'true' ) ){
                        if ( defined $versionAttr->{OPENDAP_URL_PREFIX} ) {
                            if ($versionAttr->{OPENDAP_URL_PREFIX}) {
                                $opendapInfo->{$dataName}->{$versionId}->{'OPENDAP_URL_PREFIX'} = $versionAttr->{OPENDAP_URL_PREFIX};
                                $opendapInfo->{$dataName}->{$versionId}->{'OPENDAP_URL_SUFFIX'} = $versionAttr->{OPENDAP_URL_SUFFIX} if (exists $versionAttr->{OPENDAP_URL_SUFFIX});
                                $opendapInfo->{$dataName}->{$versionId}->{'OPENDAP_RESOURCE_URL_SUFFIX'} = $versionAttr->{OPENDAP_RESOURCE_URL_SUFFIX} if (exists $versionAttr->{OPENDAP_RESOURCE_URL_SUFFIX});
                            }
                        }
                    }
                }
            } else {
                # skip if this dataset is not set to publish to CMR
                next unless ( $dataAttr->{'PUBLISH_CMR'} eq 'true' );
                # Case of version-less system.
                if ( ( defined $dataAttr->{PUBLISH_CMR_OPENDAP} ) && ( $dataAttr->{PUBLISH_CMR_OPENDAP} eq 'true' ) ){
                    if ( defined $dataAttr->{OPENDAP_URL_PREFIX} ) {
                        if ($dataAttr->{OPENDAP_URL_PREFIX}) {
                            $opendapInfo->{$dataName}->{''}->{'OPENDAP_URL_PREFIX'} = $dataAttr->{OPENDAP_URL_PREFIX};
                            $opendapInfo->{$dataName}->{''}->{'OPENDAP_URL_SUFFIX'} = $dataAttr->{OPENDAP_URL_SUFFIX} if (exists $dataAttr->{OPENDAP_URL_SUFFIX});
                            $opendapInfo->{$dataName}->{''}->{'OPENDAP_RESOURCE_URL_SUFFIX'} = $dataAttr->{OPENDAP_RESOURCE_URL_SUFFIX} if (exists $dataAttr->{OPENDAP_RESOURCE_URL_SUFFIX});
                        }
                    }
                }
            }
        }
    }

    my $skipXpath = {};
    foreach my $dataClass ( $doc->findnodes( '//provider/dataClass' ) ) {
        my @classSkipList = $dataClass->getChildrenByTagName( 'skipPublication' );
        foreach my $dataset ( $dataClass->findnodes( 'dataset' ) ) {
            my @setSkipList = $dataset->getChildrenByTagName( 'skipPublication' );
            my $dataName = $dataset->getAttribute( 'NAME' );
            my ( @dataVersionList ) = $dataset->findnodes( 'dataVersion' );
            # for versioned datasets
            if ( @dataVersionList ) {
                foreach my $version ( @dataVersionList ) {
                    my $versionId = $version->getAttribute( 'LABEL' ) || '';
                    # scan through all xpath under class, dataset, then version
                    # so lower level setting will overwrite high level one
                    foreach my $skipXpathNode ( @classSkipList, @setSkipList, 
                        $version->findnodes( 'skipPublication' ) ) {
                        $skipXpath->{$dataName}{$versionId} = {}
                            unless defined $skipXpath->{$dataName}{$versionId};
                        my $operator = $skipXpathNode->getAttribute( 'OPERATOR' );
                        $operator = 'EQ' unless defined $operator;
                        my $skipValue = $skipXpathNode->getAttribute( 'VALUE' );
                        $skipValue = '' unless defined $skipValue;
                        my $xpath = GetNodeValue( $skipXpathNode );
                        $skipXpath->{$dataName}{$versionId}{$operator}{$xpath} = $skipValue;
                    }
                }
            # for versionless datasets
            } else {
                my $versionId = '';
                # scan through all xpath under class, then dataset
                # so lower level setting will overwrite high level one
                foreach my $skipXpathNode ( @classSkipList, @setSkipList ) {
                    $skipXpath->{$dataName}{$versionId} = {}
                        unless defined $skipXpath->{$dataName}{$versionId};
                    my $operator = $skipXpathNode->getAttribute( 'OPERATOR' );
                    $operator = 'EQ' unless defined $operator;
                    my $skipValue = $skipXpathNode->getAttribute( 'VALUE' );
                    $skipValue = '' unless defined $skipValue;
                    my $xpath = GetNodeValue( $skipXpathNode );
                    $skipXpath->{$dataName}{$versionId}{$operator}{$xpath} = $skipValue;
                }
            }
        }
    }

    # disable CMR access type checking
    # # Preserve CMR_ACCESS hash if exist and consistent with $dataAccess
    # my $cmrAccess;
    # my $insertConfig = $pubCmrStationDir . "s4pa_insert_cmr.cfg";
    # if (-f $insertConfig) {
    #     my $cpt = Safe->new( 'CFG' );
    #     $cpt->share( '%CMR_ACCESS' );
    #     if ( $cpt->rdo( "$insertConfig" ) ) {
    #         if (%CFG::CMR_ACCESS) {
    #             foreach my $dataName (keys %CFG::CMR_ACCESS) {
    #                 foreach my $versionId (keys %{$CFG::CMR_ACCESS{$dataName}}) {
    #                     if ( !($CFG::CMR_ACCESS{$dataName}{$versionId} eq 'public' &&
    #                            $dataAccess->{$dataName}{$versionId} ne 'public') ) {
    #                         $cmrAccess->{$dataName}{$versionId} =
    #                                  $CFG::CMR_ACCESS{$dataName}{$versionId};
    #                     }
    #                 }
    #             }
    #         }
    #     }
    # }

    $config = {
        cfg_s4pa_root => $global->{S4PA_ROOT},
        TYPE => 'insert',
        UNRESTRICTED_ROOTURL => $global->{URL}{FTP},
        RESTRICTED_ROOTURL => $global->{URL}{HTTP},
        INSTANCE_NAME => $global->{S4PA_NAME},
        CMR_ENDPOINT_URI => $global->{CMR}{CMR_ENDPOINT_URI},
        CMR_PROVIDER => $global->{CMR}{PROVIDER},
        MAX_GRANULE_COUNT => $global->{CMR}{MAX_GRANULE_COUNT},
        DATA_ACCESS => $dataAccess,
        __TYPE__ => {
            DATA_ACCESS => 'HASH',
            }
        };

    # either ECHO token with username/password
    # or Launchpad token with certificate file/password
    if (defined $global->{CMR}{CERT_FILE}) {
        $config->{LAUNCHPAD_URI} = $global->{CMR}{CMR_TOKEN_URI};
        $config->{CMR_CERTFILE} = $global->{CMR}{CERT_FILE};
        $config->{CMR_CERTPASS} = $global->{CMR}{CERT_PASS};
    } else {
        $config->{CMR_TOKEN_URI} = $global->{CMR}{CMR_TOKEN_URI};
        $config->{CMR_USERNAME} = $global->{CMR}{USERNAME};
        $config->{CMR_PASSWORD} = $global->{CMR}{PASSWORD};
    }

    # if (defined $cmrAccess) {
    #     $config->{CMR_ACCESS} = $cmrAccess;
    #     $config->{__TYPE__}{CMR_ACCESS} = 'HASH';
    # }
    if (defined $opendapInfo) {
        $config->{CMR_OPENDAP} = $opendapInfo;
        $config->{__TYPE__}{CMR_OPENDAP} = 'HASH';
    }
    if ( defined $global->{LOGGER} ) {
        $config->{cfg_logger} = {
            LEVEL => $global->{LOGGER}{LEVEL},
            FILE => $logFile };
        $config->{__TYPE__}{cfg_logger} = 'HASH';
    }

    # configure global skip publishing PSAs
    if ( exists $global->{CMR}{SKIPPSA} ) {
        $config->{cfg_psa_skip} = $global->{CMR}{SKIPPSA};
        $config->{__TYPE__}{cfg_psa_skip} = 'LIST';
    }

    # configure dataset specific skip publishing xpath
    if ( defined $skipXpath ) {
        $config->{cfg_xpath_skip} = $skipXpath;
        $config->{__TYPE__}{cfg_xpath_skip} = 'HASH';
    }

    S4PA::WriteStationConfig( 's4pa_insert_cmr.cfg', $pubCmrStationDir,
        $config );

    $config = {
        cfg_s4pa_root => $global->{S4PA_ROOT},
        TYPE => 'delete',
        UNRESTRICTED_ROOTURL => $global->{URL}{FTP},
        RESTRICTED_ROOTURL => $global->{URL}{HTTP},
        INSTANCE_NAME => $global->{S4PA_NAME},
        CMR_ENDPOINT_URI => $global->{CMR}{CMR_ENDPOINT_URI},
        CMR_PROVIDER => $global->{CMR}{PROVIDER},
        MAX_GRANULE_COUNT => $global->{CMR}{MAX_GRANULE_COUNT},
        }; 
    
    # either ECHO token with username/password
    # or Launchpad token with certificate file/password
    if (defined $global->{CMR}{CERT_FILE}) {
        $config->{LAUNCHPAD_URI} = $global->{CMR}{CMR_TOKEN_URI};
        $config->{CMR_CERTFILE} = $global->{CMR}{CERT_FILE};
        $config->{CMR_CERTPASS} = $global->{CMR}{CERT_PASS};
    } else {
        $config->{CMR_TOKEN_URI} = $global->{CMR}{CMR_TOKEN_URI};
        $config->{CMR_USERNAME} = $global->{CMR}{USERNAME};
        $config->{CMR_PASSWORD} = $global->{CMR}{PASSWORD};
    }

    if ( defined $global->{LOGGER} ) {
        $config->{cfg_logger} = {
            LEVEL => $global->{LOGGER}{LEVEL},
            FILE => $logFile };
        $config->{__TYPE__}{cfg_logger} = 'HASH';
    }
    S4PA::WriteStationConfig( 's4pa_delete_cmr.cfg', $pubCmrStationDir,
        $config );

    # # create a job for getting CMR access
    # my @items = `ls -a $pubCmrStationDir`;
    # my @files = map {/DO\.UPDATE_CMR_ACCESS/} @items;
    # unless (@files) {
    #     my $time = time;
    #     `touch $pubCmrStationDir/DO.UPDATE_CMR_ACCESS.$time.wo`;
    # }

    push( @stationDirList, $pubCmrStationDir );
    return @stationDirList;       
}

################################################################################
# =head1 CreatePublishMiradorStations
# 
# Description
#   Creates Mirador publication stations that are used in 
#   publishing metadata for ingested data to search clients.
#
# =cut
################################################################################
sub CreatePublishMiradorStations
{
    my ( $doc, $global ) = @_;
    my ( $config, $cmdHash, $virtualJobs, $downStream ) = ( {}, {}, {}, {} );
    my $logger = $global->{LOGGER}{LOGGING} if defined $global->{LOGGER}{LOGGING};

    # Failure handler to retry a failed job.
    my $failureHandler = { 'Retry Job' => "perl -e 'use S4P; S4P::remove_job()"
                            . " if S4P::restart_job()'" };
                                
    # Holder of station directories
    my @stationDirList = ();

    my $tmpDir = $global->{S4PA_ROOT}. "tmp";   

    # Master log file
    my $logFile = defined $global->{LOGGER}
        ? "$global->{LOGGER}{DIR}/publish.log" : undef;

    # Create Mirador publication
    my $pubMiradorStationDir = $global->{S4PA_ROOT} . "publish_mirador/";
    $cmdHash = {
        DELETE_MIRADOR => "s4pa_publish_mirador.pl"
            . " -c ../s4pa_delete_mirador.cfg -p ../pending_delete"
            . " -o $tmpDir -x ../S4paGran2Mirador.xsl",
        PUBLISH_MIRADOR => "s4pa_publish_mirador.pl"
            . " -c ../s4pa_insert_mirador.cfg -p ../pending_publish"
            . " -o $tmpDir -x ../S4paGran2Mirador.xsl"
        };
    $virtualJobs = { 
        DELETE_MIRADOR => 1,
        PUBLISH_MIRADOR => 1
        };
    $downStream = { 'PUSH' => [ 'postoffice' ] };    
    $config = {
        cfg_max_children => 2,
        cfg_station_name => "PublishMirador",
        cfg_root => $global->{S4PA_ROOT},
        cfg_group => $global->{S4PA_GID},
        cfg_max_failures => 1,
	cfg_sort_jobs => 'FIFO',
        cfg_max_time => 600,
        cfg_polling_interval => 3600,
        cfg_stop_interval => 4,
        cfg_end_job_interval => 2,
        cfg_restart_defunct_jobs => 1,
        cfg_failure_handlers => $failureHandler,
        cfg_commands => $cmdHash,
        cfg_virtual_jobs => $virtualJobs,
        cfg_virtual_feedback => 1,
        cfg_ignore_duplicates => 1,
	cfg_umask => 022,
        cfg_downstream => $downStream,
        __TYPE__ => {
            cfg_failure_handlers => 'HASH',
            cfg_commands => 'HASH',
            cfg_virtual_jobs => 'HASH',
            cfg_downstream => 'HASH',
            }
        };
    # Add an interface to edit station config
    $config->{cfg_interfaces}{'Edit Station Config'} =
      qq( perl -e 'use S4PA; S4PA::EditFile( FILE => "$pubMiradorStationDir/station.cfg", TITLE => "PublishMirador" )' );
    # Add an interface to remove stale log
    $config->{cfg_interfaces}{'Remove Stale Log'} =
      qq( perl -e 'use S4PA; S4PA::RemoveStaleLog( STATION => "$pubMiradorStationDir", TITLE => "PublishMirador" )' );
    # Add an interface to republish
    $config->{cfg_interfaces}{'Republish Data'} =
      qq( perl -e 'use S4PA; S4PA::RepublishData( STATION => "$pubMiradorStationDir", TITLE => "PublishMirador" )' );
    # Add an interface for retrying all failed jobs
    $config->{cfg_interfaces}{'Retry Failed Jobs'} = qq(s4p_restart_all_jobs.pl);
    $config->{__TYPE__}{cfg_interfaces} = 'HASH';


    S4PA::CreateStation( $pubMiradorStationDir, $config, $logger );

    # configure skip publishing 
    my $skipXpath = {};
    foreach my $dataClass ( $doc->findnodes( '//provider/dataClass' ) ) {
        my @classSkipList = $dataClass->getChildrenByTagName( 'skipPublication' );
        foreach my $dataset ( $dataClass->findnodes( 'dataset' ) ) {
            my @setSkipList = $dataset->getChildrenByTagName( 'skipPublication' );
            my $dataName = $dataset->getAttribute( 'NAME' );
            my ( @dataVersionList ) = $dataset->findnodes( 'dataVersion' );
            # for versioned datasets
            if ( @dataVersionList ) {
                foreach my $version ( @dataVersionList ) {
                    my $versionId = $version->getAttribute( 'LABEL' ) || '';
                    # scan through all xpath under class, dataset, then version
                    # so lower level setting will overwrite high level one
                    foreach my $skipXpathNode ( @classSkipList, @setSkipList, 
                        $version->findnodes( 'skipPublication' ) ) {
                        $skipXpath->{$dataName}{$versionId} = {}
                            unless defined $skipXpath->{$dataName}{$versionId};
                        my $operator = $skipXpathNode->getAttribute( 'OPERATOR' );
                        $operator = 'EQ' unless defined $operator;
                        my $skipValue = $skipXpathNode->getAttribute( 'VALUE' );
                        $skipValue = '' unless defined $skipValue;
                        my $xpath = GetNodeValue( $skipXpathNode );
                        $skipXpath->{$dataName}{$versionId}{$operator}{$xpath} = $skipValue;
                    }
                }
            # for versionless datasets
            } else {
                my $versionId = '';
                # scan through all xpath under class, then dataset
                # so lower level setting will overwrite high level one
                foreach my $skipXpathNode ( @classSkipList, @setSkipList ) {
                    $skipXpath->{$dataName}{$versionId} = {}
                        unless defined $skipXpath->{$dataName}{$versionId};
                    my $operator = $skipXpathNode->getAttribute( 'OPERATOR' );
                    $operator = 'EQ' unless defined $operator;
                    my $skipValue = $skipXpathNode->getAttribute( 'VALUE' );
                    $skipValue = '' unless defined $skipValue;
                    my $xpath = GetNodeValue( $skipXpathNode );
                    $skipXpath->{$dataName}{$versionId}{$operator}{$xpath} = $skipValue;
                }
            }
        }
    }

    # support publish_mirador via different protocol than ftp.
    my $miradorHost = $global->{MIRADOR}{GRANULE}{INSERT}{HOST};
    my $miradorProtocol = ( defined $global->{PROTOCOL}{$miradorHost} ) ?
        lc( $global->{PROTOCOL}{$miradorHost} ) : 'ftp';

    $config = {
        TYPE => 'insert',
        HOST => $miradorHost,
        DESTDIR => $global->{MIRADOR}{GRANULE}{INSERT}{DIR},
        INSTANCE_NAME => $global->{S4PA_NAME},
        RESTRICTED_URL => $global->{URL}{HTTP},
        UNRESTRICTED_URL => $global->{URL}{FTP},
        PROTOCOL => $miradorProtocol,
        };
    if ( defined $global->{LOGGER} ) {
        $config->{cfg_logger} = {
            LEVEL => $global->{LOGGER}{LEVEL},
            FILE => $logFile };
        $config->{__TYPE__}{cfg_logger} = 'HASH';
    }

    # configure global skip publishing PSAs
    if ( exists $global->{MIRADOR}{SKIPPSA} ) {
        $config->{cfg_psa_skip} = $global->{MIRADOR}{SKIPPSA};
        $config->{__TYPE__}{cfg_psa_skip} = 'LIST';
    }

    # configure dataset specific skip publishing xpath
    if ( defined $skipXpath ) {
        $config->{cfg_xpath_skip} = $skipXpath;
        $config->{__TYPE__}{cfg_xpath_skip} = 'HASH';
    }

    S4PA::WriteStationConfig( 's4pa_insert_mirador.cfg',
        $pubMiradorStationDir, $config );

    # support publish_mirador via different protocol than ftp.
    my $miradorHost = $global->{MIRADOR}{GRANULE}{DELETE}{HOST};
    my $miradorProtocol = ( defined $global->{PROTOCOL}{$miradorHost} ) ?
        lc( $global->{PROTOCOL}{$miradorHost} ) : 'ftp';

    $config = {
        TYPE => 'delete',
        HOST => $miradorHost,
        DESTDIR => $global->{MIRADOR}{GRANULE}{DELETE}{DIR},
        INSTANCE_NAME => $global->{S4PA_NAME},
        RESTRICTED_URL => $global->{URL}{HTTP},
        UNRESTRICTED_URL => $global->{URL}{FTP},
        PROTOCOL => $miradorProtocol,
        };
    if ( defined $global->{LOGGER} ) {
        $config->{cfg_logger} = {
            LEVEL => $global->{LOGGER}{LEVEL},
            FILE => $logFile };
        $config->{__TYPE__}{cfg_logger} = 'HASH';
    }
    S4PA::WriteStationConfig( 's4pa_delete_mirador.cfg',
        $pubMiradorStationDir, $config );  
    push( @stationDirList, $pubMiradorStationDir );
    return @stationDirList;       
}

################################################################################
# =head1 CreatePublishGiovanniStations
# 
# Description
#   Creates Giovanni publication stations that are used in 
#   publishing metadata for ingested data to search clients.
#
# =cut
################################################################################
sub CreatePublishGiovanniStations
{
    my ( $doc, $global ) = @_;
    my ( $config, $cmdHash, $virtualJobs, $downStream ) = ( {}, {}, {}, {} );
    my $logger = $global->{LOGGER}{LOGGING} if defined $global->{LOGGER}{LOGGING};

    # Failure handler to retry a failed job.
    my $failureHandler = { 'Retry Job' => "perl -e 'use S4P; S4P::remove_job()"
                            . " if S4P::restart_job()'" };
                                
    # Holder of station directories
    my @stationDirList = ();

    my $tmpDir = $global->{S4PA_ROOT}. "tmp";   

    # Master log file
    my $logFile = defined $global->{LOGGER}
        ? "$global->{LOGGER}{DIR}/publish.log" : undef;

    # Create Giovanni publication
    my $pubGiovanniStationDir = $global->{S4PA_ROOT} . "publish_giovanni/";
    $cmdHash = {
        DELETE_GIOVANNI => "s4pa_publish_giovanni.pl"
            . " -c ../s4pa_delete_giovanni.cfg -p ../pending_delete"
            . " -s $tmpDir -x ../S4paGran2Giovanni.xsl",
        PUBLISH_GIOVANNI => "s4pa_publish_giovanni.pl"
            . " -c ../s4pa_insert_giovanni.cfg -p ../pending_publish"
            . " -s $tmpDir -x ../S4paGran2Giovanni.xsl"
        };
    $virtualJobs = { 
        DELETE_GIOVANNI => 1,
        PUBLISH_GIOVANNI => 1
        };
    $downStream = { 'PUSH' => [ 'postoffice' ] };    
    $config = {
        cfg_max_children => 2,
        cfg_station_name => "PublishGiovanni",
        cfg_root => $global->{S4PA_ROOT},
        cfg_group => $global->{S4PA_GID},
        cfg_max_failures => 1,
	cfg_sort_jobs => 'FIFO',
        cfg_max_time => 600,
        cfg_polling_interval => 3600,
        cfg_stop_interval => 4,
        cfg_end_job_interval => 2,
        cfg_restart_defunct_jobs => 1,
        cfg_failure_handlers => $failureHandler,
        cfg_commands => $cmdHash,
        cfg_virtual_jobs => $virtualJobs,
        cfg_virtual_feedback => 1,
        cfg_ignore_duplicates => 1,
	cfg_umask => 022,
        cfg_downstream => $downStream,
        __TYPE__ => {
            cfg_failure_handlers => 'HASH',
            cfg_commands => 'HASH',
            cfg_virtual_jobs => 'HASH',
            cfg_downstream => 'HASH',
            }
        };
    # Add an interface to edit station config
    $config->{cfg_interfaces}{'Edit Station Config'} =
      qq( perl -e 'use S4PA; S4PA::EditFile( FILE => "$pubGiovanniStationDir/station.cfg", TITLE => "PublishGiovanni" )' );
    # Add an interface to remove stale log
    $config->{cfg_interfaces}{'Remove Stale Log'} =
      qq( perl -e 'use S4PA; S4PA::RemoveStaleLog( STATION => "$pubGiovanniStationDir", TITLE => "PublishGiovanni" )' );
    # Add an interface to republish
    $config->{cfg_interfaces}{'Republish Data'} =
      qq( perl -e 'use S4PA; S4PA::RepublishData( STATION => "$pubGiovanniStationDir", TITLE => "PublishGiovanni" )' );
    # Add an interface for retrying all failed jobs
    $config->{cfg_interfaces}{'Retry Failed Jobs'} = qq(s4p_restart_all_jobs.pl);
    $config->{__TYPE__}{cfg_interfaces} = 'HASH';


    S4PA::CreateStation( $pubGiovanniStationDir, $config, $logger );
    $config = {
        TYPE => 'insert',
        HOST => $global->{GIOVANNI}{GRANULE}{INSERT}{HOST},
        DESTDIR => $global->{GIOVANNI}{GRANULE}{INSERT}{DIR},
        INSTANCE_NAME => $global->{S4PA_NAME},
        RESTRICTED_ROOTURL => $global->{URL}{HTTP},
        UNRESTRICTED_ROOTURL => $global->{URL}{FTP},
        };
    if ( defined $global->{LOGGER} ) {
        $config->{cfg_logger} = {
            LEVEL => $global->{LOGGER}{LEVEL},
            FILE => $logFile };
        $config->{__TYPE__}{cfg_logger} = 'HASH';
    }
    S4PA::WriteStationConfig( 's4pa_insert_giovanni.cfg',
        $pubGiovanniStationDir, $config );
    $config = {
        TYPE => 'delete',
        HOST => $global->{GIOVANNI}{GRANULE}{DELETE}{HOST},
        DESTDIR => $global->{GIOVANNI}{GRANULE}{DELETE}{DIR},
        INSTANCE_NAME => $global->{S4PA_NAME},
        RESTRICTED_ROOTURL => $global->{URL}{HTTP},
        UNRESTRICTED_ROOTURL => $global->{URL}{FTP},
        };
    if ( defined $global->{LOGGER} ) {
        $config->{cfg_logger} = {
            LEVEL => $global->{LOGGER}{LEVEL},
            FILE => $logFile };
        $config->{__TYPE__}{cfg_logger} = 'HASH';
    }
    S4PA::WriteStationConfig( 's4pa_delete_giovanni.cfg',
        $pubGiovanniStationDir, $config );  
    push( @stationDirList, $pubGiovanniStationDir );
    return @stationDirList;       
}

################################################################################
# =head1 CreatePublishDotchartStations
# 
# Description
#   Creates Dotchart publication stations that are used in 
#   publishing metadata for ingested data to search clients.
#
# =cut
################################################################################
sub CreatePublishDotchartStations
{
    my ( $doc, $global ) = @_;
    my ( $config, $cmdHash, $virtualJobs, $downStream ) = ( {}, {}, {}, {} );
    my $logger = $global->{LOGGER}{LOGGING} if defined $global->{LOGGER}{LOGGING};

    # Failure handler to retry a failed job.
    my $failureHandler = { 'Retry Job' => "perl -e 'use S4P; S4P::remove_job()"
                            . " if S4P::restart_job()'" };
                                
    # Holder of station directories
    my @stationDirList = ();

    my $tmpDir = $global->{S4PA_ROOT}. "tmp";   

    # Master log file
    my $logFile = defined $global->{LOGGER}
        ? "$global->{LOGGER}{DIR}/publish.log" : undef;

    # Create DotChart publication
    my $pubDotChartStationDir = $global->{S4PA_ROOT} . "publish_dotchart/";
    $cmdHash = {
        DELETE_DOTCHART => "s4pa_publish_dotchart.pl -c ../s4pa_delete_dotchart.cfg"
            . " -s $tmpDir -p ../pending_delete -x ../S4paGran2DotChart.xsl",
        PUBLISH_DOTCHART => "s4pa_publish_dotchart.pl -c ../s4pa_insert_dotchart.cfg"
            . " -s $tmpDir -p ../pending_publish -x ../S4paGran2DotChart.xsl"
        };
    $virtualJobs = { 
        DELETE_DOTCHART => 1,
        PUBLISH_DOTCHART => 1
        };
    # Reroute all deletion PDRs to DeleteData station
    my $dataAccess = {};
    my $cpt = new Safe 'CLASS';
    DeployLogging( 'fatal', "Failed to read dataset->data class mapping ($!)" )
        unless $cpt->rdo( "$global->{S4PA_ROOT}/storage/dataset.cfg" );
    $downStream = { 'PUSH' => [ 'postoffice' ] };

    foreach my $dataset ( keys %CLASS::data_class ) {
        my $downStreamDir = "storage/$CLASS::data_class{$dataset}/"
            . "delete_$CLASS::data_class{$dataset}";
        foreach my $version ( keys %{$CLASS::cfg_publication{$dataset}} ) {
            my $dataVersionString = $dataset;
            $dataVersionString .= "_$version" if ( $version ne '' );
            $downStream->{"INTRA_VERSION_DELETE_$dataVersionString"}
                = [ "$downStreamDir/intra_version_pending" ];
            $downStream->{"INTER_VERSION_DELETE_$dataVersionString"}
                = [ "$downStreamDir/inter_version_pending" ];
        }
    }
    $config = {
        cfg_max_children => 2,
        cfg_station_name => "PublishDotChart",
        cfg_root => $global->{S4PA_ROOT},
        cfg_group => $global->{S4PA_GID},
        cfg_max_failures => 1,
        cfg_max_time => 600,
        cfg_polling_interval => 3600,
        cfg_stop_interval => 4,
        cfg_end_job_interval => 2,
        cfg_restart_defunct_jobs => 1,
	cfg_sort_jobs => 'FIFO',
        cfg_failure_handlers => $failureHandler,
        cfg_commands => $cmdHash,
        cfg_virtual_jobs => $virtualJobs,
        cfg_virtual_feedback => 1,
        cfg_ignore_duplicates => 1,
	cfg_umask => 022,
        cfg_downstream => $downStream,
        cfg_output_work_order_suffix => '{PDR,wo}',
        __TYPE__ => {
            cfg_failure_handlers => 'HASH',
            cfg_commands => 'HASH',
            cfg_virtual_jobs => 'HASH',
            cfg_downstream => 'HASH',
            }
        };
    # Add an interface to edit station config
    $config->{cfg_interfaces}{'Edit Station Config'} =
      qq( perl -e 'use S4PA; S4PA::EditFile( FILE => "$pubDotChartStationDir/station.cfg", TITLE => "PublishDotChart" )' );
    # Add an interface to remove stale log
    $config->{cfg_interfaces}{'Remove Stale Log'} =
      qq( perl -e 'use S4PA; S4PA::RemoveStaleLog( STATION => "$pubDotChartStationDir", TITLE => "PublishDotChart" )' );
    # Add an interface to republish
    $config->{cfg_interfaces}{'Republish Data'} =
      qq( perl -e 'use S4PA; S4PA::RepublishData( STATION => "$pubDotChartStationDir", TITLE => "PublishDotChart" )' );
    # Add an interface for retrying all failed jobs
    $config->{cfg_interfaces}{'Retry Failed Jobs'} = qq(s4p_restart_all_jobs.pl);
    $config->{__TYPE__}{cfg_interfaces} = 'HASH';

    S4PA::CreateStation( $pubDotChartStationDir, $config, $logger );

    # support publish_dotchart via different protocol than ftp.
    my $dotchartHost = $global->{DOTCHART}{GRANULE}{INSERT}{HOST};
    my $dotchartProtocol = ( defined $global->{PROTOCOL}{$dotchartHost} ) ?
        lc( $global->{PROTOCOL}{$dotchartHost} ) : 'ftp';

    $config = {
        TYPE => 'insert',
        UNRESTRICTED_ROOTURL => $global->{URL}{FTP},
        RESTRICTED_ROOTURL => $global->{URL}{HTTP},
        HOST => $dotchartHost,
        DESTDIR => $global->{DOTCHART}{GRANULE}{INSERT}{DIR},
        INSTANCE_NAME => $global->{S4PA_NAME},
        PROTOCOL => $dotchartProtocol,
        };
    if ( defined $global->{LOGGER} ) {
        $config->{cfg_logger} = {
            LEVEL => $global->{LOGGER}{LEVEL},
            FILE => $logFile };
        $config->{__TYPE__}{cfg_logger} = 'HASH';
    }
    S4PA::WriteStationConfig( 's4pa_insert_dotchart.cfg',
        $pubDotChartStationDir, $config );

    # support publish_dotchart via different protocol than ftp.
    $dotchartHost = $global->{DOTCHART}{GRANULE}{DELETE}{HOST};
    $dotchartProtocol = ( defined $global->{PROTOCOL}{$dotchartHost} ) ?
        lc( $global->{PROTOCOL}{$dotchartHost} ) : 'ftp';

    $config = {
        TYPE => 'delete',
        UNRESTRICTED_ROOTURL => $global->{URL}{FTP},
        RESTRICTED_ROOTURL => $global->{URL}{HTTP},
        HOST => $dotchartHost,
        HOST => $global->{DOTCHART}{GRANULE}{DELETE}{HOST},
        DESTDIR => $global->{DOTCHART}{GRANULE}{DELETE}{DIR},
        INSTANCE_NAME => $global->{S4PA_NAME},
        PROTOCOL => $dotchartProtocol,
        }; 
    if ( defined $global->{LOGGER} ) {
        $config->{cfg_logger} = {
            LEVEL => $global->{LOGGER}{LEVEL},
            FILE => $logFile };
        $config->{__TYPE__}{cfg_logger} = 'HASH';
    }
    S4PA::WriteStationConfig( 's4pa_delete_dotchart.cfg',
        $pubDotChartStationDir, $config );
    push( @stationDirList, $pubDotChartStationDir );
    return @stationDirList;       
}

################################################################################
# =head1 CreatePublishUserStations
# 
# Description
#   Creates User publication stations that are used in 
#   publishing metadata for ingested and deleted data for user.
#
# =cut
################################################################################
sub CreatePublishUserStations
{
    my ( $doc, $global ) = @_;
    my ( $config, $cmdHash, $virtualJobs, $downStream ) = ( {}, {}, {}, {} );
    my $logger = $global->{LOGGER}{LOGGING} if defined $global->{LOGGER}{LOGGING};

    # Failure handler to retry a failed job.
    my $failureHandler = { 'Retry Job' => "perl -e 'use S4P; S4P::remove_job()"
                            . " if S4P::restart_job()'" };
                                
    # Holder of station directories
    my @stationDirList = ();

    # Master log file
    my $logFile = defined $global->{LOGGER}
        ? "$global->{LOGGER}{DIR}/publish.log" : undef;

    # Create User publication
    my $pubUserStationDir = $global->{S4PA_ROOT} . "publish_user/";
    $cmdHash = {
        DELETE_USER => "s4pa_publish_user.pl -a Delete"
            . " -c ../s4pa_publish_user.cfg -p ../pending_delete",
        PUBLISH_USER => "s4pa_publish_user.pl -a Ingest"
            . " -c ../s4pa_publish_user.cfg -p ../pending_publish"
        };
    $virtualJobs = { 
        DELETE_USER => 1,
        PUBLISH_USER => 1
        };
    $downStream = { 'PUSH' => [ 'postoffice' ] };    
    $config = {
        cfg_max_children => 2,
        cfg_station_name => "PublishUser",
        cfg_root => $global->{S4PA_ROOT},
        cfg_group => $global->{S4PA_GID},
        cfg_max_failures => 1,
	cfg_sort_jobs => 'FIFO',
        cfg_max_time => 600,
        cfg_polling_interval => 3600,
        cfg_stop_interval => 4,
        cfg_end_job_interval => 2,
        cfg_restart_defunct_jobs => 1,
        cfg_failure_handlers => $failureHandler,
        cfg_commands => $cmdHash,
        cfg_virtual_jobs => $virtualJobs,
        cfg_virtual_feedback => 1,
        cfg_ignore_duplicates => 1,
	cfg_umask => 022,
        cfg_downstream => $downStream,
        __TYPE__ => {
            cfg_failure_handlers => 'HASH',
            cfg_commands => 'HASH',
            cfg_virtual_jobs => 'HASH',
            cfg_downstream => 'HASH',
            }
        };
    # Add an interface to edit station config
    $config->{cfg_interfaces}{'Edit Station Config'} =
      qq( perl -e 'use S4PA; S4PA::EditFile( FILE => "$pubUserStationDir/station.cfg", TITLE => "PublishUser" )' );
    # Add an interface to remove stale log
    $config->{cfg_interfaces}{'Remove Stale Log'} =
      qq( perl -e 'use S4PA; S4PA::RemoveStaleLog( STATION => "$pubUserStationDir", TITLE => "PublishUser" )' );
    # Add an interface to republish
    $config->{cfg_interfaces}{'Republish Data'} =
      qq( perl -e 'use S4PA; S4PA::RepublishData( STATION => "$pubUserStationDir", TITLE => "PublishUser" )' );
    # Add an interface for retrying all failed jobs
    $config->{cfg_interfaces}{'Retry Failed Jobs'} = qq(s4p_restart_all_jobs.pl);
    $config->{__TYPE__}{cfg_interfaces} = 'HASH';
    S4PA::CreateStation( $pubUserStationDir, $config, $logger );

    my $userDataset = {};
    foreach my $dataset ( sort keys %{$global->{USER}{DATASET}} ) {
        foreach my $version ( sort keys %{$global->{USER}{DATASET}{$dataset}} ) {
            my $dir = $global->{USER}{DATASET}{$dataset}{$version}{'DIR'};
            my $box = $global->{USER}{DATASET}{$dataset}{$version}{'BOX'};
            $userDataset->{$dataset}{$version}{'DESTDIR'} = $dir;
            $userDataset->{$dataset}{$version}{'BOUNDINGBOX'} = $box;
        }
    }

    # insert configuration
    $config = {
        RESTRICTED_ROOTURL => $global->{URL}{HTTP},
        UNRESTRICTED_ROOTURL => $global->{URL}{FTP},
        cfg_interval => $global->{USER}{INTERVAL},
        cfg_retention => $global->{USER}{RETENTION},
        cfg_datasets => $userDataset,
        __TYPE__ => {
            cfg_datasets => 'HASH',
            }
        };
    if ( defined $global->{LOGGER} ) {
        $config->{cfg_logger} = {
            LEVEL => $global->{LOGGER}{LEVEL},
            FILE => $logFile };
        $config->{__TYPE__}{cfg_logger} = 'HASH';
    }
    S4PA::WriteStationConfig( 's4pa_publish_user.cfg',
        $pubUserStationDir, $config );
    push( @stationDirList, $pubUserStationDir );
    return @stationDirList;       
}

################################################################################
# =head1 CreatePostOfficeStation
# 
# Description
#   Creates a PostOffice station that is used in data pushes.
#
# =cut
################################################################################
sub CreatePostOfficeStation
{
    my ( $doc, $global ) = @_;
    my $logger = $global->{LOGGER}{LOGGING} if defined $global->{LOGGER}{LOGGING};
    
    # Failure handler to retry a failed job.
    my $failureHandler = { 'Retry Job' => "perl -e 'use S4P; S4P::remove_job()"
                                          . " if S4P::restart_job()'" };
    
    
    # Master log file
    my $logFile = defined $global->{LOGGER}
        ? "$global->{LOGGER}{DIR}/postoffice.log" : undef;
        
    # A hash ref to hold configuration, commands and downstream station 
    # directories.
    my ( $config, $cmdHash, $downStream ) = ( {}, {}, {} );
    
    my $postOfficeStationDir = $global->{S4PA_ROOT} . "postoffice/";
    $downStream = {
        EMAIL => [ "postoffice" ],
	PUSH => [ "postoffice" ],
        PUT => [ "postoffice" ],
	DELETE => [ "postoffice" ],
        };
    $downStream->{TRACK} = [ "publish_dotchart/pending_publish" ] 
        if ( $global->{PUBLISH_DOTCHART} );

    $cmdHash =  {
                     EMAIL => "s4pa_create_DN.pl -f ../s4pa_postoffice.cfg ../",
                     PUT => "s4pa_rest_worker.pl -f ../s4pa_postoffice.cfg",
                     DELETE => "s4pa_rest_worker.pl -f ../s4pa_postoffice.cfg",
		     PUSH => "s4pa_file_pusher.pl -f ../s4pa_postoffice.cfg -d $global->{TEMPORARY_DIR}"
        };
                   
    $config = {
                cfg_max_children => $global->{POSTOFFICE}{MAX_CHILDREN},
                cfg_station_name => "PostOffice",
                cfg_root => $global->{S4PA_ROOT},
                cfg_group => $global->{S4PA_GID},
                cfg_max_time => 600,
                cfg_polling_interval => $global->{POSTOFFICE}{INTERVAL},
                cfg_stop_interval => 4,
                cfg_end_job_interval => 2,
                cfg_restart_defunct_jobs => 1,
		cfg_sort_jobs => 'FIFO',
                cfg_failure_handlers => $failureHandler,
                cfg_commands => $cmdHash,
                cfg_downstream => $downStream,
                cfg_ignore_duplicates => 1,
		cfg_umask => 022,
                __TYPE__ => {
                              cfg_failure_handlers => 'HASH',
                              cfg_commands => 'HASH',
                              cfg_downstream => 'HASH'
                            }
                };
    # Add an interface to edit station config
    $config->{cfg_interfaces}{'Edit Station Config'} =
      qq( perl -e 'use S4PA; S4PA::EditFile( FILE => "$postOfficeStationDir/station.cfg", TITLE => "PostOffice" )' );
    # Add an interface to remove Stale log
    $config->{cfg_interfaces}{'Remove Stale Log'} =
      qq( perl -e 'use S4PA; S4PA::RemoveStaleLog( STATION => "$postOfficeStationDir", TITLE => "PostOffice" )' );
    $config->{cfg_interfaces}{'Manage Work Orders'} =
      qq( perl -e 'use S4PA; S4PA::ManageWorkOrder( STATION => "$postOfficeStationDir", TITLE => "PostOffice" )' );
    $config->{cfg_interfaces}{'Retry Failed Jobs'} =
      qq( s4p_restart_all_jobs.pl);
    $config->{__TYPE__}{cfg_interfaces} = 'HASH';

    S4PA::CreateStation( $postOfficeStationDir, $config, $logger );
    $config = {
        max_attempt => $global->{POSTOFFICE}{MAX_ATTEMPT},
        cfg_publish_dotchart => $global->{PUBLISH_DOTCHART},
        };

    # add Launchpad token with certificate file/password
    if (defined $global->{CMR}{CERT_FILE}) {
        $config->{LAUNCHPAD_URI} = $global->{CMR}{CMR_TOKEN_URI};
        $config->{CMR_CERTFILE} = $global->{CMR}{CERT_FILE};
        $config->{CMR_CERTPASS} = $global->{CMR}{CERT_PASS};
    }

    if ( defined $global->{LOGGER} ) {
        $config->{cfg_logger} = {
            LEVEL => $global->{LOGGER}{LEVEL},
            FILE => $logFile };
        $config->{__TYPE__}{cfg_logger} = 'HASH';
    }
    S4PA::WriteStationConfig( 's4pa_postoffice.cfg', $postOfficeStationDir,
        $config );
    return $postOfficeStationDir;
}
################################################################################
# =head1 CreateGiovanniStation
# 
# Description
#   Creates giovanni preprocessing station
#
# =cut
################################################################################
sub CreateGiovanniStation
{
    my ( $doc, $global ) = @_;
    my $logger = $global->{LOGGER}{LOGGING} if defined $global->{LOGGER}{LOGGING};

    # Failure handler to retry a failed job.
    my $failureHandler = { 'Retry Job' => "perl -e 'use S4P; S4P::remove_job()"
                                          . " if S4P::restart_job()'" };

    my $preprocessMethod = {};
    foreach my $dataClass ( $doc->findnodes( '//provider/dataClass' ) ) {
        my $classAttr = GetDataAttributes( $dataClass );
        my $className = $classAttr->{NAME};
        my $providerName = $dataClass->parentNode()->getAttribute( 'NAME' );
        DeployLogging( 'fatal', "Failed to find data class name for provider=$providerName" )
            unless defined $className;
        foreach my $dataset ( $dataClass->findnodes( 'dataset' ) ) {
            my $dataAttr = GetDataAttributes( $dataset, $classAttr );
	    
            # Make sure data name, group are defined.
            my $dataName = $dataAttr->{NAME} || undef;
            DeployLogging( 'fatal', "Failed to find data name for provider=$providerName,"
                . " dataClass=$classAttr->{NAME}" )
                unless defined $dataName;
            next unless defined $dataAttr->{METADATA}{GIOVANNIPREPROCESS};
            $preprocessMethod->{$dataName} 
                = $dataAttr->{METADATA}{GIOVANNIPREPROCESS};
        }    
    }
    
    unless ( keys %$preprocessMethod ) {
        DeployLogging( 'info', "None of the datasets require Giovanni preprocessing" )
            if defined $logger;
        return undef;
    }
       
    my ( $cmdHash, $config ) = ( {}, {} );
    
    # Create Giovanni Preprocessing station
    my $preprocessStationDir = $global->{S4PA_ROOT} . "giovanni/preprocess/";

    
    # Create Giovanni station
    $cmdHash = { '.*' => 's4pa_giovanni.pl -f ../s4pa_giovanni.cfg' };    
    $config = {
               cfg_max_children => 1,
               cfg_station_name => "GiovanniPreprocess",
               cfg_group => $global->{S4PA_GID},
               cfg_max_failures => 1,
               cfg_max_time => 600,               
               cfg_polling_interval => 60,
               cfg_stop_interval => 4,
               cfg_end_job_interval => 2,
               cfg_restart_defunct_jobs => 1,
               cfg_failure_handlers => $failureHandler,
	       cfg_sort_jobs => 'FIFO',
               cfg_work_order_pattern => '*.PDR',
               cfg_commands => $cmdHash,
               cfg_ignore_duplicates => 1,
	       cfg_umask => 022,
               __TYPE__ => {
                            cfg_failure_handlers => 'HASH',
                            cfg_commands => 'HASH'
                           }
              };

    # Add an interface to edit station config
    $config->{cfg_interfaces}{'Edit Station Config'} =
      qq( perl -e 'use S4PA; S4PA::EditFile( FILE => "$preprocessStationDir/station.cfg", TITLE => "GiovanniPreprocess" )' );
    # Add an interface to remove Stale log
    $config->{cfg_interfaces}{'Remove Stale Log'} =
      qq( perl -e 'use S4PA; S4PA::RemoveStaleLog( STATION => "$preprocessStationDir", TITLE => "GiovanniPreprocess" )' );
    # Add an interface for retrying all failed jobs
    $config->{cfg_interfaces}{'Retry Failed Jobs'} = qq(s4p_restart_all_jobs.pl);
    $config->{__TYPE__}{cfg_interfaces} = 'HASH';

    S4PA::CreateStation( $preprocessStationDir, $config, $logger );
    
    # Create station specific configuration file.
    $config = {
        cfg_giovanni_xref_file => "$preprocessStationDir/xref.db",
        cfg_giovanni_info_file => "$preprocessStationDir/info.db",
        cfg_preprocessor => $preprocessMethod,
        __TYPE__ => {
            cfg_preprocessor => 'HASH'
            }
        };
    S4PA::WriteStationConfig( 's4pa_giovanni.cfg', $preprocessStationDir,
        $config );
    return $preprocessStationDir;
}
###############################################################################
# =head1 CreateReconciliationStations
#
# Description
#   Creates reconciliation station for ECHO, CMR, MIRADOR, and GIOVANNI.
#
# =cut
###############################################################################
sub CreateReconciliationStation
{
    my ( $doc, $global ) = @_;

    my ( $config, $cmdHash, $virtualJobs, $downStream ) = ( {}, {}, {}, {} );
    my $logger = $global->{LOGGER}{LOGGING} if defined $global->{LOGGER}{LOGGING};

    # Failure handler to retry a failed job.
    my $failureHandler = { 'Retry Job' => "perl -e 'use S4P; S4P::remove_job()"
                            . " if S4P::restart_job()'",
                           'Remove Job' => "perl -e 'use S4P; S4P::remove_job()'"
                         };

    # Master log file using publish.log for the moment
    my $logFile = defined $global->{LOGGER}
        ? "$global->{LOGGER}{DIR}/publish.log" : undef;

    # Create Reconciliation station 
    my $reconStationDir = $global->{S4PA_ROOT} . "reconciliation/";

    # Get all ESDTs for reconciliation
    my $dataAccess = {};
    my $esdtCounts = 0;

    # Set up partner specific reconciliation configuration
    my ( $configMirador, $configECHO, $configCMR, $configDotchart, $configGiovanni ) =
        ( {}, {}, {}, {}, {} );

    if ( $global->{PUBLISH_ECHO} ) {
        my $jobName = "ECHO";
        $cmdHash->{$jobName} ="s4pa_recon.pl -c ../s4pa_recon_ECHO.cfg";
        $virtualJobs->{$jobName} = 1;
        $configECHO = {
            cfg_partner => $jobName,
            cfg_s4pa_root => $global->{S4PA_ROOT},
            cfg_large_count_threshold => $global->{RECON}{ECHO}{MAX_GRANULE_COUNT},
            cfg_partner_service_username => $global->{RECON}{ECHO}{USERNAME},
            cfg_partner_service_encrypted_pwd => $global->{RECON}{ECHO}{PASSWORD},
            cfg_ftp_push_host => $global->{RECON}{ECHO}{PUSH_HOST},
            cfg_ftp_push_user => $global->{RECON}{ECHO}{PUSH_USER},
            cfg_ftp_push_pwd => $global->{RECON}{ECHO}{PUSH_PWD},
            cfg_ftp_push_dir => $global->{RECON}{ECHO}{PUSH_DIR},
            cfg_service_endpoint_uri => $global->{RECON}{ECHO}{ENDPOINT_URI},
            cfg_s4pa_instance_name => $global->{S4PA_NAME},
            cfg_deletion_xml_staging_dir => $global->{RECON}{ECHO}{STAGING_DIR},
            cfg_partner_ftp_pub_host => $global->{ECHO}{GRANULE}{DELETE}{HOST},
            cfg_partner_ftp_del_pub_dir => $global->{ECHO}{GRANULE}{DELETE}{DIR},
            cfg_partner_service_provider_id => "GSFCS4PA",
            };
        if ($global->{RECON}{ECHO}{CHROOT_DIR} eq 'null') {
            $configECHO->{cfg_temp_dir} = $global->{RECON}{ECHO}{LOCAL_DIR};
        } else {
            $configECHO->{cfg_ftp_server_chroot} = $global->{RECON}{ECHO}{CHROOT_DIR};
        }
        $configECHO->{cfg_minumum_interval} = $global->{RECON}{ECHO}{MIN_INTERVAL}
            if (defined $global->{RECON}{ECHO}{MIN_INTERVAL});
        $configECHO->{cfg_local_hostname_override} = $global->{RECON}{ECHO}{DATA_HOST}
            if (defined $global->{RECON}{ECHO}{DATA_HOST});
    }

    if ( $global->{PUBLISH_CMR} ) {
        my $jobName = "CMR";
        $cmdHash->{$jobName} ="s4pa_recon.pl -c ../s4pa_recon_CMR.cfg";
        $virtualJobs->{$jobName} = 1;
        $configCMR = {
            cfg_partner => $jobName,
            cfg_s4pa_root => $global->{S4PA_ROOT},
            cfg_large_count_threshold => $global->{RECON}{CMR}{MAX_GRANULE_COUNT},
            cfg_catalog_endpoint_uri => $global->{CMR}{CMR_ENDPOINT_URI} . 'ingest/',
            cfg_s4pa_instance_name => $global->{S4PA_NAME},
            cfg_deletion_xml_staging_dir => $global->{RECON}{CMR}{STAGING_DIR},
            cfg_partner_service_provider_id => $global->{CMR}{PROVIDER},
            };

        # either ECHO token username/password
        # or Launchpad token certificate file/password
        if (defined $global->{CMR}{CERT_FILE}) {
            $configCMR->{LAUNCHPAD_URI} = $global->{CMR}{CMR_TOKEN_URI};
            $configCMR->{CMR_CERTFILE} = $global->{CMR}{CERT_FILE};
            $configCMR->{CMR_CERTPASS} = $global->{CMR}{CERT_PASS};
        } else {
            $configCMR->{cfg_service_endpoint_uri} = $global->{CMR}{CMR_TOKEN_URI};
            $configCMR->{cfg_partner_service_username} = $global->{CMR}{USERNAME};
            $configCMR->{cfg_partner_service_encrypted_pwd} = $global->{CMR}{PASSWORD};
        }

        if ($global->{RECON}{CMR}{CHROOT_DIR} eq 'null') {
            $configCMR->{cfg_temp_dir} = $global->{RECON}{CMR}{LOCAL_DIR};
        } else {
            $configCMR->{cfg_ftp_server_chroot} = $global->{RECON}{CMR}{CHROOT_DIR};
        }
        $configCMR->{cfg_minumum_interval} = $global->{RECON}{CMR}{MIN_INTERVAL}
            if (defined $global->{RECON}{CMR}{MIN_INTERVAL});
        $configCMR->{cfg_local_hostname_override} = $global->{RECON}{CMR}{DATA_HOST}
            if (defined $global->{RECON}{CMR}{DATA_HOST});
    }

    if ( $global->{PUBLISH_MIRADOR} ) {
        my $jobName = "Mirador";
        $cmdHash->{$jobName} ="s4pa_recon.pl -c ../s4pa_recon_Mirador.cfg";
        $virtualJobs->{$jobName} = 1;

        # support recon mirador via different protocol than ftp
        # we can't use the PUSH_HOST, it could be the instance's own server
        # use the granule publishing host to refect the real invenio server 
        my $miradorHost = $global->{MIRADOR}{GRANULE}{INSERT}{HOST};
        my $miradorProtocol = (defined $global->{PROTOCOL}{$miradorHost}) ?
            lc($global->{PROTOCOL}{$miradorHost}) : 'ftp';
        $configMirador = {
            cfg_partner => $jobName,
            cfg_s4pa_root => $global->{S4PA_ROOT},
            cfg_ftp_push_host => $global->{RECON}{MIRADOR}{PUSH_HOST},
            cfg_ftp_push_dir => $global->{RECON}{MIRADOR}{PUSH_DIR},
            cfg_service_endpoint_uri => $global->{RECON}{MIRADOR}{ENDPOINT_URI},
            cfg_s4pa_instance_name => $global->{S4PA_NAME},
            cfg_deletion_xml_staging_dir => $global->{RECON}{MIRADOR}{STAGING_DIR},
            cfg_partner_ftp_pub_host => $global->{MIRADOR}{GRANULE}{DELETE}{HOST},
            cfg_partner_ftp_del_pub_dir => $global->{MIRADOR}{GRANULE}{DELETE}{DIR},
            };
        if ($global->{RECON}{MIRADOR}{CHROOT_DIR} eq 'null') {
            $configMirador->{cfg_temp_dir} = (defined $global->{RECON}{MIRADOR}{LOCAL_DIR}) ?
                $global->{RECON}{MIRADOR}{LOCAL_DIR} : $global->{TEMPORARY_DIR};
        } else {
            $configMirador->{cfg_ftp_server_chroot} = $global->{RECON}{MIRADOR}{CHROOT_DIR};
        } 
        $configMirador->{cfg_ftp_push_user} = $global->{RECON}{MIRADOR}{PUSH_USER}
            if (defined $global->{RECON}{MIRADOR}{PUSH_USER});
        $configMirador->{cfg_ftp_push_pwd} = $global->{RECON}{MIRADOR}{PUSH_PWD}
            if (defined $global->{RECON}{MIRADOR}{PUSH_PWD});
        $configMirador->{cfg_large_count_threshold} = $global->{RECON}{MIRADOR}{MAX_GRANULE_COUNT}
            if (defined $global->{RECON}{MIRADOR}{MAX_GRANULE_COUNT});
        $configMirador->{cfg_minumum_interval} = $global->{RECON}{MIRADOR}{MIN_INTERVAL}
            if (defined $global->{RECON}{MIRADOR}{MIN_INTERVAL});
        $configMirador->{cfg_ftp_pull_timeout} = $global->{RECON}{MIRADOR}{PULL_TIMEOUT}
            if (defined $global->{RECON}{MIRADOR}{PULL_TIMEOUT});
        $configMirador->{cfg_local_hostname_override} = $global->{RECON}{MIRADOR}{DATA_HOST}
            if (defined $global->{RECON}{MIRADOR}{DATA_HOST});
        $configMirador->{cfg_partner_protocol} = $miradorProtocol;
    }

    if ( $global->{PUBLISH_DOTCHART} ) {
        my $jobName = "Dotchart";
        $cmdHash->{$jobName} ="s4pa_recon.pl -c ../s4pa_recon_Dotchart.cfg";
        $virtualJobs->{$jobName} = 1;

        # support recon dotchart via different protocol than ftp
        # we can't use the PUSH_HOST, it could be the instance's own server,
        # use the granule publishing host to refect the real dotchart server 
        my $dotchartHost = $global->{DOTCHART}{GRANULE}{INSERT}{HOST};
        my $dotchartProtocol = (defined $global->{PROTOCOL}{$dotchartHost}) ?
            lc($global->{PROTOCOL}{$dotchartHost}) : 'ftp';
        $configDotchart = {
            cfg_partner => $jobName,
            cfg_s4pa_root => $global->{S4PA_ROOT},
            cfg_ftp_push_host => $global->{RECON}{DOTCHART}{PUSH_HOST},
            cfg_ftp_push_dir => $global->{RECON}{DOTCHART}{PUSH_DIR},
            cfg_service_endpoint_uri => $global->{RECON}{DOTCHART}{ENDPOINT_URI},
            cfg_s4pa_instance_name => $global->{S4PA_NAME},
            cfg_deletion_xml_staging_dir => $global->{RECON}{DOTCHART}{STAGING_DIR},
            cfg_partner_ftp_pub_host => $global->{DOTCHART}{GRANULE}{DELETE}{HOST},
            cfg_partner_ftp_del_pub_dir => $global->{DOTCHART}{GRANULE}{DELETE}{DIR},
            };
        if ($global->{RECON}{DOTCHART}{CHROOT_DIR} eq 'null') {
            $configDotchart->{cfg_temp_dir} = (defined $global->{RECON}{DOTCHART}{LOCAL_DIR}) ?
                $global->{RECON}{DOTCHART}{LOCAL_DIR} : $global->{TEMPORARY_DIR};
        } else {
            $configDotchart->{cfg_ftp_server_chroot} = $global->{RECON}{DOTCHART}{CHROOT_DIR};
        } 
        $configDotchart->{cfg_ftp_push_user} = $global->{RECON}{DOTCHART}{PUSH_USER}
            if (defined $global->{RECON}{DOTCHART}{PUSH_USER});
        $configDotchart->{cfg_ftp_push_pwd} = $global->{RECON}{DOTCHART}{PUSH_PWD}
            if (defined $global->{RECON}{DOTCHART}{PUSH_PWD});
        $configDotchart->{cfg_large_count_threshold} = $global->{RECON}{DOTCHART}{MAX_GRANULE_COUNT}
            if (defined $global->{RECON}{DOTCHART}{MAX_GRANULE_COUNT});
        $configDotchart->{cfg_minumum_interval} = $global->{RECON}{DOTCHART}{MIN_INTERVAL}
            if (defined $global->{RECON}{DOTCHART}{MIN_INTERVAL});
        $configDotchart->{cfg_ftp_pull_timeout} = $global->{RECON}{DOTCHART}{PULL_TIMEOUT}
            if (defined $global->{RECON}{DOTCHART}{PULL_TIMEOUT});
        $configDotchart->{cfg_local_hostname_override} = $global->{RECON}{DOTCHART}{DATA_HOST}
            if (defined $global->{RECON}{DOTCHART}{DATA_HOST});
        $configDotchart->{cfg_partner_protocol} = $dotchartProtocol;
    }

    if ( $global->{PUBLISH_GIOVANNI} ) {
        my $jobName = "Giovanni";
        $cmdHash->{$jobName} ="s4pa_recon.pl -c ../s4pa_recon_Giovanni.cfg";
        $virtualJobs->{$jobName} = 1;
        $configGiovanni = {
            cfg_partner => $jobName,
            cfg_s4pa_root => $global->{S4PA_ROOT},
            cfg_ftp_push_host => $global->{RECON}{GIOVANNI}{PUSH_HOST},
            cfg_ftp_push_dir => $global->{RECON}{GIOVANNI}{PUSH_DIR},
            cfg_service_endpoint_uri => $global->{RECON}{GIOVANNI}{ENDPOINT_URI},
            cfg_s4pa_instance_name => $global->{S4PA_NAME},
            cfg_deletion_xml_staging_dir => $global->{RECON}{GIOVANNI}{STAGING_DIR},
            cfg_partner_ftp_pub_host => $global->{GIOVANNI}{GRANULE}{DELETE}{HOST},
            cfg_partner_ftp_del_pub_dir => $global->{GIOVANNI}{GRANULE}{DELETE}{DIR},
            };
        if ($global->{RECON}{GIOVANNI}{CHROOT_DIR} eq 'null') {
            $configGiovanni->{cfg_temp_dir} = (defined $global->{RECON}{GIOVANNI}{LOCAL_DIR}) ?
                $global->{RECON}{GIOVANNI}{LOCAL_DIR} : $global->{TEMPORARY_DIR};
        } else {
            $configGiovanni->{cfg_ftp_server_chroot} = $global->{RECON}{GIOVANNI}{CHROOT_DIR};
        } 
        $configGiovanni->{cfg_ftp_push_user} = $global->{RECON}{GIOVANNI}{PUSH_USER}
            if (defined $global->{RECON}{GIOVANNI}{PUSH_USER});
        $configGiovanni->{cfg_ftp_push_pwd} = $global->{RECON}{GIOVANNI}{PUSH_PWD}
            if (defined $global->{RECON}{GIOVANNI}{PUSH_PWD});
        $configGiovanni->{cfg_large_count_threshold} = $global->{RECON}{GIOVANNI}{MAX_GRANULE_COUNT}
            if (defined $global->{RECON}{GIOVANNI}{MAX_GRANULE_COUNT});
        $configGiovanni->{cfg_minumum_interval} = $global->{RECON}{GIOVANNI}{MIN_INTERVAL}
            if (defined $global->{RECON}{GIOVANNI}{MIN_INTERVAL});
        $configGiovanni->{cfg_ftp_pull_timeout} = $global->{RECON}{GIOVANNI}{PULL_TIMEOUT}
            if (defined $global->{RECON}{GIOVANNI}{PULL_TIMEOUT});
        $configGiovanni->{cfg_local_hostname_override} = $global->{RECON}{GIOVANNI}{DATA_HOST}
            if (defined $global->{RECON}{GIOVANNI}{DATA_HOST});
    }

    my ( $datasetMirador, $datasetECHO, $datasetCMR, $datasetDotchart, $datasetGiovanni ) =
        ( {}, {}, {}, {}, {} );

    # read the existing dataset configuration for recon
    my ( %existingMirador, %existingECHO, %existingCMR, %existingDotchart, %existingGiovanni );
    if ( -f "$reconStationDir/s4pa_recon_Dotchart.cfg" ) {
        my $cpt = new Safe( 'RECON' );
        $cpt->rdo( "$reconStationDir/s4pa_recon_Dotchart.cfg" ) or
            S4P::perish(2, "Cannot read Dotchart recon configuration in safe mode: $!");
        if ( %RECON::cfg_dataset_list ) {
            foreach my $esdt ( keys %RECON::cfg_dataset_list ) {
                $existingDotchart{$esdt} = $RECON::cfg_dataset_list{$esdt};
            }
        }
    }
    if ( -f "$reconStationDir/s4pa_recon_Mirador.cfg" ) {
        my $cpt = new Safe( 'RECON' );
        $cpt->rdo( "$reconStationDir/s4pa_recon_Mirador.cfg" ) or
            S4P::perish(2, "Cannot read Mirador recon configuration in safe mode: $!");
        if ( %RECON::cfg_dataset_list ) {
            foreach my $esdt ( keys %RECON::cfg_dataset_list ) {
                $existingMirador{$esdt} = $RECON::cfg_dataset_list{$esdt};
            }
        }
    }
    my $echoQuiescent;
    if ( -f "$reconStationDir/s4pa_recon_ECHO.cfg" ) {
        my $cpt = new Safe( 'RECON' );
        $cpt->rdo( "$reconStationDir/s4pa_recon_ECHO.cfg" ) or
            S4P::perish(2, "Cannot read ECHO recon configuration in safe mode: $!");
        if ( %RECON::cfg_dataset_list ) {
            foreach my $esdt ( keys %RECON::cfg_dataset_list ) {
                $existingECHO{$esdt} = $RECON::cfg_dataset_list{$esdt};
            }
        }
        if ( defined $RECON::cfg_ftp_push_quiescent_time ) {
            $echoQuiescent = $RECON::cfg_ftp_push_quiescent_time;
        }
    }
    my $cmrQuiescent;
    if ( -f "$reconStationDir/s4pa_recon_CMR.cfg" ) {
        my $cpt = new Safe( 'RECON' );
        $cpt->rdo( "$reconStationDir/s4pa_recon_CMR.cfg" ) or
            S4P::perish(2, "Cannot read CMR recon configuration in safe mode: $!");
        if ( %RECON::cfg_dataset_list ) {
            foreach my $esdt ( keys %RECON::cfg_dataset_list ) {
                $existingCMR{$esdt} = $RECON::cfg_dataset_list{$esdt};
            }
        }
    }
    if ( -f "$reconStationDir/s4pa_recon_Giovanni.cfg" ) {
        my $cpt = new Safe( 'RECON' );
        $cpt->rdo( "$reconStationDir/s4pa_recon_Giovanni.cfg" ) or
            S4P::perish(2, "Cannot read Giovanni recon configuration in safe mode: $!");
        if ( %RECON::cfg_dataset_list ) {
            foreach my $esdt ( keys %RECON::cfg_dataset_list ) {
                $existingGiovanni{$esdt} = $RECON::cfg_dataset_list{$esdt};
            }
        }
    }


    foreach my $dataClass ( $doc->findnodes( '//provider/dataClass' ) ) {
        my $classAttr = GetDataAttributes( $dataClass );
        foreach my $dataset ( $dataClass->findnodes( 'dataset' ) ) {
            my $dataAttr = GetDataAttributes( $dataset, $classAttr );
            my $dataName = $dataAttr->{NAME};
            my $esdtString = $dataName;
            my ( @dataVersionList ) = $dataset->findnodes( 'dataVersion' );
            $dataAccess->{$dataName} = {};
            if ( @dataVersionList ) {
                foreach my $version ( @dataVersionList ) {
                    my $versionId = $version->getAttribute( 'LABEL' );
                    my $versionAttr = GetDataAttributes( $version, $dataAttr );
                    my $esdtString = $dataName . ":$versionId";

                    my $dotchartFlag = $global->{PUBLISH_DOTCHART} ? 'true' : 'false';
                    # Without DIF_ENTRY_ID data is not publishable;
                    # considered an error for reconciliation.
                    my $echoFlag = ( defined $versionAttr )
                        ? $versionAttr->{PUBLISH_ECHO} : $dataAttr->{PUBLISH_ECHO};
                    my $cmrFlag = ( defined $versionAttr )
                        ? $versionAttr->{PUBLISH_CMR} : $dataAttr->{PUBLISH_CMR};
                    my $miradorFlag = ( defined $versionAttr )
                        ? $versionAttr->{PUBLISH_MIRADOR} : $dataAttr->{PUBLISH_MIRADOR};
                    my $giovanniFlag = ( defined $versionAttr )
                        ? $versionAttr->{PUBLISH_GIOVANNI} : $dataAttr->{PUBLISH_GIOVANNI};
                    if ($echoFlag eq 'true') {
                        DeployLogging( 'fatal', "DIF_ENTRY_ID is missing for ECHO" .
                            " dataset $dataName version $versionId" )
                            if ((not defined $dataAttr->{DIF_ENTRY_ID}) &&
                                (not defined $versionAttr->{DIF_ENTRY_ID}));
                        $esdtCounts++;
                        $datasetECHO->{$esdtString} =
                            ( exists $existingECHO{$esdtString} ) ?
                            $existingECHO{$esdtString} : 1;
                    }
                    if ($cmrFlag eq 'true') {
                        DeployLogging( 'fatal', "COLLECTION attribute is missing for CMR" .
                            " dataset $dataName version $versionId" )
                            if ((not defined $dataAttr->{COLLECTION_SHORTNAME}) &&
                                (not defined $versionAttr->{COLLECTION_VERSION}));
                        $esdtCounts++;
                        $datasetCMR->{$esdtString} =
                            ( exists $existingCMR{$esdtString} ) ?
                            $existingCMR{$esdtString} : 1;
                    }
                    if ($miradorFlag eq 'true') {
                        if ($cmrFlag eq 'true') {
                            # collection metadata from CMR
                            DeployLogging( 'fatal', "COLLECTION attribute is missing for Mirador" .
                                " dataset $dataName version $versionId" )
                                if ((not defined $dataAttr->{COLLECTION_SHORTNAME}) &&
                                    (not defined $versionAttr->{COLLECTION_VERSION}));
                        } else {
                            # collection metadata from GCMD
                            if ( $versionAttr->{ACCESS} eq 'public' ) {
                                DeployLogging( 'fatal', "COLLECTION attribute is missing for Mirador" .
                                    " dataset $dataName version $versionId" )
                                    if ((not defined $dataAttr->{COLLECTION_SHORTNAME}) &&
                                        (not defined $versionAttr->{COLLECTION_VERSION}));
                            }
                        }
                        $esdtCounts++;
                        $datasetMirador->{$esdtString} =
                            ( exists $existingMirador{$esdtString} ) ?
                            $existingMirador{$esdtString} : 1;
                    }
                    if ($dotchartFlag eq 'true') {
                        $esdtCounts++;
                        $datasetDotchart->{$esdtString} =
                            ( exists $existingDotchart{$esdtString} ) ?
                            $existingDotchart{$esdtString} : 1;
                    }
                    if ($giovanniFlag eq 'true') {
                        $esdtCounts++;
                        $datasetGiovanni->{$esdtString} =
                            ( exists $existingGiovanni{$esdtString} ) ?
                            $existingGiovanni{$esdtString} : 1;
                    }
                }
            } else {
                my $esdtString = $dataName . ":";
                my $dotchartFlag = $global->{PUBLISH_DOTCHART} ? 'true' : 'false';
                # Without dataVersion tag and no DIF_ENTRY_ID data is
                # not publishable; considered an error for reconciliation.
                my $echoFlag = $dataAttr->{PUBLISH_ECHO};
                my $cmrFlag = $dataAttr->{PUBLISH_CMR};
                my $miradorFlag = $dataAttr->{PUBLISH_MIRADOR};
                my $giovanniFlag = $dataAttr->{PUBLISH_GIOVANNI};
                # Case of version-less system
                if ($echoFlag eq 'true') {
                    DeployLogging( 'fatal', "DIF_ENTRY_ID is missing for ECHO" .
                        " dataset $dataName" ) if (not defined $dataAttr->{DIF_ENTRY_ID});
                    $esdtCounts++;
                    $datasetECHO->{$esdtString} = ( exists $existingECHO{$esdtString} ) ?
                        $existingECHO{$esdtString} : 1;
                }
                if ($cmrFlag eq 'true') {
                    DeployLogging( 'fatal', "COLLECTION attribute is missing for CMR" .
                        " dataset $dataName" ) if (not defined $dataAttr->{COLLECTION_SHORTNAME});
                    $esdtCounts++;
                    $datasetCMR->{$esdtString} = ( exists $existingCMR{$esdtString} ) ?
                        $existingCMR{$esdtString} : 1;
                }
                if ($miradorFlag eq 'true') {
                    if ($cmrFlag eq 'true') {
                        DeployLogging( 'fatal', "COLLECTION attribute is missing for CMR" .
                            " dataset $dataName" ) if (not defined $dataAttr->{COLLECTION_SHORTNAME});
                    } else {
                        if ( $dataAttr->{ACCESS} eq 'public' ) {
                            DeployLogging( 'fatal', "COLLECTION attribute is missing for Mirador" .
                                " dataset $dataName" ) if (not defined $dataAttr->{COLLECTION_SHORTNAME});
                        }
                    }
                    $esdtCounts++;
                    $datasetMirador->{$esdtString} = ( exists $existingMirador{$esdtString} ) ?
                        $existingMirador{$esdtString} : 1;
                }
                if ($dotchartFlag eq 'true') {
                    $esdtCounts++;
                    $datasetDotchart->{$esdtString} = ( exists $existingDotchart{$esdtString} ) ?
                        $existingDotchart{$esdtString} : 1;
                }
                if ($giovanniFlag eq 'true') {
                    $esdtCounts++;
                    $datasetGiovanni->{$esdtString} = ( exists $existingGiovanni{$esdtString} ) ?
                        $existingGiovanni{$esdtString} : 1;
                }
            }
        }
    }

    # Calculate polling time based on $esdtCounts (jobs to be completed one cycle per day)
    # one cycle per day is too much for dotchart reconciliation. some instance.
    # has more than 100 datasets will be too busy doing this. 
    # change it to one cycle per week.
    my $pollingInterval = 3600;
    $pollingInterval = int (604800/$esdtCounts) unless ($esdtCounts == 0);

    if ( $global->{PUBLISH_ECHO} ) { 
        $downStream->{"REPUBLISH_ECHO"} = [ 'publish_echo' ];
        $downStream->{"PUSH.EchoDel"} = [ 'postoffice' ];
    }

    if ( $global->{PUBLISH_CMR} ) { 
        $downStream->{"REPUBLISH_CMR"} = [ 'publish_cmr' ];
        $downStream->{"DELETE"} = [ 'postoffice' ];
    }

    if ( $global->{PUBLISH_MIRADOR} ) {
        $downStream->{"REPUBLISH_MIRADOR"} = [ 'publish_mirador' ];
        $downStream->{"PUSH.MiradorDel"} = [ 'postoffice' ];
    }

    if ( $global->{PUBLISH_DOTCHART} ) {
        $downStream->{"REPUBLISH_DOTCHART"} = [ 'publish_dotchart' ];
        $downStream->{"PUSH.DotchartDel"} = [ 'postoffice' ];
    }

    if ( $global->{PUBLISH_GIOVANNI} ) {
        $downStream->{"REPUBLISH_GIOVANNI"} = [ 'publish_giovanni' ];
        $downStream->{"PUSH.GiovanniDel"} = [ 'postoffice' ];
    }

    $config = {
        cfg_max_children => 1,
        cfg_station_name => "Reconciliation",
        cfg_root => $global->{S4PA_ROOT},
        cfg_group => $global->{S4PA_GID},
        cfg_max_failures => 1,
        cfg_max_time => 600,
        cfg_polling_interval => $pollingInterval,
        cfg_stop_interval => 4,
        cfg_end_job_interval => 2,
        cfg_restart_defunct_jobs => 1,
	cfg_sort_jobs => 'FIFO',
        cfg_failure_handlers => $failureHandler,
        cfg_commands => $cmdHash,
        cfg_virtual_jobs => $virtualJobs,
        cfg_virtual_feedback => 1,
        cfg_ignore_duplicates => 1,
	cfg_umask => 022,
        cfg_downstream => $downStream,
        __TYPE__ => {
            cfg_failure_handlers => 'HASH',
            cfg_commands => 'HASH',
            cfg_virtual_jobs => 'HASH',
            cfg_downstream => 'HASH',
            }
        };
    # Add an interface to edit station config
    $config->{cfg_interfaces}{'Edit Station Config'} =
      qq( perl -e 'use S4PA; S4PA::EditFile( FILE => "$reconStationDir/station.cfg", TITLE => "Reconciliation" )' );
    # Add an interface to remove Stale log
    $config->{cfg_interfaces}{'Remove Stale Log'} =
      qq( perl -e 'use S4PA; S4PA::RemoveStaleLog( STATION => "$reconStationDir", TITLE => "Reconciliation" )' );
    # Add an interface for retrying all failed jobs
    $config->{cfg_interfaces}{'Retry Failed Jobs'} = qq(s4p_restart_all_jobs.pl);
    $config->{__TYPE__}{cfg_interfaces} = 'HASH';

    if ($esdtCounts) {
        S4PA::CreateStation( $reconStationDir, $config, $logger );
        if ( $global->{PUBLISH_ECHO} ) {
            $configECHO->{cfg_dataset_list} = $datasetECHO;
            $configECHO->{__TYPE__}{cfg_dataset_list} = 'HASH';
            $configECHO->{cfg_ftp_push_quiescent_time} = $echoQuiescent
                if ( defined $echoQuiescent );
            S4PA::WriteStationConfig( 's4pa_recon_ECHO.cfg', $reconStationDir, $configECHO );
        }
        if ( $global->{PUBLISH_CMR} ) {
            $configCMR->{cfg_dataset_list} = $datasetCMR;
            $configCMR->{__TYPE__}{cfg_dataset_list} = 'HASH';
            $configECHO->{cfg_ftp_push_quiescent_time} = $cmrQuiescent
                if ( defined $cmrQuiescent );
            S4PA::WriteStationConfig( 's4pa_recon_CMR.cfg', $reconStationDir, $configCMR );
        }
        if ( $global->{PUBLISH_MIRADOR} ) {
            $configMirador->{cfg_dataset_list} = $datasetMirador;
            $configMirador->{__TYPE__}{cfg_dataset_list} = 'HASH';
            S4PA::WriteStationConfig( 's4pa_recon_Mirador.cfg', $reconStationDir, $configMirador );
        }
        if ( $global->{PUBLISH_DOTCHART} ) {
            $configDotchart->{cfg_dataset_list} = $datasetDotchart;
            $configDotchart->{__TYPE__}{cfg_dataset_list} = 'HASH';
            S4PA::WriteStationConfig( 's4pa_recon_Dotchart.cfg', $reconStationDir, $configDotchart );
        }
        if ( $global->{PUBLISH_GIOVANNI} ) {
            $configGiovanni->{cfg_dataset_list} = $datasetGiovanni;
            $configGiovanni->{__TYPE__}{cfg_dataset_list} = 'HASH';
            S4PA::WriteStationConfig( 's4pa_recon_Giovanni.cfg', $reconStationDir, $configGiovanni );
        }
        return $reconStationDir;
    } else {
        return;
    }
}
###############################################################################
# =head1 CreateStorageStations
# 
# Description
#   Creates storage stations (StoreData, CheckIntegrity, DeleteData) for 
#   provider's data classes.
#
# =cut
###############################################################################
sub CreateStorageStations
{
    my ( $provider, $global ) = @_;
    my $logger = $global->{LOGGER}{LOGGING} if defined $global->{LOGGER}{LOGGING};

    my $providerName = $provider->getAttribute( 'NAME' );
    DeployLogging( 'fatal', "Provider doesn't have a name" )
        unless defined $providerName;

    # Failure handler to retry a failed job.
    my $failureHandler = { 'Retry Job' => "perl -e 'use S4P; S4P::remove_job()"
                            . " if S4P::restart_job()'",
                           'Continue Storing' => "s4pa_store_data.pl -c"
                            . " -f ../s4pa_store_data.cfg DO.* &&"
                            . " send_downstream.pl -l STORE*.log &&"
                            . " remove_job.pl",
                           'Skip Storing' => "s4pa_store_data.pl -s"
                            . " -f ../s4pa_store_data.cfg DO.* &&"
                            . " send_downstream.pl -l STORE*.log &&"
                            . " remove_job.pl",
                         };

    # Holder of station directories
    my @stationDirList = ();
    
    # Master log file
    my $logFile = defined $global->{LOGGER}
        ? "$global->{LOGGER}{DIR}/ingest.log" : undef;
            
    # A hash ref to hold mappings of datasets to data classes, dataset association,
    # and publication switch for datasets.
    my ( $data2class, $dataAssociation, $associationType, $cfgPublish ) = 
        ( {}, {}, {}, {} );
    
    foreach my $dataClass ( $provider->findnodes( 'dataClass' ) ) {
        my $classAttr = GetDataAttributes( $dataClass );
        my $className = $classAttr->{NAME};
        DeployLogging( 'fatal', "Failed to find data class name for provider=$providerName" )
            unless defined $className;

        # Root of all storage station directories for the class
        my $storageClassDir = $global->{S4PA_ROOT} . "storage/$className/";

        # For each dataset of the class
        my ( $timeMargin, $temporalFrequency, $attrXpath, $dataVersion,
            $downStream, $cmdHash, $config, $ignoreXpath, $postStorage,
            $postDeletion, $multipleReplace ) =
            ( {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {} );

        # Station directories for StoreData, DeleteData and CheckIntegrity
        my $stationDir = {};
        foreach my $name ( 'store', 'delete', 'check' ) {
            $stationDir->{$name} = $global->{S4PA_ROOT} 
                . "storage/$className/${name}_$className/";
        } 

        # Set global publishing flag, As long as one class is publishing,
        # we will need to set up the publishing station.
        $global->{PUBLISH_ECHO} =  1
            if ( $classAttr->{PUBLISH_ECHO} eq 'true' );
        $global->{PUBLISH_CMR} =  1
            if ( $classAttr->{PUBLISH_CMR} eq 'true' );
        $global->{PUBLISH_MIRADOR} = 1
            if ( $classAttr->{PUBLISH_MIRADOR} eq 'true' );
        $global->{PUBLISH_GIOVANNI} = 1
            if ( $classAttr->{PUBLISH_GIOVANNI} eq 'true' );
        $global->{PUBLISH_WHOM} = 1
            if ( $classAttr->{PUBLISH_WHOM} eq 'true' );

        foreach my $dataset ( $dataClass->findnodes( 'dataset' ) ) {
            my $dataAttr = GetDataAttributes( $dataset, $classAttr );
            my $documentName = $dataAttr->{DOC} || '';
	    
            # Make sure data name, group are defined.
            my $dataName = $dataAttr->{NAME} || undef;
            DeployLogging( 'fatal', "Failed to find data name for provider=$providerName,"
                . " dataClass=$classAttr->{NAME}" )
                unless defined $dataName;

            my $groupName = $dataAttr->{GROUP} || undef;
            DeployLogging( 'fatal', "Failed to find data group name for provider=$providerName,"
                . " dataClass=$classAttr->{NAME}" )
                unless defined $groupName;
            
            my ( @dataVersionList ) = $dataset->findnodes( 'dataVersion' );
            my $dataDirHash = {};
            my $dataDocHash = {};
            $dataVersion->{$dataName} = [];
            my @dataXpathList = 
                $dataset->getChildrenByTagName( 'uniqueAttribute' );
            my @ignoreXpathList = 
                $dataset->getChildrenByTagName( 'ignoreCondition' );

            # check post storage and deletion tasks
            my ( $postStorageNode ) = $dataset->getChildrenByTagName( 'postStorageTask' );
            my $postStorageTask = GetNodeValue( $postStorageNode )
                if ( defined $postStorageNode );
            my ( $postDeletionNode ) = $dataset->getChildrenByTagName( 'postDeletionTask' );
            my $postDeletionTask = GetNodeValue( $postDeletionNode )
                if ( defined $postDeletionNode );

            if ( @dataVersionList ) {
                foreach my $version ( @dataVersionList ) {
                    my $versionAttr = GetDataAttributes( $version, $dataAttr );
                    my $versionId = $versionAttr->{LABEL} || '';
                    $documentName = $versionAttr->{DOC} || '';
                    if ( $versionAttr->{LABEL} eq '' ) {
                        $dataDirHash->{$dataName} = $versionAttr->{ACCESS};
                        $dataDocHash->{$dataName} = $documentName;
                    } else {
                        $dataDirHash->{"$dataName.$versionId"}
                            = $versionAttr->{ACCESS};
                        $dataDocHash->{"$dataName.$versionId"}
                            = $documentName;
                    }
                    $timeMargin->{$dataName}{$versionAttr->{LABEL}} =
                        $versionAttr->{TIME_MARGIN};
                    $temporalFrequency->{$dataName}{$versionAttr->{LABEL}} =
                        $versionAttr->{FREQUENCY};
                    push( @{$dataVersion->{$dataName}}, $versionId );
                    
                    foreach my $xpathNode (
                        $version->findnodes( 'uniqueAttribute' ), 
                        @dataXpathList ) {
                        $attrXpath->{$dataName}{$versionId} = {}
                            unless defined $attrXpath->{$dataName}{$versionId};
                        my $operator = $xpathNode->getAttribute( 'OPERATOR' );
                        $operator = 'EQ' unless defined $operator;
                        my $xpathValue = GetNodeValue( $xpathNode );          
                        $attrXpath->{$dataName}{$versionId}{$operator} = []
                            unless defined
                            $attrXpath->{$dataName}{$versionId}{$operator};
                        push( @{$attrXpath->{$dataName}{$versionId}{$operator}},
                            $xpathValue );
                    }
                    
                    foreach my $ignoreXpathNode (
                        $version->findnodes( 'ignoreCondition' ), 
                        @ignoreXpathList ) {
                        $ignoreXpath->{$dataName}{$versionId} = {}
                            unless defined $ignoreXpath->{$dataName}{$versionId};
                        my $operator = $ignoreXpathNode->getAttribute( 'OPERATOR' );
                        $operator = 'EQ' unless defined $operator;
                        my $ignoreXpathValue = GetNodeValue( $ignoreXpathNode );          
                        $ignoreXpath->{$dataName}{$versionId}{$operator} = []
                            unless defined
                            $ignoreXpath->{$dataName}{$versionId}{$operator};
                        push( @{$ignoreXpath->{$dataName}{$versionId}{$operator}},
                            $ignoreXpathValue );
                    }

                    # check post storage and deletion tasks
                    my ( $postStorageNode ) = $version->getChildrenByTagName( 'postStorageTask' );
                    if ( defined $postStorageNode ) {
                        $postStorage->{$dataName}{$versionId} = GetNodeValue( $postStorageNode );
                    } elsif ( defined $postStorageTask ) {
                        $postStorage->{$dataName}{$versionId} = $postStorageTask;
                    }
                    my ( $postDeletionNode ) = $version->getChildrenByTagName( 'postDeletionTask' );
                    if ( defined $postDeletionNode ) {
                        $postDeletion->{$dataName}{$versionId} = GetNodeValue( $postDeletionNode );
                    } elsif ( defined $postDeletionTask ) {
                        $postDeletion->{$dataName}{$versionId} = $postDeletionTask;
                    }

                    my ( $associateNode ) = $version->findnodes( 'associateData' );
                    if ( defined $associateNode ) {
                        my $associateKey = ( $versionId eq '' ) ?
                            $dataName : "$dataName.$versionId";

                        my $associateValue = $associateNode->getAttribute( 'NAME' );
                        my $associateVersion = $associateNode->getAttribute( 'VERSION' );
                        my $associateType = $associateNode->getAttribute( 'TYPE' );
                        if ( defined $associateVersion ) {
                            $associateValue .= ".$associateVersion" 
                                unless ( $associateVersion eq '' );
                        }
                        $associateType = 'Browse' unless defined $associateType;
                        $dataAssociation->{$associateKey} = $associateValue;
                        $associationType->{$associateValue} = $associateType;
                    }

                    $versionAttr->{PUBLISH_DOTCHART} = 
                        ( $global->{PUBLISH_DOTCHART} ) ? 'true' : 'false';
                    # As long as one version is publishing, we will need to
                    # set up the publishing station.
                    $global->{PUBLISH_ECHO} = 1 
                        if ( $versionAttr->{PUBLISH_ECHO} eq 'true' );
                    $global->{PUBLISH_ECHO_OPENDAP} = 1 
                        if ( $versionAttr->{PUBLISH_ECHO_OPENDAP} eq 'true' );
                    $global->{PUBLISH_CMR} = 1 
                        if ( $versionAttr->{PUBLISH_CMR} eq 'true' );
                    $global->{PUBLISH_CMR_OPENDAP} = 1 
                        if ( $versionAttr->{PUBLISH_CMR_OPENDAP} eq 'true' );
                    $global->{PUBLISH_MIRADOR} = 1 
                        if ( $versionAttr->{PUBLISH_MIRADOR} eq 'true' );
                    $global->{PUBLISH_GIOVANNI} = 1 
                        if ( $versionAttr->{PUBLISH_GIOVANNI} eq 'true' );
                    $global->{PUBLISH_WHOM} = 1 
                        if ( $versionAttr->{PUBLISH_WHOM} eq 'true' );

                    # check if publish for user is set
                    if ( exists $global->{USER}{DATASET}{$dataName}{$versionId} ) {
                        $versionAttr->{PUBLISH_USER} = 'true';
                    }

                    # check multiple replacement default action
                    $multipleReplace->{$dataName}{$versionId} = $versionAttr->{MULTIPLE_REPLACEMENT};

                    GetStoreDataDownStream( $dataAttr, $versionAttr, $className,
                        $downStream, $cfgPublish );
                }
            } else {
                $dataDirHash->{$dataName} = $dataAttr->{ACCESS};
                $dataDocHash->{$dataName} = $documentName;
                $timeMargin->{$dataName}{''} = $dataAttr->{TIME_MARGIN};
                $temporalFrequency->{$dataName}{''} = $dataAttr->{FREQUENCY};
                push( @{$dataVersion->{$dataName}}, '' );
                
                # Get XPATH expressions used for determining uniqueness of a
                # granule for replacement purposes.
                foreach my $xpathNode ( @dataXpathList ) {
                    $attrXpath->{$dataName}{''} = {}
                        unless defined $attrXpath->{$dataName}{''};
                    my $operator = $xpathNode->getAttribute( 'OPERATOR' );
                    $operator = 'EQ' unless defined $operator;
                    my $xpathValue = GetNodeValue( $xpathNode );
                    $attrXpath->{$dataName}{''}{$operator} = [] 
                        unless defined
                        $attrXpath->{$dataName}{''}{$operator};
                    push( @{$attrXpath->{$dataName}{''}{$operator}},
                        $xpathValue );
                }
                
                # Get XPATH expressions used for determining ignore 
                # condition of a incoming granule.
                foreach my $ignoreXpathNode ( @ignoreXpathList ) {
                    $ignoreXpath->{$dataName}{''} = {}
                        unless defined $ignoreXpath->{$dataName}{''};
                    my $operator = $ignoreXpathNode->getAttribute( 'OPERATOR' );
                    $operator = 'EQ' unless defined $operator;
                    my $ignoreXpathValue = GetNodeValue( $ignoreXpathNode );
                    $ignoreXpath->{$dataName}{''}{$operator} = [] 
                        unless defined
                        $ignoreXpath->{$dataName}{''}{$operator};
                    push( @{$ignoreXpath->{$dataName}{''}{$operator}},
                        $ignoreXpathValue );
                }

                # check post storage and deletion tasks
                $postStorage->{$dataName}{''} = $postStorageTask if ( defined $postStorageTask );
                $postDeletion->{$dataName}{''} = $postDeletionTask if ( defined $postDeletionTask );

                my ( $associateNode ) = $dataset->findnodes( 'associateData' );
                if ( defined $associateNode ) {
                    my $associateValue = $associateNode->getAttribute( 'NAME' );
                    my $associateVersion = $associateNode->getAttribute( 'VERSION' );
                    my $associateType = $associateNode->getAttribute( 'TYPE' );
                    if ( defined $associateVersion ) {
                        $associateValue .= ".$associateVersion" 
                            unless ( $associateVersion eq '' );
                    }
                    $associateType = 'Browse' unless defined $associateType;
                    $dataAssociation->{$dataName} = $associateValue;
                    $associationType->{$associateValue} = $associateType;
                }

                $dataAttr->{PUBLISH_DOTCHART} = 
                    ( $global->{PUBLISH_DOTCHART} ) ? 'true' : 'false';
                # As long as one dataset is publishing, we will need to
                # set up the publishing station.
                $global->{PUBLISH_ECHO} = 1 
                    if ( $dataAttr->{PUBLISH_ECHO} eq 'true' );
                $global->{PUBLISH_ECHO_OPENDAP} = 1 
                    if ( $dataAttr->{PUBLISH_ECHO_OPENDAP} eq 'true' );
                $global->{PUBLISH_CMR} = 1 
                    if ( $dataAttr->{PUBLISH_CMR} eq 'true' );
                $global->{PUBLISH_CMR_OPENDAP} = 1 
                    if ( $dataAttr->{PUBLISH_CMR_OPENDAP} eq 'true' );
                $global->{PUBLISH_MIRADOR} = 1 
                    if ( $dataAttr->{PUBLISH_MIRADOR} eq 'true' );
                $global->{PUBLISH_GIOVANNI} = 1 
                    if ( $dataAttr->{PUBLISH_GIOVANNI} eq 'true' );
                $global->{PUBLISH_WHOM} = 1 
                    if ( $dataAttr->{PUBLISH_WHOM} eq 'true' );

                # check if publish for user is set
                if ( exists $global->{USER}{DATASET}{$dataName}{''} ) {
                    $dataAttr->{PUBLISH_USER} = 'true';
                }

                # check multiple replacement default action
                $multipleReplace->{$dataName}{''} = $dataAttr->{MULTIPLE_REPLACEMENT};

                GetStoreDataDownStream( $dataAttr, undef, $className,
                    $downStream, $cfgPublish );
            }
                        
            foreach my $item ( keys %$dataDirHash ) {
                # Create FTP directory for the dataset
                my $dataFtpDir = $global->{STORAGE_DIR} . "$groupName/$item/";
                unless ( -d $dataFtpDir ) {
                    S4PA::CreateDir( $dataFtpDir, $logger );
                    if ( $dataDirHash->{$item} eq 'public' ) {
                    } elsif ( $dataDirHash->{$item} eq 'hidden' ) {
                        chmod( 0700, $dataFtpDir );
                    } elsif ( $dataDirHash->{$item} eq 'restricted' ) {
                        chmod( 0755, $dataFtpDir );
                        my $htaccessFile = $dataFtpDir . "/.htaccess";
                        if ( open( FH, ">$htaccessFile" ) ) {
                            print FH<<HTACCESS;
Order deny,allow
Deny from all
Allow from 127.0.0.1
HTACCESS
                            DeployLogging( 'error', "Failed to create $htaccessFile" )
                                unless ( close FH );
                        } else {
                            DeployLogging( 'error', "Failed to create $htaccessFile" );
                        }
                    }
                }

                # Create document directory and fetch document if required
                if ($dataDocHash->{$item}) {
                    my $documentDir = $global->{STORAGE_DIR} . "$groupName/$item/doc";
                    unless ( -d $documentDir ) {
                        S4PA::CreateDir( $documentDir, $logger )
                    }
                    my $fromUrl = $global->{DOCUMENT_DIR} . "$dataDocHash->{$item}";
                    my $toUrl = $documentDir . "/$dataDocHash->{$item}";
                    my $status = getstore($fromUrl, $toUrl);
                    if (is_error($status)) {
                        DeployLogging( 'fatal', "Failed fetching $fromUrl to $toUrl ($status)" );
                    } else {
                        DeployLogging( 'info', "File $fromUrl fetched to $toUrl" );
                    }
                }
                                
                # Create StoreData station directories
                my $storeDataDir = $storageClassDir . "$item/";
                S4PA::CreateDir( $storeDataDir, $logger );
                
                # Create data link
                unless ( -l "$storeDataDir/data" ) {
                    symlink( $dataFtpDir, "$storeDataDir/data" )
                        || DeployLogging( 'fatal', "Failed to create data link for '$item' ($!)" );
                }
                my $granuleDbFile = "$storeDataDir/granule.db";
                unless ( -f $granuleDbFile ) {
                    my ( $granRef, $fileHandle ) = 
                        S4PA::Storage::OpenGranuleDB( $granuleDbFile, "w" );
                    if ( defined $granRef ) {
                        S4PA::Storage::CloseGranuleDB( $granRef, $fileHandle );
                        DeployLogging( 'info', "Creating $granuleDbFile" );
                    } else {
                        DeployLogging( 'error', "Failed to create $granuleDbFile" );
                    }
                }
            }
            
            # Acquire data attributes for station configuration
            $data2class->{$dataName} = $className;
        }
        
        # Create StoreData station
        # Write the station configuration file
        $cmdHash = { 
            '.*' => 's4pa_store_data.pl -f ../s4pa_store_data.cfg' };
        $config = {
            cfg_max_children => 1,
            cfg_station_name => "StoreData: $className",
            cfg_root => $global->{S4PA_ROOT},
            cfg_group => $global->{S4PA_GID},
            cfg_max_failures => 2,
	    cfg_sort_jobs => 'FIFO',
            cfg_max_time => 600,
            cfg_polling_interval => 30,
            cfg_stop_interval => 4,
            cfg_end_job_interval => 2,
            cfg_restart_defunct_jobs => 1,
            cfg_failure_handlers => $failureHandler,
            cfg_commands => $cmdHash,
            cfg_downstream => $downStream,
            cfg_output_work_order_suffix => 'PDR',
            cfg_umask => 022,
            cfg_ignore_duplicates => 1,
            __TYPE__ => {
                cfg_failure_handlers => 'HASH',
                cfg_commands => 'HASH',
                cfg_downstream => 'HASH'
                }        
            };
        # Add an interface to edit station config
        $config->{cfg_interfaces}{'Edit Station Config'} =
          qq( perl -e 'use S4PA; S4PA::EditFile( FILE => "$stationDir->{store}/station.cfg", TITLE => "StoreData: $className" )' );
        # Add an interface to remove Stale log
        $config->{cfg_interfaces}{'Remove Stale Log'} =
          qq( perl -e 'use S4PA; S4PA::RemoveStaleLog( STATION => "$stationDir->{store}", TITLE => "StoreData: $className" )' );
        $config->{cfg_interfaces}{'Manage Work Orders'} =
            qq( perl -e 'use S4PA; S4PA::ManageWorkOrder( DIR => "$stationDir->{store}", TITLE => "StoreData: $className" )' );
        # Add an interface for retrying all failed jobs
        $config->{cfg_interfaces}{'Retry Failed Jobs'} = qq(s4p_restart_all_jobs.pl);
        $config->{__TYPE__}{cfg_interfaces} = 'HASH';

        S4PA::CreateStation( $stationDir->{store}, $config, $logger );
        # Create StoreData specific config file
        $config = {
            cfg_xpath => $attrXpath,
            cfg_ignore_xpath => $ignoreXpath,
            cfg_time_margin => $timeMargin,
            cfg_post_storage => $postStorage,
            cfg_temporal_frequency => $temporalFrequency,
            cfg_data_version => $dataVersion,
            __TYPE__ => {
                cfg_time_margin => 'HASH',
                cfg_post_storage => 'HASH',
                cfg_temporal_frequency => 'HASH',
                cfg_xpath => 'HASH',
                cfg_ignore_xpath => 'HASH',
                cfg_data_version => 'HASH'
                },
            };
        if ( defined $global->{LOGGER} ) {
            $config->{cfg_logger} = {
                LEVEL => $global->{LOGGER}{LEVEL},
                FILE => $logFile };
            $config->{__TYPE__}{cfg_logger} = 'HASH';
        }

        if ($multipleReplace) {
            $config->{cfg_multiple_replacement} = $multipleReplace;
            $config->{__TYPE__}{cfg_multiple_replacement} = 'HASH';
        }
        S4PA::WriteStationConfig( 's4pa_store_data.cfg', $stationDir->{store}, 
            $config );
        push( @stationDirList, $stationDir->{store} );
        # Create CheckIntegrity station        
        $cmdHash = {};
        my $cfg_virtual_jobs = {};
        foreach my $key ( keys %$timeMargin ) {
            $cmdHash->{"CHECK_$key"} = "$global->{INTEGRITY_CMD} s4pa_check_integrity.pl"
                . " -s -c -f ../s4pa_check_integrity.cfg $key";
            $cmdHash->{"CHECK_ACTIVE_FS"} = "s4pa_check_integrity.pl"
                . " -c -f ../s4pa_check_integrity.cfg";
            $cfg_virtual_jobs->{"CHECK_$key"} = 1;
        }
        my $checkIntegrityFailureHandler = {
            'Remove Job' => "perl -e 'use S4P; S4P::remove_job();'",
            'Retry Job' => "perl -e 'use S4P; S4P::remove_job()"
                            . " if S4P::restart_job()'"
        };
        $config = {
                   cfg_max_children => 1,
                   cfg_station_name => "CheckIntegrity: $className",
                   cfg_root => $stationDir->{check},
                   cfg_group => $global->{S4PA_GID},
                   cfg_max_time => 7200,
                   cfg_polling_interval => 86400,
                   cfg_stop_interval => 4,
                   cfg_end_job_interval => 2,
                   cfg_restart_defunct_jobs => 1,
		   cfg_sort_jobs => 'FIFO',
                   cfg_failure_handlers => $checkIntegrityFailureHandler,
                   cfg_commands => $cmdHash,
                   cfg_virtual_jobs => $cfg_virtual_jobs,
                   cfg_virtual_feedback => 1,
		   cfg_umask => 022,
                   __TYPE__ => {
                                cfg_failure_handlers => 'HASH',
                                cfg_commands => 'HASH',
                                cfg_virtual_jobs => 'HASH',
                               }
                  };
        # Add an interface to edit station config
        $config->{cfg_interfaces}{'Edit Station Config'} =
          qq( perl -e 'use S4PA; S4PA::EditFile( FILE => "$stationDir->{check}/station.cfg", TITLE => "CheckIntegrity: $className" )' );
        # Add an interface to remove Stale log
        $config->{cfg_interfaces}{'Remove Stale Log'} =
          qq( perl -e 'use S4PA; S4PA::RemoveStaleLog( STATION => "$stationDir->{check}", TITLE => "CheckIntegrity: $className" )' );
        # Add an interface for retrying all failed jobs
        $config->{cfg_interfaces}{'Retry Failed Jobs'} = qq(s4p_restart_all_jobs.pl);
        $config->{__TYPE__}{cfg_interfaces} = 'HASH';

        # Set the polling interval such that the system recycles in a day.
        $config->{cfg_polling_interval} = 86400./scalar( keys %$timeMargin)
            if ( keys %$timeMargin );
        S4PA::CreateStation( $stationDir->{check}, $config, $logger );
        push( @stationDirList, $stationDir->{check} );
        
        # Write out s4pa_check_integrity.cfg for use in CheckIntegrity station
        $config = {
            cfg_data_version => $dataVersion,
            __TYPE__ => {
                cfg_data_version => 'HASH' }
            };
        S4PA::WriteStationConfig( 's4pa_check_integrity.cfg',
            $stationDir->{check}, $config );        
        
        # Create DeleteData station
        $cmdHash = { 
            'INTRA_VERSION_DELETE' => "s4pa_delete_data.pl"
                . " -f $stationDir->{delete}" . "s4pa_delete_data.cfg"
                . " -d $stationDir->{delete}" . "intra_version_pending -t "
		. $global->{DELETION}{INTRA_VERSION},
            'INTER_VERSION_DELETE' => "s4pa_delete_data.pl"
                . " -f $stationDir->{delete}" . "s4pa_delete_data.cfg"
                . " -d $stationDir->{delete}" . "inter_version_pending -t "
		. $global->{DELETION}{INTER_VERSION} };

        $cfg_virtual_jobs = {
            INTRA_VERSION_DELETE => 1,
            INTER_VERSION_DELETE => 1,
            };

        # Failure handler for delete station to retry a failed job.
        my $failureHandler = { 'Retry Job' => "perl -e 'use S4P; S4P::remove_job()"
                            . " if S4P::restart_job()'" };
        $config = {
                   cfg_max_children => 2,
                   cfg_station_name => "DeleteData: $className",
                   cfg_root => $stationDir->{delete},
                   cfg_group => $global->{S4PA_GID},
                   cfg_max_failures => 1,
                   cfg_max_time => 600,
		   cfg_sort_jobs => 'FIFO',
                   cfg_polling_interval => $global->{DELETION}{INTRA_VERSION},
                   cfg_stop_interval => 4,
                   cfg_end_job_interval => 2,
                   cfg_restart_defunct_jobs => 1,
                   cfg_failure_handlers => $failureHandler,
                   cfg_commands => $cmdHash,
                   cfg_virtual_jobs => $cfg_virtual_jobs,
                   cfg_virtual_feedback => 1,
                   cfg_ignore_duplicates => 1,
		   cfg_umask => 022,
                   __TYPE__ => {
                                cfg_failure_handlers => 'HASH',
                                cfg_commands => 'HASH',
                                cfg_downstream => 'HASH',
                                cfg_virtual_jobs => 'HASH',
                               }
                  };
        $config->{cfg_polling_interval} = $global->{DELETION}{INTER_VERSION}
            if ( $global->{DELETION}{INTER_VERSION}
                 < $global->{DELETION}{INTRA_VERSION} );

        # Add an interface to delete all intra or inter_version_pending now
        $config->{cfg_interfaces} = {
            'Process INTRA_VERSION_DELETE' => "s4pa_delete_data.pl" 
                . " -f $stationDir->{delete}" . "s4pa_delete_data.cfg"
                . " -d $stationDir->{delete}" . "intra_version_pending -t NOW",
            'Process INTER_VERSION_DELETE' => "s4pa_delete_data.pl"
                . " -f $stationDir->{delete}" . "s4pa_delete_data.cfg"
                . " -d $stationDir->{delete}" . "inter_version_pending -t NOW"
            };
        # Add an interface to edit station config
        $config->{cfg_interfaces}{'Edit Station Config'} =
          qq( perl -e 'use S4PA; S4PA::EditFile( FILE => "$stationDir->{delete}/station.cfg", TITLE => "DeleteData: $className" )' );
        # Add an interface to remove Stale log
        $config->{cfg_interfaces}{'Remove Stale Log'} =
          qq( perl -e 'use S4PA; S4PA::RemoveStaleLog( STATION => "$stationDir->{delete}", TITLE => "DeleteData: $className" )' );
        # Add an interface for retrying all failed jobs
        $config->{cfg_interfaces}{'Retry Failed Jobs'} = qq(s4p_restart_all_jobs.pl);
        $config->{__TYPE__}{cfg_interfaces} = 'HASH';

        S4PA::CreateStation( $stationDir->{delete}, $config, $logger );
        $config = {
            cfg_root => $global->{S4PA_ROOT},
            cfg_post_deletion => $postDeletion,
            __TYPE__ => {
                         cfg_post_deletion => 'HASH'
                        }
            };
        if ( defined $global->{LOGGER} ) {
            $config->{cfg_logger} = {
                LEVEL => $global->{LOGGER}{LEVEL},
                FILE => $logFile };
            $config->{__TYPE__}{cfg_logger} = 'HASH';
        }
        S4PA::WriteStationConfig( 's4pa_delete_data.cfg', 
                $stationDir->{delete}, $config );        
        push( @stationDirList, $stationDir->{delete} );                
    }

    if ( -f "$global->{S4PA_ROOT}/storage/dataset.cfg" ) {
        my $cpt = Safe->new( 'CFG' );
        $cpt->share( '%data_class', '%data_association', 
            '%association_type', '%cfg_publication' );
        if ( $cpt->rdo( "$global->{S4PA_ROOT}/storage/dataset.cfg" ) ) {
            foreach my $key ( keys %CFG::data_class ) {
                next if defined $data2class->{$key};
                $data2class->{$key} = $CFG::data_class{$key};
            }
            foreach my $key ( keys %CFG::data_association ) {
                next if defined $dataAssociation->{$key};
                $dataAssociation->{$key} = $CFG::data_association{$key};
            }
            foreach my $key ( keys %CFG::association_type ) {
                next if defined $associationType->{$key};
                $associationType->{$key} = $CFG::association_type{$key};
            }
            foreach my $key ( keys %CFG::cfg_publication ) {
                next if defined $cfgPublish->{$key};
                $cfgPublish->{$key} = $CFG::cfg_publication{$key};
            }
        }
    }    
    my $rootUrl = {};
    $rootUrl->{public} = $global->{URL}{FTP} if defined $global->{URL}{FTP};
    if ( defined $global->{URL}{HTTP} ) {
     $rootUrl->{restricted} = $global->{URL}{HTTP};
     $rootUrl->{hidden} = $global->{URL}{HTTP};
    }

    # Write out the dataset->data class mappings
    my $classMap = { data_class => $data2class,
                     data_association => $dataAssociation,
                     association_type => $associationType,
                     cfg_root_url => $rootUrl,
                     cfg_publication => $cfgPublish,
                     cfg_publish_dotchart => $global->{PUBLISH_DOTCHART},
                     __TYPE__ => {
                        data_class => 'HASH',
                        data_association => 'HASH',
                        association_type => 'HASH',
                        cfg_root_url => 'HASH',
                        cfg_publication => 'HASH',
                        }
                    };
    S4PA::WriteStationConfig( 'dataset.cfg', 
        "$global->{S4PA_ROOT}/storage", $classMap );

    my $backupRoot = $global->{RESERVE_DIR};
    # preserve if reserve root directory already defined in configuration
    if (not defined $backupRoot) {
        if ( -f "$global->{S4PA_ROOT}/auxiliary_backup.cfg" ) {
            my $cpt = Safe->new( 'CFG' );
            $cpt->share( '%cfg_auxiliary_backup_root' );
            if ( $cpt->rdo( "$global->{S4PA_ROOT}/auxiliary_backup.cfg" ) ) {
                $backupRoot = $CFG::cfg_reserve_root;
            }
        }
    }
    # Write out file reservation configuration if directory defined
    if (defined $backupRoot) {
        my $reserveDir = { cfg_auxiliary_backup_root => $backupRoot };
        S4PA::WriteStationConfig( 'auxiliary_backup.cfg',
            "$global->{S4PA_ROOT}", $reserveDir );
        DeployLogging( 'error', "Failed to create $backupRoot" )
            unless S4PA::CreateDir( $backupRoot, $logger );
    }

    return @stationDirList;
}
###############################################################################
# =head1 CreateReceiveDataStation
# 
# Description
#   Creates ReceiveData stations for a provider.
#
# =cut
###############################################################################
sub CreateReceiveDataStation
{
    my ( $provider, $global ) = @_;
    my $logger = $global->{LOGGER}{LOGGING} if defined $global->{LOGGER}{LOGGING};
    
    my $providerName = $provider->getAttribute( 'NAME' );
    DeployLogging( 'fatal', "Provider doesn't have a name" )
        unless defined $providerName;

    my $recvDataDir = $global->{S4PA_ROOT} . "receiving/$providerName/";

    # Failure handler to retry a failed job.
    my $failureHandler = {
        'Retry Job' =>
            "perl -e 'use S4PA::Receiving; S4PA::Receiving::FailureHandler();'",
        'Remove Job' => "perl -e 'use S4P; S4P::remove_job()'" };
    
    # Create local directory for writing PANs.
    my $localPanDir = GetNodeValue( $provider, 'pan/local' );
    S4PA::CreateDir( $localPanDir, $logger )
        if defined $localPanDir;

    # Master log file
    my $logFile = defined $global->{LOGGER}
        ? "$global->{LOGGER}{DIR}/ingest.log" : undef;
    
    my $collectionInfo = $global->{CMR}{COLLECTION_INFO};
    # Create a ReceiveData station for each provider
    my ( $downStream, $dataAccess, $metaMethod, $compressMethod, 
        $decompressMethod, $dataToDif, $mapMethod, $collectionLink )
        = ( {}, {}, {}, {}, {}, {}, {}, {} );
    # For each data class
    foreach my $dataClass ( $provider->findnodes( 'dataClass' ) ) {
        my $classAttr = GetDataAttributes( $dataClass );
        my $className = $classAttr->{NAME};
        DeployLogging( 'fatal', "Failed to find data class name for provider=$providerName" )
            unless defined $className;
        # For each dataset of the class
        foreach my $dataset ( $dataClass->findnodes( 'dataset' ) ) {
            my $dataAttr = GetDataAttributes( $dataset, $classAttr );
            my $dataName = $dataAttr->{NAME} || undef;
            DeployLogging( 'fatal', "Failed to find data name for provider=$providerName,"
                . " dataClass=$classAttr->{NAME}" ) unless defined $dataName;
            
            my ( @dataVersionList ) = $dataset->findnodes( 'dataVersion' );
            $dataAccess->{$dataName} = {};
            if ( @dataVersionList ) {
                foreach my $version ( @dataVersionList ) {
                    my $versionId = $version->getAttribute( 'LABEL' );
                    my $versionAttr = GetDataAttributes( $version, $dataAttr );
                    if ( defined $versionAttr->{ACCESS} ) {
                        $dataAccess->{$dataName}{$versionId} =
                            ($versionAttr->{ACCESS} eq 'restricted') ? 0640
                            : ($versionAttr->{ACCESS} eq 'hidden') ? 0600: 0644;
                    }

                    # check if a CMR collection metadata is specified
                    my $fetchFlag = 0;
                    my ( $collectionShortname, $collectionVersion );
                    if ( defined $versionAttr->{COLLECTION_SHORTNAME} ) {
                        # set collection fetching flag if collection_shortname is specified
                        $collectionShortname = $versionAttr->{COLLECTION_SHORTNAME};
                        $fetchFlag = 1;
                    } else {
                        # otherwise, use dataset name
                        $collectionShortname = $dataName;
                    }
                    if ( defined $versionAttr->{COLLECTION_VERSION} ) {
                        # set collection fetching flag if collection_version is specified
                        $collectionVersion = $versionAttr->{COLLECTION_VERSION};
                        $fetchFlag = 1;
                    } else {
                        # otherwise, use dataset version label
                        $collectionVersion = $versionId;
                    }

                    my $conceptId;
                    if (exists $collectionInfo->{$providerName}{$className}{$dataName}{$versionId}) {
                        my $link = $collectionInfo->{$providerName}{$className}{$dataName}{$versionId}{'link'};
                        $collectionLink->{$dataName}{$versionId} = $link;
                        if ($link eq 'CMR') {
                            $conceptId = $collectionInfo->{$providerName}{$className}{$dataName}{$versionId}{'concept_id'};
                        }
                    } else {
                        $collectionLink->{$dataName}{$versionId} = 'S4PA';
                    }

                    # if ($fetchFlag) {
                    #     $collectionLink->{$dataName} = {} if (not exists $collectionLink->{$dataName});
                    #     # default collection metadata link to S4PA collection
                    #     if (defined $versionAttr->{COLLECTION_LINK}) {
                    #         $collectionLink->{$dataName}{$versionId} = $versionAttr->{COLLECTION_LINK};
                    #     } else {
                    #         $collectionLink->{$dataName}{$versionId} = 'S4PA';
                    #     }
                    # }

                    # CMR collection metadata is being fetched
                    if ( ($versionAttr->{ACCESS} ne 'hidden') && $fetchFlag ) {
                        # CMR entry_id is usually <shortname>_<version>
                        my $entry_id = $collectionShortname . '_' . $collectionVersion;
                        $dataToDif->{$dataName}{$versionId}{url}
                            = ($versionAttr->{ACCESS} eq 'public' 
                                ? $global->{URL}{FTP} : $global->{URL}{HTTP})
                                . $dataAttr->{GROUP}
                                . "/$dataName" 
                                . ( $versionId ne '' ? ".$versionId/" : '/' )
                                . $entry_id . "_dif.xml";
			$dataToDif->{$dataName}{$versionId}{path}
			    = $global->{STORAGE_DIR} . "/" . $dataAttr->{GROUP}
				. "/$dataName"
				. ( $versionId ne '' ? ".$versionId/" : '/' )
				. $entry_id . "_dif.xml";
                        if (defined $conceptId) {
                            my $uri = $global->{CMR}{CMR_ENDPOINT_URI};
                            $uri =~ s/\/+$//;
			    $dataToDif->{$dataName}{$versionId}{cmrUrl}
                                = $uri . "/search/concepts/" . $conceptId;
                        }

                    # GCMD DIF collection metadata is being fetched
                    } elsif ( ($versionAttr->{ACCESS} ne 'hidden') 
                        && defined $versionAttr->{DIF_ENTRY_ID} ) {
                        $dataToDif->{$dataName}{$versionId}{url}
                            = ($versionAttr->{ACCESS} eq 'public' 
                                ? $global->{URL}{FTP} : $global->{URL}{HTTP})
                                . $dataAttr->{GROUP}
                                . "/$dataName" 
                                . ( $versionId ne '' ? ".$versionId/" : '/' )
                                . $versionAttr->{DIF_ENTRY_ID} . "_dif.xml";
			$dataToDif->{$dataName}{$versionId}{path}
			    = $global->{STORAGE_DIR} . "/" . $dataAttr->{GROUP}
				. "/$dataName"
				. ( $versionId ne '' ? ".$versionId/" : '/' )
				. $versionAttr->{DIF_ENTRY_ID} . "_dif.xml";
                    }

                    if ( $versionAttr->{WITH_HDF4MAP} eq 'true' ) {
                        if ( defined $dataAttr->{METHOD}{HDF4MAP} ) {
                            $mapMethod->{$dataName}{$versionId} = 
                                $dataAttr->{METHOD}{HDF4MAP};
                        } else {
                            $mapMethod->{$dataName}{$versionId} = 
                                "s4pa_create_h4map.pl -f -z";
                        }
                    }
                }
            } else {
                # Case of version-less system.
                $dataAccess->{$dataName}{''} =
                    ( $dataAttr->{ACCESS} eq 'restricted' ) ? 0640
                    : ( $dataAttr->{ACCESS} eq 'hidden' ) ? 0600 : 0644;

                if (defined $dataAttr->{COLLECTION_SHORTNAME}) {
                    if (defined $dataAttr->{COLLECTION_LINK}) {
                        $collectionLink->{$dataName}{''} = $dataAttr->{COLLECTION_LINK};
                    } else {
                        $collectionLink->{$dataName}{''} = 'S4PA';
                    }
                }

		if ( ($dataAttr->{ACCESS} ne 'hidden') 
		    && defined $dataAttr->{DIF_ENTRY_ID} ) {
		    $dataToDif->{$dataName}{''}{url}
			= ($dataAttr->{ACCESS} eq 'public' 
			    ? $global->{URL}{FTP} : $global->{URL}{HTTP})
			    . $dataAttr->{GROUP}
			    . "/$dataName/" 
			    . $dataAttr->{DIF_ENTRY_ID} . "_dif.xml";
		    $dataToDif->{$dataName}{''}{path} = $global->{STORAGE_DIR} . "/"
			    . $dataAttr->{GROUP}
			    . "/$dataName/" . $dataAttr->{DIF_ENTRY_ID} . "_dif.xml";
		}
                if ( $dataAttr->{WITH_HDF4MAP} eq 'true' ) {
                    if ( defined $dataAttr->{METHOD}{HDF4MAP} ) {
                        $mapMethod->{$dataName}{''} = 
                            $dataAttr->{METHOD}{HDF4MAP};
                    } else {
                        $mapMethod->{$dataName}{''} = 
                            "s4pa_create_h4map.pl -f -z";
                    }
                }
            }

	    # Collect data access, methods for the class
            
	    $metaMethod->{$dataName} = $dataAttr->{METHOD}{METADATA}
		if ( defined $dataAttr->{METHOD}{METADATA} );
	    $compressMethod->{$dataName}{Cmd} 
		= $dataAttr->{METHOD}{COMPRESSION}{COMMAND}
		if ( defined $dataAttr->{METHOD}{COMPRESSION}{COMMAND} );
	    $compressMethod->{$dataName}{TmpOut} 
		= $dataAttr->{METHOD}{COMPRESSION}{TMPFILE}
		if ( defined $dataAttr->{METHOD}{COMPRESSION}{TMPFILE} );
	    $compressMethod->{$dataName}{Outfile} 
		= $dataAttr->{METHOD}{COMPRESSION}{OUTPUT}
		if ( defined $dataAttr->{METHOD}{COMPRESSION}{OUTPUT} );
	    $decompressMethod->{$dataName}{Cmd} 
		= $dataAttr->{METHOD}{DECOMPRESSION}{COMMAND}
		if ( defined $dataAttr->{METHOD}{DECOMPRESSION}{COMMAND} );
	    $decompressMethod->{$dataName}{TmpOut} 
		= $dataAttr->{METHOD}{DECOMPRESSION}{TMPFILE}
		if ( defined $dataAttr->{METHOD}{DECOMPRESSION}{TMPFILE} );
	    $decompressMethod->{$dataName}{Outfile} 
		= $dataAttr->{METHOD}{DECOMPRESSION}{OUTPUT}
		if ( defined $dataAttr->{METHOD}{DECOMPRESSION}{OUTPUT} );
	    $downStream->{"STORE_$dataName"} 
		= [ "storage/$className/store_$className" ];
	}
    }
    
    $downStream->{"PUSH"} = [ "postoffice" ];
    $downStream->{"RETRY"} = [ "receiving/$providerName" ];
    
    # Create ReceiveData station    
    my $cmdHash = { 
        '.*' => "s4pa_recv_data.pl -f ../s4pa_recv_data.cfg -p $localPanDir" };
    # A hash ref to hold station configuration.
    my $stationConfig = {
	cfg_station_name =>  "ReceiveData: $providerName",
        cfg_root => $global->{S4PA_ROOT},
        cfg_group => $global->{S4PA_GID},
        cfg_max_time => 600,
        cfg_sort_jobs => 'FIFO',
        cfg_max_failures => 2,
        cfg_polling_interval => 10,
        cfg_stop_interval => 4,
        cfg_end_job_interval => 2,
        cfg_restart_defunct_jobs => 1,
        cfg_max_children => 1,
        cfg_failure_handlers => $failureHandler,
        cfg_commands => $cmdHash,
        cfg_downstream => $downStream,
        cfg_work_order_pattern => '*.{PDR,EDR,wo}',
        cfg_umask => 022,
        cfg_ignore_duplicates => 1,
        __TYPE__ => {
	    cfg_failure_handlers => 'HASH',
	    cfg_commands => 'HASH',
	    cfg_downstream => 'HASH'
	    }
	};
    # Add an interface to retry all failed jobs
    $stationConfig->{cfg_interfaces} = {
        'Retry Failed Jobs' =>
            qq(s4p_restart_all_jobs.pl \"perl -e 'use S4PA::Receiving; S4PA::Receiving::FailureHandler(\"auto_restart\")'\")
        };
    # Add an interface to view logs if logging is enabled
    $stationConfig->{cfg_interfaces}{'View Log'} =
        qq( perl -e 'use S4PA; S4PA::ViewFile( FILE => "$logFile" )' )
        if ( defined $logFile );
    # Add an interface to edit station config
    $stationConfig->{cfg_interfaces}{'Edit Station Config'} =
      qq( perl -e 'use S4PA; S4PA::EditFile( FILE => "$recvDataDir/station.cfg", TITLE => "ReceiveData: $providerName" )' );
    # Add an interface to remove Stale log
    $stationConfig->{cfg_interfaces}{'Remove Stale Log'} =
      qq( perl -e 'use S4PA; S4PA::RemoveStaleLog( STATION => "$recvDataDir", TITLE => "ReceiveData: $providerName" )' );
    $stationConfig->{cfg_interfaces}{'Manage Work Orders'} =
        qq( perl -e 'use S4PA; S4PA::ManageWorkOrder( DIR => "$recvDataDir", TITLE => "ReceiveData: $providerName" )' );
    $stationConfig->{__TYPE__}{cfg_interfaces} = 'HASH';
    # Create the station
    S4PA::CreateStation( $recvDataDir, $stationConfig, $logger );

    # Get information on PAN destinations
    my $panPush = {};
    my $panFilter;

    # looking for provider specific pan filter
    foreach my $node ( $provider->findnodes('pan/remote/panFilter') ) {
        $panFilter = $node->textContent();
    }
    foreach my $node ( $provider->findnodes('pan/remote/originating_system') ) {
        my $name = $node->getAttribute( 'NAME' );
        my $host = $node->getAttribute( 'HOST' );
        my $dir = $node->getAttribute( 'DIR' );
        my $notify = $node->getAttribute( 'NOTIFY' );
        # get originating specific pan filter
        my $pan_filter = $node->getAttribute( 'PAN_FILTER' );
        DeployLogging( 'fatal', "Failed to get originating system's name for sending PANs" )
            unless defined $name;

        unless ( defined $notify ) {
            DeployLogging( 'fatal', "Failed to get host name for sending PANs for "
                . "originating_system=$name" )
                unless defined $host;
            DeployLogging( 'fatal', "Failed to get dir for sending PANs for "
                . "originating_system=$name, host=$host" )
                unless defined $dir;
        }
        $panPush->{$name} = { host => $host, dir => $dir, notify => $notify };

        # add originating system specific pan filter script if defined
        if ( defined $pan_filter ) {
            $panPush->{$name}{'panFilter'} = $pan_filter;
        # add provider specific pan filter script if defined
        } elsif ( defined $panFilter ) {
            $panPush->{$name}{'panFilter'} = $panFilter;
        }
    }
    
    # Get the active file system for the provider and disk usage limits
    my ( $activeFsNode ) = $provider->findnodes( 'activeFileSystem' );
    my $fileSizeMargin = $activeFsNode->getAttribute( 'FILE_SIZE_MARGIN' );
    my $diskLimit = { max => $activeFsNode->getAttribute( 'MAX' ) };
    my $configVolume = $activeFsNode->getAttribute( 'CONFIGURED_VOLUMES' );
    my $activeFs = GetNodeValue( $provider, 'activeFileSystem' );
    my $rootUrl = {};
    $rootUrl->{public} = $global->{URL}{FTP} if defined $global->{URL}{FTP};
    if ( defined $global->{URL}{HTTP} ) {
     $rootUrl->{restricted} = $global->{URL}{HTTP};
     $rootUrl->{hidden} = $global->{URL}{HTTP};
    }

    # Write out ReceiveData station specific configuration.
    my $config = {
        cfg_root => $global->{S4PA_ROOT},
        cfg_data_to_dif => $dataToDif,
        cfg_root_url => $rootUrl,
	cfg_metadata_methods => $metaMethod,
	cfg_hdf4map_methods => $mapMethod,
	cfg_compress => $compressMethod,
	cfg_uncompress => $decompressMethod,
	cfg_access => $dataAccess,
        cfg_collection_link => $collectionLink,
        cfg_protocol => $global->{PROTOCOL},
        cfg_pan_destination => $panPush,
        cfg_disk_limit => $diskLimit,
        cfg_file_size_margin => $fileSizeMargin,
        __TYPE__ => {
            cfg_data_to_dif => 'HASH',
            cfg_data_version => 'HASH',
	    cfg_metadata_methods => 'HASH',
            cfg_hdf4map_methods => 'HASH',
	    cfg_compress => 'HASH',
	    cfg_uncompress => 'HASH',
            cfg_access => 'HASH',
            cfg_collection_link => 'HASH',
            cfg_protocol => 'HASH',
            cfg_pan_destination => 'HASH',
            cfg_disk_limit => 'HASH',
            cfg_root_url => 'HASH'
	    }
	};
        
    if ( defined $global->{LOGGER} ) {
        $config->{cfg_logger} = {
            LEVEL => $global->{LOGGER}{LEVEL},
            FILE => $logFile };
        $config->{__TYPE__}{cfg_logger} = 'HASH';
    }
    
    S4PA::WriteStationConfig( 's4pa_recv_data.cfg', $recvDataDir, $config );
    
    # Create symlink to active file system. If one already exists, don't change
    chdir( $recvDataDir )
        || DeployLogging( 'fatal', "Failed to change directory to $recvDataDir ($!)" );
    unless ( -l 'active_fs' ) {
        symlink( $activeFs, 'active_fs' )
            || DeployLogging( 'fatal', "Failed to create active file system link ($!)" );
    }

    # Create volume configuration file if necessary
    if ( $configVolume eq 'true' ) {
        my $fsListFile = "$recvDataDir" . "/ActiveFs.list";
        unless ( -f $fsListFile ) {
            my $status = CreateActiveFsList( $fsListFile, $activeFs );
            if ( $status ) {
                DeployLogging( 'info', $status );
            } else {
                DeployLogging( 'fatal',
                    "Failed to create volume configuration file: $fsListFile" );
            }
        }
    }

    return $recvDataDir;
}
###############################################################################
# =head1 CreatePollingStation
# 
# Description
#   Creates PDR/data polling stations.
#
# =cut
###############################################################################
sub CreatePollingStation
{
    my ( $type, $doc, $global ) = @_;
    my $logger = $global->{LOGGER}{LOGGING} if defined $global->{LOGGER}{LOGGING};
    
    $type = 'DATA' unless ( $type eq 'PDR' );
    
    # Create poller directory
    my $recvRoot = $global->{S4PA_ROOT} . 'receiving/polling/';
    my $pollerDir = $recvRoot . ( $type eq 'PDR' ? 'pdr/' : 'data/' );
    S4PA::CreateDir( $pollerDir, $logger );
    
    # Master log file
    my $logFile = defined $global->{LOGGER}
        ? "$global->{LOGGER}{DIR}/polling.log" : undef;

    # Failure handler to retry a failed job.
    my $failureHandler = { 'Retry Job' => "perl -e 'use S4P; S4P::remove_job()"
                                          . " if S4P::restart_job()'",
                           'Remove Job' => "perl -e 'use S4P; S4P::remove_job()'"
                         };

    # A hash ref to hold station configuration.
    my $stationConfig = {
            cfg_station_name =>  "Poller: $type",
            cfg_root => $pollerDir,
            cfg_max_time => 600,
            cfg_sort_jobs => 'FIFO',
            cfg_max_failures => 1,
            cfg_polling_interval => 600,
            cfg_stop_interval => 4,
            cfg_end_job_interval => 2,
            cfg_restart_defunct_jobs => 1,
            cfg_max_children => 1,
            cfg_failure_handlers => $failureHandler,
            cfg_commands => {},
            cfg_downstream => {},
            cfg_virtual_jobs => {},
            cfg_virtual_feedback => 1,
            cfg_ignore_duplicates => 1,
	    cfg_umask => 022,
            __TYPE__ => {
                cfg_failure_handlers => 'HASH',
                cfg_commands => 'HASH',
                cfg_downstream => 'HASH',
                cfg_virtual_jobs => 'HASH'
                }
            };

    my $jobCount = 0;
    my $downStream = {};
    # Loop through all providers
    foreach my $provider ( $doc->findnodes( '//provider' ) ) {
        # Get the provider name
        my $providerName = $provider->getAttribute( 'NAME' );
        DeployLogging( 'fatal', "Provider NAME not specified" )
            unless defined $providerName;

        # Get local pan directory for PDR copy in case merge_pan is required
        my $localPdrDir = GetNodeValue( $provider, 'pan/local' );

        # Accumulate supported datasets/versions for PDR polling only
        my $supportedDataVersion = {};
        if ( $type eq 'PDR' ) {
            foreach my $dataset ( $provider->findnodes( 'dataClass/dataset' ) ) {
                my $dataName = $dataset->getAttribute( 'NAME' );
                my @dataVersionList = $dataset->findnodes( 'dataVersion' );
                if ( @dataVersionList ) {
                    foreach my $dataVersion ( @dataVersionList ) {
                        my $label = $dataVersion->getAttribute( 'LABEL' );
                        $supportedDataVersion->{$dataName}{$label} = 1;
                    }
                } else {
                    $supportedDataVersion->{$dataName}{''} = 1;
                }
            }
        }
        
        
        # Get PDR/DATA poller jobs for the provider based on the specified 
        # type.
        my $xpath = $type eq 'PDR' ? 'poller/pdrPoller' : 'poller/dataPoller'; 
        my ( $poller ) = $provider->findnodes( $xpath );
        next unless $poller;
        # get PDR filter info
        my $filterHash = {};
        if ($type eq 'PDR') {
            foreach my $filter ( $poller->findnodes( 'pdrFilter' ) ) {
                my $pattern = $filter->getAttribute( 'PATTERN' );
                $filterHash->{$pattern} = $filter->textContent();
            }
        }
        foreach my $job ( $poller->findnodes( 'job' ) ) {
            # Get job attributes
            my $attr = {};
            my $excludedDataVersion = {};
            my $jobFilterHash = {};

            # Get required attribute
            foreach my $attrName ( 'NAME', 'HOST', 'DIR' ) {
                $attr->{$attrName} = $job->getAttribute( $attrName );
                die "Failed to find $attrName for a PDR poller job"
                    . " belonging to provider $providerName"
                    unless defined $attr->{$attrName};
            }
            # Make sure the job name is unique across providers
            DeployLogging( 'fatal', "Multiple $type pollers with NAME=$attr->{NAME} found" )
                if ( defined $stationConfig->{cfg_commands}{$attr->{NAME}} );

            # Construct history file based on job name.
            my $historyFile = "../$attr->{NAME}.history";

            # For data pollers get some more attributes
            if ( $type eq 'DATA' ) {
                foreach my $attrName ( 'MAX_FILE_GROUP',
                    'ORIGINATING_SYSTEM', 'REPOLL_PAUSE', 'MINIMUM_FILE_SIZE' ) {
                    $attr->{$attrName} = $job->getAttribute( $attrName );
                }
                $attr->{PROTOCOL} = ( defined $job->getAttribute( 'PROTOCOL' ) ) ?
                    $job->getAttribute( 'PROTOCOL' ) : 'FTP';
                $attr->{PORT} = $job->getAttribute( 'PORT' );
                $attr->{EXTERNAL_API} = $job->getAttribute( 'EXTERNAL_API' );
                $attr->{IGNORE_HISTORY} = $job->getAttribute( 'IGNORE_HISTORY' ) 
                    || 'false';
                if ( $attr->{PROTOCOL} eq 'FTP' || $attr->{PROTOCOL} eq 'SFTP' ) {
                    $attr->{RECURSIVE} = $job->getAttribute( 'RECURSIVE' ) || 'false';
                    $attr->{SUB_DIR_PATTERN} = $job->getAttribute( 'SUB_DIR_PATTERN' );
                    $attr->{LATENCY} = $job->getAttribute( 'LATENCY' );
                } else {
                    $attr->{MAX_DEPTH} = $job->getAttribute( 'MAX_DEPTH' ) || 5;
                }
            } elsif ( $type eq 'PDR' ) {
                # Get PDR polling type: DEFAULT or EDOS
                $attr->{TYPE} = $job->getAttribute( 'TYPE' ) || 'DEFAULT';
                # Get the protocol from its new attribute, if attribute not defined, 
                # then based on the host name; default is FTP.
                $attr->{PROTOCOL} = ( defined $job->getAttribute( 'PROTOCOL' ) ) ?
                    $job->getAttribute( 'PROTOCOL' ) : 
                    (defined $global->{PROTOCOL}{$attr->{HOST}}) ?
                    $global->{PROTOCOL}{$attr->{HOST}} : 'FTP';
                $attr->{PORT} = $job->getAttribute( 'PORT' );
                if ( $attr->{PROTOCOL} eq 'FTP' || $attr->{PROTOCOL} eq 'SFTP' ) {
                    $attr->{RECURSIVE} = $job->getAttribute('RECURSIVE') || 'false';
                } else {
                    # if RECURSIVE was specified as 'false', set MAX_DEPTH to 1
                    if ($job->getAttribute('RECURSIVE') eq 'false') {
                        $attr->{MAX_DEPTH} = 1;
                    } elsif ($job->getAttribute('RECURSIVE') eq 'true') {
                        $attr->{MAX_DEPTH} = $job->getAttribute( 'MAX_DEPTH' ) || 5;
                    } else {
                        $attr->{MAX_DEPTH} = $job->getAttribute( 'MAX_DEPTH' ) || 1;
                    }
                }

                # Get IGNORE_HISTORY option: true or false
                $attr->{IGNORE_HISTORY} = $job->getAttribute( 'IGNORE_HISTORY' ) 
                    || 'false';
                $attr->{MERGE_PAN} = $job->getAttribute( 'MERGE_PAN' ) 
                    || 'false';

                # Get PDR name pattern: default is \.PDR$
                $attr->{PATTERN} = $job->getAttribute( 'PATTERN' );
                $attr->{PATTERN} = ( $attr->{TYPE} eq 'EDOS' ) ? '\.[PE]DR$' : '\.PDR$'
                    unless ( defined $attr->{PATTERN} );
                # Get datasets to be excluded
                foreach my $dataset ( $job->findnodes( 'exclude/dataset' ) ) {
                    my $dataName = $dataset->getAttribute( 'NAME' );
                    my $dataVersion = $dataset->getAttribute( 'VERSION' );
                    if ( defined $dataVersion ) {
                        $excludedDataVersion->{$dataName}{$dataVersion} = 1;
                    } else {
                        $excludedDataVersion->{$dataName}{''} = 1;
                    }
                }
                # get PDR filter info for particular job
                foreach my $filter ( $job->findnodes( 'pdrFilter' ) ) {
                    my $pattern = $filter->getAttribute( 'PATTERN' );
                    $jobFilterHash->{$pattern} = $filter->textContent();
                }
                %$jobFilterHash = %$filterHash if (!%$jobFilterHash);
            }
            
            my $command;
            my $config = {};
            # Create commands for PDR & data poller.
            if ( $type eq 'PDR' ) {
                if ( $attr->{TYPE} eq 'DEFAULT' ) {
                    $command = "s4pa_remote_polling_pdr.pl"
                } elsif ( $attr->{TYPE} eq 'EDOS' ) {
                    $command = "s4pa_edos_poller.pl"
                }                
                $command .= " -f '../s4pa_$attr->{NAME}.cfg'";
                $config = {
                    cfg_history_file => $historyFile,
                    cfg_ignore_history => $attr->{IGNORE_HISTORY},
                    cfg_remote_host => $attr->{HOST},
                    cfg_remote_dir => $attr->{DIR},
                    cfg_protocol => $attr->{PROTOCOL},
                    cfg_pdr_pattern => $attr->{PATTERN},
                    cfg_data_version => $supportedDataVersion,
                    cfg_exclude_data => $excludedDataVersion,
                    cfg_merge_pan => $attr->{MERGE_PAN},
                    cfg_pdr_filter => $jobFilterHash,
                    __TYPE__ => {
                        cfg_data_version => 'HASH',
                        cfg_exclude_data => 'HASH',
                        cfg_pdr_filter => 'HASH',
                    }
                };
                $config->{cfg_recursive} = $attr->{RECURSIVE} 
                    if ( defined $attr->{RECURSIVE} );
                $config->{cfg_max_depth} = $attr->{MAX_DEPTH} 
                    if ( defined $attr->{MAX_DEPTH} );
                $config->{cfg_port} = $attr->{PORT}
                    if ( defined $attr->{PORT} );

                # save PDR under pan/local directory if merge_pan was configured
                # otherwise, PDR will get passed down to associated receiving.
                if ( $attr->{MERGE_PAN} eq 'true' ) {
                    $global->{MERGE_PAN} = 1;
                    $downStream->{"MERGE"} = [ '../../../merge_pan' ];
                    $config->{cfg_local_dir} = "$localPdrDir";
                } else {
                    $config->{cfg_local_dir} = "../../../$providerName";
                }

                if ( defined $global->{LOGGER} ) {
                    $config->{cfg_logger} = {
                    LEVEL => $global->{LOGGER}{LEVEL},
                    FILE => $logFile };
                    $config->{__TYPE__}{cfg_logger} = 'HASH';
                }
                S4PA::WriteStationConfig( "s4pa_$attr->{NAME}.cfg", $pollerDir, 
                    $config );                
            } elsif ( $type eq 'DATA' ) {
                $command = "s4pa_remote_polling_data.pl"
                    . " -c '../s4pa_$attr->{NAME}.cfg'";
                $config = { 
                    cfg_remote_host => $attr->{HOST},
                    cfg_remote_dir => $attr->{DIR},
                    cfg_local_dir => "../../../$providerName",
                    cfg_protocol => $attr->{PROTOCOL},
                    cfg_history_file => $historyFile,
                    cfg_ignore_history => $attr->{IGNORE_HISTORY} };
                $config->{cfg_recursive} = $attr->{RECURSIVE} 
                    if ( defined $attr->{RECURSIVE} );
                $config->{cfg_max_depth} = $attr->{MAX_DEPTH} 
                    if ( defined $attr->{MAX_DEPTH} );
                $config->{cfg_max_fg} = $attr->{MAX_FILE_GROUP} 
                    if ( defined $attr->{MAX_FILE_GROUP} );
                $config->{cfg_min_size} = $attr->{MINIMUM_FILE_SIZE}
                    if ( defined $attr->{MINIMUM_FILE_SIZE} );
                $config->{cfg_repoll_pause} = $attr->{REPOLL_PAUSE}
                    if ( defined $attr->{REPOLL_PAUSE} );
                $config->{cfg_originator} = $attr->{ORIGINATING_SYSTEM}
                    if ( defined $attr->{ORIGINATING_SYSTEM} );
                $config->{cfg_external_api} = $attr->{EXTERNAL_API}
                    if ( defined $attr->{EXTERNAL_API} );
                $config->{cfg_port} = $attr->{PORT}
                    if ( defined $attr->{PORT} );
                if ( defined $attr->{SUB_DIR_PATTERN} ) {
                    my $date = `date +$attr->{SUB_DIR_PATTERN}`;
                    if ( $? ) {
                        DeployLogging( 'error', "Unsupported 'date' command or format." );
                    } else {
                        $config->{cfg_sub_dir_pattern} = $attr->{SUB_DIR_PATTERN};
                        $config->{cfg_latency} = $attr->{LATENCY};
                    }
                }
            }
            $stationConfig->{cfg_commands}{$attr->{NAME}} = $command;
            $stationConfig->{cfg_virtual_jobs}{$attr->{NAME}} = 1;
            $jobCount++;
            
            next unless ( $type eq 'DATA' );
            my $dataInfo = {};
            # Applies to data pollers only
            # Collect data name, version, file name pattern, associate file
            # name pattern and its alias for each data being polled in the job.
            foreach my $dataNode ( $job->findnodes( 'dataset' ) ) {
                my ( $dataVersion, $dataName ) = (
                        $dataNode->getAttribute( 'VERSION' ),
                        $dataNode->getAttribute( 'NAME' )
                    );
                # The default version is empty string;
                $dataVersion = "" unless defined $dataVersion;
                $dataVersion =~ s/^\s+|\s+$//g;
                DeployLogging( 'fatal', "Failed to find dataset name for data poller $attr->{NAME}" )
                    unless defined $dataName;
                $dataName .= ".$dataVersion" if ( $dataVersion ne '' );

                my ( $fileNode ) = $dataNode->findnodes( 'file' );
                # multiple file/alias pairs, the new schema after release 3.35
                if ( defined $fileNode ) {
                    # get the primary file name pattern and alias from the 'file' child
                    my $pattern = {};
                    $pattern->{FILE} = $fileNode->getAttribute( 'PATTERN' );
                    my $aliasPattern = $fileNode->getAttribute( 'ALIAS' );
                    $pattern->{ALIAS} = ( defined $aliasPattern ) ? $aliasPattern : "";
                    push( @{$dataInfo->{$dataName}}, $pattern );

                    # get each associated name pattern/alias from each 'associateFile' child
                    my @assocFileNodes = $dataNode->findnodes( 'associateFile' );
                    foreach my $assocFileNode ( @assocFileNodes ) {
                        my $pattern = {};
                        $pattern->{FILE} = $assocFileNode->getAttribute( 'PATTERN' );
                        my $aliasPattern = $assocFileNode->getAttribute( 'ALIAS' );
                        $pattern->{ALIAS} = ( defined $aliasPattern ) ? $aliasPattern : "";
                        push( @{$dataInfo->{$dataName}}, $pattern );
                    }

                # file name pattern in node value, the old schema before release 3.35
                } else {
                    my $pattern = {};
                    $pattern->{FILE} = GetNodeValue( $dataNode );
                    my $aliasPattern = $dataNode->getAttribute( 'ALIAS' );
                    $pattern->{ALIAS} = ( defined $aliasPattern ) ? $aliasPattern : "";
                    push( @{$dataInfo->{$dataName}}, $pattern );
                }
            }
          
            DeployLogging( 'fatal', "Datasets for data poller '$attr->{NAME}' not specified!" )
                unless ( keys %{$dataInfo} );
            # Write data poller configuration
            DeployLogging( 'info', "Writing configuration for data poller: $attr->{NAME}" );
            $config->{DATATYPES} = $dataInfo;
            $config->{__TYPE__}{DATATYPES} = 'HASH';

            if ( defined $global->{LOGGER} ) {
                $config->{cfg_logger} = {
                LEVEL => $global->{LOGGER}{LEVEL},
                FILE => $logFile };
                $config->{__TYPE__}{cfg_logger} = 'HASH';
            }
            S4PA::WriteStationConfig( "s4pa_$attr->{NAME}.cfg", 
                $pollerDir, $config );

        }
        my $maxThread = $poller->getAttribute( 'MAX_THREAD' ) || 1;
        my $pollInterval = $poller->getAttribute( 'INTERVAL' ) || 600;
        my $maxFailure = $poller->getAttribute( 'MAX_FAILURE' ) || 1;
        $stationConfig->{cfg_max_children} = $maxThread
            if ( (not $stationConfig->{cfg_max_children}) ||
	         ($stationConfig->{cfg_max_children} < $maxThread) );
        $stationConfig->{cfg_max_failures} = $maxFailure
            if ( (not $stationConfig->{cfg_max_failures}) ||
	         ($stationConfig->{cfg_max_failures} < $maxFailure) );
        $stationConfig->{cfg_polling_interval} = $pollInterval
            if ( (not defined $stationConfig->{cfg_polling_interval}) ||
	         ($stationConfig->{cfg_polling_interval} < $pollInterval) );
        $stationConfig->{cfg_group} = $global->{S4PA_GID},
    }
        
    # Add an interface to edit station config
    $stationConfig->{cfg_interfaces}{'Edit Station Config'} =
      qq( perl -e 'use S4PA; S4PA::EditFile( FILE => "$pollerDir/station.cfg", TITLE => "Poller: $type" )' );
    # Add an interface to remove Stale log
    $stationConfig->{cfg_interfaces}{'Remove Stale Log'} =
      qq( perl -e 'use S4PA; S4PA::RemoveStaleLog( STATION => "$pollerDir", TITLE => "Poller: $type" )' );
    # Add an interface to update history
    $stationConfig->{cfg_interfaces}{'Update History'} =
      qq( perl -e 'use S4PA; S4PA::SelectEditHistoryFile( STATION => "$pollerDir", TITLE => "Poller: $type" )' );
    # Add an interface for retrying all failed jobs
    $stationConfig->{cfg_interfaces}{'Retry Failed Jobs'} = qq(s4p_restart_all_jobs.pl);
    $stationConfig->{__TYPE__}{cfg_interfaces} = 'HASH';

    # Add merge pan station for downstream
    $stationConfig->{cfg_downstream} = $downStream if ( exists $downStream->{"MERGE"} );

    # Create a poller only if at least one job exists.
    if ( $jobCount ) {
        # Create PDR/DATA Polling station
        S4PA::CreateStation( $pollerDir, $stationConfig, $logger );
        DeployLogging( 'info', "$jobCount $type poller(s) created" );
        return $pollerDir;
    }
    # Otherwise, remove the poller directory which must be empty.
    DeployLogging( 'info', "No $type pollers found; removing $pollerDir" );
    rmdir( $pollerDir );
    return undef;
}    
    

###############################################################################
# =head1 GetDataAttributes
# 
# Description
#   Gets the attributs of dataClass/dataset node. If optional class attribute
#   hash is passed, dataset inherits class attributes by default.
#
# =cut
###############################################################################
sub GetDataAttributes
{
    my ( $node, $parentAttr ) = @_;

    my $nodeAttr = {};
    my $val = $node->getAttribute( 'NAME' );
    $nodeAttr->{NAME} = $val if defined $val;

    my $nodeName = $node->nodeName();
    my @attrList ;
    if ( $nodeName eq 'dataClass' ) {
        @attrList = ( 'GROUP', 'TIME_MARGIN', 'FREQUENCY', 'ACCESS',
            'PUBLISH_WHOM', 'PUBLISH_ECHO', 'PUBLISH_ECHO_OPENDAP',
            'OPENDAP_URL_PREFIX', 'OPENDAP_URL_SUFFIX', 'OPENDAP_RESOURCE_URL_SUFFIX',
            'PUBLISH_CMR', 'PUBLISH_CMR_OPENDAP',
            'PUBLISH_MIRADOR', 'PUBLISH_GIOVANNI', 'EXPIRY', 'DOC', 'WITH_HDF4MAP',
            'MULTIPLE_REPLACEMENT', 'COLLECTION_LINK' );
    } elsif ( $nodeName eq 'dataset' ) {
        @attrList = ( 'GROUP', 'COLLECTION_SHORTNAME', 'TIME_MARGIN', 'FREQUENCY', 'ACCESS',
            'DIF_ENTRY_ID', 'PUBLISH_WHOM', 'PUBLISH_ECHO', 'PUBLISH_ECHO_OPENDAP',
            'OPENDAP_URL_PREFIX', 'OPENDAP_URL_SUFFIX', 'OPENDAP_RESOURCE_URL_SUFFIX',
            'PUBLISH_CMR', 'PUBLISH_CMR_OPENDAP',
            'PUBLISH_MIRADOR', 'PUBLISH_GIOVANNI', 'EXPIRY', 'DOC', 'WITH_HDF4MAP',
            'MULTIPLE_REPLACEMENT', 'COLLECTION_LINK' );
        $nodeAttr->{CLASS} = $parentAttr->{NAME}
            if defined $parentAttr->{CLASS};
    } elsif ( $nodeName eq 'dataVersion' ) {
        @attrList = ( 'LABEL', 'COLLECTION_VERSION', 'TIME_MARGIN', 'FREQUENCY', 'ACCESS',
            'DIF_ENTRY_ID', 'PUBLISH_WHOM', 'PUBLISH_ECHO', 'PUBLISH_ECHO_OPENDAP',
            'OPENDAP_URL_PREFIX', 'OPENDAP_URL_SUFFIX', 'OPENDAP_RESOURCE_URL_SUFFIX',
            'PUBLISH_CMR', 'PUBLISH_CMR_OPENDAP',
            'PUBLISH_MIRADOR', 'PUBLISH_GIOVANNI', 'EXPIRY', 'DOC', 'WITH_HDF4MAP',
            'MULTIPLE_REPLACEMENT', 'COLLECTION_LINK' );
        $nodeAttr->{CLASS} = $parentAttr->{CLASS} 
	    if defined $parentAttr->{CLASS};
        $nodeAttr->{GROUP} = $parentAttr->{GROUP} 
	    if defined $parentAttr->{GROUP};
        $nodeAttr->{NAME} = $parentAttr->{NAME} 
	    if defined $parentAttr->{NAME};
        $nodeAttr->{COLLECTION_SHORTNAME} = $parentAttr->{COLLECTION_SHORTNAME} 
	    if defined $parentAttr->{COLLECTION_SHORTNAME};
    }
    
    foreach my $name ( @attrList ) {
	my $val = $node->getAttribute( $name );
	if ( defined $val ) {
	    $nodeAttr->{$name} = $val 
	} elsif ( defined $parentAttr->{$name} ) {
	    $nodeAttr->{$name} = $parentAttr->{$name};
	}
    }
    $nodeAttr->{TIME_MARGIN} = 0 unless defined $nodeAttr->{TIME_MARGIN};
    $nodeAttr->{FREQUENCY} = "daily" unless defined $nodeAttr->{FREQUENCY};
    $nodeAttr->{ACCESS} = "public" unless defined $nodeAttr->{ACCESS};
    $nodeAttr->{WITH_HDF4MAP} = "false" unless defined $nodeAttr->{WITH_HDF4MAP};
    
    foreach my $attribute ( 'PUBLISH_CMR' ) {
        if ( ! $global->{$attribute} ) {
            # Set publishing off if no configuration for partner was
            # set under <publication> node.
            $nodeAttr->{$attribute} = "false" 
        } else {
            # default publishing flag is true.
            $nodeAttr->{$attribute} = "true"
                unless defined $nodeAttr->{$attribute};
        }
    }

    # Set default WHOM publication off
    $nodeAttr->{PUBLISH_WHOM} = "false"
        unless defined $nodeAttr->{PUBLISH_WHOM};

    # Set default GIOVANNI publication off
    $nodeAttr->{PUBLISH_GIOVANNI} = "false"
        unless defined $nodeAttr->{PUBLISH_GIOVANNI};

    # Set default ECHO publication off
    $nodeAttr->{PUBLISH_ECHO} = "false"
        unless defined $nodeAttr->{PUBLISH_ECHO};

    # Set default MIRADOR publication off
    $nodeAttr->{PUBLISH_MIRADOR} = "false"
        unless defined $nodeAttr->{PUBLISH_MIRADOR};

    return $nodeAttr if ( $nodeName eq 'dataVersion' );
    
    foreach my $element ( 'metadata', 'hdf4map', 'giovanniPreprocess' ) {
        my $val = GetNodeValue( $node, "method/$element" );
        if ( defined $val ) {
            $nodeAttr->{METHOD}{uc($element)} = $val 
        } elsif ( defined $parentAttr->{METHOD}{uc($element)} ) {
            $nodeAttr->{METHOD}{uc($element)} = $parentAttr->{METHOD}{uc($element)};
        }
    }
    foreach my $element ( 'command', 'tmpfile', 'output' ) {
        my $val = GetNodeValue( $node, "method/compression/$element" );
        if ( defined $val ) {
            $nodeAttr->{METHOD}{COMPRESSION}{uc($element)}
                = ( $val =~ /^sub\s*{/ ) ? eval( $val ) : $val;
        } elsif ( defined $parentAttr->{METHOD}{COMPRESSION}{uc($element)} ) {
            $nodeAttr->{METHOD}{COMPRESSION}{uc($element)} 
                = $parentAttr->{METHOD}{COMPRESSION}{uc($element)};
        }
        $val = GetNodeValue( $node, "method/decompression/$element" );
        if ( defined $val ) {
            $nodeAttr->{METHOD}{DECOMPRESSION}{uc($element)}
                = ( $val =~ /^sub\s*{/ ) ? eval( $val ) : $val;
        } elsif ( defined $parentAttr->{METHOD}{DECOMPRESSION}{uc($element)} ) {
            $nodeAttr->{METHOD}{DECOMPRESSION}{uc($element)} 
                = $parentAttr->{METHOD}{DECOMPRESSION}{uc($element)};
        }
    }
    return $nodeAttr;
}
###############################################################################
# =head1 GetNodeValue
# 
# Description
#   Returns the text content of a node or its child matched by the optional
#   XPATH expression.
#
# =cut
###############################################################################
sub GetNodeValue
{
    my ( $root, $xpath ) = @_;
    my ( $node ) = ( $xpath ? $root->findnodes( $xpath ) : $root );
    return undef unless defined $node;
    my $val = $node->textContent();
    $val =~ s/^\s+|\s+$//g;
    return $val;
}
###############################################################################
# =head1 GetGlobalParameters
# 
# Description
#   Returns a hash ref containing global parameters
#
# =cut
###############################################################################
sub GetGlobalParameters
{
    my ( $doc ) = @_;
    my ( $global ) = {};
    
    $global->{S4PA_NAME} = $doc->getAttribute( 'NAME' );
    $global->{S4PA_GID} = $doc->getAttribute( 'MULTIUSERID' );
    $global->{S4PA_ROOT} = GetNodeValue( $doc, '/s4pa/root' );
    $global->{S4PA_ROOT} .= '/' unless ( $global->{S4PA_ROOT} =~ /\/$/ );
    $global->{STORAGE_DIR} = GetNodeValue( $doc, '/s4pa/storageDir' );
    $global->{STORAGE_DIR} .= '/' unless ( $global->{STORAGE_DIR} =~ /\/$/ );
    $global->{RESERVE_DIR} = GetNodeValue( $doc, '/s4pa/auxiliaryBackUpArea' );
    $global->{RESERVE_DIR} .= '/' unless ( $global->{RESERVE_DIR} =~ /\/$/ );
    $global->{TEMPORARY_DIR} = GetNodeValue( $doc, '/s4pa/tempDir' );
    $global->{TEMPORARY_DIR} .= '/' unless ( $global->{TEMPORARY_DIR} =~ /\/$/ );
    $global->{DOCUMENT_DIR} = GetNodeValue( $doc, '/s4pa/documentLocation' );
    $global->{DOCUMENT_DIR} .= '/' unless ( $global->{DOCUMENT_DIR} =~ /\/$/ );
    $global->{INTEGRITY_CMD} = GetNodeValue( $doc, '/s4pa/integrityCheckPriority' );
    $global->{INTEGRITY_CMD} = '/bin/nice -n 19 /usr/bin/ionice -c 3'
        unless ( defined $global->{INTEGRITY_CMD} );
    my ( $node ) = $doc->findnodes( '/s4pa/subscription' );
    $global->{SUBSCRIPTION}{INTERVAL} = $node->getAttribute( 'INTERVAL' )
        if defined $node;
    $global->{SUBSCRIPTION}{INTERVAL} = 86400
        unless defined $global->{SUBSCRIPTION}{INTERVAL};
    ( $node ) = $doc->findnodes( '/s4pa/deletionDelay' );
    $global->{DELETION}{INTRA_VERSION} = $node->getAttribute( 'INTRA_VERSION' )
        if defined $node;
    $global->{DELETION}{INTRA_VERSION} = 86400
        unless defined $global->{DELETION}{INTRA_VERSION};
    $global->{DELETION}{INTER_VERSION} = $node->getAttribute( 'INTER_VERSION' )
        if defined $node;
    $global->{DELETION}{INTER_VERSION} = 86400*180
        unless defined $global->{DELETION}{INTER_VERSION};
    ( $node ) = $doc->findnodes( '/s4pa/logger' );
    if ( defined $node ) {
        $global->{LOGGER}{DIR} = $node->getAttribute( 'DIR' );
        $global->{LOGGER}{LEVEL} = $node->getAttribute( 'LEVEL' );
        $global->{LOGGER}{LEVEL} = 'info' if ( $global->{LOGGER}{LEVEL} eq '' );
        S4P::logger( "WARNING", "Failed to create " . $global->{LOGGER}{DIR} )
            unless S4PA::CreateDir( $global->{LOGGER}{DIR} );
        $global->{LOGGER}{FILE} = $global->{LOGGER}{DIR} . "/deploy.log";
    }
    foreach my $hostNode ( $doc->findnodes( '/s4pa/protocol/host' ) ) {
        my $hostName = GetNodeValue( $hostNode );
        $global->{PROTOCOL}{$hostName}
            = $hostNode->parentNode()->getAttribute( 'NAME' )
            if  $hostName;
    }

    my $extractPubAttr = sub {
        my ( $parent, $xpath, @attrList ) = @_;
        my ( $node ) = $parent->findnodes( $xpath );
        return undef unless defined $node;
        my $hashRef = {};
        foreach my $attr ( @attrList ) {
            $hashRef->{$attr} = $node->getAttribute( $attr );
        }
        return $hashRef;
    };    

    # Get ECHO Publication parameters
    my ( $echoPublication ) = $doc->findnodes( '/s4pa/publication/echo' );
    $global->{PUBLISH_ECHO} = 0;
    if ( $echoPublication ) {
        $global->{PUBLISH_ECHO} = 1;
        my $echoHost = $echoPublication->getAttribute( 'HOST' );
        my $echoVersion = $echoPublication->getAttribute( 'VERSION' );
        my $granMax = $echoPublication->getAttribute( 'MAX_GRANULE_COUNT' );
        
        $global->{ECHO}{VERSION} = ( defined $echoVersion ) ? $echoVersion : '9';
        $global->{ECHO}{GRANULE}{INSERT}
            = $extractPubAttr->( $echoPublication, 'granuleInsert', 'HOST',
                'DIR', 'MAX_GRANULE_COUNT' );
        $global->{ECHO}{GRANULE}{DELETE}
            = $extractPubAttr->( $echoPublication, 'granuleDelete', 'HOST',
                'DIR', 'MAX_GRANULE_COUNT' );
        $global->{ECHO}{COLLECTION}
            = $extractPubAttr->( $echoPublication, 'collectionInsert', 'HOST',
                'DIR' );
        $global->{ECHO}{GRANULE}{INSERT}{HOST} = $echoHost
            unless defined $global->{ECHO}{GRANULE}{INSERT}{HOST};
        $global->{ECHO}{GRANULE}{DELETE}{HOST} = $echoHost
            unless defined $global->{ECHO}{GRANULE}{DELETE}{HOST};
        $global->{ECHO}{COLLECTION}{HOST} = $echoHost
            unless defined $global->{ECHO}{COLLECTION}{HOST};
        $global->{ECHO}{GRANULE}{INSERT}{MAX_GRANULE_COUNT} = $granMax
            unless defined $global->{ECHO}{GRANULE}{INSERT}{MAX_GRANULE_COUNT};
        $global->{ECHO}{GRANULE}{DELETE}{MAX_GRANULE_COUNT} = $granMax
            unless defined $global->{ECHO}{GRANULE}{DELETE}{MAX_GRANULE_COUNT};

        # browse publish to echo go to different directory as the granule
        my $browseInsert = $extractPubAttr->( $echoPublication, 'browseInsert', 
            'HOST', 'DIR' );
        if ( defined $browseInsert ) {
            $global->{ECHO}{BROWSE}{INSERT} = $browseInsert;
            $global->{ECHO}{BROWSE}{INSERT}{HOST} = $echoHost
                unless defined $global->{ECHO}{BROWSE}{INSERT}{HOST};
        }
        my $browseDelete = $extractPubAttr->( $echoPublication, 'browseDelete', 
            'HOST', 'DIR' );
        if ( defined $browseDelete ) {
            $global->{ECHO}{BROWSE}{DELETE} = $browseDelete;
            $global->{ECHO}{BROWSE}{DELETE}{HOST} = $echoHost
                unless defined $global->{ECHO}{BROWSE}{DELETE}{HOST};
        }
    }
    
    # Get CMR Publication parameters
    my ( $cmrPublication ) = $doc->findnodes( '/s4pa/publication/cmr' );
    $global->{PUBLISH_CMR} = 0;
    if ( $cmrPublication ) {
        $global->{PUBLISH_CMR} = 1;
        $global->{CMR}{CMR_ENDPOINT_URI} = $cmrPublication->getAttribute( 'CMR_ENDPOINT_URI' );
        $global->{CMR}{CMR_TOKEN_URI} = $cmrPublication->getAttribute( 'CMR_TOKEN_URI' );
        $global->{CMR}{PROVIDER} = $cmrPublication->getAttribute( 'PROVIDER' );
        if (defined $cmrPublication->getAttribute('USERNAME')) {
            $global->{CMR}{USERNAME} = $cmrPublication->getAttribute('USERNAME');
        }
        if (defined $cmrPublication->getAttribute('PASSWORD')) {
            $global->{CMR}{PASSWORD} = $cmrPublication->getAttribute('PASSWORD');
        }
        if (defined $cmrPublication->getAttribute('CERT_FILE')) {
            $global->{CMR}{CERT_FILE} = $cmrPublication->getAttribute('CERT_FILE');
        }
        if (defined $cmrPublication->getAttribute('CERT_PASS')) {
             $global->{CMR}{CERT_PASS} = $cmrPublication->getAttribute('CERT_PASS');
        }
        $global->{CMR}{MAX_GRANULE_COUNT} = $cmrPublication->getAttribute( 'MAX_GRANULE_COUNT' );

        my @skipPSANode = $cmrPublication->getChildrenByTagName( 'skipPublication' );
        foreach my $psa ( @skipPSANode ) {
            my $psaPair = {};
            $psaPair->{'PSAName'} = $psa->getAttribute( 'PSANAME' );
            $psaPair->{'PSAValue'} = $psa->getAttribute( 'PSAVALUE' );
            push @{$global->{CMR}{SKIPPSA}}, $psaPair; 
        }

        my $collectionInfo = SearchCollection($doc, $global);
        $global->{CMR}{COLLECTION_INFO} = $collectionInfo;
    }

    my ( $miradorPublication )
        = $doc->findnodes( '/s4pa/publication/mirador' );
    $global->{PUBLISH_MIRADOR} = 0;
    if ( $miradorPublication ) {
        $global->{PUBLISH_MIRADOR} = 1;
        my $miradorHost = $miradorPublication->getAttribute( 'HOST' );
        
        $global->{MIRADOR}{GRANULE}{INSERT}
            = $extractPubAttr->( $miradorPublication, 'granuleInsert', 'HOST',
                'DIR' );
        $global->{MIRADOR}{GRANULE}{DELETE}
            = $extractPubAttr->( $miradorPublication, 'granuleDelete', 'HOST',
                'DIR' );
        $global->{MIRADOR}{DOCUMENT}
            = $extractPubAttr->( $miradorPublication, 'productDocument',
                'HOST', 'DIR', 'CMS_TEMPLATE' );
        $global->{MIRADOR}{GRANULE}{INSERT}{HOST} = $miradorHost
            unless defined $global->{MIRADOR}{GRANULE}{INSERT}{HOST};
        $global->{MIRADOR}{GRANULE}{DELETE}{HOST} = $miradorHost
            unless defined $global->{MIRADOR}{GRANULE}{DELETE}{HOST};
        $global->{MIRADOR}{DOCUMENT}{HOST} = $miradorHost
            unless defined $global->{MIRADOR}{DOCUMENT}{HOST};

        my @skipPSANode = $miradorPublication->getChildrenByTagName( 'skipPublication' );
        foreach my $psa ( @skipPSANode ) {
            my $psaPair = {};
            $psaPair->{'PSAName'} = $psa->getAttribute( 'PSANAME' );
            $psaPair->{'PSAValue'} = $psa->getAttribute( 'PSAVALUE' );
            push @{$global->{MIRADOR}{SKIPPSA}}, $psaPair; 
        }
    }
    
    my ( $giovanniPublication )
        = $doc->findnodes( '/s4pa/publication/giovanni' );
    $global->{PUBLISH_GIOVANNI} = 0;
    if ( $giovanniPublication ) {
        $global->{PUBLISH_GIOVANNI} = 1;
        my $giovanniHost = $giovanniPublication->getAttribute( 'HOST' );
        
        $global->{GIOVANNI}{GRANULE}{INSERT}
            = $extractPubAttr->( $giovanniPublication, 'granuleInsert', 'HOST',
                'DIR' );
        $global->{GIOVANNI}{GRANULE}{DELETE}
            = $extractPubAttr->( $giovanniPublication, 'granuleDelete', 'HOST',
                'DIR' );
        $global->{GIOVANNI}{GRANULE}{INSERT}{HOST} = $giovanniHost
            unless defined $global->{GIOVANNI}{GRANULE}{INSERT}{HOST};
        $global->{GIOVANNI}{GRANULE}{DELETE}{HOST} = $giovanniHost
            unless defined $global->{GIOVANNI}{GRANULE}{DELETE}{HOST};
    }
    
    my ( $dotChartPublication )
        = $doc->findnodes( '/s4pa/publication/dotChart' );
    $global->{PUBLISH_DOTCHART} = 0;
    if ( $dotChartPublication ) {
        $global->{PUBLISH_DOTCHART} = 1;
        my $dotChartHost = $dotChartPublication->getAttribute( 'HOST' );
        
        $global->{DOTCHART}{GRANULE}{INSERT}
            = $extractPubAttr->( $dotChartPublication, 'granuleInsert', 'HOST',
                'DIR' );
        $global->{DOTCHART}{GRANULE}{DELETE}
            = $extractPubAttr->( $dotChartPublication, 'granuleDelete', 'HOST',
                'DIR' );
        $global->{DOTCHART}{COLLECTION}
            = $extractPubAttr->( $dotChartPublication, 'collectionInsert',
                'HOST', 'DIR' );
        $global->{DOTCHART}{GRANULE}{INSERT}{HOST} = $dotChartHost
            unless defined $global->{DOTCHART}{GRANULE}{INSERT}{HOST};
        $global->{DOTCHART}{GRANULE}{DELETE}{HOST} = $dotChartHost
            unless defined $global->{DOTCHART}{GRANULE}{DELETE}{HOST};
        $global->{DOTCHART}{COLLECTION}{HOST} = $dotChartHost
            unless defined $global->{DOTCHART}{COLLECTION}{HOST};
    }
    
    my ( $userPublication )
        = $doc->findnodes( '/s4pa/publication/user' );
    $global->{PUBLISH_USER} = 0;
    if ( $userPublication ) {
        $global->{PUBLISH_USER} = 1;
        my $userInterval = $userPublication->getAttribute( 'INTERVAL' );
        $global->{USER}{INTERVAL} = ( defined $userInterval ) ? $userInterval : 86400;
        my $userRetention = $userPublication->getAttribute( 'RETENTION_PERIOD' );
        $global->{USER}{RETENTION} = ( defined $userRetention ) ? $userRetention : 604800;
        foreach my $datasetNode ( $userPublication->findnodes( 'dataset' ) ) {
            my $shortname = $datasetNode->getAttribute( 'NAME' );
            my $version = $datasetNode->getAttribute( 'VERSION' );
            $version = '' unless ( defined $version );
            $global->{USER}{DATASET}{$shortname}{$version}{DIR} =
                $datasetNode->getAttribute( 'DIR' );
            $global->{USER}{DATASET}{$shortname}{$version}{BOX} =
                ( defined $datasetNode->getAttribute( 'BOUNDINGBOX' ) ) ?
                $datasetNode->getAttribute( 'BOUNDINGBOX' ) : 'false';
        } 
    }

    # Get reconciliation parameters
    my ( $echoRecon ) = $doc->findnodes( '/s4pa/reconciliation/echo' );
    if ($echoRecon) {
        my $reconUrl = $echoRecon->getAttribute( 'URL' );
        my ($protocol, $host, $junk, $dir) = ($reconUrl =~ /(\w+):\/\/((\w+\.)*\w+)((\/\w+)*\/?)/);
        $global->{RECON}{ECHO}{PUSH_HOST} = $host || "discette.gsfc.nasa.gov";
        $global->{RECON}{ECHO}{PUSH_DIR} = $dir || "/ftp/private/s4pa/push";
        $global->{RECON}{ECHO}{MAX_GRANULE_COUNT} = $echoRecon->getAttribute( 'MAX_GRANULE_COUNT' ) || 0;
        $global->{RECON}{ECHO}{PUSH_USER} = $echoRecon->getAttribute( 'PUSH_USER' ) || "anonymous";
        $global->{RECON}{ECHO}{PUSH_PWD} = $echoRecon->getAttribute( 'PUSH_PWD' ) || "s4pa%40";
        $global->{RECON}{ECHO}{USERNAME} = $echoRecon->getAttribute( 'USERNAME' );
        $global->{RECON}{ECHO}{PASSWORD} = $echoRecon->getAttribute( 'PASSWORD' );
        $global->{RECON}{ECHO}{CHROOT_DIR} = $echoRecon->getAttribute( 'CHROOT_DIR' ) || "null";
        $global->{RECON}{ECHO}{LOCAL_DIR} = $echoRecon->getAttribute( 'LOCAL_DIR' ) || "/var/tmp";
        $global->{RECON}{ECHO}{ENDPOINT_URI} = $echoRecon->getAttribute( 'ENDPOINT_URI' );
        $global->{RECON}{ECHO}{STAGING_DIR} = $echoRecon->getAttribute( 'STAGING_DIR' ) || $global->{S4PA_ROOT}. "tmp";
        $global->{RECON}{ECHO}{DATA_HOST} = $echoRecon->getAttribute( 'DATA_HOST' );
        $global->{RECON}{ECHO}{MIN_INTERVAL} = $echoRecon->getAttribute( 'MIN_INTERVAL' ) || 86400;
    }

    my ( $cmrRecon ) = $doc->findnodes( '/s4pa/reconciliation/cmr' );
    if ($cmrRecon) {
        my $reconUrl = $cmrRecon->getAttribute( 'URL' );
        my ($protocol, $host, $junk, $dir) = ($reconUrl =~ /(\w+):\/\/((\w+\.)*\w+)((\/\w+)*\/?)/);
        $global->{RECON}{CMR}{PUSH_HOST} = $host || "discette.gsfc.nasa.gov";
        $global->{RECON}{CMR}{PUSH_DIR} = $dir || "/ftp/private/s4pa/push";
        $global->{RECON}{CMR}{MAX_GRANULE_COUNT} = $cmrRecon->getAttribute( 'MAX_GRANULE_COUNT' ) || 0;
        $global->{RECON}{CMR}{PUSH_USER} = $cmrRecon->getAttribute( 'PUSH_USER' ) || "anonymous";
        $global->{RECON}{CMR}{PUSH_PWD} = $cmrRecon->getAttribute( 'PUSH_PWD' ) || "s4pa%40";
        $global->{RECON}{CMR}{CHROOT_DIR} = $cmrRecon->getAttribute( 'CHROOT_DIR' ) || "null";
        $global->{RECON}{CMR}{LOCAL_DIR} = $cmrRecon->getAttribute( 'LOCAL_DIR' ) || "/var/tmp";
        $global->{RECON}{CMR}{STAGING_DIR} = $cmrRecon->getAttribute( 'STAGING_DIR' ) || $global->{S4PA_ROOT}. "tmp";
        $global->{RECON}{CMR}{DATA_HOST} = $cmrRecon->getAttribute( 'DATA_HOST' );
        $global->{RECON}{CMR}{MIN_INTERVAL} = $cmrRecon->getAttribute( 'MIN_INTERVAL' ) || 86400;
    }

    my ( $miradorRecon ) = $doc->findnodes( '/s4pa/reconciliation/mirador' );
    if ($miradorRecon) {
        my $reconUrl = $miradorRecon->getAttribute( 'URL' );
        my ($protocol, $host, $junk, $dir) = ($reconUrl =~ /(\w+):\/\/((\w+\.)*\w+)((\/\w+)*\/?)/);
        $global->{RECON}{MIRADOR}{PUSH_HOST} = $host || $global->{MIRADOR}{GRANULE}{INSERT}{HOST};
        $global->{RECON}{MIRADOR}{PUSH_DIR} = $dir || "/ftp/private/reconciliation";
        $global->{RECON}{MIRADOR}{MAX_GRANULE_COUNT} = $miradorRecon->getAttribute( 'MAX_GRANULE_COUNT' ) || 500;
        $global->{RECON}{MIRADOR}{PUSH_USER} = $miradorRecon->getAttribute( 'PUSH_USER' ) || "anonymous";
        $global->{RECON}{MIRADOR}{PUSH_PWD} = $miradorRecon->getAttribute( 'PUSH_PWD' ) || "s4pa%40";
        $global->{RECON}{MIRADOR}{CHROOT_DIR} = $miradorRecon->getAttribute( 'CHROOT_DIR' ) || "null";
        $global->{RECON}{MIRADOR}{LOCAL_DIR} = $miradorRecon->getAttribute( 'LOCAL_DIR' ) || "/var/tmp";
        $global->{RECON}{MIRADOR}{ENDPOINT_URI} = $miradorRecon->getAttribute( 'ENDPOINT_URI' );
        $global->{RECON}{MIRADOR}{STAGING_DIR} = $miradorRecon->getAttribute( 'STAGING_DIR' ) || $global->{S4PA_ROOT}. "tmp";
        $global->{RECON}{MIRADOR}{DATA_HOST} = $miradorRecon->getAttribute( 'DATA_HOST' );
        $global->{RECON}{MIRADOR}{MIN_INTERVAL} = $miradorRecon->getAttribute( 'MIN_INTERVAL' ) || 86400;
        $global->{RECON}{MIRADOR}{PULL_TIMEOUT} = $miradorRecon->getAttribute( 'PULL_TIMEOUT' ) || 60;
    }
    my ( $dotchartRecon ) = $doc->findnodes( '/s4pa/reconciliation/dotchart' );
    if ($dotchartRecon) {
        my $reconUrl = $dotchartRecon->getAttribute( 'URL' ) || '';
        my ($protocol, $host, $junk, $dir) = ($reconUrl =~ /(\w+):\/\/((\w+\.)*\w+)((\/\w+)*\/?)/);
        $global->{RECON}{DOTCHART}{PUSH_HOST} = $host || $global->{DOTCHART}{GRANULE}{INSERT}{HOST};
        # dotchart database moved from tads1 to ops1 with different publishing and recon area
        if ($global->{RECON}{DOTCHART}{PUSH_HOST} =~ /^tads1/) {
            $global->{RECON}{DOTCHART}{PUSH_DIR} = $dir || "/ftp/private/reconciliation";
        } else {
            $global->{RECON}{DOTCHART}{PUSH_DIR} = $dir || "/data/private/dotchart/reconciliation";
        }
        $global->{RECON}{DOTCHART}{MAX_GRANULE_COUNT} = $dotchartRecon->getAttribute( 'MAX_GRANULE_COUNT' ) || 500;
        $global->{RECON}{DOTCHART}{PUSH_USER} = $dotchartRecon->getAttribute( 'PUSH_USER' ) || "anonymous";
        $global->{RECON}{DOTCHART}{PUSH_PWD} = $dotchartRecon->getAttribute( 'PUSH_PWD' ) || "s4pa%40";
        $global->{RECON}{DOTCHART}{CHROOT_DIR} = $dotchartRecon->getAttribute( 'CHROOT_DIR' ) || "null";
        $global->{RECON}{DOTCHART}{LOCAL_DIR} = $dotchartRecon->getAttribute( 'LOCAL_DIR' ) || "/var/tmp";
        $global->{RECON}{DOTCHART}{ENDPOINT_URI} = $dotchartRecon->getAttribute( 'ENDPOINT_URI' );
        $global->{RECON}{DOTCHART}{STAGING_DIR} = $dotchartRecon->getAttribute( 'STAGING_DIR' ) || $global->{S4PA_ROOT}. "tmp";
        $global->{RECON}{DOTCHART}{DATA_HOST} = $dotchartRecon->getAttribute( 'DATA_HOST' );
        $global->{RECON}{DOTCHART}{MIN_INTERVAL} = $dotchartRecon->getAttribute( 'MIN_INTERVAL' ) || 86400;
        $global->{RECON}{DOTCHART}{PULL_TIMEOUT} = $dotchartRecon->getAttribute( 'PULL_TIMEOUT' ) || 60;
    }
    my ( $giovanniRecon ) = $doc->findnodes( '/s4pa/reconciliation/giovanni' );
    if ($giovanniRecon) {
        my $reconUrl = $giovanniRecon->getAttribute( 'URL' );
        my ($protocol, $host, $junk, $dir) = ($reconUrl =~ /(\w+):\/\/((\w+\.)*\w+)((\/\w+)*\/?)/);
        $global->{RECON}{GIOVANNI}{PUSH_HOST} = $host || $global->{GIOVANNI}{GRANULE}{INSERT}{HOST};
        $global->{RECON}{GIOVANNI}{PUSH_DIR} = $dir || "/ftp/private/reconciliation";
        $global->{RECON}{GIOVANNI}{MAX_GRANULE_COUNT} = $giovanniRecon->getAttribute( 'MAX_GRANULE_COUNT' ) || 500;
        $global->{RECON}{GIOVANNI}{PUSH_USER} = $giovanniRecon->getAttribute( 'PUSH_USER' ) || "anonymous";
        $global->{RECON}{GIOVANNI}{PUSH_PWD} = $giovanniRecon->getAttribute( 'PUSH_PWD' ) || "s4pa%40";
        $global->{RECON}{GIOVANNI}{CHROOT_DIR} = $giovanniRecon->getAttribute( 'CHROOT_DIR' ) || "null";
        $global->{RECON}{GIOVANNI}{LOCAL_DIR} = $giovanniRecon->getAttribute( 'LOCAL_DIR' ) || "/var/tmp";
        $global->{RECON}{GIOVANNI}{ENDPOINT_URI} = $giovanniRecon->getAttribute( 'ENDPOINT_URI' );
        $global->{RECON}{GIOVANNI}{STAGING_DIR} = $giovanniRecon->getAttribute( 'STAGING_DIR' ) || $global->{S4PA_ROOT}. "tmp";
        $global->{RECON}{GIOVANNI}{DATA_HOST} = $giovanniRecon->getAttribute( 'DATA_HOST' );
        $global->{RECON}{GIOVANNI}{MIN_INTERVAL} = $giovanniRecon->getAttribute( 'MIN_INTERVAL' ) || 86400;
        $global->{RECON}{GIOVANNI}{PULL_TIMEOUT} = $giovanniRecon->getAttribute( 'PULL_TIMEOUT' ) || 60;
    }

    # Get difFetcher parameters
    my ( $difFetcher ) = $doc->findnodes( '/s4pa/difFetcher' );
    if ($difFetcher) {
        $global->{DIFFETCHER}{ENDPOINT_URI} = $difFetcher->getAttribute( 'ENDPOINT_URI' );
        if ( defined $difFetcher->getAttribute( 'TOKEN_URI' ) ) {
            $global->{DIFFETCHER}{TOKEN_URI} = $difFetcher->getAttribute( 'TOKEN_URI' );
        } elsif ( defined $global->{CMR}{CMR_TOKEN_URI} ) {
            $global->{DIFFETCHER}{TOKEN_URI} = $global->{CMR}{CMR_TOKEN_URI};
        }
        if ( defined $difFetcher->getAttribute( 'PROVIDER' ) ) {
            $global->{DIFFETCHER}{PROVIDER} = $difFetcher->getAttribute( 'PROVIDER' );
        } elsif ( defined $global->{CMR}{PROVIDER} ) {
            $global->{DIFFETCHER}{PROVIDER} = $global->{CMR}{PROVIDER};
        }
        if ( defined $difFetcher->getAttribute( 'USERNAME' ) ) {
            $global->{DIFFETCHER}{USERNAME} = $difFetcher->getAttribute( 'USERNAME' );
        } elsif ( defined $global->{CMR}{USERNAME} ) {
            $global->{DIFFETCHER}{USERNAME} = $global->{CMR}{USERNAME};
        }
        if ( defined $difFetcher->getAttribute( 'PASSWORD' ) ) {
            $global->{DIFFETCHER}{PASSWORD} = $difFetcher->getAttribute( 'PASSWORD' );
        } elsif ( defined $global->{CMR}{PASSWORD} ) {
            $global->{DIFFETCHER}{PASSWORD} = $global->{CMR}{PASSWORD};
        }
        if ( defined $difFetcher->getAttribute( 'CERT_FILE' ) ) {
            $global->{DIFFETCHER}{CERT_FILE} = $difFetcher->getAttribute( 'CERT_FILE' );
        } elsif ( defined $global->{CMR}{CERT_FILE} ) {
            $global->{DIFFETCHER}{CERT_FILE} = $global->{CMR}{CERT_FILE};
        }
        if ( defined $difFetcher->getAttribute( 'CERT_PASS' ) ) {
            $global->{DIFFETCHER}{CERT_PASS} = $difFetcher->getAttribute( 'CERT_PASS' );
        } elsif ( defined $global->{CMR}{CERT_PASS} ) {
            $global->{DIFFETCHER}{CERT_PASS} = $global->{CMR}{CERT_PASS};
        }
    }

    # Get project location
    foreach my $node ( $doc->findnodes( '/s4pa/project/location' ) ) {
        my $location = GetNodeValue( $node );
        if ( defined $global->{PROJECT} ) {
            push( @{$global->{PROJECT}}, $location );
        } else {
            $global->{PROJECT} = [ $location ];
        } 
    }
    
    # Get root data URL
    ( $node ) = $doc->findnodes( '/s4pa/urlRoot' );
    if ( $node ) {
        $global->{URL}{FTP} = $node->getAttribute( 'FTP' );
        $global->{URL}{FTP} .= '/';
        $global->{URL}{HTTP} = $node->getAttribute( 'HTTP' );
        $global->{URL}{HTTP} .= '/';
    }
    
    # Get PostOffice attributes
    ( $node ) = $doc->findnodes( '/s4pa/postOffice' );
    if ( $node ) {
        $global->{POSTOFFICE}{INTERVAL} = $node->getAttribute( 'INTERVAL' );
        $global->{POSTOFFICE}{MAX_ATTEMPT}
            = $node->getAttribute( 'MAX_ATTEMPT' );
        $global->{POSTOFFICE}{MAX_CHILDREN}
            = $node->getAttribute( 'MAX_JOBS' );
    }
    $global->{POSTOFFICE}{INTERVAL} = 10 
        unless defined $global->{POSTOFFICE}{INTERVAL};
    $global->{POSTOFFICE}{MAX_CHILDREN} = 1
        unless defined $global->{POSTOFFICE}{MAX_CHILDREN};
    $global->{POSTOFFICE}{MAX_ATTEMPT} = 1
        unless defined $global->{POSTOFFICE}{MAX_ATTEMPT};
    return $global;
}
###############################################################################
# =head1 CopyConfigFiles
# 
# Description
#   Copies necessary configuration files from project sandboxes
#
# =cut
###############################################################################
sub CopyConfigFiles
{
    my ( $doc, $global, $skipXSL ) = @_;
    my $logger = $global->{LOGGER}{LOGGING} if defined $global->{LOGGER}{LOGGING};
    
    # Collect all project directories
    my @projectDirList = ();    
    foreach my $location ( $doc->findnodes( '/s4pa/project/location' ) ) {
        push( @projectDirList, GetNodeValue( $location ) );
    }
    return unless @projectDirList;
    
    my $readDir = sub {
        my ( $dirName ) = @_;
        $dirName .= '/' unless ( $dirName =~ /\/$/ );
        return () unless opendir( DH, $dirName );
        my @list = grep( !/^\./, readdir( DH ) );
        return () unless @list;
        return map { ( -f $dirName . $_ ) ? $dirName . $_ : () } @list;
    };
    
    # Collect all project config files
    my @configFileList = ();
    foreach my $projectDir ( @projectDirList ) {
        foreach my $subDir ( 'cfg', 'doc/xsd', 'doc/xsl' ) {
            my @list = $readDir->( "$projectDir/$subDir" );
            push( @configFileList, @list ) if ( @list );
        }
    }
    # Return if no config files are found
    return unless @configFileList;
    
    # Try to guess config files needed
    foreach my $methodNode ( $doc->findnodes( '//method/metadata' ) ) {
        my $methodVal = GetNodeValue( $methodNode );
        my ( $providerNode ) = $methodNode->findnodes( 'ancestor::provider' );
        my $providerName = $providerNode->getAttribute( 'NAME' );
        foreach my $configFile ( @configFileList ) {
            my $file = basename($configFile);
            if ( $methodVal =~ /$file/ ) {
                my $dir = "$global->{S4PA_ROOT}/receiving/$providerName";
                DeployLogging( 'info', "Copied $configFile to $dir" )
                    if copy( $configFile, $dir );
            }
        }
    }

    # skip all XSL installation if specified.
    if ( $skipXSL ) {
        DeployLogging( 'info', "Skipped all XSL stylesheets installation" );
        return;
    };
    foreach my $configFile ( @configFileList ) {
        if ( basename($configFile) eq 's4pa_publish_whom.cfg' ) {
            my $dir = "$global->{S4PA_ROOT}/publish_whom";
            DeployLogging( 'info', "Copied $configFile to $dir" )
                if ( -d $dir && copy( $configFile, $dir ) );
        } elsif ( basename($configFile) eq 'S4paGran2ECHO.xsl' ) {
            my $dir = "$global->{S4PA_ROOT}/publish_echo";
            DeployLogging( 'info', "Copied $configFile to $dir" )
                if ( -d $dir && copy( $configFile, $dir ) );
        } elsif ( basename($configFile) eq 'S4paGran2ECHO10.xsl' ) {
            my $dir = "$global->{S4PA_ROOT}/publish_echo";
            DeployLogging( 'info', "Copied $configFile to $dir" )
                if ( -d $dir && copy( $configFile, $dir ) );
        } elsif ( basename($configFile) eq 'S4paGran2EchoBrowse.xsl' ) {
            my $dir = "$global->{S4PA_ROOT}/publish_echo";
            DeployLogging( 'info', "Copied $configFile to $dir" )
                if ( -d $dir && copy( $configFile, $dir ) );
        } elsif ( basename($configFile) eq 'S4paGran2EchoBrowse10.xsl' ) {
            my $dir = "$global->{S4PA_ROOT}/publish_echo";
            DeployLogging( 'info', "Copied $configFile to $dir" )
                if ( -d $dir && copy( $configFile, $dir ) );
        } elsif ( basename($configFile) eq 'S4paGran2CMR.xsl' ) {
            my $dir = "$global->{S4PA_ROOT}/publish_cmr";
            DeployLogging( 'info', "Copied $configFile to $dir" )
                if ( -d $dir && copy( $configFile, $dir ) );
        } elsif ( basename($configFile) eq 'S4paGran2CmrBrowse.xsl' ) {
            my $dir = "$global->{S4PA_ROOT}/publish_cmr";
            DeployLogging( 'info', "Copied $configFile to $dir" )
                if ( -d $dir && copy( $configFile, $dir ) );
        } elsif ( basename($configFile) eq 'S4paGran2DotChart.xsl' ) {
            my $dir = "$global->{S4PA_ROOT}/publish_dotchart";
            DeployLogging( 'info', "Copied $configFile to $dir" )
                if ( -d $dir && copy( $configFile, $dir ) );
        } elsif ( basename($configFile) eq 'S4paGran2Mirador.xsl' ) {
            my $dir = "$global->{S4PA_ROOT}/publish_mirador";
            DeployLogging( 'info', "Copied $configFile to $dir" )
                if ( -d $dir && copy( $configFile, $dir ) );
        } elsif ( basename($configFile) eq 'S4paGran2Giovanni.xsl' ) {
            my $dir = "$global->{S4PA_ROOT}/publish_giovanni";
            DeployLogging( 'info', "Copied $configFile to $dir" )
                if ( -d $dir && copy( $configFile, $dir ) );
        } elsif ( (basename($configFile) eq 'S4paGran2HTML.xsl')
            || (basename($configFile) eq 'S4paDIF2HTML.xsl')
            || (basename($configFile) eq 'S4paDIF102HTML.xsl') ) {
            if ( copy ( $configFile, $global->{STORAGE_DIR} )
                && chmod( 0644, "$global->{STORAGE_DIR}/" . basename( $configFile ) ) ) {
                DeployLogging( 'info', "Copied $configFile to $global->{STORAGE_DIR}" );
            } else {
                DeployLogging( 'error', "Failed to copy $configFile to $global->{STORAGE_DIR} ($!)" );
            }
        }
        foreach my $file ( 'S4paDIF2Collect.xsl', 'S4paDIF102Collect.xsl',
            'S4paDIF2ECHO.xsl', 'S4paDIF2ECHO10.xsl', 'S4paDIF2Mirador.xsl' ) {
            if ( basename($configFile) eq "$file" ) {
                my $dir = "$global->{S4PA_ROOT}/other/housekeeper";
                DeployLogging( 'info', "Copied $configFile to $dir" )
                    if copy( $configFile, $dir );
            }
        }
    }
}

###############################################################################
# =head1 GetStoreDataDownStream 
# 
# Description
#   Form downstream station paths for output work orders of StoreData
#
# =cut
################################################################################
sub GetStoreDataDownStream
{
    my ( $dataAttr, $versionAttr, $className, $downStream, $cfgPublish ) = @_;

    my $dataVersionString = "$dataAttr->{NAME}";
    my $versionLabel = ( defined $versionAttr
	     && defined $versionAttr->{LABEL}
	     && $versionAttr->{LABEL} ne '' ) ? $versionAttr->{LABEL} : '';
    $dataVersionString .= "_$versionAttr->{LABEL}" if ( $versionLabel ne '' );

    # Downstream stations for insertions
    $downStream->{"SUBSCRIBE_$dataVersionString"} = [ 
	"subscribe/pending",
	];

    # Downstream stations for publication only
    $downStream->{"PUBLISH_$dataVersionString"} = [];

    # Publish only if configured
    my $dotchartFlag = ( defined $versionAttr )
        ? $versionAttr->{PUBLISH_DOTCHART} : $dataAttr->{PUBLISH_DOTCHART};
    if ( $dotchartFlag eq 'true' ) {
	push( @{$downStream->{"SUBSCRIBE_$dataVersionString"}}, 
	    "publish_dotchart/pending_publish" );
	push( @{$downStream->{"PUBLISH_$dataVersionString"}}, 
	    "publish_dotchart/pending_publish" );
	push( @{$downStream->{"INTRA_VERSION_DELETE_$dataVersionString"}}, 
	    "publish_dotchart/pending_delete" );
	push( @{$downStream->{"INTER_VERSION_DELETE_$dataVersionString"}}, 
	    "publish_dotchart/pending_delete" );
        push( @{$cfgPublish->{$dataAttr->{NAME}}{$versionLabel}},
            "publish_dotchart/pending_publish" );
    } else {
	push( @{$downStream->{"INTRA_VERSION_DELETE_$dataVersionString"}}, 
	    "storage/$className/delete_$className/intra_version_pending" );
	push( @{$downStream->{"INTER_VERSION_DELETE_$dataVersionString"}}, 
	    "storage/$className/delete_$className/inter_version_pending" );
    }

    my $whomFlag = ( defined $versionAttr )
        ? $versionAttr->{PUBLISH_WHOM} : $dataAttr->{PUBLISH_WHOM};
    if ( $whomFlag eq 'true' ) {
	push( @{$downStream->{"SUBSCRIBE_$dataVersionString"}}, 
	    "publish_whom/pending_publish" );
	push( @{$downStream->{"PUBLISH_$dataVersionString"}}, 
	    "publish_whom/pending_publish" );
	push( @{$downStream->{"INTRA_VERSION_DELETE_$dataVersionString"}}, 
	    "publish_whom/pending_delete" );
	push( @{$downStream->{"INTER_VERSION_DELETE_$dataVersionString"}}, 
	    "publish_whom/pending_delete" );
        push( @{$cfgPublish->{$dataAttr->{NAME}}{$versionLabel}},
            "publish_whom/pending_publish" );
    }

    my $echoFlag = ( defined $versionAttr )
        ? $versionAttr->{PUBLISH_ECHO} : $dataAttr->{PUBLISH_ECHO};
    if ( $echoFlag eq 'true' ) {
	push( @{$downStream->{"SUBSCRIBE_$dataVersionString"}}, 
	    "publish_echo/pending_publish" );
	push( @{$downStream->{"PUBLISH_$dataVersionString"}}, 
	    "publish_echo/pending_publish" );
	push( @{$downStream->{"INTRA_VERSION_DELETE_$dataVersionString"}}, 
	    "publish_echo/pending_delete" );
	push( @{$downStream->{"INTER_VERSION_DELETE_$dataVersionString"}}, 
	    "publish_echo/pending_delete" );
        push( @{$cfgPublish->{$dataAttr->{NAME}}{$versionLabel}},
            "publish_echo/pending_publish" );
    }

    my $cmrFlag = ( defined $versionAttr )
        ? $versionAttr->{PUBLISH_CMR} : $dataAttr->{PUBLISH_CMR};
    if ( $cmrFlag eq 'true' ) {
	push( @{$downStream->{"SUBSCRIBE_$dataVersionString"}}, 
	    "publish_cmr/pending_publish" );
	push( @{$downStream->{"PUBLISH_$dataVersionString"}}, 
	    "publish_cmr/pending_publish" );
	push( @{$downStream->{"INTRA_VERSION_DELETE_$dataVersionString"}}, 
	    "publish_cmr/pending_delete" );
	push( @{$downStream->{"INTER_VERSION_DELETE_$dataVersionString"}}, 
	    "publish_cmr/pending_delete" );
        push( @{$cfgPublish->{$dataAttr->{NAME}}{$versionLabel}},
            "publish_cmr/pending_publish" );
    }

    my $miradorFlag = ( defined $versionAttr )
        ? $versionAttr->{PUBLISH_MIRADOR} : $dataAttr->{PUBLISH_MIRADOR};
    if ( $miradorFlag eq 'true' ) {
	push( @{$downStream->{"SUBSCRIBE_$dataVersionString"}}, 
	    "publish_mirador/pending_publish" );
	push( @{$downStream->{"PUBLISH_$dataVersionString"}}, 
	    "publish_mirador/pending_publish" );
	push( @{$downStream->{"INTRA_VERSION_DELETE_$dataVersionString"}}, 
	    "publish_mirador/pending_delete" );
	push( @{$downStream->{"INTER_VERSION_DELETE_$dataVersionString"}}, 
	    "publish_mirador/pending_delete" );
        push( @{$cfgPublish->{$dataAttr->{NAME}}{$versionLabel}},
            "publish_mirador/pending_publish" );
    }

    my $giovanniFlag = ( defined $versionAttr )
        ? $versionAttr->{PUBLISH_GIOVANNI} : $dataAttr->{PUBLISH_GIOVANNI};
    if ( $giovanniFlag eq 'true' ) {
	push( @{$downStream->{"SUBSCRIBE_$dataVersionString"}}, 
	    "publish_giovanni/pending_publish" );
	push( @{$downStream->{"PUBLISH_$dataVersionString"}}, 
	    "publish_giovanni/pending_publish" );
	push( @{$downStream->{"INTRA_VERSION_DELETE_$dataVersionString"}}, 
	    "publish_giovanni/pending_delete" );
	push( @{$downStream->{"INTER_VERSION_DELETE_$dataVersionString"}}, 
	    "publish_giovanni/pending_delete" );
        push( @{$cfgPublish->{$dataAttr->{NAME}}{$versionLabel}},
            "publish_giovanni/pending_publish" );
    }

    my $userFlag = ( defined $versionAttr )
        ? $versionAttr->{PUBLISH_USER} : $dataAttr->{PUBLISH_USER};
    if ( $userFlag eq 'true' ) {
	push( @{$downStream->{"SUBSCRIBE_$dataVersionString"}}, 
	    "publish_user/pending_publish" );
	push( @{$downStream->{"PUBLISH_$dataVersionString"}}, 
	    "publish_user/pending_publish" );
	push( @{$downStream->{"INTRA_VERSION_DELETE_$dataVersionString"}}, 
	    "publish_user/pending_delete" );
	push( @{$downStream->{"INTER_VERSION_DELETE_$dataVersionString"}}, 
	    "publish_user/pending_delete" );
        push( @{$cfgPublish->{$dataAttr->{NAME}}{$versionLabel}},
            "publish_user/pending_publish" );
    }

    push ( @{$downStream->{"SUBSCRIBE_$dataVersionString"}},
	"giovanni/preprocess" ) if defined $dataAttr->{METHOD}{GIOVANNI};
}
###############################################################################
# =head1 CreateMergePanStation
# 
# Description
#   Creates MergePAN stations.
#
# =cut
###############################################################################
sub CreateMergePanStation
{
    my ( $doc, $global ) = @_;
    my $logger = $global->{LOGGER}{LOGGING} if defined $global->{LOGGER}{LOGGING};

    # Create merge PAN directory
    my $mergePanDir = $global->{S4PA_ROOT} . 'merge_pan/';

    # Master log file
    my $logFile = defined $global->{LOGGER}
        ? "$global->{LOGGER}{DIR}/polling.log" : undef;

    # Failure handler to retry a failed job.
    my $failureHandler = { 'Retry Job' => "perl -e 'use S4P; S4P::remove_job()"
                                          . " if S4P::restart_job()'",
                           'Remove Job' => "perl -e 'use S4P; S4P::remove_job()'",
                           'Continue' => "s4pa_merge_pan.pl -c -s -f ../s4pa_merge_pan.cfg DO.* && send_downstream.pl -l RETRY.MERGE*.log && remove_job.pl"
                         };

    my $downStream = {};
    $downStream->{"PUSH"} = [ "postoffice" ];
    $downStream->{"RETRY"} = [ "merge_pan" ];

    my $cmdHash = {
        "MERGE" => "s4pa_merge_pan.pl -s -f '../s4pa_merge_pan.cfg'",
        "RETRY" => "s4pa_merge_pan.pl -s -f '../s4pa_merge_pan.cfg'",
    };

    # A hash ref to hold station configuration.
    my $stationConfig = {
            cfg_station_name =>  "MergePAN",
            cfg_root => $global->{S4PA_ROOT},
            cfg_group => $global->{S4PA_GID},
            cfg_max_time => 600,
            cfg_sort_jobs => 'FIFO',
            cfg_max_failures => 1,
            cfg_polling_interval => 600,
            cfg_stop_interval => 4,
            cfg_end_job_interval => 2,
            cfg_restart_defunct_jobs => 1,
            cfg_max_children => 1,
            cfg_failure_handlers => $failureHandler,
            cfg_commands => $cmdHash,
            cfg_downstream => $downStream,
            cfg_ignore_duplicates => 1,
            cfg_umask => 022,
            __TYPE__ => {
                cfg_failure_handlers => 'HASH',
                cfg_commands => 'HASH',
                cfg_downstream => 'HASH'
                }
            };

    # Add an interface to retry all failed jobs
    $stationConfig->{cfg_interfaces} = {
        'Retry Failed Jobs' => qq(s4p_restart_all_jobs.pl)
        };  
    # Add an interface to view logs if logging is enabled
    $stationConfig->{cfg_interfaces}{'View Log'} =
        qq( perl -e 'use S4PA; S4PA::ViewFile( FILE => "$logFile" )' )
        if ( defined $logFile );
    # Add an interface to edit station config
    $stationConfig->{cfg_interfaces}{'Edit Station Config'} =
      qq( perl -e 'use S4PA; S4PA::EditFile( FILE => "$mergePanDir/station.cfg", TITLE => "MergePAN" )' );
    # Add an interface to remove Stale log
    $stationConfig->{cfg_interfaces}{'Remove Stale Log'} =
      qq( perl -e 'use S4PA; S4PA::RemoveStaleLog( STATION => "$mergePanDir", TITLE => "MergePAN" )' );
    $stationConfig->{__TYPE__}{cfg_interfaces} = 'HASH';
    # Create the station
    S4PA::CreateStation( $mergePanDir, $stationConfig, $logger );

# Get information on PAN destinations
    my $panPush = {};
    my $panDir = {};
    foreach my $provider ( $doc->findnodes( 'provider' ) ) {
        my $localPanDir = GetNodeValue( $provider, 'pan/local' );
        foreach my $node ( $provider->findnodes( 'pan/remote/originating_system' ) ) {
            my $name = $node->getAttribute( 'NAME' );
            my $host = $node->getAttribute( 'HOST' );
            my $dir = $node->getAttribute( 'DIR' );
            my $notify = $node->getAttribute( 'NOTIFY' );
            DeployLogging( 'fatal', "Failed to get originating system's name for sending PANs" )
                unless defined $name;

            unless ( defined $notify ) {
                DeployLogging( 'fatal', "Failed to get host name for sending PANs for "
                    . "originating_system=$name" )
                    unless defined $host;
                DeployLogging( 'fatal', "Failed to get dir for sending PANs for "
                    . "originating_system=$name, host=$host" )
                    unless defined $dir;
            }
            $panPush->{$name} = { host => $host, dir => $dir, notify => $notify };
            $panDir->{$name} = $localPanDir;
        }
    }

    # Write out ReceiveData station specific configuration.
    my $config = {
        cfg_retention_time => 7200,
        cfg_protocol => $global->{PROTOCOL},
        cfg_pan_destination => $panPush,
        cfg_pan_dir => $panDir,
        __TYPE__ => {
            cfg_protocol => 'HASH',
            cfg_pan_destination => 'HASH',
            cfg_pan_dir => 'HASH'
            }
        };

    if ( defined $global->{LOGGER} ) {
        $config->{cfg_logger} = {
            LEVEL => $global->{LOGGER}{LEVEL},
            FILE => $logFile };
        $config->{__TYPE__}{cfg_logger} = 'HASH';
    }

    S4PA::WriteStationConfig( 's4pa_merge_pan.cfg', $mergePanDir, $config );
    return $mergePanDir;
}
###############################################################################
# =head1 DeployLogging
# 
# Description
#   Logging info/error message into NDC stack and STDERR
#
# =cut
###############################################################################
sub DeployLogging
{
    my ( $level, $msg ) = @_;
    print STDERR "$msg\n";
    if ( $level =~ /(error|warn)/i ) {
        $logger->error( "$msg" ) if defined $logger;
        Log::Log4perl::NDC->push( "$msg" );
    } elsif ( $level =~ /fatal/i ) {
        $logger->error( "Deployment terminated: $msg" ) if defined $logger;
        exit;
    } else {
        $logger->info( "$msg" );
    }
}
###############################################################################
# =head1 CreateAssociateDb
# 
# Description
#   Create associate.db under each associated dataset
#
# =cut
###############################################################################
sub CreateAssociateDb
{
    my ( $global ) = @_;
    my $cpt = new Safe 'CLASS';
    DeployLogging( 'fatal', "Failed to read dataset->data class mapping ($!)" )
        unless $cpt->rdo( "$global->{S4PA_ROOT}/storage/dataset.cfg" );

    foreach my $key ( keys %CLASS::data_association ) {
        my $associate = $CLASS::data_association{$key};
        my ( $dataset, $version ) = split '\.', $associate, 2;
        my $dataClass = $CLASS::data_class{$dataset};
        DeployLogging( 'error', "No dataClass defined for associated dataset: $dataset" )
            unless ( defined $dataClass );
        my $datasetDir = "$global->{S4PA_ROOT}/storage/$dataClass/$associate";
        DeployLogging( 'error', "Dataset direcotry '$datasetDir' does not exist" )
            unless ( -d $datasetDir );

        my $associateDbFile = "$datasetDir/associate.db";
        unless ( -f $associateDbFile ) {
            my ( $associateRef, $fileHandle ) = 
                S4PA::Storage::OpenGranuleDB( $associateDbFile, "w" );
            if ( defined $associateRef ) {
                S4PA::Storage::CloseGranuleDB( $associateRef, $fileHandle );
                DeployLogging( 'info', "Creating $associateDbFile" );
            } else {
                DeployLogging( 'error', "Failed to create $associateDbFile" );
            }
        }
    }
}

###############################################################################
# =head1 PublishDescriptor
# 
# Description
#   Returns the postoffice work order for pushing descriptor
#   file to dotchart database host. Return undef with message if failed.
#
# =cut
###############################################################################
sub PublishDescriptor
{
    my ( $doc, $global ) = @_;
    my $logger = $global->{LOGGER}{LOGGING} if defined $global->{LOGGER}{LOGGING};

    # get Instance info
    my $instance = $global->{S4PA_NAME};
    my $s4paRoot = $global->{S4PA_ROOT};
    $s4paRoot =~ s/\/$//;
    my $workOrder = "${s4paRoot}/postoffice/DO.PUSH.DotchartDes_${instance}.wo";
    my $localPath = "${s4paRoot}/tmp/DotchartDes.${instance}.xml";

    # get Host/Protocal listing
    my $dotchartHost = $global->{DOTCHART}{COLLECTION}{HOST};
    my $dotchartDir = $global->{DOTCHART}{COLLECTION}{DIR};
    unless ( $dotchartDir =~ /^\// ) {
        DeployLogging( 'error', "Dotchart/collectionInsert DIR does not start with '/'." );
        return undef;
    }
    my $dotchartProtocol = ( defined $global->{PROTOCOL}{$dotchartHost} ) ?
        lc( $global->{PROTOCOL}{$dotchartHost} ) : 'ftp';

    # Create an XML DOM parser.
    my $parser = XML::LibXML->new();
    $parser->keep_blanks(0);

    # create an XML document containing one FilePacket node, which
    # contains one FileGroup node, which contains one File node.
    my $woDom = $parser->parse_string('<FilePacket/>');
    my $woDoc = $woDom->documentElement();

    # set attributes of the FilePacket node to describe a destination
    my ($filePacketNode) = $woDoc->findnodes('/FilePacket');
    $filePacketNode->setAttribute('status', 'I');
    my $destination = "$dotchartProtocol:$dotchartHost$dotchartDir";
    $filePacketNode->setAttribute('destination', $destination);
    
    # Set attributes of the File node to specify the local path of the file
    # being sent
    my $fileNode = XML::LibXML::Element->new('File');
    $fileNode->setAttribute('status', 'I');
    $fileNode->setAttribute('localPath', $localPath);
    $fileNode->setAttribute('cleanup', 'Y');

    my $fileGroupNode = XML::LibXML::Element->new('FileGroup');
    $fileGroupNode->appendChild($fileNode);
    $woDoc->appendChild($fileGroupNode);

    if ( open( DESCRIPTOR, "> $localPath" ) ) {
        print DESCRIPTOR $doc->toString(1);
        close( DESCRIPTOR );
    } else {
        DeployLogging( 'error', "Could not open $localPath for writing." );
        return undef;
    }

    if ( open( DESWO, "> $workOrder") ) {
        print DESWO $woDom->toString(1);
        close( DESWO );
    } else {
        unlink $localPath;
        DeployLogging( 'error', "Could not open $workOrder for writing." );
        return undef;
    }

    return $workOrder;
}

###############################################################################
# =head1 PrepareConfig
# 
# Description
#   Wrapper for deploy from cvs pository. 
#
# =cut
###############################################################################
sub PrepareConfig {
    my ( %arg ) = @_;

    my $confName = 'S4PA_CONFIG';
    my $scmCommand = 'cvs';
    my $scmCheckout = "$scmCommand checkout";
    my $scmCoLabel = "$scmCommand checkout -r";

    my $instanceName =  $arg{INSTANCE};
    my $s4paRelease = $arg{S4PA_VERSION};
    my $projectName = $arg{PROJECT};
    my $projectRelease = $arg{PROJECT_RELEASE};
    my $skipXSL = $arg{SKIP_XSL};
    
    my $descriptor = 'descriptor_' . $instanceName . '.xml';
    my $subscription = 'subscription_' . $instanceName . '.xml';
    
    # create temp working directory and cd there
    my $tmpDir = tempdir( CLEANUP => 1 );
    die "Can't not create a temporary directory: $!\n"
        unless defined $tmpDir;
    die "Failed to cd to $tmpDir: $!\n"
        unless ( chdir "$tmpDir");
    
    # checkout descriptor
    print "checking out with '$scmCheckout $confName/$descriptor'\n";
    my $result = `$scmCheckout $confName/$descriptor`;
    my $descriptorPath = "$tmpDir/$confName/$descriptor";
    die "Failed to checkout descriptor: $!\n"
        unless ( -f "$descriptorPath" );

    # checkout subscription configuration
    print "checking out with '$scmCheckout $confName/$subscription'\n";
    $result = `$scmCheckout $confName/$subscription`;
    my $subscriptionPath = "$tmpDir/$confName/$subscription";
    my $updateSubFlag = ( -f $subscriptionPath ) ? 1 : 0;
    
    # Create an XML DOM parser.
    my $xmlParser = XML::LibXML->new();
    $xmlParser->keep_blanks(0);
    
    # Parse the descriptor file.
    my $dom = $xmlParser->parse_file( $descriptorPath );
    my $doc = $dom->documentElement();
    
    # Create config directory under s4pa_root
    my $s4paRoot = GetNodeValue( $doc, '/s4pa/root' );
    $s4paRoot =~ s/\/+$//;
    unless ( -d $s4paRoot ) {
        die "Cannot mkdir $s4paRoot: $!\n"
            unless ( mkdir( $s4paRoot, 0775 ) );
    }
    my $configDir = "$s4paRoot/config";
    unless ( -d $configDir ) {
        die "Cannot mkdir $configDir: $!\n"
            unless ( mkdir( $configDir, 0775 ) );
    }
    
    # Start working under config directory
    die "Failed to cd to $configDir: $!\n"
        unless ( chdir "$configDir");
    
    # checkout S4PA
    my $s4paLabel;
    if ( $s4paRelease ) {
        $s4paRelease =~ s/\./\_/g;
        my $s4paLabel = 'Release-' . $s4paRelease;
    } else {
        my $line = `grep "@@@" $0`;
        $line =~ /(Release-\d+_\d+_\d+)/;
        $s4paLabel = $1;
    }
    if ( defined $s4paLabel ) {
        print "checking out with '$scmCoLabel \"$s4paLabel\" S4PA/doc'\n";
        $result = `$scmCoLabel "$s4paLabel" S4PA/doc`;
    } else {
        print "checking out with '$scmCheckout S4PA/doc'\n";
        $result = `$scmCheckout S4PA/doc`;
    }
    
    # locate descriptor and subscription schema
    my $s4paSchema = "$configDir/" . 'S4PA/doc/xsd/S4paDescriptor.xsd';
    my $subSchema = "$configDir/" . 'S4PA/doc/xsd/S4paSubscription.xsd';
    die "Failed to checkout S4PA schema: $!\n"
        unless ( -f $s4paSchema );
    
    # Validate using the specified schema
    my $schema = XML::LibXML::Schema->new( location => $s4paSchema );
    die "Failed to read XML schema, $s4paSchema\n" unless $schema;
    
    eval { $schema->validate( $dom ); };
    die "Failed to validate $descriptorPath with $s4paSchema\n$@\n" if $@;
    
    # checkout project
    my $projectFlag = 0;
    my @projects;
    unless ( $projectName eq '' ) {
        $projectFlag = 1;
        @projects = split /\,/, $projectName;
        my @releases = split /\,/, $projectRelease;
        for ( my $i = 0; $i < @projects; $i++ ) {
            my $project = $projects[$i];
            my $release = $releases[$i];
            if ( $release eq '' ) {
                print "checking out with '$scmCheckout $project/cfg'\n";
                $result = `$scmCheckout $project/cfg`;
            } else {
                print "checking out with '$scmCoLabel \"$release\" $project/cfg'\n";
                $result = `$scmCoLabel "$release" $project/cfg`;
            }
        }
    }
    
    my $newProject = XML::LibXML::Element->new( 'project' );
    my $s4paLocation = XML::LibXML::Element->new( 'location' );
    $s4paLocation->appendText( "$configDir/S4PA" );
    $newProject->appendChild( $s4paLocation );
    if ( $projectFlag ) {
        foreach my $project ( @projects ) {
            my $projLocation = XML::LibXML::Element->new( 'location' );
            $projLocation->appendText( "$configDir/$project" );
            $newProject->appendChild( $projLocation );
        }
    }
    
    my ( $oldProject ) = $doc->findnodes( '/s4pa/project' );
    if ( $oldProject ) {
        $oldProject->replaceNode( $newProject );
    } else {
        my ( $loggerNode ) = $doc->findnodes( '/logger' );
        $doc->insertAfter( $loggerNode, $newProject );
    }
    
    # create the new descriptor file under config directory
    my $newDescriptor = "$configDir/$descriptor";

    # backup curent descriptor under config directory
    my $timestamp = `date +%Y%m%d%H%M`;
    chomp($timestamp);
    if (-f $newDescriptor) {
        my $oldDescriptor = $newDescriptor . ".$timestamp";
        if (copy($newDescriptor, $oldDescriptor)) {
            print "Backup current descriptor to $oldDescriptor\n";
        }
    }

    die "Can't open $newDescriptor: $!\n"
        unless ( open (OUTFILE, "> $newDescriptor") );
    print OUTFILE $dom->toString(1);
    die "Failed to create $newDescriptor: $!\n"
        unless ( close OUTFILE );
    
    # return the deploy action if no subscription was provided
    my $deploy = "s4pa_deploy.pl -f $newDescriptor -s $s4paSchema $skipXSL";
    return ( $deploy, undef ) unless ( $updateSubFlag );

    my $newSubscription = "$configDir/$subscription";
    # backup curent subscription under config directory
    if (-f $newSubscription) {
        my $oldSubscription = $newSubscription . ".$timestamp";
        if (copy($newSubscription, $oldSubscription)) {
            print "Backup current subscription to $oldSubscription\n";
        }
    }

    # copy the subscription configuration to config directory
    die "Failed to copy $subscriptionPath to $configDir: $!\n"
        unless ( copy( $subscriptionPath, $configDir ) );

    # return both deploy action and update subscription action
    my $updateSub = "s4pa_update_subscription.pl -d $newDescriptor " .
        "-f $newSubscription -s $subSchema";
    return $deploy, $updateSub;
}

###############################################################################
# =head1 CreateActiveFsList
# 
# Description
#   Create a volume configuration file under receiving station
#
# =cut
###############################################################################
sub CreateActiveFsList {
    my ( $fsListFile, $activeFs ) = @_;
    my $fsRoot = dirname( $activeFs );
    my @list;
    if ( opendir( DH, $fsRoot ) ) {
        @list = grep( !/^\./, readdir( DH ) );
        closedir( DH );
    } else {
        S4P::logger( 'ERROR', "Failed to open $fsRoot for reading" );
        return undef;
    }

    open( FH, ">$fsListFile" ) or return undef;
    print FH "$fsRoot\n";
    foreach my $dir ( sort @list ) {
        print FH "$dir\n" if ( -d "$fsRoot/" . $dir );
    }
    unless ( close( FH ) ) {
        S4P::logger( "ERROR", "Failed to close $fsListFile ($!)" );
        unlink $fsListFile;
        return undef;
    }
    my $numVolume = scalar( @list );
    return "Added $numVolume volume(s) to $fsListFile";
}

###############################################################################
# =head1 SearchCollection
# 
# Description
#   Search CMR concept-id for metadata link
#
# =cut
###############################################################################
sub SearchCollection {

    my ($doc, $global) = @_;
    # collect CMR collection metadata needs
    my $needToken = 0;
    my $collectionInfo = {};
    foreach my $provider ($doc->findnodes('provider')) {
        my $providerName = $provider->getAttribute('NAME');
        foreach my $dataClass ($provider->findnodes('dataClass')) {
            my $classAttr = GetDataAttributes($dataClass);
            my $className = $classAttr->{NAME};
            foreach my $dataset ($dataClass->findnodes('dataset')) {
                my $dataAttr = GetDataAttributes($dataset, $classAttr);
                my $dataName = $dataAttr->{NAME};
                foreach my $dataVersion ($dataset->findnodes('dataVersion')) {
                    my $versionAttr = GetDataAttributes($dataVersion, $dataAttr);
                    my $versionId = $versionAttr->{LABEL} || '';
                    if (defined $versionAttr->{COLLECTION_SHORTNAME}) {
                        $collectionInfo->{$providerName}{$className}{$dataName}{$versionId}{'shortname'} = $versionAttr->{COLLECTION_SHORTNAME};
                    } else {
                        $collectionInfo->{$providerName}{$className}{$dataName}{$versionId}{'shortname'} = $dataName;
                    }
                    if (defined $versionAttr->{COLLECTION_VERSION}) {
                        $collectionInfo->{$providerName}{$className}{$dataName}{$versionId}{'version'} = $versionAttr->{COLLECTION_VERSION};
                    } else {
                        $collectionInfo->{$providerName}{$className}{$dataName}{$versionId}{'version'} = $versionId;
                    }
                    if (defined $versionAttr->{COLLECTION_LINK}) {
                        $collectionInfo->{$providerName}{$className}{$dataName}{$versionId}{'link'} = $versionAttr->{COLLECTION_LINK};
                        if ($versionAttr->{COLLECTION_LINK} eq 'CMR') {
                            $needToken = 1;
                        }
                    } else {
                        $collectionInfo->{$providerName}{$className}{$dataName}{$versionId}{'link'} = 'S4PA';
                    }
                }
            }
        }
    }

    # get a CMR token for deployment use
    if ($needToken) {
        my ($cmrToken, $errmsg);
        # CMR switch from Earthdata login to Launchpad token for authentication
        if (defined $global->{CMR}{CERT_FILE}) {
            my $tokenParam = {};
            $tokenParam->{LP_URI} = $global->{CMR}{CMR_TOKEN_URI};
            $tokenParam->{CMR_CERTFILE} = $global->{CMR}{CERT_FILE};
            $tokenParam->{CMR_CERTPASS} = $global->{CMR}{CERT_PASS};
            ($cmrToken, $errmsg) = S4PA::get_cmr_token($tokenParam);
            unless (defined $cmrToken) {
                DeployLogging('warn', "Unable to get Launchpad token: $errmsg");
            }

        # the original token acquiring from CMR/ECHO
        } else {
            my $tokenParam = {};
            $tokenParam->{ECHO_URI} = $global->{CMR}{CMR_TOKEN_URI};
            $tokenParam->{CMR_USERNAME} = $global->{CMR}{USERNAME};
            my $cmr_encrypted_pwd = $global->{CMR}{PASSWORD};
            $tokenParam->{CMR_PASSWORD} = Clavis::decrypt($cmr_encrypted_pwd) if $cmr_encrypted_pwd;
            $tokenParam->{CMR_PROVIDER} = $global->{CMR}{PROVIDER};
            ($cmrToken, $errmsg) = S4PA::get_cmr_token($tokenParam);
            unless (defined $cmrToken) {
                DeployLogging('warn', "Unable to get Launchpad token: $errmsg");
            }

            # my $cmr_username = $global->{CMR}{USERNAME};
            # my $cmr_encrypted_pwd = $global->{CMR}{PASSWORD};
            # my $cmr_decrypted_pwd = Clavis::decrypt( $cmr_encrypted_pwd ) if $cmr_encrypted_pwd;
            # $cmrToken = login($cmr_username, $cmr_decrypted_pwd, $global->{CMR}{PROVIDER});
        }

        my $ua = LWP::UserAgent->new;
        my $cmrProvider = $global->{CMR}{PROVIDER};
        my $baseUri = $global->{CMR}{CMR_ENDPOINT_URI} . 'search/collections.umm-json?provider=' .
            $global->{CMR}{PROVIDER};
        foreach my $provider (keys %{$collectionInfo}) {
            foreach my $class (keys %{$collectionInfo->{$provider}}) {
                foreach my $set (keys %{$collectionInfo->{$provider}{$class}}) {
                    foreach my $ver (keys %{$collectionInfo->{$provider}{$class}{$set}}) {
                        next unless ($collectionInfo->{$provider}{$class}{$set}{$ver}{'link'} eq 'CMR');
                        my $shortname = $collectionInfo->{$provider}{$class}{$set}{$ver}{'shortname'};
                        my $version = $collectionInfo->{$provider}{$class}{$set}{$ver}{'version'};
                        my $restUrl = $baseUri . '&short_name=' . $shortname .
                            '&version=' . $version;
                        my $request = HTTP::Request->new('GET', $restUrl, [Echo_Token => $cmrToken]);
                        my $response = $ua->request($request);
                        if ($response->is_success) {
                            my $dif = $response->content;
                            my $jsonRef = decode_json($dif);
                            if ((exists $jsonRef->{'hits'}) && ($jsonRef->{'hits'}) == 0) {
                                DeployLogging('warn', "Warning: Unable to find collection in CMR for $set.$ver");
                                next;
                            }
                            my $conceptId;
                            foreach my $coll (@{$jsonRef->{'items'}}) {
                                $conceptId = $coll->{'meta'}{'concept-id'};
                            }
                            $collectionInfo->{$provider}{$class}{$set}{$ver}{'concept_id'} = $conceptId;
                        } else {
                            DeployLogging('warn', "Failed on CMR search request for $set.$ver");
                        }
                    }
                } 
            }
        }
    }

    return $collectionInfo;
}
    
###############################################################################
# =head1 Usage
# 
# Description
#   Display script usage and option menu
#
# =cut
###############################################################################
sub Usage {
  die << "EOF";
Usage: $0 <-f s4pa_descriptor> <-s s4pa_schema> | 
          <-i instance_name>] [options]
Options are:
        -v s4pa_version      s4pa version (ex. 3.33.0), default to this script's label 
        -p project           project cvs repository name for metadata template
        -r project_release   project's cvs tag, default to current cvs version.
        -x                   skip xsl installation.
EOF
}

