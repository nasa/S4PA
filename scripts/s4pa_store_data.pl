#!/usr/bin/perl

=head1 NAME

s4pa_store_data.pl - the station script for StoreDataset stations in S4PA.

=head1 SYNOPSIS

s4pa_store_data.pl <work order>

=head1 ABSTRACT

B<Pseudo code:>

    Read the work order (PDR).
    Extract file groups from the PDR.
    For each file group
        Extract dataset/data type and metadata file.
        Check whether version of file group is supported
        For each supported version of the dataset
            Check whether the file group is a replacement for existing data.
            If the file group is a replacement
                If the file group is older than existing data
                    Delete all files in the file group
                    Continue to next file group
                Endif
                Mark the file group for deletion.
                Save marked file group according to whether it replaces
                    a group with the same version or one with a different
                    version
            Endif
        Endfor
        Extract data files (type=SCIENCE).
        For each data file in file group
            Call S4PA::Storage::StoreData() with the paired data
                and metadata files to create symbolic links to data downloaded
                on file system.
            Exit with error if S4PA::Storage::StoreData() returns false.
        Endfor
        Create a file group consisting of symbolic links returned by StoreData()
        Add the file group to the subscription work order
    Endfor

    Write a delete work order (PDR) of file groups marked for deletion,
        for file groups whose version matches the incoming version
    Write a delete work order (PDR) of file groups marked for deletion,
        for file groups whose version does not match the incoming version
    Write a subscription work order (PDR) of file groups of symbolic links.
    End

=head1 DESCRIPTION

s4pa_store_data.pl is the station script for StoreDataset stations in S4PA.
It is a wrapper for S4PA::Storage::StoreData(). The input work order should be
in the form of a PDR. S4PA::Storage::StoreData() is invoked with the data and
metadata file names extracted from the PDR. If any invocation of
S4PA::Storage::StoreData() fails (returns false), the station exits with
failure.

=head1 SEE ALSO

L<S4PA>
L<S4PA::Storage>

=head1 AUTHOR

M. Hegde

=cut

################################################################################
# $Id: s4pa_store_data.pl,v 1.28 2019/06/20 13:45:58 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use Getopt::Std;
use S4PA::Storage;
use S4PA::Metadata;
use S4PA;
use S4P::PDR;
use Safe;
use S4P::FileGroup;
use S4P::FileSpec;
use File::Basename;
use File::Copy;
use File::stat;
use Log::Log4perl;
use Cwd;
use S4P;

use vars qw( $opt_f $opt_c $opt_s );

# Expect a configuration file passed via -f command line switch. It contains
# a hash whose keys are dataset names. The hash values are time margin in
# seconds, used to determine whether an incoming granule is a replacement for
# an existing granule. By default, it is zero, meaning that the start date time
# of the incoming granule must exactly match the existing granule.
getopts( 'f:cs' );
S4P::perish( 1, "Failed to find config file $opt_f" ) unless ( defined $opt_f );

# Try to read the configuration file, and on failure, stop.
my $cpt = new Safe( 'CFG' );
$cpt->rdo( $opt_f ) or S4P::perish( 1, "Failed to read conf file $opt_f ($@)" );

# Expect the work order, a PDR, to be the argument.
S4P::perish( 1, "Specify a work order" ) unless defined $ARGV[0];

# Extract job ID; fail if unable to get one.
my $id = ( $ARGV[0] =~ /^DO\.[^.]+\.(.+)/ ) ? $1 : undef;
S4P::perish( 1, "Failed to find the job ID" ) unless defined $id;

# continue store or skip flag
my $continueFlag = 0;
my $skipFlag = 0;

# Slurp in the PDR as an S4P::PDR object; work order is expected to be a PDR.
my $pdrName = basename( $ARGV[0] );
my $pdr = S4P::PDR::read_pdr( $ARGV[0] );

# Create a logger
my $logger = S4PA::CreateLogger( $CFG::cfg_logger{FILE},
    $CFG::cfg_logger{LEVEL} ) if ( %CFG::cfg_logger );

$logger->info( "Processing " . basename( $ARGV[0] ) ) if defined $logger;
    
# Extract file groups in the PDR
my @fileGroupList = @{$pdr->file_groups()};
unless ( @fileGroupList > 0 ) {
    my $message = "No file groups found in " . basename( $ARGV[0] );
    S4P::perish( 1, $message );
    $logger->info( $message );
}

# Create a hash of output PDRs; keys are dataset names.
# (If we are guaranteed that there will be only one dataset, a hash
# is not needed).
my %outPdrHash;
my %deletePdrHash;
my %republishHash;
my %backupFileHash;

# An array to store file groups with a version matching the incoming
# version that need to be flagged for deletion.
my @delete_matched_v_file_groups = ();
# An array to store file groups with a version not matching the incoming
# version that need to be flagged for deletion.
my @delete_unmatched_v_file_groups = ();

# locate s4pa root directory, assuming we are running under storage job directory
my $currentDir = cwd();
my $stationDir = dirname( $currentDir );
my $s4paRoot = dirname(dirname(dirname(dirname($currentDir))));

# Loop over the file group list.
FILEGROUP: foreach my $fileGroup ( @fileGroupList ) {
    # Get the dataset/data-type for the group.
    my $dataset = $fileGroup->data_type();

    unless ( defined $dataset ) {
        $logger->error( "Storage of $pdrName failed; dataset not found for "
            . "a granule" ) if defined $logger;
        S4P::perish( 1, "Dataset not found in PDR" );
    }

    # Get the metadata filename for the group.
    my $metadataFile = $fileGroup->met_file();
    if ( $metadataFile eq '0' ) {
        $logger->error( "Storage of $pdrName failed; metadata file not found "
            . "for a granule" ) if defined $logger;
        S4P::perish( 1, "Metadata file not found" );
    }

    # Check that the data version for the file group is supported
    my @supported_versions;
    if ( %CFG::cfg_data_version
         && defined $CFG::cfg_data_version{$dataset} ) {
        @supported_versions = @{$CFG::cfg_data_version{$dataset}};
    } else {
        $logger->error( "$dataset in $pdrName has no supported versions" )
            if defined $logger;
        S4P::perish( 1, "%cfg_data_version value for dataset $dataset" .
                        " not found in configuration" );
    }
    my $incoming_version = $fileGroup->data_version();
    my $stored_version;
    foreach my $supported_version ( @supported_versions ) {
        if ( $supported_version ne '' ) {
            if ( $incoming_version eq $supported_version ) {
                # The incoming version is supported and will be stored
                # as that version.
                $stored_version = $supported_version;
                last;
            }
        } else {
            # If one of the supported versions is '', then
            # the incoming version is supported, and will
            # be stored as version '' (i.e. no version at all).
            $stored_version = $supported_version;
            last;
        }
    }
    unless ( defined $stored_version ) {
        $logger->error( "Metadata file $metadataFile in $pdrName has version $incoming_version, which is not supported" );
        S4P::perish( 1, "Metadata file $metadataFile has version"
            . " $incoming_version, which is not supported in the configuration" );
    }


    # Check each supported version to see if its files should be marked for
    # replacement by the files of the incoming granule
    my @replace_list;
    foreach my $supported_version ( @supported_versions ) {

        my $time_margin;
        if ( defined $CFG::cfg_time_margin{$dataset} &&
             defined $CFG::cfg_time_margin{$dataset}->{$supported_version} ) {
            $time_margin
                = $CFG::cfg_time_margin{$dataset}->{$supported_version};
        } else {
            $time_margin = 0;
        }

        my $xpath;
        if ( defined $CFG::cfg_xpath{$dataset}
             && defined $CFG::cfg_xpath{$dataset}->{$supported_version} ) {
            $xpath = $CFG::cfg_xpath{$dataset}->{$supported_version};
        }

        my $ignoreXpath;
        if ( defined $CFG::cfg_ignore_xpath{$dataset}
             && defined $CFG::cfg_ignore_xpath{$dataset}->{$supported_version} ) {
            $ignoreXpath = $CFG::cfg_ignore_xpath{$dataset}->{$supported_version};
        }

        # Check for a match with an existing granule for this supported version.
        # If a match is found, get a list of files to be replaced,
        # starting with the metadata file, and followed by the data file(s).
        ( @replace_list ) = S4PA::Storage::IsReplaceData( $dataset,
                                                          $supported_version,
                                                          $metadataFile,
                                                          $time_margin,
                                                          $xpath,
                                                          $ignoreXpath );

        my $deleteFileGroup = 0;

        # command line option take the highest precedence,
        # if that is not set, check the configured %cfg_multiple_replacement
        # for continue store or skip store flag
        if ($opt_c) {
            $continueFlag = 1;
        } elsif (exists $CFG::cfg_multiple_replacement{$dataset}{$stored_version}) {
            $continueFlag = 1 
                if ($CFG::cfg_multiple_replacement{$dataset}{$stored_version} eq 'STORE' && ! $opt_s);
        } else {
            $continueFlag = 0;
        }
        if ($opt_s) {
            $skipFlag = 1;
        } elsif (exists $CFG::cfg_multiple_replacement{$dataset}{$stored_version}) {
            $skipFlag = 1 
                if ($CFG::cfg_multiple_replacement{$dataset}{$stored_version} eq 'SKIP' && ! $opt_c);
        } else {
            $skipFlag = 0;
        }

        # Unless continue switch was seelcted in the failure hander,
        # we will check if the incoming granule is going to replace 
        # multiple existing granules, or is qualified to be ignored,
        # or is older than the existing granule.
        unless ( $continueFlag ) {
            # Check if incoming granule has marked multiple existing 
            # granules to be replaced
            if ( S4PA::Storage::IsMultipleDelete() ) {
                # If the incoming granule trigger multiple deletion
                # and <Skip> switch was selected in the failure handler,
                # set the delete file group flag.
                unless ( $skipFlag ) {
                    S4P::perish( 1, "There are more than one granule marked to be replaced. "
                                  . "Select <Continue Storing> to mark all granules as delete, or "
                                  . "select <Skip Storing> to drop the incoming granule." );
                }
                $deleteFileGroup = 1;

            # check if the incoming granule is qualified to be ignored
            } elsif ( S4PA::Storage::IsIgnoreData() ) {
                # If the incoming granule is determined to be ignored
                # and <Skip> switch was selected in the failure handler,
                # set the delete file group flag.
                unless ( $skipFlag ) {
                    S4P::perish( 1, "Incoming granule qualified to be ignored. "
                                  . "Select <Continue Storing> to force the incoming granule to "
                                  . "replace the existing ones, or "
                                  . "select <Skip Storing> to drop the incoming granule." );
                }
                $deleteFileGroup = 1;

            # Check if the incoming granule is older than existing matching granules
            } elsif ( S4PA::Storage::IsOldData() ) {
                # If the incoming granule is determined to be an old granule
                # and <Skip> switch was selected in the failure handler,
                # set the delete file group flag.
                unless ( $skipFlag ) {
                    S4P::perish( 1, "Incoming granule is older than the existing one. "
                                  . "Select <Continue Storing> to force the incoming granule to "
                                  . "replace the existing ones, or "
                                  . "select <Skip Storing> to drop the incoming granule." );
                }
                $deleteFileGroup = 1;
            }
        }
    
        # delete all files from this fileGroup
        if ( $deleteFileGroup ) {
            my @fileList = $fileGroup->science_files();
            push( @fileList, $metadataFile );
            # add browse if exist
            my $browseFile = $fileGroup->browse_file();
            push( @fileList, $browseFile ) if ( $browseFile );
            # add hdf4map if exist
            my $mapFile = $fileGroup->map_file();
            push( @fileList, $mapFile ) if ( $mapFile );
            foreach my $file ( @fileList ) {
                S4P::logger( "INFO", "Removing $file as it was flagged as old" );
                unlink $file || S4P::perish( 1, "Failed to remove $file ($!)" );
            }
            # Skip rest of the processing and go to next file group
            next FILEGROUP;
        }

        # If files to be replaced were found, create a new group for them,
        foreach my $file_name ( @replace_list ) {
            # we can't not use the whole list return from IsReplaceData
            # since that might have multiple granules in it.
            # only parse metadata file here and create fileGroup
            # for each granules, ticket #7666.
            next unless ( $file_name =~ /\.xml$/ );

            # Parse the existing metadata file.
            my $granule = S4PA::Metadata->new( FILE => $file_name );
            my $deleteFileGroup = $granule->getFileGroup();
            $deleteFileGroup->data_version( $supported_version, "%s" );

            # Add the file group to a list of groups to be flagged for
            # deletion. Keep two lists, one for groups whose version
            # matches the incoming group, and one for groups whose
            # version differs from the incoming group.
            if ( $supported_version eq $incoming_version ) {
                my $intraVersionPdr =
                    $deletePdrHash{INTRA_VERSION}{$dataset}{$stored_version};
                unless ( defined $intraVersionPdr ) {
                    $intraVersionPdr = S4P::PDR::create();
                    $intraVersionPdr->originating_system(
                        $pdr->originating_system() );
                }
                $intraVersionPdr->add_file_group( $deleteFileGroup );
                $deletePdrHash{INTRA_VERSION}{$dataset}{$stored_version} =
                    $intraVersionPdr;
            } else {
                my $interVersionPdr =
                    $deletePdrHash{INTER_VERSION}{$dataset}{$stored_version};
                unless ( defined $interVersionPdr ) {
                    $interVersionPdr = S4P::PDR::create();
                    $interVersionPdr->originating_system(
                        $pdr->originating_system() );
                }
                $interVersionPdr->add_file_group( $deleteFileGroup );
                $deletePdrHash{INTER_VERSION}{$dataset}{$stored_version} =
                    $interVersionPdr;
            }
        }
    }  # END foreach my $supported_version

    # Get a list of the science files in the incoming group to be stored
    my @dataFileList = $fileGroup->science_files();
    my @metLinkList = ();

    # Create a file group to be stored
    my $outFileGroup = S4P::FileGroup->new();
    $outFileGroup->data_type( $dataset );
    $outFileGroup->data_version( $incoming_version, "%s" );

    # foreach my $dataFile ( @dataFileList ) {
    foreach my $fileSpec ( @{$fileGroup->file_specs()} ) {
        unless ( $fileSpec->file_type() eq 'METADATA' ) {
	    # Invoke StoreData() for each science file paired with the metadata
	    # file; get back the data and metadata links.
	    my ( $dataLink,
		$metLink ) = S4PA::Storage::StoreData( $dataset,
							$stored_version,
							$fileSpec->pathname(),
							$metadataFile );
	    S4P::perish( 1, "Failed to store data" )
		unless ( defined $dataLink || defined $metLink );
	    push( @metLinkList, $metLink );
	    # Add the data symbolic link to the file group.
            # added browse file support.
            if ( $fileSpec->file_type() eq 'BROWSE' ) {
	        my $fileSpecListRef
	   	    = $outFileGroup->add_file_spec( $dataLink, 'BROWSE' );
	        my $newFileSpec = @$fileSpecListRef[-1];
                if ( defined $fileSpec->{file_cksum_type} ) {
	            $newFileSpec->{file_cksum_type} = $fileSpec->{file_cksum_type};
	            $newFileSpec->{file_cksum_value} = $fileSpec->{file_cksum_value};
                 }
            } elsif ( $fileSpec->file_type() eq 'HDF4MAP' ) {
	        my $fileSpecListRef
	   	    = $outFileGroup->add_file_spec( $dataLink, 'HDF4MAP' );
	        my $newFileSpec = @$fileSpecListRef[-1];
                if ( defined $fileSpec->{file_cksum_type} ) {
	            $newFileSpec->{file_cksum_type} = $fileSpec->{file_cksum_type};
	            $newFileSpec->{file_cksum_value} = $fileSpec->{file_cksum_value};
                 }
            } else {
		my $fileSpecListRef
	    	    = $outFileGroup->add_file_spec( $dataLink, 'SCIENCE' );
	        my $newFileSpec = @$fileSpecListRef[-1];
	        $newFileSpec->{file_cksum_type} = $fileSpec->{file_cksum_type};
	        $newFileSpec->{file_cksum_value} = $fileSpec->{file_cksum_value};
            }
            $logger->info( "Created data link: $dataLink" ) if defined $logger;

            # executing post storage task
            if ( exists $CFG::cfg_post_storage{$dataset}{$stored_version} ) {
                my $cmd = $CFG::cfg_post_storage{$dataset}{$stored_version} . " $dataLink";
                S4P::logger( 'INFO', "Executing post storage script: $cmd" );
                my ( $errstr, $rc ) = S4P::exec_system( "$cmd" );
                if ( $rc ) {
                    S4P::raise_anomaly( "Post_Storage", $stationDir, 'ERROR',
                        "Post storage task: $errstr.", 0 );
                } else {
                    $logger->info( "Successfully executed post storage task: $cmd" );
                }
            }
	}
    }
    # Add the metadata file to the file group.
    $outFileGroup->add_file_spec( $metLinkList[0], 'METADATA' );
    $logger->info( "Created metadata link: $metLinkList[0]" ) if defined $logger;

    # Handle association
    my ( $relation, $associateType, @associateDataset ) = 
        S4PA::Storage::CheckAssociation( $s4paRoot, $dataset, $stored_version );
    if ( $relation ) {
        # open dataset configuration file to locate dataClass
        my $cpt = new Safe( 'DATACLASS' );
        $cpt->share( '%data_class' );

        # Read config file
        my $cfgFile = "$s4paRoot/storage/dataset.cfg";
        S4P::perish( 1, "Cannot read config file $cfgFile")
            unless $cpt->rdo( $cfgFile );

        # forward association, data -> browse
        if ( $relation == 1 ) {
            my $dataClass = $DATACLASS::data_class{$dataset};
            S4P::perish( 1, "Cannot find dataClass for dataset: $dataset" )
                unless ( defined $dataClass );
            my $dataGran = { "DATASET" => $dataset,
                             "VERSION" => $stored_version,
                             "METFILE" => $metLinkList[0],
                             "DATACLASS" => $dataClass
                           };
            # there should be only one association, still loop it through
            foreach my $associated ( @associateDataset ) {
                my ( $assocDataset, $assocVersion ) = split /\./, $associated, 2;
                my $dataClass = $DATACLASS::data_class{$assocDataset};
                S4P::perish( 1, "Cannot find dataClass for dataset: $assocDataset" )
                    unless ( defined $dataClass );
                my $assocGran = { "DATASET" => $assocDataset,
                                  "VERSION" => $assocVersion,
                                  "TYPE" => $associateType,
                                  "DATACLASS" => $dataClass
                                };
                my @assocGranules = S4PA::Storage::SearchAssociate(
                    $s4paRoot, $relation, $assocGran, $dataGran ); 
                if ( ( scalar @assocGranules ) > 1 ) {
                    S4P::perish( 1, "Found multiple associate granules (@assocGranules)." );
                }
                foreach my $assoMetFile ( @assocGranules ) {
                    $assocGran->{METFILE} = $assoMetFile;
                    my ( $assocLink, $updatedFile ) = S4PA::Storage::StoreAssociate(
                        $s4paRoot, $assocGran, $dataGran );
	            S4P::perish( 1, "Failed to store associated data" )
		        unless ( defined $assocLink );
                    $logger->info( "Created associate link: $assocLink" )
                        if defined $logger;
                }
            }

        # backworad association, browse -> data
        } elsif ( $relation == -1 ) {
            my $dataClass = $DATACLASS::data_class{$dataset};
            S4P::perish( 1, "Cannot find dataClass for dataset: $dataset" )
                unless ( defined $dataClass );
            my $assocGran = { "DATASET" => $dataset,
                              "VERSION" => $stored_version,
                              "TYPE" => $associateType,
                              "METFILE" => $metLinkList[0],
                              "DATACLASS" => $dataClass
                            };
            # add granule to associate.db if it is not already there
            my $dataGran = {};
            my ( $assocGranule, $updatedFile ) = S4PA::Storage::StoreAssociate( 
                $s4paRoot, $assocGran, $dataGran );
            if ( defined $assocGranule ) {
                $logger->info( "Added $assocGranule into association DB" )
                    if defined $logger;
            }

            # there could be more dataset associated with the current dataset
            foreach my $associated ( @associateDataset ) {
                my ( $assocDataset, $assocVersion ) = split /\./, $associated, 2;
                my $republishKey = "$assocDataset" .
                    ( $assocVersion eq '' ? '' : "_$assocVersion" );
                my $dataClass = $DATACLASS::data_class{$assocDataset};
                S4P::perish( 1, "Cannot find dataClass for dataset: $assocDataset" )
                    unless ( defined $dataClass );
                my $dataGran = { "DATASET" => $assocDataset,
                                 "VERSION" => $assocVersion,
                                 "DATACLASS" => $dataClass
                               };
                my @assocGranules = S4PA::Storage::SearchAssociate(
                    $s4paRoot, $relation, $assocGran, $dataGran ); 
                foreach my $dataMetaFile ( @assocGranules ) {
                    $dataGran->{METFILE} = $dataMetaFile;
                    my ( $assocLink, $updatedFile ) = S4PA::Storage::StoreAssociate( 
                        $s4paRoot, $assocGran, $dataGran, \@replace_list );
	            S4P::perish( 1, "Failed to store associated data" )
		        unless ( defined $assocLink );
                    $logger->info( "Created associate link: $assocLink" )
                        if defined $logger;

                    # add this granule into republish hash
                    push ( @{$backupFileHash{$associated}}, $updatedFile )
                        if ( defined $updatedFile );
                    push ( @{$republishHash{$republishKey}}, $dataMetaFile );
                }
            }
        }
    }

    # Check the file permissions; no need for publication & subscription for
    # hidden datasets.
    my $stat = stat( $metLinkList[0] );
    next if ( ($stat->mode() & 07777) eq 0600 );

    # Create an output PDR for each dataset (to be used for subscriptions).
    # (We expect only one dataset, so we do this only for the first
    # file group)
    unless ( defined $outPdrHash{$dataset}{$stored_version} ) {;
        $outPdrHash{$dataset}{$stored_version} = S4P::PDR::create();
        $outPdrHash{$dataset}{$stored_version}->originating_system(
            $pdr->originating_system() );
    }

    # Add the file group to the corresponding dataset's output PDR hash.
    $outPdrHash{$dataset}{$stored_version}->add_file_group( $outFileGroup );
}

# Write out a delete work order (PDR) as output work order if any granule
# with a version matching the incoming version needs to be replaced.
foreach my $type ( keys %deletePdrHash ) {
    foreach my $dataset ( keys %{$deletePdrHash{$type}} ) {
        foreach my $version ( keys %{$deletePdrHash{$type}{$dataset}} ) {
            my $deleteWorkOrder = $type . "_DELETE_$dataset"
                . ( $version eq '' ? '' : "_$version" ) . ".$id.PDR";
            S4P::perish( 1,
                "Failed to write PDR $deleteWorkOrder containing granules"
                . " to be deleted" )
                if $deletePdrHash{$type}{$dataset}{$version}->write_pdr(
                    $deleteWorkOrder );
            S4P::logger( 'INFO', "Wrote a delete work order $deleteWorkOrder" );
        }
    }
}

# Write out a subscription work order (PDR) for each dataset
foreach my $dataset ( keys %outPdrHash ) {
    foreach my $version ( keys %{$outPdrHash{$dataset}} ) {
        my $pdrWorkOrder = "SUBSCRIBE_$dataset" 
            . ( $version eq '' ? '' : "_$version" ) . ".$id.PDR";
        S4P::perish( 1, "Failed to create the PDR work order $pdrWorkOrder" )
            if $outPdrHash{$dataset}{$version}->write_pdr( $pdrWorkOrder );
    }
}

# Republish data granules with updated associate information
foreach my $dataset ( keys %republishHash ) {
    my $republishPdr = S4P::PDR::create();
    $republishPdr->originating_system( $pdr->originating_system() );
    foreach my $granule ( @{$republishHash{$dataset}} ) {
        my $metadata = S4PA::Metadata->new( FILE => $granule );
        $republishPdr->add_file_group( $metadata->getFileGroup() );
    }

    my $republishWorkOrder = "PUBLISH_$dataset" . ".$id.PDR";
    S4P::perish( 1, "Failed to create the PDR work order $republishWorkOrder" )
        if $republishPdr->write_pdr( $republishWorkOrder );
    S4P::logger( 'INFO', "Wrote a republish work order $republishWorkOrder" );
}

# Backup modified metadata files

# Retrieve backup site root
my $config = $s4paRoot . "/auxiliary_backup.cfg";
S4P::perish( 1, "Configuration file for backup $config does not exist: $!\n" )
    unless ( -f $config );
my $cptBackup = new Safe 'BACKUP';
$cptBackup->rdo( $config ) ||
    S4P::perish( 1, "Cannot read config file $config in safe mode: $!\n");

foreach my $key ( keys %backupFileHash ) {
    ( my $backupDir = $BACKUP::cfg_auxiliary_backup_root ) =~ s/\/+$//;
    $backupDir .= "/$key";
    unless ( -d $backupDir ) {
        S4P::perish( 1, "Cannot mkdir $backupDir: $!" )
            unless ( mkdir( $backupDir, 0775 ) );
    }
    foreach my $metaFile ( @{$backupFileHash{$key}} ) {
        my $toFile = "$backupDir/" . basename( $metaFile );
        S4P::logger( "ERROR", "Failed to copy $metaFile to $backupDir" )
            unless ( copy( $metaFile, $toFile ) );
    }
}

exit( 0 );
