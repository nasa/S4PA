#!/usr/bin/perl

=head1 NAME

s4pa_transient_archive.pl - script for create PDR for transient deletion

=head1 SYNOPSIS

s4pa_transient_archive.pl
B<-f> I<configuration_file>
B<-r> I<s4pa_root_directory>
B<-d> I<dataset>
B<-e> I<endTime_or_expiration_period_in_days>
B<-s> I<startTime_or_deletion_window_in_days>]
[B<-v> I<dataVersion>]
[B<-n> I<number_of_granule_per_pdr>]
[B<-p> I<pdr_prefix>]
[B<-l> I<pdr_staging_directory>]
[B<-g> I<file_with_list_of_granules>]
[B<-m>]
[B<-w>]

=head1 DESCRIPTION

s4pa_transient_archive.pl accepts a dataset and expiration range,
find the expired granules, create PDR(s) for granule deletion.
It can also be used for general PDR creation for re-publishing
or re-submitting subscription. If called with a configuration file,
multiple datasets can be specified at the same. This will be mainly
the case of routine housekeeping job under s4pa station. If called
with command line option, only one dataset can be specified.

=head1 ARGUMENTS

=over 4

=item B<-f>

Configuration file, used from s4pa station for multiple dataset.
If not specified, command line options will be used for one dataset only.
The following variables and hash should be in the configuration file.

required parameters:

$cfg_s4pa_root, %cfg_transient_dataset

optional parameters:

$cfg_max_granule_count, $cfg_pdr_prefix, $cfg_pdr_staging

=over

$cfg_s4pa_root: same as B<-r> for s4pa root directory.

%cfg_transient_dataset: a has for datasets, version, and range.
    Keys are the dataset name, each dataset has its own hash;
    keys of dataset hash are the version label, empty string ""
        for versionless dataset, each version also has its own hash;
    keys of version are 'startTime' and 'expirationDays' and both can
        have valus of either starndard s4pa timestamp or number of days.
        timestamp in startTime and expirationDays will be used
            as RangeDataTime for seaching of qualified granuels.
        number of days in expirationDays represent the expiration
            period (age), will be converted to RangeEndingDatetime,
        number of days in startTime represent the deletion window,
            will be converted to the RangeBeginningDateTime.

Example of the a typical %cfg_transient_dataset hash:

%cfg_transient_dataset = (
    "TRMM_3A12" => {
        "" => { "startTime" => "2006-01-01 00:00:00",
                "expirationDays" => 30
              },
         5 => { "startTime" => "2006-01-01 00:00:00",
                "expirationDays" => 30
              },
         6 => { "startTime" => "2006-01-01 00:00:00",
                "expirationDays" => 30
         }
     },
     "TRMM_3A11" => {
         "" => { "startTime" => "2006-01-01 00:00:00",
                 "expirationDays" => 30
               }
         }
     }
);

$cfg_max_granule_count: same as B<-n> for maximum number of 
    granule per PDR. Optional, default to 100.

$cfg_pdr_prefix: same as B<-p> for pdr filename prefix.
    Optional, default to 'INTRA_VERSION_DELETE'.

$cfg_pdr_staging: same as B<-l> for pdr staging directory.
    Optional, default to './'

=back

=item B<-r>

S4pa root directory.

=item B<-d>

Dataset name.

=item B<-e>

RangeEndDateTime in 'YYYY-MM-DD HH:MM:SS' format or
Expiration period in days. 

=item B<-b>

RangeStartDataTime in 'YYYY-MM-DD HH:MM:SS' format or
Deletion window in days. 

=item B<-v>

Optional dataset version. Versionless dataset will be assume
if not specified.

=item B<-n>

Optional maximum number of granules per PDR. Default to 100.

=item B<-p>

PDR filename prefix. Default to 'INTRA_VERSION_DELETE'.

=item B<-l>

PDR staging directory. Default to current working directory (./).

=item B<-g>

File with list of granule metadata files. If specified, it will
only create PDR/WO based on the list bypassing the startTime
and endTime search. Listed metadata files can contain full path
but have to belongs to the same dataset and version.

=item B<-m>

Optional flag for automatic copy all PDRs to publishing's
pending_delete directories. 

=item B<-w>

Optional work order (.wo) PDR filename.

=item B<-x>

Exclude granules already in pending deletion queue.

=back

=head1 AUTHORS

Guang-Dih Lei (guang-dih.lei@gsfc.nasa.gov)

=cut

################################################################################
# $Id: s4pa_transient_archive.pl,v 1.16 2017/11/08 13:07:18 glei Exp $
# -@@@ S4PA, Version $Name:  $
###############################################################################
#
# name: s4pa_transient_archive.pl
# revised: 04/08/2007 glei 
#

use strict;
use Safe;
use Getopt::Std;
use S4P::PDR;
use S4P::TimeTools;
use S4PA::MachineSearch;
use S4PA::GranuleSearch;
use S4PA::Metadata;
use File::Basename;
use Cwd;
use vars qw( $opt_f $opt_r $opt_d $opt_e $opt_s 
             $opt_v $opt_n $opt_p $opt_l $opt_g $opt_m $opt_w $opt_x );

getopts('f:r:d:e:s:v:n:p:l:g:mwx');
usage() if ( !$opt_f && !$opt_r );

##############################################################################
# Assign option value
##############################################################################

my $stationDir = dirname( cwd() );
my $station = basename( $stationDir );

# retrieve config values
my $cpt = new Safe 'CFG';
$cpt->share( '$cfg_s4pa_root', '$cfg_max_granule_count', 
             '%cfg_transient_dataset', '$cfg_pdr_prefix',
             '$cfg_pdr_staging');

# required dataset name(s)
my @transientDataset;
if ( $opt_f ) { 
    $cpt->rdo( $opt_f ) or
        S4P::perish( 1, "Failed to read configuration file $opt_f ($@)" );
    @transientDataset = keys %CFG::cfg_transient_dataset;
} else {
    @transientDataset = ( $opt_d );
}
S4P::perish( 1, "Dataset not defined" ) unless ( scalar @transientDataset );

# required s4pa root directory 
my $s4paRoot = ( defined $CFG::cfg_s4pa_root ) ?
    $CFG::cfg_s4pa_root : $opt_r;
S4P::perish( 1, "s4pa root directory not defined" ) unless ( $s4paRoot );

# optional maximum granule per PDR, default to 100.
my $maxCount = ( defined $CFG::cfg_max_granule_count ) ?
    $CFG::cfg_max_granule_count : $opt_n ? $opt_n : 100;
S4P::logger( "INFO",  "Maximum number of granules in PDR: $maxCount" );

# optional PDR filename prefix, default to 'INTRA_VERSION_DELETE'.
my $prefix = ( defined $CFG::cfg_pdr_prefix ) ?
    $CFG::cfg_pdr_prefix : $opt_p ? $opt_p : 'INTRA_VERSION_DELETE';
S4P::logger( "INFO",  "PDR filename prefix: $prefix" );

# optional PDR filename suffix, default to 'PDR'.
# rename fot '.wo' if $cfg_pdr_suffix or <-w> was specified.
my $suffix = ( defined $CFG::cfg_pdr_suffix ) ?
    $CFG::cfg_pdr_suffix : $opt_w ? 'wo' : 'PDR';
S4P::logger( "INFO",  "PDR filename suffix: $suffix" );

# optional PDR staging directory, default to current directory './'
my $stageDir = ( defined $CFG::cfg_pdr_staging ) ?
    $CFG::cfg_pdr_staging : $opt_l ? $opt_l : '.';
$stageDir =~ s/\/$//;
S4P::perish( 2, "Staging directory: $stageDir does not exist" )
    unless ( -d $stageDir );
S4P::logger( "INFO", "PDR staging directory: $stageDir" );
my $filePrefix = "$stageDir/" . "$prefix";

# read dataset/dataClass requirement
my $datasetCFG = "$s4paRoot/storage/dataset.cfg";
my $cptDataset = new Safe 'DATASET';
$cptDataset->rdo( $datasetCFG ) or
    S4P::perish( 3, "Failed to read config file $datasetCFG: ($!)" );
S4P::logger( "INFO", "Will copy all PDRs to publishing pending_delete directory" )
    if ( $opt_m );

# read granule listing file if specified
my @granuleList;
if ( $opt_g ) {
    S4P::perish( 4, "Only one dataset at a time for deleting granules " .
        "from a listing file." ) if ( ( scalar @transientDataset ) > 1 );
    open ( LIST,"$opt_g" ) or
        S4P::perish( 4, "Failed to open granule listing file $opt_g: ($!)" );
    while ( <LIST> ) {
        chomp() ;
        push ( @granuleList, $_ );
    }
    close( LIST );
}

##############################################################################
# Loop on dataset for granule search
##############################################################################

my $excludeHash = {};
my $totalGranule = 0;
my $totalPdr = 0;
my %argTransient;
$argTransient{home} = $s4paRoot;
foreach my $dataset ( @transientDataset ) {
    $argTransient{dataset} = $dataset;
    S4P::logger( "INFO", "Dataset: $dataset" );

    my $dataClass = defined $DATASET::data_class{ $dataset }
        ? $DATASET::data_class{ $dataset } : undef;
    S4P::perish( 5, "dataClass not defined for dataset: $dataset" ) 
        unless ( defined $dataClass );

    my @versionIds;
    if ( defined $CFG::cfg_transient_dataset{$dataset} ) {
        @versionIds = keys %{$CFG::cfg_transient_dataset{$dataset}};
    } elsif ( $opt_v ) {
        @versionIds = ( "$opt_v" );
    } else {
        @versionIds = ( "" );
    }

##############################################################################
# Loop on dataVersion for granule search
##############################################################################

    foreach my $version ( @versionIds ) {
        my $versionLabel;
        if ( $version eq "" ) {
            $argTransient{version} = "";
            $versionLabel = "";
            S4P::logger( "INFO", "Dataset Version: versionless" );
        } else {
            $argTransient{version} = $version;
            $versionLabel = ".$version";
            S4P::logger( "INFO", "Dataset Version: $version" );
        }

##############################################################################
# Decode startTime and endTime for granule search
##############################################################################

        my $startTime;
        my $endTime;

        # construct start and end time for granule search
        unless ( @granuleList ) {
            ( $startTime, $endTime ) = 
                searchRange( $dataset, $version, $opt_s, $opt_e );
            S4P::logger( "INFO", "Start Date: $startTime" );
            S4P::logger( "INFO", "End Date: $endTime" );
        }

##############################################################################
# use MachineSearch and GranuleSearch for PDR creation
##############################################################################

        my $pdr;
        # locate granule based on granule list file
        if ( @granuleList ) {
            S4P::logger( "INFO", "Locating granules from list file..." );
            # construct a machine search object
            my %argSearch;
            $argSearch{home} = $s4paRoot;
            $argSearch{user} = 'transient';
            $argSearch{dataset} = $dataset;
            $argSearch{version} = $version;
            $argSearch{action} = 'order';
            my $search = S4PA::MachineSearch->new( %argSearch );
            $search->getDatasetPath();

            my @metaList;
            foreach my $xmlFile ( @granuleList ) {
                S4P::perish( 6, "Not an S4PA metadata file: $xmlFile")
                    if ( $xmlFile !~ /\.xml$/ );

                # push to metadata file list if granule has full path
                if ( $xmlFile =~ /^\// ) {
                    S4P::perish( 6, "Metadata file does not exist: $xmlFile")
                        unless ( -f $xmlFile );
                    push ( @metaList, $xmlFile );

                # construct a granule locator to find the full path
                } else {
                    my $metaFile = basename( $xmlFile );
                    my $locator = $dataset . $versionLabel;
                    $locator .= ":" . $metaFile;
                    my $granule = $search->locateGranule( $locator );
                    S4P::perish( 6, "Metadata file does not exist: $xmlFile")
                        unless ( -f $granule );
                    push ( @metaList, $granule );
                }
            }

            # create a new PDR object to store all fileGroup
            $pdr = S4P::PDR::create();
            foreach my $xmlFile ( @metaList ) {
                my %argMeta;
                $argMeta{FILE} = $xmlFile;
                my $metadata = new S4PA::Metadata( %argMeta );
                my $fileGroup = $metadata->getFileGroup();
                if ( defined $fileGroup ) {
                    $pdr->add_file_group( $fileGroup );
                    S4P::logger( "INFO", "Added $xmlFile fileGroup to PDR" );
                } else {
                    S4P::perish( 7, "Can not extract fileGroup from $xmlFile");
                }
            }

        # locate granule base on transient start/end datetime
        } else {
            my $transient = S4PA::MachineSearch->new( %argTransient );
            my $datasetPath = $transient->getDatasetPath;
            S4P::perish( 8, $transient->errorMessage ) if $transient->onError;
    
            my %argSearch;
            $argSearch{action} = $transient->getAction();
            $argSearch{dataPath} = readlink ( "$datasetPath/data" );
            S4P::perish( 8, "Data Path: $argSearch{dataPath} does not exist" ) 
                unless ( -d $argSearch{dataPath} ); 
            S4P::logger( "INFO", "Data Path: $argSearch{dataPath}" );
    
            $argSearch{frequency} = $transient->getFrequency;
            S4P::perish( 8, $transient->errorMessage ) if $transient->onError;
            S4P::logger( "INFO", "Frequency: $argSearch{frequency}" );
    
            $argSearch{startTime} = $startTime; 
            $argSearch{endTime} = $endTime;
            my $search = S4PA::GranuleSearch->new( %argSearch );
            S4P::perish( 8, $search->errorMessage ) if $search->onError;

            # exclude granules already in pending delete queue
            # but only if it is running from transientArchive station
            if ( $station eq 'transientArchive' or defined $opt_x ) {

                # building up metadatafile list for any granules that is already
                # in each dataClass's pending deletion queue
                my $deletePath = "$s4paRoot/storage/$dataClass/delete_$dataClass";
                unless ( exists $excludeHash->{$dataClass} ) {
                    $excludeHash = excludeList( $dataClass, $deletePath ); 
                }

                # create a new PDR object to store all fileGroup
                $pdr = S4P::PDR::create();

                # get qualified metadata files list
                my @xmlFiles = $search->findMetadataFile();
                foreach my $xmlFile ( @xmlFiles ) {
                    # get only qualified metadata files list
                    my $file = basename( $xmlFile );
                    if ( exists $excludeHash->{$dataClass}{$dataset}{$file} ) {
                        S4P::logger( "INFO", "$dataset:$file is already in pending deletion." );
                        next;
                    }

                    # For yearly/monthly frequency collection, we can't take
                    # all metadata files under those qualified directories. It will
                    # be purging some granules too early. So, Instead of taking 
                    # all granules, we need to parse each each metadata file for its
                    # temporal coverage to make sure it falls into the transient
                    # archive period before adding the file group.
                    #
                    # my %argMeta;
                    # $argMeta{FILE} = $xmlFile;
                    # my $metadata = new S4PA::Metadata( %argMeta );
                    # my $fileGroup = $metadata->getFileGroup();
                    #
                    my $fileGroup = $search->parseGranule($xmlFile);
                    if (defined $fileGroup) {
                        if (defined $fileGroup->data_type()) {
                            $pdr->add_file_group($fileGroup);
                            S4P::logger("INFO", "Added $xmlFile fileGroup to PDR");
                        } else {
                            S4P::logger("WARN", "Skipped non-granule metadata: $xmlFile");
                            next;
                        }
                    } else {
                        # granule search will return undef if the granule is
                        # not qualified for the transient period. So, instead of
                        # failing the job, we just skip to the next metadata file
                        S4P::logger("INFO", "Skipped non-qualified granule: $xmlFile");
                        next;
                        # S4P::perish(7, "Can not extract fileGroup from $xmlFile");
                    }
                }

            # not running from transientArchive station
            # create pdr with the full search result
            } else {
                $pdr = $search->createPdr();
            }
        }

        # creating pdr/wo file(s) under staging directory
        my $pdrFiles = [];
        if ( $pdr->recount == 0 ) {
            S4P::logger( "INFO", "No matching granule found for " .
                "$dataset$versionLabel" );
        } else {
            my ( $granuleCount, $pdrCount );
            ( $granuleCount, $pdrCount, $pdrFiles ) = writePdr( $pdr, 
                $dataset, $version, $maxCount, $filePrefix, $suffix );
            S4P::logger( "INFO", "Total $pdrCount PDR, " .
                "$granuleCount Granules for $dataset$versionLabel" );
            $totalGranule += $granuleCount;
            $totalPdr += $pdrCount;
        }

        # copy PDRs to all publishing directories
        if ( $opt_m ) {
            my @publishDir;

            # publish to dotchart becomes optional, ticket #7336.
            if ( $DATASET::cfg_publish_dotchart ) {
                # dotchart publishing will pass PDR down to delete station
                # so, just push PDR to publish dotchart if it is configured
                my $dotchartDir = "$s4paRoot/" . "publish_dotchart/pending_delete";
                push ( @publishDir, $dotchartDir ) 
            } else {
                # push PDR to delete station directory
                # when publish dotchart is not configured
                my $deleteDir = "$s4paRoot/" . "storage/$dataClass/"
                    . "delete_$dataClass/intra_version_pending";
                push ( @publishDir, $deleteDir ) 
            }

            # collect publishing requirement
            if ( exists $DATASET::cfg_publication{$dataset}{$version} ) {
                # publish to rest of the requirement for this dataset.version
                foreach my $dir ( @{$DATASET::cfg_publication{$dataset}{$version}} ) {
                    next if ( $dir =~ /dotchart/ );
                    $dir =~ s/pending_publish/pending_delete/;
                    $dir = "$s4paRoot/" . "$dir";
                    push ( @publishDir, $dir ) if ( -d $dir);
                }
            }

            # distribute PDRs
            foreach my $dir ( @publishDir ) {
                foreach my $pdr ( @$pdrFiles ) {
                    my $cpStatus = `cp $pdr $dir`;
                    if  ( $? ) {
                        S4P::logger( "WARN", "Can not copy $pdr to $dir" );
                    } else {
                        S4P::logger( "INFO", "Successfully copied $pdr to $dir" );
                    }
                }
            }
        }
    }
}
S4P::logger( "INFO", "Total $totalGranule Granule(s) in $totalPdr PDR(s).");

##############################################################################
# Subroutine searchRange:  calculate startTime and endTime
##############################################################################
sub searchRange{
    my ( $dataset, $version, $startDateTime, $endDateTime ) = @_;

    my $startDate;
    my $endDate;
    my $expirationStartDate;
    my $expirationEndDate;

    # read range ending data/time from either configuration file or 
    # command line option
    if ( defined $CFG::cfg_transient_dataset{$dataset}{$version}{expirationDays} ) {
        $endDate = $CFG::cfg_transient_dataset{$dataset}{$version}{expirationDays};
    } else {
        $endDate = $endDateTime;
    }

    my $endTime;
    if ( $endDate =~ /\d{4}-\d{2}-\d{2}/ ) {
        if ( $endDate =~ /\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/ ) {
            $endTime = $endDate;
        } else {
            $endTime = "$endDate" . " 23:59:59";
        }
        $expirationEndDate = S4P::TimeTools::timestamp2CCSDSa( $endTime )
    } elsif ( $endDate =~ /\d+/ ) {
        my $now = S4P::TimeTools::CCSDSa_Now;
        # subscract expiration period days worth of seconds to $now to get endTime
        $expirationEndDate = S4P::TimeTools::CCSDSa_DateAdd( $now, 
            -( $endDate * 24 * 60 * 60) );
        $endTime = S4P::TimeTools::CCSDSa2timestamp( $expirationEndDate );
    } else {
        S4P::perish( 9, "expirationDays has to be in either " .
            "'YYYY-MM-DD HH:MM:SS' or 'nnn' format");
    }

    # read range beginning data/time from either configuration file or 
    # command line option
    if ( defined $CFG::cfg_transient_dataset{$dataset}{$version}{startTime} ) {
        $startDate = $CFG::cfg_transient_dataset{$dataset}{$version}{startTime};
    } else {
        $startDate = $startDateTime;
    }

    my $startTime;
    if ( $startDate =~ /\d{4}-\d{2}-\d{2}/ ) {
        if ( $startDate =~ /\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/ ) {
            $startTime = $startDate;
        } else {
            $startTime = "$startDate" . " 00:00:00";
        }
    } elsif ( $startDate =~ /\d+/ ) {
        # subscract deletion window worth of seconds to $endTime to get startTime
        $expirationStartDate = S4P::TimeTools::CCSDSa_DateAdd( $expirationEndDate, 
            -( $startDate * 24 * 60 * 60) );
        $startTime = S4P::TimeTools::CCSDSa2timestamp( $expirationStartDate );
    } else {
        S4P::perish( 10, "startTime has to be in either " . 
            "'YYYY-MM-DD HH:MM:SS' or 'nnn' format");
    }

    return ( $startTime, $endTime );
}

##############################################################################
# Subroutine excludeList: list of pending deletion grandules
##############################################################################
sub excludeList {
    my ( $dataClass, $path ) = @_;

    my @dir_list = qw( inter_version_pending intra_version_pending );

    foreach my $dir ( @dir_list ) {
        my @pdr_file_list = glob( "$path/$dir/*.PDR" );

        foreach my $pdr_file ( @pdr_file_list ) {
            my $pdr = S4P::PDR::read_pdr( $pdr_file );
            foreach my $fg ( @{$pdr->file_groups} ) {
                my $dataType = $fg->data_type();
                my $metFile = basename( $fg->met_file() );
                $excludeHash->{$dataClass}{$dataType}{$metFile} = 1;
            }
        }
    }

    return $excludeHash;
}

##############################################################################
# Subroutine writePdr:  split PDR and write to files
##############################################################################
sub writePdr {
    my ( $pdr, $dataset, $version, $maxCount, 
        $filePrefix, $fileSuffix ) = @_;

    my $pdrFile;
    my @pdrFileList;
    my $granuleCount = 0;
    my $pdrCount = 0;
    my $pdrId = time();
    my $versionLabel = ( $version ne "" ) ? "_$version" : "";
    my $pdrPrefix = $filePrefix . "_$dataset" . $versionLabel . 
        ".$pdrId";
    my $newPdr = S4P::PDR::start_pdr(
        'originating_system'=> "S4PA_TRANSIENT_PDR",
        'expiration_time' => S4P::PDR::get_exp_time(3, 'days') );

    foreach my $fileGroup ( @{$pdr->file_groups} ) {
        $fileGroup->data_version( $version, "%s" );
        if ( $granuleCount == $maxCount ) {
            $pdrCount++;
            $pdrFile = $pdrPrefix . "S$pdrCount" . ".$fileSuffix";
            S4P::logger( "INFO", "Successfully create PDR: $pdrFile" )
                unless $newPdr->write_pdr( $pdrFile );
            push ( @pdrFileList, $pdrFile );
            $newPdr = S4P::PDR::start_pdr(
                'originating_system'=> "S4PA_TRANSIENT_PDR",
                'expiration_time' => S4P::PDR::get_exp_time(3, 'days') );
            $granuleCount = 0;
        }
        $newPdr->add_file_group( $fileGroup );
        $granuleCount++;
    }
    
    if ( $pdrCount ) {
        if ( $granuleCount ) {
            $pdrCount++;
            $pdrFile = $pdrPrefix . "S$pdrCount" . ".$fileSuffix";
            S4P::logger( "INFO", "Successfully create PDR: $pdrFile" )
                unless $newPdr->write_pdr( $pdrFile );
            push ( @pdrFileList, $pdrFile );
        }
    } else {
        $pdrFile = $pdrPrefix . ".$fileSuffix";
        S4P::logger( "INFO", "Successfully create PDR: $pdrFile" )
            unless $newPdr->write_pdr( $pdrFile );
        push ( @pdrFileList, $pdrFile );
        $pdrCount++;
    }
    my $totalGranule = $granuleCount + ( $pdrCount - 1 ) * $maxCount;
    return ( $totalGranule, $pdrCount, \@pdrFileList );
}

##############################################################################
# Subroutine usage:  print usage and die
##############################################################################
sub usage {
  die << "EOF";
Usage: $0 <-f configuration_file> | 
          <-r s4pa_root> <-d dataset> <-e expiration_period> <-s startTime>
          [options]
Options are:
        -v label1[,lable2,..]  versions labels, separated by comma.
        -n nnn                 number of granule per pdr, default to 100.
        -p <pdr_prefix>        PDR filename prefix, default to 'INTRA_VERSION_DELETE'
        -l <pdr_staging_dir>   PDR staging directory, default to './'
        -g <granule_list_file> only delete granule listed in this file
        -m                     Copy all PDRs to publishing pending_delete directory
        -w                     Optional work order (.wo) filename suffix
EOF
}

