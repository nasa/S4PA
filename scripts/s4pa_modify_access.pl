#!/usr/bin/perl

=head1 NAME

s4pa_modify_access.pl - script for create PDR for migrate
dataset/dataclass from restricted to public or vise versa.

=head1 SYNOPSIS

s4pa_modify_access.pl
B<-r> I<s4pa_root_directory>
B<-a> I<action>
B<-c> I<dataClass>
B<-d> I<dataset>
[B<-v> I<dataVersion>]
[B<-s> I<startTime_or_deletion_window_in_days>]
[B<-e> I<endTime_or_expiration_period_in_days>]
[B<-g> I<list_of_xml_file>]
[B<-n> I<number_of_granule_per_pdr>]
[B<-p> I<pdr_prefix>]
[B<-l> I<pdr_staging_directory>]
[B<-w>]

=head1 DESCRIPTION

s4pa_modify_access.pl can be used to modify a dataset or a whole
dataClass's access permission. Three action can be performed, 
migrate from restricted/hidden to public, migrate from restricted/
public to hidden, or migrate from public/hidden to restricted.
PDRs will created for publication to update the access information.

=head1 ARGUMENTS

=item B<-r>

S4pa root directory.

=item B<-a>

Action. One of "public", "restricted", "hidden".

=item B<-c>

DataClass name. All datasets under that class will be modified.

=item B<-d>

Dataset name. Only the specified dataset will be modified.

=item B<-s>

Optional RangeBeginningDateTime in 'YYYY-MM-DD HH:MM:SS' format or
Deletion window in days. Default to '1900-01-01 00:00:00'.

=item B<-e>

Optional RangeEndingDateTime in 'YYYY-MM-DD HH:MM:SS' format or
Expiration period in days. Default to '2100-12-31 23:59:59'.

=item B<-g>

Optional xml metadata file list.

=item B<-v>

Optional dataset version. Versionless dataset will be assume
if not specified.

=item B<-n>

Optional maximum number of granules per PDR. Default to 100.

=item B<-p>

PDR filename prefix. Default to 'DO.DATA_MIGRATION'.

=item B<-l>

PDR staging directory. Default to current working directory (./).

=item B<-w>

Optional work order (.wo) PDR filename.

=head1 AUTHORS

Guang-Dih Lei (guang-dih.lei@gsfc.nasa.gov)

=cut

################################################################################
# $Id: s4pa_modify_access.pl,v 1.17 2016/09/29 19:36:16 glei Exp $
# -@@@ S4PA, Version $Name:  $
###############################################################################
#
# name: s4pa_modify_access.pl
# revised: 06/01/2007 glei 
#

use strict;
use Safe;
use Getopt::Std;
use File::Copy;
use File::Basename;
use File::stat;
use S4P::PDR;
use S4P::TimeTools;
use S4PA::MachineSearch;
use S4PA::GranuleSearch;
use S4PA::Metadata;
use vars qw( $opt_r $opt_a $opt_c $opt_d $opt_v $opt_g
             $opt_e $opt_s $opt_n $opt_p $opt_l $opt_w );

getopts('r:a:c:d:e:g:s:v:n:p:l:w');
usage() unless ( $opt_a && $opt_r );

##############################################################################
# Process required arguments
##############################################################################

my $dataClass;
my @dataSets;

# required s4pa root directory 
my $s4paRoot = $opt_r;
S4P::perish( 1, "s4pa root directory not defined." ) unless ( $s4paRoot );
$s4paRoot =~ s/\/$//;

# required action
my $action = $opt_a;
S4P::perish( 2, "Action has to be one of 'public', 'restricted', 'hidden'" )
    unless ( $action =~ /public|restricted|hidden/ );

my $conf;
# for dataclass level search all dataset under that dataclass
if ( defined $opt_c ) {

    if ( defined $opt_g ) {
        S4P::perish( 3, "Metadata file listing optiona can only go with particular dataset, not the whole class." )
    }

    $dataClass = $opt_c;

    # collect all dataset, version from s4pa_store_data.cfg
    $conf = "$s4paRoot/storage/$dataClass/store_$dataClass/s4pa_store_data.cfg";
    S4P::perish( 3, "Dataset configuration file, $conf, does not exist." )
        unless ( -f $conf );

    # Read dataset configuration file
    my $cpt = new Safe 'CFG';
    $cpt->share( '%cfg_data_version', '%cfg_root_url' );
    $cpt->rdo( $conf ) or
        S4P::perish( 4, "Failed to read configuration file $conf ($@)" );

    foreach my $dataset ( keys %CFG::cfg_data_version ) {
        foreach my $version ( @{$CFG::cfg_data_version{$dataset}} ) {
            my $versionID = ( $version ne '' ) ? ".$version" : "";
            push @dataSets, "$dataset" . "$versionID";
        }
    }
}

# locate dataset.cfg to read urlRoot, and downstream info
$conf = "$s4paRoot/storage/dataset.cfg";
S4P::perish( 5, "Dataclass configuration file, $conf, does not exist." )
    unless ( -f $conf );

# read dataClass configuration file
my $cpt = new Safe 'CFG';
$cpt->share( '%data_class', '%cfg_root_url', '%cfg_publication', 
    '$cfg_publish_dotchart' );
$cpt->rdo( $conf ) or
    S4P::perish( 6, "Failed to read configuration file $conf ($@)" );
my $urlRoot = $CFG::cfg_root_url{$action};
S4P::perish( 7, "Can't locate URL root in $conf for action $action" )
    unless ( defined $urlRoot );

if ( defined $opt_d ) {
    # locate dataClass from dataset.cfg 
    my $dataset = $opt_d;
    my $versionID = ( defined $opt_v && $opt_v ne '' ) ? ".$opt_v" : "";
    push @dataSets, "$dataset" . "$versionID";
    $dataClass = $CFG::data_class{$dataset}
        if ( defined $CFG::data_class{$dataset} );
}

my $numDataset = scalar( @dataSets );
if ( $numDataset > 1 ) {
    foreach my $dataset ( @dataSets ) {
        S4P::logger( "INFO", "DataClass: $dataClass, DataSet: $dataset" );
    }
} elsif ( $numDataset > 0 ) {
    S4P::perish( 8, "No dataClass found for $dataSets[0]" )
        if ( $dataClass eq "" );
    S4P::logger( "INFO", "DataClass: $dataClass, DataSet: $dataSets[0]" );
} else {
   S4P::perish( 9, "No dataset defined" );
}

##############################################################################
# Process optional arguments
##############################################################################

# optional maximum granule per PDR, default to 1000.
my $maxCount = $opt_n ? $opt_n : 1000;
S4P::logger( "INFO",  "Maximum number of granules in PDR: $maxCount" );

# optional PDR filename prefix, default to 'DATA_MIGRATION'.
my $prefix = $opt_p ? $opt_p : 'DO.DATA_MIGRATION';
S4P::logger( "INFO",  "PDR filename prefix: $prefix" );

# optional PDR filename suffix, default to 'PDR'.
# rename to '.wo' if $cfg_pdr_suffix or <-w> was specified.
my $suffix = $opt_w ? 'wo' : 'PDR';
S4P::logger( "INFO",  "PDR filename suffix: $suffix" );

# optional PDR staging directory, default to current directory './'
my $stageDir = $opt_l ? $opt_l : '.';
$stageDir =~ s/\/$//;
S4P::perish( 10, "Staging directory: $stageDir does not exist" )
    unless ( -d $stageDir );
S4P::logger( "INFO", "PDR staging directory: $stageDir" );
my $filePrefix = "$stageDir/" . "$prefix";

##############################################################################
# Decode startTime and endTime for granule search
##############################################################################

my $startDate;
my $endDate;
my $expirationStartDate;
my $expirationEndDate;

# read range beginning data/time from command line option or
# default to Year-start 1900.
if ( $opt_s ) {
    $startDate = $opt_s;
} else {
    $startDate = '1900-01-01 00:00:00';
}

# read range ending data/time from command line option or 
# default to Year-end 2100.
if ( $opt_e ) {
    $endDate = $opt_e;
} else {
    $endDate = '2100-12-31 23:59:59';
}

my ( $startTime, $endTime ) = getRange( $startDate, $endDate );
S4P::logger( "INFO", "Start Date: $startTime" );
S4P::logger( "INFO", "End Date: $endTime" );

##############################################################################
# Loop on dataset for granule search
##############################################################################

my %fsList;
my $totalGranule = 0;
my $totalPdr = 0;
my %argMigrate;
$argMigrate{home} = $s4paRoot;
foreach my $dataSet ( @dataSets ) {
    my $dataset;
    my $version;
    if ( $dataSet =~ /\./ ) {
        ( $dataset, $version ) = split /\./, $dataSet, 2
    } else {
        $dataset = $dataSet;
        $version = '';
    }
    $argMigrate{dataset} = $dataset;
    S4P::logger( "INFO", "Dataset: $dataset" );
    $argMigrate{version} = $version;
    S4P::logger( "INFO", "Dataset Version: $version" );

##############################################################################
# use MachineSearch and GranuleSearch for metadata file search
##############################################################################

    my $migrate = S4PA::MachineSearch->new( %argMigrate );
    my $datasetPath = $migrate->getDatasetPath;
    S4P::perish( 11, $migrate->errorMessage ) if $migrate->onError;

    my %argSearch;
    $argSearch{dataPath} = readlink ( "$datasetPath/data" );
    S4P::perish( 12, "Data Path: $argSearch{dataPath} does not exist" ) 
        unless ( -d $argSearch{dataPath} ); 
    S4P::logger( "INFO", "Data Path: $argSearch{dataPath}" );

    my $frequency = $migrate->getFrequency;
    S4P::perish( 13, $migrate->errorMessage ) if $migrate->onError;
    $argSearch{frequency} = $frequency;
    S4P::logger( "INFO", "Frequency: $argSearch{frequency}" );

    $argSearch{startTime} = $startTime; 
    $argSearch{endTime} = $endTime;
    my $search = S4PA::GranuleSearch->new( %argSearch );
    S4P::perish( 14, $search->errorMessage ) if $search->onError;

    my @xmlFileList;
    if ( defined $opt_g ) {
        open(LIST, "<$opt_g")
            or S4P::perish( 14, "Failed opening metadata listing file $opt_g: $!" ); 
        while( <LIST> ) {
            chomp($_);
            push @xmlFileList, $_;
        }
        close(LIST);
    } else {
        @xmlFileList = $search->findMetadataFile();
    }
    S4P::perish( 15, $search->errorMessage ) if $search->onError;

##############################################################################
# Proces metadata file to move the link and change attribute
##############################################################################

    my @newFileList;
    my $pdr = S4P::PDR::create();
    $pdr->{originating_system} = "S4PA_DATA_MIGRATION";

    foreach my $xmlFile ( @xmlFileList ) {
        my %argMetadata;
        $argMetadata{FILE} = $xmlFile;
        $argMetadata{START} = $search->getStartTime();
        $argMetadata{END} = $search->getEndTime();

        my $metadata = new S4PA::Metadata( %argMetadata );
        next unless ( $metadata->compareDateTime( %argMetadata ) );
        my $fileHash = $metadata->getFiles();

        # move link and change permission
        my $newFileHash = migrateFile( $action, $frequency, $fileHash );
        next unless ( defined $newFileHash );

        # add new xml file to list for granule update
        foreach my $newFile ( keys %$newFileHash ) {
            # update xsl url root in the metadata file
            if ( $newFileHash->{$newFile} eq 'METADATA' ) {
                my $status = updateXML( $urlRoot, $newFile );
                if ( $status ) {
                    S4P::logger( "INFO", "Updated xml-stylesheet" .
                    " in $newFile" );
                } else {
                    S4P::logger( "WARN", "Problem update xml-stylesheet" .
                    " target in $newFile" );
                }
            }
            push @newFileList, $newFile;
        }

        # add fileGroup to pdr
        my $fileGroup = S4P::FileGroup->new();
        $fileGroup->data_type( $dataset );
        $fileGroup->data_version( $version, "%s" );
        foreach my $key ( keys %$newFileHash ) {
            $fileGroup->add_file_spec( $key, $newFileHash->{$key} );
        }
        $pdr->add_file_group( $fileGroup );
    }

    my %datasetFsList = updateGranuleRecord( $dataSet, 
        $datasetPath, @newFileList );
    foreach my $fs ( keys %datasetFsList ) {
        $fsList{$fs} = 1 unless ( defined $fsList{$fs} );
    }

##############################################################################
# Create PDR for re-publishing
##############################################################################

    my @publishDir;
    # publish to dotchart becomes an option, ticket #7336.
    my $dotchartDir = "$s4paRoot/" . "publish_dotchart/pending_publish";
    push ( @publishDir, $dotchartDir ) if ( $CFG::cfg_publish_dotchart );

    if ( defined $CFG::cfg_publication{$dataset}{$version} ) {
        # publish to rest of the requirement for this dataset.version
        foreach my $dir ( @{$CFG::cfg_publication{$dataset}{$version}} ) {
            # skip publish_dotchart directory
            next if ( $dir =~ /publish_dotchart/ );
            $dir = "$s4paRoot/" . "$dir";
            # for 'hidden' action, switch PDR to pending_delete directory
            $dir =~ s/pending_publish/pending_delete/ 
                if ( $action eq 'hidden' );
            push ( @publishDir, $dir ) if ( -d $dir);
        }
    }

    if ( $pdr->recount == 0 ) {
        S4P::logger( "INFO", "No matching granule found for " .
            "$dataSet" );
    } else {
        my ($granuleCount, $pdrCount ) = writePdr( $pdr, $dataset, 
            $version, $maxCount, $filePrefix, $suffix, @publishDir );
        S4P::logger( "INFO", "Total $pdrCount PDR, " .
            "$granuleCount Granules for $dataSet" );
        $totalGranule += $granuleCount;
        $totalPdr += $pdrCount;
    }
}

S4P::logger( "INFO", "Total $totalPdr PDR and $totalGranule Granules");
foreach my $fs ( keys %fsList ) {
    S4P::logger( "INFO", "Don't forget to backup $fs" );
}

exit;


##############################################################################
# sub getRange: decode startTime and endTime for granule search
##############################################################################
sub getRange {
    my $expirationStartDate;
    my $expirationEndDate;
    my $startTime;
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
        S4P::perish( 16, "expirationDays has to be in either " .
            "'YYYY-MM-DD HH:MM:SS' or 'nnn' format");
    }
    
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
        S4P::perish( 17, "startTime has to be in either " . 
            "'YYYY-MM-DD HH:MM:SS' or 'nnn' format");
    }

    return ( $startTime, $endTime );
}

##############################################################################
# Subroutine migrateFile:  move link and change permission on file
##############################################################################
sub migrateFile {
    my ( $action, $frequency, $fileList ) = @_;
    my $newFileList = {};
    my %dataDirList;
    my %hiddenDirList;

    # directory permission
    my $dirAccess = ( $action eq "restricted" ) ? 0750 : 
                ( $action eq "hidden" ) ? 0700 : 0755;
    # file permission
    my $fileAccess = ( $action eq "restricted" ) ? 0640 : 
                 ( $action eq "hidden" ) ? 0600 : 0644;

    foreach my $oldPath ( keys %$fileList ) {
        my $file = basename( $oldPath );
        my $type = $fileList->{$oldPath};
        my $oldDir = dirname( $oldPath );
        my $dataDir;
        my $newPath;

        # move link out of .hidden directory and change target permission
        if ( $action eq "public" ) {
            if ( $oldDir =~ m#(/.*)/\.hidden$# ) {
                $dataDir = $1;
                $newPath = "$dataDir/$file";
                # move link out of hidden directory
                return undef unless ( move( $oldPath, $newPath ) );
                S4P::logger( "INFO", "Moved link $oldPath to $newPath" );
                $hiddenDirList{$oldDir} = 1 
                    unless ( defined $hiddenDirList{$oldDir} );
            } else {
                $dataDir = $oldDir;
                # skip if it is not under .hidden directory
                S4P::logger( "WARN", "$oldPath is not under .hidden directory" );
            }
            $newPath = "$dataDir/$file";

            # change permission to public
            return undef unless ( chmod( $fileAccess, $newPath ) );
            S4P::logger( "INFO", "Changed $newPath permission to public" );
            $newFileList->{$newPath} = $type;

        # move link into .hidden directory and change target permission
        } elsif ( $action eq "restricted" || $action eq "hidden" ) {
            # file is already under .hidden directory
            if ( $oldDir =~ m#(/.*)/\.hidden$# ) {
                $dataDir = $1;
                $newPath = $oldPath; 

            # file is probably public
            } else {
                $dataDir = $oldDir;
                my $newDir = "$oldDir/.hidden";
                unless ( -d $newDir ) {
                     unless ( mkdir( $newDir, $dirAccess ) || ( -d $newDir ) ) {
                         S4P::perish( 18,
                             "Directory, $newDir, doesn't exist and"
                                 . " failed to create it ($!)" );
                     }
                     S4P::logger( "INFO", "Created hidden directory: $newDir" );
                }

                # move link into hidden directory
                $newPath = "$newDir/$file";
                return undef unless ( move( $oldPath, $newPath ) );
                S4P::logger( "INFO", "Moved link $oldPath to $newPath" );
            }

            # change permission to restricted or hidden
            return undef unless ( chmod( $fileAccess, $newPath ) );
            S4P::logger( "INFO", "Changed $newPath permission to $action" );
            $newFileList->{$newPath} = $type;

        } else {
            S4P::perish( 19, "Action $action not supported" );
        }
        $dataDirList{$dataDir} = 1
            unless ( defined $dataDirList{$dataDir} );
    }

    # reset directory permission
    foreach my $dir ( keys %dataDirList ) {
        my $yearDir;
        if ( $action eq 'public' ) {
            S4P::logger( "WARN", "Can't change $dir permission to $action" )
                unless ( chmod( $dirAccess, $dir ) );
            if ( $frequency eq 'yearly' || $frequency eq 'none' ) {
                $yearDir = $dir;
            } else {
                $yearDir = dirname( $dir );
                S4P::logger( "WARN", "Can't change $yearDir permission to $action" )
                    unless ( chmod( $dirAccess, $yearDir ) );
            }
            my $datasetDir = ( $frequency eq 'none' ) ? $yearDir : dirname( $yearDir );
            S4P::logger( "WARN", "Can't change $datasetDir permission to $action" )
                unless ( chmod( $dirAccess, $datasetDir ) );
            my $groupDir = dirname( $datasetDir );
            S4P::logger( "WARN", "Can't change $groupDir permission to $action" )
                unless ( chmod( $dirAccess, $groupDir ) );
        } else {
            # make sure there is no granule left in the current data directory
            # before we restrict the directory access. otherwise leave if unchanged
            my @xmlList = glob("$dir/*.xml");
            unless ( @xmlList ) {
                S4P::logger( "WARN", "Can't change $dir permission to $action" )
                    unless ( chmod( $dirAccess, $dir ) );
            }
        }
    }

    # remove empty .hidden directory
    foreach my $hiddenDir ( keys %hiddenDirList ) {
        rmdir( $hiddenDir );
    }

    return $newFileList;
}

##############################################################################
# Subroutine writePdr:  split PDR and write to files
##############################################################################
sub writePdr {
    my ( $pdr, $dataset, $version, $maxCount, 
        $filePrefix, $fileSuffix, @publishDir ) = @_;

    my $pdrFile;
    my $granuleCount = 0;
    my $pdrCount = 0;
    my @createdPdr;
    my $pdrId = time();

    my $versionID = ( $version ne '' ) ? ".$version" : "";
    my $dataSet = $dataset . $versionID;
    my $pdrPrefix = $filePrefix . "_$dataSet" . ".$pdrId";
    my $newPdr = S4P::PDR::start_pdr(
        'originating_system'=> "S4PA_MIGRATE_DATA",
        'expiration_time' => S4P::PDR::get_exp_time(3, 'days') );

    foreach my $fileGroup ( @{$pdr->file_groups} ) {
        $fileGroup->data_version( $version, "%s" );
        if ( $granuleCount == $maxCount ) {
            $pdrCount++;
            $pdrFile = $pdrPrefix . "S$pdrCount" . ".$fileSuffix";
            S4P::logger( "INFO", "Successfully created PDR: $pdrFile" )
                unless $newPdr->write_pdr( $pdrFile );
            push @createdPdr, $pdrFile;
            $newPdr = S4P::PDR::start_pdr(
                'originating_system'=> "S4PA_MIGRATE_DATA",
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
            S4P::logger( "INFO", "Successfully created PDR: $pdrFile" )
                unless $newPdr->write_pdr( $pdrFile );
            push @createdPdr, $pdrFile;
        }
    } else {
        $pdrFile = $pdrPrefix . ".$fileSuffix";
        S4P::logger( "INFO", "Successfully created PDR: $pdrFile" )
            unless $newPdr->write_pdr( $pdrFile );
        push @createdPdr, $pdrFile;
        $pdrCount++;
    }
    my $totalGranule = $granuleCount + ( $pdrCount - 1 ) * $maxCount;

    # distribute PDRs
    foreach my $dir ( @publishDir ) {
        foreach my $pdr ( @createdPdr ) {
            my $cpStatus = `cp $pdr $dir`;
            if  ( $? ) {
                S4P::logger( "WARN", "Can not copy $pdr to $dir" );
            } else {
                S4P::logger( "INFO", "Successfully copied $pdr to $dir" );
            }
        }
    }

    return ( $totalGranule, $pdrCount );
}

##############################################################################
# Subroutine updateXML:  Update metadata xsl url
##############################################################################
sub updateXML {
    my ( $urlRoot, $xmlFile ) = @_;
   
    my $xmlParser = XML::LibXML->new();
    $xmlParser->keep_blanks(0);
    my $dom = $xmlParser->parse_file( $xmlFile );
    my $doc = $dom->documentElement();

    my $newXml = XML::LibXML->new();
    $newXml->keep_blanks(0);
    my $newDom = $newXml->parse_string( $doc->toString(1) );

    $urlRoot =~ m#(://([^/]+))?(/.*)$#;
    my $styleSheet = $3 or '/data';
    $styleSheet .= ( $styleSheet =~ /\/$/
        ? 'S4paGran2HTML.xsl' : '/S4paGran2HTML.xsl' );
    $newDom->insertProcessingInstruction( 'xml-stylesheet',
        qq(type="text/xsl" href="$styleSheet") );

    local(*FH);
    open( FH, ">$xmlFile" ) || return 0;
    print FH $newDom->toString(1);
    return 1 if (close( FH ));
    return 0;
}

##############################################################################
# Subroutine updateGranuleRecord:  Update metadata file and granule.db
##############################################################################
sub updateGranuleRecord {
    my ( $dataset, $datasetPath, @fileList ) = @_;

    my $dbFile = "$datasetPath/granule.db";
    my %fsList;
    my $granuleRec = {};

    # Compute attributes to be stored for each file and store them in DBM file.
    # Attributes stored are: checksum, date, file permissions/mode.
    foreach my $linkFile ( @fileList ) {

        my $file = readlink( $linkFile );
        unless ( -f $file ) {
            S4P::logger( "ERROR", "Failed to locate target file " .
                "for: $linkFile" );
            next;
        }

        # Get file dateString
        my $key = basename( $file );
        my $date =
            ( $linkFile =~ /\/\w+\/+$dataset\/+(\d{4}\/+\d{3})\// ||
              $linkFile =~ /\/\w+\/+$dataset\/+(\d{4}\/+\d{2})\// ||
              $linkFile =~ /\/\w+\/+$dataset\/+(\d{4})\// ) ? $1 : undef;
        unless ( defined $date ) {
            # for climatology dataset
            if ( $linkFile =~ /\/\w+\/+$dataset\/+/ ) {
                $date = '';
            } else {
                S4P::logger( "ERROR", "Failed to match dateString in: $linkFile" );
                next;
            }
        }

        # Get file access attribute
        my $fs = stat( $file );
        $granuleRec->{$key}{mode} = ( $fs->mode() & 07777 );

        # Compute CRC and store it in the granule database
        my $crc = S4PA::Storage::ComputeCRC( $file );

        $granuleRec->{$key}{cksum} = $crc;
        $granuleRec->{$key}{date} = $date;

        # Find the file system of the stored granule
        # ( .+/<fs>/<dataset>/<data file> )
        if ( $file =~ /\/(\d+)\/+$dataset/ ) {
            $granuleRec->{$key}{fs} = $1;
            my $fsDir = $1 if ( $file =~ /(\/.*\/\d+)\/+$dataset/ );
            $fsList{$fsDir} = 1 unless ( defined $fsList{$fsDir} );
        } else {
            S4P::logger( "ERROR", "Failed to locate file system of $file" );
        }
    }

    # Backup granule.db before open it.
    my $cpStatus = `cp $dbFile $dbFile.bak`;
    if ( $? ) {
        S4P::perish( 20, "Can not do a backup copy of $dbFile" ) 
    } else {
        S4P::logger( "INFO", "Backup $dbFile to $dbFile.bak" );
    }

    my ( $granuleHashRef, $fileHandle ) = 
        S4PA::Storage::OpenGranuleDB( $dbFile, "rw" );

    # If unable to open granule database, complain and return.
    unless ( defined $granuleHashRef ) {
        S4P::perish( 21, "Failed to open granule database for dataset, " .
            "$dataset." );
    }

    # Transfer granule record to tied hash.
    foreach my $file ( @fileList ) {
        my $key = basename( $file );
        my $action = ( defined $granuleHashRef->{$key} ) ? "Updated" : "Inserted";
        $granuleHashRef->{$key} = $granuleRec->{$key};
        S4P::logger( "INFO", "$action $file in granule.db" );
    }
    S4PA::Storage::CloseGranuleDB( $granuleHashRef, $fileHandle );
    return %fsList;
};

##############################################################################
# Subroutine usage:  print usage and die
##############################################################################
sub usage {
  die << "EOF";
Usage: $0 <-r s4pa_root> <-a public|restricted|hidden> <-c dataClass>|<-d dataset> 
          [options]
Options are:
        -s startTime           RangeBeginningDateTime, default to 1900-01-01
        -e endTime             RangeEndingDateTime, default to 2100-12-31
        -v label1              versions labels, separated by comma.
        -g <metadata_list>     Metadata listing file.
        -n nnn                 number of granule per pdr, default to 500.
        -p <pdr_prefix>        PDR filename prefix, default to 'DO.DATA_MIGRATION'
        -l <pdr_staging_dir>   PDR staging directory, default to './'
        -w                     Optional work order (.wo) filename suffix
EOF
}

