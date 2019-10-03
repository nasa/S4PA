#!/usr/bin/perl 

=head1 NAME

s4pa_recon_associate.pl - script to reconciliate association between datasets.

=head1 SYNOPSIS

s4pa_recon_associate.pl
B<-r> I<s4pa_root_directory>
B<-d> I<dataset>
B<-s> I<range_beginningdatetime>]
B<-e> I<range_endingdatetime>
[B<-v> I<dataVersion>]
[B<-n> I<number_of_granule_per_pdr>]
[B<-p> I<pdr_prefix>]
[B<-l> I<pdr_staging_directory>]
[B<-m>]
[B<-w>]

=head1 DESCRIPTION

s4pa_recon_associate.pl accepts a dataset and time range,
find the qualified granules, add entry into associate db,
create symbolic link, and PDR for republishing update granules.

=head1 ARGUMENTS

=item B<-r>

S4pa root directory.

=item B<-d>

Dataset name.

=item B<-s>

RangeBeginDateTime in 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD' format.
Default to '1900-01-01 00:00:00'.

=item B<-e>

RangeEndDateTime in 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD' format.
Default to '2010-12-31 23:59:59'.

=item B<-v>

Optional dataset version. Versionless dataset will be assume
if not specified.

=item B<-n>

Optional maximum number of granules per PDR. Default to 500.

=item B<-p>

PDR filename prefix. Default to 'PUBLISH'.

=item B<-l>

PDR staging directory. Default to current working directory (./).

=item B<-m>

Optional flag for automatic copy all PDRs to publishing's
pending_publish directories. 

=item B<-w>

Optional work order (.wo) PDR filename.

=head1 AUTHORS

Guang-Dih Lei (guang-dih.lei@gsfc.nasa.gov)

=cut

################################################################################
# $Id: s4pa_recon_associate.pl,v 1.10 2008/10/24 16:05:01 glei Exp $
# -@@@ S4PA, Version $Name:  $
###############################################################################
#
# name: s4pa_recon_associate.pl
# revised: 09/29/2008 glei 
#

use strict;
use Safe;
use Getopt::Std;
use File::Basename;
use File::Copy;
use S4P::PDR;
use S4P::TimeTools;
use S4PA::MachineSearch;
use S4PA::GranuleSearch;
use S4PA::Metadata;
use S4PA::Storage;
use vars qw( $opt_r $opt_d $opt_v $opt_s $opt_e
             $opt_n $opt_p $opt_l $opt_m $opt_w );

getopts('r:d:s:e:v:n:p:l:mw');
usage() if ( !$opt_r && !$opt_d );

##############################################################################
# Assign option value
##############################################################################

# required s4pa root directory
my $s4paRoot = $opt_r;
S4P::perish( 1, "s4pa root directory not defined." ) unless ( $s4paRoot );
$s4paRoot =~ s/\/$//;

# required dataset
my $dataset = $opt_d;
S4P::perish( 1, "dataset not defined." ) unless ( $dataset );
S4P::logger( "INFO",  "Reconciliation target dataset: $dataset" );

# optional version, default to versionless.
my $version = ( $opt_v ) ? $opt_v : '';
S4P::logger( "INFO",  "Reconciliation target version: $version" );

# read optional range beginning data/time,
# default to Year-start 1900 and Year-end 2100.
my ( $startTime, $endTime );
if ( $opt_s ) {
    if ( $opt_s =~ /\d{4}-\d{2}-\d{2}/ ) {
        if ( $opt_s =~ /\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/ ) {
            $startTime = $opt_s;
        } else {
            $startTime = "$opt_s" . " 00:00:00";
        }
    } else {
        S4P::perish( 1, "Please specify startTime in 'YYYY-MM-DD' format" );
    }
} else {
    $startTime = '1900-01-01 00:00:00';
}
S4P::logger( "INFO", "startTime: $startTime" );

if ( $opt_e ) {
    if ( $opt_e =~ /\d{4}-\d{2}-\d{2}/ ) {
        if ( $opt_e =~ /\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/ ) {
            $endTime = $opt_e;
        } else {
            $endTime = "$opt_e" . " 23:59:59";
        }
    } else {
        S4P::perish( 1, "Please specify endTime in 'YYYY-MM-DD' format" );
    }
} else {
    $endTime = '2100-12-31 23:59:59';
}
S4P::logger( "INFO", "endTime: $endTime" );

# optional maximum granule per PDR, default to 500.
my $maxCount = $opt_n ? $opt_n : 500;
S4P::logger( "INFO",  "Maximum number of granules in PDR: $maxCount" );

# optional PDR filename prefix, default to 'PUBLISH'.
my $prefix = $opt_p ? $opt_p : 'PUBLISH';
S4P::logger( "INFO",  "PDR filename prefix: $prefix" );

# optional PDR filename suffix, default to 'PDR'.
my $suffix = $opt_w ? 'wo' : 'PDR';
S4P::logger( "INFO",  "PDR filename suffix: $suffix" );

# optional PDR staging directory, default to current directory './'
my $stageDir = $opt_l ? $opt_l : '.';
$stageDir =~ s/\/$//;
S4P::perish( 2, "Staging directory: $stageDir does not exist" )
    unless ( -d $stageDir );
S4P::logger( "INFO", "PDR staging directory: $stageDir" );
my $filePrefix = "$stageDir/" . "$prefix";

# read publishing requirement
my $cfgFile = "$s4paRoot/storage/dataset.cfg";
my $cpt = new Safe 'DATASET';
$cpt->rdo( $cfgFile ) ||
    S4P::perish( 1, "Failed to read config file $cfgFile: ($!)\n" );

# Retrieve backup site root
my $config = $s4paRoot . "/auxiliary_backup.cfg";
S4P::perish( 1, "Configuration file for backup $config does not exist: $!\n" )
    unless ( -f $config );
my $cptBackup = new Safe 'BACKUP';
$cptBackup->rdo( $config ) ||
    S4P::perish( 1, "Cannot read config file $config in safe mode: $!\n");
my $backupDir = $BACKUP::cfg_auxiliary_backup_root;
unless ( -d $backupDir ) {
    S4P::perish( 1, "Cannot mkdir $backupDir: $!" )
        unless ( mkdir( $backupDir, 0775 ) );
}

##############################################################################
# Search for update metadata files
##############################################################################

# check association
my ( $relation, $associateType, @associateDataset ) =
    S4PA::Storage::CheckAssociation( $s4paRoot, $dataset, $version );
S4P::perish( 1, "Not associate dataset found for $dataset." ) unless ( $relation );

# it is more efficient to do the reverse reconciliation.
# so, we will reverse the recon by switching to to the new associated dataset
my $origDataset = $dataset;
if ( $relation == 1 ) {
    S4P::logger( "INFO", "Found associated dataset: $associateDataset[0]" );
    S4P::logger( "INFO", "Convert it to reverse association, new target " .
        "dataset: $associateDataset[0]." );
    ( $dataset, $version ) = split /\./, $associateDataset[0], 2;
    ( $relation, $associateType, @associateDataset ) = 
        S4PA::Storage::CheckAssociation( $s4paRoot, $dataset, $version );
} else {
    S4P::logger( "INFO", "Found associated dataset: (@associateDataset)" );
}

my %modifiedFS;
my %pdrHash;
foreach my $associated ( @associateDataset ) {
    $pdrHash{$associated} = S4P::PDR::create();
}

my $dataClass = $DATASET::data_class{$dataset};
S4P::perish( 1, "Cannot find dataClass for dataset: $dataset." )
    unless ( defined $dataClass );

my %argSearch;
$argSearch{home} = $s4paRoot;
$argSearch{dataset} = $dataset;
$argSearch{version} = $version;
$argSearch{startTime} = $startTime;
$argSearch{endTime} = $endTime;

my $search = S4PA::MachineSearch->new( %argSearch );
S4P::perish( 1, "$search->errorMessage()." ) if $search->onError();

my %metaFileHash;
my @granuleLocators = $search->getGranuleLocator();
my $totalLocator = scalar( @granuleLocators );
S4P::logger( "INFO", "Found $totalLocator qualified granules" );

my $totalGranule;
my $totalUpdate;
foreach my $granule ( @granuleLocators ) {
    my $assocMetaFile = $search->locateGranule( $granule );

    my $assocGran = { "DATASET" => $dataset,
                      "VERSION" => $version,
                      "TYPE" => $associateType,
                      "METFILE" => $assocMetaFile,
                      "DATACLASS" => $dataClass
                            };

    # clean up the existing associate granule key
    my $assocDbFile = ( $version eq '' ) ?
        "$s4paRoot/storage/$dataClass/$dataset/associate.db" :
        "$s4paRoot/storage/$dataClass/$dataset.$version/associate.db";
    my ( $associateHashRef, $fileHandle ) = 
        S4PA::Storage::OpenGranuleDB( $assocDbFile, "rw" );
    # If unable to open associate database, fail the job.
    S4P::perish( 1, "Failed to open associate database: $assocDbFile." )
        unless ( defined $associateHashRef );
    my $key = basename( $assocMetaFile );
    delete $associateHashRef->{$key} if ( exists $associateHashRef->{$key} );
    S4PA::Storage::CloseGranuleDB( $associateHashRef, $fileHandle );

    # add granule to associate.db if it is not already there
    my $dataGran = {};
    my ( $assocGranule, $updatedFile ) = S4PA::Storage::StoreAssociate(
        $s4paRoot, $assocGran, $dataGran );
    if ( defined $assocGranule ) {
        S4P::logger( "INFO", "Added $assocGranule into association DB" );
        $totalGranule++;
    }

    # there could be more dataset associated with the current dataset
    foreach my $associated ( @associateDataset ) {
        my $pdr = S4P::PDR::create();
        my ( $assocDataset, $assocVersion ) = split /\./, $associated, 2;
        my $dataClass = $DATASET::data_class{$assocDataset};
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
                $s4paRoot, $assocGran, $dataGran );
            S4P::perish( 1, "Failed to store associated data" )
                unless ( defined $assocLink );
            S4P::logger( "INFO", "Created associate link: $assocLink" );

            # add this granule into republish pdr
            my $metadata = new S4PA::Metadata( FILE => $dataMetaFile );
            $pdrHash{$associated}->add_file_group( $metadata->getFileGroup() );
            push ( @{$metaFileHash{$associated}}, $updatedFile )
                if ( defined $updatedFile );

            # locate file system of the modified metadata file
            my $targetFile = readlink( $dataMetaFile );
            my $partition = dirname( dirname( $targetFile ) );
            $modifiedFS{$partition} = 1 unless ( exists $modifiedFS{$partition} );

            $totalUpdate++;
        }
    }
}
S4P::logger( "INFO", "Updated $totalGranule associated granules " .
    "and modified $totalUpdate metadata files." );

##############################################################################
# backup metadat files and create PDRs for publishing
##############################################################################

foreach my $key ( keys %metaFileHash ) {
    ( my $backupDir = $BACKUP::cfg_auxiliary_backup_root ) =~ s/\/+$//;
    $backupDir .= "/$key";
    unless ( -d $backupDir ) {
        S4P::perish( 1, "Cannot mkdir $backupDir: $!" )
            unless ( mkdir( $backupDir, 0775 ) );
    }
    foreach my $metaFile ( @{$metaFileHash{$key}} ) {
        my $toFile = "$backupDir/" . basename( $metaFile );
        S4P::logger( "ERROR", "Failed to copy $metaFile to $backupDir" )
            unless ( copy( $metaFile, $toFile ) );
    }
}

# creating pdr/wo file(s) under staging directory
foreach my $key ( keys %pdrHash ) {
    my ( $dataset, $version ) = split /\./, $key, 2;
    my $pdr = $pdrHash{$key};
    my $pdrFiles = [];

    if ( $pdr->recount == 0 ) {
        S4P::logger( "INFO", "No matching granule found for $dataset" );
    } else {
        my ( $granuleCount, $pdrCount );
        ( $granuleCount, $pdrCount, $pdrFiles ) = writePdr( $pdr, 
            $dataset, $version, $maxCount, $filePrefix, $suffix );
        S4P::logger( "INFO", "Total $pdrCount PDR, " .
            "$granuleCount Granules for $origDataset" );
    }
    
    # copy PDRs to all publishing directories
    if ( $opt_m ) {
        my @publishDir;
    
        # collect publishing requirement
        if ( defined $DATASET::cfg_publication{$dataset}{$version} ) {
            # publish to rest of the requirement for this dataset.version
            foreach my $dir ( @{$DATASET::cfg_publication{$dataset}{$version}} ) {
                # No publish to dotchart is needed.
                next if ( $dir =~ /dotchart/ );
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

# reconcile disk space usage of modifiled file system
foreach my $fs ( keys %modifiedFS ) {
    # do reconcile disk to adjust .FS_SIZE 
    my $fsRoot = dirname( $fs );
    my $fsDir = basename( $fs );
    if ( -f "$fs/.FS_SIZE" ) {
        my $status = `s4pa_reconcile_disk.pl -a update -r $fsRoot -d $fsDir`;
        my @diskSizeList =
            S4PA::Receiving::DiskPartitionTracker( "$fs/.FS_SIZE" );
        S4P::logger( "INFO", "$fs/.FS_SIZE was updated with " .
            "$diskSizeList[1] bytes left." );
    }
    S4P::logger( "INFO", "Please backups $fs if possible." );
}
exit;

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
        'originating_system'=> "S4PA_PUBLISH",
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
                'originating_system'=> "S4PA_PUBLISH",
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
Usage: $0 <-r s4pa_root> <-d dataset>
          [options]
Options are:
        -s startTime           in 'YYYY-MM-DD' format, default to 1900-01-01.
        -e endTime             in 'YYYY-MM-DD' format, default to 2101-12-31.
        -v label               versions labels.
        -n nnn                 number of granule per pdr, default to 500.
        -p <pdr_prefix>        PDR filename prefix, default to 'PUBLISH'
        -l <pdr_staging_dir>   PDR staging directory, default to './'
        -m                     Copy all PDRs to publishing pending_publish directory
        -w                     Optional work order (.wo) filename suffix
EOF
}

