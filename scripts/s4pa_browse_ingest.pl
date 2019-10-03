#!/usr/bin/perl

=head1 NAME

s4pa_browse_ingest.pl - script to replace/ingest AIRS browse files.

=head1 SYNOPSIS

s4pa_recon_associate.pl
B<-r> I<s4pa_root_directory>
B<-d> I<dataset>
B<-s> I<browse_staging_directory>
[B<-v> I<dataVersion>]
[B<-n> I<number_of_granule_per_pdr>]
[B<-p> I<pdr_prefix>]
[B<-l> I<pdr_staging_directory>]
[B<-m>]

=head1 DESCRIPTION

s4pa_browse_ingest.pl accepts a dataset and staging directory,
pickup all browse files from the staging directory, locate
each associated granule metadata file, replace the old browse 
file or insert the new one, modify metadata file with the new 
browse if necessary, update granule.db, backup any modified 
metadata file, create PDR for re-publishing to ECHO, then do
s4pa_reconcile_disk (update) for each modified file system.

=head1 ARGUMENTS

=item B<-r>

S4pa root directory.

=item B<-d>

Dataset name.

=item B<-s>

Browse file staging directory.

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
pending_publish directories. Currently, only ECHO needs a 
replacement of the browse file.

=head1 AUTHORS

Guang-Dih Lei (guang-dih.lei@gsfc.nasa.gov)

=cut

################################################################################
# $Id: s4pa_browse_ingest.pl,v 1.4 2008/10/09 00:24:26 glei Exp $
# -@@@ S4PA, Version $Name:  $
###############################################################################
#
# name: s4pa_browse_ingest.pl
# revised: 09/30/2008 glei 
#

use strict;
use Safe;
use Getopt::Std;
use File::Basename;
use File::Copy;
use S4P::PDR;
use S4PA::Metadata;
use S4PA::Storage;
use vars qw( $opt_r $opt_d $opt_v $opt_s
             $opt_n $opt_p $opt_l $opt_m );

getopts('r:d:s:v:n:p:l:m');
usage() if ( !$opt_r || !$opt_d || !$opt_s );

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

# required pending directory
my $pendingDir = $opt_s;
S4P::perish( 1, "pending directory not defined." ) unless ( $pendingDir );
S4P::logger( "INFO",  "New browse file directory: $pendingDir" );
$pendingDir =~ s/\/+$//;

# optional version, default to versionless.
my $version = ( $opt_v ) ? $opt_v : '';
S4P::logger( "INFO",  "Reconciliation target version: $version" );

# optional maximum granule per PDR, default to 500.
my $maxCount = ( defined $CFG::cfg_max_granule_count ) ?
    $CFG::cfg_max_granule_count : $opt_n ? $opt_n : 500;
S4P::logger( "INFO",  "Maximum number of granules in PDR: $maxCount" );

# optional PDR filename prefix, default to 'PUBLISH'.
my $prefix = ( defined $CFG::cfg_pdr_prefix ) ?
    $CFG::cfg_pdr_prefix : $opt_p ? $opt_p : 'PUBLISH';
S4P::logger( "INFO",  "PDR filename prefix: $prefix" );

# optional PDR staging directory, default to current directory './'
my $stageDir = ( defined $CFG::cfg_pdr_staging ) ?
    $CFG::cfg_pdr_staging : $opt_l ? $opt_l : '.';
$stageDir =~ s/\/+$//;
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
$backupDir =~ s/\/+$//;
unless ( -d $backupDir ) {
    S4P::perish( 1, "Cannot mkdir $backupDir: $!" )
        unless ( mkdir( $backupDir, 0775 ) );
}
$backupDir .= "/$dataset" . ($version eq '' ? "" : ".$version");
unless ( -d $backupDir ) {
    S4P::perish( 1, "Cannot mkdir $backupDir: $!" )
        unless ( mkdir( $backupDir, 0775 ) );
}

##############################################################################
# Browse replace or ingest
##############################################################################

# locate dataClass and dataset's data path
my $dataClass = $DATASET::data_class{$dataset};
S4P::perish( 1, "Cannot find dataClass for dataset: $dataset" )
    unless ( defined $dataClass );
my $datasetPath = ( $version eq '' ) ?
    "$s4paRoot/storage/$dataClass/$dataset" :
    "$s4paRoot/storage/$dataClass/$dataset.$version";
my $dataLink = $datasetPath . "/data";
my $dataPath = readlink( $dataLink );
$dataPath =~ s/\/+$//;

# locate granule.db and open it.
my $dbFile = $datasetPath . "/granule.db";

# If unable to open granule database, fail it.
my ( $granuleHashRef, $fileHandle ) = S4PA::Storage::OpenGranuleDB( $dbFile, "rw" );
S4P::perish( 1, "Failed to open granule database for dataset, $dataset." )
    unless ( defined $granuleHashRef );

# collect browse filename from pending directory
opendir( PDRDIR,"$pendingDir" ) 
   || S4P::perish( 3, "Failed to open $pendingDir: $!" );
my @files = readdir( PDRDIR );
close( PDRDIR ) ;

my @modifiedXML;
my %modifiedFS;
my $pdr = S4P::PDR->new();
$pdr->{originating_system} = "S4PA_REPUBLISH";

foreach my $browseFile ( @files ) {
    next unless ( $browseFile =~ /^(.*)\.jpg$/i );

    # Assuning new browse file has the same filename as the 
    # granule file # with '.jpg' append to it.
    my $metaFile = "$1" . '.xml';

    my $metaRec = $granuleHashRef->{$metaFile};
    unless ( defined $metaRec ) {
        S4P::logger( "INFO", "$browseFile does not have an existing " .
            "granule record in granule.db, skip it." );
        next;
    }
    my $metaPath = $dataPath . "/" . $metaRec->{date}
        . "/$metaFile";
    unless ( -f $metaPath ) {
        S4P::logger( "ERROR", "Metadata ($metaPath) file not found, skip it" );
        next; 
    }

    # copy the browse file to file system
    my $browsePath = "$pendingDir/$browseFile";
    my $targetDir = dirname( readlink( $metaPath ) );
    if ( File::Copy::copy( $browsePath, $targetDir ) ) {
        S4P::logger( 'INFO', "Copied $browsePath to $targetDir" );
        my $partition = dirname( $targetDir );
        $modifiedFS{$partition} = 1 unless ( exists $modifiedFS{$partition} );
    } else {
        S4P::perish( 1, "Failed to copy $browsePath to $targetDir" );
    }

    # create new browse file link
    my $browseLink = dirname( $metaPath ) . "/$browseFile";
    my $linkTarget = $targetDir . "/$browseFile";
    unlink $browseLink if ( -l $browseLink );
    unless ( symlink( $linkTarget, $browseLink ) ) {
        S4P::perish( 1, "Failed to create symbolic link, $linkTarget to the file, "
            . "$browseLink: $!" );
    }

    # modify metadata for new browse file if necessary
    my $modified = 0;
    my $metadata = new S4PA::Metadata( FILE => $metaPath );
    my ( $browseValue ) = $metadata->getValue(
        "/S4PAGranuleMetaDataFile/DataGranule/BrowseFile" );

    # <BrowseFile> should be already in metadata if this is a replacement
    if ( defined $browseValue ) {

        # based on our assumption of the how the metadata was located,
        # incoming browse should have the same filename as the current one.
        # if not, we will replace it.
        unless ( $browseValue eq $browseFile ) {
            $metadata->replaceNode(
                XPATH => "/S4PAGranuleMetaDataFile/DataGranule/BrowseFile",
                VALUE => $browseFile );
            $modified = 1;
        }

    # new browse file for granule that does not have a <BrowseFile>.
    } else {
        $metadata->insertNode(
            NAME => 'BrowseFile',
            BEFORE => "/S4PAGranuleMetaDataFile/DataGranule/SizeBytesDataGranule",
            VALUE => $browseFile );
        $modified = 1;
    }

    # pdr is for publish ECHO only. So, no matter if the metadata file got
    # modified or not, we will need to publish to ECHO for the new browse file
    $pdr->add_file_group( $metadata->getFileGroup() );

    # update metadata file and granule.db checksum if necessary.
    if ( $modified ) {
        S4P::perish( 1, "Faile to write metadata file:$metaPath ($!)" )
            unless( $metadata->write() );
        push @modifiedXML, $metaPath;
        my $metaCRC = S4PA::Storage::ComputeCRC( $metaPath );
        $metaRec->{cksum} = $metaCRC;
        $granuleHashRef->{$metaFile} = $metaRec;
    }

    # update the browse record in granule.db
    $browsePath = "$targetDir/$browseFile";
    my $browseCRC = S4PA::Storage::ComputeCRC( $browsePath );
    my $browseRec = $granuleHashRef->{$browseFile};
    if ( defined $browseRec ) {
        $browseRec->{cksum} = $browseCRC;
        $granuleHashRef->{$browseFile} = $browseRec;

    # insert the new browse record in granule.db
    } else {
        my $browseRec = {};
        $browseRec->{cksum} = $browseCRC;
        $browseRec->{date} = $metaRec->{date};
        $browseRec->{fs} = $metaRec->{fs};
        $browseRec->{mode} = $metaRec->{mode};
        $granuleHashRef->{$browseFile} = $browseRec;
    }
}
S4PA::Storage::CloseGranuleDB( $granuleHashRef, $fileHandle );

##############################################################################
# backup metadat files and create PDRs for publishing
##############################################################################

foreach my $metaFile ( @modifiedXML ) {
    my $toFile = "$backupDir/" . basename( $metaFile );
    S4P::logger( "ERROR", "Failed to copy $metaFile to $backupDir" )
        unless ( copy( $metaFile, $toFile ) );
}

# creating pdr file(s) under staging directory
my $pdrFiles = [];
if ( $pdr->recount == 0 ) {
    S4P::logger( "INFO", "No granule need to be published." );
} else {
    my ( $granuleCount, $pdrCount );
    ( $granuleCount, $pdrCount, $pdrFiles ) = writePdr( $pdr, 
        $dataset, $version, $maxCount, $filePrefix );
    S4P::logger( "INFO", "Total $pdrCount PDR, " .
        "$granuleCount Granules for publishing." );
}

# copy PDRs to all publishing directories
if ( $opt_m ) {
    my @publishDir;

    # collect publishing requirement
    if ( defined $DATASET::cfg_publication{$dataset}{$version} ) {
        # publish to rest of the requirement for this dataset.version
        foreach my $dir ( @{$DATASET::cfg_publication{$dataset}{$version}} ) {
            # So far, only publish to ECHO is needed for new browse file
            # ingest or replacement.
            next unless ( $dir =~ /echo/ );
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
    my ( $pdr, $dataset, $version, $maxCount, $filePrefix ) = @_;

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
            $pdrFile = $pdrPrefix . "S$pdrCount" . ".PDR";
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
            $pdrFile = $pdrPrefix . "S$pdrCount" . ".PDR";
            S4P::logger( "INFO", "Successfully create PDR: $pdrFile" )
                unless $newPdr->write_pdr( $pdrFile );
            push ( @pdrFileList, $pdrFile );
        }
    } else {
        $pdrFile = $pdrPrefix . ".PDR";
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
Usage: $0 <-r s4pa_root> <-d dataset> <-s browse_pending_directory>
          [options]
Options are:
        -v label               versions labels.
        -n nnn                 number of granule per pdr, default to 500.
        -p <pdr_prefix>        PDR filename prefix, default to 'PUBLISH'
        -l <pdr_staging_dir>   PDR staging directory, default to './'
        -m                     Copy all PDRs to publishing pending_publish directory
EOF
}

