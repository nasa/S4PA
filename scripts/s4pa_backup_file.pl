#!/usr/bin/perl
=head1 NAME

s4pa_backup_file.pl - A script to backup changes made to granule metadata.

=head1 SYNOPSIS

s4pa_dbimport.pl
[B<-r> I<S4PA station root directory>]
[B<-d> I<dataset name>]
[B<-v> I<version ID>]
[B<-s> I<script to make metadata changes>]
[B<-b> I<start date time to search for granules>]
[B<-e> I<end date time to search for granules>]
[B<-l> I<list of new files or files to be modified>]
[B<-a> I<archive root path>]
[B<-p>] 
[B<-n>]

=head1 DESCRIPTION

s4pa_backup_file.pl is a tool to take a given list of files or search
for granule metadata files based on date/time range, change the files by
running specified tool, update the granule.db after changes are made, and
copy to designated location as specified in a backup config file at storage
station.

=head1 ARGUMENTS

=over 4

=item [B<-r> I<S4PA station root directory>]

Root directory of S4PA stations.

=item [B<-d> I<dataset name>]

Dataset name.

=item [B<-v> I<version ID>]

Version ID. For versionless datasets, use quoted empty string ('').

=item [B<-s> I<metadata modification script>]

Tool to change the metadata files.

=item [B<-b> I<begin date time>]

Begin date time the granule metadata files to be modified.

=item [B<-e> I<end date time>]

End date time the granule metadata files to be modified.

=item [B<-l> I<list of meta files>]

List of metadata files to be modified.

=item [B<-a> I<archive root path>]

Root path of the archive for dataset

=item [B<-p>]

Optional publishing flag. All modified granules will get republished if set.

=item [B<-n>]

Optional no backup flag. All modified metadata will not get backup if set.

=back

=head1 AUTHOR

F. Fang

=cut
# $Id: s4pa_backup_file.pl,v 1.8 2017/04/20 15:27:41 glei Exp $
# -@@@ S4PA, Version $Name:  $

use Getopt::Std;
use XML::LibXML;
use File::stat;
use File::Basename;
use File::Copy;
use vars qw($opt_r $opt_d $opt_v $opt_s $opt_b $opt_e $opt_l
    $opt_a $opt_p $opt_n);
use S4P;
use S4PA::Storage;
use S4PA::GranuleSearch;
use S4PA::Metadata;
use S4P::PDR;
use Safe;

getopts('r:d:v:s:b:e:l:a:pn');

usage() if ( (not defined $opt_r) || (not defined $opt_d) );

my $s4paRoot = $opt_r;
my $dataset = $opt_d;
my $version = ( defined $opt_v ) ? $opt_v : '';

my %inputs = ( ROOT => $opt_r, DATASET => $dataset, VERSION => $version );

# Get storage station and data directory
my %result = GetDataRootDirectory(%inputs);
my ( $storageStation, $deletionStation, $operationDbFile, $dataLink ) = (
        "$result{STORAGESTATION}",
        "$result{DELETIONSTATION}",
        "$result{DATADIR}/granule.db",
        "$result{DATADIR}/data" );
if ( !(-d $storageStation) ) {
    S4P::perish( 1, "Storage station for dataset $dataset does not exist: $!\n" );
}

# Retrieve backup site root
my $config = $s4paRoot . "/auxiliary_backup.cfg";
if ( !(-f $config) ) {
    S4P::perish( 1, "Configuration file for backup $config does not exist: $!\n" );
}
my $cpt = new Safe 'CFG';
$cpt->share('$cfg_auxiliary_backup_root' );
$cpt->rdo($config) or
    S4P::perish( 4, "Cannot read config file $config in safe mode: $!\n");
my $backupDir = $CFG::cfg_auxiliary_backup_root;
$backupDir =~ s/\/+$//;
$backupDir .= "/$dataset" . ($version eq '' ? "" : ".$version");
unless ((defined $opt_n) || (-d $backupDir)) {
    S4P::perish( 4, "Cannot mkdir $backupDir: $!") if (!mkdir($backupDir, 0775));
}

# Check granule.db existence
unless ( -f $operationDbFile ) {
    S4P::perish( 1, "Granule DB file, $operationDbFile, doesn't exist" );
}

# Get storage location
my $storageDir = readlink( $dataLink );
unless ( defined $storageDir ) {
    S4P::perish( 1, "Failed to read link $dataLink" );
}
$storageDir =~ s/\/+$//g;

# Retrieve list of files if specified
my @metaFileList;
if ( $opt_l ) {
    open ( LIST,"$opt_l" ) or
      S4P::perish( 1, "Failed to open granule listing file $opt_l: ($!)" );
      while ( <LIST> ) {
          chomp() ;
          push ( @metaFileList, $_ );
      }
      close( LIST );
} else {
    # Get time-range for metadata file
    usage() if ($opt_b !~ /\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}:\d{2})*/ );
    usage() if ($opt_e !~ /\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}:\d{2})*/ );
    my ($beginDate, $beginTime) = split /\s+/, $opt_b;
    my ($endDate, $endTime) = split /\s+/, $opt_e;
    usage() if ( (not defined $beginDate) || (not defined $endDate) );
    $beginTime = '00:00:00' if (not defined $beginTime);
    $endTime = '00:00:00' if (not defined $endTime);
    my $beginDateTime = join ' ', ($beginDate, $beginTime);
    my $endDateTime = join ' ', ($endDate, $endTime);
    # Get frequency for dataset
    my $configFile = $storageStation . "/s4pa_store_data.cfg";
    my $cpt = new Safe 'CFG';
    $cpt->share( '%cfg_temporal_frequency' );
    unless ( $cpt->rdo( $configFile ) ) {
        undef $cpt;
        S4P::perish( 1, "Failed to open $configFile: ($!)" );
    }

    my $frequency = $CFG::cfg_temporal_frequency{$dataset}->{$version};
    unless ( defined $frequency ) {
        undef $cpt;
        S4P::perish( 1, "Temporal frequency not defined for " .
            "$dataset.$version" );
    }
    # Search for metadata file to modify
    my $metaSearch = new S4PA::GranuleSearch(
                      'action' => 'search',
                      'dataPath' => $storageDir,
                      'frequency' => $frequency,
                      'startTime' => $beginDateTime,
                      'endTime' => $endDateTime);
    my @xmlFileList = $metaSearch->findMetadataFile();
    # Include only meta files in time range
    foreach my $xmlFile ( @xmlFileList ) {
        my %argMetadata;
        $argMetadata{FILE} = $xmlFile;
        $argMetadata{START} = $beginDateTime;
        $argMetadata{END} = $endDateTime;
        my $metadata = new S4PA::Metadata( %argMetadata );
        next unless ( $metadata->compareDateTime( %argMetadata ) );
        push @metaFileList, $xmlFile;
    }
}
S4P::perish(1, "Failed to find metadata files") if (!@metaFileList);

# modify data if requested
if ( !($opt_s) ) {
    S4P::perish(1, "Please provide root path to archive via switch -a")
     unless ( defined ($opt_a) );
    # need check if files have valid storage/archive path
    foreach my $file (@metaFileList) {
        if ($file !~ /$dataLink/ && $file !~ /$storageDir/ && $file !~ /$opt_a/) {
            S4P::perish(1, "File $file does not have valid path");
        }
    }
    S4P::logger( "INFO", "Supplied list of files will be copied to $backupDir" );
} else {
    # set environment variables for script to use
    $ENV{S4PA_ROOT} = $s4paRoot;
    $ENV{DATASET_SHORTNAME} = $dataset;
    $ENV{DATASET_VERSION} = $version;

    # Retrieve tool for metadata modification if supplied
    my $mod_script = $opt_s;
    # Run modification tool
    foreach my $file (@metaFileList) {
        my $xmlFile = ( -l $file ) ? readlink $file : $file;
        # modifier script shall return non-empty filename as successful
        my $returnFile = `$mod_script $xmlFile`;
        if ($!) {
            # modification failed, skip this metadata file
            S4P::logger("ERROR", "Failed on running $mod_script on $xmlFile: $?.");
        } else {
            if ($returnFile) {
                S4P::logger("INFO", "Successfully update $xmlFile.");
            } else {
                S4P::logger("ERROR", "Failed to update $xmlFile: $?.");
            }
        }
    }
}

# Update granule.db for modified metadata file
my ( $granuleRef, $fileHandle ) =
           S4PA::Storage::OpenGranuleDB( $operationDbFile, "rw" );
unless ( defined $granuleRef ) {
    S4P::perish( 1, "Failed to open granule DB file, $operationDbFile" );
}
foreach my $newMetaFile (@metaFileList) {
    my $entry = basename $newMetaFile;
    my $record = $granuleRef->{$entry};
    if ( defined $record ) {
        my $replaceRecord;
        # calculate checksum for new file
        $replaceRecord->{$entry}{cksum} = S4PA::Storage::ComputeCRC( $newMetaFile );
        # maintain file system and date
        $replaceRecord->{$entry}{fs} = $record->{fs};
        $replaceRecord->{$entry}{date} = $record->{date};
        # update mode if changed
        my $st = stat( $newMetaFile );
        if ( ($st->mode() & 07777) != $record->{mode} ) {
            unless ( chmod( $record->{mode}, $newMetaFile ) ) {
                S4PA::Storage::CloseGranuleDB( $granuleRef, $fileHandle );
                S4P::perish( 1, "'chmod $record->{mode} $newMetaFile' " . "failed ($!)" );
            }
            S4P::logger( "INFO", "Changed file permissions of $newMetaFile" );
        }
        $replaceRecord->{$entry}{mode} = $record->{mode};
        # update record
        $granuleRef->{$entry} = $replaceRecord->{$entry};
        S4P::logger( "INFO", "Record in granule.db for $entry updated" );
    }
}
S4PA::Storage::CloseGranuleDB( $granuleRef, $fileHandle );

# create a PDR object with all metadata file as granules
my $pdr = S4P::PDR->new();
$pdr->{originating_system} = "S4PA_REPUBLISH";

# Copy list of files to designated backup location
foreach $newMetaFile (@metaFileList) {
    unless (defined $opt_n) {
        my $toFile = $backupDir . "/" . (basename $newMetaFile);
        S4P::perish(1, "Failed to copy $newMetaFile to $backupDir")
            unless ( copy($newMetaFile, $toFile) );
    }
    my $granule = S4PA::Metadata->new( FILE => "$newMetaFile" );
    my $fileGroup = $granule->getFileGroup();
    $pdr->add_file_group( $fileGroup );
}

# Publish all updated granules if specified
if ( defined $opt_p ) {
    # write to pdr file
    my $datasetName = ( $version eq '' ) ? $dataset : "$dataset.$version";
    my $pdrFile = $backupDir . "/DO.REPUBLISH_${datasetName}." .
        time() . ".PDR";
    S4P::perish(1, "Failed to write $pdrFile")
        if $pdr->write_pdr( $pdrFile );

    # distribute PDR
    foreach my $dir ( @{$result{PUBLISHDIR}} ) {
        my $cpStatus = `cp $pdrFile $dir`;
        if  ( $? ) {
            S4P::logger( "WARN", "Can not copy $pdrFile to $dir" );
        } else {
            S4P::logger( "INFO", "Successfully copied $pdrFile to $dir" );
        }
    }
    unlink $pdrFile if ( -f $pdrFile );
}

exit ( 0 );

################################################################################
sub usage
{
    print STDERR 
        "Use: $0\n"
        . "    -r <S4PA root>: S4PA station root directory\n"
        . "    -d <Dataset>: Dataset name\n"
        . "    -v <Version>: Dataset version label\n"
        . "    -s <Script>: Tool to make metadata modifications\n"
        . "    -b <BeginTime>: Begin time in YYYY-MM-DD [HH:MM:SS]; optional\n"
        . "    -e <EndTime>: End time in YYYY-MM-DD [HH:MM:SS]; optional\n"
        . "    -l <FileList>: List of files to be modified or backed up\n"
        . "    -p Publishing flag; optional\n"
        . "    -n No backing up metaata files flag; optional\n"
        . "    -a <ArchivePath>: Archive root path\n";
    exit( 0 );   
}
################################################################################
sub GetDataRootDirectory {
    my ( %arg ) = @_;

    my %result;
        
    # Open dataset.cfg for read;
    my $cfg_file = "$arg{ROOT}/storage/dataset.cfg";

    # Setup compartment and read config file
    my $cpt = new Safe('CFG');
    $cpt->share('%data_class');

    # Read config file
    unless ( $cpt->rdo( $cfg_file ) )  {
        S4P::logger( "ERROR", "Cannot read config file $cfg_file" );
        return %result;
    }

    # Check for required variables
    if ( !%CFG::data_class ) {
        S4P::logger( "ERROR", "No data_class in $cfg_file" );
        return %result;
    }

    # Get dataclass from config hash
    my $dataClass = $CFG::data_class{$arg{DATASET}};

    unless ( defined $dataClass ) {
        S4P::logger( "ERROR", "Dataset, $arg{DATASET}, not supported" );
        return %result;
    }
    # We can now find store_cfg file
    my $storeCfgFile = "$arg{ROOT}/storage/$dataClass/"
        . "store_$dataClass/s4pa_store_data.cfg";

    # Read store_config files
    my $cptv = new Safe( 'CFGv' );
    $cptv->share( '%cfg_data_version' );
    unless ( $cptv->rdo($storeCfgFile) ) {
        S4P::logger( "ERROR", "Cannot read config file $storeCfgFile");
        return %result;
    }

    # Check for required variables
    if ( !%CFGv::cfg_data_version ) {
        S4P::logger( "ERROR", "Data version information not found in"
            . " $storeCfgFile");
        return %result;
    }

    # Get version from config hash
    my @versionList = @{$CFGv::cfg_data_version{$arg{DATASET}}};

    my $storedVersion;
    foreach my $version ( @versionList ) {
        if ( $version eq '' ) {
            $result{DATADIR} = "$arg{ROOT}/storage/$dataClass/$arg{DATASET}";
            $result{STORAGESTATION} = "$arg{ROOT}/storage/$dataClass/store_$dataClass";
            $result{DELETIONSTATION} = "$arg{ROOT}/storage/$dataClass/delete_$dataClass";
            $result{DATAVERSION} = '';
            $result{DATACLASS} = $dataClass;
            last;
        } elsif ( $arg{VERSION} eq $version ) {
            $result{DATADIR} = "$arg{ROOT}/storage/$dataClass/$arg{DATASET}"
                . ".$version";
            $result{STORAGESTATION} = "$arg{ROOT}/storage/$dataClass/store_$dataClass";
            $result{DELETIONSTATION} = "$arg{ROOT}/storage/$dataClass/delete_$dataClass";
            $result{DATAVERSION} = $version;
            $result{DATACLASS} = $dataClass;
            last;
        }
    }

    # Get publication requirement
    my @publishDir;
    # collect publishing requirement
    if ( defined $CFG::cfg_publication{$arg{DATASET}}{$result{DATAVERSION}} ) {
        # publish to rest of the requirement for this dataset.version
        foreach my $dir ( @{$CFG::cfg_publication{$arg{DATASET}}{$result{DATAVERSION}}} ) {
            $dir = "$arg{ROOT}/" . "$dir";
            push ( @publishDir, $dir ) if ( -d $dir);
        }
    }
    $result{PUBLISHDIR} = [ @publishDir ];

    return %result;
}
