#!/usr/bin/perl
=head1 NAME

s4pa_add_h4map.pl - A script to create HDF4 map file on the existing archive.

=head1 SYNOPSIS

s4pa_add_h4map.pl
B<-r> I<S4PA station root directory>
B<-d> I<dataset name>
B<-b> I<start date time to search for granules>
B<-e> I<end date time to search for granules>
[B<-v> I<version ID>]
[B<-s> I<script to make metadata changes>]

=head1 DESCRIPTION

s4pa_add_h4map.pl is a tool to create HDF4 Map file on the existing archive.

=head1 ARGUMENTS

=over 4

=item B<-r> I<S4PA station root directory>

Root directory of S4PA stations.

=item B<-d> I<dataset name>

Dataset name.

=item [B<-v> I<version ID>]

Version ID. For versionless datasets, use quoted empty string ('').

=item B<-b> I<begin date time>

Begin date time the granule metadata files to be modified.

=item B<-e> I<end date time>

End date time the granule metadata files to be modified.

=item [B<-s> I<metadata modification script>]

HDF4 Map file creation script, default to 's4pa_create_h4map.pl -f -z'.

=back

=head1 AUTHOR

Guang-Dih Lei

=cut
# $Id: s4pa_add_h4map.pl,v 1.3 2011/06/23 16:55:28 glei Exp $
# -@@@ S4PA, Version $Name:  $

use strict;
use Getopt::Std;
use File::stat;
use File::Basename;
use S4P;
use S4PA::Storage;
use S4PA::GranuleSearch;
use S4PA::Receiving;
use S4P::PDR;
use Safe;
use vars qw($opt_r $opt_d $opt_v $opt_s $opt_b $opt_e);

getopts('r:d:v:s:b:e:l:p');
usage() unless ( defined $opt_r && defined $opt_d );

# Get specified dataset
my $s4paRoot = $opt_r;
my $dataset = $opt_d;
my $version = ( defined $opt_v ) ? $opt_v : '';

# Get time-range for metadata search 
usage() if ($opt_b !~ /\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}:\d{2})*/ );
usage() if ($opt_e !~ /\d{4}-\d{2}-\d{2}(\s+\d{2}:\d{2}:\d{2})*/ );
my ($beginDate, $beginTime) = split /\s+/, $opt_b;
my ($endDate, $endTime) = split /\s+/, $opt_e;
usage() unless ( defined $beginDate && defined $endDate );
$beginTime = '00:00:00' if (not defined $beginTime);
$endTime = '23:59:59' if (not defined $endTime);
my $beginDateTime = join ' ', ($beginDate, $beginTime);
my $endDateTime = join ' ', ($endDate, $endTime);

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

# Search for granules to modify
my %granuleList;
my $metaSearch = new S4PA::GranuleSearch(
    'action' => 'search',
    'dataPath' => $storageDir,
    'frequency' => $frequency,
    'startTime' => $beginDateTime,
    'endTime' => $endDateTime);
my $pdr = $metaSearch->createPdr();
S4P::perish(1, "Failed to find metadata files")
    unless ( $pdr->file_groups );
foreach my $fg ( @{$pdr->file_groups} ) {
    my $metaFile = $fg->met_file;
    my @dataFiles = $fg->science_files;
    $granuleList{$metaFile}{'science'} = $dataFiles[0];
}

# create hdf4 map file
my %affectedVolumes;
my $mod_script = ( defined $opt_s ) ? $opt_s : "s4pa_create_h4map.pl -f -z";
foreach my $file ( sort keys %granuleList ) {
    my $xmlFile = ( -l $file ) ? readlink $file : $file;
    my $dataFile = $granuleList{$file}{'science'};
    my $hdfFile = ( -l $dataFile ) ? readlink $dataFile : $dataFile;
    # script shall return non-empty filename as successful
    my $mapFile = `$mod_script $hdfFile $xmlFile`;

    if ( $? ) {
        S4P::logger( "ERROR", "HDF4 map creation failed on $dataFile: $!");
    } else {
        chomp( $mapFile );
        if ( -s $mapFile ) {
            S4P::logger( "INFO", "Created HDF4 map file $mapFile");
            $granuleList{$file}{'map'} = $mapFile;
            # update directory based disk space management
            my $volumePath = dirname( dirname( $mapFile ) );
            my $fsSizeFile = $volumePath . '/.FS_SIZE';
            next unless ( -f $fsSizeFile );
            # Get map file size
            my $stat = File::stat::stat( $file );
            my $mapFileSize = $stat->size;
            # Update disk space        
            my @sizeList = S4PA::Receiving::DiskPartitionTracker(
                $fsSizeFile, "update", -$mapFileSize );
            unless ( @sizeList == 2 ) {
                S4P::logger( "ERROR",
                    "Failed to update $fsSizeFile for $mapFile" );
            }
            if ( $sizeList[0]->is_nan() || $sizeList[1]->is_nan() ) {
                S4P::logger( "ERROR",
                    "Size read from disk space tracker file, $fsSizeFile, contains"
                    . " non-number" );
            } else {
                S4P::logger( "INFO",
                    "Updated $fsSizeFile for $mapFile" );
                my $volume = basename( $volumePath );
                $affectedVolumes{$volume} = $sizeList[1];
            }
        } else {
            S4P::logger( "ERROR", "HDF4 map file is empty on $dataFile.");
        }
    }
}

# Update granule.db for modified metadata file
my ( $granuleRef, $fileHandle ) =
           S4PA::Storage::OpenGranuleDB( $operationDbFile, "rw" );
unless ( defined $granuleRef ) {
    S4P::perish( 1, "Failed to open granule DB file, $operationDbFile" );
}
foreach my $newMetaFile ( sort keys %granuleList ) {
    # skip granule.db update if no map file created
    my $newMapFile = $granuleList{$newMetaFile}{'map'};
    next unless ( defined $newMapFile );

    # update metadata entry
    my $entry = basename( $newMetaFile );
    my $record = $granuleRef->{$entry};
    my $replaceRecord = {};
    if ( defined $record ) {
        # calculate checksum for new file
        $replaceRecord->{cksum} = S4PA::Storage::ComputeCRC( $newMetaFile );
        # maintain file system, mode and date
        $replaceRecord->{fs} = $record->{fs};
        $replaceRecord->{date} = $record->{date};
        $replaceRecord->{mode} = $record->{mode};
        # update record
        $granuleRef->{$entry} = $replaceRecord;
        S4P::logger( "INFO", "Record updated in granule.db for $entry" );
    }

    # add mapfile entry into granule.db
    my $mapEntry = basename( $newMapFile );
    $replaceRecord->{cksum} = S4PA::Storage::ComputeCRC( $newMapFile );
    $granuleRef->{$mapEntry} = $replaceRecord;
    S4P::logger( "INFO", "Record updated in granule.db for $mapEntry" );

    # add mapfile storage link
    my $metaPath = dirname( $newMetaFile );
    chmod( $replaceRecord->{mode}, $newMapFile );
    my $linkTarget = "$metaPath/" . $mapEntry;
    unlink $linkTarget if ( -l $linkTarget );
    if ( symlink( $newMapFile, $linkTarget ) ) {
        S4P::logger( "INFO", "Created a symbolic link, $linkTarget to $mapEntry" );
    } else {
        S4P::logger( "ERROR", "Failed to create a symbolic link, $linkTarget " .
            "to $mapEntry: $!" );
    }
}
S4PA::Storage::CloseGranuleDB( $granuleRef, $fileHandle );

# Raise flag on over-filled volume
foreach my $volume ( sort keys %affectedVolumes ) {
    S4P::logger( "INFO", "Volume $volume need to be backup." );
    S4P::logger( "WARN", "Volume $volume is over-filled by " .
        "-$affectedVolumes{$volume} bytes." ) if ( $affectedVolumes{$volume} < 0 );
}

exit ( 0 );

################################################################################
sub usage
{
    print STDERR 
        "Use: $0 -r <S4paRoot> -d <Dataset> -b <StartDate> -e <EndDate>\n"
        . "      [-v <Version>: Dataset version label, default to '']\n"
        . "      [-s <Script>: Map creation script, default to 's4pa_create_h4map.pl -f -z']\n";
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

    return %result;
}
