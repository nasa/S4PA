#!/usr/bin/perl
=head1 NAME

s4pa_dbmerge.pl - A script to reconcile and merge operation granule.db with
a working granule DB file.

=head1 SYNOPSIS

s4pa_dbmerge.pl
[B<-r> I<S4PA station root directory>]
[B<-d> I<dataset name>]
[B<-v> I<version ID>]
[B<-f> I<working granule DB filePath>]
[B<-c>]


=head1 DESCRIPTION

s4pa_dbmerge.pl is a tool to update granule.db at the storage station using
a working granule DB file.  To ensure the updated granule.db file is current,
the script checks if the storage/deletion process is still on and if so,
exit and asking for turning off the storage/deletion stations.

=head1 ARGUMENTS

=over 4

=item [B<-r> I<S4PA station root directory>]

Root directory of S4PA stations.

=item [B<-d> I<dataset name>]

Dataset name.

=item [B<-v> I<version ID>]

Version ID. For versionless datasets, use quoted empty string ('').

=item [B<-f> I<working granule data base>]

Full path and filename of working granule database file.

=item [B<-c> I<clean up>]

Remove working and copy of operation database files.

=back

=head1 AUTHOR

F. Fang

=cut
# $Id: s4pa_dbmerge.pl,v 1.3 2008/05/28 17:40:31 glei Exp $
# -@@@ S4PA, Version $Name:  $

use Getopt::Std;
use XML::LibXML;
use File::stat;
use File::Basename;
use File::Copy;
use vars qw($opt_r $opt_d $opt_v $opt_f $opt_c);
use S4P;
use S4PA::Storage;
use S4PA::Metadata;
use S4P::PDR;
use strict;

getopts('r:d:v:f:c');

usage() if ( (not defined $opt_r) || (not defined $opt_d)
          || (not defined $opt_v) || (not defined $opt_f) ); 

my $s4paRoot = $opt_r;
my $workingDbFile = $opt_f;

my %inputs = ( ROOT => $opt_r, DATASET => $opt_d, VERSION => $opt_v );

# check if any data storage or deletion station running
my %result = GetDataRootDirectory(%inputs);

my ( $storageStation, $deletionStation, $operationDbFile, $dataLink ) = (
        "$result{STORAGESTATION}",
        "$result{DELETIONSTATION}",
        "$result{DATADIR}/granule.db",
        "$result{DATADIR}/data" );

if (!(-d $storageStation)) {
    S4P::perish( 1, "Storage station for dataset $opt_d does not exist" );
}
if (!(-d $deletionStation)) {
    S4P::perish( 1, "Deletion station for dataset $opt_d does not exist" );
}

S4P::perish( 1, "Please shut down storage station for dataset $opt_d")
          if ( S4P::check_station($storageStation) );
S4P::perish( 1, "Please shut down deletion station for dataset $opt_d")
          if ( S4P::check_station($deletionStation) );

unless ( -f $operationDbFile ) {
    S4P::perish( 1, "Granule DB file, $operationDbFile, doesn't exist" );
}
my $dbFile = $operationDbFile;

# Make a backup copy of the operation database
my $dbFileCopy = $operationDbFile . ".bak";
if ( copy( $operationDbFile, $dbFileCopy ) ) {
    S4P::logger( 'INFO', "Copied $operationDbFile to $dbFileCopy" );
} else {
    S4P::perish( 1, "Failed to copy $operationDbFile to $dbFileCopy" );
    unlink( $dbFileCopy );
}

# Open operation DB file
my ( $operationGranuleRef, $operationFileHandle ) =
               S4PA::Storage::OpenGranuleDB( $operationDbFile, "rw" );
unless ( defined $operationGranuleRef ) {
    S4P::perish( 1, "Failed to open granule DB file, $operationDbFile" );
}

# Open working DB file
my ( $workingGranuleRef, $workingFileHandle ) =
               S4PA::Storage::OpenGranuleDB( $workingDbFile, "rw" );
unless ( defined $workingGranuleRef ) {
    S4P::perish( 1, "Failed to open granule DB file, $workingDbFile" );
}

my $storageDir = readlink( $dataLink );
unless ( defined $storageDir ) {
    S4P::perish( 1, "Failed to read link $dataLink" );
}
$storageDir =~ s/\/+$//g;

my $operationFsDir;
my $operationFs;
my $workingFsDir;
my $workingFs;
my $fileCount = 0;

# Copy the operation DB to a temporary hash
my $tempGranuleRef;
foreach my $opFile (keys %$operationGranuleRef) {
    $tempGranuleRef->{$opFile} = $operationGranuleRef->{$opFile};
}

# Reconcile with working DB
RECORD: foreach my $file (keys %$workingGranuleRef) {
    # Insert record if file not in operation DB.
    if (not defined $operationGranuleRef->{$file}) {
        $tempGranuleRef->{$file} = $workingGranuleRef->{$file};
        S4P::logger( "INFO", "Inserted $file to operation from working DB");
    } else {
        my $operationStorageDir;
        # establish storage and archive paths for operation DB file
        if ( defined $operationGranuleRef->{$file}{date} ) {
            $operationStorageDir = "$storageDir/$operationGranuleRef->{$file}{date}";
        } else {
            S4P::logger( "ERROR",
                "Date directory not defined for $file in $operationDbFile" );
        }
        next RECORD unless defined $operationStorageDir;
        if ( defined $operationGranuleRef->{$file}{mode} ) {
            $operationStorageDir .= "/.hidden"
                if ( $operationGranuleRef->{$file}{mode} == 0640 ||
                     $operationGranuleRef->{$file}{mode} == 0600 );
        } else {
            undef $operationStorageDir;
            S4P::logger( "ERROR", "File mode not defined for $file in $operationDbFile" );
        }
        next RECORD unless defined $operationStorageDir;
        my $operationArchiveFile = readlink("$operationStorageDir/$file");
        if (defined $operationArchiveFile) {
            $operationFsDir = dirname(dirname $operationArchiveFile);
            $operationFs = basename $operationFsDir;
        } else {
            S4P::logger( "INFO", "Storage link for operation file $file may not exist" );
        }

        # establish storage and archive paths for working DB file
        my $workingStorageDir;
        if ( defined $workingGranuleRef->{$file}{date} ) {
            $workingStorageDir = "$storageDir/$workingGranuleRef->{$file}{date}";
        } else {
            S4P::logger( "ERROR",
                "Date directory not defined for $file in $workingDbFile" );
        }
        next RECORD unless defined $workingStorageDir;
        if ( defined $workingGranuleRef->{$file}{mode} ) {
            $workingStorageDir .= "/.hidden"
                if ( $workingGranuleRef->{$file}{mode} == 0640 ||
                     $workingGranuleRef->{$file}{mode} == 0600 );
        } else {
            undef $workingStorageDir;
            S4P::logger( "ERROR", "File mode not defined for $file in $workingDbFile" );
        }
        next RECORD unless defined $workingStorageDir;
        my $workingArchiveFile = readlink("$workingStorageDir/$file");
        if (defined $workingArchiveFile) {
            $workingFsDir = dirname(dirname $workingArchiveFile);
            $workingFs = basename $workingFsDir;
        } else {
            S4P::logger( "INFO", "Storage link for working file $file may not exist" );
        }

        # Check existence of both records if 'date' different
        if ($operationGranuleRef->{$file}{date} ne
                 $workingGranuleRef->{$file}{date}) {
            # Check existence of both records in storage if 'date' different
            if ( (-e "$operationStorageDir/$file") && (-e "$workingStorageDir/$file") ) {           
                S4P::logger( "ERROR", "Cannot resolve $file in Operation and working DB; both files exist" );
                next RECORD;
            } elsif ( !(-e "$operationStorageDir/$file") && (-e "$workingStorageDir/$file") ) {
                $tempGranuleRef->{$file} = $workingGranuleRef->{$file};
                S4P::logger( "INFO", "Replaced $file in operation from working DB" );
            }
        }
        if ($operationGranuleRef->{$file}{fs} ne $workingGranuleRef->{$file}{fs}) {
            # check file system of both records in archive if differ
            if ( ($operationFs ne $operationGranuleRef->{$file}{fs}) &&
                 ($workingFs eq $workingGranuleRef->{$file}{fs}) ) {
                $tempGranuleRef->{$file} = $workingGranuleRef->{$file};
                S4P::logger( "INFO", "Replaced $file to operation from working DB");
            } elsif ( ($operationFs ne $operationGranuleRef->{$file}{fs}) &&
                      ($workingFs ne $workingGranuleRef->{$file}{fs}) ) {
                S4P::logger( "ERROR", "File systems of $file in either operation or working DB incorrect");
                next RECORD;
            }
        }
        if ($operationGranuleRef->{$file}{mode} ne $workingGranuleRef->{$file}{mode}) {
            # check mode of both records if differ
            my $operationMode = stat($operationArchiveFile)->mode;
            my $workingMode = stat($workingArchiveFile)->mode;
            if ( ($operationMode ne $operationGranuleRef->{$file}{mode}) &&
                 ($workingMode eq $workingGranuleRef->{$file}{mode}) ) {
                $tempGranuleRef->{$file} = $workingGranuleRef->{$file};
                S4P::logger( "INFO", "Replaced $file to operation from working DB");
            } elsif ( ($operationMode ne $operationGranuleRef->{$file}{mode}) &&
                      ($workingMode ne $workingGranuleRef->{$file}{mode}) ) {
                S4P::logger( "ERROR", "File modes of $file in either operation or working DB incorrect");
                next RECORD;
            }
        }
        if ($operationGranuleRef->{$file}{cksum} ne $workingGranuleRef->{$file}{cksum}) {
            # Update checksum if records differ
            my $operationFilePath = "$operationStorageDir/$file";
            my $workingFilePath = "$workingStorageDir/$file";
            if (-f $operationFilePath) {
                $tempGranuleRef->{$file}{cksum} =
                           S4PA::Storage::ComputeCRC( $operationFilePath );
                S4P::logger( "INFO", "Updated checksum for $file in operation DB" );
            } elsif (-f $workingFilePath) {
                $tempGranuleRef->{$file}{cksum} =
                           S4PA::Storage::ComputeCRC( $workingFilePath );
                S4P::logger( "INFO", "Replaced checksum for $file in operation based on computing using working DB record ");
            } else {
                S4P:logger( "ERROR", "File $file in neither operation nor working DB exist in storage" );
                next RECORD;
            }
        }
    }
}

# Remove DB file under storage station and replace with merged copy
if (unlink($dbFile)) {
    my ( $granuleRef, $fileHandle ) = S4PA::Storage::OpenGranuleDB( $dbFile, "rw" );
    unless ( defined $granuleRef ) {
        S4P::logger( "ERROR", "Failed to open granule DB file, $dbFile" );
    }
    foreach my $file (keys %$tempGranuleRef) {
        $granuleRef->{$file} = $tempGranuleRef->{$file};
    }
    S4PA::Storage::CloseGranuleDB( $granuleRef, $fileHandle );
} else {
    S4P::logger( "Error", "Failed to remove operation DB $dbFile");
}

# Close database files
S4PA::Storage::CloseGranuleDB( $workingGranuleRef, $workingFileHandle );
S4PA::Storage::CloseGranuleDB( $operationGranuleRef, $operationFileHandle );

# Remove working and copy of operation database files if requested
if ($opt_c) {
    unlink($dbFileCopy);
    S4P::logger( "INFO", "Removed copy of operation DB $dbFileCopy" );
    unlink($workingDbFile);
    S4P::logger( "INFO", "Removed working DB file $workingDbFile" );
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
        . "    -f <Working DB>: Working DB file path\n"
        . "    -c remove working and copy of operation DB files\n";
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
