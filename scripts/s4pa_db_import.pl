#!/usr/bin/perl

=head1 NAME

s4pa_db_import.pl - script for restore granule db file
from the instance db export dump file.

=head1 SYNOPSIS

s4pa_db_import.pl
B<-r> I<s4pa_root_directory>
B<-a> I<action>
B<-f> I<dbExport_file>
B<-c> I<dataClass>|B<-d> I<dataset>
B<-l> I<staging_directory>
[B<-v> I<dataVersion>]

=head1 DESCRIPTION

s4pa_db_import.pl can be used to restore one (specified dataset) or 
multiple (all datasets in specified dataClass) granule.db file(s) from 
the db export dump file. Three actions can be chosen from: <replace> will 
replace the operational granule.db file under each dataset path; 
<restore> will only create new granule.db file(s) under the specified 
staging directory; <append> will merge the new granule recrods from
the dump file to the operational granule.db.

=head1 ARGUMENTS

=item B<-r>

S4pa root directory.

=item B<-a>

Action. Either 'restore', 'replace' or 'append'.

=item B<-f>

dbExport file.

=item B<-c>

DataClass name. All datasets under that class will be restored.

=item B<-d>

Dataset name. Only the specified dataset will be restored.

=item B<-v>

Optional dataset version. Versionless dataset will be assume
if not specified.

=item B<-l>

granule.db staging directory. Default to current working directory (./).

=head1 AUTHORS

Guang-Dih Lei (guang-dih.lei@gsfc.nasa.gov)

=cut

################################################################################
# $Id: s4pa_db_import.pl,v 1.7 2016/09/27 12:43:04 glei Exp $
# -@@@ S4PA, Version $Name:  $
###############################################################################
#
# name: s4pa_db_import.pl
# created: 04/09/2008 glei 
#

use strict;
use Safe;
use Getopt::Std;
use File::Copy;
use File::Temp qw(tempfile);
use S4P;
use S4PA::Storage;
use vars qw( $opt_r $opt_a $opt_f $opt_c $opt_d $opt_v $opt_l );

getopts('r:a:f:c:d:v:l:');
usage() unless ( $opt_a && $opt_r && $opt_f );

##############################################################################
# Process required arguments
##############################################################################

my $dataClass;
# hash for granule.db location. key is "dataset.version" and 
# valud is the full path of its granule.db file.
my %dataSets;

# required s4pa root directory 
my $s4paRoot = $opt_r;
S4P::perish( 1, "s4pa root directory not defined." ) unless ( $s4paRoot );
$s4paRoot =~ s/\/$//;

# required action
my $action = $opt_a;
S4P::perish( 2, "Action has to be either 'restore', 'replace' or 'append'" )
    unless ( $action eq 'restore' || $action eq 'replace' || 
        $action eq 'append' );

# required export file
my $exportFile = $opt_f;
S4P::perish( 3, "No db export file specified or file does not exist" )
    unless ( -f $opt_f );
S4P::perish( 3, "$opt_f is a non-text file, please uncompress it first." )
    unless ( -T $opt_f );

my $conf;
# for dataclass level search all dataset under that dataclass
if ( defined $opt_c ) {
    $dataClass = $opt_c;

    # collect all dataset, version from s4pa_store_data.cfg
    $conf = "$s4paRoot/storage/$dataClass/store_$dataClass/s4pa_store_data.cfg";
    S4P::perish( 4, "Dataset configuration file, $conf, does not exist." )
        unless ( -f $conf );

    # Read dataset configuration file
    my $cpt = new Safe 'DATASET';
    $cpt->rdo( $conf ) or
        S4P::perish( 4, "Failed to read configuration file $conf ($@)" );

    foreach my $dataset ( keys %DATASET::cfg_data_version ) {
        foreach my $version ( @{$DATASET::cfg_data_version{$dataset}} ) {
            my $versionID = ( $version ne '' ) ? ".$version" : "";
            my $dataSet = $dataset . $versionID;
            my $targetDir = "$s4paRoot/storage/$dataClass/$dataSet/";
            S4P::perish( 6, "Dataset directory does not exist: $targetDir" )
                unless ( -d $targetDir );
            my $targetDb = $targetDir . "granule.db";
            $dataSets{$dataSet} = $targetDb;
        }
    }
}
elsif ( defined $opt_d ) {
    my $dataset = $opt_d;

    # locate dataset.cfg to for dataSet -> dataClass configuration
    $conf = "$s4paRoot/storage/dataset.cfg";
    S4P::perish( 4, "Dataclass configuration file, $conf, does not exist." )
        unless ( -f $conf );

    # read dataClass configuration file
    my $cpt = new Safe 'DATACLASS';
    $cpt->rdo( $conf ) or
    S4P::perish( 4, "Failed to read configuration file $conf ($@)" );

    # locate dataClass from dataset.cfg 
    $dataClass = $DATACLASS::data_class{$dataset}
        if ( defined $DATACLASS::data_class{$dataset} );
    S4P::perish( 4, "dataClass not difined for dataset: $dataset" )
        unless ( defined $dataClass );
    my $versionID = ( defined $opt_v ) ? ".$opt_v" : "";
    my $dataSet = $dataset . $versionID;
    my $targetDir = "$s4paRoot/storage/$dataClass/$dataSet/";
    S4P::perish( 4, "Dataset directory does not exist: $targetDir" )
        unless ( -d $targetDir );
    my $targetDb = $targetDir . "granule.db";
    $dataSets{$dataSet} = $targetDb;
}
else {
    S4P::logger( "ERROR", "Please specify a dataClass (-c) or dataSet (-d)" );
    usage();
}

# make sure storage, delete, check integrity stations are stop
# for replace and append action
if ( $action =~ /replace|append/ ) {
    my $storageStation = "$s4paRoot/storage/$dataClass/store_$dataClass";
    S4P::perish( 10, "Please shutdown storage station for $dataClass" )
        if ( S4P::check_station( $storageStation ) );
    my $deleteStation = "$s4paRoot/storage/$dataClass/delete_$dataClass";
    S4P::perish( 10, "Please shutdown delete station for $dataClass" )
        if ( S4P::check_station( $deleteStation ) );
    my $checkStation = "$s4paRoot/storage/$dataClass/check_$dataClass";
    S4P::perish( 10, "Please shutdown check integrity station for $dataClass" )
        if ( S4P::check_station( $checkStation ) );
}

# optional staging directory, default to current directory './'
my $stageDir = $opt_l ? $opt_l : '.';
$stageDir =~ s/\/+$//;
S4P::perish( 11, "Staging directory: $stageDir does not exist" )
    unless ( -d $stageDir );
S4P::logger( "INFO", "granule.db staging directory: $stageDir" );

##############################################################################
# Loop on dataset for granule db restore
##############################################################################

foreach my $dataSet ( keys %dataSets ) {
    my $dataset;
    my $version;
    if ( $dataSet =~ /\./ ) {
        ( $dataset, $version ) = split /\./, $dataSet, 2
    } else {
        $dataset = $dataSet;
        $version = '';
    }
    S4P::logger( "INFO", "Dataset: $dataset" );
    S4P::logger( "INFO", "Version: $version" );

    # create a temporary granule.db extension name
    my $template = 'XXXXXXXXXX';
    my (undef, $dbExtension) = File::Temp::tempfile($template, OPEN => 0);

    my $newDbFile = "$stageDir/granule.db." . $dbExtension;
    my ( $granuleHashRef, $fileHandle ) =
        S4PA::Storage::OpenGranuleDB( $newDbFile, "rw" );

    # If unable to open granule database, complain and return.
    unless ( defined $granuleHashRef ) {
        S4P::perish( 12,
            "Failed to open granule database for dataset, $dataSet." );
    }

    # for append action, dump the current granule.db and insert all 
    # records to the newly created granuleHashRef before insert records
    # from the dump file.
    if ( $action eq 'append' ) {
        my $oldDbFile = $dataSets{$dataSet};
        my ( $oldGranuleHash, $oldFileHandle ) = 
            S4PA::Storage::OpenGranuleDB( $oldDbFile, "r" );
        unless ( defined $oldGranuleHash ) {
            S4P::perish( 12,
                "Failed to open original granule database for dataset, $dataSet." );
        }
        while ( my ($key,$value) = each %$oldGranuleHash ) {
            $granuleHashRef->{$key} = $value
        }
        S4PA::Storage::CloseGranuleDB( $oldGranuleHash, $oldFileHandle );
    }

    open ( DUMP, "$exportFile" ) ||
        S4P::perish( 12, "Failed to open export file: $!" );

    my $granuleCount = 0;
    my $startImport = 0;
    while ( <DUMP> ) {
        my $record = $_;
        chomp $record;

        # empty line implies the end of dataset records 
        last if ( $startImport && $record =~ /^$/ );

        # decode record into granule hash if startImport flag was set
        if ( $startImport ) {
            my $granule;
            my $granuleRec = {};
            foreach ( split /\|/, $record ) {
                my ( $key, $value ) = split /:/, $_;
                if ( $key =~ /file/ ) {
                    $granule = $value;
                } else {
                    $granuleRec->{$granule}{$key} = $value;
                }
            }

            # insert new one from dump file or replace the original one
            # with new content
            $granuleHashRef->{$granule} = $granuleRec->{$granule};
            $granuleCount++;
        }

        # set the startImport flag only if both dataset and version match
        elsif ( $record =~ /Dataset=$dataset,/ && $record =~ /DataVersion=$version,/ ) {
            $startImport = 1;
        }
    }

    S4PA::Storage::CloseGranuleDB( $granuleHashRef, $fileHandle );

    my $targetDbFile;
    if ( $action =~ /replace|append/ ) {
        $targetDbFile = $dataSets{$dataSet};
        move ( $targetDbFile, "$targetDbFile".".bak" ) if ( -f $targetDbFile );
    } else {
        $targetDbFile = $stageDir . "/granule.db." . $dataSet;
    }

    if ( move( $newDbFile, $targetDbFile ) ) {
        if ( $action = 'append' ) {
            S4P::logger( "INFO", "Inserted $granuleCount records ". 
                "into db file $targetDbFile" );
        } else {
            S4P::logger( "INFO", "Created db file $targetDbFile" .
                " with $granuleCount records." );
        }
    } else {
        S4P::perish( 13, "Failed to move the temporary db file: $newDbFile"
            . " to the target db file: $targetDbFile: $!" );
    }
}


##############################################################################
# Subroutine usage:  print usage and die
##############################################################################
sub usage {
  die << "EOF";
Usage: $0 <-r s4pa_root> <-a restore|replace|append> <-f export_file> 
          <-c dataClass>|<-d dataset> [options]
Options are:
        -v label               versions labels, separated by comma.
        -l <staging_dir>       granule.db staging directory, default to './'
EOF
}

