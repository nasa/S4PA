#!/usr/bin/perl

=head1 NAME

s4pa_db_export.pl - a script to dump contents of granule.db files in to a text 
file and ship it to a remote host via PostOffice.

=head1 SYNOPSIS

s4pa_db_export.pl -f <Configuration filename>

=head1 ABSTRACT

B<Pseudo code:>
    Obtain dataset->dataclass mappings
    For each dataset/version
        Dump the granule.db file and append the contents to a text file.
    Endfor
    Create a PostOffice work order to ship out the text file.
    End

=head1 DESCRIPTION

s4pa_db_export.pl is the station script for dumping granule DBM file contents in
to a text file and creating a work order for shipping the text file to a remote
location by the PostOffice.

=head1 SEE ALSO

L<s4pa_dbdump.pl>

=cut

################################################################################
# $Id: s4pa_db_export.pl,v 1.10 2018/01/04 16:31:44 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use Getopt::Std;
use File::Temp;
use File::Basename;
use Safe;
use S4P;

use vars qw($opt_f);
getopts('f:');
usage() if (!$opt_f);

# Read the script configuration file
my $cpt = new Safe 'CFG';
$cpt->rdo( $opt_f ) or
    S4P::perish( 1, "Cannot read config file $opt_f in safe mode: ($!)" );

# Read StoreData configuration to get data versions
my $cfgFile = "$CFG::cfg_root/storage/dataset.cfg";
$cpt = new Safe 'DATASET';
$cpt->rdo( "$CFG::cfg_root/storage/dataset.cfg" ) or
    S4P::perish( 1, "Cannot read config file $cfgFile in safe mode: ($!)" );

my $curTime = time();
my $oldTime;
if ( -f "../db_export.time" ) {
    $oldTime = S4P::read_file( "../db_export.time" );
    S4P::perish( 2, "Failed to read ../db_export.time" ) if ( $oldTime eq 0 );
} else {
    $oldTime = $curTime - $CFG::cfg_interval - 1;
}
if ( ( $curTime - $oldTime ) < $CFG::cfg_interval ) {
    S4P::logger( "INFO",
        "Last polling was within the specified time interval" );
    exit( 0 );
}

# Create a temporary file to ouput granule DB dumps
my $fh = File::Temp->new();
my $tmpFile = $fh->filename;
# skip filename with trailing '_z', it cause gzip command think it is already compressed.
while ($tmpFile =~ /_z$/i) {
    $fh = File::Temp->new();
    $tmpFile = $fh->filename;
}

# Loop over all datasets
foreach my $dataset ( keys %DATASET::data_class ) {
    my $storeDataConfig = "$CFG::cfg_root/storage/"
        . "$DATASET::data_class{$dataset}/"
        . "store_$DATASET::data_class{$dataset}/s4pa_store_data.cfg";
    $cpt = new Safe 'STORE';
    $cpt->rdo( $storeDataConfig ) or
        S4P::perish( 1, "Cannot read config file $storeDataConfig in safe mode: ($!)" );    
    # Loop over all versions of the dataset
    foreach my $version (  @{$STORE::cfg_data_version{$dataset}} ) {
        my $granuleDbFile = "$CFG::cfg_root/storage/"
            . "$DATASET::data_class{$dataset}/$dataset"
            . ( $version ne ''  ? ".$version/" : '/' )
            . 'granule.db';
        # Print dataset, version, data class and current time
        print $fh "Dataset=$dataset, DataVersion=$version,",
            "DataClass=$DATASET::data_class{$dataset}, Time="
            . S4P::timestamp() . "\n";
        # Dump the granule DB
        my $str = `s4pa_dbdump.pl -f $granuleDbFile`;
        if ( $? ) {
            S4P::logger( "ERROR", "Failed to dump $granuleDbFile ($!)" );
            unlink( $tmpFile );
            exit ( 1 );
        } else {
            print $fh "$str\n";
            undef $str;
        }        
    }
}

my $outFile = dirname( $tmpFile ) . "/$CFG::cfg_instance."
    . basename( $tmpFile );
rename( $tmpFile, $outFile );

# Compress the file
`gzip -f $outFile`;
if ( $? ) {
    S4P::logger( "ERROR",
        "Failed compress $outFile; returned " . $?>>8 . "($!)" );
    exit( 1 );
} else {
    $outFile .= '.gz';
}
my $id = 'P' . $$ . 'T' . time();
if ( open( FH, ">PUSH.GRANULE_DB_$id.wo" ) ) {
    print FH<<CONTENT;
<FilePacket status="I" destination="$CFG::cfg_destination">
    <FileGroup>
        <File localPath="$outFile" status="I" cleanup="Y"/>
    </FileGroup>
</FilePacket>
CONTENT
    S4P::write_file( "../db_export.time", $curTime );
} else {
    S4P::logger( "ERROR", "Failed to open work order ($!)" );
    unlink( $outFile );
    exit( 1 );
}

##############################################################################
# Subroutine usage:  print usage and die
##############################################################################
sub usage {
  die << "EOF";
Usage: $0 <-f configuration_file>
    configuration file should have the following variables specified:
    \$cfg_instance    -> instance name
    \$cfg_root        -> s4pa root directory
    \$cfg_interval    -> export interval
    \$cfg_destination -> dump file location
EOF
}

