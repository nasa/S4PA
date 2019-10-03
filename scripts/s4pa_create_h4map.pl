#!/usr/bin/perl 

=head1 NAME

s4pa_create_h4map.pl - wrapper script to generate HDF4 map file 
and update the metadata file.

=head1 SYNOPSIS

s4pa_create_h4map.pl
[B<-e> <map_file_extension>]
[B<-z>]
data_file
metadata_file

=head1 DESCRIPTION

s4pa_create_h4map.pl is a wrapper script to generate HDF4 map file
using 'h4mapwriter' tool. It takes two arguments, first should be
the HDF4 data file and second the XML metadata file. It will update
the granule metadata file with the new <MapFile> entry. 

=head1 ARGUMENTS

=item B<-e>

Optional output map file extension. Default to 'map'

=item B<-z>

Optional gzip the map file.

=item B<-f>

Optional not to insert the full file path information.

=head1 AUTHORS

Guang-Dih Lei (guang-dih.lei@nasa.gov)

=cut

################################################################################
# $Id: s4pa_create_h4map.pl,v 1.2 2011/06/23 16:55:28 glei Exp $
# -@@@ S4PA, Version $Name:  $
###############################################################################
#
# name: s4pa_create_h4map.pl
# revised: 05/20/2011 glei 
#

use strict;
use Getopt::Std;
use File::Basename;
use XML::LibXML;
use Cwd;
use S4P;
use vars qw( $opt_e $opt_z $opt_f );

##############################################################################
# Assign option value
##############################################################################

getopts('e:zf');
my $metaFile = pop @ARGV;
my $dataFile = pop @ARGV;
usage() unless ( $metaFile && $dataFile );
my $mapFileExtension = ( defined $opt_e ) ? $opt_e : 'map';
die "ERROR: xml file extension could destroy existing metadata file"
    if ( $mapFileExtension eq 'xml' );
my $compressFlag = ( defined $opt_z ) ? '-z' : '';
my $nopathFlag = ( defined $opt_f ) ? '-f' : '';

my $workDir = Cwd::cwd();
my $mapWriter = 'h4mapwriter';

# In order to eliminate the full path in map file,
# we need to execute hdf4mapwriter in the data directory
$dataFile = readlink $dataFile if ( -l $dataFile );
my $dataPath = dirname( $dataFile );
my $hdfFile = basename( $dataFile );

# Format the hdf4 map writer command
die "ERROR: Can't change to working data directory: $dataPath"
    unless ( chdir( $dataPath) );
my $mapFile = $dataPath . "/$hdfFile" . ".$mapFileExtension";
my $command = "$mapWriter $nopathFlag $compressFlag $hdfFile $mapFile";
$mapFile .= '.gz' if ( $compressFlag );

# Execute hdf4 map writer
my $runStatus = `$command`;
die "ERROR: Failed to extract HDF4 mapping with $command: $!" if ( $? );

# Read in the existing metadata file
my $xmlParser = XML::LibXML->new();
$xmlParser->keep_blanks( 0 );
my $dom = $xmlParser->parse_file( $metaFile );
my $doc = $dom->documentElement();
my ( $granuleNode ) = $doc->findnodes( 'DataGranule' );
die "ERROR: Can't locate DataGranule node in metadata file: $metaFile"
    unless ( defined $granuleNode );
my ( $mapFileNode ) = $granuleNode->findnodes( 'MapFile' );
$granuleNode->removeChild( $mapFileNode ) if ( defined $mapFileNode );

$mapFileNode = XML::LibXML::Element->new( 'MapFile' );
$mapFileNode->appendText( basename($mapFile) );
# Try to locate the best place for 'MapFile' node
if ( my ( $sizeNode ) = $granuleNode->findnodes( 'SizeBytesDataGranule' ) ) {
    # It should be right before SizeBytesDataGranule if available
    $granuleNode->insertBefore( $mapFileNode, $sizeNode );
} elsif ( my ( $browseNode ) = $granuleNode->findnodes( 'BrowseFile' ) ) {
    # or it can be right after BrowseFile
    $granuleNode->insertAfter( $mapFileNode, $browseNode );
} elsif ( my ( $checksumNode ) = $granuleNode->findnodes( 'CheckSum' ) ) {
    # then, try CheckSum, Format, LocalGranuleID, and GranuleID in the order
    $granuleNode->insertAfter( $mapFileNode, $checksumNode );
} elsif ( my ( $formatNode ) = $granuleNode->findnodes( 'Format' ) ) {
    $granuleNode->insertAfter( $mapFileNode, $formatNode );
} elsif ( my ( $LocalIdNode ) = $granuleNode->findnodes( 'LocalGranuleID' ) ) {
    $granuleNode->insertAfter( $mapFileNode, $LocalIdNode );
} elsif ( my ( $GranuleIdNode ) = $granuleNode->findnodes( 'GranuleID' ) ) {
    $granuleNode->insertAfter( $mapFileNode, $GranuleIdNode );
} else {
    # metadata does not even have GranuleID node
    die "ERROR: Metadata file missing key <DataGranule> components";
}
unless ( S4P::write_file( $metaFile, $dom->toString(1) ) ) {
    die "ERROR: Can't update metadata file: $metaFile";
}

# All done. Return the map file pathname.
print "$mapFile\n";
exit;

##############################################################################
# Subroutine usage:  print usage and die
##############################################################################
sub usage {
  die << "EOF";
Usage: $0 [options] <data_file> <metadata_file>
Options are:
        -e                     map file extension, default to 'map'
        -z                     compress with gzip.
EOF
}

