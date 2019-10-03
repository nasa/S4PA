#!/usr/bin/perl
=head1 NAME

s4pa_filter_ODL.pl - script to filter data producer metadata from S4PA 
metadata file.

=head1 SYNOPSIS

s4pa_filter_ODL.pl -o <output directory> <metadata filepath>

=head1 ARGUMENTS


=head1 DESCRIPTION

This script extracts and filters the metadata supplied by data producer in
S4PA's metadata file and prints it to a file in the specified directory.
The metadata file suffix is changed to .xml from .odl.

=head1 AUTHOR

F. Fang, ADNET Systems, Inc., NASA/GSFC, Code 610.2, Greenbelt, MD  20771.

=head1 CHANGELOG

04/26/07 Initial version

=cut

################################################################################
# $Id: s4pa_filter_ODL.pl,v 1.5 2008/05/28 17:40:31 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################
use strict;
use XML::LibXML;
use Getopt::Std ;
use S4P;
use S4P::OdlTree;
use S4P::OdlGroup;
use S4P::OdlObject;
use S4PA::Metadata;
use File::Temp;
use File::Basename;
use vars qw( $opt_o );

getopts('o:');
unless (defined($opt_o)) {
   S4P::logger("ERROR","Failure to specify -o <output dir> on command line.");
   exit(2);
}

# Parse xml file
my $xmlParser = XML::LibXML->new();
my $dom = $xmlParser->parse_file( $ARGV[0] );
S4P::perish( 1, "Failed to read work order $ARGV[0]" ) unless $dom;
my $doc = $dom->documentElement();
S4P::perish( 2, "Failed to find document element in $ARGV[0]" ) unless $doc;

# Set the suffix of output filename to .met
my ( $fileName ) = reverse(split /\//, $ARGV[0]);
$fileName = "$opt_o/$fileName";
$fileName =~ s/\.xml$/\.met/i;

my $met = S4PA::Metadata->new( FILE => $ARGV[0] );
S4P::perish( 3, "Failed to parse $ARGV[0]:" . $met->errorMessage() )
    if ( $met->onError() );
my $fileHash = $met->getFileAttributes();

# Get the Producer's metadata and save it to a temporary file.
my $odlContent = $met->getProducersMetadata();
$odlContent =~ s/\s+END\s*$//;
my $fh = File::Temp->new( UNLINK => 0 );
print $fh $odlContent;
my $tmpFile = $fh->filename();
undef $fh;
my $odlTree = S4P::OdlTree->new( FILE => $tmpFile );
unlink( $tmpFile );

my ( $inventoryMetadata ) = $odlTree->search( NAME => 'InventoryMetadata',
    __CASE_INSENSITIVE__ => 1 );
    
my ( $dataFilesGroup ) = ( defined $inventoryMetadata ) 
    ? $inventoryMetadata->search( NAME => 'DataFiles',
        __CASE_INSENSITIVE__ => 1 )
    : 1;
$inventoryMetadata->delete( NAME => 'DataFiles' ) if defined $dataFilesGroup;

my $ckType = $met->getCheckSumType();
$ckType = "CKSUM" if ( $ckType eq 'CRC32' );
$dataFilesGroup = S4P::OdlGroup->new( NAME => 'DataFiles' );
my $limb = S4P::OdlTree->new( NODE => $dataFilesGroup ); 
my $count = 1;
foreach my $file ( keys %$fileHash ) {
    my $fileName = basename( $file );
    my $size = $fileHash->{$file}{SIZE};
    my $cksum = $fileHash->{$file}{CHECKSUMVALUE};
    my $dataFileObject = S4P::OdlObject->new( NAME => 'DataFileContainer',
        CLASS => qq("$count") );
    my $dataFileLimb = S4P::OdlTree->new( NODE => $dataFileObject );
    my $distFileObject = S4P::OdlObject->new( NAME => 'DistributedFileName',
        CLASS => qq("$count"), VALUE => qq("$fileName"),
        TYPE => qq("STRING"), NUM_VAL => 1 );
    my $fileSizeObject =
        S4P::OdlObject->new( NAME => 'FileSize', CLASS => qq("$count"),
            VALUE => $size, TYPE => qq("INTEGER"), NUM_VAL => 1 );
    my $ckTypeObject =
        S4P::OdlObject->new( NAME => 'ChecksumType', CLASS => qq("$count"),
            VALUE => qq("$ckType"), TYPE => qq("STRING"), NUM_VAL => 1 );                
    my $ckValueObject =
        S4P::OdlObject->new( NAME => 'Checksum', CLASS => qq("$count"),
            VALUE => $cksum, TYPE => qq("STRING"), NUM_VAL => 1 );
    my $ckOriginObject =
        S4P::OdlObject->new( NAME => 'ChecksumOrigin',
            CLASS => qq("$count"), VALUE => qq("S4PA"),
            TYPE => qq("STRING"), NUM_VAL => 1 );
    $dataFileLimb->insert( $distFileObject, $fileSizeObject, $ckTypeObject,
        $ckValueObject, $ckOriginObject ); 
    $limb->insert( $dataFileLimb );
    $count++;
}
$inventoryMetadata->insert( $limb );
open ( OUTFILE, ">$fileName" ) || die "Can't open $fileName\n";
print OUTFILE $odlTree->toString(), "\nEND\n";
print "$fileName\n";
close ( OUTFILE ) || S4P::perish( 4, "Failed to close file ($!)" );

exit;
