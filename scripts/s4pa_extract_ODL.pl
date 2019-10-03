#!/usr/bin/perl
=head1 NAME

s4pa_extract_ODL.pl - script to extract data producer metadata from S4PA 
metadata file.

=head1 SYNOPSIS

s4pa_extract_ODL.pl -o <output directory> <metadata filepath>

=head1 ARGUMENTS


=head1 DESCRIPTION

This script extracts the metadata supplied by data producer in S4PA's metadata
file and prints it to a file in the specified directory. The metadata file
suffix is changed to .met from .xml.

=head1 AUTHOR

L. Fenichel, SSAI, NASA/GSFC, Code 610.2, Greenbelt, MD  20771.

=head1 CHANGELOG

02/15/06 Initial version

=cut

################################################################################
# $Id: s4pa_extract_ODL.pl,v 1.4 2008/05/28 17:40:31 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################
use strict;
use XML::LibXML;
use Getopt::Std ;
use S4P;
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

# Get the Producer's metadata and save it to a file.
my ( $node ) = $doc->findnodes( '//ProducersMetaData' );
S4P::perish( 3, "Failed to find 'ProducersMetaData'" ) unless $node;
my $str = $node->textContent();
$str =~ s/^\s+|\s+$//g;
open ( OUTFILE, ">$fileName" ) || die "Can't open $fileName\n";
print OUTFILE "$str\n";
print "$fileName\n";
close ( OUTFILE ) || S4P::perish( 4, "Failed to close file ($!)" );
exit(0);
