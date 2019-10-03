#!/usr/bin/perl

=head1 NAME

4pa_relocate_metadata_method.pl - general metadata extraction script.

=head1 SYNOPSIS

s4pa_relocate_metadata_method.pl "extaction_script" filelist

=head1 DESCRIPTION

s4pa_relocate_metadata_method.pl is a wrapper for metadata extraction
script under both normal operation and dataset relocation.
The script will take several arguments. The first one has to be the
normal operation metadata extraction script with options. The rest of
the arguments are the listing of the datafile. 

Example: 
    s4pa_relocate_metadata_method.pl "s4pa_get_metadata.pl -t ../template.xml"
        datafile.dat [metadatafile.xml]

=head1 AUTHORS

Guang-Dih Lei (guang-dih.lei@gsfc.nasa.gov)

=cut

################################################################################
# $Id: s4pa_relocate_metadata_method.pl,v 1.7 2019/05/06 15:48:01 glei Exp $
# -@@@ S4PA, Version $Name:  $
###############################################################################
#
# name: s4pa_relocate_metadata_method.pl
# originator DATASET_RELOCATE
# revised: 11/22/2006 glei
#


use strict;
use XML::LibXML;
use S4P;

my $extractScript = $ARGV[0];

my @fileList;
my $allFiles;
for my $i (1 .. $#ARGV) {
    push @fileList, $ARGV[$i];
    $allFiles .= " $ARGV[$i]";
}

my $extracted = 0;

# print s4pa metadata file to STDOUT if it exist in argument
foreach my $file ( @fileList ) {

    # skip datafiles
    next if ( $file !~ /\.xml$/ );

    # Create an XML DOM parser.
    S4P::logger("INFO", "Parsing xml file: $file");
    my $xmlParser = XML::LibXML->new();
    $xmlParser->keep_blanks(0);

    # Parse the metadata file.
    my $dom = $xmlParser->parse_file( $file );
    my $doc = $dom->documentElement();

    # make sure there is <S4PAGranuleMetaDataFile>
    my @s4paNodes = $doc->findnodes( '//S4PAGranuleMetaDataFile' );
    next unless ( @s4paNodes );
    my $s4paNode = $s4paNodes[0];

    # make sure there is <CollectionMetaData>
    my ( $collectionNode )
        = $doc->getElementsByTagName( 'CollectionMetaData' );
    next unless ( defined $collectionNode );

    # Look for collection URL; if one exists, remove it.
    # it will be added back with the correct URL is necessary
    my ( $urlNode ) = $collectionNode->getElementsByTagName( 'URL' );
    $collectionNode->removeChild( $urlNode ) if ( defined $urlNode );

    # make sure there is <DataGranule>
    my @granuleNodes = $s4paNode->findnodes( './/DataGranule' );
    next unless ( @granuleNodes );
    my $granuleNode = $granuleNodes[0];

    # make sure there is <InsertDateTime>
    my @insertNode = $granuleNode->findnodes( './/InsertDateTime' );

    if ( @insertNode ) {
        print $doc->toString(1);
        $extracted = 1;
        last;
    }
}

# for normal operation, run normal metadata extraction script
unless ( $extracted ) {
    my $metadata = `$extractScript $allFiles`;
    unless ($? == 0) {
        S4P::perish(1, "Failed execute $extractScript: $?");
    }

    $extracted = 1 if ( $metadata =~ /S4PAGranuleMetaDataFile/ );
    print $metadata;
}

if ( $extracted ) {
    S4P::logger("INFO", "Metadata extracted for $fileList[0]");
    exit 0;
}
else {
    S4P::logger("ERROR", "No metadata extracted");
    exit 1;
}

