#!/usr/bin/perl

#
# Program Name: s4pa_get_trmm_nc_metadata.pl
#
# SYNOPSIS:
#   s4pa_get_trmm_nc_metadata.pl [-v] <data file> 
#       -v => verbose
#
# RETURN VALUE:  0  for success 
#                1  usage error
#                2  could not find or bad file name
#                3  could not compress data file
#                4  metadata extraction error 
#                5  bad metadata
#                6  could not generate xml
#
# DESCRIPTION:  gets metadata from a TRMM RT netcdf file and writes metadata
#   for file to STDOUT
#
# EXTERNAL CALLS:
#    File::Basename::basename()
#    Getopt::Std::getopts()
#    S4P::TimeTools::CCSDSa_DateParse()
#    XML::LibXML::(various objects and methods)
#
# AUTHORS:  Guang-Dih Lei
#

################################################################################
# $Id: s4pa_get_trmm_nc_metadata.pl,v 1.1 2016/04/12 16:19:05 bdeshong Exp $
# -@@@ S4PA_HDISC, Version $Name:  $
################################################################################

use strict;
use File::Basename;
use Getopt::Std;
use S4P::TimeTools;
use XML::LibXML;

# get command line info (a file in necessary and an verbose flag in optional
our $opt_v;
my $usage = "\nUsage:  $0 [-v]". 
            "\n\t-v ==> verbose \n";
if (!getopts('v'))  {
     print STDERR $usage;
     exit(1);
}
my $file = shift;
if (! $file) {
    print STDERR $usage;
    exit(1);
}

# define %longName (longName indexed by shortName)
my %longNames = 
  ( 'TRMM_3B42RT' => 
    'TRMM (TMPA) L3 3-hour 0.25 x 0.25 degree V7 (TRMM_3B42RT)',
  );

# does file exist
if (!(-s $file)) {
     print STDERR 
      "($0,$$) ERROR: '$file' does not exist or has zero size\n" if $opt_v;
     exit 2;
}

my %metadata;
my $fileName = basename( $file );
my ( $shortName, $parameterType, $year, $month, $day, $hour, $version );

if ( $fileName =~ /^(3B42RT)\.(\d{10})\.(\d)\w*\.nc4/ ) {
    $shortName = "TRMM_$1";
    $version = $3;
    ( $year, $month, $day, $hour ) = ( $2 =~ /(\d\d\d\d)(\d\d)(\d\d)(\d\d)/);
} else {
    print STDERR "($0,$$) ERROR: invalid file name--'$file'\n" if $opt_v;
    exit 2;
}
$metadata{'LongName'} = $longNames{$shortName} if $longNames{$shortName} ;

# get measured parameters
my ($parameters, $temporal) = getParameter( $file );

if ($shortName =~ /^TRMM_3B42/) {
    addMetadata(\%metadata, $file, $shortName, $version, $year, $month, $day, $hour );
}
$metadata{'LongName'} = $longNames{$shortName} if $longNames{$shortName} ;

# create a nodeHash which is use to generate XML:
my $nodeHash = createNodeHash( \%metadata );

# create and write XML:
createXML( $nodeHash, $parameters );

exit 0;


sub addMetadata {
    my ( $metadata, $file, $product, $version, $year, $month, $day, $hour ) = @_;

    $metadata->{'product'} = $product;
    $metadata->{'VersionID'} = $version;
    $metadata->{'Format'} = 'NETCDF';
    $metadata->{'SizeBytesDataGranule'} = 0;

    $metadata->{'ShortName'} = "$product";

    $metadata->{'LongName'} = '';
    my $fileName = basename($file);

    $metadata->{'GranuleID'} = "$fileName";

    # format and add end date to %metadata:
    $metadata->{'RangeBeginningDate'} = $temporal->{'BeginDate'};
    $metadata->{'RangeBeginningTime'} = $temporal->{'BeginTime'};
    $metadata->{'RangeEndingDate'} = $temporal->{'EndDate'};
    $metadata->{'RangeEndingTime'} = $temporal->{'EndTime'};

    if ( $product =~ /^TRMM_3B42/ ) {
        $metadata->{'SouthBoundingCoordinate'} = '-60.0';
        $metadata->{'WestBoundingCoordinate'} = '-180.0';
        $metadata->{'NorthBoundingCoordinate'} = '60.0';
        $metadata->{'EastBoundingCoordinate'} = '180.0';
    }
}

sub createXML {
    # pass a pointer to a  node hash, and a pointer to a metdata hash
    # create and print XML for metadata
    #

    my $nodeHash = shift;
    my $gribMetadata = shift;

    my $xmlParser = XML::LibXML->new();
    $xmlParser->keep_blanks(0);
    my $dom = $xmlParser->parse_string( '<S4PAGranuleMetaDataFile />' );
    my $doc = $dom->documentElement();

    CreateXMLTree( $doc, $nodeHash );

    my $node = addParamsToXML( $parameters );
    $doc->appendChild( $node );
    print $dom->toString(1);
}

sub CreateXMLTree {
    # pass hash containing metadata
    # recursively build tree
    #
    my ( $parent, $tree ) = @_;
    foreach my $key ( sort(keys %$tree) ) {
        my $tag = $key;
        $tag =~ s/^\d+_(.+)/$1/;
        my $node = XML::LibXML::Element->new( $tag );
        $parent->appendChild( $node );
        CreateXMLTree( $node, $tree->{$key} ) if ( ref( $tree->{$key} ) eq 'HASH' );
        $node->appendText( $tree->{$key} ) 
            if ( (ref( $tree->{$key} ) ne 'HASH') && (defined $tree->{$key}) );
    }
}

sub createNodeHash {
    # pass ptr to  metadata hash
    # format node hash
    # return ptr to  node hash
    #
    
    my $ptr = shift;
    my %metadata = %$ptr;
    
    # define and populate node hash:
    my $nodeHash = { '001_CollectionMetaData' => {
                         '001_LongName' => $metadata{'LongName'},
                         '002_ShortName' => $metadata{'ShortName'},
                         '003_VersionID' => $metadata{'VersionID'},
                      },
                      '002_DataGranule' => {
                          '001_GranuleID' => $metadata{'GranuleID'},
                          '002_Format' => $metadata{'Format'},
                          '003_CheckSum' => {
                              '001_CheckSumType' => 'CRC32',
                              '002_CheckSumValue' => 0
                          },
                          '004_SizeBytesDataGranule' => 0,
                          '005_InsertDateTime' => undef,
#                          '006_ProductionDateTime' => 
#                                 $metadata{'ProductionDateTime'}
                      },
                      '003_RangeDateTime' => {
                           '001_RangeEndingTime' => $metadata{'RangeEndingTime'},
                           '002_RangeEndingDate' => $metadata{'RangeEndingDate'},
                           '003_RangeBeginningTime' => 
                                $metadata{'RangeBeginningTime'},
                           '004_RangeBeginningDate' => 
                                $metadata{'RangeBeginningDate'},
                      },
                      '004_SpatialDomainContainer' => {
                            '001_HorizontalSpatialDomainContainer' => {
                                '001_BoundingRectangle' => {
                                    '001_WestBoundingCoordinate' =>
                                        $metadata{'WestBoundingCoordinate'},
                                    '002_NorthBoundingCoordinate' =>
                                        $metadata{'NorthBoundingCoordinate'},
                                    '003_EastBoundingCoordinate' =>
                                        $metadata{'EastBoundingCoordinate'},
                                    '004_SouthBoundingCoordinate' =>
                                        $metadata{'SouthBoundingCoordinate'},
                                }
                            }
                      }
                   };
    return $nodeHash;
}

sub getParameter {
    my $ncFile = shift;

    my @headers = `/opt/anaconda/bin/ncdump -h $ncFile`;
    return undef unless ( @headers );
    my $parameters = {};
    my $temporal = {};
    foreach my $line ( @headers ) {
        chomp( $line );
        next unless ( $line =~ /long_name|units|:End|:Begin/ );
        if ( $line =~ /^\s+:(\w+[Date|Time])\s?=\s?"(.*\d)Z?"\s?;$/ ) {
            $temporal->{$1} = $2;
        }
        if ( $line =~ /^\s+(\w+):long_name\s?=\s?"(.*)"\s?;$/ ) {
            next if ( $1 eq 'lat' || $1 eq 'lon' || $1 eq 'time' || $1 eq 'source' );
            $parameters->{$1}{'long_name'} = $2;
        }
        if ( $line =~ /^\s+(\w+):units\s?=\s?"(.*)"\s?;$/ ) {
            next if ( $1 eq 'lat' || $1 eq 'lon' || $1 eq 'time' || $1 eq 'source' );
            $parameters->{$1}{'units'} = $2;
        }
    }
    return $parameters, $temporal;
}

sub addParamsToXML {
    # pass a pointer to a hash (containing measured parameters)
    # createa 'MeasuredParameters' node and added 'MeasuredParameter'
    # return MeasuredParameters node

    my $parameters = shift;

    my $node = XML::LibXML::Element->new( 'MeasuredParameters' );

    # add parameter type to tree if it exist:
    foreach my $param ( sort keys %{$parameters} ) {
        my $paramNode = XML::LibXML::Element->new( 'MeasuredParameter' );
        my $paramNameNode = XML::LibXML::Element->new( 'ParameterName' );
        my $text = "$param:" . $parameters->{$param}{'long_name'} .
            ' [' . $parameters->{$param}{'units'} . ']';
        $paramNameNode->appendText( $text );
        $paramNode->appendChild( $paramNameNode );
        $node->appendChild( $paramNode );
    }
    return $node;
}

sub endofMonth {

    my ( $year, $month ) = @_;

    my $endDayofMonth = 31;
    if ( ($month eq '04') || ($month eq '06') || ($month eq '09') || ($month eq '11')) {
        $endDayofMonth = 30;
    }
    elsif ( $month eq '02' ) {
        $endDayofMonth = 28 + S4P::TimeTools::is_leapyear($year);
    }
    return $endDayofMonth;
}
