#!/usr/bin/perl

=pod

=head1 NAME

s4pa_giovanni.pl -  Giovanni station script for S4PA

=head1 SYNOPSIS

s4pa_giovanni.pl -h | -H | -f conf work_order

=head1 DESCRIPTION

s4pa_giovanni.pl is the station script for Giovanni Preprocessor. Giovanni 
Preprocessor processes S4PA data products for online display and analysis.
When a work order is received, it first determines if the Giovanni processing
applies to the product.  If it does, the script either preprocess the input
data or makes a symlink to the input data, depending on data category.  

=head1 OPTIONS
=over
=item -h          Display usage information
=item -H          Same as -h
=item -f conf     Script configuration file
=item work_order  Work order for the station
=back

=head1 AUTHOR

Jianfu Pan, May 6, 2004

=cut


################################################################################
# $Id: s4pa_giovanni.pl,v 1.2 2008/05/28 17:40:31 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use Getopt::Std;
use File::Basename;
use Safe;
use Fcntl;
use S4P;
use S4P::PDR;
use S4P::FileGroup;
use S4PA::Storage;

my $Usage = "$0 -h | -H | -f conf work_order\n".
            "   -h | -H     Display this usage information\n".
            "   -f conf     Sscript configuration file\n".
            "   work_order  Work order for this station\n";

# Parse command line options
my %opts;
getopts("hHf:", \%opts);
die $Usage if $opts{h} or $opts{H};
my $conf = $opts{f} or die $Usage;
my $wo = shift(@ARGV) or die $Usage;

# Read configuration file
my $cpt = new Safe('CFG');
$cpt->share( '$cfg_giovanni_xref_file',
             '$cfg_giovanni_info_file',
             '%cfg_preprocessor' );
$cpt->rdo($conf) or S4P::perish( 1, "Failed to read conf file $conf");

# Process work order file (work order is a PDR)
my $pdr = S4P::PDR::read_pdr($wo);
my @fileGroupList = @{$pdr->file_groups()};
foreach my $fileGroup ( @fileGroupList ) {
    # Get the dataset/data type.
    my $dataset = $fileGroup->data_type()
                  or S4P::perish( 1, "Dataset not found" );
    # Skip non-Giovanni dataset
    next unless exists $CFG::cfg_preprocessor{$dataset};
    
    # Get the metadata filename for the group.
    my $metadataFile = $fileGroup->met_file();
    S4P::perish( 1, "Metadatafile not found" ) if ( $metadataFile eq '0' ); 

    # Get the science files in the group
    my @dataFileList = $fileGroup->science_files();

    # Determine preprocessor
    my $proc = $CFG::cfg_preprocessor{$dataset};
    S4P::perish( 1, "Failed to find preprocessor for $dataset" )
        unless defined $proc;
    
    my $timeHash = GetDate( $metadataFile );
    S4P::perish( 1, "Failed to get begind/end date" )
        unless ( defined $timeHash->{start} || defined $timeHash->{end} );
    my $giovanniXref = {};
    my $giovanniInfo = {};
    foreach my $inFile (@dataFileList) {
        # Preprocess data if necessary

        # Preprocess file
        my @fileList = `$proc $inFile`;
        chomp( @fileList );
        my $message = "Preprocessing failed ($proc). $!";
        $message .= join( "\n", @fileList ) if ( @fileList );
        S4P::perish( 1, $message ) if $?;
        my $count = 0;
        chomp( @fileList );
        foreach my $outFile ( @fileList ) {
            my $key = 'file_' . $count;
            $giovanniXref->{$inFile}{$key} = $outFile;
        }
        if ( defined $giovanniInfo->{$dataset}{start} ) {
            $giovanniInfo->{$dataset}{start} = $timeHash->{start}
                if ( $giovanniInfo->{$dataset}{start} gt $timeHash->{start} );
        } else {
            $giovanniInfo->{$dataset}{start} = $timeHash->{start};
        }

        if ( defined $giovanniInfo->{$dataset}{end} ) {
            $giovanniInfo->{$dataset}{end} = $timeHash->{end}
                if ( $giovanniInfo->{$dataset}{end} gt $timeHash->{end} );
        } else {
            $giovanniInfo->{$dataset}{end} = $timeHash->{end};
        }
    } 

    # Write xref for tracking
    WriteGiovanniXref( $CFG::cfg_giovanni_xref_file, $giovanniXref ) 
        or S4P::perish( 1,
                        "Failed to updated Giovanni xref file "
                        . $CFG::cfg_giovanni_xref_file );
    # Write info (begin, end times) for updating Giovanni web pages.
    WriteGiovanniInfo( $CFG::cfg_giovanni_info_file, $giovanniInfo )
        or S4P::perish( 1,
                        "Failed to update Giovanni info file "
                        . $CFG::cfg_giovanni_info_file );
}  # END foreach(filegroup)
exit 0;
################################################################################
sub WriteGiovanniInfo 
{
    my ( $infoFile, $giovanniInfo ) = @_;
    my ( $dataHash, 
         $fileHandle ) = S4PA::Storage::OpenGranuleDB( $infoFile, "rw" );
    S4P::perish( 1, "Failed to open Giovanni info file, $infoFile" )
        unless defined $dataHash;

    foreach my $priKey ( keys %$giovanniInfo ) {
        my $dummy = $dataHash->{$priKey};
        if ( defined $dummy ) {
            $dummy->{start} = $giovanniInfo->{$priKey}{start}
                if ( (not defined $dummy->{start}) 
                     || ($dummy->{start} gt $giovanniInfo->{$priKey}{start}) );
            $dummy->{end} = $giovanniInfo->{$priKey}{end}
                if ( (not defined $dummy->{end}) 
                     || ($dummy->{end} lt $giovanniInfo->{$priKey}{end}) );            
        } else {
            $dummy->{start} = $giovanniInfo->{$priKey}{start};
            $dummy->{end} = $giovanniInfo->{$priKey}{end};    
        }
        $dataHash->{$priKey} = $dummy;
    }
    S4PA::Storage::CloseGranuleDB( $dataHash, $fileHandle );
    return 1;    
}
################################################################################
sub WriteGiovanniXref
{
    my ( $dbmFile, $giovanniXref ) = @_;

    my ( $granuleHash, 
         $fileHandle ) = S4PA::Storage::OpenGranuleDB( $dbmFile, "rw" );
    S4P::perish( 1, "Failed to open Giovanni xref file, $dbmFile" )
        unless defined $granuleHash;

    foreach my $priKey ( keys %$giovanniXref ) {
        $granuleHash->{$priKey} = $giovanniXref->{$priKey};
    }
    S4PA::Storage::CloseGranuleDB( $granuleHash, $fileHandle );
    return 1;
}
################################################################################
sub GetDate
{
    my ( $met_file ) = @_;

    my $timeHash = {};    
    # Create a DOM parser and parse the met file.
    my $xml_parser = XML::LibXML->new();
    my $dom = $xml_parser->parse_file( $met_file  );
    
    # On failure to create a DOM from the met file, fail.
    S4P::perish( 1, "Failed to parse $met_file" ) unless ( defined $dom );
    my $doc = $dom->documentElement();
    # Get the date and time nodes
    my ( $dateNode1 ) = $doc->findnodes( './RangeDateTime/RangeBeginningDate' );
    unless ( defined $dateNode1 ) {
        S4P::logger( 'ERROR', "Failed to find <RangeBeginningDate>" );
        return $timeHash;
    }
    my ( $dateNode2 ) = $doc->findnodes( './RangeDateTime/RangeEndingDate' );
    unless ( defined $dateNode2 ) {
        S4P::logger( 'ERROR', "Failed to find <RangeEndingDate>" );
        return $timeHash;
    }    
    
    # Stringify the date/time nodes
    my $date1 = $dateNode1->string_value();
    my $date2 = $dateNode2->string_value();
    
    # Remove leading/trailing white spaces
    $date1 =~ s/^\s+|\s+$//g;
    $date2 =~ s/^\s+|\s+$//g;
    $timeHash->{start} = $date1;
    $timeHash->{end} = $date2;
    return $timeHash;       
}
