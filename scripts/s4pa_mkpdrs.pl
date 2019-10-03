#!/usr/bin/perl -w

eval 'exec /usr/bin/perl -w -S $0 ${1+"$@"}'
    if 0; # not running under some shell

my $debug = 0;

=head1 NAME

s4pa_mkpdrs.pl - script for create PDR for dataset relocation

=head1 SYNOPSIS

s4pa_mkpdrs.pl
[B<-d> I<dataset_root_directory>]
[B<-l> I<metadata_file_list>]
[B<-p> I<pdr_staging_directory>]
[B<-n> I<number_of_granule_per_pdr>]
[B<-h> I<node_name>]
[B<-e> I<expiration_period>]
[B<-f> I<pdr_filename_prefix>]
[B<-o> I<originating_system>]
[B<-c>]
[B<-s>]
[B<-v>]

=head1 DESCRIPTION

s4pa_mkpdrs.pl can accept a DATA directory for a particular 
dataset, scan recursively for metadata file, create PDR(s) for
dataset relocation purpose.

=head1 ARGUMENTS

=over 4

=item B<-d>

Dataset root directory. Specify the full path to the dataset's
root directory, usually at: /ftp/data/s4pa/<GROUP>/<dataset>

=item B<-l>

Pathname listing of metadata files.

=item B<-p>

PDR staging directory. Specify the full path where new instance
can ftp poll the PDRs.

=item B<-n>

Optional maximum number of granules per PDR. Default to 50.

=item B<-h>

Optional NODE_NAME in the PDR. Default to the return value of 
S4P::PDR::gethost

=item B<-e>

Optional expiration period. Default to 3 days. This range
will be used to calculate the EXPIRATION_TIME in PDR.

=item B<-f>

Optional PDR filename prefix. Default to 'dsrelocate.<dataset>.'
where <dataset> will be extracted from Dataset root directory
specified in B<-d>.

=item B<-o>

Optional PDR originating_system. Default to 'DATASET_RELOCATE'.

=item B<-c>

Optional flag to remove 'DATA_VERSION' from PDR.
if set, DATA_VERSION line for every file group will be removed.

=item B<-s>

Optional flag to remove 'METADATA' fileSpec from PDR.
if set, METADATA fileSpec for every file group will be removed.

=item B<-v>

Verbose.

=back

=head1 AUTHORS

Dennis Gerasimov
Guang-Dih Lei (guang-dih.lei@gsfc.nasa.gov)

=cut

################################################################################
## s4pa_mkpdrs.pl,v 1.14 2016/09/21 14:46:34 glei Exp
## -@@@ S4PA, Version $Name:  $
#################################################################################
#
# name: s4pa_mkpdrs.pl
# originator DATASET_RELOCATE
# revised: 11/22/2006 glei 
#

use strict;
use File::Basename;
use Getopt::Std;
use S4P::PDR;
use XML::LibXML;
use vars qw($opt_v $opt_p $opt_l $opt_d $opt_h $opt_n $opt_c $opt_e $opt_f $opt_o $opt_s);

getopts('p:d:n:h:e:f:l:o:cvs');
usage() unless ( $opt_d || $opt_l );

##############################################################################
# Assign default values
##############################################################################

my $stageDir = $opt_p ? $opt_p : '.';
$stageDir =~ s/\/$//;
die "Specified staging directory: $stageDir does not exist"
    unless ( -d $stageDir );

my $hostname = $opt_h ? $opt_h : S4P::PDR::gethost;
print "INFO: PDR's NODE_NAME: $hostname\n" if ( $opt_v );

my $maxCount = $opt_n ? $opt_n : 50;
print "INFO: Maximum number of granules in PDR: $maxCount\n" if ( $opt_v );

my $removeVersion = (defined $opt_c) ? 1 : 0;
print "INFO: Data_Version removal flag: $removeVersion\n" if ( $opt_v );

my $expiration = $opt_e ? $opt_e : 3;
print "INFO: PDR's EXPIRATION_TIME: $expiration days\n" if ( $opt_v );

my $origSys = $opt_o || 'DATASET_RELOCATE';
my $pdr = S4P::PDR::start_pdr('originating_system'=> "$origSys",
    'expiration_time' => S4P::PDR::get_exp_time($expiration, 'days') );


##############################################################################
# Find all xml files and make pdrs out of them
##############################################################################

my $granuleCount = 0;
my $pdrCount = 1;

my $pdrPrefix;
if ( $opt_d ) {
    open (FIND, "find $opt_d\|sort|") or die "Cannot execute find: $!";
    my $dataset = basename( dirname( $opt_d ) );
    $pdrPrefix = ( defined $opt_f ) ? $opt_f : "dsrelocate.$dataset";
} elsif ( $opt_l ) {
    open (FIND, "$opt_l") or die "Failed to open listing file $opt_l: $!";
    $pdrPrefix = ( defined $opt_f ) ? $opt_f : "dsrelocate";
}

while (<FIND>) {
    chop;
    /\.xml$/ or next;
    /\.relocate\.xml$/ and next;
    my $met = $_;

    # add granule to the existing pdr
    my $err = 0;
    ($pdr, $err) = append_granule( $met, $hostname, $pdr );
    $granuleCount++ unless ( $err );

    # output pdr to file and re-start a new pdr
    if ( $granuleCount >= $maxCount ) {
        my $pdrFile = "$stageDir/$pdrPrefix.$pdrCount.PDR";
        output_pdr( $pdrFile, $pdr, $removeVersion );
        print "INFO: $pdrFile created for $granuleCount granules\n";

        $granuleCount = 0;
        $pdrCount++;
        $pdr = S4P::PDR::start_pdr('originating_system'=> "$origSys",
            'expiration_time' => S4P::PDR::get_exp_time($expiration, 'days') );
    }
}

##############################################################################
# Output remaining granules to the last PDR.
##############################################################################

my $totalGranule = ( $pdrCount - 1 ) * $maxCount + $granuleCount;
if ( $granuleCount ) {
    my $pdrFile = "$stageDir/$pdrPrefix.$pdrCount.PDR";
    output_pdr( $pdrFile, $pdr, $removeVersion );
    print "INFO: $pdrFile created for $granuleCount granules\n";
} else {
    $pdrCount--;
}

print "INFO: Total $totalGranule Granules in $pdrCount PDR(s).\n";
exit;


##############################################################################
# Subroutine output_pdr:  output pdr contain to file
##############################################################################
sub output_pdr {
    my ( $pdrFile, $pdr, $removeVersion ) = @_;
    open ( PDR, ">$pdrFile" ) or die "Cannot open $pdrFile for output";
    my $pdrtext = $pdr->sprint();
    $pdrtext =~ s/\s*DATA_VERSION=.*;//g if ( $removeVersion );
    print PDR $pdrtext;
    close PDR;
}


##############################################################################
# Subroutine append_granule:  append granule to pdr
##############################################################################
sub append_granule {
    my ( $xmlFile, $host, $pdr ) = @_;
    my $xmlPath = dirname( $xmlFile );

    my $xmlParser = XML::LibXML->new();
    $xmlParser->keep_blanks(0);
    my $dom;
    eval{$dom = $xmlParser->parse_file( $xmlFile )};
    if ($@) {
        print "ERROR: Can't parse metadata file: $@\n";
        return ($pdr, 1);
    }
    my $doc = $dom->documentElement();

    unless ( $doc->nodeName eq 'S4PAGranuleMetaDataFile' ) {
        print "INFO: Skip non-S4PA granule metadata file: $xmlFile\n";
        return($pdr, 1);
    }

    my $esdtNode = $doc->findnodes( '//ShortName' );
    unless ( defined $esdtNode ) {
        print "ERROR: Element ShortName not found in $xmlFile\n";
        return ($pdr, 1);
    }
    my $shortname = $esdtNode->string_value(); 
 
    my $vidNode = $doc->findnodes( '//VersionID' );
    unless ( defined $vidNode ) {
        print "ERROR: Element VersionID not found in $xmlFile\n";
        return ($pdr, 1);
    }
    my $versionid = $vidNode->string_value(); 
 
    my @scienceFiles;
    my $dataFile;
    my $granule;

    # if 'Granulit' element exist, treat it as multiply file granule
    my @granulitNodes = $doc->findnodes( '//Granulit' );
    if ( scalar(@granulitNodes) ) {
        my @granulitList;
        foreach my $granulitNode ( @granulitNodes ) {
            my $fileNode = $granulitNode->findnodes( './/FileName' );
            unless ( defined $fileNode ) {
                print "ERROR: Element FileName not found in $xmlFile\n";
                return ($pdr, 1);
            }
            $dataFile = $fileNode->string_value();
            push @granulitList, $dataFile;
        }

        # Swap P*01.PDS if not first to insure it's listed first and
        # will donate it's name to the metadata file.  BZ 298
        # also swap E*01.EDS, Bug 13667.
        GRANULIT: for ( my $i = 1; $i < @granulitList; $i++ ) {
            my $granulit = $granulitList[$i];
            if ( $granulit =~ /^[EP].*01\.[EP]DS$/ ) {
                ($granulitList[0], $granulitList[$i]) = ($granulit, $granulitList[0]);
                last GRANULIT;
            }
        }

        foreach ( @granulitList ) {
            my $granule = "$xmlPath/" . $_;
            unless ( -f $granule ) {
                print "ERROR: Data file: $granule does not exist\n";
                return ($pdr, 1);
            }
            push @scienceFiles, $granule;
        }

    # otherwise, use 'GranuleID' as the datafile name. 
    } else {
        my $granuleNode = $doc->findnodes( '//GranuleID' );
        unless ( defined $granuleNode ) {
            print "ERROR: Element GranuleID not found in $xmlFile\n";
            return ($pdr, 1);
        }
        $dataFile = $granuleNode->string_value();
        $granule = "$xmlPath/$dataFile";
        push @scienceFiles, $granule;
    }   

    # create new filegroup for the current granule
    my $fileGroup = S4P::FileGroup->new();
    $fileGroup->data_type( $shortname );
    $fileGroup->data_version( $versionid, "%s" );
    $fileGroup->node_name( $host );
    foreach my $file ( @scienceFiles ) {
        $fileGroup->add_file_spec( $file, 'SCIENCE' );
    }

    # search for browse file if it exist
    my $browseNode = $doc->findnodes( '//BrowseFile' );
    if ( scalar @{$browseNode} > 0 ) {
        my $browse = $browseNode->string_value();
        my $browseFile = "$xmlPath/$browse";
        $fileGroup->add_file_spec( $browseFile, 'BROWSE' );
    }

    # search for hdf4map file if it exist
    my $mapNode = $doc->findnodes( '//MapFile' );
    if ( scalar @{$mapNode} > 0 ) {
        my $map = $mapNode->string_value();
        my $mapFile = "$xmlPath/$map";
        $fileGroup->add_file_spec( $mapFile, 'HDF4MAP' );
    }

    # skip metadata fileSpec if opt_s is defined
    unless ($opt_s) {
        $fileGroup->add_file_spec( $xmlFile, 'METADATA' );
    }

    $pdr->add_file_group( $fileGroup );
    return ($pdr, 0);
}


##############################################################################
# Subroutine usage:  print usage and die
##############################################################################
sub usage {
  die << "EOF";
usage: $0 [<-d data_directory>|<-l metadata_listing_file>] [options]
Options are:
        -p staging_dir      PDR staging directory, default to './'.
        -n nnn              number of granule per pdr, default to 50.
        -h <hostname>       NODE_NAME, default to S4P::PDR::gethost value.
        -e nnn              expiration range in days, default to 3.
        -f <pdr_prefix>     prefix of pdr filename, default to 'dsrelocate.<dataset>'
        -o <originating_system>     originating_system, default to 'DATASET RELOCATE'
        -c                  remove DATA_VERSION from pdr
        -s                  remove METADATA from fileGroup
        -v                  Verbose
EOF
}

