#!/usr/bin/perl

=head1 NAME

s4pa_republish.pl - Generate work orders to republish data.

=head1 PROJECT

GES DISC

=head1 SYNOPSIS

s4pa_republish.pl
B<-o> I<output file template> [B<-r> I<root of search>]
[B<-e> I<echo mockfile>] [B<-n> I<max files per PDR>]
[<list of files>]

=head1 DESCRIPTION

I<s4pa_republish.pl> scans a directory (-r) or the files and directories in the
command line and their subdirectories for metadata files.  It builds PDRs
pointing to these files and the science files they point at to be placed in the
pending_publish directory (-o) for republishing to ECHO, Mirador, WHOM, or
dotcharts.  To balance the number of PDRs and their size, a count can be given
as -n and new sequentially numbered PDRs will be generated whenever the file
count is exceeded.  The -o can contain a path and partial file name; the
sequence number will be added.  An optional file -e can be specified.  This will
get an ECHO Granule UR written for each filename output.  Once sorted this can
be used as input to s4pa_ECHO_recon.pl as though it were the ECHO dumpfile. This
will report any discrepancies between what is being published and what is on the
RAID.  This will normally be only files received during processing if -r is the
root of the whole dataset.

=head1 OPTIONS

=over 4

=item B<-o> I<output file template>

Path and prefix of output PDR file names.  Sequential number and ".PDR" will be
added.  Example:  -o /vol1/TS2/s4pa/publish_dotchart/pending_publish/repub_ will
create files in /vol1/TS2/s4pa/publish_dotchart/pending_publish/ starting with
repub_0001.PDR and incrementing.  (Exceeding 9999 files does not fail.)

=item B<-r> I<root of search>

Directory for start of search.  All subdirectories will be searched as well.
Depricated; use <list of files> instead.

=item B<-e> I<echo mockfile>

Path and file for optional mock ECHO dumpfile.

=item B<-n> I<max files per PDR>

Maximum number of files before a new PDR is started.  Default is 1000.

=item I<list of files>

List of files and/or directories for start of search.  All subdirectories of any
directory will be searched as well.

=back

=head1 AUTHOR

Randy Barth

=head1 ADDRESS

NASA/GSFC, Code 610.2, Greenbelt, MD  20771

=head1 CREATED

9/14/06
10/5/06 Expanded to allow individual files to be specified.

=cut
# -@@@ S4PA, Version $Name:  $
#########################################################################

use Getopt::Std;
use strict;
use File::Copy;
use File::Spec;
use S4P::PDR;
use S4P::FileGroup;
use S4P;
use XML::LibXML;

use vars qw($opt_r $opt_o $opt_e $opt_n);

getopts('r:o:e:n:');

# Set default.
$opt_n ||= 1000;

die 'Usage: s4pa_republish_ECHO.pl' .
        "\n\t-o <output file path and filename prefix>" .
        "\n\t[-e <echo mockfile>]" .
        "\n\t[-n <max files per PDR>]" .
        "\n\t[<list of files>]" if $opt_o eq '';
# Start with overflow in 0th file so we start with _0001.
our ($pdrctr, $filesinpdr, $pdrname) = (0, $opt_n, '');
# Global PDR we're building.
our $pdr;

open ECHOFILE, ">$opt_e" if $opt_e ne '';

# Process the root and its subdirectories.
process_dir ($opt_r) if $opt_r ne '';

# Process the command line file list.
foreach my $f (@ARGV) {
    my $fullfn = File::Spec->rel2abs($f);
    if (-d $fullfn) {
        process_dir($fullfn)
    } else {
        my ($vol, $dir, $f) = File::Spec->splitpath($fullfn);
        addgranule($dir, $f) if $f =~ /\.xml$/;
    }
}


$pdr->write_pdr($pdrname) if $pdrname ne '';
close ECHOFILE if $opt_e ne '';

0;

###########################################################
# Process the directory passed in and its subdirectories.

sub process_dir {
    my ($dir) = @_;

    die "$dir is not a directory" unless -d $dir;
    opendir DIR, $dir or die "Can't open $dir.";
    my @files = readdir DIR;
    closedir DIR;
    foreach my $f (@files) {
        next if $f eq '.' or $f eq '..';
        if (-d "$dir/$f") { # Recurse on directories.
            process_dir("$dir/$f");
        } else { # Add granule for each XML file.
            addgranule($dir, $f) if $f =~ /\.xml$/;
        }
    }
    return;
}

############################################################
# Process XML file passed in. Read to find science file(s)
# and add file group to this PDR or start another.

sub addgranule {
    our ($pdrctr, $filesinpdr, $pdrname, $pdr);

    my ($dir, $xmlfile) = @_;
    my $xmlParser = XML::LibXML->new();
    my $dom = $xmlParser->parse_file("$dir/$xmlfile");
    my $doc = $dom->documentElement();
    my ($shortname) = textvals($doc, '//CollectionMetaData/ShortName');
    my ($ver) = textvals($doc, '//CollectionMetaData/VersionID');
    my @fn = textvals($doc, '//GranuleID');
    my @granulits = $doc->findnodes('//Granulits');
    # Get all if multifile granule.
    @fn = textvals($doc, '//FileName') if scalar(@granulits) > 0;

    # we will need to record the file type of each file
    my %ft;
    foreach ( @fn ) {
        $ft{"$dir/$_"} = 'SCIENCE';
    }

    # add browse file support
    my $browseNode = $doc->findnodes( '//BrowseFile' );
    if ( scalar @{$browseNode} > 0 ) {
        my $browseFile = $browseNode->string_value();
        push( @fn, $browseFile );
        $ft{"$dir/$browseFile"} = 'BROWSE';
    }

    # add hdf4map file support
    my $mapNode = $doc->findnodes( '//MapFile' );
    if ( scalar @{$mapNode} > 0 ) {
        my $mapFile = $mapNode->string_value();
        push( @fn, $mapFile );
        $ft{"$dir/$mapFile"} = 'HDF4MAP';
    }

    foreach my $f (@fn) { # Prefix path to filename.
        print ECHOFILE "$shortname.$ver:$f\n" if $opt_e;
        $f = "$dir/$f";
    }
    push @fn, "$dir/$xmlfile";
    $ft{"$dir/$xmlfile"} = 'METADATA';
    if ($filesinpdr + scalar(@fn) > $opt_n) { # Start new PDR.
        $pdr->write_pdr($pdrname) if $pdrname ne '';
        $pdrname = $opt_o . sprintf("%04d", ++$pdrctr) . '.PDR';
        $pdr = S4P::PDR->new(file_groups => [], originating_system => 'S4PA');
        my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=localtime();
        # Give an expiration date roughly a year from now.
        $pdr->expiration_time(sprintf '%4d-%02d-01T00:00:00Z', ($year + 1901), ($mon + 1));
        $filesinpdr = 0;
    }
    my $fg = new S4P::FileGroup;
    $fg->data_type($shortname);
    $fg->data_version($ver, '%s');
    foreach my $f (@fn) {
        my $t = $ft{$f};
        $fg->add_file_spec($f, $t);
    }
    # Add this file group.
    my $filegrref = $pdr->file_groups;
    push(@{$filegrref}, $fg);
    $pdr->file_groups($filegrref);
    $filesinpdr += scalar(@fn);
    $pdr->total_file_count($filesinpdr);
}



sub textvals {
    # Return the textContent of each node in the Xpath $xp of doc $d.
    my ($d, $xp) = @_;
    my @nodes = $d->findnodes($xp);
    my @vals;
    foreach my $n (@nodes) {
        push @vals, $n->textContent;
    }
    return @vals;
}
