#!/usr/bin/perl

=head1 NAME

s4pa_whom_dfa_from_pdr.pl - script to extracts meta from pdr and ftp-push it to WHOM server 

=head1 SYNOPSIS

s4pa_whom_dfa_from_pdr.pl -d <directory_name> -u <Publication URL> -m <mode> -v
[B<-f> I<config_file>]
[B<-w> I<PDR directory/data_class directory>]
[B<-m> I<mode>]
[B<-P> I<publication_url>]
[B<-v>]
[B<-l> I<local directory for publication>]

=head1 DESCRIPTION

s4pa_whom_dfa_from_pdr.pl is script for deleting data to WHOM server.
It takes the PDR directory name as an argument, finds in that directory PDRs
containing granules to be deleted, create an work order for downstream
processing.

=head1 AUTHOR

Irina Gerasimov, SSAI

=cut

################################################################################
# $Id: s4pa_whom_dfa_from_pdr.pl,v 1.8 2016/09/27 12:43:05 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use lib '.';
use Getopt::Std;
use File::Basename;
use XML::LibXML;
use Safe;
use S4P;
use S4P::PDR;
use vars qw($opt_f $opt_w $opt_p $opt_l $opt_v $opt_m);

getopts('f:w:p:l:vd:m:');
usage() if (!$opt_f || !$opt_w);

my @config_vars = qw($FILENAME_PREFIX $LOCAL_DIR %PUBLICATION_URL);
my $cp = new Safe 'CFG';
$cp->share(@config_vars) if (@config_vars);
my $rc = $cp->rdo($opt_f);

my $pdrdir = $opt_w;
my $localdir = $opt_l ? $opt_l : $CFG::LOCAL_DIR;
my $mode = $opt_m ? $opt_m : "OPS";
my $url = $opt_p ? $opt_p : $CFG::PUBLICATION_URL{$mode};
undef $url if ($url eq ".");

print STDERR "pdrdir=\"$pdrdir\" localdir=\"$localdir\" url=\"$url\"\n"
    if ($opt_v);

my $file_prefix = $CFG::FILENAME_PREFIX;

# get list of .xml file for data to be deleted
S4P::perish( 1, "Failed to open directory $pdrdir: $!" )
    unless opendir(PDRDIR, $pdrdir);
my @files = readdir (PDRDIR);
closedir (PDRDIR);

my @pdrfiles;
my @xmlfiles;
foreach my $pdrfile (@files) {
  next if ($pdrfile !~ /PDR$/);
  $pdrfile = "$pdrdir/$pdrfile";
  my $pdr = S4P::PDR::read_pdr($pdrfile);
  if (!$pdr) {
    S4P::logger('ERROR', "Cannot read PDR file $pdrfile: $!");
    next;
  }
  push @pdrfiles, $pdrfile;
  foreach my $fg (@{$pdr->file_groups}) {
    push @xmlfiles, $fg->met_file();
  }
}
my %attributes = ( 'GranuleID' => 1, 'ShortName' => 1, 'InsertDateTime'=>1,
                'RangeBeginningDate'=>1, 'VersionID'=>1);
my @toDFA = ();
foreach my $file (@xmlfiles) {
  unless (open XML, "<$file") {
    S4P::logger('ERROR', "Fail to open xml file $file ($!)");
    next;
  }
  my %granule;
  while (<XML>) {
    chomp;
    next if (! /<(.+)>\s*(.+)\s*<\/.+>/);
    my ($parameter, $value) = ($1,$2);
    next if (!exists $attributes{$parameter});
    $granule{$parameter} = $value;
  }
  close XML;
  $granule{'RangeBeginningDate'} =~ s/-//g;
  $granule{'InsertDateTime'} = (split ' ', $granule{'InsertDateTime'})[0];
  $granule{'InsertDateTime'} =~ s/-//g;
  push @toDFA, $granule{'GranuleID'}."=".$granule{'ShortName'}."=".$granule{'RangeBeginningDate'}."=".$granule{'InsertDateTime'}."=".$granule{'VersionID'};
}

if (!scalar @toDFA) {
  S4P::logger ( 'INFO', "No meta files to DFA is found");
  exit (0);
} 

# S4PA.<MODE>.DELETE.<DATE>. or S4PA.TS2.DELETE.20040811141221
my @date = localtime(time);
my $dfa_file = sprintf("%s/%s.%s.DELETE.%04d%02d%02d%02d%02d%02d",
	$localdir, $file_prefix, $mode, $date[5]+1900, $date[4]+1, 
        $date[3], $date[2], $date[1], $date[0]);
unless (open DFA, ">$dfa_file") {
  S4P::logger ( 'ERROR', "Failed to open DFA file $dfa_file: $!");
  exit(1);
}
foreach (@toDFA) {
  print DFA "$_\n";
}
close DFA;

# Construct work order file name
my $wo = 'WHOMDel';
my $wo_type = 'PUSH';
@date = localtime(time);
my $wo_file = sprintf("%s.%s.T%04d%02d%02d%02d%02d%02d.wo", $wo_type, $wo,
    $date[5]+1900, $date[4]+1, $date[3], $date[2], $date[1], $date[0]);

# create work order
my $pdrCount = scalar( @pdrfiles );
my $status = create_wo( $wo_file, $dfa_file );
unless ( $status ) {
    foreach my $pdr ( @pdrfiles ) {
        unlink $pdr;
        S4P::logger('INFO', "Processed $pdr.");
    }
    S4P::logger( "INFO", "$wo_file created for $pdrCount PDRs" );
}
exit ( $status );


sub create_wo {
    my ( $wo, $dfafile ) = @_;

    my $wo_parser = XML::LibXML->new();
    my $wo_dom = $wo_parser->parse_string('<FilePacket/>');
    my $wo_doc = $wo_dom->documentElement();

    my ($filePacketNode) = $wo_doc->findnodes('/FilePacket');
    $filePacketNode->setAttribute('status', "I");

    my ($protocol, $host, $bla, $dest_dir) = $url =~ /^(\w+):\/\/((\w|\.)+)(\/\S+)/;
    $dest_dir = "/ftp" . $dest_dir;
    S4P::logger ( "INFO", "protocol=$protocol, host=$host, dest_dir=\"".$dest_dir."\"" )
         if $opt_v;
    my $destination = "$protocol\:" . $host . $dest_dir;
    $filePacketNode->setAttribute('destination', $destination);

    my $filegroupNode = XML::LibXML::Element->new('FileGroup');
    my $fileNode = XML::LibXML::Element->new('File');
    $fileNode->setAttribute('localPath', $dfafile);
    $fileNode->setAttribute('status', "I");
    $fileNode->setAttribute('cleanup', "Y");

    $filegroupNode->appendChild($fileNode);
    $wo_doc->appendChild($filegroupNode);

    open (WO, ">$wo") || S4P::perish(2, "Failed to open workorder file $wo: $!");
    print WO $wo_dom->toString(1);
    close WO;

    return(0) ;
}

# Subroutine usage:  print usage and die
sub usage {
  die << "EOF";
usage: $0 <-f config_file> <-w work_order_dir|data_class_dir> [options]
Options are:
        -f                      Configuration file containing WHOM data specs
        -p publication_url      URL with host to publish (set to . to publish to local_dir only)
        -l local_dir            Local directory to which csv files should be placed
        -m mode                 mode of location to publish the metadata
        -v                      Verbose
EOF
}

