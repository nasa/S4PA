#!/usr/bin/perl -w

=head1 NAME

s4pa_odl2xml.pl - script to convert ODL metadata (.met file) to XML format

=head1 SYNOPSIS

s4pa_odl2xml.pl
[B<-h>] 
[B<-t> I<template>]
I<odl_metafile>

=head1 ARGUMENTS

=over

=item B<-h>

Displays usage information.

=item B<-t> I<template>

Specifies XML template file of the product used for the ODL-to-XML conversion.

=item I<odl_metafile>

Specifies the ODL metadata file to be converted.  It must use .met as extension.  A list of files could be
specified, but only the first .met file will be converted.

=head1 DESCRIPTION

This script converts a metadata file in ODL format (.met file) into an XML metadata file.

=head1 AUTHOR

J. Pan, SSAI, NASA/GSFC, Greenbelt, MD 20771

=head1 CHANGELOG
09/07/06 J Pan      Added ODL expression and Perl block capability in templates
03/21/06 J Pan      Initial version

=cut

################################################################################
# $Id: s4pa_odl2xml.pl,v 1.9 2008/05/28 17:40:31 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use Getopt::Std;
use Safe;
use XML::LibXML;
use S4P::OdlTree;


my $Usage = "Usage:  $0 -h | -t template odl\n".
            "        -t template     XML template file of the product\n".
            "        odl             Input ODL file\n";

#
# Process command line options
#

my %opts = ();
getopts("ht:", \%opts);

ExitOnError($Usage, 1) if ( $opts{h} );

my $template = $opts{t} or ExitOnError("Template missing\n$Usage", 2);
ExitOnError( "ERROR: file, $opts{t}, doesn't exist", 3 ) unless ( -f $opts{t} );

# --- Find odl file from the list of arguments ---

my $odlfile = "";
foreach my $arg (@ARGV) {
    if ($arg =~ /\.met$/) {
        $odlfile = $arg;
        last;
    }
}

ExitOnError("ODL file (.met) missing\n$Usage", 3) unless $odlfile;


#
# Create ODL tree on the input ODL file
#

my $odl = S4P::OdlTree->new(FILE => $odlfile) or ExitOnError("ERROR: Fail to create ODL tree", 10);


#
# Create XML tree on the XML template
#

my $parser = XML::LibXML->new();
my $dom = $parser->parse_file($template) or ExitOnError("ERROR: Fail to parse template $template", 11);
my $doc = $dom->documentElement() or ExitOnError("ERROR: Fail to find document element in $template", 12);

my ($root) = $doc->findnodes('/S4PAGranuleMetadataTemplate');

#
# Create ODL data source
#

my $odldata = {};
GetOdlData($root, $odl, $odldata);

#
# Traverse template tree and interpret it
#

my ($root_granule) = $root->findnodes('S4PAGranuleMetaDataFile');

ExpandTemplate($root_granule, $odl, $odldata, undef) or 
    ExitOnError("ERROR: Fail to expand template", 20);

# Check data values format and convert them into appropriate format if needed
ConvertData($root_granule);

 print $root_granule->toString(1), "\n";

exit 0;


sub GetOdlData
{
    my ($root, $odl, $odldata) = @_;

    # Find all ODLDATA nodes and process them one by one
    my @datanodes = $root->findnodes('ODLDATA');
    foreach my $node (@datanodes) {
        my $name = $node->getAttribute('Name') or ExitOnError("ERROR: ODLDATA node must have a name", 100);
        my $curr_path = $node->getAttribute('Path') or ExitOnError("ERROR: Path missing in ODLDATA node", 101);
        $odldata->{$name} = ExpandDataNode($node, $curr_path);
    }
}


sub ExpandDataNode
{
    my ($node, $parent_path) = @_;
    my $curr_path = $parent_path;

    my $data = {};

    my @arraynodes = $node->findnodes('Array');
    foreach my $arrnod (@arraynodes) {
        my $array_nm = $arrnod->getAttribute('Name') or ExitOnError("ERROR: Array name missing", 120);
        my $array_path = $arrnod->getAttribute('Path');

        # ---------------------------------------------
        # --- Process Field tags (array elements)   ---
        # --- Map field name (data tag) and ODL tag ---
        # ---------------------------------------------
        my @fieldnodes = $arrnod->findnodes('Field');
        my %map = ();
        foreach my $fnod (@fieldnodes) {
            my $field_nm = $fnod->getAttribute('Name') or ExitOnError("ERROR: Field name missing", 121);
            my $field_tag = $fnod->getAttribute('Path') or ExitOnError("ERROR: Field path missing", 122);
            $map{$field_nm} = $field_tag;
        }

        # --- Retrieve data from ODL ---
        my @array_dat = ();
        my $odlparent = GotoOdlNode($odl, $curr_path);
        my @containers = $odlparent->search(NAME=>$array_path, __CASE_INSENSITIVE__ => 1);
        foreach my $container (@containers) {
           my %data_item = ();
           foreach my $f (keys %map) {
              #-my ($f_odl_node) = $container->search(NAME=>$map{$f});
              #-my $f_val = $f_odl_node->getAttribute('VALUE');
              #-$f_val = $f_odl_node->getAttribute('Value') unless $f_val;
              my $f_val = GetOdlValue($container, $map{$f});
              $data_item{$f} = $f_val;
           }
           push @array_dat, \%data_item;
        }  #---END-OF-foreach(container)---

        # ---------------------------------------------
        # --- Add the Array data to ODL data hash   ---
        # ---------------------------------------------
        
        $data->{$array_nm} = \@array_dat;
    }  #---END-OF-foreach(Array)---

    return $data;
}


sub ExpandTemplate
{
    my ($node, $odl, $datasource, $data) = @_;

    my $safe = Safe->new('CFG');
    $safe->permit_only(qw(sprintf return padany lineseq const rv2sv pushmark list sassign leaveeval));

    foreach my $child ($node->getChildNodes()) {
        if (ref($child) eq "XML::LibXML::Text") {   # === Text node ===
            my $xmltext = $child->data();
            $xmltext =~ s/^\s+|\s*$//g;

            # Delete this child when it doesn't have value in ODL
            if (! $xmltext) {
                $node->removeChild($child);
                next;
            }

            if ($xmltext eq "ODL") {
                # Insert entire ODL metadata text
                $child->setData($odl->toString() . "\nEND\n");
            } elsif ($xmltext =~ /^DATA\.(.+)$/) {
                my $tag = $1;
                $child->setData($data->{$tag});
            } else {
                my @odl_paths = ($xmltext =~ /ODL\.(.+?)(?:[^\.|\w]|$)/sg);
                # Get ODL values and plug them into the text
                foreach my $path (@odl_paths) {
                    my $odltext = GetOdlValue($odl, $path);
                    $xmltext =~ s/ODL\.$path/$odltext/;
                }

                # Process perl blocks marked by _PERLEVAL_[[...]]
                #- $xmltext =~ s/\[\[PERL:(.+?)\]\]/eval($1)/egs;
                $xmltext =~ s/\[\[PERL:(.+?)\]\]/$safe->reval($1)/egs;

                #-my @blocks = ($xmltext =~ /\[\[PERL:(.+?)\]\]/);
                #-foreach my $block (@blocks) {
                #-    my $block_text = $safe->reval($block) or 
                #-       die ("ERROR: Fail to eval $block ($@)\n");
                #-    $xmltext =~ s/\[\[PERL:$block\]\]/$block_text/gs;
                #-}


                # Set new text for the node
                $child->setData($xmltext);
    
            }

            next;    # Move on to next child

        } elsif (ref($child) eq "XML::LibXML::Comment") {   # === Comment node ===
            next;

        } else {                                    # === Element node ===

            # --- Process "ODL" attribute ---
            my $odlattrib = $child->getAttribute('ODL');
            if ($odlattrib and ($odlattrib eq "MISSING")) {
                # Delete this child
                $node->removeChild($child);
                next;    # Move on to next child
            }

            # --- Process "Action" attribute ---
            my $act = $child->getAttribute('Action');
            $act = "" unless $act;
            if ($act =~ /^LOOP:Data=(.+)$/) {
                my $datapath = $1;
                my @datapas = split('\.', $datapath);
                my $ds = $datasource;
                foreach my $pas (@datapas) {
                   $ds = $ds->{$pas};
                }

                # Insert nodes
                foreach my $i (@$ds) {
                   # Copy the node
                   my $child_copy = $child->cloneNode(1);
                   $child_copy->removeAttribute('Action');
                   ExpandTemplate($child_copy, $odl, $datasource, $i);
                   $node->addChild($child_copy);
                }
                $node->removeChild($child);
                next;    # Move on to next child
            }   #---END-OF-Action---
           
            ExpandTemplate($child, $odl, $datasource, $data);
        }
    }

    return 1;
}

sub ConvertData
{
  my ($xml) = @_;

  my $bd_node = ($xml->findnodes('//RangeBeginningDate'))[0];
  if ($bd_node->textContent =~ /(\d{4})-(\d{3})/) {
    my $ymd = DOY_to_YMD($1, $2);
    SetNewText($bd_node, $ymd);
  }
  my $ed_node = ($xml->findnodes('//RangeEndingDate'))[0];
  if ($ed_node->textContent =~ /(\d{4})-(\d{3})/) {
    my $ymd = DOY_to_YMD($1, $2);
    SetNewText($ed_node, $ymd);
  }

  my @eq_nodes = $xml->findnodes('//EquatorCrossingDate');
  return if (!scalar @eq_nodes);
  if ($eq_nodes[0]->textContent =~ /(\d{4})-(\d{3})/) {
    my $ymd = DOY_to_YMD($1, $2);
    SetNewText($eq_nodes[0], $ymd);
  }
}

sub SetNewText {
  my ($node, $new_text) = @_;
  foreach my $child ($node->getChildNodes()) {
    if (ref($child) eq "XML::LibXML::Text") {   # === Text node ===
      $child->setData($new_text);
    }
  }
}

sub DOY_to_YMD {
  my ($yr, $doy) = @_;
  my $leap = 0;
  $leap = 1 if ($yr%4 == 0 && ($yr%100 != 0 || $yr%400 == 0));
  my @mday = ( 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 );
  my $ymd;
  for (my $mo=1; $mo<=12; $mo++) {
    my $mdays = ($leap && $mo == 2) ? 29 : $mday[$mo];
    if ($doy <= $mdays) {
      $ymd = sprintf "%d-%02d-%02d", $yr, $mo, $doy;
      last;
    }
    $doy -= $mdays;
  }
  return $ymd;
}


sub GetOdlValue
{
    my ($odl, $fullpath) = @_;

    return undef unless $fullpath;

    my @paths = split('\.', $fullpath);
    my $path = shift @paths;
    my ($node) = $odl->search(NAME => $path, __CASE_INSENSITIVE__ => 1) or 
                 ExitOnError("ERROR: Path ($path of $fullpath) not found in ODL", 130);
    while ($path = shift @paths) {
        ($node) = $node->search(NAME => $path, __CASE_INSENSITIVE__ => 1) or 
                  ExitOnError("ERROR: Path ($path of $fullpath) not found in ODL", 131);
    }

    my $value = $node->getAttribute('VALUE');
    $value = $node->getAttribute('Value') unless defined $value;
    if (defined $value) {
        $value =~ s/^"|"$//g;
    } else {
        warn( "Can't find the value for $fullpath!" );
    }
    return $value;
}

sub GotoOdlNode
{
    my ($odl, $path) = @_;
    return undef unless $path;

    my @paths = split('\.', $path);
    shift @paths if $paths[0] eq "ODL";
    my $pas = shift @paths;
    my ($node) = $odl->search(NAME => $pas, __CASE_INSENSITIVE__ => 1) or ExitOnError("ERROR: Path ($pas) not found in ODL", 140);
    while ($pas = shift @paths) {
        ($node) = $node->search(NAME => $pas, __CASE_INSENSITIVE__ => 1) or ExitOnError("ERROR: Path ($pas) not found in ODL", 141);
    }
    return $node;
}


sub ExitOnError
{
    my ($msg, $code) = @_;
    warn("$msg\n");
    exit($code);
}
