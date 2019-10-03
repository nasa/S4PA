#!/usr/bin/perl

=head1 NAME

s4pa_m2m_search.pl - script for MRI granule searching.

=head1 SYNOPSIS

s4pa_m2m_search.pl <work_order>

=head1 DESCRIPTION

s4pa_m2m_search.pl accepts a work order in machine search station and
proceeds with the search and creates PDR for the subscribe station.
If the work order is already an PDR embeded one, it just writes out
the PDR and exit.

=head1 AUTHOR

M. Hegde, Adnet
Guang-Dih Lei, Adnet

=cut

################################################################################
# $Id: s4pa_m2m_search.pl,v 1.14 2016/09/27 12:43:05 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use XML::LibXML;
use S4P;
use S4PA::GranuleSearch;

# Expect the work order to be the argument.
S4P::perish( 1, "Specify a work order" ) unless defined $ARGV[0];

# Extract job ID; fail if unable to get one.
my $id = ( $ARGV[0] =~ /^DO\.[^.]+\.(.+)/ ) ? $1 : undef;
S4P::perish( 1, "Failed to find the job ID" ) unless defined $id;

# Create an XML parser and DOM
my $xmlParser = XML::LibXML->new();
$xmlParser->keep_blanks(0);
my $dom = $xmlParser->parse_file( $ARGV[0] );
my $doc = $dom->documentElement();

# Output PDR filename
my $pdrId = 'P' . $$ . 'T' . sprintf( "%x", time() );
my $pdrFile;

# For machine order, the contains is already in PDR format,
# just dump it out the exit.
if ( $doc->nodeName() eq 'order') {
    my $pdrText = $doc->textContent();
    if ( $pdrText =~ m/SUBSCRIPTION_ID=(.+);/ ) {
        $pdrFile = "SEARCH.$1.$pdrId.PDR";
        S4P::write_file( $pdrFile, $pdrText );
        exit (0);
    } else {
        S4P::perish( 1, "No SUBSCRIPTION_ID in PDR");
        exit (1);
    }
}

my %arg = ();
foreach $key ( 'dataPath', 'dataset', 'id', 'frequency',
               'startTime', 'endTime', 'overlap', 'exclusive' ) { 
    $arg{$key} = $doc->getAttribute( $key );
}
$arg{action} = 'search';
$pdrFile = "SEARCH.$arg{id}.$pdrId.PDR";

my $search = S4PA::GranuleSearch->new( %arg );
S4P::perish( 1, $search->errorMessage ) if $search->onError;

my $pdr = $search->createPdr();
S4P::perish( 1, $search->errorMessage ) if $search->onError;

if ( $pdr->recount == 0 ) {
    S4P::logger( 'INFO', "No matching granule found" );
} else {
    S4P::logger( 'INFO', "Successfully create PDR: $pdrFile" )
        if $pdr->write_pdr( $pdrFile );
}
exit;
