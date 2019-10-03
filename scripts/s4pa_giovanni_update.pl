#!/usr/bin/perl

=pod

=head1 NAME

s4pa_giovanni_update.pl -  Giovanni update station script for S4PA

=head1 SYNOPSIS

s4pa_giovanni_update.pl
[B<-h>]
B<-f> I<configuration file>

=head1 DESCRIPTION

Giovanni update station executes updt_Giovanni.pl to update the end date of a data product,
after the Giovanni station receives and processes a new data.

=head1 OPTIONS
=over
=item -h        Display usage information
=item -f        Configuration file
=back

=head1 AUTHOR

Jianfu Pan, August 3, 2004

=cut


################################################################################
# $Id: s4pa_giovanni_update.pl,v 1.2 2008/05/28 17:40:31 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use Getopt::Std;
use Safe;
use S4P;
use S4PA::Storage;

my $Usage = "$0 -h |" .
            "   -f conf     Station config file \n" .
            "   work_order  Work order for this station\n";


# Command line options
my %opts;
getopts("hHf:", \%opts);
die $Usage if $opts{h} or $opts{H};
die "ERROR: specify configuration file (-f)\n$Usage" unless defined $opts{f};

# Read config file
my $cpt = new Safe('CFG');
$cpt->share( '$cfg_giovanni_info_file',
             '%cfg_giovanni_update_script', 
             '%cfg_giovanni_class', 
             '%cfg_giovanni_category');
$cpt->rdo( $opts{f} ) or S4P::perish( 1, "Failed to read conf file $opts{f}" );

my ( $dataHash,
     $fileHandle ) = S4PA::Storage::OpenGranuleDB( $CFG::cfg_giovanni_info_file,
                                                   "r" );
S4P::perish( 1, "Failed to open $CFG::cfg_giovanni_info_file" )
    unless defined $dataHash;
my $info = {};
foreach my $dataset ( keys %$dataHash ) {
    my $class = $CFG::cfg_giovanni_class{$dataset};
    S4P::perish( 1, "Class not defined for $dataset" ) unless defined $class;
    my $category = $CFG::cfg_giovanni_category{$dataset};
    S4P::perish( 1, "Category not defined for $dataset" )
        unless defined $category;
        
    my $key = "$category.$class";
    if ( defined $info->{$key} ) {
        $info->{$key}{start} = $dataHash->{start}
            if ( $info->{$key}{start} gt $dataHash->{$dataset}{start} );
        $info->{$key}{end} = $dataHash->{end}
            if ( defined $info->{$key}{end} 
                 && ($info->{$key}{end} lt $dataHash->{$dataset}{end}) );
    } else {
        $info->{$key}{start} = $dataHash->{$dataset}{start};
        $info->{$key}{end} = $dataHash->{$dataset}{end};
    }
}

foreach my $key ( keys %$info ) {
        S4P::perish( 1, "Failed to find Giovanni update script for $key" )
            unless defined $CFG::cfg_giovanni_update_script{$key};
        my $script = $CFG::cfg_giovanni_update_script{$key};
        $script =~ s/<<start_date>>/$info->{$key}{start}/g;
        $script =~ s/<<end_date>>/$info->{$key}{end}/g;
        #`$script`;
        #if ( $? ) {
        #    S4P::perish( 1, "Failed to execute $script ($!)" );
        #}
        print $script, "\n";
}
S4PA::Storage::CloseGranuleDB( $dataHash, $fileHandle );                                                    
exit 0;
