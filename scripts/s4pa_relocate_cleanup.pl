#!/usr/bin/perl -w

=head1 NAME

s4pa_relocate_cleanup.pl - script for cleanup PDR on successful PAN

=head1 SYNOPSIS

s4pa_relocate_cleanup.pl
B<-r> I<s4pa_root_directory>
B<-p> I<pdr_staging_directory>
B<-a> I<pan_staging_directory>
B<-d> I<dataset>
[B<-i> I<dataVersion>]
[B<-f> I<pdr_filename_prefix>]
[B<-v>]

=head1 DESCRIPTION

s4pa_relocate_cleanup.pl scan a specified PAN directory for dataset
relocation PAN pushed back from the target server. Move the corresponding
PDR to s4pa intra_version_delete station and delete the PAN if it
was a SUCCESSFUL PAN (short pan). Otherwise, leave both PAN/PDR 
un-touched and print out Long PAN message.

=head1 ARGUMENTS

=over 4

=item B<-r>

S4PA stations root directory.

=item B<-p>

Staging directory for relocateion PDRs.

=item B<-a>

Staging directory for PANs pushed back from the new server.

=item B<-d>

Dataset name to be relocated.

=item B<-f>

Optional PDR filename prefix. Default to 'dsrelocate.<dataset>.'
where <dataset> will be extracted from Dataset root directory
specified in B<-d>.

=item B<-i>

Optional dataVersion ID. Do not include this option if dataset
is versionless.

=item B<-v>

Verbose.

=back 

=head1 AUTHORS

Dennis Gerasimov
Guang-Dih Lei (guang-dih.lei@gsfc.nasa.gov)

=cut

################################################################################
# $Id: s4pa_relocate_cleanup.pl,v 1.5 2016/09/27 12:43:05 glei Exp $
# -@@@ S4PA, Version $Name:  $ 
###############################################################################
#
# name: s4pa_relocate_cleanup.pl
# originator DATASET_RELOCATE
# revised: 11/22/2006 glei  
#

use strict;
use Getopt::Std;
use Safe;
use S4P::PAN;
use File::Copy;
use vars qw( $opt_r $opt_p $opt_a $opt_d $opt_v $opt_f $opt_i );

getopts('r:p:a:d:f:i:v');
usage() if ( !$opt_r || !$opt_p || !$opt_a );

die "Specified S4PA root directory: $opt_r does not exist" unless ( -d $opt_r );
die "Specified PDR staging directory: $opt_p does not exist" unless ( -d $opt_p );
die "Specified PAN staging directory: $opt_a does not exist" unless ( -d $opt_a );

my $rootDir = $opt_r;
my $pdrDir = $opt_p;
my $panDir = $opt_a;

my $conf = "$opt_r/storage/dataset.cfg";
die "Dataset to data class configuration file, $conf, doesn't exist"
    unless ( -f $conf );

# Read configuration file
my $cpt = new Safe 'CFG';
$cpt->share('%data_class' );
$cpt->rdo( $conf ) or
    die "Cannot read config file, $conf, in safe mode: $!";

my $dataset = $opt_d;
die "Dataset $dataset not found in configuration file $conf\n"
    unless ( defined $CFG::data_class{$dataset} );
my $dataclass = $CFG::data_class{$dataset};
my $deleteDir = "$rootDir/storage/$dataclass/" .
                "delete_$dataclass/intra_version_pending";
print "INFO: Granule deletion pending directory: $deleteDir\n" if ( $opt_v );

# data version
my $versionID = $opt_i ? ".$opt_i" : "";

# PDR naming convention: $prefix.<dataset>.n.PDR
my $prefix = 'dsrelocate';
my $pdrPrefix = $opt_f ? $opt_f : "$prefix.$dataset$versionID";
print "INFO: PAN/PDR naming perfix: $pdrPrefix\n" if ( $opt_v );

# walk through PANs and submit PDRs for deletion
my $successPan = 0;
my $failPan = 0;
opendir(DIR, $panDir) || die "can't opendir $panDir: $!";
foreach my $panFile ( readdir(DIR) ) {

    # only search for PAN match relocation prefix pattern
    next unless $panFile =~ /$pdrPrefix\.\d+\.PAN$/;
    print "INFO: Found $panFile\n" if ( $opt_v );

    my $panPath = "$panDir/" . $panFile;
    my $panText = S4P::read_file($panPath);
    unless ( $panText ) {
        print "ERROR: No text in PAN: $panFile\n";
        next;
    }
    my $pan = new S4P::PAN($panText);
    my $type = $pan->msg_type();

    if ($type ne "SHORTPAN") {
        print "INFO: Long PAN: $panFile\n";
        $failPan++;
        next;
    }
        
    my $disposition = $pan->disposition();
    $disposition =~ s/\"//g;
    if ( $disposition ne "SUCCESSFUL" ) {
        print "INFO: Short PAN: $panFile, disp: $disposition\n";
        $failPan++;
        next;
    }

    ( my $pdrFile = $panFile ) =~ s/PAN/PDR/;
    my $pdrPath = "$pdrDir/" . $pdrFile;

    unless ( -f "$pdrDir/" . "$pdrFile" ) {
        print "ERROR: No matching PDR found for $panFile.\n";
        next;
    }

    if ( File::Copy::copy( $pdrPath, $deleteDir ) ) {
        unlink $pdrPath;
        unlink $panPath;
        print "INFO: Success in cleanup $panFile\n";
    }
    $successPan++;
}
closedir DIR;

print "INFO: Processed $successPan Short PANs and skipped $failPan Long PAN.\n";

##############################################################################
# Subroutine usage:  print usage and die
##############################################################################
sub usage {
  die << "EOF";
usage: $0 <-r s4pa_root_directory> <-p pdr_directory> <-a pan_directory> 
          <-d dataset> [options]
Options are:
        -f <pdr_prefix>     prefix of pdr filename, default to 'dsrelocate.<dataset>'
        -i <versionID>      specify version label if dataset is not versionless
        -v                  Verbose
EOF
}


