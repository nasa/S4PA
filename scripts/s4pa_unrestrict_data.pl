#!/usr/bin/perl

=head1 NAME

s4pa_unrestrict_data.pl 

=head1 SYNOPSIS

s4pa_unrestrict_data.pl  
B<-s> <dataset> 
B<-b> <begin date>
B<-e> <end date>
[B<-r> <root dir>
B<-m> <mapping file>]

=head1 DESCRIPTION

s4pa_unrestrict_data.pl will unrestrict data for a specifed data range 
and dataset.  For each file listed in granule.db for the specified 
dataset which falls in the specified date range, this script will chmod
the file and its corresponding metadata file to 0644, update the mode 
field for the file in granule.db, and include the file in a PDR to be
used for updating WHOM.

=head1 ARGUMENTS

=over 4

=item B<-s> I<data set>

dataset name for the files to be unrestricted

=item B<-b> I<begin date>

start of the date range--format is 'YYYY-MM-DD'

=item B<-e> I<end date>

end of the date range--format is 'YYYY-MM-DD'


=item B<-r> I<root>

s4pa root--default is current directory.

=item B<-m> I<mapping file>

A file containing the perl hashes %datagroup_X_dataset (data group 
referenced by dataset) ;  if this option is not exercised, the default will be 
's4pa_root/storage/dataset.cfg'

=back


=head1 AUTHOR

John Bonk
M. Hegde

=cut

################################################################################
# $Id: s4pa_unrestrict_data.pl,v 1.1.1.1 2006/03/08 12:31:19 hegde Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use Getopt::Std;
use S4P::TimeTools;
use S4PA::Storage;
use Fcntl;
use Safe;
use S4P::PDR;
use Cwd;
use vars qw($opt_b $opt_e $opt_s $opt_r $opt_m);

# File permissions for unrestricted data
my $MODE = 420;  # decimal for 0644. The file

# Get command line options; begin/end date (-b, -e), and dataset name (-s) are 
#mandatory.
getopts('b:e:r:s:m:') || usage();
usage() unless($opt_b, $opt_e, $opt_s);

# Get the S4PA root directory; by default, it is the current directory. Make
# sure the directory exists.
my $s4paRoot = $opt_r || cwd();
S4P::perish( 1, "S4PA root directory, $s4paRoot, doesn't exist" )
    unless ( -d $s4paRoot );

# Dataset name.     
my $dataset = $opt_s;

# Get the dataset => dataclass mapping file and make sure it exists.
my $mappingFile = $opt_m || $s4paRoot . "/storage/dataset.cfg";
S4P::perish( 1, "Mapping file, $mappingFile, doesn't exist" )
    unless ( -f $mappingFile );
    
# Read the mapping file.
my $cpt = new Safe('CFG');
$cpt->share( '%data_class' );
die "Could not find '$mappingFile': $!" unless ( $cpt->rdo( $mappingFile ) );
S4P::perish( 1, "Data class not found for $dataset in $mappingFile" )
    unless defined $CFG::data_class{$dataset};

my $curDir = cwd();
my $datasetDir = "$s4paRoot/storage/" . $CFG::data_class{$dataset} 
                 . "/$dataset";
S4P::perish( 1, "Directory, $datasetDir, doesn't exist" )
    unless ( -d $datasetDir );

my ( $beg_year, 
     $beg_month, 
     $beg_day ) = S4P::TimeTools::CCSDSa_DateParse( $opt_b . 'T00:00:00Z' );
my ( $end_year, 
     $end_month, 
     $end_day ) = S4P::TimeTools::CCSDSa_DateParse( $opt_e . 'T00:00:00Z' );

# Create a PDR for WHOM publication.
my $orinatingSystem = 'S4PA';
my $now = S4P::TimeTools::CCSDSa_Now;

# Add 30 days worth of seconds to $now to get expiration:
my $expirationDate = S4P::TimeTools::CCSDSa_DateAdd($now, (30 * 24 * 60 * 60));
my $pdr = S4P::PDR::start_pdr( 'originating_system' => $orinatingSystem,
                               'expiration_time' => $expirationDate );
                         
# Open granule.db and read contexts into $granuleInfo:
my ( $granuleInfo,
     $fileHandle ) = S4PA::Storage::OpenGranuleDB( "$datasetDir/granule.db", 
                                                   'rw' );
       
for ( my $year = $beg_year ; $year <= $end_year ; $year++ ) {
    my $beg_doy = ( $year == $beg_year )
                    ? S4P::TimeTools::day_of_year( $beg_year, $beg_month, $beg_day )
                    : 1;
    my $end_doy = ( $year == $end_year ) 
                  ? S4P::TimeTools::day_of_year( $end_year, $end_month, $end_day )
                  : 366;
    for ( my $doy = $beg_doy ; $doy <= $end_doy ; $doy++ ) {
        my $dir = "$datasetDir/data/" . sprintf( "%4.4d/%3.3d", $year, $doy );
        next unless ( -d $dir );
        if ( opendir( DIR, $dir ) ) {
            my @files = grep( !/^\.{1,2}$/, readdir( DIR ) ); 
            closedir( DIR );
            foreach my $file ( @files ) {
                my $path = $dir . '/' . $file;
                unless ( chmod( $MODE, $path ) ) {
                    S4PA::Storage::CloseGranuleDB( $granuleInfo, $fileHandle );
                    S4P::perish( 1, "Failed to unrestrict $path" );
                }
                
                my $granuleRec = $granuleInfo->{$file};
                $granuleRec->{mode} = $MODE;
                $granuleInfo->{$file} = $granuleRec;
                $pdr->add_granule( data_type => $dataset,
                                   files => [ $path ] )
                    if ( $file =~ /\.xml$/ );
            }
        } else {
            S4P::logger( 'ERROR', "Failed to open $dir for reading" );
        }
    }
}
S4PA::Storage::CloseGranuleDB( $granuleInfo, $fileHandle );
my $pdrFile = "$s4paRoot/storage/publish_whom/pending_publish/"
              . "UNRESTRICT_$dataset.$$." . sprintf( "%x", time() ) . ".PDR";

if ( $pdr->total_file_count ) {
    if ( $pdr->write_pdr( $pdrFile ) ) {
        S4P::perish( 1, "Failed to create $pdrFile" );
    } else {
        S4P::logger( 'INFO', "Created $pdrFile" );
    }
}
exit( 0 );

################################################################################
sub usage {
print "
usage: $0 -s <data set> -b <begin date> -e <end date> [-r <s4pa_root>] -m <mapping file>

-s dataset : data set name
-b begin_date : begin date for the files to be unrestricted
-e end_date : end data for the files to be unrestricted
-r s4pa root : defaults to pwd
-m mapping file : maps dataset to datagroups, defaults to 
s4pa_root/storage/dataset.cfg

date format: yyyy-mm-dd

";

die;
}

