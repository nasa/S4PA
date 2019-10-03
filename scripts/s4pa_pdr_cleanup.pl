#!/usr/bin/perl -w

=head1 NAME

s4pa_pdr_cleanup.pl - script for cleanup PDR on successful PAN

=head1 SYNOPSIS

s4pa_pdr_cleanup.pl
B<-p> I<pdr_staging_directory>
B<-a> I<pan_staging_directory>
[B<-r>] I<retention_time_in_seconds>
[B<-l>]
[B<-e>] 
[B<-d>]
[B<-v>]

=head1 DESCRIPTION

s4pa_pdr_cleanup.pl scan a specified directory for PAN/PDR. Scan 
PDR to delete each file group if it belongs to a successful PAN.
Otherwise, leave both PAN/PDR un-touched and print out Long PAN message.

=head1 ARGUMENTS

=over 4

=item B<-p>

Staging directory for relocateion PDRs.

=item B<-a>

Staging directory for PANs pushed back from the new server.

=item B<-r>

Cleanup retention time in seconds after successful PAN received.

=item B<-l>

Cleanup local PDR in the PAN directory with DO. prefix.

=item B<-e>

Cleanup EDOS PAN only.

=item B<-d>

Keep datafiles, only cleanup PDR/PAN

=item B<-v>

Verbose.

=back 

=head1 AUTHORS

Guang-Dih Lei (guang-dih.lei@gsfc.nasa.gov)

=cut

################################################################################
# $Id: s4pa_pdr_cleanup.pl,v 1.9 2016/09/27 12:43:05 glei Exp $
# -@@@ S4PA, Version $Name:  $ 
###############################################################################
#
# name: s4pa_pdr_cleanup.pl
# revised: 1/15/2007 glei  
#

use strict;
use Getopt::Std;
use S4P::PAN;
use S4P::PDR;
use File::stat;
use vars qw( $opt_r $opt_p $opt_a $opt_l $opt_e $opt_d $opt_v );

getopts('r:p:a:eldv');
usage() if ( !$opt_p || !$opt_a );

my $retentionTime = $opt_r ? $opt_r : 86400;
my $pdrDir = $opt_p;
my $panDir = $opt_a;

# walk through PANs and submit PDRs for deletion
my $successPan = 0;
my $failPan = 0;

opendir(PANDIR, $panDir) || 
    S4P::perish(1, "Failed to opendir PAN directory $panDir: $!");
my @panList = readdir( PANDIR );
closedir( PANDIR );
my $currentTime = time();

PAN: foreach my $panFile ( @panList ) {

    # only search for PAN match relocation prefix pattern
    next unless $panFile =~ /\.[PE]AN$/;
    S4P::logger( 'INFO', "Found PAN: $panFile") if ( $opt_v );
    my $panPath = "$panDir/" . $panFile;

    # check if PAN file is older than the retention period
    if ( -l $panPath ) {
	my $source = readlink( $panPath );
        unlink( $panPath ) unless( -f $source );
	next PAN;
    }
    my $st = stat( "$panPath" );
    S4P::perish( 1, "Failed to stat file, $panPath" ) unless defined $st;
    next unless ( ($currentTime - $st->ctime()) > $retentionTime );
    S4P::logger( 'INFO', "PAN timestamp is qualified to be cleaned up.") 
        if ( $opt_v );

    # PAN parsing
    my $panText = S4P::read_file($panPath);
    unless ( $panText ) {
        S4P::logger( 'ERROR', "No text in PAN: $panFile");
        next;
    }

    # check if this is an EDOS PAN
    if ( substr($panText, 0, 2) eq "\x0c\x01" ) {
        my $success = (length($panText) > 296);
        # First disposition at offset 296; each following is 257 further.
        for (my $i = 296; $success and ($i < length($panText)); $i += 257) {
                $success = (substr($panText, $i, 1) eq "\x00");
        }
        unless ( $success ) {
            S4P::logger( 'INFO', "Skipped failed EDOS PAN: $panFile");
            $failPan++;
            next;
        }    
    } else {
        next if ( $opt_e );
        my $pan = S4P::PAN::read_pan( $panText );
        my $type = $pan->msg_type();
    
        if ( $type ne "SHORTPAN" ) {
            S4P::logger( 'INFO', "Skipped Long PAN: $panFile") if ( $opt_v );
            $failPan++;
            next;
        }
            
        my $disposition = $pan->disposition();
        $disposition =~ s/\"//g;
        if ( $disposition ne "SUCCESSFUL" ) {
            S4P::logger( 'INFO', "Skipped Short PAN: $panFile, disp: $disposition")
                if ( $opt_v );
            $failPan++;
            next;
        }
    }

    ( my $pdrFile = $panFile ) =~ s/[PE]AN$/PDR/;
    my $pdrPath = "$pdrDir/" . $pdrFile;
    unless ( -f "$pdrPath" ) {
        # search for EDR if PDR not found
        $pdrPath =~ s/PDR$/EDR/;
        unless ( -f "$pdrPath" ) {
            # look for the saved copy of the PDR for a data poller
            # PDR file usually has a 'DO.' prefix.
            $pdrPath = "$pdrDir/DO." . $pdrFile;
            unless ( -f "$pdrPath" ) {
                S4P::logger( 'ERROR', "No matching PDR found for $panFile.");
                next;
            }
        }
    }
    ( my $xfrPath = $pdrPath ) =~ s/DR$/DR.XFR/;
    S4P::logger( 'INFO', "Found associated PDR: $pdrPath") if ( $opt_v );

    my $totalFileCount;
    my $fileCount = 0;

    unless ( $opt_d ) {
        # PDR parsing
        my $pdr = S4P::PDR::read_pdr( $pdrPath );
        $totalFileCount = $pdr->total_file_count;
        S4P::logger( 'INFO', "Total $totalFileCount files in PDR") if ( $opt_v );
        my @files = $pdr->files();
        foreach my $file ( @files ) {
            if ( -f $file ) {
                $fileCount++ if ( unlink $file );
                S4P::logger( 'INFO', "Removed datafile: $file") if ( $opt_v );
                my $xfrFile = $file . ".XFR";
                unlink $xfrFile if ( -f $xfrFile );
            }
        }
        S4P::logger( 'INFO', "$fileCount files removed from PDR") if ( $opt_v );
    }

    if ( ( $fileCount == $totalFileCount ) || $opt_d ) {
        S4P::logger( 'INFO', "Removed PDR: $pdrPath") if ( unlink $pdrPath );
        S4P::logger( 'INFO', "Removed PAN: $panPath") if ( unlink $panPath );
        unlink $xfrPath if ( -f $xfrPath );
        if ( $opt_l ) {
            # locate the associated local PDR in PAN directory
            # it usually has a DO. prefix. 
            ( my $localPdr = $panFile ) =~ s/PAN$/PDR/;
            $localPdr = "$panDir/DO." . $localPdr;
            unless ( -f "$localPdr" ) {
                $localPdr =~ s/PDR$/EDR/;
            }
            S4P::logger( 'INFO', "Removed local PDR: $localPdr") 
                if ( unlink $localPdr );
        }
        $successPan++;
    } else {
        S4P::logger( 'ERROR', "File count mis-matched, skipped $panFile clean up");
    }
}

S4P::logger( 'INFO', "Cleaned up $successPan Short PANs and skipped $failPan Long PAN.");

##############################################################################
# Subroutine usage:  print usage and die
##############################################################################
sub usage {
  die << "EOF";
usage: $0 <-p pdr_directory> <-a pan_directory> 
Options are:
        -r                  Retention period (in seconds)
        -l                  Clean up local PDR in PAN directory
        -e                  Clean up EDOS PAN only
        -v                  Verbose
EOF
}


