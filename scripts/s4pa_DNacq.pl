#!/usr/bin/perl

=head1 NAME

s4pa_DNacq.pl - ECS DN acquisition

=head1 PROJECT

GES DISC

=head1 SYNOPSIS

s4pa_DNacq.pl
B<-p> I<polling_dir> B<-s> I<DN_suffix>

=head1 DESCRIPTION
I<s4pa_DNacq.pl> scans a directory for DNs, uses s4pa_dn2pdr.pl
to convert them to PDRs, and moves successfully converted DNs in a
"good" subdirectory (inserting the corresponding PDRs name into
the filename for later matching).  Then it scans the same directory for PANs
for completed transfers, refers back to the corresponding DN to
find the files to erase, and moves the PANs to the good subdirectory.
Errors in either half cause the files to be moved to a BAD subdirectory.

=head1 OPTIONS

=over 4

=item B<-p> I<polling_dir>

Directory to poll for DNs and under which to build good and bad
directories.

=item B<-s> I<DN_suffix>

Suffix that identifies DNs.  Default is "notify".

=back

=head1 AUTHOR (of original program s4pm_receive_dn.pl)

Randy Barth

=head1 ADDRESS

NASA/GSFC, Code 610.2, Greenbelt, MD  20771

=head1 CREATED

7/17/06 Created
9/07/06 Added handling for failed DNs in s4pa_dn2pdr.pl.
10/27/06 BZ 298 Specify -e on s4pa_dn2pdr.pl call.

=cut

# $Id: s4pa_DNacq.pl,v 1.6 2006/10/27 18:01:18 barth Exp $
# -@@@ s4pa_DNacq.pl, Version $Name:  $
#########################################################################

use Getopt::Std;
use strict;
use File::Copy;
use S4P::PDR;
use S4P::PAN;
use S4P;

use vars qw($opt_p $opt_s);

getopts('p:s:');

# Set output directory from calling parameter
$opt_p ||= '.';

# Pick up DN suffix.
$opt_s ||= 'notify';

my $results = 0;

chdir $opt_p;
# Make sure we have the subdirectories.
(mkdir 'good' or die "Can't make good directory") unless -e 'good';
(mkdir 'bad'  or die "Can't make bad directory")  unless -e 'bad';

foreach my $dn (glob ('*' . ".$opt_s")) {
    my $target = 'good';
    my $logmsg = `s4pa_dn2pdr.pl -e -w $opt_p <$dn`;
    print $logmsg;
    my $pdrprefix;
    if ($? != 0) {
	$target = 'bad';
	$results = 1;
    } else {
         ($pdrprefix) = $logmsg =~ /\s(\S+)\.PDR/;
         $pdrprefix .= '-';
    }
    move($dn, "$target/$pdrprefix$dn") or die "Can't move $dn to $target";
}

foreach my $panfile (glob ('*.PAN')) {
    my $pan = S4P::PAN->new($panfile);
    my $pangood = $pan->is_successful();
    if ($pangood) {
        my $pdrfile = $panfile;
        $pdrfile =~ s/\.PAN$/.PDR/;
        $pangood = -e $pdrfile;
        if ($pangood) {
            my $pdr = S4P::PDR::read_pdr($pdrfile);
            foreach my $filename ($pdr->files()) {
                $pangood = 0 unless unlink $filename;
            }
            move($pdrfile, $pangood ? 'good' : 'bad') or die "Can't move PDR to subdirectory";
        }
    }
    move($panfile, $pangood ? 'good' : 'bad') or die "Can't move PAN to subdirectory";
    $results = 1 unless $pangood;
}

exit $results;
