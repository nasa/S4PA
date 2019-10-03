#!/usr/bin/perl

=head1 NAME

s4pa_dn2pdr.pl - create PDR from ECS DN

=head1 PROJECT

GSFC DAAC

=head1 SYNOPSIS

s4pa_dn2pdr.pl
B<-o> I<originating_system>
B<-p> I<output_record_name_prefix>
B<-w> I<output_PDR_directory>
B<-e>

=head1 DESCRIPTION

I<s4pa_dn2pdr.pl> detects Distribution Notices (DNs) coming from
STDIN (presumably from a procmail filter setup) and creates
corresponding PDRs, writing them in a user-specified directory.

=head1 OPTIONS

=over 4

=item B<-o> I<originating_system>

String text to insert after "ORIGINATING_SYSTEM=" in output PDR
If not supplied, the default value is "ECS".

=item B<-p> I<output_record_name_prefix>

String text to prepend to the file name of each output PDR record.
(PDR file name = <prefix>.<orderID>_<requestID>.PDR)
If not supplied, the default value is "ECS".

=item B<-w> I<output_PDR_directory>

Full path of the PDR output directory.
If not supplied, the default is the current working directory.

=item B<-e>

Special EDOS processing; during conversions any filename matching
P*01.PDS will be put at the front of the list so metadata filename
will be based on this.

=back

=head1 AUTHOR (of original program s4pm_receive_dn.pl)

Long B. Pham

=head1 ADDRESS

NASA/GSFC, Code 610.2, Greenbelt, MD  20771

=head1 CREATED (original s4pm_receive_dn.pl)

08/11/2000

=head1 Y2K

Certified Correct by Long Pham on DATE UNKNOWN

=head1 AUTHOR (of this derivative program s4pa_dn2pdr.pl)

A. Drake

=head1 CREATED

06/21/2006

8/17/06 Updated to use timestamp and pid to if orderid and requestid are NONE. Randy
9/07/06 BZ 137: Stripped header, tested for failure DN and missing files
9/19/2006 Restored  r1.8 changes to program name in pod, restored year fix
from r1.9, added spaces to improve readability, added required blank line
after pod, added standard header comments after pod. -- E. Seiler
9/20/2006 Completely refactored in the process of adding checksum handling. -- Randy
10/27/06 BZ 298 P*01.PDS files always appear first in output PDR FileGroups if -e specified. -- Randy

=cut

################################################################################
# s4pa_dn2pdr.pl,v 1.13 2006/10/27 18:01:54 barth Exp
# -@@@ S4PA, Version $Name:  $
################################################################################

use Getopt::Std;
use strict;
use S4P::PDR;
use S4P::FileGroup;
use S4P::FileSpec;
use S4P;
use Time::Local;

use vars qw($opt_o $opt_p $opt_w $opt_e);

getopts('o:p:w:e');

# Set output directory from calling parameter
$opt_w ||= '.';
my $wo_dir = $opt_w;

# Set a value for ORIGINATING_SYSTEM in output record
# from calling parameter, or "ECS" if not supplied
$opt_o ||= 'ECS';
my $orig_sys = $opt_o;

# Files seen so far.
my $tot_file_cnt = 0;
# Last (should be only) corresponding value found in DN.
my ($ftp_host, $order_id, $req_id, $dir_id);
# ESDT, split at the dot.
my (@data_type, @data_version);
# List of UR's seen.
my @ur;
# Size, checksum type and value, keyed on <ur>|<filename>.
my (%filesize, %filecktype, %fileckval);
# File lists; hashed on UR, value is reference to list.
my %files;


# Set the prefix for the output PDR record name from
# calling parameter, or use default if not supplied
$opt_p ||= 'ECS';
my $out_prefix = $opt_p;

# Read the whole file via STDIN
my @dn_lines = <>;

# Remove EOL from each line
chomp @dn_lines;

# Remove all leading & trailing spaces
foreach (@dn_lines) {s/^\s+//; s/\s+$//;}

# Remove lines down to first beginning with +++++.
while ($#dn_lines >= 0 and $dn_lines[0] !~ /^\+\+\+\+\+/) {
    shift(@dn_lines);
}
die "Can't find header on DN." if scalar(@dn_lines) == 0;

# See if this is a failure.
die "Failed DN." if scalar(grep(m"The request failed", @dn_lines)) > 0;

# Current values should change before being used in well-formed DN.
my ($currur, $urfile) = ('bogus', 'bogus|bogus');

# Extract data from DN
foreach my $line (@dn_lines) {
    my ($key, $val) = ($line =~ /^([^:]+):\s*(.*)$/);
    next if $key eq '';
    # Strip GRANULE: UR: val.
    ($key, $val) = ($val =~ /^([^:]+):\s*(.*)$/) if $key eq 'GRANULE';
    $ftp_host = $val if $key eq 'FTPHOST';
    $order_id = $val if $key eq 'ORDERID';
    $req_id = $val if $key eq 'REQUESTID';
    $dir_id = $val if $key eq 'FTPDIR';
    push @ur, ($currur = $val) if $key eq 'UR';
    if ($key eq 'ESDT') {
        my ($t, $v) = ($val =~ /(\w+)\.(\d+)/);
        push @data_type, $t;
        push @data_version, $v;
    }
    if ($key eq 'FILENAME') {
        $tot_file_cnt++;
        $urfile = "$currur|$val";
        if ($opt_e and $val =~ /^[EP].*01\.[EP]DS$/) { # EDOS special: put P*01.PDS first.
            unshift @{$files{$currur}}, "$dir_id/$val";
        } else {
            push @{$files{$currur}}, "$dir_id/$val";
        }
    }
    $filesize{$urfile} = $val if $key eq 'FILESIZE';
    $filecktype{$urfile} = $val if $key eq 'FILECKSUMTYPE';
    $fileckval{$urfile} = $val if $key eq 'FILECKSUMVALUE';
}


# Create work order file name and directory
if ($order_id eq 'NONE') { # Use timestamp for orderid if NONE.
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime();
    $order_id = sprintf '%4d%02d%02d%02d%02d%02d',
                   ($year + 1900), ($mon + 1), $mday, $hour, $min, $sec;
}
# Use pid for request id if NONE.
$req_id = $$ if $req_id eq 'NONE';

my $WORK_ORDER = $out_prefix . "." . $order_id . "_" . $req_id . ".PDR";

# Start a new S4P::PDR for output
my $pdr = S4P::PDR::start_pdr();



# Create PDR output

$pdr->originating_system($orig_sys);

die 'No granules.' if scalar(@ur) == 0;
for (my $ur_cnt = 0; $ur_cnt < scalar(@ur); $ur_cnt++) {
    # Add granule info to pdr; get file group pointer.
    my $fg =
            $pdr->add_granule('ur'           => $ur[$ur_cnt],
                              'data_type'    => $data_type[$ur_cnt],
                              'data_version' => $data_version[$ur_cnt],
                              'node_name'    => $ftp_host,
                              'files'        => $files{$ur[$ur_cnt]});
    foreach my $fs (@{$fg->file_specs()}) {
        my $urfile = "$ur[$ur_cnt]|" . $fs->file_id;
        $fs->file_size($filesize{$urfile});
        $fs->{'file_cksum_type'} = $filecktype{$urfile} if defined $filecktype{$urfile};
        $fs->{'file_cksum_value'} = $fileckval{$urfile} if defined $fileckval{$urfile};
    }

}

# Output new S4P::PDR to file
$pdr->write_pdr("$wo_dir/$WORK_ORDER");

# Log distribution notice
print "Finished processing $WORK_ORDER.\n";

