#!/usr/bin/perl

=head1 NAME

s4pa_DNacq.t - test script to do DN acquisition

=head1 SYNOPSIS

Test s4pa_DNacq.pl.

=head1 ARGUMENTS

=over 4

=item B<-d>

Debug flag: keep output file; debug called program.

=back

=head1 AUTHOR

Dr. C. Wrandle Barth, SSAI, NASA/GSFC, Code 610.2, Greenbelt, MD  20771.

=head1 CHANGE LOG

7/17/06 Initial version
7/27/06 Added configurable DN suffix
8/3/06 Changed to test for data in separate directory from DNs
10/27/06 Added test for BZ 298 push P*01.PDS first.

=cut

use strict;
use POSIX;
use S4P;
use S4P::PDR;
use S4P::FileGroup;
use S4P::FileSpec;
use vars qw($opt_d);
use Getopt::Std;
use Test::More 'no_plan';

getopts('d');
my $debug = $opt_d;
# Define path for test files.
my $temppath = '/var/tmp/s4pa_DNacq_test';
my $datapath = 'data';

# Define work order filename for test.
my $myDN = 'MyDN';
my $myDNsuf = 'notifyxyz';
# Define data and metadata filenames.
my $mydata = 'mydata';
my $mycrdata = "$mydata.cr.txt";
my $mymet = "$mydata.met";
my $nodename = 'g0abcde';
my %crcs = ($mydata => 12345, $mycrdata => 54321, $mymet => undef);


# Clean up from previous run.
`rm -fr $temppath` if -d $temppath;
mkdir $temppath or die "Can't make temporary directory $temppath.";
mkdir "$temppath/$datapath" or die "Can't make temporary directory $temppath/$datapath.";




# Create data file
open FH, ">$temppath/$datapath/$mydata" or
		die "Can't create $temppath/$datapath/$mydata.";
print FH "She blinded me with science!\n";
close FH;
my $mydata_size = -s "$temppath/$datapath/$mydata";

# Create construction record file
open FH, ">$temppath/$datapath/$mycrdata" or
		die "Can't create $temppath/$datapath/$mycrdata.";
print FH "I've been workin' on the railroad!\n";
close FH;
my $mycrdata_size = -s "$temppath/$datapath/$mycrdata";

# Create metadata file for extracting information
open FH, ">$temppath/$datapath/$mymet" or
		die "Can't create $temppath/$datapath/$mymet.";
print FH "Looks like metadata\n";
close FH;
my $mymet_size = -s "$temppath/$datapath/$mymet";

# Create DN for this.
open FH, ">$temppath/$myDN.$myDNsuf" or
		die "Can't create $temppath/$myDN.$myDNsuf";

print FH <<"DN_end";
Stuff.
++++++++++
ORDERID: 12345
REQUESTID: 67890
USERSTRING: FTP-Push
FINISHED: 06/12/2006 10:32:57

MEDIATYPE: FtpPush
FTPHOST: $nodename.ecs.nasa.gov
FTPDIR: $temppath/$datapath
MEDIA 1 of 1
MEDIAID:

    GRANULE: UR:10:DsShESDTUR:UR:15:DsShSciServerUR:13:[GSF:DSSDSRV]:23:SC:D5OTVDYN.001
    ESDT: D5OTVDYN.001

        FILENAME: $mydata
        FILESIZE: $mydata_size
        FILECKSUMTYPE: CKSUM
        FILECKSUMVALUE: $crcs{$mydata}

        FILENAME: $mycrdata
        FILESIZE: $mycrdata_size
        FILECKSUMTYPE: MD5
        FILECKSUMVALUE: $crcs{$mycrdata}

        FILENAME: $mymet
        FILESIZE: $mymet_size
DN_end
close FH;

# Create bad DN as well.
open FH, ">$temppath/${myDN}_isbad.$myDNsuf" or
		die "Can't create $temppath/${myDN}_isbad.$myDNsuf";

print FH <<'DNbad_end';
Dear User,

We regret not being able to distribute your requested data using FTP Push at this time.
This might be due to GES DAAC ECS system problems or network problems.  If you wish to contact us about this order, please provide the Tracking Number (<REQUESTID> for subscriptions, <ORDERID> for other types of orders) appearing in this message to the GES DAAC User Services Office.

GES DAAC Help Desk
NASA/Goddard Space Flight Center, Code 902.2 Greenbelt, MD 20771 USA
301-614-5224 [Voice]
1-877-422-1222 [Toll Free Voice]
301-614-5304 [Fax]
Email: help@daac.gsfc.nasa.gov



++++++++++

ORDERID: NONE
REQUESTID: NONE
USERSTRING: ML0ENG3
FINISHED: 09/04/2006 12:35:12

FAILURE
The request failed
MEDIATYPE: FtpPush
FTPHOST: auraraw1.gsfcb.ecs.nasa.gov
FTPDIR: /ftp/private/push/TS1/ECS/DATA/MLS/


None of the requested granules are considered distributed.
The requested granules were:

UR:10:DsShESDTUR:UR:15:DsShSciServerUR:13:[GSF:DSSDSRV]:23:SC:ML0ENG3.001:81420563
DNbad_end
close FH;


# Run the script.
print "===Running s4pa_DNacq.pl===\n";
my $stdouttext = `blib/script/s4pa_DNacq.pl -p $temppath -s $myDNsuf`;
print "$stdouttext\n============\n" if ($stdouttext ne '');

# Check the output.
is($?, 256, 's4pa_DNacq.pl return code includes failure.');
my @outfiles = glob ("$temppath/*.PDR");
is(scalar(@outfiles), 1, "Test for 1 *.PDR file created in $temppath.");
my $outfile = $outfiles[0];
if ($outfile) {
    # Echo PDR to output.
    print "====PDR Output========\n", `cat $outfile`, "============\n";
    my $pdr = S4P::PDR::read_pdr($outfile);
    # Check the PDR-level fields.
    is($pdr->originating_system, "ECS", 'Originating system test.');
    is($pdr->total_file_count, 3, 'Should be 3 files here.');
    my @file_groups = @{ $pdr->file_groups };
    is(scalar @file_groups, 1, 'Should be 1 group.');
    is(scalar $pdr->files('SCIENCE'), 2, 'Should be 2 science files.');
    is(scalar $pdr->files('METADATA'), 1, 'Should be 1 metadata file.');
    my $fg = $file_groups[0];
    # Check the FILE_GROUP-level fields.
    if ($fg) {
        is($fg->data_type, 'D5OTVDYN', 'Type check in group.');
        is($fg->data_version, '001', 'Version check in group.');
        my $node = $fg->node_name;
        is($node, "$nodename.ecs.nasa.gov", 'Nodename check.');
        foreach my $fs ($pdr->file_specs) {
        	# Check the FILE_SPEC fields.
        	my $fn = $fs->file_id;
        	is($fs->directory_id, "$temppath/$datapath", "Directory on $fn test.");
        	my $fsize = $fn eq $mydata ? $mydata_size :
    			($fn eq $mycrdata ? $mycrdata_size :
    			($fn eq $mymet ? $mymet_size : 0));
        	if ($fsize == 0) {
           	    fail("Bad file name $fn");
        	} else {
                is($fs->file_size, $fsize, 'Size of $fn check.');
                is($fs->file_type, $fn eq $mymet ? 'METADATA' : 'SCIENCE', "Type of $fn test.");
                is($fs->{'file_cksum_value'}, $crcs{$fn}, 'CRC correct');
        	}
        }
    }
    my ($pdrname) = reverse split ('/', $outfile);
    my ($pdrprefix) = $pdrname =~ /(.+)\.PDR/;
    ok(-e "$temppath/good/$pdrprefix-$myDN.$myDNsuf", "DN moved to subdirectory.");
    unlink "$outfile" or "Can't erase $outfile for next attempt.";
}
ok (-e "$temppath/bad/${myDN}_isbad.$myDNsuf", "Bad DN ${myDN}_isbad.$myDNsuf moved to subdirectory.");

# Let's test BZ 298.
my $newdata = "P$mydata.01.PDS";
rename "$temppath/$datapath/$mydata", "$temppath/$datapath/$newdata" or die "Can't rename data.";
$crcs{$newdata} = $crcs{$mydata};
$mydata = $newdata;

# Create DN with data second.
open FH, ">$temppath/$myDN.$myDNsuf" or
		die "Can't create $temppath/$myDN.$myDNsuf second time";

print FH <<"DN2_end";
Stuff.
++++++++++
ORDERID: 12345
REQUESTID: 67890
USERSTRING: FTP-Push
FINISHED: 06/12/2006 10:32:57

MEDIATYPE: FtpPush
FTPHOST: $nodename.ecs.nasa.gov
FTPDIR: $temppath/$datapath
MEDIA 1 of 1
MEDIAID:

    GRANULE: UR:10:DsShESDTUR:UR:15:DsShSciServerUR:13:[GSF:DSSDSRV]:23:SC:D5OTVDYN.001
    ESDT: D5OTVDYN.001

        FILENAME: $mycrdata
        FILESIZE: $mycrdata_size
        FILECKSUMTYPE: MD5
        FILECKSUMVALUE: $crcs{$mycrdata}

        FILENAME: $mydata
        FILESIZE: $mydata_size
        FILECKSUMTYPE: CKSUM
        FILECKSUMVALUE: $crcs{$mydata}

        FILENAME: $mymet
        FILESIZE: $mymet_size
DN2_end

# Run the script.
print "===Running s4pa_DNacq.pl for EDOS===\n";
my $stdouttext = `blib/script/s4pa_DNacq.pl -e -p $temppath -s $myDNsuf`;
print "$stdouttext\n============\n" if ($stdouttext ne '');

# Check the output.
is($?, 0, 's4pa_DNacq.pl return code good now.');
my @outfiles = glob ("$temppath/*.PDR");
is(scalar(@outfiles), 1, "Test for 1 *.PDR file created in $temppath.");
my $outfile = $outfiles[0];
if ($outfile) {
    # Echo PDR to output.
    print "====PDR Output========\n", `cat $outfile`, "============\n";
    my $pdr = S4P::PDR::read_pdr($outfile);
    # Check the PDR-level fields.
    is($pdr->originating_system, "ECS", 'Originating system test.');
    is($pdr->total_file_count, 3, 'Should be 3 files here.');
    my @file_groups = @{ $pdr->file_groups };
    is(scalar @file_groups, 1, 'Should be 1 group.');
    is(scalar $pdr->files('SCIENCE'), 2, 'Should be 2 science files.');
    is(scalar $pdr->files('METADATA'), 1, 'Should be 1 metadata file.');
    my $fg = $file_groups[0];
    # Check the FILE_GROUP-level fields.
    if ($fg) {
        is($fg->data_type, 'D5OTVDYN', 'Type check in group.');
        is($fg->data_version, '001', 'Version check in group.');
        my $node = $fg->node_name;
        is($node, "$nodename.ecs.nasa.gov", 'Nodename check.');
        my $first = 1;
        foreach my $fs ($pdr->file_specs) {
        	# Check the FILE_SPEC fields.
        	my $fn = $fs->file_id;
        	is($fn, $mydata, "P*01.PDS moved first") if $first;
        	$first = 0;
        	is($fs->directory_id, "$temppath/$datapath", "Directory on $fn test.");
        	my $fsize = $fn eq $mydata ? $mydata_size :
    			($fn eq $mycrdata ? $mycrdata_size :
    			($fn eq $mymet ? $mymet_size : 0));
        	if ($fsize == 0) {
           	    fail("Bad file name $fn");
        	} else {
                is($fs->file_size, $fsize, 'Size of $fn check.');
                is($fs->file_type, $fn eq $mymet ? 'METADATA' : 'SCIENCE', "Type of $fn test.");
                is($fs->{'file_cksum_value'}, $crcs{$fn}, 'CRC correct');
        	}
        }
    }
    my ($pdrname) = reverse split ('/', $outfile);
    my ($pdrprefix) = $pdrname =~ /(.+)\.PDR/;
    ok(-e "$temppath/good/$pdrprefix-$myDN.$myDNsuf", "DN moved to subdirectory.");
}


# Create bad PAN.
open FH, ">$temppath/BadPan.PAN" or
		die "Can't create $temppath/BadPan.PAN.";
print FH <<"end_bpan";
MESSAGE_TYPE=LONGPAN;
FILE_DIRECTORY=$temppath/$datapath;
FILE_NAME=$mydata;
DISPOSITION="METADATA PREPROCESSING ERROR";
TIME_STAMP=2006-06-28T15:27:24Z;
FILE_DIRECTORY=$temppath/$datapath;
FILE_NAME=$mycrdata;
DISPOSITION="METADATA PREPROCESSING ERROR";
TIME_STAMP=2006-06-28T15:27:24Z;
FILE_DIRECTORY=$temppath/$datapath;
FILE_NAME=$mymet;
DISPOSITION="METADATA PREPROCESSING ERROR";
TIME_STAMP=2006-06-28T15:27:24Z;
end_bpan
close FH;


# Run the script for bad pan.
print "===Running s4pa_DNacq.pl===\n";
my $stdouttext = `blib/script/s4pa_DNacq.pl -p $temppath -s $myDNsuf`;
print "$stdouttext\n============\n" if ($stdouttext ne '');

# Check the output.
isnt($?, 0, 's4pa_DNacq.pl return code.');
my ($outfile) = glob ("$temppath/bad/*.PAN");
ok(defined $outfile, "Test for *.PAN file created in $temppath/bad.");
if ($outfile) {
    # Echo PDR to output.
    print "====PAN Output========\n", `cat $outfile`, "============\n";
}
ok(-e "$temppath/$datapath/$mydata", "Science data unmoved.");
($outfile) = glob ("$temppath/*.PDR");
ok(defined $outfile, "PDR unmoved.");

# Create good PAN
open FH, ">$temppath/ECS.12345_67890.PAN" or
		die "Can't create $temppath/ECS.12345_67890.PAN.";
print FH <<"end_gpan";
MESSAGE_TYPE=SHORTPAN;
DISPOSITION="SUCCESSFUL";
TIME_STAMP=2006-07-03T15:02:43Z;
end_gpan
close FH;

# Run the script for good pan.
print "===Running s4pa_DNacq.pl===\n";
my $stdouttext = `blib/script/s4pa_DNacq.pl -p $temppath -s $myDNsuf`;
print "$stdouttext\n============\n" if ($stdouttext ne '');

# Check the output.
is($?, 0, 's4pa_DNacq.pl return code.');
($outfile) = glob ("$temppath/good/*.PDR");
ok(defined $outfile, "Test for *.PDR file moved to $temppath/good.");
($outfile) = glob ("$temppath/good/*.PAN");
ok(defined $outfile, "Test for *.PAN file moved to $temppath/good.");

if ($outfile) {
    # Echo PAN to output.
    print "====PAN Output========\n", `cat $outfile`, "============\n";
    ok(not (-e "$temppath/$datapath/$mydata"), "Test for deleting data.");
    ok(not (-e "$temppath/$datapath/$mycrdata"), "Test for deleting crdata.");
    ok(not (-e "$temppath/$datapath/$mymet"), "Test for deleting metadata.");
}

# Clean up.
`rm -fr $temppath` if not $debug;




