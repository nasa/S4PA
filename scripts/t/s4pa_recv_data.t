#!/usr/bin/perl

=head1 NAME

s4pa_recv_data.t - test script to do EDOS PDRs and PANs

=head1 SYNOPSIS

Test s4pa_recv_data.pl, PDR.pm, and PAN.pm features to handle EDOS PDRs and PANs.

=head1 ARGUMENTS

=over 4

=item B<-d>

Debug flag: keep output file.

=back

=head1 AUTHOR

Dr. C. Wrandle Barth, ADNET, NASA/GSFC, Code 610.2, Greenbelt, MD  20771.

=head1 CHANGE LOG

11/8/06

=cut

use strict;
use POSIX;
use S4P;
use S4P::PDR;
use S4P::PAN;
use S4P::FileGroup;
use S4P::FileSpec;
use S4PA::Storage;
use vars qw($opt_d);
use Getopt::Std;
use Test::More 'no_plan';

getopts('d');
my $debug = '-d' if $opt_d;
# Define path for test files.
my $temppath = '/var/tmp/s4pa_edospan_test';

# Clean up from previous run.
`rm -fr $temppath` if -e $temppath;
mkdir $temppath or die "Can't make temporary directory $temppath.";

# Define path for PAN to be dropped on discette.
my $ftppushpath = 'private/s4pa/push';
my $ftppullpath = 'private/s4pa';
my $mydata = 'PtestEDOSpanAAAAAAAAAA06270084704301.PDS';
my $mymetadata = 'PtestEDOSpanAAAAAAAAAA06270084704301.met';
# Define work order filename for test.
my $woname = 'DO.Test.EDOSPAN.PDR';
my $cfgname = 'EDOSPAN.cfg';

# Create data file
open FH, ">/ftp/$ftppullpath/$mydata" or
		die "Can't create /ftp/$ftppullpath/$mydata.";
print FH "ScienceData=HeresTheData\n";
close FH;
my $mydata_size = -s "/ftp/$ftppullpath/$mydata";

# Create metadata file
open FH, ">/ftp/$ftppullpath/$mymetadata" or
		die "Can't create /ftp/$ftppullpath//$mymetadata.";
# Create content metadata extractor should return and use cat for extractor.
print FH <<endmeta;
RangeBeginningDate = 2006-09-27
RangeBeginningTime = 08:47:20Z
RangeEndingDate = 2006-09-27
RangeEndingTmie = 08:47:50Z;
endmeta
close FH;
my $mymetadata_size = -s "/ftp/$ftppullpath/$mymetadata";

my $agglength = $mydata_size + $mymetadata_size;

# Create work order for station.
open FH, ">$temppath/$woname" or
		die "Can't create $temppath/$woname.";

my $groundmsgheader = '09000106001f4a7bb727800000ccf4820500045d00000000';
my $twolabels = '00000Z000001    1073000000000000    1053';
my $wo = <<"wo_end";
ORIGINATING_SYSTEM = S4PA;
CONSUMER_SYSTEM = discette.gsfc.nasa.gov;
DAN_SEQ_NO = 433031;
PRODUCT_NAME = PDS;
MISSION = AURA;
TOTAL_FILE_COUNT = 0002;
AGGREGATE_LENGTH = $agglength;
EXPIRATION_TIME = 999-99-99T99:99:99Z;
OBJECT = FILE_GROUP;
     DATA_SET_ID = P2041840AAAAAAAAAAAAAA06270084704300;
     DATA_TYPE = OML0V;
     DESCRIPTOR = NOT USED;
     DATA_VERSION = 00;
     NODE_NAME = discette.gsfc.nasa.gov;
     OBJECT = FILE_SPEC;
          DIRECTORY_ID = $ftppullpath;
          FILE_ID = $mymetadata;
          FILE_TYPE = METADATA;
          FILE_SIZE = $mymetadata_size;
     END_OBJECT = FILE_SPEC;
     OBJECT = FILE_SPEC;
          DIRECTORY_ID = $ftppullpath;
          FILE_ID = $mydata;
          FILE_TYPE = DATA;
          FILE_SIZE = $mydata_size;
     END_OBJECT = FILE_SPEC;
     BEGINNING_DATE/TIME = 2006-09-27T08:47:20Z;
     ENDING_DATE/TIME = 2006-09-27T08:47:50Z;
END_OBJECT = FILE_GROUP;
wo_end
# EDOS PDRs really have 0c0a as new line (Windows style).
$wo =~ s/\x0a/\x0c\x0a/sg;
print FH pack('H48', $groundmsgheader) . $twolabels . $wo;
close FH;

# Create cfg file
open FH, ">$temppath/s4pa_recv_data.cfg" or
		die "Can't create $temppath/s4pa_recv_data.cfg.";
print FH <<'endcfg';
%cfg_metadata_methods = ("OML0V" => "cat");
%cfg_pan_destination = ("S4PA" => {
                                     "dir" => "/ftp/private/s4pa/push",
                                     "host" => "discette.gsfc.nasa.gov",
                                     "notify" => "rbarth\\@pop600.gsfc.nasa.gov"
                                   }
                       );
%cfg_protocol  = (
                  "discette.gsfc.nasa.gov" => "FTP",
                );
endcfg
close FH;

# Create station.cfg

open FH, ">$temppath/station.cfg" or die "Can't create $temppath/station.cfg.";
print FH <<"statend";
\$cfg_root  =  "$temppath";
\%cfg_downstream  = (
                    "PUSH" => [
                                "postoffice"
                              ],
                    "STORE_OML0V" => [
                                          "storeage"
                                        ]
                  );
statend
close FH;
mkdir "$temppath/postoffice" or die "Can't make postoffice subdirectory.";
mkdir "$temppath/storage" or die "Can't make storage subdirectory.";



my $pdr = S4P::PDR::read_pdr("$temppath/$woname");
ok($pdr->is_edos, 'Recognized header.');
is($pdr->groundmsghdr, $groundmsgheader, 'Saved it.');
is($pdr->dan_seq_no, 433031, 'Kept DAN_SEQ_NO, too.');
my $pan = S4P::PAN->new($pdr);
ok($pan->is_edos, 'Captured header from PDR.');
is($pan->groundmsghdr, $groundmsgheader, 'GMH right');
$pdr->groundmsghdr('F00');
is($pdr->groundmsghdr, 'F00', 'Can change PDR header.');
$pdr->dan_seq_no(1234);
is($pdr->dan_seq_no, 1234, 'Can change DAN_SEQ_NO.');
$pan->edos_header($pdr);
is($pan->groundmsghdr, 'F00', 'Can change PAN header.');


# Now run recv_data.
my $stdouttext = `perl $debug -I @INC[0] ~/S4PA/blib/script/s4pa_recv_data.pl -f $temppath/s4pa_recv_data.cfg -s $temppath/station.cfg -p $temppath $temppath/$woname`;
print "$stdouttext\n============\n" if ($stdouttext ne '');

# Check the output.
isnt($?, 0, 's4pa_create_DN.pl return code due to no space.');
my ($outfile) = glob ("$temppath/*.PAN");
ok(defined $outfile, "Test for *.PAN file created in $temppath.");
if ($outfile) {
    open FH, "<$outfile";
    my $holdnl = $/;
    undef $/;
    my $binpan = <FH>;
    close FH;
    $/ = $holdnl;
    my $hexpan = unpack('H9999', $binpan);
    print "$hexpan\n===========\n";
    is(substr($hexpan, 0, 2), '0c', 'PAN flag in GMH.');
    is(substr($hexpan, 4, 4), '0601', 'Source/destination in GMH');
    is(substr($hexpan, 24, 16), substr($groundmsgheader, 24, 16), 'Match GMH items 7-11');
    is(substr($hexpan, 48, 2), '0c', 'PAN flag in PAN proper.');
    is(unpack('N', pack('H8', '00' . substr($hexpan, 50, 6))), length($binpan), 'Length field test.');
    is(substr($hexpan, 56, 8), '00000000', 'Zeros.');
    is(unpack('N', substr($binpan, 32, 4)), 433031, 'Copied seq no here.');
    is(substr($hexpan, 72, 8), '00000002', 'Two files');
    is(substr($binpan, 40, length($ftppullpath)), $ftppullpath, 'Directory.');
    is(substr($binpan, 256, 40), $mymetadata, 'Metadata file');
    is(substr($hexpan, 592, 2), '08', 'Disposition');
    is(substr($binpan, 297, length($ftppullpath)), $ftppullpath, 'Directory.');
    is(substr($binpan, 513, 40), $mydata, 'Data file');
    is(substr($hexpan, 1106, 2), '08', 'Disposition');
    unlink($outfile);
}
###################
# Create bad work order for station--file count wrong.
open FH, ">$temppath/$woname" or
		die "Can't create $temppath/$woname.";

$wo = pack('H48', $groundmsgheader) . $twolabels . <<"wo1b_end";
ORIGINATING_SYSTEM = S4PA;
CONSUMER_SYSTEM = discette.gsfc.nasa.gov;
DAN_SEQ_NO = 433031;
PRODUCT_NAME = PDS;
MISSION = AURA;
TOTAL_FILE_COUNT = 0003;
AGGREGATE_LENGTH = $agglength;
EXPIRATION_TIME = 999-99-99T99:99:99Z;
OBJECT = FILE_GROUP;
     DATA_SET_ID = P2041840AAAAAAAAAAAAAA06270084704300;
     DATA_TYPE = OML0V;
     DESCRIPTOR = NOT USED;
     DATA_VERSION = 00;
     NODE_NAME = discette.gsfc.nasa.gov;
     OBJECT = FILE_SPEC;
          DIRECTORY_ID = $ftppullpath;
          FILE_ID = $mymetadata;
          FILE_TYPE = METADATA;
          FILE_SIZE = $mymetadata_size;
     END_OBJECT = FILE_SPEC;
     OBJECT = FILE_SPEC;
          DIRECTORY_ID = $ftppullpath;
          FILE_ID = $mydata;
          FILE_TYPE = DATA;
          FILE_SIZE = $mydata_size;
     END_OBJECT = FILE_SPEC;
     BEGINNING_DATE/TIME = 2006-09-27T08:47:20Z;
     ENDING_DATE/TIME = 2006-09-27T08:47:50Z;
END_OBJECT = FILE_GROUP;
wo1b_end

print FH $wo;
close FH;

# Now run recv_data.
$stdouttext = `perl $debug -I @INC[0] ~/S4PA/blib/script/s4pa_recv_data.pl -f $temppath/s4pa_recv_data.cfg -s $temppath/station.cfg -p $temppath $temppath/$woname`;
print "$stdouttext\n============\n" if ($stdouttext ne '');

# Check the output.
isnt($?, 0, 's4pa_create_DN.pl return code due to bad count.');
($outfile) = glob ("$temppath/*.PAN");
ok(not (defined $outfile), "Test for no *.PAN file created in $temppath.");
($outfile) = glob ("$temppath/*.PDRD");
ok(defined $outfile, "Test for *.PDRD file created in $temppath.");
if ($outfile) {
    open FH, "<$outfile";
    my $holdnl = $/;
    undef $/;
    my $pdrd = <FH>;
    close FH;
    print "$pdrd\n===========\n";
    unlink $outfile;
}


############################
# Create new work order for station that isn't EDOS.
open FH, ">$temppath/$woname" or
		die "Can't create $temppath/$woname.";

$wo = <<"wo2_end";
ORIGINATING_SYSTEM = S4PA;
CONSUMER_SYSTEM = discette.gsfc.nasa.gov;
DAN_SEQ_NO = 433031;
PRODUCT_NAME = PDS;
MISSION = AURA;
TOTAL_FILE_COUNT = 0002;
AGGREGATE_LENGTH = $agglength;
EXPIRATION_TIME = 999-99-99T99:99:99Z;
OBJECT = FILE_GROUP;
     DATA_SET_ID = P2041840AAAAAAAAAAAAAA06270084704300;
     DATA_TYPE = OML0V;
     DESCRIPTOR = NOT USED;
     DATA_VERSION = 00;
     NODE_NAME = discette.gsfc.nasa.gov;
     OBJECT = FILE_SPEC;
          DIRECTORY_ID = $ftppullpath;
          FILE_ID = $mymetadata;
          FILE_TYPE = METADATA;
          FILE_SIZE = $mymetadata_size;
     END_OBJECT = FILE_SPEC;
     OBJECT = FILE_SPEC;
          DIRECTORY_ID = $ftppullpath;
          FILE_ID = $mydata;
          FILE_TYPE = DATA;
          FILE_SIZE = $mydata_size;
     END_OBJECT = FILE_SPEC;
     BEGINNING_DATE/TIME = 2006-09-27T08:47:20Z;
     ENDING_DATE/TIME = 2006-09-27T08:47:50Z;
END_OBJECT = FILE_GROUP;
wo2_end

print FH $wo;
close FH;

$pdr = S4P::PDR::read_pdr("$temppath/$woname");
ok(not ($pdr->is_edos), 'Recognized no header.');
$pan = S4P::PAN->new($pdr);
ok(not ($pan->is_edos), 'No from PDR.');
$pan->edos_header($pdr);
ok(not ($pan->is_edos), 'Still none.');
is($pan->groundmsghdr, undef, 'Still none.');

# Now run recv_data again.
$stdouttext = `perl $debug -I @INC[0] ~/S4PA/blib/script/s4pa_recv_data.pl -f $temppath/s4pa_recv_data.cfg -s $temppath/station.cfg -p $temppath $temppath/$woname`;
print "$stdouttext\n============\n" if ($stdouttext ne '');

# Check the output.
isnt($?, 0, 's4pa_create_DN.pl return code due to no space.');
($outfile) = glob ("$temppath/*.PAN");
ok(defined $outfile, "Test for *.PAN file created in $temppath.");
if ($outfile) {
    open FH, "<$outfile";
    my $holdnl = $/;
    undef $/;
    my $pan = <FH>;
    close FH;
    $/ = $holdnl;
    my $hexpan = unpack('H9999', $pan);
    print "$pan\n===========\n";
    isnt(substr($hexpan, 0, 2), '0c', 'No PAN flag, no GMH.');
    unlink $outfile;
}

#############################
# Create new work order for station that isn't EDOS with bad file count.
open FH, ">$temppath/$woname" or
		die "Can't create $temppath/$woname.";

$wo = <<"wo2_end";
ORIGINATING_SYSTEM = S4PA;
CONSUMER_SYSTEM = discette.gsfc.nasa.gov;
DAN_SEQ_NO = 433031;
PRODUCT_NAME = PDS;
MISSION = AURA;
TOTAL_FILE_COUNT = 0003;
AGGREGATE_LENGTH = $agglength;
EXPIRATION_TIME = 999-99-99T99:99:99Z;
OBJECT = FILE_GROUP;
     DATA_SET_ID = P2041840AAAAAAAAAAAAAA06270084704300;
     DATA_TYPE = OML0V;
     DESCRIPTOR = NOT USED;
     DATA_VERSION = 00;
     NODE_NAME = discette.gsfc.nasa.gov;
     OBJECT = FILE_SPEC;
          DIRECTORY_ID = $ftppullpath;
          FILE_ID = $mymetadata;
          FILE_TYPE = METADATA;
          FILE_SIZE = $mymetadata_size;
     END_OBJECT = FILE_SPEC;
     OBJECT = FILE_SPEC;
          DIRECTORY_ID = $ftppullpath;
          FILE_ID = $mydata;
          FILE_TYPE = DATA;
          FILE_SIZE = $mydata_size;
     END_OBJECT = FILE_SPEC;
     BEGINNING_DATE/TIME = 2006-09-27T08:47:20Z;
     ENDING_DATE/TIME = 2006-09-27T08:47:50Z;
END_OBJECT = FILE_GROUP;
wo2_end

print FH $wo;
close FH;

# Now run recv_data again.
$stdouttext = `perl $debug -I @INC[0] ~/S4PA/blib/script/s4pa_recv_data.pl -f $temppath/s4pa_recv_data.cfg -s $temppath/station.cfg -p $temppath $temppath/$woname`;
print "$stdouttext\n============\n" if ($stdouttext ne '');

# Check the output.
isnt($?, 0, 's4pa_create_DN.pl return code due to no space.');
($outfile) = glob ("$temppath/*.PAN");
ok(not (defined $outfile), "Test for no *.PAN file created in $temppath.");
($outfile) = glob ("$temppath/*.PDRD");
ok(defined $outfile, "Test for *.PDRD file created in $temppath.");
if ($outfile) {
    open FH, "<$outfile";
    my $holdnl = $/;
    undef $/;
    my $pdrd = <FH>;
    close FH;
    print "$pdrd\n===========\n";
}

# Clean up.
`rm -fr $temppath` if not $debug;



