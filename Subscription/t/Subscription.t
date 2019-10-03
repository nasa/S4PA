# $Header: /tools/gdaac/cvsroot/S4PA/Subscription/t/Subscription.t,v 1.5 2006/09/25 14:25:50 jpan Exp $ 

use strict;
use POSIX;
use vars qw($opt_v);
use Getopt::Std;
use File::Path;
use File::Find;
use S4P::PDR;
use S4PA::Storage;
use S4PA::Subscription;
use Test::More;

BEGIN { plan tests => 7 };

################################################################################
# Unit tests for S4PA::Subscription
################################################################################

getopts('v');
my $verbose = $opt_v;

# -----------------------
# Preparation for testing
# -----------------------

# Lists to store temporary files and directories.
my @tmpInodeList;

# Get the current die handler and set a new die handler.
my $oldDieHandler = $SIG{__DIE__};
$SIG{__DIE__} = "DieHandler";

# Create temporary working directory
my $tmpDir = "/tmp/Subscription_t";
die "Subscription.t: failed to create temporary directory, $tmpDir"
    unless mkdir($tmpDir);
push @tmpInodeList, $tmpDir;

# Create subscription configuration
my $cfg = $tmpDir . "/subscriptions.cfg";
CreateTempCfg($cfg);
push @tmpInodeList, $cfg;

# Create fake PDRs
my @pdrFileList;
my $pdr1 = $tmpDir . "/t1.PDR";
CreateTempPDR1($pdr1);
push @pdrFileList, $pdr1;
push @tmpInodeList, $pdr1;

my $pdr2 = $tmpDir . "/t2.PDR";
CreateTempPDR2($pdr2);
push @pdrFileList, $pdr2;
push @tmpInodeList, $pdr2;

# Create path for temporary data file
my $dataDir1 = $tmpDir . "/s4pa/TRMM_L1A/TRMM_1A01/2006/049";
die "Subscription.t: failed to create data directory, $dataDir1"
    unless mkpath($dataDir1);

my $dataDir2 = $tmpDir . "/s4pa/modis_aqua/MY09MM6/2004/05";
die "Subscription.t: failed to create data directory, $dataDir2"
    unless mkpath($dataDir2);

# Create data and metadata files
my @data1FileList = CreateData1($dataDir1);
foreach my $data1 (@data1FileList) {
    push @tmpInodeList, $data1;
}

my @data2FileList = CreateData2($dataDir2);
foreach my $data2 (@data2FileList) {
    push @tmpInodeList, $data2;
}

# Lump PDRs
my $num_pdr = scalar(@pdrFileList);
my $pdr;
foreach my $pdrFile (@pdrFileList) {
    my $dummyPdr = S4P::PDR::read_pdr($pdrFile)
        || S4P::perish( 2, "Cannot read/parser PDR $pdrFile" );
    if ( defined $pdr ) {
        foreach my $fileGroup ( @{$dummyPdr->file_groups()} ) {
            $pdr->add_file_group( $fileGroup );
        }
    } else {
        $pdr = $dummyPdr;
    }
}
my @fileGroups = @{$pdr->file_groups};

# -----------------------
# Testing module methods
# -----------------------

# Test read_config_file method
diag("Testing read_config_file method");
my ($cfg_url, $cfg_subscriptions) = S4PA::Subscription::read_config_file($cfg);
my $num_url = scalar(keys %{$cfg_url});
isnt($num_url, 0, "$num_url URL conversion pattern found in CFG"); 

my $num_subscription = scalar(keys %{$cfg_subscriptions});
isnt($num_subscription, 0, "$num_subscription Subscription IDs found in CFG"); 

# Test get_subscription_info method
my $file_group = $fileGroups[0];
my $data_type = $file_group->data_type;
my $data_version = $file_group->data_version;

diag("Testing get_subscription_info method");
my ($url_pattern, $subscription) = S4PA::Subscription::get_subscription_info(
    $data_type, $data_version, $cfg_url, $cfg_subscriptions);
ok(defined $url_pattern, "Found url pattern for dataset $data_type");

my $num_subs = scalar(keys %{$subscription});
isnt($num_subs, 0, "$data_type has $num_subs matching subscription ID");

# Test expand_url method
diag("Testing expand_url method");
my @science_files = $file_group->science_files();
my $met_file = $file_group->met_file();
my @urls = S4PA::Subscription::expand_url($url_pattern, $url_pattern, 
    $data_type, $data_version, $met_file, @science_files);
my $xml_url = @urls[1];
unlike($xml_url, qr/%=T\/%Y\/%j/, "Is a valid conversion");

# Test match_subscriptions method
diag("Testing match_subscription method");
my $match = S4PA::Subscription::match_subscriptions(
    $pdr, $cfg_url, $cfg_subscriptions);
my $num_match = scalar(keys %{$match});
isnt($num_match, 0, "$num_match matches in $num_pdr PDRs");

# Test fill_workorder method
diag("Testing fill_workorder method");
my @work_orders = S4PA::Subscription::fill_workorder($match, $cfg_subscriptions);
my $num_order = scalar(@work_orders);
isnt($num_order, 0, "$num_order work orders filled");

# -----------------------
# Cleanup testing files
# -----------------------

# Summary of work order generated
diag("Test Summary:");
foreach my $wo (@work_orders) {
    print "\n" if ($verbose);
    push @tmpInodeList, $wo;
    if ($wo =~ /EMAIL/) {
        diag("Pull subscription: $wo");
    }
    else {
        diag("Push subscription: $wo");
    }

    # print content of each worder
    if ($verbose) {
        open(WO, "<$wo") or die "Can not open work order $wo: $!";
        while (<WO>) {
            print $_;
        }
    }
}

# Cleanup
Cleanup();
exit;

###############################################################################
# DieHandler()
#   Performs end of execution tasks.
###############################################################################
sub DieHandler
{
    my ($msg) = @_;

    # Cleanup before exiting
    Cleanup();

    # Reset the die handler
    $SIG{__DIE__} = $oldDieHandler;

    # Die with the message from the earlier die attempt.
    die $msg;
}

###############################################################################
# Cleanup()
#   Clean up temporary files/directories created during execution.
###############################################################################
sub Cleanup
{
    # Remove all the temporary files created.
    foreach my $inode (@tmpInodeList) {
        if ( -f $inode or -l $inode ) {
            unlink( $inode ) or warn "Subscription.t: failed to delete $inode";
        }
    }

    # Remove all the temporary directories created.
    foreach my $inode (@tmpInodeList) {
        if (-d $inode) {
            finddepth (sub {
                rmdir $_;
                }, "$inode");
            rmdir( $inode ) or warn "Subscription.t: failed to delete $inode";
        }
    }
}

###############################################################################
# CreateTempCfg()
#   Create sample subscription configuration for testing
###############################################################################
sub CreateTempCfg {
    my $cfg_file = shift;
    open(CFG, ">$cfg_file") or die "Failed creating CFG $cfg_file: $!";
    {
    print CFG <<End_of_CFG
%cfg_url = (
    "TRMM_1A01..*" => "ftp://disc2.nascom.nasa.gov/data/s4pa/TRMM_L1A/%=T/%Y/%j/%=F",
    "MY09MM6..*" => "ftp://reason.gsfc.nasa.gov/data/s4pa/modis_aqua/%=T/%Y/%j/%=F"
);

%cfg_subscriptions = (
    "SUB_ID01" => {
        "notify" => {
            "address" => "glei\\\@pop600.gsfc.nasa.gov",
            "format" => "LEGACY"
            },
        "destination" => "sftp:disc2.nascom.nasa.gov/home/lei",
        "label" => "LEI_V3",
        "TRMM_1A01..*" => {
            "validator" => ["true"],
            "filter" => {"\\\\.xml\\\$" => "xml2odl -o /var/tmp",
                         "\\\\.Z\\\$" => "gzcat -o /var/tmp"
            }
        },
        "MY09MM6..*" => {
            "validator" => ["true"],
            "filter" => {"\\\\.xml\\\$" => "xml2odl -o /var/tmp"}
        }
    },

    "SUB_ID02" => {
        "notify" => {
            "address" => "glei\\\@pop600.gsfc.nasa.gov",
            "format" => "LEGACY"
            },
        "destination" => "mailto:glei\\\@pop600.gsfc.nasa.gov",
        "label" => "LEI_V1",
        "MY09MM6..*" => {
            "validator" => ["true"],
        }
    },

    "SUB_ID03" => {
        "notify" => {
            "address" => "glei\\\@g0ins01u.gsfcb.ecs.nasa.gov",
            "format" => "S4PA"
            },
        "label" => "LEI_ECS",
        "TRMM_1A01..*" => {
            "validator" => ["true"],
        },
    },

    "SUB_ID04" => {
        "notify" => {
            "address" => "glei\\\@pop600.gsfc.nasa.gov",
            "format" => "LEGACY"
            },
        "destination" => "sftp:disc1.gsfc.nasa.gov/home/lei",
        "label" => "LEI_V3",
        "MY09MM6..*" => {
            "validator" => ["false"],
            "filter" => {"\\\\.xml\\\$" => "xml2odl -o /var/tmp"}
        }
    },

);
End_of_CFG
    }
    close(CFG);
}

###############################################################################
# CreateTempPDR1()
#   Create sample PDR for subscription checking
###############################################################################
sub CreateTempPDR1 {
    my $pdr1_file = shift;
    open(PDR,">$pdr1_file") or die "Failed creating PDR $pdr1_file: $!";
    {
    print PDR <<End_of_PDR
ORIGINATING_SYSTEM=S4PA;
TOTAL_FILE_COUNT=2;
EXPIRATION_TIME=2005-05-30T12:38:58Z;
OBJECT=FILE_GROUP;
        DATA_TYPE=MY09MM6;
        DATA_VERSION=001;
        OBJECT=FILE_SPEC;
                FILE_TYPE=SCIENCE;
                FILE_SIZE=2094570;
                DIRECTORY_ID=$tmpDir/s4pa/modis_aqua/MY09MM6/2004/05;
                FILE_ID=A20041222004152.L3m_MO_SST_9KM;
        END_OBJECT=FILE_SPEC;
        OBJECT=FILE_SPEC;
                FILE_TYPE=METADATA;
                FILE_SIZE=2192;
                DIRECTORY_ID=$tmpDir/s4pa/modis_aqua/MY09MM6/2004/05;
                FILE_ID=A20041222004152.L3m_MO_SST_9KM.xml;
        END_OBJECT=FILE_SPEC;
END_OBJECT=FILE_GROUP;
End_of_PDR
    }
    close(PDR);
}

###############################################################################
# CreateTempPDR2()
#   Create sample PDR for subscription checking
###############################################################################
sub CreateTempPDR2 {
    my $pdr2_file = shift;
    open(PDR,">$pdr2_file") or die "Failed creating PDR $pdr2_file: $!";
    {
    print PDR <<End_of_PDR
ORIGINATING_SYSTEM=tsdissmc;
TOTAL_FILE_COUNT=6;
EXPIRATION_TIME=2006-03-07T11:53:12Z;
OBJECT=FILE_GROUP;
        DATA_TYPE=TRMM_1A01;
        DATA_VERSION=6;
        OBJECT=FILE_SPEC;
                FILE_TYPE=SCIENCE;
                FILE_SIZE=30728627;
                DIRECTORY_ID=$tmpDir/s4pa/TRMM_L1A/TRMM_1A01/2006/049;
                FILE_ID=1A01.060218.47083.6.Z;
        END_OBJECT=FILE_SPEC;
        OBJECT=FILE_SPEC;
                FILE_TYPE=METADATA;
                FILE_SIZE=2216;
                DIRECTORY_ID=$tmpDir/s4pa/TRMM_L1A/TRMM_1A01/2006/049;
                FILE_ID=1A01.060218.47083.6.xml;
        END_OBJECT=FILE_SPEC;
END_OBJECT=FILE_GROUP;
OBJECT=FILE_GROUP;
        DATA_TYPE=TRMM_1A01;
        DATA_VERSION=6;
        OBJECT=FILE_SPEC;
                FILE_TYPE=SCIENCE;
                FILE_SIZE=31517505;
                DIRECTORY_ID=$tmpDir/s4pa/TRMM_L1A/TRMM_1A01/2006/049;
                FILE_ID=1A01.060218.47084.6.Z;
        END_OBJECT=FILE_SPEC;
        OBJECT=FILE_SPEC;
                FILE_TYPE=METADATA;
                FILE_SIZE=2215;
                DIRECTORY_ID=$tmpDir/s4pa/TRMM_L1A/TRMM_1A01/2006/049;
                FILE_ID=1A01.060218.47084.6.xml;
        END_OBJECT=FILE_SPEC;
END_OBJECT=FILE_GROUP;
OBJECT=FILE_GROUP;
        DATA_TYPE=TRMM_1A01;
        DATA_VERSION=6;
        OBJECT=FILE_SPEC;
                FILE_TYPE=SCIENCE;
                FILE_SIZE=32530527;
                DIRECTORY_ID=$tmpDir/s4pa/TRMM_L1A/TRMM_1A01/2006/049;
                FILE_ID=1A01.060218.47085.6.Z;
        END_OBJECT=FILE_SPEC;
        OBJECT=FILE_SPEC;
                FILE_TYPE=METADATA;
                FILE_SIZE=2216;
                DIRECTORY_ID=$tmpDir/s4pa/TRMM_L1A/TRMM_1A01/2006/049;
                FILE_ID=1A01.060218.47085.6.xml;
        END_OBJECT=FILE_SPEC;
END_OBJECT=FILE_GROUP;
End_of_PDR
    }
    close(PDR);
}

###############################################################################
# CreateData1()
#   Create sample data and metadata files
###############################################################################
sub CreateData1 {
    my $dataDir = shift;
    my @fileList = ();
    my @nameList = ("1A01.060218.47083.6", "1A01.060218.47084.6", "1A01.060218.47085.6");

    foreach my $name (@nameList) {
        # create empty data file with .Z extension
        my $dataFile = $dataDir . "/" . $name . ".Z";
        open(DATA, ">$dataFile") or die "Failed creating data $dataFile: $!";
        close DATA;
        push @fileList, $dataFile;

        # create metadata file with .xml extension
        my $metaFile = $dataDir . "/" . $name . ".xml";
        open(META, ">$metaFile") or die "Failed creating metadata $metaFile: $!";
        {
        print META <<End_of_META
<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<!-- SchemaLocation=\"http://disc.gsfc.nasa.gov/xsd/s4pa/S4paGranule.xsd\" -->
<S4PAGranuleMetaDataFile>
  <CollectionMetaData>
    <LongName>TRMM Visible-IR Scanner L1A</LongName>
    <ShortName>TRMM_1A01</ShortName>
    <VersionID>6</VersionID>
  </CollectionMetaData>
  <DataGranule>
    <GranuleID>$name.Z</GranuleID>
    <Format>binary</Format>
    <CheckSum>
      <CheckSumType>CRC32</CheckSumType>
      <CheckSumValue>3219189446</CheckSumValue>
    </CheckSum>
    <SizeBytesDataGranule>32530527</SizeBytesDataGranule>
    <InsertDateTime>2006-02-21 11:53:00</InsertDateTime>
  </DataGranule>
  <RangeDateTime>
    <RangeEndingTime>22:30:38</RangeEndingTime>
    <RangeEndingDate>2006-02-18</RangeEndingDate>
    <RangeBeginningTime>20:58:16</RangeBeginningTime>
    <RangeBeginningDate>2006-02-18</RangeBeginningDate>
  </RangeDateTime>
</S4PAGranuleMetaDataFile>
End_of_META
        }
        close(META);
        push @fileList, $metaFile;
    }
    return @fileList;
}

###############################################################################
# CreateData2()
#   Create sample data and metadata files
###############################################################################
sub CreateData2 {
    my $dataDir = shift;
    my @fileList = ();
    my @nameList = ("A20041222004152.L3m_MO_SST_9KM");

    foreach my $name (@nameList) {
        # create empty data file 
        my $dataFile = $dataDir . "/" . $name;
        open(DATA, ">$dataFile") or die "Failed creating data $dataFile: $!";
        close DATA;
        push @fileList, $dataFile;

        # create metadata file with .xml extension
        my $metaFile = $dataDir . "/" . $name . ".xml";
        open(META, ">$metaFile") or die "Failed creating metadata $metaFile: $!";
        {
        print META <<End_of_META
<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<S4PAGranuleMetaDataFile xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance \" xsi:noNamespaceSchemaLocation=\"http://disc.gsfc.nasa.gov/xsd/s4pa/S4paGranule.xsd\">
  <CollectionMetaData>
    <LongName>MODIS_Aqua Level 3 monthly 9KM SMI SST</LongName>
    <ShortName>MY09MM6</ShortName>
    <VersionID>1</VersionID>
  </CollectionMetaData>
  <DataGranule>
    <GranuleID>$name</GranuleID>
    <Format>HDF</Format>
    <CheckSum>
      <CheckSumType>CRC32</CheckSumType>
      <CheckSumValue>509871</CheckSumValue>
    </CheckSum>
    <SizeBytesDataGranule>2094570</SizeBytesDataGranule>
    <InsertDateTime>2005-05-16 12:38:57</InsertDateTime>
    <ProductionDateTime>2005-02-25 23:23:37</ProductionDateTime>
  </DataGranule>
  <RangeDateTime>
    <RangeEndingTime>23:59:59</RangeEndingTime>
    <RangeEndingDate>2004-05-31</RangeEndingDate>
    <RangeBeginningTime>00:00:01</RangeBeginningTime>
    <RangeBeginningDate>2004-05-01</RangeBeginningDate>
  </RangeDateTime>
</S4PAGranuleMetaDataFile>
End_of_META
        }
        close(META);
        push @fileList, $metaFile;
    }
    return @fileList;
}
