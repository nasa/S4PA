#!/usr/bin/perl

=head1 NAME

s4pa_create_DN.t - test script to create DN

=head1 SYNOPSIS

Test s4pa_create_DN.pl features to pass back PDR.

=head1 ARGUMENTS

=over 4

=item B<-d>

Debug flag: keep output file.

=back

=head1 AUTHOR

Dr. C. Wrandle Barth, ADNET, NASA/GSFC, Code 610.2, Greenbelt, MD  20771.

=head1 CHANGE LOG

05/1/06 Initial version
07/28/06 Bugzilla fix 16
08/01/06 Bugzilla fix 29
8/14/06 Added CRC tests

=cut

use strict;
use POSIX;
use S4P;
use S4P::PDR;
use S4P::FileGroup;
use S4P::FileSpec;
use S4PA::Storage;
use vars qw($opt_d);
use Getopt::Std;
use Test::More 'no_plan';

getopts('d');
my $debug = '-d ' if $opt_d;
# Define path for test files.
my $temppath = '/var/tmp/s4pa_create_DN_test';
# Define path for notification to be dropped on discette.
my $ftppath = 'private/s4pa/push';

# Define work order filenames for test.
my $woname = 'DO.EMAIL.TestSub.wo';
my $wo2name = 'DO.EMAIL.TestSub2.wo';
my $wo3name = 'DO.EMAIL.TestSub3.wo';
my $wo4name = 'DO.EMAIL.TestSub4.wo';
# Define data and metadata filenames.
my $mydata = 'mydata';
my $mycrdata = "$mydata.cr.txt";
my $myxml = "$mydata.xml";
my $myodl = "$mydata.odl";
my $myproto = 'http';
my $myhost = 'xxdisc';
my $startdate = '1997-09-04';
my $starttime = '16:26:34';
my $enddate = '1997-09-04';
my $endtime = '16:32:20';
# Create phony CRC with random number.
my $randomCRC = int(rand(10000000));
# Clean up from previous run.
if (-d $temppath) {
        `rm -fr $temppath`;
}
mkdir $temppath or die "Can't make temporary directory $temppath.";
unlink glob("/ftp/$ftppath/DN*");


# Create data file
open FH, ">$temppath/$mydata" or
		die "Can't create $temppath/$mydata.";
print FH "She blinded me with science!\n";
close FH;
my $mydata_size = -s "$temppath/$mydata";

# Create construction record file
open FH, ">$temppath/$mycrdata" or
		die "Can't create $temppath/$mycrdata.";
print FH "I've been workin' on the railroad!\n";
close FH;
my $mycrdata_size = -s "$temppath/$mycrdata";

# Create phony ODL metadata extraction stuff.
my $odlmetadata = "Provider's original metadata here.";

# Create filtererd file.
open FH, ">$temppath/$myodl" or
		die "Can't create $temppath/$myodl.";
print FH "$odlmetadata\n";
close FH;
my $myodl_size = -s "$temppath/$myodl";

# Create metadata file for extracting information
open FH, ">$temppath/$myxml" or
		die "Can't create $temppath/$myxml.";

print FH <<"xml_end";
<?xml version="1.0" encoding="UTF-8"?>
<S4PAGranuleMetaDataFile xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance " xsi:noNamespaceSchemaLocation="http://disc.gsfc.nasa.gov/xsd/s4pa/S4paGranule.xsd">
  <CollectionMetaData>
    <LongName>XYZZY Test Data Long Name based on SeaWiFS metadata file</LongName>
    <ShortName>XYZZY</ShortName>
    <VersionID>2</VersionID>
  </CollectionMetaData>
  <DataGranule>
    <GranuleID>$mycrdata</GranuleID>
    <Format>HDF</Format>
    <CheckSum>
      <CheckSumType>CRC32</CheckSumType>
      <CheckSumValue>1090638325</CheckSumValue>
    </CheckSum>
    <SizeBytesDataGranule>50</SizeBytesDataGranule>
    <InsertDateTime>2005-08-16 13:51:47</InsertDateTime>
    <ProductionDateTime>2005-06-22 00:24:25</ProductionDateTime>
    <Granulits>
      <Granulit>
        <GranulitID>main</GranulitID>
        <FileName>$mydata</FileName>
        <CheckSum>
          <CheckSumType>CRC32</CheckSumType>
          <CheckSumValue>$randomCRC</CheckSumValue>
        </CheckSum>
        <FileSize>$mydata_size</FileSize>
      </Granulit>
      <Granulit>
        <GranulitID>x00</GranulitID>
        <FileName>$mycrdata</FileName>
        <CheckSum>
          <CheckSumType>CRC32</CheckSumType>
          <CheckSumValue>12345</CheckSumValue>
        </CheckSum>
        <FileSize>$mycrdata_size</FileSize>
      </Granulit>
     </Granulits>
  </DataGranule>
  <RangeDateTime>
    <RangeEndingTime>$endtime</RangeEndingTime>
    <RangeEndingDate>$enddate</RangeEndingDate>
<RangeBeginningTime>${starttime}Z</RangeBeginningTime>
    <RangeBeginningDate>$startdate</RangeBeginningDate>
  </RangeDateTime>
  <SpatialDomainContainer>
    <HorizontalSpatialDomainContainer>
      <BoundingRectangle>
        <WestBoundingCoordinate>-70.867981</WestBoundingCoordinate>
        <NorthBoundingCoordinate>47.458332</NorthBoundingCoordinate>
        <EastBoundingCoordinate>-47.188404</EastBoundingCoordinate>
        <SouthBoundingCoordinate>25.625000</SouthBoundingCoordinate>
      </BoundingRectangle>
    </HorizontalSpatialDomainContainer>
  </SpatialDomainContainer>
  <Platform>
    <PlatformShortName>OrbView-2</PlatformShortName>
    <Instrument>
      <InstrumentShortName>SeaWiFS</InstrumentShortName>
      <Sensor>
        <SensorShortName>SeaWiFS</SensorShortName>
      </Sensor>
    </Instrument>
  </Platform>
  <PSAs>
    <PSA>
      <PSAName>EndDataDateTime</PSAName>
      <PSAValue>1997-09-04 16:32:20</PSAValue>
    </PSA>
    <PSA>
      <PSAName>EndOrbitNumber</PSAName>
      <PSAValue>519</PSAValue>
    </PSA>
    <PSA>
      <PSAName>StartDataDateTime</PSAName>
      <PSAValue>1997-09-04 16:26:34</PSAValue>
    </PSA>
    <PSA>
      <PSAName>StartOrbitNumber</PSAName>
      <PSAValue>519</PSAValue>
    </PSA>
    <PSA>
      <PSAName>UncompressSizeBytes</PSAName>
      <PSAValue>87080</PSAValue>
    </PSA>
  </PSAs>
  <ODLMETA>
    $odlmetadata
  </ODLMETA>
</S4PAGranuleMetaDataFile>
xml_end

close FH;
my $myxml_size = -s "$temppath/$myxml";

# Create work order for station.
open FH, ">$temppath/$woname" or
		die "Can't create $temppath/$woname.";

print FH <<"wo_end";
<?xml version="1.0" encoding="UTF-8"?>
<FilePacket status="C"
    		label="My filename is $woname - by Randy"
    		notify="ftp:discette/$ftppath"
    		messageFormat="PDR"
    		destination="sftp:yy.zz.com/dir1"
    		numAttempt="1"
    		completed="2006-05-01 12:00:15" >
    <FileGroup>
    	<File status="C"
    		url="${myproto}://$myhost.gsfc.nasa.gov/data/dgroup/dset/2006/365/$mydata"
    		localPath="$temppath/$mydata"
    		cleanup="N"
    		completed="2006-05-01 12:00:11" />
    	<File status="C"
    		url="${myproto}://$myhost.gsfc.nasa.gov/data/dgroup/dset/2006/365/$mycrdata"
    		localPath="$temppath/$mycrdata"
    		cleanup="N"
    		completed="2006-05-01 12:00:13" />
    	<File status="C"
    		url="${myproto}://$myhost.gsfc.nasa.gov/data/dgroup/dset/2006/365/$myxml"
    		localPath="$temppath/$myxml"
        	filter="odlextractor.pl"
        	filteredFilepath="$temppath/$myodl"
        	filteredSize="$myodl_size"
    		cleanup="N"
    		completed="2006-05-01 12:00:14" />
    </FileGroup>
</FilePacket>
wo_end

close FH;

# Run the script.
print "===Running s4pa_create_DN.pl===\n";
my $stdouttext = `blib/script/s4pa_create_DN.pl $temppath $temppath/$woname`;
print "$stdouttext\n============\n" if ($stdouttext ne '');

# Check the output.
is($?, 0, 's4pa_create_DN.pl return code.');
my ($outfile) = glob ("/ftp/$ftppath/DN*.PDR");
ok(defined $outfile, "Test for DN*.PDR file created in /ftp/$ftppath.");
if ($outfile) {
    # Echo PDR to output.
    print "====PDR Output========\n", `cat $outfile`, "============\n";
    my $pdr = S4P::PDR::read_pdr($outfile);
    # Check the PDR-level fields.
    is($pdr->originating_system, "S4PA_$myhost", 'Originating system test.');
    is($pdr->total_file_count, 3, 'Should be 3 files here.');
    my @file_groups = @{ $pdr->file_groups };
    is(scalar @file_groups, 1, 'Should be 1 group.');
    is(scalar $pdr->files('SCIENCE'), 2, 'Should be 2 science files.');
    is(scalar $pdr->files('METADATA'), 1, 'Should be 1 metadata file.');

# Bugzilla fix 16 to add EXPIRATION_TIME.
    like($pdr->expiration_time, qr/^\d\d\d\d.+T.+Z$/, 'Looks like an expiration time.');

    my $fg = $file_groups[0];
    # Check the FILE_GROUP-level fields.
    if ($fg) {
        is($fg->data_type, 'XYZZY', 'Type check in group.');
        is($fg->data_version, '002', 'Version check in group.');
        my $node = $fg->node_name;

# Buzilla fix 29: report pushed hostname as nodename
        is($node, 'yy.zz.com', 'Nodename check.');

# Bugzilla fix 16 to remove UR and start and end date.

#       like($fg->ur, qr/$mycrdata/, "Construction record contained in UR.");
#       like($fg->ur, qr/$node/, "Node contained in UR.");
#       like($fg->ur, qr/$myproto/, "Protocol contained in UR.");
#       is($fg->data_start, "${startdate}T${starttime}Z", 'Start stamp.');
#       is($fg->data_end, "${enddate}T${endtime}Z", 'End stamp.');

        is($fg->ur, undef, 'UR removed.');
        is($fg->data_start, undef, 'Start stamp removed.');
        is($fg->data_end, undef, 'End stamp removed.');

        foreach my $fs ($pdr->file_specs) {
        	# Check the FILE_SPEC fields.
        	my $fn = $fs->file_id;

# Buzilla fix 29: report pushed directory name.
        	is($fs->directory_id, '/dir1', "Directory on $fn test.") if ($fn ne $myodl);
        	my $fsize = $fn eq $mydata ? $mydata_size :
    			($fn eq $mycrdata ? $mycrdata_size :
    			($fn eq $myodl ? $myodl_size : 0));
        	if ($fsize == 0) {
           	    fail("Bad file name $fn");
        	} else {
                is($fs->file_size, $fsize, 'Size of $fn check.');
                is($fs->file_type, $fn eq $myodl ? 'METADATA' : 'SCIENCE', "Type of $fn test.");
        	}
        }
    }
}


# Create second work order for station.
open FH, ">$temppath/$wo2name" or
		die "Can't create $temppath/$wo2name.";

print FH <<"wo2_end";
<?xml version="1.0" encoding="UTF-8"?>
<FilePacket status="C"
    		label="My filename is $wo2name. - by Randy"
    		notify="ftp:discette/$ftppath"
    		messageFormat="LEGACY"
    		destination="sftp:yy.zz.com/dir1"
    		numAttempt="1"
    		completed="2006-05-01 12:00:15" >
    <FileGroup>
    	<File status="C"
    		url="${myproto}://$myhost.gsfc.nasa.gov/data/dgroup/dset/2006/365/$mydata"
    		localPath="$temppath/$mydata"
    		cleanup="N"
    		completed="2006-05-01 12:00:11" />
    	<File status="C"
    		url="${myproto}://$myhost.gsfc.nasa.gov/data/dgroup/dset/2006/365/$mycrdata"
    		localPath="$temppath/$mycrdata"
    		cleanup="N"
    		completed="2006-05-01 12:00:13" />
    	<File status="C"
    		url="${myproto}://$myhost.gsfc.nasa.gov/data/dgroup/dset/2006/365/$myxml"
    		localPath="$temppath/$myxml"
        	filter="odlextractor.pl"
        	filteredFilepath="$temppath/$myodl"
        	filteredSize="$myodl_size"
    		cleanup="N"
    		completed="2006-05-01 12:00:14" />
    </FileGroup>
</FilePacket>
wo2_end

close FH;

# Create txt file
open FH, ">$temppath/scpSuccess.txt" or
		die "Can't create $temppath/scpSuccess.txt.";
print FH "You're a success!\n";
close FH;

# Run the script.
print "===Running s4pa_create_DN.pl===\n";
my $stdouttext = `blib/script/s4pa_create_DN.pl $temppath $temppath/$wo2name`;
print "$stdouttext\n============\n" if ($stdouttext ne '');

# Check the output.
is($?, 0, 's4pa_create_DN.pl return code.');
# Find most recent (highest named) .notify file.
my ($outfile) = reverse(sort(glob ("/ftp/$ftppath/DN*.notify")));
ok(defined $outfile, "Test for DN*.notify file created in /ftp/$ftppath.");
if ($outfile) {
    # Echo DN to output.
    open (FH, $outfile) or die "Can't open $outfile";
    local $/;
    my $legDN = <FH>;
    print "====DN Output========\n$legDN\n============\n";
    $legDN=~ s/FILECKSUMTYPE:\s*(\w+)//s;
    is($1, 'CKSUM', 'TYPE correct');
    $legDN=~ s/FILENAME:\s*$mydata.+?FILECKSUMVALUE:\s*([0-9]+)//s;
    is($1, $randomCRC, 'CRC for science file.');
    $legDN=~ s/FILENAME:\s*$mycrdata.+?FILECKSUMVALUE:\s*([0-9]+)//s;
    is($1, 12345, 'CRC for creation record file.');
    unlike ($legDN, qr/FILENAME:\s*$myodl.+?FILECKSUMVALUE:/s, 'No CKSUM on metadata');
}

# Create new phony CRC with random number.
$randomCRC = int(rand(10000000));
# Rewrite metadata file for extracting information for single granule
open FH, ">$temppath/$myxml" or
		die "Can't create $temppath/$myxml.";

print FH <<"xml_end";
<?xml version="1.0" encoding="UTF-8"?>
<S4PAGranuleMetaDataFile xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance " xsi:noNamespaceSchemaLocation="http://disc.gsfc.nasa.gov/xsd/s4pa/S4paGranule.xsd">
  <CollectionMetaData>
    <LongName>XYZZY Test Data Long Name based on SeaWiFS metadata file</LongName>
    <ShortName>XYZZY</ShortName>
    <VersionID>2</VersionID>
  </CollectionMetaData>
  <DataGranule>
    <GranuleID>$mydata</GranuleID>
    <Format>HDF</Format>
    <CheckSum>
      <CheckSumType>MD5</CheckSumType>
      <CheckSumValue>$randomCRC</CheckSumValue>
    </CheckSum>
    <SizeBytesDataGranule>50</SizeBytesDataGranule>
    <InsertDateTime>2005-08-16 13:51:47</InsertDateTime>
    <ProductionDateTime>2005-06-22 00:24:25</ProductionDateTime>
  </DataGranule>
  <RangeDateTime>
    <RangeEndingTime>$endtime</RangeEndingTime>
    <RangeEndingDate>$enddate</RangeEndingDate>
<RangeBeginningTime>${starttime}Z</RangeBeginningTime>
    <RangeBeginningDate>$startdate</RangeBeginningDate>
  </RangeDateTime>
  <SpatialDomainContainer>
    <HorizontalSpatialDomainContainer>
      <BoundingRectangle>
        <WestBoundingCoordinate>-70.867981</WestBoundingCoordinate>
        <NorthBoundingCoordinate>47.458332</NorthBoundingCoordinate>
        <EastBoundingCoordinate>-47.188404</EastBoundingCoordinate>
        <SouthBoundingCoordinate>25.625000</SouthBoundingCoordinate>
      </BoundingRectangle>
    </HorizontalSpatialDomainContainer>
  </SpatialDomainContainer>
  <Platform>
    <PlatformShortName>OrbView-2</PlatformShortName>
    <Instrument>
      <InstrumentShortName>SeaWiFS</InstrumentShortName>
      <Sensor>
        <SensorShortName>SeaWiFS</SensorShortName>
      </Sensor>
    </Instrument>
  </Platform>
  <PSAs>
    <PSA>
      <PSAName>EndDataDateTime</PSAName>
      <PSAValue>1997-09-04 16:32:20</PSAValue>
    </PSA>
    <PSA>
      <PSAName>EndOrbitNumber</PSAName>
      <PSAValue>519</PSAValue>
    </PSA>
    <PSA>
      <PSAName>StartDataDateTime</PSAName>
      <PSAValue>1997-09-04 16:26:34</PSAValue>
    </PSA>
    <PSA>
      <PSAName>StartOrbitNumber</PSAName>
      <PSAValue>519</PSAValue>
    </PSA>
    <PSA>
      <PSAName>UncompressSizeBytes</PSAName>
      <PSAValue>87080</PSAValue>
    </PSA>
  </PSAs>
  <ODLMETA>
    $odlmetadata
  </ODLMETA>
</S4PAGranuleMetaDataFile>
xml_end

close FH;


# Create third work order for station.
open FH, ">$temppath/$wo3name" or
		die "Can't create $temppath/$wo3name.";

print FH <<"wo3_end";
<?xml version="1.0" encoding="UTF-8"?>
<FilePacket status="C"
    		label="My filename is $wo3name. - by Randy"
    		notify="ftp:discette/$ftppath"
    		messageFormat="LEGACY"
    		destination="sftp:yy.zz.com/dir1"
    		numAttempt="1"
    		completed="2006-05-01 12:00:15" >
    <FileGroup>
    	<File status="C"
    		url="${myproto}://$myhost.gsfc.nasa.gov/data/dgroup/dset/2006/365/$mydata"
    		localPath="$temppath/$mydata"
    		cleanup="N"
    		completed="2006-05-01 12:00:11" />
    	<File status="C"
    		url="${myproto}://$myhost.gsfc.nasa.gov/data/dgroup/dset/2006/365/$myxml"
    		localPath="$temppath/$myxml"
        	filter="odlextractor.pl"
        	filteredFilepath="$temppath/$myodl"
        	filteredSize="$myodl_size"
    		cleanup="N"
    		completed="2006-05-01 12:00:14" />
    </FileGroup>
</FilePacket>
wo3_end

close FH;

# Run the script.
print "===Running s4pa_create_DN.pl===\n";
my $stdouttext = `blib/script/s4pa_create_DN.pl $temppath $temppath/$wo3name`;
print "$stdouttext\n============\n" if ($stdouttext ne '');

# Check the output.
is($?, 0, 's4pa_create_DN.pl return code.');
# Find most recent (highest named) .notify file.
my ($outfile) = reverse(sort(glob ("/ftp/$ftppath/DN*.notify")));
ok(defined $outfile, "Test for DN*.notify file created in /ftp/$ftppath.");
if ($outfile) {
    # Echo DN to output.
    open (FH, $outfile) or die "Can't open $outfile";
    local $/;
    my $legDN = <FH>;
    print "====DN Output========\n$legDN\n============\n";
    $legDN=~ s/FILECKSUMTYPE:\s*(\w+)//s;
    is($1, 'MD5', 'TYPE correct');
    $legDN=~ s/FILENAME:\s*$mydata.+?FILECKSUMVALUE:\s*([0-9]+)//s;
    is($1, $randomCRC, 'CRC for science file.');
}
###########################################################
# New test for segmented DNs
#
my $myxml2 = "${mydata}2.xml";
# Write metadata file or second granule
open FH, ">$temppath/$myxml2" or
		die "Can't create $temppath/$myxml2.";

print FH <<"xml2_end";
<?xml version="1.0" encoding="UTF-8"?>
<S4PAGranuleMetaDataFile xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance " xsi:noNamespaceSchemaLocation="http://disc.gsfc.nasa.gov/xsd/s4pa/S4paGranule.xsd">
  <CollectionMetaData>
    <LongName>XYZZY Test Data Long Name based on SeaWiFS metadata file</LongName>
    <ShortName>XYZZY</ShortName>
    <VersionID>2</VersionID>
  </CollectionMetaData>
  <DataGranule>
    <GranuleID>$mycrdata</GranuleID>
    <Format>HDF</Format>
    <CheckSum>
      <CheckSumType>MD5</CheckSumType>
      <CheckSumValue>$randomCRC</CheckSumValue>
    </CheckSum>
    <SizeBytesDataGranule>50</SizeBytesDataGranule>
    <InsertDateTime>2005-08-16 13:51:47</InsertDateTime>
    <ProductionDateTime>2005-06-22 00:24:25</ProductionDateTime>
  </DataGranule>
  <RangeDateTime>
    <RangeEndingTime>$endtime</RangeEndingTime>
    <RangeEndingDate>$enddate</RangeEndingDate>
<RangeBeginningTime>${starttime}Z</RangeBeginningTime>
    <RangeBeginningDate>$startdate</RangeBeginningDate>
  </RangeDateTime>
  <SpatialDomainContainer>
    <HorizontalSpatialDomainContainer>
      <BoundingRectangle>
        <WestBoundingCoordinate>-70.867981</WestBoundingCoordinate>
        <NorthBoundingCoordinate>47.458332</NorthBoundingCoordinate>
        <EastBoundingCoordinate>-47.188404</EastBoundingCoordinate>
        <SouthBoundingCoordinate>25.625000</SouthBoundingCoordinate>
      </BoundingRectangle>
    </HorizontalSpatialDomainContainer>
  </SpatialDomainContainer>
  <Platform>
    <PlatformShortName>OrbView-2</PlatformShortName>
    <Instrument>
      <InstrumentShortName>SeaWiFS</InstrumentShortName>
      <Sensor>
        <SensorShortName>SeaWiFS</SensorShortName>
      </Sensor>
    </Instrument>
  </Platform>
  <PSAs>
    <PSA>
      <PSAName>EndDataDateTime</PSAName>
      <PSAValue>1997-09-04 16:32:20</PSAValue>
    </PSA>
    <PSA>
      <PSAName>EndOrbitNumber</PSAName>
      <PSAValue>519</PSAValue>
    </PSA>
    <PSA>
      <PSAName>StartDataDateTime</PSAName>
      <PSAValue>1997-09-04 16:26:34</PSAValue>
    </PSA>
    <PSA>
      <PSAName>StartOrbitNumber</PSAName>
      <PSAValue>519</PSAValue>
    </PSA>
    <PSA>
      <PSAName>UncompressSizeBytes</PSAName>
      <PSAValue>87080</PSAValue>
    </PSA>
  </PSAs>
  <ODLMETA>
    $odlmetadata
  </ODLMETA>
</S4PAGranuleMetaDataFile>
xml2_end

close FH;


# Create fourth work order for station.
open FH, ">$temppath/$wo4name" or
		die "Can't create $temppath/$wo4name.";

print FH <<"wo4_end";
<?xml version="1.0" encoding="UTF-8"?>
<FilePacket status="C"
    		label="My filename is $wo4name. - by Randy"
    		notify="ftp:discette/$ftppath"
    		messageFormat="LEGACY"
    		destination="sftp:yy.zz.com/dir1"
    		max_granule_count="1"
    		numAttempt="1"
    		completed="2006-05-01 12:00:15" >
    <FileGroup>
    	<File status="C"
    		url="${myproto}://$myhost.gsfc.nasa.gov/data/dgroup/dset/2006/365/$mydata"
    		localPath="$temppath/$mydata"
    		cleanup="N"
    		completed="2006-05-01 12:00:11" />
    	<File status="C"
    		url="${myproto}://$myhost.gsfc.nasa.gov/data/dgroup/dset/2006/365/$myxml"
    		localPath="$temppath/$myxml"
    		cleanup="N"
    		completed="2006-05-01 12:00:14" />
    </FileGroup>
    <FileGroup>
    	<File status="C"
    		url="${myproto}://$myhost.gsfc.nasa.gov/data/dgroup/dset/2006/365/$mycrdata"
    		localPath="$temppath/$mycrdata"
    		cleanup="N"
    		completed="2006-05-01 12:00:11" />
    	<File status="C"
    		url="${myproto}://$myhost.gsfc.nasa.gov/data/dgroup/dset/2006/365/$myxml2"
    		localPath="$temppath/$myxml2"
    		cleanup="N"
    		completed="2006-05-01 12:00:14" />
    </FileGroup>
</FilePacket>
wo4_end

close FH;

# Run the script.
print "===Running s4pa_create_DN.pl===\n";
my $stdouttext = `perl $debug blib/script/s4pa_create_DN.pl $temppath $temppath/$wo4name`;
print "$stdouttext\n============\n" if ($stdouttext ne '');

# Check the output.
is($?, 0, 's4pa_create_DN.pl return code.');
# Find most recent (highest named) .notify file.
my @outfiles = glob ("/ftp/$ftppath/DN*of2.notify");
is(scalar(@outfiles), 2, "Two DNs produced");
# Look at one.
$outfile = $outfiles[0];
if ($outfile) {
    # Echo DN to output.
    open (FH, $outfile) or die "Can't open $outfile";
    local $/;
    my $legDN = <FH>;
    print "====DN Output========\n$legDN\n============\n";
    $legDN=~ s/FILENAME:\s*(\S+)//s;
    $legDN=~ s/FILENAME:\s*(\S+)//s;
    unlike($legDN, qr/FILENAME/, 'Just one granule');
}

# Clean up.
if (not $debug) {
        `rm -fr $temppath`;
        unlink glob("/ftp/$ftppath/DN*");
}




