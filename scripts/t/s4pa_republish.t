#!/usr/bin/perl

=head1 NAME

s4pa_republish.t - test script to republish metadata

=head1 ARGUMENTS

=over 4

=item B<-d>

Debug flag: keep output file.

=back

=head1 AUTHOR

Dr. C. Wrandle Barth, ADNET, NASA/GSFC, Code 610.2, Greenbelt, MD  20771.

=head1 CHANGE LOG

9/14/06 Initial version
10/5/06 Tests for file names added.

=cut

use strict;
use POSIX;
use S4P;
use S4P::PDR;
use S4P::FileGroup;
use S4P::FileSpec;
use XML::LibXML;
use vars qw($opt_d);
use Getopt::Std;
use Test::More 'no_plan';

getopts('d');
my $debug = $opt_d;
# Define path for test files.
my $temppath = '/var/tmp/s4pa_republish_test';
# Define data and metadata filenames.
my $mydata = 'mydata';
my $mycrdata = "$mydata.cr.txt";
my $myxml = "$mydata.xml";
my $mydata2 = 'G2B31.20060909.50242.6.BIN.Z';
my $myxml2 = "$mydata2.xml";




# Clean up from previous run.
`rm -fr $temppath` if -d $temppath;
mkdir $temppath or die "Can't make temporary directory $temppath.";
mkdir "$temppath/data" or die "Can't make subdirectory $temppath/data.";
mkdir "$temppath/symdata" or die "Can't make subdirectory $temppath/symdata.";
mkdir "$temppath/symdata/sub" or die "Can't make subdirectory $temppath/symdata/sub.";

# Create data file
open FH, ">$temppath/data/$mydata" or
		die "Can't create $temppath/data/$mydata.";
print FH "She blinded me with science!\n";
close FH;
my $mydata_size = -s "$temppath/data/$mydata";

# Create construction record file
open FH, ">$temppath/data/$mycrdata" or
		die "Can't create $temppath/data/$mycrdata.";
print FH "I've been workin' on the railroad!\n";
close FH;
my $mycrdata_size = -s "$temppath/data/$mycrdata";

# Create metadata file for extracting information
open FH, ">$temppath/data/$myxml" or
		die "Can't create $temppath/data/$myxml.";

print FH <<"xml_end";
<?xml version="1.0" encoding="UTF-8"?>
<S4PAGranuleMetaDataFile xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance " xsi:noNamespaceSchemaLocation="http://disc.gsfc.nasa.gov/xsd/s4pa/S4paGranule.xsd">
  <CollectionMetaData>
    <LongName>XYZZY Test Data Long Name based on SeaWiFS metadata file</LongName>
    <ShortName>XYZZY</ShortName>
    <VersionID>002</VersionID>
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
          <CheckSumValue>1234</CheckSumValue>
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
</S4PAGranuleMetaDataFile>
xml_end

close FH;
my $myxml_size = -s "$temppath/data/$myxml";

# Create second data file
open FH, ">$temppath/data/$mydata2" or
		die "Can't create $temppath/data/$mydata2.";
print FH "Gee, Mr. Wizard!\n";
close FH;
my $mydata_size2 = -s "$temppath/data/$mydata2";


# Create metadata file for extracting information
open FH, ">$temppath/data/$myxml2" or
		die "Can't create $temppath/data/$myxml2.";

print FH <<"xml_end";
<?xml version="1.0" encoding="UTF-8"?>
<S4PAGranuleMetaDataFile xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance " xsi:noNamespaceSchemaLocation="http://disc.gsfc.nasa.gov/xsd/s4pa/S4paGranule.xsd">
  <CollectionMetaData>
    <LongName>Gridded TRMM Combined Instrument (TCI) Rainfall Data</LongName>
    <ShortName>TRMM_G2B31</ShortName>
    <VersionID>6</VersionID>
  </CollectionMetaData>
  <DataGranule>
    <GranuleID>$mydata2</GranuleID>
    <Format>BIN</Format>
    <CheckSum>
      <CheckSumType>CRC32</CheckSumType>
      <CheckSumValue>1090638325</CheckSumValue>
    </CheckSum>
    <SizeBytesDataGranule>50</SizeBytesDataGranule>
    <InsertDateTime>2005-08-16 13:51:47</InsertDateTime>
    <ProductionDateTime>2005-06-22 00:24:25</ProductionDateTime>
  </DataGranule>
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
</S4PAGranuleMetaDataFile>
xml_end

close FH;
my $myxml_size2 = -s "$temppath/data/$myxml2";

# Put symbolic links to real data.
link "$temppath/data/$mydata", "$temppath/symdata/$mydata";
link "$temppath/data/$mycrdata", "$temppath/symdata/$mycrdata";
link "$temppath/data/$myxml", "$temppath/symdata/$myxml";
link "$temppath/data/$mydata2", "$temppath/symdata/sub/$mydata2";
link "$temppath/data/$myxml2", "$temppath/symdata/sub/$myxml2";

# Run the script.
print "===Running s4pa_republish.pl===\n";
my $stdouttext = `s4pa_republish.pl -o $temppath/test1_ -e $temppath/echofile.txt -n 50 $temppath/symdata`;
print "$stdouttext\n============\n" if ($stdouttext ne '');

# Check the output.
is($?, 0, 's4pa_s4pa_republish.pl return code.');
my @outfiles = glob ("$temppath/*.PDR");
is(scalar @outfiles, 1, "Test for one PDR created.");
my $outfile = $outfiles[0];
if ($outfile) {
    # Echo PDR to output.
    print "====PDR Output========\n", `cat $outfile`, "============\n";
    my $pdr = S4P::PDR::read_pdr($outfile);
    # Check the PDR-level fields.
    is($pdr->originating_system, "S4PA", 'Originating system test.');
    is($pdr->total_file_count, 5, 'Should be 5 files here.');
    my @file_groups = @{ $pdr->file_groups };
    is(scalar @file_groups, 2, 'Should be 2 groups.');
    is(scalar $pdr->files('SCIENCE'), 3, 'Should be 3 science files.');
    is(scalar $pdr->files('METADATA'), 2, 'Should be 2 metadata files.');

    foreach my $fg (@file_groups) {
    # Check the first FILE_GROUP-level fields.
        my $type = $fg->data_type;
        if ($type eq 'XYZZY') {
            is($fg->data_version, '002', 'Version check in group.');
            my $node = $fg->node_name;

            foreach my $fs (@{$fg->file_specs}) {
            	# Check the FILE_SPEC fields.
            	my $fn = $fs->file_id;
            	# Should report the symbolic directory.
            	is($fs->directory_id, "$temppath/symdata", 'Directory id');
            	my $fsize = $fn eq $mydata ? $mydata_size :
        			($fn eq $mycrdata ? $mycrdata_size :
        			($fn eq $myxml ? $myxml_size : 0));
            	if ($fsize == 0) {
               	    fail("Bad file name $fn");
            	} else {
                    is($fs->file_size, $fsize, "Size of $fn check.");
                    is($fs->file_type, $fn eq $myxml ? 'METADATA' : 'SCIENCE', "Type of $fn test.");
            	}
            }
        } elsif ($type eq 'TRMM_G2B31') {
            is($fg->data_version, '6', 'Version check in group.');
            my $node = $fg->node_name;

            foreach my $fs (@{$fg->file_specs}) {
            	# Check the FILE_SPEC fields.
            	my $fn = $fs->file_id;
            	is($fs->directory_id, "$temppath/symdata/sub", 'Directory id');
            	my $fsize = $fn eq $mydata2 ? $mydata_size2 :
        			($fn eq $myxml2 ? $myxml_size2 : 0);
            	if ($fsize == 0) {
               	    fail("Bad file name $fn");
            	} else {
                    is($fs->file_size, $fsize, "Size of $fn check.");
                    is($fs->file_type, $fn eq $myxml2 ? 'METADATA' : 'SCIENCE', "Type of $fn test.");
            	}
            }
        } else {
            fail("Data type bad: $type");
        }
    }
}
ok(-e "$temppath/echofile.txt", 'ECHO file created');
if (-e "$temppath/echofile.txt") {
    undef $/;
    open FH, "$temppath/echofile.txt" or die "Can't open ECHO file.";
    my $echotext = <FH>;
    $/ = "\n";
    like($echotext, qr/^XYZZY.002:$mydata/m, 'ECHO file 1');
    like($echotext, qr/^XYZZY.002:$mycrdata/m, 'ECHO file 2');
    # Don't put the XML files there.
    unlike($echotext, qr/^XYZZY.002:$myxml/m, 'ECHO file 3');
    like($echotext, qr/^TRMM_G2B31.6:$mydata2/m, 'ECHO file 4');
    unlike($echotext, qr/^TRMM_G2B31.6:$myxml2/m, 'ECHO file 5');
}


# Run the script with specific data files, one XML, one not.
print "===Running s4pa_republish.pl===\n";
$stdouttext = `s4pa_republish.pl -o $temppath/test3_ -n 30 $temppath/symdata/$myxml $temppath/data/$mydata2`;
print "$stdouttext\n============\n" if ($stdouttext ne '');

# Check the output.
is($?, 0, 's4pa_s4pa_republish.pl return code.');
@outfiles = glob ("$temppath/test3*.PDR");
is(scalar @outfiles, 1, "Test for one PDR created.");
$outfile = $outfiles[0];
if ($outfile) {
    # Echo PDR to output.
    print "====PDR Output========\n", `cat $outfile`, "============\n";
    my $pdr = S4P::PDR::read_pdr($outfile);
    # Check the PDR-level fields.
    is($pdr->originating_system, "S4PA", 'Originating system test.');
    is($pdr->total_file_count, 3, 'Should be 3 files here.');
    my @file_groups = @{ $pdr->file_groups };
    is(scalar @file_groups, 1, 'Should be 1 group.');
    is(scalar $pdr->files('SCIENCE'), 2, 'Should be 2 science files.');
    is(scalar $pdr->files('METADATA'), 1, 'Should be 1 metadata file.');

    my $fg = $file_groups[0];
    # Check the FILE_GROUP-level fields.
    if ($fg) {
        is($fg->data_type, 'XYZZY', 'Type check in group.');
        is($fg->data_version, '002', 'Version check in group.');
        my $node = $fg->node_name;

        foreach my $fs (@{$fg->file_specs}) {
        	# Check the FILE_SPEC fields.
        	my $fn = $fs->file_id;
        	is($fs->directory_id, "$temppath/symdata", 'Directory id');
        	my $fsize = $fn eq $mydata ? $mydata_size :
    			($fn eq $mycrdata ? $mycrdata_size :
    			($fn eq $myxml ? $myxml_size : 0));
        	if ($fsize == 0) {
           	    fail("Bad file name $fn");
        	} else {
                is($fs->file_size, $fsize, 'Size of $fn check.');
                is($fs->file_type, $fn eq $myxml ? 'METADATA' : 'SCIENCE', "Type of $fn test.");
        	}
        }
    }
}



# Clean up.
`rm -fr $temppath` if ! $opt_d;





