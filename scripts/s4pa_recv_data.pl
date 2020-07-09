#!/usr/bin/perl

=head1 NAME

s4pa_recv_data - station script to receive data for S4PA

=head1 SYNOPSIS

s4pa_recv_data.pl
[B<-f> I<config_file>]
[B<-p> I<PAN_dir/PDR_dir>]
[B<-h>]
[B<-v>]

=head1 DESCRIPTION

s4pa_recv_data.pl is an S4P station script to receive data from an
external provider.  It takes as input a PDR, whereupon it transfers
the data to a local area, extracts the metadata, and outputs a
PAN, as well as output PDR work orders.

If the resulting PAN is a SHORTPAN, i.e., all files successful,
the exit code is 0.  However, if the PAN is a LONGPAN, the exit code is 100.

=head1 ARGUMENTS

=over 4

=item B<-f> I<config_file>

The configuration file contains a Perl hash that maps dataset
(shortname) to the script/program used to extract metadata from data files
of that type. For example:

  # Config file for s4pa_recv_data.cfg
  my $metadata_method_dir = "methods/metadata";
  %cfg_metadata_methods = (
    "test1" => "$metadata_method_dir/s4pa_test1_metadata.pl",
  );


The default configuration file path is "../s4pa_recv_data.cfg".

=item B<-p> I<PAN_dir/PDR_dir>

Local directory in which to put output PAN and save a copy of the incoming PDR.
Default is ".", which is the
current job directory.  When this is used, make sure to configure the output
work order to go to a downstream station.

=item B<-v>

Verbose mode.

=item B<-h>

Help -- print out usage information.

=back

=head1 CHANGELOG
06/09/21 J Pan Implemented push work order and email PAN

=head1 SEE ALSO

L<S4PA::ReceiveData>

=head1 AUTHOR

Christopher Lynnes, NASA/GSFC, Code 902.
M. Hegde

01/11/06 A Eudell     Added PAN for compression/decompression errors
=cut

################################################################################
# s4pa_recv_data.pl,v 1.116 2007/09/04 10:11:27 mhegde Exp
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use Getopt::Std;
use File::Basename;
use File::Copy;
use XML::LibXML;
use S4PA;
use S4PA::Receiving;
use S4PA::Storage;
use S4P;
use S4P::PAN;
use S4P::PDR;
use S4P::FileGroup;
use S4P::TimeTools;
use Net::FTP;
use Safe;
use Log::Log4perl;
use vars qw($opt_f $opt_h $opt_p $opt_v $cfg_metadata_methods);


# Parse command-line for PAN directory, config file
getopts('f:hp:s:v');
usage() if $opt_h;
my $pan_dir = $opt_p || '.';
my $cfg_file = $opt_f || '../../s4pa_recv_data.cfg';
my $pdr_file = shift(@ARGV) or usage();
my $job_id = basename($pdr_file);
$job_id =~ s/^DO\.//;
my $job_suffix;
if ( $job_id =~ /(.*)\.([^\.]+)$/ ) {
    $job_id = $1;
    $job_suffix = $2;
} else {
    $job_id = undef;
}

S4P::perish( 1, "Job ID not found for $pdr_file" ) unless defined $job_id;
S4P::perish( 1, "Job ID is an empty string for $pdr_file" ) if $job_id eq '';

my $verbose = $opt_v;
my $message;    # variable to hold log messages

# Check for necessary files
S4P::perish( 1, "PAN directory '$pan_dir' does not exist") unless (-d $pan_dir);
S4P::perish( 1, "Config file '$cfg_file' does not exist") unless (-f $cfg_file);
S4P::perish( 1, "PDR file '$pdr_file' does not exist") unless (-f $pdr_file);

# Read configuration file
my $cpt = new Safe 'CFG';
$cpt->share( '%cfg_metadata_methods', '%cfg_compress', '%cfg_pan_destination',
             '%cfg_access', '%cfg_uncompress', '%cfg_disk_limit',
             '%cfg_protocol', '%cfg_data_to_dif', '%cfg_root_url' );
$cpt->rdo($cfg_file) or
    S4P::perish(2, "Cannot read config file $cfg_file in safe mode: ($!)");
S4P::logger( 'INFO', "Got metadata_methods from config file $cfg_file" )
    if $verbose;

my $logger = S4PA::CreateLogger( $CFG::cfg_logger{FILE},
    $CFG::cfg_logger{LEVEL} ) if ( %CFG::cfg_logger );

# Disk usage limit
$CFG::cfg_disk_limit{max} = 0.95 unless ( defined $CFG::cfg_disk_limit{max} );
S4P::perish( 2, "Max disk limit ($CFG::cfg_disk_limit{max}) must be < 1.0" )
    if ( $CFG::cfg_disk_limit{max} > 1.0 );
# File size margin used to reserve space before downloading
$CFG::cfg_file_size_margin = 1.0 unless ( defined $CFG::cfg_file_size_margin );
S4P::perish( 2, "File size margin ($CFG::cfg_file_size_margin) must be > 0" )
    if ( $CFG::cfg_file_size_margin{max} < 0.0 );

# Check whether the input work order is a RETRY job
my $retryFlag = eval {
    my $xmlParser = XML::LibXML->new();
    my $dom = $xmlParser->parse_file( $pdr_file );
    $dom ? 1 : 0;
};

# If it is a retry job just move the PDR content to station directory
if ( $retryFlag ) {
    S4P::perish( 2, "At least one file has failed to transfer" );
}

$logger->info( "Processing $pdr_file" ) if defined $logger;

# Read the input PDR
my $inPdr = S4P::PDR::read_pdr( $pdr_file );
$inPdr->{NAME} = $pdr_file; # Internal S4PA use only

my $orig_system;
if ( $S4P::PDR::err ) {
    $message = "$pdr_file is an invalid PDR ($S4P::PDR::errstr)";
    S4P::logger( 'ERROR', $message );
    $logger->error( $message ) if defined $logger;
    my $str = S4P::read_file( $pdr_file );
    $orig_system = $1 if ( $str =~ /ORIGINATING_SYSTEM\s*=\s*([^;]+)\s*;/ );
} else {
    $orig_system = $inPdr->originating_system
}

unless ( defined $orig_system ) {
    $message = "Originating system missing in PDR ($pdr_file)";
    $logger->error( $message ) if defined $logger;
    S4P::perish( 10, $message );
}

# Determine remote host for pushing PAN
my $remote_host = ( exists $CFG::cfg_pan_destination{$orig_system}->{host} )
                 ? $CFG::cfg_pan_destination{$orig_system}->{host}
                 : undef;

my $remote_dir = ( exists $CFG::cfg_pan_destination{$orig_system}->{dir} )
                 ? $CFG::cfg_pan_destination{$orig_system}->{dir}
                 : undef;

my $provider_notify = exists $CFG::cfg_pan_destination{$orig_system}->{notify}
    ? $CFG::cfg_pan_destination{$orig_system}->{notify}
    : undef;

my $pan_filter = ( exists $CFG::cfg_pan_destination{$orig_system}->{panFilter} )
                 ? $CFG::cfg_pan_destination{$orig_system}->{panFilter}
                 : undef;

# Read dif_fetching configuration to get collection info.
my $difCfg = "$CFG::cfg_root/other/dif_fetcher/s4pa_dif_info.cfg";
my $difCpt = new Safe "DIF";
if (-f $difCfg) {
    $difCpt->rdo($difCfg) or S4P::perish(4, "Cannot read station config file $difCfg");
}

# Get postoffice station for PAN
my $postOfficeDir = "$CFG::cfg_root/postoffice";
unless (-d $postOfficeDir) {
    # reading station.cfg was removed since 3.43.8, to make it backward compatible,
    # reading station root directory from station.cfg file at the station.
    my $stationcfg = '../station.cfg';
    my $stationCpt = new Safe "STATION";
    $stationCpt->rdo($stationcfg) or
        S4P::perish( 4, "Cannot read station config file $stationcfg" );
    $postOfficeDir = "$STATION::cfg_root/postoffice";
}
S4P::perish( 4, "PostOffice directory, $postOfficeDir, doesn't exist" )
    unless ( -d $postOfficeDir );

if ( $S4P::PDR::err == 0 ) {
    # Set the attempt counter for the PDR
    $inPdr->attempt( 1 ) unless defined $inPdr->attempt();
    S4P::logger( "INFO", "Checking support for data types in $pdr_file" );
    my $count = 1;
    foreach my $fileGroup ( @{$inPdr->file_groups} ) {
        my $dataType = $fileGroup->data_type;
        my $dataVersion = $fileGroup->data_version;
        S4P::perish( 5,
            "File group #$count doesn't have a data type in $pdr_file" )
             unless defined $dataType;
        if (  defined $CFG::cfg_access{$dataType}{$dataVersion}
            || defined $CFG::cfg_access{$dataType}{''} ) {            
        } else {
            S4P::logger( "ERROR",
                "Data type/version for file group #$count is not supported" );
            $S4P::PDR::err = 100;
            $S4P::PDR::errstr .= "DATA_TYPE = $dataType;\n"
                . "DISPOSITION = \"INVALID DATA TYPE\";\n";
        }
        $count++;
    }
    S4P::logger( "INFO", "Completed the data support check for $pdr_file" );
} 

if ( $S4P::PDR::err != 0 ) {
    my $msg = '';
    if ( $S4P::PDR::err == 1 ) {
        $msg = "MESSAGE_TYPE = SHORTPDRD\;\n"
            . "DISPOSITION = \"INVALID OR UNREADABLE FILE\"\;"
    } elsif ( $S4P::PDR::err == 2 ) {
        $msg = "MESSAGE_TYPE = SHORTPDRD\;\n"
            . "DISPOSITION = \"INVALID FILE GROUP\"\;";
    } elsif ( $S4P::PDR::err == 3 ) {
        $msg = "MESSAGE_TYPE = SHORTPDRD\;\n"
            . "DISPOSITION = \"INVALID FILE COUNT\"\;";
    } elsif ( $S4P::PDR::err == 100 ) {
        $msg = "MESSAGE_TYPE = LONGPDRD\;\n"
            . "NO_FILE_GROUPS = " . scalar( @{$inPdr->file_groups} ) . "\;\n"
            . $S4P::PDR::errstr;
    }
    my ($file, $dir, $ext) = fileparse( $pdr_file, $job_suffix );
    $file =~ s/^DO\.//;
    my $pdrd_file = $pan_dir . '/' . $file . 'PDRD';
    open PDRD, ">$pdrd_file"
        || S4P::perish(  "Cannot write to $pdrd_file: $!" );
    print PDRD "$msg\n";
    close PDRD || S4P::perish( 6, "Cannot write to $pdrd_file: $!" );

    # execute pan/pdrd conversion script if defined
    my $skipPDRD = 0;
    if ( defined $pan_filter ) {
        # panFilter script is required to return a file path to the 
        # converted pan or pdrd file.
        my $filterResponse = `$pan_filter $pdrd_file`;
        S4P::perish( 100, "Failed to run PAN filter $pan_filter on $pdrd_file " .
            "($?)" ) if ($?);
        # replace the pdrd file with the converted file path if pan filter return positive
        # otherwise, reset skipPDRD flag to indicate nothing need to be returned.
        if ( $filterResponse ) {
            chomp $filterResponse;
            S4P::logger( 'INFO', "Success in converting $pdrd_file to $filterResponse." );
            $pdrd_file = $filterResponse;
        } else {
            S4P::logger( 'INFO', "Skip PDRD conversion from $pan_filter." );
            $skipPDRD = 1;
        }
    }

    # Push out the PDRD, unless pan filter return nothing.
    unless ( $skipPDRD ) {
        my @woFileList = ();
        if ( defined $remote_host && defined $remote_dir ) {
            my $protocol = $CFG::cfg_protocol{$remote_host} || 'FTP';
            my $woFile = 'DO.PUSH.' . $job_id . '.1.wo';
            push( @woFileList, $woFile )
                if  create_wo( $woFile,
                    { dest => lc($protocol) . ":$remote_host/$remote_dir",
                      location => $pdrd_file,
                      cleanup => "N" } );
        }
        if (defined $provider_notify) {
            my $email_file = $pdrd_file . '.email';
            create_email_file( $email_file, $pdrd_file ) or
                S4P::perish( 101,
                    "Fail to create PDRD file, $email_file, for e-mailing");
            my $woFile = 'DO.PUSH.' . $job_id . '.2.wo';
            push( @woFileList, $woFile )
                if create_wo( $woFile,
                   { dest => "mailto:$provider_notify",
                     location => $email_file,
                     cleanup => "Y"} );
        }
        # Move PostOffice work orders for PDRD transfer.
        foreach my $woFile ( @woFileList ) {
            move( $woFile, $postOfficeDir );
        }
    }

    $message = "PDR was not valid; wrote PDRD to $pdrd_file";
    S4P::perish( 100, $message );
    $logger->error( $message );
}

# Call ReceiveData with PDR file as argument
my ( $outPdr ) = S4PA::Receiving::ReceiveData( $inPdr, $pdr_file );
unless (defined $outPdr) {
    S4P::perish( 1, "PDR file $pdr_file failed transfer");
}

# Loop over each file group that was successfully transferred
FILE_GROUP: foreach my $fileGroup ( @{$outPdr->file_groups} ) {
    next unless ( $fileGroup->status() eq 'SUCCESSFUL' );
    
    my $dataset = $fileGroup->data_type();
    my $dataVersion = $fileGroup->data_version;
    my $metFile = $fileGroup->met_file() ? $fileGroup->met_file() : undef;
    my $browseFile = $fileGroup->browse_file()
    ? $fileGroup->browse_file() : undef;
    my $mapFile = $fileGroup->map_file()
    ? $fileGroup->map_file() : undef;
    my @dataFileList = $fileGroup->data_files();
    my $fileGroupError = undef;
    # A hash to remember file mappings: input file is the key and output file
    # is the value.
    my $fileMap = {};

    # Make sure all files in the file group exist
    FILE_LIST: foreach my $file ( @dataFileList, $metFile, $browseFile, $mapFile ) {
        next FILE_LIST unless defined $file;
        unless ( -f $file ) {
            $message = "$file downloaded; but, does not exist";
            S4P::logger( "ERROR", $message );
            $logger->error( $message ) if defined $logger;
            $fileGroup->status( 'FAILURE' );
            $fileGroupError = "METADATA PREPROCESSING ERROR";
            last FILE_LIST;
        }
    }

    # Try uncompressing if configured
    if ( ( $fileGroup->status() eq 'SUCCESSFUL' )
        && defined $CFG::cfg_uncompress{$dataset} ) {
        # For each file in the file group except for metadata files,
        # uncompress, recalculate size and filename.
        UNCOMPRESS: foreach my $file_spec ( @{$fileGroup->file_specs} ) {
            next if ( $file_spec->file_type =~ /METADATA|BROWSE|HDF4MAP|QA/ );
            my $old_file = $file_spec->pathname();
            my ( $new_file,
                $new_size ) = S4PA::Storage::Uncompress( $old_file, $dataset );
            if ( defined $new_file && defined $new_size ) {
                $message = "Decompressed $old_file to $new_file";
                S4P::logger( "INFO", $message );
                $logger->debug( $message ) if defined $logger;
                $file_spec->pathname( $new_file );
                $file_spec->file_size( $new_size );
                $file_spec->checksum();
                $fileMap->{basename($old_file)} = basename( $new_file );
            } else {
                $message = "Failed to decompress $old_file";
                S4P::logger( "ERROR", $message );
                $logger->error( $message ) if defined $logger;
                $fileGroupError = "DATA CONVERSION ERROR";
                $file_spec->status( $fileGroupError );
                $fileGroup->status( 'FAILURE' );
                last UNCOMPRESS;
            } 
        } # End of foreach my $file_spec
    } # End of if ( defined $CFG::cfg_uncompress{$dataset} )
    
    # Try extracting metadata if configured
    if ( ( $fileGroup->status() eq 'SUCCESSFUL' ) && 
        defined $CFG::cfg_metadata_methods{$dataset} ) {
        # Get data files again as they may have been uncompressed by S4PA 
        @dataFileList = $fileGroup->data_files();
        S4P::logger( 'INFO',
            "Extracting metadata from ". join( ',', @dataFileList ) 
            . " using $CFG::cfg_metadata_methods{$dataset}" );

        # Extract metadata
        my $rh_metadata = S4PA::Receiving::GetMetadata(
            $CFG::cfg_metadata_methods{$dataset},
            @dataFileList, ($metFile ? $metFile : ()) );
        # If metadata generation failed, mark the file group as bad.
        if ( ! $rh_metadata ) {
            S4P::logger( "ERROR", "Metadata extraction failed" );
            $logger->error( "Failed to extract metadata for "
                . join( ',', @dataFileList ) ) if defined $logger;
            $fileGroupError = "METADATA PREPROCESSING ERROR";
            $fileGroup->status( 'FAILURE' );
        } else {
            $logger->info( "Extracted metadata for " 
                . join( ',', @dataFileList ) ) if defined $logger;
            # Generate metadata in XML
            my $xml = (ref $rh_metadata eq 'HASH' )
                ? S4PA::Receiving::Metadata2XML( %$rh_metadata )
                : $rh_metadata;
            if ( defined $xml ) {
                # Add a met file to file group if none defined.
                unless ( defined $metFile ) {
                    $metFile = $dataFileList[0] . '.xml'; 
                    $fileGroup->add_file_spec( $metFile, 'METADATA' );
                }
                unless ( S4P::write_file( $metFile, $xml ) ) {
                    S4P::logger( "ERROR",
                        "Failed to write metadata to $metFile" );
	                  $fileGroupError = "METADATA PREPROCESSING ERROR";
                    $fileGroup->status( 'FAILURE' );
                }
            } else {
                S4P::logger( 'ERROR', "Failed to generate metadata" );
                $fileGroupError = "METADATA PREPROCESSING ERROR";
                $fileGroup->status( 'FAILURE' );
            }
        }
        # Make every file in the granule fail if metadata extraction fails.
        if ( $fileGroup->status() eq 'FAILURE' ) {
            foreach my $file_spec ( @{$fileGroup->file_specs} ) {
                $file_spec->status( 'METADATA PREPROCESSING ERROR' );
            }
        }
    } elsif ( not defined $metFile ) {
        S4P::logger( 'ERROR', "File group containing " 
            . join( ',', @dataFileList ) 
            . ", neither has the metadata file nor a metadata extractor" );
        $fileGroup->status( 'FAILURE' );
        $fileGroupError = "METADATA NOT FOUND";
    }

    # Check if HDF4 Map file is needed
    my $mapExtractCommand = S4PA::Receiving::NeedMapFile( $fileGroup ); 
    if ( ( $fileGroup->status() eq 'SUCCESSFUL' ) && $mapExtractCommand ) {
        # Get data files again as they may have been uncompressed by S4PA 
        @dataFileList = $fileGroup->data_files();
        S4P::logger( 'INFO',
            "create HDF4 map from $dataFileList[0] using $mapExtractCommand" );

        # Create hdf4 map
        $mapExtractCommand .= " $dataFileList[0] $metFile";
        my $mapFile = `$mapExtractCommand`;
        # note:  when backticks are used status of command is returned in $?
        # If metadata generation failed, mark the file group as bad.
        if ( $? ) {
            S4P::logger( "ERROR", "HDF4 map creation failed: $!" );
            $logger->error( "Failed to create HDF4 map: $!" )
                if defined $logger;
	    $fileGroupError = "METADATA PREPROCESSING ERROR";
            $fileGroup->status( 'FAILURE' );
        } else {
            chomp( $mapFile );
            if ( ! -s $mapFile ) {
                S4P::logger( "ERROR", "HDF4 map file is empty." );
                $logger->error( "HDF4 map is empty: $mapFile" )
                    if defined $logger;
	        $fileGroupError = "METADATA PREPROCESSING ERROR";
                $fileGroup->status( 'FAILURE' );
            } else {
                $logger->info( "Created HDF4 map file for $dataFileList[0]" )
                    if defined $logger;
                # Add the map file to file group
                $fileGroup->add_file_spec( $mapFile, 'HDF4MAP' );
            }
        }
        # Make every file in the granule fail if map file creation failed.
        if ( $fileGroup->status() eq 'FAILURE' ) {
            foreach my $file_spec ( @{$fileGroup->file_specs} ) {
                $file_spec->status( 'METADATA PREPROCESSING ERROR' );
            }
        }
    }

    # Try compressing data if configured.
    if ( ( $fileGroup->status() eq 'SUCCESSFUL' ) &&
        defined $CFG::cfg_compress{$dataset} ) {
        # For each file in the file group except for metadata files,
        # compress and recalculate checksum, size and filename.
        COMPRESS: foreach my $file_spec ( @{$fileGroup->file_specs} ) {
            next if ( $file_spec->file_type =~ /METADATA|BROWSE|HDF4MAP|QA/ );
            my $old_file = $file_spec->pathname();
            my ( $new_file,
                $new_size ) = S4PA::Storage::Compress( $old_file, $dataset );
            # If compression is successful, modify file spec object
            # to reflect new values.
            if ( defined $new_file && defined $new_size ) {
                $message = "Compressed $old_file to $new_file";
                S4P::logger( "INFO", $message );
                $logger->debug( $message ) if defined $logger;
                $file_spec->pathname( $new_file );
                $file_spec->file_size( $new_size );
                $file_spec->checksum();
                $fileMap->{basename($old_file)} = basename( $new_file );
            } else {
                # If compression fails, mark the file group as failed
                # and exit the loop over file specs.
                $message = "Failed to compress $old_file";
                S4P::logger( "ERROR", $message );
                $logger->error( $message ) if defined $logger;
                $fileGroupError = "DATA CONVERSION ERROR";
                $file_spec->status( $fileGroupError );
                $fileGroup->status( 'FAILURE' );
                last COMPRESS;
            }
        } # End of foreach my $file_spec
    }   # End of  if ( $CFG::cfg_compress{$dataset} )    

    # Update the metadata; on failure to do so, fail the file group.
    if ( $fileGroup->status() eq 'SUCCESSFUL' ) {
        unless ( S4PA::Receiving::UpdateMetadata( $fileGroup, $fileMap ) ) {
            $fileGroupError = "METADATA PREPROCESSING ERROR";
            $fileGroup->status( 'FAILURE' );
            foreach my $fileSpec ( @{$fileGroup->file_specs} ) {
                $fileSpec->status( $fileGroupError );
            }
        }
    }

    # If the file group has been successfully processed, continue to the next.
    next FILE_GROUP if ( $fileGroup->status() eq 'SUCCESSFUL' );
    # If not, find the corresponding file group in the input PDR and set its
    # status as FAILURE.
    my @inFileGroupList = @{$inPdr->file_groups};
    my $index = $fileGroup->{index};
    $inFileGroupList[$index]->status( 'FAILURE' );
    foreach my $fileSpec ( @{$inFileGroupList[$index]->file_specs} ) {
	$fileSpec->status( $fileGroupError );
    }
} # End of foreach my $fileGroup

# Move successfully processed data and cleanup failed file groups
# QA should have been inserted into metadata file at this point
# drop the QA file from the fileGroup and update PDR for downstream
my ( $moveStatus, $newPdr ) = S4PA::Receiving::MoveProcessedFiles( $inPdr, $outPdr );
S4P::perish( 1, "Failed to cleanup" ) unless ( $moveStatus );

my $dateStamp = S4P::TimeTools::CCSDSa_Now();
my $pan = S4P::PAN->new( $inPdr );
if ( $inPdr->status() eq 'SUCCESSFUL' ) {
    $pan->{TIME_STAMP} = $dateStamp;
} else {
    # Create a long PAN
    foreach my $fileGroup ( @{$inPdr->file_groups} ) {
	foreach my $fileSpec ( @{$fileGroup->file_specs} ) {
            if ( ($fileGroup->status() eq 'FAILURE') 
                && ($fileSpec->status() eq 'SUCCESSFUL') ) {
                $pan->disposition( $fileSpec->pathname,
                    "ASSOCIATED FILE FAILURE", $dateStamp );
            } else {
	        $pan->disposition( $fileSpec->pathname, 
                    $fileSpec->status(), $dateStamp );
	    }
        }
    }
}

# Create work orders for StoreData stations for only successful file groups.
my $downstreamPdr = {};
foreach my $fileGroup ( @{$newPdr->file_groups} ) {
    next unless ( $fileGroup->status() eq 'SUCCESSFUL' );
    my $dataset = $fileGroup->data_type();
    my $version = $fileGroup->data_version();
    unless ( defined $downstreamPdr->{$dataset} ) {
        $downstreamPdr->{$dataset} = S4P::PDR::create();
        $downstreamPdr->{$dataset}->originating_system(
            $newPdr->originating_system() )
            if ( defined $newPdr->originating_system() );
    }
    $downstreamPdr->{$dataset}->add_file_group( $fileGroup );
    my $perm = S4PA::Receiving::GetAccessType( $dataset, $version, 'FILE' );
    foreach my $fileSpec ( @{$fileGroup->file_specs} ) {
        chmod( $perm, $fileSpec->pathname );
    }
}

my $woSuffix = '.N' . $inPdr->attempt()  . '.wo';
foreach my $dataset ( keys %$downstreamPdr ) {
    my $woFile = "STORE_" . $dataset . "." . $job_id . $woSuffix;
    if ( $downstreamPdr->{$dataset}->write_pdr( $woFile ) ) {
        S4P::perish( 1, "Failed to write PDR for $dataset: $woFile" );
    } else {
        S4P::logger( "INFO", "Wrote PDR for $dataset: $woFile" );
    }
}

# Create the PAN
my $pan_file = $pan_dir . '/';
my ($file, $dir, $ext) = fileparse( $pdr_file, $job_suffix );
$file =~ s/^DO\.//;
$pan_file .= $file;
my $edos_pan_file;
if ( $inPdr->is_edos() ) {
    my @time = localtime();
    # Convention=IYYYYDDDHHMMSS
    $time[5] -= 100; # Convert year in to two digits
    $edos_pan_file = $pan_dir . '/I' . sprintf( "%2.2d%3.3d%2.2d%2.2d%2.2d",
        $time[5], $time[7], $time[2], $time[1], $time[0] );;
    $edos_pan_file .= ( $inPdr->is_expedited() ? '.EAN' : '.PAN' );
    $pan_file .= ( $inPdr->is_expedited() ? 'EAN' : 'PAN' );
} else {
    $pan_file .= 'PAN';
}

# Copy PDR's EDOS header
$pan->edos_header( $inPdr );
if ( $pan->write( $pan_file ) ) {
 
    # execute pan/pdrd conversion script if defined
    my $skipPAN = 0;
    if ( defined $pan_filter ) {
        # panFilter script is required to return a file path to the 
        # converted pan or pdrd file.
        my $filterResponse = `$pan_filter $pan_file`;
        S4P::perish( 100, "Failed to run PAN filter $pan_filter on $pan_file " .
            "($?)" ) if ($?);
        # replace the pan file with the converted file path if pan filter return positive
        # otherwise, do nothing.
        if ( $filterResponse ) {
            chomp $filterResponse;
            S4P::logger( 'INFO', "Success in converting $pan_file to $filterResponse." );
            $pan_file = $filterResponse;
        } else {
            S4P::logger( 'INFO', "Skip PAN conversion from $pan_filter." );
            $skipPAN = 1;
        }
    }

    # Symlink EDOS pan file name to the local PAN file. This is done so that
    # EDOS PDR/PAN cleanup is backward compatible.
    if ( $inPdr->is_edos() ) {
        $pan_file = $edos_pan_file if symlink( $pan_file, $edos_pan_file );
    }
    # EDOS does not want successful PAN back, bugs #546.
    if ( ( not $inPdr->is_edos() )
        || ( $inPdr->is_edos() && $pan->msg_type() eq 'LONGPAN' ) ) {        
        # A list to accumulate PostOffice work orders for pushing PAN/PDRD files
        unless ( $skipPAN ) {
            my @woFileList = ();
            if ( defined $remote_host && defined $remote_dir ) {
                my $protocol = $CFG::cfg_protocol{$remote_host} || 'FTP';
                my $woFile = 'DO.PUSH.file.' . $job_id . $woSuffix;
                push( @woFileList, $woFile )
                    if create_wo( $woFile,
                        {
                            dest => lc($protocol) . ":$remote_host/$remote_dir",
                            location => $pan_file, cleanup => "N"
                        } );
            }
            if (defined $provider_notify) {
                my $email_file = $pan_file . '.email';
                create_email_file( $email_file, $pan_file ) or
                    S4P::perish( 101,
                        "Fail to create PAN file, $email_file, for e-mailing");
                my $woFile = 'DO.PUSH.email.' .  $job_id . $woSuffix;
                push( @woFileList, $woFile )
                    if create_wo( $woFile,
                        {
                            dest => "mailto:$provider_notify",
                            location => $email_file, cleanup => "Y"
                        } );
            }
            # Move PostOffice work orders for PAN/PDRD transfer
            foreach my $woFile ( @woFileList ) {
                move( $woFile, $postOfficeDir );
            }
        }
    }    
} else {
    S4P::logger( "ERROR", "Failed to write PAN: $pan_file" );
}

# Write out the work order to continue processing the partially succeeded PDR
if ( $pan->msg_type eq 'LONGPAN' ) {
    my $woFile = "RETRY.$job_id.wo";
    # Get the original work order name
    my @jobStatus = S4P::check_job( "." );
    $jobStatus[3] = $pdr_file unless defined $jobStatus[3];
    my $xmlParser = XML::LibXML->new();
    $xmlParser->keep_blanks( 0 );
    my $dom = $xmlParser->parse_string( "<workOrder />" );
    my $doc = $dom->documentElement();
    $doc->appendTextChild( 'original', $jobStatus[3] );
    $doc->appendTextChild( 'pan', $pan_file ) if ( -f $pan_file );
    $inPdr->attempt( $inPdr->attempt() + 1 );
    $doc->appendTextChild( 'content', $inPdr->sprint( 1 ) );
    open( FH, ">$woFile" )
        || S4P::perish( 1, "Failed to open $woFile for writing" );
    print FH $dom->toString( 1 ), "\n";
    close( FH ) || S4P::perish( 1, "Failed to close $woFile for writing" );
} else {
    copy( $pdr_file, $opt_p );
}
exit ( 0 );
################################################################################
sub usage {
    die << "EOF";
Usage: $0 [-f config_file] [-p pan_dir] pdr_file
  -f config_file:  Configuration file w/ metadata method map (default=../s4pa_recv_data.cfg)
  -p pan_dir:      Directory for outgoing PAN (default='.')
EOF
}
################################################################################
sub create_email_file
{
    my ($email_file, $pdrd_file) = @_;

    if (-z $pdrd_file) {
        warn("WARNING: File $pdrd_file empty");
        return undef;
    }

    my $pdrd = `cat $pdrd_file`;

    local( *FH );
    open( FH, ">$email_file" ) or return undef;
    print FH "Subject: " . basename($pdrd_file) . "\n\n";
    print FH $pdrd;
    unless ( close(FH) ) {
        S4P::logger( "ERROR", "Failed to close $email_file ($!)" );
        unlink $email_file;
    }
    return ( -f $email_file ? 1 : 0 );
}
################################################################################
sub create_wo
{
    my ($wo_file, $info) = @_;

    local( *FH );
    open( FH, ">$wo_file" ) or return undef;

    print FH<<CONTENT;
<FilePacket status="I" destination="$info->{dest}">
    <FileGroup>
        <File localPath="$info->{location}" status="I" cleanup="$info->{cleanup}"/>
    </FileGroup>
</FilePacket>
CONTENT

    unless ( close(FH) ) {
        S4P::logger( "ERROR", "Failed to close $wo_file ($!)" );
        unlink $wo_file;
    }
    return ( -f $wo_file ? 1 : 0 );
}

