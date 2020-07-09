=head1 NAME

S4PA::Receiving - S4PA subsystem to receive data

=head1 SYNOPSIS

  use S4PA::Receiving;
  $directory = S4PA::Receiving::GetDirectory($pdrfile, $dataset, $filesize, $max_filesystem_usage, $filesize_margin);
  $rh_metadata = S4PA::Receiving::GetMetadata($storage_root, $dataset, @data_file_list );
  $ESDT = S4PA::Receiving::IdentifyDataset( $dataTypeCfg, $dataFile );
  $xml = S4PA::Receiving::Metadata2XML(%metadata);  
  $output_pdr = S4PA::Receiving::ReceiveData($input_pdr, $pdr_file);
  $status = S4PA::Receiving::put( host => $remoteHost, dir => $remoteDir, file => $localFile, protocol => SFTP/FTP/FILE );
  $sftp = S4PA::Receiving::SftpConnect($remoteHost, $login, $password, $port);
    
=head1 DESCRIPTION

TBS.

=head1 SEE ALSO

L<S4PA>, L<S4PA::Storage>

=cut

###############################################################
# Receiving.pm,v 1.97 2010/08/19 19:21:56 glei Exp
# -@@@ S4PA, Version $Name:  $
###############################################################

package S4PA::Receiving;

use 5.00503;
use strict;
use S4P;
use S4P::PDR;
use S4P::PAN;
use S4P::FileGroup;
use S4P::FileSpec;
use S4P::TimeTools;
use S4P::Connection;
use Safe;
use Net::FTP;
use Net::Netrc;
use Cwd;
use File::Basename;
use File::stat;
use File::Copy;
use XML::LibXML;
use Fcntl;
use Math::BigInt;
use Tk;
use Tk::DialogBox;
use Tk::Label;
use Net::Netrc;
use Net::SFTP::Foreign;
use vars qw($VERSION);

$VERSION = '.01';

1;

################################################################################

=head1 GetDirectory

Description:

    Finds (creates one if necessary) a directory on an empty file system to
    store data for a given dataset and given size. PDR filename is used to
    create a unique directory. Caller needs to avoid race 
    condition arising with multiple just-sub-critical requests. Assumes that 
    the symbolic link, ../active_fs, exists to the active file system.

Input:

    PDR filename, Dataset name, total size in bytes [, maximum allowed usage 
    of file system (0-1)] [, file margin that is used to guestimate disk 
    space required to store/process a given file]
    
Output:

    Returns a directory name if successulf and undefined value on failure.
    
Algorithm:

    Function GetDirectory
        While the active file system exists
            If the active file system is full
                Point the active file system to the next file system.
            Else If the active file has less than requested space
                Mark the active file system full.
                Point the active file system to the next file system. 
            Else
                If the active file system doesn't have the dataset directory
                    Create a dataset directory in the active file system.
                Endif
                Return dataset directory name.
            Endif
        Endwhile
        Return undefined.
    End

=head1 AUTHOR

Mike Theobald
M. Hegde

=cut
sub GetDirectory {
    #  Assume strict pragma in effect.
    #  Read argument list (dataset, file_size).
    my ( $file_group, $pdr_file ) = @_;

    my $dataset = $file_group->data_type();
    
    # Max file system utilization
    my $fs_max_used = defined $CFG::cfg_disk_limit{max}
                      ? $CFG::cfg_disk_limit{max} : .95;
    my $file_size_margin = ( defined $CFG::cfg_file_size_margin ? 
        $CFG::cfg_file_size_margin : 1.0 );

    # Compute total file size in bytes contained in the file group.
    my $total_file_size = 0;
    foreach my $fspec ( @{$file_group->file_specs} ) {
        $total_file_size += $fspec->file_size;
    }
    my $requiredSize = $total_file_size * ( 1.0 + $file_size_margin );
        
    # Begin looping through file systems until a file system with required
    # space is found.
    my $recvDataDir = dirname( cwd() );
    my $active_fs = "../active_fs";
    while ( -d $active_fs ) {    
        # A true value of flag indicates that the active file system is full.
        my $flag = ( -f "$active_fs/.FS_COMPLETE" ) ? 1 : 0;
        my $fsSizeFile = "$active_fs/.FS_SIZE";
        my $freeSize = undef;
                        
        # If the active file system is not flagged full yet, check to see
        # the availability of space.
        unless ( $flag ) {
            # Determine the type of disk management: file system based
            # or directory based.
	    
            if ( -f $fsSizeFile ) {
                # Case of directory based disk management
		# Deduct the reserved size from the file system size
                my @sizeList = DiskPartitionTracker( $fsSizeFile );
		
                unless ( @sizeList == 2 ) {
                    S4P::logger( "ERROR",
                        "Disk size not read from $fsSizeFile" );
                    return ( undef, undef );
                }
                
                if ( $sizeList[0]->is_nan() || $sizeList[1]->is_nan() ) {
                    S4P::logger( "ERROR",
                        "Disk size tracker file, $fsSizeFile, contains"
                        . " non-number" );
                    return ( undef, undef );
                }

                # Check whether the space is available
                $flag = 1 if ( $sizeList[1]->bcmp( $requiredSize ) < 0 );
            } else {
                # Case of file system based disk management.
                # Get file system information using UNIX df.
                my $fsinfo = (readpipe("/bin/df -k $active_fs"))[-1];
                unless ( defined $fsinfo ) {
                    S4P::logger( 'ERROR',
                        "Failed to get disk information for $active_fs ($!)" );
                    return ( undef, undef );
                }
                # Extract individual columns in file system information.
                my ( undef, 
                    $fs_size, $fs_used, $fs_free ) = split( /\s+/, $fsinfo );
                 
                # Compute available free space. Free space returned by 'df' is
                # used as the total space returned by it may not entirely
                # be available for use on journaling file systems.
                # (free+used)*max - used; computed this way to avoid problems
                # seen with df in reporting the percentage of disk that is
                # full.
                my $available_space = ( $fs_free * $fs_max_used 
                    - $fs_used * ( 1.0 - $fs_max_used ) ) * 1024;
                $flag = 1 if ( $requiredSize > $available_space );
            }
        }
        
        # Check if the active system is full
        if ( $flag ) {
            # Case of space not found on the active file system: point the
            # active file system to next possible value.
            my $fs = readlink( $active_fs );
            unless ( $fs =~ /(.+)\/(\d+)\/*$/ ) {
                S4P::logger( 'ERROR', 
                             "Expecting the active file system to point to a"
                             . " directory of the format .*/<digits>/ or"
                             . " .*/<digits>" );
		close( LOCKFH );
                return ( undef, undef );
            }
            
            my $newfs;
            my $fsListFile = $recvDataDir . "/ActiveFs.list";
            if ( ! -f $fsListFile ) {
                # Create the new file system's path name.
                $newfs = sprintf( "$1/%" . length($2) . '.' . length($2) . 'd/',
                                   $2+1 );
            } else {
                $newfs = FindNextFs( $fsListFile, $fs );
                unless ( $newfs ) {
                    S4P::logger( 'ERROR', 'Can not locate the next available volume' );
                    return( undef, undef );
                }
            }
            
            # Mark the active file system full; at any point of failure, return
            # undef.
            # only create .FS_COMPETE file if one is not already exist.
            unless ( -f "$active_fs/.FS_COMPLETE" ) {
            local( *FH );
                if ( open( FH, ">$active_fs/.FS_COMPLETE" ) ){
                    unless ( close( FH ) ) {
                        S4P::logger( 'ERROR', 
                                     "Failed to write .FS_COMPLETE in active file"
                                     . " system $fs ($!)" );
                        return ( undef, undef );                    
                    }
                } else {
                    S4P::logger( 'ERROR', 
                                 "Failed to open .FS_COMPLETE in active file"
                                 . " system $fs ($!)" );
                    return ( undef, undef );
                }
            }
          
            # Remove the existing link to active file system.    
            unless ( unlink( $active_fs ) ) {
                S4P::logger( 'ERROR',
                             "Failed to remove existing active file system,"
                             . " $active_fs ($!)" );
                return ( undef, undef );
            }
            
            # Create a symbolic link to the new file system.
            unless ( symlink( $newfs, $active_fs ) ) {
                S4P::logger( 'ERROR',
                             "Failed to create a symbolic link to $newfs"
                             . " from $active_fs ($!)" );
                return ( undef, undef );
            } 
        } else {
            # Case of space found on the active file system: make sure
            # the dataset directory exists on the active file system and return
            # dataset directory. On failure to find+create dataset directory,
            # return undef.
            my $dir = readlink( "$active_fs" );
            $dir .= "/$pdr_file";
            unless ( -d $dir ) {
                if ( mkdir( $dir, 0777 ^ umask ) ) {
                    S4P::logger( "INFO", "Created $dir" );
                } else {
                    unless ( -d $dir ) {
                        S4P::logger( "ERROR", "Failed to create $dir ($!)" );
                        return ( undef, undef );
                    }
                }
            }

            my $datasetDir = "$dir/$dataset";
            unless ( mkdir( $datasetDir, 0777 ^ umask ) ) {
                unless ( -d $datasetDir ) {
                    rmdir ( dirname $datasetDir );
                    S4P::logger( 'ERROR', "Failed to create $datasetDir ($!)" );
                    return ( undef, undef );
                }
            }

            # Update necessary files for directory based disk management
            if ( -f $fsSizeFile ) {
                my @sizeList = DiskPartitionTracker( $fsSizeFile, "update",
		    -$requiredSize );
		return ( undef, undef ) unless @sizeList;
            }
            # Return the dataset directory on the active file system along
            # with the reserved size.
            return ( $datasetDir, $requiredSize );   
        }
    }
    # Return undef if all file systems have been exhausted.
    S4P::logger( 'ERROR', "Reached end of file systems; no space found." );
    return ( undef, undef );
}

################################################################################

=head1 GetMetadata

Description:

    Given a metadata extraction script name and a data filename, executes the
    metadata extractions script with the filename as an argument. Its output,
    a series of parameter name/value pairs (parameter name=value) one per line, 
    is parsed into a hash whose keys are parameter names and values are 
    parameter values. If a parameter is a product specific attribute 
    (something that is not part of standard attributes list), 'PSA:'
    will be prepended to the parameter name.
    
Input:

    Metadata extraction script name and data file.

Output:

    Returns a hash whose keys/values are paramter names/values upon success.
    On failure returns undef.

Algorithm:

    Function GetMetadata
        Run metadata extraction script with data file as argument.
        If metadata extraction is successful
            Read standard output of metadata extraction script a line at a time.
            Split the line into parameter names and values.
            If a standar parameter
                Prfix parameter name with 'PSA:'.
            Endif
            Store the parameter value in a hash member whose key is the 
            parameter name.
        Else
            Return undef.
        EndIf
        Return the hash reference containing parameter name/value.
    End
    
=head1 AUTHOR

John Bonk

=cut

sub GetMetadata {
    my ( $metadata_extraction_script, @data_file_list ) = @_;      

    my %metadata;
    my $returnValue = undef;
    
    # @coreAttributes contains attributes cut and pasted from the XML data 
    # model; it is probably overkill.     
    my @coreAttributes = qw( BeginningDateTime
                             Boundary
                             BoundingRectangle
                             BrowseFormat
                             BrowseGranuleID
                             CheckSumType
                             CheckSumValue 
                             CRC32
                             EastBoundingCoordinate
                             EndingDateTime
                             EquatorCrossingDate
                             EquatorCrossingLongitude
                             EquatorlCrossingTime
                             EquatorCrossingDateTime
                             Format
                             GPolygon
                             GranuleID
                             HorizontalSpatialDomainContainer
                             InsertDateTime
                             InstrumentShortName
                             LongName
                             NorthBoundingCoordinate
                             OrbitNumber
                             PlatformShortName
                             PointLatitude
                             PointLongitude
                             ProductionDateTime
                             RangeBeginningDate
                             RangeBeginningTime
                             RangeEndingDate
                             RangeEndingTime
                             SensorShortName
                             ShortName
                             SizeBytesDataGranule 
                             SouthBoundingCoordinate
                             VersionID
                             WestBoundingCoordinate
                            );
    
    my $file_str = join( " ", @data_file_list );
    my @metadata = `$metadata_extraction_script $file_str`;  
    # note:  when backticks are used status of command is returned in $? 
    if ( $? ) {  
        # If there is an OS error, complain
        S4P::logger( 'ERROR',
                    "$metadata_extraction_script $file_str returned $?" );
    } else {
        # Check whether the output of metadata extractor is in XML
        my $status = eval {
            my $xmlParser = XML::LibXML->new();
            my $dom = $xmlParser->parse_string( join( "", @metadata ) );
            $dom ? 1 : 0;
            };
        if ( $status ) {
            # If a DOM is found, the metadata is already in XML format; use it
            # directly.
            $returnValue = join( "", @metadata );
        } else {
            # Otherwise break it down in to keyword=value format
            foreach ( @metadata ) {
                chomp;
                my ( $parameter, $value ) = split /=/;
                if ( !( grep /$parameter/, @coreAttributes ) ) { 
                    # If the parameter is not a standard one,
                    # prepend 'PSA:' to parameter to identify it as a 
                    # Product Specific Attribute:
                    $parameter = "PSA:$parameter" 
                }
                $metadata{$parameter} = $value;
            }
            $returnValue = \%metadata;
        }
    }    
    $returnValue;
}

################################################################################

=head1 IdentifyDataset

    Given a configuration hash for determining data types and the data file
    name, a string in the form of <datatype>.<version> is returned.
    It is used to associate a data file with a data type and version. The 
    configuration hash must contain keys of <datatype>.<version>
    and values of pattern for matching data file name.

Input:

    The configuration hashand the data file name being identified.
    
Output:

    Returns a string of the form <data type name>.<version number> if a match is
    found. Returns a zero length string otherwise.

=head1 AUTHOR

Krishna Tewari
Guang-Dih Lei, changed from configuration file input to hash input

=cut

sub IdentifyDataset {
    my ( $datasetPattern, $dataFile ) = @_;

    my $ESDT = "";
    my $pattern;
    foreach my $dataset ( keys %$datasetPattern ) {
        if ( ref($datasetPattern->{$dataset}) eq 'HASH' ) {
            $pattern = $datasetPattern->{$dataset}{FILE};
        } else {
            $pattern = $datasetPattern->{$dataset};
        }

        if ( $dataFile =~ m/$pattern/ ) {
            $ESDT = $dataset;
            last;
        }
    }
    return $ESDT;
}
        
################################################################################

=head1 Metadata2XML

Description:

    Metadata2XML converts a metadata hash into an XML string. The input hash 
    is a hash structure whose keys/values are parameter names/values.
    As a result, Metadata2XML uses attribute-specific logic to construct the
    nestings in the output XML.  Following is a mapping of input keys and
    output XML tags:
        BeginningDateTime       -- RangeBeginningTime, RangeBeginningDate
        EndingDateTime          -- RangeEndingTime, RangeEndingDate
        GPolygon                -- PointLatitude, PointLongitude
        EquatorCrossingDateTime -- EquatorCrossingDate, EquatorCrossingTime
        CRC32                   -- CheckSumType, CheckSumValue
        PSA:*                   -- PSAName, PSAValue
  
Input:

    A hash whose keys/values are parameter names/values.
    
Output:

    Returns an XML string.

Algorithm:
    
    Function Metadata2XML
        Foreach key in passed metadata hash
            Map the key to an XML tag/block and use the hash value to
            provide XML tag content.
        Endfor
    End    

=head1 AUTHOR

Christopher Lynnes, NASA/GSFC

=cut

### Brute force technique for writing metadata from hash to XML
sub Metadata2XML {
    # Copy metadata hash
    my %met = @_;

    # Initialize array for xml
    # Removing the trailing space after XMLSchema-instance by deleting
    # the line feed after it.
    my $xml_head =<< "EOF";
<?xml version="1.0" encoding="UTF-8"?>
<S4PAGranuleMetaDataFile xsi:noNamespaceSchemaLocation="http://disc.gsfc.nasa.gov/xsd/s4pa/S4paGranule.xsd" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
EOF
    my @xml;

    # Product information
    push @xml, '  <CollectionMetaData>';
    foreach my $attr('LongName', 'ShortName', 'VersionID') {
        push @xml, tag_field(4, $attr, $met{$attr});
    }
    push @xml, '  </CollectionMetaData>';
    push @xml, '  <DataGranule>';

    ### Core granule attributes: GranuleID and Format
    push (@xml, tag_field(4, 'GranuleID', $met{'GranuleID'}));
    if ($met{'Format'}) {
        push (@xml, tag_field(4, 'Format', $met{'Format'}));
    }

    ### Checksum:  CRC32 is the only one supported so far
    if ($met{'CRC32'}) {
        push (@xml, '    <CheckSum>','      <CheckSumType>CRC32</CheckSumType>');
        push (@xml, tag_field(6, 'CheckSumValue', $met{'CRC32'}));
        push (@xml, '    </CheckSum>');
    }
    push @xml, tag_field(4, 'SizeBytesDataGranule', $met{'SizeBytesDataGranule'});
    push @xml, tag_field(4, 'InsertDateTime', $met{'InsertDateTime'});
    push @xml, tag_field(4, 'ProductionDateTime', $met{'ProductionDateTime'}) 
        if $met{'ProductionDateTime'};
    push @xml, '  </DataGranule>';


    ### RangeDateTime
    push (@xml, '  <RangeDateTime>');
    foreach my $attr ('EndingDateTime','BeginningDateTime') {
        my ($date, $time, $dummy) = split('[TZ ]', $met{$attr});
        my $attr_root = $attr;
        $attr_root =~ s/DateTime//;
        push @xml, tag_field(4, "Range$attr_root" . 'Time', $time);
        push @xml, tag_field(4, "Range$attr_root" . 'Date', $date);
    }
    push (@xml, '  </RangeDateTime>');

    # GPolygon
    if ($met{'GPolygon'}) {
        # Split up string into components
        $met{'GPolygon'} =~ s/[()]//g;
        my @gpolygon = split(',', $met{'GPolygon'});

        # Split up array of lat/longs into lats and longs
        my $npts = scalar(@gpolygon)/2;
        my ($i, @latitude, @longitude);
        for ($i = 0; $i < $npts; $i++) {
            $latitude[$i] = $gpolygon[$i*2];
            $longitude[$i] = $gpolygon[$i*2+1];
        }

        # Write XML 
        push (@xml, '  <SpatialDomainContainer>');
        push (@xml, '    <HorizontalSpatialDomainContainer>');
        push (@xml, '      <GPolygon>');
        push (@xml, '        <Boundary>');
        for ($i = 0; $i < $npts; $i++) {
            push @xml, '          <Point>';
            push @xml, tag_field(12, 'PointLongitude', $longitude[$i]);
            push @xml, tag_field(12, 'PointLatitude', $latitude[$i]);
            push @xml, '          </Point>';
        }
        push (@xml, '        </Boundary>');
        push (@xml, '      </GPolygon>');
        push (@xml, '    </HorizontalSpatialDomainContainer>');
        push (@xml, '  </SpatialDomainContainer>');
    }
    # Bounding Box
    # If WestBoundingCoordinate exists
    if ($met{'WestBoundingCoordinate'}) {
        push (@xml, '  <SpatialDomainContainer>');
        push (@xml, '    <HorizontalSpatialDomainContainer>');
        push (@xml, '      <BoundingRectangle>');
        foreach my $attr (qw(WestBoundingCoordinate NorthBoundingCoordinate 
                            EastBoundingCoordinate SouthBoundingCoordinate)) {
            push (@xml, tag_field(8, $attr, $met{$attr}));
        }
        push (@xml, '      </BoundingRectangle>');
        push (@xml, '    </HorizontalSpatialDomainContainer>');
        push (@xml, '  </SpatialDomainContainer>');
    } 
    ### Orbital information
    if ($met{'EquatorCrossingLongitude'}) {
        push (@xml, '  <OrbitCalculatedSpatialDomain>');
        push (@xml, '    <OrbitCalculatedSpatialDomainContainer>');
        push (@xml, tag_field(6,'OrbitNumber',$met{'OrbitNumber'}));
        push (@xml, tag_field(6,'EquatorCrossingLongitude',$met{'EquatorCrossingLongitude'}));
        my ($date, $time, $dummy) = split('[TZ ]', $met{'EquatorCrossingDateTime'});
        push @xml, tag_field(6, 'EquatorCrossingDate', $date);
        push @xml, tag_field(6, 'EquatorCrossingTime', $time);
        push (@xml, '    </OrbitCalculatedSpatialDomainContainer>');
        push (@xml, '  </OrbitCalculatedSpatialDomain>');
    }
    ### Platform information
    if ($met{'PlatformShortName'}) {
        push (@xml, '  <Platform>');
        push (@xml, tag_field(4, 'PlatformShortName', $met{'PlatformShortName'}));
        push (@xml, '    <Instrument>');
        push (@xml, tag_field(6, 'InstrumentShortName', $met{'InstrumentShortName'}));
        push (@xml, '      <Sensor>');
        my $sensor = $met{'SensorShortName'} || $met{'InstrumentShortName'};
        push (@xml, tag_field(8, 'SensorShortName', $sensor));
        push (@xml, '      </Sensor>');
        push (@xml, '    </Instrument>');
        push (@xml, '  </Platform>');
    }
    ### PSAs (Product-specific attributes)
    my @psa = grep /^PSA:/, keys(%met);
    if (@psa) {
        push (@xml, '  <PSAs>');
        foreach my $attr(sort @psa) {
            push (@xml, '    <PSA>');
            push (@xml, tag_field(6, 'PSAName', substr($attr,4)));
            push (@xml, tag_field(6, 'PSAValue', $met{$attr}));
            push (@xml, '    </PSA>');
        }
        push (@xml, '  </PSAs>');
    }

    # Join array elements together with newlines into string
    # Return string
    push @xml, '</S4PAGranuleMetaDataFile>';
    return ($xml_head . join("\n", @xml, ''));
}
sub tag_field {
    my ($indent, $attr, $val) = @_;
    return (sprintf("%s<%s>%s</%s>", ' ' x $indent, $attr, $val, $attr));
}

################################################################################

=head1 ReceiveData

Description:

    Given a PDR file, ReceiveData() will download the files specified in PDR
    one file group at a time. For each file downloaded, it will validate
    the size and checksum (if specified in PDR). It will return a PDR containing
    the downloaded file groups (granules). It will store the status of
    each file group in the input PDR for use later.

Input:
    Input PDR (S4P::PDR), Input PDR filename
     
Output:
    Output PDR (S4P::PDR)
    
Algorithm:

    Function ReceiveData
        Foreach file group in PDR
            Allocate a local directory for download.
            Download files in the file group.
            Foreach file in file group
                Check file modification time and make sure it's a fresh copy.
                Compare local file size with the file size in PDR.
                Compare local file checksum with the file's checksum in PDR.
                If above three conditions are met
                    Mark the file as valid.
                Else
                    Note the disposition for the file.
                    Set file group status, both input and output, as failure.
                Endif                
            Endfor
            If the file group is successful, add the file group to the output
            PDR.
        Endfor
        Return the output PDR. 
    End

=head1 AUTHOR

Krishna Tewari
M. Hegde

=cut

sub ReceiveData {
    my ( $pdr, $pdr_file ) = @_;
    my $datestamp = S4P::TimeTools::CCSDSa_Now();
    
    my ( $session, $session_host, $count );    
    # Create a holder PDR for downloaded files.
    my $outPdr = S4P::PDR->new();
    $outPdr->originating_system( $pdr->originating_system );
    $outPdr->expiration_time( $pdr->expiration_time );
    # Process each file group in the PDR,    
    FILE_GROUP: foreach my $file_group ( @{$pdr->file_groups} ) {
        # Keep a counter for FileGroups for logging purposes.
        ++$count;
        
        # No need to process FileGroups that have been processed already         
        if ( $file_group->status() eq 'SUCCESSFUL' ) {
            S4P::logger( "INFO",
                "File group #$count has been processed: skipping" );
            next FILE_GROUP;
        }
        
        # Start out by marking every FileGroup's status as success.
        $file_group->status( 'SUCCESSFUL' );
        
        # Set the protocol to be used in transferring each file group; default
        # is 'FTP'.
        my $node_name = $file_group->node_name();
        my $protocol = $CFG::cfg_protocol{$node_name};
        $file_group->protocol( $protocol );
        
        my $data_type = $file_group->data_type; 
        my $data_version = $file_group->data_version; 
        my $active_fs = '../active_fs';
        
	# Place a lock so that only one process is trying to reserve disk
	# space. Uses file locking.
        unless ( open( LOCKFH, ">../active_fs/active_fs.lock" ) ) {
	    S4P::logger( "ERROR",
                "Failed to open file system lock file ($!)" );
	    return undef;
        }
        unless ( flock( LOCKFH, 2 ) ) {
	    close( LOCKFH );
	    S4P::logger( "ERROR", "Failed to obtain a lock ($!)" );
	    return undef;
        }
        # Get the local directory for a given space requirement.
        my ( $dirPath, $reservedSize )
            = S4PA::Receiving::GetDirectory( $file_group, $pdr_file );
        unless (defined $dirPath && defined $reservedSize) {
            S4P::logger( "ERROR", "Failed to get local directory ($!)" );
            return undef;
        }

	# Free the lock on file system access.
	flock( LOCKFH, 8 );
	close( LOCKFH );

        # Leave a flag file to indicate the data is being actively downloaded;
        # The file path is <active_fs>/.RUNNING_<pdr name>
        my $activeDownloadIndicatorFile = dirname( dirname( $dirPath ) )
            . "/.RUNNING_" . $pdr_file;
        if ( open( FH, ">$activeDownloadIndicatorFile" ) ) {
            close( FH );
        }
        # If the file group's host is different than the host of previous
        # file group, undefine the Net::FTP or Net::SFTP::Foreign object.
        if ( defined $file_group->node_name 
             && $file_group->node_name ne $session_host ) {
            $session_host = $file_group->node_name;
            if ( defined $session ) {
                if ( ref( $session ) eq 'Net::FTP' ) {
                    $session->quit();
                } elsif ( ref( $session ) eq 'Net::SFTP::Foreign' ) {
                    $session->disconnect();
                }
                undef $session;
            }
        }

        # Use time to figure out whether a new file was downloaded
        my $time = time();
        if ( defined $dirPath ) {
            # Since S4P::FileGroup http_get routine needs only
            # a local download directory for input, we will need
            # to call it with that as the first parameter.
            # http_get return the number of files downloaded 
            # via http for the current file group.
            # For the remote http that need user authentication,
            # http_get is using S4P get_ftp_login routine to locate
            # username and password in the $HOME/.netrc file.
            # In .netrc, machine xxx login xxx password xxx 
            # need to be appear in one single line for get_ftp_login
            # to locate the right user/passwd.
            if ( $protocol eq 'HTTP' ) {
                my $filesGot = $file_group->download( $dirPath );
                if ( $filesGot == scalar @{$file_group->file_specs} ) {
                    $session = 1;
                } else {
                    undef $session;
                }
            } elsif ($protocol eq 'SFTP') {
                # Transfer files of the file group via SFTP using Net::SFTP::Foreign
                # instead of using S4P::FileGroup::download.
                $session = S4PA::Receiving::SftpGetFileGroup($file_group,
                    $session_host, $dirPath, $session);
            } else {
                # Transfer files of the file group via FTP or FILE.
                $session = $file_group->download( $session_host, $dirPath,
                    $session, 5, 60 );
            }
            if ( defined $session ) {
                S4P::logger( 'INFO', 
                    "Downloaded file group #$count to $dirPath" );
            } else {
                S4P::logger( 'WARNING',
                    "Failed to transfer file group #$count" );
	        rmdir( $dirPath );
            }
        }
                
        # Loop through each file belonging to the file group.
        # Add files to output PDR if successful; mark as failed in PAN otherwise
        my @filelist = ();
        # Create a file group to hold transferred files
        my $outFileGroup = new S4P::FileGroup;
        $outFileGroup->data_type( $data_type );
        $outFileGroup->data_version( $data_version, "%s" );
        # By default, set the FileGroup status to SUCCESSFUL.
        $outFileGroup->status( 'SUCCESSFUL' );
        # Set the reserved size for output file group for use in reclaiming
        # disk space later.
        $outFileGroup->{RESERVED_SIZE} = $reservedSize;
        FILE_SPEC: foreach my $fspec ( @{$file_group->file_specs} ) {
            my $filename = $dirPath . '/' . $fspec->file_id;
            my $fileStat = stat( $filename );
            my $pdrSize = $fspec->file_size;
            
            # A flag to indicate file download; by default, set it to true.
            my $downloadFlag = 1;

            # rename download files if PDR defined alias
            if ($fspec->alias) {
                my $download_filename = $filename;
                $filename = $dirPath . '/' . $fspec->alias;
                unless (rename $download_filename, $filename) {
                    $filename = $download_filename;
                    S4P::logger( 'ERROR',
                                 "Failed to rename " . $fspec->file_id
                                 . " to $fspec->alias" );
                    $fspec->status( "RENAME FAILURE" );
                    $downloadFlag = 0;
                }
            }            
            # Skip files that have been processed successfully already
            # next if ( $fspec->status() eq 'SUCCESSFUL' );

            if ( not defined $dirPath ) {
                # If the file system was not available, the file group would
                # not be downloaded. Hence, mark all files in the file group
                # as failed.
                S4P::logger( 'ERROR',
                             "Failed to download " . $fspec->file_id 
                             . " due tolack of available file system" );
                $fspec->status( "FAILURE-DISK SPACE NOT AVAILABLE" );
                $downloadFlag = 0;
            # } elsif ( not defined $fileStat or $fileStat->mtime() < $time ) {
                # If the file doesn't exist (file stat not found) or
                # if the file modification time is older than expected,
                # mark the file as failed.
            } elsif ( (not defined $fileStat) or (($fileStat->mtime()+1) < $time) ) {
                # time() round to the nearest second but stat() round down to the second
                # for a fast machine or small file group, mtime from stat could be
                # one second less the time() and failed the job. To overcome this,
                # add one second to the downloaded file timestamp before comparing
                # to the downloading start time.
                S4P::logger( 'ERROR',
                             "Failed to download " . $fspec->file_id );
                $fspec->status( "TRANSFER FAILURE" );
                $downloadFlag = 0;
            } elsif ( $fileStat->size() != $pdrSize ) {
                # If the file size is less than what was specified in PDR,
                # mark the file as failed.
                S4P::logger( 'ERROR',
                             "Size mismatch for $filename.  Size"
                             . "(pre/post-transfer): $pdrSize vs "
                             . $fileStat->size() );
                $fspec->status( 'POST-TRANSFER FILE SIZE CHECK FAILURE' );
            } elsif ( defined $fspec->{file_cksum_value} ) {
                # If PDR specifies a checksum value, make sure computed
                # and PDR specified checksums match.
                my $errmsg = "";
                my $cksum_text = "";
                if ($fspec->{file_cksum_type}) {
                    if (uc($fspec->{file_cksum_type}) eq "MD5") {
                        $cksum_text = `md5sum $filename`;
                        $errmsg = "Unable to compute md5sum for $filename"
                            if $?;
                    } elsif (uc($fspec->{file_cksum_type}) eq "CKSUM") {
                        $cksum_text = `cksum $filename`;
                        $errmsg = "Unable to compute cksum for $filename" if $?;
                    } elsif (uc($fspec->{file_cksum_type}) eq "SHA1") {
                        $cksum_text = `sha1sum $filename`;
                        $errmsg = "Unable to compute sha1sum for $filename" 
                            if $?;
                    } else {
                        $errmsg = 'Unsupported checksum type: '
                            . $fspec->{file_cksum_type};
                    }
                } else {
                    $errmsg = "Missing file_cksum_type for $filename";
                }
                if ( $errmsg ) {
                    # On OS error to generate checksum, report and mark the
                    # faile as failed
                    S4P::logger( 'ERROR', "$errmsg ($!)" );
                    $fspec->status( 'CHECKSUM VERIFICATION FAILURE' );
                } else {
                    # If the checksum generation is successful, get the
                    # checksum value.
                    my ( $cksum, $dummy ) = split( /\s+/, $cksum_text, 2 );
                    if ( $cksum ne $fspec->{file_cksum_value} ) {
                        # If checksums don't match, report and mark the file
                        # as failed.
                        S4P::logger( 'ERROR', 
                                     "Checksum mismatch for $filename"
                                     . " ($cksum instead of"
                                     . " $fspec->{file_cksum_value})" );
                        $fspec->status( 'CHECKSUM VERIFICATION FAILURE' );
                    } else {
                        $fspec->status( 'SUCCESSFUL' );
                    }
                } # end of if ( $errmsg )
            } else { 
                # All is well with this file.
                $fspec->status( 'SUCCESSFUL' );
            } # end of if ( not defined $dirPath )
            
            # If the file has been downloaded
            if ( $downloadFlag ) {
                # Create a new FileSpec with the local directory name.
                my $fileSpecList = $outFileGroup->add_file_spec( $filename );
                my $newFileSpec = $fileSpecList->[-1];
                
                # Set the file type to either METADATA, BROWSE, QA, HDF4MAP or SCIENCE.
                my $oldFileType = $fspec->file_type();
                my $newFileType = ( $oldFileType eq 'METADATA'
                    ? 'METADATA' : ( $oldFileType eq 'BROWSE'
                    ? 'BROWSE' : ( $oldFileType eq 'HDF4MAP'
                    ? 'HDF4MAP' : ( $oldFileType eq 'QA'
                    ? 'QA' : 'SCIENCE' ) ) ) );
                $newFileSpec->file_type( $newFileType );
                
                # Make 'CKSUM', CRC checksum, the default check sum type.
                # $newFileSpec->{file_cksum_type} ||= 'CKSUM';

                # Carry original fileGroup's checksum type and value if specified
                # otherwise, make 'CKSUM' the default checksum type
                if ( defined $fspec->{file_cksum_value} ) {
                    $newFileSpec->{file_cksum_type} = $fspec->{file_cksum_type};
                    $newFileSpec->{file_cksum_value} = $fspec->{file_cksum_value};
                } else {
                    $newFileSpec->{file_cksum_type} = 'CKSUM';
                }
            }
            if ( $fspec->status() ne 'SUCCESSFUL' ) {
                # Mark the FileGroup status as failed if any of its FileSpecs 
                # fail.
                $file_group->status( 'FAILURE' );
                $outFileGroup->status( 'FAILURE' );
            }
        } # end of foreach my $fspec
        
        # Add the newly downloaded FileGroup to the new PDR if it contains
        # any file. 
        if ( defined $outFileGroup->file_specs ) {
	    $outFileGroup->{index} = $count - 1;
            $outPdr->add_file_group( $outFileGroup );
	} else {
            # Otherwise, give up the reserved space
            my $dataDir = dirname( $dirPath );
            my $pdrDir = dirname( $dataDir );
            my $fsSizeFile = "$pdrDir/.FS_SIZE";
            # Give back the space if using directory based disk space manager
            DiskPartitionTracker( $fsSizeFile, "update", $reservedSize )
                if ( -f $fsSizeFile );
            S4P::logger( 'ERROR', "cannot remove $dirPath" ) unless (rmdir( $dirPath ));
            rmdir( $dataDir );
            rmdir( $pdrDir );
        }
         
    } # end of foreach my $file_group
    $pdr->status();

    # If any transfer session is still alive, terminate it.    
    if ( defined $session ) {
        if ( ref( $session ) eq 'Net::FTP' ) {
            $session->quit();
        } elsif ( ref( $session ) eq 'Net::SFTP::Foreign' ) {
            $session->disconnect();
        }
    }

    $outPdr->file_groups( [] ) unless ( $outPdr->total_file_count() > 0 );
    # Return PDR object
    return( $outPdr );
}

################################################################################

=head1 sftp_put

    Given the remote host name, directory and the local file, this method
    pushes the file via SFTP. It expects the entries for remote host to
    exist in .netrc.

Input:

    A hash with keys localHost, remoteHost, remoteDir.
    
Output:

    Returns true/false (1/0) depending on successful/failed push.

Algorithm:
    
    Function sftp_put
        Lookup .netrc for the hostname.
        Connect to host via SSH.
        Authenticate using public/private key in ~/.ssh/id_dsa.
        Put the local file in the remote directory.
    End    

=head1 AUTHOR

M. Hegde

=cut

sub sftp_put
{
    my ( %arg ) = @_;
    
    # Look up hostname in .netrc
    my $machine = Net::Netrc->lookup( $arg{host} );
    S4P::logger( "INFO", "Looking up $arg{host} in .netrc" );
    unless ( $machine ) {
        S4P::logger( "ERROR",
            "Couldn't find an entry in .netrc for $arg{host}" );
	return 0;
    }

    # Get the login name for the host
    my $login = $machine->login();
    unless ( $login ) {
	S4P::logger( "ERROR",
	    "Failed to find login info for $arg{host} from .netrc" );
        return 0;
    }
    
    my $passwd = ( defined $login ) ? ( $machine->password() ) : undef;
    S4P::logger( "INFO", "Found login info for $arg{host}" );

    my $remoteFile = basename( $arg{file} );
    my $sftp_batch_put = "s4pa_sftp_put_cmd";
    open ( BATCH, "> $sftp_batch_put" );
    print BATCH "cd $arg{dir}\n";
    print BATCH "put $arg{file}\n";
    print BATCH "chmod 644 $remoteFile\n";
    print BATCH "lpwd\nls -l $remoteFile\n" if ( $arg{verify} );
    print BATCH "quit\n";
    close ( BATCH );

    # execute sftp through system call for file transfering
    my $status = 0;
    my $sftp_log = "s4pa_sftp.log";
    my $sftp_session = "sftp" . " -b $sftp_batch_put " . "$login\@$arg{host}";
    my $sftpStatus = system( "$sftp_session" . "> $sftp_log 2>&1" );
    if ( $sftpStatus ) {
	if ( -f $sftp_batch_put ) {
	    if ( open( FH, $sftp_batch_put ) ) {
	        local( $/ ) = undef;
		my $cmdString = <FH>;
		close( FH );
		S4P::logger( "INFO", "Batch file content:\n$cmdString" );
	    } else {
		S4P::logger( "WARNING",
		    "Failed to open command batch file $sftp_batch_put" );
	    }
	}
	if ( -f $sftp_log ) {
	    if ( open( FH, $sftp_log ) ) {
	        local( $/ ) = undef;
		my $logString = <FH>;
		close( FH );
		S4P::logger( "INFO", "Log file content:\n$logString" );
	    } else {
		S4P::logger( "WARNING",
		    "Failed to open command batch file $sftp_log" );
	    }
	}
        unlink $sftp_log;
        unlink $sftp_batch_put;
	S4P::logger( "ERROR",
	    "Failed on sftp push $arg{file} to $arg{host}:$arg{dir}" );
    } else {
        S4P::logger( "INFO", "Succeeded on sftp push $arg{file} to $arg{host}:$arg{dir}" );
        if ( $arg{verify} && defined $arg{logger} ) {
            if ( open( FH, "<$sftp_log" ) ) {
                local ( $/ ) = undef;
                my $logContent = <FH>;
                close( FH );
                $arg{logger}->info( $logContent );
            }    
        }
        unlink $sftp_log;
        unlink $sftp_batch_put;
        $status = 1;
    }
    return $status;
}

################################################################################

=head1 bbftp_put

    Given the remote host name, directory and the local file, this method
    pushes the file via BBFTP. It expects the entries for remote host to
    exist in .netrc.

Input:

    A hash with keys localHost, remoteHost, remoteDir.

Output:    Returns true/false (1/0) depending on successful/failed push.

Algorithm:

    Function bbftp_put
        Lookup .netrc for the hostname.
        Connect to host via BBFTP.
        Put the local file in the remote directory.
    End

=head1 AUTHOR

F. Fang

=cut

sub bbftp_put{
    my ( %arg ) = @_;
    # Look up hostname in .netrc
    my $machine = Net::Netrc->lookup( $arg{host} );
    S4P::logger( "INFO", "Looking up $arg{host} in .netrc" );
    unless ( $machine ) {
        S4P::logger( "ERROR",
            "Couldn't find an entry in .netrc for $arg{host}" );
        return 0;
    }
    # Get the login name for the host
    my $login = $machine->login();
    unless ( $login ) {
        S4P::logger( "ERROR",
            "Failed to find login info for $arg{host} from .netrc" );
        return 0;
    }
    my $passwd = ( defined $login ) ? ( $machine->password() ) : undef;
    S4P::logger( "INFO", "Found login info for $arg{host}" );

    # execute bbftp through system call for file transfering
    my $status = 0;
    my $connection = S4P::Connection->new( PROTOCOL => 'BBFTP',
                                      HOST => $arg{host},
                                      LOGIN => $login );
    if ( $connection->onError() ) {
        S4P::logger( "ERROR", $connection->errorMessage() );
    }
    my ($bbftpStatus, $bbftp_log) = $connection->put($arg{dir}, $arg{file});

    # avoid bbFTP problem with timestamp of transfered file.
    my $fileName = basename $arg{file};
    my $ssh = "ssh $login\@$arg{host} touch $arg{dir}/$fileName";
    my @sshStatus = system("$ssh");
    S4P::logger( "INFO", "status string of remote-updating the timestamp"
                 . " of transfered file: @sshStatus");

    if ( !$bbftpStatus ) {
        if ( -f $bbftp_log ) {
            if ( open( FH, $bbftp_log ) ) {
                local( $/ ) = undef;
                my $logString = <FH>;
                close( FH );
                S4P::logger( "INFO", "Log file content:\n$logString" );
            } else {
                S4P::logger( "WARNING",
                    "Failed to open command BBFTP log file $bbftp_log" );
            }
        }
        unlink ( $bbftp_log );
        S4P::logger( "ERROR",
            "Failed on bbftp push $arg{file} to $arg{host}:$arg{dir}" );
    } else {
        S4P::logger( "INFO", "Succeeded on bbftp push $arg{file} to $arg{host}:$arg{dir}" );
        if ( $arg{verify} && defined $arg{logger} ) {
            if ( open( FH, "<$bbftp_log" ) ) {
                local ( $/ ) = undef;
                my $logContent = <FH>;
                close( FH );
                $arg{logger}->info( $logContent );
            }
        }
        unlink ( $bbftp_log );
        $status = 1;
    }
    return $status;
}

################################################################################

=head1 ftp_put

    Given the remote host name, directory and the local file, this method
    pushes the file via FTP. It expects the entries for remote host to
    exist in .netrc.

Input:

    A hash with keys localHost, remoteHost, remoteDir.
    
Output:

    Returns Net::FTP object on success; 0 on failure.
    
Algorithm:
    
    Function ftp_put
        Connect to host via FTP.
        Authenticate using .netrc entries.
        Put the local file in the remote directory.
    End    

=head1 AUTHOR

M. Hegde

=cut

sub ftp_put
{
    my ( %arg ) = @_;
    
    my $status = 1;

    my $ftp;
    # If an active session is passed, use it if the hostname hasn't changed.
    if ( defined $arg{session}
        && ( ref($arg{session}) eq 'Net::FTP' )
	&& $arg{session}->_NOOP() ) {
        if ( $arg{host} eq $arg{session}->host() ) {
            $arg{session}->cwd();
            $ftp = $arg{session};
        } else {
            $arg{session}->quit();
        }
    }

    my $ftpLogfile = defined $ENV{FTP_LOGFILE} 
        ? $ENV{FTP_LOGFILE} : undef;

    unless ( defined $ftp ) {    
        # specify default firewall type
        my $firewallType = defined $ENV{FTP_FIREWALL_TYPE} 
            ? $ENV{FTP_FIREWALL_TYPE} : 1;

        my $ftpPassive = defined $ENV{FTP_PASSIVE}
	    ? $ENV{FTP_PASSIVE} : 1;
        
        my $ftpBlocksize = defined $ENV{FTP_BLOCKSIZE}
	    ? $ENV{FTP_BLOCKSIZE} : 10240;

        my $localAddress = defined $ENV{FTP_LOCAL_ADDRESS}
            ? $ENV{FTP_LOCAL_ADDRESS} : `hostname -f`;
        chomp($localAddress);

        # Support 'init' macro only; that too for 'passive' command only.
        my $machine = Net::Netrc->lookup( $arg{host} );
        if ( defined $machine ) {
            # Look for passive command to turn-off passive mode (default)
            foreach my $command ( @{$machine->{machdef}{init}} ) {
                $command =~ s/^\s+|\s+$//g;
                next unless ( $command eq 'passive' );
                $ftpPassive = 0;
                last;
            }
        }
        if ( $ENV{FTP_FIREWALL} ) {
            # Create an Net::FTP object with Firewall option
            my $firewall = $ENV{FTP_FIREWALL};
            $ftp = Net::FTP->new( $arg{host},
                Firewall => $firewall, FirewallType => $firewallType,
                Passive => $ftpPassive, LocalAddr => $localAddress, BlockSize => $ftpBlocksize );
        } else {
            # No firewall specified, let .libnetrc resolve if firewall is 
            # required
            $ftp = Net::FTP->new( $arg{host}, Passive => $ftpPassive,
                LocalAddr => $localAddress, BlockSize => $ftpBlocksize );
        }
        S4P::logger( "INFO","Using local address $localAddress, blocksize $ftpBlocksize, ftplog $ftpLogfile" );
        # Try to login
        if ( $ftp->login() ) {
            S4P::logger( "INFO", "Logged in to $arg{host} for FTP" );
            $ftp->binary();
        } else {
            # On failure to login, log a message.
            S4P::logger( "ERROR",
                "Failed to login to $arg{host}: " . $ftp->message() );
            $status = 0;
        }
    }

    if ( defined $ftp ) {
        # Get the current directory
        my $curDir = $ftp->pwd();
        # Try to change directory to remote directory, but only if we're not there already
        if ($curDir ne $arg{dir}) {
            if ( $ftp->cwd( $arg{dir} ) ) {
                 S4P::logger( "INFO", "Changed directory to $arg{dir}" );
                 $arg{logger}->info( "Changed directory to $arg{dir}" )
                      if ( $arg{verify} && defined $arg{logger} );
            } else {
                 S4P::logger( "INFO", "Directory, $arg{dir}, doesn't exist" );
                 # On failure to change directory, try to create a directory
                 if ( $ftp->mkdir( $arg{dir} ) ) {
                      S4P::logger( "INFO", "Create directory $arg{dir}" );
                      unless ( $ftp->cwd( $arg{dir} ) ) {
                           # log a message
                           S4P::logger( "ERROR", 
                               "Unable to change directory to $arg{dir}: "
                               . $ftp->message() );
                           $status = 0;
                      }
                } else {
                     S4P::logger( "ERROR", "Failed to create directory $arg{dir}: "
                         . $ftp->message() );
                     $status = 0;
                }              
            }              
        } else {
            S4P::logger( "INFO", "Remaining in directory $arg{dir}" );
            $arg{logger}->info( "Remaining in directory $arg{dir}" )
                if ( $arg{verify} && defined $arg{logger} );
        }
        
        if ( $status ) {
            # Try to put the file

            my $start = time() if ($ftpLogfile);
            unless ( $ftp->put( $arg{file} ) ) {
                S4P::logger( "ERROR",
                    "Failed to put $arg{file} in $arg{dir} on "
                    . "$arg{host}: " . $ftp->message() );
                $status = 0;
            }

            if ($ftpLogfile) {
            # Collect transfer statistics
                my $stop = time();
                my $size = (-s $arg{file});
                my $elapsed = $stop - $start;
                $elapsed = ($elapsed) ? $elapsed : 1; # use minimum of 1 sec to prevent div by 0
                my $kBrate = $size/$elapsed/1024;
                my @ts = gmtime() ;
                my $ts = sprintf("%4.4d-%2.2d-%2.2d %2.2d:%2.2d:%2.2d",1900+$ts[5],1+$ts[4],$ts[3],$ts[2],$ts[1],$ts[0]) ;
                my $ratemsg = sprintf("%s: %d bytes sent in %d secs (%7.2f Kbytes/sec)",$ts,$size,$elapsed,$kBrate);
                if (open(FTPLOG,">>$ftpLogfile")) {
                    print FTPLOG "$ratemsg\n";
                    close(FTPLOG);
                } else {
                    S4P::logger("INFO","$ratemsg");
                }
            }

            if ( $arg{verify} && defined $arg{logger} ) {
                my $fileName = basename( $arg{file} );
		my @fileList = $ftp->ls( '-lL', $fileName );
                $arg{logger}->info( "Listing $fileName: " . 
                    join( "\n", @fileList) );
            }    
            $ftp->cwd( $curDir ) if ((defined $curDir) and ($curDir ne $arg{dir}));
        }
        $ftp->quit() unless $status;
    } else {
        # On failure to create Net::FTP, log a message
        S4P::logger( "ERROR", "Failed to create Net::FTP" );
        $status = 0;
    }
    
    return $status ? $ftp : 0;
}

################################################################################

=head1 file_put

    Given the localhost name, directory and the local file, this method
    pushes the file via copy.

Input:

    A hash with keys localHost, remoteHost, remoteDir.

Output:

    Returns true/false (1/0) depending on successful/failed push.

Algorithm:

    Function file_put
        Put the local file in the localhost directory.
    End

=head1 AUTHOR

G. Lei

=cut

sub file_put
{
    my ( %arg ) = @_;
    my $status = 1;

    ( my $localdir = $arg{dir} ) =~ s#/$##;
    my $localfile = "$localdir/" . basename($arg{file});
    unless ( -d $localdir || -l $localdir ) {
        S4P::logger ( "WARNING",
            "Destination $localdir does not exist; trying to create one" );
        if ( mkdir( $localdir ) ) {
            S4P::logger( "INFO", "Created $localdir" );
        } else {
            S4P::logger( "ERROR", "Failed to create $localdir ($!)" );            
            $status = 0;
        }
    }
    return $status unless $status;
    
    if ( File::Copy::copy( $arg{file}, $localfile ) ) {
        S4P::logger( "INFO", "Success in copy of $arg{file}" );
        chmod ( 0644, "$localfile" );
    } else {
        S4P::logger ( "ERROR", "Failure to copy " . $arg{file} 
            . " to $localdir" );
        $status = 0;
    }
    return $status;
}

################################################################################

=head1 put

    Given the protocol, remote host name, directory and the local file, this 
    method pushes the file via SFTP. It expects the entries for remote host to
    exist in .netrc.

Input:

    A hash with keys localHost, remoteHost, remoteDir.
    
Output:

    Returns true/false (1/0) depending on successful/failed push. Returns
    Net::FTP object for FTP connections on success. This will be refactored
    later.

Algorithm:
    
    Function put
        Call ftp_put() 
    End    

=head1 AUTHOR

M. Hegde

=cut

sub put
{
    my ( %arg ) = @_;
    
    my $status = 1;
    if ( $arg{protocol} eq 'SFTP' ) {            
        # $status = S4PA::Receiving::sftp_put( %arg );
        $status = S4PA::Receiving::SftpPut(%arg);
    } elsif ( $arg{protocol} eq 'FTP' ) {    
        $status = S4PA::Receiving::ftp_put( %arg );
    } elsif ( $arg{protocol} eq 'BBFTP' ) {
        $status = S4PA::Receiving::bbftp_put( %arg );
    } elsif ( $arg{protocol} eq 'FILE' ) {
        $status = S4PA::Receiving::file_put ( %arg );
    } else {
        # For unsupported protocols, log a message
        S4P::logger( "ERROR", "Unsupported protocol: $arg{protocol}" );
        $status = 0;
    }
    return $status;
}

################################################################################

=head1 DiskPartitionTracker 

    Given the file that is used for tracking disk space available in a
    directory, the optional operation type (read or update) and an optional
    size in bytes, this method reads or updates the available disk space. It 
    returns an array. Array length of zero indicates a failure. If the array
    length is two, the first element remains the same across calls and the
    second element can vary across calls. Array length of two is used to track
    a directory's original allocated and used space. Array length of one is
    used to track the directory's used space.

Input:

    Filename used to track available disk space.
    Optional operation (read or update).
    Optional size for update operation in bytes.

Output:

    Returns an array of disk space in bytes.

Algorithm:

=head1 AUTHOR

M. Hegde

=cut

sub DiskPartitionTracker
{
    my ( $fileName, $operation, $updateSize ) = @_;
    
    # A variable to hold size being tracked
    my @sizeList = ();
    # Make 'read' the default operation
    $operation = 'read' unless defined $operation;
    
    if ( $operation eq 'read' ) {
        return @sizeList unless ( -f $fileName );
        if ( open( SIZE_FH, $fileName ) ) {
#	    S4P::logger( "INFO", "Trying to obtain lock on $fileName" );
            if ( flock( SIZE_FH, 2 ) ) {
#	        S4P::logger( "INFO", "Obtained the lock on $fileName" );
		my @strList = <SIZE_FH>;
		close( SIZE_FH );
		chomp @strList;
		foreach my $str ( @strList ) {                        
		    push( @sizeList, Math::BigInt->new( $str ) );
		}
	    } else {
		S4P::logger( "ERROR", "Failed to get a lock on $fileName"
		    . " while finding out the available disk space" );
	    }
        } else {
            S4P::logger( "ERROR", "Failed to open $fileName for reading" );
        }
    } elsif ( $operation eq 'update' ) {
        if ( -f $fileName ) {
	    @sizeList = DiskPartitionTracker( $fileName, 'read' );
	    unless ( @sizeList ) {
		S4P::logger( "ERROR",
		    "Error in finding available disk space; returning" );
		return ();
	    }
	    my $index = (@sizeList == 2) ? 1 : 0;
	    $sizeList[$index] += $updateSize;
	    if ( open( SIZE_FH, "+<$fileName" ) ) {
		if ( flock( SIZE_FH, 2 ) ) {
		    truncate( SIZE_FH, 0 );
		    foreach my $size ( @sizeList ) {
		        print SIZE_FH $size, "\n";
		    }
		} else {
		    S4P::logger( "ERROR", "Failed to get a lock on $fileName" );
		}
		close( SIZE_FH );
	    } else {
		S4P::logger( "ERROR",
		    "Failed to open $fileName for read/writing ($!)" );
		@sizeList = ();
	    }
	} else {
	    if ( open( SIZE_FH, ">$fileName" ) ) {
#	        S4P::logger( "INFO", "Trying to get a lock on $fileName" );
		if ( flock( SIZE_FH, 2 ) ) {
		    S4P::logger( "INFO", "Obtained lock on $fileName" );
		    print SIZE_FH $updateSize, "\n";
		    close( SIZE_FH );
		    @sizeList = ( $updateSize );
		} else {
		    S4P::logger( "ERROR", "Failed to get a lock on $fileName" );
		}
	    } else {
		S4P::logger( "ERROR",
		    "Failed to open $fileName for wriring ($!)" );
		@sizeList = ();
	    }
	}
    } else {
        S4P::logger( "WARNING",
            "Unknown operation for DiskPartitionTracker: $operation" );
        @sizeList = ();
    }
    return @sizeList;
}

################################################################################

=head1 UpdateMetadata 

    Updates metadata after a granule has been processed.
    
Input:

    S4P::FileGroup
    A hash ref containing file mappings with input files as keys and output
    files as values.

Output:

    Returns 0/1 for failure/success

Algorithm:

=head1 AUTHOR

M. Hegde

=cut

sub UpdateMetadata
{
    my ( $fileGroup, $fileMap ) = @_;

    # Create an XML DOM parser and set the flag to remove unnecessary white
    # space. This allows for clean formatting when printed later.
    my $xmlParser = XML::LibXML->new();
    $xmlParser->keep_blanks( 0 );

    # Parse metadata file in the file group and obtain the document element. If
    # an error occurs during any step, return 0.
    my $metFile = $fileGroup->met_file();
    unless ( $metFile ) {
        S4P::logger( 'ERROR', "Failed to find metadata file in file group" );
        return 0;
    }
    my $dom = $xmlParser->parse_file( $metFile );
    unless ( defined $dom ) {
        S4P::logger( 'ERROR', "Failed to get DOM from $metFile" );
        return 0;
    }

    my $doc = $dom->documentElement();
    unless ( defined $doc ) {
        S4P::logger( 'ERROR', "Failed to find document element in $metFile" );
        return 0;
    }
    
    # get rid of the existing processing instruction by recreating the doc
    my $newXml = XML::LibXML->new();
    $newXml->keep_blanks(0);
    $dom = $newXml->parse_string($doc->toString(1));
    $doc = $dom->documentElement();
    
    # Find the version ID
    my ( $versionNode ) = $doc->findnodes( '//CollectionMetaData/VersionID' );
    my $versionID = (defined $versionNode)
        ? $versionNode->string_value() : undef;

    my ( $nameNode ) = $doc->findnodes( '//CollectionMetaData/ShortName' );
    my $shortName = (defined $nameNode) ? $nameNode->string_value() : undef;
    my $dataset = $fileGroup->data_type();

    # Compare shortnames to make sure one in metadata and the PDR match
    unless ( $dataset eq $shortName ) {
        S4P::logger( "ERROR", "Shortname in metadata file differs from the"
            . " one in file group of $metFile" );
        return 0;
    }

    unless ( defined $versionID ) {
        S4P::logger( "ERROR", "Failed to find version ID in $metFile" );
	return 0;
    }

    my $accessType = S4PA::Receiving::GetAccessType( $dataset, $versionID );
    unless ( defined $accessType ) {
        S4P::logger( "ERROR",
            "Failed to find access type for $dataset (version=$versionID)" );
        return 0;
    }
    if ( defined $CFG::cfg_root_url{$accessType} ) {
        $CFG::cfg_root_url{$accessType} =~ m#(://([^/]+))?(/.*)$#;
        my $styleSheet = $3 or '/data';
        $styleSheet .= ( $styleSheet =~ /\/$/ 
            ? 'S4paGran2HTML.xsl' : '/S4paGran2HTML.xsl' );
        $dom->insertProcessingInstruction( 'xml-stylesheet',
            qq(type="text/xsl" href="$styleSheet") );
    }
    my $supportedVersion =
    S4PA::Receiving::FindSupportedVersion( $dataset, $versionID );

    # If no matching version found, complain and return.
    unless ( defined $supportedVersion ) {
        S4P::logger( "ERROR", "$versionID of $dataset not supported" );
        return 0;
    }
    # Set the correct version ID for the file group based on metadata.
    $fileGroup->data_version( $versionID, "%s" );

    # Insert URL to collection metadata
    my $collUrl;
    if (defined $CFG::cfg_collection_link{$shortName}{$supportedVersion}) {
        if ($CFG::cfg_collection_link{$shortName}{$supportedVersion} eq 'CMR') {
            # make sure concept-id from deployment is the same with dif_fetcher
            my $difUrl;
            my $difConcept = $DIF::cmr_collection_id{$shortName}{$supportedVersion}{'concept_id'};
            if (defined $difConcept) {
                my $uri = $DIF::CMR_ENDPOINT_URI;
                $uri =~ s/\/+$//;
                $difUrl = $uri . "/search/concepts/" . $difConcept;
            }
            my $cmrUrl = $CFG::cfg_data_to_dif{$shortName}{$supportedVersion}{'cmrUrl'};
            # Replace local collection metadata file, use CMR's collection metadata link instead
            if ((defined $difUrl) && (defined $cmrUrl) && ($difUrl eq $cmrUrl)) {
                $collUrl = $difUrl;
            } elsif (defined $difUrl) {
                $collUrl = $difUrl;
            } elsif (defined $cmrUrl) {
                $collUrl = $cmrUrl;
            }
        } elsif (defined $CFG::cfg_data_to_dif{$shortName}{$supportedVersion}{path}
            && (-f $CFG::cfg_data_to_dif{$shortName}{$supportedVersion}{path})
            && ($CFG::cfg_collection_link{$shortName}{$supportedVersion} eq 'S4PA')) {
            # use relative URL from the URL root, excluding protocol and hostname
            $CFG::cfg_data_to_dif{$shortName}{$supportedVersion}{url} =~ m#(://([^/]+))?(/.*)$#;
            $collUrl = $3;
        }
    } else {
        if (defined $CFG::cfg_data_to_dif{$shortName}{$supportedVersion}{path}
            && (-f $CFG::cfg_data_to_dif{$shortName}{$supportedVersion}{path})) {
            # use relative URL from the URL root, excluding protocol and hostname
            $CFG::cfg_data_to_dif{$shortName}{$supportedVersion}{url} =~ m#(://([^/]+))?(/.*)$#;
            $collUrl = $3;
        }
    }

    if (defined $collUrl) {
        my ( $collectionNode )
            = $doc->getElementsByTagName( 'CollectionMetaData' );
        # CollectionMetaData is defined at this point; no need to check for its
        # existence.
        # Look for collection URL; if one exists, replace it.
        my ( $urlNode ) = $collectionNode->getElementsByTagName( 'URL' );

        # Can't just replace the URL with new collection metadata link
        # there is a lot more text under URL other than just the link
        # So, it will be earsier just remove the current URL and create a new one
        if ( defined $urlNode ) {
            # ReplaceXMLNode( $urlNode, 
            #     $CFG::cfg_data_to_dif{$shortName}{$supportedVersion}{url} );    
            $collectionNode->removeChild( $urlNode );
        }

        $urlNode = XML::LibXML::Element->new( 'URL' );
        $urlNode->setAttribute( "xmlns:xlink",
            "http://www.w3.org/1999/xlink" );
        $urlNode->setAttribute( "xlink:type", "simple" );
        $urlNode->setAttribute( "xlink:href", $collUrl );
        $urlNode->setAttribute( "xlink:show", "new" );
        $urlNode->setAttribute( "xlink:actuate", "onRequest" );
        $urlNode->setAttribute( "xlink:title",
            "Click to view $shortName collection");            
        $urlNode->appendText($collUrl);
        $collectionNode->appendChild( $urlNode );
    }


    # Find the data granule node and its children for checksum, size,
    # insert_date_time.
    my ( $granule ) = $doc->findnodes( 'DataGranule' );
    my ( $granuleId ) = $granule->getElementsByTagName( 'GranuleID' );
    unless ( defined $granuleId ) {
	S4P::logger( "ERROR", "Failed to find 'GranuleID' in $metFile" );
	return 0;
    }

    # Determine the checksum type in the incoming granule
    my ( $fileGroupCheckSumType );
    foreach my $fileSpec ( @{$fileGroup->file_specs} ) {
	next unless defined $fileSpec->{file_cksum_type};
	$fileGroupCheckSumType = $fileSpec->{file_cksum_type};
	last;
    }
    $fileGroupCheckSumType = 'CRC32' unless defined $fileGroupCheckSumType;
    $fileGroupCheckSumType = 'CRC32' if ( $fileGroupCheckSumType eq 'CKSUM' );

    my ( $checksum ) = $granule->findnodes( 'CheckSum' );
    unless ( defined $checksum ) {
	$checksum = XML::LibXML::Element->new( 'CheckSum' );
	my ( $prevNode ) = $granule->findnodes( 'Format' );
	( $prevNode ) = $granule->findnodes( 'LocalGranuleID' )
	unless defined $prevNode;
	$prevNode = $granuleId unless defined $prevNode;
	$granule->insertAfter( $checksum, $prevNode );
    }
    my ( $checksumType ) = $checksum->findnodes( 'CheckSumType' );
    my ( $mainChecksum ) = $checksum->findnodes( 'CheckSumValue' );
    if ( defined $checksumType ) {
        ReplaceXMLNode( $checksumType, $fileGroupCheckSumType );
    } else {
	$checksumType = XML::LibXML::Element->new( 'CheckSumType' );
	$checksumType->appendText( $fileGroupCheckSumType );
	if ( defined $mainChecksum ) {
	    $checksum->insertBefore( $checksumType, $mainChecksum );
	} else {
	    $checksum->appendChild( $checksumType );
	}
    }
    unless ( defined $mainChecksum ) {
        $mainChecksum = XML::LibXML::Element->new( 'CheckSumValue' );
        $checksum->appendChild( $mainChecksum );
    }
    my ( $mainSize ) = $granule->getElementsByTagName( 'SizeBytesDataGranule' );
    # If size tag doesn't exist, create one.
    unless ( defined $mainSize ) {
        $mainSize = XML::LibXML::Element->new( 'SizeBytesDataGranule' );
        $granule->insertAfter( $mainSize, $checksum );
    }

    my ( $browse ) = $fileGroup->browse_file();
    # If browse tag doesn't exist, create one.
    if ( $browse ) {
        my ( $browseFile ) = $granule->findnodes( 'BrowseFile' );
        unless ( defined $browseFile ) {
            $browseFile = XML::LibXML::Element->new( 'BrowseFile' );
            $granule->insertBefore( $browseFile, $mainSize );
        }
        ReplaceXMLNode( $browseFile, basename($browse) );
    }

    my ( $insertDateTime ) = $granule->findnodes( '//InsertDateTime' );
    # If insert_date_time tag doesn't exist, create one.
    unless ( defined $insertDateTime ) {
        $insertDateTime = XML::LibXML::Element->new( 'InsertDateTime' );
        $granule->insertAfter( $insertDateTime, $mainSize );
    }

    # Replace the granule ID and checksum; compute the total size of
    # file group.
    my $mainFile = $granuleId->string_value();
    $mainFile =~ s/^\s+|\s+$//g;
    my $found = 0;
    my $totalSize = 0;
    
    # Try to find the archived filename corresponding to the granule ID
    $mainFile = $fileMap->{$mainFile} if ( defined $fileMap->{$mainFile} );

    # First try for an exact match with the archived file
    my $flag = 0;
    foreach my $fileSpec ( @{$fileGroup->file_specs()} ) {
        next if ( $fileSpec->file_type =~ /(METADATA|BROWSE|QA|HDF4MAP)/ );
        $totalSize += ( -s $fileSpec->pathname() );
	# Make all non-METADATA types "SCIENCE"; from this point
	# onwards, S4PA will have to deal with either "SCIENCE" or
	# "METADATA" files only.
        # adding "BROWSE", "QA", "HDF4MAP" file support
	$fileSpec->file_type( 'SCIENCE' );
        if ( $fileSpec->file_id eq $mainFile ) {
            ReplaceXMLNode( $granuleId, $fileSpec->file_id );
            my $crc = S4PA::Storage::ComputeCRC( $fileSpec->pathname(),
		$fileGroupCheckSumType );
            ReplaceXMLNode( $mainChecksum, $crc );
	    $fileSpec->{file_cksum_value} = $crc;
	    $fileSpec->{file_cksum_type} = $fileGroupCheckSumType;
            $flag = 1;
        }
    }
    
    # Next try pattern matching to match with the archived file
    unless ( $flag ) {
        foreach my $fileSpec ( @{$fileGroup->file_specs()} ) {
            next if ( $fileSpec->file_type =~ /(METADATA|BROWSE|QA|HDF4MAP)/ );
	    # Make all non-METADATA types "SCIENCE"; from this point
	    # onwards, S4PA will have to deal with either "SCIENCE" or
	    # "METADATA" files only.
            # adding "BROWSE", "QA", "HDF4MAP" file support
	    $fileSpec->file_type( 'SCIENCE' );
            if ( $fileSpec->file_id =~ /$mainFile/ ) {
                ReplaceXMLNode( $granuleId, $fileSpec->file_id );
                my $crc = S4PA::Storage::ComputeCRC( $fileSpec->pathname(),
		    $fileGroupCheckSumType );
                ReplaceXMLNode( $mainChecksum, $crc );
	        $fileSpec->{file_cksum_value} = $crc;
	        $fileSpec->{file_cksum_type} = $fileGroupCheckSumType;
                $flag = 1;
            }
        }
    }
    
    # If we still don't find a match, fail
    unless ( $flag ) {
        S4P::logger( "ERROR", "Failed to match the granule ID in metadata"
            . " with an archived file" );
        return 0;
    }

    # Update granule size and insert date time.
    ReplaceXMLNode( $mainSize, $totalSize );
    ReplaceXMLNode( $insertDateTime, S4P::timestamp() );

    # If the granule is a multi-file granule and if the supplied metadata
    # contains their attributes, update them.
    my ( @granulitList ) = $granule->findnodes( '//Granulits/Granulit' );
    foreach my $granulit ( @granulitList ) {
        my ( $fileNode ) = $granulit->findnodes( 'FileName' );
        my $fileName = $fileNode->string_value();
        foreach my $fileSpec ( @{$fileGroup->file_specs()} ) {
            next if ( $fileSpec->file_type =~ /(METADATA|BROWSE|QA|HDF4MAP)/ );
            if ( $fileSpec->file_id =~ /$fileName/ ) {
                my ( $fileSize ) = $granulit->findnodes( 'FileSize' );
		my ( $checksumType ) = $granulit->findnodes( 'CheckSum/CheckSumType' );
                my ( $checksumVal ) = $granulit->findnodes( 'CheckSum/CheckSumValue' );
                ReplaceXMLNode( $fileNode, $fileSpec->file_id );
                ReplaceXMLNode( $fileSize, -s $fileSpec->pathname() );
                my $crc = S4PA::Storage::ComputeCRC( $fileSpec->pathname(),
		    $fileGroupCheckSumType );
                ReplaceXMLNode( $checksumVal, $crc );
		ReplaceXMLNode( $checksumType, $fileGroupCheckSumType );
		$fileSpec->{file_cksum_value} = $crc;
		$fileSpec->{file_cksum_type} = $fileGroupCheckSumType;
            }
        }
    }

    # If the granule is a multi-file granule and if the supplied metadata
    # doesn't contain their attributes, create them.
    if ( @granulitList == 0 && scalar( $fileGroup->data_files() ) > 1 ) {
        my $granulIts = XML::LibXML::Element->new( 'Granulits' );
        my $count = 0;
        foreach my $fileSpec ( @{$fileGroup->file_specs()} ) {
            next if ( $fileSpec->file_type =~ /(METADATA|BROWSE|QA|HDF4MAP)/ );
            my $granulIt = XML::LibXML::Element->new( 'Granulit' );
            my $granulItId = XML::LibXML::Element->new( 'GranulitID' );
            if ( $count ) {
                $granulItId->appendText( sprintf( "x%2.2d", $count-1 ) );
            } else {
                $granulItId->appendText( 'main' );
            }
            my $fileName = XML::LibXML::Element->new( 'FileName' );
            $fileName->appendText( $fileSpec->file_id );
            my $checkSum = XML::LibXML::Element->new( 'CheckSum' );
            my $checkSumType = XML::LibXML::Element->new( 'CheckSumType' );
            $checkSumType->appendText( $fileGroupCheckSumType );
            my $checkSumValue = XML::LibXML::Element->new( 'CheckSumValue' );
            $checkSumValue->appendText(
                S4PA::Storage::ComputeCRC( $fileSpec->pathname(),
		    $fileGroupCheckSumType ) );
            $checkSum->appendChild( $checkSumType );
            $checkSum->appendChild( $checkSumValue );
            $granulIt->appendChild( $granulItId );
            $granulIt->appendChild( $fileName );
            $granulIt->appendChild( $checkSum );
            $granulIt->appendTextChild( 'FileSize',
                                        (-s $fileSpec->pathname()) );
            $granulIts->appendChild( $granulIt );
            $count++;
        }
	my ( $prevNode ) = $granule->findnodes( 'DayNightFlag' );
	if ( defined $prevNode ) {
	    $granule->insertBefore( $granulIts, $prevNode )
	} else {
	    ( $prevNode ) = $granule->findnodes( 'PGEVersionClass' );
	    ( $prevNode ) = $granule->findnodes( 'ProductionDateTime' )
		unless defined $prevNode;
	    ( $prevNode ) = $granule->findnodes( 'InsertDateTime' )
		unless defined $prevNode;
	    $granule->insertAfter( $granulIts, $prevNode )
	}
        $granule->appendChild( $granulIts );
    }

    # Update file group's version with the value from metadata
    ( $versionNode ) = $doc->findnodes( '//CollectionMetaData/VersionID' );
    $fileGroup->data_version( $versionNode->string_value(), "%s" )
        if $versionNode;

    # include QA file content into <ProducersQA> if exists.
    my $qaFile = $fileGroup->qa_file();
    if ( $qaFile ) {
        my $qaContent = S4P::read_file( $qaFile );
        unless ( $qaContent ) {
            S4P::logger( 'ERROR', "Failed to read $qaFile" );
            return 0;
        }
        my $producersQA = XML::LibXML::Element->new( 'ProducersQA' );
        $producersQA->appendText( $qaContent );
        $doc->appendChild( $producersQA );
    }

    # Make sure that the metadata file has .xml suffix
    $metFile =~ s/\.(\w+)$/\.xml/ unless ( $metFile =~ /\.xml$/ );
    unless ( S4P::write_file( $metFile, $dom->toString(1) ) ) {
        S4P::logger( 'ERROR', "Failed to update $metFile" );
        return 0;
    }

    # Make sure that the file group's metadata file is renamed with a .xml
    # suffix
    unless ( $fileGroup->met_file() =~ /\.xml$/ ) {
        unlink( $fileGroup->met_file() );
	foreach my $fileSpec ( @{$fileGroup->file_specs} ) {
	    next unless ( $fileSpec->file_type() eq 'METADATA' );
	    $fileSpec->pathname( $metFile );
	    $fileSpec->file_size( -s $metFile );
	}
    }
    return 1;
}

################################################################################

=head1 ReplaceXMLNode

Description:

    Given a XML::LibXML::Node and a value, it is replaced in the DOM with the
    specified value. It is for internal use only.

Input:
    XML::LibXML::Node and its intended text content.
     
Output:
    None
    
Algorithm:


=head1 AUTHOR

M. Hegde

=cut

sub ReplaceXMLNode
{
    my ( $oldNode, $value ) = @_;

    # Get the parent node of the node being replaced
    my $parent = $oldNode->parentNode();
    return 0 unless defined $parent;

    # Get the next sibling of the node being replaced
    my $sibling = $oldNode->nextSibling();

    # Clone the node being replaced and replace the content with the new
    # value
    my $newNode = XML::LibXML::Element->new( $oldNode->getName() );
    $newNode->appendText( $value );

    # Remove the node being replaced from the tree
    $parent->removeChild( $oldNode );

    # If the old node had a sibling, insert the new node after that. Otherwise,
    # insert as a child.
    if ( $sibling ) {
        $parent->insertBefore( $newNode, $sibling );
    } else {
        $parent->appendChild( $newNode );
    }
}

################################################################################

=head1 FindSupportedVersion

Description:

    Given a dataset and its version, finds the version used for storing in S4PA.

Input:
    Dataset and version ID
     
Output:
    Supported version in S4PA or undefined
    
Algorithm:

=head1 AUTHOR

M. Hegde

=cut

sub FindSupportedVersion
{
    my ( $dataset, $versionID ) = @_;

    # Find the match for version from configuration
    my @versionList = defined $CFG::cfg_access{$dataset}
        ? keys %{$CFG::cfg_access{$dataset}}
        : ();
    unless ( @versionList ) {
        S4P::logger( "ERROR",
            "Data version information not found for $dataset" );
        return 0;
    }

    # Find the supported version
    my $supportedVersion = defined $CFG::cfg_access{$dataset}{$versionID}
        ? $versionID
        : defined $CFG::cfg_access{$dataset}{''}
        ? '' : undef;
    return $supportedVersion;
}

################################################################################

=head1 GetAccessType

Description:

    Given a dataset, its version and an optional type ('FILE'), returns the
    granule access type in S4PA. If type='FILE', access types are UNIX file
    modes. Otherwise, access types are 'restricted', 'hidden' and 'public'.

Input:
    Dataset, version ID and an optional type ('FILE').
     
Output:
    S4PA granule access type or undefined.
    
Algorithm:


=head1 AUTHOR

M. Hegde

=cut

sub GetAccessType
{
    my ( $dataset, $version, $type ) = @_;

    # Set the file permission to implement access restriction.
    my $perm = defined $CFG::cfg_access{$dataset}{$version}
            ? $CFG::cfg_access{$dataset}{$version}
            : defined $CFG::cfg_access{$dataset}{""}
            ? $CFG::cfg_access{$dataset}{""}
            : defined $CFG::cfg_access{_default}
            ? $CFG::cfg_access{_default}
            : undef;
    return $perm if ( $type eq 'FILE' );
    return undef unless defined $perm;
    my $access = ( $perm == 420 ) ? 'public'
        : ( $perm == 416 ) ? 'restricted'
        : ( $perm == 384 ) ? 'hidden'
        : undef;
    return $access;
}

################################################################################

=head1 MoveProcessedFiles

Description:

    Given an input PDR and output PDR (used by s4pa_recv_data.pl), it moves
    files to their archive location and cleans up..

Input:
    Input and output PDR (S4P::PDR)
     
Output:
    Returns 1 or 0 (success or failure) and new PDR without QA files.
    
Algorithm:


=head1 AUTHOR

M. Hegde

=cut

sub MoveProcessedFiles
{
    my ( $inPdr, $outPdr ) = @_;
    
    my $status = 1;
    my $dirHash = {};

    # Create a holder PDR for archived files.
    my $newPdr = S4P::PDR->new();
    $newPdr->originating_system( $outPdr->originating_system );
    $newPdr->expiration_time( $outPdr->expiration_time );

    FILE_GROUP: foreach my $outFileGroup ( @{$outPdr->file_groups} ) {
        my $fileGroupSize = Math::BigInt->new();
        my $dataset = $outFileGroup->data_type();
        my $versionId = $outFileGroup->data_version();
        my $supportedVersion =
            S4PA::Receiving::FindSupportedVersion( $dataset, $versionId );
        # Index for mapping the file group in the output PDR with the input PDR
        my $index = $outFileGroup->{index};
        my $oldPath;
        FILE_SPEC: foreach my $outFileSpec ( @{$outFileGroup->file_specs} ) {
            # Move the file to dataset+version directory
            unless ( defined $oldPath ) {
                $oldPath = $outFileSpec->directory_id();
                $dirHash->{$oldPath} = 1;
                $dirHash->{dirname($oldPath)} = 1;
            }
            if ( $outFileGroup->status() eq 'SUCCESSFUL' ) {
                my $newPath = dirname( dirname( $oldPath ) ) . "/$dataset";
                $newPath .= ".$supportedVersion"
                    unless ( $supportedVersion eq '' );
                my $errorFlag = 0;
                unless ( -d $newPath ) {
                    unless ( mkdir ( $newPath, 0777 ^ umask ) && ( -d $newPath ) ) {
                        unless ( -d $newPath ) {
                            S4P::logger( "ERROR",
                                "Failed to create $newPath for moving files from"
                                . " $oldPath ($!)" );
                            $outFileGroup->status( 'FAILURE' );
                            $errorFlag = 1;
                        }
                    }
                }
                if ( move( $outFileSpec->pathname, "$newPath/" ) ) {
                    $outFileSpec->directory_id ( $newPath );
                } else {
                    S4P::logger( "ERROR",
                        "Failed to move " . $outFileSpec->pathname 
                        . " to $newPath" );
                    $outFileGroup->status( 'FAILURE' );
                    $errorFlag = 1;
                }
                # If all files have not been moved, indicate an error
                if ( $outFileGroup->status() ne 'SUCCESSFUL' ) {
                    my @inFileGroupList = @{$inPdr->file_groups};
                    $inFileGroupList[$index]->status( 'FAILURE' );
                    foreach my $inFileSpec ( @{$inFileGroupList[$index]->file_specs} ) {
                        $inFileSpec->status( 'INTERNAL ERROR (FAILURE TO MOVE DATA)' );
                    }         
                }
                last FILE_SPEC if $errorFlag;                
            } 
        }   # End of FILE_SPEC:
        

        # Create a file group to hold archived files
        my $newFileGroup = new S4P::FileGroup;
        $newFileGroup->data_type( $outFileGroup->data_type() );
        $newFileGroup->data_version( $outFileGroup->data_version(), "%s" );
        $newFileGroup->status( $outFileGroup->status() );

        # Free up disk space
        my $actualFileGroupSize = Math::BigInt->new();
        foreach my $outFileSpec ( @{$outFileGroup->file_specs} ) {
            my $file = $outFileSpec->pathname();
            my $fileType = $outFileSpec->file_type();
            if ( $outFileGroup->status() eq 'SUCCESSFUL' ) {
                if ( $fileType eq 'QA' ) {
                    unlink( $file );
                    next;
                } else {
                    # Accumulate the size of the file group
                    $actualFileGroupSize += ( -s $file );
                }
            } else {
                # Remove files that have not been processed
                unlink ( $file );
            }
            my $fileSpecList = $newFileGroup->add_file_spec( $file, $fileType );
            my $newFileSpec = $fileSpecList->[-1];
            if ( defined $outFileSpec->{file_cksum_value} ) {
                $newFileSpec->{file_cksum_type} = $outFileSpec->{file_cksum_type};
                $newFileSpec->{file_cksum_value} = $outFileSpec->{file_cksum_value};
            }
        }
        $newPdr->add_file_group( $newFileGroup );

        # Update directory based disk space management
        my $fsSizeFile = dirname( dirname $oldPath ) . '/.FS_SIZE';
        next unless ( -f $fsSizeFile );
        # If the file group wasn't successful, give up the entire reserved 
        # size
        my $delta = $outFileGroup->{RESERVED_SIZE};
        if ( $outFileGroup->status() eq 'SUCCESSFUL' ) {
            # Readjust the disk space based on the original reserved size and
            # the actual size.
            $delta -= $actualFileGroupSize;
        }
        # Update disk space        
        my @sizeList = DiskPartitionTracker( $fsSizeFile, "update", $delta );
        unless ( @sizeList == 2 ) {
            S4P::logger( "ERROR",
                "Failed to update $fsSizeFile while giving back reserved"
                . " space" );
            $status = 0;
        }
        if ( $sizeList[0]->is_nan() || $sizeList[1]->is_nan() ) {
            S4P::logger( "ERROR",
                "Size read from disk space tracker file, $fsSizeFile, contains"
                . " non-number" );
            $status = 0;
        }
    }   # End of FILE_GROUP:

    foreach my $dir ( reverse sort keys %$dirHash ) {
        my $activeDownloadIndicatorFile = dirname($dir) . "/.RUNNING_" 
            . $inPdr->{NAME};
        unlink( $activeDownloadIndicatorFile )
            if ( -f $activeDownloadIndicatorFile );
        unless ( rmdir ( $dir ) ) {
            S4P::logger( "ERROR",
                "Failed to remove $dir while cleaning up ($!)" );
            $status = 0;
        } 
    }

    $newPdr->file_groups( [] ) unless ( $newPdr->total_file_count() > 0 );
    return ( $status, $newPdr );
}

################################################################################

=head1 FailureHandler

Description:

    It is used as the failure handler for ReceiveData station. For a "RETRY"
    job, it asks for confirmation before proceeding. For everything else,
    the job is resubmitted.

Input:
    None
     
Output:
    Returns 1 or 0 (success or failure).
    
Algorithm:


=head1 AUTHOR

M. Hegde

=cut

sub FailureHandler
{
    my $option = shift;
    my $dir = cwd();
    return 0 if S4P::still_running( $dir );
    my ( $status, $pid, $owner, $wo, $comment ) = S4P::check_job( $dir );
    unless ( $status ) {
        S4P::logger( "ERROR", "Could not get status" );
        return 0;
    }
    # Check whether the input work order is a RETRY job
    my $retryFlag = eval {
        my $xmlParser = XML::LibXML->new();
        my $dom = $xmlParser->parse_file( $wo );
        $dom ? 1 : 0;
    };
    
    if ( $retryFlag ) {
        # For "RETRY" jobs, get a confirmation for retrying if the PAN has been
        # sent.
        my $panConfirmation = ( $option eq 'auto_restart' ) ? 0 : 1;
        
        my $xmlParser = XML::LibXML->new();
        my $dom = $xmlParser->parse_file( $wo );
        my $doc = $dom->documentElement();
        my ( $node ) = $doc->getChildrenByTagName( 'original' );
        my $originalWorkOrder = defined $node
            ? '../' . $node->string_value() : undef;
        my ( $panNode ) = $doc->getChildrenByTagName( 'pan' );
        if ( (defined $panNode) && $panConfirmation ) {
            # create the GUI
            my $main = MainWindow->new( -title   => 'Wait A Second!' );

            # title label
            my $titleFrame = $main->Frame()->pack( -anchor => 'w' );
            my $titleLabel = $titleFrame->Label( -text => 'PAN has been sent!',
                -justify    => 'left', -foreground => 'blue' )->pack();
            # a dummy separator
            my $separator = $main->Frame( -height     => 2, 
                              -relief     => 'ridge', 
                              -background => 'black' )->pack( -fill => 'x' );

            # PAN has been sent
            my $descFrame = $main->Frame( -borderwidth => 2, 
                              -relief      => 'flat' )->pack( -anchor => 'w' );
            my $yScroll = $descFrame->Scrollbar();
            my $messageBox = $descFrame->Text( -width => 80,
                                  -height => 10,
                                  -yscrollcommand => [ 'set', $yScroll ],
                                  -wrap => 'word',
                                  );
            $yScroll->configure( -command => [ 'yview', $messageBox ] );
            $yScroll->pack( -side => 'right', -fill => 'y' );
            $messageBox->pack( -side => 'left', -fill => 'both' );
            
            # dummy separator
            $separator = $main->Frame( -height     => 2, 
                           -relief     => 'ridge', 
                           -background => 'black' )->pack( -fill => 'x' );
            my $panFile = $panNode->textContent();
            my $panContent = S4P::read_file( $panFile );
            $panContent = "PAN, $panFile, doesn't exist!" unless ( $panContent );
            $messageBox->insert( 'end', $panContent );
            # ok/cancel buttons                           
            my $botFrame = $main->Frame()->pack( -ipady => 5 );
            my $cancelButton = $botFrame->Button( -text    => 'Cancel', 
                                      -command => [sub { exit(0); }] );
            my $continueButton = $botFrame->Button( -text    => 'Continue & Retry', 
                                      -command => [sub { $main->destroy() }] );  
            $cancelButton->grid( -column => 0, -row => 0 );
            $continueButton->grid( -column => 1, -row => 0 );
            MainLoop;
        }
        
        # At this point, extract the work order content and name it with
        # the original work order name.
        if ( defined $originalWorkOrder ) {
            ( $node ) = $doc->getChildrenByTagName( 'content' );
            if ( defined $node ) {
                if ( S4P::write_file( $originalWorkOrder,
                    $node->textContent() . "\n" ) ){
                    return S4P::remove_job();
                } else {
                    S4P::logger( "ERROR",
                        "Failed to write $originalWorkOrder" );
                    return 0;
                }
            } else {
                S4P::logger( "ERROR",
                    "$wo is a retry job; but, can't find PDR content!" );
                return 0;
            }
        } else {
            S4P::logger( "ERROR",
                "$wo is in XML; but, not a retry job!" );
            return 0;
        }
    } else {
        # If it is not a "RETRY" job, resubmit the job.
        return S4P::remove_job() if S4P::restart_job();
        return 0;
    }
    return 0;       
}

################################################################################

=head1 FindNextFs

Description:

    It is used to locate the next available volume from a configuration file
    for the new active_fs.

Input:
    configuration file path, current active_fs link
     
Output:
    Returns 'next available active_fs' or undef (success or failure).
    
Algorithm:


=head1 AUTHOR

Guang-Dih Lei

=cut

sub FindNextFs {
    my ( $fsListFile, $activeFs ) = @_;

    # remove trailing '/' if exist
    $activeFs =~ s/\/+$//;

    my @fsList;
    if ( open( FH, "$fsListFile" ) ) {
        @fsList = <FH>;
        close( FH );
    } else {
        S4P::logger( 'ERROR', "Failed to open $fsListFile for reading" );
        return undef;
    }
    chomp @fsList;
    my $numFs = scalar( @fsList );

    # make sure the current active_fs parent directory match with the
    # volume configuration file record (first line in file).
    my $activeVolume = basename( $activeFs );
    my $fsRoot = dirname( $activeFs );
    if ( $fsList[0] ne $fsRoot ) {
        S4P::logger( 'ERROR', "ActiveFS parent directory mismatch" );
        return undef;
    }

    # locate the next available volume
    for ( my $i = 1; $i < $numFs; $i++ ) {
        if ( $fsList[$i] eq "$activeVolume" ) {
            # locate next volume
            my $nextIndex = ( $i == $numFs - 1 ) ? 1 : $i + 1;
            # loop around for one round only
            while ( $nextIndex != $i ) {
                my $nextActiveFs = "$fsRoot/$fsList[$nextIndex]";
                # make sure next volume entry is a directory
                # and only assign active_fs if the volume is not closed
                if ( -d $nextActiveFs && 
                     ! -e $nextActiveFs . "/.FS_COMPLETE" ) {
                     S4P::raise_anomaly('ActiveFs_Rotated', dirname( $fsListFile ),
                         'WARN', "active_fs got rotated back to $nextActiveFs.", 0)
                         if ( $nextIndex < $i );
                    return "$nextActiveFs";
                }
                # advancing to the next volume, loop back if hit bottom
                $nextIndex = ( $nextIndex == $numFs - 1 ) ? 1 : $nextIndex + 1;
            }
        }
    }
    return undef;
}

################################################################################

=head1 NeedMapFile

Description:
    It is used to check if HDF4 Map file is needed for the current fileGroup

Input:
    FileGroup. 
     
Output:
    Returns HDF4 Map extraction command if needed. Otherwise, 0.
    
Algorithm:

=head1 AUTHOR

Guang-Dih Lei

=cut

sub NeedMapFile {
    my ( $fileGroup ) = @_;

    # No need for extraction if map file is already included in the 
    # incoming fileGropu. This is probably a replication PDR.
    return 0 if ( $fileGroup->map_file() );

    # No need for map file if the whole dataset was not configured as needed.
    my $dataset = $fileGroup->data_type();
    return 0 unless ( defined $CFG::cfg_hdf4map_methods{$dataset} );

    # we can't rely on the data_version in the original fileGroup.
    # All S4PM PDRs has 3 digits version (e.g. 002) but S4PA only has
    # one digit (e.g. 2). we need to figure out the actual version
    # from the extracted metadata.

    # First, parse the metadata file
    my $metFile = $fileGroup->met_file();
    my $xmlParser = XML::LibXML->new();
    my $dom = $xmlParser->parse_file( $metFile );
    my $doc = $dom->documentElement();
    # Find the version ID
    my ( $versionNode ) = $doc->findnodes( '//CollectionMetaData/VersionID' );
    my $versionID = ( defined $versionNode ) ?  $versionNode->string_value() : undef;
    return 0 unless ( defined $versionID );
    my $supportedVersion =
        S4PA::Receiving::FindSupportedVersion( $dataset, $versionID );
    my $extractCommand = $CFG::cfg_hdf4map_methods{$dataset}{$supportedVersion};
    return 0 unless ( defined $extractCommand );

    return $extractCommand;
}

################################################################################

=head1 SftpConnect

Description:
    It is used to connect to remote host via SFTP protocol

Input:
    RemoteHost, Port.
     
Output:
    Returns an sftp session object.
    
Algorithm:
    Find login information fron ${HOME}/.netrc file.
    Try establish sftp session with private key.
    If no ssh-key exchange, try establish sftp session with password.

=head1 AUTHOR

Guang-Dih Lei

=cut

sub SftpConnect {
    my ($host, $port) = @_;

    my $machine = Net::Netrc->lookup($host);
    my $login = (defined $machine) ? ($machine->login()) : undef;
    unless (defined $login) {
        S4P::logger('ERROR', "Machine/login entry not found in .netrc for $host");
        return undef;
    }
    my $passwd = (defined $login) ? ($machine->password()) : undef;
    S4P::logger('INFO', "Found login info for $host");

    my $sftp;
    # use private keys to authenticate first.
    # Foreign.pm does not take public keys file with 644 attribute,
    # and it will require a passphrase too.
    my $dsaKeyFile = "$ENV{HOME}/.ssh/id_dsa";
    my $rsaKeyFile = "$ENV{HOME}/.ssh/id_rsa";

    my $remoteHost = $host;
    my $remotePort = ($port) ? $port : '';
    if ($ENV{FTP_FIREWALL}) {
        $remoteHost = $ENV{FTP_LOCALHOST} ? $ENV{FTP_LOCALHOST} : 'localhost';
        my $localPort = $ENV{FTP_LOCAL_PORT} ? $ENV{FTP_LOCAL_PORT} : '30001';
        $remotePort = $localPort;
    }

    # establish ssh connection to remote host via specific port
    if ($port) {
        # try using dsa key-exchange first
        $sftp = Net::SFTP::Foreign->new($remoteHost, user=>"$login",
            key_path=>"$dsaKeyFile", port=>"$remotePort");
        if ($sftp->error) {
            # then rsa key
            $sftp = Net::SFTP::Foreign->new($remoteHost, user=>"$login",
                key_path=>"$rsaKeyFile", port=>"$remotePort");
            if ($sftp->error) {
                # then try password
                $sftp = Net::SFTP::Foreign->new($remoteHost, user=>"$login",
                    password=>"$passwd", port=>"$remotePort");
                if ($sftp->error) {
                    S4P::logger('ERROR', "Failed to sftp connect to $login\@$host.");
                    return undef;
                }
            }
        }
    } else {
        # try using dsa key-exchange first
        $sftp = Net::SFTP::Foreign->new($remoteHost, user=>"$login",
            key_path=>"$dsaKeyFile");
        if ($sftp->error) {
            # then try rsa key
            $sftp = Net::SFTP::Foreign->new($remoteHost, user=>"$login",
                key_path=>"$rsaKeyFile");
            if ($sftp->error) {
                # then try password
                $sftp = Net::SFTP::Foreign->new($remoteHost, user=>"$login",
                    password=>"$passwd");
                if ($sftp->error) {
                    S4P::logger('ERROR', "Failed to sftp connect to $login\@$host.");
                    return undef;
                }
            }
        }
    }

    # sftp session created
    S4P::logger('INFO', "Successfully sftp login into $host");
    return $sftp;
}

################################################################################

=head1 SftpGet

Description:
    It is used to download a single file via SFTP

Input:
    Remote Host, Remote FilePath, LocalDir, SFTP session, Port
     
Output:
    Returns a Net::SFTP::Foreign object for sftp session.
    
Algorithm:

=head1 AUTHOR

Guang-Dih Lei

=cut

sub SftpGet {
    my ($remoteHost, $remotePath, $localDir, $sftp, $port) = @_;

    # Downloaded file name, same as the remote file
    $localDir =~ s/\/+$//;
    my $localFile = "$localDir/" . basename($remotePath);

    # check if there is an existing sftp session
    unless (defined $sftp) {
        # no live session, establish sftp connection to remote host
        $sftp = S4PA::Receiving::SftpConnect($remoteHost, $port);
        unless (defined $sftp) {
            S4P::logger('FATAL', "Failed to get an SFTP connection to $remoteHost");
            return undef;
        }
    }

    # transfer file to local directory without the remote file timestamp
    # and return the sftp session
    $sftp->get($remotePath, $localFile, perm => 0644, copy_time => 0);
    if ($sftp->error) {
        S4P::logger('ERROR', "Failed to transfer $remotePath from $remoteHost.");
        return undef;
    } else {
        return $sftp;
    }

    return $sftp;
}

################################################################################

=head1 SftpGetFileGroup

Description:
    It is used to download all files in a FileGroup via SFTP

Input:
    FileGroup, LocalDir, SFTP session, Port
     
Output:
    Returns a Net::SFTP::Foreign object for sftp session.
    
Algorithm:

=head1 AUTHOR

Guang-Dih Lei

=cut

sub SftpGetFileGroup {
    my ($fileGroup, $remoteHost, $localDir, $sftp, $port) = @_;

    my $numFiles = scalar(@{$fileGroup->file_specs});
    my $count = 0;
    foreach my $fs (@{$fileGroup->file_specs}) {
        my $remotePath = $fs->pathname();
        $sftp = S4PA::Receiving::SftpGet($remoteHost, $remotePath, $localDir, $sftp, $port);
        unless (defined $sftp) {
            S4P::logger('ERROR', "Failed to transfer $remotePath from $remoteHost.");
            return undef;
        }
        $count++;
    }

    # all files transferred, return sftp session
    if ($count == $numFiles) {
        S4P::logger('INFO', "Successfully sftp pull $count files from $remoteHost");
    } else {
        S4P::logger('WARN', "Only sftp pull $count out of $numFiles files from $remoteHost");
    }
    return $sftp;
}

################################################################################

=head1 SftpPut

Description:
    It is used to push a single file to remote host via SFTP

Input:
    A hasf with keys as Remote Host, Remote directory, Local File Path,
    SFTP session, Require verification flag, S4PA logger object
     
Output:
    Returns a Net::SFTP::Foreign object for sftp session.
    
Algorithm:

=head1 AUTHOR

Guang-Dih Lei

=cut

sub SftpPut {
    my (%arg) = @_;

    my $remoteHost = $arg{host};
    my $remoteDir = $arg{dir};
    my $localPath = $arg{file};
    my $remoteFile = basename($arg{file});
    my $sftp = $arg{session};

    # check if there is an existing sftp session
    unless (defined $sftp) {
        # no live session, establish sftp connection to remote host
        $sftp = S4PA::Receiving::SftpConnect($remoteHost);
        unless (defined $sftp) {
            S4P::logger('FATAL', "Failed to get an SFTP connection to $remoteHost");
            return undef;
        }
    }

    # make sure we are in the remote directory
    $sftp->setcwd($remoteDir);
    if ($sftp->error) {
        S4P::logger('ERROR', "Failed to change to $remoteDir on $remoteHost.");
        return undef;
    }

    # transfer file to remote directory with current timestamp and set attribute to 644 
    $sftp->put($localPath, $remoteFile, perm => 0644, copy_time => 0 );
    if ($sftp->error) {
        S4P::logger('ERROR', "Failed to transfer $localPath to $remoteDir.");
        return undef;
    }
    S4P::logger("INFO", "Succeeded on sftp push $localPath to $remoteHost:$remoteDir" );

    # verify if the file actually landed on the remote directory
    if ($arg{verify} && defined $arg{logger}) {
        my $stat = $sftp->stat($remoteFile);
        if ($sftp->error) {
            $arg{logger}->error("Pushed file $remoteFile does not exist on $remoteHost.");
        } elsif ($stat->{size} == 0) {
            $arg{logger}->error("Pushed file $remoteFile has zero length on $remoteHost.");
        } else {
            $arg{logger}->info("Verified $remoteFile landed on $remoteHost:$remoteDir.");
        }
    }

    return $sftp;
}

