=head1 NAME

S4PA::Storage - S4PA subsystem for storing science data

=head1 SYNOPSIS

  use S4PA::Storage;
  S4PA::Storage::Compress()
  S4PA::Storage::ComputeCRC()
  S4PA::Storage::GetStartDate()
  S4PA::Storage::StoreData()
  S4PA::Storage::StoreLink()
  S4PA::Storage::CheckCRC()
  S4PA::Storage::PublishWHOM()
  S4PA::Storage::OpenGranuleDB()
  S4PA::Storage::CloseGranuleDB()
  S4PA::Storage::IsOldData()
  S4PA::Storage::IsIgnoreData()
  S4PA::Storage::IsMultipleDelete()

=head1 DESCRIPTION

S4PA::Storage contains methods involved in "storing" data. They include methods
to compress/uncompress, creating links and storing granule records. Most of
them are used in S4PA's station script, s4pa_store_data.pl.

=head1 SEE ALSO

L<S4PA>, L<S4PA::Receiving>

=cut

################################################################################
# Storage.pm,v 1.15.2.6 2006/10/05 18:13:34 hegde Exp
# -@@@ S4PA, Version: $Name:  $
################################################################################

package S4PA::Storage;

use 5.00503;
use strict;
use POSIX;
use Safe;
use File::Basename;
use File::stat;
use File::Copy;
use File::Temp;
use MLDBM qw(DB_File Storable);
use DB_File;
use Fcntl;
use S4P;
use S4P::TimeTools;
use S4PA::Receiving;
use S4PA::MachineSearch;
use S4PA::Metadata;
use XML::LibXML;
use XML::LibXML::NodeList;
use Time::Local;
use Cwd;
use vars qw($VERSION);

$VERSION = '0.01';

# A flag used to find out whether the incoming data is older than existing
# data based on ProductionDateTime metadata
my $oldDataFlag;
my $ignoreDataFlag;
my $multipleDeleteFlag;

1;

################################################################################

=head1 Anonymous Method

Description:

    Stores a granule's record in DBM file. It is used only in
    S4PA::Storage::StoreData().

Input:

    Dataset name, Date (YYYY/DDD or YYYY/MM or YYYY), List of file names.

Output:

    Returns 0/1 on failure/success.

Algorithm:

    Function
        Open the granule DBM file.
        For each specified file
            Compute UNIX CRC.
            Find the file system based on the specified path.
            Find the file permissions.
            Store CRC, date, file system number and permission in a hash ref.
            If any of above steps fails, return 0.
        Endfor
        For each specified file
            Store the hash ref for the file created above in a hash tied to DBM.
        Endfor
        Close the granule DBM file.
        Return 1.
    End

=head1 AUTHOR

M. Hegde

=cut

my $_storeGranuleRecord =  sub {
    my ( $dataset, $version, $date, @fileList ) = @_;

    my  $datasetDir = ( $version ne '' )
        ? "../../$dataset.$version"
        : "../../$dataset";

    my ( $granuleHashRef, $fileHandle ) =
        ( $version ne '' )
        ? OpenGranuleDB( "$datasetDir/granule.db", "rw" )
        : OpenGranuleDB( "$datasetDir/granule.db", "rw" );

    # If unable to open granule database, complain and return.
    unless ( defined $granuleHashRef ) {
        S4P::logger( 'ERROR',
                     "Failed to open granule database for dataset, $dataset." );
        return 0;
    }
    my $granuleRec = {};

    # Compute attributes to be stored for each file and store them in DBM file.
    # Attributes stored are: checksum, date, file permissions/mode.
    foreach my $file ( @fileList ) {
        # Compute CRC and store it in the granule database
        my $crc = ComputeCRC( $file );

        my $key = basename( $file );
        $granuleRec->{$key}{cksum} = $crc;
        $granuleRec->{$key}{date} = $date;

        # Find the file system of the stored granule
        # ( .+/<fs>/<dataset>/<data file> )
        if ( $file =~ /\/(\d+)\/+$dataset/ ) {
            $granuleRec->{$key}{fs} = $1;
        } else {
            S4P::logger( 'ERROR',
                         "Failed to find the file system of $file" );
            CloseGranuleDB( $granuleHashRef, $fileHandle );
            return 0;
        }
        my $fs = stat( $file );
        $granuleRec->{$key}{mode} = ( $fs->mode() & 07777 );
    }

    # Transfer granule record to tied hash.
    foreach my $file ( @fileList ) {
        my $key = basename( $file );

        # remove the existing record's link target and source if exist
        # and are different with the incoming one to avoid dangling link. 
        # This happened when the incoming data having the same filename 
        # as an existing one but with a different beginning date.
        if ( exists $granuleHashRef->{$key} ) {
            if ( $granuleHashRef->{$key}{date} ne $granuleRec->{$key}{date} ) {
                my $dataDir = readlink( "$datasetDir/data" );
                my $linkTargetDir = "$dataDir/" . $granuleHashRef->{$key}{date};
                $linkTargetDir .= '/.hidden' 
                    if ( $granuleHashRef->{$key}{mode} == 0640 ||
                         $granuleHashRef->{$key}{mode} == 0600 );
                my $linkTarget = "$linkTargetDir/" . $key;
                my $linkSrc = readlink( $linkTarget );
                unless ( unlink $linkTarget ) {
                    S4P::logger( 'ERROR',
                        "Failed to remove an existing symbolic link,"
                        . " $linkTarget. ($!)" );
                }
                # only remove the link source file if it has different 
                # fs volume with the incoming file.
                if ( -f $linkSrc && 
                    ( $granuleHashRef->{$key}{fs} ne $granuleRec->{$key}{fs} ) ) {
                    unless ( unlink( $linkSrc ) ) {
                        S4P::logger( 'WARNING',
                            "Failed to remove an existing file,"
                            . " $linkSrc. ($!)" );
                    }
                }
            }
        }
        $granuleHashRef->{$key} = $granuleRec->{$key};
    }
    CloseGranuleDB( $granuleHashRef, $fileHandle );
    return 1;
};

################################################################################

=head1 Compress

Description:
    Execute compression on a datafile as directed in station's
    s4pa_compress.cfg file.

Input:

    $infile    -  Input file to be compressed.
    $dataset   -  Dataset that $infile belongs to.

Output:

    $outfile   -  Compressed output file. File name is set in cfg file.
    $newsize   -  New file size of $outfile.

Return:

    ($outfile, $newsize)    if successful
    (undef, undef)          if fail

Algorithm:

    Calculate input file size
    Load compression configuration/control file from station directory tree
    If compression is specified for the dataset; Then
        Perform compression
        Calculate new filesize
        Return new filename and size
    Else
        Return original filename and size
    Endif

=head1 AUTHOR

J. Pan, July 19, 2004

=head1 Changelog

10/27/04 J Pan   Added uncompression
04/20/05 J Pan   Removed uncompression and modified according to new cfg format

=cut

sub Compress {
    my ($infile, $dataset) = @_;

    # Original file stats
    my $stat_original = stat($infile);
    my $infile_size = $stat_original->size();

    # Working directory for compression
    my $wkdir = ".";
    $wkdir = $CFG::cfg_compress{Workdir} if exists $CFG::cfg_compress{Workdir};

    # Return original file if compression method not specified in cfg
    return ($infile, $infile_size) if not exists $CFG::cfg_compress{$dataset};

    # Compress
    my $cmd = $CFG::cfg_compress{$dataset}->{Cmd} or return (undef, undef);
    my $outfile_tmp = $CFG::cfg_compress{$dataset}->{TmpOut}
                      or return (undef, undef);
    my $outfile = $CFG::cfg_compress{$dataset}->{Outfile}
                  or return (undef, undef);

    $cmd =~ s/INFILE/$infile/g;
    $outfile_tmp =~ s/INFILE/$infile/g;
    $outfile =~ s/INFILE/$infile/g;

    `$cmd`;
    if ($?) {
        S4P::logger("ERROR", "Compression $cmd failed ($!)");
        unlink $outfile_tmp if -s $outfile_tmp and $outfile_tmp ne $infile;
        return (undef, undef);
    }

    # Create outfile
    if ($outfile_tmp ne $outfile) {
        if (! move($outfile_tmp, $outfile)) {
            S4P::logger("ERROR", "Cannot move file $outfile_tmp $outfile ($!)");
            return (undef, undef);
        }
    }

    # Remove original file
    unlink $infile unless $infile eq $outfile;

    # Get new file stat
    my $st = stat( $outfile );
    my $newsize = $st->size();

    # Return results
    return ($outfile, $newsize);
}

################################################################################

=head1 Uncompress

Description:
    Execute uncompression on a datafile as directed in stations'
    s4pa_compress.cfg file.

Input:

    $infile    -  Input file to be uncompressed.
    $dataset   -  Dataset that $infile belongs to.

Output:

    $outfile   -  Uncompressed output file. File name is set in cfg file.
    $newsize   -  New file size of $outfile.

Return:

    ($outfile, $newsize)    if successful
    (undef, undef)          if fail

Algorithm:

    Calculate input file size
    Load uncompression configuration/control file from station directory tree
    If uncompression is specified for the dataset; Then
        Perform uncompression
        Calculate new filesize
        Return new filename and size
    Else
        Return original filename and size
    Endif

=head1 AUTHOR

J. Pan, April 20, 2005

=head1 Changelog

=cut

sub Uncompress {
    my ($infile, $dataset) = @_;

    # Original file stats
    my $stat_original = stat($infile);
    my $oldsize = $stat_original->size();

    # Working directory for uncompressing data
    my $wkdir = ".";
    $wkdir = $CFG::cfg_uncompress{Workdir}
        if exists $CFG::cfg_uncompress{Workdir};

    return ($infile, $oldsize) if not exists $CFG::cfg_uncompress{$dataset};

    # Uncompress
    my $cmd = $CFG::cfg_uncompress{$dataset}->{Cmd} or return (undef, undef);
    my $outfile_tmp = $CFG::cfg_uncompress{$dataset}->{TmpOut}->($infile)
                      or return ( undef, undef );
    my $outfile = $CFG::cfg_uncompress{$dataset}->{Outfile}->($infile)
                  or return ( undef, undef );

    $cmd =~ s/INFILE/$infile/g;
    $outfile_tmp =~ s/INFILE/$infile/g;
    $outfile =~ s/INFILE/$infile/g;

    `$cmd`;
    if ($?) {
        S4P::logger( "ERROR", "Uncompression $cmd failed" );
        return ( undef, undef );
    }

    if (! -s $outfile_tmp) {
        S4P::logger( "ERROR", "Cannot find uncompressed file $outfile_tmp" );
	unlink( $outfile_tmp ) if ( $infile ne $outfile_tmp );
        return ( undef, undef );
    }

    # Create outfile
    if ($outfile_tmp ne $outfile) {
        if (! move($outfile_tmp, $outfile)) {
            S4P::logger( "ERROR", "Cannot move file $outfile_tmp $outfile" );
            return ( undef, undef );
        }
    }

    # Remove original file
    unlink $infile unless $infile eq $outfile;

    # Get new file stat
    my $st = stat( $outfile );
    my $newsize = $st->size();

    # Return results
    return ( $outfile, $newsize );
}

################################################################################

=head1 ComputeCRC

Description:
    Compute CRC checksum for a given file

Input:

    $fullpath  -  Input file for which CRC is computed

Output:

    $crc - CRC checksum of the file

Return:

    $crc    if successful
    undef   if fail

Algorithm:
    Verify input file (fullpath)
    Call system command cksum or md5sum to compute CRC, depending on type
    Return undef if the system call failed
    Parse cksum results for CRC
    Return CRC

=head1 CHANGELOG
    06/09/18 J Pan    Added MD5 checksum

=head1 AUTHOR

J. Pan

=cut

sub ComputeCRC {
    my ( $file, $type ) = @_;

    # Make sure input file exists and is readable
    unless ( -e $file and -r $file ) {
        S4P::logger( 'ERROR', "File, $file, not found for computing checksum" );
        return undef;
    }

    # Call system cksum to compute
    my $crc_string = "";
    if (defined($type) and uc($type) eq "MD5") {
        $crc_string = `md5sum $file`;
        if ( $? ) {
            S4P::logger( 'ERROR', "`md5sum $file` failed on $file ($!)" );
            return undef;
        }
    } else {
        $crc_string = `cksum $file`;
        if ( $? ) {
            S4P::logger( 'ERROR', "`cksum $file` failed on $file ($!)" );
            return undef;
        }
    }

    # Parse for crc
    my @crc = split( /\s+/, $crc_string );

    return $crc[0];
}

################################################################################

=head1 GetStartDate

Description:

    Get start date in year and day-of-year from metadata.

Input:

    $metadata  -  A string of the metadata document.

Output:

    $year - Year of the data start date
    $month - Month (1-12)
    $doy  - Day of the year of the data start date

Return:

    ($year, $month, $doy) if successful
    (undef, undef, undef) if fail

Algorithm:

    Extract start date string from metadata
    Parse StartDate for year, month and day (all digital literals)
    Convert the date into day-of-year
    If successful
       Return ($year, $month, $doy)
    Else
       Return false
    Endif

=head1 AUTHOR

J. Pan

=cut

sub GetStartDate {
    my ( $metadata ) = @_;

    # Extract start date string from metadata
    my $datestring = $metadata->{RangeBeginningDate};

    # Parse the date string (YYYY-MM-DD)
    my ( $year, $month, $day );
    if ( $datestring =~ /^\s*(\d\d\d\d)-(\d\d)-(\d\d)\s*$/ ) {
        ( $year, $month, $day ) = ( $1, $2, $3 );

        if ( $month < 1 or $month > 12 ) {
            S4P::logger( 'ERROR', "Invalid month ($month)" );
            return ( undef, undef, undef );
        }

        my @dom = ( 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 );
        $dom[2] = 29 if S4P::TimeTools::is_leapyear( $year );
        if ( $day < 1 or $day > $dom[$month] ) {
            S4P::logger( 'ERROR', "Invalid day ($day)" );
            return ( undef, undef, undef );
        }
    } else {
        S4P::logger( 'ERROR', "Failed to extract date from metadata" );
        return ( undef, undef, undef );
    }

    # Convert date to day of the year.
    # Find the day of the year for a local time of midnight on
    # the given day, month, and year
    my $t = POSIX::mktime( 0, 0, 0, $day, $month - 1, $year - 1900 );
    my ( $sec, $min, $hr, $mday, $mon, $yr, $wday, $yday,
         $isdst) = localtime( $t ) or return ( undef, undef, undef );

    return ( $year, $month, $yday + 1 );
}

################################################################################

=head1 StoreData

Description:

    Extract metadata, make browse, compress and move data files to
    storage from archive.

Input:

    $dataset     - Dataset name
    $version     - Dataset version (may be undef)
    $datafile    - Data file name with full path
    $metafile    - Metadata file

Output:

    Files and links in Storage

Return:

    1  - Success
    0  - Fail

Algorithm:

    GetMetadata($datafile)
    MakeBrowse($datafile, $dataset)
    Move metadata_file to archive
    Move browse_file to archive
    ComputeCRC($datafile)
    StoreLink($datafile)
    StoreLink($metadata_file)
    StoreLink($browse_file)
    If successful
       Return true
    Else
       Return false
    Endif

=head1 AUTHOR

J. Pan

=cut

sub StoreData {
    my ( $dataset, $version, $datafile, $metafile ) = @_;

    # Determine path of $datafile, used to create browse and metadata in it
    #
    # May not be needed if path is taken care of by MakeBrowse(), but may still
    # be needed for creating metadata in the path
    my $datapath = dirname( $datafile );

    # Get metadata

    # Parse the metafile for start date. This should be done by calling
    # GetMetadata() or
    # MetFile::methods() eventually, but those are not stable at this time.

    #- my %metadata = S4PA::Receiving::GetMetadata($datapath, $dataset,
    #-                $datafile, $metafile);

    my %metadata = ();
    my $meta = `cat $metafile`;
    my ($datestring) = $meta =~ /.+<RangeBeginningDate>\ *(.+)\ *<\/RangeBeginningDate>.+/is;
    if ( ! $datestring ) {
        S4P::logger( 'ERROR', "Failed to get beginning date time from $metafile" );
        return ( undef, undef );
    }

    $metadata{RangeBeginningDate} = $datestring;

    # --- Date info ---
    my ( $year, $month, $doy ) = S4PA::Storage::GetStartDate(\%metadata);
    unless ( defined $year && defined $month && defined $doy ) {
        S4P::logger( 'ERROR', "Failed to get start date info from metadata" );
        return ( undef, undef );
    }

    # Store a link for the data file
    my $data_link = StoreLink( $datafile, $dataset, $version,
                               $year, $month, $doy );
    unless ( -e $data_link ) {
        S4P::logger( 'ERROR',
                     "Failed to create a link to datafile ($datafile)" );
        return ( undef, undef );
    }

    # Store a link for the metadata file
    my $metadata_link = StoreLink( $metafile, $dataset, $version,
                                   $year, $month, $doy );
    unless ( -e $metadata_link ) {
        S4P::logger( 'ERROR',
                     "Failed to create a link to metadata file ($metafile)" );
        unlink( $data_link );
        return ( undef, undef );
    }

    my $dateString;
    if ( %CFG::cfg_temporal_frequency
         && defined $CFG::cfg_temporal_frequency{$dataset} ) {
        my $frequency = $CFG::cfg_temporal_frequency{$dataset}->{$version};
        if ( $frequency eq 'yearly' ) {
            $dateString = sprintf( "%4.4d", $year );
        } elsif ( $frequency eq 'monthly' ) {
            $dateString = sprintf( "%4.4d/%2.2d", $year, $month );
        } elsif ( $frequency eq 'daily' ) {
            $dateString = sprintf( "%4.4d/%3.3d", $year, $doy );
        } elsif ( $frequency eq 'none' ) {
            # for climatology dataset
            $dateString = '';
        } else {
            S4P::logger( 'ERROR', "Temporal frequency, $frequency"
                                  . ", not supported for dataset $dataset" );
            return ( undef, undef );
        }
    } else {
        $dateString = sprintf( "%4.4d/%3.3d", $year, $doy );
    }

    # Store granule record in a DBM file.
    unless ( $_storeGranuleRecord->( $dataset,
                                     $version,
                                     $dateString,
                                     $datafile, $metafile ) ) {
        # Remove symbolic links that were created.
        unlink( $data_link )
            || S4P::logger( 'ERROR',
                            "Failed to remove $data_link ($!)" );
        unlink( $metadata_link )
            || S4P::logger( 'ERROR',
                            "Failed to remove $metadata_link ($!)" );
        return ( undef, undef );
    }

    return ( $data_link, $metadata_link );
}

################################################################################

=head1 StoreLink

Creates a symbolic link to a specified file given the year and day of the year.
Returns the name of the link if successful; otherwise, returns undefined value.

Algorithm:

    Function StoreLink
        If the station data directory does not exist
            Return undefined.
        Endif

        If the file does not exist
            Return undefined.
        Endif

        If the subdirectories for the specified date do not exist
            Create the subdirectories in the data directory for the
                specified year and day of the year.
            If the creation of subdirectories fails
                Return undefined.
            Endif
        Endif

        Create the link name based on the input (dataset, year,
            day of the year).
        Create a link to the specified file with the link name.

        If the link creation succeeds
            Return the link name.
        Else
            Return undefined.
        Endif

    End

=head1 AUTHOR

M. Hegde

=cut

sub StoreLink {
    my ( $filePath, $dataset, $version, $year, $month, $doy ) = @_;

    # Remove leading and trailing white spaces if any.
    $year =~ s/^\s+|\s+$//g;
    $month =~ s/^\s+|\s+$//g;
    $doy =~ s/^\s+|\s+$//g;

    $month = sprintf( "%2.2d", $month ); # Make the month 2 char long
    $doy = sprintf( "%3.3d", $doy ); # Make the day of the year 3 char long.

    my $dataDir = ($version ne '')
                  ? "../../$dataset.$version/data"
                  : "../../$dataset/data";

    # Check for the existence of the specified file.
    unless ( -f $filePath ) {
        S4P::logger( 'ERROR', "File, $filePath, does not exist" );
        return undef
    }

    # Check whether data directory exists under the Store<dataset> station
    # directory.
    unless ( -d $dataDir and -l $dataDir ) {
        S4P::logger( 'ERROR', "Data directory, $dataDir, doesn't exist" );
        return undef
    }

    # Extract the filename from its full path name.
    my $fileName = basename( $filePath );

    # Create the link's target name.
    my $linkTargetDir;
    if ( %CFG::cfg_temporal_frequency
         && defined $CFG::cfg_temporal_frequency{$dataset} ) {
        my $frequency = $CFG::cfg_temporal_frequency{$dataset}->{$version};
        if ( $frequency eq 'yearly' ) {
            $linkTargetDir = "$dataDir/$year/";
        } elsif ( $frequency eq 'monthly' ) {
            $linkTargetDir = "$dataDir/$year/$month/";
        } elsif ( $frequency eq 'daily' ) {
            $linkTargetDir = "$dataDir/$year/$doy/";
        } elsif ( $frequency eq 'none' ) {
            # for climatology dataset
            $linkTargetDir = "$dataDir/";
        } else {
            S4P::logger( 'ERROR', "Temporal frequency, $frequency"
                                  . ", not supported" );
            return undef;
        }
    } else {
        $linkTargetDir = "$dataDir/$year/$doy/";
    }

    # Determine whether a dataset is hidden based on the file permission
    my $stat = File::stat::stat( $filePath );

    # Tests based on file permissions; the order of these ifs is critical.
    my ( $access, $umask );
    if ( $stat->mode() & 0004 ) {
	$access = 'PUBLIC';
	$umask = 0755;
    } elsif ( $stat->mode() & 0040 ) {
        $access = 'RESTRICTED';
	$umask = 0750;
    } elsif ( $stat->mode() & 0400 ) {
        $access = 'HIDDEN';
	$umask = 0700;
    } else {
        $access = 'PUBLIC';
	$umask = 0755;
    }

    # Modify the path if the data is not public
    $linkTargetDir .= '.hidden/' unless ( $access eq 'PUBLIC' );
    my $linkTarget = $linkTargetDir . $fileName;

    # Create directories needed for storing the symbolic link
    my @dirList = ();
    my $dirName = dirname( $linkTarget );
    while ( ! (-d $dirName ) ) {
        push( @dirList, $dirName );
        $dirName = dirname( $dirName );
    }
    foreach my $dirName ( reverse @dirList ) {
        if ( -d $dirName ) {
	    my $stat = File::stat::stat( $dirName );

	    if ( $access eq 'PUBLIC' ) {
	        # Make sure the parent directory is "public" if the data
		# is public and directory tree at some point contains
		# restricted/hidden data.
	        chmod( 0755, $dirName );
	    } elsif ( $access eq 'RESTRICTED' ) {
	        # Make sure the parent directory permissions relfect restricted
		# access if the directory tree at some point contains
		# hidden data. If anything is public in the directory tree,
		# skip.
	        chmod( 0750, $dirName )
		    unless ( ($stat->mode() & 0777) ^ 0700 );
	    }
	} else {
	    unless ( mkdir( $dirName, $umask ) || ( -d $dirName ) ) {
		S4P::logger( 'ERROR',
			     "Directory, $dirName, doesn't exist and"
			     . " failed to create it ($!)" );
		return undef;
	    }
	}
    }

    # If the link already exists, remove it.
    if ( -l $linkTarget ) {
        my $linkSrc = readlink( $linkTarget );
        unless ( unlink $linkTarget ) {
            S4P::logger( 'ERROR',
                         "Failed to remove an existing symbolic link,"
                         . " $linkTarget. ($!)" );
            return undef;
        }

        # If the link source already exists, make sure it is not the newly
        # downloaded file. This condition will occur when a new version of
        # the file is downloaded to the same active file system.
        if ( -f $linkSrc ) {
            # Make sure that we don't delete the new file by comparing device
            # id and inode number of the file for which a link is requested with
            # the source of the current link. If either device id or inode is
            # different, delete the source.
            my $st_old = stat( $linkSrc );
            my $st_new = stat( $filePath );
            if ( ($st_old->dev != $st_new->dev) ||
                 ($st_old->ino != $st_new->ino) ) {
                unless ( unlink( $linkSrc ) ) {
                    S4P::logger( 'WARNING',
                                 "Failed to remove an existing file,"
                                 . " $linkSrc" );
                    return undef;
                }
            }
        }

    }

    # Create a symbolic link and return the link name with respect to the
    # directory tree pointed to by data subdirectory under Store<dataset>
    # station. If symbolic link creation fails, return undefined.
    unless ( symlink( $filePath, $linkTarget ) ) {
        S4P::logger( 'ERROR',
                     "Failed to create a symbolic link, $linkTarget, to the"
                     . " file, $filePath ($!)" );
        return undef
    }
    my $linkName = readlink( $dataDir );
    unless ( defined $linkName ) {
        S4P::logger( 'ERROR',
                     "Failed to read the link for $dataDir ($!)" );
        return undef
    }

    $linkTarget =~ s/$dataDir/$linkName/;
    return $linkTarget;
}

################################################################################

=head1 PublishWHOM

Description:

    Create csv file with metadata for a given datatype

Input:

    dataset name, datatype name, array of metadata file names, publication
    directory

Output:

    csv filename if successful
    undef if fail

Algorithm:

    For each metadata file name
       compose a string with metadata separated by comma
       store a string in array
    End for
    Compose csv file name
    Create header and write it to csv file
    Write metadata from the array to csv file
    Return csv file name

=head1 AUTHOR

Irina Gerasimov

=cut

sub PublishWHOM {
    my ($attributes, $metafiles, $csvfile, $urls) = @_;
    my %meta;
    my %columns = map { $_ => '1' } values %{$attributes};
    my @columns = sort keys %columns;

    # Get metadata
    foreach my $file (@$metafiles) {
        # Create an XML DOM parser.
        my $xmlParser = XML::LibXML->new();
        my $dom = $xmlParser->parse_file( $file );
        unless ( defined $dom ) {
            S4P::logger( 'ERROR', "Failed to get DOM from generated metadata for $file" );
            next;
        }
        my $doc = $dom->documentElement();
        unless ( defined $doc ) {
            S4P::logger( 'ERROR', "Failed to find document element in $file" );
            next;
        }

        my %granule;
        foreach my $tag (keys %$attributes) {
            my ($node) =  $doc->findnodes("//$tag");
            $granule{$tag} = $node->string_value()        if (defined $node);
        }
        # Check for required attributes
        if (!$granule{'SizeBytesDataGranule'} ||
            !$granule{'RangeBeginningDate'} ||
            !$granule{'RangeEndingDate'}) {
            S4P::logger( 'ERROR', "Failed to extract required attributes in $file" );
            next;
        }

        my (@granulitList) = $doc->findnodes( '//Granulit' );
        foreach my $granulit ( @granulitList ) {
            my ($FileName) = $granulit->findnodes( 'FileName' );
            push @{$granule{'FileName'}}, $FileName->string_value();
            my ($FileSize) = $granulit->findnodes( 'FileSize' );
            push @{$granule{'FileSize'}}, $FileSize->string_value();
        }

        my (@PSAlist) = $doc->findnodes('//PSA');
        foreach my $PSA (@PSAlist) {
            my ($PSAName) = $PSA->findnodes('PSAName');
            next if (!exists $attributes->{$PSAName->string_value()});
            my ($PSAValue) = $PSA->findnodes('PSAValue');
            $granule{$PSAName->string_value()} = $PSAValue->string_value();
        }

        if (exists $granule{'PathNr'}) {
          $granule{'PathNr'} =~ s/\(|\)|\"|\ //g;
          $granule{'PathNr'} =~ s/,/;/g;
        }
        if (exists $granule{'StartBlockNr'}) {
          $granule{'StartBlockNr'} =~ s/\(|\)|\"|\ //g;
          $granule{'StartBlockNr'} =~ s/,/;/g;
        }
        if (exists $granule{'EndBlockNr'}) {
          $granule{'EndBlockNr'} =~ s/\(|\)|\"|\ //g;
          $granule{'EndBlockNr'} =~ s/,/;/g;
        }

        if (exists $attributes->{'Latitude1'}) {
            my ($boundary) = $doc->findnodes('//Boundary');
            my (@points) = $boundary->findnodes('Point');
            my $i=1;
            foreach my $point (@points) {
                my ($lat) = $point->findnodes('PointLatitude');
                $granule{"Latitude$i"} = $lat->string_value();
                my ($lon) = $point->findnodes('PointLongitude');
                $granule{"Longitude$i"} = $lon->string_value();
                $i++;
            }
        }

        foreach my $tag (keys %$attributes) {
            $granule{$attributes->{$tag}} = $granule{$tag} if ($tag ne $attributes->{$tag});
        }

        next if (exists $meta{$granule{'GranuleID'}});

        $granule{'RangeBeginningTime'} =~ s/\..*//;
        $granule{'RangeEndingTime'} =~ s/\..*//;

        $granule{'BeginningDateTime'} = $granule{'RangeBeginningDate'}
                                        . " " . $granule{'RangeBeginningTime'};
        $granule{'EndingDateTime'} = $granule{'RangeEndingDate'}
                                     . " " . $granule{'RangeEndingTime'};

        my $datafile = dirname($file) . "/" . $granule{'GranuleID'};
        if (!-e $datafile) {
            S4P::logger('ERROR', "Data file $datafile doesn't exist for $file");
            next;
        }
        $granule{'URL'} = GetRelativeUrl( $datafile );
        S4P::perish( 1, "Failed to get relative URL for $datafile" )
            unless defined $granule{'URL'};
        $granule{'XML'} = basename($file);
	$granule{'VersionID'} =~ s/\(//g;
        $granule{'VersionID'} =~ s/\)//g;
        $granule{'VersionID'} = sprintf "%03d", $granule{'VersionID'};

        $granule{URL} =~ s/\/+/\//g;
        my $fs = stat( $file );
        if ($fs->mode() & 004) {
            $granule{'RESTRICTED'} = "";
            $granule{'URL'} = $urls->{'UNRESTRICTED'}. $granule{'URL'};
        } else {
            $granule{'RESTRICTED'} = "*";
            $granule{'URL'} = $urls->{'RESTRICTED'}. $granule{'URL'};
        }

        if (!exists $granule{'FileName'}) {
            foreach (@columns) {
                $meta{$granule{'GranuleID'}} .= $granule{$_} . ",";
            }
            chop $meta{$granule{'GranuleID'}};
        } else {
            for (my $i=0; $i<scalar @{$granule{'FileName'}}; $i++) {
               $granule{'GranuleID'} = $granule{'FileName'}[$i];
               $granule{'SizeBytesDataGranule'} = $granule{'FileSize'}[$i];
               foreach (@columns) {
                  $meta{$granule{'GranuleID'}} .= $granule{$_} . ",";
               }
               chop $meta{$granule{'GranuleID'}};
            }
        }
    }
    return if (!scalar keys %meta);

    # Construct header
    my $header = join ',', @columns;

    # Write metadata into csv file
    unless (open (FILE, ">$csvfile")) {
        S4P::logger('ERROR', "Failed to create csv file $csvfile ($!)");
        return;
    }
    print FILE "$header\n" or die "fail to write to $csvfile";
    foreach (sort keys %meta) {
        print FILE $meta{$_} . "\n";
    }
    close FILE;
    return;
}

################################################################################

=head1 CheckCRC

Description:

    Verifies the CRCs in the checksum database for a given dataset and an
    optional filename. If a filename is not supplied, verifies CRCs for
    all entries in the dataset's granule database. It also verifies file
    permission and file system for a file.

Input:

    Dataset name, An optional filename, Optional flags for verbose and to
    complete the scan on error.

Output:

    Returns 0/1 on failure/success.

Algorithm:

    Function CheckCRC
        Read the checksum database belonging to the dataset.
        If a file is passed as the argument
            If the file does not exist
                Return false.
            Endif
            Compute CRC for the file and compare against CRC from the checksum
            database.
            If the computed and stored CRCs don't match
                Return false.
            Endif
        Else
            For each entry in the checksum database
                Get the file name corresponding to the entry and its CRC from
                the checksum database.

                If the file does not exist
                    Return false.
                Endif
                Compute CRC for the file and compare against CRC from the
                checksum database.
                If the computed and stored CRCs don't match
                    Return false.
                Endif
            Endfor
        Endif
        Return true
    End

=head1 AUTHOR

M. Hegde

=cut

sub CheckCRC
{
    my ( %arg ) = @_;
    my ( $dataset,
        $fileName,
        $continueOnError,
        $verbose,
        $queue,
        $storageDir,
        $interruptFlag,
        $cksumFlag,
        $fileSystem,
        $entireFsJob ) = ( $arg{DATASET}, $arg{FILENAME},
            $arg{CONTINUE_ON_ERROR}, $arg{VERBOSE}, $arg{QUEUE},
            $arg{STORAGE}, $arg{INTERRUPT_FLAG}, $arg{VERIFY_CKSUM},
            $arg{FILE_SYSTEM}, $arg{ENTIRE_FS_JOB} );
        
    my %granuleHash;
    S4P::logger( "INFO",
        "Checking $dataset" . (defined $fileName ? ", file=$fileName" : '' ) )
        if $verbose;

    $storageDir = "../.." unless defined $storageDir;

    # check for associate dataset
    my $currentDir = cwd();
    my $s4paRoot = dirname(dirname(dirname(dirname($currentDir))));
    my ( $shortname, $version ) = split /\./, $dataset, 2;
    my ( $relation, $associateType, @associateDataset ) =
        S4PA::Storage::CheckAssociation( $s4paRoot, $shortname, $version );

    # for forward association, data -> browse
    my $associated = 0;
    my ( $assocHash, $assocFileHandle, $tmpAssocDbFile );
    if ( $relation == 1 ) {
        $associated = 1;
        # open dataset configuration file to locate dataClass
        my $cpt = new Safe( 'DATACLASS' );
        $cpt->share( '%data_class' );

        # Read config file
        my $cfgFile = "$s4paRoot/storage/dataset.cfg";
        S4P::logger( 'ERROR', "Cannot read config file $cfgFile" )
            unless $cpt->rdo( $cfgFile );

        # forward association, data -> browse
        my ( $assocDataset, $assocVersion ) = split /\./, $associateDataset[0], 2;
        my $dataClass = $DATACLASS::data_class{$assocDataset};
        my $assocDbFile = "$s4paRoot/storage/$dataClass/" .
            "$associateDataset[0]" . "/associate.db";

        # Open the granule database.
        unless ( -f $assocDbFile ) {
            S4P::logger( 'ERROR',
                     "Associate database not found for dataset, $assocDataset." );
            return 0;
        }
        $tmpAssocDbFile = File::Temp::tmpnam();
        if ( copy( $assocDbFile, $tmpAssocDbFile ) ) {
            S4P::logger( 'INFO', "Copied $assocDbFile to $tmpAssocDbFile" );
        } else {
            S4P::logger( 'ERROR', "Failed to copy $assocDbFile to $tmpAssocDbFile" );
            unlink( $tmpAssocDbFile );
            return 0;
        }

        ( $assocHash, $assocFileHandle ) = OpenGranuleDB( $tmpAssocDbFile, "r" );
        unless ( defined $assocHash ) {
            S4P::logger( 'ERROR',
                "Failed to open associate database for dataset, $assocDataset." );
            unlink( $tmpAssocDbFile );
            return 0;
        }
    }

    # Open the granule database.
    my $dbFile = "$storageDir/$dataset/granule.db";
    unless ( -f $dbFile ) {
        S4P::logger( 'ERROR',
                     "Granule database not found for dataset, $dataset." );
        return 0;
    }
    my $tmpDbFile = File::Temp::tmpnam();
    if ( copy( $dbFile, $tmpDbFile ) ) {
        S4P::logger( 'INFO', "Copied $dbFile to $tmpDbFile" );
    } else {
        S4P::logger( 'ERROR', "Failed to copy $dbFile to $tmpDbFile" );
        unlink( $tmpDbFile );
        return 0;
    }

    my ( $granuleHash, $fileHandle ) = OpenGranuleDB( $tmpDbFile, "r" );
    unless ( defined $granuleHash ) {
        S4P::logger( 'ERROR',
                     "Failed to open granule database for dataset, $dataset." );
        unlink( $tmpDbFile );
        return 0;
    }

    # If a file name is specified, verify CRC for the given filename.
    # If no file names were specified, check CRC for all entries in the granule
    # database.
    my @fileList = ( defined $fileName )
                   ? ( basename $fileName ) : keys( %$granuleHash );
    my $getRecord = sub {
        my ( $dbFile, $key ) = @_;
        S4P::logger( "INFO",
            "Record, $key, was not found: retrying to access it" );
        my ( $localGranuleHash,
            $localFileHandle ) = OpenGranuleDB( $dbFile, "r" );
        unless ( defined $localGranuleHash ) {
            S4P::logger( 'ERROR',
            "Failed to open granule database for dataset, $dataset" );
            return undef;
        }
        my $record =
            $localGranuleHash->{$key} ? $localGranuleHash->{$key} : undef;
        CloseGranuleDB( $localGranuleHash, $localFileHandle );
        return $record;
    };

    my $getFilePath = sub {
        my ( $dataset, $file, $fileRecord ) = @_;
        my $filePath = "$storageDir/$dataset/data/$fileRecord->{date}/";
        $filePath .= '.hidden/'
            if ( $fileRecord->{mode} == 0640 || $fileRecord->{mode} == 0600 );
        $filePath .= $file;

        $filePath = readlink( $filePath ) if ( -l $filePath );
        return $filePath;
    };

    my %errorFsVolume;
    my $createCheckFsJobs = sub {
        my ( $fs ) = @_;
        my $fs_status = 0;
        my %dataClasses;

        # Open dataset.cfg for mapping dataset and dataclass
        my $cfg_file = "$s4paRoot/storage/dataset.cfg";
        my $cpt = new Safe( 'DATACLASS' );
        $cpt->share('%data_class');
        unless ( $cpt->rdo($cfg_file) )  {
            S4P::logger( "ERROR", "Cannot read config file $cfg_file");
            return undef;
        }
        unless ( %CFG::data_class ) {
            S4P::logger( "ERROR", "No data_class in $cfg_file");
            return undef;
        }

        # locate all the dataclass on this volume
        if ( opendir( DIR, $fs ) ) {
            my @dirs = grep( !/^\..*$/, readdir( DIR ) );
            closedir( DIR );
            foreach my $dir ( @dirs ) {
                next unless ( -d "${fs}/$dir" );
                my ($dataset, $version) = split( /\./, $dir, 2 );
                # Get dataclass from config hash
                if ( defined $dataset && exists $DATACLASS::data_class{$dataset} ) {
                    my $dataclass = $DATACLASS::data_class{$dataset};
                    $dataClasses{$dataclass} = 1 if ( $dataclass );
                }
            }
        } else {
            S4P::logger( 'ERROR', "Failed to open $fs for reading" );
            return undef;
        }

        # create a priority job under each class's check integrity station
        foreach my $class ( keys %dataClasses ) {
            my $volume = basename( $fs );
            my $checkDir = "$s4paRoot/storage/${class}/check_${class}";
            my $jobName = "PRI1.DO.CHECK_ACTIVE_FS." . $volume . ".wo";
            open( WO, ">${checkDir}/$jobName" );
            print WO "Virtual Work Order: $jobName\n";
            if ( close( WO ) ) {
                S4P::logger( 'INFO', "Created priority job $checkDir/$jobName" );
                $fs_status = 1;
            } else {
                S4P::logger( 'ERROR', "Failed to write to $checkDir/$jobName" );
            }
        }
        return $fs_status;
    };

    my $status = 1;
    my $totalFileCount = @fileList;
    my $fileCount = 0;
    
    if ( $arg{SCAN_STORAGE} ) {
        my $listDir = sub {
            my ( $dir ) = @_;
            my @dirList = ();
            if ( opendir( DH, $dir ) ) {
                @dirList = grep( !/^\.{1,2}$/, readdir( DH ) );
                closedir( DH );
            }
            foreach my $entry ( @dirList ) {
                $entry = $dir . "/$entry";
            }
            return @dirList;
        };
        my $scanDir; 
        $scanDir = sub {
            my ( $dir ) = @_;
            my $statusFlag = 1;
            my @dirList = $listDir->( $dir );
            foreach my $entry ( @dirList ) {
                if ( -d $entry ) {
                    $statusFlag = 0 unless $scanDir->( $entry );
                } elsif ( -l $entry ) {
                    my $key = basename( $entry );
                    my $record = $granuleHash->{$key};
                    $record = $getRecord->( $dbFile, $key )
                        unless defined $record;
                    # check fs and date for record, ticket #8992.
                    if ( defined $record ) {
                        my ( $fs ) = ( split( /\/+/, readlink( $entry ) ) )[-3];
                        if ( defined $record->{fs} && $record->{fs} ne $fs ) {
                            S4P::logger( 'ERROR',
                                 "Expecting file system $record->{fs};"
                                 . " instead found $fs for $entry" );
                            $statusFlag = 0;
                        }

                        my $entryDir;
                        if ( $record->{mode} == 0640 || $record->{mode} == 0600 ) {
                            $entryDir = dirname( dirname( $entry ) );
                        } else {
                            $entryDir = dirname( $entry );
                        }
                        my $entryDate;
                        my $recordDate = $record->{date};
                        my ( $year, $modoy ) = split( /\//, $recordDate, 2);
                        if ( defined $modoy ) {
                            $entryDate = basename( dirname( $entryDir ) ) . "/" .
                                basename( $entryDir );
                        } elsif ( defined $year ) {
                            $entryDate = basename( $entryDir );
                        } else {
                            # for climatology dataset
                            $entryDate = '';
                        }
                        if ( defined $record->{date} && $record->{date} ne $entryDate ) {
                            S4P::logger( 'ERROR',
                                 "Expecting date directory $record->{date};"
                                 . " instead found $entryDate for $entry" );
                            $statusFlag = 0;
                        }

                    } else {
                        if ( $associated ) {
                            # locate the associated granule's metadata file
                            # first try append .xml to the granule's fileid
                            my $assocMetaPath = readlink( $entry ) . ".xml";
                            # in case metadata filename was twisted, try matching
                            # the granuleid with all xml files in that directory
                            unless ( -f $assocMetaPath ) {
                                my $assocDir = dirname( $assocMetaPath );
                                if ( opendir( DH, "$assocDir" ) ) {
                                    my @fileList = map( $_,
                                        grep( /\.xml$/, readdir( DH ) ) );
                                    closedir( DH );
                                    XMLFILE: foreach my $file ( @fileList ) {
                                        $file =~ s/\.xml$//;
                                        if ( $key =~ /$file/ ) {
                                            $assocMetaPath = "$assocDir" . "/$file" . ".xml";
                                            last XMLFILE;
                                        }
                                    }
                                }
                            }
                            my $assocMetaFile = basename( $assocMetaPath );
                            my $assocRecord = $assocHash->{$assocMetaFile};
                            unless ( defined $assocRecord ) {
                                S4P::logger( "ERROR",
                                    "Entry for $entry is missing in associate database" );
                                $statusFlag = 0;
                            }
                        } else {
                            S4P::logger( "ERROR",
                                "Entry for $entry is missing in granule database" );
                            $statusFlag = 0;
                        }
                    }
                }
            }
            return $statusFlag;
        };
        
        $status = $scanDir->( $storageDir . "/$dataset/data/" );
    }
    KEY_LOOP: foreach my $key ( @fileList ) {
        # Skip if the process has been interrupted (for use with error handler)
        next if ( defined $interruptFlag && ($$interruptFlag != 1)  );
        $queue->enqueue( 'ERROR' ) if ( defined $queue && (not $status) );
        $fileCount++;
        S4P::logger( "INFO", "Checking $key" ) if $verbose;
        if ( defined $queue ) {
            $queue->enqueue(
                sprintf( "UPDATE:%d,%.1f", $fileCount,
                $fileCount*100./$totalFileCount ) );
        }
        my $record = $granuleHash->{$key};

        $record = $getRecord->( $dbFile, $key )
            unless defined $record;

        # Abort if the granule's record doesn't exist.
        next unless ( defined $record );

        # If file system is specified, limit the search to the specified file
        # system.
        next if ( defined $fileSystem && ($record->{fs} ne $fileSystem) );
        
        # Abort if the file doesn't exist.
        my $filePath = $getFilePath->( $dataset, $key, $record );
        unless ( -f $filePath ) {
            my $record = $getRecord->( $dbFile, $key );
            unless ( defined $record ) {
                S4P::logger( "INFO",
                "$key existed at the start of scan. Doesn't exist anymore;"
                . " skipping" );
                next KEY_LOOP;
            }
            $filePath = $getFilePath->( $dataset, $key, $record );
        }
        unless ( -f $filePath ) {
            S4P::logger( 'ERROR',
                         "File, $filePath, doesn't exist" );
            $status = 0;
            last KEY_LOOP unless $continueOnError;
            next KEY_LOOP;
        }

        # Abort if file can't be 'stat'ed.
        my $st = stat( $filePath );
        unless ( defined $st ) {
            S4P::logger( 'ERROR', "Failed to stat $filePath ($!)" );
            $status = 0;
            last KEY_LOOP unless $continueOnError;
            next KEY_LOOP;
        }

        # Abort if the file system number doesn't match the value in DBM file.
        my ( $fs ) = ( split( /\/+/, $filePath ) )[-3];
        if ( defined $record->{fs} && $record->{fs} ne $fs ) {
            S4P::logger( 'ERROR',
                         "Expecting file system $record->{fs}; instead found"
                         . " $fs for $key" );
            $status = 0;
            last KEY_LOOP unless $continueOnError;
            next KEY_LOOP;
        }

        # Skip checksum verification for light weight integrity checking 
        $fs = dirname(dirname($filePath));
        if ( $cksumFlag ) {
            # Abort if CRC doesn't match what is in DBM.
            my $crc = S4PA::Storage::ComputeCRC( $filePath );
            if ( $crc != $record->{cksum} ) {
                S4P::logger( 'ERROR',
                         "Checksum mismatch for $filePath: found $crc, expected"
                         . " $record->{cksum}" );
                $status = 0;
                # only create check entire fs volume job if it is not the current
                # job or this trouble volume has been detected on a regular job
                unless ( $entireFsJob ) {
                    unless ( $errorFsVolume{$fs} ) {
                       my $fs_status = $createCheckFsJobs->( $fs );
                       if ( $fs_status ) {
                           $errorFsVolume{$fs} = 1;
                           S4P::raise_anomaly('CKSUM_MISMATCH', dirname($currentDir), 
                               'WARN', "Created check entire active_fs jobs for $fs", 0);
                       }
                    }
                }
                last KEY_LOOP unless $continueOnError;
                next KEY_LOOP;
            }
        }

        my $mode = $st->mode & 07777;
        if ( defined $record->{mode} && $mode != $record->{mode} ) {
            S4P::logger( 'ERROR',
                         "Expecting file mode $record->{mode}; instead found"
                         . " $mode for $key" );
            $status = 0;
            last KEY_LOOP unless $continueOnError;
            next KEY_LOOP;
        }

        # log the check success for this record
        S4P::logger( "INFO", "$key cksum: $record->{cksum}" );
    }
    S4P::logger( "INFO",
        "Scanned $fileCount of $totalFileCount files in $dataset" )
        if $continueOnError;
    CloseGranuleDB( $granuleHash, $fileHandle );
    unlink( $tmpDbFile );

    if ( $associated ) {
        CloseGranuleDB( $assocHash, $assocFileHandle );
        unlink( $tmpAssocDbFile );
    }
    return $status;
}

################################################################################

=head1 Anonymous Method

Description:

    Extracts the Range Beginning Date and Beginning Time from a parsed
    metadata document element and returns a reference to a hash containing
    the data and time components

Input:

    Parsed metadata document element

Output:

    Reference to a hash containing the following keys:
      year - 4-digit year
      month - 2-digit month number [01-12]
      day - 2-digit day-of-month [01-31]
      hour - 2-digit hour [00-23]
      min - 2-digit minute of hour [00-59]
      sec - 2-digit second of minute [00-59]
      epoch - number of non-leap seconds since January 1, 1970 UTC
      doy - 3-digit day of year [001-366]

Algorithm:

    Function
        Get date and time nodes
        Stringify date and time nodes
        Separate date string into year, month, day
        Separate time string into hour, minute, second
        Compute day-of-year from year, month, and day
        Compute epoch from year, month, day, hour, minute, second
        Convert strings to desired fixed length
    End

=head1 AUTHOR

M. Hegde

=cut

my $_GetBeginningDateTime = sub {
    my ( $doc ) = @_;

    # A hash reference to hold date-time info.
    my $timeHash = {};

    # Get the date and time nodes
    my ( $dateNode ) = $doc->findnodes( './RangeDateTime/RangeBeginningDate' );
    unless ( defined $dateNode ) {
        S4P::logger( 'ERROR', "Failed to find <RangeBeginningDate>" );
        return $timeHash;
    }
    my ( $timeNode ) = $doc->findnodes( './RangeDateTime/RangeBeginningTime' );
    unless ( defined $timeNode ) {
        S4P::logger( 'ERROR', "Failed to find <RangeBeginningTime>" );
        return $timeHash;
    }

    # Stringify the date and time nodes
    my $date = $dateNode->string_value();
    my $time = $timeNode->string_value();

    # Remove leading/trailing white spaces
    $date =~ s/^\s+|\s+$//g;
    $time =~ s/^\s+|\s+$//g;

    @{$timeHash}{'year', 'month', 'day'} = split( /-/, $date, 3 );
    @{$timeHash}{'hour', 'min', 'sec'} = split ( /:/, $time, 3 );
    $timeHash->{doy} = S4P::TimeTools::day_of_year( $timeHash->{year},
                                                    $timeHash->{month},
                                                    $timeHash->{day} );
    $timeHash->{epoch} = timegm( $timeHash->{sec}, $timeHash->{min},
                                 $timeHash->{hour}, $timeHash->{day},
                                 $timeHash->{month}-1, $timeHash->{year} );

    # Make the month and day two characters long and
    # the day of the year 3 chars long
    $timeHash->{month} = sprintf( "%2.2d", $timeHash->{month} );
    $timeHash->{day} = sprintf( "%2.2d", $timeHash->{day} );
    $timeHash->{doy} = sprintf( "%3.3d", $timeHash->{doy} );

    return $timeHash;
};

################################################################################

=head1 Anonymous Method

Description:

    Create a list of data directories for a dataset within a specified
    margin of a particular reference date/time

Input:

    Temporal frequency of lowest level of directories (yearly, monthly, daily)
    Reference date/time
    Time margin, in seconds, relative to reference date/time
    Dataset
    Version of dataset (optional)

Output:

    List of full pathnames of data directories within the time margin

Algorithm:

    Function
        Determine home directory for the dataset from the link pointing to it
        Find date/time when margin is subtracted from reference
        Find date/time when margin is added to reference
        Determine pathname for reference date/time, based upon frequency,
            and add to list
        Determine pathname for earlier date/time, based upon frequency,
            and add it to list if it is different than the reference
        Determine pathname for later date/time, based upon frequency
            and add it to list if it is different than the reference
    End

=head1 AUTHOR

E. Seiler

=cut

my $_get_data_dirs = sub {
    my ( $frequency, $refTime, $time_margin, $dataset, $version ) = @_;

    # Determine home directory for the dataset from the link pointing to it
    my $rel_path = ( $version ne '' )
                   ? "../../$dataset.$version/data"
                   : "../../$dataset/data";
    my $data_home = readlink( $rel_path );
    if ( ! defined $data_home ) {
        S4P::perish( 1, "Could not determine home directory from " .
                        "link $rel_path: $!" );
    }
    $data_home .= "/" unless ( $data_home =~ /\/$/ );

    # Find the date/time when the time margin (in seconds) is subtracted from
    # the time of the reference time.
    # The last 4 arguments to add_delta_dhms are
    # delta-days, delta-hours, delta-minutes, delta-seconds.
    # Ignore everything but the first three values returned by add_delta_dhms.
    my ( $earlier_year, $earlier_month, $earlier_day ) =
        S4P::TimeTools::add_delta_dhms($refTime->{year},
                                       $refTime->{month},
                                       $refTime->{day},
                                       $refTime->{hour},
                                       $refTime->{min},
                                       $refTime->{sec},
                                       0, 0, 0, -$time_margin);
    # Find the date/time when the time margin (in seconds) is added to
    # the time of the reference time.
    # The last 4 arguments to add_delta_dhms are
    # delta-days, delta-hours, delta-minutes, delta-seconds.
    # Ignore everything but the first three values returned by add_delta_dhms.
    my ($later_year, $later_month, $later_day) =
        S4P::TimeTools::add_delta_dhms( $refTime->{year},
                                        $refTime->{month},
                                        $refTime->{day},
                                        $refTime->{hour},
                                        $refTime->{min},
                                        $refTime->{sec},
                                        0, 0, 0, $time_margin );

    # Set $data_dir to the pathname of the directory for data with
    # a date equal to the reference date/time.
    # If a different pathname results when the time margin is subtracted from
    # or added to the reference date/time, set the value of $earlier_data_dir
    # or $later_data_dir
    my ($data_dir, $earlier_data_dir, $later_data_dir);
    if ( $frequency eq 'yearly' ) {
        $data_dir = $data_home . $refTime->{year} . '/';
        $earlier_data_dir = $data_home . $earlier_year . '/'
            if ( $earlier_year ne $refTime->{year} );
        $later_data_dir = $data_home . $later_year . '/'
            if ( $later_year ne $refTime->{year} );
    } elsif ( $frequency eq 'monthly' ) {
        $data_dir = $data_home . $refTime->{year} . '/'
                    . $refTime->{month} . '/';
        $earlier_month = sprintf( "%2.2d", $earlier_month );
        $earlier_data_dir = $data_home . $earlier_year . '/'
                            . $earlier_month . '/'
            if ( $earlier_month ne $refTime->{month} );
        $later_month = sprintf( "%2.2d", $later_month );
        $later_data_dir = $data_home . $later_year . '/'
                          . $later_month . '/'
            if ( $later_month ne $refTime->{month} );
    } elsif ( $frequency eq 'daily' ) {
        $data_dir = $data_home . $refTime->{year} . '/'
                    . $refTime->{doy} . '/';
        my $earlier_doy = S4P::TimeTools::day_of_year($earlier_year,
                                                      $earlier_month,
                                                      $earlier_day);
        $earlier_doy = sprintf( "%3.3d", $earlier_doy );
        $earlier_data_dir = $data_home . $earlier_year . '/'
                            . $earlier_doy . '/'
            if ( $earlier_doy ne $refTime->{doy} );
        my $later_doy = S4P::TimeTools::day_of_year($later_year,
                                                    $later_month,
                                                    $later_day);
        $later_doy = sprintf( "%3.3d", $later_doy );
        $later_data_dir = $data_home . $later_year . '/'
                          . $later_doy . '/'
            if ( $later_doy ne $refTime->{doy} );
    } elsif ( $frequency eq 'none' ) {
        # for climatology dataset
        $data_dir = $data_home . '/';
    } else {
        S4P::perish( 1, "Temporal frequency, $frequency, not supported" );
    }
    my @data_dirs;
    push @data_dirs, $data_dir if (-d $data_dir);
    # bypass adding earlier and later data directory for climatology dataset
    unless ( $frequency eq 'none' ) {
        push @data_dirs, $earlier_data_dir
            if ($earlier_data_dir && -d $earlier_data_dir);
        push @data_dirs, $later_data_dir
            if ($later_data_dir && -d $later_data_dir);
    }

    return @data_dirs;
};

################################################################################

=head1 IsReplaceData

Description:

    Checks to see if a granule is going to replace an existing
    granule using beginning date-time of data. If the granule being
    replaced has the same file name as the new granule, it is
    considered a "modification" case and not a replacement.
    In the case of modification, no match should be returned.

Input:

    Dataset name,
    Dataset version,
    Metadata with full path,
    Acceptable margin of time difference in seconds for detecting replacement,
    Reference to a list of XPath nodes that must match in order
    for replacement to occur (optional),
    Reference to a list of XPath nodes that must match in order
    for them to be ignored.

Output:

    Array containing full pathnames of symbolic links pointing to matching
    data and metadata files.
    Stops if any error occurs during matching process.

Algorithm:

    Function IsReplaceData
        Get beginning date-time from the supplied metadata file and
            compute day of the year.
        If the dataset's corresponding day of year directory doesn't exist
            Return empty list
        Endif
        Scan all metadata files in the day_of_year directory.
        Foreach metadata file found
            Get beginning date time of metadata file found.
            If the above beginning date time is within the time margin of
                beginning date time of supplied data file
                Increment counter for matching metadata files.
                Get all data file names from metadata file.
                Accumulate data file names and metadata file name.
            Endif
        Endfor

        If number of matching metadata files is greater than 1
            Log a message and stop.
        Endif
        Return accumulated file names.
    End

=head1 AUTHOR

Yangling Huang

=cut

################################################################################

sub IsReplaceData {
    my ( $dataset, $version, $new_met_file, $time_margin, 
         $xpath_list, $ignoreXpath_list ) = @_;

    # Set the old data flag to false. It is used to indicate whether the
    # production date-time of the incoming granule is older than the existing
    # granule.
    $oldDataFlag = 0;

    # Set the ignore data flag to be false. It is used to indicate whether
    # the incoming granule is to be ignored if the ignoreCondition is 
    # satisfied.
    $ignoreDataFlag = 0;

    # Set the multiple delete flag to be false. It is used to indicate
    # whether the incoming granule is going to replace multiple existing
    # granules.
    $multipleDeleteFlag = 0;

    # Create a DOM parser
    my $xml_parser = XML::LibXML->new();

    # Parse the incoming metadata file.
    # On failure to create a DOM from the metadata file, perish.
    my $newDom = $xml_parser->parse_file( $new_met_file  );
    S4P::perish( 1, "Failed to parse $new_met_file" ) unless ( defined $newDom );

    # Get the document element from the DOM and extract values from the
    # beginning date/time nodes.
    my $newDoc = $newDom->documentElement();
    my $newTime = $_GetBeginningDateTime->( $newDoc );

    # Get the production date-time for the incoming granule if it exists
    my ( $newProdTimeNode ) = $newDoc->findnodes(
                                          './DataGranule/ProductionDateTime' );
    my $newProdDateTime = ( defined $newProdTimeNode )
                          ? $newProdTimeNode->string_value() : undef;

    my @replace_data = ();
    my $count = 0;

    # Determine full path of the directories that could contain a metadata
    # file within the time margin of the incoming metadata file.
    my $frequency;
    if ( %CFG::cfg_temporal_frequency
         && defined $CFG::cfg_temporal_frequency{$dataset} ) {
        $frequency = $CFG::cfg_temporal_frequency{$dataset}->{$version};
    } else {
        $frequency = 'daily';
    }
    my @data_dirs = $_get_data_dirs->( $frequency, $newTime, $time_margin,
                                       $dataset, $version );

    # If no data directory exists, there is nothing to replace.
    return () unless @data_dirs;

    # Get all the metadata files (.xml) in all the data directories
    # that could contain a metadata file within the time margin of the
    # incoming metadata file.
    local ( *DH );
    my @old_met_file_list;
    foreach my $data_dir (@data_dirs) {
        S4P::perish( 1, "Failed to open directory, $data_dir" )
            unless ( opendir( DH, "$data_dir" ) );
        push @old_met_file_list, map "$data_dir/$_",
                                     grep( /\.xml$/, readdir( DH ) );
        closedir( DH );
	# If a hidden directory exists, include it to the list.
	push( @data_dirs, "$data_dir/.hidden" ) if ( -d "$data_dir/.hidden" );
    }

# Commented out below the computation of seconds-of-year, since we no longer
# assume that a granule being replaced must be in the same year as the
# incoming granule.
#   my $newSec = $newTime->{sec}
#                + 60 * ( $newTime->{min}
#                         + 60 * ( $newTime->{hour} + 24 * $newTime->{doy} ) );

    # Extract the filename from the full path name of the incoming metadata
    # file.
    my $new_file_name = basename( $new_met_file );

    # Loop through all the existing metadata files in the list found above.
    foreach my $old_met_file ( @old_met_file_list ) {

        # Parse the existing metadata file.
        # On failure to create a DOM from the existing metadata file,
        # log an error and return an empty list.
        my $oldDom = $xml_parser->parse_file( $old_met_file );
        unless( defined $oldDom ) {
            S4P::logger( 'ERROR', "Failed to parse $old_met_file");
            return ();
        }

        # If the same file already exists, return an empty list. It is an
        # in-place replacement. No need to delete any files.
        if ( $new_file_name eq basename( $old_met_file ) ) {
            S4P::logger( "INFO",
                         "A file of the same name as the incoming metadata"
                         . " file, $new_met_file, already exists" );
            return ();
        }

        # Get the document element from the DOM and extract values from the
        # beginning date/time nodes.
        my $oldDoc = $oldDom->documentElement();
        my $oldTime = $_GetBeginningDateTime->( $oldDoc );

        # skip collection metadata files under climatology dataset directory
        if ( !(exists $oldTime->{'day'}) && ($frequency eq 'none') ) {
            S4P::logger( "INFO", "Skip collection metadata file: $old_met_file" );
            next;
        }

# Commented out below the computation of seconds-of-year, since we no longer
# assume that a granule being replaced must be in the same year as the
# incoming granule.
#       my $oldSec =  $oldTime->{sec}
#                     + 60 * ( $oldTime->{min}
#                     + 60 * ( $oldTime->{hour} + 24 * $oldTime->{doy} ) );
#        if ( abs( $oldSec - $newSec ) <= $time_margin ) {

        # If beginning time of the existing granule is within the specified
        # time margin of the beginning time of the new granule, the new
        # granule is a replacement granule if all XPath expressions match.
        my $replaceFlag = 0;
        if ( abs( $oldTime->{epoch} - $newTime->{epoch} ) <= $time_margin ) {
            if ( defined $xpath_list ) {
                $replaceFlag = CompareXpathValue( $xpath_list, $new_met_file,
                    $newDoc, $old_met_file, $oldDoc );
            } else {
                $replaceFlag = 1;
            }
        }

        if ( $replaceFlag ) {
            # Check whether the incoming granule qualified to be ignored.
            # If no ignoreCondition was specified then use production 
            # data time as the criteria.
            if ( defined $ignoreXpath_list ) {
                $ignoreDataFlag = CompareXpathValue( $ignoreXpath_list, 
                    $new_met_file, $newDoc, $old_met_file, $oldDoc );

            # If the incoming granule has a production date time, compare it
            # with that of the matching existing granule.
            } elsif ( defined $newProdDateTime ) {
                # Get the production date-time if it exists
                my ( $oldProdTimeNode ) = $oldDoc->findnodes(
                                          './DataGranule/ProductionDateTime' );
                my $oldProdDateTime = ( defined $oldProdTimeNode )
                                      ? $oldProdTimeNode->string_value() : undef;
                # Set the flag if the incoming granule's production date-time
                # is older than an existing granule
                if ( defined $oldProdDateTime
                     && ($newProdDateTime lt $oldProdDateTime ) ) {
                    $oldDataFlag = 1;
                    S4P::logger( "WARN",
                                 "Existing file, $old_met_file, newer " .
                                 "than $new_met_file" );
                }
            }
            $count++;

            # Start the list of files to be replaced with the
            # existing metadata file
            push( @replace_data, $old_met_file );
            S4P::logger( 'INFO',
                         "Marking $old_met_file for replacement by $new_met_file" );

            # Get list of data files from the DataGranule node of the
            # metadata file, either from the FileName tag or the
            # GranuleID tag.
            my ( $main_file ) = $oldDoc->findnodes( 'DataGranule' );
            my ( @file_list ) = $main_file->getElementsByTagName( 'FileName' );
            @file_list = $main_file->getElementsByTagName('GranuleID')
                unless ( @file_list );
            S4P::perish( 1, "Unexpected condition found: $old_met_file"
                            . " doesn't contain data files" ) unless @file_list;

            # Add browse file to the replacing file_list if exist
            my ( $browseFile ) = $main_file->getElementsByTagName( 'BrowseFile' );
            push( @file_list, $browseFile ) if ( defined $browseFile );

            # Add full pathname of each data file to the list of files
            # to be replaced.
            my $data_dir = dirname( $old_met_file );
            foreach my $assoc_file ( @file_list ) {
                my $data_file = $assoc_file->string_value();
                push( @replace_data, "$data_dir/$data_file" );
                S4P::logger( 'INFO',
                             "Marking $data_file in $data_dir for replacement");
            }
        }
    }

    # depend on how often delete station bounce and delete retention time,
    # there could be more than one granule to be deleted at the same time.
    # Therefore, comment out the following fail condition and replace it 
    # with a flag for calling script to handle.

    # Expect only a single match for a metadata file
    # S4P::perish( 1, "There are more than one granules marked as replacement")
    #     if ( $count > 1 );
    $multipleDeleteFlag = 1 if ( $count > 1 );

    return ( @replace_data );
}

################################################################################

=head1 DeleteData

Input:

    Full pathname of the data file or symbolic link.

Output:

    Returns 1/0 depending on success/failure.

Algorithm:

    Function DeleteData
        If the specified path is a symbolic link
            Read the source of symbolic link.
            Remove the sybmolic link.
            Remove the source of symbolic link.
            Return 0 if any of the above steps fails.
        Else
            Remove the file with specified path.
            Return 0 if above step fails.
        End
        Return 1.
    End

=head1 AUTHOR

Yangling Huang

=cut

sub DeleteData {
    my ( $data_file )= @_;

    # If the specified path is a symbolic link
    if ( -l $data_file  ) {
        # Find the source of symbolic link.
        my $data = readlink ( $data_file );

        # Remove the symbolic link.
        unless ( unlink ( $data_file ) ) {
            S4P::logger( 'ERROR', "Could not remove $data_file: $!");
            return 0;
        }

        # re-assign $data_file with the symbolic link source
        $data_file = $data;
    }

    if ( -f $data_file ) {
        # get file size before deletion
        my $st = stat( $data_file );
        my $data_size = $st->size();

        # If the specified path is a regular file, remove the file.
        unless ( unlink ( $data_file ) ) {
            S4P::logger( 'ERROR', "Could not remove $data_file: $!");
            return 0;
        }

        # increase .FS_SIZE free space, ticket #6467.
        my $fsSizeFile = dirname( dirname $data_file ) . '/.FS_SIZE';
        if ( -f $fsSizeFile ) {
            my @sizeList = S4PA::Receiving::DiskPartitionTracker( 
                $fsSizeFile, "update", $data_size );
            unless ( @sizeList ) {
                S4P::logger( 'ERROR', 
                    "Could not upate free space in $fsSizeFile" );
                return 0;
            }
        }
    } else {
        S4P::logger( "WARNING", "File, $data_file, doesn't exist" );
    }
    # Return 1 on successfully removing the symbolic link/file.
    return 1;
}

################################################################################

=head1 OpenGranuleDB

    Opens an MLDBM file, ties a hash to the DBM file and returns the hash. It
    locks the DBM file if a read-write or a write access is requested. It takes
    an MLDBM file and an option access type ('r', 'rw', 'w' for read only,
    read+write and write only). It returns a hash reference tied to MLDBM file
    and an optional file handle when a read+write or write access is requested.

Input:

    Filename and an optional file permission ('r', 'rw', 'w').

Output:

    A hash tied to the DBM file and a file handle.

Algorithm:

    Function OpenGranuleDB
        If the file exists
            If a write or read-write access is requested
                Open the file and get a file handle.
                Place an exclusive lock on the file.
            Endif
            Tie a hash to the DBM file.
        Else
            Tie a hash to the DBM file.
            If a write or read-write access is requested
                Open the file and get a file handle.
                Place an exclusive lock on the file.
            Endif
        Endif
        Return hash reference and file handle.
    End

=head1 AUTHOR

M. Hegde

=cut

sub OpenGranuleDB
{
    my ( $file, $permission ) = @_;
    local( *FH );
    my $hashRef = {};  # A hash ref to be used in "tie"ing the hash to DBM file.

    # Get the constant corresponding to the specified file permission; default
    # is read only.
    my $flag = ( $permission eq 'r' ) ? O_RDONLY
               : ( ( $permission eq 'rw' ) ? O_RDWR
                     : ( ( $permission eq 'w' ) ? O_WRONLY : O_RDONLY ) );

    # Check to see if the DBM file already exists.
    if ( -f $file ) {
        # Case of an existing DBM file.
        # Return (undef, undef) on failure to open the file.
        return ( undef, undef ) unless ( open( FH, $file ) );

        # If the DBM file is opened for reading, try to lock the file and
        # return (undef, undef) if locking fails.
        #if ( $permission =~ /w/ ) {
            unless ( flock( FH, 2 ) ) {
                close( FH );
                return ( undef, undef );
            }
        #}

        # Tie the hash to DBM file. Return (undef, undef) if tie operation
        # fails.
        unless ( tie( %$hashRef, "MLDBM", $file, $flag )  ) {
            close( FH );
            return ( undef, undef );
        }
    } else {
        # Case of a new DBM file: tie the hash to DBM file first.
        return( $hashRef, undef ) unless ( $permission =~ /w/ );
        return( undef, undef )
            unless ( tie( %$hashRef, "MLDBM", $file, $flag | O_CREAT ) );

        # Depending on the file permission, lock it later.
        if ( open( FH, $file ) ) {
            unless ( flock( FH, 2 ) ) {
                close( FH );
                return( undef, undef );
            }
        } else {
            untie %$hashRef;
            undef $hashRef;
        }
    }
    return( $hashRef, *FH );
}

################################################################################

=head1 CloseGranuleDB

Description:

    Unties the specified hash and unlocks the file if a file handle is
    specified. This method is used in conjunction with
    S4PA::Storage::OpenGranuleDB().

Input:

    A hash reference that was tied to the DBM file and an optional file handle
    in the case where a write/read+write access was used with the DBM file.

Output:

    None.

Algorithm:

    Function CloseGranuleDB
        Untie the hash.
        Unlock the file handle if a file handle exists.
    End

=head1 AUTHOR

M. Hegde

=cut

sub CloseGranuleDB
{
    my ( $hashRef, $fh ) = @_;

    # Untie the hash first and unlock the file if a file handle is passed.
    untie %$hashRef;
    close( $fh ) if defined $fh;
}

################################################################################

=head1 IsOldData

Description:

    An accessor method to a status flag. It returns true (1) if the current
    incoming data is older than the existing data.

Input:

    None

Output:

    True/False (1/0) if the current granule is old/new.

Algorithm:

    Function IsOldData
        Return old data flag.
    End

=head1 AUTHOR

M. Hegde

=cut

sub IsOldData
{
   return $oldDataFlag;
}

################################################################################

=head1 IsIgnoreData

Description:

    An accessor method to a status flag. It returns true (1) if the current
    incoming data is to be ignored.

Input:

    None

Output:

    True (1) if the current granule is to be ignored.

Algorithm:

    Function IsIgnoreData
        Return ignore data flag.
    End

=head1 AUTHOR

Guang-Dih Lei

=cut

sub IsIgnoreData
{
   return $ignoreDataFlag;
}

################################################################################

=head1 IsMultipleDelete

Description:

    An accessor method to a status flag. It returns true (1) if the current
    incoming data is to going to replace multiple existing granules.

Input:

    None

Output:

    True (1) if the current granule is to going to replace multiple existing ones.

Algorithm:

    Function IsMultipleDatele
        Return multiple delete flag.
    End

=head1 AUTHOR

Guang-Dih Lei

=cut

sub IsMultipleDelete
{
   return $multipleDeleteFlag;
}

################################################################################

=head1 GetRelativeUrl

Description:

    A method to find the relative URL of an S4PA data/metadata file based on its
    full path.

Input:

    Full path of the file.

Output:

    Relative URL or undef.

=head1 AUTHOR

M. Hegde

=cut

sub GetRelativeUrl
{
    my ( $datafile ) = @_;
    my $url = ( $datafile =~ /(\/\w+\/+[^\s\/]+\/+\d{4}\/+\d{3}\/+\.hidden\/)/
            || $datafile =~ /(\/\w+\/+[^\s\/]+\/+\d{4}\/+\d{3}\/)/
            || $datafile =~ /(\/\w+\/+[^\s\/]+\/+\d{4}\/+\d{2}\/+\.hidden)\//
            || $datafile =~ /(\/\w+\/+[^\s\/]+\/+\d{4}\/+\d{2}\/)/
            || $datafile =~ /(\/\w+\/+[^\s\/]+\/+\d{4}\/+\.hidden\/)/
            || $datafile =~ /(\/\w+\/+[^\s\/]+\/+\d{4}\/)/ ) ? $1 : undef;
    # for climatology dataset
    unless ( defined $url ) {
        my $path = dirname( $datafile );
        my $dataset = basename( $path );
        # for restricted dataset
        if ( $dataset eq '.hidden' ) {
            $path = dirname( $path );
            $dataset = basename( $path ) . "/.hidden";
        }
        my $datagroup = basename( dirname( $path ));
        $url = "/$datagroup/$dataset/"; 
    }
    return $url;
}
################################################################################
=head1 CheckIntegrityFailureHandler

Description:

    It is used as the failure handler for CheckIntegrity station. For a "RETRY"
    job, the work order is renamed with "RETRY" appended to job type.

Input:
    None

Output:
    Returns 1 or 0 (success or failure).

Algorithm:


=head1 AUTHOR

M. Hegde

=cut

sub CheckIntegrityFailureHandler
{
    my $dir = cwd();
    return 0 if S4P::still_running( $dir );
    my ( $status, $pid, $owner, $wo, $comment ) = S4P::check_job( $dir );
    unless ( $status ) {
        S4P::logger( "ERROR", "Could not get status" );
        return 0;
    }
    $wo =~ s/\.wo//;
    if ( $wo =~ /CHECK_([^\.]+)\.(.+)/ ) {
        my $newWorkOrder = "../PRI1.DO.CHECK_$1.$2.wo";
        S4P::write_file( $newWorkOrder, '' );
        S4P::remove_job();
    } else {
        S4P::logger( "ERROR",
            "Work order's name, $wo, deosn't mach the format: DO.TYPE.ID.wo" );
    }
}


################################################################################
=head1 GetDataRootDirectory

Description:

    Given an s4pa instance root and the shortname and version of a granule return the
    path to the data directory root and the dataclass to which the shortname belongs

Input:
    S4PA root, dataset name , version

Output:
    Path to root of data directory and dataclass

Algorithm:


=head1 AUTHOR

A. Eudell

=cut

sub GetDataRootDirectory {
    my ( $root, $dataset, $version ) = @_;

    # Open dataset.cfg for read;
    my $cfg_file = "$root/storage/dataset.cfg";

    # Setup compartment and read config file
    my $cpt = new Safe('CFG');
    $cpt->share('%data_class');

    # Read config file
    unless ($cpt->rdo($cfg_file))  {
        S4P::logger( "ERROR", "Cannot read config file $cfg_file");
        return undef;
    }

    # Check for required variables
    if (!%CFG::data_class) {
        S4P::logger( "ERROR", "No data_class in $cfg_file");
        return undef;
    }

    # Get dataclass from config hash
    my $dataclass = $CFG::data_class{$dataset};

    # We can now find store_cfg file
    my $store_cfg_file = "$root/storage/$dataclass/store_$dataclass/s4pa_store_data.cfg";

    # Read store_config files
    my $cptv = new Safe('CFGv');
    $cptv->share('%cfg_data_version');
    unless ($cptv->rdo($store_cfg_file)) {
        S4P::logger( "ERROR", "Cannot read config file $store_cfg_file");
        return undef;
    }

    # Check for required variables
    if ( !%CFGv::cfg_data_version ) {
        S4P::logger( "ERROR", "No version data in $store_cfg_file");
        return undef;
    }

    # Get version from config hash
    my $version_array = $CFGv::cfg_data_version{$dataset};

    # Just to make things more readable
    my @version_array = @{$version_array};

    my $dataDir;
    foreach my $cfg_version (@version_array) {
        if ($cfg_version eq '') {
            $dataDir = "$root/storage/$dataclass/$dataset";
            last;
        }
        elsif ( "$version" eq "$cfg_version" ) {
            $dataDir = "$root/storage/$dataclass/$dataset.$cfg_version";
            last;
        }
    }

    # Might be ECHO version with non digits, strip off non-digits to
    # since ECHO was replaced by CMR, we probably don't need to do this any more,
    # but keep it here for backward compatibility. However, we do have to
    # single this out of the above loop since it is causing '7r' to be '7' problem 
    # in oco2 instance.
    unless (defined $dataDir) {
        (my $stripped_version = $version) =~ s/\D//g;
        foreach my $cfg_version (@version_array) {
            if  ( "$stripped_version" eq "$cfg_version" ) {
                $dataDir = "$root/storage/$dataclass/$dataset.$cfg_version";
                last;
            }
        }
    }
    return ($dataDir , $dataclass);
}


################################################################################
=head1 CompareXpathValue

Description:

    Check whether the result of each expression in the incoming granule 
    matches with that of the existing granule.

Input:
    Xpath hash for comparison, incoming granules's xmlDoc, existing granule's xmlDoc 

Output:
    True if comparison failed, false if comparison passwd.

Algorithm:


=head1 AUTHOR

Fan Fang

=cut

sub CompareXpathValue {
    my ( $xpath_list, $newFile, $newDoc, $oldFile, $oldDoc ) = @_;

    my $returnFlag = 1;
    foreach my $operator ( keys %$xpath_list ) {
        foreach my $xpath ( @{$xpath_list->{$operator}} ) {
            my $newVal = $newDoc->findvalue( $xpath );
            S4P::perish( 1, "Failed to find $xpath in "
                . $newFile ) unless defined $newVal;
            my $oldVal = $oldDoc->findvalue( $xpath );
            S4P::perish( 1, "Failed to find $xpath in "
                . $oldFile ) unless defined $oldVal;
            # Make sure the input and the output are of the same type
            my $oldType = S4PA::IsNumber( $oldVal );
            my $newType = S4PA::IsNumber( $newVal );
            S4P::logger( "WARN",
                "Values of $xpath in $newFile and "
                 . "$oldFile are not of the same type!" )
                unless ( $oldType == $newType );

            # Reset replace flag if any of the specified attributes
            # don't match.

            my $cmpFlag = ( $newType != $oldType )
                ? $newVal cmp $oldVal
                : ( $newType
                    ? $newVal <=> $oldVal
                    : $newVal cmp $oldVal );

            if ( $operator eq 'EQ' ) {
                $returnFlag = 0 unless ( $cmpFlag == 0 );
            } elsif ( $operator eq 'NE' ) {
                $returnFlag = 0 unless ( $cmpFlag != 0 );
            } elsif ( $operator eq 'LT' ) {
                $returnFlag = 0 unless ( $cmpFlag == -1 );
            } elsif ( $operator eq 'LE' ) {
                $returnFlag = 0 unless ( $cmpFlag <= 0 );
            } elsif ( $operator eq 'GT' ) {
                $returnFlag = 0 unless ( $cmpFlag == 1 );
            } elsif ( $operator eq 'GE' ) {
                $returnFlag = 0 unless ( $cmpFlag >= 0 );
            } else {
                S4P::perish( 1,
                    "Operator specified for $xpath, $operator, not"
                    . " supported" );
            }
        }
    }
    return $returnFlag;
}

################################################################################

=head1 Anonymous Method

Description:

    Stores a granule's association record in DBM file. It is used only in
    S4PA::Storage::StoreAssociate().

Input:

    associate DB file, associate granule, data granule.

Output:

    Returns 0/1 on failure/success.

Algorithm:

    Function
        Open the associate DBM file.
        Add data granule to array if associate granule existed in hash ref.
        Otherwise, insert the new associate granule into hash ref.
        Close the associate DBM file.
        Return 1.
    End

=head1 AUTHOR

Guang-Dih Lei

=cut

my $_storeAssocRecord = sub {
    my ( $dbFile, $associateMetaFile, $dataMetaFile ) = @_;

    my ( $associateHashRef, $fileHandle ) = OpenGranuleDB( $dbFile, "rw" );
    # If unable to open associate database, complain and return.
    unless ( defined $associateHashRef ) {
        S4P::logger( 'ERROR',
                     "Failed to open associate database: $dbFile" );
        return 0;
    }

    my $key = basename( $associateMetaFile );
    my $value = basename( $dataMetaFile ) if defined $dataMetaFile;
    if ( exists $associateHashRef->{$key} ) {
        # no data granule defined and associate granule already existed,
        # this is probably re-ingest of the associate granule again.
        # Nothing need to be done. Close DB and return.
        unless ( defined $value ) {
            CloseGranuleDB( $associateHashRef, $fileHandle );
            return 1;
        }

        # add the incoming data granule into its associated granule key's
        # array if it was not already there.
        my @assocArray = @{$associateHashRef->{$key}};
        push ( @assocArray, $value )
            unless ( exists {map { $_ => 1 } @assocArray}->{$value} );
        $associateHashRef->{$key} = [ @assocArray ];
    } else {
        $associateHashRef->{$key} = ( defined $value ) ?
            [ "$value" ] : [];
    }
    CloseGranuleDB( $associateHashRef, $fileHandle );
    return 1;
};

################################################################################

=head1 Anonymous Method

Description:

    delete a granule's association record from DBM file. It is used only in
    S4PA::Storage::deleteAssociate().

Input:

    associate DB file, associate granule metadata file, 
    data granule metadata file, data granule directory, associate type.

Output:

    Returns 0/1 on failure/success and message

Algorithm:

    Function
        Open the associate DBM file.
        If the data granule was undefined, delete the whole associated key
            from hash only when the associated array is empty.
        Otherwise, delete that data granule from the associated key's 
            array, and then remove the symbolic link if it is not being
            associated by any other granule in that same directory.
        Close the associate DBM file.
        Return 1.
    End

=head1 AUTHOR

Guang-Dih Lei

=cut

my $_deleteAssocRecord = sub {
    my ( $dbFile, $associateMetaFile, $dataMetaFile, $associateType ) = @_;

    my $message;
    my $granuleDir;
    unless ( defined $associateMetaFile ) {
        return 1, "Associated key not defined, continue";
    }

    my ( $associateHashRef, $fileHandle ) = OpenGranuleDB( $dbFile, "rw" );
    # If unable to open associate database, complain and return.
    unless ( defined $associateHashRef ) {
        $message = "Failed to open associate database: $dbFile.";
        S4P::logger( 'ERROR', "$message" );
        return 0, $message;
    }

    my $key = basename( $associateMetaFile );
    unless ( exists $associateHashRef->{$key} ) {
        CloseGranuleDB( $associateHashRef, $fileHandle );
        return 1, "$key does not exist in associate.db"
    }
    my @assocArray = @{$associateHashRef->{$key}};

    # delete an associated granule key, ex. browse
    unless ( defined $dataMetaFile ) {
        # only delete a key when its associated array is empty
        if ( @assocArray ) {
            CloseGranuleDB( $associateHashRef, $fileHandle );
            $message = "$key is still associated with other data";
            S4P::logger( 'ERROR', "$message" );
            return 0, $message;
        }
        delete $associateHashRef->{$key};
        $message = "Deleted $key from associate.db";
        CloseGranuleDB( $associateHashRef, $fileHandle );
        return 1, $message;
    }

    # delete a data granule, remove it out from the associate array
    my $value = basename( $dataMetaFile );
    @assocArray = grep { $_ ne $value } @assocArray;
    $associateHashRef->{$key} = [ @assocArray ];
    $message = "Deleted $value from key $key array.";
    CloseGranuleDB( $associateHashRef, $fileHandle );

    $granuleDir = dirname( $dataMetaFile );
    my $assocMeta = S4PA::Metadata->new( FILE => $associateMetaFile );
    my ( $associateFile ) = 
        $assocMeta->getValue( "/S4PAGranuleMetaDataFile/DataGranule/GranuleID" );
    my $replacedLink = "$granuleDir" . "/$associateFile";
    return 1, $message unless ( -l $replacedLink );

    # remove the symbolic link in the data directory if it is
    # not used by any other granule under this directory any more.
    my $linkUsed = 0;

    # we need to make sure there is no other granule in this
    # same directory is still reference to this link before we
    # delete this replaced symbolic link.
    foreach my $granule ( @assocArray ) {
        my $dataMetaFile = $granuleDir . "/$granule";
        next unless ( -f $dataMetaFile );
        my $metadata = S4PA::Metadata->new( FILE => $dataMetaFile );
        my ( $associateValue ) = $metadata->getValue(
            "/S4PAGranuleMetaDataFile/DataGranule/$associateType" );
        if ( ( defined $associateValue ) &&
             ( $associateValue eq $associateFile ) ) {
            $linkUsed = 1;
            last;
        }
    }
    unlink $replacedLink unless ( $linkUsed );
    $message .= " Removed symbolic link $replacedLink.";
    return 1, $message;
};

################################################################################

=head1 StoreAssociate

Description:

    Stores a granule's association record in the metadata file and update
    the association DB.

Input:

    s4pa root directory, associated granule hash, data granule hash

Output:

    Returns create symbolic llink and modified metadata file

Algorithm:

    Function
        Create link point to the associated granule
        Update association DB
        Update metadata file with the associated file
        Update granule DB for the new metadata file checksum
        Return 1.
    End

=head1 AUTHOR

Guang-Dih Lei

=cut

sub StoreAssociate {
    my ( $rootDir, $assocGran, $dataGran, $replacedMetaFile ) = @_;

    # locate associate.db file
    my $dataClass = $assocGran->{DATACLASS};
    my $dataset = $assocGran->{DATASET};
    my $version = $assocGran->{VERSION};
    my $assocDbFile = ( $version eq '' ) ?
        "$rootDir/storage/$dataClass/$dataset/associate.db" :
        "$rootDir/storage/$dataClass/$dataset.$version/associate.db";
    return undef, undef unless ( -f $assocDbFile );

    my $associateMetaFile = $assocGran->{METFILE};
    my $assocMeta = S4PA::Metadata->new( FILE => $associateMetaFile );
    my $associateFile = 
        $assocMeta->getValue( '/S4PAGranuleMetaDataFile/DataGranule/GranuleID' );
    my $associateGranule = dirname( $associateMetaFile ) . "/$associateFile";

    # no data granule defined, just add associate granule into db file
    unless ( defined $dataGran->{METFILE} ) {
        return $associateMetaFile, undef
            if ( $_storeAssocRecord->( $assocDbFile,
                                       $associateMetaFile, 
                                       undef ) );
        return undef, undef;
    }

    my $dataMetaFile = $dataGran->{METFILE};
    my $dataDir = dirname( $dataMetaFile );

    # remove the current link if exist, then create the new one.
    my $linkTarget = "$dataDir/" . $associateFile;
    unlink $linkTarget if ( -l $linkTarget );
    unless ( symlink( $associateGranule, $linkTarget ) ) {
        S4P::logger( 'ERROR',
            "Failed to create symbolic link, $linkTarget to the file, "
            . "$associateGranule: $!" );
        return undef, undef;
    }

    # store the data granule into association db
    unless ( $_storeAssocRecord->( $assocDbFile,
                                   $associateMetaFile, 
                                   $dataMetaFile) ) {
        # remove symbolic link that were created.
        unlink $linkTarget || S4P::logger( 'ERROR', 
            "Failed to remove associate link: $!" );
        return undef, undef;
    }   

    # update data granule's metadata file with the new associate granule 
    my $modified = 0;
    my $metadata = S4PA::Metadata->new( FILE => $dataMetaFile );
    my $associateNodeName = $assocGran->{TYPE} . "File";
    my ( $associateValue ) = $metadata->getValue( 
        "/S4PAGranuleMetaDataFile/DataGranule/$associateNodeName" );
    if ( defined $associateValue ) {
        unless ( $associateValue eq $associateFile ) {
            $metadata->replaceNode( 
                XPATH => "/S4PAGranuleMetaDataFile/DataGranule/$associateNodeName",
                VALUE => $associateFile );
            $modified = 1;

            # we want to delete the current dataGranule from the original
            # associated granule key's array, so we don't have to worry about
            # a dangling reference when the original associated granule get
            # to be deleted.
            my $replaced;
            if ( defined $replacedMetaFile ) {
                foreach $replaced ( @{$replacedMetaFile} ) {
                    # skip science file, only xml file for keys
                    next unless ( $replaced =~ /\.xml$/ );
                    $_deleteAssocRecord->( $assocDbFile,
                                           $replaced,
                                           $dataMetaFile,
                                           $associateNodeName );
                }
            } else {
                $replaced = "$associateValue" . ".xml";
                $_deleteAssocRecord->( $assocDbFile,
                                       $replaced,
                                       $dataMetaFile,
                                       $associateNodeName );
            }
        }
    } else {
        $metadata->insertNode( 
            NAME => $associateNodeName,
            BEFORE => "/S4PAGranuleMetaDataFile/DataGranule/SizeBytesDataGranule",
            VALUE => $associateFile );
        $modified = 1;
    }

    # only update xml file and granule.db if metadata was modified
    return $linkTarget, undef unless ( $modified );
    $metadata->write();

    # update granule.db with the new xml file checksum
    my $crc = ComputeCRC( $dataMetaFile );
    $dataClass = $dataGran->{DATACLASS};
    $dataset = $dataGran->{DATASET};
    $version = $dataGran->{VERSION};
    my $dbFile = ( $version eq '' ) ?
        "$rootDir/storage/$dataClass/$dataset/granule.db" :
        "$rootDir/storage/$dataClass/$dataset.$version/granule.db";
    my ( $granuleHashRef, $fileHandle ) = OpenGranuleDB( $dbFile, "rw" );
    my $key = basename( $dataMetaFile );
    my $record = $granuleHashRef->{$key};
    $record->{cksum} = $crc;
    $granuleHashRef->{$key} = $record;
    CloseGranuleDB( $granuleHashRef, $fileHandle );

    return $linkTarget, $dataMetaFile;
}

################################################################################

=head1 DeleteAssociate

Description:

    Delete a granule with association.

Input: 
    
    s4pa root directory, associated granule hash, data granule hash, and
    fileGroup object containing the granule to be deleted.

Output:

    Returns new fileGroup and message

Algorithm:
    
    Function
        Locate the associate.db file
        For forwared association, delete the granule from associate
            unlink the associate file, then create new fileGroup
            without the associated granule
        For reverse association, just delete the key from associate.db
            if this granule is not associated by any others
        Return the update fileGroup and message.
    End

=head1 AUTHOR

Guang-Dih Lei

=cut
    
sub DeleteAssociate {
    my ( $rootDir, $assocGran, $dataGran, $fileGroup ) = @_;

    my $newFg = new S4P::FileGroup;
    my $assocClass = $assocGran->{DATACLASS};
    my $assocDataset = $assocGran->{DATASET};
    my $assocVersion = $assocGran->{VERSION};
    my $assocType = $assocGran->{TYPE};

    my $assocDbFile = ( $assocVersion eq '' ) ?
        "$rootDir/storage/$assocClass/$assocDataset/associate.db" :
        "$rootDir/storage/$assocClass/$assocDataset.$assocVersion/associate.db";

    # forward association, data -> browse
    # delete this granule record from associate.db and then remove the 
    # associate link only without deleting the associated granule.
    if ( defined $dataGran ) {

        # Get the associate file node from the metadata file
        my $dataMetaFile = $fileGroup->met_file();
        unless ( -f $dataMetaFile ) {
            $newFg = $fileGroup->copy();
            return $newFg, "Metadata file: $dataMetaFile not found";
        }

        my $granuleDir = dirname( $dataMetaFile );
        my $metadata = S4PA::Metadata->new( FILE => $dataMetaFile );
        my $dataGranule = "$granuleDir/" .
            $metadata->getValue( '/S4PAGranuleMetaDataFile/DataGranule/GranuleID' );
        my $nodeName = $assocType . "File";
        my ( $assocGranule ) = $metadata->getValue(
            "/S4PAGranuleMetaDataFile/DataGranule/$nodeName" );
        my $assocGranuleLink = "$granuleDir/$assocGranule";
        unless ( -l $assocGranuleLink ) {
            $newFg = $fileGroup->copy();
            return $newFg, "Associate link: '$assocGranuleLink' not found";
        }

        # locate the associated granule's metadata file
        # first try append .xml to the granule's fileid
        my $assocMetaPath = readlink( $assocGranuleLink ) . ".xml";
        # in case metadata filename was twisted, try matching
        # the granuleid with all xml files in that directory
        unless ( -f $assocMetaPath ) {
            my $assocDir = dirname( $assocMetaPath );
            if ( opendir( DH, "$assocDir" ) ) {
                my @fileList = map( $_,
                    grep( /\.xml$/, readdir( DH ) ) );
                closedir( DH );
                XMLFILE: foreach my $file ( @fileList ) {
                    $file =~ s/\.xml$//;
                    if ( $assocGranule =~ /$file/ ) {
                        $assocMetaPath = "$assocDir" . "/$file" . ".xml";
                        last XMLFILE;
                    }
                }
            }
        }
        my $assocMetaFile = basename( $assocMetaPath );

        # delete this granule from associate.db
        my ( $status, $message ) = $_deleteAssocRecord->( $assocDbFile,
            $assocMetaPath, $dataMetaFile, $nodeName );

        # so far, our policy is not to do a cascade deletion
        # even if this associated granule only has one parent.
        # we might change this policy latter.

        # we now need to just unlink the associate symbolic link,
        # create a new fileGroup to exclude the associate granule
        # so the actual file deletion will not delete this
        # associated granule.
        if ( $status ) {
            # remove the associate granule from incoming fileGroup
            my @attributes = ('data_type', 'data_version', 'node_name');
            my @fileSpecs;
            map { $newFg->{$_} = $fileGroup->{$_} } @attributes;
            foreach my $fs ( @{$fileGroup->file_specs} ) {
                next if ( $fs->file_id eq $assocGranule );
                push @fileSpecs, $fs;
            }
            $newFg->file_specs(\@fileSpecs);

        # probably no association record found, 
        # proceed to file deletion directly.
        } else {
            $newFg = $fileGroup->copy();
        }
        return $newFg, $message;

    # reverse association, browse -> data
    # this associated granule (ex. browse) can be deleted
    # only if it is not associated by any data granule
    } else {

        # check associate.db to see if it is qualified for deletion
        # no dataGranule info is needed to pass in
        my $assocMetaFile = $fileGroup->met_file();
        my ( $status, $message ) = $_deleteAssocRecord->( $assocDbFile,
            $assocMetaFile, undef, undef );

        # no deletion done, return undefined fileGroup to 
        # prevent # file deletion
        return undef, $message unless $status;;

        # record has been deleted from associate.db
        # copy the current file_group to new fileGroup and return
        $newFg = $fileGroup->copy();
        return $newFg, $message;
    }
}

################################################################################

=head1 SearchAssociate

Description:

    Search for the associated granule or the data granule.

Input:

    $rootDir     - s4paRoot directory
    $direction   - association relation
    $assocGran   - associated (browse) granule info hash
    $dataGran    - data granule's info hash

Return:

    Array of associated granules

Algorithm:


=head1 AUTHOR

Guang-Dih Lei

=cut

sub SearchAssociate {
    my ( $rootDir, $direction, $assocGran, $dataGran ) = @_;

    my $metFile;
    my %argSearch;
    # for forward search, data -> browse, search the browse dataset
    if ( $direction == 1 ) {
        $metFile = $dataGran->{METFILE};
        $argSearch{dataset} = $assocGran->{DATASET};
        $argSearch{version} = $assocGran->{VERSION};
        $argSearch{exclusive} = 1;

    # for backward search, browse -> data, search the data dataset
    } elsif ( $direction == -1 ) {
        $metFile = $assocGran->{METFILE};
        $argSearch{dataset} = $dataGran->{DATASET};
        $argSearch{version} = $dataGran->{VERSION};

    # unknown search direction
    } else {
        return undef;
    }

    # get range datetime from the source metadata file
    return undef unless ( -f $metFile );
    my $metadata = S4PA::Metadata->new( FILE => $metFile );
    my $beginTime = $metadata->getBeginTime();
    my $beginDate = $metadata->getBeginDate();
    my $endTime = $metadata->getEndTime();
    my $endDate = $metadata->getEndDate();
    my $beginDateTime = $beginDate . ' ' . $beginTime;
    my $endDateTime = $endDate . ' ' . $endTime;

    $argSearch{home} = $rootDir;
    $argSearch{startTime} = $beginDateTime;
    $argSearch{endTime} = $endDateTime;

    my $search = S4PA::MachineSearch->new( %argSearch );
    return undef if $search->onError();
    my @associatedLocator = $search->getGranuleLocator();
    my @associated;
    foreach my $granule ( @associatedLocator ) {
        my $metFile = $search->locateGranule( $granule );
        push @associated, $metFile;
    }
    return ( @associated );
}

################################################################################

=head1 CheckAssociation

Description:

    Find the dataset relationship and locate the associated dataset

Input:

    $root             - S4PA instance root directory
    $dataset          - Dataset name
    $version          - Dataset version (may be undef)

Return:

    Relationship      1: associated dataset (ex. browse)
                     -1: data dataset (ex. airs data)
                      0: no association dataset found
    Associated Type
    Array of associated dataset

Algorithm:


=head1 AUTHOR

Guang-Dih Lei

=cut

sub CheckAssociation {
    my ( $root, $dataset, $version ) = @_;
    # Setup compartment and read config file
    my $cpt = new Safe( 'CFG' );
    $cpt->share( '%data_association', '%association_type' );

    # Read config file
    my $cfgFile = "$root/storage/dataset.cfg";
    S4P::perish( 1, "Cannot read config file $cfgFile")
        unless ( $cpt->rdo( $cfgFile ) );
    return ( 0, undef, undef ) unless ( %CFG::data_association );

    my ( $sourceSet, @targetSet, $relation );
    # Format current dataset for association matching
    $sourceSet = ( $version eq '' ) ? "$dataset" : "$dataset.$version";

    # Matched dataset in the association hash key,
    # this is a DATA dataset, return 1 and its associated dataset (BROWSE).
    if ( defined $CFG::data_association{$sourceSet} ) {
        my $associated = $CFG::data_association{$sourceSet};
        my $type = $CFG::association_type{$associated};
        push @targetSet, $associated;
        return ( 1, $type, @targetSet );

    } else {
        # Collect all keys whose value matched with the current dataset
        foreach my $key ( keys %CFG::data_association ) {
            push @targetSet, $key
                if ( $CFG::data_association{$key} eq $sourceSet );
        }

        # Matched at least one in association hash value,
        # this is an associated datset (BROWSE), return -1 and it DATA dataset.
        if ( @targetSet ) {
            my $type = $CFG::association_type{$sourceSet};
            return ( -1, $type, @targetSet );

        # No association, return 0.
        } else {
            return ( 0, undef, undef );
        }
    }
}

1;
