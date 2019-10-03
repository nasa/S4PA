#!/usr/bin/perl

=head1 NAME

s4pa_prune_recovery.pl - A command-line  script for pruning file system after 
recovery.

=head1 SYNOPSIS

s4pa_prune_recovery.pl -r $root -a $archive -s @file_system [-f $conf] 
[-d @data_set] [-c @data_class]


=head1 DESCRIPTION

s4pa_prune_recovery.pl is a script for pruning file systems after they have been
restored. This script will scan archive directory (ex: /ftp/.provider/) looking 
for files belonging to an optionally specified dataset and validating files 
against the granule DBM file located in station directories.
 -r: s4pa root directory (ex: /vol1/TS1/s4pa/)
 -a: data archive directory (ex: /ftp/.provider)
 -f: config file with dataset to data class mapping ( optional )
 -s: file system number (optional); use space as delimiter if more than one.
 -d: dataset; use space as delimiter if specifying more than one.
 -c: dataclass; use space as delimiter if specifing more than one.
 
 Examples:
    s4pa_prune_recovery.pl -r /vol1/OPS/s4pa -a /ftp/.seawifs -s "001 002"
    s4pa_prune_recovery.pl -r /vol1/OPS/s4pa -a /ftp/.seawifs -s "001 002" 
        -d "SL3MOM1 SL3MOM2"
    s4pa_prune_recovery.pl -r /vol1/OPS/s4pa -d "SL3MOM1 SL3MOM2"
    s4pa_prune_recovery.pl -r /vol1/OPS/s4pa -c "seawifs_l3m seawifs_gac"

If -a option is specified, the file system is cleaned up. Otherwise, it cleans
up storage area (directory tree containing symoblic links to files on the
file system).

Algorithm:
 
TBD
	
=head1 AUTHOR

Yangling Huang L-3
M. Hegde

=cut

################################################################################
# $Id: s4pa_prune_recovery.pl,v 1.8 2008/04/30 19:24:36 ffang Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use Getopt::Std;
use File::stat;
use File::Basename;
use File::Copy;
use S4PA::Storage;
use S4P;
use Safe;

use vars qw($opt_r $opt_a $opt_s $opt_d $opt_f $opt_c);     

# Parse command-line 
getopts('r:a:s:d:f:c:') || usage();

die "Specify S4PA root (-r)" unless defined $opt_r;


if ( defined $opt_a ) {
    # Clean up file sytem.
    PruneFileSystem( $opt_r, $opt_a, $opt_s, $opt_d, $opt_f );
} elsif ( defined $opt_d || defined $opt_c ) {
    # Clean up storage area.
    PruneStorage( $opt_r, $opt_c, $opt_d, $opt_f );
} else {
    die "Specify either archive root (-a) or a dataset (-d)"
        . " or a data class (-c)";
}
################################################################################

=head1 PruneStorage

Description:
    Removes dangling symbolic links in S4PA's storage area.
    
Input:
    $opt_r      - S4PA root directory
    $opt_c      - Data class
    $opt_d      - Dataset
    $opt_f      - data class to dataset mapping file

Output:
    Returns true on success. Perishes for any error.
    
Algorithm:

    Function PruneStorage
        Find year directories for the dataset.
        For each year directory
            Accummulate symbolic links in the year directory.
            For each month or day directory in year
                Accumulate symbolic links.
            Endfor
        Endfor
        
        For each symbolic link accumulated
            Get the source file of symbolic link.
            If the source file is defined and if the source file doesn't exist
                Delete the symbolic link.
            Endif
        Endfor
    End

=head1 AUTHOR

M. Hegde

=cut
sub PruneStorage
{
    my ( $opt_r, $opt_c, $opt_d, $opt_f ) = @_;
    
    my $conf = $opt_f || "$opt_r/storage/dataset.cfg";
    die "Dataset to data class configuration file, $conf, doesn't exist" 
	unless ( -f $conf );

    # Read configuration file
    my $cpt = new Safe 'CFG';
    $cpt->share('%data_class' );
    $cpt->rdo( $conf ) or
        S4P::perish( 1, "Cannot read config file, $conf, in safe mode: $!\n");
    
    my ( @datasetList, @dataclassList );
    
    @dataclassList = split( /\s+/, $opt_c ) if ( defined $opt_c );
    
    # Figure out involved datasets
    if ( defined $opt_d ) {
        # If datasets are defined via command line, use them.
        @datasetList = split( /\s+/, $opt_d );
    } else {
        # If datasets are not defined, grab all datasets belonging to data 
        # class.
        my %flag = map{ $_ => 1} @dataclassList;
        foreach my $key ( keys %CFG::data_class ) {
            push( @datasetList, $key ) if ( $flag{$CFG::data_class{$key}} );
        }
    }
    
    # Loop over each dataset
    foreach my $dataset ( @datasetList ) {
	S4P::logger( "INFO", "Checking dataset $dataset" );
        S4P::perish( 1, "Failed to find data class for $dataset" )
            unless defined $CFG::data_class{$dataset};
	# Read CheckIntegrity's config file to get supported data versions
	my $cfgFile = "$opt_r/storage/$CFG::data_class{$dataset}"
	    . "/check_$CFG::data_class{$dataset}/s4pa_check_integrity.cfg";
	my $cpt = new Safe( 'CFG' );
	$cpt->share( '%cfg_data_version' );
	$cpt->rdo( $cfgFile )
	    or S4P::perish( 1, "Failed to read conf file $cfgFile ($@)" );

	foreach my $version ( @{$CFG::cfg_data_version{$dataset}} ) {
	    S4P::logger( "INFO", "Checking version $version" )
		unless ( $version eq '' );
	    # Find the path of granule DB file based on the version supported.
	    my $granule_db_file = "$opt_r/storage/$CFG::data_class{$dataset}/"
		. $dataset . ($version eq '' ? "" : ".$version")
		. "/granule.db";

	    # Make sure the granule DB file exists; otherwise, log a message an skip.
            unless ( -f $granule_db_file ) {
		S4P::logger( "WARNING", "File, $granule_db_file, doesn't exist");
		next;
	    }

	    # Open granule DB file and exit on failure to open.
	    my ( $granule_ref, $file_handle ) 
		= S4PA::Storage::OpenGranuleDB( $granule_db_file, "rw" );
	    S4P::perish( 1, "Failed to open granule DB, $granule_db_file" )
		unless defined $granule_ref;
            
	    # Follow the data link.
	    my $dataLink = "$opt_r/storage/$CFG::data_class{$dataset}/"
		. $dataset . ($version eq '' ? "" : ".$version")
		. "/data";
	    my $dataDir = ( -l $dataLink ) ? readlink( $dataLink ) : undef;
	    S4P::perish( 1, "$dataLink is not a symbolic link" )
		unless defined $dataDir;
	    # Open dataset's directory and loop over year or year/month or 
	    # year/day directories underneath it.
	    local ( *DH );
	    unless ( opendir( DH, "$dataDir" ) ) {
		S4PA::Storage::CloseGranuleDB( $granule_ref, $file_handle );    
		S4P::perish( 1, "Failed to open $dataDir" );
	    }
	    my @yearList = grep( !/^\./ && -d "$dataDir/$_", readdir( DH ) );
	    closedir( DH );        
	    foreach my $year ( @yearList ) {
		unless ( opendir( DH, "$dataDir/$year" ) ) {
		    S4PA::Storage::CloseGranuleDB( $granule_ref, $file_handle );    
		    S4P::perish( 1, "Failed to open $dataDir/$year" );
		}
		my @inodeList = grep( !/^\./, readdir( DH ) );
		closedir( DH );
		foreach my $entry ( @inodeList ) {
		    my @fileList = ();
		    my $inode = "$dataDir/$year/$entry";
		    if ( -l $inode ) {
			# If the year directory has a link, accumulate it.
			push( @fileList, $inode );
		    } elsif ( -d $inode ) {
			# If the year directory has a sub-directory, list it.
			unless ( opendir( DH, "$inode" ) ) {
			    S4PA::Storage::CloseGranuleDB( $granule_ref, 
							   $file_handle );
			    S4P::perish( 1, "Failed to open $inode" );
			}
			@fileList = grep( !/^\./ && -l "$inode/$_",
			                  readdir( DH ) );
			foreach my $file ( @fileList ) {
			    $file = $inode . '/' . $file;
			}
			closedir( DH );
		    } else {
			# If the inode is not a symoblic link or a directory,
			# complain and stop.
			S4PA::Storage::CloseGranuleDB( $granule_ref,
			                               $file_handle );
			S4P::perish( 1, "$inode is neither a file or a dir!" );
		    }
		    # For each symbolic link found:
		    foreach my $file ( @fileList ) {
			my $source = readlink( $file );
			# Unlink if a dangling symbolic link is found.
			if ( defined $source && (! -f $source) ) {
			    if ( unlink ( $file ) ) {
				S4P::logger( 'INFO',
					     "Deleted $file" );
			    } else {
				S4P::logger( 'ERROR',
					     "Failed to delete $file ($!)" );
			    }
			}
		    }
		}
	    }
	    S4PA::Storage::CloseGranuleDB( $granule_ref, $file_handle );
	}
    }
    return ( 1 );
}
################################################################################

=head1 PruneFileSystem

Description:
    Removes dangling symbolic links in S4PA's storage area.
    
Input:
    $opt_r      - S4PA root directory
    $opt_a      - data archive root
    $opt_s      - File systems numbers (space delimited; 001 ...)
    $opt_d      - Dataset
    $opt_f      - data class to dataset mapping file

Output:
    Returns true on success. Perishes for any error.
    
Algorithm:

    Function PruneArchive
        Find datasets on file systems specified.
        For each file system
            For each dataset
                Locate and read granule DB file.
                For each file found in the datset directory on file system
                    If the file record exists in DB file & file system matches
                        Change file permissions if file permissions mismatch.
                    Else If the file record exists
                        Make sure that file exists on the file system on record.
                        Delete file.
                    Else
                        Delete file.
                    Endif
                Endfor
            Endfor
        Endfor
    End

=head1 AUTHOR

M. Hegde

=cut
sub PruneFileSystem
{
    my ( $opt_r, $opt_a, $opt_s, $opt_d, $opt_f ) = @_;
    
    my $archive_dir = $opt_a;
    my $root = $opt_r;
    S4P::perish( 1, "Specify root of data archive directory (-a)" )
        unless defined $opt_a;

    my $conf = $opt_f || "$opt_r/storage/dataset.cfg";
    die "Dataset to data class configuration file, $conf, doesn't exist" 
	unless ( -f $conf );

    # Read configuration file
    my $cpt = new Safe 'CFG';
    $cpt->share( '%data_class' );
    $cpt->rdo( $conf ) or
        S4P::perish( 3, "Cannot read config file, $conf, in safe mode ($!)");

    # read configuration for backup files
    my $config = "$opt_r/auxiliary_backup.cfg";
    die "Backup configuration file, $config, doesn't exist"
        unless ( -f $config );
    my $cpt1 = new Safe 'BACKUP';
    $cpt1->share('$cfg_auxiliary_backup_root' );
    $cpt1->rdo( $config ) or
        S4P::perish( 1, "Cannot read config file, $config, in safe mode ($!)");
    my $backupRoot = $BACKUP::cfg_auxiliary_backup_root;
    $backupRoot =~ s/\/+$//;

    # Split file systems based on the delimiter.
    my @fss = split( /\s+/, $opt_s );   
    foreach my $fs ( @fss ) {
        next if ( $fs eq undef );
        my ( $file_handle, $status );
        my $granule_ref = {};
        # If datasets are specified
        my @datasets;
	# A hash to hold dataset/version derived from directory in cases where
	# dataset is not specified via command line.
	my $versionHash = {};
        if ( $opt_d ) {
            @datasets = split( /\s+/, $opt_d );
        } else {
            # If datasets are not specified, figure it out from directory 
            # listing.
            local ( *DH );
            opendir(DH, "$archive_dir/$fs" )
                || S4P::perish( 1, 
                            "Failed to open directory, $archive_dir/$fs ($!)" );
            # Read directory; filter out hidden files (. files)
            @datasets = grep( !/^\./ && -d "$archive_dir/$fs/$_",
                             readdir ( DH ) );
            closedir (DH) 
                || S4P::perish( 1, 
                            "Failed to close directory, archive_dir/$fs ($!)" );
            foreach my $dataset ( @datasets ) {
                if ( $dataset =~ /([^.]+)\.(.*)/ ) {
		    $dataset = $1;
		    $versionHash->{$dataset}{$2} = 1;
		}
            }
        }
    
        # For each dataset found:
        foreach my $dataset ( @datasets ) {
            next if ( $dataset eq 'lost+found' );
	    S4P::logger( "INFO", "Checking dataset $dataset" );
            S4P::perish( 1, "Data class not found for $dataset" )
                unless defined $CFG::data_class{$dataset};
	    # Read CheckIntegrity's config file to get supported data versions
	    my $cfgFile = "$opt_r/storage/$CFG::data_class{$dataset}"
	        . "/check_$CFG::data_class{$dataset}/s4pa_check_integrity.cfg";
	    my $cpt = new Safe( 'CFG' );
	    $cpt->share( '%cfg_data_version' );
	    $cpt->rdo( $cfgFile )
	        or S4P::perish( 1, "Failed to read conf file $cfgFile ($@)" );
                        
	    # Use the version list for a dataset or the version list derived from
	    # directory name.
	    my @versionList = ( defined $versionHash->{$dataset} )
		? ( keys %{$versionHash->{$dataset}} )
		: @{$CFG::cfg_data_version{$dataset}};
	    foreach my $version ( @versionList ) {

		S4P::logger( "INFO", "Checking version $version" )
		    unless ( $version eq '' );
	        # Find the path of granule DB file based on the version
                # supported.
	        my $granule_db_file = "$opt_r/storage/"
                    . "$CFG::data_class{$dataset}/$dataset"
                    . ($version eq '' ? "" : ".$version")
		    . "/granule.db";
                
                S4P::perish( 1, "File, $granule_db_file, doesn't exist") 
                    unless ( -f $granule_db_file );

                # Open granule DB file and exit on failure to open.
                ( $granule_ref, 
                $file_handle ) = S4PA::Storage::OpenGranuleDB( $granule_db_file,
                                                           "rw" );
                S4P::perish( 1, "Failed to open granule DB, $granule_db_file" )
                    unless defined $granule_ref;
            

                # Read dataset's directory on the file system.
                local( *DH );
                my $data_dir = "$archive_dir/$fs/$dataset";
		$data_dir .= ".$version" unless ( $version eq '' );

                S4P::perish( 1, "Failed to open directory $data_dir ($!)" )
                    unless ( opendir( DH, $data_dir ) );
                my @file_list
                    = grep( !/^\./ && -f "$data_dir/$_", readdir( DH ) );
            
                # For each file found:
                foreach my $entry ( @file_list ) {
                    my $file = "$data_dir/$entry";
                    my $record = $granule_ref->{$entry};

                    if ( defined $record && ($record->{fs} eq $fs) ) {
                        # Granule record exists; check its file permissions.
                        my $st = stat( $file );
                        # Make sure the file permissions match.
                        if ( ($st->mode() & 07777) != $record->{mode} ) {
                            unless ( chmod( $record->{mode}, $file ) ) {
                                S4PA::Storage::CloseGranuleDB( $granule_ref, 
                                                               $file_handle );
                                S4P::perish( 1, "'chmod $record->{mode} $file' "
                                            . "failed ($!)" );
                            }
                            S4P::logger( "INFO", 
                                         "Changed file permissions of $file" );
                        }
                    } elsif ( defined $record ) {
                        # If the granule record is present, but the file is
                        # on a different file system, make sure that the 
                        # recorded file exists somewhere else before deleting 
                        # it.
                        my $loc = $file;
                        $loc =~ s/\/$record->{fs}\//\/$fs\//;
                        unless ( -f $loc ) {
                            S4PA::Storage::CloseGranuleDB( $granule_ref, 
                                $file_handle );
                            S4P::perish( 1, 
                                "Recorded copy of $file on $record->{fs} "
                                . "is not found" );
                        }
                        S4P::logger( "INFO", "$file should be on $fs" );
                        unlink ( $file ) 
			    || S4P::perish( 1, "Failed to delete $file ($!)" );
                    } else {
                        # If the granule record is absent or if the granule is
                        # supposed to be on a different file system, delete it.
                        S4P::logger( "INFO", "$file in not on record: deleting" );
                        unlink ( $file )
			    || S4P::perish( 1, "Failed to delete $file ($!)" );
                    }
                }

                # update archive based on backup file list
                my $esdt = $dataset . ($version eq '' ? "" : ".$version");
                my @backFileList = ();
                my $backupDir = $backupRoot . "/$esdt";
                if (-d $backupDir) {
                    local ( *DH );
                    unless ( opendir( DH, "$backupDir" ) ) {
                        S4P::perish( 1, "Failed to open $backupDir" );
                    }
                    @backFileList = grep( !/^\./, readdir( DH ) );
                    closedir( DH );
                } else {
                    S4P::logger( "INFO", "Back-up directory, $backupDir, does not exist" );
                }
                if (@backFileList) {
                    foreach my $entry (@backFileList) {
                        my $record = $granule_ref->{$entry};        
                        if ( defined $record ) {
                            my $replaceRecord;
                            # replace archive copy with backup
                            my $fromFile = $backupDir . "/$entry";
                            my $toFile = $data_dir . "/$entry";
                            my $fromFileModTime = stat( $fromFile )->mtime();
                            my $toFileModTime = stat( $toFile )->mtime();
                            if ($fromFileModTime gt $toFileModTime) {
                                S4P::perish(1, "Failed to copy $fromFile to $toFile: ($!)")
                                    unless ( copy($fromFile, $toFile) );
                                S4P::logger("INFO", "copied $fromFile to $toFile");
                                # calculate checksum for new file
                                $replaceRecord->{$entry}{cksum} = 
                                      S4PA::Storage::ComputeCRC( $toFile );
                                # assign file system and maintain date
                                $replaceRecord->{$entry}{fs} = $fs;
                                # maintain date and mode
                                $replaceRecord->{$entry}{date} = $record->{date};
                                my $st = stat( $toFile );
                                if ( ($st->mode() & 07777) != $record->{mode} ) {
                                    unless ( chmod( $record->{mode}, $toFile ) ) {
                                        S4PA::Storage::CloseGranuleDB( $granule_ref,
                                                                       $file_handle );
                                        S4P::perish( 1, "'chmod $record->{mode} $toFile' "
                                                    . "failed ($!)" );
                                    }
                                    S4P::logger( "INFO",
                                                 "Changed file permissions of $toFile" );
                                }
                                $replaceRecord->{$entry}{mode} = $record->{mode};
                                # update record
                                $record = $replaceRecord->{$entry};
                                S4P::logger( "INFO",
                                  "Record in granule.db for $entry updated" );
                            } else {
                                S4P::logger( "INFO",
                                  "Reserve file $fromFile older than existing archive file $toFile");
                            }
                        }
                    }
                }
                S4PA::Storage::CloseGranuleDB( $granule_ref, $file_handle );
            }   # End of for-each version
        }   # End of for-each dataset

        S4P::logger( 'INFO', 
                     "pruned $archive_dir/$fs for " . join( ",", @datasets ) );
    }

    return ( 1 );
}
################################################################################
# Subroutine usage:  print usage and die
sub usage {
    S4P::perish( 1, "
        Usage: $0 -r <S4PA root directory> -a <root directory of archive> -s <versions> -d <dataset> -f [confg]");
}

