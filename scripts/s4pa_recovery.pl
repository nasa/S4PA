#!/usr/bin/perl
=head1 NAME

s4pa_recovery.pl - A script to restore granule.db and storage links.

=head1 SYNOPSIS

s4pa_recovery.pl
[B<-r> I<S4PA station root directory>]
[B<-a> I<action>]
[B<-d> I<dataset name>]
[B<-v> I<version ID>]
[B<-f> I<active file system root>]
[B<-u>]


=head1 DESCRIPTION

s4pa_recovery.pl is a tool to recover granule.db and storage links based if one
of them is lost. Valid actions are: 'storage->database' for recovering
granule.db from storage links, 'database->storage' for recovering storage links
based on granule.db, and 'archive->database' for recovering granule.db and
storage links from the archive area. For all actions granule.db is inserted with
any missing entry and existing entries are updated. Optionally, if switch '-u' 
is specified for 'storage->database' action, existing entries are not updated. 

=head1 ARGUMENTS

=over 4

=item [B<-r> I<S4PA station root directory>]

Root directory of S4PA stations.

=item [B<-a> I<action>]

Action to be performed. Valids are 'archive->database', 'storage->database' and
'database->storage'.

=item [B<-d> I<dataset name>]

Dataset name.

=item [B<-v> I<version ID>]

Version ID. For versionless datasets, use quoted empty string ('').

=item [B<-f> I<active file system root>]

Root directory of active file systems. It has meaning only for
'archive->database' action.

=item [B<-u>]

If present, this switch will result in updates to existing granule.db entries
and insertion of missing entries. By default, the behavior is to overwrite
exising granule.db entries.

=back

=head1 AUTHOR

M. Hegde

=cut
# $Id: s4pa_recovery.pl,v 1.11 2008/12/22 17:57:55 ffang Exp $
# -@@@ S4PA, Version $Name:  $

use Getopt::Std;
use XML::LibXML;
use File::stat;
use File::Basename;
use File::Copy;
use vars qw($opt_r $opt_d $opt_v $opt_a $opt_f $opt_u);
use S4P;
use S4PA::Storage;
use S4PA::Metadata;
use S4P::PDR;

getopts('r:d:v:a:f:u');

usage() if ( (not defined $opt_r) || (not defined $opt_d) 
    || (not defined $opt_a) || (not defined $opt_v) );
usage() if ( ($opt_a eq 'database->storage' || $opt_a eq 'archive->database') 
    && (not defined $opt_f) );
S4P::logger( "WARN", "Neglecting -f; not needed for action=$opt_a" )
    if ( (defined $opt_f) && ($opt_a eq 'storage->database') );
# Remove the trailing backslash
$opt_r =~ s/\/+$//g;
$opt_u = $opt_u ? 0 : 1;

my $status = 0;
if ( $opt_a eq 'storage->database' ) {
    # Case of restoring granule.db from storage directory
    $status = StorageToDatabase( ROOT => $opt_r, DATASET => $opt_d, 
        VERSION => $opt_v, OVERWRITE => $opt_u );
} elsif ( $opt_a eq 'database->storage' ) {
    # Case of restoring links from granule.db
    $status= DatabaseToStorage( ROOT => $opt_r, DATASET => $opt_d, 
        VERSION => $opt_v, ARCHIVE => $opt_f );
} elsif ( $opt_a eq 'archive->database' ) {
    # Case of restoring database from archive directory
    $status = ArchiveToDatabase( ROOT => $opt_r, DATASET => $opt_d, 
        VERSION => $opt_v, ARCHIVE => $opt_f, OVERWRITE => $opt_u );
} else {
    print STDERR "Unknown value for -a: $opt_a\n";
    usage();
}
################################################################################
sub usage
{
    print STDERR 
        "Use: $0\n"
        . "    -r <S4PA root>: S4PA station root directory\n"
        . "    -a <Action>: The valids are 'archive->database',\n"
        . "          'storage->database' or 'database->storage'\n"        
        . "    -d <Dataset>: Dataset name\n"
        . "    -v <Version>: Dataset version label\n"
        . "    [-u] : Add missing entries to the granule.db. By default, the\n"
        . "           behavior is to overwrite. The database recovery\n"
        . "           is performed on original granule DB file; no DB\n"
        . "           copy is made under this usage.  This has effect\n"
        . "           on 'storage->database' action only\n"
        . "    [-f <Active File System Root>]: The archive file system's \n"
        . "          root to be used for 'archive->database' and\n"
        . "          'database->storage' actions\n";
    exit( 0 );   
}
################################################################################
sub ScanStorageDir
{
    my ( %arg ) = @_;
    local( *DH );
    my $date = $1 if ( $arg{DIR} =~ /\/(\d+)$/ );
    if ( defined $date ) {
        if (defined $arg{DATE} ) {
            $arg{DATE} .= "/$date";
        } else {
            $arg{DATE} = "$date";
        }
    }
    my $linkHash = {};
    if ( opendir( DH, $arg{DIR} ) ) {
        my @dirContent = map( $arg{DIR} . "/$_", 
            grep( !/^\.{1,2}$/, readdir( DH ) ) );
        closedir( DH );
        ITEM: foreach my $item ( @dirContent ) {
            if ( -d $item ) {
                ScanStorageDir( DIR => $item, DB => $arg{DB},
                    DATE => $arg{DATE}, OVERWRITE => $arg{OVERWRITE} );
            } elsif ( -l $item && $item =~ /\.xml$/ ) {
                my $metadata = S4PA::Metadata->new( FILE => $item );
                if ( $metadata->onError() ) {
                    S4P::logger( "ERROR",
                        "Failed to open $metadata: " 
                        . $metadata->errorMessage() );
                } else {
                    my $fileHash = $metadata->getFiles();
                    my @fileList = keys( %$fileHash );
                    FILE_IN_METADATA: foreach my $file ( @fileList ) {
                        my $key = basename( $file );
                        if ( !$arg{OVERWRITE} && defined $arg{DB}{$key} ) {
                            S4P::logger( "INFO",
                                "$file exists in the granule DBl; skipping" );
                            next FILE_IN_METADATA;
                        }
                        
                        unless ( defined $arg{DB}{$key} ) {
                            S4P::logger( "INFO",
                                "$file doesn't exist in granule DB; recreating"
                                . " the record" );
                        }
                        my $record = {};
                        my $stat = File::stat::stat( $file );
                        $record->{mode} = $stat->mode() & 07777;
                        $record->{cksum} = S4PA::Storage::ComputeCRC( $file );
                        $record->{date} = $arg{DATE};
                        my $linkSrc = readlink( $file );
                        if ( $linkSrc =~ /\/(\d+)\/+$arg{DATASET}/ ) {
                            $record->{fs} = $1;
                            $arg{DB}{$key} = $record;
                            delete $linkHash->{$key};
                        } else {
                            S4P::logger( "ERROR",
                                "Failed to find active file system for "
                                . $file );
                        }
                    } 
                }
            } elsif ( -l $item ) {
                my $key = basename( $item );
                $linkHash->{$key} = 1 unless ( defined $arg{DB}{$key} );
            }
        }
        foreach my $key ( keys %$linkHash ) {
            S4P::logger( "WARNING",
                $arg{DIR} . "/$key is still missing; it is not in any"
                . " metadata file\n" );
        }
    } else {
        S4P::logger( "ERROR", "Failed to open directory $arg{DIR}" );
    }
}
################################################################################
sub StorageToDatabase
{
    my ( %arg ) = @_;
    
    my %result = 
        GetDataRootDirectory( %arg, MATCH_TYPE => 'EXACT' );
    my ( $dataLink, $dbFile ) = ( "$result{DATADIR}/data",
        "$result{DATADIR}/granule.db" ); 
    unless ( -l $dataLink ) {
        S4P::logger( "ERROR", "Data link, $dataLink, doesn't exist" );
        return 0;
    }
    unless ( -f $dbFile ) {
        S4P::logger( "ERROR", "Granule DB file, $dbFile, doesn't exist" );
        return 0;
    }

    # Check if there already exits a copy of the granule.db
    my $dbFileCopy;
    if($arg{OVERWRITE}) {
        $dbFileCopy = $dbFile . ".copy";
        if (-e $dbFileCopy) {
            print "A copy of $dbFileCopy already exist; overwrite? (y/n)\n";
            my $promptAnswer = <STDIN>;
            chomp $promptAnswer;
            if ($promptAnswer !~ /^y/i) {
                S4P::logger( 'ERROR', "Please remove $dbFileCopy and try again" );
                return 0;
            } else {
                `rm -rf $dbFileCopy`;
                if ($?) {
                    S4P::logger( 'ERROR', "Cannot remove DB file $dbFileCopy");
                    return 0;
                }
            }
        }
    } else {
        $dbFileCopy = $dbFile;
    }

    my ( $granuleRef, $fileHandle ) =
        S4PA::Storage::OpenGranuleDB( $dbFileCopy, "rw" );
    unless ( defined $granuleRef ) {
        S4P::logger( "ERROR", "Failed to open granule DB file, $dbFileCopy" );
        return 0;
    }
    my $storageDir = readlink( $dataLink );
    my $status = 0;
    if ( defined $storageDir ) {
        $storageDir =~ s/\/+$//g;
        $status = ScanStorageDir( DIR => $storageDir, DB => $granuleRef, 
            DATASET => $arg{DATASET}, OVERWRITE => $arg{OVERWRITE} );
    } else {
        S4P::logger( "ERROR", "Failed to read link $dataLink" );
        $status = 0;
    }
    S4PA::Storage::CloseGranuleDB( $granuleRef, $fileHandle );
    return $status;
}
################################################################################
sub ArchiveToDatabase
{
    my ( %arg ) = @_;
    
    my %result = 
        GetDataRootDirectory( %arg, MATCH_TYPE => 'EXACT' );
    my ( $dataLink, $dbFile ) = ( "$result{DATADIR}/data",
        "$result{DATADIR}/granule.db" ); 
    unless ( -l $dataLink ) {
        S4P::logger( "ERROR", "Data link, $dataLink, doesn't exist" );
        return 0;
    }
    unless ( -f $dbFile ) {
        S4P::logger( "ERROR", "Granule DB file, $dbFile, doesn't exist" );
        return 0;
    }
  
    # Check if there already exits a copy of the granule.db
    my $dbFileCopy;
    if($arg{OVERWRITE}) {
        $dbFileCopy = $dbFile . ".copy";
        if (-e $dbFileCopy) {
            print "A copy of $dbFileCopy already exist; overwrite? (y/n)\n";
            my $promptAnswer = <STDIN>;
            chomp $promptAnswer;
            if ($promptAnswer !~ /^y/i) {
                S4P::logger( 'ERROR', "Please remove $dbFileCopy and try again" );
                return 0;
            } else {
                `rm -rf $dbFileCopy`;
                if ($?) {
                    S4P::logger( 'ERROR', "Cannot remove DB file $dbFileCopy");
                    return 0;
                }
            }
        }
    } else {
        $dbFileCopy = $dbFile;
    }

    my ( $granuleRef, $fileHandle ) =
        S4PA::Storage::OpenGranuleDB( $dbFileCopy, "rw" );
    unless ( defined $granuleRef ) {
        S4P::logger( "ERROR", "Failed to open granule DB file, $dbFileCopy" );
        return 0;
    }
    my $storageDir = readlink( $dataLink );
    unless ( defined $storageDir ) {
        S4P::logger( "ERROR", "Failed to read link $dataLink" );
        S4PA::Storage::CloseGranuleDB( $granuleRef, $fileHandle );
        return 0;
    }
    $storageDir =~ s/\/+$//g;
    
    my @fileSystemList = ();
    $arg{ARCHIVE} =~ s/\/+$//g;
    if ( opendir( DH, "$arg{ARCHIVE}" ) ) {
        @fileSystemList = map( $arg{ARCHIVE} . "/$_",
            grep( /^\d+$/, readdir( DH ) ) );
        closedir( DH );
    } else {
        S4P::logger( "ERROR", "Failed to list $arg{ARCHIVE}" );
        S4PA::Storage::CloseGranuleDB( $granuleRef, $fileHandle );
        return 0;
    }


    FILE_SYSTEM: foreach my $fileSystem ( @fileSystemList ) {
        if ( opendir( DH, "$fileSystem" ) ) {
            my @dataDirList = map( $fileSystem . "/$_",
                grep( /[^PDR]$/, grep( /^$arg{DATASET}/, readdir( DH ) ) ) );
            closedir( DH );
            my $pdr = S4P::PDR::create();
            $pdr->originating_system( "S4PA_RECOVERY" );
            DATA_DIR: foreach my $dataDir ( @dataDirList ) {
                S4P::logger( "INFO", "Scanning $dataDir" );
                if ( opendir( DH, "$dataDir" ) ) {
                    my @fileList = map( $dataDir . "/$_",
                        grep( /\.xml$/, readdir( DH ) ) );
                    closedir( DH );
                    FILE: foreach my $file ( @fileList ) {
                        my $metadata = S4PA::Metadata->new( FILE => $file );
                        if ( $metadata->onError() ) {
                            S4P::logger( "ERROR", $metadata->errorMessage() );
                            next FILE;
                        }
                        my $fileGroup = $metadata->getFileGroup();
                        $pdr->add_file_group( $fileGroup );
                        $pdr->recount();
                    }
                } else {
                    S4P::logger( "ERROR", "Failed to read $dataDir" );
                    next DATA_DIR;
                }
            }
            if ( $pdr->total_file_count() ) {
                my $storageWorkOrder = "$result{DATADIR}/../"
                    . "store_$result{DATACLASS}/DO.STORE_$arg{DATASET}.t" 
                    . time() . "p" . $$ . "n" . basename( $fileSystem ) . ".wo";
                if ( $pdr->write_pdr( $storageWorkOrder ) ) {
                    S4P::logger( "ERROR", "Failed to write $storageWorkOrder" );
                } else {
                    S4P::logger( "INFO", "Wrote $storageWorkOrder" );
                }
            }
        } else {
            S4P::logger( "ERROR", "Failed to read $fileSystem" );
            next FILE_SYSTEM;
        }
    }
    S4PA::Storage::CloseGranuleDB( $granuleRef, $fileHandle );

}
################################################################################
sub DatabaseToStorage
{
    my ( %arg ) = @_;
    
    my %result = 
        GetDataRootDirectory( %arg, MATCH_TYPE => 'EXACT' );
    my ( $dataLink, $dbFile ) = ( "$result{DATADIR}/data",
        "$result{DATADIR}/granule.db" ); 
    unless ( -l $dataLink ) {
        S4P::logger( "ERROR", "Data link, $dataLink, doesn't exist" );
        return 0;
    }
    unless ( -f $dbFile ) {
        S4P::logger( "ERROR", "Granule DB file, $dbFile, doesn't exist" );
        return 0;
    }
    my ( $granuleRef, $fileHandle ) =
        S4PA::Storage::OpenGranuleDB( $dbFile, "rw" );
    unless ( defined $granuleRef ) {
        S4P::logger( "ERROR", "Failed to open granule DB file, $dbFile" );
        return 0;
    }
    my $storageDir = readlink( $dataLink );
    unless ( defined $storageDir ) {
        S4P::logger( "ERROR", "Failed to read link $dataLink" );
        return 0;
    }
    $storageDir =~ s/\/+$//g;
    KEY: foreach my $key ( keys %$granuleRef ) {
        my $record = $granuleRef->{$key};
        my $dir;
        if ( defined $record->{date} ) {        
            $dir = "$storageDir/$record->{date}";
        } else {
            S4P::logger( "ERROR",
                "Date directory not defined for $key in $dbFile" );
        }
        next KEY unless defined $dir;
        if ( defined $record->{mode} ) {
            $dir .= "/.hidden"
                if ( $record->{mode} == 0640 || $record->{mode} == 0600 );
        } else {
            undef $dir;
            S4P::logger( "ERROR", "File mode not defined for $key in $dbFile" );
        }
        next KEY unless defined $dir;
        my $srcLink ;
        if ( defined $record->{fs} ) {
            $srcLink = "$arg{ARCHIVE}/$record->{fs}/$arg{DATASET}"
        } else {
            S4P::logger( "ERROR",
                "File system number not defined for $key in $dbFile" );
            next KEY;
        }
        
        my $targetLink = "$dir/$key";
        if ( -l $targetLink ) {
            S4P::logger( "INFO", "$targetLink exists; skipping" );
            next KEY;
        }        
        $srcLink .= ".$result{DATAVERSION}" if ( $result{DATAVERSION} ne '' );
        $srcLink .= "/$key";
        
        if ( -f $srcLink ) {
            my $dir = dirname( $targetLink );
            while( !( -d $dir ) ) {
                push( @dirList, $dir );
                $dir = dirname( $dir );
            }
            foreach $dir ( reverse @dirList ) {
                unless ( -d $dir ) {
                    mkdir( $dir );
                    if ( $rec->{mode} == 0640 ) {
                        chmod( 0750, $dir );
                    } elsif ( $rec->{mode} == 0600 ) {
                        chmod( 0700, $dir );
                    } else {
                        chmod( 0755, $dir ); 
                    }
                }
            }
            if ( symlink( $srcLink, $targetLink ) ) {
                S4P::logger( "INFO",
                    "Created symlink $targetLink to $srcLink" );
            } else {
                S4P::logger( "ERROR",
                    "Failed to symlink $targetLink to $srcLink ($!)" );
            }
        } else {
            S4P::logger( "ERROR", "$srcLink is missing; can't restore $key" );
        }
        next KEY unless defined $dir;
    }
}
################################################################################
sub GetDataRootDirectory {
    my ( %arg ) = @_;

    my %result;
        
    # Open dataset.cfg for read;
    my $cfg_file = "$arg{ROOT}/storage/dataset.cfg";

    # Setup compartment and read config file
    my $cpt = new Safe('CFG');
    $cpt->share('%data_class');

    # Read config file
    unless ( $cpt->rdo( $cfg_file ) )  {
        S4P::logger( "ERROR", "Cannot read config file $cfg_file" );
        return %result;
    }

    # Check for required variables
    if ( !%CFG::data_class ) {
        S4P::logger( "ERROR", "No data_class in $cfg_file" );
        return %result;
    }

    # Get dataclass from config hash
    my $dataClass = $CFG::data_class{$arg{DATASET}};

    unless ( defined $dataClass ) {
        S4P::logger( "ERROR", "Dataset, $arg{DATASET}, not supported" );
        return %result;
    }
    # We can now find store_cfg file
    my $storeCfgFile = "$arg{ROOT}/storage/$dataClass/"
        . "store_$dataClass/s4pa_store_data.cfg";

    # Read store_config files
    my $cptv = new Safe( 'CFGv' );
    $cptv->share( '%cfg_data_version' );
    unless ( $cptv->rdo($storeCfgFile) ) {
        S4P::logger( "ERROR", "Cannot read config file $storeCfgFile");
        return %result;
    }

    # Check for required variables
    if ( !%CFGv::cfg_data_version ) {
        S4P::logger( "ERROR", "Data version information not found in"
            . " $storeCfgFile");
        return %result;
    }

    # Get version from config hash
    my @versionList = @{$CFGv::cfg_data_version{$arg{DATASET}}};

    my $storedVersion;
    foreach my $version ( @versionList ) {
        if ( $version eq '' ) {
            $result{DATADIR} = "$arg{ROOT}/storage/$dataClass/$arg{DATASET}";
            $result{DATAVERSION} = '';
            $result{DATACLASS} = $dataClass;
            last;
        } elsif ( $arg{VERSION} eq $version ) {
            $result{DATADIR} = "$arg{ROOT}/storage/$dataClass/$arg{DATASET}"
                . ".$version";
            $result{DATAVERSION} = $version;
            $result{DATACLASS} = $dataClass;
            last;
        }
    }
    return %result;
}
