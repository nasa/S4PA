#!/usr/bin/perl

=head1 NAME

s4pa_remote_polling_without_pdr.pl - script to poll a remote directory for data

=head1 SYNOPSIS

s4pa_remote_polling_without_pdr.pl
[B<-m> I<max_file_group_count_per_data_type>]
[B<-c> I<data_type_cfg_file>]
[B<-r>]
[B<-l> I<Originating system>]
[B<-i> I<Ignore history checking>]
[B<-p> I<repolling_pause_in_second>]
[B<-s> I<minimum_file_size>]
B<history_file>
B<remote_hostname>
B<remote_directory>
B<local_pdr_directory>

=head1 ARGUMENTS

=over

=item B<-m> I<max_file_group_count_per_data_type>

Optional max file group count in a PDR (per data type) for a scan attempt.

=item B<-c> I<data_type_cfg_file>

Configuration file that supplies data type key word / data type cross reference.
Used to identify the data type. Defaults to ../s4pa_datatype.cfg

=item B<-r>

Optional switch to indicate traversal of directory tree.

=item B<-l> I<Originating system>

The name of originating system to be used in resulting PDR.

=item B<-i> I<Ignore history checking>

Optional swith to indicate skipping history record chcking.
If specified, all existing data will be polled again.

=item B<-p> I<repolling_pause_in_second>

Optional sleep time in second before repolling to 
check if file size of the polled file is changing.

=item B<-s> I<minimym_file_size>

Optional minimum file size for remote files to be polled.
Default to 1 byte, or skip 0 byte size file.

=item B<history_file>

Filename containing list of previously downloaded files.

=item B<remote_hostname>

FTP host to be polled. A suitable entry in the .netrc file must exist for this 
host.

=item B<remote_directory>

Directory on the remote host (as seen in the ftp session) which will be examined
for new files.

=item B<local_pdr_directory>

Local directory to which new PDRs will be directed.

=head1 DESCRIPTION

This script polls the I<remote_polling> directory on host I<remote_hostname> 
using ftp.It compares the contents of the polling directory with entries in 
the I<history_file> file.  If a new entry that matches specified dataset 
pattern is found, it is added to a PDR in the <local_pdr_directory>. An option,
 -r, to recursively traverse the <remote_directory> can be specified. Patterns
 for mapping files to datasets is specified in the <data_type_cfg_file>. A 
 suitable entry in the .netrc file must exist for the <remote_host>.

=head1 AUTHOR

Krishna Tewari, NASA/GSFC, Code 902, Greenbelt, MD  20771.
M. Hegde, SSAI

=cut

################################################################################
# $Id: s4pa_remote_polling_without_pdr.pl,v 1.34 2019/07/10 12:54:03 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use Net::FTP;
use Net::Netrc;
use S4P;
use S4PA::Receiving;
use S4P::TimeTools;
use File::Basename;
use File::Copy;
use Getopt::Std;
use Safe;
use S4PA;
use Log::Log4perl;
use vars qw($opt_r $opt_c $opt_l $opt_m $opt_i $opt_p $opt_s);
use vars qw($oldlist);

my ( $oldlistfile, $hostname, $polldir, $destdir );

# Read and parse command line options
getopts('m:l:c:p:ris:') ;

# retrieve config values
my $dataTypeCfg = $opt_c || "../s4pa_data_type.cfg" ;
my $cpt = new Safe 'CFG';
# $cpt->share( '@DATATYPES' );
$cpt->share( '%DATATYPES' );
$cpt->rdo( $dataTypeCfg )
    || S4P::logger( 'ERROR', "Failed to read configuration file, $dataTypeCfg" );

my $logger = S4PA::CreateLogger( $CFG::cfg_logger{FILE},
    $CFG::cfg_logger{LEVEL} ) if ( %CFG::cfg_logger );

my $ignore_history = ( defined $opt_i ) ? 1 : 0;
my $minimum_size = ( defined $opt_s ) ? $opt_s : 1;
my $fileAliasPattern = {};

my $primaryFilePattern = {};
my $associateFilePattern = {};
foreach my $dataset ( keys %CFG::DATATYPES ) {
    my $fileCounts = scalar @{$CFG::DATATYPES{$dataset}};
    # assign first element in the array to be the primary file
    $primaryFilePattern->{$dataset}{FILE} = $CFG::DATATYPES{$dataset}[0]{FILE};
    $primaryFilePattern->{$dataset}{ALIAS} = $CFG::DATATYPES{$dataset}[0]{ALIAS};

    # assign reset of the elements in the array to be the associated files
    for my $i ( 1 .. $fileCounts-1 ) {
        $associateFilePattern->{$dataset}[$i-1]{FILE} = $CFG::DATATYPES{$dataset}[$i]{FILE};
        $associateFilePattern->{$dataset}[$i-1]{ALIAS} = $CFG::DATATYPES{$dataset}[$i]{ALIAS};
    }
}

$oldlistfile = shift(@ARGV) or S4P::perish( 1, "Missing oldlist_file" );
$hostname = shift(@ARGV) or S4P::perish( 1, "Missing hostname" );
$polldir = shift(@ARGV) or S4P::perish( 1, "Missing polling directory" );
$destdir = shift(@ARGV) or S4P::perish( 1, "Missing destination directory" );

my %oldlist ;
unless ($destdir =~ /\/$/) { $destdir .="/" ; }
S4P::logger( "INFO",
             "Config=$dataTypeCfg, Old List=$oldlistfile,"
             . " Polling Host=$hostname, Polling Directory=$polldir,"
             . " PDR Desination=$destdir" );


# Lock a dummy file to lock out access old list file.
open( LOCKFH, ">$oldlistfile.lock" ) 
    || S4P::perish( 1, "Failed to open lock file, $oldlistfile.lock" );
unless( flock( LOCKFH, 2 ) ) {
    close( LOCKFH );
    S4P::perish( 1, "Failed to get a lock on $oldlistfile.lock" );
}

$SIG{__DIE__} = "DieHandler";

# Read oldlist (%oldlist hash in external file of remote files already 
# processed)
if ( -f $oldlistfile ) {
    open ( OLDLIST,"$oldlistfile") 
        || S4P::perish(1 ,"Failed opening oldlist file $oldlistfile ") ;
    while ( <OLDLIST> ) {
        chomp() ;
        $oldlist{$_} = "old" ;
    }
    close( OLDLIST ) ;
}

# specify default firewall type
my $firewallType = $ENV{FTP_FIREWALL_TYPE} ? $ENV{FTP_FIREWALL_TYPE} : 1;

# Open FTP connection, login, cd to polldir and ls contents.
my $ftp;
if ( $ENV{FTP_FIREWALL} ) {
    # Create an Net::FTP object with Firewall option
    my $firewall = $ENV{FTP_FIREWALL};
    $ftp = Net::FTP->new( Host => $hostname, Timeout => 900,
        Firewall => $firewall, FirewallType => $firewallType );
} else {
    # No firewall specified, let .libnetrc resolve if firewall is required
    $ftp = Net::FTP->new( Host => $hostname, Timeout => 900 );
}


S4P::perish( 1, "Unable to login to host $hostname (" . $ftp->message . ")" )
    unless $ftp->login();
S4P::logger( "INFO", "Beginning scan of $hostname: $polldir" );
$logger->info( "Scanning $hostname:$polldir" ) if defined $logger;

# Use binary mode
$ftp->binary();

# Make sure the polling directory begins with a '/' and ends with '/'. It is 
# just for easier coding later.
$polldir .= '/' unless ( $polldir =~ /\/$/ );
unless ( $polldir =~ /^\// ) {
    my $homeDir = $ftp->pwd();
    $polldir = $homeDir . "/$polldir";
}

my @dirStack = ( $polldir );
my $fileAssociates;
my $aliasAssociates;
my %fileListInfo;
my @fileList;

# Accumulate remote files, skipping files already in history.
while ( my $dir = shift @dirStack ) {
    S4P::logger( "INFO", "Scanning $dir" );
    unless ( $ftp->cwd( $dir ) ) {
        S4P::logger( "WARN",
	    "Failed to change directory to $dir:" . $ftp->message );
        next;
    }
    # Get a long listing of directory
    my @list = $ftp->dir();
    # Switch to using "ls -lL" if dir() doesn't produce a long listing
    if ( @list > 0 ) {
        # @list = $ftp->ls( "-lL" ) unless ( $list[-1] =~ /^[d-]r/ );
        # some ftp site's directory listing came back as d--------- as directory
        @list = $ftp->ls( "-lL" ) unless ( $list[-1] =~ /^[d-][r-]/ );
    }
    # Get a listing of files first
    foreach my $item ( @list ) {
        # some ftp site's directory listing came back as d--------- as directory
        # if ( $opt_r && $item =~ /^dr[w-]x/ ) {
        if ( $opt_r && $item =~ /^d[r-][w-][x-]/ ) {
            my $subDir = (split( /\s+/, $item))[-1];
            push( @dirStack, $dir . $subDir . '/' )
                unless ( $subDir =~ /^\./ );
        } else {
	    my $file = (split( /\s+/, $item))[-1];
	    if ( defined $file && $file !~ /^\./ ) {
                my $filePathName = $dir . $file;

                my $dataType = S4PA::Receiving::IdentifyDataset(
                                 $primaryFilePattern, $filePathName ); 
                if ( $dataType ) {
                    $filePathName = $file
                        unless ( $primaryFilePattern->{$dataType}{ALIAS} );
                }

		my $fs = $ftp->size( $file );
		if ( defined $fs ) {
                    if ( $fs < $minimum_size ) {
                       S4P::logger( "INFO", "$file file size is less than $minimum_size." );
		    } elsif ( $oldlist{$filePathName} eq 'old' 
                        && $ignore_history == 0 ) {
			S4P::logger( "INFO", "$file in $dir is old" );
		    } else {
                        # Check again to make sure the file is not changing
                        my $newfs = $ftp->size( $file );
                        if ( $fs eq $newfs ) {
                            my $filePath = $dir . $file;
                            $fileListInfo{$filePath}{'size'} = $fs;
                            $logger->debug( "Found new file $file" )
                                if defined $logger;
                            push @fileList, $filePath;
                        } else {
                            S4P::logger( "INFO",
				"Size of $file is changing during polling;"
				. " $file will not be included" );
                        }
		    }    
		} else {
		    S4P::logger( "WARN",
			"Size not defined for $file in $dir" );
		}
	    }
        }
    }
}

# repolling to check if file size changed after pausing.
if ( $opt_p ) {
    S4P::logger( "INFO", "Sleep for $opt_p second before repoll." );
    $logger->info( "Sleep for $opt_p second before repoll." )
        if defined $logger;
    sleep $opt_p;
    foreach my $file ( keys %fileListInfo ) {
        my $newfs = $ftp->size( $file );
        next if ( $fileListInfo{$file}{'size'} eq $newfs );
        # file size changed, remove it from list
        delete $fileListInfo{$file};
        S4P::logger( "INFO", "Size of $file has changed since last polled;"
            . " $file will not be included" );
        $logger->debug( "$file size changed, removed it from polling list." )
            if defined $logger;
    }
}

# Quit FTP
$ftp->quit();
S4P::logger( "INFO", "End of scan" );
my $numFiles = scalar( keys %fileListInfo );
$logger->info( "Found $numFiles new files" ) if defined $logger;

foreach my $filePathName ( @fileList ) {
    my $file = basename $filePathName;
    my $dir = dirname $filePathName;
    $dir .= '/' unless ( $dir =~ /\/$/ );
    foreach my $dataset ( keys %{$primaryFilePattern} ) {
        my $pattern = $primaryFilePattern->{$dataset}{FILE};
        if ( $filePathName =~ m/$pattern/ ) {
            my ( $esdt, $version ) = split( /\./, $dataset, 2 );
            $fileListInfo{$filePathName}{'dataset'} = $esdt;
            $fileListInfo{$filePathName}{'version'} = $version;
            $fileListInfo{$filePathName}{'associate'} = 0;
            $fileListInfo{$filePathName}{'alias'} = '';
            my @associateFileList = ();
            my @associateAliasList = ();
            if (exists $associateFilePattern->{$dataset} and @{$associateFilePattern->{$dataset}}) {
                my $numAssociates = scalar @{$associateFilePattern->{$dataset}};
                for my $i ( 0 .. $numAssociates-1 ) {
                    eval("\$associateFileList[$i]=qq($associateFilePattern->{$dataset}[$i]{FILE});");
                    eval("\$associateAliasList[$i]=qq($associateFilePattern->{$dataset}[$i]{ALIAS});");
                }

                my $numAssocFileLists = scalar @associateFileList;
                if ( $numAssocFileLists ) {
                    for my $i ( 0 .. $numAssocFileLists-1 ) {
                        foreach my $listFilePath (@fileList) {
                            my $listFileName = basename $listFilePath;
                            if ( $listFileName eq $associateFileList[$i] ) {
                                push @{$fileAssociates->{$filePathName}}, $listFilePath;
                                $fileListInfo{$listFilePath}{'associate'} = 1;
                                $fileListInfo{$listFilePath}{'alias'} = 
                                    $associateAliasList[$i];
                                last;
                            }
                        }
                    }
                }

                # we can not poll this file if any of the associate file were not found
                unless ( defined $fileAssociates->{$filePathName} &&
                    (scalar @{$fileAssociates->{$filePathName}} == scalar @associateFileList) ) {
                    S4P::logger( "INFO", "Found no associated file for $filePathName" .
                        ", removed from polling list." );
                    delete $fileListInfo{$filePathName};
                }
                last;
            }
        }
    }
}
undef @fileList;
undef @dirStack;

# Create PDRs for each data type found
my $pdrHash = {};
my $fileGroupCount = {};

foreach my $file ( keys %fileListInfo ) {
    next if ($fileListInfo{$file}{'associate'});
    my $esdt = $fileListInfo{$file}{'dataset'};
    my $version = $fileListInfo{$file}{'version'};
    my $size = $fileListInfo{$file}{'size'}; 
#    my $esdt_full = $esdt . ".$version";
    my $esdt_full = ( $version ) ? $esdt . ".$version" : $esdt;

    if ( $esdt eq '' ) {
        S4P::logger( "INFO", "$file has no associated data type" );
    } else {
        # Make sure the maximum file group count is within limit when
        # specified for a given data type.
        if ( (defined $opt_m) && ($fileGroupCount->{$esdt} >= $opt_m) ) {
            # Flush out PDR once the granule count/data type is limit is reached
            my $pdrName = "$esdt." . time() . rand() . ".PDR";
            S4P::perish( 1, "Failed to write PDR ($pdrName)" )
                if $pdrHash->{$esdt}->write_pdr( $pdrName );
            move( $pdrName, $destdir ) || 
                S4P::perish( 1, "Failed to move PDR to $destdir" );
            $logger->info( "Create $pdrName and moved to $destdir" )
                if defined $logger;
            open ( OLDLIST,">>$oldlistfile") || 
                S4P::perish( 1, 
                    "Failed opening oldlist file $oldlistfile for write.");
            foreach my $file ( $pdrHash->{$esdt}->files() ) {
                if ($primaryFilePattern->{$esdt_full}{ALIAS}) {
                    next if ( $oldlist{$file} eq 'old' );
                    print OLDLIST $file, "\n" ||
                        S4P::perish( 1, "Failed to update file $oldlistfile." );
                } else {
                    next if ( $oldlist{basename($file)} eq 'old' );
                    print OLDLIST basename($file), "\n" ||
                        S4P::perish( 1, "Failed to update file $oldlistfile." );
                }
            }
            close( OLDLIST );
            # Reset the data type's counter
            delete $pdrHash->{$esdt};
            $fileGroupCount->{$esdt} = 0;
        }
        
        # Create a PDR.
        unless ( defined $pdrHash->{$esdt} ) {
            my $originatingSystem = ( defined $opt_l ) ? $opt_l : 'S4PA';
            # Add 30 days worth of seconds to $now to get expiration:
            my $expirationDate = S4P::TimeTools::CCSDSa_DateAdd(
                                        S4P::TimeTools::CCSDSa_Now, 2592000 );
            # Initialize the PDR.
            $pdrHash->{$esdt} = S4P::PDR::start_pdr( 'originating_system' 
                                                        => $originatingSystem,
                                                     'expiration_time' 
                                                        => $expirationDate );
        }

        # Create and add the file group for the data file.
        my $fileGroup = new S4P::FileGroup;
        $fileGroup->data_type( $esdt );
        $fileGroup->data_version( $version, "%s" ) if ( $version ne "" );
        $fileGroup->node_name( $hostname );      
        $fileGroup->add_file_spec( $file );      
        my ( $fileSpec ) = @{$fileGroup->file_specs()};
        $fileSpec->file_size( $size );
        $fileSpec->file_type( 'SCIENCE' );

        # If primary file has an alias specified, create alias based on pattern
        # and place 'alias' in FileSpec
        if ($primaryFilePattern->{$esdt_full}{ALIAS}) {
            my $patternString = $primaryFilePattern->{$esdt_full}{FILE};
            my $patternQuote = qr($patternString);
            my ($outFile);
            if ( $file =~ /$patternQuote/ ) {
                eval("\$outFile=qq($primaryFilePattern->{$esdt_full}{ALIAS});" );
            } else {
                S4P::perish( 1, "input file does not match specified pattern");
            }
            $fileSpec->alias($outFile);
	}

        # Add associated files as SCIENCE files if exist
        if (exists $fileAssociates->{$file} and @{$fileAssociates->{$file}}) {
            foreach my $associateFile (@{$fileAssociates->{$file}}) {
                $fileGroup->add_file_spec($associateFile, 'SCIENCE');
                my @associateFileSpec = @{$fileGroup->file_specs()};
                $associateFileSpec[-1]->file_size( $fileListInfo{$associateFile}{'size'} );
                if ( $fileListInfo{$associateFile}{'alias'} ) {
                    my $patternString = $primaryFilePattern->{$esdt_full}{FILE};
                    my $patternQuote = qr($patternString);
                    my ( $outFile );
                    if ( $file =~ /$patternQuote/ ) {
                        eval("\$outFile=qq($fileListInfo{$associateFile}{'alias'});" );
                    } else {
                        S4P::perish( 1, "input file does not match specified pattern");
                    }
                    $associateFileSpec[-1]->alias( $outFile );
                }
            }
        }

        $pdrHash->{$esdt}->add_file_group( $fileGroup );
        $fileGroupCount->{$esdt}++;
        $logger->info( "Added $file to fileGroup list" )
            if defined $logger;
    }     
}

# Write out remaining PDRs and update the file containing polled file names.
foreach my $esdt ( keys %$pdrHash ) {
    my $pdrName = "$esdt." . time() . rand() . ".PDR";
    S4P::perish( 1, "Failed to write PDR ($pdrName)" )
        if $pdrHash->{$esdt}->write_pdr( $pdrName );
    move( $pdrName, $destdir )
        || S4P::perish( 1, "Failed to move PDR to $destdir" );
    $logger->info( "Create $pdrName and moved to $destdir" )
        if defined $logger;
    open ( OLDLIST,">>$oldlistfile") 
        || S4P::perish( 1, "Failed opening oldlist file $oldlistfile for write.");
    foreach my $fg ( @{$pdrHash->{$esdt}->file_groups()} ) {
        my $dataVersion = $fg->data_version();
        $dataVersion = '' if ($dataVersion eq '000');
#        my $esdt_full = $esdt . ".$dataVersion";
        my $esdt_full = ( $dataVersion ) ? $esdt . ".$dataVersion" : $esdt;
        foreach my $file ( $fg->data_files() ) {
            if ($primaryFilePattern->{$esdt_full}{ALIAS}) {
                next if ( $oldlist{$file} eq 'old' );
                print OLDLIST $file, "\n"
                    || S4P::perish( 1,"Failed writing oldlist file $oldlistfile." );
            } else {
                next if ( $oldlist{basename($file)} eq 'old' );
                print OLDLIST basename($file), "\n"
                    || S4P::perish( 1,"Failed writing oldlist file $oldlistfile." );
            }
        }
    }
    close( OLDLIST );
}
# Close and release file locks
close( LOCKFH ) ;
flock( LOCKFH, 8 );
exit( 0 );

################################################################################
# Die handler for cleanup
sub DieHandler
{
    close( LOCKFH );
    flock( LOCKFH, 8 );
}

