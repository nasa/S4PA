#!/usr/bin/perl

=head1 NAME

s4pa_remote_polling_data.pl - script to poll a remote directory for data

=head1 SYNOPSIS

s4pa_remote_polling_data.pl
[B<-c> I<data_type_cfg_file>]
[B<-u> I<polling_url>]
[B<-h> I<remote_host>]
[B<-g> I<remote_directory>]
[B<-t> I<protocol>]
[B<-d> I<depth>]
[B<-e> I<external_api>]
[B<-s> I<Originating_system>]
[B<-m> I<max_file_group>]
[B<-p> I<repolling_pause_in_second>]
[B<-f> I<minimum_file_size>]
[B<-o> I<polling_history_file>]
[B<-l> I<local_pdr_directory>]
[B<-i>]
[B<-r>]

=head1 ARGUMENTS

=over

=item B<-c> I<data_type_cfg_file>

Configuration file that supplies data type key word / data type cross reference.
Used to identify the data type.

=item B<-u> I<polling_url>

An URL for the polling protocol, hostname, and base directory
in one of the following format:
http://hostname.domain/target_directory
ftp://hostname.domain/target_directory
file://hostname.domain/target_directory

=item B<-h> I<remote_host>

Remote hostname where data will be polling from.

=item B<-g> I<remote_directory>

Remote base direcotry on the remote host where data will be polling from.

=item B<-t> I<protocol>

Polling protocol. Default to FTP.

=item B<-d> I<depth>

Optional directory depth for HTTP and FILE polling. Default to 5.

=item B<-e> I<external_api>

Optional external script to generate polling list for HTTP polling.

=item B<-s> I<Originating system>

The name of originating system to be used in resulting PDR.

=item B<-m> I<max_file_group>

Optional max file group count in a PDR (per data type) for a scan attempt.

=item B<-i> I<Ignore history checking>

Optional swith to indicate skipping history record chcking.
If specified, all existing data will be polled again.

=item B<-p> I<repolling_pause_in_second>

Optional sleep time in second before repolling to 
check if file size of the polled file is changing.

=item B<-r> 

Optional recursive ftp polling flag.

=item B<-f> I<minimum_file_size>

Optional minimum file size for remote files to be polled.
Default to 1 byte, or skip 0 byte size file.

=item B<-o> I<history_file>

Filename containing list of previously downloaded files.

=item B<-l> I<local_pdr_directory>

Local directory to which new PDRs will be directed.

=item B<-x> I<port>

SFTP special port number to be used.

=item B<remote_url>

HTTP host to be polled. A suitable entry in the .netrc file must exist for this 
host if it need user authentication.

=head1 DESCRIPTION

This script polls the I<remote_directory> directory on host I<remote_host> 
using either FTP, SFTP, FILE, or HTTP protocol. It compares the contents of the 
polling directory with entries in the I<history_file> file.  
If a new entry that matches specified dataset 
pattern is found, it is added to a PDR in the <local_pdr_directory>. An option,
-r, to recursively traverse the <remote_directory> can be specified for ftp polling.
Patterns for mapping files to datasets is specified in the configuration file.
A suitable entry in the .netrc file must exist for the <remote_host> if user 
authentication is required.

=head1 AUTHOR

Guang-Dih Lei, AdNET, Code 910.2, Greenbelt, MD  20771.

=cut

################################################################################
# s4pa_remote_polling_data.pl,v 1.4 2009/10/25 00:15:15 glei Exp
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use Net::Netrc;
use Net::FTP;
use Net::SFTP::Foreign;
use S4P;
use S4P::TimeTools;
use S4PA::Receiving;
use File::Basename;
use File::Copy;
use File::stat;
use Getopt::Std;
use Safe;
use S4PA;
use Log::Log4perl;
use URI::URL        qw(url);
use HTML::Entities  ();
use HTTP::Request;
use LWP::UserAgent;
use vars qw( $opt_r $opt_d $opt_c $opt_p $opt_l $opt_h $opt_g
    $opt_m $opt_i $opt_s $opt_e $opt_t $opt_f $opt_o $opt_u $opt_x );

# Read and parse command line options
getopts('f:g:h:m:l:c:p:d:s:t:e:o:u:x:ir') ;

usage() unless ( defined $opt_c );
my $cfgFile = $opt_c;
# retrieve config values
my $cpt = new Safe 'CFG';
$cpt->rdo( $cfgFile )
    || S4P::logger( 'ERROR', "Failed to read configuration file, $cfgFile" );

my $logger = S4PA::CreateLogger( $CFG::cfg_logger{FILE},
    $CFG::cfg_logger{LEVEL} ) if ( %CFG::cfg_logger );

# Command line options override the definitions in configuration file
my $protocol = ( defined $opt_t ) ? $opt_t :
    ( defined $CFG::cfg_protocol ) ? $CFG::cfg_protocol : 'FTP';
my $port = ( defined $opt_x ) ? $opt_x :
    ( defined $CFG::cfg_port ) ? $CFG::cfg_port : '';
my $max_fg_count = ( defined $opt_m ) ? $opt_m :
    ( defined $CFG::cfg_max_fg ) ? $CFG::cfg_max_fg : undef;
my $repoll_pause = ( defined $opt_p ) ? $opt_p :
    ( defined $CFG::cfg_repoll_pause ) ? $CFG::cfg_repoll_pause : undef;
my $originatingSystem = ( defined $opt_s ) ? $opt_s : 
    ( defined $CFG::cfg_originator ) ? $CFG::cfg_originator : 'S4PA';
my $ignore_history = ( defined $opt_i ) ? 1:
    ( defined $CFG::cfg_ignore_history && $CFG::cfg_ignore_history eq 'true' ) ? 1 : 0;
my $minimum_size = ( defined $opt_f ) ? $opt_f :
    ( defined $CFG::cfg_min_size ) ? $CFG::cfg_min_size : 1;
my $RECURSIVE = ( defined $opt_r ) ? 1 :
    ( defined $CFG::cfg_recursive && $CFG::cfg_recursive eq 'true' ) ? 1 : 0;
my $MAX_DEPTH = ( defined $opt_d ) ? $opt_d :
    ( defined $CFG::cfg_max_depth ) ? $CFG::cfg_max_depth : 5;
my $external_api = ( defined $opt_e ) ? $opt_e :
    ( defined $CFG::cfg_external_api ) ? $CFG::cfg_external_api : undef;
my $remote_host = ( defined $opt_h ) ? $opt_h :
    ( defined $CFG::cfg_remote_host ) ? $CFG::cfg_remote_host : undef;
my $remote_dir = ( defined $opt_g ) ? $opt_g :
    ( defined $CFG::cfg_remote_dir ) ? $CFG::cfg_remote_dir : undef;
$remote_dir = "/$remote_dir" unless ( $remote_dir =~ /^\// );
my $oldlistfile = ( defined $opt_o ) ? $opt_o :
    ( defined $CFG::cfg_history_file ) ? $CFG::cfg_history_file :
    S4P::perish( 1, "Missing polled history file" );
my $destdir = ( defined $opt_l ) ? $opt_l :
    ( defined $CFG::cfg_local_dir ) ? $CFG::cfg_local_dir :
    S4P::perish( 1, "Missing destination directory" );
$destdir .= "/" unless ( $destdir =~ /\/$/ );

# construct starting url from protocol, remote_host, and remote_dir
my $starturl = ( defined $opt_u ) ? $opt_u :
    ( defined $CFG::cfg_polling_url ) ? $CFG::cfg_polling_url :
    ( $protocol eq 'FTP' ) ? "ftp://$remote_host" . "$remote_dir" :
    ( $protocol eq 'FILE' ) ? "file://$remote_host" . "$remote_dir" :
    ( $protocol eq 'HTTP' ) ? "http://$remote_host" . "$remote_dir" :
    ( $protocol eq 'SFTP' ) ? "sftp://$remote_host" . "$remote_dir" :
    S4P::perish( 1, "Starting URL not defined" );

# build up alias and associate hash
my $fileAliasPattern = {};
my $primaryFilePattern = {};
my $associateFilePattern = {};
foreach my $dataset ( keys %CFG::DATATYPES ) {
    my $fileCounts = scalar @{$CFG::DATATYPES{$dataset}};
    # assign first element in the array to be the primary file
    $primaryFilePattern->{$dataset}{FILE} = $CFG::DATATYPES{$dataset}[0]{FILE};
    $primaryFilePattern->{$dataset}{ALIAS} = $CFG::DATATYPES{$dataset}[0]{ALIAS};

    # assign rest of the elements in the array to be the associated files
    for my $i ( 1 .. $fileCounts-1 ) {
        $associateFilePattern->{$dataset}[$i-1]{FILE} = $CFG::DATATYPES{$dataset}[$i]{FILE};
        $associateFilePattern->{$dataset}[$i-1]{ALIAS} = $CFG::DATATYPES{$dataset}[$i]{ALIAS};
    }
}

my %oldlist;
# Lock a dummy file to lock out access old list file.
open( LOCKFH, ">$oldlistfile.lock" ) 
    || S4P::perish( 1, "Failed to open lock file, $oldlistfile.lock" );
unless( flock( LOCKFH, 2 ) ) {
    close( LOCKFH );
    S4P::perish( 1, "Failed to get a lock on $oldlistfile.lock" );
}

# Read oldlist (%oldlist hash in external file of remote files already processed)
if ( -f $oldlistfile ) {
    open ( OLDLIST,"$oldlistfile") 
        || S4P::perish(1 ,"Failed opening oldlist file $oldlistfile ") ;
    while ( <OLDLIST> ) {
        chomp() ;
        $oldlist{$_} = "old" ;
    }
    close( OLDLIST ) ;
}

$starturl =~ m#^((https?|ftp|file|sftp)://([^/]+))(/.*)$#;
my $hostname = $3 or S4P::perish( 1, "Starting URL missing hostname" );
my $baseurl  = $1 or S4P::perish( 1, "Starting URL missing base url" );
my $basedir  = $4 or S4P::perish( 1, "Starting URL missing pathname" );
S4P::logger( "INFO",
    "Config=$cfgFile, Old List=$oldlistfile," .
    " PDR Desination=$destdir," . " Polling URL=$starturl" );

# Look up hostname in .netrc
my ( $login, $passwd, $AUTH );
my $machine = Net::Netrc->lookup( $hostname );
if ( defined $machine ) {
    # Get the login name for the host
    $login = $machine->login();
    if ( defined $login ) {
        $passwd = $machine->password();
        if ( defined $passwd ) {
            S4P::logger( "INFO", "Found login info for $hostname" );
            $AUTH = "$login:$passwd";
        }
    } else {
        S4P::logger( "WARN", "Failed to find login info for $hostname from .netrc" );
    }
}

my @pollList;
my $ftp;           # keep ftp session open
my $ssh;           # ssh2 session open
my $sftp;          # sftp session open
my %seen = ();     # mapping from URL => local_file
my $userAgent = new LWP::UserAgent;
$userAgent->agent("S4PA/3.37");
$userAgent->env_proxy;

# polling hash to store common parameter
my $pollParameters = {
    PROTOCOL => $protocol,
    HOST => $hostname,
    BASEURL => $baseurl,
    BASEDIR => $basedir
};

if ($protocol eq 'FTP') {
    $ftp = ftp_connect($hostname);
    $pollParameters->{SESSION} = $ftp;
    @pollList = ftp_poll($pollParameters);

} elsif ($protocol eq 'FILE') {
    @pollList = file_poll($basedir, $pollParameters);

} elsif ($protocol eq 'HTTP') {
    if (defined $external_api) {
        @pollList = `$external_api`;
    } else {
        my $filename = http_poll($starturl, $starturl);
    }
} elsif ($protocol eq 'SFTP') {
    if (defined $external_api) {
        @pollList = `$external_api`;
    } else {
        $sftp = S4PA::Receiving::SftpConnect($hostname);
        $pollParameters->{SESSION} = $sftp;
        @pollList = sftp_poll($pollParameters);
    }
}

my %fileListInfo;     # hash for all polled files
my $fileAssociates;   # hash for associate file
my $aliasAssociates;

# Accumulate remote files, skipping files already in history.
foreach my $record ( @pollList ) {
    chomp $record;
    # each record should contain a url and file size separated by '|'
    my ( $url, $fs ) = split( /\|/, $record, 2 );
    # extract full path in url
    $url =~ m#^(https?|ftp|file|sftp)://([^/]+)/(.*)$#;
    my $filePathName = $3;
    $filePathName = "/$filePathName" unless ( $filePathName =~ /^\// );
    my $file = basename( $filePathName );

    # identify dataset name and alias for primary file
    my $dataType = S4PA::Receiving::IdentifyDataset(
        $primaryFilePattern, $filePathName );
    my $oldFile = $filePathName;
    if ( $dataType ) {
        $oldFile = $file
            unless ( $primaryFilePattern->{$dataType}{ALIAS} );
    }

    # get file size if not defined
    unless ( defined $fs ) {
        if ( $protocol eq 'HTTP' ) {
            # Fetch header
            my $hreq = HTTP::Request->new( HEAD => $url );
            $hreq->authorization_basic(split (/:/, $AUTH))
                if ( defined $AUTH );
            my $hres = $userAgent->request( $hreq );
            $fs = $hres->content_length or
                S4P::perish( 1, "No file size defined for $url" );
        } else {
            S4P::perish( 1, "No file size defined for $url" );
        }
    }

    # check file size requirement and polling history
    if ( $fs < $minimum_size ) {
        S4P::logger( "INFO", "$filePathName file size is less than $minimum_size." );
    } elsif ( $oldlist{$oldFile} eq 'old' && $ignore_history == 0 ) {
        S4P::logger( "INFO", "$filePathName is old" );
    } else {
        $fileListInfo{$filePathName}{'size'} = $fs;
        $fileListInfo{$filePathName}{'url'} = $url;
        $logger->debug( "Found new file $filePathName" ) if defined $logger;
    }
}

# repolling to check if file size changed after pausing.
if ( $repoll_pause ) {
    S4P::logger( "INFO", "Sleep for $repoll_pause second before repoll." );
    $logger->info( "Sleep for $repoll_pause second before repoll." )
        if defined $logger;
    sleep $repoll_pause;

    # make sure the ftp connection is still active after waking up
    if ( $protocol eq 'FTP' ) {
        unless ($ftp->pwd()) {
            $ftp = ftp_connect( $hostname );
            $pollParameters->{SESSION} = $ftp;
        }
    # make sure the ssh connection is still active after waking up
    } elsif ($protocol eq 'SFTP') {
        if ($sftp->error) {
            $sftp = S4PA::Receiving::SftpConnect($hostname);
            $pollParameters->{SESSION} = $sftp;
        }
    }

    foreach my $file ( keys %fileListInfo ) {
        my $newfs = get_size( $file, $pollParameters );
        if (defined $newfs) {
            next if ( $fileListInfo{$file}{'size'} eq $newfs );
        } else {
            S4P::logger("WARN", "$file file size can't be resolved, skip checking");
            next;
        }

        # file size changed, remove it from list
        delete $fileListInfo{$file};
        S4P::logger( "INFO", "Size of $file has changed since last polled;"
            . " $file will not be included" );
        $logger->debug( "$file size changed, removed it from polling list." )
            if defined $logger;
    }
}

if ($protocol eq 'FTP') {
    $ftp->quit();
} elsif (($protocol eq 'SFTP') && (defined $ssh)) {
    $ssh->disconnect();
} elsif (($protocol eq 'SFTP') && (defined $sftp)) {
    $sftp->disconnect();
}
S4P::logger( "INFO", "End of scan" );
my $numFiles = scalar( keys %fileListInfo );
$logger->info( "Found $numFiles new files" ) if defined $logger;

foreach my $file ( keys %fileListInfo ) {
    foreach my $dataset ( keys %{$primaryFilePattern} ) {
        my $pattern = $primaryFilePattern->{$dataset}{FILE};
        if ( $file =~ m/$pattern/ ) {
            my ( $esdt, $version ) = split( /\./, $dataset, 2 );
            $fileListInfo{$file}{'dataset'} = $esdt;
            $fileListInfo{$file}{'version'} = $version;
            $fileListInfo{$file}{'associate'} = 0;
            $fileListInfo{$file}{'alias'} = '';
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
                        foreach my $listFilePath ( keys %fileListInfo ) {
                            my $listFileName = basename( $listFilePath );
                            if ( $listFileName eq $associateFileList[$i] ) {
                                push @{$fileAssociates->{$file}}, $listFilePath;
                                $fileListInfo{$listFilePath}{'associate'} = 1;
                                $fileListInfo{$listFilePath}{'alias'} = 
                                    $associateAliasList[$i];
                                last;
                            }
                        }
                    }
                }

                # we can not poll this file if any of the associate file were not found
                unless ( defined $fileAssociates->{$file} &&
                    (scalar @{$fileAssociates->{$file}} == scalar @associateFileList) ) {
                    S4P::logger( "INFO", "Found no associated file for $file" .
                        ", removed from polling list." );
                    delete $fileListInfo{$file};
                }
                last;
            }
        }
    }
}

# Create PDRs for each data type found
my $pdrHash = {};
my $fileGroupCount = {};

foreach my $file ( sort keys %fileListInfo ) {
    next if ( $fileListInfo{$file}{'associate'} );
    my $esdt = $fileListInfo{$file}{'dataset'};
    my $version = $fileListInfo{$file}{'version'};
    my $size = $fileListInfo{$file}{'size'}; 
    my $url = $fileListInfo{$file}{'url'}; 
    my $esdt_full = ( $version ) ? $esdt . ".$version" : $esdt;

    if ( $esdt eq '' ) {
        S4P::logger( "INFO", "$file has no associated data type" );
        next;
    } else {
        # Make sure the maximum file group count is within limit when
        # specified for a given data type.
        if ( ( defined $max_fg_count ) 
            && ( $fileGroupCount->{$esdt} >= $max_fg_count ) ) {
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
            foreach my $pdrFilePath ( $pdrHash->{$esdt}->files() ) {
                if ($primaryFilePattern->{$esdt_full}{ALIAS}) {
                    next if ( $oldlist{$pdrFilePath} eq 'old' );
                    print OLDLIST $pdrFilePath, "\n";
                } else {
                    my $pdrFile = basename( $pdrFilePath );
                    next if ( $oldlist{$pdrFile} eq 'old' );
                    print OLDLIST $pdrFile, "\n";
                }
            }
            close( OLDLIST ) 
                || S4P::perish( 1, "Failed to update file $oldlistfile." );
            # Reset the data type's counter
            delete $pdrHash->{$esdt};
            $fileGroupCount->{$esdt} = 0;
        }
        
        # Create a PDR.
        unless ( defined $pdrHash->{$esdt} ) {
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
        $url =~ m#^(https?|ftp|file|sftp)://([^/]+)(/.*)$#;
        my $nodeName = $2;
        my $fileGroup = new S4P::FileGroup;
        $fileGroup->data_type( $esdt );
        $fileGroup->data_version( $version, "%s" ) if ( $version ne "" );
        $fileGroup->node_name( $nodeName );      
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
                if ( $associateFile =~ /\.xml$|\.met$/ ) {
                    $fileGroup->add_file_spec($associateFile, 'METADATA');
                } else {
                    $fileGroup->add_file_spec($associateFile, 'SCIENCE');
                }
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
        my $esdt_full = ( $dataVersion ) ? $esdt . ".$dataVersion" : $esdt;
        foreach my $fgFilePath ( $fg->data_files(), $fg->met_file(), $fg->browse_file() ) {
            next unless ( $fgFilePath );
            if ($primaryFilePattern->{$esdt_full}{ALIAS}) {
                next if ( $oldlist{$fgFilePath} eq 'old' );
                print OLDLIST $fgFilePath, "\n";
            } else {
                my $fgFile = basename( $fgFilePath );
                next if ( $oldlist{$fgFile} eq 'old' );
                print OLDLIST $fgFile, "\n";
            }
        }
    }
    close( OLDLIST ) || S4P::perish( 1,"Failed writing oldlist file $oldlistfile." );
}
# Close and release file locks
close( LOCKFH ) ;
flock( LOCKFH, 8 );
exit( 0 );

sub http_poll
{
    my( $url, $BASEURL, $type, $depth ) = @_;
 
    # Fix http://sitename.com/../blah/blah.html to
    #     http://sitename.com/blah/blah.html
    $url = $url->as_string if ( ref($url) );
    while ( $url =~ s#(https?://[^/]+/)\.\.\/#$1# ) {}
    
    $url = url( $url );
    $type  ||= 'a';
    # Might be the background attribute
    $type = 'img' if ( $type eq 'body' || $type eq 'td' );
    $depth ||= 0;

    # skip mailto things
    if ( $url->scheme eq 'mailto' ) {
        return $url->as_string;
    }

    # The $plain_url is a URL without the fragment part
    my $plain_url = $url->clone;
    $plain_url->frag( undef );

    # Check base url, but not for <IMG ...> links
    if ( $type ne 'img' and  $url->as_string !~ /^\Q$BASEURL/o ) {
        return $url->as_string;
    }

    # If we already have it, then there is nothing to be done
    my $seen = $seen{$plain_url->as_string};
    return $seen if ( $seen );

    # Too much or too deep
    if ( $depth > $MAX_DEPTH and $type ne 'img' ) {
        return $url;
    }

    # Fetch header
    my $hreq = HTTP::Request->new( HEAD => $url );
    $hreq->authorization_basic(split (/:/, $AUTH)) if ( defined $AUTH );
    my $hres = $userAgent->request( $hreq );
    my $ct = $hres->content_type;
    if ( $ct !~ m#text/html# ) {
        my $size = $hres->content_length;
        push @pollList, "$url|$size";
        return $url;
    }

    # Fetch document
    my $req = HTTP::Request->new( GET => $url );
    $req->authorization_basic(split (/:/, $AUTH)) if ( defined $AUTH );
    my $res = $userAgent->request( $req );

    # Check outcome
    if ($res->is_success) {
        my $doc = $res->content;
        my $ct = $res->content_type;
        $seen{$plain_url->as_string} = $url;

        # If the file is HTML, then we look for internal links
        if ($ct eq "text/html") {
            # Save an unprosessed version of the HTML document.  This
            # both reserves the name used, and it also ensures that we
            # don't loose everything if this program is killed before
            # we finish.
            my $base = $res->base;

            # Follow and substitute links...
            $doc =~
                s/
                  (
                    <(img|a|body|area|frame|td)\b   # some interesting tag
                    [^>]+                           # still inside tag
                    \b(?:src|href|background)       # some link attribute
                    \s*=\s*                         # =
                  )
                    (?:                             # scope of OR-ing
                         (")([^"]*)"    |           # value in double quotes OR
                         (')([^']*)'    |           # value in single quotes OR
                            ([^\s>]+)               # quoteless value
                    )
                /       
                  new_link($1, lc($2), $3||$5, HTML::Entities::decode($4||$6||$7),
                           $base, $url, $depth+1)
                /giex;
           # The regular expression above is not strictly correct.
           # It is not really possible to parse HTML with a single
           # regular expression, but it is faster.  Tags that might
           # confuse us include:
           #    <a alt="href" href=link.html>
           #    <a alt=">" href="link.html">
           #
        }
        return $url;
    }       
    else {
        $seen{$plain_url->as_string} = $url->as_string;
        return $url->as_string;
    }
}   
    
sub new_link
{       
    my( $pre, $type, $quote, $url, $base, $localbase, $depth ) = @_;
    $url = http_poll(url($url, $base)->abs, $base, $type, $depth);
    $url = url("file:$url", "file:$localbase")->rel
        unless $url =~ /^[.+\-\w]+:/;
    return $pre . $quote . $url . $quote;
}           

sub file_poll
{
    my ( $dirName, $pollParameters, $depth ) = @_;

    $depth ||= 0;
    my $basedir = $pollParameters->{BASEDIR};
    my $baseurl = $pollParameters->{BASEURL};
    my @urlList;

    if ( $dirName !~ /^\Q$basedir/o || $depth >= $MAX_DEPTH ) {
        # return $dirName;
        return;
    }

    $dirName .= '/' unless ( $dirName =~ /\/$/ );
    my @contentList = ();
    if ( opendir( FH, $dirName ) ) {
        @contentList = map{ "$dirName$_" } grep( !/^\.+$/, readdir( FH ) );
        close( FH );
        foreach my $entry ( @contentList ) {
            if ( -d $entry ) {
                my @dirList = file_poll( $entry, $pollParameters, $depth+1 );
                push @urlList, @dirList;
            } elsif ( -f $entry ) {
                my $fs = stat( $entry );
                push @urlList, $baseurl . $entry . "|" . $fs->size();
            }
        }
    } else {
        die "Failed to open $dirName for reading ($!)";
    }
    return @urlList;
}

sub ftp_connect
{
    my $hostname = shift;

    # specify default firewall type
    my $firewallType = $ENV{FTP_FIREWALL_TYPE} ? $ENV{FTP_FIREWALL_TYPE} : 1;
    my $ftpPassive = defined $ENV{FTP_PASSIVE} ? $ENV{FTP_PASSIVE} : 1;

    # Open FTP connection, login, cd to polldir and ls contents.
    my $ftp;
    if ( $ENV{FTP_FIREWALL} ) {
        # Create an Net::FTP object with Firewall option
        my $firewall = $ENV{FTP_FIREWALL};
        if ( defined $ENV{FTP_PASSIVE} ) {
            $ftp = Net::FTP->new( Host => $hostname, Timeout => 900,
                Firewall => $firewall, FirewallType => $firewallType,
                Passive => $ftpPassive );
        } else {
            $ftp = Net::FTP->new( Host => $hostname, Timeout => 900,
                Firewall => $firewall, FirewallType => $firewallType );
        }
    } else {
        # No firewall specified, let .libnetrc resolve if firewall is required
        if ( defined $ENV{FTP_PASSIVE} ) {
            $ftp = Net::FTP->new( Host => $hostname, Timeout => 900,
                Passive => $ftpPassive );
        } else {
            $ftp = Net::FTP->new( Host => $hostname, Timeout => 900 );
        }
    }

    S4P::perish( 1, "Unable to login to host $hostname (" . $ftp->message . ")" )
        unless $ftp->login();
    return $ftp;
}

sub ftp_poll
{
    my $pollParameters = shift;

    # Open FTP connection, login, cd to polldir and ls contents.
    # my $ftp = ftp_connect( $hostname );
    my $hostname = $pollParameters->{HOST};
    my $polldir = $pollParameters->{BASEDIR};
    my $baseurl = $pollParameters->{BASEURL};
    my $ftp = $pollParameters->{SESSION};
    my @urlList;
 
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

    # building a qualified sub-directory list
    my ( $subDirPattern, $latency, $qualifiedDir );
    if ( defined $CFG::cfg_sub_dir_pattern ) {
        $subDirPattern = $CFG::cfg_sub_dir_pattern;
        $latency = $CFG::cfg_latency || 31;
        $qualifiedDir = build_sub_dir_list( $polldir, $subDirPattern, $latency );
    }

    my @dirStack = ( $polldir );
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
            if ( $RECURSIVE && $item =~ /^d[r-][w-][x-]/ ) {
                my $subDir = (split( /\s+/, $item))[-1];
                # implement subdirectory pattern
                my $subPath = $dir . $subDir . '/';
                if ( defined $subDirPattern ) {
                    push( @dirStack, $subPath ) if ( exists $qualifiedDir->{$subPath} );
                } else {
                    push( @dirStack, $subPath ) unless ( $subDir =~ /^\./ );
                }
            } else {
                # skip directory if not recursive
                next if ( $item =~ /^d/ );
                my $file = (split( /\s+/, $item))[-1];
                if ( defined $file && $file !~ /^\./ ) {
                    my $filePath = $dir . $file;
                    my $fs = $ftp->size( $file );
                    if ( defined $fs ) {
                        push @urlList, $baseurl . $filePath . '|' . $fs;
                    } else {
                        S4P::logger( "WARN",
                            "Size not defined for $file in $dir" );
                    }
                }
            }
        }
    }
    return @urlList;
}

sub sftp_poll
{
    my $pollParameters = shift;

    my $hostname = $pollParameters->{HOST};
    my $polldir = $pollParameters->{BASEDIR};
    my $baseurl = $pollParameters->{BASEURL};
    my $sftp = $pollParameters->{SESSION};
 
    S4P::logger("INFO", "Beginning scan of $hostname: $polldir");
    $logger->info("Scanning $hostname:$polldir") if defined $logger;

    # building a qualified sub-directory list
    my ($subDirPattern, $latency, $qualifiedDir, @paths);
    if (defined $CFG::cfg_sub_dir_pattern) {
        $subDirPattern = $CFG::cfg_sub_dir_pattern;
        $latency = $CFG::cfg_latency || 31;
        $qualifiedDir = build_sub_dir_list($polldir, $subDirPattern, $latency);
        foreach my $dir (keys %{$qualifiedDir}) {
            push @paths, $dir;
        }
    }

    my @urlList;
    if ($RECURSIVE) {
        my @lists;
        if (defined $subDirPattern) {
            # recursive listing under qualified directories only
            @lists = $sftp->find(\@paths, no_wanted => qr/^\./, follow_links => 1);
        } else {
            # recursive listing without hidden files
            @lists = $sftp->find($polldir, no_wanted => qr/^\./, follow_links => 1);
        }
        if ($sftp->error) {
            S4P::logger('ERROR', "Failed to poll $polldir directorys.");
            return undef;
        }

        foreach my $entry (@lists) {
            # directory has no 'longname', skip it
            next unless (exists $entry->{longname});
            # filename has the full path from a recursive search
            my $filename = $entry->{filename};
            my $size = $entry->{a}{size};
    
            # format url
            my $url = "${baseurl}${filename}";
            push @urlList, "$url|$size";
        }

    } else {
        my $lists;
        # directory listing without hidden files
        $lists = $sftp->ls($polldir, no_wanted => qr/^\./, follow_links => 1);
        if ($sftp->error) {
            S4P::logger('ERROR', "Failed to poll $polldir directorys.");
            return undef;
        }

        foreach my $entry (@{$lists}) {
            # skip directory which has longname start with 'd'
            my $longname = $entry->{longname};
            next if ($longname =~ /^d/);

            # filename from single directory list has no path
            my $filename = $entry->{filename};
            my $size = $entry->{a}{size};
    
            # format url
            my $url = "${baseurl}${polldir}/$filename";
            push @urlList, "$url|$size";
        }
    }

    return @urlList;
}

sub get_size
{
    my ( $filePath, $pollParameters ) = @_;

    my $protocol = $pollParameters->{PROTOCOL};
    my $fileSize;
    if ( $protocol eq 'HTTP' ) {
        my $url = $pollParameters->{BASEURL} . $filePath;
        my $hreq = HTTP::Request->new( HEAD => $url );
        $hreq->authorization_basic(split (/:/, $AUTH)) 
            if ( defined $AUTH );
        my $hres = $userAgent->request( $hreq );
        $fileSize = $hres->content_length;
    } elsif ( $protocol eq 'FILE' ) {
        my $fs = stat( $filePath ) if ( -f $filePath );
        $fileSize = $fs->size() if ( defined $fs );
    } elsif ( $protocol eq 'FTP' ) {
        my $ftp = $pollParameters->{SESSION};
        $fileSize = $ftp->size( $filePath ); 
    } elsif ($protocol eq 'SFTP') {
         my $stat;
         my $sftp = $pollParameters->{SESSION};

         # check if the remote path is a symlink
         my $realPath = $sftp->readlink($filePath);
         if (defined $realPath) {
             $stat = $sftp->stat($realPath);
         } else {
             $stat = $sftp->stat($filePath);
         }

         if ($sftp->error) {
             # target file can't be open, return undef
             return undef;
         } else {
             $fileSize = $stat->{size};
         }
    }
    return $fileSize;
}

sub build_sub_dir_list
{
    my ( $rootDir, $subDirPattern, $latency ) = @_;

    my $qualifiedDir = {};
    $rootDir .= '/' unless ($rootDir =~ /\/$/);
    for ( my $i = 0; $i <= $latency; $i++ ) {
        my $date = `date --date=${i}-day-ago +$subDirPattern`;
        chomp( $date );
        my $currentDir = "${rootDir}${date}/";
        $qualifiedDir->{$currentDir} = 1;
    }
    return $qualifiedDir;
}

sub usage {
  die << "EOF";
Usage: $0 <-c configuration_file> [options]
       Options are:
          -o history_file         polling history file
          -t protocol             polling protocol, FTP, HTTP, or FILE, SFTP
          -h remote_host          polling hostname
          -g remote_directory     polling base directory
          -u polling_url          complete URL for polling
          -e external_api         external API script for HTTP/SFTP polling
          -s originating_system   PDR's ORIGINATING_SYSTEM
          -m max_file_group       maximum number of filegroup per PDR
          -p pause_in_second      pause before second polling for size
          -f minimum_file_size    minimum file size for qualified poll
          -l local_pdr_directory  downstream PDR directory
          -d max_depth            maximum directory depth for HTTP and FILE polling
          -r                      recursive down base directory for FTP/SFTP polling
          -i                      ignore polling history

EOF
}

