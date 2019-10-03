#!/usr/bin/perl

=head1 NAME

s4pa_publish_user.pl - script of publication for user

=head1 SYNOPSIS

s4pa_publish_user.pl 
[B<-a> I<action>]
[B<-c> I<config_file>]
[B<-p> I<pdrdir>]
[B<-v> I<verbose>]

=head1 DESCRIPTION

s4pa_publish_user.pl requires an action switch <-a> of either 'Ingest' or 'Delete',
a configuration file switch <-c> of usually '../s4pa_publish_user.cfg', and a PDR pending
directory swith <-p> of usually either '../pending_publish' or '../pending_delete'.
It extracts temporal information from metadata file into the proper csv format 
ingest/deletion information files and copy them to a local staging directory 
for user to fetch. 

=head1 ARGUMENTS

=over 4

=item B<-a>

action: Ingest or Delete.

=item B<-c>

configuration file. It should contain the following variables:
    $UNRESTRICTED_ROOTURL='ftp://<server_full_name>/data/s4pa/';
    $RESTRICTED_ROOTURL='http://<server_full_name>/data/s4pa/';
    $cfg_interval=86400;
    $cfg_retention=604800;
    %cfg_datasets=(
        "<Shortname>" => {
            "<VersionID>" => {
                "BOUNDINGBOX" => "false",
                "DESTDIR" => "<local_staging_directory>"
            }
        },
    );


=item B<-p>

pending PDR and wo directory. For S4PA system, it is usually at
    ../pending_publish, or ../pending_delete

=item B<-v>

Verbose.

=back

=head1 AUTHORS

Guang-Dih Lei (guang-dih.lei@gsfc.nasa.gov)

=cut

################################################################################
# s4pa_publish_user.pl,v 1.28 2010/12/14 15:55:38 glei Exp
# -@@@ S4PA, Version $Name:  $
###############################################################################
#
# name: s4pa_publish_user.pl
# revised: 06/05/2014 glei initial release
#

use strict ;
use Getopt::Std ;
use S4P::PDR ;
use Safe ;
use XML::LibXSLT ;
use XML::LibXML ;
use File::Basename ;
use File::Copy;
use File::Path;
use File::stat ;
use S4PA::Storage;
use S4PA::Receiving;
use S4PA;
use Log::Log4perl;
use vars qw( $opt_a $opt_c $opt_p $opt_v );

getopts('a:c:p:v');

unless (defined($opt_a)) {
    S4P::logger("ERROR","Failure to specify -a <action> on command line.") ;
    usage();
}
my $action = $opt_a;

unless (defined($opt_c)) {
    S4P::logger("ERROR","Failure to specify -c <ConfigFile> on command line.") ;
    usage();
}
my $configFile = $opt_c;

unless (defined($opt_p)) {
    S4P::logger("ERROR","Failure to specify -p <pdrdir> on command line.") ;
    usage();
}
my $pdrdir = $opt_p ;
my $verbose = $opt_v;

#####################################################################
# Configuration setup
#####################################################################

# retrieve config values
my $cpt = new Safe 'CFG';
$cpt->share( '%cfg_datasets', 'cfg_interval', 'cfg_retention',
    '$UNRESTRICTED_ROOTURL', '$RESTRICTED_ROOTURL');

$cpt->rdo($opt_c) ||
    S4P::perish( 2, "Cannot read config file $opt_c in safe mode: $!\n");

my $logger = S4PA::CreateLogger( $CFG::cfg_logger{FILE},
    $CFG::cfg_logger{LEVEL} ) if ( %CFG::cfg_logger );

# checking the last run timestamp
my $heartBeatFile = "../Last_Publish_$action";
my $publishing = 0;
if ( -f $heartBeatFile ) {
    my $currentTime = time();
    my $st = stat( $heartBeatFile );
    $publishing = 1 if ( ( $currentTime - $st->mtime() ) > $CFG::cfg_interval );
} else {
    $publishing = 1;
}

unless ( $publishing ) {
    S4P::logger('INFO', "Not long enough since the last run, skip processing.");
    $logger->info( "Not long enough since the last run, skip processing." )
        if defined $logger;
    exit( 0 );
}

my @date = localtime(time);
my $dateString = sprintf("T%04d%02d%02d%02d%02d%02d", 
    $date[5]+1900, $date[4]+1, $date[3], $date[2], $date[1], $date[0]);
my $delete_time = sprintf("%04d\-%02d\-%02dT%02d\:%02d\:%02dZ",
    $date[5]+1900, $date[4]+1, $date[3], $date[2], $date[1], $date[0]);
my $message;

# make sure all published file staging area exist
my @pubDirs;
foreach my $shortname ( keys %CFG::cfg_datasets ) {
    foreach my $version ( keys %{$CFG::cfg_datasets{$shortname}} ) {
        my $localDir = $CFG::cfg_datasets{$shortname}{$version}{'DESTDIR'};
        File::Path::mkpath( $localDir ) || S4P::perish( 3,
            "Failed to create $localDir" ) unless ( -d $localDir );
        push ( @pubDirs, $localDir )
            unless ( exists {map { $_ => 1 } @pubDirs}->{$localDir} );
    }
}

#####################################################################
# PDR processing
#####################################################################

# examine PDR directory
opendir (PDRDIR,"$pdrdir") || S4P::perish( 4, "Failed to open $pdrdir" );
my @files = readdir (PDRDIR) ;
close (PDRDIR) ;
# excluding . and .. files under pending directory
my $numFiles = scalar(@files) - 2;
$logger->debug( "Found $numFiles files under $pdrdir" )
    if defined $logger;

my @processedPDR;
my @invalidPDR;

# parse each PDR
my $current_time = time();
my $pubDataset = {};
foreach my $pdrfile ( @files ) {
    chomp( $pdrfile );
    next if ($pdrfile !~ /\.(PDR|wo)$/);
    my $pdrPath = "$pdrdir/$pdrfile";

    # count file_group in the current pdr
    my $fgCount = `grep OBJECT=FILE_GROUP $pdrPath | wc -l` / 2;
    $logger->debug( "Found $pdrfile with $fgCount granule(s)." ) 
        if defined $logger;
    # skip empty PDR or work order
    if ( $fgCount == 0 ) {
        push @invalidPDR, $pdrPath;
        next;
    };

    # processing pdr
    S4P::logger("INFO", "Processing $pdrfile.");
    $logger->info( "Processing $pdrfile." ) if defined $logger;
    $message = process_pdr( $pdrPath, $action, $delete_time,
        $pubDataset);
    if ( $message ) {
        S4P::logger('ERROR', "Can't process $pdrfile: $message");
        $logger->error( "Can't process $pdrfile: $message" ) if defined $logger;
        push @invalidPDR, $pdrPath;
        next;
    }
    push @processedPDR, $pdrPath;
}

#####################################################################
# Creating publishing file
#####################################################################

foreach my $shortname ( sort keys %{$pubDataset} ) {
    foreach my $version ( sort keys %{$pubDataset->{$shortname}} ) {
        my $dataset = ( $version ) ? "$shortname.V$version" : $shortname;
        my $csvFile = "$action.$dataset.$dateString.csv";
        my $csvPath = "./$csvFile";
        open( CSV, ">$csvPath" ) ||
            S4P::perish( 5, "Failed to open file $csvPath: $!" );

        # records heading
        my $heading;
        $dataset = ( $version ) ? "$shortname v. $version" : $shortname;
        if ( $action eq 'Ingest' ) {
            $heading = "Access URL for $dataset,BeginDateTime,EndDateTime,InsertDateTime";
            if ( $CFG::cfg_datasets{$shortname}{$version}{BOUNDINGBOX} eq 'true' ) {
                $heading .= ",WestLongitude,SouthLatitude,EastLongitude,NorthLatitude";
            }
        } elsif ( $action eq 'Delete' ) {
            $heading = "Data file for $dataset,DeleteDateTime";
        }
        print CSV "$heading\n";

        foreach my $record ( sort @{$pubDataset->{$shortname}{$version}} ) {
            print CSV "$record\n";
        }
        close( CSV ) || S4P::perish( 6, "Failed to close file $csvPath: $!" );
        $logger->info( "Successfully created $csvPath for $shortname.$version" )
            if defined $logger;

        # copy file to local directory
        my $localDir = $CFG::cfg_datasets{$shortname}{$version}{'DESTDIR'};
        my $status = S4PA::Receiving::put( file => $csvPath,
            dir => $localDir, protocol => 'FILE' );
        if ( $status ) {
            $logger->info( "Successfully copy $csvPath to $localDir" ) if defined $logger;
            unlink $csvPath;
        } else {
            $logger->error( "Failed to copy $csvPath to $localDir" ) if defined $logger;
            S4P::perish( 7, "Failed to copy $csvPath to $localDir" );
        }
    }
}

# recreating the heart beat file for the new timestamp
if ( open( FH, ">$heartBeatFile" ) ) {
    S4P::logger( "INFO", "created new heartbeat file." );
} else {
    $logger->fatal( "Can't create Heartbeat File $heartBeatFile: $!" );
    S4P::perish( 9, "Can't create Heartbeat File $heartBeatFile: $!" );
}
close( FH );

#####################################################################
# Clean up and purging
#####################################################################

# delete processed PDRs
my $pdrCount = scalar( @processedPDR );
if ( $pdrCount ) {
    S4P::logger('INFO', "Processed $pdrCount qualified PDRs.");
    foreach my $pdr ( @processedPDR ) {
        unlink $pdr;
        S4P::logger('INFO', "Removed $pdr");
        $logger->debug( "Deleted $pdr" ) if defined $logger;
        # also remove the log file if exist
        my $file = basename( $pdr );
        my $path = dirname( $pdr );
        $file =~ s/^DO\.//;
        $file =~ s/\.PDR/.log/;
        unlink "$path/$file" if ( -f "$path/$file" );
    }
} else {
    S4P::logger( "INFO", "No qualified PDRs found" );
    $logger->info( "Found no qualified PDRs" ) if defined $logger;
}

# purging older staged published files
foreach my $dir ( @pubDirs ) {
    opendir (CSVDIR, "$dir") || S4P::perish( 8, "Failed to open $dir" );
    my @files = readdir (CSVDIR) ;
    close (CSVDIR) ;
    foreach my $file ( @files ) {
        chomp( $file );
        next if ($file !~ /\.(csv)$/);
        my $csvPath = "$dir/$file";

        # make sure csv file is old enough to be purged
        my $st = stat( $csvPath );
        if ( ($current_time - $st->ctime()) > $CFG::cfg_retention ) {
            unlink $csvPath;
            $logger->debug( "Purged $csvPath." ) if defined $logger;
        }
    }
}

# fail the job if there is any invalid PDR
if ( @invalidPDR ) {
    foreach my $pdr ( @invalidPDR ) {
        S4P::logger('WARN', "Invalid PDR: $pdr");
    }
    S4P::perish( 9, "There are invalid PDRs, please examine and " .
        "then remove them" );
}

exit( 0 );


#####################################################################
# process_pdr:  transform batch of PDRs into one csv files
#####################################################################
sub process_pdr {

    my ($pdrfile, $action, $delete_time, $pubDataset) = @_;
    my $message = '';

    my $pdr = S4P::PDR::read_pdr($pdrfile);
    if (!$pdr) {
        $message = "Failed reading $pdrfile";
        S4P::logger('ERROR', "Cannot read PDR file $pdrfile: $!");
        $logger->error( "$message" ) if defined $logger;
        return $message;
    }

    # parse through each FILE_GROUP
    foreach my $fg (@{$pdr->file_groups}) {
        my $datatype = $fg->data_type();
        my $version = $fg->data_version();
        $version = '' if ( ! defined $version || $version eq '000' );

        my $bbox;
        # make sure the dataset is set for publishing
        if ( ! exists $CFG::cfg_datasets{$datatype} ) {
            $message = "$datatype is not configured for publication";
            S4P::logger('ERROR', "$message");
            $logger->error( "$message" ) if defined $logger;
            return $message;
        # if there versionless is not specified in publish for user
        } elsif ( ! exists $CFG::cfg_datasets{$datatype}{$version} &&
            ! exists $CFG::cfg_datasets{$datatype}{''} ) {
            my $dataset = ( $version ) ? "$datatype.$version" : $datatype;
            $message = "$dataset is not configured for publication";
            S4P::logger('ERROR', "$message");
            $logger->error( "$message" ) if defined $logger;
            return $message;
        # exact match on version
        } elsif ( exists $CFG::cfg_datasets{$datatype}{$version} ) {
            $bbox = $CFG::cfg_datasets{$datatype}{$version}{'BOUNDINGBOX'};
        # use the versionless parameter instead
        } elsif ( exists $CFG::cfg_datasets{$datatype}{''} ) {
            # overwrite the version ID with the versionless in configuration
            $version = '';
            $bbox = $CFG::cfg_datasets{$datatype}{''}{'BOUNDINGBOX'};
        } else {
            $message = "Can't figure for publishing on $datatype, version $version";
            S4P::logger('ERROR', "$message");
            $logger->error( "$message" ) if defined $logger;
            return $message;
        }

        my $meta_file = $fg->met_file();
        my @sci_files = $fg->science_files();

        # make sure all metadata and science files exist
        unless ( -f $meta_file ) {
            S4P::logger('ERROR', "Missing metadata file: $meta_file, skipped");
            $logger->error( "Missing metadata file: $meta_file, skipped") 
                if defined $logger;
            next;
        }

        my $missingFileCount = 0;
        foreach my $data_file ( @sci_files ) {
            unless ( -f $data_file ) {
                S4P::logger('ERROR', "Missing data file: $data_file, skipped");
                $logger->error( "Missing data file: $data_file, skipped" )
                    if defined $logger;
                $missingFileCount = 1;
                last;
            }
        }
        next if ( $missingFileCount );

        # convert s4pa xml to csv
        my @records;
        if ( $action eq 'Ingest' ) {
            @records = convert_file( $bbox, $meta_file, @sci_files );
        } elsif ( $action eq 'Delete' ) {
            foreach my $path ( @sci_files ) {
                my $file = basename( $path );
                my $record = "$file,$delete_time";
                push @records, $record;
            }
        }

        # update $pubDataset with all records
        foreach my $record ( @records ) {
            push @{$pubDataset->{$datatype}{$version}}, $record;
        }
    }
    return;
}


#####################################################################
# convert_file:  transform S4PA metadata file to publish user string
#####################################################################
sub convert_file {
    my ( $bbox, $meta_file, @science_files ) = @_ ;

    # parsing metadata file
    my $parser = XML::LibXML->new();
    $parser->keep_blanks(0);
    my $dom = $parser->parse_file( $meta_file );
    my $doc = $dom->documentElement();
    # collection information
    my $shortName = GetNodeValue( $doc, './CollectionMetaData/ShortName' );
    my $versionID = GetNodeValue( $doc, './CollectionMetaData/VersionID' );

    # temporal information in iso format <YYYY-MM-DD>T<HH:MI:SS>Z
    my $beginDate = GetNodeValue( $doc, './RangeDateTime/RangeBeginningDate' );
    my $beginTime = GetNodeValue( $doc, './RangeDateTime/RangeBeginningTime' );
    $beginTime =~ s/\.\d+Z?$//;
    $beginTime =~ s/$/Z/;
    my $beginDateTime = "$beginDate" . "T$beginTime";
    my $endDate = GetNodeValue( $doc, './RangeDateTime/RangeEndingDate' );
    my $endTime = GetNodeValue( $doc, './RangeDateTime/RangeEndingTime' );
    $endTime =~ s/\.\d+Z?$//;
    $endTime =~ s/$/Z/;
    my $endDateTime = "$endDate" . "T$endTime";
    my $insertDateTime = GetNodeValue( $doc, './DataGranule/InsertDateTime' );
    $insertDateTime =~ s/\.\d+Z?$//;
    $insertDateTime =~ s/(\d{4}-\d{2}-\d{2})\ (\d{2}:\d{2}:\d{2})/${1}T${2}Z/; 

    # spatial bounding box
    my ( $westLon, $eastLon, $northLat, $southLat );
    if ( $bbox eq 'true' ) {
        my ( $bboxNode ) = $doc->findnodes( 'SpatialDomainContainer/HorizontalSpatialDomainContainer/BoundingRectangle' );
        if ( defined $bboxNode ) {
            $westLon = GetNodeValue( $bboxNode, './WestBoundingCoordinate' );
            $eastLon = GetNodeValue( $bboxNode, './EastBoundingCoordinate' );
            $northLat = GetNodeValue( $bboxNode, './NorthBoundingCoordinate' );
            $southLat = GetNodeValue( $bboxNode, './SouthBoundingCoordinate' );
        } else {
            $westLon = '';
            $eastLon = '';
            $northLat = '';
            $southLat = '';
        }
    }

    # get url for science file
    my @records;
    foreach my $science_file ( @science_files ) {
        my $url = S4PA::Storage::GetRelativeUrl( $science_file );
        S4P::perish( 10, "Failed to get relative URL for $science_file" )
            unless defined $url;
        my $filename = basename( $science_file );
        $url = $url . '/' . $filename;
        $url =~ s/\/+/\//g;
        my $sci_fs = stat( $science_file );
        my $filesize = $sci_fs->size();
        if ( $sci_fs->mode() & 004 ) {
            $url = $CFG::UNRESTRICTED_ROOTURL . $url;
        } else {
            $url = $CFG::RESTRICTED_ROOTURL . $url;
        }

        if ( $bbox eq 'true' ) {
            push @records, "$url,$beginDateTime,$endDateTime,$insertDateTime" .
                ",$westLon,$southLat,$eastLon,$northLat";
        } else {
            push @records, "$url,$beginDateTime,$endDateTime,$insertDateTime";
        }
    }
    return @records;
}


#####################################################################
# GetNodeValue:  get xml node value from parent node and relative xpath
#####################################################################
sub GetNodeValue
{
    my ( $root, $xpath ) = @_;
    my ( $node ) = ( $xpath ? $root->findnodes( $xpath ) : $root );
    return undef unless defined $node;
    my $val = $node->textContent();
    $val =~ s/^\s+|\s+$//g;
    return $val;
}


#####################################################################
# usage:  print usage and die
#####################################################################
sub usage {
  die << "EOF";
usage: $0 <-a action> <-c config_file>
          <-p pending_pdr_dir> <-s staging_dir> [options]
Options are:
        -v                  Verbose
EOF
}

################  end of s4pa_publish_dotchart  ################
