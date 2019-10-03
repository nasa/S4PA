#!/usr/bin/perl

=head1 NAME

s4pa_delete_data.pl - A command-line  script for deletion data

=head1 SYNOPSIS

s4pa_delete_data.pl -f <configuration file> -d queue(dir) [-t time_in_secs ]

=head1 DESCRIPTION

s4pa_delete_data.pl is script for deletion data on command-line. It is a wrapper
for S4PA::Storage::DeleteData(). The queue will hold PDRs containing granules to
be deleted. All granules older by the optionally specified time interval are
deleted. The default time interval is a day (86400 secs). If any invokation of
S4PA::Storage::DeleteData() fails (returns false), exits with failure.

=head1 AUTHOR

Yangling Huang L-3

=cut

################################################################################
# $Id: s4pa_delete_data.pl,v 1.23 2019/05/06 15:48:00 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use S4PA;
use S4PA::Storage;
use S4P;
use S4P::PDR;
use S4P::FileGroup;
use Fcntl;
use Getopt::Std;
use File::stat;
use File::Basename;
use Tk;
use Cwd;
use vars qw($opt_d $opt_t $opt_f);

# Get command line options.
getopts( 'f:d:t:' );

# Expect a directory (queue) to be an argument.
S4P::perish( 1, "Specify pending work order directory" ) unless defined $opt_d;

my $logger;
my $dataClassDir;
# Read configuration file
if ( defined $opt_f ) {
    my $cpt = new Safe 'CFG';
    $cpt->rdo($opt_f) or
        S4P::perish( 1,
            "Cannot read config file $opt_f in safe mode: ($!)" );
    # Create a logger
    $logger = S4PA::CreateLogger( $CFG::cfg_logger{FILE},
        $CFG::cfg_logger{LEVEL} ) if ( %CFG::cfg_logger );
    $dataClassDir = dirname ( dirname ( $opt_f ) );
}

my $currentDir = cwd();
my $stationDir = dirname( $currentDir );
# if $dataClassDir was not defined somehow, assume we are running
# under a RUNNING or temporary directory under delete data station
unless ( defined $dataClassDir ) {
    $dataClassDir = dirname(dirname( $currentDir ));
}

# Set the delay in deleting to a day by default. This is the min time for which
# files are kept once the files are marked for deletion.
$opt_t = 86400 unless defined $opt_t;
my $retentionTime = ( $opt_t eq 'NOW' ) ? 0 : $opt_t;

# Open the queue
S4P::perish( 1, "Failed to open pending work order directory, $opt_d, for"
                . " reading" ) unless ( opendir( PDRDIR, $opt_d ) );
my @fileList = readdir( PDRDIR );
closedir( PDRDIR );
my $cur_t = time();

# collect PDR that is older than the retention time and has been published.
my $pdrList = ();            # A list to qualified PDR.
my $pendingPdr = ();         # A list of pending publishing PDR.
PDR: foreach my $file ( @fileList ) {
    # Make sure the PDR file is old enough so that files it contained can be
    # deleted.
    next PDR unless ( $file =~ /\.(PDR|wo)$/ );
    my $st = stat( "$opt_d/$file" );
    S4P::perish( 1, "Failed to stat file, $opt_d/$file" ) unless defined $st;
    next PDR unless ( ($cur_t - $st->ctime()) > $retentionTime );

    my $pendingPublishFlag = 1;
    if ( defined $CFG::cfg_root ) {
        my $pdrName = basename( $file );
        # strip off the DO. prefix since publishing only has original PDR.
        $pdrName =~ s/^DO\.//;
        foreach my $station ( qw(whom echo mirador giovanni cmr) ) {
            my $pdrFile = "$CFG::cfg_root/publish_$station/pending_delete/"
                . $pdrName;
            if ( -f $pdrFile ) {
                $pendingPublishFlag = 0;
                S4P::logger( "WARNING",
                    "$pdrName is pending publication to $station; skipping" );
                $logger->info( "$pdrName is pending publication to $station; skipping" )
                    if defined $logger;
                push ( @$pendingPdr, $file );
            }
            last unless $pendingPublishFlag;
        }
    }
    next PDR unless $pendingPublishFlag;
    push ( @$pdrList, $file );
}
    
# Warning message for delete NOW
if ( $retentionTime == 0 ) {
    my $dataClass = basename( $dataClassDir );
    my $confirmed = deleteWarning( $dataClass, $opt_d, $pdrList, $pendingPdr );
    exit unless ( $confirmed );
}

my $error = 0; # Error Status indicator: true/false
my @dataDirList = ();   # A list to hold directories where data exist.
foreach my $file ( @$pdrList ) {
    # Slurp in the PDR as an S4P::PDR object.
    my $pdr = S4P::PDR::read_pdr( "$opt_d/$file" );
    unless ( defined $pdr ) {
        S4P::logger( "ERROR", "Failed read pdr file, $opt_d/$file: $!");
        $error = 1;
        next;
    }

    $logger->info( "Processing " . $file ) if defined $logger;
    FILE_GROUP: foreach my $file_group(@{ $pdr->file_groups }) {
        # Get the dataset and its associated granule DB.
        my $dataset = $file_group->data_type();
        my $version = $file_group->data_version();
        unless ( defined $dataset ) {
            S4P::logger( "ERROR", "Dataset not found in $opt_d/$file" );
            $error = 1;
            next FILE_GROUP;
        }

        # Check association before deletion
        my $s4paRoot = dirname( dirname( $dataClassDir ) );
        my ( $relation, $associateType, @associateDataset ) =
            S4PA::Storage::CheckAssociation( $s4paRoot, $dataset, $version );

        # Create a new FileGroup for actual file deletion latter
        # we might need to remove the associate granule from the current
        # file_group since our current policy is not to do cascade delete.
        my $fileGroup = new S4P::FileGroup;

        # deletion with associate dataset
        if ( $relation ) {

            # forward association, data -> browse
            # delete this granule record from associate.db
            # and then remove the associate link only without deleting
            # the associated granule.
            if ( $relation == 1 ) {
                my $dataGran = { "DATASET" => $dataset,
                                 "VERSION" => $version,
                                 "DATACLASS" => basename( $dataClassDir )
                               };

                # locate associated dataset's dataClass
                my ( $assocDataset, $assocVersion ) =
                    split /\./, $associateDataset[0], 2;
                # open dataset configuration file to locate dataClass
                my $cpt = new Safe( 'DATACLASS' );
                $cpt->share( '%data_class' );

                # Read config file
                my $cfgFile = "$s4paRoot/storage/dataset.cfg";
                S4P::perish( 1, "Cannot read config file $cfgFile")
                    unless $cpt->rdo( $cfgFile );
                my $assocClass = $DATACLASS::data_class{$assocDataset};
                S4P::perish( 1, "Cannot find dataClass for dataset: $assocDataset" )
                    unless ( defined $assocClass );
                my $assocGran = { "DATASET" => $assocDataset,
                                  "VERSION" => $assocVersion,
                                  "TYPE" => $associateType,
                                  "DATACLASS" => $assocClass
                                };

                # delete the current granule from associated.db 
                # and get an updated fileGroup for actual file deletion
                my $message;
                ( $fileGroup, $message ) = S4PA::Storage::DeleteAssociate(
                    $s4paRoot, $assocGran, $dataGran, $file_group );
                $logger->info( $message ) if defined $logger

            # reverse association, browse -> data
            # this associated granule (ex. browse) can be deleted
            # only if it is not associated by any data granule
            } elsif ( $relation == -1 ) {
                my $assocGran = { "DATASET" => $dataset,
                                  "VERSION" => $version,
                                  "TYPE" => $associateType,
                                  "DATACLASS" => basename( $dataClassDir )
                                };
                my $message;
                ( $fileGroup, $message ) = S4PA::Storage::DeleteAssociate(
                    $s4paRoot, $assocGran, undef, $file_group );

                if ( defined $fileGroup ) {
                    $logger->info( $message ) if defined $logger
                } else {
                    $logger->error( $message ) if defined $logger;
                    next FILE_GROUP;
                }
            }

        # no association, copy fileGroup and pass down
        } else {
            $fileGroup = $file_group->copy();
        }

        # regular granule deletion
        my ( %granule_hash );
        # Use version to find granule.db if it's defined.
        my $version = $fileGroup->data_version();
        my $db_file = $version ne '' ?
            "$dataClassDir/$dataset.$version/granule.db" :
            "$dataClassDir/$dataset/granule.db";
        S4P::perish( 1, "Granule database $db_file file not found" )
            unless ( -f $db_file );
        my ( $granule_hash,
             $file_handle ) = S4PA::Storage::OpenGranuleDB( $db_file, "rw" );
        unless ( defined $granule_hash ) {
            $error = 1;
            S4P::logger( "WARNING",
                "Failed to open granule database, $db_file, for dataset " );
            next FILE_GROUP;
        }
        FILE_SPEC: foreach my $file_spec ( @{ $fileGroup->file_specs } ) {
            my $data_type = $file_spec->file_type;
            my $data_dir = $file_spec->directory_id;
            my $data_file = $file_spec->file_id;
            my $status = S4PA::Storage::DeleteData ( "$data_dir/$data_file" );
            unless ( $status ) {
                $error = 1;
                S4P::logger( "ERROR",
                    "Failed to delete file $data_dir/$data_file");
                next FILE_SPEC;
            }
            delete $granule_hash->{$data_file};
            $logger->info( "Deleted " . $data_file ) if defined $logger;
            push( @dataDirList, $data_dir );

            # executing post storage task on science file only
            if ( exists $CFG::cfg_post_deletion{$dataset}{$version} &&
                 $data_type =~ /SCIENCE/i ) {
                my $dataLink = "$data_dir/$data_file";
                my $cmd = $CFG::cfg_post_deletion{$dataset}{$version} . " $dataLink";
                S4P::logger( 'INFO', "Executing post deletion script: $cmd" );
                my ( $errstr, $rc ) = S4P::exec_system( "$cmd" );
                if ( $rc ) {
                    S4P::raise_anomaly( "Post_Deletion", $stationDir, 'ERROR',
                        "Post deletion task: $errstr.", 0 );
                } else {
                    $logger->info( "Successfully executed post deletion task: $cmd" );
                }
            }

        }
        S4PA::Storage::CloseGranuleDB( $granule_hash, $file_handle );
    }
    unless ( $error ) {
        unless ( unlink( "$opt_d/$file" ) ) {
            S4P::logger( "ERROR", "Failed to delete $file in $opt_d ($!)" );
            $error = 1;
        }
        # Delete job log file if one exists.
        my $logFile = $file;
        $logFile =~ s/\.(PDR|wo)$/\.log/;
        next unless ( -f $logFile );
        unlink( "$opt_d/$logFile" );
    }
}

# Remove empty directories
foreach my $dir ( @dataDirList ) {
    next unless ( -d $dir );
    if ( $dir =~ /.hidden$/ ) {
        rmdir( $dir );
        $dir = dirname( $dir );
    }
    my $level = 0;
    if ( $dir =~ /\/\d{4}$/ ) {
        $level = 1;
    } elsif ( $dir =~ /\/\d{4}\/\d{2}$/ ) {
        $level = 2;
    } elsif ( $dir =~ /\/\d{4}\/\d{3}$/ ) {
        $level = 2;
    }
    for ( my $i = 0 ; $i < $level ; $i++ ) {
        rmdir ( $dir );
        $dir = dirname( $dir );
    } 
}

sub deleteWarning
{
    my ( $dataClass, $pendingDir, $deleteList, $pendingList ) = @_;
    my $confirmation = 0;
    my $title = "DeleteData: $dataClass";
    my ( $msg, $deleteFlag, @list);
    if ( $deleteList ) {
        $deleteFlag = 1;
        map { push @list, $_ } @$deleteList;
        $msg = "Please confirm on deleting all granules " 
            . "from the following PDRs:";
    } elsif ( $pendingList ) {
        $deleteFlag = 0;
        map { push @list, $_ } @$pendingList;
        $msg = "No qualified PDR found.\n"
            . "The following PDRs are pending publication:";
    }

    my $main = MainWindow->new();
    $main->title( $title );
    if ( $deleteList or $pendingList ) {
        my $labelFrame = $main->Label( -text=> $msg, -bd => 2, 
            -relief => 'groove', -padx => 10, -pady => 10 )
            ->pack(-fill => 'both');

        my $selectedPdr;
        my $listFrame = $main->Scrolled( 'Listbox', 
            -scrollbars => 'e', -relief=>'sunken' )
            ->pack( -fill => 'both', -anchor => 'ne', -expand => 1 );
        $listFrame->insert( 'end', @list );
        $listFrame->bind( '<<ListboxSelect>>',
            sub { 
                my $pdr = "$pendingDir/" . $list[$listFrame->curselection->[0]];
                S4PA::ViewFile( PARENT => $listFrame, 
                    TITLE => "File Viewer", FILE => "$pdr" ); 
            } );

        my $actionFrame = $main->Frame( -bd => 2, -relief => 'groove' );
        if ( $deleteFlag ) {
            $actionFrame->Button(-text => 'Cancel',
                -command => sub { $confirmation = 0; $main->destroy; })
                ->pack(-side => 'right', -padx => 10, -pady => 5);
            $actionFrame->Button(-text => 'OK',
                -command => sub { $confirmation = 1; $main->destroy; })
                ->pack(-side => 'right', -padx => 10, -pady => 5);
        } else {
            $actionFrame->Button(-text => 'OK',
                -command => sub { $confirmation = 0; $main->destroy; })
                ->pack(-side => 'right', -padx => 10, -pady => 5);
        }
        $actionFrame->pack(-fill=>'both',-anchor=>'ne');

    } else {
        $main->withdraw;
        $confirmation = 0;
        $msg = "No PDR found.";
        my $message = $main->messageBox( -title   => $title,
            -message => "$msg", -type    => "OK",
            -icon    => 'info', -default => 'ok' );
        $main->destroy;
    }
    MainLoop();
    return $confirmation;
}

exit ( $error );
