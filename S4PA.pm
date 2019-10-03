=head1 NAME

S4PA - Simple, Scalable, Script-based Science Processing Archive

=head1 SYNOPSIS

  use S4PA;

=head1 ABSTRACT


=head1 DESCRIPTION

=head2 EXPORT

None by default.

=head1 SEE ALSO

L<S4PA::Receiving>, L<S4PA::Storage>

==head1 COPYRIGHTS
Copyright © 2002-2011 United States Government as represented by Administrator for The National Aeronautics and Space Administration. All Rights Reserved.

=head1 AUTHORS

Chris Lynnes, E<lt>Christopher.S.Lynnes@nasa.govE<gt>
M. Hegde E<lt>Mahabaleshwa.Hegde@gsfc.nasa.gov<gt>

=cut

# S4PA.pm,v 1.120 2010/10/17 17229:25 glei Exp
# -@@@ S4PA, Version $Name:  $

package S4PA;

use 5.008;
use warnings;
use File::Basename;
use Tk;
use Tk::TextUndo;
use Tk::DialogBox;
use Tk::ROText;
use Tk::Pane;
use S4P::TkJob;
use Log::Log4perl;
use Time::Local;
use File::Find;
use File::stat;
use File::Copy;
use Cwd;
use Safe;
use Data::Dumper;
use strict;

our $VERSION = '3.43.7';

###############################################################################
# =head1 LoadStationConfig
# 
# Description
#   Loads station configuration (station.cfg) for use with S4PA deployment.
# =cut
###############################################################################
sub LoadStationConfig
{
    my ( $configFileName, $nameSpace ) = @_;
    no strict "refs";
    return unless ( -f $configFileName );
    $nameSpace = 'LOAD' unless defined $nameSpace;
    my $cpt = new Safe $nameSpace;
    $cpt->rdo( $configFileName ) or 
        S4P::perish(2,
            "Cannot read config file $configFileName in safe mode: ($!)");

    my $config;
    foreach my $hash ( 'cfg_failure_handlers', 'cfg_commands', 'cfg_interfaces',
        'cfg_reservations', 'cfg_downstream', 'cfg_virtual_jobs', 'cfg_auto_restart' ) {
        if ( keys %{"${nameSpace}::$hash"} ) {
            $config->{$hash} = \%{"${nameSpace}::$hash"};
            $config->{__TYPE__}{$hash} = 'HASH';
        }
    }
    foreach my $arrayRef ( 'cfg_sort_jobs' ) {
        $config->{$arrayRef} = ${"${nameSpace}::$arrayRef"}
            if defined ${"${nameSpace}::$arrayRef"};
    }    
    foreach my $scalar ( 'cfg_max_children', 'cfg_max_time', 'cfg_root',
        'cfg_max_failures', 'cfg_polling_interval', 'cfg_station_name',
        'cfg_virtual_feedback', 'cfg_ignore_duplicates', 'cfg_umask', 'cfg_group',
        'cfg_disable', 'cfg_restart_defunct_jobs', 'cfg_stop_interval',
        'cfg_end_job_interval', 'cfg_restart_interval' ) {
        $config->{$scalar} = ${"${nameSpace}::$scalar"}
            if defined ${"${nameSpace}::$scalar"};
    }
    return $config;
}
###############################################################################
# =head1 MergeStationConfig
# 
# Description
#   Retain existing station configuration items:
#   $cfg_max_children, $cfg_max_time, $cfg_max_failures, $cfg_polling_interval
#   %cfg_reservations, $cfg_sort_jobs, $cfg_auto_restart
#   $cfg_disable, %cfg_virtual_jobs, $cfg_restart_defunct_jobs
#   $cfg_stop_interval, $cfg_end_job_interval, $cfg_restart_interval
# =cut
###############################################################################
sub MergeStationConfig
{
    my ($configFileName, $config) = @_;
    no strict "refs";
    return unless ( -f $configFileName );
    my $nameSpace = $config->{cfg_station_name};
    if ( defined $nameSpace ) {
        $nameSpace =~ s/\W/_/g;
    } else {
        $nameSpace = 'MERGE';
    }
    my $cpt = new Safe $nameSpace;
    $cpt->rdo( $configFileName ) or 
        S4P::perish(2, "Cannot read config file $configFileName in safe mode: ($!)");
    foreach my $hash ( 'cfg_reservations', 'cfg_auto_restart', 'cfg_virtual_jobs' ) {
        if ( keys %{"${nameSpace}::$hash"} ) {
            $config->{__TYPE__}{$hash} = 'HASH';
            # we want to retain the current %cfg_virtual_jobs 
            # and add new job(s) if there is any.
            foreach my $job ( keys %{"${nameSpace}::$hash"} ) {
                $config->{$hash}{$job} = ${"${nameSpace}::$hash"}{$job};
            }
        }
    }

    foreach my $arrayRef ( 'cfg_sort_jobs' ) {
        $config->{$arrayRef} = ${"${nameSpace}::$arrayRef"}
            if defined ${"${nameSpace}::$arrayRef"};
    }        

    foreach my $scalar ( 'cfg_max_children', 'cfg_max_time', 'cfg_root',
        'cfg_max_failures', 'cfg_polling_interval', 'cfg_disable',
        'cfg_restart_defunct_jobs', 'cfg_stop_interval', 'cfg_end_job_interval',
        'cfg_restart_interval' ) {
        $config->{$scalar} = ${"${nameSpace}::$scalar"}
            if defined ${"${nameSpace}::$scalar"};
    }
}
###############################################################################
# =head1 WriteStationConfig
# 
# Description
#   Writes an S4P station configuration.
#
# =cut
###############################################################################
sub WriteStationConfig
{
    my ( $fileName, $dir, $config ) = @_;
    
    my $configStr = '';
    foreach my $key ( keys %$config ) {
        next if ( $key eq '__TYPE__' || $key eq '__SCRIPTS__');
        my $obj = Data::Dumper->new( [ $config->{$key} ], [ $key ] );
        $obj->Useqq( 1 );
        $obj->Sortkeys( 1 );
        $obj->Deparse( 1 );
        $obj->Purity( 1 );
        my ( $lhs, $rhs ) = split( /=/, $obj->Dump(), 2 );
        if ( defined $config->{__TYPE__}{$key} ) {
            if ( $config->{__TYPE__}{$key} eq 'LIST' ) {
                $lhs =~ s/^\$(\S+)/\@$1/;
                my $name = $1;
                $rhs =~ s/^\s*\[/\(/;
                $rhs =~ s/\](\s*;\s*)/\)$1/;
                $rhs =~ s/$name\-\>/$name/g;
            } elsif ( $config->{__TYPE__}{$key} eq 'HASH' ) {
                next unless ( keys( %{$config->{$key}} ) );
                $lhs =~ s/^\$(\S+)/\%$1/;
                my $name = $1;
                $rhs =~ s/^\s*\{/\(/;
                $rhs =~ s/\}(\s*;\s*)/\)$1/;
                $rhs =~ s/$name\-\>/$name/g;            
            } else {
                die "Unknow type: $config->{__TYPE__}{$key}";
            }
        }
        $rhs =~ s/\s*use strict.*;//g;
        $configStr .= "$lhs = $rhs" . "\n";
    }

    # If the last statement in the configuration file does not evaluate
    # to true, then the rdo command of Safe.pm will fail. Therefore, add
    # a line to guarantee that the last statement evaluates to true.
    $configStr .= "1;\n";

    chdir( $dir ) || die "Failed to change directory to $dir";
    local ( *FH );   
    die "Failed to open station.cfg in $dir for writing"
        unless ( open( FH, ">$fileName" ) );
    print FH $configStr;
    close( FH ) || die "Failed to close $fileName in $dir for writing";  
}
################################################################################
# =head1 CreateStation
# 
# Description
#   Creates a station (station.cfg) given its directory and configuration hash.
#
# =cut
################################################################################
sub CreateStation
{
    my ( $dir, $config, $logger ) = @_;
    
    my $msg = "Creating '" . $config->{cfg_station_name}
        . "' station...........";
    print STDERR "$msg\n";
    $logger->info( "$msg" ) if defined $logger;
    
    # Create station's directory
    unless ( CreateDir( $dir, $logger ) ) { 
        $msg = "Failed to create $config->{cfg_station_name}" .
            " station directory: $dir";
        $logger->error( "Deployment terminated: $msg" ) if defined $logger;
        die "$msg\n";
    }
    
    # Write station configuration
    MergeStationConfig( "$dir/station.cfg", $config );
    WriteStationConfig( 'station.cfg', $dir, $config );
    
    # Create downstream stations if necessary
    foreach my $key ( keys %{$config->{cfg_downstream}} ) {
        foreach my $dir ( @{$config->{cfg_downstream}{$key}} ) {
            my $dir = $config->{cfg_root} . '/' . $dir;
            CreateDir( $dir, $logger );
        }
    }
}
################################################################################
# =head1 CreateDir
# 
# Description
#   Creates a specified directory
#
# =cut
################################################################################
sub CreateDir
{
    my ( $dir, $logger ) = @_;

    # Get the permissions based on umask.    
    my $mode = umask() ^ 0777;
    
    # Check to see if the parent directory exists; create one if non-existent.
    my $parentDir = dirname( $dir );
    my $status = ( -d $parentDir ) ? 1 : CreateDir( $parentDir, $logger );
    
    # If the parent directory doesn't exist, complain and return false.
    unless ( $status ) {
        my $msg = "Failed to create $parentDir";
        print STDERR "$msg\n";
        $logger->error( "$msg" ) if defined $logger;
        Log::Log4perl::NDC->push( "$msg" );
        return 0;
    }
    # If a directory exists, return true.
    return 1 if ( -d $dir );

    print STDERR "Creating directory $dir: ";    
    $logger->info( "Creating directory $dir" ) if defined $logger;
    # Create the directory if successful so far and warn on failure to create 
    # one.
    $status = mkdir( $dir, $mode ) if $status;
    unless ( $status ) {
        my $msg = "Failed to create $dir ($!)";
        print STDERR "$msg\n";
        $logger->error( "$msg" ) if defined $logger;
        Log::Log4perl::NDC->push( "$msg" );
        return 0;
    }
    
    print STDERR "created\n";
    $logger->info( "$dir created" ) if defined $logger;
    
    # Return the result of creating a directory.
    return $status;
}
################################################################################
# =head1 ViewFile
# 
# Description
#   Creates a dialog to view file
#
# =cut
################################################################################
sub ViewFile
{
    my ( %arg ) = @_;
    my $parent = $arg{PARENT};
    my $title = $arg{TITLE};
    my $logFile = $arg{FILE};
    
    my $topWin = defined $parent ? $parent->Toplevel() : MainWindow->new();
    $topWin->title( $title );
    my $menuBar = $topWin->Menu();
    $topWin->configure( -menu => $menuBar );
    my $menuFile = $menuBar->cascade( -label => 'File' );
    my $filterFrame = $topWin->Frame()->pack( -fill => 'both' );
    my $filterLabel = $filterFrame->Label( -text => "Regular Expression" )->pack( -side => 'left' );
    my $filterBox = $filterFrame->Entry( -relief => 'sunken',
        -justify => 'left' )->pack( -side => 'left' );
    my $case = 0;
    my $filterCase = $filterFrame->Checkbutton( -text => "Case Sensitive", -onvalue => 1, -offvalue => 0, -variable => \$case, -indicatoron => 'true', -offrelief => 'raised', -overrelief => 'raised' )->pack( -side => 'left' );
    my $filterButton =
        $filterFrame->Button( -text => 'Filter' )->pack( -side => 'left' );
    my $messageBox = $topWin->Scrolled( 'ROText',
        -scrollbars => 'e' )->pack( -expand => 1, -fill => 'both' );
    my $readFile = sub {
        my ( $file, $filter, $case ) = @_;
        local( *FH );
        my @fileContent;
        if( open( FH, $file ) ) {
            if ( $filter ne '' ) {
                @fileContent = $$case
                    ? grep( /$filter/, <FH> ) : grep( /$filter/i, <FH> );
            } else {
                @fileContent = <FH>;
            }
            close( FH );
        } else {
            @fileContent = ( "Failed to read $logFile ($!)" );
        }
	# return $message;
        return join( "", @fileContent );
    };
    $messageBox->insert( 'end', "Opening $logFile\n" );
    $messageBox->insert( 'end',
        $readFile->( $logFile, $filterBox->get(), \$case ) );
    $menuFile->command( -label => 'Refresh',
        -command => sub { 
	    $messageBox->delete( '0.0', 'end' );
	    $messageBox->insert( 'end',
                $readFile->( $logFile, $filterBox->get(), \$case ) );
        } );
    $menuFile->command( -label => 'Close',
        -command => sub { $topWin->destroy; } );
    $filterButton->configure( -command => sub { 
	    $messageBox->delete( '0.0', 'end' );
	    $messageBox->insert( 'end',
                $readFile->( $logFile, $filterBox->get(), \$case ) );
        } );
    MainLoop unless defined $parent;
}
################################################################################
# =head1 EditFile
#
# Description
#   Edit specified file
#
# =cut
################################################################################
sub EditFile
{
    my ( %arg ) = @_;
    my $parent = $arg{PARENT};
    my $title = $arg{TITLE};
    my $editFile = $arg{FILE};

    my $topWin = defined $parent ? $parent->Toplevel() : MainWindow->new();
    $title = 'Edit File' if (not defined $title);
    $topWin->title( $title );

    # menu
    my $menu = $topWin->Frame(-relief => 'raised', -bd => 2)->pack(-side => 'top', -fill => 'x');
    my $menuFile = $menu->Menubutton(-text => 'File')->pack(-side, 'left');
    my $menuEdit = $menu->Menubutton(-text => 'Edit')->pack(-side, 'left');

    # text field
    my $textField = $topWin->TextUndo(-borderwidth => 2, -setgrid => 1);
    my $scrollbar = $topWin->Scrollbar(-command => [yview => $textField]);
    $textField->configure(-yscrollcommand => [set => $scrollbar]);
    $scrollbar->pack(-side => 'right', -fill => 'both');
    $textField->pack(-side => 'top', -fill => 'both', -expand => 'yes');

    my $readFile = sub {
        my $file = shift;
        local( *FH );
        my @fileContent;
        if( open( FH, $file ) ) {
            @fileContent = <FH>;
            close( FH );
        } else {
            @fileContent = ( "Failed to read $file ($!)" );
        }
        # return $message;
        return join( "", @fileContent );
    };

    my $saveFile = sub {
        my $file = shift;
        local( *FH );
        my $log;
        my $lines = $textField->get("1.0", "end");
        my $cpt = new Safe 'SAVEFILE';
        my $ret = $cpt->reval($lines);
        if ($@) {
            my $error = $@;
            my $tl = $topWin->DialogBox(-title => "Warning", -buttons =>["OK"]);
            $tl->add('Label', -text => "Cannot read edited file in safe mode ($error)")->pack();
            my $button = $tl->Show;
        } else {
            if( open( FH, ">$file" ) ) {
                print FH $lines;
                close(FH);
            } else {
                $log = "Error writing to file $file\n";
            }
        }
        return $log;
    };

    # menu items
    $menuEdit->command( -label => 'Undo All',
                        -command => sub {
        $textField->delete( '0.0', 'end' );
        $textField->insert( 'end',
          $readFile->($editFile) );
                    } );
    $menuEdit->command( -label => 'Undo',
                        -command => sub {
        $textField->undo;
                    } );
    $menuEdit->command( -label => 'Redo',
                        -command => sub {
        $textField->redo;
                    } );

    $menuFile->command(-label => 'Save',
                       -command => sub {
        $textField->insert( 'end',
          $saveFile->($editFile) );
                    } );
    $menuFile->command( -label => 'Close',
                         -command => sub {
         my $changeFlag = 0;
         my $text = $textField->get("1.0", "end");
         $text =~ s/^\s+//g;
         $text =~ s/\s+$//g;
         my $origText = $readFile->($editFile);
         $origText =~ s/^\s+//g;
         $origText =~ s/\s+$//g;
         $changeFlag=1 if ($text ne $origText);
         if ($changeFlag) {
             my $tl = $topWin->DialogBox(-title => "Warning",
                                         -buttons =>["Cancel", "Quit"]);
             $tl->add('Label', -text => "File has been modified; " .
                      "are you sure you want to quit?")->pack();
             my $button = $tl->Show;
             $topWin->destroy if ($button eq 'Quit');
         } else {
             $topWin->destroy;
         }
                            } );

    # read in file for editing
    $textField->insert( 'end',
        $readFile->($editFile) );
    $textField->ResetUndo;

    MainLoop unless defined $parent;
}
################################################################################
# = head1 SelectViewFile
#
# Description
#   select listed files for viewing
#
# =cut
################################################################################
sub SelectViewFile
{
    my (%arg) = @_;
    my $parent = $arg{PARENT};
    my $title = $arg{TITLE};
    my $fileList = $arg{FILELIST};
    my @targetDir = @{$arg{TODIR}};
    my $summary = $arg{SUMMARY};
    my $process = $arg{PROC};
    my $topWin = defined $parent ? $parent->Toplevel() : MainWindow->new();
    $topWin->title( $title );
    $topWin->geometry("800x600");

    # frame for summary message
    my $message = $topWin->Label(-text => $$summary)->pack();
    # frame for file text field
    my $selFrame = $topWin->Frame(-relief => "ridge", -borderwidth => 2)
                          ->pack(-fill => "both", -expand => "yes", -side => "right");
    my $fileField = $selFrame
         ->ScrlListbox(-selectmode => "extended", -exportselection => 0)
         ->pack(-fill => "both", -expand => "yes", -side => "top");
    $fileField->configure(scrollbars => 'se');
    my $entry = $selFrame->Entry(-relief => "sunken")->pack(-side => "right", -expand => "yes");

    # frame for file listbox
    my $listFrame = $topWin->Frame(-relief => "ridge", -borderwidth => 2)
                           ->pack(-fill => "both", -expand => "yes", -side => "right");
    my $listBoxFiles = $listFrame
         ->ScrlListbox(-selectmode => "single", -label => "Click to view file",
                       -exportselection => 0)
         ->pack(-fill => "both", -expand => "yes", -side => "top");
    $listBoxFiles->configure(scrollbars => 'se');

    # fill in listbox
    foreach my $item (@$fileList) {
        my $fileName = basename $item;
        $listBoxFiles->insert("end", $fileName);
    }

    # define button action in file list box
    $listBoxFiles->bind( "<Any-Button>"
         => sub {
                 my @lineList = ();
                 my $fileName = $listBoxFiles->Getselected();
                 $fileField->delete(0, "end");
                 foreach my $item (@$fileList) {
                     if ($item =~ /$fileName/) {
                         local( *FH );
                         my $fileContent;
                         if( open( FH, $item ) ) {
                             while (<FH>) {
                                 chomp;
                                 my $tabs = '';
                                 while (/^\t/) {
                                     $_ =~ s/^\t//;
                                     $tabs .= "       ";
                                 }
                                 my $line = "$tabs$_";
                                 $fileField->insert( 'end', $line );
                                 push @lineList, $line;
                             }
                             close( FH );
                             $arg{LINELIST} = \@lineList;
                         } else {
                             $fileField->insert( 'end', "Cannot open $item ($!)" );
                         }
                         last;
                     }
                 }
            });

    # call-back to highlight items from search
    my $highlightItemList = sub {
        my $lineList = shift;
        my $searchPattern = $entry->get();
        my $index = 0;
        foreach my $line (@$lineList) {
            if ($line =~ /$searchPattern/) {
                $fileField->selection("set", $index, $index);
            }
            $index++;
        }
    };

    # buttons
    my $cancelButton = $listFrame->Button(-text => "Cancel", -command => sub {
         my $dir = dirname $fileList->[0];
         foreach my $item (@$fileList) {
             unlink $item;
         }
         unless (rmdir $dir) {
             my $tl = $topWin->DialogBox(-title => "Warning", -buttons =>["OK"]);
             $tl->add('Label', -text => "Cannot remove directory $dir ($!)")->pack();
             my $button = $tl->Show;
         }
         $topWin->destroy;
    })->pack(-side => "left", -expand => "yes");
    my $confirmButton = $listFrame->Button(-text => "Proceed to $process all", -command => sub {
        my $movedList;
        foreach my $item (@$fileList) {
            my $cleanFlag = 1;
            foreach my $toDir (@targetDir) {
                mkdir $toDir unless (-d $toDir);
                unless (copy($item, $toDir)) {
                    my $tl = $topWin->DialogBox(-title => "Warning", -buttons =>["OK"]);
                    $tl->add('Label', -text => "Cannot copy $item to $toDir ($!)")->pack();
                    my $button = $tl->Show;
                    $cleanFlag = 0;
                }
            }
            if ($cleanFlag) {
                $movedList .=  basename $item . "\n";
                unlink $item;
            }
        }
        my $tl = $topWin->DialogBox(-title => "Warning", -buttons =>["OK"]);
        $tl->add('Label', -text => "Copied the following PDRs to $process:\n$movedList")->pack();
        my $button = $tl->Show;
        my $dir = dirname $fileList->[0];
        unless (rmdir $dir) {
            my $tl = $topWin->DialogBox(-title => "Warning", -buttons =>["OK"]);
            $tl->add('Label', -text => "Cannot remove directory $dir ($!)")->pack();
            my $button = $tl->Show;
        }
        $topWin->destroy;
    })->pack(-side => "left", -expand => "yes");
    # search button
    my $searchButton = $selFrame->Button(-text => "Search", -command => sub {
         my $curList = $arg{LINELIST};
         $highlightItemList->($curList); })->pack(-side => "right", -expand => "yes");
    MainLoop unless defined $parent;
}
################################################################################
# =head1 SelectEditHistoryFile
#
# Description
#   select listed history file and delete/add items
#
# =cut
################################################################################
sub SelectEditHistoryFile
{
    my ( %arg ) = @_;
    my $parent = $arg{PARENT};
    my $title = $arg{TITLE};
    my $dir = $arg{STATION};

    # retrieve job names for history
    my $message = '';
    my $stationConfig = $dir . "/station.cfg";
    my $cpt = new Safe 'CFG';
    $cpt->share( '%cfg_virtual_jobs');
    if ( -f $stationConfig ) {
        $cpt->rdo( $stationConfig ) or
        $message = "Failed to read configuration file $stationConfig ($@)";
    } else {
        $message = "Configuration file, $stationConfig, does not exist ($@)";
    }

    my $topWin = defined $parent ? $parent->Toplevel() : MainWindow->new();
    $topWin->title( $title );

    # frame for file listbox
    my $listFrame = $topWin->Frame(-relief => "ridge", -borderwidth => 2)
                           ->pack(-fill => "both", -side => "left");
    my $listBoxFiles = $listFrame
         ->ScrlListbox(-selectmode => "single", -label => "Click to select job",
                       -exportselection => 0)
         ->pack(-fill => "both", -side => "left");
    $listBoxFiles->configure(scrollbars => 'se');
    # fill in listbox
    if (keys %CFG::cfg_virtual_jobs) {
        foreach my $job (keys %CFG::cfg_virtual_jobs) {
            $listBoxFiles->insert("end", $job);
        }
    } else {
        my $tl = $topWin->DialogBox(-title => "Warning", -buttons =>["OK"]);
        if ($message) {
            $tl->add('Label', -text => $message)->pack();
        } else {
            $tl->add('Label', -text => "No job found")->pack();
        }
        my $button = $tl->Show;
    }

    # frame for file-item list box
    my $selFrame = $topWin->Frame(-relief => "ridge", -borderwidth => 2)
                          ->pack(-fill => "both", -expand => "yes", -side => "top");
    my $listBoxItems = $selFrame
         ->ScrlListbox(-selectmode => "extended", -exportselection => 0,
                       -label => "Select item(s) to remove from list")
         ->pack(-fill => "both", -expand => "yes", -side => "top");
    $listBoxItems->configure(scrollbars => 'se');
    my $entry = $selFrame->Entry(-relief => "sunken")->pack(-side => "right", -expand => "yes");

    # call-back to reach content of selected file
    my $getItemList = sub {
        my @lineList;
        $arg{FILE} = $listBoxFiles->Getselected() . ".history";
        my $file = $arg{FILE};
        local( *FH );
        unless (-e $file) {
            my $tl = $topWin->DialogBox(-title => "Warning", -buttons =>["OK"]);
            $tl->add('Label', -text => "History file, $file, does not exist")->pack();
            my $button = $tl->Show;
        }
        if( open( FH, $file ) ) {
            while(<FH>) {
                chomp;
                push @lineList, $_;
            }
            close( FH );
        } else {
            my $tl = $topWin->DialogBox(-title => "Warning", -buttons =>["OK"]);
            $tl->add('Label', -text => "Failed to open $file ($!)")->pack();
            my $button = $tl->Show;
        }
        if (@lineList) {
            $listBoxItems->delete(0, "end");
            foreach my $line (@lineList) {
                next if ($line eq '');
                $listBoxItems->insert("end", $line);
            }
        } else {
            my $tl = $topWin->DialogBox(-title => "Warning", -buttons =>["OK"]);
            $tl->add('Label', -text => "Empty file")->pack();
            my $button = $tl->Show;
        }
        return \@lineList;
    };
    # call-back to update item list
    my $updateItemList = sub {
        my $lineList = shift;
        my $curList;
        my @itemList = $listBoxItems->Getselected();
        $listBoxItems->delete(0, 'end');
        foreach my $line (@$lineList) {
            my $flag = 0;
            foreach my $item (@itemList) {
                if ($item =~ /$line/) {
                    $flag = 1;
                    last;
                }
            }
            if ($flag == 0) {
                $listBoxItems->insert("end", $line);
                push @$curList, $line;
            }
        }
        $arg{ITEMLIST} = $curList;
        $entry->delete(0, 'end');
    };
    # call-back to highlight items from search
    my $highlightItemList = sub {
        my $lineList = shift;
        my $searchPattern = $entry->get();
        my $index = 0;
        foreach my $line (@$lineList) {
            if ($line =~ /$searchPattern/) {
                $listBoxItems->selection("set", $index, $index);
            }
            $index++;
        }
    };

    # define double-click action in file list box
#    $listBoxFiles->bind( "<Double-Button-1>"
    $listBoxFiles->bind( "<Any-Button>"
         => sub { my $lineList = $getItemList->(); $arg{ITEMLIST} = $lineList; } );

    # search button
    my $searchButton = $selFrame->Button(-text => "Search", -command => sub {
         my $curList = $arg{ITEMLIST};
         $highlightItemList->($curList); })->pack(-side => "right", -expand => "yes");

    # button frame
    my $buttonFrame = $topWin->Frame(-relief => "ridge", -borderwidth => 2)
                             ->pack(-fill => "both", -expand => "yes", -side => "top");;
    my $updateButton = $buttonFrame->Button(-text => "Remove", -command => sub {
         my $curList = $arg{ITEMLIST};
         $updateItemList->($curList); })->pack(-side => "left", -expand => "yes");
    my $saveButton = $buttonFrame->Button(-text => "Save", -command => sub {
        my $file = $arg{FILE};
        my $curList = $arg{ITEMLIST};
        local( *FH );
        if( open( FH, ">$file" ) ) {
            if (defined $curList) {
                foreach my $line (@$curList) {
                    print FH "$line\n";
                }
            }
            close(FH);
        }
    } )->pack(-side => "left", -expand => "yes");
    my $quitButton = $buttonFrame->Button(-text => "Quit", -command => sub {
             my $tl = $topWin->DialogBox(-title => "Warning", -buttons =>["Cancel", "Quit"]);
             $tl->add('Label', -text => "Are you sure you want to quit?")->pack();
             my $button = $tl->Show;
             $topWin->destroy if ($button eq 'Quit');
                            })->pack(-side => "left", -expand => "yes");
    MainLoop unless defined $parent;
}
################################################################################
# =head1 RemoveStaleLog
#
# Description
#   delete log files with a time-stamp older than given date/time
#
# =cut
################################################################################
sub RemoveStaleLog
{
    my ( %arg ) = @_;
    my $parent = $arg{PARENT};
    my $title = $arg{TITLE};
    my $dir = $arg{STATION};

    my @logList;
    find( sub {
      if (/\.log$/) {
          my ($se, $mi, $hr, $da, $mo, $yr) = gmtime(stat($_)->mtime);
          $mo += 1;
          $yr += 1900;
          $mo = "0$mo" if ($mo < 10);
          $da = "0$da" if ($da < 10);
          $hr = "0$hr" if ($hr < 10);
          $mi = "0$mi" if ($mi < 10);
          $se = "0$se" if ($se < 10);
          my $mtime = "$yr-$mo-$da $hr:$mi:$se";
          push @logList, $File::Find::name . "  $mtime";
      } }, $dir );

    my $topWin = defined $parent ? $parent->Toplevel() : MainWindow->new();
    $topWin->title( $title );
    my $frame = $topWin->Frame()->pack(-fill => "both", -expand => "yes", -side => "top");
    my $label = $frame->Label(-text => "Enter date time (YYYY-MM-DD HH:MM:SS) to search for older log files")
                      ->pack();
    my $entry = $frame->Entry(-relief => "sunken")
                      ->pack(-fill => "both", -expand => "yes", -side => "top");
    my $listBox = $frame->ScrlListbox(-selectmode => "extended", -exportselection => 0)
                        ->pack(-fill => "both", -expand => "yes", -side => "top");
    $listBox->configure(scrollbars => 'se');
    
    # insert all log files
    if (@logList) {
        my @fileList;
        foreach my $logFile (@logList) {
            $listBox->insert("end", $logFile);
            my ($file, $mtime) = split /\s\s/, $logFile;
            push @fileList, $file;
        }
        $arg{ITEMLIST} = \@fileList;
    } else {
        my $tl = $topWin->DialogBox(-title => "Warning", -buttons =>["OK"]);
        $tl->add('Label', -text => "There is no .log file under $dir")
           ->pack();
        my $button = $tl->Show;
    }

    my $getList = sub {
        my @removeList = ();
        my $timeString = $entry->get();
        if ($timeString !~ /\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}/) {
            if ($timeString =~ /\d{4}-\d{2}-\d{2}\s*$/) {
                $timeString .= " 00:00:00";
            } else {
                my $tl = $topWin->DialogBox(-title => "Warning", -buttons =>["OK"]);
                $tl->add('Label', -text => "Please enter YYYY-MM-DD at minimum for date time")
                   ->pack();
                my $button = $tl->Show;
                return if ($button eq 'OK');
            }
        }
        my ($inDate, $inTime) = split /\s+/, $timeString;
        my ($se, $mi, $hr, $da, $mo, $yr) = gmtime(time);
        if (defined $inDate) {
            ($yr, $mo, $da) = split /-/, $inDate;
            $mo -= 1;
        }
        if (defined $inTime) {
            ($hr, $mi, $se) = split /:/, $inTime;
        }
        my $epoch = timegm($se, $mi, $hr, $da, $mo, $yr);
        if (@logList && $epoch != -1) {
            foreach my $log (@logList) {
                my ($logFile, $mtime) = split /\s\s/, $log;
                $mtime = stat($logFile)->mtime;
                if (defined $epoch) {
                    if ($mtime < $epoch) {
                        push @removeList, $log;
                    }
                }
            }
        } else {
            push @removeList, "no log file found";
        }
        return \@removeList;
    };
    my $searchButton = $frame->Button(-text => "Search", -command => sub {
         my $deleteList = $getList->();
         my $deleteFiles;
         $listBox->delete(0, "end");
         foreach my $item (@$deleteList) {
             $listBox->insert("end", $item);
             my ($logFile, $mtime) = split /\s\s/, $item;
             push @$deleteFiles, $logFile;
         }
         $arg{ITEMLIST} = $deleteFiles;
                 })->pack(-side => "left");
    my $deleteAllButton = $frame->Button(-text => "Remove All", -command => sub {
         foreach my $item (@{$arg{ITEMLIST}}) {
             unlink $item;
         }
         $listBox->delete(0, "end");
                 })->pack(-side => "left");
    my $deleteButton = $frame->Button(-text => "Remove", -command => sub {
          my @allList = $listBox->get(0, "end");
          my @selItems = $listBox->Getselected();
          $listBox->delete(0, "end");
          foreach my $item (@allList) {
              my $flag = 0;
              foreach my $selItem (@selItems) {
                  if ($item eq $selItem) {
                      my ($logFile, $mtime) = split /\s\s/, $selItem;
                      unlink $logFile;
                      $flag = 1;
                      last;
                  }
              }
              if ($flag == 0) {
                  $listBox->insert("end", $item);
              }
          }
#         my @selIndices = $listBox->curselection();
#         my @selItems = $listBox->Getselected();
#         foreach my $index (@selIndices) {
#             $listBox->delete($index, $index);
#         }
#         foreach my $item (@selItems) {
#             my ($logFile, $mtime) = split /\s\s/, $item;
#             unlink $logFile;
#         }
                 })->pack(-side => "left");
    my $quitButton = $frame->Button(-text => "Quit", -command => sub {
         $topWin->destroy; })->pack(-side => "left");
    MainLoop unless defined $parent;
}
####################################################################################
# =head1 RepublishData
#
# Description
#   Searches for data and places PDRs for publication or deletion
#
# =cut
####################################################################################
sub RepublishData
{
    my ( %arg ) = @_;
    my $parent = $arg{PARENT};
    my $title = $arg{TITLE};
    my $dir = $arg{STATION};

    $dir =~ s/\/+$//;
    my $s4paRoot = $dir . "/..";
    my $station = basename $dir;
    my $message = '';
    # retrieve config values
    my $datasetConfig = $s4paRoot . "/storage/dataset.cfg";
    my $cpt = new Safe 'CFG';
    $cpt->share( '%cfg_publication');
    if ( -f $datasetConfig ) {
        $cpt->rdo( $datasetConfig ) or
        $message = "Failed to read configuration file $datasetConfig ($@)";
    } else {
        $message = "Configuration file, $datasetConfig, does not exist ($@)";
    }

    my $topWin = defined $parent ? $parent->Toplevel() : MainWindow->new();
    $topWin->title( $title );

    # dataset and version
    my $dataFrame = $topWin->Frame(-relief => "ridge", -borderwidth => 2)
                           ->pack(-fill => "both", -expand => "yes", -side => "top");
    my $listBoxDatasets = $dataFrame
         ->ScrlListbox(-selectmode => "single", -label => "Click to select dataset",
                       -exportselection => 0, -height => 10)
         ->pack(-fill => "both", -expand => "yes", -side => "left");
    $listBoxDatasets->configure(scrollbars => 'se');
    if (keys %CFG::cfg_publication) {
        $listBoxDatasets->delete(0, 'end');
        my @datasetList;
        foreach my $dataset (keys %CFG::cfg_publication) {
            push @datasetList, $dataset;
        }
        my @sortedDatasetList = sort @datasetList;
        foreach my $dataset (@sortedDatasetList) {
            $listBoxDatasets->insert("end", $dataset);
        }
    } else {
        $listBoxDatasets->insert("end", $message);
    }
    my $listBoxVersions = $dataFrame
         ->ScrlListbox(-selectmode => "single", -label => "Select version",
                       -exportselection => 0, -height => 10)
         ->pack(-fill => "both", -expand => "yes", -side => "left");
    $listBoxVersions->configure(scrollbars => 'se');

    # define double-click action in dataset list box
#    $listBoxDatasets->bind( "<Double-Button-1>"
    $listBoxDatasets->bind( "<Any-Button>"
         => sub { $listBoxVersions->delete(0, 'end');
                  my $dataset = $listBoxDatasets->Getselected();
                  $arg{DATASET} = $dataset;
                  my @versionList = keys %{$CFG::cfg_publication{$dataset}};
                  if (@versionList) {
                      foreach my $version (@versionList) {
                          $version = 'versionless' if (!$version);
                          $listBoxVersions->insert("end", $version);
                      }
                  } else {
                      $listBoxVersions->insert("end", "No data version found");
                  }
                } );

    # time range and republish stations
    my $begTimeFrame = $topWin->Frame()->pack(-fill => "both", -expand => "yes", -side => "left");
    my $beginTimeLabel = $begTimeFrame->Label(-text => "Beginning Datetime (YYYY-MM-DD HH:MM:SS)")
                                   ->pack();
    my $beginTimeEntry = $begTimeFrame->Entry(-relief => "sunken")
                                   ->pack(-fill => "both", -expand => "yes");
    my $endTimeFrame = $topWin->Frame()->pack(-fill => "both", -expand => "yes", -side => "left");
    my $endTimeLabel = $endTimeFrame->Label(-text => "Ending Dateiime (YYYY-MM-DD HH:MM:SS)")
                                 ->pack();
    my $endTimeEntry = $endTimeFrame->Entry(-relief => "sunken")
                                 ->pack(-fill => "both", -expand => "yes");

    my $runTransientArchive = sub {
        my $dataset = $arg{DATASET};
        my $selVersion = $listBoxVersions->Getselected();
        my $beginDateTime = $beginTimeEntry->get();
        my $endDateTime = $endTimeEntry->get();
        my @publishVerList;
        my @pdrList = ();
        my $repubHash;
        if ($beginDateTime !~ /\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}/) {
            if ($beginDateTime =~ /\d{4}-\d{2}-\d{2}\s*$/) {
                $beginDateTime .= " 00:00:00";
            } else {
                my $tl = $topWin->DialogBox(-title => "Warning", -buttons =>["OK"]);
                $tl->add('Label', -text => "Please enter YYYY-MM-DD at minimum for beginning datetime")
                   ->pack();
                my $button = $tl->Show;
                return if ($button eq 'OK');
            }
        }
        if ($endDateTime !~ /\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}/) {
            if ($endDateTime =~ /\d{4}-\d{2}-\d{2}\s*$/) {
                $endDateTime .= " 00:00:00";
            } else {
                my $tl = $topWin->DialogBox(-title => "Warning", -buttons =>["OK"]);
                $tl->add('Label', -text => "Please enter YYYY-MM-DD at minimum for beginning datetime")
                   ->pack();
                my $button = $tl->Show;
                return if ($button eq 'OK');
            }
        }
        my $version = $selVersion;
        my $publishFlag=0;
        $version = '' if ($version eq 'versionless');
        foreach my $path (@{$CFG::cfg_publication{$dataset}{$version}}) {
            if ($path =~ /$station/) {
                $publishFlag=1;
                last;
            }
        }
        if ($publishFlag==0) {
            my $tl = $topWin->DialogBox(-title => "Warning", -buttons =>["OK"]);
            $tl->add('Label', -text => "Dataset $dataset version $version " .
                                       "not configured to publish to $station")
               ->pack();
            my $button = $tl->Show;
#            next if ($button eq 'OK');
        }
        my @publishDir;
        push @publishDir, $s4paRoot . "/$station/pending_publish";
        my $stagingDir = "/var/tmp/" . time() . '-' . $$ . '/';
        mkdir $stagingDir unless (-d $stagingDir);
        $version = "\'\'" if (!$version || ($version eq 'versionless'));
        my $runString = "s4pa_transient_archive.pl -r $s4paRoot -d $dataset " .
               "-v $version -s '$beginDateTime' -e '$endDateTime' " .
               "-l $stagingDir -p 'REPUBLISH' ";
        my $total = '';
        open (PIPE, "$runString 2>&1 |");
        while(<PIPE>) {
            chomp;
            if (/: (Total \d+ Granule\(s\) in \d+ PDR\(s\))/) {
                $total = $1;
            }
        }
        if (opendir(DH, $stagingDir)) {
            my @content = map{"$stagingDir$_"} grep(/^REPUBLISH.*\.PDR$/, readdir(DH));
            foreach my $entry (@content) {
                if (-f $entry) {
                    push @pdrList, $entry;
                }
            }
        }
        if (@pdrList) {
            S4PA::SelectViewFile( PARENT => $parent, TITLE => $title, FILELIST => \@pdrList,
                                  TODIR => \@publishDir, SUMMARY => \$total, PROC => "republish" );
        } else {
            my $tl = $topWin->DialogBox(-title => "Warning", -buttons =>["OK"]);
            $tl->add('Label', -text => "No granule found")
               ->pack();
            my $button = $tl->Show;
        }
    };
    # buttons
    my $buttonFrame = $topWin->Frame()->pack(-fill => "both", -expand => "yes", -side => "top");
    my $publishButton = $buttonFrame->Button(-text => "Publish", -command => sub {
         $runTransientArchive->(); })->pack(-side => "left");
    my $quitButton = $buttonFrame->Button(-text => "Cancel", -command => sub {
         $topWin->destroy; })->pack(-side => "left");
    MainLoop unless defined $parent;
}
####################################################################################
# =head1 DeleteGranule
#
# Description
#   Searches for granule and places deletion PDRs based on config
#
# =cut
####################################################################################
sub DeleteGranule
{
    my ( %arg ) = @_;
    my $parent = $arg{PARENT};
    my $title = $arg{TITLE};
    my $s4paRoot = $arg{ROOT};

    # retrieve config values
    my $datasetConfig = $s4paRoot . "/storage/dataset.cfg";
    my $cpt = new Safe 'CFG';
    $cpt->share( '$cfg_publish_dotchart', '%data_class', '%cfg_publication');
    my $message = '';
    if ( -f $datasetConfig ) {
        $cpt->rdo( $datasetConfig ) or
        $message = "Failed to read configuration file $datasetConfig ($@)";
    } else {
        $message = "Configuration file, $datasetConfig, does not exist ($@)";
    }

    my $topWin = defined $parent ? $parent->Toplevel() : MainWindow->new();
    $topWin->title( $title );

    # dataset and version
    my $dataFrame = $topWin->Frame(-relief => "ridge", -borderwidth => 2)
                           ->pack(-fill => "both", -expand => "yes", -side => "top");
    my $listBoxDatasets = $dataFrame
         ->ScrlListbox(-selectmode => "single", -label => "Click to select dataset",
                       -exportselection => 0, -height => 10)
         ->pack(-fill => "both", -expand => "yes", -side => "left");
    $listBoxDatasets->configure(scrollbars => 'se');
    if (keys %CFG::cfg_publication) {
        $listBoxDatasets->delete(0, "end");
        my @datasetList;
        foreach my $dataset (keys %CFG::cfg_publication) {
            push @datasetList, $dataset;
        }
        my @sortedDatasetList = sort @datasetList;
        foreach my $dataset (@sortedDatasetList) {
            $listBoxDatasets->insert("end", $dataset);
        }
    } else {
        my $tl = $topWin->DialogBox(-title => "Warning", -buttons =>["OK"]);
        $message = "No publication stations configured" unless $message;
        $tl->add('Label', -text => $message)->pack();
        my $button = $tl->Show;
    }
    my $listBoxVersions = $dataFrame
         ->ScrlListbox(-selectmode => "single", -label => "Select version",
                       -exportselection => 0, -height => 10)
         ->pack(-fill => "both", -expand => "yes", -side => "left");
    $listBoxVersions->configure(scrollbars => 'se');
    # define double-click action in file list box
#    $listBoxDatasets->bind( "<Double-Button-1>"
    $listBoxDatasets->bind( "<Any-Button>"
         => sub { $listBoxVersions->delete(0, "end");
                  my $dataset = $listBoxDatasets->Getselected();
                  $arg{DATASET} = $dataset;
                  my @versionList = keys %{$CFG::cfg_publication{$dataset}};
                  if (@versionList) {
                      foreach my $version (@versionList) {
                          $version = 'versionless' if (!$version);
                          $listBoxVersions->insert("end", $version);
                      }
                  } else {
                      $listBoxVersions->insert("end", "No data version found");
                  }
                } );

    # time range
    my $begTimeFrame = $topWin->Frame()->pack(-fill => "both", -expand => "yes", -side => "left");
    my $beginTimeLabel = $begTimeFrame->Label(-text => "Beginning Datetime (YYYY-MM-DD HH:MM:SS)")
                                      ->pack();
    my $beginTimeEntry = $begTimeFrame->Entry(-relief => "sunken")
                                      ->pack(-fill => "both", -expand => "yes");
    my $endTimeFrame = $topWin->Frame()->pack(-fill => "both", -expand => "yes", -side => "left");
    my $endTimeLabel = $endTimeFrame->Label(-text => "Ending Datetime (YYYY-MM-DD HH:MM:SS)")
                                    ->pack();
    my $endTimeEntry = $endTimeFrame->Entry(-relief => "sunken")
                                    ->pack(-fill => "both", -expand => "yes");
    my $runTransientArchive = sub {
        my $dataset = $arg{DATASET};
        my $selVersion = $listBoxVersions->Getselected();
        my $beginDateTime = $beginTimeEntry->get();
        my $endDateTime = $endTimeEntry->get();
        if ($beginDateTime !~ /\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}/) {
            if ($beginDateTime =~ /\d{4}-\d{2}-\d{2}\s*$/) {
                $beginDateTime .= " 00:00:00";
            } else {
                my $tl = $topWin->DialogBox(-title => "Warning", -buttons =>["OK"]);
                $tl->add('Label', -text => "Please enter YYYY-MM-DD at minimum for beginning datetime")
                   ->pack();
                my $button = $tl->Show;
                return if ($button eq 'OK');
            }
        }
        if ($endDateTime !~ /\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}/) {
            if ($endDateTime =~ /\d{4}-\d{2}-\d{2}\s*$/) {
                $endDateTime .= " 00:00:00";
            } else {
                my $tl = $topWin->DialogBox(-title => "Warning", -buttons =>["OK"]);
                $tl->add('Label', -text => "Please enter YYYY-MM-DD at minimum for ending datetime")
                   ->pack();
                my $button = $tl->Show;
                return if ($button eq 'OK');
            }
        }
        my @publishDir = ();
        my $dataClass = $CFG::data_class{$dataset};
        my $version = $selVersion;
        $version = '' if ($selVersion eq 'versionless');
        foreach my $dir ( @{$CFG::cfg_publication{$dataset}{$version}} ) {
            next if ( $dir =~ /dotchart/ );
            # use a new variable to avoid changing values for cfg_publication
            my $path = $dir;
            $path =~ s/pending_publish/pending_delete/;
            $path = "$s4paRoot/" . "$path";
            if (!(-d $path)) {
                mkdir $path;
            }
            push @publishDir, $path;
        }
        if ( $CFG::cfg_publish_dotchart ) {
            my $dotchartDir = "$s4paRoot/" . "publish_dotchart/pending_delete";
            push @publishDir, $dotchartDir;
        } else {
            my $deleteDir = "$s4paRoot/" . "storage/$dataClass/"
                . "delete_$dataClass/intra_version_pending";
            push @publishDir, $deleteDir;
        }

        my $stagingDir = "/var/tmp/" . time() . '-' . $$ . '/';
        mkdir $stagingDir unless (-d $stagingDir);
        $version = "\'\'" if (!$version || $version eq 'versionless');
        my $runString = "s4pa_transient_archive.pl -r $s4paRoot -d $dataset " .
               "-v $version -s '$beginDateTime' -e '$endDateTime' " .
               "-l $stagingDir ";
        my $total = '';
        open (PIPE, "$runString 2>&1 |");
        while(<PIPE>) {
            chomp;
            if (/: (Total \d+ Granule\(s\) in \d+ PDR\(s\))/) {
                $total = $1;
            }
        }
        my @pdrList = ();
        if (opendir(DH, $stagingDir)) {
            my @content = map{"$stagingDir$_"} grep(/^INTRA_VERSION_DELETE.*\.PDR$/, readdir(DH));
            foreach my $entry (@content) {
                if (-f $entry) {
                    push @pdrList, $entry;
                }
            }
        }
        if (@pdrList) {
            S4PA::SelectViewFile( PARENT => $parent, TITLE => $title, FILELIST => \@pdrList,
                                  TODIR => \@publishDir, SUMMARY => \$total, PROC => "delete" );
        } else {
            my $tl = $topWin->DialogBox(-title => "Warning", -buttons =>["OK"]);
            $tl->add('Label', -text => "No granule found")
               ->pack();
            my $button = $tl->Show;
        }
    };
    # buttons
    my $buttonFrame = $topWin->Frame(-relief => "ridge")
                             ->pack(-fill => "both", -expand => "yes", -side => "top");
    my $deleteButton = $buttonFrame->Button(-text => "Search Granule(s)", -command => sub {
#             my $tl = $topWin->DialogBox(-title => "Warning", -buttons =>["Cancel", "Confirm"]);
#             $tl->add('Label', -text => "Please confirm the search parameters for deletion")
#                ->pack();
#             my $button = $tl->Show;
#             $runTransientArchive->() if ($button eq 'Confirm'); })->pack();
             $runTransientArchive->(); })->pack();
    my $quitButton = $buttonFrame->Button(-text => "Cancel", -command => sub {
         $topWin->destroy; })->pack();
    MainLoop unless defined $parent;
}
################################################################################
# =head1 CreateLogger
# 
# Description
#   Creates a logger
#
# =cut
################################################################################
sub CreateLogger
{
    my ( $logFile, $level ) = @_;
    my $logger = Log::Log4perl::get_logger();
    $level = ( $level eq 'debug' )
        ? $Log::Log4perl::DEBUG
	: $Log::Log4perl::INFO;
    $logger->level( $level );
    if ( defined $logFile ) {
        my $layout =
            Log::Log4perl::Layout::PatternLayout->new( "%d %p %F{1} %m%n" );
        my $appender = Log::Log4perl::Appender->new( "Log::Dispatch::File",
            filename => $logFile,
            mode => "append" );
        $appender->layout( $layout );
        $logger->add_appender( $appender );
    }
    return $logger; 
}
################################################################################
# =head1 IsNumber
# 
# Description
#   Checks whether the input string is a number; returns true or false.
#
# =cut
################################################################################
sub IsNumber
{
    my ( $n ) = @_;
    return 1 if ( $n =~ /^[+-]?\d+$/ );
    return 1 if ( $n =~ /^-?\d+\.?\d*$/ );
    return 1 if ( $n =~ /^-?(?:\d+(?:\.\d*)?|\.\d+)$/ );
    return 1 if ( $n =~ /^([+-]?)(?=\d|\.\d)\d*(\.\d*)?([Ee]([+-]?\d+))?$/ );
    return 0;
}
################################################################################
# =head1 DifFetching
# 
# Description
#   Creates a dialog for DIF Fetching
#
# =cut
################################################################################
sub DifFetching
{
    my ( %arg ) = @_;
    my $parent = $arg{PARENT};
    my $title = $arg{TITLE};
    my $station = $arg{STATION};

    my $main = defined $parent ? $parent->Toplevel() : MainWindow->new();
    $main->title( $title );
    $station =~ s/\/$//;
    
    my $cfgFile = $station . "/s4pa_dif_info.cfg";
    my $historyFile = $station . "/lastupdate.txt";

    # Get configuration
    my $cpt = new Safe 'CFG';
    $cpt->share ('%cmr_collection_id');
    unless ( $cpt->rdo($cfgFile) ) {
        $main->withdraw();
        $main->messageBox(-title => $title,
                -message => "ERROR!\n\nFailed to open $cfgFile: $!",
                -type    => "OK", -icon    => 'error', -default => 'ok');
        $main->destroy;
        return;
    }

    my %lastUpdate;
    # Read the file containing the date of last update for each Entry_ID.
    # Expect each line to contain two fields separated by whitespace.
    # The first field is the Entry_ID, the second is a date of the
    # form yyyy-mm-dd
    unless ( open(LASTUPDATE, "< $historyFile") ) {
        $main->withdraw();
        $main->messageBox(-title => $title,
                -message => "INFO: No lastupdate.txt file found." .
                            "\n\nPlease bounce CmrDifFetcher station to" .
                            " fetch all configured DIFs once.",
                -type => "OK", -icon => 'info', -default => 'ok');
        $main->destroy;
        return;
    }
    while (<LASTUPDATE>) {
        chomp();
        next unless length();  # Skip empty lines
        my ($entryId, $updateDate) = split(/\s+/, $_);
        $lastUpdate{$entryId} = $updateDate;
    }
    close(LASTUPDATE);    

    my %difSelection;
    my %dif_to_dataset;
    # collect all dif_entry_id from the configuration file
    foreach my $dataset ( keys %CFG::cmr_collection_id ) {
        foreach my $version ( keys %{$CFG::cmr_collection_id{$dataset}} ) {
            if ( exists $CFG::cmr_collection_id{$dataset}{$version}{'entry_id'} ) {
                my $entryId = $CFG::cmr_collection_id{$dataset}{$version}{'entry_id'};
                $difSelection{$entryId} = 0;
                $dif_to_dataset{$entryId} = ( $version eq '' ) 
                    ? $dataset : "$dataset.$version";
            }
        }
    }

    my $labelFrame = $main->Label(
        -text=>'  Select Configured DIF ENTRY_ID for Fetching  ', 
        -bd => 2, -relief => 'groove' )
        ->pack(-fill => 'both');

    my @checkButtons;
    my $difFrame = $main->Scrolled('Pane', -sticky => 'nw', 
        -scrollbars => 'e', -relief=>'sunken' );
    foreach my $entryId ( sort keys %difSelection ) {
        my $dif = $entryId . "     ( $dif_to_dataset{$entryId} )  ";
        push @checkButtons, $difFrame->Checkbutton(-text => $dif,
            -variable => \$difSelection{$entryId}, -justify => 'left',
            -highlightthickness => 0, -state => 'normal',
            -offrelief => 'raised', -overrelief => 'raised')->pack(
            -anchor => 'w', -padx => 20, -pady => 5, -expand => 1 );
    }
    $difFrame->pack(-fill => 'both', -anchor => 'ne', -expand => 1 );

    my $fetching = 0;
    my $actionFrame = $main->Frame( -bd => 2, -relief => 'groove' );
    my $checkallButton = $actionFrame->Button(-text => 'Select All',
        -command => sub { map { $_->select } @{checkButtons}; })->pack(-side => 'left',
        -padx => 10, -pady => 5); 
    my $resetButton = $actionFrame->Button(-text => 'Reset',
        -command => sub { map { $_->deselect } @{checkButtons}; })->pack(-side => 'left',
        -padx => 10, -pady => 5); 
    my $publishButton = $actionFrame->Button(-text => 'Fetch Selected DIFs',
        -command => sub { $fetching = 1; })->pack(-side => 'left',
        -padx => 10, -pady => 5); 
    my $closeButton = $actionFrame->Button(-text => 'Cancel',
        -command => sub { $fetching = -1; $main->destroy; })->pack(-side => 'left',
        -padx => 10, -pady => 5); 
    $actionFrame->pack(-fill=>'both',-anchor=>'ne');
    $main->grab;
    $main->waitVariable( \$fetching );;

    my $confirmed = 'Cancel';
    my $selectedDif = 0;
    if ( $fetching == 1 ) {
        my $dialog = $main->DialogBox( -title => $title,
            -buttons => ["OK", "Cancel"] ); 
        $dialog->add('Label', -text => " Please confirm on re-fetching"
            . " the following DIF(s): " )->pack( -side => 'top' );
	my $difList = $dialog->add('Scrolled', 'Listbox', -scrollbars => 'e')
            ->pack( -fill => 'both', -side => 'top', -expand => 1 );
        foreach my $dif ( sort keys %difSelection ) {
            if ( $difSelection{$dif} ) {
                $difList->insert( 'end', $dif );
                $selectedDif++;
            }
        }
        $difList->insert( 'end', "No DIF selected" ) unless ( $selectedDif );
        $confirmed = $dialog->Show();
    }

    if ( $confirmed eq 'OK' && $selectedDif ) {
        unless ( open(LASTUPDATE, "> $historyFile") ) {
            $main->messageBox( -title   => $title,
                -message => "ERROR! Cannot open 'lastupdate.txt' for writing: $!",
                -type => "OK", -icon => 'error', -default => 'ok');
            $main->destroy;
            return;
        }

        # Write the file containing the date of last update for each Entry_ID
        # which is not selected by republishing.
        # Each line contains an Entry_ID and a date, separated by a tab character.
        foreach my $entryId ( sort keys %lastUpdate ) {
            next if ( $difSelection{$entryId} );
            print LASTUPDATE "$entryId\t$lastUpdate{$entryId}\n";
        }
        close(LASTUPDATE);

        # bounce dif_fetcher station to fetch selected DIFs.
        chdir $station;
        if ( S4P::check_station( $station ) ) {
            S4P::stop_station();
        }
        if ( S4P::TkJob::start_station( undef, $station ) ) {
            # fetching might take a few seconds to complete
            sleep 5;
            # remove main window
            $main->withdraw();

            my @fetchedDif;
            open(LASTUPDATE, "< $historyFile");
            while (<LASTUPDATE>) {
                chomp();
                next unless length();  # Skip empty lines
                my ($entryId, $updateDate) = split(/\s+/, $_);
                push @fetchedDif, $entryId
                    if ( defined $difSelection{$entryId} && $difSelection{$entryId} );
            }
            close(LASTUPDATE);
            my $fetched = scalar( @fetchedDif );
            my $msg = "Selected $selectedDif and fetched $fetched DIF(s).\n\n";
            unless ( $fetched == $selectedDif ) {
                $msg .= "Only following DIF(s) were fetched:\n\n";
            }
            map { $msg .= "$_\n" } @fetchedDif;
            $msg .= "\nPlease check housekeeper station for DIFs pending " .
                            "conversion and publication.";
            $main->messageBox( -title   => $title,
                -message => $msg, -type => "OK", -icon => 'info', -default => 'ok');
        }
    }
    $main->destroy;
        
    MainLoop unless defined $parent;
}
################################################################################
# =head1 ManageWorkOrder
# 
# Description
#   Creates a GUI to manage work orders; can move from work orders from station
#   directories to hold area and vice versa.
#
# =cut
################################################################################
sub ManageWorkOrder
{
    # Expects window title, directory to be listed, file/directory name pattern,
    # parent window (optional)
    my ( %arg ) = @_;    
    my $title = defined $arg{TITLE} ? $arg{TITLE} : '';
    my $dir = defined $arg{DIR} ? $arg{DIR} : '.';
    my $pattern = defined $arg{PATTERN} ? $arg{PATTERN} : undef;
    my $parent = defined $arg{PARENT} ? $arg{PARENT} : undef;
    
    # Gets the directory contents; returns an array ref
    my $getDirContent = sub {
        my ( %arg ) = @_;
        local( *DH );
        my $pattern = defined $arg{PATTERN} ? $arg{PATTERN} : '.*';
        my $dirContentList = [];
        if ( $arg{GLOB} ) {
            my $cwd = cwd();
            chdir( $arg{DIR} );
            @$dirContentList = glob( $pattern );
            push( @$dirContentList, glob( 'FAILED.*' ) );
            chdir( $cwd );
        } else {
            if ( opendir( DH, $arg{DIR} ) ) {
                @$dirContentList = grep( !/^\./ && /$pattern/, readdir(DH) );
                closedir( DH );
            } else {
                return undef;
            }
        }
        return $dirContentList;
    };
    
    # Toggles the source for file/directory move; toggles the button text
    # accordingly
    my $toggleSource = sub {
        my ( $list, $button, $flag ) = @_;
        my $text = $flag ? '<---' : '--->';
        $button->configure( -text => $text );
    };
    
    # Shows the file content in a separate window
    my $showFile = sub {
        my ( $list, $parent, $curDirName ) = @_;
        my $event = $list->XEvent();
        my $index = $list->nearest( $event->y() );
        my ( $selection ) = $list->get( $index, $index );
        
        my $file = $curDirName . '/' . $selection;
        if ( -f $file ) {
            S4PA::ViewFile( PARENT => $parent, TITLE => $file, FILE => $file );
        } else {
            $parent->messageBox( -message => "$file is not a file!", -type => 'ok' );
        }
    };

    # Sets the content of source and target list boxes
    my $setListBoxContent = sub {
        my ( %arg ) = @_;
        my $sourceListBox = $arg{SOURCE_LIST};
        my $targetListBox = $arg{TARGET_LIST};
        my $dir = $arg{DIR};
        my $pattern = $arg{PATTERN};
        my $sourceList = [];
        if ( -f "$dir/station.cfg" ) {
            my $config = S4PA::LoadStationConfig( "$dir/station.cfg", 'MyCFG' );
            $pattern = $MyCFG::cfg_work_order_pattern || S4P::work_order_pattern( $S4P::work_order_prefix, $MyCFG::cfg_input_work_order_suffix );
            $sourceList = $getDirContent->( DIR => $dir, PATTERN => $pattern, GLOB => 1 );        
        } else {
            $sourceList = $getDirContent->( DIR => $dir, PATTERN => $pattern, GLOB => 0 );
        }
        my $targetList = $getDirContent->( DIR => "$dir/.hold" );
        $sourceList = [] unless defined $sourceList;
        $targetList = [] unless defined $targetList;
        
        # Find the maximum width
        my $width = 0;
        foreach my $element ( @$sourceList, @$targetList ) {
            my $length = length( $element );
            $width = $length if ( $length > $width );
        }
        $width = 60 if ( $width == 0 || $width > 60);
        $sourceListBox->configure( -width => $width );
        $targetListBox->configure( -width => $width );
        $sourceListBox->delete( 0, 'end' );
        $targetListBox->delete( 0, 'end' );
        $sourceListBox->insert( 0, @$sourceList );
        $targetListBox->insert( 0, @$targetList );
    };
    
    # Refreshes lists
    my $refreshListCallBack = sub {
        my ( $srcListBox, $tarListBox, $srcDir, $pattern ) = @_;
        $setListBoxContent->( SOURCE_LIST => $srcListBox, TARGET_LIST => $tarListBox, DIR => $srcDir, PATTERN => $pattern );
    };
    
    # Transfers selected files/directories
    my $transferCallBack = sub {
        my ( $topWin, $srcListBox, $tarListBox, $srcDir, $tarDir, $pattern ) = @_;
        my @srcSelectionIndex = $srcListBox->curselection;
        my @tarSelectionIndex = $tarListBox->curselection;
        if ( @tarSelectionIndex ) {
            foreach my $index ( @tarSelectionIndex ) {
                my ( $file ) = $tarListBox->get( $index, $index );
                unless ( rename( "$tarDir/$file", "$srcDir/$file" ) ) {
                    $topWin->messageBox( -message => "Unable to move '$tarDir/$file' to '$srcDir/$file' ($!)", -type => 'ok' );
                }
            }
            $setListBoxContent->( SOURCE_LIST => $srcListBox, TARGET_LIST => $tarListBox, DIR => $srcDir, PATTERN => $pattern );
        } elsif ( @srcSelectionIndex ) {
            foreach my $index ( @srcSelectionIndex ) {
                my ( $file ) = $srcListBox->get( $index, $index );
                unless ( rename( "$srcDir/$file", "$tarDir/$file" ) ) {
                    $topWin->messageBox( -message => "Unable to move '$srcDir/$file' to '$tarDir/$file' ($!)", -type => 'ok' );
                }
            }
            $setListBoxContent->( SOURCE_LIST => $srcListBox, TARGET_LIST => $tarListBox, DIR => $srcDir, PATTERN => $pattern );
        }
    };

    # Create the window and necessary widgets
    my $topWin = defined $parent ? $parent->Toplevel() : MainWindow->new();
    $topWin->title( $title );
    my $menuBar = $topWin->Menu();
    $topWin->configure( -menu => $menuBar );
    my $menuFile = $menuBar->cascade( -label => 'File' );

    my $holdDir = "$dir/.hold";
    unless ( -d $holdDir ) {
        unless ( mkdir( $holdDir ) ) {
            $topWin->messageBox( -message => "Unable to create $holdDir ($!)", -type => 'ok' );
            return;
        };
    }
    my $listFrame = $topWin->Frame()->pack( -fill => 'both' );
    my $sourceListFrame = $listFrame->Frame()->pack( -side => 'left' );
    my $controlFrame = $listFrame->Frame()->pack( -side => 'left' );
    my $transferButton = $controlFrame->Button( -text => '--->' )->pack();
    my $targetListFrame = $listFrame->Frame()->pack( -side => 'left' );
    my $sourceListLabel = $sourceListFrame->Label( -text => 'Source' )->pack();
    my $sourceListBox = $sourceListFrame->Scrolled( 'Listbox', -scrollbars => 'se', -width => 0, -selectmode => 'extended' )->pack( -expand => 'yes' );
    my $targetListLabel = $targetListFrame->Label( -text => 'Hold' )->pack();
    my $targetListBox = $targetListFrame->Scrolled( 'Listbox', -scrollbars => 'se', -width => 0, -selectmode => 'extended' )->pack( -expand => 'yes' );
    $sourceListBox->bind( '<Any-Button>' => [ $toggleSource, $transferButton, 0 ]   );
    $targetListBox->bind( '<Any-Button>' => [ $toggleSource, $transferButton, 1 ]   );
    $sourceListBox->bind( '<Any-Double-Button>' => [ $showFile, $topWin, $dir  ] );
    $targetListBox->bind( '<Any-Double-Button>' => [ $showFile, $topWin, $holdDir  ] );
    $setListBoxContent->( SOURCE_LIST => $sourceListBox, TARGET_LIST => $targetListBox, DIR => $dir, PATTERN => $pattern );
    $transferButton->configure( -command => [ $transferCallBack, $topWin, $sourceListBox, $targetListBox, $dir, $holdDir, $pattern ] );

    $menuFile->command( -label => 'Refresh',
        -command => [ $refreshListCallBack, $sourceListBox, $targetListBox, $dir, $pattern ] );
    $menuFile->command( -label => 'Close',
        -command => sub { $topWin->destroy; } );
    MainLoop unless defined $parent;
}
