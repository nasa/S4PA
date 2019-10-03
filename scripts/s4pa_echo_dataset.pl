#!/usr/bin/perl

=head1 NAME

s4pa_echo_dataset.pl - A command-line script for ECHO dataset maintenance

=head1 SYNOPSIS

s4pa_echo_dataset.pl
B<-r> I<s4pa_root_directory>

=head1 DESCRIPTION

s4pa_echo_dataset.pl can be used to maintain a published ECHO collection.
Three actions are supported now: <delete> will delete dataset and all its
published granules, <hide> can make the dataset hidden on ECHO search, and
<expose> will make a hidden dataset visible on ECHO search.

=head1 ARGUMENTS

=item B<-r>

S4pa root directory.

=head1 AUTHOR

Guang-Dih Lei

=cut

################################################################################
# $Id: s4pa_echo_dataset.pl,v 1.10 2019/05/06 15:48:00 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use S4P;
use S4P::TimeTools;
use S4PA;
use Getopt::Std;
use XML::LibXML;
use File::Basename;
use Data::Dumper;
use Safe;
use Tk;
use vars qw( $opt_r );

# Get command line options.
getopts( 'r:' );
usage() unless ( $opt_r );

##############################################################################
# Process options
##############################################################################

# Required s4pa root directory
my $s4paRoot = $opt_r;
S4P::perish( 1, "s4pa root directory not defined." ) unless ( $s4paRoot );
$s4paRoot =~ s/\/$//;

# Get DIF info configuration
my $datasetConfig = $s4paRoot . "/other/housekeeper/s4pa_dif_info.cfg";
S4P::perish(1, "Configuration file $datasetConfig does not exist")
    unless ( -f $datasetConfig );
my $cpt = new Safe 'CFG';
$cpt->rdo( $datasetConfig )
    || S4P::perish(2, "Unable to read $datasetConfig: ($!)");

# Retrieve data access configuration
my $accessConfig = $s4paRoot . "/publish_echo/s4pa_insert_echo.cfg";
S4P::perish(3, "Configuration file $accessConfig does not exist")
    unless ( -f $accessConfig );
my $cpt = new Safe 'ACCESS';
$cpt->rdo( $accessConfig )
    || S4P::perish(4, "Unable to read $accessConfig: ($!)");

# Dataset and version mapping
my $s4paToEchoMap = {};

# Only include datasets that were set to published to ECHO
# and have been configured in ECHO_ACCESS hash
foreach my $dataset ( keys %ACCESS::ECHO_ACCESS ) {
    my $hasVersionless = 0;

    # Collect all configured ECHO-side versions from ECHO_ACCESS
    my @echoVersions = ( keys %{$ACCESS::ECHO_ACCESS{$dataset}} );

    # Iterate through the configured S4PA-side versions from DATA_ACCESS
    foreach my $s4paVersion ( keys %{$ACCESS::DATA_ACCESS{$dataset}} ) {
        if ( $s4paVersion eq "" ) {
            # We will handle the versionless cases at the end
            $hasVersionless = 1;
            next;
        } else {
            # Add this (non-versionless) version to the map and pop this
            # version from our list of ECHO versions for this dataset
            if ( exists $ACCESS::ECHO_ACCESS{$dataset}{$s4paVersion} ) {
                $s4paToEchoMap->{$dataset}{$s4paVersion} = $s4paVersion;
                @echoVersions = grep { $_ ne $s4paVersion } @echoVersions;
            }
        }
    }
    if ( $hasVersionless ) {
        # The dataset is versionless in S4PA.
        # There should be only one version value remaining in the list of
        # ECHO versions for the dataset, so map the versionless S4PA
        # designation to that ECHO version designation.
        my $echoVersion = pop @echoVersions;
        $s4paToEchoMap->{$dataset}{""} = $echoVersion
            if ( $echoVersion );
    }
}
S4P::perish( 5, "No dataset found in configuration file $accessConfig." .
    " Please execute s4pa_get_echo_access.pl script first to populate ECHO_ACCESS hash" )
    unless ( keys %{$s4paToEchoMap} );

# Start GUI
SelectEchoDataset( S4PAROOT => "$s4paRoot", TITLE => "PublishECHO" );

##############################################################################
# Subroutine usage:  Selecting dataset and action gui
##############################################################################
sub SelectEchoDataset
{
    my ( %arg ) = @_;
    my $parent = $arg{PARENT};
    my $title = $arg{TITLE};
    my $s4paRoot = $arg{S4PAROOT};
    $s4paRoot =~ s/\/+$//;

    # Get currently configured version of ECHO
    $CFG::ECHO_XSLFILE =~ /S4paDIF2ECHO(\d{2})\.xsl/;
    my $echoSchemaVersion = ( defined $1 ) ? $1 : 9;

    my $topWin = defined $parent ? $parent->Toplevel() : MainWindow->new();
    $topWin->title( $title );
    my $dataFrame = $topWin->Frame(-relief => "ridge", -borderwidth => 2)
                           ->pack(-fill => "both", -expand => "yes", -side => "top");
    my $listBoxDatasets = $dataFrame
         ->ScrlListbox(-selectmode => "single", -label => "Click to select dataset",
                       -exportselection => 0, -height => 10)
         ->pack(-fill => "both", -expand => "yes", -side => "left");
    $listBoxDatasets->configure(scrollbars => 'se');

    $listBoxDatasets->delete(0, 'end');
    my @datasetList;
    foreach my $dataset ( keys %{$s4paToEchoMap} ) {
        push @datasetList, $dataset;
    }
    my @sortedDatasetList = sort @datasetList;
    foreach my $dataset (@sortedDatasetList) {
        $listBoxDatasets->insert("end", $dataset);
    }

    my $listBoxVersions = $dataFrame
         ->ScrlListbox(-selectmode => "single", -label => "Select version",
                       -exportselection => 0, -height => 10)
         ->pack(-fill => "both", -expand => "yes", -side => "left");
    $listBoxVersions->configure(scrollbars => 'se');

    # Define any-click action in dataset list box
    $listBoxDatasets->bind( "<Any-Button>"
         => sub { $listBoxVersions->delete(0, 'end');
                  my $dataset = $listBoxDatasets->Getselected();
                  $arg{DATASET} = $dataset;
                  my @versionList = ( keys %{$s4paToEchoMap->{$dataset}} );
                  if (@versionList) {
                      foreach my $version (@versionList) {
                          $version = 'versionless' if (!$version);
                          $listBoxVersions->insert("end", $version);
                      }
                  } else {
                      $listBoxVersions->insert("end", "No data version found");
                  }
                } );

    my $runDatasetAction = sub {
        my ( $action ) = @_;
        my $dataset = $arg{DATASET};

        # Locate the associate version ID in ECHO
        my $s4paVersion = $listBoxVersions->Getselected();
        my $version;
        if ( $s4paVersion eq 'versionless' ) {
            $version = $s4paToEchoMap->{$dataset}{""};
            $arg{VERSION} = "";
        } else {
            $version = $s4paToEchoMap->{$dataset}{$s4paVersion};
            $arg{VERSION} = $version;
        }

        my $confirmed = "Cancel";
        if ( $version ) {
            my $tl = $topWin->DialogBox(-title => "Warning",
                -buttons =>["OK", "Cancel"]);
            if ( $action eq "Delete" ) {
                $tl->add('Label', -text => "\n   Please confirm on deleting   \n" .
                    "   Dataset $dataset version $version   \n " .
                    "   and all its granules from ECHO !!   \n")->pack();
            } elsif ( $action eq "Hide" ) {
                $tl->add('Label', -text => "\n   Please confirm on setting   \n" .
                    "   Dataset $dataset version $version   \n " .
                    "   to be HIDDEN on ECHO !!   \n")->pack();
            } elsif ( $action eq "Expose" ) {
                $tl->add('Label', -text => "\n   Please confirm on setting   \n" .
                    "   Dataset $dataset version $version   \n " .
                    "   to be VISIBLE on ECHO !!   \n")->pack();
            }
            $confirmed = $tl->Show();
        }

        if ( $confirmed eq 'OK' ) {
            # Write ECHO collection xml
            my $collection = ( $version ) ?
                "${dataset}.${version}" : "$dataset";
            my $echoXmlFile = "$CFG::TMPDIR/EchoColl_${action}_${collection}.xml";
            open(ECHO_OUTPUT, "> $echoXmlFile");

            if ( $action eq "Delete" ) {
                if ( $echoSchemaVersion == 9 ) {
                    print ECHO_OUTPUT construct_echo_delete_xml( $dataset, $version );
                } else {
                    print ECHO_OUTPUT construct_echo10_delete_xml( $dataset, $version );
                }
            } elsif ( $action eq "Hide" || $action eq "Expose" ) {
                print ECHO_OUTPUT construct_echo_visible_xml( $dataset, $version, $action );
            }
            unless ( close(ECHO_OUTPUT) ) {
                my $tl = $topWin->DialogBox(-title => "Error", -buttons =>["OK"]);
                $tl->add('Label', -text => "Error writing $echoXmlFile")
                    ->pack();
                $tl->Show();
                return;
            }

            # Create output work orders
            my $echoWoFile = $arg{S4PAROOT} .
                "/postoffice/DO.PUSH.ECHO_${action}_${collection}.wo";
            my $destination = $CFG::destination{'ECHO'};
            open(ECHO_WO, "> $echoWoFile");
            print ECHO_WO construct_wo_xml( $echoXmlFile, $destination );
            unless ( close(ECHO_WO) ) {
                my $tl = $topWin->DialogBox(-title => "Error", -buttons =>["OK"]);
                $tl->add('Label', -text => "Error writing $echoWoFile")->pack();
                $tl->Show();
                return;
            }

            # Set echo_visible for this dataset to reflect the current action
            if ( $action eq "Hide" || $action eq "Expose" ) {
                $arg{ACTION} = $action;
                UpdateDifInfoConfig( $datasetConfig, \%arg );
            }

            $topWin->messageBox(-title => $title,
                -message => "INFO: Postoffice work order\n" .
                            basename($echoWoFile) .
                            "\nwas created for ECHO publishing.\n",
                -type => "OK", -icon => 'info', -default => 'ok');
        }
    };

    # Buttons
    my $buttonFrame = $topWin->Frame()->pack(-fill => "both",
        -expand => "yes", -side => "top");
    my $publishButton = $buttonFrame->Button(-text => "Delete Dataset",
        -command => sub { $runDatasetAction->('Delete'); })->pack(-side => "left");
    if ( $echoSchemaVersion > 9 ) {
        my $publishButton = $buttonFrame->Button(-text => "Hide Dataset",
            -command => sub { $runDatasetAction->('Hide'); })->pack(-side => "left");
        my $publishButton = $buttonFrame->Button(-text => "Expose Dataset",
            -command => sub { $runDatasetAction->('Expose'); })->pack(-side => "left");
    }
    my $quitButton = $buttonFrame->Button(-text => "Close",
        -command => sub { $topWin->destroy; })->pack(-side => "left");
    MainLoop unless defined $parent;
}

##############################################################################
# Create publish xml file for deletion
##############################################################################
sub construct_echo_delete_xml {
    my ( $dataset, $version ) = @_;

    my $dom = XML::LibXML::Document->new( '1.0', 'UTF-8' );
    my $root = XML::LibXML::Element->new( 'CollectionMetaDataFile' );
    $dom->setDocumentElement( $root );
    my $doc = $dom->documentElement();
    $doc->appendTextChild( 'DTDVersion' , '1.0' );
    $doc->appendTextChild( 'DataCenterId' , 'GSFCS4PA' );

    # ECHO xml elements

    my $CollectionNode = XML::LibXML::Element->new('CollectionMetaDataSets');
    my $DeleteNodes = XML::LibXML::Element->new('DeleteCollections');
    my $DeleteCollectionNode = XML::LibXML::Element->new('DeleteCollection');
    $DeleteCollectionNode->appendTextChild( 'ShortName', $dataset );
    $DeleteCollectionNode->appendTextChild( 'VersionID', $version );
    $DeleteNodes->appendChild( $DeleteCollectionNode );
    $CollectionNode->appendChild( $DeleteNodes );
    $doc->appendChild( $CollectionNode );

    return $dom->toString(1);
}

##############################################################################
# Create publish xml file for ECHO10 deletion
##############################################################################
sub construct_echo10_delete_xml {
    my ( $dataset, $version ) = @_;

    my $dom = XML::LibXML::Document->new( '1.0', 'UTF-8' );
    my $root = XML::LibXML::Element->new( 'CollectionMetaDataFile' );
    $root->setAttribute( 'xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance' );
    $root->setAttribute( 'xsi:noNamespaceSchemaLocation',
        'http://www.echo.nasa.gov/ingest/schemas/operations/Collection.xsd' );

    $dom->setDocumentElement( $root );
    my $doc = $dom->documentElement();

    # ECHO xml elements

    my $DeleteNodes = XML::LibXML::Element->new('CollectionDeletes');
    my $DeleteCollectionNode = XML::LibXML::Element->new('CollectionDelete');
    $DeleteCollectionNode->appendTextChild( 'ShortName', $dataset );
    $DeleteCollectionNode->appendTextChild( 'VersionId', $version );
    $DeleteNodes->appendChild( $DeleteCollectionNode );
    $doc->appendChild( $DeleteNodes );

    return $dom->toString(1);
}

##############################################################################
# Create publish xml file for visibility
##############################################################################
sub construct_echo_visible_xml {
    my ( $dataset, $version, $action ) = @_;

    my $lastUpdate = S4P::TimeTools::CCSDSa_Now;

    my $dom = XML::LibXML::Document->new( '1.0', 'UTF-8' );
    my $root = XML::LibXML::Element->new( 'CollectionMetaDataFile' );
    $root->setAttribute( 'xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance' );
    $root->setAttribute( 'xsi:noNamespaceSchemaLocation',
        'http://www.echo.nasa.gov/ingest/schemas/operations/Collection.xsd' );
    $dom->setDocumentElement( $root );
    my $doc = $dom->documentElement();

    # ECHO xml elements

    my $PartialAddsNode = XML::LibXML::Element->new('CollectionPartialAdds');
    my $PartialAddNode = XML::LibXML::Element->new('CollectionPartialAdd');
    my $TargetsNode = XML::LibXML::Element->new('Targets');

    my $UpdateTargetNode = XML::LibXML::Element->new('Target');
    $UpdateTargetNode->appendTextChild( 'LastUpdate', $lastUpdate );
    my $CollectionNode = XML::LibXML::Element->new('Collection');
    $CollectionNode->appendTextChild( 'ShortName', $dataset );
    $CollectionNode->appendTextChild( 'VersionId', $version );
    $UpdateTargetNode->appendChild( $CollectionNode );
    $TargetsNode->appendChild( $UpdateTargetNode );

    my $FieldsNode = XML::LibXML::Element->new('Fields');
    my $FieldNode;

    # Use of the Visible tag is deprecated after ECHO 10.23
    my $visible = ( $action eq 'Hide' ) ? 'false' : 'true';
    $FieldNode = XML::LibXML::Element->new('Field');
    $FieldNode->appendTextChild( 'Visible', $visible );
    $FieldsNode->appendChild( $FieldNode );

    # Set RestrictionFlag to 1 if collection is to be hidden, 0 otherwise
    my $restrictionFlag = ( $action eq 'Hide' ) ? 1 : 0;
    $FieldNode = XML::LibXML::Element->new('Field');
    $FieldNode->appendTextChild( 'RestrictionFlag', $restrictionFlag );
    $FieldsNode->appendChild( $FieldNode );

    $PartialAddNode->appendChild( $TargetsNode );
    $PartialAddNode->appendChild( $FieldsNode );
    $PartialAddsNode->appendChild( $PartialAddNode );
    $doc->appendChild( $PartialAddsNode );

    return $dom->toString(1);
}

##############################################################################
# Create work order xml file
##############################################################################
sub construct_wo_xml {
    my ( $echoFile, $destination ) = @_;

    # Write a work order to push a file via ftp
    my $woParser = XML::LibXML->new();
    my $woDom = $woParser->parse_string('<FilePacket/>');
    my $woDoc = $woDom->documentElement();

    my ($filePacketNode) = $woDoc->findnodes('/FilePacket');
    $filePacketNode->setAttribute('status', "I");
    $filePacketNode->setAttribute('destination', $destination);

    my $filegroupNode = XML::LibXML::Element->new('FileGroup');
    my $fileNode = XML::LibXML::Element->new('File');
    $fileNode->setAttribute('localPath', $echoFile);
    $fileNode->setAttribute('status', "I");
    $fileNode->setAttribute('cleanup', "Y");
    $filegroupNode->appendChild($fileNode);
    $woDoc->appendChild($filegroupNode);

    return $woDom->toString(1);
}

##############################################################################
# Subroutine usage:  update s4pa_dif_info.cfg file
#  This whole routine was borrowed from UpdatePublishEchoConfig in the
#  s4pa_get_echo_access.pl script
##############################################################################
sub UpdateDifInfoConfig {
    my ( $file, $arg ) = @_;

    my $config;
    my $type_hash;
    # We want to write a new configuration file that has everything that
    # the original configuration file did, and add some new values.
    # But we want to use S4PA::WriteStationConfig to write the configuration
    # file, and it's third parameter (which we will call $config) is a
    # reference to a hash whose values must be either scalars,
    # array references, or hash references. When a value is a reference,
    # then $config->{'__TYPE__'} must be assigned a reference to an array
    # that pairs keys with reference types.
    # For example, if the key 'ALPHA' has a value that is a hash reference,
    # then $config->{'__TYPE__'}->{'ALPHA'} must be assigned the value 'HASH';
    #
    # The challenge is to find a way to assign every value in the original
    # configuration file to a spot in $config. Since the rdo() function of the
    # Safe module was used to read the configuration file variables into the
    # 'CFG' namespace, the solution is to iterate through every symbol
    # in the CFG namespace symbol table, assign it to a temporary typeglob,
    # figure out whether the symbol is a scalar, array, or hash, and then
    # store the value of the symbol into the hash pointed to by $config
    # using a key with the same name as the symbol.

    # Iterate through every symbol in the CFG namespace
    foreach my $name (keys %CFG::) {
        # We can't assigm to a typeglob if 'use strict' is in effect,
        # so disable it in this loop.
        no strict 'vars';

        # Skip special symbols whose name begins with an underscore
        next if $name =~ /^_/;

        # The CFG namespace also references symbols in the main namespace!
        # Skip those too, or else face an endless loop.
        # next if $name =~ /^main::/;
        # Symbols in other namespaces will begin with a name (consisting
        # of word characters) followed by two colons.
        next if $name =~ /^\w+::/;

        # Assign the symbol to a local typeglob
        local *sym = $CFG::{$name};

        # Depending upon the type of the symbol, add an entry to the hash
        # referenced by $config. The hash key will be the name of the
        # symbol. If the symbol is a scalar, the hash value is the value of
        # the scalar. If the symbol is an array or a hash, the hash value
        # is a reference, and we save the referency type in the hash
        # referenced by $type_hash.
        if (defined $sym) {
            $config->{$name} = $sym;
        } elsif (@sym) {
            $config->{$name} = \@sym;
            $type_hash->{$name} = 'ARRAY';
        } elsif (%sym) {
            # There is a special case to ignore %INC because Safe::rdo()
            # will set it to the names of files that were read.
            # If the configuration file we read contains a %INC value,
            # then we will lose it by ignoring it here;
            next if ($name eq 'INC');
            $config->{$name} = \%sym;
            $type_hash->{$name} = 'HASH';
        }
    }
    $config->{__TYPE__} = $type_hash if (defined $type_hash);

    # The hash referenced by $config now contains all of the information
    # in the configuration file read by this script. Now update the
    # echo_visible for the specified dataset.

    if ( exists $config->{echo_visible}{$arg->{DATASET}}{$arg->{VERSION}} ) {
        $config->{echo_visible}{$arg->{DATASET}}{$arg->{VERSION}} =
            ( $arg->{ACTION} eq 'Hide' ) ? 0 : 1;
    }

    # Replace the configuration file with a new configuration file
    # under both housekeeper and dif_fetch stations.
    my $station = dirname( $file );
    S4PA::WriteStationConfig( basename( $file ), $station , $config );
    $station =~ s/housekeeper/dif_fetcher/;
    S4PA::WriteStationConfig( basename( $file ), $station , $config );

    return;
}

##############################################################################
# Subroutine usage:  print usage and die
##############################################################################
sub usage {
  die << "EOF";
Usage: $0 <-r s4pa_root>
EOF
}

