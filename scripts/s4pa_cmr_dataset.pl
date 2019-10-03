#!/usr/bin/perl

=head1 NAME

s4pa_cmr_dataset.pl - A command-line script for CMR dataset maintenance

=head1 SYNOPSIS

s4pa_cmr_dataset.pl
B<-r> I<s4pa_root_directory>

=head1 DESCRIPTION

s4pa_cmr_dataset.pl can be used to maintain a published CMR collection.
Three actions are supported now: <delete> will delete dataset and all its
published granules, <hide> can make the dataset hidden on CMR search, and
<expose> will make a hidden dataset visible on CMR search.

=head1 ARGUMENTS

=item B<-r>

S4pa root directory.

=head1 AUTHOR

Guang-Dih Lei

=cut

################################################################################
# $Id: s4pa_cmr_dataset.pl,v 1.3 2019/05/06 15:48:00 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use S4P;
use S4P::TimeTools;
use S4PA;
use Getopt::Std;
use LWP::UserAgent;
use XML::LibXML;
use File::Basename;
use Clavis;
use Data::Dumper;
use URI::Escape;
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
my $accessConfig = $s4paRoot . "/publish_cmr/s4pa_insert_cmr.cfg";
S4P::perish(3, "Configuration file $accessConfig does not exist")
    unless ( -f $accessConfig );
$cpt = new Safe 'ACCESS';
$cpt->rdo( $accessConfig )
    || S4P::perish(4, "Unable to read $accessConfig: ($!)");

# Dataset and version mapping
my $s4paToCmrMap = {};

# Only include datasets that were set to published to CMR
# and have been configured in CMR_ACCESS hash
foreach my $dataset ( keys %ACCESS::CMR_ACCESS ) {
    foreach my $s4paVersion ( keys %{$ACCESS::DATA_ACCESS{$dataset}} ) {
        my $collection_shortname = $CFG::cmr_collection_id{$dataset}{$s4paVersion}{'short_name'};
        my $collection_version = $CFG::cmr_collection_id{$dataset}{$s4paVersion}{'version_id'};
        $s4paToCmrMap->{$dataset}{$s4paVersion}{'short_name'} = $collection_shortname;
        $s4paToCmrMap->{$dataset}{$s4paVersion}{'version_id'} = $collection_version;
        if ( exists $CFG::cmr_collection_id{$dataset}{$s4paVersion}{'entry_id'} ) {
            my $entry_id = $CFG::cmr_collection_id{$dataset}{$s4paVersion}{'entry_id'};
            $s4paToCmrMap->{$dataset}{$s4paVersion}{'entry_id'} = $entry_id;
        }
    }
}

S4P::perish( 5, "No dataset found in configuration file $accessConfig." .
    " Please execute s4pa_get_cmr_access.pl script first to populate CMR_ACCESS hash" )
    unless ( keys %{$s4paToCmrMap} );

# Start GUI
SelectCmrDataset( S4PAROOT => "$s4paRoot", TITLE => "PublishCMR" );

##############################################################################
# Subroutine usage:  Selecting dataset and action gui
##############################################################################
sub SelectCmrDataset
{
    my ( %arg ) = @_;
    my $parent = $arg{PARENT};
    my $title = $arg{TITLE};
    my $s4paRoot = $arg{S4PAROOT};
    $s4paRoot =~ s/\/+$//;

    my $topWin = defined $parent ? $parent->Toplevel() : MainWindow->new();
    $topWin->title( $title );
    my $dataFrame = $topWin->Frame(-relief => "ridge", -borderwidth => 2)
                           ->pack(-fill => "both", -expand => "yes", -side => "top");
    my $listBoxDatasets = $dataFrame
         ->ScrlListbox(-selectmode => "single",
                       -label => "Click to select dataset",
                       -exportselection => 0, -height => 10)
         ->pack(-fill => "both", -expand => "yes", -side => "left");
    $listBoxDatasets->configure(scrollbars => 'se');

    $listBoxDatasets->delete(0, 'end');
    my @datasetList;
    foreach my $dataset ( keys %{$s4paToCmrMap} ) {
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
                  my @versionList = ( keys %{$s4paToCmrMap->{$dataset}} );
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

        # Locate the associated version ID in CMR
        my $s4paVersion = $listBoxVersions->Getselected();
        my $version;
        if ( $s4paVersion eq 'versionless' ) {
            $s4paVersion = "";
            $version = $s4paToCmrMap->{$dataset}{""}{'version_id'};
            $arg{VERSION} = "";
        } else {
            $version = $s4paToCmrMap->{$dataset}{$s4paVersion}{'version_id'};
            $arg{VERSION} = $version;
        }

        my $confirmed = "Cancel";
        if ( $version ) {
            my $tl = $topWin->DialogBox(-title => "Warning",
                -buttons =>["OK", "Cancel"]);
            if ( $action eq "Delete" ) {
                $tl->add('Label', -text => "\n   Please confirm on deleting   \n" .
                    "   Dataset $dataset version $version   \n " .
                    "   and all its granules from CMR !!   \n")->pack();
            } elsif ( $action eq "Hide" ) {
                $tl->add('Label', -text => "\n   Please confirm on setting   \n" .
                    "   Dataset $dataset version $version   \n " .
                    "   to be HIDDEN on CMR !!   \n")->pack();
            } elsif ( $action eq "Expose" ) {
                $tl->add('Label', -text => "\n   Please confirm on setting   \n" .
                    "   Dataset $dataset version $version   \n " .
                    "   to be VISIBLE on CMR !!   \n")->pack();
            }
            $confirmed = $tl->Show();
        }

        if ( $confirmed eq 'OK' ) {
            # Write CMR collection xml
            my $entry_id = $s4paToCmrMap->{$dataset}{$s4paVersion}{'entry_id'}
                if exists $s4paToCmrMap->{$dataset}{$s4paVersion}{'entry_id'};
            unless ( defined $entry_id ) {
                my $tl = $topWin->DialogBox(-title => "Error", -buttons =>["OK"]);
                $tl->add('Label', -text => "No CMR entry_id found for dataset '$dataset' version '$version'")->pack();
                $tl->Show();
                return;
            }
            my $queryContentType;
            my $destinationContentType;
            my $query = $CFG::CMR_ENDPOINT_URI;
            my $destination = $CFG::CMR_ENDPOINT_URI;
            if ($CFG::CMR_ENDPOINT_URI =~ /cmr/i) {
                $query =~ s#ingest/##;
#                $query .= 'search/collections.echo10?provider=' . $CFG::CMR_PROVIDER . '&page_size=2000&dataset_id=';
                $query .= 'search/collections.echo10?provider=' . $CFG::CMR_PROVIDER . '&page_size=2000&entry_id=';
                $destination .=  'ingest/providers/' . $CFG::CMR_PROVIDER . '/collections/';
#                $destinationContentType = 'application/echo10+xml';
                $destinationContentType = 'application/echo10+xml';
            } else {
                $query .=  'providers/' . $CFG::CMR_PROVIDER . '/datasets/';
                $destination = $query;
                $queryContentType = 'application/xml';
                $destinationContentType = 'application/xml';
            }
            # $query .= uri_escape($datasetId);
            $query .= $entry_id;
            # $destination .= uri_escape($datasetId);
            $destination .= $entry_id;
            my $collection = ( $version ) ?
                "${dataset}.${version}" : "$dataset";
            my $payloadNode;

            my $parser = XML::LibXML->new();
            $parser->keep_blanks(0);

            my $password = Clavis::decrypt( $ACCESS::CMR_PASSWORD )
                if ( defined $ACCESS::CMR_PASSWORD );
            my $token = login($ACCESS::CMR_USERNAME, $password,
                              $ACCESS::CMR_PROVIDER);

            my $cmrWoFile;
            if ( $action eq "Delete" ) {
                $cmrWoFile = $arg{S4PAROOT} .
                "/postoffice/DO.DELETE.CMR_${collection}.wo";
            } elsif ( $action eq "Hide" || $action eq "Expose" ) {
                $cmrWoFile = $arg{S4PAROOT} .
                "/postoffice/DO.PUT.CMR_${action}_${collection}.wo";
                $payloadNode = construct_cmr_visible_xml( $parser,
                                                          $entry_id,
                                                          $query,
                                                          $queryContentType,
                                                          $token,
                                                          $action );
            }

            # Create output work orders
            open(CMR_WO, "> $cmrWoFile");
            print CMR_WO construct_rest_wo_xml( $parser, $destination,
                                                $destinationContentType,
                                                $token,
                                                $payloadNode  );
            unless ( close(CMR_WO) ) {
                my $tl = $topWin->DialogBox(-title => "Error", -buttons =>["OK"]);
                $tl->add('Label', -text => "Error writing $cmrWoFile")->pack();
                $tl->Show();
                return;
            }

            # Set cmr_visible for this dataset to reflect the current action
            if ( $action eq "Hide" || $action eq "Expose" ) {
                $arg{ACTION} = $action;
                UpdateDifInfoConfig( $datasetConfig, \%arg );
            }

            $topWin->messageBox(-title => $title,
                -message => "INFO: Postoffice work order\n" .
                            basename($cmrWoFile) .
                            "\nwas created for CMR publishing.\n",
                -type => "OK", -icon => 'info', -default => 'ok');
        }
    };

    # Buttons
    my $buttonFrame = $topWin->Frame()->pack(-fill => "both",
        -expand => "yes", -side => "top");
    my $publishButton = $buttonFrame->Button(-text => "Delete Dataset",
        -command => sub { $runDatasetAction->('Delete'); })->pack(-side => "left");
    $publishButton = $buttonFrame->Button(-text => "Hide Dataset",
        -command => sub { $runDatasetAction->('Hide'); })->pack(-side => "left");
    $publishButton = $buttonFrame->Button(-text => "Expose Dataset",
        -command => sub { $runDatasetAction->('Expose'); })->pack(-side => "left");
    my $quitButton = $buttonFrame->Button(-text => "Close",
        -command => sub { $topWin->destroy; })->pack(-side => "left");
    MainLoop unless defined $parent;
}


##############################################################################
# Create publish xml file for visibility
##############################################################################
sub construct_cmr_visible_xml {
    my ($parser, $entry_id, $datasetUrl, $contentType, $token, $action) = @_;

    my $request = HTTP::Request->new( 'GET',
                                      $datasetUrl,
                                      [Content_Type => $contentType,
                                      Echo_Token => $token]
                                    );
    my $ua = LWP::UserAgent->new;
    my $response = $ua->request($request);
    my $content = $response->content();
    unless ($response->is_success) {
        print STDERR "Failed to get dataset info for '$entry_id' at $datasetUrl\n$content\n";

        return;
    }
    my $dom;
    eval {$dom = $parser->parse_string( $content ); };
    if ($@) {
        print STDERR "Could not parse dataset xml: $@\n";
        return;
    }
    my $responseDoc = $dom->documentElement();
    my ($doc) = $responseDoc->findnodes('//Collection');

    my $lastUpdate = S4P::TimeTools::CCSDSa_Now;

#    my $lastUpdateNode = $doc->findnodes('/Collection/LastUpdate');
#    $lastUpdateNode->removeChildNodes();
#    $lastUpdateNode->appendText($lastUpdate);

    # Use of the Visible tag is deprecated after ECHO 10.23
    my $visible = ( $action eq 'Hide' ) ? 'false' : 'true';
    my ($visibleNode) = $doc->findnodes('Visible');
    $visibleNode->removeChildNodes();
    $visibleNode->appendText($visible);

    # Set RestrictionFlag to 1 if collection is to be hidden, 0 otherwise
    my $restrictionFlag = ( $action eq 'Hide' ) ? 1 : 0;
    my ($restrictionFlagNode) = $doc->findnodes('RestrictionFlag');
    # curent CMR setting has no RestrictionFlag field
    if ( defined $restrictionFlagNode ) {
        $restrictionFlagNode->removeChildNodes();
        $restrictionFlagNode->appendText($restrictionFlag);
    }

    return $doc;
}

##############################################################################
# Create work order xml file
##############################################################################
sub construct_rest_wo_xml {
    my ($parser, $destination, $contentType, $token, $payloadContentsNode) = @_;

    # Write a work order to submit a REST request
    my $woDom = $parser->parse_string('<RestPackets/>');
    my $woDoc = $woDom->documentElement();
    $woDoc->setAttribute('status', "I");

    my $restPacketNode = XML::LibXML::Element->new('RestPacket');
    $restPacketNode->setAttribute('status', "I");
    $restPacketNode->setAttribute('destination', $destination);

    if ($token) {
        my $headerNode = XML::LibXML::Element->new('HTTPheader');
        $headerNode->appendText("Echo-Token: $token");
        $restPacketNode->appendChild($headerNode);
    }
    if ($contentType) {
        my $headerNode = XML::LibXML::Element->new('HTTPheader');
        $headerNode->appendText("Content-Type: $contentType");
        $restPacketNode->appendChild($headerNode);
    }

    if ($payloadContentsNode) {
        my $payloadNode = XML::LibXML::Element->new('Payload');
        $payloadNode->appendChild($payloadContentsNode);
        $restPacketNode->appendChild($payloadNode);
    }
    $woDoc->appendChild($restPacketNode);

    return $woDom->toString(1);
}

##############################################################################
# Subroutine usage:  update s4pa_dif_info.cfg file
#  This whole routine was borrowed from UpdatePublishCmrConfig in the
#  s4pa_get_cmr_access.pl script
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
    # cmr_visible for the specified dataset.

    if ( exists $config->{cmr_visible}{$arg->{DATASET}}{$arg->{VERSION}} ) {
        $config->{cmr_visible}{$arg->{DATASET}}{$arg->{VERSION}} =
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


sub login {

    my ($username, $pwd, $provider) = @_;

    my $hostname = Sys::Hostname::hostname();
    my $packed_ip_address = (gethostbyname($hostname))[4];
    my $ip_address = join('.', unpack('C4', $packed_ip_address));

    my $tokenNode = XML::LibXML::Element->new('token');
    $tokenNode->appendTextChild('username', $username);
    $tokenNode->appendTextChild('password', $pwd);
    $tokenNode->appendTextChild('client_id', 'GESDISC');
    $tokenNode->appendTextChild('user_ip_address', $ip_address);
    $tokenNode->appendTextChild('provider', $provider);

    my $id;
    my $tokenUrl = $CFG::CMR_TOKEN_URI . 'tokens';
    my $request = HTTP::Request->new( 'POST',
                                      $tokenUrl,
                                      [Content_Type => 'application/xml'],
                                      $tokenNode->toString() );
    my $ua = LWP::UserAgent->new;
    my $response = $ua->request($request);
    if ($response->is_success) {
        my $xml = $response->content;
        my $tokenDom;
        my $xmlParser = XML::LibXML->new();
        eval {$tokenDom = $xmlParser->parse_string($xml); };
        S4P::perish( 2, "Could not parse response from CMR token request:  $@\n" ) if $@;
        my $tokenDoc = $tokenDom->documentElement();
        my ($idNode) = $tokenDoc->findnodes('/token/id');
        $id = $idNode->textContent();
    } else {
        S4P::perish( 2, "Could not connect to CMR API for login; check if CMR API is down.\nReason:  $@\n" );
    }

    return $id;
}


##############################################################################
# Subroutine usage:  print usage and die
##############################################################################
sub usage {
  die << "EOF";
Usage: $0 <-r s4pa_root>
EOF
}

