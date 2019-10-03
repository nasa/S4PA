#!/usr/bin/perl

=head1 NAME

s4pa_fetch_revised_DIFs.pl - Checks for and retrieves revised DIF files for a set of datasets in an S4PA installation

=head1 SYNOPSIS

s4pa_fetch_revised_DIFs.pl B<-c> I<configFileName> work_order

=head1 DESCRIPTION

s4pa_fetch_revised_DIFs.pl iterates through a list of products for an S4PA
instance, and for each product, determines if the DIF file corresponding to
that product has been revised since the last time that DIF file was used to
update services. If so, the DIF file is fetched, and one or more output work
orders are created.

=head1 AUTHOR

Edward Seiler, SSAI

=cut

###############################################################################
# $Id: s4pa_fetch_revised_DIFs.pl,v 1.16 2019/05/06 15:48:00 glei Exp $
# -@@@ S4PA, Version $Name:  $
###############################################################################

use strict;
use lib '/tools/gdaac/TS2/src/s4p';
use Getopt::Std;
use LWP::UserAgent;
use XML::Simple;
use XML::LibXML;
use Encode qw(encode);
use S4P;
use S4PA;
use Safe;
use Clavis;
use Sys::Hostname;
use JSON;
use File::Basename;
use Cwd;
use vars qw($opt_c $opt_u $opt_e $opt_E $opt_h $opt_v);

# Read and parse command line options
getopts('c:u:e:E:hv');
usage() if $opt_h;
my $cfg_file = $opt_c || '../s4pa_dif_info.cfg';
my $verbose = $opt_v;

S4P::perish( 1, "Configuration file $cfg_file does not exist" )
  unless ( -f $cfg_file );

S4P::logger( 'INFO', "Configuration file is $opt_c" );

my $stationDir = dirname( cwd() );
my $dif_info_file = "$stationDir/" . basename($cfg_file);

# Get configuration
my $cpt = new Safe 'CFG';
$cpt->rdo($cfg_file) || S4P::perish( 2, "Unable to read $cfg_file: ($!)" );

my $cmr_username = ( defined $opt_u ) ? $opt_u :
    $CFG::CMR_USERNAME;
my $cmr_encrypted_pwd = ( defined $opt_e ) ? $opt_e :
    $CFG::CMR_PASSWORD;
my $cmr_decrypted_pwd = Clavis::decrypt( $cmr_encrypted_pwd )
    if $cmr_encrypted_pwd;
my $cmrToken = login($cmr_username, $cmr_decrypted_pwd, $CFG::CMR_PROVIDER);

my $cmr_endpoint_uri = ( defined $opt_E ) ? $opt_E :
    $CFG::CMR_ENDPOINT_URI;
my $cmr_rest_base_url = $cmr_endpoint_uri . 'search/collections';
my $ua = LWP::UserAgent->new;

# Read the date of last update for each product from a file.
# Expect the file to be in the parent directory of the running S4P job
# directory.
my $last_update_filename = "$stationDir/lastupdate.txt";
my $last_dif_update      = read_last_updates($last_update_filename);
my $update_count         = 0;

my $date_of_last_dif_update;
my ( $day, $month, $year ) = (localtime)[ 3, 4, 5 ];
my $yyyymmdd_today = sprintf "%04d%02d%02d", $year + 1900, $month + 1, $day;

my $dataSetIds = {};
# Process each dataset flagged with collection metadata fetching
foreach my $dataset ( keys %CFG::cmr_collection_id ) {

    # There can be more than one version of a dataset.
    # The version designation is allowed to be an empty string.
    #my $document_url = $CFG::dataset_doc_url{$dataset}
    #  if (defined $CFG::dataset_doc_url{$dataset});
    foreach my $version ( keys %{ $CFG::cmr_collection_id{$dataset} } ) {
        my $document_url = $CFG::dataset_doc_url{$dataset}{$version}
          if ( defined $CFG::dataset_doc_url{$dataset}{$version} );
        my $collection_shortname = $CFG::cmr_collection_id{$dataset}{$version}{'short_name'};
        my $collection_version = $CFG::cmr_collection_id{$dataset}{$version}{'version_id'};
        $dataSetIds->{$dataset}{$version}{'short_name'} = $collection_shortname;
        $dataSetIds->{$dataset}{$version}{'version_id'} = $collection_version;
        my ( $entry_id, $native_id );

        my $partial_dif_url = $cmr_rest_base_url . '.umm-json?provider=' .
            $CFG::CMR_PROVIDER . '&short_name=' . $collection_shortname .
            '&version=' . $collection_version;

        # Loop in case there are problems with the network or the remote host
        my $dif;
        my $partial_dif_req = HTTP::Request->new( 'GET',
                                          $partial_dif_url,
                                          [Echo_Token => $cmrToken]
                                        );

        # Pause before fetching in order to prevent too
        # many fetches in a short period of time.
        sleep $CFG::sleep_seconds;
      LOOP1:
        for ( my $attempt = 1 ;
              $attempt <= $CFG::max_fetch_attempts ;
              $attempt++ )
        {

            # Get date of last DIF revision from GCMD site, as XML text
            # (gcmdbase is an alias that we expect to be defined in
            # /etc/hosts on the machine that this script is running on)
            my $response = $ua->request($partial_dif_req);
            if ($response->is_success) {
                $dif = $response->content;
            }
            last LOOP1 if ($dif);
            S4P::logger(
                         'INFO',
                         "Unsuccessful attempt [$attempt of "
                           . "$CFG::max_fetch_attempts] to fetch partial DIF"
                           . " for $collection_shortname.$collection_version, sleeping..."
                       );
            sleep $CFG::sleep_seconds;
        }

        unless ( $dif ) {
            S4P::logger( 'ERROR', "Failed to fetch partial DIF for " .
                "$collection_shortname.$collection_version" );
            next;
        }

        else {
            # Parse the JSON to extract the revision date
            my $Last_DIF_Revision_Date;
            my $jsonRef = decode_json($dif);
            my $hits = $jsonRef->{'hits'};
            unless ( $hits == 1 ) {
                $hits = 'No' if ( $hits == 0 );
                S4P::raise_anomaly( "COLLECTION_NOT_FOUND", $stationDir, "WARN",
                    "$hits collection found for $collection_shortname.$collection_version", 0 );
                next;
            }

            # there should be only one collection under 'items' array
            foreach my $coll (@{$jsonRef->{'items'}}) {
                $entry_id = $coll->{'umm'}{'entry-id'};
                $native_id = $coll->{'meta'}{'native-id'};
                $Last_DIF_Revision_Date = $coll->{'meta'}{'revision-date'};
            }

            if ( defined $entry_id && defined $native_id ) {
                $dataSetIds->{$dataset}{$version}{'entry_id'} = $entry_id;
                $dataSetIds->{$dataset}{$version}{'native_id'} = $native_id;
            }

            # Determine the date of the last update for the dataset's Entry_ID
            $date_of_last_dif_update = ( exists $last_dif_update->{$entry_id} ) ?
                $last_dif_update->{$entry_id} : 0;

            unless ( $Last_DIF_Revision_Date ) {
                S4P::logger( 'ERROR', "Failed to obtain $Last_DIF_Revision_Date from DIF for $entry_id" );
                next;
            }

            # Convert revision date of the form yyyy-mm-ddThh:mi:ss to yyyymmdd.
            ( my $date_of_last_dif_revision = $Last_DIF_Revision_Date ) =~
              s/T\d\d.*Z?$//;
            $date_of_last_dif_revision =~ s/-//g;

            # Compare the revision date to the date of the last update performed
            if ( $date_of_last_dif_revision > $date_of_last_dif_update ) {

                # The DIF file was revised since our last update, so perform
                # a new update.
                S4P::logger(
                             'INFO',
                             "Found DIF for $entry_id revised on "
                               . $Last_DIF_Revision_Date
                           );

                # Get the entire DIF file contents from GCMD site, as XML text
                my $full_dif_url = $cmr_rest_base_url . '.dif10?provider=' .
                    $CFG::CMR_PROVIDER . '&entry_id=' . $entry_id . '&pretty=true';

                my $full_dif_req = HTTP::Request->new( 'GET',
                                          $full_dif_url,
                                          [Echo_Token => $cmrToken]
                                        );
                $dif = undef;

                # Pause before fetching again in order to prevent too
                # many fetches in a short period of time.
                sleep $CFG::sleep_seconds;
              LOOP2:
                for ( my $attempt = 1 ;
                      $attempt <= $CFG::max_fetch_attempts ;
                      $attempt++ )
                {
                    my $response = $ua->request($full_dif_req);
                    if ($response->is_success) {
                        $dif = $response->content;
                    }
                    last LOOP2 if ( defined $dif );
                    S4P::logger(
                               'INFO',
                               "Unsuccessful attempt [$attempt of "
                                 . "$CFG::max_fetch_attempts] to fetch full DIF"
                                 . " for $entry_id, sleeping..."
                               );
                    sleep $CFG::sleep_seconds;
                }
                unless ( $dif ) {
                    S4P::logger( 'ERROR',
                                "Failed to fetch full DIF" . " for $entry_id" );
                    next;
                }

                # If the DIF defines a default namespace, then every XPATH
                # expression will need to use a prefix (according to the
                # documentation for XML::LibXML::Node). In order to avoid
                # forcing this script or any other script that parses
                # the fetched DIF from having to use a prefix in every
                # XPATH expression, we will use a hack here and delete any
                # definition of a default namespace.
                $dif =~ s/xmlns=".+"//g;
                $dif =~ s/xsi:schemaLocation=".+"//;

                # extract <DIF> node
                my $difParser = XML::LibXML->new();
                $difParser->keep_blanks(0);
                my $difDom = $difParser->parse_string($dif);
                my $difDoc = $difDom->documentElement();
                my $encoding = $difDom->encoding();
                my $xmlVersion = $difDom->version();
                my ($difNode) = $difDoc->findnodes('//DIF');
                $dif = encode( $encoding, $difNode->toString(1) );
                $dif = "<?xml version=\"$xmlVersion\" encoding=\"$encoding\"?>\n" . $dif;

                my $dif_out;

                if ($document_url) {

                    # Parse the XML document
                    my $xmlParser = XML::LibXML->new();
                    $xmlParser->keep_blanks(0);
                    my $dom      = $xmlParser->parse_string($dif);
                    my $doc      = $dom->documentElement();
                    # my $encoding = $dom->encoding();

                    # Create a new Related_URL node containing the URL
                    # for the collection readme document
                    my $newNode = XML::LibXML::Element->new('Related_URL');
                    my $contentType =
                      XML::LibXML::Element->new('URL_Content_Type');
                    my $mainType = XML::LibXML::Element->new('Type');
                    $mainType->appendText('VIEW RELATED INFORMATION');
                    my $subType = XML::LibXML::Element->new('Subtype');
                    $subType->appendText('USER\'S GUIDE');
                    my $urlLink = XML::LibXML::Element->new('URL');
                    $urlLink->appendText($document_url);
                    my $docDescription =
                      XML::LibXML::Element->new('Description');
                    $docDescription->appendText('product README file');
                    $contentType->appendChild($mainType);
                    $contentType->appendChild($subType);
                    $newNode->appendChild($contentType);
                    $newNode->appendChild($urlLink);
                    $newNode->appendChild($docDescription);

                    # Check if the DIF already has a product README url
                    my ($sumNode)    = $doc->findnodes('//Summary');
                    my @RelUrlNodes  = $doc->findnodes('//Related_URL');
                    my $original_url = '';
                    my $documentURLNode;

                    # Locate the node with document url
                    foreach my $node (@RelUrlNodes) {
                        my @subtypeNodes =
                          $node->findnodes('./URL_Content_Type/Subtype');
                        foreach my $subtypeNode (@subtypeNodes) {
                            my $subtype = $subtypeNode->textContent;
                            if ( $subtype =~ /USER'S GUIDE/i ) {
                                my @urlNodes = $node->findnodes('./URL');
                                $original_url    = $urlNodes[0]->textContent;
                                $documentURLNode = $node;
                                last;
                            }
                        }
                    }

                    # No replacement needed if the original DIF document url is
                    # the same as the s4pa document url
                    if ( $original_url eq $document_url ) {
                        $dif_out = encode( $encoding, $dom->toString(1) );

                    }
                    else {

                        # Otherwise, either replace the DIF url with s4pa
                        # document url if original DIF has a different document
                        # url...
                        if ( defined $documentURLNode ) {
                            $doc->insertBefore( $newNode, $documentURLNode );
                            $doc->removeChild($documentURLNode);

                          # ...or insert the s4pa document url either before the
                          # first Related_URL node or after the Summary node.
                        }
                        elsif (@RelUrlNodes) {
                            $doc->insertBefore( $newNode, $RelUrlNodes[0] );
                        }
                        elsif ($sumNode) {
                            $doc->insertAfter( $newNode, $sumNode );
                        }
                        else {
                            S4P::logger( 'WARN',
                                 "Summary and Related_URL fields missing in DIF"
                            );
                            $doc->appendChild($newNode);
                        }
                        $dif_out = encode( $encoding, $dom->toString(1) );
                    }
                }
                else {
                    $dif_out = $dif;
                }

                # Create an output work order for this dataset/Entry_ID
                my $wo_name = 'CONVERT_DIF.' . $entry_id . '.wo';
                unless ( S4P::write_file( $wo_name, $dif_out ) ) {
                    S4P::logger( 'ERROR', "Failed to write $wo_name\n" );
                    next;
                }

                # Set the date that we performed the update.
                # If another dataset/version corresponds to the same Entry_ID,
                # we will not have to fetch the same DIF again
                $last_dif_update->{$entry_id} = $yyyymmdd_today;
                $update_count++;
            }
            else {    # Don't consider the DIF to have been updated
                S4P::logger(
                             'INFO',
                             "DIF for $entry_id last revised on "
                               . $Last_DIF_Revision_Date
                           ) if $verbose;
            }
        }
    }    # END foreach my $version
}    # END foreach my $dataset

# update %cmr_collection_id in configuration file with entry_id and native_id
UpdateDifInfoConfig($dif_info_file, $dataSetIds);

# If any updates were found, save the date of the last update for
# every Entry_ID value
write_last_updates( $last_update_filename, $last_dif_update ) if $update_count;
S4P::logger(
             'INFO',
             "Found updates for $update_count of " .
               keys(%CFG::cmr_collection_id) . " datasets"
           );

exit 0;

sub usage {
    my $usage = "
Usage: $0 [-c config_file] work_order
  -c config_file:  Configuration file containing dataset to Entry_ID map (default=../s4pa_fetch_revised_DIFs.cfg)
";
    S4P::perish( 1, $usage );
}

sub read_last_updates {
    my ($filename) = @_;

    my %last_update;

    # If $filename does not exist, return a reference to an empty hash
    return \%last_update unless -f $filename;

    # Read the file containing the date of last update for each Entry_ID.
    # Expect each line to contain two fields separated by whitespace.
    # The first field is the Entry_ID, the second is a date of the
    # form yyyy-mm-dd
    open( LASTUPDATE, "< $filename" )
      or S4P::perish( 1, "Could not open $filename for reading\n" );
    my ( $entry_id, $update_date );
    while (<LASTUPDATE>) {
        chomp();
        next unless length();    # Skip empty lines
        ( $entry_id, $update_date ) = split( /\s+/, $_ );

        # Eliminate '-' from date, e.g. convert 2006-01-02 to 20060102
        $update_date =~ s/-//g;
        $last_update{$entry_id} = $update_date;
    }
    close(LASTUPDATE);

    return \%last_update;
}

sub write_last_updates {
    my ( $filename, $last_update_ref ) = @_;

    # Write the file containing the date of last update for each Entry_ID.
    # Each line contains an Entry_ID and a date, separated by a tab character.
    open( LASTUPDATE, "> $filename" )
      or S4P::perish( 1, "Could not open $filename for writing\n" );
    my %last_update = %$last_update_ref;
    foreach my $entry_id ( sort keys %last_update ) {

        # Save update date of the form yyyymmdd as yyyy-mm-dd
        my $update_date = sprintf "%s\-%s\-%s",
          unpack( "A4 A2 A2", $last_update{$entry_id} );
        print LASTUPDATE "$entry_id\t$update_date\n";
    }
    close(LASTUPDATE);

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
    $tokenNode->appendTextChild('client_id', 'GES_DISC');
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

sub UpdateDifInfoConfig {
    my ( $file, $dataSetIds ) = @_;

    my $config;
    my $type_hash;
    #  This whole routine was borrowed from UpdatePublishCmrConfig of
    #  s4pa_publish_cmr.pl 

    # Iterate through every symbol in the CFG namespace
    foreach my $name (keys %CFG::) {
        # We can't assign to a typeglob if 'use strict' is in effect,
        # so disable it in this loop.
        no strict 'vars';

        # Skip special symbols whose name begins with an underscore
        next if $name =~ /^_/;

        # Skip cmr_collection_id values already in the config file
        next if $name eq 'cmr_collection_id';

        # The CFG namespace also references symbols in the main namespace!
        # Skip those too, or else face an endless loop.
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
            $type_hash->{$name} = 'LIST';
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

    # The hash referenced by $config now contains all of the information
    # in the configuration file read by this script. Now update the
    # cmr_collection_id for the specified dataset.

    $config->{cmr_collection_id} = $dataSetIds;
    $type_hash->{cmr_collection_id} = 'HASH';
    $config->{__TYPE__} = $type_hash if (defined $type_hash);

    # Replace the configuration file with a new configuration file
    # under both housekeeper and dif_fetch stations.
    my $station = dirname( $file );
    S4PA::WriteStationConfig( basename( $file ), $station , $config );
    $station =~ s/dif_fetcher/housekeeper/;
    S4PA::WriteStationConfig( basename( $file ), $station , $config );

    return;
}

