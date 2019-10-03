#!/usr/bin/perl

=head1 NAME

s4pa_get_cmr_access.pl - script for CMR access verification

=head1 SYNOPSIS

s4pa_get_cmr_access.pl
[B<-f> I<config_file>]
[B<-b> I<browse_description>]

=head1 DESCRIPTION

s4pa_get_cmr_access.pl accepts a CMR publications configuration file and
updates it with CMR access information (public or restricted) for each
shortname/version pair by querying CMR. It also adds the browse description
for each dataset with a browse file.

Usage of this script requires a CMR account that has provider context.

Since this script uses the HTTP protocol to connect to the CMR server,
the HTTP_proxy environment variable must be set to the URL of an HTTP proxy
server if this script is to be run behind a firewall.

=head1 ARGUMENTS

=over 4

=item B<-f> I<config_file>

A configuration file for used for publishing granules to CMR. It should
contain:

=over 4

=item I<%DATA_ACCESS>: S4PA access hash, keys are dataset shortnames, values
are hashes whose keys are version strings and values are "public",
"private", or "restricted".

=item I<$DESTDIR>: Destination directory published files will be transferred to

=item I<$HOST>: Host that published files will be transferred to

=item I<$UNRESTRICTED_ROOTURL>: Root URL for access to unrestricted files

=item I<$RESTRICTED_ROOTURL>: Root URL for access to restricted files

=item I<$TYPE>: Publishing type (insert or delete)

=item I<$INSTANCE_NAME>: Identifier string for the S4PA instance

=back

=item B<-m> test|ops

CMR mode (test or ops)

=item B<-u> I<CMR username>

Username for the CMR data provider account

=item B<-p> I<CMR password>

Password for the CMR data provider account

=back

=item B<-b> I<browse_description>

A configuration file containing the browse description for each dataset.
Only the dataset with browse file needs to be added in the BROWSE_DESCRIPTION
hash. Keys are dataset shortname, values are hashes whose keys are version
strings and values are the description string.

=over 4

=item Example of Browse Description hash in configuration file:

=item %BROWSE_DESCRIPTION  = (
                        "AIRABRAD" => {
                                        "003" => "AIRS Level 2 Physical "
                                      }
                      );

=back

=head1 AUTHORS

M. Hegde
E. Seiler

=cut

################################################################################
# $Id: s4pa_get_cmr_access.pl,v 1.5 2019/05/06 15:48:00 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################
use strict;
use Getopt::Std;
use Safe;
use LWP::UserAgent;
use XML::LibXML;
use Sys::Hostname;
use S4P;
use S4PA;
use File::Basename;
use Clavis;
use Data::Dumper;
use Cwd;
use vars qw( $opt_f $opt_b $opt_d $opt_t );

my $usage =
    "Usage: " . basename($0) . "-f s4pa_insert_cmr.cfg [-b browse_config_file]\n";
# Check for required options
getopts( 'f:b:d:t' );
unless ( defined $opt_f ) {
    S4P::perish( 1, $usage );
}

# Retrieve values from configuration file
my $cfg_file = $opt_f;
my $cpt = new Safe 'CFG';
$cpt->rdo( $cfg_file ) or
    S4P::perish( 2, "Cannot read config file $cfg_file in safe mode: $!\n" );

# Assuming we are in a working directory under publish_cmr
# then the 's4pa_dif_info.cfg' file should be under the directory
# ../../other/housekeeper
# my $dif_info_path = dirname(dirname(getcwd()));
my $dif_info_path = "$CFG::cfg_s4pa_root/other/dif_fetcher";

# Retrieve values from DIF info configuration file
my $dif_info_file = "$dif_info_path/s4pa_dif_info.cfg";
my $dif_info_cpt = new Safe 'DIF_INFO_CFG';
$dif_info_cpt->rdo( $dif_info_file ) or
    S4P::perish( 2, "Cannot read config file $dif_info_file in safe mode: $!\n" );

my $logger = S4PA::CreateLogger( $CFG::cfg_logger{FILE},
    $CFG::cfg_logger{LEVEL} ) if ( %CFG::cfg_logger );
$logger->info( "Running $0 for $cfg_file at " . scalar(localtime) )
    if defined $logger;

# Retrieve description from the browse configuration file if one is specified
my $browse_cpt;
if ( $opt_b ) {
    my $browse_cpt = new Safe 'BROWSE_CFG';
    $browse_cpt->rdo( $opt_b ) or
        S4P::perish( 2, "Cannot read browse config file $opt_b in safe mode: $!\n" );
}

my $debug = $opt_d;

# If tracing, make generated xml readable
my $readable = $opt_t ? 1 : 0;

# Log in as a CMR client and obtain a session token
my $username = $CFG::CMR_USERNAME;
my $password = Clavis::decrypt( $CFG::CMR_PASSWORD ) if ( defined $CFG::CMR_PASSWORD );
my $provider = $CFG::CMR_PROVIDER;
my $endpoint_uri = $CFG::CMR_TOKEN_URI;

unless ( defined $username ) {
    S4P::perish( 1, "No CMR user name (\$CMR_USERNAME) provided in $cfg_file" );
}
unless ( defined $password ) {
    S4P::perish( 1, "No encrypted CMR password (\$CMR_PASSWORD) provided in $cfg_file" );
}
unless ( defined $provider ) {
    S4P::perish( 1, "No provider ID (\$CMR_PROVIDER) provided in $cfg_file" );
}

my $ua = LWP::UserAgent->new;

my $token = login($username, $password, $provider);

# Submit the first query and obtain $resultSetGuid, which will be used
# in the second query
my $queryResponse = execute_catalog_query($provider, $token);
S4P::perish(4, "Could not get datasets information\n")
    unless $queryResponse;

# A hash to store access dataSetId for each dataset/version
my $dataSetIds = parse_catalog_query_results($queryResponse, \%CFG::DATA_ACCESS);

# A hash to store access info for each dataset/version
# according to which data sets are visible to a
# CMR user once the CMR Access Control Lists are applied.
my $accessHash = getPermittedCatalogItems($provider, $token);

# If there is a conflict between %CFG::DATA_ACCESS and $accessHash,
# change the CMR public/private status to match what S4PA has.
# (Unless $CFG::CREATE_RULES is set, we will control access/visibility by
# changing the value of the restrictionFlag in the collection metadata.)
update_cmr_access(\%CFG::DATA_ACCESS, $accessHash, $dataSetIds)
    if $CFG::CREATE_RULES;

# Update the PublishCmr configuration
UpdatePublishCmrConfig( $cfg_file, $accessHash, $opt_b );

exit 0;


sub login {

    my ($username, $pwd, $provider) = @_;

    my $hostname = Sys::Hostname::hostname();
    my $packed_ip_address = (gethostbyname($hostname))[4];
    my $ip_address = join('.', unpack('C4', $packed_ip_address));

    my $tokenNode = XML::LibXML::Element->new('token');
    $tokenNode->appendTextChild('username', $username);
    $tokenNode->appendTextChild('password', $password);
    $tokenNode->appendTextChild('client_id', 'GES_DISC');
    $tokenNode->appendTextChild('user_ip_address', $ip_address);
    $tokenNode->appendTextChild('provider', $provider);

    my $id;
    my $tokenUrl = $endpoint_uri . 'tokens';
    my $request = HTTP::Request->new( 'POST',
                                      $tokenUrl,
                                      [Content_Type => 'application/xml'],
                                      $tokenNode->toString() );
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

sub execute_catalog_query {
    my ($provider, $token) = @_;

    # Construct and submit a query to get all of the CMR metadata for the
    # collection corresponding to $dataset

    my $datasetsUrl = $CFG::CMR_ENDPOINT_URI;
    if ($CFG::CMR_ENDPOINT_URI =~ /cmr/i) {
        $datasetsUrl =~ s#ingest/##;
        $datasetsUrl .= 'search/collections?provider=' . $provider . '&page_size=2000';
    } else {
        $datasetsUrl .= 'providers/' . $provider . '/datasets';
    }
    my $request = HTTP::Request->new( 'GET',
                                      $datasetsUrl,
                                      [Echo_Token => $token]
                                    );
    my $response = $ua->request($request);
    if ($response->is_success) {
        return ($response->content);
    }
}


sub parse_catalog_query_results {
    my ($queryResponse, $dataHash) = @_;

    # Parse the xml from the datasets query response to get the
    # dataSetId for each shortname/version combination in the query response
    # whose shortName exists in $dataHash

    my $xmlParser = XML::LibXML->new();
    my $dom = $xmlParser->parse_string( $queryResponse );
    my $doc = $dom->documentElement();

    foreach my $node ( $doc->findnodes( '//reference' ) ) {

        my ( $nameNode ) = $node->getChildrenByTagName( 'name' );
        next unless defined $nameNode;
        my $longName = $nameNode->textContent();

        # If there are no versions of this ShortName, go to the next one
        my ( $locationNode ) = $node->getChildrenByTagName( 'location' );
        next unless defined $locationNode;
        my $locationUrl = $locationNode->textContent() . ".echo10";

        my $request = HTTP::Request->new( 'GET',
                                          $locationUrl,
                                          [Content_Type => 'application/xml',
                                           Echo_Token => $token]
                                        );
        my $response = $ua->request($request);
        unless ($response->is_success) {
            print STDERR "Failed to get dataset info for '$longName' at $locationUrl\n";
            next;
        }
        my $xml = $response->content;
        my $cDom = $xmlParser->parse_string( $xml );
        my $cDoc = $cDom->documentElement();
        my ($cNode) = $cDoc->findnodes( '/Collection' );
        unless ($cNode) {
            print STDERR "Did not find Collection in dataset info for '$longName' at $locationUrl\n";
            next;
        }

        my ( $dataSetIdNode ) = $cNode->getChildrenByTagName( 'DataSetId' );
        next unless defined $dataSetIdNode;

        # If there are no versions of this ShortName, go to the next one
        my ( $shortNameNode ) = $cNode->getChildrenByTagName( 'ShortName' );
        next unless defined $shortNameNode;

        my ( $versionIdNode ) = $cNode->getChildrenByTagName( 'VersionId' );
#        my ( $flagNode ) = $cNode->getChildrenByTagName( 'RestrictionFlag' );

        my $dataSetId = $dataSetIdNode->textContent();
        my $shortName = $shortNameNode->textContent();
        my $versionId = $versionIdNode ? $versionIdNode->textContent() : '';
#        my $restrictionFlag = $flagNode ? $flagNode->textContent() : '';

        next unless exists $dataHash->{$shortName};

        $dataSetIds->{$shortName}{$versionId} = $dataSetId;
    }

    return $dataSetIds;
}


sub getPermittedCatalogItems {
    # Determine the visibility of all of the provider's collections
    # as determined by the CMR Access Control Lists

    my ($provider, $token) = @_;

    # Updates accessHash with a visibility value ('restricted' or 'public')
    # for all collections
    my $accessHash = {};
    foreach my $shortName (keys %CFG::DATA_ACCESS ) {
        foreach my $versionId (keys %{$CFG::DATA_ACCESS{$shortName}}) {
            # By default, each dataset is restricted
            $accessHash->{$shortName}{$versionId} = 'restricted';
        }
    }

    my $serviceName;

    my $aclsUrl = $endpoint_uri . 'acls?object_identity_type=CATALOG_ITEM&provider_id='. $provider;
    my $request = HTTP::Request->new( 'GET',
                                      $aclsUrl,
                                      [Content_Type => 'application/xml',
                                       Echo_Token => $token]
                                    );
    my $response = $ua->request($request);
    unless ($response->is_success) {
        S4P::perish( 2, "Error requesting ACLs via $aclsUrl" );
    }

    my %visibleDatasets;
    my $xml = $response->content;
    my $xmlParser = XML::LibXML->new();
    my $dom = $xmlParser->parse_string( $xml );
    my $doc = $dom->documentElement();


    # Iterate through all ACLs
    foreach my $node ( $doc->findnodes( '//reference' ) ) {

        my ( $locationNode ) = $node->getChildrenByTagName( 'location' );
        next unless defined $locationNode;
        my $locationUrl = $locationNode->textContent();

        $request = HTTP::Request->new( 'GET',
                                       $locationUrl,
                                       [Content_Type => 'application/xml',
                                        Echo_Token => $token]
                                     );
        $response = $ua->request($request);
        unless ($response->is_success) {
            print STDERR "Failed to get ACL at $locationUrl\n";
            next;
        }

        my $xml = $response->content;
        my $aDom = $xmlParser->parse_string( $xml );
        my $aDoc = $aDom->documentElement();
        my ($aNode) = $aDoc->findnodes( '/acl' );
        unless ($aNode) {
            print STDERR "Did not find acl at $locationUrl\n";
            next;
        }
        my ( $ciiNode ) = $aNode->getChildrenByTagName( 'catalog_item_identity' );
        next unless defined $ciiNode;
        my ( $caNode ) = $ciiNode->getChildrenByTagName( 'collection_applicable' );

        # Skip any ACL that does not apply to collections.
        next unless $caNode && $caNode->textContent() eq 'true';

        my ( $acesNode ) = $aNode->getChildrenByTagName( 'access_control_entries' );
        next unless $acesNode;
        my @aceNodes = $acesNode->getChildrenByTagName('ace');

        my $readAccess;
        foreach my $aceNode (@aceNodes) {
            # We are only interested in entries associated with a
            # UserAuthorizationTypeSid (i.e. entries that apply to all users
            # of a certain type), and not entries associated with a
            # GroupSid (i.e. entries that apply to all users who are a member
            # of a certain group).
            my ($uatNode) = $aceNode->findnodes('./sid/user_authorization_type_sid/user_authorization_type');
            next unless $uatNode;

            # The UserAuthorizationType can be either "GUEST' or 'REGISTERED',
            # Although a collection could be visible to guests but not to
            # registered users, we will consider such a collection to be visible,
            # i.e. unrestricted.
            #next unless $uatNode->textContent() eq 'GUEST';

            # Skip the cases where no permissions are allowed.
            my @permNodes = $aceNode->findnodes('./permissions/permission');
            next unless @permNodes;

            # There can be more than one permission allowed from the set
            # 'CREATE', 'READ', 'UPDATE', 'DELETE', and 'ORDER'. We are only
            # concerned with 'READ', i.e. "view".
            foreach my $permNode (@permNodes) {
                $readAccess = 1 if $permNode->textContent() eq 'READ';
            }
        }
        if ($readAccess) {
            my @collNodes = $ciiNode->findnodes('./collection_identifier/collection_ids/collection_id');
            # ACL has an entry for a UserAuthorizationType that has READ access
            # Mark all collections in that ACL as 'visible'
            foreach my $collNode (@collNodes) {
                my ( $dsNode ) = $collNode->getChildrenByTagName( 'data_set_id' );
                my $dataSetId  = $dsNode->textContent() if $dsNode;
                my ( $snNode ) = $collNode->getChildrenByTagName( 'short_name' );
                my $shortName  = $snNode->textContent() if $snNode;
                next unless $shortName;
                my ( $vNode )  = $collNode->getChildrenByTagName( 'version' );
                my $versionId  = $vNode->textContent() if $vNode;
                next unless $versionId;

                $visibleDatasets{$shortName}{$versionId} = 1;
            }
        }
    }

    # Set access to 'public' for each dataSetId that is visible
    # to the public according to the ACLs
    foreach my $shortName (keys %{$accessHash}) {
        foreach my $versionId (keys %{$accessHash->{$shortName}}) {
            my $collection_shortname = $DIF_INFO_CFG::cmr_collection_id{$shortName}{$versionId}{'short_name'};
            my $collection_version = $DIF_INFO_CFG::cmr_collection_id{$shortName}{$versionId}{'version_id'};
            $accessHash->{$shortName}{$versionId} = 'public'
                if exists $visibleDatasets{$collection_shortname}{$collection_version};
        }
    }

    return $accessHash;
}


sub update_cmr_access {
    my ($s4pa_access, $cmr_access, $dataSetIds) = @_;

    # Change the %CMR_ACCESS value from 'restricted' to 'public' for any
    # collection whose %DATA_ACCESS value is 'public'.
    # If we wanted to change the %CMR_ACCESS value from 'public' to
    # 'restricted' for any collection whose %DATA_ACCESS value is 'restricted',
    # we would have to delete all all ACLs that allow access to that
    # collection, which is a bit trickier to do, so that is not done here.

    # Get the provider GUID
    my $providersUrl = $endpoint_uri . '/providers/' . $provider;
    my $request = HTTP::Request->new( 'GET',
                                      $providersUrl,
                                      [Content_Type => 'application/xml',
                                       Echo_Token => $token]
                                    );
    my $response = $ua->request($request);
    unless ($response->is_success) {
        S4P::perish( 2, "Error requesting provider info via $providersUrl" );
    }

    my $xml = $response->content;
    my $xmlParser = XML::LibXML->new();
    my $dom = $xmlParser->parse_string( $xml );
    my $doc = $dom->documentElement();
    my ($idNode) = $doc->findnodes('/provider/id');
    my $providerGuid = $idNode->textContent if $idNode;

    # Get the GUID for the 'Administrators' group
    # Expect provider $provider to be a member of the
    # group named 'Administrators'.
    my $groupGuid;
    my $groupsUrl = $endpoint_uri . '/groups?owner_id=' . $providerGuid;
    $request = HTTP::Request->new( 'GET',
                                   $groupsUrl,
                                   [Content_Type => 'application/xml',
                                    Echo_Token => $token]
                                 );
    $response = $ua->request($request);
    unless ($response->is_success) {
        S4P::perish( 2, "Error requesting group info via $groupsUrl" );
    }

    $xml = $response->content;
    my $gdom = $xmlParser->parse_string( $xml );
    my $gdoc = $gdom->documentElement();
    my @groupNodes = $gdoc->findnodes('/groups/group');
    foreach my $groupNode (@groupNodes) {
        my ($nameNode) = $groupNode->getChildrenByTagName( 'name' );
        my $name = $nameNode->textContent() if $nameNode;
        if ($name eq 'Administrators') {
            my ($idNode) = $groupNode->getChildrenByTagName( 'id' );
            $groupGuid = $idNode->textContent if $idNode;
            last if $groupGuid;
        }
    }

    foreach my $shortName (keys %$cmr_access) {
        my @versionIds = keys %{$cmr_access->{$shortName}};
        foreach my $versionId (@versionIds) {
            my $s4paAccessType;
            my $cmrAccessType = $cmr_access->{$shortName}->{$versionId};
            if (exists $s4pa_access->{$shortName}->{$versionId}) {
                $s4paAccessType = $s4pa_access->{$shortName}->{$versionId};
            } elsif (((scalar @versionIds) == 1) &&
                     (exists $s4pa_access->{$shortName}->{''}) &&
                     ((scalar (keys %{$s4pa_access->{$shortName}})) == 1)) {
                # If shortName is unversioned in S4PA and there is a single
                # version in CMR, assume they are the same version.
                $s4paAccessType = $s4pa_access->{$shortName}->{''};
            } else {
                # CMR has a version for the shortName that S4PA does not,
                # or shortName is unversioned in S4PA and there is more
                # than one version in CMR, preventing us from assuming that
                # the CMR version is the same as the unversioned S4PA version.
                # Do not update the access type.
                next;
            }
            next if ($cmrAccessType eq $s4paAccessType);
            if (($s4paAccessType eq "public") &&
                ($cmrAccessType eq "restricted")) {
#                 # Collection is public in S4PA but restricted in CMR.
#                 # Since S4PA access is determined at deployment time,
#                 # for any collection intended to be published to CMR,
#                 # the access types should agree, so make any restricted
#                 # collection public by creating a CMR access rule
#                 # that allows the collection to be viewed.

#                 my $description = "Allow view for $shortName $versionId";
                my $collection_shortname = $DIF_INFO_CFG::cmr_collection_id{$shortName}{$versionId}{'short_name'};
                my $collection_version = $DIF_INFO_CFG::cmr_collection_id{$shortName}{$versionId}{'version_id'};
                my $dataSetId = $dataSetIds->{$collection_shortname}{$collection_version};

                # Create a new ACL that will provide View (read) permission
                # for the collection with shortname $shortName and version
                # $versionId

                # access_control_entries element specifies read access for
                # Registered Users, Guest Users, and the Administrators
                # group

                # $catalogItemIdentityName is the name of the new Catalog
                # Item ACL.
                my $catalogItemIdentityName = "View_${collection_shortname}_$collection_version";

                # The Catalog Item ACL will apply to the collection with
                # shortName $shortName and version $versionId, and its
                # granules.


                my $aclXml = <<ENDXML;
   <acl>
     <access_control_entries type="array">
       <ace>
         <sid>
           <group_sid>
             <group_guid>$groupGuid</group_guid>
           </group_sid>
         </sid>
         <permissions type="array">
           <permission>READ</permission>
         </permissions>
       </ace>
       <ace>
         <sid>
           <user_authorization_type_sid>
             <user_authorization_type>REGISTERED</user_authorization_type>
           </user_authorization_type_sid>
         </sid>
         <permissions type="array">
           <permission>READ</permission>
         </permissions>
       </ace>
       <ace>
         <sid>
           <user_authorization_type_sid>
             <user_authorization_type>GUEST</user_authorization_type>
           </user_authorization_type_sid>
         </sid>
         <permissions type="array">
           <permission>READ</permission>
         </permissions>
       </ace>
     </access_control_entries>
     <catalog_item_identity>
       <name>$catalogItemIdentityName</name>
       <provider_guid>$providerGuid</provider_guid>
       <collection_applicable type="boolean">true</collection_applicable>
       <collection_identifier>
         <collection_ids type="array">
           <collection_id>
             <data_set_id>$dataSetId</data_set_id>
             <short_name>$collection_shortname</short_name>
             <version>$collection_version</version>
           </collection_id>
         </collection_ids>
       </collection_identifier>
       <granule_applicable type="boolean">true</granule_applicable>
       <granule_identifier>
         <granule_ur_patterns type="array"/>
       </granule_identifier>
     </catalog_item_identity>
   </acl>
ENDXML

                # Create the new ACL
                my $createUrl = $endpoint_uri . 'acls';
                $request = HTTP::Request->new( 'POST',
                                               $createUrl,
                                               [Content_Type => 'application/xml'],
                                               $aclXml );
                $response = $ua->request($request);
                unless ($response->is_success) {
                    S4P::perish( 2, "Error posting ACL for shortname $collection_shortname version $collection_version via $createUrl" );
                }
            }
            elsif (($s4paAccessType eq "restricted") &&
                   ($cmrAccessType eq "public")) {
                # Change the CMR collection access from public to restricted.
                # To do this we would have to delete all ACLs that allow
                # access for the collection. If we created an ACL to make
                # the restricted collection publc in the code above, then
                # we would delete that ACL here. But if there are any other
                # ACLs in existence that allow access, then the collection
                # will remain public.
                # For now we will not attempt to restrict the access here.
            }
        }
    }
}


sub UpdatePublishCmrConfig {
    my ( $file, $accessHash, $browse ) = @_;

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
        # We can't assign to a typeglob if 'use strict' is in effect,
        # so disable it in this loop.
        no strict 'vars';

        # Skip special symbols whose name begins with an underscore
        next if $name =~ /^_/;

        # Skip any CMR_ACCESS values already in the config file
        next if $name eq 'CMR_ACCESS';

        # The CFG namespace also references symbols in other namespaces,
        # including the main namespace.
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

            # S4PA::WriteStationConfig has been naming array type as 'LIST',
            # we just never have an array type parameter in configuration file
            # until the new @cfg_psa_skip was added.
            # $type_hash->{$name} = 'ARRAY';
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
    # in the configuration file read by this script. Now add any new values
    # that are desired.

    if ( defined $browse ) {
        $config->{BROWSE_DESCRIPTION} = \%BROWSE_CFG::BROWSE_DESCRIPTION;
        $type_hash->{BROWSE_DESCRIPTION} = 'HASH';
    }
    $config->{CMR_ACCESS} = $accessHash;
    $type_hash->{CMR_ACCESS} = 'HASH';
    $config->{__TYPE__} = $type_hash if (defined $type_hash);
    $config->{BROWSEDIR} = $CFG::BROWSEDIR if (defined $CFG::BROWSEDIR);

    # Replace the configuration file with a new configuration file.
    S4PA::WriteStationConfig( basename( $file ), dirname( $file ), $config );

    return;
}


