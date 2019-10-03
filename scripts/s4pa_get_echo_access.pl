#!/usr/bin/perl

=head1 NAME

s4pa_get_echo_access.pl - script for ECHO access verification

=head1 SYNOPSIS

s4pa_get_echo_acces.pl
[B<-f> I<config_file>]
[B<-b> I<browse_description>]

=head1 DESCRIPTION

s4pa_get_echo_access.pl accepts an ECHO publications configuration file and
updates it with ECHO access information (public or restricted) for each
shortname/version pair by querying ECHO. It also adds the browse description
for each dataset with a browse file.

Usage of this script requires an ECHO account that has provider context.

Since this script uses the HTTP protocol to connect to the ECHO server,
the HTTP_proxy environment variable must be set to the URL of an HTTP proxy
server if this script is to be run behind a firewall.

=head1 ARGUMENTS

=over 4

=item B<-f> I<config_file>

A configuration file for used for publishing granules to ECHO. It should
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

ECHO mode (test or ops)

=item B<-u> I<ECHO username>

Username for an ECHO data provider account

=item B<-p> I<ECHO password>

Password for the ECHO data provider account

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
# $Id: s4pa_get_echo_access.pl,v 1.32 2019/05/06 15:48:00 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################
use strict;
use Getopt::Std;
use Safe;
use SOAP::Lite;
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
    "Usage: " . basename($0) . "-f s4pa_insert_echo.cfg [-b browse_config_file]\n";
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

# assuming we are in a working directory under publish_echo
# then 's4pa_dif_info.cfg' file should be under ../../other/housekeeper directory
my $dif_info_path = dirname(dirname(getcwd()));
# Retrieve values from DIF info configuration file
my $dif_info_file = "$dif_info_path/other/housekeeper/s4pa_dif_info.cfg";
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

SOAP::Lite->import(+trace => 'all') if $opt_t;

# If tracing, make generated xml readable
my $readable = $opt_t ? 1 : 0;

# A hash to store access info for each dataset/version
my $accessHash = {};
# A hash to store access dataSetId for each dataset/version
my $dataSetIds = {};
# A hash to store restriction status for each restricted dataSetId
my %restrictedDataSetIds;

# Log in as an ECHO client and obtain a session token
my $username = $CFG::ECHO_USERNAME;
my $password = Clavis::decrypt( $CFG::ECHO_PASSWORD ) if ( defined $CFG::ECHO_PASSWORD );
my $provider = $CFG::ECHO_PROVIDER;
my $endpoint_uri = $CFG::ECHO_ENDPOINT_URI;

unless ( defined $username ) {
    S4P::perish( 1, "No ECHO user name (\$ECHO_USERNAME) provided in $cfg_file" );
}
unless ( defined $password ) {
    S4P::perish( 1, "No encrypted ECHO password (\$ECHO_PASSWORD) provided in $cfg_file" );
}
unless ( defined $provider ) {
    S4P::perish( 1, "No provider ID (\$ECHO_PROVIDER) provided in $cfg_file" );
}

my $echo_version = 10;
$echo_version = $1 if $endpoint_uri =~ m#/echo-v(\d+)#;
my $echo_uri     = "http://echo.nasa.gov/echo/v${echo_version}";
my $types_uri    = "http://echo.nasa.gov/echo/v${echo_version}/types";

my $TokenElement = login($username, $password, $provider);

# Submit the first query and obtain $resultSetGuid, which will be used
# in the second query
my ($resultSetGuid, $hits) = execute_catalog_query($provider);
S4P::perish(4, "Could not perform first query\n")
    unless $resultSetGuid;
S4P::perish(5, "Could not find any datasets for provider $provider\n")
    unless $hits;

my $queryResponse = get_catalog_query_results($resultSetGuid);

# Parse the query response and update accessHash
$dataSetIds = parse_catalog_query_results($queryResponse,
                                          \%CFG::DATA_ACCESS,
                                          $accessHash);

# Update accessHash according to which data sets are visible to an
# ECHO user once the ECHO Access Control Lists are applied.
$accessHash = getPermittedCatalogItems($dataSetIds, $accessHash);

# If there is a conflict between %CFG::DATA_ACCESS and $accessHash,
# change the ECHO public/private status to match what S4PA has.
# (Unless $CFG::CREATE_RULES is set, we will control access/visibility by
# changing the value of the restrictionFlag in the collection metadata.)
update_echo_access(\%CFG::DATA_ACCESS, $accessHash, $dataSetIds)
    if $CFG::CREATE_RULES;

# End the session
logout();

# Update the PublishEcho configuration
UpdatePublishEchoConfig( $cfg_file, $accessHash, $opt_b );

# Update the DIF_INFO configuration with ECHO DatsetId
UpdateDifInfoConfig( $dif_info_file, $dataSetIds );

exit 0;


sub login {

    my ($username, $pwd, $provider) = @_;

    # Username of the user logging in
    my $usernameElement = SOAP::Data->uri($echo_uri)
                                    ->name('username')
                                    ->attr({'xmlns:echoType' => $types_uri})
                                    ->type('echoType:StringMax50')
                                    ->value($username);

    # Password of the user
    my $pwdElement = SOAP::Data->uri($echo_uri)
                               ->name('password')
                               ->attr({'xmlns:echoType' => $types_uri})
                               ->type('echoType:StringMax1000')
                               ->value($pwd);

    #  The string identifier of the ECHO client used to make this request
    my $hostname = Sys::Hostname::hostname();
    my $packed_ip_address = (gethostbyname($hostname))[4];
    my $ip_address = join('.', unpack('C4', $packed_ip_address));
    my $clientInfoElement = SOAP::Data->uri($echo_uri)
                                      ->name("clientInfo" => \SOAP::Data->value(
        SOAP::Data->uri($types_uri)
                  ->name("ClientId")
                  ->attr({'xmlns:echoType' => $types_uri})
                  ->type('echoType:StringMax50')
                  ->value($provider),
        SOAP::Data->uri($types_uri)
                  ->name("UserIpAddress")
                  ->attr({'xmlns:echoType' => $types_uri})
                  ->type('echoType:StringMax39')
                  ->value($ip_address),
                                            ));

    # Name of the user an Admin wants to act as,
    # null for non-ECHO administrator users
    my $actAsUserNameElement = SOAP::Data->uri($echo_uri)
                                         ->name('actAsUserName')
                                         ->attr({'xmlns:echoType' => $types_uri})
                                         ->type('echoType:StringMax50')
                                         ->value(undef);

    # Provider the user wants to act as,
    # null for guests and registered users with no ProviderRoles
    my $behalfOfProviderElement = SOAP::Data->uri($echo_uri)
                                            ->name('behalfOfProvider')
                                            ->attr({'xmlns:echoType' => $types_uri})
                                            ->type('echoType:StringMax50')
                                            ->value($provider);

    # Execute the Login operation of the Authentication service
    my $serviceName = 'AuthenticationServicePortImpl';

    my $som;
    eval { $som = SOAP::Lite
                     ->uri($echo_uri)
                     ->proxy($endpoint_uri.$serviceName, timeout=>'3600')
                     ->autotype(0)
                     ->on_fault(sub {
                                     my($soap, $result) = @_;
                                     if ( ref $result ) {
                                         die $result->faultstring, "\n";
                                     } else {
                                         die $soap->transport->status, "\n";
                                     }
                                    }
                               )
                     ->Login($usernameElement, $pwdElement, $clientInfoElement,
                             $actAsUserNameElement, $behalfOfProviderElement);
            1 };
    S4P::perish( 2, "Could not connect to ECHO API for login; check if ECHO API is down.\nReason:  $@\n" ) if $@;

    my $token = $som->valueof('//result');
    S4P::perish( 2, "Error obtaining value for authentication token")
        unless defined $token;

    # Save the token in an element for use elsewhere
    my $TokenElement = SOAP::Data->uri($echo_uri)
                                 ->name('token')
                                 ->attr({'xmlns:echoType' => $types_uri})
                                 ->type('echoType:StringMax200')
                                 ->value($token);

    return $TokenElement;
}


sub execute_catalog_query {
    my ($provider) = @_;

    # Construct and submit a query to get all of the ECHO metadata for the
    # collection corresponding to $dataset

    my $query_template = qq(
<s0:query xmlns:s0=\"$echo_uri\"><![CDATA[<?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE query SYSTEM "$endpoint_uri/echo/dtd/IIMSAQLQueryLanguage.dtd">
      <query>
        <for value="collections"/>
        <dataCenterId>
          <value>$provider</value>
        </dataCenterId>
        <where></where>
      </query>
    ]]>
</s0:query>
);

    my $query = $query_template;
    my $queryElement = SOAP::Data->type('xml' => $query);
    my $queryResultTypeElement = SOAP::Data->uri($echo_uri)
                                           ->name('queryResultType')
                                           ->value('HITS');
    my $iteratorSizeElement = SOAP::Data->uri($echo_uri)
                                        ->name('iteratorSize')
                                        ->type('int')
                                        ->value(0);
    my $cursorElement = SOAP::Data->uri($echo_uri)
                                  ->name('cursor')
                                  ->type('int')
                                  ->value(0);
    my $maxResultsElement = SOAP::Data->uri($echo_uri)
                                      ->name('maxResults')
                                      ->type('int')
                                      ->value(0);
    my $metadataAttrElement = SOAP::Data->uri($echo_uri)
                                        ->name('metadataAttributes')
                                        ->value('');

    # Execute the ExecuteQuery operation of the Catalog service
    my $serviceName = 'CatalogServicePortImpl';
    my $som = SOAP::Lite
        ->uri($echo_uri)
        ->autotype(0)
        ->proxy($endpoint_uri.$serviceName, timeout=>'300')
        ->ExecuteQuery($TokenElement, $queryElement, $queryResultTypeElement,
                       $iteratorSizeElement, $cursorElement,
                       $maxResultsElement,
                       $metadataAttrElement);
    print "\nQuery response:\n", $som->{'_context'}->{'_transport'}->{'_proxy'}->{'_http_response'}->{'_content'}, "\n" if $debug;
    S4P::perish( 2, "Error executing catalog query: " .
                    $som->fault->{faultstring} )
        if $som->fault;
    my $resultSetGuid = $som->valueof('//ResultSetGuid');
    my $hits = $som->valueof('//Hits/Size');

    return ($resultSetGuid, $hits);
}


sub get_catalog_query_results {
    my ($resultSetGuid) = @_;

    # Obtain the xml resulting from the submitted query

    my $resultSetGuidElement = SOAP::Data->uri($echo_uri)
                                         ->name('resultSetGuid')
                                         ->attr({'xmlns:echoType' => $types_uri})
                                         ->type('echoType:Guid')
                                         ->value($resultSetGuid);
    my $metadataAttrElement;
        $metadataAttrElement = SOAP::Data->uri($echo_uri)
                                         ->name('metadataAttributes' =>
        \SOAP::Data->value(
             SOAP::Data->uri($types_uri)->name('Item' =>
                 \SOAP::Data->value(
                     SOAP::Data->uri($types_uri)
                               ->name('AttributeName')
                               ->attr({'xmlns:echoType' => $types_uri})
                               ->type('echoType:StringMax50')
                               ->value('DataSetId'),
                     SOAP::Data->uri($types_uri)
                               ->name('PrimitiveValueType'=>'STRING')
                                   )
                                              ),
             SOAP::Data->uri($types_uri)->name('Item' =>
                 \SOAP::Data->value(
                     SOAP::Data->uri($types_uri)
                               ->name('AttributeName')
                               ->attr({'xmlns:echoType' => $types_uri})
                               ->type('echoType:StringMax50')
                               ->value('ShortName'),
                     SOAP::Data->uri($types_uri)
                               ->name('PrimitiveValueType'=>'STRING')
                                   )
                                              ),
             SOAP::Data->uri($types_uri)->name('Item' =>
                 \SOAP::Data->value(
                     SOAP::Data->uri($types_uri)
                               ->name('AttributeName')
                               ->attr({'xmlns:echoType' => $types_uri})
                               ->type('echoType:StringMax50')
                               ->value('VersionId'),
                     SOAP::Data->uri($types_uri)
                               ->name('PrimitiveValueType'=>'STRING')
                                   )
                                              ),
            SOAP::Data->uri($types_uri)->name('Item' =>
                 \SOAP::Data->value(
                     SOAP::Data->uri($types_uri)
                               ->name('AttributeName')
                               ->attr({'xmlns:echoType' => $types_uri})
                               ->type('echoType:StringMax50')
                               ->value('RestrictionFlag'),
                     SOAP::Data->uri($types_uri)
                               ->name('PrimitiveValueType'=>'STRING')
                                   )
                                              ),
                          )
                                               );

    my $iteratorSizeElement = SOAP::Data->uri($echo_uri)
                                        ->name('iteratorSize')
                                        ->type('int')
                                        ->value(2000);
    my $cursorElement = SOAP::Data->uri($echo_uri)
                                  ->name('cursor')
                                  ->type('int')
                                  ->value(1);

    my $serviceName = 'CatalogServicePortImpl';
    my $som = SOAP::Lite
        ->uri($echo_uri)
        ->autotype(0)
        ->proxy($endpoint_uri.$serviceName, timeout=>'300')
        ->GetQueryResults($TokenElement, $resultSetGuidElement,
                          $metadataAttrElement, $iteratorSizeElement,
                          $cursorElement);
    print "\nCatalog query results response:\n", $som->{'_context'}->{'_transport'}->{'_proxy'}->{'_http_response'}->{'_content'}, "\n" if $debug;
    S4P::perish( 2, "Error getting catalog query results: " .
                    $som->fault->{faultstring} )
        if $som->fault;

    my $results = $som->valueof('//ReturnData');
    print "\nCatalog query results: $results\n" if $debug;

    return $results;
}


sub parse_catalog_query_results {
    my ($queryResponse, $dataHash, $accessHash) = @_;

    # Parse the xml from the query response to set the access
    # ('public' or 'restricted') for each shortname/version
    # combination in the query response

    #$queryResponse =~ s/<!DOCTYPE.+>\n//;

    my $xmlParser = XML::LibXML->new();
    my $dom = $xmlParser->parse_string( $queryResponse );
    my $doc = $dom->documentElement();

    foreach my $node ( $doc->findnodes( '//CollectionMetaData' ) ) {

        my ( $dataSetIdNode ) = $node->getChildrenByTagName( 'DataSetId' );
        next unless defined $dataSetIdNode;

        # If there are no versions of this ShortName, go to the next one
        my ( $shortNameNode ) = $node->getChildrenByTagName( 'ShortName' );
        next unless defined $shortNameNode;

        my ( $versionIdNode ) = $node->getChildrenByTagName( 'VersionId' );
#        my ( $flagNode ) = $node->getChildrenByTagName( 'RestrictionFlag' );

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
    # as determined by the ECHO Access Control Lists

    my ($dataSetIds, $accessHash) = @_;

    # Updates accessHash with a visibility value ('restricted' or 'public')
    # for all collections
    my $serviceName;

    # Get the provider GUID
    $serviceName = 'ProviderServicePortImpl';
    my $providerElement = SOAP::Data->uri($echo_uri)
                                    ->name('providerIds' =>
        \SOAP::Data->value(
             SOAP::Data->uri($types_uri)->name('Item' => $provider)
                          )
                                          );
    my $som = SOAP::Lite
              ->uri($echo_uri)
              ->proxy($endpoint_uri.$serviceName, timeout=>'300')
              ->autotype(0)
              ->GetProviderNamesByProviderId($TokenElement, $providerElement);
    print "\nGetProviderNamesByProviderId response:\n", $som->{'_context'}->{'_transport'}->{'_proxy'}->{'_http_response'}->{'_content'}, "\n" if $debug;
    S4P::perish( 2, "Error obtaining provider GUID: " . $som->fault->{faultstring} )
        if $som->fault;
    my $providerGuid;
    foreach my $item ($som->valueof('//result/Item')) {
        my $name = $item->{Name};
        $providerGuid = $item->{Guid};
    }

    my %visibleDataSetIds;
    foreach my $shortName (keys %$dataSetIds) {
        foreach my $versionId (keys %{$dataSetIds->{$shortName}}) {
            # By default, each dataset is restricted
            $accessHash->{$shortName}{$versionId} = 'restricted';
        }
    }

    # Execute the GetAclsByType operation of the Access Control service
    $serviceName = 'AccessControlServicePortImpl';
    my @objectIdentityTypes;
    push @objectIdentityTypes, SOAP::Data->uri($types_uri)
                                         ->name('Item')
                                         ->value('CATALOG_ITEM');
    my $objectIdentityTypesList = SOAP::Data->uri($echo_uri)
                                        ->name('objectIdentityTypes' =>
                                       \SOAP::Data->value(@objectIdentityTypes)
                                              );
    my $providerGuidFilterElement = SOAP::Data->uri($echo_uri)
                                              ->name('providerGuidFilter')
                                              ->value($providerGuid);
    $som = SOAP::Lite
        ->uri($echo_uri)
        ->proxy($endpoint_uri.$serviceName, timeout=>'300')
        ->autotype(0)
        ->GetAclsByType($TokenElement, $objectIdentityTypesList,
                        $providerGuidFilterElement);
    print "\nGetAclsByType response:\n", $som->{'_context'}->{'_transport'}->{'_proxy'}->{'_http_response'}->{'_content'}, "\n" if $debug;
    S4P::perish( 2, "Error obtaining GetAclsByType: " . $som->fault->{faultstring} )
        if $som->fault;


    # Iterate through all ACLs
    foreach my $acl ($som->valueof('//result/Item')) {
        # Skip any ACL that does not apply to collections.
        next unless $acl->{CatalogItemIdentity}->{CollectionApplicable} eq 'true';
        next unless $acl->{AccessControlEntries};
        my @items = (ref($acl->{AccessControlEntries}->{Item}) eq 'ARRAY') ?
            @{$acl->{AccessControlEntries}->{Item}} :
                $acl->{AccessControlEntries}->{Item};
        my $readAccess;
        foreach my $item (@items) {
            # We are only interested in entries associated with a
            # UserAuthorizationTypeSid (i.e. entries that apply to all users
            # of a certain type), and not entries associated with a
            # GroupSid (i.e. entries that apply to all users who are a member
            # of a certain group).
            next unless exists $item->{'Sid'}->{'UserAuthorizationTypeSid'};
            next unless exists $item->{'Sid'}->{'UserAuthorizationTypeSid'}->{'UserAuthorizationType'};

            # The UserAuthorizationType can be either "GUEST' or 'REGISTERED',
            # Although a collection could be visible to guests but not to
            # registered users, we will consider such a collection to be visible,
            # i.e. unrestricted.
            #next unless $item->{'Sid'}->{'UserAuthorizationTypeSid'}->{'UserAuthorizationType'} eq 'GUEST';

            # Skip the cases where no permissions are allowed.
            next unless ref($item->{'Permissions'}) eq 'HASH';
            next unless exists $item->{'Permissions'}->{'Item'};

            # There can be more than one permission allowed from the set
            # 'CREATE', 'READ', 'UPDATE', 'DELETE', and 'ORDER'. We are only
            # concerned with 'READ', i.e. "view".
            if (ref($item->{'Permissions'}->{'Item'}) eq 'ARRAY') {
                foreach my $permItem (@{$item->{'Permissions'}->{'Item'}}) {
                    $readAccess = 1 if $permItem eq 'READ';
                }
            }
            else {
                $readAccess = 1 if $item->{'Permissions'}->{'Item'} eq 'READ';
            }
        }
        if ($readAccess) {
            # ACL has an entry for a UserAuthorizationType that has READ access
            # Mark all collections in that ACL as 'visible'
            foreach my $collection (@{$acl->{CatalogItemIdentity}->{CollectionIdentifier}->{CollectionIds}->{Item}}) {
                my $DataSetId = $collection->{DataSetId};
                $visibleDataSetIds{$DataSetId} = 1;
            }
        }
    }

    # Set access to 'public' for each dataSetId that is visible
    # to the public according to the ACLs
    foreach my $shortName (keys %$dataSetIds) {
        foreach my $versionId (keys %{$dataSetIds->{$shortName}}) {
            my $dataSetId = $dataSetIds->{$shortName}->{$versionId};
            $accessHash->{$shortName}{$versionId} = 'public'
                if exists $visibleDataSetIds{$dataSetId};
        }
    }

    return $accessHash;
}


sub update_echo_access {
    my ($s4pa_access, $echo_access, $dataSetIds) = @_;

    # Change the %ECHO_ACCESS value from 'restricted' to 'public' for any
    # collection whose %DATA_ACCESS value is 'public'.
    # If we wanted to change the %ECHO_ACCESS value from 'public' to
    # 'restricted' for any collection whose %DATA_ACCESS value is 'restricted',
    # we would have to delete all all ACLs that allow access to that
    # collection, which is a bit trickier to do, so that is not done here.

    # Get the provider GUID
    my $serviceName = 'ProviderServicePortImpl';
    my $providerElement = SOAP::Data->uri($echo_uri)
        ->name('providerIds' =>
               \SOAP::Data->value(
                                  SOAP::Data->uri($types_uri)->name('Item' => $provider)
                                 )
              );
    my $som = SOAP::Lite
              ->uri($echo_uri)
              ->proxy($endpoint_uri.$serviceName, timeout=>'300')
              ->autotype(0)
              ->GetProviderNamesByProviderId($TokenElement, $providerElement);
    print "\nGetProviderNamesByProviderId response:\n", $som->{'_context'}->{'_transport'}->{'_proxy'}->{'_http_response'}->{'_content'}, "\n" if $debug;
    S4P::perish( 2, "Error obtaining provider GUID: " . $som->fault->{faultstring} )
          if $som->fault;
    my $providerGuid;
    foreach my $item ($som->valueof('//result/Item')) {
        # There should be only one GUID for provider $provider,
        # so this loop should find only one Item.
        my $name = $item->{Name};
        $providerGuid = $item->{Guid};
    }

    # Get the GUID for the 'Administrators' group
    # Expect provider $provider to be a member of the
    # group named 'Administrators'.
    $serviceName = 'Group2ManagementServicePortImpl';
    my $providerGuidElement = SOAP::Data->uri($echo_uri)
                                        ->name('providerGuid')
                                        ->value($providerGuid);
    $som = SOAP::Lite
           ->uri($echo_uri)
           ->proxy($endpoint_uri.$serviceName, timeout=>'300')
           ->autotype(0)
           ->GetGroupNamesByOwner($TokenElement, $providerGuidElement);
    print "\nGetGroupNamesByOwner response:\n", $som->{'_context'}->{'_transport'}->{'_proxy'}->{'_http_response'}->{'_content'}, "\n" if $debug;
    S4P::perish( 2, "Error in GetGroupNamesByOwner: " . $som->fault->{faultstring} )
          if $som->fault;
    my $groupGuid;
    foreach my $item ($som->valueof('//result/Item')) {
        my $name = $item->{Name};
        $groupGuid = $item->{Guid} if $name eq 'Administrators';
    }

    foreach my $shortName (keys %$echo_access) {
        my @versionIds = keys %{$echo_access->{$shortName}};
        foreach my $versionId (@versionIds) {
            my $s4paAccessType;
            my $echoAccessType = $echo_access->{$shortName}->{$versionId};
            if (exists $s4pa_access->{$shortName}->{$versionId}) {
                $s4paAccessType = $s4pa_access->{$shortName}->{$versionId};
            } elsif (((scalar @versionIds) == 1) &&
                     (exists $s4pa_access->{$shortName}->{''}) &&
                     ((scalar (keys %{$s4pa_access->{$shortName}})) == 1)) {
                # If shortName is unversioned in S4PA and there is a single
                # version in ECHO, assume they are the same version.
                $s4paAccessType = $s4pa_access->{$shortName}->{''};
            } else {
                # ECHO has a version for the shortName that S4PA does not,
                # or shortName is unversioned in S4PA and there is more
                # than one version in ECHO, preventing us from assuming that
                # the ECHO version is the same as the unversioned S4PA version.
                # Do not update the access type.
                next;
            }
            next if ($echoAccessType eq $s4paAccessType);
            if (($s4paAccessType eq "public") &&
                ($echoAccessType eq "restricted")) {
#                 # Collection is public in S4PA but restricted in ECHO.
#                 # Since S4PA access is determined at deployment time,
#                 # for any collection intended to be published to ECHO,
#                 # the access types should agree, so make any restricted
#                 # collection public by creating an ECHO access rule
#                 # that allows the collection to be viewed.

#                 my $ruleName = "view_${shortName}_$versionId";
#                 my $description = "Allow view for $shortName $versionId";
#                 my $serviceName = 'DataManagementServicePortImpl';
#                 my $dataSetId = $dataSetIds->{$shortName}{$versionId};

                # Create a new ACL that will provide View (read) permission
                # for the collection with shortname $shortName and version
                # $versionId

                # $accessControlEntriesElement specifies read access for
                # Registered Users, Guest Users, and the Administrators
                # group
                my $accessControlEntriesElement = SOAP::Data->uri($types_uri)
                                                            ->name('AccessControlEntries' =>
                  \SOAP::Data->value(
                      SOAP::Data->uri($types_uri)->name('Item' =>
                          \SOAP::Data->value(
                              SOAP::Data->uri($types_uri)->name('Sid' =>
                                  \SOAP::Data->value(
                                      SOAP::Data->uri($types_uri)->name('GroupSid' =>
                                          \SOAP::Data->value(
                                              SOAP::Data->uri($types_uri)->name('GroupGuid' => $groupGuid),
                                                            )
                                                                       )
                                                    )
                                                               ),
                              SOAP::Data->uri($types_uri)->name('Permissions' =>
                                  \SOAP::Data->value(
                                      SOAP::Data->uri($types_uri)->name('Item' => 'READ')
                                                    )
                                                               )
                                            )
                                                       ),
                      SOAP::Data->uri($types_uri)->name('Item' =>
                          \SOAP::Data->value(
                              SOAP::Data->uri($types_uri)->name('Sid' =>
                                  \SOAP::Data->value(
                                      SOAP::Data->uri($types_uri)->name('UserAuthorizationTypeSid' =>
                                          \SOAP::Data->value(
                                              SOAP::Data->uri($types_uri)->name('UserAuthorizationType' => 'REGISTERED'),
                                                            )
                                                                       )
                                                    )
                                                               ),
                              SOAP::Data->uri($types_uri)->name('Permissions' =>
                                  \SOAP::Data->value(
                                      SOAP::Data->uri($types_uri)->name('Item' => 'READ')
                                                    )
                                                               )
                                            )
                                                       ),
                      SOAP::Data->uri($types_uri)->name('Item' =>
                          \SOAP::Data->value(
                              SOAP::Data->uri($types_uri)->name('Sid' =>
                                  \SOAP::Data->value(
                                      SOAP::Data->uri($types_uri)->name('UserAuthorizationTypeSid' =>
                                          \SOAP::Data->value(
                                              SOAP::Data->uri($types_uri)->name('UserAuthorizationType' => 'GUEST'),
                                                            )
                                                                       )
                                                    )
                                                               ),
                              SOAP::Data->uri($types_uri)->name('Permissions' =>
                                  \SOAP::Data->value(
                                      SOAP::Data->uri($types_uri)->name('Item' => 'READ')
                                                    )
                                                               )
                                            )
                                                       )
                                    )
                                                                  );

                # $catalogItemIdentityName is the name of the new Catalog
                # Item ACL.
                my $catalogItemIdentityName = "View_${shortName}_$versionId";

                # The Catalog Item ACL will apply to the collection with
                # shortName $shortName and version $VersionId, and its
                # granules.
                my $catalogItemIdentityElement = SOAP::Data->uri($types_uri)
                                                           ->name('CatalogItemIdentity' =>
                  \SOAP::Data->value(
                       SOAP::Data->uri($types_uri)->name('Name' => $catalogItemIdentityName),
                       SOAP::Data->uri($types_uri)->name('ProviderGuid' => $providerGuid),
                       SOAP::Data->uri($types_uri)->name('CollectionApplicable' => 1),
                       SOAP::Data->uri($types_uri)->name('CollectionIdentifier' =>
                           \SOAP::Data->value(
                               SOAP::Data->uri($types_uri)->name('CollectionIdPatterns'=>
                                   \SOAP::Data->value(
                                       SOAP::Data->uri($types_uri)->name('Item' =>
                                           \SOAP::Data->value(
                                               SOAP::Data->uri($types_uri)->name('ShortName' => $shortName)
                                                             )
                                                                        ),
                                       SOAP::Data->uri($types_uri)->name('Item' =>
                                           \SOAP::Data->value(
                                               SOAP::Data->uri($types_uri)->name('Version' => $versionId)
                                                             )
                                                                        ),
                                                     )
                                                                )
                                             )
                                                        ),
                       SOAP::Data->uri($types_uri)->name('GranuleApplicable' => 1),
                                   )
                                                                 );

                # Create the new ACL
                $serviceName = 'AccessControlServicePortImpl';
                my $aclElement = SOAP::Data->uri($echo_uri)
                                           ->name('acl' =>
                                  \SOAP::Data->value(
                                                     $accessControlEntriesElement,
                                                     $catalogItemIdentityElement
                                                    )
                                                 );
                $som = SOAP::Lite
                       ->uri($echo_uri)
                       ->proxy($endpoint_uri.$serviceName, timeout=>'300')
                       ->autotype(0)
                       ->CreateAcl($TokenElement, $aclElement);
                print "\nCreateAcl response:\n", $som->{'_context'}->{'_transport'}->{'_proxy'}->{'_http_response'}->{'_content'}, "\n" if $debug;
                S4P::perish( 2, "Error in CreateAcl: " . $som->fault->{faultstring} )
                      if $som->fault;
            }
            elsif (($s4paAccessType eq "restricted") &&
                   ($echoAccessType eq "public")) {
                # Change the ECHO collection access from public to restricted.
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


sub logout {
    # Execute the Logout operation of the Authentication service
    my $serviceName = 'AuthenticationServicePortImpl';
    my $response = SOAP::Lite
        ->uri($echo_uri)
        ->proxy($endpoint_uri.$serviceName, timeout=>'300')
        ->outputxml(1)
        ->readable($readable)
        ->Logout($TokenElement);
    print "\nLogout response: $response\n" if $debug;
}


sub UpdatePublishEchoConfig {
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

        # Skip any ECHO_ACCESS values already in the config file
        next if $name eq 'ECHO_ACCESS';

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

    # The hash referenced by $config now contains all of the information
    # in the configuration file read by this script. Now add any new values
    # that are desired.

    if ( defined $browse ) {
        $config->{BROWSE_DESCRIPTION} = \%BROWSE_CFG::BROWSE_DESCRIPTION;
        $type_hash->{BROWSE_DESCRIPTION} = 'HASH';
    }
    $config->{ECHO_ACCESS} = $accessHash;
    $type_hash->{ECHO_ACCESS} = 'HASH';
    $config->{__TYPE__} = $type_hash if (defined $type_hash);
    $config->{BROWSEDIR} = $CFG::BROWSEDIR if (defined $CFG::BROWSEDIR);

    # Replace the configuration file with a new configuration file.
    S4PA::WriteStationConfig( basename( $file ), dirname( $file ), $config );

    return;
}


sub UpdateDifInfoConfig {
    my ( $file, $dataSetIds ) = @_;

    my $config;
    my $type_hash;
    #  This whole routine was borrowed from UpdatePublishEchoConfig above

    # Iterate through every symbol in the DIF_INFO_CFG namespace
    foreach my $name (keys %DIF_INFO_CFG::) {
        # We can't assigm to a typeglob if 'use strict' is in effect,
        # so disable it in this loop.
        no strict 'vars';

        # Skip special symbols whose name begins with an underscore
        next if $name =~ /^_/;

        # Skip echo_dataset_id values already in the config file
        next if $name eq 'echo_dataset_id';

        # The DIF_INFO_CFG namespace also references symbols in the main namespace!
        # Skip those too, or else face an endless loop.
        # Symbols in other namespaces will begin with a name (consisting
        # of word characters) followed by two colons.
        next if $name =~ /^\w+::/;

        # Assign the symbol to a local typeglob
        local *sym = $DIF_INFO_CFG::{$name};

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

    # The hash referenced by $config now contains all of the information
    # in the configuration file read by this script. Now update the
    # echo_dataset_id for the specified dataset.

    $config->{echo_dataset_id} = $dataSetIds;
    $type_hash->{echo_dataset_id} = 'HASH';
    $config->{__TYPE__} = $type_hash if (defined $type_hash);

    # Replace the configuration file with a new configuration file
    # under both housekeeper and dif_fetch stations.
    my $station = dirname( $file );
    S4PA::WriteStationConfig( basename( $file ), $station , $config );
    $station =~ s/housekeeper/dif_fetcher/;
    S4PA::WriteStationConfig( basename( $file ), $station , $config );

    return;
}

