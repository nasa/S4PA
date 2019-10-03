#!/usr/bin/perl -w

=head1 NAME

s4pa_update_subscription - a script to receive data for S4PA

=head1 SYNOPSIS

s4pa_update_subscription.pl 
[B<-d> I<S4PA deployment descriptor>]
[B<-f> I<Subscription configuration file>]
[B<-s> I<Subscription configuration schema>]

=head1 DESCRIPTION

Creates necessary S4PA subscription station configurtion files: station.cfg
for SubscribeData and Postoffice and s4pa_subscription.cfg based on the
supplied subscription information in XML.

=head1 ARGUMENTS

=over 4

=item B<-d> I<S4PA deployment descriptor>

S4PA descriptor file.

=item B<-s> I<Subscription configuration file>

Subscription configuration file in XML containing subscription info about
subscribers and data being subscribed to.

=item B<-s> I<Subscription schema>

XML schema for S4PA subscription.

=back

=head1 AUTHOR

M. Hegde

=cut
################################################################################
# s4pa_update_subscription.pl,v 1.58 2011/05/24 16:20:41 glei Exp
# -@@@ S4PA, Version $Name:  $
################################################################################

use Getopt::Std;
use Data::Dumper;
use XML::LibXML;
use S4PA;
use Safe;
use strict;
use Log::Log4perl;

my $opt = {};

# Get command line arguments
getopts( "d:f:s:", $opt );

die "Specify deployment descriptor file (-d)"
    unless defined $opt->{d};
die "Specify subscription configuration file (-f)" 
    unless defined $opt->{f};
die "Specify schema for the subscription configuration (-s)"
    unless defined $opt->{s};
die "Deployment descriptor file, $opt->{d}, doesn't exist"
    unless ( -f $opt->{d} );
die "Subscription configuration file, $opt->{f}, doesn't exist" 
    unless ( -f $opt->{f} );
die "XML schema, $opt->{s}, doesn't exist"
    unless ( -f $opt->{s} );
    
# Create an XML DOM parser.
my $xmlParser = XML::LibXML->new();
$xmlParser->keep_blanks(0);

# Parse the descriptor file.
my $descriptorDom = $xmlParser->parse_file( $opt->{d} );
my $descriptorDoc = $descriptorDom->documentElement();

# Get S4PA root from the S4PA descriptor
my $s4paRoot = GetNodeValue( $descriptorDoc, '/s4pa/root' );
die "Failed to get S4PA root from $opt->{d}" unless defined $s4paRoot;

# Get tempDir from the S4PA descriptor
my $tempDir = GetNodeValue( $descriptorDoc, '/s4pa/tempDir' );
die "Failed to get S4PA tempDir from $opt->{d}" unless defined $tempDir;

# Get dataset attributes (group, frequency and access) from S4PA descriptor
my $dataAttr = GetDataAttributes( $descriptorDoc );

# Get logger parameters
my ( $loggerNode ) = $descriptorDoc->findnodes( '/s4pa/logger' );
my $logFile;
my $logLevel;
my $logger;
if (defined $loggerNode) {
    my $loggerDir = $loggerNode->getAttribute( 'DIR' );
    $logLevel = $loggerNode->getAttribute( 'LEVEL' );
    $logFile = defined $loggerDir ? "$loggerDir/subscribe.log" : undef;
    $logger = S4PA::CreateLogger( "$loggerDir/deploy.log",
        $logLevel );
}
$logger->info( "Subscription update with $0 -f $opt->{f} -d $opt->{d} " .
    "-s $opt->{s}" ) if defined $logger;

# Parse the Subscription doc
my $dom = $xmlParser->parse_file( $opt->{f} );
my $doc = $dom->documentElement();
# Validate using the specified schema
my $schema = XML::LibXML::Schema->new( location => $opt->{s} );
die "Failed to read XML schema, $opt->{s}" unless $schema;

eval { $schema->validate( $dom ); };
die "Failed to validate $opt->{f} with $opt->{s}\n$@" if $@;

# Write s4pa_subscription.cfg
WriteSubscriptionConfig( $doc, $s4paRoot );
# Update SubscribeData station config
UpdateSubscriptionStation( $doc, $s4paRoot, $logger );
# Update PostOffice station config
UpdatePostOfficeStation( $doc, $s4paRoot, $tempDir, $logger );
# Write machine-to-machine search config
WriteMachineSearchConfig( $doc, $s4paRoot );

# publish subscription to dotchart if configured
my ( $workOrder, $message ) = publishSubscription( $dom, $descriptorDoc );
if ( defined $workOrder ) {
    print "$message\n";
    $logger->info( "$message" ) if defined $logger;
} else {
    print "Skipped publish subscription: $message\n";
    $logger->info( "Skipped publish subscription: $message" ) 
        if defined $logger;
}
$logger->info( "Subscription update completed." ) if defined $logger;

###############################################################################
# =head1 WriteMachineSearchConfig
# 
# Description
#    Writes configuration needed for machine search. It has mappings of
#    user names to subscription IDs.
#
# =cut
###############################################################################
sub WriteMachineSearchConfig
{
    my ( $doc, $s4paRoot ) = @_;

    # Look in all subscriptions for "USER" attribute
    my ( @subscriptionList ) = ( $doc->findnodes( '//pushSubscription' ),
        $doc->findnodes( '//pullSubscription' ) );
    # Compile a hash whose keys are user names and values are arrays of
    # subscription IDs.
    my $userSubscriptionHash = {};
    foreach my $subscription ( @subscriptionList ) {
        my $user = $subscription->getAttribute( 'USER' );
        next unless defined $user;
        my $id = $subscription->getAttribute( 'ID' );
	$userSubscriptionHash->{$user} = []
	    unless defined $userSubscriptionHash->{$user};
        push( @{$userSubscriptionHash->{$user}}, $id );
    }
    # Write out the configuration in the S4PA root directory
    my $config = {
	cfg_machine_search_user => $userSubscriptionHash,
        __TYPE__ => {
            cfg_machine_search_user => 'HASH',
	}
    };
    S4PA::WriteStationConfig( "machine_search.cfg", $s4paRoot, $config );
}

###############################################################################
# =head1 UpdatePostOfficeStation
# 
# Description
#   Update PostOffice station configuration.
#
# =cut
###############################################################################
sub UpdatePostOfficeStation
{
    my ( $doc, $s4paRoot, $tempDir, $logger ) = @_;

    my $config = S4PA::LoadStationConfig( "$s4paRoot/postoffice/station.cfg",
        "TempPostOffice" );
    foreach my $destNode ( $doc->findnodes( '//destination' ) ) {
        my $destination = $destNode->getAttribute( 'ADDRESS' );
        my $protocol = $destNode->getAttribute( 'PROTOCOL' );
        if ( $protocol eq 'mailto' ) {
            $destination =~ s/\@/_/g;
        } else {
            ($destination) = split( /\//, $destination );
        }
        $destination =~ s/\./_/g;        
        $config->{cfg_downstream}{$destination} =  [ 'postoffice/' ];
        $config->{cfg_commands}{$destination} =  "s4pa_file_pusher.pl"
            . " -n -f ../s4pa_postoffice.cfg -d $tempDir";       
    }
    $config->{__TYPE__}{cfg_downstream} = 'HASH';
    $config->{__TYPE__}{cfg_downstream} = 'HASH';
    S4PA::CreateStation( "$s4paRoot/postoffice", $config, $logger );
}
###############################################################################
# =head1 UpdateSubscriptionStation
# 
# Description
#   Update SubscribeData station configuration.
#
# =cut
###############################################################################
sub UpdateSubscriptionStation
{
    my ( $doc, $s4paRoot, $logger ) = @_;
    my $config = S4PA::LoadStationConfig( "$s4paRoot/subscribe/station.cfg",
        "Subscription" );
    foreach my $destNode ( $doc->findnodes( '//destination' ) ) {
        my $destination = $destNode->getAttribute( 'ADDRESS' );
        my $protocol = $destNode->getAttribute( 'PROTOCOL' );
        if ( $protocol eq 'mailto' ) {
            $destination =~ s/\@/_/g;
        } else {
            ($destination) = split( /\//, $destination );
        }
        $destination =~ s/\./_/g;
        $config->{cfg_downstream}{$destination} =  [ 'postoffice/' ];
    }
    $config->{__TYPE__}{cfg_downstream} = 'HASH';
    S4PA::CreateStation( "$s4paRoot/subscribe", $config, $logger );
}
###############################################################################
# =head1 WriteSubscriptionConfig
# 
# Description
#   Creates S4PA subscription configuration.
#
# =cut
###############################################################################
sub WriteSubscriptionConfig
{
    my ( $doc, $s4paRoot ) = @_;
    my $httpRoot = $doc->getAttribute( 'HTTP_ROOT' );
    my $ftpRoot = $doc->getAttribute( 'FTP_ROOT' );
    my $notifySubject = $doc->getAttribute( 'NOTICE_SUBJECT' );

    # Variables to hold URL root and URL path for each dataset/version
    my ( $cfg_data_access, $cfg_url_root, $cfg_url_path ) = ( {}, {}, {} );
    $cfg_url_root->{public} = $ftpRoot;
    $cfg_url_root->{restricted} = $httpRoot;
    foreach my $dataset ( keys %$dataAttr ) {
        foreach my $version ( keys %{$dataAttr->{$dataset}} ) {
            next if ( $dataAttr->{$dataset}{$version}{ACCESS} eq 'hidden' );
	    $cfg_data_access->{$dataset}{$version}
	    	= $dataAttr->{$dataset}{$version}{ACCESS};
            my $url = "$dataAttr->{$dataset}{$version}{GROUP}/$dataset";
            $url .= ".$version" if ( $version ne '' );
            if ( $dataAttr->{$dataset}{$version}{FREQUENCY} eq 'daily' ) {
                $url .= '/%Y/%j/';
            } elsif ( $dataAttr->{$dataset}{$version}{FREQUENCY} eq 'monthly' ) {
                $url .= '/%Y/%m/';
            } elsif ( $dataAttr->{$dataset}{$version}{FREQUENCY} eq 'yearly' ) {
                $url .= '/%Y/';
            } elsif ( $dataAttr->{$dataset}{$version}{FREQUENCY} eq 'none' ) {
                $url .= '/';
            } else {
                die "Unknown temporal frequency for dataset=$dataset";
            }
            $url .= '.hidden/'
                if ( $dataAttr->{$dataset}{$version}{ACCESS} eq 'restricted' );
            $url .= '%=F';
            $cfg_url_path->{$dataset}{$version} = $url;
        }
    }

    my $cfg_subscriptions = {};
    my ( @subscriptionList ) = ( $doc->findnodes( '//pushSubscription' ),
        $doc->findnodes( '//pullSubscription' ) );
    foreach my $subscription ( @subscriptionList ) {
        my $type = ( $subscription->nodeName() eq 'pushSubscription' )
            ? 'push' : 'pull';
        my $label = $subscription->getAttribute( 'LABEL' );
        my $id = $subscription->getAttribute( 'ID' );
	my $maxGranuleCount =
            $subscription->getAttribute( 'MAX_GRANULE_COUNT' );
	my $includeBrowse = $subscription->getAttribute( 'INCLUDE_BROWSE' );
	my $includeMap = $subscription->getAttribute( 'INCLUDE_HDF4MAP' );
        my ( $notify ) = $subscription->findnodes( 'notification' );
        my $format = $notify->getAttribute( 'FORMAT' );
        my $notifyAddress = $notify->getAttribute( 'PROTOCOL' )
        . ':' . $notify->getAttribute( 'ADDRESS' );
        my $notifySuffix = $notify->getAttribute( 'NOTICE_SUFFIX' );
        my $subSubject = $notify->getAttribute( 'NOTICE_SUBJECT' );
        my $notifyFilter;
        if ( $format eq 'USER-DEFINED' ) {
            my ( $filter ) = $notify->findnodes( 'filter' );
            $notifyFilter = $filter->textContent();
            die "Subscription $id with $format format on notification but no filter!"
                unless ( $notifyFilter );
        }
        my ( $destination ) = $subscription->findnodes( 'destination' );
        my $destAddress = $destination->getAttribute( 'PROTOCOL' ) 
            . ':' . $destination->getAttribute( 'ADDRESS' )
            if defined $destination;
        my $pickUpUrl = $destination->getAttribute( 'URL_ROOT' )
            if defined $destination;
        # Get the subscription level HTTP/FTP root
        my $subHttpRoot = $subscription->getAttribute( 'HTTP_ROOT' );
        my $subFtpRoot = $subscription->getAttribute( 'FTP_ROOT' );
        
        # Get the flag indicating whether the data transfers need to be verified
        my $verifyFlag = $subscription->getAttribute( 'VERIFY' );
        $verifyFlag = 'false' unless defined $verifyFlag;
        
        die "Subscription with ID=$id already exists!"
            if defined $cfg_subscriptions->{$id};
        $cfg_subscriptions->{$id}{verify} = $verifyFlag;
        $cfg_subscriptions->{$id}{label} = $label if defined $label;
        $cfg_subscriptions->{$id}{notify}{address} = $notifyAddress;
        $cfg_subscriptions->{$id}{notify}{format} = $format;
        $cfg_subscriptions->{$id}{notify}{suffix} = $notifySuffix
            if defined $notifySuffix;
        $cfg_subscriptions->{$id}{notify}{subject} = $notifySubject
            if defined $notifySubject;
        $cfg_subscriptions->{$id}{notify}{subject} = $subSubject
            if defined $subSubject;
        $cfg_subscriptions->{$id}{notify}{filter} = $notifyFilter
            if ( $notifyFilter );
        $cfg_subscriptions->{$id}{type} = $type;
        $cfg_subscriptions->{$id}{destination} = $destAddress
            if defined $destAddress;
        $cfg_subscriptions->{$id}{include_browse} = $includeBrowse
	    if defined $includeBrowse;
        $cfg_subscriptions->{$id}{include_map} = $includeMap
	    if defined $includeMap;
	$cfg_subscriptions->{$id}{urlRoot}{public} = $subFtpRoot
	    if defined $subFtpRoot;
	$cfg_subscriptions->{$id}{urlRoot}{restricted} = $subHttpRoot
	    if defined $subHttpRoot;
        $cfg_subscriptions->{$id}{urlRoot}{pickup} = $pickUpUrl
	    if defined $pickUpUrl;
        $cfg_subscriptions->{$id}{max_granule_count} = $maxGranuleCount
            if defined $maxGranuleCount;
        foreach my $dataset ( $subscription->findnodes( 'dataset' ) ) {
            my $name = $dataset->getAttribute( 'NAME' );
            my $version = $dataset->getAttribute( 'VERSION' );
            $version = '.*' unless defined $version;
            
            # Check to see if the dataset is hidden
            my ( $dataAccess, $dataVersion );
            
            # Loop over all versions in the order they were specified.
            foreach my $element (
                sort { $dataAttr->{$name}{$a}{INDEX} 
                    <=> $dataAttr->{$name}{$b}{INDEX} } 
                    keys %{$dataAttr->{$name}}
                ) {
                # next unless ( $element =~ m{$version} || $element eq '' );
                next unless ( $element =~ m{$version} );
                $dataVersion = $element;
                $dataAccess = $dataAttr->{$name}{$dataVersion}{ACCESS};
                die "Subscription ID=$id, Dataset=$name, Version=$version is hidden;"
                    . "subscription not allowed" if ( $dataAccess eq 'hidden' );
            }
            die "Subscription ID=$id, Dataset=$name, Version=$version:"
                . " matching data version in the descriptor not found"
                unless defined $dataVersion;
                
            my $key = $name . '.' . $version;
            $cfg_subscriptions->{$id}{$key}{validator} = [];
            my @validatorList = ();
            my $filterHash = {};
            foreach my $validator ( $dataset->findnodes( 'validator' ) ) {
                push( @validatorList, $validator->textContent() );
            }
            if ( ( $subscription->nodeName() eq 'pushSubscription' ) ||
                 ( $subscription->nodeName() eq 'pullSubscription' &&
                   $subscription->findnodes( 'destination') ) ) {
                foreach my $filter ( $dataset->findnodes( 'filter' ) ) {
                    my $pattern = $filter->getAttribute( 'PATTERN' );
                    $filterHash->{$pattern} = $filter->textContent();            
                }
                $cfg_subscriptions->{$id}{$key}{filter} = $filterHash
                    if ( keys %$filterHash );     
            }
            @validatorList = ( 'true' ) unless @validatorList;
            $cfg_subscriptions->{$id}{$key}{validator} = [ @validatorList ];

            # HTTP_service for pull subscription
            my ( $service ) = $dataset->findnodes( 'service' );
            if ( defined $service ) {
                my $svcParams = getServiceOptions( $service );
                $svcParams->{SERVICE} = $service->getAttribute( 'NAME' );
                $svcParams->{SHORTNAME} = $dataset->getAttribute( 'NAME' );
                $cfg_subscriptions->{$id}{$key}{service} = $svcParams;
            }
        }
    }

    my $config = {
	cfg_data_access => $cfg_data_access,
        cfg_url_root => $cfg_url_root,
        cfg_url_path => $cfg_url_path,
        cfg_data_access => $cfg_data_access,
        cfg_subscriptions => $cfg_subscriptions,
        __TYPE__ => {
	    cfg_data_access => 'HASH',
	    cfg_url_root => 'HASH',
            cfg_url_path => 'HASH',
            cfg_subscriptions => 'HASH',
            }
        };

    if ( defined $loggerNode ) {
        $config->{cfg_logger} = {
            LEVEL => $logLevel,
            FILE => $logFile };
        $config->{__TYPE__}{cfg_logger} = 'HASH';
    }
    S4PA::WriteStationConfig( 's4pa_subscription.cfg', "$s4paRoot/subscribe",
        $config );
}
###############################################################################
# =head1 GetDataAttributes
# 
# Description
#   Returns relevant attributes of a dataset as a hash ref.
#
# =cut
###############################################################################
sub GetDataAttributes
{
    my ( $doc ) = @_;
    
    my $dataAttr = {};
    foreach my $dataset ( $doc->findnodes( '//dataClass/dataset' ) ) {
        # Get dataset name, group and access type
        my $dataName = $dataset->getAttribute( 'NAME' );
        my $dataGroup = $dataset->getAttribute( 'GROUP' );        
        $dataGroup = $dataset->parentNode()->getAttribute( 'GROUP' )
            unless ( defined $dataGroup );
        die "Data group not found for $dataName" unless defined $dataGroup;
        my $dataAccess = $dataset->getAttribute( 'ACCESS' );
        $dataAccess = $dataset->parentNode()->getAttribute( 'ACCESS' )
            unless defined $dataAccess;
        $dataAccess = "public" unless defined $dataAccess;
        my $dataFrequency =  $dataset->getAttribute( 'FREQUENCY' );
        $dataFrequency = $dataset->parentNode()->getAttribute( 'FREQUENCY' )
            unless defined $dataFrequency;
        $dataFrequency = 'daily' unless defined $dataFrequency;
        my @dataVersionList = $dataset->findnodes( 'dataVersion' );
        if ( @dataVersionList ) {
            my $index = 0;
            foreach my $dataVersion ( @dataVersionList ) {
                my $versionId = $dataVersion->getAttribute( 'LABEL' );
                my $versionAccess = $dataVersion->getAttribute( 'ACCESS' );
                $versionAccess = $dataAccess unless defined $versionAccess;
                my $versionFrequency
                    = $dataVersion->getAttribute( 'FREQUENCY' );
                $versionFrequency = $dataFrequency
                    unless defined $versionFrequency;
                $dataAttr->{$dataName}{$versionId}{GROUP} = $dataGroup;
                $dataAttr->{$dataName}{$versionId}{ACCESS}
                    = $versionAccess;
                $dataAttr->{$dataName}{$versionId}{FREQUENCY}
                    = $versionFrequency;
                # Added to retrieve the order in which the data versions are
                # specified.
                $dataAttr->{$dataName}{$versionId}{INDEX} = $index++;
            }
        } else {
            $dataAttr->{$dataName}{''}{GROUP} = $dataGroup;
            $dataAttr->{$dataName}{''}{ACCESS} = $dataAccess;
            $dataAttr->{$dataName}{''}{FREQUENCY} = $dataFrequency;
            $dataAttr->{$dataName}{''}{INDEX} = 0;
        }
    }
    return $dataAttr;
}

###############################################################################
# =head1 GetNodeValue
# 
# Description
#   Returns the text content of a node or its child matched by the optional
#   XPATH expression.
#
# =cut
###############################################################################
sub GetNodeValue
{
    my ( $root, $xpath ) = @_;
    my ( $node ) = ( $xpath ? $root->findnodes( $xpath ) : $root );
    return undef unless defined $node;
    my $val = $node->textContent();
    $val =~ s/^\s+|\s+$//g;
    return $val;
}

###############################################################################
# =head1 publishSubscription
# 
# Description
#   Returns the postoffice work order for pushing subscription configuration
#   file to dotchart database host. Return undef with message if failed.
#
# =cut
###############################################################################
sub publishSubscription 
{
    my ( $subscriptionDom, $descriptorDoc ) = @_;
    my $message;

    # get Instance info
    my $s4paRoot = GetNodeValue( $descriptorDoc, '/s4pa/root' );
    $s4paRoot =~ s/\/$//;
    my $instance = $descriptorDoc->getAttribute( 'NAME' );
    my $workOrder = "${s4paRoot}/postoffice/DO.PUSH.DotchartSub_${instance}.wo";
    my $localPath = "${s4paRoot}/tmp/DotchartSub.${instance}.xml";

    # get Host/Protocal listing
    my %protocol;
    foreach my $hostNode ( $descriptorDoc->findnodes( '/s4pa/protocol/host' ) ) {
        my $hostName = GetNodeValue( $hostNode );
        $protocol{$hostName}
            = $hostNode->parentNode()->getAttribute( 'NAME' )
            if  $hostName;
    }

    # get publish Dotchart info
    my ( $dotchartNode ) = $descriptorDoc->findnodes( '/s4pa/publication/dotChart' );
    unless ( defined $dotchartNode ) {
        $message = "No dotChart defined in instance descriptor."; 
        return ( undef, $message );
    }

    my ( $collectionNode ) = $dotchartNode->findnodes( 'collectionInsert' );
    unless ( defined $collectionNode ) {
        $message = "No dotchart/collectionInsert defined in instance descriptor.";
        return ( undef, $message );
    }

    # assign dotchart Host, Dir, and Protocol
    my $dotchartHost = ( defined $collectionNode->getAttribute( 'HOST' ) ) ?
        $collectionNode->getAttribute( 'HOST' ) : $dotchartNode->getAttribute( 'HOST' );
    my $dotchartDir = $collectionNode->getAttribute( 'DIR' );
    unless ( $dotchartDir =~ /^\// ) {
        $message = "Dotchart/collectionInsert DIR does not start with '/'.";
        return ( undef, $message );
    }
    my $dotchartProtocol = ( defined $protocol{$dotchartHost} ) ?
        lc( $protocol{$dotchartHost} ) : 'ftp';

    # Create an XML DOM parser.
    my $parser = XML::LibXML->new();
    $parser->keep_blanks(0);

    # create an XML document containing one FilePacket node, which
    # contains one FileGroup node, which contains one File node.
    my $woDom = $parser->parse_string('<FilePacket/>');
    my $woDoc = $woDom->documentElement();

    # set attributes of the FilePacket node to describe a destination
    my ($filePacketNode) = $woDoc->findnodes('/FilePacket');
    $filePacketNode->setAttribute('status', 'I');
    my $destination = "$dotchartProtocol:$dotchartHost$dotchartDir";
    $filePacketNode->setAttribute('destination', $destination);
    
    # Set attributes of the File node to specify the local path of the file
    # being sent
    my $fileNode = XML::LibXML::Element->new('File');
    $fileNode->setAttribute('status', 'I');
    $fileNode->setAttribute('localPath', $localPath);
    $fileNode->setAttribute('cleanup', 'Y');

    my $fileGroupNode = XML::LibXML::Element->new('FileGroup');
    $fileGroupNode->appendChild($fileNode);
    $woDoc->appendChild($fileGroupNode);

    if ( open( SUBCONF, "> $localPath" ) ) {
        print SUBCONF $subscriptionDom->toString(1);
        close( SUBCONF );
    } else {
        $message = "Could not open $localPath for writing.";
        return ( undef, $message );
    }

    if ( open( SUBWO, "> $workOrder") ) {
        print SUBWO $woDom->toString(1);
        close( SUBWO );
    } else {
        unlink $localPath;
        $message = "Could not open $workOrder for writing.";
        return ( undef, $message );
    }

    $message = "work order DO.PUSH.DotchartSub_${instance}.wo " .
        "created for publishing subscription.";
    return ( $workOrder, $message ); 
}

sub getServiceOptions {
    my ( $service ) = shift;
    my @options = qw( CHANNELS CHNUMBERS WVNUMBERS VARIABLES 
        BBOX FORMAT COMPRESS_ID REASON );

    my $params = {};
    foreach my $option ( @options ) {
        next unless $service->hasAttribute( $option );
        my $attribute = $service->getAttribute( $option );

        my $optionList = [];
        foreach ( split  ',', $attribute ) {
            # remove whitespace
            s/\s//g;
            push @{$optionList}, $_;
        }
        $params->{$option} = $optionList; 
    }
    return $params;
}

