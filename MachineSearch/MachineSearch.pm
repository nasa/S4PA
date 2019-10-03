=pod
=head1 NAME

S4PA:MachineSearch - provides access to Machine Request Interface

=head1 SYNOPSIS

  use S4PA::MachineSearch;
  $search = S4PA::MachineSearch->new( %arg );
  $errorFlag = $search->onError();
  $errorMessage = $search->errorMessage();
  $s4paRoot = $search->getRoot();
  $shortName = $search->getDataset();
  $versionID = $search->getVersion();
  $startTime = $search->getStartTime();
  $endTime = $search->getEndTime();
  $overlap = $search->getOverlap();
  $exclusive = $search->getExclusive();
  $action = $search->getAction();

  $supported = $search->isSupported();
  $subscriptionID = $search->getSubscriptionID();
  $dataClass = $search->getDataClass();
  $storedVersion = $search->getStoreVersion();
  $datasetPath = $search->getDatasetPath();
  $dataPath = $search->getDataPath();
  $frequency = $search->getFrequency();
  @granuleLocatorList = $search->getGranuleLocator();
  $metadataFilePath = $search->locateGranule( $granuleLocator );
  $fileGroup = $search->getFileGroup( $granuleLocator );
  $woDom = $search->createWorkOrder();
  @granuleInfoList = $search->getGranuleInfo();

=head1 DESCRIPTION

S4PA::MachineSearch contains methods involved in Machine Request 
Interface. It provides methods for three basic MRI actions: 
search (request a search/order off-line), list (search granules on-line), 
order (place an order on requested granules). 'list' action provide 
two different format of granule listings. The default (short) format 
list the granule locator for each qualified granule in a comma-separated 
plain text format. The long format list the granule locator and data 
file(s) associated with each qualified granule in an XML format. A granule 
locator is the combination of <ShortName>[.VersionID]:<Metadata Filename>.

It can also be used for general granule search interface. It provides
methoeds to identify dataClass, storedVersion, frequency and to locate 
datasetPath, dataPath, metadataFilePath. 

=head1 AUTHOR

Guang-Dih Lei

=head1 METHODS

=cut

# $Id: MachineSearch.pm,v 1.16 2019/05/06 15:48:00 glei Exp $
# -@@@ S4PA, Version $Name:  $

package S4PA::MachineSearch;

use strict;
use Safe;
use XML::LibXML;
use S4PA::Storage;
use S4PA::GranuleSearch;
use vars '$AUTOLOAD';

################################################################################

=head2 Constructor

Description:
    Constructs the object from argument list of the machine search cgi script

Input:
    A hash containing required keys as: s4pa root directory, dataset, 
    startTime, endTime, and action. Optional keys include: subscription user,
    dataset version, list format. startTime and endTime has to be in the
    standard s4pa time format of 'YYYY-MM-DD HH:MI:SS'. Default action is 
    'search' and default list format is 'short' for granule locator only.

Output:
    Returns S4PA::MachineSearch object.

=cut

sub new
{
    my ( $class, %arg ) = @_;

    my $m2mSearch = {};
    $m2mSearch->{__S4PAROOT} = $arg{home};
    $m2mSearch->{__USER} = $arg{user};

    $m2mSearch->{__ACTION} = ( defined $arg{action} ) ?
        $arg{action} : 'search';
    if ( $m2mSearch->{__ACTION} eq 'list' ) {
        $m2mSearch->{__ACTION} = ( defined $arg{format} ) ?
            "list:$arg{format}" : 'list';
    }

    if ( defined $arg{dataset} ) {
        $m2mSearch->{__DATASET} = $arg{dataset};
    } else {
        $m2mSearch->{__ERROR} = "Dataset not supplied!";
    }

    if ( defined $arg{version} ) {
        $m2mSearch->{__VERSION} = $arg{version};
    }

    if ( defined $arg{startTime} ) {
        $m2mSearch->{__ERROR} = "Supply start date time as YYYY-MM-DD HH:MI:SS"
            unless ( $arg{startTime} =~ /\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/ );
        $m2mSearch->{__STARTTIME} = $arg{startTime};
    }

    if ( defined $arg{endTime} ) {
        $m2mSearch->{__ERROR} = "Supply end date time as YYYY-MM-DD HH:MI:SS"
            unless ( $arg{endTime} =~ /\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/ );
        $m2mSearch->{__ENDTIME} = $arg{endTime};
    }

    # default overlap search condition to be off, ticket #6550.
    if ( defined $arg{overlap} ) {
        $m2mSearch->{__OVERLAP} = 1;
    } else {
        $m2mSearch->{__OVERLAP} = 0;
    }

    # default exclusive search condition to be off, for association.
    if ( defined $arg{exclusive} ) {
        $m2mSearch->{__EXCLUSIVE} = 1;
    } else {
        $m2mSearch->{__EXCLUSIVE} = 0;
    }

    return bless( $m2mSearch, $class );
}

# Boolean returning method to check error status
sub onError
{
    my ( $self ) = @_;
    return ( defined $self->{__ERROR} ? 1 : 0 );
}

# Accessor to get current error message
sub errorMessage
{
    my ( $self ) = @_;
    return $self->{__ERROR};
}

################################################################################

=head2 getSubscriptionID

Description:
    Returns the subscription ID associated with current user.

Input:
    None.

Output:
    Subscription ID.

=cut

sub getSubscriptionID
{
    my ( $self ) = @_;

    unless ( defined $self->{__USER} ) {
        $self->{__ERROR} = 'User name not supplied';
        return undef;
    }
    my $user = $self->{__USER};

    # Check to see whether the machine search is allowed to begin with
    my $configFile = $self->{__S4PAROOT} . '/machine_search.cfg';
    unless ( -f $configFile ) {
        $self->{__ERROR} = "Machine search not supported on this host";
        return undef;
    }

    my $cpt = new Safe 'CFG';
    $cpt->share( '%cfg_machine_search_user' );
    unless ( $cpt->rdo( $configFile ) ) {
        $self->{__ERROR} = "Failed to open MRI configuration file";
        undef $cpt;
        return undef;
    }

    # Get the list of subscription IDs for this user
    my @subscriptionIdList = 
        defined $CFG::cfg_machine_search_user{$user}
        ? @{$CFG::cfg_machine_search_user{$user}} : ();
    unless ( @subscriptionIdList ) {
        $self->{__ERROR} = "User, $user, does not have any "
            . "subscriptions; create a subscription before searching";
    }
    undef $cpt;

    # Check to see if the subscription exists
    $configFile = $self->{__S4PAROOT} . '/subscribe/s4pa_subscription.cfg';
    $cpt = new Safe 'CFG';
    $cpt->share( '%cfg_subscriptions' );
    unless ( $cpt->rdo( $configFile ) ) {
        $self->{__ERROR} = "Failed to open subscription configuration file";
        undef $cpt;
        return undef;
    }

    # match the subscription id with the search dataset
    my $subscriptionID;
    foreach my $id ( @subscriptionIdList ) {
        next unless defined $CFG::cfg_subscriptions{ $id };
        foreach my $key ( %{$CFG::cfg_subscriptions{ $id }} ) {
            next if ( $key =~ /destination|notify|label|urlRoot|max_granule_count/);
            next unless ( $key =~ /^$self->{__DATASET}\.(.+)/ );
            my $version = $1;
            next unless ( $self->{__VERSION} =~ m{$version} || $self->{__VERSION} eq '' );
            $subscriptionID = $id;
            last;
        }
        last if defined $subscriptionID;
    }

    undef $cpt;
    unless ( defined $subscriptionID ) {
        $self->{__ERROR} = "User, $user, does not have any subscription "
            . "on $self->{__DATASET}";
        return undef;
    }
    return $subscriptionID;
}

################################################################################

=head2 getDataClass

Description:
    Returns the dataClass associated with the search dataset.

Input:
    None.

Output:
    DataClass name.

=cut

sub getDataClass
{
    my ( $self ) = @_;
    my $dataset = $self->{__DATASET};

    # Check whether the dataset requested is available
    my $configFile = $self->{__S4PAROOT} . '/storage/dataset.cfg';
    my $cpt = new Safe 'CFG';
    $cpt->share( '%data_class' );
    unless ( $cpt->rdo( $configFile ) ) {
        $self->{__ERROR} = "Failed to open dataset configuration file";
        return undef;
    }

    my $dataClass = defined $CFG::data_class{ $dataset }
        ? $CFG::data_class{ $dataset } : undef;
    unless ( defined $dataClass ) {
        $self->{__ERROR} = "Dataset, $dataset, not found!";
        return undef;
    }

    undef $cpt;
    return $dataClass;
}

################################################################################

=head2 getStoredVersion

Description:
    Returns the configured VersionID associated wtih the search version,
    returns null if dataset is versionless

Input:
    None.

Output:
    VersionID.

=cut

sub getStoredVersion
{
    my ( $self ) = @_;
    my $dataset = $self->{__DATASET};

    my $dataClass = $self->getDataClass;
    return undef if $self->onError;

    my $configFile = $self->{__S4PAROOT}
        . "/storage/$dataClass/store_$dataClass/s4pa_store_data.cfg";
    my $cpt = new Safe 'CFG';
    $cpt->share( '%cfg_data_version' );
    unless ( $cpt->rdo( $configFile ) ) {
        $self->{__ERROR} = "Failed to open storage configuration file";
        return undef;
    }

    my @supportedVersionList = ();
    @supportedVersionList = @{$CFG::cfg_data_version{ $dataset }}
        if ( %CFG::cfg_data_version
        && defined $CFG::cfg_data_version{ $dataset } );
    unless ( @supportedVersionList ) {
        $self->{__ERROR} = "Missing server-side version information "
            . "for $dataset";
        undef $cpt;
        return undef;
    }

    my $storedVersion;
    foreach my $version ( @supportedVersionList ) {
        if ( ($version eq '') || ($version eq $self->getVersion) ) {
            $storedVersion = $version;
            last;
        }
    }
    unless ( defined $storedVersion ) {
        $self->{__ERROR} = "Version couldn't be found for $dataset";
        undef $cpt;
        return undef;
    }

    undef $cpt;
    return $storedVersion;
}

################################################################################

=head2 getFrequency

Description:
    Returns the configured frequency associate wtih the dataset.

Input:
    None.

Output:
    Frequency ('daily', 'monthly', 'yearly').

=cut

sub getFrequency
{
    my ( $self ) = @_;
    my $dataset = $self->{__DATASET};

    my $dataClass = $self->getDataClass;
    return undef if $self->onError;

    my $version = $self->getStoredVersion;
    return undef if $self->onError;

    my $configFile = $self->{__S4PAROOT}
        . "/storage/$dataClass/store_$dataClass/s4pa_store_data.cfg";
    my $cpt = new Safe 'CFG';
    $cpt->share( '%cfg_temporal_frequency' );
    unless ( $cpt->rdo( $configFile ) ) {
        $self->{__ERROR} = "Failed to open storage configuration file";
        return undef;
    }

    my $frequency = $CFG::cfg_temporal_frequency{$dataset}->{$version};
    unless ( defined $frequency ) {
        $self->{__ERROR} = "Temporal frequency not defined for " .
            "$dataset.$version";
        undef $cpt;
        return undef;
    }

    undef $cpt;
    $self->{__FREQUENCY} = $frequency;
    return $frequency;
}

################################################################################

=head2 getDatasetPath

Description:
    Returns the path to the dataset directory.

Input:
    None.

Output:
    Directory path of the dataset.

=cut

sub getDatasetPath
{
    my ( $self ) = @_;

    my $dataClass = $self->getDataClass;
    return undef if $self->onError;

    my $storedVersion = $self->getStoredVersion;
    return undef if $self->onError;

    my $datasetPath = $self->{__S4PAROOT} . "/storage/$dataClass/"
        . $self->{__DATASET};
    $datasetPath .= ".$storedVersion" if ( $storedVersion ne '' );
    unless ( -d $datasetPath ) {
        $self->{__ERROR} = "Invalid directory: $datasetPath";
        return undef;
    }
    $self->{__DATASETPATH} = $datasetPath;
    return $datasetPath;
}

################################################################################

=head2 isSupported

Description:
    Verify if the search is supported, also registers 
    SUBSCRIPTIONID and DATASETPATH attribute to the class.

Input:
    None.

Output:
    Boolean. 1 if supported, undef if not supported.

=cut

# Verify the search is supported, also registers 
# SUBSCRIPTIONID and DATASETPATH attribute to the class.
sub isSupported
{
    my ( $self ) = @_;
    my $subscriptionID = $self->getSubscriptionID;
    return undef if $self->onError;
    $self->{__SUBSCRIPTIONID} = $subscriptionID;

    my $datasetPath = $self->getDatasetPath;
    return undef if $self->onError;
    $self->{__DATASETPATH} = $datasetPath;

    my $frequency = $self->getFrequency;
    return undef if $self->onError;
    $self->{__FREQUENCY} = $frequency;

    return 1;
}

################################################################################

=head2 locateGranule

Description:
    Returns the full path of the metadata file for the Granule Locator.

Input:
    Granule locator: <ShortName>[.VersionID]:<Metadata Filename>

Output:
    Full path of the metadata file of the granule locator,
    Undef if granule does not exist.

=cut

sub locateGranule
{
    my ( $self, $granuleLocator ) = @_;

    my $datasetPath = $self->{__DATASETPATH};
    my $dbFile = $datasetPath . "/granule.db";
    return undef if $self->onError;

    # open granule.db
    my %granuleHash;
    my ( $granuleHash, $fileHandle ) 
        = S4PA::Storage::OpenGranuleDB( $dbFile, "r" );
    unless ( defined $granuleHash ) { 
        $self->{__ERROR} = "Unable to open $dbFile"; 
        return undef; 
    }

    my ( $dataset, $xmlFile ) = split /:/, $granuleLocator;
    my $record = $granuleHash->{ $xmlFile };
    unless ( defined $record ) {
        $self->{__ERROR} .= "Record not found for $xmlFile in $dbFile";
        return undef;
    }

    my $xmlPath = readlink( "$datasetPath/data" ) . "$record->{date}/";
    $xmlPath .= '.hidden/' if ( $record->{mode} == 0640 || $record->{mode} == 0600 );
    $xmlPath .= $xmlFile;

    unless ( -f $xmlPath ) {
        $self->{__ERROR} .= "File, $xmlPath, does not exist";
        return undef;
    }

    S4PA::Storage::CloseGranuleDB( $granuleHash, $fileHandle );
    return $xmlPath;
}

################################################################################

=head2 getDataPath

Description:
    Returns the link path to the search dataset's data directory.

Input:
    None.

Output:
    Actual directory path to dataset storage root directory.

=cut

sub getDataPath
{
    my ( $self ) = @_;

    $self->getDatasetPath() unless ( defined $self->{__DATASETPATH} );
    my $datasetPath = $self->{__DATASETPATH};
    my $linkPath = $datasetPath . "/data";
    my $dataPath = readlink( $linkPath );
    unless ( -d $dataPath ) {
        $self->{__ERROR} = "Invalid data path: $dataPath";
        return undef;
    }

    return $dataPath;
}

################################################################################

=head2 createWorkOrder

Description:
    Returns a work order styled XML DOM object for the machine search station.

Input:
    None.

Output:
    XML DOM object. 

=cut

sub createWorkOrder
{
    my ( $self ) = @_;

    my $dataPath = $self->getDataPath;
    my $subID = $self->{__SUBSCRIPTIONID};
    return undef if $self->onError;

    # Create an XML parser and DOM
    my $xmlParser = XML::LibXML->new();
    $xmlParser->keep_blanks(0);

    my $Dom = $xmlParser->parse_string( "<input />" );
    my $Doc = $Dom->documentElement();
    $Doc->setAttribute( "dataset", $self->{__DATASET} );
    $Doc->setAttribute( "dataPath", $dataPath );
    $Doc->setAttribute( "id", $subID );
    $Doc->setAttribute( "startTime", $self->{__STARTTIME} );
    $Doc->setAttribute( "endTime", $self->{__ENDTIME} );
    $Doc->setAttribute( "frequency", $self->getFrequency );
    $Doc->setAttribute( "overlap", $self->getOverlap );
    $Doc->setAttribute( "exclusive", $self->getExclusive);

    return $Dom;
}

################################################################################

=head2 getGranuleLocator

Description:
    Based on the search criteira, this method create a GranuleSearch
    object to find all metadata file in the covering range data, then
    parse each granule to return all qualified granule locators.

Input:
    None.

Output:
    An array of qualified granule locators.

=cut

sub getGranuleLocator
{
    my ( $self ) = @_;

    my %arg;
    $arg{dataPath} = $self->getDataPath;
    $arg{startTime} = $self->getStartTime;
    $arg{endTime} = $self->getEndTime;
    $arg{overlap} = $self->getOverlap;
    $arg{exclusive} = $self->getExclusive;
    $arg{frequency} = $self->getFrequency;
    $arg{action} = 'list';

    my $granSearch = S4PA::GranuleSearch->new( %arg );
    return undef if $granSearch->onError;

    my @locatorList;
    my @xmlFileList = $granSearch->findMetadataFile();
    return undef if $granSearch->onError;

    if ( @xmlFileList ) {
        foreach my $xmlFile ( @xmlFileList ) {
            my $granuleLocator = $granSearch->parseGranule( $xmlFile );
            next if $granSearch->onError;
            next unless ( defined $granuleLocator );
            push ( @locatorList, $granuleLocator );
        }
    }

    return @locatorList;
}

################################################################################

=head2 getGranuleInfo

Description:
    Based on the search criteira, this method create a GranuleSearch
    object to find all metadata file in the covering range data, then
    parse each granule to return an array of granule information hash
    which includes granule locator and all data files.

Input:
    None.

Output:
    An array of qualified granule info hash.

=cut

sub getGranuleInfo
{
    my ( $self ) = @_;

    my %arg;
    $arg{dataPath} = $self->getDataPath;
    $arg{startTime} = $self->getStartTime;
    $arg{endTime} = $self->getEndTime;
    $arg{overlap} = $self->getOverlap;
    $arg{exclusive} = $self->getExclusive;
    $arg{frequency} = $self->getFrequency;
    $arg{action} = 'list:long';

    my $granSearch = S4PA::GranuleSearch->new( %arg );
    return undef if $granSearch->onError;

    my @granuleInfo;
    my @xmlFileList = $granSearch->findMetadataFile();
    return undef if $granSearch->onError;

    if ( @xmlFileList ) {
        foreach my $xmlFile ( @xmlFileList ) {
            my $granule = $granSearch->parseGranule( $xmlFile );
            next if $granSearch->onError;
            next unless ( defined $granule->{locator} );
            push ( @granuleInfo, $granule );
        }
    }

    return @granuleInfo;
}

################################################################################

=head2 getFileGroup

Description:
    Returns the FileGroup object for PDR creation of the granule locator.

Input:
    Granule locator in form of <ShortName>[.<VersionID>]:<Metadata Filename>

Output:
    A FileGroup object of the granule.

=cut

sub getFileGroup
{
    my ( $self, $granuleLocator ) = @_;

    my $xmlFile = $self->locateGranule( $granuleLocator );
    return undef if $self->onError;

    my %arg;
    $arg{dataPath} = $self->getDataPath;
    $arg{action} = $self->getAction;
    $arg{overlap} = $self->getOverlap;
    $arg{exclusive} = $self->getExclusive;
    my $granSearch = S4PA::GranuleSearch->new( %arg );
    my $fileGroup = $granSearch->parseGranule( $xmlFile );
    if ( $granSearch->onError ) {
        $self->{__ERROR} = $granSearch->errorMessage;
        return undef;
    }

    return $fileGroup;
}

################################################################################

=head2 Accessor Methods

Description:
    Has accessor methods for S4PA::MachineSearch.
    getRoot(): returns the s4pa root directory.
    getDataset(): returns the ShortName.
    getVersion(): returns the VersionID.
    getFrequency(): returns the dataset storage frequency.
    getStartTime(): returns the search startTime.
    getEndTime(): returns the search endTime.
    getOverlap(): returns the StartTime search overlap flag.
    getExclusive(): returns the StartTime search exclusive flag.
    getAction(): returns the search action (search|order|list).
    getSubscriptionID(): returns the subscription ID of the user.
    getDatasetPath(): returns the storage directory path of the dataset.
    onError(): returns boolean (1/0) to indicate error condition.
    errorMessage(): returns the last error message.

Input:
    None.

Output:
    A hash ref containing attributes of the object.

=cut

sub AUTOLOAD
{
    my ( $self, @arg ) = @_;

    return undef if $self->onError();    

    if ( $AUTOLOAD =~ /.*::getRoot/ ) {
        return $self->{__S4PAROOT};
    } elsif ( $AUTOLOAD =~ /.*::getDataset/ ) {
        return $self->{__DATASET};
    } elsif ( $AUTOLOAD =~ /.*::getVersion/ ) {
        return $self->{__VERSION};
    } elsif ( $AUTOLOAD =~ /.*::getFrequency/ ) {
        return $self->{__FREQUENCY};
    } elsif ( $AUTOLOAD =~ /.*::getStartTime/ ) {
        return $self->{__STARTTIME};
    } elsif ( $AUTOLOAD =~ /.*::getEndTime/ ) {
        return $self->{__ENDTIME};
    } elsif ( $AUTOLOAD =~ /.*::getOverlap/ ) {
        return $self->{__OVERLAP};
    } elsif ( $AUTOLOAD =~ /.*::getExclusive/ ) {
        return $self->{__EXCLUSIVE};
    } elsif ( $AUTOLOAD =~ /.*::getAction/ ) {
        return $self->{__ACTION};
    } elsif ( $AUTOLOAD =~ /.*::getSubscriptionID/ ) {
        return $self->{__SUBSCRIPTIONID};
    } elsif ( $AUTOLOAD =~ /.*::getDatasetPath/ ) {
        return $self->{__DATASETPATH};
    }
    return undef;
}
1;

