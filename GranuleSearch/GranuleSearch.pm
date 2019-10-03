=pod
=head1 NAME

S4PA:GranuleSearch - provides methods for granule searching.

=head1 SYNOPSIS

  use S4PA::GranuleSearch;
  $search = S4PA::GranuleSearch->new( %arg );
  $errorFlag = $search->onError();
  $errorMessage = $search->errorMessage();
  $dataPath = $search->getDataPath();
  $subscriptionID = $search->getSubscriptionID();
  $startTime = $search->getStartTime();
  $endTime = $search->getEndTime();
  $action = $search->getAction();
  $frequency = $search->getFrequency();
  $overlap = $search->getOverlap();
  $exclusive = $search->getExclusive();

  @dirList = $search->getDataDirs();
  @xmlFileList = $search->findMetadataFile( [$dataPath] );
  $fileGroup = $search->parseGranule( $metadataFile );
  $granuleLocator = $search->parseGranule( $metadataFile );
  $granuleHash = $search->parseGranule( $metadataFile );
  $pdr = $search->createPdr();

=head1 DESCRIPTION

S4PA::GranuleSearch contains methods involved in Machine Search station.
It provides methods for locating qualified granule metadata file, creating
FileGroup or GranuleLocator, and PDR.

=head1 AUTHOR

Guang-Dih Lei

=head1 METHODS

=cut

# $Id: GranuleSearch.pm,v 1.13 2010/09/20 19:27:18 glei Exp $
# -@@@ S4PA, Version $Name:  $

package S4PA::GranuleSearch;

use strict;
use XML::LibXML;
use S4P::PDR;
use S4P::TimeTools;
use S4PA::Metadata;
use vars '$AUTOLOAD';

################################################################################

=head2 Constructor

Description:
    Constructs the object from storage dataPath, startTime, and endTime.

Input:
    A hash containing required keys as: dataPath containing metadata files,
    startTime, endTime, and action. Optional keys include: subscription ID,
    data storage frequency. startTime and endTime has to be in the standard 
    s4pa time format of 'YYYY-MM-DD HH:MI:SS'. Default action is 'order'. 

Output:
    Returns S4PA::GranuleSearch object.

=cut

sub new
{
    my ( $class, %arg ) = @_;

    my $granSearch = {};
    $granSearch->{__ACTION} = ( defined $arg{action} ) ?
        $arg{action} : 'order';

    $granSearch->{__SUBSCRIPTIONID} = ( defined $arg{id} ) ?
        $arg{id} : undef;

    $granSearch->{__FREQUENCY} = ( defined $arg{frequency} ) ?
        $arg{frequency} : undef;

    if ( defined $arg{dataPath} ) {
        $granSearch->{__DATAPATH} = $arg{dataPath};
        unless ( -d $arg{dataPath} ) {
            $granSearch->{__ERROR} = "Invalid directory path: $arg{dataPath}";
        }
    }

    if ( defined $arg{startTime} ) {
        $granSearch->{__ERROR} = "Supply start date time (YYYY-MM-DD HH:MM:SS"
            unless ( $arg{startTime} =~ /\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/ );
        $granSearch->{__STARTTIME} = $arg{startTime};
    }

    if ( defined $arg{endTime} ) {
        $granSearch->{__ERROR} = "Supply end date time (YYYY-MM-DD HH:MM:SS"
            unless ( $arg{endTime} =~ /\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/ );
        $granSearch->{__ENDTIME} = $arg{endTime};
    }

    # default overlap search condition to be off, ticket #6550.
    if ( defined $arg{overlap} ) {
        $granSearch->{__OVERLAP} = $arg{overlap};
    } else {
        $granSearch->{__OVERLAP} = 0;
    }

    # default exclusive search condition to be off, for association
    if ( defined $arg{exclusive} ) {
        $granSearch->{__EXCLUSIVE} = $arg{exclusive};
    } else {
        $granSearch->{__EXCLUSIVE} = 0;
    }

    return bless( $granSearch, $class );
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

=head2 findMetadataFile

Description:
    Returns an array of metadata files under the specified or DataPath directory.
    If attribute FREQUENCY was defined, only directories between startTime
    and endTime will be searched. Otherwise, all directories will be searched
    recurrsively.

Input:
    Optional directory path.

Output:
    An array of metadata file path.

=cut

sub findMetadataFile
{
    my ( $self, $dirName ) = @_;

    my @xmlList = ();
    unless ( defined $dirName ) {
        if ( defined $self->{__FREQUENCY} ) {
        # this is coming from a machine search with FREQUCECY attribute
        # search for qualify directories instead of scaning the whole dataset
            my @dataDirs = $self->getDataDirs();
            return undef if $self->onError;

            foreach my $dataDir ( @dataDirs ) {
                my @list = $self->findMetadataFile( $dataDir );
	        push @xmlList, @list;
            }
            return @xmlList;

        } else {
        # this could be a general search with specific a DATAPATH attribute
            $dirName = $self->{__DATAPATH} unless ( defined $dirName );
            unless ( -d $dirName ) {
                $self->{__ERROR} = "Invalid Directory: $dirName";
                return undef;
            }
        }
    }

    $dirName .= '/' unless ( $dirName =~ /\/$/ );

    my @contentList = ();

    if ( opendir( FH, $dirName ) ) {
        @contentList = map{ "$dirName$_" } grep( !/^\.+$/, readdir( FH ) );
        close( FH );
        foreach my $entry ( @contentList ) {
            if ( -d $entry ) {
                my @list = $self->findMetadataFile( $entry );
	        push @xmlList, @list;
            } elsif ( -f $entry && ( $entry =~ /\.xml$/ ) ) {
                push @xmlList, $entry;
            }
        }
    } else {
        $self->{__ERROR} = "Failed to open $dirName for reading ($!)";
        return undef;
    }
    return @xmlList;
}

################################################################################

=head2 parseGranule

Description:
    By constructing a Metadata object with the seaching startTime and endTime,
    it returns a FileGroup object for qualilfied granule of the search or 
    order action. For list action, it returns the granule locator for the
    default short format and granule info hash for the long format.

Input:
    Full path of a metadata file.

Output:
    FileGroup for 'search' or 'order' action.
    GranuleLocator for 'list' action and 
    GranuleHash for 'list:long' action.

=cut

sub parseGranule
{
    my ( $self, $xmlFile ) = @_;

    my %search;
    $search{START} = $self->{__STARTTIME};
    $search{END} = $self->{__ENDTIME};
    $search{OVERLAP} = $self->{__OVERLAP};
    $search{EXCLUSIVE} = $self->{__EXCLUSIVE};
    $search{FILE} = $xmlFile;

    my $metadata = new S4PA::Metadata( %search );
    return undef if $metadata->onError();
    
    if ( $self->{__ACTION} =~ /list(.*)/ ) {
        my $format = $1;

        # return empty if granule not in search range
        return undef unless ( $metadata->compareDateTime( %search ) );

        # return granule locator string for default listing
        my $locator = $metadata->getGranuleLocator();
        return $locator unless ( $format =~ /long/ );

        my $granuleHash = {};
        $granuleHash->{locator} = $locator;
        $granuleHash->{shortName} = $metadata->getShortName();
        $granuleHash->{versionID} = $metadata->getVersionID();
        $granuleHash->{granuleID} = 
                $metadata->getValue( '//DataGranule/GranuleID' );

        $granuleHash->{insertDateTime} = 
                    $metadata->getValue( '//DataGranule/InsertDateTime' );
        $granuleHash->{productionDateTime} = 
                    $metadata->getValue( '//DataGranule/ProductionDateTime' );
        my $beginDate = $metadata->getValue( '//RangeDateTime/RangeBeginningDate' );
        $beginDate =~ s/T|Z//;
        my $beginTime = $metadata->getValue( '//RangeDateTime/RangeBeginningTime');
        $beginTime =~ s/T|Z//;
        $granuleHash->{rangeBegin} = "$beginDate $beginTime";
        my $endDate = $metadata->getValue( '//RangeDateTime/RangeEndingDate' );
        $endDate =~ s/T|Z//;
        my $endTime = $metadata->getValue( '//RangeDateTime/RangeEndingTime');
        $endTime =~ s/T|Z//;
        $granuleHash->{rangeEnd} = "$endDate $endTime";

        $granuleHash->{checkSumType} = $metadata->getValue( '//DataGranule/CheckSum/CheckSumType' );
        $granuleHash->{files} = $metadata->getFiles();
        $granuleHash->{fileAttribute} = $metadata->getFileAttributes();
        return $granuleHash;

    } elsif ( $self->{__ACTION} eq 'order' ) {
        return $metadata->getFileGroup();

    } elsif ( $self->{__ACTION} eq 'search' ) {
        return $metadata->getFileGroup()
            if $metadata->compareDateTime( %search );

    }
    return undef;
}

################################################################################

=head2 createPdr

Description:
    Construct a PDR object with all qualified granule of the search.
    SUBSCRIPTION will be added in PDR only if it is defined.

Input:
    None.

Output:
    a PDR object.

=cut

sub createPdr
{
    my ( $self ) = @_;

    my @xmlFileList = $self->findMetadataFile();
    return undef if $self->onError;

    my $pdr = S4P::PDR::create();
    $pdr->{originating_system} = "S4PA_Machine_Search";
    $pdr->{subscription_id} = $self->{__SUBSCRIPTIONID}
        if ( defined $self->{__SUBSCRIPTIONID} );
    foreach my $xmlFile ( @xmlFileList ) {
        my $fileGroup = $self->parseGranule( $xmlFile );
        $pdr->add_file_group( $fileGroup )
            if ( defined $fileGroup );
    }

    return $pdr;
}

################################################################################

=head2 getDataDirs

Description:
    Based on the search FREQUENCY, it returns all directories path between 
    search startTime and endTime. 

Input:
    None.

Output:
    an array of qualified directories.

=cut

sub getDataDirs 
{
    my ( $self ) = @_;

    my $dataHome = $self->{__DATAPATH};
    $dataHome .= "/" unless ( $dataHome =~ /\/$/ );
    $self->{__STARTTIME} =~ /(\d{4})-(\d{2})-(\d{2}) \d{2}:\d{2}:\d{2}/;
    my ( $startYear, $startMonth, $startDay ) = ( $1, $2, $3 );
    $self->{__ENDTIME} =~ /(\d{4})-(\d{2})-(\d{2}) \d{2}:\d{2}:\d{2}/;
    my ( $endYear, $endMonth, $endDay ) = ( $1, $2, $3 );
    my $frequency = $self->{__FREQUENCY};

    my @dataDirs = ();
    my $dataDir;
    if ( $frequency eq 'yearly' ) {
        # under overlap/exclusive search condition, extend one year before the startYear
        $startYear-- if ( $self->getOverlap() || $self->getExclusive() );

        foreach my $dataYear ( $startYear .. $endYear ) {
            $dataDir = $dataHome . $dataYear;
            push ( @dataDirs, $dataDir ) if ( -d $dataDir );
        }

    } elsif ( $frequency eq 'monthly') {
        # under overlap/exclusive search condition, extend one month before the startMonth
        if ( $self->getOverlap() || $self->getExclusive() ) {
            if ( $startMonth eq '01' ) {
                $startMonth = '12';
                $startYear--;
            } else {
                $startMonth = sprintf( "%2.2d", $startMonth-- );
            }
        }

        foreach my $dataYear ( $startYear .. $endYear ) {
            $dataDir = $dataHome . $dataYear . '/';
            my $firstMonth = ( $dataYear == $startYear ) ? $startMonth : 1;
            my $lastMonth = ( $dataYear == $endYear ) ? $endMonth : 12;
            foreach my $dataMonth ( $firstMonth .. $lastMonth ) {
                my $monthDir = $dataDir . sprintf( "%2.2d", $dataMonth );
                push ( @dataDirs, $monthDir ) if ( -d $monthDir );
            }
        }

    } elsif ( $frequency eq 'daily' ) {
        # under overlap/exclusive search condition, extend one day before the startDay
        if ( $self->getOverlap() || $self->getExclusive() ) {
            my ( $newYear, $newMonth, $newDay ) = 
                S4P::TimeTools::add_delta_days( $startYear, $startMonth, $startDay, -1 );
            $startYear = $newYear;
            $startMonth = sprintf( "%2.2d", $newMonth );
            $startDay = sprintf( "%2.2d", $newDay );
        }

        my $startDoy = S4P::TimeTools::day_of_year($startYear,
                                                   $startMonth,
                                                   $startDay);
        my $endDoy = S4P::TimeTools::day_of_year($endYear,
                                                 $endMonth,
                                                 $endDay);
        foreach my $dataYear ( $startYear .. $endYear ) {
            $dataDir = $dataHome . $dataYear . '/';
            my $firstDoy = ( $dataYear == $startYear ) ? $startDoy : 1;
            my $lastDoy = ( $dataYear == $endYear ) ? $endDoy : 366;
            foreach my $dataDoy ( $firstDoy .. $lastDoy ) {
                my $doyDir = $dataDir . sprintf( "%3.3d", $dataDoy );
                push ( @dataDirs, $doyDir ) if ( -d $doyDir );
            }
        }

    } elsif ( $frequency eq 'none' ) {
        # for climatology dataset
        $dataHome =~ s/\/$//;
        push ( @dataDirs, $dataHome );

    } else {
        $self->{__ERROR} = "Temporal frequency, $frequency, not supported";
        return undef;
    }
    return @dataDirs;
}

################################################################################

=head2 Accessor Methods

Description:
    Has accessor methods for S4PA::GranuleSearch.
    getDataPath(): returns storage data path.
    getSubscriptionID(): returns the Subscription ID.
    getStartTime(): returns the search startTime.
    getEndTime(): returns the search endTime. 
    getAction(): returns the search action.
    getFrequency(): returns the dataset storage frequency.
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
    if ( $AUTOLOAD =~ /.*::getDataPath/ ) {
        return $self->{__DATAPATH};
    } elsif ( $AUTOLOAD =~ /.*::getSubscriptionID/ ) {
        return $self->{__SUBSCRIPTIONID};
    } elsif ( $AUTOLOAD =~ /.*::getStartTime/ ) {
        return $self->{__STARTTIME};
    } elsif ( $AUTOLOAD =~ /.*::getEndTime/ ) {
        return $self->{__ENDTIME};
    } elsif ( $AUTOLOAD =~ /.*::getAction/ ) {
        return $self->{__ACTION};
    } elsif ( $AUTOLOAD =~ /.*::getFrequency/ ) {
        return $self->{__FREQUENCY};
    } elsif ( $AUTOLOAD =~ /.*::getOverlap/ ) {
        return $self->{__OVERLAP};
    } elsif ( $AUTOLOAD =~ /.*::getExclusive/ ) {
        return $self->{__Exclusive};
    }
    return undef;
}
1;

