#!/usr/bin/perl -T

=head1 NAME

s4pa_m2m_cgi.pl - CGI interface to machine listing/search service.

=head1 SYNOPSIS

Invoked via CGI.
http://.../cgi-bin/s4pa_m2m_cgi.pl
?B<dataset>=I<ShortName>
&B<startTime>=I<RangeBeginTime 'yyyy-mm-dd hh-mi-ss'>
&B<endTime>=I<RangeEndTime 'yyyy-mm-dd hh-mi-ss'>
[&B<version>=I<VersionID>]
[&B<action>=I<search|list|list:long>]
[&B<home>=I<s4pa_root_directory>]

=head1 DESCRIPTION

s4pa_m2m_cgi.pl is the CGI interfact to machine listing/search service. Expects
root directory (CGI variable=home) of the S4PA instance and uses HTTP basic
authentication. It provides methods for three basic MRI actions:
search (request a search/order off-line),
list (search granules on-line; output listing in short format),
list:long (search granules on-line; output listing in long format).
The short format lists the granule locator for each
qualified granule in a comma-separated plain text format.
The long format lists the granule locator and data file(s)
associated with each qualified granule in an XML format.
A granule locator is the combination of
<ShortName>[.VersionID]:<Metadata Filename>.

=head1 AUTHOR

M. Hegde, Adnet
Guang-Dih. Lei, Adnet

=cut

################################################################################
# $Id s4pa_m2m_cgi.pl,v 1.32 2010/05/04 12:51:56 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

BEGIN{
    if ( $ENV{SCRIPT_FILENAME} =~ /\/tools\/gdaac\/(\w+)\// ) {
        my $path = "/tools/gdaac/$1/share/perl5/";
        push( @INC, $path );
        $path = "/tools/gdaac/$1/lib/perl5/site_perl/". sprintf( "%vd", $^V );
        push( @INC, $path );
    }
}
use strict;
use CGI;
use Safe;
use XML::LibXML;
use File::Copy;
use File::Basename;
use S4P;
use S4P::PDR;
use S4PA::MachineSearch;

my $cgi = CGI->new();
my %input = $cgi->Vars;
my @uploadedList;
my %actionAlias = ('Order' => 'search',
                   'List Granule(s)' => 'list',
                   'List Granule Info' => 'list:long');

front_page($cgi) if ( !(defined $input{keywords}) &&
                      ( !($input{dataset}) ||
                        !($input{startTime}) ||
                        !($input{endTime})
                      ) );

if ( !($input{action}) ) {
    $input{action} = 'search';
} else {
    # Change value of key action for input hash
    my $readAction = $input{action};
    $input{action} = $actionAlias{$readAction} if (defined $actionAlias{$readAction});
}

# Create an XML parser and DOM for the request result message
my $xmlParser = XML::LibXML->new();
$xmlParser->keep_blanks(0);
my $resultDom = $xmlParser->parse_string( '<result />' );
my $resultDoc = $resultDom->documentElement();

# Validate S4PA root directory
unless ( defined $input{home} ) {
    # some http setup might use QUERY_STRING to pass s4pa root 
    if ( $ENV{QUERY_STRING} =~ /&/ ) {
        $input{home} = $1 if ( $ENV{QUERY_STRING} =~ /home=([^&].+)&/ );
    } else {
        $input{home} = $1 if ( $ENV{QUERY_STRING} =~ /home=(.+)/ );
    }
    $input{user} = $1 if ( $ENV{QUERY_STRING} =~ /user=([^&].+)/ );
}
$input{home} = ValidateInput( HOME => $input{home} );
ExitOnFailure( $resultDom, "Data archive not defined; check proxy configuration!" )
    unless defined $input{home};
ExitOnFailure( $resultDom, "Data archive can not be found!" )
    unless ( -d $input{home} );

# Validate Username
if ( defined $ENV{REMOTE_USER} ) {
    $input{user} = ValidateInput( USER => $ENV{REMOTE_USER} );
} else {
    $input{user} = ValidateInput( USER => $input{user} );
}
ExitOnFailure( $resultDom, "Username invalid" ) unless defined $input{user};
    
if ( defined $input{keywords} ) {
    $input{keywords} = ValidateInput( KEYWORDS => $input{keywords} );
    $input{action} = 'order';
} else {
    # Validate action
    $input{action} = 'search' unless defined $input{action};
    $input{action} = ValidateInput( ACTION => $input{action} );
    ExitOnFailure( $resultDom, "Valid actions are: list, list:long, search" )
        unless defined $input{action};

    # Validate dataset name
    $input{dataset} = ValidateInput( DATASET => $input{dataset} );
    ExitOnFailure( $resultDom, "Dataset is not valid" )
        unless defined $input{dataset};

    if ( defined $input{version} ) {
	$input{version} = ValidateInput( VERSION => $input{version} );
	ExitOnFailure( $resultDom, "Version is not valid" )
	    unless defined $input{version};
    }
    # Validate time fields
    $input{endTime} = ValidateInput( ENDTIME => $input{endTime} );
    $input{startTime} = ValidateInput( STARTTIME => $input{startTime} );
    ExitOnFailure( $resultDom, "Please supply startTime as YYYY-MM-DD HH:MI:SS" )
        unless defined $input{startTime};
    ExitOnFailure( $resultDom, "Please supply endTime as YYYY-MM-DD HH:MI:SS" )
        unless defined $input{endTime};
}

# For ordering from a uploaded file, assuming no dataset parameter
# was passed in through the wget command. So, we have to construct
# the search object from granule locator to find the actual stored
# version id.
unless ( defined $input{dataset} ) {    
    my $orderDataset;
    # remove NULL character from upload file
    $input{keywords} =~ s/\0//g;

    # split granule locator at comma (,)
    @uploadedList = split( /,/, $input{keywords} );

    # loop through each granule locator
    foreach my $entry ( @uploadedList ) {

        # split dataset and granuleid at colon (:)
        my ( $dataset, $granule ) = split( '\:', $entry, 2);

        # split dataset and version if any at period (.)
        my ( $shortname, $version ) = split( /\./, $dataset, 2 );

	$version = '' unless defined $version;
        $shortname = ValidateInput( DATASET => $shortname );
        ExitOnFailure( $resultDom, "Dataset is not valid" )
            unless defined $shortname;
        $version = ValidateInput( VERSION => $version );
        ExitOnFailure( $resultDom, "Version is not valid" )
            unless defined $version;
                    
        # record the dataset name from the first granule locator
        $orderDataset = $shortname unless defined $orderDataset;
        my $errorMessage;
        if ( not defined $shortname ) {
            # Make sure that the short name is found.            
            $errorMessage = "Short name not found in $entry";
        } elsif ( $shortname ne $orderDataset ) {
            # make sure there is no mixing of different datasets in this order
            $errorMessage = "Mixed dataset not allowed!"
                . " ($shortname/$orderDataset)";
        } else {
            # construct a MachineSearch object from the first
            # granule locator to identified the actual stored version id
            $input{dataset} = $shortname;
            $input{version} = $version;
            $input{action} = 'order';
            my $versionSearch = S4PA::MachineSearch->new( %input );
            $errorMessage = $versionSearch->errorMessage
                unless ( $versionSearch->isSupported );
            $input{version} = $versionSearch->getStoredVersion;
        }
        ExitOnFailure( $resultDom, $errorMessage )
            if ( defined $errorMessage );

    }
}

# make sure all information is supplied 
my $search = S4PA::MachineSearch->new( %input );
ExitOnFailure( $resultDom, $search->errorMessage )
    unless ( $search->isSupported );

my $action = $search->getAction;
if ( $action eq 'search' ) {
    # default search action will create a work order for
    # machine search station to performe the seach.
    my $woId = ActionSearch( $search, $resultDom );
    ExitWithMessage( $resultDom, "Request, $woId, accepted", "success" );
} elsif ( $action eq 'order' ) {
    # order action will create a PDR embedded work order for
    # machine search station to pass to subscribe station.
    my $pdrId = ActionOrder( $search, $resultDom );
    ExitWithMessage( $resultDom, "Order, $pdrId, accepted", "success" );
} elsif ( $action =~ /list/ ) {
    # list action will do the search on the fly,
    # the http section might timed out if the search range is too big
    if ( $action eq 'list:long' ) {
        my $granuleList = ActionListLong( $search, $resultDom );
        ExitWithMessage( $resultDom, "No matching granule found", "success" )
            unless defined $granuleList;
        ExitWithMessage( $granuleList );
    } else {
        my $locatorList = ActionList( $search, $resultDom );
        ExitWithMessage( $resultDom, "No matching granule found", "success" )
            unless ( defined $locatorList );
        print $cgi->header( -type => 'text/plain' );
        print $locatorList;
    }
}

# Prints the passed XML document with status attached
sub PrintOutput
{
    my ( $dom, $message, $status ) = @_;
    my $doc = $dom->documentElement();
    if ( defined $status ) {
        my $messageNode = XML::LibXML::Element->new( 'message' );
        $messageNode->appendText( $message );
        $doc->appendChild( $messageNode );
        $doc->setAttribute( "status", $status );
    }
    print $dom->toString( 0 );
}

# Prints the XML document with the passed message
sub ExitWithMessage
{
    print $cgi->header( -type => 'text/xml' );
    PrintOutput( @_ );
    exit;
}

# Prints the XML document with an error message
sub ExitOnFailure
{
    ExitWithMessage( @_, "failure" );
}

sub CreateWo
{
    my ( $Dom, $destDir ) = @_;

    my $woId = 'P' . $$ . 'T' . sprintf( "%x", time() );
    my $woFile = "/var/tmp/DO.SEARCH.$woId.wo";
    my $errorMessage;
    if ( open( FH, ">$woFile" ) ) {
        chmod( 0666, $woFile );
        print FH $Dom->toString( 1 ), "\n";
        unless ( close( FH ) ) {
            $errorMessage = "Failed to create the request ($!)";
            return $errorMessage;
        }
        unless ( move( $woFile, $destDir ) ) {
            $errorMessage = "Failed to move the request ($!)";
            return $errorMessage;
        }
    } else {
        $errorMessage = "Failed to create the request ($!)";
        return $errorMessage;
    }

    return $woId;
}

sub ActionSearch
{
    my ( $search, $resultDom ) = @_;
    my $woDom = $search->createWorkOrder();
    my $stationDir = $search->getRoot() . "/other/machine_search/";

    my $status = CreateWo( $woDom, $stationDir );
    ExitOnFailure( $resultDom, $status ) 
        if ( $status =~ /Fail/ );

    return $status;
}

sub ActionOrder
{
    my ( $search, $resultDom ) = @_;
    my $stationDir = $search->getRoot() . "/other/machine_search/";
    my $subId = $search->getSubscriptionID;

    my $pdr = S4P::PDR::create();
    foreach my $locator ( @uploadedList ) {
        my $fileGroup = $search->getFileGroup( $locator );
        ExitOnFailure( $resultDom, $search->errorMessage )
            if $search->onError;
        $pdr->add_file_group( $fileGroup );
    }
    $pdr->{originating_system} = "S4PA_Machine_Search";
    $pdr->{subscription_id} = $subId;

    # Create an XML parser and DOM
    my $pdrParser = XML::LibXML->new();
    $pdrParser->keep_blanks(0);

    # Embed the PDR inside the XML style work order
    my $pdrDom = $pdrParser->parse_string( "<order />" );
    my $pdrDoc = $pdrDom->documentElement();
    $pdrDoc->appendText( $pdr->sprint() );

    my $status = CreateWo( $pdrDom, $stationDir );
    ExitOnFailure( $resultDom, $status ) 
        if ( $status =~ /Fail/ );

    return $status;
}

sub ActionList
{
    my @granuleList = $search->getGranuleLocator;
    ExitOnFailure( $resultDom, $search->errorMessage ) if $search->onError;

    return undef unless ( @granuleList );
    my $listMessage;
    foreach my $granule ( @granuleList ) {
        $listMessage .= "$granule,\n";
    }
    return $listMessage;
}

sub ActionListLong
{
    my ( $search, $resultDom ) = @_;

    my @granuleList = $search->getGranuleInfo;
    if ( $search->onError ) {
        ExitOnFailure( $resultDom, $search->errorMessage );
    }
    return undef unless ( @granuleList );

    # Create an XML parser and DOM
    my $xmlParser = XML::LibXML->new();
    $xmlParser->keep_blanks(0);
    my $listDom = $xmlParser->parse_string( '<GranuleList />' );
    my $listDoc = $listDom->documentElement();

    foreach my $granule ( @granuleList ) {
        my $granuleNode = XML::LibXML::Element->new( 'Granule' );
        $granuleNode->setAttribute( "LOCATOR", $granule->{locator} );

        appendNode( $granuleNode, 'ShortName', $granule->{shortName} );
        appendNode( $granuleNode, 'VersionID', $granule->{versionID} );
        appendNode( $granuleNode, 'GranuleID', $granule->{granuleID} );
        appendNode( $granuleNode, 'RangeBeginningDateTime', $granule->{rangeBegin} );
        appendNode( $granuleNode, 'RangeEndingDateTime', $granule->{rangeEnd} );
        appendNode( $granuleNode, 'ProductionDateTime', $granule->{productionDateTime} );
        appendNode( $granuleNode, 'InsertDateTime', $granule->{insertDateTime} );

        foreach my $file ( keys %{$granule->{files}} ) {
            my $fileNode = XML::LibXML::Element->new( 'File' );
            next unless ( $granule->{files}{$file} eq 'SCIENCE' );
            appendNode( $fileNode, 'FileName', basename( $file ) );
            appendNode( $fileNode, 'FileSize', $granule->{fileAttribute}{$file}{SIZE} );
            appendNode( $fileNode, 'CheckSumType', $granule->{checkSumType} ); 
            appendNode( $fileNode, 'CheckSumValue', 
                $granule->{fileAttribute}{$file}{CHECKSUMVALUE} );
            $granuleNode->appendChild( $fileNode );
        }
        $listDoc->appendChild( $granuleNode );
    }
    return $listDom;
}

sub appendNode
{
    my ( $self, $name, $value ) = @_;
    my $newNode = XML::LibXML::Element->new( $name );
    $newNode->appendText( $value );
    $self->appendChild( $newNode );
    return 1;
}

# Validates input
sub ValidateInput
{
    my ( %arg ) = @_;
    
    # Dataset shortname pattern
    my $datasetPattern = qr/(([a-z]|[A-Z]|[0-9]|_|-|){1,30})/;
    # Dataset version pattern
    my $versionPattern = qr/(\w{0,10})/;
    # S4PA root directory pattern
    my $homeDirPattern = qr/([^\.\$\'\`\|\;]+)/;
    # Time pattern
    my $timePattern = qr/(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})/;
    # Filename pattern
    my $fileNamePattern = qr/([\$\'\`\|\;]+)/;
    # Action pattern
    my $actionPattern = qr/(list:long|list|search)/;
    if ( defined $arg{DATASET} ) {
        # dataset has to be alphanumerical with -/_ and 30 char long at max
        return $1 if ( $arg{DATASET} =~ /^${datasetPattern}$/ );
    } elsif ( defined $arg{VERSION} ) {
        return $1 if ( $arg{VERSION} =~ /^(\s{0})$/ );
        return $1 if ( $arg{VERSION} !~ /\.{2,}/ &&
            $arg{VERSION} =~ /^([^\s\$\'\`\|\;\\]{1,10})$/ );
    } elsif ( defined $arg{HOME} ) {
        # home shouldn't contain character set .$'`
        $arg{HOME}=~ s/\%2F/\//g;
        return $1 if ( $arg{HOME} =~ /^${homeDirPattern}$/ );
    } elsif ( defined $arg{ACTION} ) {
        # action has to be from the valids list
        return $1 if ( $arg{ACTION} =~ /^${actionPattern}$/ );
    } elsif ( defined $arg{ENDTIME} ) {
        return $1
            if ( $arg{ENDTIME} =~ /^${timePattern}$/ );
    } elsif ( defined $arg{STARTTIME} ) {
        return $1
            if ( $arg{STARTTIME} =~ /^${timePattern}$/ );
    } elsif ( defined $arg{KEYWORDS} ) {
        return $arg{KEYWORDS}
            if ( $arg{KEYWORDS} =~ /(.+)/s );
    } elsif( $arg{USER} ) {
        return $1 if ( $arg{USER} =~ /(.+)/ );
    }
    return undef;
}

sub front_page {
    my $q = shift;
    print $q->header();
    print $q->start_html(-title  => 'Machine Search Parameters');
    print $q->p("<b> Please enter the following required search parameters: </b>");
    my $selfurl = $q->url( -path_info => 1, -query => 1, -relative => 1 );
    print $q->startform(-method => 'POST',
                        -action => $selfurl,
                        -encoding => 'multipart/form-data');
    unless ( defined $ENV{REMOTE_USER} ) {
        print "User Name ", $q->textfield(-name => 'user', -override => 1);
        print $q->p;
    }
    print "Dataset name ", $q->textfield(-name => 'dataset', -override => 1);
    print $q->p;
    print "Version label (leave empty if versionless) ",
          $q->textfield(-name => 'version', -override => 1);
    print $q->p;
    print "Beginning date time (YYYY-MM-DD HH:MI:SS) ",
          $q->textfield(-name => 'startTime', -override => 1);
    print $q->p;
    print "Ending date time (YYYY-MM-DD HH:MI:SS) ",
          $q->textfield(-name => 'endTime', -override => 1);
    print $q->p;
    my @actionList;
    foreach my $item (keys %actionAlias) {
        push @actionList, $item;
        @actionList = sort @actionList;
    }
    print "Action you want to perform ",
          $q->scrolling_list(-name => 'action', -values => \@actionList,
                             -default => 'Order');
    print $q->p;
    print $q->submit(-label => 'submit', -value => 'Submit', -onClick => $selfurl);
    print $q->end_html;
    exit;
}
