=pod
=head1 NAME

S4PA::Metadata - provides access to S4PA granule metadata/attributes

=head1 SYNOPSIS

  use S4PA::Metadata;
  $granule = S4PA::Metadata->new( FILE => 'xyz.xml' );
  $errorFlag = $granule->onError();
  $errorMessage = $granule->errorMessage();
  $granuleID = $granule->getGranuleLocator();
  $fileHash = $granule->getFiles();
  $fileHash = $granule->getFileAttributes();
  $fileGroup = $granule->getFileGroup();
  $granule->insertNode();
  $granule->replaceNode();
  $granule->replaceValue();
  $flag = $granule->compareDateTime();
  $shortName = $granule->getShortName();
  $versionID = $granule->getVersionID();
  $cksumType = $granule->getCheckSumType();
  $odl = $granule->getProducersMetadata();
  $value = $granule->getValue( $xpath );
  $status = $granule->write( $file );
  $status = $granule->insertPrcessingInstruction($name, $value);
  

=head1 DESCRIPTION

S4PA::Metadata encapsulates S4PA granule metadata. It provides methods to 
access attributes in S4PA metadata, compare data time coverage, get files 
belonging to the granule, create an S4P::FileGroup for the granule etc., The
constructor takes the metadata file name or a string.

=head1 AUTHOR

M. Hegde

=head1 METHODS

=cut

# $Id: Metadata.pm,v 1.33 2011/05/24 19:30:59 glei Exp $
# -@@@ S4PA, Version $Name:  $

package S4PA::Metadata;

use strict;
use XML::LibXML;
use S4P::FileGroup;
use S4P::FileSpec;
use File::Basename;
use vars '$AUTOLOAD';

################################################################################

=head2 Constructor

Description:
    Constructs the object either from granule metadata file or a string.

Input:
    Either a granule metadata filename (FILE) or string containing granule 
    metadata (TEXT) itself.

Output:
    Returns S4PA::Metadata object.

Author:
    M. Hegde
    
=cut

sub new
{
    my ( $class, %arg ) = @_;

    my $metadata = {};
    # Create an XML parser
    my $xmlParser = XML::LibXML->new();
    $xmlParser->keep_blanks( 0 );
    if ( defined $arg{FILE} ) {
        # Case of granule metadata filename specified: check the XML for
        # well formedness.
        if ( -f $arg{FILE} ) {
            my $result = eval {
            $xmlParser->parse_file( $arg{FILE} );
            };
            $metadata->{__DOM} = $result;
            $metadata->{__FILE} = $arg{FILE};
            if ( defined $metadata->{__DOM} ) {
                $metadata->{__DOC} = $metadata->{__DOM}->documentElement();
            } else {
                $metadata->{__ERROR} = 1;
                $metadata->{__MESSAGE} = "Failed to parse $arg{FILE}";
            }
        } else {
            $metadata->{__ERROR} = 1;
            $metadata->{__MESSAGE} = "$arg{FILE} doesn't exist";
        } 
    } elsif ( defined $arg{TEXT} ) {
        # Case of S4PA granule metadata specifried as a string: check the
        # XML for well formedness.
        my $result = eval {
            $xmlParser->parse_string( $arg{TEXT} );
            };
        $metadata->{__DOM} = $result;
        if ( defined $metadata->{__DOM} ) {
            $metadata->{__DOC} = $metadata->{__DOM}->documentElement();
            unless ( $metadata->{__DOC}->nodeName() eq 'S4PAGranuleMetaDataFile' ) {
                $metadata->{__ERROR} = 1;
                $metadata->{__MESSAGE} = 'Root element is not'
                    . ' S4PAGranuleMetaDataFile;'
                    . ' instead it is S4PACollectionMetaDataFile';
            }
        } else {
            $metadata->{__ERROR} = 1;
            $metadata->{__MESSAGE} = "Failed to parse text";
        }
    }
    # Set the error message if an error occurs.
    if ( not defined $metadata->{__ERROR} && $@ ) {
        my $message = $@;
        $message =~ s/\s+/ /g;        
        if ( $message =~ /(.+)\s+at/ ) {
            $metadata->{__ERROR} = 1;
            $metadata->{__MESSAGE} .= "($1)";
        }
    }    
    return bless( $metadata, $class );
}
################################################################################

=head2 onError

Description:
    Returns a boolean indicating whether any error flag has been raised.
    
Input:
    None.
    
Output:
    1/0 => error flag raised or not.
    
Author:
    M. Hegde
    
=cut

sub onError
{
    my ( $self ) = @_;    
    return ( defined $self->{__ERROR} ? 1 : 0 );
}

################################################################################

=head2 errorMessage

Description:
    Returns the error message
    
Input:
    None.
    
Output:
    Returns the error message if one exists.
    
Author:
    M. Hegde
    
=cut

sub errorMessage
{
    my ( $self ) = @_;    
    return ( $self->onError() ? $self->{__MESSAGE} : undef );
}
################################################################################

=head2 getGranuleLocator

Description:
    Returns the unique granule locator in S4PA. It is of the format
    <shortname>.<versionID>:<metadata filename>. This method is available only
    to S4PA::Metadata created using the granule's metadata filename.
    
Input:
    None.
    
Output:
    S4PA granule locator.
    
Author:
    M. Hegde
    
=cut

sub getGranuleLocator
{
    my ( $self ) = @_;
    return undef unless defined $self->{__FILE};
    return $self->getShortName() . '.' . $self->getVersionID() 
        . ':' . basename( $self->{__FILE} );
}
################################################################################

=head2 getFiles

Description:
    Returns a hash ref containing files belonging to the granule. The keys are
    filenames and values are the file types ('SCIENCE', 'BROWSE', 'HDF4MAP',
    'METADATA').

Input:
    None.

Output:
    A hash ref containing files belonging to the granule.

Author:
    M. Hegde
    
=cut

sub getFiles
{
    my ( $self ) = @_;

    my $fileHash = {};
    
    my $dirName = defined $self->{__FILE}
        ? dirname( $self->{__FILE} ) . '/' : '';

    my ( @fileNodeList ) = $self->{__DOC}->findnodes(
        '/S4PAGranuleMetaDataFile/DataGranule/Granulits/Granulit/FileName' );
    @fileNodeList = $self->{__DOC}->findnodes(
        '/S4PAGranuleMetaDataFile/DataGranule/GranuleID' )
        unless @fileNodeList;
    foreach my $fileNode ( @fileNodeList ) {
        my $fileName = $dirName . $fileNode->textContent();
        $fileHash->{$fileName} = 'SCIENCE';
    }
    my ( @browseNodeList ) = $self->{__DOC}->findnodes(
        '/S4PAGranuleMetaDataFile/DataGranule/BrowseFile' );
    foreach my $fileNode ( @browseNodeList ) {
        my $fileName = $dirName . $fileNode->textContent();
        $fileHash->{$fileName} = 'BROWSE';
    }
    my ( @mapNodeList ) = $self->{__DOC}->findnodes(
        '/S4PAGranuleMetaDataFile/DataGranule/MapFile' );
    foreach my $fileNode ( @mapNodeList ) {
        my $fileName = $dirName . $fileNode->textContent();
        $fileHash->{$fileName} = 'HDF4MAP';
    }
    $fileHash->{$dirName . basename($self->{__FILE})} = 'METADATA'
        if ( defined $self->{__FILE} );
    return $fileHash;
}
################################################################################

=head2 getFileAttributes

Description:
    Returns a hash ref containing files belonging to the granule. The primary
    key is the file name, secondary keys are attribute names and the values are
    attribute values.

Input:
    None.

Output:
    A hash ref containing files belonging to the granule.

Author:
    M. Hegde
    
=cut

sub getFileAttributes
{
    my ( $self ) = @_;

    my $fileHash = {};
    
    my $dirName = defined $self->{__FILE}
        ? dirname( $self->{__FILE} ) . '/' : '';

    my ( @fileNodeList ) = $self->{__DOC}->findnodes(
        '/S4PAGranuleMetaDataFile/DataGranule/Granulits/Granulit/FileName' );

    my $attrHash = {
            'FileSize' => 'SIZE',
            'CheckSum/CheckSumValue' => 'CHECKSUMVALUE'
        };        
    if ( @fileNodeList ) {
        foreach my $fileNode ( @fileNodeList ) {
            my $fileName = $dirName . $fileNode->textContent();
            
            foreach my $path ( keys %$attrHash ) {
                $fileHash->{$fileName}{$attrHash->{$path}} =
                    $fileNode->findvalue( "../$path" );
            }
        }
    } else {    
        @fileNodeList = $self->{__DOC}->findnodes(
            '/S4PAGranuleMetaDataFile/DataGranule/GranuleID' );
        foreach my $fileNode ( @fileNodeList ) {
            my $fileName = $dirName . $fileNode->textContent();
            $attrHash->{SizeBytesDataGranule} = 'SIZE';
            delete $attrHash->{FileSize};
            foreach my $path ( keys %$attrHash ) {
                $fileHash->{$fileName}{$attrHash->{$path}} =
                    $fileNode->findvalue( "../$path" );
            }
        }
    }
    #my ( @fileNodeList ) = $self->{__DOC}->findnodes(
    #    '/S4PAGranuleMetaDataFile/DataGranule/BrowseFile' );
    return $fileHash;
}
################################################################################
=head2 getFileGroup

Description:
    Return S4P::FileGroup object for the files in the granule.

Input:
    None.

Output:
    S4P::FileGroup object.

Author:
    M. Hegde

=cut

sub getFileGroup
{
    my ( $self ) = @_;
    my $fileGroup = S4P::FileGroup->new();
    $fileGroup->data_type( $self->getShortName() );
    $fileGroup->data_version( $self->getVersionID(), "%s" );
    my $fileHash = $self->getFiles();
    foreach my $key ( keys %$fileHash ) {
        $fileGroup->add_file_spec( $key, $fileHash->{$key} );
    }
    return $fileGroup;
}

################################################################################
=head2 insertNode

Description:
    Inserts an attribute node given the XPATH expression, node name and
    attributes.

Input:
    Accepts a hash whose keys are: NAME (for XML element name), AFTER (XPATH for
    inserting after an element), BEFORE(XPATH for inserting before an element),
    VALUE (text content of the element) and all others are attribute names for 
    the node being inserted.

Output:
    Return 0/1 => failure/success

Author:
    M. Hegde

=cut

sub insertNode
{
    my ( $self, %arg ) = @_;
    my $newNode = XML::LibXML::Element->new( $arg{NAME} );
    foreach my $key ( keys %arg ) {
        next if ( $key eq 'AFTER' || $key eq 'BEFORE' || $key eq 'NAME' );
        if ( $key eq 'VALUE' ) {
            $newNode->appendText( $arg{$key} );
        } else {
            $newNode->setAttribute( $key, $arg{$key} );
        }
    }
    if ( defined $arg{BEFORE} ) {
        my ( @nodeList ) = $self->{__DOC}->findnodes( $arg{BEFORE} );
        return 0 unless @nodeList;
        my $parent = $nodeList[0]->parentNode();
        $parent->insertBefore( $newNode, $nodeList[0] );
    } elsif ( defined $arg{AFTER} ) {
        my ( @nodeList ) = $self->{__DOC}->findnodes( $arg{AFTER} );
        return 0 unless @nodeList;
        my $parent = $nodeList[0]->parentNode();
        $parent->insertAfter( $newNode, $nodeList[0] );
    } else {
        return 0;
    }
    return 1; 
}

################################################################################
=head2 replaceNode

Description:
    Replaces an attribute node given the XPATH expression and attributes.

Input:
    Accepts a hash whose keys are: XPATH (XPATH for replacing after an element), 
    VALUE (text content of the element) and all others are attribute names for 
    the node being replaced.

Output:
    Returns 0/1 => failure/success

Author:
    M. Hegde

=cut
sub replaceNode
{
    my ( $self, %arg ) = @_;
    my ( @nodeList ) = $self->{__DOC}->findnodes( $arg{XPATH} );
    return 0 unless @nodeList;
    my $newNode = XML::LibXML::Element->new( $nodeList[0]->getName() );
    foreach my $key ( keys %arg ) {
        next if ( $key eq 'XPATH' );
        if ( $key eq 'VALUE' ) {
            $newNode->appendText( $arg{$key} );
        } else {
            $newNode->setAttribute( $key, $arg{$key} );
        }
    }
    $nodeList[0]->replaceNode( $newNode );
    return 1;
}

################################################################################
=head2 replaceValue

Description:
    Replaces the value of a text node given the XPath expression and its new
    value.

Input:
    XPath expression for the node to be replaced and its new value.

Output:
    Returns 1 on success and 0 on failure.

Author:
    M. Hegde

=cut

sub replaceValue
{
    my ( $self, $xpath, $value ) = @_;
    
    my ( $node ) = $self->{__DOC}->findnodes( $xpath );
    return 0 unless defined $node;
    
    my $parent = $node->parentNode();
    my $sibling = $node->nextSibling();
    my $newNode = XML::LibXML::Element->new( $node->getName() );
    $newNode->appendText( $value );
    $parent->removeChild( $node );
    if ( $sibling ) {
        $parent->insertBefore( $newNode, $sibling );
    } else {
        $parent->appendChild( $newNode );
    }
    return 1;
}

################################################################################
=head2 insertCollectionURL

Description:
    Inserts Collection metadata's URL

Input:
    URL for collection metadata

Output:
    Returns 1 on success and 0 on failure.

Author:
    M. Hegde

=cut

sub insertCollectionURL
{
    my ( $self, $url ) = @_;
    
    my $curUrl = $self->getValue(
    	'/S4PAGranuleMetaDataFile/CollectionMetaData/URL' );
    my $shortName = $self->getShortName();
    if ( defined $curUrl ) {
        return $self->replaceNode( 'XPATH' => '/S4PAGranuleMetaDataFile/CollectionMetaData/URL',
            VALUE => $url,
            'xmlns:xlink' => 'http://www.w3.org/1999/xlink',
            'xlink:type' => 'simple',
            'xlink:href' => $url,
            'xlink:show' => 'new',
            'xlink:actuate' => 'onRequest',
            'xlink:title' => 'Click to view $shortName collection' );        
    } else {
        return $self->insertNode( NAME => 'URL',
            AFTER => '/S4PAGranuleMetaDataFile/CollectionMetaData/VersionID',
            VALUE => $url,
            'xmlns:xlink' => 'http://www.w3.org/1999/xlink',
            'xlink:type' => 'simple',
            'xlink:href' => $url,
            'xlink:show' => 'new',
            'xlink:actuate' => 'onRequest',
            'xlink:title' => "Click to view $shortName collection" );
    }
    return 0;
}
################################################################################

=head2 compareDateTime

Description:
    Compares the granule's data time coverage with the specified range. Returns
    1 if the granule time coverage is within the specified range; otherwise,
    returns 0.

Input:
    A hash with keys START and END. START/END correspond to start and end
    date times.

Output:
    Returns 1 if the granule time coverage is within the specified range;
    otherwise, returns 0.

Author:
    M. Hegde

=cut

sub compareDateTime
{
    my ( $self, %arg ) = @_;
    
    my $beginTime = $self->getBeginTime();
    my $beginDate = $self->getBeginDate();
    my $endTime = $self->getEndTime();
    my $endDate = $self->getEndDate();
    my $beginDateTime = $beginDate . ' ' . $beginTime;
    my $endDateTime = $endDate . ' ' . $endTime;
    
    return 1 
        if ( ($arg{START} le $beginDateTime) && ($arg{END} ge $endDateTime) );

    # under overlap search condition, return no match only if either
    #     the granule's EndingDateTime is before the search's startTime, or
    #     the granule's BeginningDateTime is after the search's endTime.
    # in other word, it will include any granule with its covering range
    #     cross either end of the search's startTime or endTime, ticket #6550. 
    if ( $arg{OVERLAP} ) {
        return 0
            if ( ($endDateTime lt $arg{START}) || ($beginDateTime gt $arg{END}) );
        return 1;
    } elsif ( $arg{EXCLUSIVE} ) {
        return 1
            if ( ($arg{START} ge $beginDateTime) && ($arg{END} le $endDateTime) );
        return 0;
    }
}

################################################################################

=head2 Accessor Methods

Description:
    Has accessor methods for S4PA::Metadata.
    getShortName(): returns the short name.
    getCheckSumType(): returns the checksum type.
    getProducersMetadata(): returns the producers metadata (ODL)
    getValue(): returns the value of XPATH expression.
    toString(): serializer for S4PA::Metadata.
    write(): serializer for S4PA::Metadata; writes to the file or stdout.
    insertProcessingInstruction(): inserts XML processing instruction.
    getVersionID(): returns the version ID.
    onError(): returns boolean (1/0) to indicate error condition.
    errorMessage(): returns the last error message.

Input:
    Depends on the function.

Output:
    Depends on the function.

Author:
    M. Hegde

=cut

sub AUTOLOAD
{
    my ( $self, @arg ) = @_;
    
    return undef if $self->onError();
        
    if ( $AUTOLOAD =~ /.*::getShortName/ ) {
        my $result = $self->{__DOC}->find( 
            '/S4PAGranuleMetaDataFile/CollectionMetaData/ShortName' );
        return ( @{$result} ? $result->to_literal : undef );
    } elsif ( $AUTOLOAD =~ /.*::getVersionID/ ) {
        my $result = $self->{__DOC}->find( 
            '/S4PAGranuleMetaDataFile/CollectionMetaData/VersionID' );
        return ( @{$result} ? $result->to_literal : undef );
    } elsif ( $AUTOLOAD =~ /.*::getCheckSumType/ ) {
        my $result = $self->{__DOC}->find( 
            '/S4PAGranuleMetaDataFile/DataGranule/CheckSum/CheckSumType' );
        return ( @{$result} ? $result->to_literal : undef );
    } elsif ( $AUTOLOAD =~ /.*::getProducersMetadata/ ) {
        my $result = $self->{__DOC}->find( 
            '/S4PAGranuleMetaDataFile/ProducersMetaData' );
        return ( @{$result} ? $result->to_literal : undef );
    } elsif ( $AUTOLOAD =~ /.*::getValue/ ) {
        my $result = $self->{__DOC}->find( $arg[0] );
        return ( @{$result} ? $result->to_literal : undef );
    } elsif ( $AUTOLOAD =~ /.*::toString/ ) {
        return ( @arg ? $self->{__DOM}->toString( @arg ) 
            : $self->{__DOM}->toString() );
    } elsif ( $AUTOLOAD =~ /.*:write/ ) {
        if ( defined $arg[0] || defined $self->{__FILE} ) {
            my $file = defined $arg[0] ? $arg[0] : $self->{__FILE};
            open( FH, ">$file" );
            print FH $self->toString( 1 );
            return close( FH );
        } else {
            $self->toString( 1 );
            return 1;
        }
    } elsif ( $AUTOLOAD =~ /.*:insertProcessingInstruction/ ) {
        my @nodeList = $self->{__DOM}->findnodes(
            qq(processing-instruction("$arg[0]")) );
        foreach my $node ( @nodeList ) {
            $self->{__DOM}->removeChild( $node );
        }
        $self->{__DOM}->insertProcessingInstruction( @arg );
    } elsif ( $AUTOLOAD =~ /.*:getBeginDate/ ) {
        return $self->getValue( 
            '/S4PAGranuleMetaDataFile/RangeDateTime/RangeBeginningDate' );
    } elsif ( $AUTOLOAD =~ /.*:getBeginTime/ ) {
        return $self->getValue(
            '/S4PAGranuleMetaDataFile/RangeDateTime/RangeBeginningTime' );
    } elsif ( $AUTOLOAD =~ /.*:getEndDate/ ) {
        return $self->getValue( 
            '/S4PAGranuleMetaDataFile/RangeDateTime/RangeEndingDate');
        
    } elsif ( $AUTOLOAD =~ /.*:getEndTime/ ) {
        return $self->getValue(
            '/S4PAGranuleMetaDataFile/RangeDateTime/RangeEndingTime');
    } elsif ( $AUTOLOAD =~ /.*::DESTROY/ ) {
    } else {
        warn "$AUTOLOAD: method not found";
    }
    return undef;
}
1;
