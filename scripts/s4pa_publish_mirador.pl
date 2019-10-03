#!/usr/bin/perl

=head1 NAME

s4pa_publish_mirador.pl - script for Mirador publication

=head1 SYNOPSIS

s4pa_publish_mirador.pl 
[B<-c> I<config_file>]
[B<-p> I<pdrdir>]
[B<-o> I<directory to store transformed metadata files>]
[B<-x> I<XSL file for transformation>]

=head1 DESCRIPTION

s4pa_publish_mirador.pl accepts a directory containing PDRs, transformes
metadata using supplied XSL file and then creates a PostOffice work order 
referring  transformed meatadata files.

=head1 AUTHORS

Guang-Dih Lei 
Lou Fenichel (lou.fenichel@gsfc.nasa.gov)
M. Hegde

=cut

################################################################################
# $Id: s4pa_publish_mirador.pl,v 1.27 2010/12/14 16:02:25 glei Exp
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict ;
use Getopt::Std ;
use S4P;
use S4P::PDR;
use XML::LibXSLT;
use XML::LibXML;
use Safe;
use S4PA;
use S4PA::Storage;
use File::Basename;
use Cwd;
use S4PA;
use Log::Log4perl;
use File::stat;
use vars qw( $opt_c $opt_p $opt_o $opt_x );

# Get command line options
getopts('o:c:p:x:v');
S4P::perish( 1, "Failure to specify -c <ConfigFile> on command line." )
    unless defined $opt_c;
S4P::perish( 2, "Failure to specify -p <pdrdir> on command line." )
    unless defined $opt_p ;
$opt_o = getcwd() unless defined $opt_o;
S4P::perish( 3, "Directory, $opt_o, to store publication files doesn't exist" )
    unless ( -d $opt_o );
S4P::perish( 4, "Failure to specify -x <TransformFile> on command line." )
    unless defined $opt_x;

# Retrieve config values
my $cpt = new Safe 'CFG';
$cpt->share( '$DESTDIR', '$HOST', '$PROTOCOL', '$TYPE', '$RESTRICTED_URL',
    '$UNRESTRICTED_URL', '$INSTANCE_NAME', '%DATA_ACCESS',
    '%cfg_xpath_skip', '@cfg_psa_skip' ) ;
$cpt->rdo($opt_c) or
    S4P::perish( 5, "Cannot read config file $opt_c in safe mode: $!\n");

my $logger = S4PA::CreateLogger( $CFG::cfg_logger{FILE},
    $CFG::cfg_logger{LEVEL} ) if ( %CFG::cfg_logger );

my $pdrdir = $opt_p ;
my $xslFile = $opt_x;
my $action = $CFG::TYPE;
my $maxCount = $CFG::MAX_GRANULE_COUNT ? $CFG::MAX_GRANULE_COUNT : 1000;

my @processed_pdr;
my @meta_files;
# Examine PDRs
opendir (PDRDIR,"$pdrdir") || S4P::perish( 5, "Can't open $pdrdir: $!" );
my @files = readdir (PDRDIR) ;
close (PDRDIR) ;
my $numFiles = scalar(@files);
$logger->debug( "Found $numFiles files under $pdrdir" )
    if defined $logger;

foreach my $pdrfile ( @files ) {
    my @pdrfiles ;
    unlink( "$pdrdir/$pdrfile" ) unless ( $pdrfile =~ /\.PDR$/ );
    next if ($pdrfile !~ /\.PDR$/);
    my $pdrfile_fullpath = "$pdrdir/$pdrfile";
    my $pdr = S4P::PDR::read_pdr($pdrfile_fullpath);
    if (!$pdr) {
      S4P::logger('ERROR', "Cannot read PDR file $pdrfile: $!");
      $logger->error( "Failed reading $pdrfile" ) if defined $logger;
      next;
    }
    $logger->info( "Processing $pdrfile" ) if defined $logger;
    foreach my $fg (@{$pdr->file_groups}) {
      my $datatype = $fg->data_type();
      my $meta_file = $fg->met_file();

      # print out error message for missing metadata files
      # and skip the current fileGroup, ticket #6848.
      unless ( -f $meta_file ) {
        S4P::logger('ERROR', "Missing metadata file: $meta_file, skipped");
        $logger->error( "Missing metadata file: $meta_file, skipped")
          if defined $logger;
        next;
      }

      # skip if datatype is not defined, this is probably a collection metadata
      unless (defined $datatype) {
        S4P::logger('ERROR', "No datatype defined in file: $meta_file, skipped");
        $logger->error( "No datatype defined in file: $meta_file, skipped")
          if defined $logger;
        next;
      }

      push @meta_files, $meta_file
          unless ( exists {map {$_ => 1} @meta_files}->{$meta_file} );

      $logger->info( "Added $meta_file for mirador $action." )
          if defined $logger;
    }
    push @processed_pdr, $pdrfile_fullpath;
}

# Create work order
my $pdrCount = scalar( @processed_pdr );
my $metafileCount = scalar(@meta_files);
if ( $pdrCount && $metafileCount ) {
    my $wo_file = create_wo( $xslFile, @meta_files );
    if ( -f $wo_file ) {
        foreach my $pdr ( @processed_pdr ) {
            unlink $pdr;
            S4P::logger('INFO', "Removed $pdr.");
            $logger->debug( "Deleted $pdr" ) if defined $logger;
        }
        S4P::logger( "INFO", "$wo_file created for $pdrCount PDRs" );
        $logger->info( "Created work order $wo_file for postoffice" ) 
            if defined $logger;
    }
# all PDRs are unpublishable, purge them
} elsif ( $pdrCount ) {
    foreach my $pdr ( @processed_pdr ) {
        unlink $pdr;
        S4P::logger('INFO', "Removed $pdr.");
        $logger->debug( "Deleted $pdr" ) if defined $logger;
    }
} else {
   S4P::logger( "INFO", "No PDRs found" );
   $logger->info( "Found no PDRs" ) if defined $logger;
}

exit( 0 );

# Creates the post office work order
sub create_wo {
    my ( $granule_xsl_file, @meta_files ) = @_;
    
    # Construct work order file name
    my $wo;
    if ( $CFG::TYPE eq 'insert' ) {
        $wo = 'MiradorIns';
    } elsif ( $CFG::TYPE eq 'delete' ) {
        $wo = 'MiradorDel';
    } else {
        S4P::logger('INFO', "Type: $CFG::TYPE not supported, skip PDR");
        $logger->error( "Nonsupported operation $CFG::TYPE, skip PDR" )
            if defined $logger;
    }
    
    my $wo_type = 'PUSH';
    my @date = localtime(time);
    my $dateString = sprintf("%04d%02d%02d%02d%02d%02d",
        $date[5]+1900, $date[4]+1, $date[3], $date[2], $date[1], $date[0]);
    my $wo_file = sprintf("%s.%s.T$dateString.wo", $wo_type, $wo);
    
    # Create an postoffice work order XML 
    my $xmlParser = XML::LibXML->new();
    $xmlParser->keep_blanks( 0 );
    my $wo_dom = $xmlParser->parse_string('<FilePacket/>');
    my $wo_doc = $wo_dom->documentElement();
    my ($filePacketNode) = $wo_doc->findnodes('/FilePacket');
    $filePacketNode->setAttribute('status', "I");
    my $protocol = ($CFG::PROTOCOL) ? $CFG::PROTOCOL : 'ftp';
    my $destination = $protocol . ":" . $CFG::HOST . $CFG::DESTDIR;
    $filePacketNode->setAttribute('destination', $destination);

    my $instance = $CFG::INSTANCE_NAME;
    my $granuleCount = 0;
    my $bundledCount = 0;
    my $tarFilePrefix = "$opt_o/" . "$wo.$instance.F$maxCount.T$dateString";
    my @tarFiles;
    my @xmlFiles;

    # Create bundled publishing XML
    my $xml_dom = $xmlParser->parse_string('<S4paGranuleMetaDataFile/>');
    my $xml_doc = $xml_dom->documentElement();
    $xml_doc->setAttribute( 'INSTANCE', $instance );

    my $status = 1; # A variable to hold status
    foreach my $meta_file ( @meta_files ) {
        # Skip publishing hidden granule unless it is a delete action
        my $fs = stat( $meta_file );
        if ( ( ( $fs->mode & 07777 ) == 0600 ) && ( $CFG::TYPE eq 'insert' ) ) {
            S4P::logger('ERROR', "Skip publishing hidden granule: $meta_file");
            $logger->error( "Skip publishing hidden granule: $meta_file")
                if defined $logger;
            next;
        }

        # convert it first
        my $transformedXML = transform_xml( $wo, $meta_file, $granule_xsl_file );

        if ( $transformedXML eq '' ) {
	    $status = 0;
	    S4P::logger( "ERROR", "Failed to transform $meta_file" );
            $logger->error( "Failed transforming $meta_file" ) if defined $logger;

        # skip this granule when it has qualified PSA or XPATH
        } elsif ( $transformedXML =~ /^Skip/ ) {
            $logger->warn( "$transformedXML for $meta_file" ) if defined $logger;
            next;

        # add it to the output xml file granule node if successfully transformed
        } else {
            # write out the transformed string to file
            my $xmlFile = basename( $meta_file );
            if ( open( OUTFILE, "> $xmlFile") ) {
                print OUTFILE $transformedXML;
                unless ( close( OUTFILE ) ) {
                    $status = 0;
		    S4P::logger( "ERROR", "Failed to close $xmlFile ($!)" );
                    $logger->error( "Failed closing $xmlFile" )
                        if defined $logger;
		}
                $granuleCount++;
	    } else {
	        $status = 0;
	        S4P::logger( "ERROR",
		    "Failed to open $xmlFile for writing ($!)" );
                $logger->error( "Failed opening $xmlFile for writing" )
                    if defined $logger;
	    }
            $logger->info( "Transformed $meta_file" ) if defined $logger;
            push @xmlFiles, $xmlFile;
        }

        if ( $granuleCount == $maxCount ) {
            $bundledCount++;
            my $tarFile = $tarFilePrefix .  "_$bundledCount.tar";
            my $tarCommand = "tar -cf $tarFile *.xml";
            my $tarStatus = `$tarCommand`;
            if ( $? ) {
                $status = 0;
                S4P::logger( "ERROR", "Failed to create $tarFile ($!)" );
                $logger->error( "Failed creating $tarFile" )
                    if defined $logger;
            } else {
                push @tarFiles, $tarFile;
                $logger->info( "Created $tarFile for work order $wo_file" )
                    if defined $logger;
                # Cleanup all transformed xml files
                foreach my $file ( @xmlFiles ) {
                    unlink $file;
                }
            }
            $granuleCount = 0;
        }

        # Stop if error occurred
        unless ( $status ) {
            # Cleanup all transformed xml files
            foreach my $file ( @xmlFiles ) {
                unlink $file;
            }
            S4P::perish( 7, "Exiting" );
        }
    }

    # dump the remaining transformed metadata file
    if ( $granuleCount > 0 ) {
        # replace granule count in tar filename
        $tarFilePrefix =~ s/\.F\d+\./\.F$granuleCount\./;
        my $lastCount = $bundledCount + 1;
        my $tarFile = ( $bundledCount > 0 ) ?
            $tarFilePrefix . "_$lastCount.tar" :
            $tarFilePrefix .  ".tar";
        my $tarCommand = "tar -cf $tarFile *.xml";
        my $tarStatus = `$tarCommand`;
        if ( $? ) {
            $status = 0;
	    S4P::logger( "ERROR",
                "Failed to create $tarFile ($!)" );
            $logger->error( "Failed creating $tarFile" )
                if defined $logger;
        } else {
            push @tarFiles, $tarFile;
            $logger->info( "Created $tarFile for work order $wo_file" )
                if defined $logger;
        }

	# Cleanup all transformed xml files
	foreach my $file ( @xmlFiles ) {
            unlink $file;
	}
    }

    foreach my $tmpFile ( @tarFiles ) {
        my $filegroupNode = XML::LibXML::Element->new('FileGroup');
        my $fileNode = XML::LibXML::Element->new('File');
        $fileNode->setAttribute('localPath', $tmpFile );
        $fileNode->setAttribute('status', "I");
        $fileNode->setAttribute('cleanup', "Y");
        $filegroupNode->appendChild($fileNode);
        $wo_doc->appendChild($filegroupNode);
    }

    open (WO, ">$wo_file") 
        || S4P::perish( 8, "Failed to open workorder file $wo_file: $!");
    print WO $wo_dom->toString(1);
    close WO || S4P::perish( 9, "Failed to close $wo_file: $!" );
    return $wo_file;
}

sub transform_xml {
    my ( $wo, $meta_file, $granule_xsl_file ) = @_ ;

    my $transformedXML = '';
    my $parser = XML::LibXML->new();
    $parser->keep_blanks(0);
    my $dom = $parser->parse_file( $meta_file );
    my $doc = $dom->documentElement();

    # verify if the granule is qualified to be skipped
    my $publishFlag = 1;
    # global PSA skip setting
    if ( @CFG::cfg_psa_skip ) {
        $publishFlag = VerifyPSA( $doc );
        unless ( $publishFlag ) {
            S4P::logger( "WARN", "Matching skip PSA in $meta_file, "
                       . "skipping this granule." );
            return 'Skip publishing on matching PSA';
        }
    }

    # collection specific XPATH skip setting
    if ( %CFG::cfg_xpath_skip ) {
        my $shortname = GetNodeValue($doc, '//CollectionMetaData/ShortName');
        my $versionid = GetNodeValue($doc, '//CollectionMetaData/VersionID');
        my $xpath;
        if ( exists $CFG::cfg_xpath_skip{$shortname}{$versionid} ) {
            $xpath = $CFG::cfg_xpath_skip{$shortname}{$versionid};
        # versionless collection
        } elsif ( exists $CFG::cfg_xpath_skip{$shortname}{''} ) {
            $xpath = $CFG::cfg_xpath_skip{$shortname}{''};
        }
        if ( defined $xpath ) {
            $publishFlag = VerifyXpathValue( $meta_file, $doc, $xpath );
            unless ( $publishFlag ) {
                return 'Skip publishing on qualified XPATH';
            }
        }
    }

    my $xslt = XML::LibXSLT->new();
    my $styleSheet = $xslt->parse_stylesheet_file( $granule_xsl_file );
    my $output = $styleSheet->transform( $dom );
    my $transformedString = $styleSheet->output_string( $output );

    my $metDom = $parser->parse_string( $transformedString );
    my $metDoc = $metDom->documentElement();
    my ( $granNode ) = $metDoc->findnodes( 'DataGranule' );
    my ( $granIdNode ) = $granNode->getChildrenByTagName( 'GranuleID' );
    my $granUrlNode = XML::LibXML::Element->new( 'GranuleURL' );
    $granNode->insertBefore( $granUrlNode, $granIdNode );
    
    my @dataFileNodeList =
        $granNode->findnodes( '//Granulits/Granulit/FileName' );
    unless ( @dataFileNodeList ) {
        @dataFileNodeList = $granNode->getElementsByTagName( 'GranuleID' );
    }
    my $dataDir = dirname( $meta_file );
    my @fileList = ();
    foreach my $dataFileNode ( @dataFileNodeList ) {
        my $file = $dataDir . '/' . $dataFileNode->string_value();
        push( @fileList, $file );
    }

    # adding browse file if exist
    my ( $browseFileNode ) = $granNode->getElementsByTagName( 'BrowseFile' );
    my $browse_file = '';
    if ( defined $browseFileNode ) {
        $browse_file = $dataDir . '/' . $browseFileNode->string_value();
        push( @fileList, $browse_file );
    }

    # adding metadata file itself
    push( @fileList, $meta_file );

    my $status = 1;
    foreach my $file ( @fileList ) {
        my $stat = File::stat::stat( $file );
        my $url; 
        if ( $stat->mode() & 0004 ) {
            # public data
            $url = $CFG::UNRESTRICTED_URL;
        } elsif ( $stat->mode() & 0040 ) {
            # restricted data
            $url = $CFG::RESTRICTED_URL;
        } elsif ( $wo =~ /MiradorDel/ ) {
            # hidden data for delete only
            $url = $CFG::RESTRICTED_URL;
        } else {
            S4P::logger( "ERROR", "Can not publish hidden file: $file" );
            $logger->error( "Can not publish hidden file: $file" )
                if defined $logger;
		$status = 0;
        }
        my $urlNode = ( $file eq $meta_file )
            ? XML::LibXML::Element->new( 'MetadataURL' ) : ( $file eq $browse_file )
            ? XML::LibXML::Element->new( 'BrowseURL' ) : XML::LibXML::Element->new( 'DataURL' );

	my $relativeUrl = S4PA::Storage::GetRelativeUrl( $file );
	$relativeUrl .= '/' . basename( $file );
	$relativeUrl =~ s/\/+/\//g;
	unless ( defined $relativeUrl ) {
		$status = 0;
		S4P::logger( "ERROR", 
		"Failed to extract relative URL for $meta_file" );
            $logger->error( "Failed extracting relative URL for $meta_file" )
                if defined $logger;
	}
	$url .= $relativeUrl;
        $urlNode->appendText( $url );
        $granUrlNode->appendChild( $urlNode );
	last unless $status;
    }
    return unless ( $status );
    $transformedXML = $metDom->toString( 1 );
    return $transformedXML;
}

sub GetNodeValue {
    my ($root, $xpath) = @_;
    my ($node) = ($xpath ? $root->findnodes($xpath) : $root);
    return undef unless defined $node;
    my $val = $node->textContent();
    $val =~ s/^\s+|\s+$//g;
    return $val;
}

sub VerifyPSA {
    my ($doc) = @_;

    my $returnFlag = 1;
    # default PSA xpath should be the following in granule metadata
    my $xpath = '//PSAs/PSA';
    foreach my $psaNode ( $doc->findnodes($xpath) ) {
        my $psaName = GetNodeValue($psaNode, 'PSAName');
        my $psaValue = GetNodeValue($psaNode, 'PSAValue');
        foreach my $psa ( @CFG::cfg_psa_skip ) {
            my $skipName = $psa->{'PSAName'};
            my $skipValue = $psa->{'PSAValue'};

            # both PSAName and PSAValue match with the skip setting,
            # skip this granule
            if ( ($skipName eq $psaName) && ($skipValue eq $psaValue) ) {
                $returnFlag = 0;
                return $returnFlag;
            }
        }
    }
    return $returnFlag;
}

sub VerifyXpathValue {
    my ($met_file, $doc, $xpath_list) = @_;

    my $returnFlag = 1;
    foreach my $operator ( keys %$xpath_list ) {
        foreach my $xpath ( keys %{$xpath_list->{$operator}} ) {
            my ( $xpathNode ) = $doc->findnodes($xpath);
            unless (defined $xpathNode) {
                S4P::logger( "WARN", "XPATH $xpath in $met_file is not defined, "
                    . "skipping this granule." );
                $returnFlag = 0;
                return $returnFlag;
            }

            my $targetVal = $xpath_list->{$operator}{$xpath};
            # only verify the existence of the xpath if target value is empty
            # and make sure the node value is empty too
            if ( $targetVal eq '' ) {
                my $value = GetNodeValue($xpathNode);
                if ($value) {
                    return $returnFlag;
                } else {
                    S4P::logger( "WARN", "XPATH $xpath in $met_file is not defined, "
                        . "skipping this granule." );
                    $returnFlag = 0;
                    return $returnFlag;
                }
            }

            my $granuleVal = $doc->findvalue( $xpath );
            # Make sure the input and the output are of the same type
            my $targetType = S4PA::IsNumber( $targetVal );
            my $granuleType = S4PA::IsNumber( $granuleVal );
            S4P::logger( "WARN",
                "Values of $xpath in $met_file is not the same type "
                 . "as the target value specified in configuration!" )
                unless ( $targetType == $granuleType );

            # Reset replace flag if any of the specified attributes don't match.
            my $cmpFlag = ( $granuleType != $targetType )
                ? $granuleVal cmp $targetVal
                : ( $granuleType
                    ? $granuleVal <=> $targetVal
                    : $granuleVal cmp $targetVal );

            if ( $operator eq 'EQ' ) {
                $returnFlag = 0 if ( $cmpFlag == 0 );
            } elsif ( $operator eq 'NE' ) {
                $returnFlag = 0 if ( $cmpFlag != 0 );
            } elsif ( $operator eq 'LT' ) {
                $returnFlag = 0 if ( $cmpFlag == -1 );
            } elsif ( $operator eq 'LE' ) {
                $returnFlag = 0 if ( $cmpFlag <= 0 );
            } elsif ( $operator eq 'GT' ) {
                $returnFlag = 0 if ( $cmpFlag == 1 );
            } elsif ( $operator eq 'GE' ) {
                $returnFlag = 0 if ( $cmpFlag >= 0 );
            } else {
                S4P::perish( 1,
                    "Operator specified for $xpath, $operator, not"
                    . " supported" );
            }
            unless ( $returnFlag ) {
                S4P::logger( "WARN",
                    "Values of $xpath in $met_file is '$operator' " .
                    "the target value specified in configuration, " .
                    "skipping this granule." );
                return $returnFlag;
            }
        }
    }
    return $returnFlag;
}

################  end of s4pa_publish_mirador.pl  ################
