#!/usr/bin/perl

=head1 NAME

s4pa_publish_dotchart.pl - script for DotChart publication

=head1 SYNOPSIS

s4pa_publish_dotchart.pl 
[B<-c> I<config_file>]
[B<-p> I<pdrdir>]
[B<-x> I<xslfile>]
[B<-s> I<stagingdir>]
[B<-v> I<verbose>]

=head1 DESCRIPTION

s4pa_publish_dotchart.pl can accept a PDR directory containing work
orders or ALL as input, and then transforms every metadata file 
it finds into the proper DotChart format. DotChart xml formated file 
are written to a staging directory and work order created for
downstream processing.

=head1 ARGUMENTS

=over 4

=item B<-c>

configuration file. It should contain the following variables:
    $INSTANCE_NAME='goldsfs1u'
    $DESTDIR='/ftp/private/.xxxx/<MODE>/pending_insert';
    $HOST='tads1u.ecs.nasa.gov';
    $PROTOCOL='ftp';
    $UNRESTRICTED_ROOTURL='ftp://goldsfs1u.ecs.nasa.gov/data/s4pa/';
    $RESTRICTED_ROOTURL='http://goldsfs1u.ecs.nasa.gov/data/s4pa/';
    $TYPE='insert';
    $MAX_GRANULE_COUNT='1000';

=item B<-p>

pending PDR and wo directory. For S4PA system, it is usually at
    ../pending_publish, or ../pending_delete

=item B<-x>

XSL file for transform s4pa meatadata file to dotchart xml format.

=item B<s>

staging directory for generated xml files.

=item B<-v>

Verbose.

=back

=head1 AUTHORS

Guang-Dih Lei (guang-dih.lei@gsfc.nasa.gov)

=cut

################################################################################
# s4pa_publish_dotchart.pl,v 1.28 2010/12/14 15:55:38 glei Exp
# -@@@ S4PA, Version $Name:  $
###############################################################################
#
# name: s4pa_publish_dotchart.pl
# revised: 09/29/2006 glei initial release
#

use strict ;
use Getopt::Std ;
use S4P::PDR ;
use Safe ;
use XML::LibXSLT ;
use XML::LibXML ;
use File::Basename ;
use File::Copy;
use File::stat ;
use S4PA::Storage;
use S4PA;
use Log::Log4perl;
use vars qw( $opt_c $opt_p $opt_x $opt_s $opt_v );

getopts('c:p:x:s:v');

unless (defined($opt_c)) {
    S4P::logger("ERROR","Failure to specify -c <ConfigFile> on command line.") ;
    usage();
}
my $configFile = $opt_c;

unless (defined($opt_p)) {
    S4P::logger("ERROR","Failure to specify -p <pdrdir> on command line.") ;
    usage();
}
my $pdrdir = $opt_p ;

unless (defined($opt_s)) {
    S4P::logger("ERROR","Failure to specify -s <stagingdir> on command line.") ;
    usage();
}

unless (defined($opt_x)) {
    S4P::logger("ERROR","Failure to specify -x <xslfile> on command line.") ;
    usage();
}


#####################################################################
# Configuration setup
#####################################################################
my $stagingDir = $opt_s;
my $verbose = $opt_v;

# retrieve config values
my $cpt = new Safe 'CFG';
$cpt->share( '$INSTANCE_NAME', '$DESTDIR', '$HOST', '$UNRESTRICTED_ROOTURL', 
    '$RESTRICTED_ROOTURL', '$TYPE', '$MAX_GRANULE_COUNT', '$PROTOCOL' ) ;
$cpt->rdo($opt_c) or
    S4P::perish( 1, "Cannot read config file $opt_c in safe mode: $!\n");

my $logger = S4PA::CreateLogger( $CFG::cfg_logger{FILE},
    $CFG::cfg_logger{LEVEL} ) if ( %CFG::cfg_logger );

my @date = localtime(time);
my $dateString = sprintf("T%04d%02d%02d%02d%02d%02d", 
    $date[5]+1900, $date[4]+1, $date[3], $date[2], $date[1], $date[0]);
my $maxCount = $CFG::MAX_GRANULE_COUNT ? $CFG::MAX_GRANULE_COUNT : 1000;
my $opsType = $CFG::TYPE;
my $instance = $CFG::INSTANCE_NAME;

my $xmlPrefix;
if ( $opsType eq 'insert' ) {
    $xmlPrefix = 'DotchartIns';
} elsif ( $opsType eq 'delete' ) {
    $xmlPrefix = 'DotchartDel';
} else {
    S4P::perish( 2, "Operation Type: $opsType not supported");
}

# append instance name to prefix if INSTANCE_NAME if in configuration
# if not, append the nodename in UNRESTRICTED_ROOTURL to prefix
if ( ! defined $instance ) {
    $instance = "$1" if ( $CFG::UNRESTRICTED_ROOTURL =~ m#://(\w+)\.# );
}
$xmlPrefix .= ".$instance";

# create staging area if it doesn't exist.
mkdir( $stagingDir ) || S4P::perish( 4, "Failed to create $stagingDir" )
    unless ( -d $stagingDir );

my $xslFile = $opt_x;
S4P::perish( 1, "Cannot find Specified $xslFile") unless ( -f $xslFile );

#####################################################################
# PDR processing
#####################################################################
# examine PDR directory
opendir (PDRDIR,"$pdrdir") || S4P::perish( 3, "Failed to open $pdrdir" );
my @files = readdir (PDRDIR) ;
close (PDRDIR) ;
my $numFiles = scalar(@files);
$logger->debug( "Found $numFiles files under $pdrdir" )
    if defined $logger;

# batch all PDRs
my @processed_pdr;
my @batchPDR;
my @xmlFiles;
my $totalPDR = 0;
my $granuleCount = 0;
my $batchCount = 0;
my $pdrCount = 0;
my @invalidPDR;

# parse each PDR
foreach my $pdrfile ( @files ) {
    chomp( $pdrfile );

    # remove log files and skip files that are not PDR files
    unlink("$pdrdir/$pdrfile") if ($pdrfile =~ /\.log$/);
    next if ($pdrfile !~ /\.(PDR|wo)$/);

    my $pdrPath = "$pdrdir/$pdrfile";

    $totalPDR++;
    # count file_group in the current pdr
    my $fgCount = `grep OBJECT=FILE_GROUP $pdrPath | wc -l` / 2;
    $logger->debug( "Found $pdrfile with $fgCount granule(s)." ) 
        if defined $logger;

    # skip empty PDR or work order
    next if ( $fgCount == 0 );
    $granuleCount += $fgCount;
    push @batchPDR, $pdrPath;

    # convert pdr to xml when granule count exceeded MAX_GRANULE_COUNT
    $pdrCount++;
    if ( $granuleCount >= $maxCount ) {
        S4P::logger("INFO", "Processing $pdrCount PDRs, $granuleCount Granules.");
        $logger->info( "Processing $pdrCount PDR(s) with total $granuleCount " .
            "granule(s)." ) if defined $logger;

        my $xmlPath;
        my $badPDR;
        ( $batchCount, $xmlPath, $badPDR ) = 
            create_xml($batchCount, $maxCount, $instance, @batchPDR);
        if ( defined $xmlPath ) {
            foreach my $file ( sort @{$xmlPath} ) {
                S4P::logger( "INFO", "$file created." );
                $logger->info( "Created $file for publishing" ) 
                    if defined $logger;
            }
            push @xmlFiles, @{$xmlPath};
        }
        push @invalidPDR, @{$badPDR} if ( defined $badPDR );

        # counter/array reset
        $granuleCount = 0;
        $pdrCount = 0;
        @batchPDR = ();
    }
}

if ( $pdrCount > 0 ) {
    S4P::logger("INFO", "Processing $pdrCount PDRs, $granuleCount Granules.");
    $logger->info( "Processing $pdrCount PDR(s) with total $granuleCount " .
        "granule(s)." ) if defined $logger;

    my $xmlPath;
    my $badPDR;
    ( $batchCount, $xmlPath, $badPDR ) = 
        create_xml($batchCount, $maxCount, $instance, @batchPDR); 
    if ( defined $xmlPath ) {
        foreach my $file ( sort @{$xmlPath} ) {
            S4P::logger( "INFO", "$file created." );
            $logger->info( "Created $file for publishing" )
                if defined $logger;
        }
        push @xmlFiles, @{$xmlPath};
    }
    push @invalidPDR, @{$badPDR} if ( defined $badPDR );
}

# $pdrCount = scalar( @processed_pdr );
my $xmlCount = scalar(@xmlFiles);

#####################################################################
# TRACK wo processing
#####################################################################
# collect all tracking files
my @trackFiles;
my @processed_wo;
foreach my $wofile ( @files ) {
    chomp( $wofile );
    next if ($wofile !~ /TRACK\.(.*)\.wo$/);
    $logger->info( "Processing tracking work order $wofile" )
        if defined $logger;
    my $woStamp = $1;
    my $wofile_fullpath = "$pdrdir/" . $wofile;

    # rename wo to xml file
    ( my $trackFile = $xmlPrefix ) =~ s/DotchartIns/DotchartDist/;
    
    my $trackFile = "$stagingDir/$trackFile.$woStamp.xml";
    if ( File::Copy::copy( $wofile_fullpath, $trackFile ) ) {
        S4P::logger( "INFO", "Success in copy of $wofile" );
        $logger->info( "Processed $wofile to $trackFile" ) 
            if defined $logger;
        push @trackFiles, $trackFile;
        push @processed_wo, $wofile_fullpath;
    } else {
        S4P::logger( "ERROR", "Failed to copy " . $wofile 
                     . " to $stagingDir" );
        $logger->error( "Failed copying $wofile" ) if defined $logger;
    }

}
my $woCount = scalar( @trackFiles );


#####################################################################
# Creating work order for postoffice
#####################################################################
# write out work order for postoffice
my $wo_type = 'PUSH';
my $wo_file = sprintf("%s.%s.%s.wo", $wo_type, $xmlPrefix, $dateString);

if ( $xmlCount || $woCount ) {
    my $status = create_wo( $wo_file, $xmlCount, $woCount, \@xmlFiles, \@trackFiles );
    unless ( $status ) {
        foreach my $pdr ( @processed_pdr ) {
            $logger->info( "Created work order $wo_file for postoffice" )
                if defined $logger;
            # keep a copy of the PDR for the delete station
            # pending deletion PDR from storage station will land it
            # in dotchart publish station first before it get to 
            # delete and other publish stations.
            if ( $opsType eq 'delete' ) {
                # remove the DO. prefix on the PDR if exist
                ( my $deletePdr = basename($pdr) ) =~ s/^DO\.//;
                if ( File::Copy::copy( $pdr, "./$deletePdr" ) ) {
                    unlink $pdr;
                    S4P::logger( "INFO", "Moved $pdr to ./" ) if ($verbose);
                    $logger->info( "Created $deletePdr for Delete station" )
                        if defined $logger;
                } else {
                    S4P::logger( "ERROR", "Failed to copy $pdr to ./" );
                    $logger->error( "Failed creating $deletePdr for " .
                        "Delete station" ) if defined $logger;
                }
            # ingest publish still follow the original route, there are
            # no downstream after pending_publish station beside postoffice
            } else {
                unlink $pdr;
                S4P::logger( "INFO", "Removed $pdr" ) if ($verbose);
                $logger->debug( "Deleted $pdr" ) if defined $logger;
            }
        }
        foreach my $wo ( @processed_wo ) {
            unlink $wo;
            S4P::logger('INFO', "Removed $wo") if ($verbose);
            $logger->debug( "Deleted tracking $wo" ) if defined $logger;
        }
        S4P::logger("INFO", "Completed: Processed $totalPDR PDRs and $woCount TRACK wo.");
        $logger->info( "Completed $totalPDR PDR(s) and $woCount tracking wo" )
            if defined $logger;
    }
} 
else {
    S4P::logger( "INFO", "No PDRs or WOs found" );
    $logger->debug( "Found no PDR or WO" ) if defined $logger;
    foreach my $xmlfile ( @xmlFiles ) {
        unlink $xmlfile;
    }
    # remove all processed PDRs
    foreach my $pdr (@processed_pdr) {
        unlink $pdr if (-f $pdr);
    }
}

# Fail the job if there is any invalid PDR encountered, Ticket #5995.
if ( @invalidPDR ) {
    my $failMessage = "Encountered invalid PDR(s): ";
    foreach my $pdr ( @invalidPDR ) {
        $failMessage .= $pdr . " ";
    }
    $failMessage .= " All valid PDR(s) has been processed.";
    $logger->error( $failMessage ) if defined $logger;
    $failMessage .= " Please check master publish.log for details.";
    S4P::perish( 8, $failMessage );
}

exit( 0 );


#####################################################################
# create_xml:  transform batch of PDRs into one xml file
#####################################################################
sub create_xml {

    my ($batchCount, $maxCount, $instance, @batchPDR) = @_;
    my $skippedGranule = 0;
    my $skippedPDR = 0;
    my $invalidPDR = ();
    my $xmlFiles = ();

    # generate DotChart xml header
    my $xmlParser = XML::LibXML->new();
    my $dom = $xmlParser->parse_string('<GranuleMetaDataFile/>');

    my $doc = $dom->documentElement();

    $doc->appendTextChild( 'InstanceName', $instance );
    my $DataSetNode = XML::LibXML::Element->new('GranuleMetaDataSet');
    my $GranulesNode = XML::LibXML::Element->new('Granules');
    my $DelGranulesNode = XML::LibXML::Element->new('DeleteGranules');

    # parse each PDR
    my $fgCount;
    foreach my $pdrfile ( @batchPDR ) {
        my $pdr = S4P::PDR::read_pdr($pdrfile);
        if (!$pdr) {
            S4P::logger('ERROR', "Cannot read PDR file $pdrfile: $!");
            $logger->error( "Failed reading $pdrfile" ) if defined $logger;
            $skippedPDR++;
            push @{$invalidPDR}, $pdrfile;
            next;
        }
        $logger->info( "Processing $pdrfile" ) if defined $logger;

        # parse through each FILE_GROUP
        my $url ;
        foreach my $fg (@{$pdr->file_groups}) {
            my $datatype = $fg->data_type();
            my $meta_file = $fg->met_file();
            my @sci_files = $fg->science_files();
            # treat browse file as data file if exist
            push ( @sci_files, $fg->browse_file() ) if ( $fg->browse_file() );

            # make sure metadata file exist
            unless ( -f $meta_file ) {
                S4P::logger('ERROR', "Missing metadata file: $meta_file, skipped");
                $logger->error( "Missing metadata file: $meta_file, skipped") 
                    if defined $logger;
                $skippedGranule++;
                next;
            }

            # skip if datatype is not defined or no science files
            # this is probably a collection metadata
            unless (defined $datatype) {
                S4P::logger('ERROR', "No datatype defined in file: $meta_file, skipped");
                $logger->error( "No datatype defined in file: $meta_file, skipped") 
                    if defined $logger;
                $skippedGranule++;
                next;
            }

            unless (@sci_files) {
                S4P::logger('ERROR', "No science file in metadata: $meta_file, skipped");
                $logger->error( "No science file in metadata: $meta_file, skipped") 
                    if defined $logger;
                $skippedGranule++;
                next;
            }

            # make sure all science files exist
            my $missingFileCount = 0;
            foreach my $data_file ( @sci_files ) {
                unless ( -f $data_file ) {
                    S4P::logger('ERROR', "Missing data file: $data_file, skipped");
                    $logger->error( "Missing data file: $data_file, skipped" )
                        if defined $logger;
                    $missingFileCount = 1;
                    $skippedGranule++;
                    last;
                }
            }
            next if ( $missingFileCount );

            # convert s4pa xml to dotchart xml
            my $dc_string;
            $dc_string = convert_file( $meta_file, $xslFile, @sci_files );
            my $fg_parser = XML::LibXML->new();

            my $fg_dom = $fg_parser->parse_string( $dc_string );
            my $fg_doc = $fg_dom->documentElement();

            if ( $opsType eq 'insert' ) {
                # extract GranuleURMetaData node
                my $fg_nodes = $fg_doc->findnodes('//GranuleURMetaData');
                my $URMetaDataNode = $fg_nodes->get_node(1);
                $GranulesNode->appendChild($URMetaDataNode);
                $logger->info( "Added $meta_file for dotchart publishing" )
                    if defined $logger;
            }
            elsif ( $opsType eq 'delete' ) {
                my $DelGranuleNode = XML::LibXML::Element->new('DeleteGranule');

                # append ShortName node
                my $fg_nodes = $fg_doc->findnodes('//ShortName');
                my $ShortNameNode = $fg_nodes->get_node(1);
                $DelGranuleNode->appendChild($ShortNameNode);

                # append VersionID node
                my $fg_nodes = $fg_doc->findnodes('//VersionID');
                my $VersionIDNode = $fg_nodes->get_node(1);
                $DelGranuleNode->appendChild($VersionIDNode);

                # append GranuleID node
                my $fg_nodes = $fg_doc->findnodes('//GranuleID');
                my $GranuleIDNode = $fg_nodes->get_node(1);
                $DelGranuleNode->appendChild($GranuleIDNode);

                # append DeleteTime node
                my $delete_time = sprintf("%04d\-%02d\-%02d %02d\:%02d\:%02d",
                    $date[5]+1900, $date[4]+1, $date[3], $date[2], $date[1], $date[0]);
                $DelGranuleNode->appendTextChild( 'DeleteTime' , $delete_time );
                $DelGranulesNode->appendChild($DelGranuleNode);
                $logger->info( "Added $meta_file for dotchart deletion" )
                    if defined $logger;
            }

            # output to xml file when we hit the maximum granule count
            $fgCount++;
            if ( $fgCount == $maxCount ) {
                if ( $opsType eq 'insert' ) {
                    $DataSetNode->appendChild($GranulesNode);
                }
                elsif ( $opsType eq 'delete' ) {
                    $DataSetNode->appendChild($DelGranulesNode);
                }
                $doc->appendChild($DataSetNode);
                my $batchString = "";
                if ( $batchCount > 0 ) {
                    $batchString = "_" . "$batchCount";
                }
                my $dotchart_xml = sprintf("%s.%s%s.xml", $xmlPrefix, $dateString, $batchString);
                my $dcXML = "$stagingDir/$dotchart_xml";
            
                open (OUTFILE, "> $dcXML") or S4P::perish( 5, "Can't open $dcXML\n" );
                print OUTFILE $dom->toString(1);
                close (OUTFILE);
                push @{$xmlFiles}, $dcXML;

                # reset counter and clean up nodes
                $GranulesNode->removeChildNodes;
                $DelGranulesNode->removeChildNodes;
                $DataSetNode->removeChildNodes;
                $doc->removeChild($DataSetNode);

                $fgCount = 0;
                $batchCount++;
            }
        }
        push @processed_pdr, $pdrfile;
    }

    # dump the remaining file group out
    if ( $fgCount > 0 ) {
        if ( $opsType eq 'insert' ) {
            $DataSetNode->appendChild($GranulesNode);
        }
        elsif ( $opsType eq 'delete' ) {
            $DataSetNode->appendChild($DelGranulesNode);
        }
        $doc->appendChild($DataSetNode);
    
        my $batchString = "";
        if ( $batchCount > 0 ) {
            $batchString = "_" . "$batchCount";
        }
        my $dotchart_xml = sprintf("%s.%s%s.xml", $xmlPrefix, $dateString, $batchString);
        my $dcXML = "$stagingDir/$dotchart_xml";
    
        open (OUTFILE, "> $dcXML") or S4P::perish( 5, "Can't open $dcXML\n" );
        print OUTFILE $dom->toString(1);
        close (OUTFILE);
        push @{$xmlFiles}, $dcXML;
        $batchCount++;
    }

    S4P::logger( "INFO", "skipped $skippedPDR PDRs and $skippedGranule Granules");
    if ( $skippedPDR || $skippedGranule ) {
        $logger->error( "Skipped $skippedPDR PDR(s) and $skippedGranule granule(s)." )
            if defined $logger;
    }

    return ( $batchCount, $xmlFiles, $invalidPDR );
}


#####################################################################
# convert_file:  transform S4PA metadata file to Dotchart xml file
#####################################################################
sub convert_file {
    my ( $meta_file, $granule_xsl_file, @science_files ) = @_ ;
    my $parser = XML::LibXML->new();
    $parser->keep_blanks(0);
    my $dom = $parser->parse_file( $meta_file );

    my $xslt = XML::LibXSLT->new();
    my $styleSheet = $xslt->parse_stylesheet_file( $granule_xsl_file );
    my $output = $styleSheet->transform( $dom );
    my $storedString = $styleSheet->output_string( $output );

    # replace the metadata file URL before output
    my $metaFileString = "<MetaDataURL>" ;

    # SorceTelemetryPass could have two form, \d+ or ("\d+")
    # removed (" ") with just pass number.
    $storedString =~ s/<SorceTelemetryPass>\(\"(\d+)\"\)/<SorceTelemetryPass>$1/;

    my $url = S4PA::Storage::GetRelativeUrl( $meta_file );
    S4P::perish( 6, "Failed to get relative URL for $meta_file" )
        unless defined $url;
    my $fs = stat( $meta_file );
    $url = $url . '/' . basename( $meta_file );
    $url =~ s/\/+/\//g;
    if ( $fs->mode() & 004 ) {
        $url = $CFG::UNRESTRICTED_ROOTURL . $url;
    } else {
        $url = $CFG::RESTRICTED_ROOTURL . $url;
    }
    $metaFileString .= "$url</MetaDataURL>" ;
    $storedString =~ s/<MetaDataURL\/>/$metaFileString/ ;

    # replace science file URL(s) before output
    my $sciFileString = "<Granulits>\n" ;

    foreach my $sci_file ( @science_files ) {
        my $url = S4PA::Storage::GetRelativeUrl( $sci_file );
        my $filename = basename( $sci_file );
        $url = $url . '/' . $filename;
        $url =~ s/\/+/\//g;
        my $sci_fs = stat( $sci_file );
        my $filesize = $sci_fs->size();
        if ( $sci_fs->mode() & 004 ) {
            $url = $CFG::UNRESTRICTED_ROOTURL . $url;
        } else {
            $url = $CFG::RESTRICTED_ROOTURL . $url;
        }

        $sciFileString .= 
            "            <Granulit>\n" .
            "              <FileName>$filename</FileName>\n" .
            "              <FileSize>$filesize</FileSize>\n" .
            "              <FileURL>$url</FileURL>\n" .
            "            </Granulit>\n";
    }
    $sciFileString .= "          </Granulits>" ;
    $storedString =~ s/<Granulits\/>/$sciFileString/ ;

    return $storedString;
}

#####################################################################
# create_wo:  create postoffice work order
#####################################################################
sub create_wo {
    my ( $wo, $xmlCount, $woCount, $xmlFiles, $trackFiles ) = @_;

    my $wo_parser = XML::LibXML->new();
    my $wo_dom = $wo_parser->parse_string('<FilePacket/>');
    my $wo_doc = $wo_dom->documentElement();

    my ($filePacketNode) = $wo_doc->findnodes('/FilePacket');
    $filePacketNode->setAttribute('status', "I");

    my $protocol = ($CFG::PROTOCOL) ? $CFG::PROTOCOL : 'ftp';
    my $destination = $protocol . ":" . $CFG::HOST . $CFG::DESTDIR;
    $filePacketNode->setAttribute('destination', $destination);

    # create filegroup for dotchart xml file
    if ( $xmlCount ) {
        my $filegroupNode = XML::LibXML::Element->new('FileGroup');
        foreach my $xmlFile ( @{$xmlFiles} ) {
            my $fileNode = XML::LibXML::Element->new('File');
            $fileNode->setAttribute('localPath', $xmlFile);
            $fileNode->setAttribute('status', "I");
            $fileNode->setAttribute('cleanup', "Y");
            $filegroupNode->appendChild($fileNode);
        }
        $wo_doc->appendChild($filegroupNode);
    }

    # create filegroup for every tracking xml file
    if ( $woCount ) {
        my $trackingNode = XML::LibXML::Element->new('FileGroup');
        foreach my $xmlFile ( @{$trackFiles} ) {
            my $fileNode = XML::LibXML::Element->new('File');
            $fileNode->setAttribute('localPath', $xmlFile);
            $fileNode->setAttribute('status', "I");
            $fileNode->setAttribute('cleanup', "Y");

            $trackingNode->appendChild($fileNode);
        }
        $wo_doc->appendChild($trackingNode);
    }

    open (WO, ">$wo") || S4P::perish( 7, "Failed to open workorder file $wo: $!");
    print WO $wo_dom->toString(1);
    close WO;

    return(0) ;
}


#####################################################################
# usage:  print usage and die
#####################################################################
sub usage {
  die << "EOF";
usage: $0 <-c config_file> <-x granule_to_dotchart_xls_file>
                         <-p pending_pdr_dir> <-s staging_dir> [options]
Options are:
        -v                  Verbose
EOF
}

################  end of s4pa_publish_dotchart  ################
