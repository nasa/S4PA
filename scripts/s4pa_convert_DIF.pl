#!/usr/bin/perl

=head1 NAME

s4pa_convert_DIF.pl - Converts GCMD DIFs to ECHO xml and S4PA xml, and EMS xml

=head1 SYNOPSIS

s4pa_convert_DIF.pl B<-c> I<configFileName> [B<-v>] [B<-h>] workOrder

=head1 DESCRIPTION

s4pa_convert_DIF.pl uses an xsl transform as well as a mapping located in a
config file to produce ECHO collection xml, a Mirador Product Page,
Mirador Google xml, S4PA and EMS collection xml.

=head1 ARGUMENTS

=over 4

=item B<-c> I<config_file>

A configuration file for used for GCMD DIF processing. It should contain:

=over 4

=item I<$ECHO_XSLFILE>

XSL stylesheet for transforming DIF xml to ECHO xml

=item I<$S4PA_XSLFILE>

XSL stylesheet for transforming DIF xml to S4PA xml

=item I<$TMPDIR>

Directory where transformed files will be staged before being transferred
to another host

=item I<%dataset_to_dif_entry_id>

Hash of hashes, whose primary hash key is the dataset (shortname),
and whose value is a hash which has the S4PA version
label as the key and the GCMD DIF Entry_ID as the value.

=item I<%destination>

Destination URL transformed files will be transferred to for each type of
transformation

=item I<$S4PA_ROOT>

Root directory transformed files will be transferred to for S4PA

=item I<%data_class>

Data class directory transformed files will be transferred to for S4PA for
each shortname

=back

=item [B<-v>]

Compare S4PA version and DIF version as strings. Otherwise, if there are
values for both, they will be compared numerically after removing non-digits.

=item [B<-h>]

This help.

=item I<work_order>

Work order consisting of a GCMD DIF to be converted

=back


=head1 AUTHOR

Lou Fenichel
Ed Seiler (revisions)

=cut

################################################################################
# $Id: s4pa_convert_DIF.pl,v 1.43 2017/04/21 19:32:47 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################
#
# name: s4pa_convert_DIF.pl
# purpose: convert GCMD Directory Interchange Format (DIF)
#          file to ECHO-compliant collection metadata xml
# revised: 02/26/2006 lhf
# revised: 03/13/2006 lhf add shortNameMapping
# revised: 05/03/2006 lhf checked in
#

use strict;
use Getopt::Std;
use XML::LibXML;
#use XML::Simple;
use XML::LibXSLT;
use Encode qw(encode);
use File::Copy;
use S4P;
use Safe;
use File::Basename;
use Cwd;
use vars qw($opt_c $opt_h $opt_v);

getopts('c:hv');
usage() if $opt_h;

my $stationDir = dirname(cwd());
my $cfg_file = $opt_c || '../s4pa_dif_info.cfg';

S4P::perish(1, "Configuration file $cfg_file does not exist")
    unless (-f $cfg_file);
S4P::logger('INFO', "Configuration file is $cfg_file");

# Get configuration, create output file
my $cpt = new Safe 'CFG';
$cpt->rdo($cfg_file) || S4P::perish(2, "Unable to read $cfg_file: ($!)");

# Read DIF from input work order
my $dif_file = $ARGV[0];

# Expect the work order, a DIF file, to be the argument.
S4P::perish(1, "No work order specified") unless defined $dif_file;
S4P::perish(1, "$dif_file not readable") unless (-r $dif_file);
S4P::logger('INFO', "Converting $dif_file");

# Check for existence of other stylesheets
foreach my $ssfile ($CFG::S4PA_XSLFILE,
                    $CFG::ECHO_XSLFILE) {
    S4P::perish(1, "Stylesheet file $ssfile not found") unless (-f $ssfile);
}

# Read table that maps dataset to data class
S4P::perish(1, "No S4PA_ROOT specified in cfg")
    unless (defined $CFG::S4PA_ROOT);
my $s4pa_storage_root = "$CFG::S4PA_ROOT/storage";
my $dataset_cfg = "$s4pa_storage_root/dataset.cfg";
S4P::perish(1, "Could not read $dataset_cfg") unless (-r $dataset_cfg);
$cpt->rdo($dataset_cfg) || S4P::perish(2, "Unable to read $dataset_cfg: ($!)");

my $defaultErrorHandler = $SIG{__DIE__};
my $parser = XML::LibXML->new();
$parser->keep_blanks(0);
$SIG{__DIE__} = 'DomErrorHandler';
my $dom = $parser->parse_file($dif_file);
my $encoding = $dom->encoding();

# Insert instruction to use a stylesheet in the S4PA archive for the HTML
# display of a DIF in the S4PA archive.
# Technically, this shouldn't be done for DIFs that will be published
# without first being converted by a stylesheet, since the HTML
# stylesheet will not be available at the destination of the published file,
# but we will assume that including this instruction does not cause any harm.
my $styleSheet = "/data/S4paDIF102HTML.xsl";
$dom->insertProcessingInstruction('xml-stylesheet',
                                  qq(type="text/xsl" href="$styleSheet"));

my $doc = $dom->documentElement();
S4P::perish(2, "Failed to find document element in DIF file")
    unless (defined $doc);

# Invert %CFG::cmr_collection_id, which is a hash of hashes, whose
# primary hash key is the s4pa dataset name, and whose value is a hash
# which has the s4pa version label as the key, and whose value is a hash
# which has various CMR field's key and value pair.
# The inverted hash has the CMR entry_id as the primary hash key, and its
# value is a hash which has the dataset name as the key and the s4pa version
# label as the value.
my %collection_to_s4pa;
foreach my $dataset (keys %CFG::cmr_collection_id) {
    foreach my $version (keys %{$CFG::cmr_collection_id{$dataset}}) {
        my $short_name =  $CFG::cmr_collection_id{$dataset}{$version}{'short_name'};
        my $version_id =  $CFG::cmr_collection_id{$dataset}{$version}{'version_id'};
        my $entry_id =  $CFG::cmr_collection_id{$dataset}{$version}{'entry_id'};
        my $s4pa_dataset = {};
        $s4pa_dataset->{'dataset'} = $dataset;
        $s4pa_dataset->{'version'} = $version;
        push @{$collection_to_s4pa{$short_name}{$version_id}{'datasets'}}, $s4pa_dataset;
        $collection_to_s4pa{$short_name}{$version_id}{'entry_id'} = $entry_id;
    }
}

# identify shortname and version of this collection metadata
my ($difNode) = $doc->findnodes('//DIF');
my ($difShortnameNode) = $difNode->findnodes('./Entry_ID/Short_Name');
my $dif_ShortName;
if (defined $difShortnameNode) {
    $dif_ShortName = $difShortnameNode->textContent();
} else {
    S4P::perish(2, "Failed to find '/Entry_ID/Short_Name' element in $dif_file.");
}
my ($difVersionNode) = $difNode->findnodes('./Entry_ID/Version');
my $dif_VersionID;
if (defined $difVersionNode) {
    $dif_VersionID = $difVersionNode->textContent();
} else {
    S4P::perish(2, "Failed to find '/Entry_ID/Version' element in $dif_file.");
}

# identify entry_id associated with this collection metadata
my $entry_id = $collection_to_s4pa{$dif_ShortName}{$dif_VersionID}{'entry_id'};

# Perform the transformation for each dataset (ShortName) which uses
# the DIF with this Entry_ID
foreach my $s4pa_dataset ( @{$collection_to_s4pa{$dif_ShortName}{$dif_VersionID}{'datasets'}} ) { 
    my $ShortName = $s4pa_dataset->{'dataset'};
    my $S4PA_VersionID = $s4pa_dataset->{'version'};

    # Publish to dotchart becomes optional, ticket #7336.
    my $publishEMS = $CFG::cfg_publish_dotchart;

    # Convert DIF based on publication requirement, ticket #6092.
    my $publishMirador = 0;
    if (exists $CFG::cfg_publication{$ShortName}) {
        foreach my $version (keys %{$CFG::cfg_publication{$ShortName}}) {
            next unless ($version eq $S4PA_VersionID);
            foreach my $publication (@{$CFG::cfg_publication{$ShortName}{$version}}) {
                $publishMirador = 1 if ($publication =~ /publish_mirador/i);
            }
        }
    }

    # Transform DIF xml to produce S4PA dataset xml
    my $s4pa_xml = create_generic_xml($dom, $doc, $CFG::S4PA_XSLFILE);

    S4P::perish(3, "Could not determine data class for $ShortName")
        unless (exists $CFG::data_class{$ShortName});
    my $s4pa_xml_output_dir = "$s4pa_storage_root/$CFG::data_class{$ShortName}/$ShortName";
    $s4pa_xml_output_dir .= ".$S4PA_VersionID" if ($S4PA_VersionID ne '');
    $s4pa_xml_output_dir .= '/data';
    S4P::perish(3, "S4PA storage directory $s4pa_xml_output_dir does not exist")
        unless (-d "$s4pa_xml_output_dir");
    my $s4pa_xml_output_file = "$s4pa_xml_output_dir/$ShortName.xml";
    my $s4pa_dif_file = "$s4pa_xml_output_dir/${entry_id}_dif.xml";

    open(S4PA_OUTPUT, "> $s4pa_xml_output_file") or
        S4P::perish(1, "Could not open $s4pa_xml_output_file for writing");

    # Write DIF file to be stored in S4PA archive
    unless (S4P::write_file($s4pa_dif_file, encode($encoding, $dom->toString(1)))) {
        S4P::logger('ERROR', "Failed to update $s4pa_dif_file");
        return 0;
    }

    # Write S4PA dataset xml file
    print S4PA_OUTPUT encode($encoding, $s4pa_xml);
    close(S4PA_OUTPUT) or S4P::perish(1, "Error writing $s4pa_xml_output_file");

    my ($protocol, $host_id, $remoteHost, $remoteDir);
    if ($publishEMS) {
        my $emsVersionID = ($S4PA_VersionID ne '') ? ".$S4PA_VersionID" : '';
        my $ems_dif_file = "$CFG::TMPDIR/EMSColl_${ShortName}${emsVersionID}.xml";
        # Write DIF file to be published to EMS
        unless (S4P::write_file($ems_dif_file, encode($encoding, $dom->toString(1)))) {
            S4P::logger('ERROR', "Failed to update $ems_dif_file");
            return 0;
        }

        # Create output work orders
        my $ems_wo_file = "PUSH.EMS_${entry_id}.${ShortName}${emsVersionID}.wo";
        open(EMS_WO, "> $ems_wo_file") or
            S4P::perish(1, "Could not open $ems_wo_file for writing");
        ($protocol, $host_id) = split ":", $CFG::destination{'EMS'};
        ($remoteHost, $remoteDir) = $host_id =~ m#(.+?)/(.+)#;
        print EMS_WO construct_wo_xml($parser, $ems_dif_file,
                                      $remoteHost, $remoteDir, $protocol);
        close(EMS_WO) or S4P::perish(1, "Error writing $ems_wo_file");
    }

    if ($publishMirador) {
        my $miradorVersionID = ($S4PA_VersionID ne '') ? ".$S4PA_VersionID" : '';
        my $mirador_dif_file = "$CFG::TMPDIR/${ShortName}${miradorVersionID}.xml";

        # Write DIF file to be published to Mirador
        unless (S4P::write_file($mirador_dif_file, encode($encoding, $dom->toString(1)))) {
            S4P::logger('ERROR', "Failed to update $mirador_dif_file");
            return 0;
        }

        # Create output work orders
        my $mirador_wo_file = "PUSH.MIRADOR_${entry_id}.${ShortName}${miradorVersionID}.wo";
        open(MIRADOR_WO, "> $mirador_wo_file") or
            S4P::perish(1, "Could not open $mirador_wo_file for writing");
        ($protocol, $host_id) = split ":", $CFG::destination{'MIRADOR'};
        ($remoteHost, $remoteDir) = $host_id =~ m#(.+?)/(.+)#;
        print MIRADOR_WO construct_wo_xml($parser, $mirador_dif_file,
                                          $remoteHost, $remoteDir, $protocol);
        close(MIRADOR_WO) or S4P::perish(1, "Error writing $mirador_wo_file");
    }
}

exit 0;


sub usage {
    my $usage = "
Usage: $0 [-c config_file] work_order
  -c config_file:  Configuration file containing dataset to Entry_ID map (default=../s4pa_convert_DIF.cfg)
";
    S4P::perish(1, $usage)
}

sub create_ECHO_xml {
    my ($dom, $doc, $ShortName, $VersionID, $stylesheet_file) = @_;

    # Get current configured ECHO version
    $stylesheet_file =~ /S4paDIF2ECHO(\d{2})\.xsl/;
    my $echoSchemaVersion = ( defined $1 ) ? $1 : 9;

    # Transform DIF xml to produce ECHO collection xml

    if ($VersionID eq '') {
        S4P::perish(1, "No VersionID arg. provided to create_ECHO_xml")
    }

    # Replace the Data_Set_Citation/Version in the DIF doc with
    # a numerical version
    if ($VersionID !~ /\d+\.?\d*/) {
        $VersionID =~ s/\D//g;
        $VersionID = sprintf("%3.3d", $VersionID);
    }
    my ($difVersionNode) = $doc->findnodes('//Data_Set_Citation/Version');
    S4P::perish(1, "Could not replace Version in DIF for ECHO conversion")
        unless (_ReplaceXMLNode($difVersionNode, $VersionID));

    # Parse the stylesheet
    $SIG{__DIE__} = $defaultErrorHandler;
    my $xslt = XML::LibXSLT->new();
    $SIG{__DIE__} = 'StyleSheetErrorHandler';
    my $styleSheet = $xslt->parse_stylesheet_file($stylesheet_file);

    # Run the transform
    my $transform = $styleSheet->transform($dom);

    # Convert transform output to a string
    my $transform_string = $styleSheet->output_string($transform);

    # If the CollectionDescription field in the output string is too long
    # for ECHO, truncate it. (We can't do this in the stylesheet, because
    # there we do not have the output string, in which characters
    # such as the less-than symbol have been HTML escaped).

    # ECHO10 schema changes <CollectionDescription> to <Description>.
    # So, we will need to switch it according the stylesheet version.
    # However, there are many <Description> section inside the converted
    # sring, we will need to parse it in as XML structure then edit the
    # collection description only for ECHO10.

    my $xmlParser = XML::LibXML->new();
    $xmlParser->keep_blanks( 0 );
    my $convertedDom = $xmlParser->parse_string( $transform_string );
    my $convertedDoc = $convertedDom->documentElement();

    # If a DIF update changes the value of Data_Set_Citation/Dataset_Title, 
    # the updated DIF that is published by S4PA will not be ingested by ECHO,
    # because ECHO does not allow the DataSetId for an existing collection to be modified.
    # A new hash table should already get created in s4pa_dif_info.cfg file that
    # contains the mapping of s4pa collection and ECHO DataSetId. We can use that 
    # table to make sure that the collection metadata published to ECHO contains
    # the DataSetId that ECHO already has for that dataset. 

    # if no matching dataset in the hash table, it could be a new collection,
    # skip checking DataSetId and publish as is.
    if ( defined $CFG::echo_dataset_id{$ShortName} ) {
        foreach my $version ( keys %{$CFG::echo_dataset_id{$ShortName}} ) {
            # found a matching version, make sure the DataSetId is still the same.
            if ( $version eq $VersionID ) {
                my $echo_datasetid = $CFG::echo_dataset_id{$ShortName}{$version};
                my $dataSetIdNode;
                ( $dataSetIdNode ) = $convertedDoc->findnodes('//Collections/Collection/DataSetId');
                my $dif_datasetid = $dataSetIdNode->textContent();
                # if not, replace the current DataSetId with the stored one in ECHO.
                if ( $echo_datasetid ne $dif_datasetid ) {
                    my $newDataSetIdNode = XML::LibXML::Element->new( 'DataSetId' );
                    $newDataSetIdNode->appendText( $echo_datasetid );
                    $dataSetIdNode->replaceNode( $newDataSetIdNode );
                    S4P::logger('WARN', "Replaced <DataSetId> from DIF's '$dif_datasetid'" .
                        " to ECHO's '$echo_datasetid'");
                }
            }
        }
    }

    my $descriptionNode;
    if ( $echoSchemaVersion == 9 ) {
        ( $descriptionNode ) = $convertedDoc->findnodes('//Collections/CollectionMetaData/CollectionDescription');
    } else {
        ( $descriptionNode ) = $convertedDoc->findnodes('//Collections/Collection/Description');
    }
    my $CollectionDescription = $descriptionNode->textContent();
    my $newCollectionDescription;

    if ($CollectionDescription) {
        # The DIF is obtained by an S4PA station that runs a script that
        # obtains the xml output produced by a CGI script. That xml output
        # is indented in such a way that fields containing newline
        # characters have every line indented, rather than just the
        # first line. This effectively changes the content of the field.
        # For the Summary field of the DIF, which the stylesheet transforms
        # to CollectionDescription, the indentation is 6 spaces. Here
        # we remove that undesired indentation.
        $CollectionDescription =~ s/^      //mg;
    }
    my $maxCollectionDescriptionLength = 4000;
    my $collectionDescriptionLength = $CollectionDescription ?
                                      length($CollectionDescription) :
                                      0;
    if ($collectionDescriptionLength > $maxCollectionDescriptionLength) {

        # We want to truncate such that the truncated field length
        # plus '...' is less than or equal to the maximum allowed.
        # Determine the length of the truncated description.
        my $substr_length = $maxCollectionDescriptionLength - 3;

        # We don't want to truncate within an escape sequence (an ampersand
        # followed by two or more characters followed by a semicolon),
        # so truncate at an earlier point if the truncation would prevent
        # an escape sequence from being completed.

        # Find the index of the last ampersand in the truncated description
        # by finding the index of the the first ampersand in the reversed
        # description, starting at the point where the truncation would occur.
        my $amp_index = index(scalar(reverse($CollectionDescription)), '&',
                              $collectionDescriptionLength - $substr_length);
        if ($amp_index >= 0) {

            # We found an ampersand within the truncated description.
            # Convert the index counted from the end of the entire description
            # to an index counted from the start of the description.
            $amp_index = $collectionDescriptionLength - $amp_index - 1;

            # Find the index of the first semicolon after the last ampersand.
            my $semi_index = index($CollectionDescription, ';', $amp_index);

            # If the first semicolon after the last ampersand would not be
            # included in the truncated description, change the truncated
            # field length to truncate just before the last ampersand
            if ($semi_index > ($substr_length-1)) {
                $substr_length = $amp_index;
            }
        }
        $newCollectionDescription = substr($CollectionDescription, 0,
                                              $substr_length)
                                       . '...';
    } else {
        $newCollectionDescription = $CollectionDescription;

    }
    my $newDescriptionNode;
    if ( $echoSchemaVersion == 9 ) {
        $newDescriptionNode = XML::LibXML::Element->new( 'CollectionDescription' );
    } else {
        $newDescriptionNode = XML::LibXML::Element->new( 'Description' );
    }
    $newDescriptionNode->appendText( $newCollectionDescription );
    $descriptionNode->replaceNode( $newDescriptionNode );
    $transform_string = $convertedDoc->toString(1);

    # Make sure a DeleteTime tag does not appear. This is to guarantee that the
    # transformation does not include a DeleteTime tag, which would then
    # be assigned a value by ECHO ingest, resulting in the deletion of the
    # collection in some circumstances.
    $transform_string =~ s/<DeleteTime\/>//;

    $SIG{__DIE__} = $defaultErrorHandler;

    return $transform_string;
}


sub create_generic_xml {
    my ($dom, $doc, $stylesheet_file) = @_;

    # Transform DIF xml using XSLT alone

    # Parse the stylesheet
    $SIG{__DIE__} = $defaultErrorHandler;
    my $xslt = XML::LibXSLT->new();
    $SIG{__DIE__} = 'StyleSheetErrorHandler';
    my $styleSheet = $xslt->parse_stylesheet_file($stylesheet_file);

    # Run the transform
    my $transform = $styleSheet->transform($dom);

    # Convert transform output to a string
    my $transform_string = $styleSheet->output_string($transform);

    $SIG{__DIE__} = $defaultErrorHandler;

    return $transform_string;
}


sub _ReplaceXMLNode
{
    my ($oldNode, $value) = @_;

    # Replace node $oldNode with a node containing value $value

    return 0 unless defined $oldNode;

    # Get the parent node of the node being replaced
    my $parent = $oldNode->parentNode();
    return 0 unless defined $parent;

    # Get the next sibling of the node being replaced
    my $sibling = $oldNode->nextSibling();

    # Clone the node being replaced and replace the content with the new
    # value
    my $newNode = XML::LibXML::Element->new($oldNode->getName());
    $newNode->appendText($value);

    # Remove the node being replaced from the tree
    $parent->removeChild($oldNode);

    # If the old node had a sibling, insert the new node after that. Otherwise,
    # insert as a child.
    if ($sibling) {
        $parent->insertBefore($newNode, $sibling);
    } else {
        $parent->appendChild($newNode);
    }

    return 1;
}


sub _InsertXMLNode
{
    my ($parent, $name, $value) = @_;

    # Insert a new node with name $name and value $value as a child of $parent

    return 0 unless defined $parent;

    # Create the node being inserted
    my $newNode = XML::LibXML::Element->new($name);
    $newNode->appendText($value);

    # Insert the new node as a child.
    $parent->appendChild($newNode);

    return 1;
}


sub construct_wo_xml {
    my ($parser, $localPath, $remoteHost, $remoteDir, $protocol) = @_;

    # Return a text string consisting of an XML document that can
    # be used to transfer a local file to a remote destination.

    # Create an XML document containing one FilePacket node, which
    # contains one FileGroup node, which contains one File node.
    my $wo_dom = $parser->parse_string('<FilePacket/>');
    my $wo_doc = $wo_dom->documentElement();

    # Set attributes of the FilePacket node to describe a destination
    my ($FilePacket_node) = $wo_doc->findnodes('/FilePacket');
    $FilePacket_node->setAttribute('status', 'I');
    my $destination = "$protocol:$remoteHost/$remoteDir";
    $FilePacket_node->setAttribute('destination', $destination);

    # Set attributes of the File node to specify the local path of the file
    # being sent
    my $File_node = XML::LibXML::Element->new('File');
    $File_node->setAttribute('status', 'I');
    $File_node->setAttribute('localPath', $localPath);
    $File_node->setAttribute('cleanup', 'Y');

    my $FileGroup_node = XML::LibXML::Element->new('FileGroup');
    $FileGroup_node->appendChild($File_node);

    $wo_doc->appendChild($FileGroup_node);

    return $wo_dom->toString(1);
}


sub DomErrorHandler
{
    my ($msg) = @_;

    S4P::perish(1, $msg)
}


sub StyleSheetErrorHandler
{
    my ($msg) = @_;

    S4P::perish(1, $msg)
}
