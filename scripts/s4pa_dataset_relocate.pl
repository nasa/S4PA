#!/usr/bin/perl

=head1 NAME

s4pa_dataset_relocate.pl - script to configure the dataset relocation.

=head1 SYNOPSIS

s4pa_dataset_relocate.pl
B<-c> I<configuration_file>
B<-s> I<source_descriptor>
B<-t> I<destination_descriptor>
B<-p> I<pdr_staging_directory>
B<-a> I<pan_staging_directory>
B<-d> I<dataset>
[B<-h> I<hostname>]
[B<-o> I<new_descriptor>]
[B<-m> I<protocol>]
[B<-n> I<maximum_granule>]
[B<-e> I<pdr_expiration_days>]
[B<-f> I<pdr_prefix>]
[B<-v>]

=head1 DESCRIPTION

s4pa_dataset_relocate.pl is design for updating new descriptor file
for dataset relocation. The relocating dataset has to be defined in 
both source and destination descriptors. Follow-up procedure will be
print out at the end of the script execution. Option specified on 
the command line will override the options in the configuration file.

=head1 ARGUMENTS

=over 4

=item B<-c>

Configuration file. Command line option will override the option
specified in the configuration file.

=item B<-s>

Deployment descriptor file on source server

=item B<-t>

Deployment descriptor file on destination server

=item B<-p>

Staging directory for relocateion PDRs.

=item B<-a>

Staging directory for PANs pushed back from the new server.

=item B<-d>

Dataset name to be relocated.

=item B<-h>

Optional NODE_NAME in the PDR. Default to the return value of
S4P::PDR::gethost

=item B<-n>

Optional maximum number of granules per PDR. Default to 50.

=item B<-o>

Optional new descriptor filename. Default to the destination descriptor.

=item B<-m>

Optional protocol for NODE_NAME in destination descriptor. Default to FTP.

=item B<-e>

Optional expiration period. Default to 3 days. This range
will be used to calculate the EXPIRATION_TIME in PDR.

=item B<-f>

Optional PDR filename prefix. Default to 'dsrelocate.<dataset>.'
where <dataset> will be extracted from Dataset root directory
specified in B<-d>.

=item B<-v>

Verbose.

=back

=head1 AUTHORS

Dennis Gerasimov,
Guang-Dih Lei (guang-dih.lei@gsfc.nasa.gov)

=cut

################################################################################
# $Id: s4pa_dataset_relocate.pl,v 1.12 2019/05/06 15:48:00 glei Exp $
# -@@@ S4PA, Version $Name:  $
###############################################################################
#
# name: s4pa_dataset_relocate.pl
# originator DATASET_RELOCATE
# revised: 11/22/2006 glei
#

use strict;
use Getopt::Std;
use Safe;
use XML::LibXML;
use S4P::PDR;
use vars qw( $opt_s $opt_t $opt_p $opt_a $opt_d $opt_c
             $opt_h $opt_o $opt_m $opt_n $opt_e $opt_f $opt_v );

getopts('c:d:s:t:p:a:h:o:m:n:e:f:v');
# usage() if ( !$opt_s || !$opt_t || !$opt_d );


# retrieve config values
my $cpt = new Safe 'CFG';
$cpt->share( '$DATASET', '$SOURCE_DESCRIPTOR', '$DESTINATION_DESCRIPTOR', 
    '$PDR_STAGING_DIRECTORY', '$PAN_STAGING_DIRECTORY', '$SOURCE_HOSTNAME',
    '$SOURCE_PROTOCOL', '$NEW_DESCRIPTOR', '$MAX_GRANULE_COUNT',
    '$EXPIRATION_DAYS', '$PDR_PREFIX');

if ( $opt_c ) {
    $cpt->rdo($opt_c) or
        die "ERROR: Cannot read config file $opt_c in safe mode: $!\n";
} 

##############################################################################
# Assign default values
##############################################################################

my $verbose = $opt_v;

#
# Required arguments
#

# relocating dataset 
my $dataset = $opt_d ? $opt_d : $CFG::DATASET;
usage() unless ( defined $dataset );
print "\n";
print "INFO: Relocating dataset: $dataset\n" if ( $verbose );

# s4pa descriptor file on source server
my $srcDesc = $opt_s ? $opt_s : $CFG::SOURCE_DESCRIPTOR;
die "Source descriptor: $srcDesc does not exist" unless ( -f $srcDesc );
print "INFO: Source descriptor: $srcDesc\n" if ( $verbose );

# s4pa descriptor file on destination server
my $dstDesc = $opt_t ? $opt_t : $CFG::DESTINATION_DESCRIPTOR;
die "Destination descriptor: $dstDesc does not exist" unless ( -f $dstDesc );
print "INFO: Destination descriptor: $dstDesc\n" if ( $verbose );

# pending PDR directory on source server
my $pdrDir = $opt_p ? $opt_p : $CFG::PDR_STAGING_DIRECTORY;
die "PDR staging directory: $pdrDir does not exist" unless ( -d $pdrDir );
print "INFO: PDR staging directory: $pdrDir\n" if ( $verbose );

# received PAN directory on source server
my $panDir = $opt_a ? $opt_a : $CFG::PAN_STAGING_DIRECTORY;
die "PAN staging directory: $panDir does not exist" unless ( -d $panDir );
print "INFO: PAN staging directory: $panDir\n" if ( $verbose );

#
# Optional arguments
#

# new descriptor filename on destination server
my $newDesc = $opt_o ? $opt_o : 
    $CFG::NEW_DESCRIPTOR ? $CFG::NEW_DESCRIPTOR : $dstDesc;
print "INFO: New descriptor: $newDesc\n" if ( $verbose );

# hostname appeared in PDR's NODE_NAME
my $hostname = $opt_h ? $opt_h : 
    $CFG::SOURCE_HOSTNAME ? $CFG::SOURCE_HOSTNAME : $S4P::PDR::gethost;
print "INFO: source NODE_NAME: $hostname\n" if ( $verbose );

# PDR fetching protocol from destination server
my $protocol = $opt_m ? $opt_m : 
    $CFG::SOURCE_PROTOCOL ? $CFG::SOURCE_PROTOCOL : "FTP";
print "INFO: protocl for PAN/PDR transfer: $protocol\n" if ( $verbose );

# maximum number of granule per PDR
my $maxCount = $opt_n ? $opt_n : 
   $CFG::MAX_GRANULE_COUNT ? $CFG::MAX_GRANULE_COUNT : 50;
print "INFO: Maximum number of granules in PDR: $maxCount\n" if ( $verbose );

# PDR naming convention: $prefix.<dataset>.n.PDR
my $prefix = 'dsrelocate';
my $pdrPrefix = $opt_f ? $opt_f : 
    $CFG::PDR_PREFIX ? $CFG::PDR_PREFIX : "$prefix.$dataset";
print "INFO: PDR naming prefix: $pdrPrefix\n" if ( $verbose );

# PDR expiration range in days
my $expiration = $opt_e ? $opt_e : 
    $CFG::EXPIRATION_DAYS ? $CFG::EXPIRATION_DAYS : 3;
print "INFO: PDR's EXPIRATION_TIME: $expiration days\n" if ( $verbose );

##############################################################################
# descriptor update
##############################################################################

# parse both descriptors

my %srcDescriptor = getDescInfo( $srcDesc, $hostname, $verbose, $dataset );
my %dstDescriptor = getDescInfo( $dstDesc, $hostname, $verbose, $dataset );

my $newMethod = "s4pa_relocate_metadata_method.pl \"$dstDescriptor{'metaMethod'} \" ";

# open destination descriptor for update
my $xmlParser = XML::LibXML->new();
$xmlParser->keep_blanks(0);

# Parse the descriptor file.
my $dom = $xmlParser->parse_file( $dstDesc );
my $doc = $dom->documentElement();

# adding new protocol or adding host to existing protocol
if ( ! defined $dstDescriptor{'protocol'} ) {
    my $foundProtocol = 0;
    my $lastProtocol;

    # if specified protocol exist, add hostname to it
    foreach my $protocolNode ( $dom->findnodes( '//protocol' )) {
        if ( $protocolNode->getAttribute( 'NAME' ) eq "$protocol" ) {
            $protocolNode->appendTextChild( 'host', "$hostname" );
            $foundProtocol = 1;
            print "INFO: new host added to protocol $protocol\n" if ( $verbose );
            last;
        }
        $lastProtocol = $protocolNode;
    }

    # if specified protocol was not defined, create new protocol with the hostname
    unless ( $foundProtocol ) {
        my $newProtocol = XML::LibXML::Element->new( 'protocol' );
        $newProtocol->setAttribute( 'NAME', "$protocol" );
        $newProtocol->appendTextChild( 'host', "$hostname" );
        $doc->insertAfter($newProtocol, $lastProtocol);
        print "INFO: new protocol $protocol added for host $hostname\n" if ( $verbose );
    }
} elsif ( $dstDescriptor{'protocol'} ne "$protocol" ) {
    print "WARNING: $hostname is already defined under $dstDescriptor{'protocol'} protocol in $dstDesc,\n";
    print "         please manually move it to under $protocol protocol in $newDesc if necessary.\n";
}
    

foreach my $provider ( $doc->findnodes( '//provider' )) {
    next unless ( $provider->getAttribute('NAME') eq $dstDescriptor{'provider'} );

    # add new pdr poller to destination descriptor
    foreach my $poller ( $provider->findnodes( './/poller' )) {
        my $newJob = XML::LibXML::Element->new( 'job' );
        $newJob->setAttribute( 'NAME', "$dataset\_RELOCATE_POLLING");
        $newJob->setAttribute( 'HOST', "$hostname" );
        $newJob->setAttribute( 'DIR', "$pdrDir" );

        my @pdrPollers =  $poller->findnodes( './/pdrPoller' ); 
        my $pdrPoller = @pdrPollers ? $pdrPollers[0] : 
            XML::LibXML::Element->new( 'pdrPoller' );
        $pdrPoller->appendChild($newJob);
    }
    print "INFO: new pdrPoller added to provider: $dstDescriptor{'provider'}\n";

    # add remote pan directory
    foreach my $pan ( $provider->findnodes( './/pan' )) {
        my $newOs = XML::LibXML::Element->new( 'originating_system' );
        $newOs->setAttribute( 'NAME', "DATASET_RELOCATE");
        $newOs->setAttribute( 'HOST', "$hostname" );
        $newOs->setAttribute( 'DIR', "$panDir" );

        my @remotes = $pan->findnodes( './/remote' );
        my $remote = @remotes ? $remotes[0] : 
            XML::LibXML::Element->new( 'remote' );
        $remote->appendChild($newOs);
    }
    print "INFO: new remote pan added to provider: $dstDescriptor{'provider'}\n";

    # add new metadata method
    foreach my $dataclass ( $provider->findnodes( './/dataClass' )) {
        next unless ( $dataclass->getAttribute('NAME') eq "$dstDescriptor{'dataClass'}" );

        foreach my $dataset ( $dataclass->findnodes( './/dataset' )) {
            next unless( $dataset->getAttribute('NAME') eq "$dstDescriptor{'dataset'}" );

            # if dataset already has a method, replace the metadata child with the $newMethod
            my @methods = $dataset->findnodes( './/method' );
            if ( @methods ) {
                my $method = $methods[0];
                my @metadatas = $method->findnodes( './/metadaata' );
                $method->removeChild($metadatas[0]) if ( @metadatas );
                $method->appendTextChild("metadata", "$newMethod");
                print "INFO: metadata method replaced for dataset: $dstDescriptor{'dataset'}\n";

            # if dataset does not have a method, create a new one with the $newMethod metadata
            } else {
                my $method = XML::LibXML::Element->new( 'method' );
                $method->appendTextChild("metadata", "$newMethod");

                # check if dataVersion was defined. 
                # If yes, method have be inserted before that
                my @versionNodes = $dataset->findnodes( './/dataVersion' );
                if ( @versionNodes ) {
                    $dataset->insertBefore( $method, $versionNodes[0] );
                } else {
                    $dataset->appendChild($method);
                }
                print "INFO: new metadata method added to dataset: $dstDescriptor{'dataset'}\n";
            }
            last;
        }
        last;
    }
    
}

open (OUTFILE, "> $newDesc") or die "Can't open $newDesc: $!";
print OUTFILE $dom->toString(1);
close (OUTFILE);
print "INFO: Descriptor '$newDesc' updated.\n";

##############################################################################
# Instruction for follow-up commands
##############################################################################

# follow-up procedures, re-deploy on destination server
print "\n";
print "Follow-up procedures to startup relocation process:\n";
print "1. Re-deploy destination instance:\n";
print "   * Transfer new descriptor '$newDesc' back to destination server\n";
print "   * Deploy instance on destination server:\n";
print "      > s4pa_deploy.pl -f $newDesc -s S4paDescriptor.xsd\n";

# To generate PDRs
print "\n";
print "2. Create PDRs to kick off relocation process:\n";
print "   * Execute the following command on source server:\n";

# basic data storage directory for versionless dataset
my $datasetLink = "$srcDescriptor{'storageDir'}/" .
    "$srcDescriptor{'group'}/" . $dataset;

# run s4pa_mkpdrs multiple times for multiple version dataset
if ( defined $srcDescriptor{'dataVersion'} ) {
    foreach my $versionid ( @{$srcDescriptor{'dataVersion'}} ) {
        my $mkpdrCommand = "s4pa_mkpdrs.pl " . "-d $datasetLink.$versionid " . 
            "-p $pdrDir " . "-n $maxCount " . "-f $pdrPrefix.$versionid " . 
            "-e $expiration " . "-v ";
        print "      > $mkpdrCommand\n";
    }
} else {
    my $mkpdrCommand = "s4pa_mkpdrs.pl " . "-d $datasetLink " . "-p $pdrDir " . 
        "-n $maxCount " . "-f $pdrPrefix " . "-e $expiration " . "-c -v ";
    print "      > $mkpdrCommand\n";
}

# For PAN pickup and PDR cleanup
print "\n";
print "3. PDR cleanup and dataset deletion:\n";
print "   * Routinely execcute the following command to scan PAN directory\n";
print "     while relocation is in progress:\n";

# run s4pa_relocate_cleanup multiple times for multiple version dataset
if ( defined $srcDescriptor{'dataVersion'} ) {
    foreach my $versionid ( @{$srcDescriptor{'dataVersion'}} ) {
        my $cleanupCommand = "s4pa_relocate_cleanup.pl " . "-r $srcDescriptor{'root'} " .
            "-p $pdrDir " . "-a $panDir " . "-d $dataset " . "-i $versionid " . "-v";
        print "      > $cleanupCommand\n";
    }
} else {
    my $cleanupCommand = "s4pa_relocate_cleanup.pl " . "-r $srcDescriptor{'root'} " .
                         "-p $pdrDir " . "-a $panDir " . "-d $dataset " . "-v";
    print "      > $cleanupCommand\n";
}
print "\n";
exit;


##############################################################################
# Subroutine getDescInfo:  parse descriptor xml and get basic info
#     returning hash includes keys for some basic descriptor value:
#         root, storageDir, protocol, provider, dataClass, 
#         group, dataset, metaMethod, versions
##############################################################################
sub getDescInfo {
    my ($xmlFile, $hostname, $verbose, $dataset) = @_;
    my %descConfig;

    # Create an XML DOM parser.
    my $xmlParser = XML::LibXML->new();
    $xmlParser->keep_blanks(0);

    # Parse the descriptor file.
    my $dom = $xmlParser->parse_file( $xmlFile );
    my $doc = $dom->documentElement();

    my $rootNode = $doc->findnodes( '//root' );
    die "ERROR: root element not found in $xmlFile\n" unless ( defined $rootNode );
    $descConfig{'root'} = $rootNode->string_value();

    my $storageNode = $doc->findnodes( '//storageDir' );
    die "ERROR: storageDir element not found in $xmlFile\n" 
        unless ( defined $storageNode );
    $descConfig{'storageDir'} = $storageNode->string_value();

    # check if hostname is defined under existing protocols
    PROTOCOL: foreach my $protocol ( $doc->findnodes( '//protocol' )) {
        foreach my $host ( $protocol->findnodes( './/host' )) {
            next unless ( $host->string_value() eq "$hostname" );
            $descConfig{'protocol'} = $protocol->getAttribute( 'NAME' );
            last PROTOCOL;
        }
    }
    
    my $providerName;
    my $className;
    my $classGroup;
    my $classMethod;
    my $setMethod;
    my @dataVersions;

    # go through each provider to locate dataset
    PROVIDER: foreach my $provider ( $doc->findnodes( '//provider' )) {
        $providerName = $provider->getAttribute( 'NAME' );

        # go through each dataclass to locate dataset
        foreach my $dataClass ( $provider->findnodes( './/dataClass' )) {
            $className = $dataClass->getAttribute( 'NAME' );
            $classGroup = $dataClass->getAttribute( 'GROUP' );

            # methods can be in the dataclass or inside particular dataset, handle both
            foreach my $method ( $dataClass->findnodes( 'method' )) {
                foreach my $metadata ( $method->findnodes( 'metadata' )) {
                    $classMethod = $metadata->textContent;
                }
            }

            foreach my $dataSet ( $dataClass->findnodes( './/dataset' )) {
                next unless ( $dataSet->getAttribute( 'NAME' ) eq $dataset );

                # check if dataset has version specified
                foreach my $version ( $dataSet->findnodes( './/dataVersion' )) {
                    push @dataVersions, $version->getAttribute( 'LABEL' );
                }

                # check if metadata method was defined in dataset level
                foreach my $method ( $dataSet->findnodes( 'method' )) {
                    foreach my $metadata ( $method->findnodes( 'metadata' )) {
                        $setMethod = $metadata->textContent;
                    }
                }
                last PROVIDER;
            }
        }
    }

    $descConfig{'dataset'} = $dataset;
    $descConfig{'provider'} = $providerName;
    $descConfig{'dataClass'} = $className;
    $descConfig{'group'} = $classGroup;
    $descConfig{'metaMethod'} = $setMethod ? $setMethod : $classMethod;
    $descConfig{'dataVersion'} = [ @dataVersions ] if ( scalar @dataVersions > 0 ); 

    if ( $verbose ) {
        print "INFO: Parsed Descriptor $xmlFile:\n";
        foreach my $descKey ( keys %descConfig ) {
            if ( $descKey eq 'dataVersion' ) {
                foreach my $version ( @{$descConfig{$descKey}} ) {
                    print "      $descKey => $version\n";
                }
            } else {
                print "      $descKey => $descConfig{$descKey}\n";
            }
        }
    }
    return %descConfig;
}


##############################################################################
# Subroutine usage:  print usage and die
##############################################################################
sub usage {
  die << "EOF";
Usage: $0 <-t destination_descriptor> <-s source_descriptor> <-d dataset> 
          <-p pdr_staging_directory> <-a pan_staging_directory> [options]
    Options are:
          -h <hostname>     source NODE_NAME for destination's poller,
                                default to S4P::PDR::gethost value.
          -o <descriptor>   new descriptor, default to <destination_descriptor>
          -m <protocol>     protocol for PDR/PAN transfer, default to FTP
          -n nnn            number of granule per pdr, default to 50
          -e nnn            expiration range in days, default to 3.
          -f <pdr_prefix>   prefix of pdr filename, default to 'dsrelocate.<dataset>'
          -v                Verbose
EOF
}

