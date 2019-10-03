#!/usr/bin/perl

=head1 SYNOPSIS

s4pa_reconciliation_recon.pl

[B<-m> mode                   I< flag to create either "passive" or "active" mode>]

[B<-i> instance               I< flag for configurationmode "failover", "normal" or "resync">]

[B<-d> descriptor_file        I< name of descriptor file>]

[B<-s> subscription_file      I< name of subscription file>]

[B<-x> descriptor_schema      I< schema file for validting descriptor file>]

[B<-z> subscription_schema    I< schema file for validating subscription XML file>]

[B<-p> ip_flag                I< flag for lost IP/DNS>]

=cut

use strict;
use XML::Simple;
use XML::LibXML;
use Storable qw(dclone);
use FileHandle;
use S4P;
use Data::Dumper;
use Getopt::Std;
use File::Copy qw(copy);

my %opt;

# Get command line arguments
getopts( "m:i:d:s:x:z:ph", \%opt );

usage() if $opt{h};

#"/tools/gdaac/OPS/cfg/s4pa/replication/replication.cfg" for production

# Assign cli parameters to variables
my $instance           = $opt{i} || die "Need to specify deployment instance";
my $mode               = $opt{m} || die "Need to specify deployment mode";
my $descriptorFile     = $opt{d} || die "Need to specify deployment descriptor";
my $subscriptionXML    = $opt{s} || die "Need to specify subscription XML file";
my $descriptorSchema   = $opt{x};
my $subscriptionSchema = $opt{z};

my $ipFlag = 0;
$ipFlag = 1 if (exists $opt{p});
usage() if (exists $opt{h}); 

# set S4PA name suffix for replication instance
my $suffix = '_dup';

# Make backup copies of the descriptor and sunscription XML files
copy( "$descriptorFile",   "$descriptorFile.bak" )   || die "Could not back up file $descriptorFile";
copy( "$subscriptionXML", "$subscriptionXML.bak" ) || die "Could not back up file $subscriptionXML";

# Read in descriptor file as XML::libXML object
my $parser = XML::LibXML->new();
my $ddoc   = $parser->parse_file($descriptorFile);

# Read in subscription file as XML::libXML object
my $sdoc = $parser->parse_file($subscriptionXML);

# Find datasets for replication
my @replicateDatasetList = ();
my @replicateProviderList = ();
my @providerList = $ddoc->findnodes('//provider');
foreach my $provider (@providerList) {
    my $providerName = $provider->getAttribute("NAME");
    my @dataClassList = $ddoc->findnodes('//provider/dataClass');
    foreach my $dataclass (@dataClassList) {
        my @datasetList = $dataclass->findnodes('//provider/dataClass/dataset');
        my $replicate_flag = $dataclass->getAttribute('REPLICATE');
        if ($replicate_flag eq 'true') {
            push @replicateProviderList, $providerName;
            foreach my $dataset (@datasetList) {
                my $datasetName = $dataset->getAttribute("NAME");
                push @replicateDatasetList, $datasetName;
            }
        } else {
            foreach my $dataset (@datasetList) {
                my $datasetName = $dataset->getAttribute("NAME");
                my @dataVersionList = $dataset->findnodes('//provider/dataClass/dataset/dataVersion');
                my $replicate_flag = $dataset->getAttribute('REPLICATE');
                if ($replicate_flag eq 'true') {
                    push @replicateProviderList, $providerName;
                    push @replicateDatasetList, $datasetName;
                } else {
                    foreach my $dataversion (@dataVersionList) {
                        my $replicate_flag = $dataversion->getAttribute('REPLICATE');
                        if ($replicate_flag eq 'true') {
                            push @replicateProviderList, $providerName;
                            push @replicateDatasetList, $datasetName;
                        }
                    }
                }
            }
        }
    }
}

if ( ( $instance eq 'active' ) && ( $mode eq 'normal' ) ) {
    if (@replicateDatasetList) {
        generate_syncup_subscriptions( $ddoc, $sdoc, $ipFlag, @replicateDatasetList );
    }
}
elsif ( ( $instance eq 'passive' ) && ( $mode eq 'normal' ) ) {
    if (@replicateDatasetList) {
        create_passive_instance_descriptor( $ddoc, $suffix, $ipFlag, @replicateProviderList );
    } else {
        S4P::perish(1, "No dataset to replicate");
    }
}
elsif ( ( $instance eq 'active' ) && ( $mode eq 'failover' ) ) {
    create_active_failover_descriptor( $ddoc, $suffix );
    if (@replicateDatasetList) {
        generate_syncup_subscriptions( $ddoc, $sdoc, $ipFlag, @replicateDatasetList );
    }
}
elsif ( ( $instance eq 'passive' ) && ( $mode eq 'resync' ) ) {
    if (@replicateDatasetList) {
        create_passive_instance_descriptor( $ddoc, "", $ipFlag, @replicateProviderList );
    } else {
        S4P::perish(1, "No dataset to replicate");
    }
}
else { usage() }

open( DOUT, ">$descriptorFile" );
open( SOUT, ">$subscriptionXML" );

print DOUT $ddoc->toString(0);
print SOUT $sdoc->toString(0);

close(SOUT);
close(DOUT);

# If schema specified for descriptor, validate and deploy
if ($descriptorSchema) {
    my $runDeploy = "s4pa_deploy.pl -f $descriptorFile -s $descriptorSchema";
    my $exec = `$runDeploy `;
    if ($?) {
        S4P::perish(1, "Error in S4PA deployment ($?)");
    }
}

# If schema specified for subscription XML file, validate and update subscription
if ($subscriptionSchema) {
    my $runSub = "s4pa_update_subscription.pl -f $subscriptionXML -d $descriptorFile -s $subscriptionSchema";
    my $exec = `$runSub `;
    if ($?) {
        S4P::perish(1, "Error in updating subscription ($?)");
    }
}

exit 0;

#####################
# Subroutines
#####################

sub create_active_failover_descriptor {

    my ( $ddoc, $suffix ) = @_;

    # find replication element
    my ($replicationNode) = $ddoc->findnodes('//replication');
    if (!$replicationNode) {
        print "Cannot replicate: No replication element found in descriptor";
        return;
    }

    # get S4PA root, active file system path, and notify address
    my $root = $replicationNode->getAttribute('ROOT');
    my $active_fs = $replicationNode->getAttribute('ACTIVE_FILE_SYSTEM');
    my $notify = $replicationNode->getAttribute('NOTIFY_ON_FULL');

    # Append suffix to instance name
    my ($s4pa_node) = $ddoc->getElementsByTagName("s4pa");
    $s4pa_node->setAttribute( "NAME", $s4pa_node->getAttribute("NAME") . $suffix );

    # reset S4PA root
    $ddoc->findnodes("/s4pa/root/text()")->pop()->setData($root);
    
    # reset activefilesystem, and NOTIFY_ON_FULL if provided, within each provider
    $ddoc->findnodes("//provider/activeFileSystem/text()")->pop()->setData($active_fs);
    if ($notify) {
        my ($afs_node) = $ddoc->findnodes("//provider/activeFileSystem");
        $afs_node->setAttribute("NOTIFY_ON_FULL", $notify);
    }
}

sub create_passive_instance_descriptor {

    my ( $ddoc, $suffix, $ipFlag, @replicateProviderList ) = @_;

    # find replication element
    my ($replicationNode) = $ddoc->findnodes('//replication');
    if (!$replicationNode) {
        print "Cannot replicate: No replication element found in descriptor";
        return;
    }

    # get replication parameters
    my $root = $replicationNode->getAttribute('ROOT');
    my $active_fs = $replicationNode->getAttribute('ACTIVE_FILE_SYSTEM');
    my $notify = $replicationNode->getAttribute('NOTIFY_ON_FULL');
    my $pdr_dir = $replicationNode->getAttribute('PDR_LOCATION');
    my $pan_dir = $replicationNode->getAttribute('PAN_LOCATION');
    my $httpRoot = $replicationNode->getAttribute('HTTP_ROOT');
    my $ftpRoot = $replicationNode->getAttribute('FTP_ROOT');
    my ($protocol, $host, $junk, $dir) = ($pdr_dir =~ /(\w+):\/\/((\w+\.)*\w+)((\/\w+)*\/?)/);

    # Append suffix to instance name
    my ($s4pa_node) = $ddoc->getElementsByTagName("s4pa");
    $s4pa_node->setAttribute( "NAME", $s4pa_node->getAttribute("NAME") . $suffix );

    # reset S4PA root if $suffix non-empty
    $ddoc->findnodes("/s4pa/root/text()")->pop()->setData($root) if ($suffix);

    # reset protocol and host
    my @protoNodeList = $ddoc->findnodes('//protocol');
    foreach my $node (@protoNodeList) {
        $node->unbindNode();
    }
    my $protoNode = XML::LibXML::Element->new('protocol');
    $protoNode->setAttribute("NAME", $protocol);
    $protoNode->appendTextChild('host', $host);
#    $protoNode->appendTextChild('host', 'localhost');
    my ($pubNode) = $ddoc->findnodes('//publication');
    $s4pa_node->insertBefore($protoNode, $pubNode); 
    
    # For each provider gather the dataset names under it and create job tags
    foreach my $providerNode ( $ddoc->findnodes("//s4pa/provider") ) {
        my $replicateFlag = 0;
        foreach my $replicateProvider (@replicateProviderList) {
            if ($providerNode->getAttribute("NAME") eq $replicateProvider) {
                $replicateFlag = 1;
                last;
            }
        }
        if ($replicateFlag) {
            # In normal mode, reset active file system, and NOTIFY_ON_FULL if provided
            if ($suffix) {
                my ($afs_node) = $providerNode->findnodes("activeFileSystem/text()");
                $afs_node->setData($active_fs);
                if ($notify) {
                    $afs_node->parentNode()->setAttribute("NOTIFY_ON_FULL","$notify");
                } else {
                    $afs_node->parentNode()->removeAttribute("NOTIFY_ON_FULL");
                } 
            }

            # Re-write single PDR poller
            my ($pollerNode) = $ddoc->findnodes("//s4pa/provider/poller");
            $pollerNode->unbindNode();
            my $poller = XML::LibXML::Element->new('poller');
            my $pdrPoller = XML::LibXML::Element->new('pdrPoller');
            my $job = XML::LibXML::Element->new('job');
            my $pollName = $providerNode->getAttribute("NAME") . "_replication";
            $job->setAttribute("NAME", $pollName);
            $job->setAttribute("HOST", $host);
            $job->setAttribute("DIR", $dir);
            $pdrPoller->appendChild($job);
            $poller->appendChild($pdrPoller);
            my ($afsNode) = $providerNode->findnodes('//activeFileSystem');
            $providerNode->insertAfter($poller, $afsNode);

            # Re-write pan node
            my ($panNode) = $ddoc->findnodes("//s4pa/provider/pan");
            $panNode->unbindNode();
            my $pan = XML::LibXML::Element->new('pan');
            my $localPanDir = $root . "/pan";
            $pan->appendTextChild('local', $localPanDir);
            my $remotePan = XML::LibXML::Element->new('remote');
            my $origSys = XML::LibXML::Element->new('originating_system');
            my $sysName = $providerNode->getAttribute("NAME") . "_replication";
            $origSys->setAttribute("NAME", $sysName);
            $origSys->setAttribute("HOST", $host);
            $origSys->setAttribute("DIR", $pan_dir);
            $remotePan->appendChild($origSys);
            $pan->appendChild($remotePan);
            $providerNode->insertAfter($pan, $poller);

            foreach my $dataClassNode ( $ddoc->findnodes("//s4pa/provider/dataClass") ) {

                # reset all ECHO, MIRADOR, and WHOM publication to false
                $dataClassNode->setAttribute( "PUBLISH_ECHO", "false" );
                $dataClassNode->setAttribute( "PUBLISH_MIRADOR", "false" );
                $dataClassNode->setAttribute( "PUBLISH_WHOM", "false" );
                foreach my $datasetNode ($dataClassNode->findnodes("dataset") ) {
                    $datasetNode->setAttribute( "PUBLISH_ECHO", "false" );
                    $datasetNode->setAttribute( "PUBLISH_MIRADOR", "false" );
                    $datasetNode->setAttribute( "PUBLISH_WHOM", "false" );
                    foreach my $dataVersionNode ( $datasetNode->findnodes("dataVersion") ) {
                        $dataVersionNode->setAttribute( "PUBLISH_ECHO", "false" );
                        $dataVersionNode->setAttribute( "PUBLISH_MIRADOR", "false" );
                        $dataVersionNode->setAttribute( "PUBLISH_WHOM", "false" );
                    }
                }

                # Skip any dataClass, dataset, dataVersion not for replication
                if ($dataClassNode->getAttribute("REPLICATE") ne "true") {
                    foreach my $datasetNode ($dataClassNode->findnodes("dataset") ) {
                        if ($datasetNode->getAttribute("REPLICATE") ne "true") {
                            foreach my $dataVersionNode ( $datasetNode->findnodes("dataVersion") ) {
                                if ($dataVersionNode->getAttribute("REPLICATE") ne "true") {
                                    $dataVersionNode->unbindNode();
                                }
                            }
                            my @replicateDataVersionList = $datasetNode->childNodes();
                            if (!@replicateDataVersionList) {
                                $datasetNode->unbindNode();
                            }
                        }
                    }
                    my @replicateDatasetList = $dataClassNode->childNodes();
                    if (!@replicateDatasetList) {
                        $dataClassNode->unbindNode();
                    }
                }

            }

            # Remove method tags
            my @dataset_methods = $providerNode->findnodes("./dataClass/dataset/method");
            foreach my $dataset_method ( $providerNode->findnodes("./dataClass/dataset/method")) {
                $dataset_method->unbindNode();
            }

        } else {
            $providerNode->unbindNode();
        }
    }

    # Reset urlRoot if ip/DNS not available
    if ($ipFlag) {
        my ($urlNode) = $ddoc->findnodes("//urlRoot");
        $urlNode->unbindNode();
        my $urlRoot = XML::LibXML::Element->new("urlRoot");
        $urlRoot->setAttribute("HTTP", $httpRoot);
        $urlRoot->setAttribute("FTP", $ftpRoot);
        $s4pa_node->insertBefore($urlRoot, $protoNode);
    }

}

sub generate_syncup_subscriptions {

    my ( $ddoc, $sdoc, $ipFlag, @replicateDatasetList ) = @_;

    # Hard-wire label, id, and user
    my ($s4pa_node) = $ddoc->getElementsByTagName("s4pa");
    my ($replicationNode) = $ddoc->getElementsByTagName("replication");
    my $label = "REPLICATOR_" . $s4pa_node->getAttribute('NAME');
    my $id = "REPLICATOR_" . $s4pa_node->getAttribute('NAME');
    my $user = "REPLICATOR";

    # Extract other parameters from descriptor
    my $pdr_dir = $replicationNode->getAttribute('PDR_LOCATION');
    my $maxGranule = $replicationNode->getAttribute('MAX_PDR_SIZE');
    my ($protocol, $host, $junk, $dir) = ($pdr_dir =~ /(\w+):\/\/((\w+\.)*\w+)((\/\w+)*\/?)/);
    my $address = $host . $dir;
    my $protocol = 'file';

    # Insert additional PDR pull subscription
    my ($subscriptionNode) = $sdoc->getElementsByTagName("subscription");
    my $pdrSubscription = XML::LibXML::Element->new("pullSubscription");
    $pdrSubscription->setAttribute("LABEL", $label);
    $pdrSubscription->setAttribute("ID", $id);
    $pdrSubscription->setAttribute("MAX_GRANULE_COUNT", $maxGranule);
    $pdrSubscription->setAttribute("USER", $user);
    my $notify = XML::LibXML::Element->new("notification");
    $notify->setAttribute("FORMAT", "PDR");
    $notify->setAttribute("PROTOCOL", $protocol);
    $notify->setAttribute("ADDRESS", $address);
    $pdrSubscription->appendChild($notify);
    foreach my $dataset (@replicateDatasetList) {
        my $datasetNode = XML::LibXML::Element->new("dataset");
        $datasetNode->setAttribute("NAME", $dataset);
        $pdrSubscription->appendChild($datasetNode);
    }
    $subscriptionNode->appendChild($pdrSubscription);

}


sub usage {

    print <<'USAGE';
    
        Command Line Option
                                
        instance                $opt{i}   "passive" or "active"
        mode                    $opt{m}   "failover", "normal" or "resync"
        descriptor_file         $opt{d}    local name of descriptor file
        subscription_file       $opt{s}    local name of subscription file
        descriptor_schema       $opt{x}    schema file for validating descriptor file
        subscription_schema     $opt{z}    schema file for validating subscription XML file
        ip_flag                 $opt{p}    flag for lost IP address/DNS (if specified)

USAGE

    exit;
}

