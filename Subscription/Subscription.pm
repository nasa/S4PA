=head1 NAME

S4PA::Subscription - simple subscription fulfillment for online data

=head1 SYNOPSIS

use S4PA::Subscription;

S4PA::Subscription::fill_subscriptions($config_file, $pdr, $verbose);

=head1 DESCRIPTION

The S4PA::Subscription module satisfies basic subscriptions for online
data.  That is, it prepares a work order for the postoffice station to
request it to send email to users to let them know that their
data are available for download.  It has the flexibility for each
user to have an arbitrary "match" script check the subscription.  A filter
can be specified to be run by the postoffice on certain files (for example,
to extract ODL from the metadata), and an output format can be specified.

=head2 fill_subscriptions

This is the main driver routine, taking simply a configuration filename
with the subscriptions in it, a PDR with the files to be evaluated, and
a verbose flag.  The PDR may be a single file, but more often is a dummy
PDR made up of all the file groups of all PDRs accumulated in a day.  This
allows a single notification message to be sent for a full day's worth of 
granules.

=head1 FILES

The configuration file consists of Perl variables:

=over 4

=item %cfg_url_root

This is the URL root for data in S4PA. Valid keys are 'public' and 'restricted'.

For example, 
%cfg_url_root = (
    'public' => 'ftp://disc2.nascom.nasa.gov/data/s4pa/',
    'private' => 'http://disc2.nascom.nasa.gov/s4pa/',
);

=item %cfg_url_path

This is the pattern for forming a URL path.  It is based on strftime-style
date/time formats, with extensions for other elements like data type,
version and filename.  The currently supported patterns are:

         %Y:   four-digit year
         %m:   two-digit month (01-12)
         %d:   two-digit day of month (01-31)
         %j:   three-digit day of year (01-366)
         %=T:  data type (dataset name)
         %=V:  three-digit data version (001-999)
         %=F:  filename

For example, a Data Pool url for a MOD02SSH granule would look like this:

    %=T.%=V/%Y.%m.%d/%=F

=item %cfg_data_access

This is a two level hash indicating the data access type, 'public' or 
'restricted', of a given dataset and version. First level key is the dataset
name and the second level key is the data version label.

For example,

%cfg_data_access = (
   TRMM_3A25 => {
       '5' => 'public',
       '6' => 'restricted'
   }
);

=item %cfg_subscriptions

This is a complex hash, keyed on subscription ID. The value is an anonymous hash
with "notify", "label", "destination" and dataset/version keys. The dataset
(datatype) must be an exact match, but the version can be a regular expression.
These dataype/version keys also reference anonymous hashes with validator and
filter keys. The validator array is a sequence of programs to be run which, if
successful, will send the subscription out.  The programs are provided with the
list of files (metadata last) and can do coverage checks or the like.  The
program 'true' should be run if the send is unconditional.  A single
subscription key can select any number of dataset/version keys and all will be
reported in the same notification message.  Conversely separate subscriptions
for the same destination can be used to segregate notices for different
datasets.

The filter key references an anonymous hash whose keys are are patterns which,
if they match the URL, will have their corresponding values passed to the post
office to filter the outgoing file.  These can be used to, for example, extract
ODL from metadata files.  The value is the program to be run to do the
filtering; it must print the name of the results file to stdout. The filtering
is current done as a part of the push processing.  The current design ignores
it for ftp pull.  The data notificiations do not give useful URL information 
for filtered files.

The notify key references a hash with an address key providing a URL (sendto for
mail, but can also be ftp for anonymous push) and a format string
(=S4PA|LEGACY|PDR to specify original S4PA format, ECS DN notification format,
or PDR format repectively). The label key provides a user-defined string to be
passed for identifying the subscription. The optional destination key gives a
URL to which the corresponding files will be pushed by ftp or sftp or mailto
(and whose hostname part will be used to name the work order).

For example:

%cfg_subscriptions = (
    "FTP-Pull-S4PA" => {
        urlRoot {
	    public => 'ftp://disc2.nascom.nasa.gov/data/s4pa/',
	    },
        notify => {
            address => 'mailto: hegde@daac.gsfc.nasa.gov',
            format => 'S4PA'
            },
        label => "FTP-Pull-S4PA",
        "MOD021KM..*" => {
            validator => [ 'true' ]
            }
        },
    "FTP-Pull-ECS" => {
        urlRoot => {
	    restricted => 'http://disc2.nascom.nasa.gov/data/s4pa/',
	    },
        notify => {
            address => 'mailto: hegde@daac.gsfc.nasa.gov',
            format => 'LEGACY'
            },
        label => "FTP-Pull-ECS",
        "MOD021KM..*" => {
            validator => [ 'true' ]
            }
        },
     "S4PA-Pull-PDR" => {
        notify => {
            address => 'mailto: hegde@daac.gsfc.nasa.gov',
            format => 'PDR'
            },
        label => "S4PA-Pull-PDR",
        "MOD021KM..*" => {
            validator => [ 'true' ]
            }
        },
    "FTP-Push" => {
        urlRoot => {
		public => 'ftp://disc2.nascom.nasa.gov/data/s4pa/',
		},
        notify => {
            address => 'mailto: hegde@daac.gsfc.nasa.gov',
            format => 'LEGACY'
            },
        destination => "ftp:discette.gsfc.nasa.gov/private/s4pa/push",
        label => "FTP-Push",
        "MOD021KM..*" => {
            validator => [ 'true' ]
            }
        },
    "SFTP" => {
        urlRoot => {
	    public => 'ftp://disc2.nascom.nasa.gov/data/s4pa/',
	    restricted => 'http://disc2.nascom.nasa.gov/s4pa/',
	    },
        notify => {
            address => 'ftp:discette.gsfc.nasa.gov/private/s4pa/push',
            format => 'LEGACY'
            },
        destination => 'sftp:reason.gsfc.nasa.gov/var/tmp/hegde',
        label => "SFTP",
        "MOD021KM..*" => {
            validator => [ 'true' ],
            filter => {
                'xml' => 's4pa_extract_ODL.pl -o /var/tmp/'
                }
            }
        }
);

The output work order will be DO.<hostname>.<id>.wo where <hostname> is
extracted from the destination or, if none, then is just EMAIL (where files are
not pushed and only a notice it sent), and <id> is P followed by the
subscription id (key above) followed by T followed by a date-time stamp.  Note
again that files can be pushed by mailto and <hostname> will be extracted from
the mail address, even though this is only practical for small files.

The output work order looks like:

<FilePacket status="I"
    		label="userlabel"
    		notify="mailto: xx@yy.zz.com"
    		messageFormat="S4PA|LEGACY|PDR"
                track="yes|no"
    		[destination="[s]ftp:yy.zz.com/dir|mailto:xx2@aa.bb.edu"] >
    <FileGroup>
    	<File status="I"
    		url="ftp://xxdisc.gsfc.nasa.gov/data/yyyy/zzzz/2006/365/filename"
    		localPath="/ftp/.provider/nnn/zzzz/filename"
    		[filter="filter-program"]
                [cksumtype="CKSUM|MD5"]
                [cksumvalue="nnn"]
    		[cleanup="N"] />
    </FileGroup>
</FilePacket>

=back

=head1 CHANGELOG

06/09/25 J Pan     Added checksum in output wo for (s)ftp protocol in fill_workorder()

=head1 AUTHOR

Guang-Dih Lei
M. Hegde

=cut

# $Id: Subscription.pm,v 1.62 2011/05/24 16:18:42 glei Exp $
# -@@@ S4PA, Version $Name:  $

package S4PA::Subscription;
use File::Basename;
use File::Temp qw(tempfile);
use S4P;
use S4PA::Metadata;
use S4P::TimeTools;
use S4PA::Storage;
use Time::Local;
use strict;
use Safe;
use XML::LibXML;
use vars qw($VERSION);
$VERSION = '0.01';
1;

#=====================================================================
sub expand_url {
    my ($data_url_root, $metadata_url_root, $data_type, $data_version,
        $met_file, @science_files) = @_;
    # For data and metadata root (pattern), replace %=T and %=V with type and 
    # version,
    # %=Y, %=m, %=d, and %=j with 4-digit year, 2-digit month and day, and 
    # 3-digit julian day,
    # and %=F with base filename of each science and metadata file.  Return 
    # single list of completed
    # URLs with metadata last.

    # Next two items are calculated, but never used.
    my $esdt = $data_type;
    my $vvv = sprintf("%03d", $data_version);

    # Get datetime from .met (or .xml) file
    my $date;
    my $beginTimeStamp;
    if ($met_file) {
        unless (-f $met_file) {
            S4P::logger('ERROR', "$met_file does not exist");
            return;
        }
        my $met =S4PA::Metadata->new( FILE => $met_file );
        if ( $met->onError() ) {
            S4P::logger( "ERROR", 
                "Error reading $met_file:" . $met->errorMessage() );
        } else {
            my $beginDate = $met->getValue( 
                '/S4PAGranuleMetaDataFile/RangeDateTime/RangeBeginningDate' );
            my $beginTime = $met->getValue( 
                '/S4PAGranuleMetaDataFile/RangeDateTime/RangeBeginningTime' );
            $date = $beginDate . "T" . $beginTime . "Z";
            my ( $year, $month, $day, $hour, $min, $sec, $error ) = 
                S4P::TimeTools::CCSDSa_DateParse( $date );
            $beginTimeStamp = timegm($sec, $min, $hour, $day, $month-1, $year);
        }
    }
    return () unless defined $date;
    my ($yyyy, $mm, $dd) = ($date =~ /^(\d\d\d\d)-(\d\d)-(\d\d)/);
    return () unless ( defined $yyyy && defined $mm & defined $dd );
    my $doy = sprintf("%03d", S4P::TimeTools::day_of_year($yyyy, $mm, $dd));

    # Got all of the components: now plug 'em in
    my @urls;
    $data_url_root =~ s/%=T/$data_type/;
    $data_url_root =~ s/%=V/$data_version/;

    # Date follows strftime conventions
    $data_url_root =~ s/%Y/$yyyy/;
    $data_url_root =~ s/%m/$mm/;
    $data_url_root =~ s/%d/$dd/;
    $data_url_root =~ s/%j/$doy/;

    # Loop through all data files in granule
    foreach my $science_file(@science_files) {
        my $url = $data_url_root;
        my $file = basename($science_file);
        $url =~ s/%=F/$file/;
        push @urls, $url;
    }

    # do not include metadata file for service
    return ( $beginTimeStamp, @urls )
        unless ( defined $metadata_url_root );
    # Put metadata_file on last
    $metadata_url_root =~ s/%=T/$data_type/;
    $metadata_url_root =~ s/%=V/$data_version/;
    $metadata_url_root =~ s/%Y/$yyyy/;
    $metadata_url_root =~ s/%m/$mm/;
    $metadata_url_root =~ s/%d/$dd/;
    $metadata_url_root =~ s/%j/$doy/;
    $met_file = basename($met_file);
    $metadata_url_root =~ s/%=F/$met_file/;
    push @urls, $metadata_url_root;

    return ( $beginTimeStamp, @urls );
};

#=====================================================================
sub fill_subscriptions {
    my ($cfg_file, $pdr, $verbose) = @_;

    # Read config file
    my ($rh_cfg_url_path, $rh_cfg_subscriptions, $rh_cfg_url_root, 
	$rh_cfg_data_access) =
        read_config_file($cfg_file) or return 0;

    # Look for subscription matches
    my $match = match_subscriptions($pdr, $rh_cfg_url_root, $rh_cfg_url_path,
	$rh_cfg_subscriptions, $rh_cfg_data_access)
        or return 0;
    # $match is list of hashed on subscription id; each entry is reference to 
    # list of hash references,
    # each with url list of files, local path list of files, and filter hash 
    # from configuration file.

    if ( defined $pdr->{'subscription_id'} ) {
    	foreach my $key ( keys %$match ) {
	    delete $match->{$key} unless ( $key eq $pdr->{'subscription_id'} );
	}
    }
    # Fill out work order for DataPusher
    my @work_orders = fill_workorder($match, $rh_cfg_subscriptions, $verbose);

    # Clean up all work orders if number of match is greater than 
    # the number of work orders.  Should not occur.
    if ( scalar(keys %{$match}) > scalar(@work_orders) ) {
        foreach my $order (@work_orders) {
            unlink( $order ) || S4P::perish( 2, "Failed to remove $order" );
        }
    }
    return 1;
}

#=====================================================================
sub fill_workorder {
    my ($rh_match, $rh_subscriptions, $verbose) = @_;

    # Return list of work order file names.  One work order per subscription 
    # id.  XML root is FilePacket, with status (=I), label, notify, messageFormat,
    # and optional destination attributes, and FileGroup child. FileGroup has 
    # File children with status (=I), url, and localPath attributes and optional 
    # filter and cleanup (=N) attributes.  The filter is present only where 
    # present in $rh_match and where some one of its keys matches the URL.

    my @orders = ();
    # key is subscription ID
    foreach my $wo (keys %$rh_match) {

        my $wo_type;
        my $destination;
        my $notify = $rh_subscriptions->{$wo}{"notify"}{"address"};
        my $msg_format = $rh_subscriptions->{$wo}{"notify"}{"format"};
        my $msg_filter = $rh_subscriptions->{$wo}{"notify"}{"filter"};
        my $notice_suffix = $rh_subscriptions->{$wo}{"notify"}{"suffix"};
        my $notice_subject = $rh_subscriptions->{$wo}{"notify"}{"subject"};
        my $label = $rh_subscriptions->{$wo}{"label"};
	my $maxGranCount = $rh_subscriptions->{$wo}{"max_granule_count"};
        my $verifyFlag = $rh_subscriptions->{$wo}{"verify"};

        # Create DO.<hostname> work order if destination was defined
        # (with <hostname> pulled from URL and "." replaced by "_");
        # create DO.EMAIL work order if not.
        if (defined $rh_subscriptions->{$wo}{"destination"}) {
            $destination = $rh_subscriptions->{$wo}{"destination"};
            my ($protocol, $hostid) = split ":", $destination;
            if ( $protocol =~ "mailto" ) {
                #$hostid =~ m/(.+)@(.+)/;
                ($wo_type = $hostid) =~ s/\.|\@/_/g;
            } else {
                $hostid =~ m#(.+?)/#;
                ($wo_type = $1) =~ s/\./_/g;
            }
        } else {
            $wo_type = "EMAIL";
        }

        # work order file name
        #   for sftp and ftp: DO.<hostname>.<id>.wo
        #   for mailto: DO.EMAIL.<id>.wo
        #   where <id> = P<subscription-id>T<time>
        my @date = localtime(time);
        my $wo_file = sprintf("%s.P%sT%04d%02d%02d%02d%02d%02d",
           $wo_type, $wo, $date[5]+1900, $date[4]+1, $date[3], $date[2], 
	   $date[1], $date[0]);
        my $woCount = 0;

        # how many work order based on max_granule_count
        my $totalGroups = scalar( keys %{$rh_match->{$wo}} );
        my $totalWO = ( defined $maxGranCount ) ?
            int((( $totalGroups - 1 ) / $maxGranCount ) + 1 ) : 1;
        my $subsetLength = length( $totalWO );

        my $xmlParser = XML::LibXML->new();
        my $dom = $xmlParser->parse_string('<FilePacket/>');
        my $doc = $dom->documentElement();

        my ($filePacketNode) = $doc->findnodes('/FilePacket');
        $filePacketNode->setAttribute('status', "I");
        $filePacketNode->setAttribute('notify', $notify);
        $filePacketNode->setAttribute('id', $wo );
        $filePacketNode->setAttribute('label', $label);
        $filePacketNode->setAttribute('messageFormat', $msg_format);
        $filePacketNode->setAttribute('messageFilter', $msg_filter )
            if defined $msg_filter;
        $filePacketNode->setAttribute('noticeSuffix', $notice_suffix )
            if defined $notice_suffix;
        $filePacketNode->setAttribute('noticeSubject', $notice_subject )
            if defined $notice_subject;
        $filePacketNode->setAttribute('max_granule_count', $maxGranCount )
            if defined $maxGranCount;

        # track pull subscription on email sent.
	$filePacketNode->setAttribute('track', 'yes');
        
        # Set the flag for verification
        $verifyFlag = ( $verifyFlag eq 'true' ) ? 'yes' : 'no';
        $filePacketNode->setAttribute('verify', $verifyFlag);

        my $pullDir = '';
        if ($wo_type ne "EMAIL") {
            # for type 'pull' subscription with destination specified
            # we assumed this is a conversion of push to pull type
            # a random hidden directory name will be used to append
            # under the destination directory for each work order
            if ( $rh_subscriptions->{$wo}{"type"} eq 'pull' ) {
                my $template = 'XXXXXXXXXX';
                my (undef, $pushDir) = File::Temp::tempfile($template, OPEN => 0);
                $destination =~ s/\/$//;
                $destination .= "/$pushDir";
                
                # get the URL_ROOT for DN to convert the destination
                # to the relative path on ftp url
                unless ( defined $rh_subscriptions->{$wo}{"urlRoot"}{"pickup"} ) {
	            S4P::perish(3, "pickup urlRoot not defined in configuration" .
                        ", please specify URL_ROOT in subscription descriptor" .
                        " for ID: $wo");
                }
                $pullDir = $rh_subscriptions->{$wo}{"urlRoot"}{"pickup"};
                $pullDir =~ s/\/$//;
                $pullDir .= "/$pushDir";
                $filePacketNode->setAttribute('pullPath', $pullDir);
            }
            $filePacketNode->setAttribute('destination', $destination);
        }

        my $granuleCount = 0;
        # Next index is file group number sorted by the beginTime
        foreach my $groupIndex ( sort { $rh_match->{$wo}{$a}{time}
            <=> $rh_match->{$wo}{$b}{time} } keys %{$rh_match->{$wo}} ) {
            my $groupInfo = $rh_match->{$wo}{$groupIndex};
            my $filegroupNode = XML::LibXML::Element->new('FileGroup');
            $filegroupNode->setAttribute( 'dataset', $groupInfo->{dataset} );
            $filegroupNode->setAttribute( 'version', $groupInfo->{version} );
	    next unless (exists $groupInfo->{file}{url}) ;
            for ( my $index=0 ; $index<@{$groupInfo->{file}{url}} ; $index++ ) {
                my $url = $groupInfo->{file}{url}[$index];
                my $localPath = $groupInfo->{file}{path}[$index];
                my $fileNode = XML::LibXML::Element->new('File');

                # assign fileNode attribute
                $fileNode->setAttribute('url', $url);
                $fileNode->setAttribute('localPath', $localPath);

                # check for filter pattern
                if ( defined $groupInfo->{file}{filter} ) {
                    foreach my $filter (keys %{$groupInfo->{file}{filter}} ) {
                        next unless ( $url =~ m/$filter/ );
                        my $action = $groupInfo->{file}{filter}{$filter};
                        $fileNode->setAttribute('filter', $action);
                        last;
                    }
                }
                $fileNode->setAttribute('status', "I");
                
                # VERIFY-----
                if ($wo_type ne "EMAIL") {
                    $fileNode->setAttribute('cleanup', "N");
                }
                #----END VERIFY---

                # Add checksum for ftp orders
                if ( defined $groupInfo->{file}{checksum}{$localPath}{type} &&
		    defined $groupInfo->{file}{checksum}{$localPath}{value} ) {
                    $fileNode->setAttribute('cksumtype', 
			$groupInfo->{file}{checksum}{$localPath}{type} );
                    $fileNode->setAttribute('cksumvalue', 
			$groupInfo->{file}{checksum}{$localPath}{value} );
		}
                $filegroupNode->appendChild($fileNode);
            }
            $filePacketNode->appendChild($filegroupNode);

            # fill a work order when granule count hit MAX_GRANULE_COUNT
            $granuleCount++;
            if ( (defined $maxGranCount) && ($granuleCount == $maxGranCount) ) {
                $woCount++;
                # work order file name
                #   for sftp and ftp: DO.<hostname>.<id>.wo
                #   for mailto: DO.EMAIL.<id>.wo
                #   where <id> = P<subscription-id>T<time>S<order-sequence>
                my $woFile;
                if ( $totalWO == 1 ) {
                   $woFile = $wo_file . ".wo";
                } else {
                   $woFile = $wo_file . sprintf("S%0${subsetLength}d", $woCount) .
                       ".wo";
                }
                open (WO, ">$woFile") 
	            || S4P::perish(2, "Failed to open workorder file $woFile: $!");
                print WO $dom->toString(1);
                close WO;
                push @orders, $woFile;

                # remove processed fileGroup and reset granuleCount
                $filePacketNode->removeChildNodes();
                $granuleCount = 0;
            }
        }

        # write out last batch of fileGroup if any.
        if ( $granuleCount > 0 ) {
            my $woFile;

            # write out to a new segment work order file name
            # if this is the last one of the sequence
            if ( $woCount > 0 ) {
                $woCount++;
                $woFile = $wo_file . sprintf("S%0${subsetLength}d", $woCount) .
                    ".wo";
            # write out to the original work order 
            # if this is the only one.
            } else {
                $woFile = $wo_file . ".wo";
            }

            open (WO, ">$woFile") 
	        || S4P::perish(2, "Failed to open workorder file $woFile: $!");
            print WO $dom->toString(1);
            close WO;
            push @orders, $wo_file;
        }
    }
    return @orders;
}

#=====================================================================
sub match_subscriptions {
    my ($pdr, $rh_url_root, $rh_url_path, $rh_subscriptions, 
        $rh_data_access, $verbose) = @_;

    # Get FILE_GROUPS from PDR
    my @file_groups = @{$pdr->file_groups};
    unless (@file_groups) {
        S4P::logger('ERROR', "No FILE_GROUPS in PDR");
        return;
    }

    # Foreach granule (FILE_GROUP), check for matches
    my $match = {};
    my $groupIndex = 0;
    foreach my $file_group(@file_groups) {
        # Extract data_type and data_version of this file group.
        my $data_type = $file_group->data_type;
        my $data_version = $file_group->data_version;

        # Get $url_pattern string of first match for datatype/version; get 
        # subscription
        # hash to validators and filters for all subscription ids that match 
        # datatype/version.
        my ( $url_hash, $subscription ) =
            get_subscription_info( $data_type, $data_version,
                                   $rh_url_root,
                                   $rh_url_path,
                                   $rh_subscriptions,
                                   $rh_data_access );
        next unless $url_hash;
        next unless $subscription;

        # Get data files and metadata file that someone subscribed to.
        my @science_files;
        my $met_file;
        my $browse_file;
        my $map_file;
        my $checksum;
        foreach my $file_spec ( @{$file_group->file_specs()} ) {
            if ( $file_spec->file_type eq 'METADATA' ) {
                $met_file = $file_spec->pathname();
            } elsif ( $file_spec->file_type eq 'BROWSE' ) {
                $browse_file = $file_spec->pathname();
            } elsif ( $file_spec->file_type eq 'HDF4MAP' ) {
                $map_file = $file_spec->pathname();
            } else {
                my $file = $file_spec->pathname();
                push( @science_files, $file );
                if ( defined $file_spec->{file_cksum_type}
                    && defined $file_spec->{file_cksum_value} ) {
                    $checksum->{$file}{type} = $file_spec->{file_cksum_type};
                    $checksum->{$file}{value} = $file_spec->{file_cksum_value};
                }
            }
        }
        foreach my $subscriber ( keys %$subscription ) {
        # For subscriber specific PDRs, neglect validators.
        $subscription->{$subscriber}{"validator"} = [ 'true' ]
        if ( defined $pdr->{subscription_id} 
            && $pdr->{subscription_id} eq $subscriber );
            foreach my $exec ( @{$subscription->{$subscriber}{"validator"}} ) {
                # Execute each validator passing all science plus metadata file.
                # This allows each subscription to have a list of programs that 
                # can test to see if they want the data.
                # This looks a little fishy: each successful validator pushes 
                # another identical entry onto $match.
                # no validator for browse file or map file.
                my $cmd = join( ' ', $exec, @science_files, $met_file );
                S4P::logger('INFO', "Subscription script call: $cmd") 
                    if $verbose;
                my ($errstr, $rc) = S4P::exec_system($cmd);
                if ($rc == 0) {
                    my $file_info = ();
                    # That is, $file_info = undef.
                    # Transfer filter criteria (hash) to $file_info to be 
                    # pushed onto $match.
                    if ( defined $subscription->{$subscriber}{"filter"} ) {
                        $file_info->{"filter"} = 
                            $subscription->{$subscriber}{"filter"};
                    } elsif ($verbose) {
                        S4P::logger('INFO', 
                            "No filter for $subscriber / $science_files[0]");
                    }

                    my @urls = ();
                    my @localPath = ();
                    my $beginEpoch;

                    if ( defined $subscription->{$subscriber}{"service"} ) {
                        eval { require HTTP_service_URL};
                        if ($@) {
                            S4P::perish( 2, "Failed to load module HTTP_service_URL ($@)" );
                        }
                        # currently service does not support browse file
                        # nor metadata file
                        push( @localPath, @science_files );
                        my $service = $subscription->{$subscriber}{"service"};

                        # expand localPath to service URL
                        my @dataUrls;
                        ( $beginEpoch, @dataUrls ) = expand_url(
                            $url_hash->{$subscriber}, undef, $data_type,
                            $data_version, $met_file, @localPath);

                        @urls = HTTP_service_URL::make_service_urls( \@dataUrls, $service );

                    } else {
                        push( @localPath, @science_files );
                        # include browse file, it is only needed for replication
                        # subscription, not for general user subscription.
                        if ( $subscription->{$subscriber}{"include_browse"} ) {
                            push( @localPath, $browse_file ) if ( defined $browse_file );
                        }

                        # include map file, it is only needed for replication
                        # subscription, not for general user subscription.
                        if ( $subscription->{$subscriber}{"include_map"} ) {
                            push( @localPath, $map_file ) if ( defined $map_file );
                        }

                        # expand local path to url
                        ( $beginEpoch, @urls ) = expand_url($url_hash->{$subscriber}, 
                            $url_hash->{$subscriber}, $data_type,
                            $data_version, $met_file, @localPath);

                        # adding meta file at the end
                        push ( @localPath, $met_file );
                    }

                    $file_info->{"url"} = [@urls];
                    $file_info->{"path"} = [@localPath];
                    $file_info->{"checksum"} = $checksum;
                    # Push onto $match list for this $subscriber id a reference 
                    # to the url/path/filter hash for the current file group 
                    # and validator.
                    $match->{$subscriber}{$groupIndex}{file} = $file_info;
                    $match->{$subscriber}{$groupIndex}{time} = $beginEpoch;
                    last;
                } elsif ($rc > 255) {
                    S4P::logger('ERROR',
			"Failed to execute match command $cmd");
                    return;
                } elsif ($verbose) {
                    S4P::logger('INFO',
			"No match for $subscriber / $science_files[0]");
                }
            }
            # Save dataset/version info only if the subscription has any file
            if ( defined $match->{$subscriber} 
	        && defined $match->{$subscriber}{$groupIndex}{file} ) {
                $match->{$subscriber}{$groupIndex}{dataset} = $data_type;
                $match->{$subscriber}{$groupIndex}{version} = $data_version;
            }
        }
        $groupIndex++;
    }

    return $match;
}

#=====================================================================
sub read_config_file {
    my ($cfg_file) = shift;

    # Setup compartment and read config file
    my $cpt = new Safe('CFG');
    $cpt->share('%cfg_url_root', '%cfg_url_path', '%cfg_subscriptions', 
        '%cfg_data_access');

    # Read config file
    if (! $cpt->rdo($cfg_file)) {
        S4P::logger('ERROR', "Cannot read config file $cfg_file");
    }
    # Check for required variables
    elsif (! %CFG::cfg_url_path) {
        S4P::logger('ERROR', "No \%cfg_url_path in $cfg_file");
    }
    elsif (! %CFG::cfg_subscriptions) {
        S4P::logger('ERROR', "No \%cfg_subscriptions in $cfg_file");
    }
    else {
        return (\%CFG::cfg_url_path, \%CFG::cfg_subscriptions,
	    \%CFG::cfg_url_root, \%CFG::cfg_data_access);
    }
    return;    # If we got here, there must have been an error above
}

#=====================================================================
sub get_subscription_info
{
    my ( $data_type, $data_version, $rh_url_root, $rh_url_path, $rh_subscriptions, 
        $rh_data_access ) = @_;
    # Return url pattern from first matching $data_type.$data_version out of 
    # $rh_url and
    # hash of subscriptions (keyed by subscription ids) to hash of refs to 
    # validator arrays
    # and filter hashes.  Match must be exact on $data_type, but $rh_url and 
    # $rh_subscriptions
    # can use regular expressions to match the $data_version.

    my ( $url_pattern, $subscription, $url_hash );
    if ( defined $rh_url_path->{$data_type}{$data_version} ) {
        # There is an exact match for type.version, e.g., "MOD021KM.004"; get 
        # URL pattern.
        $url_pattern = $rh_url_path->{$data_type}{$data_version};
    } else {
        # Find (alphabetically) first key where type exactly matches and 
        # regular expression in version matches $data_version: 
        # e.g. "MOD021KM..+"
        # Actually this code could have handled exact match, too.
        foreach my $key ( sort keys %{$rh_url_path->{$data_type}} ) {
            next unless ( $data_version =~ m{$key} );
            $url_pattern = $rh_url_path->{$data_type}{$key};
            last;
        }
    }

    my $service;
    foreach my $sub_id (keys %$rh_subscriptions) {
        foreach my $key ( sort keys %{$rh_subscriptions->{$sub_id}} ) {
            # Skip keys other than first dataset in each $sub_id matching 
            # $data_type.$data_version.
            next if ( $key =~ /destination|notify|label|urlRoot/);
            next unless ( $key =~ /^$data_type\.(.+)/ );
            my $version = qr($1);
            next unless ( $data_version =~ m{$version} );

            my $subInfo = $rh_subscriptions->{$sub_id};
            # Add to subscription hash for this $sub_id: hash of refs to 
            # validator array and filter hash.
            my $validator = $subInfo->{$key}{"validator"};
            my $filter = $subInfo->{$key}{"filter"};
            $subscription->{$sub_id} = {
                "validator"   => $validator,
                "filter"      => $filter
            };

            # check if service is configure for this dataset
            if ( defined $subInfo->{$key}{"service"} ) {
                $subscription->{$sub_id}{"service"} = $subInfo->{$key}{"service"};
            }

            if ( defined $subInfo->{"include_browse"} ) {
                $subscription->{$sub_id}{"include_browse"} = $subInfo->{"include_browse"};
            }

            if ( defined $subInfo->{"include_map"} ) {
                $subscription->{$sub_id}{"include_map"} = $subInfo->{"include_map"};
            }

	    my $accessType;
	    if ( defined $rh_data_access->{$data_type}{$data_version} ) {
		$accessType = $rh_data_access->{$data_type}{$data_version};
	    } elsif ( defined $rh_data_access->{$data_type}{''} ) {
	        $accessType = $rh_data_access->{$data_type}{''};
	    }
	    my $urlRoot;
            if ( defined $accessType ) {
		$urlRoot = ( defined $subInfo->{urlRoot}{$accessType} )
		    ? $subInfo->{urlRoot}{$accessType}
		    : $rh_url_root->{$accessType};
                $urlRoot .= '/' unless ( $urlRoot =~ /\/$/ );
                $url_hash->{$sub_id} = $urlRoot . $url_pattern;
	    }
        }
    }
    return ( $url_hash, $subscription );
}
