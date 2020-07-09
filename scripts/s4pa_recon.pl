#!/usr/bin/perl

=head1 NAME

s4pa_recon.pl - Reconcile S4PA instance holdings with those of a metadata partner

=head1 PROJECT

GES DISC

=head1 SYNOPSIS

s4pa_recon.pl
B<-p> I<provider>
B<-s> I<shortName>
B<-v> I<versionId>
B<-r> I<s4pa_root_directory>
[B<-t> I<large_granule_count_threshold>]
B<-H> I<ftp_push_host>
B<-D> I<ftp_push_directory>
[B<-U> I<ftp_push_user>]
[B<-P> I<ftp_push_pwd>]
[B<-q> I<ftp_push_quiescent_time>]
[B<-C> I<ftp_server_chroot> | B<-T> I<local_temp_directory_for_pushed_files>]
B<-E> I<partner_service_endpoint_uri>
B<-i> I<instance_name>
B<-S> I<partner_granule_deletion_xml_staging_dir>
B<-h> I<partner_granule_publication_ftp_host>
B<-f> I<partner_granule_delete_publication_ftp_directory>
[B<-a> I<ignore_file_list>]
[B<-u> I<partner_service_username> B<-e> I<partner_service_encrypted_password>]
[B<-I> I<partner_service_prodier_ID>]
[B<-l> I<local_host_name_override>]
[B<-w> I<ftp_pull_timeout>]
[B<-d>]


=head1 DESCRIPTION

I<s4pa_recon.pl> performs reconciliation for a dataset in an S4PA instance,
comparing the S4PA holdings with the holdings of a metadata partner. When
granules in the S4PA archive are found to be missing from the partner's
holdings, a PDR is generated written to the pending_publish directory
of the S4PA station that published granules to the partner. When granules
in the partner's holdings are not found in S4PA, a metadata file for
deleting the partner's granules is written to a staging directory, and a
work order is created in the reconciliation station directory to push
that metadata file to the partner's granule metadtaa publication ftp
directory.

A report indicates the number of
matches, the number of granules the partner is missing, the number of
granules the partner has that are not present in the S4PA instance (according
to the granule.db), and the percentage of those in the granule.db
that are in the partner's holdings.

=head1 OPTIONS

=head1 AUTHOR

Edward Seiler

=head1 ADDRESS

ADNET, Code 610.2, Greenbelt, MD  20771

=head1 CREATED

02/21/2007

=head1 MODIFIED

11/30/2007

=cut

################################################################################
# $Id: s4pa_recon.pl,v 1.57 2020/06/23 12:34:28 s4pa Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use Getopt::Std;
use Safe;
use S4P;
use S4P::PDR;
use S4P::TimeTools;
use S4PA;
use S4PA::Reconciliation;
use S4PA::Storage;
use S4PA::Metadata;
use File::Basename;
use File::Copy;
use Time::Local;
use Clavis;
use LWP::UserAgent;
use Cwd;

use vars qw( $opt_p $opt_s $opt_v $opt_r $opt_t $opt_H $opt_U $opt_P $opt_D
             $opt_q $opt_C $opt_T $opt_E $opt_z $opt_i $opt_S $opt_h $opt_f
             $opt_u $opt_e $opt_I $opt_l $opt_d $opt_m $opt_a $opt_n $opt_w
             $opt_c $opt_L);

$| = 1;

getopts( 'p:s:v:r:t:H:U:P:D:q:C:T:E:z:i:S:h:f:u:e:I:l:a:dm:n:w:c:L:' );

my $config_file = $opt_c;
my $cpt = new Safe 'CFG';
if ( defined $config_file ) {
    $cpt->rdo( $config_file )
        || S4P::logger( 'ERROR', "Failed to read configuration file, $config_file" );
}

# Command line switches override the configuration value in configure file
my $partner = ( defined $opt_p ) ? $opt_p : $CFG::cfg_partner;
my $s4pa_root = ( defined $opt_r ) ? $opt_r : $CFG::cfg_s4pa_root;
my $large_count_threshold = ( defined $opt_t ) ? $opt_t :
    $CFG::cfg_large_count_threshold;
my $ftp_push_host = ( defined $opt_H ) ? $opt_H : $CFG::cfg_ftp_push_host;
my $ftp_push_user = ( defined $opt_U ) ? $opt_U : $CFG::cfg_ftp_push_user;
my $ftp_push_pwd = ( defined $opt_P ) ? $opt_P : $CFG::cfg_ftp_push_pwd;
my $ftp_push_dir = ( defined $opt_D ) ? $opt_D : $CFG::cfg_ftp_push_dir;
my $ftp_push_quiescent_time = ( defined $opt_q ) ? $opt_q :
    $CFG::cfg_ftp_push_quiescent_time;
my $ftp_server_chroot = ( defined $opt_C ) ? $opt_C :
    $CFG::cfg_ftp_server_chroot;
my $temp_dir = ( defined $opt_T ) ? $opt_T : $CFG::cfg_temp_dir;
my $service_endpoint_uri = ( defined $opt_E ) ? $opt_E :
    $CFG::cfg_service_endpoint_uri;
my $catalog_endpoint_uri = ( defined $opt_L ) ? $opt_L :
    $CFG::cfg_catalog_endpoint_uri;
my $service_timeout = ( defined $opt_z ) ?  $opt_z :
    $CFG::cfg_service_timeout;
my $s4pa_instance_name = ( defined $opt_i ) ? $opt_i :
    $CFG::cfg_s4pa_instance_name;
my $deletion_xml_staging_dir = ( defined $opt_S ) ? $opt_S :
    $CFG::cfg_deletion_xml_staging_dir;
my $partner_ftp_pub_host = ( defined $opt_h ) ? $opt_h :
    $CFG::cfg_partner_ftp_pub_host;
my $partner_ftp_del_pub_dir = ( defined $opt_f ) ? $opt_f :
    $CFG::cfg_partner_ftp_del_pub_dir;
my $partner_service_username = ( defined $opt_u ) ? $opt_u :
    $CFG::cfg_partner_service_username;
my $partner_service_encrypted_pwd = ( defined $opt_e ) ? $opt_e :
    $CFG::cfg_partner_service_encrypted_pwd;
my $partner_service_provider_id = ( defined $opt_I ) ? $opt_I :
    $CFG::cfg_partner_service_provider_id;
my $local_hostname_override = ( defined $opt_l ) ? $opt_l :
    $CFG::cfg_local_hostname_override;
my $ignore_file_list = ( defined $opt_a ) ? $opt_a : $CFG::cfg_ignore_file_list;
my $debug = ( defined $opt_d ) ? $opt_d : $CFG::cfg_debug;
my $ftp_pull_timeout = ( defined $opt_w ) ? $opt_w : $CFG::cfg_ftp_pull_timeout;
my $cmr_certfile = $CFG::CMR_CERTFILE;
my $cmr_passfile = $CFG::CMR_CERTPASS;
my $launchpad_uri = $CFG::LAUNCHPAD_URI;

# Minimum number of seconds that must elapse before a
# newly ingested granule is republished.
my $ingest_interval = ( defined $opt_n ) ? $opt_n :
    ( defined $CFG::cfg_ingest_interval ) ? $CFG::cfg_ingest_interval : 86400;

# Minimum number of seconds that must elapse between reconciliation runs
# for a collection.
my $minimum_interval = ( defined $opt_m ) ? $opt_m :
    ( defined $CFG::cfg_minumum_interval ) ? $CFG::cfg_minumum_interval : 86400;

# In order for us to make the job type more uniform for reservation,
# we need to figure out which dataset to go for next:
# For new dataset not in history file yet, go by alphabetical order
# For old dataset in history file, go by each dataset's last recon timestamp, oldest first.

# shortname and version will be provided either via <-s> and <-v> switch
# or we will need to figure out which dataset for recon next
my $stationDir = dirname( cwd() );
my $history_file = "../${partner}_reconciliation_history";
my $s4pa_shortName;
my $s4pa_versionId;
if ( defined $opt_s ) {
    $s4pa_shortName = $opt_s;
    $s4pa_versionId = $opt_v;
} else {
    ( $s4pa_shortName, $s4pa_versionId ) = next_dataset( $history_file,
        %CFG::cfg_dataset_list);
    S4P::logger( 'INFO', "$partner reconciliation on '$s4pa_shortName' " .
                 "version '$s4pa_versionId'." );
    unless ( defined $ignore_file_list ) {
        $ignore_file_list = "../ignore-list." . uc($partner) . "_$s4pa_shortName";
        if ( $s4pa_versionId ) {
            $ignore_file_list .= "_$s4pa_versionId" . ".txt";
        } else {
            $ignore_file_list .= ".txt";
        }
    }
}

my $usage =
    "Usage: " . basename($0) . "-p ECHO|CMR|Mirador|Dotchart|Giovanni " .
    "\t -s <shortName>\n" .
    "\t -v <versionId>\n" .
    "\t -r <s4pa_root_directory>\n" .
    "\t [-t <large_granule_count_threshold>]\n" .
    "\t -H <ftp_push_host|ftp_pull_host>\n" .
    "\t -D <ftp_push_dir|ftp_pull_dir>\n" .
    "\t [-U <ftp_push_user>]\n" .
    "\t [-P <ftp_push_pwd>]\n" .
    "\t [-q <ftp_push_quiescent_time>]\n" .
    "\t [-C <ftp_server_chroot> | -T <local_temp_directory_for_pushed_files>]\n" .
    "\t -E <partner_service_endpoint_uri>\n" .
    "\t [-z <partner_service_timeout>]\n" .
    "\t -i <s4pa_instance_name>\n" .
    "\t -S <partner_granule_deletion_xml_staging_dir>\n" .
    "\t -h <partner_granule_publication_ftp_host>\n" .
    "\t -f <partner_granule_delete_publication_ftp_directory>\n" .
    "\t [-u <partner_service_username> -e <partner_service_encrypted_password>]\n".
    "\t [-I <partner_service_provider_ID>\n" .
    "\t [-l I<local_host_name_override>]\n" .
    "\t [-a <ignore_list_filename>]\n" .
    "\t [-w <ftp_pull_timeout>]\n" .
    "\t [-d ]\n\n" .
    " -H, -U, -P, -D are used by partner to push granule information\n";

# Removed requirement for ftp_push options.
# We will be doing ftp_pull for dotchart and mirador reconciliation.
unless ( $partner && $s4pa_shortName && defined( $s4pa_versionId ) &&
         $s4pa_root && ($service_endpoint_uri or $launchpad_uri) && $s4pa_instance_name &&
         $deletion_xml_staging_dir ) {
    print STDERR $usage;
    exit 1;
}

if ( $partner ne 'CMR' ) {
    unless ( $partner_ftp_pub_host && $partner_ftp_del_pub_dir ) {
        print STDERR $usage;
        exit 1;
    }
}

# to support different push/pull protocl on the partner other than ftp
my $partner_protocol = (defined $CFG::cfg_partner_protocol) ?
    $CFG::cfg_partner_protocol : 'ftp';

S4P::perish( 1, "S4PA root directory $s4pa_root not found" )
    unless ( -d $s4pa_root );
if ( $temp_dir ) {
    S4P::perish( 1, "Directory $temp_dir not found" )
        unless ( -d $temp_dir );
}

# locate dif_info file and read collection configuration
my $collection_shortname;
my $collection_version;
my $dif_info_file = "$s4pa_root/other/housekeeper/s4pa_dif_info.cfg";

# some instances are not publishing to either Mirador nor CMR, there will be no 's4pa_dif_info.cfg'
# file created by deployment.
if ( -f $dif_info_file ) {
    my $dif_info_cpt = new Safe 'DIF_INFO_CFG';
    $dif_info_cpt->rdo( $dif_info_file ) or
        S4P::perish( 2, "Cannot read config file $dif_info_file in safe mode: $!\n" );
    $collection_shortname = $DIF_INFO_CFG::cmr_collection_id{$s4pa_shortName}{$s4pa_versionId}{'short_name'};
    $collection_version = $DIF_INFO_CFG::cmr_collection_id{$s4pa_shortName}{$s4pa_versionId}{'version_id'};
}

my ( $matches, $missing_s4pa, $missing_partner );
$matches = 0;
$missing_s4pa = 0;
$missing_partner = 0;
my $missing_message = '';

# Read history file and check if the minimum time between runs for
# a collection has elapsed. If not, do nothing and exit.
my $collection_id = "$s4pa_shortName:$s4pa_versionId";
my $history = read_history( $history_file, $collection_id );

my $last_run = '1995-01-01T00:00:00Z';
my $this_run = S4P::TimeTools::CCSDSa_Now();
if ( @$history ) {
    $last_run = shift @$history;
}
unless ( $debug ) {
    # Convert old epoch timestamp to CCSDS timestring
    if ( $last_run =~ /\d{10}/ ) {
        my ($sec, $min, $hour, $day, $month, $year) = gmtime($last_run);
        $last_run = S4P::TimeTools::CCSDSa_DateUnparse($year + 1900, $month + 1, $day, $hour, $min, $sec);
    }
    if ( S4P::TimeTools::CCSDSa_Diff( $last_run, $this_run ) < $minimum_interval ) {
        exit 0;
    }
}

# Read ignore file for a list of granules to be ignored for republishing
my $ignoreGranuleHashRef = {};
if ( defined $ignore_file_list && -s $ignore_file_list ) {
    open ( LIST, "<$ignore_file_list" ) or
        S4P::perish( 1, "Could not open file $ignore_file_list for reading: ($!)" );
    while ( <LIST> ) {
        chomp;
        my $fn = basename($_);
        $ignoreGranuleHashRef->{$fn} = 1;
    }
    close( LIST );
}

my $partner_service_pwd = Clavis::decrypt( $partner_service_encrypted_pwd )
    if $partner_service_encrypted_pwd;

# updated LWP::UserAgent under C7 change this default to 1 causing Dotchart recon to fail
# # so, explicit set it not to verify hostname for backward compatability.
$ENV{PERL_LWP_SSL_VERIFY_HOSTNAME} = 0;

my $recon = recon_object( $partner, $s4pa_root, $large_count_threshold,
                          $ftp_push_host, $ftp_push_user, $ftp_push_pwd,
                          $ftp_push_dir, $ftp_push_quiescent_time,
                          $ftp_server_chroot, $temp_dir, $catalog_endpoint_uri,
                          $service_endpoint_uri,
                          $service_timeout, $partner_service_username,
                          $partner_service_pwd, $partner_service_provider_id,
                          $local_hostname_override, $ftp_pull_timeout,
                          $partner_protocol, $cmr_certfile, $cmr_passfile,
                          $launchpad_uri);

my $ds_name_width = length( $s4pa_shortName ) + 6;

# Get list of S4PA granules for this dataset
my @s4pa_granules = get_s4pa_granule_list( $s4pa_root, $s4pa_shortName,
                                           $s4pa_versionId );
unless ( @s4pa_granules ) {
    $missing_message = "No granules found for shortName $s4pa_shortName" .
        ", version $s4pa_versionId";
    #S4P::logger( 'WARN', $missing_message );
    $history = [ $this_run, $matches, $missing_s4pa, $missing_partner ];
    save_history( $history_file, $collection_id, $history );
    S4P::raise_anomaly( "MISMATCH_$partner", $stationDir, 'WARN', $missing_message, 0 );
    exit 0;
}

unless ( $recon->login() ) {
    my $login_error = "Could not login to partner $partner: " .
                      $recon->getErrorMessage();
    #S4P::logger( 'ERROR', $login_error );
    S4P::raise_anomaly( "LOGIN_$partner", $stationDir, 'WARN', $login_error, 0 );
    # Don't save the history, so we don't have to wait to try again,
    # in case the partner login is disabled for only a short time.
    exit 0;
}

# Find a partner version id that matches the S4PA version id
my $partner_versionId;
if ( $partner eq 'CMR' ) {
    $partner_versionId = get_partner_version_id( $s4pa_instance_name,
                                                 $s4pa_root,
                                                 $collection_shortname,
                                                 $collection_version,
                                                 $recon );
} else {
    $partner_versionId = get_partner_version_id( $s4pa_instance_name,
                                                 $s4pa_root,
                                                 $s4pa_shortName,
                                                 $s4pa_versionId,
                                                 $recon );
}

if ( $debug ) {
    printf "%-${ds_name_width}s %s\n", 'DATASET', '  PERCENT        MATCHES       S4PALESS    PARTNERLESS';
}
# If there were no matches in the partner's holdings for
# this shortName/versionId combination, the dataset is
# missing from the partner's holdings
unless ( defined $partner_versionId ) {
    if ( $debug ) {
        $missing_partner = @s4pa_granules;
        printf "%-${ds_name_width}s  %s               Missing dataset %14d\n", "$s4pa_shortName v$s4pa_versionId",
               '  0.000%', $missing_partner;
    }
    $missing_message = "Partner $partner does not have collection for" .
        " shortName $s4pa_shortName, version $s4pa_versionId";
    #S4P::logger( 'WARN', $missing_message );
    $history = [ $this_run, $matches, $missing_s4pa, $missing_partner ];
    save_history( $history_file, $collection_id, $history );
    S4P::raise_anomaly( "MISMATCH_$partner", $stationDir, 'WARN', $missing_message, 0 );
    exit 0;
}

# If there were multiple versions return, check if any version
# match the dif configuration version. If it does, assume it is the partner version.
if ( $partner_versionId =~ /\,/ ) {
    my $match = 0;
    my @versions = split(',', $partner_versionId);
    foreach my $version ( @versions ) {
        if ( $version eq $collection_version ) {
            $partner_versionId = $collection_version;
            $match = 1;
            last;
        }
    }
    # there is no matching configured version
    unless ( $match ) {
        $missing_message = "Partner $partner does not have matching collection for" .
        " shortName $s4pa_shortName, version $s4pa_versionId";
        $history = [ $this_run, $matches, $missing_s4pa, $missing_partner ];
        save_history( $history_file, $collection_id, $history );
        S4P::raise_anomaly( "MISMATCH_$partner", $stationDir, 'WARN', $missing_message, 0 );
        exit 0;
    }
}

# Now that we know the shortName and versionId, get partner granule list
# Added an S4PA_VERSION for Dotchart reconciliation. Since a versionless s4pa
# dataset could have any versionid in the dotchart database, we want to
# make sure that the return granule list only contain the granule from the
# versionless dataset.
my %partner_urls;
my @partner_granules;
if ( $partner eq 'Giovanni' ) {
    %partner_urls = $recon->getPartnerGranuleList( INSTANCE => $s4pa_instance_name,
                                                   SHORTNAME => $s4pa_shortName,
                                                   VERSION => $partner_versionId,
                                                   S4PA_VERSION => $s4pa_versionId );
    foreach my $granule ( sort keys %partner_urls ) {
        push @partner_granules, $granule;
    }
} elsif ( $partner eq 'CMR' ) {
    @partner_granules = $recon->getPartnerGranuleList( INSTANCE => $s4pa_instance_name,
                                                       SHORTNAME => $collection_shortname,
                                                       VERSION => $partner_versionId,
                                                       S4PA_VERSION => $s4pa_versionId );
} else {
    @partner_granules = $recon->getPartnerGranuleList( INSTANCE => $s4pa_instance_name,
                                                       SHORTNAME => $s4pa_shortName,
                                                       VERSION => $partner_versionId,
                                                       S4PA_VERSION => $s4pa_versionId );
}
S4P::perish( 2, $recon->getErrorMessage() ) if $recon->onError();

unless ( @partner_granules ) {
    # If the partner doesn't have any granules for the collection,
    # chances are that the collection is new and no granules have been
    # published to the partner. Reconciliation will not "republish"
    # all of the granules to the partner in this case.
    if ( $debug ) {
        $missing_partner = @s4pa_granules;
        printf "%-${ds_name_width}s  %s              Missing granules %14d\n",
            "$s4pa_shortName v$partner_versionId", '  0.000%', $missing_partner;
    }
    $missing_message = "No granules found in collection for" .
        " shortName $s4pa_shortName, version $partner_versionId" .
        " for partner $partner";
    #S4P::logger( 'WARN', $missing_message );
    $history = [ $this_run, $matches, $missing_s4pa, $missing_partner ];
    save_history( $history_file, $collection_id, $history );
    S4P::raise_anomaly( "MISMATCH_$partner", $stationDir, 'WARN', $missing_message, 0 );
    exit 0;
}

# Compare the s4pa granule list to partner granule list
my @missing_partner_granules;
my @missing_s4pa_granules;
my $s4pa_granule    = shift @s4pa_granules;
my $partner_granule = shift @partner_granules;
while ( (defined $partner_granule) && (defined $s4pa_granule) ) {
    if ( $partner_granule lt $s4pa_granule ) {
#       $missing_s4pa++;
        push @missing_s4pa_granules, $partner_granule;
        $partner_granule = shift @partner_granules;
    }
    elsif ( $partner_granule gt $s4pa_granule ) {
#       $missing_partner++;
        push @missing_partner_granules, $s4pa_granule;
        $s4pa_granule = shift @s4pa_granules;
    }
    else {
        $matches++;
        $s4pa_granule    = shift @s4pa_granules;
        $partner_granule = shift @partner_granules;
    }
}
if ( defined $s4pa_granule ) {
    unshift @s4pa_granules, $s4pa_granule;
#    $missing_partner += scalar( @s4pa_granules );
#    foreach ( @s4pa_granules ) {
#        push @missing_partner_granules, $_;
#    }
    push @missing_partner_granules, @s4pa_granules;
}
if ( defined $partner_granule ) {
    unshift @partner_granules, $partner_granule;
#    $missing_s4pa += scalar( @partner_granules );
#    foreach ( @partner_granules ) {
#        push @missing_s4pa_granules, $_;
#    }
    push @missing_s4pa_granules, @partner_granules;
}

# Republish missing partner granules.
if ( @missing_partner_granules ) {
    $missing_partner = republish_partner_granules( $partner,
                                                   $s4pa_root, $s4pa_shortName,
                                                   $s4pa_versionId,
                                                   $partner_versionId,
                                                   \@missing_partner_granules,
                                                   $ignoreGranuleHashRef,
                                                   $ingest_interval,
                                                   $debug );
    if ( $missing_partner ) {
        $missing_message = "Publish $missing_partner granules for" .
            " shortName $s4pa_shortName, version $partner_versionId" .
            " to partner $partner";
        S4P::raise_anomaly( "MISMATCH_$partner", $stationDir, 'WARN',
            $missing_message, 0 );
    }
}

# Delete extra partner granules (granules partner has but s4pa does not)
# As a precaution, do not delete extra partner granules if there were no
# matches between S4PA granules and the partner's granules, as this is
# probably an indication that something went wrong. If it is desired to
# delete all of the granules in a partner's collection, we expect that
# can be done in another way.

$missing_s4pa = @missing_s4pa_granules;
if ( $matches && @missing_s4pa_granules ) {
    delete_partner_granules( $s4pa_root, $s4pa_shortName, $s4pa_versionId,
                             $partner_versionId, \@missing_s4pa_granules,
                             $partner, $deletion_xml_staging_dir,
                             $s4pa_instance_name, $partner_ftp_pub_host,
                             $partner_ftp_del_pub_dir, \%partner_urls,
                             $partner_service_username,
                             $partner_service_pwd,
                             $partner_service_provider_id,
                             $service_endpoint_uri, $catalog_endpoint_uri,
                             $partner_protocol,
                             $cmr_certfile,
                             $cmr_passfile,
                             $launchpad_uri );
    $missing_message = "Delete $missing_s4pa granules for" .
        " shortName $s4pa_shortName, version $partner_versionId" .
        " to partner $partner";
    S4P::raise_anomaly( "MISMATCH_$partner", $stationDir, 'WARN', $missing_message, 0 );
}

if ( $debug ) {
    printf "%-${ds_name_width}s  %7.3f%% %14d %14d %14d\n", "$s4pa_shortName v$partner_versionId",
    $matches ? ( $matches / ( $matches + $missing_partner ) * 100 ) : 0, $matches, $missing_s4pa,
    $missing_partner;
}

$history = [ $this_run, $matches, $missing_s4pa, $missing_partner ];
save_history( $history_file, $collection_id, $history );

exit 0;


sub next_dataset {
    my ( $history_file, %dataset_list ) = @_;

    my ( $shortname, $versionId );

    # No history file. Return the first enabled dataset from the sorted list.
    unless ( -f $history_file ) {
        foreach my $dataset ( sort keys %dataset_list ) {
            next unless ( $dataset_list{$dataset} );
            ( $shortname, $versionId ) = split /\:/, $dataset, 2;
            return $shortname, $versionId;
        }
    }

    # Expect each line in history to contain fields separated by whitespace.
    # The first field contains shortname:version (where version may be # null),
    # the second filed contains the timestamp (could be epoch time).
    my %history;
    open( HISTORY, "< $history_file" )
        or S4P::perish( 1, "Could not open $history_file for reading\n" );
    while ( <HISTORY> ) {
        chomp();
        next unless length();  # Skip empty lines
        my ( $dataset, $last_run, @history_vals ) = split( /\s+/, $_ );
        # Convert old epoch timestamp to CCSDS timestring
        if ( $last_run =~ /\d{10}/ ) {
            my ($sec, $min, $hour, $day, $month, $year) = gmtime($last_run);
            $last_run = S4P::TimeTools::CCSDSa_DateUnparse($year + 1900,
                $month + 1, $day, $hour, $min, $sec);
        }
        $history{$dataset} = $last_run;
    }
    close( HISTORY );

    # we first search for new datasets that has not been reconed before
    # return the first new dataset in alphabetical order
    foreach my $dataset ( sort keys %dataset_list ) {
        next if ( exists $history{$dataset} );
        next unless ( $dataset_list{$dataset} );
        ( $shortname, $versionId ) = split /\:/, $dataset, 2;
        return $shortname, $versionId;
    }

    # we are only here because there is no new dataset
    # return the oldest dateset from the recon history
    foreach my $dataset ( sort {$history{$a} cmp $history{$b}} keys %history ) {
        next unless ( $dataset_list{$dataset} );
        ( $shortname, $versionId ) = split /\:/, $dataset, 2;
        return $shortname, $versionId;
    }
}


sub read_history {
    my ( $filename, $id ) = @_;

    # Read history file and look for a line whose first whitepace-separated
    # field equals $id. Return a reference to an array of the values
    # found in the other fields of such a line. If no such line is found,
    # return a reference to an empty array.
    my $history = [];

    # If $filename does not exist, return
    return $history unless -f $filename;

    # Expect each line to contain fields separated by whitespace.
    # The first field contains shortname:version (where version may be
    # null)
    open( HISTORY, "< $filename" )
        or S4P::perish( 1, "Could not open $filename for reading\n" );
    while ( <HISTORY> ) {
        chomp();
        next unless length();  # Skip empty lines
        my ( $collection_id, @history_vals ) = split( /\s+/, $_ );
        if ( $collection_id eq $id ) {
            @$history = @history_vals;
            last;
        }
    }
    close( HISTORY );

    return $history;
}


sub save_history {
    my ( $filename, $modified_id, $modified_vals_ref ) = @_;

    my %history;

    if ( -f $filename ) {
        open( HISTORY, "+< $filename" )
            or S4P::perish( 1, "Could not open $filename for updating\n" );

        # Wait until file is unlocked then lock file
        flock( HISTORY, 2 );

        # Expect each line to contain fields separated by whitespace.
        # The first field contains shortname:version (where version may be
        # null)
        while ( <HISTORY> ) {
            chomp();
            next unless length();  # Skip empty lines
            my ( $collection_id, @history ) = split( /\s+/, $_ );
            $history{$collection_id} = \@history;
        }
        seek( HISTORY, 0, 0 );
    } else {
        open( HISTORY, "> $filename" )
            or S4P::perish( 1, "Could not open $filename for writing\n" );
    }

    # Update the record for the collection
    $history{$modified_id} = $modified_vals_ref;

    foreach my $collection_id ( sort keys %history ) {
        my @history_vals = @{$history{$collection_id}};
        my $line = join( ' ', $collection_id, @history_vals );
        print HISTORY "$line\n";
    }
    truncate( HISTORY, tell(HISTORY) );
    close( HISTORY );
}


sub recon_object {
    my ( $partner, $s4pa_root, $large_count_threshold,
         $ftp_push_host, $ftp_push_user, $ftp_push_pwd,
         $ftp_push_dir, $ftp_push_quiescent_time,
         $ftp_server_chroot, $temp_dir, $catalog_endpoint_uri,
         $service_endpoint_uri,
         $service_timeout, $partner_service_username,
         $partner_service_pwd, $partner_service_provider_id,
         $local_hostname_override, $ftp_pull_timeout, $partner_protocol,
         $cmr_certfile, $cmr_passfile, $launchpad_uri) = @_;

    my $robj;
    my $local_push_dir = ($ftp_server_chroot) ? "$ftp_server_chroot/$ftp_push_dir"
                                              : undef;

    if ( $partner eq 'ECHO' ) {
        $robj = S4PA::Reconciliation::ECHO->new(
                            FTP_PUSH_HOST         => $ftp_push_host,
                            FTP_PUSH_USER         => $ftp_push_user,
                            FTP_PUSH_PWD          => $ftp_push_pwd,
                            FTP_PUSH_DIR          => $ftp_push_dir,
                            FTP_PUSH_QUIESCENT_TIME => $ftp_push_quiescent_time,
                            TEMP_DIR              => $temp_dir,
                            LOCAL_PUSH_DIR        => $local_push_dir,
                            LARGE_COUNT_THRESHOLD => $large_count_threshold,
                            ENDPOINT_ROOT         => $service_endpoint_uri,
                            CATALOG_ENDPOINT_ROOT => $catalog_endpoint_uri,
                            SERVICE_TIMEOUT       => $service_timeout,
                            LOCAL_HOST_NAME       => $local_hostname_override,
                            USERNAME              => $partner_service_username,
                            PASSWORD              => $partner_service_pwd,
                            PROVIDER              => $partner_service_provider_id,
                                               );
    } elsif ( $partner eq 'CMR' ) {
        # for Launchpad token
        if (defined $launchpad_uri) {
            $robj = S4PA::Reconciliation::CMR->new(
                            FTP_PUSH_HOST         => $ftp_push_host,
                            FTP_PUSH_USER         => $ftp_push_user,
                            FTP_PUSH_PWD          => $ftp_push_pwd,
                            FTP_PUSH_DIR          => $ftp_push_dir,
                            FTP_PUSH_QUIESCENT_TIME => $ftp_push_quiescent_time,
                            TEMP_DIR              => $temp_dir,
                            LOCAL_PUSH_DIR        => $local_push_dir,
                            LARGE_COUNT_THRESHOLD => $large_count_threshold,
                            ENDPOINT_ROOT         => $launchpad_uri,
                            CATALOG_ENDPOINT_ROOT => $catalog_endpoint_uri,
                            SERVICE_TIMEOUT       => $service_timeout,
                            LOCAL_HOST_NAME       => $local_hostname_override,
                            CERTFILE              => $cmr_certfile,
                            CERTPASS              => $cmr_passfile,
                            PROVIDER              => $partner_service_provider_id,
                                               );
        # for ECHO token
        } else {
            $robj = S4PA::Reconciliation::CMR->new(
                            FTP_PUSH_HOST         => $ftp_push_host,
                            FTP_PUSH_USER         => $ftp_push_user,
                            FTP_PUSH_PWD          => $ftp_push_pwd,
                            FTP_PUSH_DIR          => $ftp_push_dir,
                            FTP_PUSH_QUIESCENT_TIME => $ftp_push_quiescent_time,
                            TEMP_DIR              => $temp_dir,
                            LOCAL_PUSH_DIR        => $local_push_dir,
                            LARGE_COUNT_THRESHOLD => $large_count_threshold,
                            ENDPOINT_ROOT         => $service_endpoint_uri,
                            CATALOG_ENDPOINT_ROOT => $catalog_endpoint_uri,
                            SERVICE_TIMEOUT       => $service_timeout,
                            LOCAL_HOST_NAME       => $local_hostname_override,
                            USERNAME              => $partner_service_username,
                            PASSWORD              => $partner_service_pwd,
                            PROVIDER              => $partner_service_provider_id,
                                               );
        }
    } elsif ( $partner eq 'Mirador' ) {
        $robj = S4PA::Reconciliation::Mirador->new(
                            FTP_PUSH_HOST         => $ftp_push_host,
                            FTP_PUSH_USER         => $ftp_push_user,
                            FTP_PUSH_PWD          => $ftp_push_pwd,
                            FTP_PUSH_DIR          => $ftp_push_dir,
                            FTP_PUSH_QUIESCENT_TIME => $ftp_push_quiescent_time,
                            TEMP_DIR              => $temp_dir,
                            LOCAL_PUSH_DIR        => $local_push_dir,
                            LARGE_COUNT_THRESHOLD => $large_count_threshold,
                            ENDPOINT_ROOT         => $service_endpoint_uri,
                            SERVICE_TIMEOUT       => $service_timeout,
                            LOCAL_HOST_NAME       => $local_hostname_override,
                            FTP_PULL_TIMEOUT      => $ftp_pull_timeout,
                            PARTNER_PROTOCOL      => $partner_protocol,
                                                  );
    } elsif ( $partner eq 'Dotchart' ) {
        $robj = S4PA::Reconciliation::Dotchart->new(
                            FTP_PUSH_HOST         => $ftp_push_host,
                            FTP_PUSH_USER         => $ftp_push_user,
                            FTP_PUSH_PWD          => $ftp_push_pwd,
                            FTP_PUSH_DIR          => $ftp_push_dir,
                            FTP_PUSH_QUIESCENT_TIME => $ftp_push_quiescent_time,
                            TEMP_DIR              => $temp_dir,
                            LOCAL_PUSH_DIR        => $local_push_dir,
                            LARGE_COUNT_THRESHOLD => $large_count_threshold,
                            ENDPOINT_ROOT         => $service_endpoint_uri,
                            SERVICE_TIMEOUT       => $service_timeout,
                            LOCAL_HOST_NAME       => $local_hostname_override,
                            FTP_PULL_TIMEOUT      => $ftp_pull_timeout,
                            RECONCILE_ALL_FILES   => 1,
                            PARTNER_PROTOCOL      => $partner_protocol,
                                                  );
    } elsif ( $partner eq 'Giovanni' ) {
        $robj = S4PA::Reconciliation::Giovanni->new(
                            FTP_PUSH_HOST         => $ftp_push_host,
                            FTP_PUSH_USER         => $ftp_push_user,
                            FTP_PUSH_PWD          => $ftp_push_pwd,
                            FTP_PUSH_DIR          => $ftp_push_dir,
                            FTP_PUSH_QUIESCENT_TIME => $ftp_push_quiescent_time,
                            TEMP_DIR              => $temp_dir,
                            LOCAL_PUSH_DIR        => $local_push_dir,
                            LARGE_COUNT_THRESHOLD => $large_count_threshold,
                            ENDPOINT_ROOT         => $service_endpoint_uri,
                            SERVICE_TIMEOUT       => $service_timeout,
                            LOCAL_HOST_NAME       => $local_hostname_override,
                            FTP_PULL_TIMEOUT      => $ftp_pull_timeout,
                                                  );
    } else {
        S4P::perish( 1, "Unknown reconciliation partner $partner" )
    }

    return $robj;
}


sub get_s4pa_granule_list {
    my ( $s4pa_root, $shortName, $version ) = @_;

#    my $cpt = new Safe( 'CFG' );

    # Determine root directory and class for the shortName/version combination
    my ( $data_directory, $data_class ) =
        S4PA::Storage::GetDataRootDirectory( $s4pa_root,
                                             $shortName,
                                             $version );
    S4P::perish( 1, "Could not obtain \$data_directory for root " .
                    " $s4pa_root, shortName $shortName, version $version" )
        unless $data_directory;

    my $dataSet = ( $version eq "" ) ? $shortName : "$shortName.$version";
    my $dbname = "$data_directory/granule.db";
    S4P::perish( 1, "Missing $dbname for dataset $dataSet")
        unless ( -e $dbname );
    my ( $granuleHashRef, $fileHandle ) = S4PA::Storage::OpenGranuleDB(
                                                                       $dbname,
                                                                       "r"
                                                                      );
    unless ( defined $granuleHashRef ) {
        S4P::perish( 1, "Failed to open granule database in " .
                     "$data_directory for dataset $dataSet" );
    }

    # If called in a list context, return a sorted list of
    # the granule names in the granule.db for the dataset.
    # If called in a scalar context, return a count of the granules.
    if ( wantarray ) {

        # Consider granules to be keys that do not end in .xml
        # Also, if browse files can be found in the granule.db,
        # ignore them too by assuming they end in .jpg
        my @s4pa_granules = sort( grep !(/\.xml$/ || /\.jpg$/ || /\.map\.gz$/),
            keys(%$granuleHashRef) );

        # For a browse dataset, there are only xml and jpg file in granule.db
        # jpg file will be considered as the data file of the granule
        unless ( @s4pa_granules ) {
            @s4pa_granules = sort( grep !(/\.xml$/), keys(%$granuleHashRef) );
        }
        S4PA::Storage::CloseGranuleDB( $granuleHashRef, $fileHandle );
        return @s4pa_granules;
    }
    else {
        my $granule_count = ( grep !/\.xml$/, keys(%$granuleHashRef) );
        S4PA::Storage::CloseGranuleDB( $granuleHashRef, $fileHandle );
        return $granule_count;
    }
}


sub get_partner_version_id {
    my ( $s4pa_instance_name, $s4pa_root, $s4pa_shortName, $s4pa_versionId, $recon ) = @_;

    my @partner_versionIds = $recon->getPartnerDatasetVersions( INSTANCE => $s4pa_instance_name,
                                                                SHORTNAME => $s4pa_shortName );
    S4P::perish( 2, $recon->getErrorMessage() ) if $recon->onError();

    # Find the versionId on the partner system that is equivalent to
    # the S4PA versionId.
        if ( $s4pa_versionId ne "" ) {
            foreach $partner_versionId ( @partner_versionIds ) {

                # Normally we expect to find a match between version strings
                return $partner_versionId if ( $partner_versionId eq $s4pa_versionId );

                # If the partner system (e.g. ECHO) converts the
                # version to a number, consider that a match too
                # (at least for now)
                #
                # ECHO is gone and CMR has string type for version ID.
                # The following number matching is causing problem
                # in OCO2 version '7' and '7r'. Comment the following line out.
                # return $partner_versionId if ( $partner_versionId == $s4pa_versionId );
            }
        }
        else {
            if ( @partner_versionIds > 1 ) {
                # If the S4PA dataset is versionless, and more than
                # one version exists on the partner system, we need to
                # exclude those version(s) configured in s4pa and
                # group the rest of them together to be the
                # versionless.
                # read publishing requirement
                my $cfgFile = "$s4pa_root/storage/dataset.cfg";
                my $cpt = new Safe 'DATASET';
                $cpt->rdo( $cfgFile ) ||
                    S4P::perish( 2, "Failed to read config file $cfgFile: ($!)\n" );

                my @partner_versionless_ids;
                foreach my $version ( @partner_versionIds ) {
                    push( @partner_versionless_ids, $version )
                        unless ( exists $DATASET::cfg_publication{$s4pa_shortName}{$version} );
                }

                # If there is still more than one version exists on
                # the partner system, there is a problem. Return undef
                # in this case.
                if ( @partner_versionless_ids > 1 ) {
                    my $versions = join( ',', @partner_versionIds );
                    S4P::logger( 'WARN', "$s4pa_shortName is versionless in S4PA " .
                        "but found versions $versions in $partner." );
                    return $versions;
                } else {
                    return $partner_versionless_ids[0];
                }
            }
            else {
                # The S4PA dataset is versionless, and only one version
                # exists on the partner system.
                return $partner_versionIds[0];
            }
        }

    return;
}


sub output_pdr {
    my ( $pdrFile, $pdr, $removeVersion ) = @_;

    open( PDR, "> $pdrFile" )
        or S4P::perish ( 1, "Cannot open $pdrFile for output" );
    my $pdrtext = $pdr->sprint();
    $pdrtext =~ s/\s*DATA_VERSION=.*;//g if ( $removeVersion );
    print PDR $pdrtext;
    close PDR;
}


sub get_exclude_list {
    my ( $path, $data_class ) = @_;

    # Create a hash of files in data class $data_class that are
    # pending deletion, so those files can be excluded from being
    # republished to the data partner
    my $exclude_hash = {};

    my @dir_list = qw( inter_version_pending intra_version_pending );

    foreach my $dir ( @dir_list ) {
        my @pdr_file_list = glob( "$path/$dir/*.PDR" );

        foreach my $pdr_file ( @pdr_file_list ) {
            my $pdr = S4P::PDR::read_pdr( $pdr_file );
            my @fileList = $pdr->files();
            foreach my $file ( @fileList ) {
                $exclude_hash->{$data_class}{basename( $file) } = 1;
            }
        }
    }

    return $exclude_hash;
}


sub republish_partner_granules {
    my ( $partner, $s4pa_root, $shortName, $s4pa_version, $partner_version,
         $missing_granules, $ignoreGranuleHashRef, $ingest_interval, $debug ) = @_;

    # Make a PDR object
    my $pdr = S4P::PDR::start_pdr(
        'originating_system' => "RECONCILIATION_REPUB",
        'expiration_time'    => S4P::PDR::get_exp_time( 30, 'days' )
    );

    # Determine root directory and class for the shortName/version combination
    # We use $s4pa_version because that is the same version used to find
    # the granules being republished.
    my ( $data_directory, $data_class ) =
        S4PA::Storage::GetDataRootDirectory( $s4pa_root,
                                             $shortName,
                                             $s4pa_version );
    S4P::perish( 1, "Could not obtain \$data_directory for root " .
                 "$s4pa_root, shortName $shortName, version $s4pa_version" )
        unless $data_directory;

    # In the data directory open the granule.db to get fields for the
    # full path name
    my $dataSet = ( $s4pa_version eq "" ) ? $shortName : "$shortName.$s4pa_version";
    my $dbname = "$data_directory/granule.db";
    S4P::perish( 1, "Missing $dbname for dataset $dataSet")
        unless ( -e $dbname );
    my ( $granuleHashRef, $fileHandle ) =
        S4PA::Storage::OpenGranuleDB( $dbname, "r");
    unless ( defined $granuleHashRef ) {
        S4P::perish( 1, "Failed to open granule database in " .
                     "$data_directory for dataset $dataSet" );
    }

    # With path name, we can get entries slated for deletion and
    # exclude them from PDR generation
    my $exclude_hash =
        get_exclude_list( "$s4pa_root/storage/$data_class/delete_$data_class",
                          $data_class );

    # Read actual link for metadata directory
    my $metadata_root = readlink "$data_directory/data";

    my $missing_count = 0;
    foreach my $granule ( @$missing_granules ) {
        # See if granule exists in granule db
        unless ( exists $granuleHashRef->{$granule} ) {
            S4P::logger( 'WARN', "Granule $granule does not exist in " .
                         "$data_directory/granule.db, so will not republish" .
                         " to $partner" );
            next;
        }

        # Skip republishing 'hidden' granules to search partner
        if ( $partner ne 'Dotchart' &&
             $granuleHashRef->{$granule}{mode} == 0600 ) {
            S4P::logger( 'INFO', "Granule $granule is hidden, skip republish" .
                         " to $partner" );
            next;
        }

        # Check if granule is in ignore granule list
        if ( exists $ignoreGranuleHashRef->{$granule} ) {
            S4P::logger( 'WARN', "Granule $granule is specified to be ignored " .
                         "for any re-publication to $partner" );
            next;
        }

        # Determine metadata directory
        my $metadata_dir = "$metadata_root/$granuleHashRef->{$granule}{date}";
        # Append .hidden to directory path if the granule is not public data
        $metadata_dir .= '/.hidden'
            if ( $granuleHashRef->{$granule}{mode} == 0640 ||
                 $granuleHashRef->{$granule}{mode} == 0600 );

        # Try to guess the name of the metadata file associated with
        # the granule file

        # Initialize metadata file name before trying alternatives
        my $xml_basename = $granule;

        if ( $xml_basename =~ /(^.*)\.Z$/ ) {
            # If name ends in .Z, remove it
            $xml_basename = $1;
        } elsif ( $xml_basename =~ /(^.*)\.gz$/ ) {
            # If name ends in .gz, remove it
            $xml_basename = $1;
        }

        # Add .xml extension for metadata file
        my $xml_file = $xml_basename . ".xml";

        # Most metadata files have names with .xml appended to the granule
        # name, but some have a name such that .xml replaces the extension.
        unless ( -e "$metadata_dir/$xml_file" ) {
            # If we don't find a metadata file with the name we expect,
            # try replacing the extansion with .xml
            $xml_file =~ s/(.+)\..+\.xml$/$1.xml/;
        }

        # Add a filegroup to the PDR for all files found in metadata files
        # that are not marked for deletion.
        # We skip files marked for deletion because we should have already
        # sent a deletion request to the data partner, so the data partner
        # has probably not yet processed that deletion request.
        unless ( $exclude_hash->{$data_class}{$xml_file} ) {
            if ( -e "$metadata_dir/$xml_file" ) {
                # Create Metadata object from metadata file
                my %argMetadata;
                $argMetadata{FILE} = "$metadata_dir/$xml_file";
                my $metadata  = new S4PA::Metadata( %argMetadata );

                # checking insert time and skip re-publish on
                # newly ingested granules
                my $insertTime = $metadata->getValue(
                    "/S4PAGranuleMetaDataFile/DataGranule/InsertDateTime" );
                $insertTime = S4P::TimeTools::timestamp2CCSDSa( $insertTime );
                my $currentTime = S4P::TimeTools::CCSDSa_Now();
                my $elapseTime = S4P::TimeTools::CCSDSa_Diff( $insertTime, $currentTime );
                next unless ( $elapseTime > $ingest_interval );

                S4P::logger( 'INFO', "Republishing files in" .
                             " $metadata_dir/$xml_file to $partner" );
                $missing_count++;

                my $fileGroup = $metadata->getFileGroup();
                # Populate fileGroup for pdr
                # Because $partner_version is used, the PDR that is created
                # will have a FILE_GROUP for the granule with a DATA_VERSION
                # value set to $partner_version, which may be different than
                # the DATA_VERSION in the PDR when the granule was first
                # published if the partner converted the value
                # (e.g. converted 002 to 2).
                $fileGroup->data_type( $shortName );
                $fileGroup->data_version( $partner_version, "%s" );

                # Add fileGroup to pdr
                $pdr->add_file_group( $fileGroup );
            } else {
                # Skip files that do not have an existing metadata file
                S4P::logger( 'WARN', "Could not find metadata file " .
                             "$metadata_dir/$xml_file for republishing" .
                             " $granule to $partner" );
                next;
            }
        } else {
            # The metadata file for the granule is marked for deletion
            # from s4pa, so do not republish the granule
            S4P::logger( 'INFO', "Granule $xml_file marked for deletion," .
                         "so will not republish to $partner" );
            next;
        }
    }

    # Close database
    S4PA::Storage::CloseGranuleDB( $granuleHashRef, $fileHandle );

    # Create unique PDR name using system time
    my $time = time();
    my $pdrDir = ($debug) ? '.' : "$s4pa_root/publish_" . lc( $partner ) .
                                  "/pending_publish";
    my $pdrFile = "$pdrDir/REPUBLISH_${partner}.$time" . '-' . "$$.PDR";
    if ( defined $pdr->file_groups() ) {
        # Write out PDR
        output_pdr( "$pdrFile", $pdr );
    } else {
        S4P::logger( "WARN", "None of the granules missing from $partner" .
                     " need to be republished!" );
    }

    return $missing_count;
}


sub create_wo {
    my ( $wo_name, $dest_protocol, $dest_host, $dest_dir, $files ) = @_;

    # Write a work order to push a file via ftp
    my $wo_parser = XML::LibXML->new();
    my $wo_dom = $wo_parser->parse_string( '<FilePacket/>' );
    my $wo_doc = $wo_dom->documentElement();

    my ( $filePacketNode ) = $wo_doc->findnodes( '/FilePacket' );
    $filePacketNode->setAttribute( 'status', "I" );

    my $destination = $dest_protocol . ":${dest_host}${dest_dir}";
    $filePacketNode->setAttribute( 'destination', $destination );

    foreach my $file ( @$files ) {
        my $filegroupNode = XML::LibXML::Element->new( 'FileGroup' );
        my $fileNode = XML::LibXML::Element->new( 'File' );
        $fileNode->setAttribute( 'localPath', $file );
        $fileNode->setAttribute( 'status', "I" );
        $fileNode->setAttribute( 'cleanup', "Y" );
        $filegroupNode->appendChild( $fileNode );
        $wo_doc->appendChild( $filegroupNode );
    }

    unless ( open (WO, "> $wo_name") ) {
        S4P::logger( "ERROR", "Failed to open work order $wo_name ($!)" );
        return 0;
    }
    print WO $wo_dom->toString(1);
    unless ( close(WO) ) {
        S4P::logger( "ERROR", "Failed to close work order $wo_name ($!)" );
        return 0;
    }

    return 1;
}


sub create_rest_wo {
    my ($wo_name,
        $shortName,
        $version,
        $partner_service_username,
        $partner_service_pwd,
        $partner_service_provider_id,
        $service_endpoint_uri,
        $catalog_endpoint_uri,
        $granules,
        $cmr_certfile,
        $cmr_passfile,
        $launchpad_uri) = @_;
 
    my ($cmrToken, $errmsg);
    # CMR switch from Earthdata login to Launchpad token for authentication
    if (defined $launchpad_uri) {
        my $tokenParam = {};
        $tokenParam->{LP_URI} = $launchpad_uri;
        $tokenParam->{CMR_CERTFILE} = $cmr_certfile;
        $tokenParam->{CMR_CERTPASS} = $cmr_passfile;
        ($cmrToken, $errmsg) = S4PA::get_cmr_token($tokenParam);
        unless (defined $cmrToken) {
            S4P::perish(3, "$errmsg");
        }
    # the original token acquiring from CMR/ECHO
    } else {
        my $tokenParam = {};
        $tokenParam->{ECHO_URI} = $service_endpoint_uri;
        $tokenParam->{CMR_USERNAME} = $partner_service_username;
        $tokenParam->{CMR_PASSWORD} = $partner_service_pwd;
        $tokenParam->{CMR_PROVIDER} = $partner_service_provider_id;
        ($cmrToken, $errmsg) = S4PA::get_cmr_token($tokenParam);
        unless (defined $cmrToken) {
            S4P::perish(3, "$errmsg");
        }
    }

    # Write a work order to submit a REST request
    my $parser = XML::LibXML->new();
    my $woDom = $parser->parse_string('<RestPackets/>');
    my $woDoc = $woDom->documentElement();
    $woDoc->setAttribute('status', "I");

    # move http request header outside each packet
    if ($cmrToken) {
        my $headerNode = XML::LibXML::Element->new('HTTPheader');
        $headerNode->appendText("Echo-Token: $cmrToken");
        $woDoc->appendChild($headerNode);
    }
    my $headerNode = XML::LibXML::Element->new('HTTPheader');
    if ($CFG::CMR_ENDPOINT_URI =~ /cmr/i) {
        $headerNode->appendText("Content-Type: application/echo10+xml");
    } else {
        $headerNode->appendText("Content-Type: application/xml");
    }
    $woDoc->appendChild($headerNode);

    my $destinationBase = $catalog_endpoint_uri . 'providers/' .
                          $partner_service_provider_id . '/granules/';

    my $UR_prefix = "$shortName.$version:";
    foreach my $granule (@$granules) {
        my $granuleUR = "$UR_prefix" . $granule;
        my $restPacketNode = XML::LibXML::Element->new('RestPacket');
        $restPacketNode->setAttribute('status', "I");
        my $destination = $destinationBase . $granuleUR;
        $restPacketNode->setAttribute('destination', $destination);
        $woDoc->appendChild($restPacketNode);
    }

    unless ( open (WO, "> $wo_name") ) {
        S4P::logger( "ERROR", "Failed to open work order $wo_name ($!)");
        return 0;
    }
    print WO $woDom->toString(1);
    unless ( close WO ) {
        S4P::logger( "ERROR", "Failed to close work order $wo_name ($!)" );
        return 0;
    }
    return(1);
}


sub create_mirador_wo {
    my ( $wo_name, $dest_protocol, $dest_host, $dest_dir, $instance, $files ) = @_;

    # Mirador requires bundled xml files in tar-ball
    my $maxCount = 1000;
    my $graCount = 0;
    my $tarCount = 1;
    my @tarFiles;
    my $tarFile;
    my @date = localtime(time);
    my $dateString = sprintf("%04d%02d%02d%02d%02d%02d",
        $date[5]+1900, $date[4]+1, $date[3], $date[2], $date[1], $date[0]);
    my $tarPrefix = "MiradorDel.$instance.F$maxCount.T$dateString";
    foreach my $path ( @$files ) {
        my $file = basename( $path );
        my $dir = dirname( $path );
        my $tarOption;
        if ( $graCount == 0 ) {
            $tarOption = 'c';
        } elsif ( $graCount == $maxCount ) {
            push @tarFiles, $tarFile;
            $tarCount++;
            $graCount = 0;
            $tarOption = 'c';
        } else {
            $tarOption = 'r';
        }
        $tarFile = "$dir/$tarPrefix" . "_$tarCount.tar";
        my $command = "tar -$tarOption --remove-files -f $tarFile -C $dir $file";
        `$command`;
        if ( $? ) {
            S4P::logger( "ERROR", "Failed to add $file to archive $tarFile ($!)" );
            return 0;
        }
        $graCount++;
    }
    ( my $newTarFile = $tarFile ) =~ s/\.F\d+\./\.F$graCount\./;
    unless ( move( $tarFile, $newTarFile ) ) {
        S4P::logger( "ERROR", "Failed to rename $tarFile to $newTarFile ($!)" );
        return 0;
    }
    push @tarFiles, $newTarFile;

    # Write a work order to push a file via ftp
    my $wo_parser = XML::LibXML->new();
    my $wo_dom = $wo_parser->parse_string( '<FilePacket/>' );
    my $wo_doc = $wo_dom->documentElement();

    my ( $filePacketNode ) = $wo_doc->findnodes( '/FilePacket' );
    $filePacketNode->setAttribute( 'status', "I" );

    my $destination = $dest_protocol . ":${dest_host}${dest_dir}";
    $filePacketNode->setAttribute( 'destination', $destination );

    foreach my $file ( @tarFiles ) {
        my $filegroupNode = XML::LibXML::Element->new( 'FileGroup' );
        my $fileNode = XML::LibXML::Element->new( 'File' );
        $fileNode->setAttribute( 'localPath', $file );
        $fileNode->setAttribute( 'status', "I" );
        $fileNode->setAttribute( 'cleanup', "Y" );
        $filegroupNode->appendChild( $fileNode );
        $wo_doc->appendChild( $filegroupNode );
    }

    unless ( open (WO, "> $wo_name") ) {
        S4P::logger( "ERROR", "Failed to open work order $wo_name ($!)" );
        return 0;
    }
    print WO $wo_dom->toString(1);
    unless ( close(WO) ) {
        S4P::logger( "ERROR", "Failed to close work order $wo_name ($!)" );
        return 0;
    }

    return 1;
}


sub delete_partner_granules {
    my ( $s4pa_root, $s4pa_shortName, $s4pa_versionId, $partner_versionId,
         $extra_granules, $partner, $staging_dir, $s4pa_instance_name,
         $partner_ftp_pub_host, $partner_ftp_del_pub_dir, $partner_urls,
         $partner_service_username, $partner_service_pwd,
         $partner_service_provider_id,
         $service_endpoint_uri, $catalog_endpoint_uri, $partner_protocol,
         $cmr_certfile, $cmr_passfile, $launchpad_uri) = @_;

    # Generate one or more xml files for deleting granules in @$extra_granules
    # from the partner because those granules are no longer in the S4PA
    # archive, and then create a work order to push the xml files to
    # the partner via ftp.

    my $version = $s4pa_versionId ? $s4pa_versionId : $partner_versionId;
    my @date = localtime( time );
    my $date_str = sprintf( "T%04d%02d%02d%02d%02d%02d",
                            $date[5]+1900, $date[4]+1, $date[3], $date[2],
                            $date[1], $date[0] );
    my $xml_paths;
    my $wo_prefix;
    if ( $partner eq 'ECHO' ) {
        $wo_prefix = 'EchoDel';
        $xml_paths = echo_granule_deletion_file ( $s4pa_shortName,
                                                  $version,
                                                  $extra_granules,
                                                  $staging_dir,
                                                  $s4pa_instance_name,
                                                  $date_str,
                                                  $wo_prefix );
    } elsif ( $partner eq 'CMR' ) {
        $wo_prefix = 'CmrDel';
        # Klugey way to define $xml_paths if there are granules
        # to be deleted.
        $xml_paths = $extra_granules;
    } elsif ( $partner eq 'Mirador' ) {
        $wo_prefix = 'MiradorDel';
        $xml_paths = mirador_granule_deletion_files ( $s4pa_shortName,
                                                      $version,
                                                      $extra_granules,
                                                      $staging_dir,
                                                      $s4pa_instance_name );
    } elsif ( $partner eq 'Dotchart' ) {
        $wo_prefix = 'DotchartDel';
        $xml_paths = dotchart_granule_deletion_files ( $s4pa_shortName,
                                                       $version,
                                                       $extra_granules,
                                                       $staging_dir,
                                                       $s4pa_instance_name,
                                                       $date_str,
                                                       $wo_prefix );
    } elsif ( $partner eq 'Giovanni' ) {
        $wo_prefix = 'GiovanniDel';
        $xml_paths = giovanni_granule_deletion_files ( $s4pa_shortName,
                                                       $version,
                                                       $extra_granules,
                                                       $staging_dir,
                                                       $s4pa_instance_name,
                                                       $date_str,
                                                       $wo_prefix,
                                                       $partner_urls );
    } else {
        S4P::perish( 1, "Unknown reconciliation partner $partner" .
                        ", cannot delete granules" )
    }

    # Write work order for postoffice station for the delivery of the
    # granule deletion xml
    if ($xml_paths) {
        my $wo_type = 'PUSH';
        my $wo_name = sprintf( "PUSH.%s.%s-%s.wo", $wo_prefix, $date_str, $$ );
        my $status;
        if ( $partner eq 'Mirador' ) {
            $status = create_mirador_wo( $wo_name, $partner_protocol, $partner_ftp_pub_host,
                $partner_ftp_del_pub_dir, $s4pa_instance_name, $xml_paths );
        } elsif ( $partner eq 'CMR' ) {
            $wo_name = sprintf( "DELETE.%s.%s-%s.wo", $wo_prefix, $date_str, $$ );
            $status = create_rest_wo( $wo_name,
                                      $s4pa_shortName,
                                      $version,
                                      $partner_service_username,
                                      $partner_service_pwd,
                                      $partner_service_provider_id,
                                      $service_endpoint_uri,
                                      $catalog_endpoint_uri,
                                      $extra_granules,
                                      $cmr_certfile,
                                      $cmr_passfile,
                                      $launchpad_uri )
        } else {
            $status = create_wo( $wo_name, $partner_protocol, $partner_ftp_pub_host,
                $partner_ftp_del_pub_dir, $xml_paths );
        }
        if ( $status ) {
           S4P::logger( 'INFO', "Work order $wo_name created" );
        }
    }
}


sub echo_granule_deletion_file {
    my ( $shortName, $version, $extra_granules, $staging_dir,
         $s4pa_instance_name, $date_str, $prefix ) = @_;

    # Create an xml file for deleting all granules in @$extra_granules
    # from ECHO because those granules are no longer in the S4PA archive

    my $dom = XML::LibXML::Document->new( '1.0', 'UTF-8' );
    my $root = XML::LibXML::Element->new( 'GranuleMetaDataFile' );
    $root->setAttribute( 'xmlns:xsi',
                         'http://www.w3.org/2001/XMLSchema-instance' );
    $root->setAttribute( 'xsi:noNamespaceSchemaLocation',
        'http://www.echo.nasa.gov/ingest/schemas/operations/Granule.xsd' );
    $dom->setDocumentElement( $root );
    my $doc = $dom->documentElement();

    # Create a GranuleDeletes node
    my $GranuleDeletesNode = XML::LibXML::Element->new( 'GranuleDeletes' );

    my $UR_prefix = "$shortName.$version:";
    foreach my $granule ( @$extra_granules ) {
        my $GranuleDeleteNode = XML::LibXML::Element->new( 'GranuleDelete' );
        my $granuleUR = "$UR_prefix" . $granule;
        $GranuleDeleteNode->appendTextChild( 'GranuleUR', $granuleUR );
        $GranuleDeletesNode->appendChild( $GranuleDeleteNode );
    }
    $doc->appendChild( $GranuleDeletesNode );

    my $xml_name = sprintf( "%s.%s_recon.%s-%s.xml",
        $prefix, $s4pa_instance_name, $date_str, $$ );
    my $xml_path = "$staging_dir/$xml_name";
    unless ( open (OUTFILE, "> $xml_path") ) {
        S4P::logger( 'ERROR', "Failed to open $xml_path ($!)" );
        return;
    }
    print OUTFILE $dom->toString(1);
    unless ( close (OUTFILE) ) {
        S4P::logger( 'ERROR', "Failed to close $xml_path ($!)" );
        return;
    }
    my @xml_paths = ($xml_path);

    return \@xml_paths;
}


sub mirador_granule_deletion_files {
    my ( $shortName, $version, $extra_granules, $staging_dir,
         $s4pa_instance_name ) = @_;

    # Create an xml file for every granule in @$extra_granules to delete
    # that granule from Mirador because that granule is no longer in the
    # S4PA archive

    my @xml_files;

    my $xmlParser = XML::LibXML->new();

    foreach my $granule ( @$extra_granules ) {

        my $dom = $xmlParser->parse_string( "<S4PAGranuleMetaDataFile INSTANCE=\"$s4pa_instance_name\"/>" );
        my $doc = $dom->documentElement();

        # Create a CollectionMetaData node
        my $CollectionMetaDataNode = XML::LibXML::Element->new( 'CollectionMetaData' );
        # We don't bother to create a LongName child of CollectionMetaData,
        # since Mirador doesn't seem to use it when deleting granules.
        $CollectionMetaDataNode->appendTextChild( 'ShortName', $shortName );
        $CollectionMetaDataNode->appendTextChild( 'VersionID', $version );

        # Create a DataGranule node
        my $DataGranuleNode = XML::LibXML::Element->new( 'DataGranule' );
        $DataGranuleNode->appendTextChild( 'GranuleID', $granule );

        # Add the nodes to the xml document
        $doc->appendChild( $CollectionMetaDataNode );
        $doc->appendChild( $DataGranuleNode );

        # Write the xml document file for deleting granule $granule
        # to the directory $staging_dir
        my $xml_name = "$granule.xml";
        my $xml_path = "$staging_dir/$xml_name";
        unless ( open (OUTFILE, "> $xml_path") ) {
            S4P::logger( 'ERROR', "Failed to open $xml_path ($!)" );
            return;
        }
        print OUTFILE $dom->toString(1);
        unless ( close (OUTFILE) ) {
            S4P::logger( 'ERROR', "Failed to close $xml_path ($!)" );
            return;
        }
        push @xml_files, $xml_path;
    }

    # Return a reference to a list of all xml files that were created
    return \@xml_files;
}


sub dotchart_granule_deletion_files {
    my ( $shortName, $version, $extra_granules, $staging_dir,
         $s4pa_instance_name, $date_str, $prefix ) = @_;

    # Create an xml file for deleting all granules in @$extra_granules
    # from Dotchart because those granules are no longer in the S4PA archive

    # Generate the Dotchart xml header

    my $xmlParser = XML::LibXML->new();
    my $dom = $xmlParser->parse_string( '<GranuleMetaDataFile/>' );

    my $doc = $dom->documentElement();
    $doc->appendTextChild( 'InstanceName', $s4pa_instance_name );

    # Create a GranuleMetaDataSet node
    my $gMetaDataSetNode = XML::LibXML::Element->new( 'GranuleMetaDataSet' );

    # Create a DeleteGranules node
    my $DeleteGranulesNode = XML::LibXML::Element->new( 'DeleteGranules' );

    my @date = localtime( time );
    my $delete_time = sprintf( "%04d\-%02d\-%02d %02d\:%02d\:%02d",
                               $date[5]+1900, $date[4]+1, $date[3],
                               $date[2], $date[1], $date[0] );
    foreach my $granule ( @$extra_granules ) {
        # For a versionless dataset, we might have multiple versions in
        # partner's database. If that was the case, the $version string
        # should contain all version IDs separated by comma. We will
        # need to publish all version for this granule
        foreach ( split  ',', $version ) {
            my $partner_version = $_;
            my $DeleteGranuleNode = XML::LibXML::Element->new( 'DeleteGranule' );
            $DeleteGranuleNode->appendTextChild( 'ShortName', $shortName );
            $DeleteGranuleNode->appendTextChild( 'VersionID', $partner_version );
            $DeleteGranuleNode->appendTextChild( 'GranuleID', $granule );
            $DeleteGranuleNode->appendTextChild( 'DeleteTime', $delete_time );
            $DeleteGranulesNode->appendChild( $DeleteGranuleNode );
        }
    }
    $gMetaDataSetNode->appendChild( $DeleteGranulesNode );
    $doc->appendChild( $gMetaDataSetNode );

    my $xml_name = sprintf( "%s.%s.recon.%s-%s.xml",
        $prefix, $s4pa_instance_name, $date_str, $$ );
    my $xml_path = "$staging_dir/$xml_name";
    unless ( open (OUTFILE, "> $xml_path") ) {
        S4P::logger( 'ERROR', "Failed to open $xml_path ($!)" );
        return;
    }
    print OUTFILE $dom->toString(1);
    unless ( close (OUTFILE) ) {
        S4P::logger( 'ERROR', "Failed to close $xml_path ($!)" );
        return;
    }
    my @xml_paths = ($xml_path);

    return \@xml_paths;
}


sub giovanni_granule_deletion_files {
    my ( $shortName, $version, $extra_granules, $staging_dir,
         $s4pa_instance_name, $date_str, $prefix, $partner_urls ) = @_;

    # Create an xml file for deleting all granules in @$extra_granules
    # from Giovanni because those granules are no longer in the S4PA archive

    # Generate the Dotchart xml header

    my $xmlParser = XML::LibXML->new();
    my $dom = $xmlParser->parse_string( '<GranuleMetaDataFile/>' );
    my $doc = $dom->documentElement();

    my @date = localtime( time );
    my $delete_time = sprintf( "%04d\-%02d\-%02d %02d\:%02d\:%02d",
                               $date[5]+1900, $date[4]+1, $date[3],
                               $date[2], $date[1], $date[0] );

    foreach my $granule ( @$extra_granules ) {
        # For a versionless dataset, we might have multiple versions in
        # partner's database. If that was the case, the $version
        # string should contain all version IDs separated by comma. We will
        # need to publish all versions for this granule
        my $granuleURL = $partner_urls->{$granule};
        foreach ( split  ',', $version ) {
            my $partner_version = $_;
            my $DeleteGranuleNode = XML::LibXML::Element->new( 'DeleteGranule' );
            $DeleteGranuleNode->appendTextChild( 'ShortName', $shortName );
            $DeleteGranuleNode->appendTextChild( 'VersionID', $partner_version );
            $DeleteGranuleNode->appendTextChild( 'DataURL', $granuleURL );
            $DeleteGranuleNode->appendTextChild( 'DeleteDateTime', $delete_time );
            $doc->appendChild( $DeleteGranuleNode );
        }
    }

    my $xml_name = sprintf( "%s.%s.recon.%s-%s.xml",
        $prefix, $s4pa_instance_name, $date_str, $$ );
    my $xml_path = "$staging_dir/$xml_name";
    unless ( open (OUTFILE, "> $xml_path") ) {
        S4P::logger( 'ERROR', "Failed to open $xml_path ($!)" );
        return;
    }
    print OUTFILE $dom->toString(1);
    unless ( close (OUTFILE) ) {
        S4P::logger( 'ERROR', "Failed to close $xml_path ($!)" );
        return;
    }
    my @xml_paths = ($xml_path);

    return \@xml_paths;
}


