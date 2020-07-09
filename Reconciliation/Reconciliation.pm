=head1 NAME

S4PA::Reconciliation - A class to reconcile S4PA holdings with those of a data partner

=head1 SYNOPSIS

use S4PA::Reconciliation

=head1 DESCRIPTION

S4PA::Reconciliation contains methods for obtaining metadata from a
metadata partner, in order to perform reconciliation between the
S4PA archive holdings and the metadata partner's holdings.

=head1 AUTHOR

E. Seiler, ADNET Systems Inc

=cut

################################################################################
# $Id: Reconciliation.pm,v 1.47 2020/05/21 10:59:39 s4pa Exp $
# -@@@ S4PA, Version: $Name:  $
################################################################################

package S4PA::Reconciliation;

use strict;
use SOAP::Lite;
use XML::LibXML;
use XML::Twig;
use Sys::Hostname;
use Net::FTP;
use Net::SFTP::Foreign;
use S4PA;
use S4PA::Receiving;
use File::Basename;
use File::stat;
use File::Copy;
use LWP::UserAgent;
use POSIX;
use JSON;
use vars '$AUTOLOAD';
use vars qw($VERSION);

$VERSION = '1.0';

=head2 Constructor

Description:
    Constructs an S4PA reconciliation object

Input:
    Accepts optional arguments.

Output:
    Returns S4PA::Reconciliation object

Author:
    E. Seiler

=cut

sub new {
    my ( $class, %arg ) = @_;

    my $self = {};
    $self->{_error} = '';
    $self->{debug} = defined $arg{DEBUG} ? $arg{DEBUG} : 0;
    $self->{reconcile_all_files} =  defined $arg{RECONCILE_ALL_FILES} ?
                                    $arg{RECONCILE_ALL_FILES} :
                                    0;
    $self->{ftp_push_host} =  defined $arg{FTP_PUSH_HOST} ?
                                      $arg{FTP_PUSH_HOST} :
                                      'discette.gsfc.nasa.gov';
    $self->{ftp_push_user} =  defined $arg{FTP_PUSH_USER} ?
                                      $arg{FTP_PUSH_USER} :
                                      'anonymous';
    $self->{ftp_push_pwd} =  defined $arg{FTP_PUSH_PWD} ?
                                     $arg{FTP_PUSH_PWD} :
                                     's4pa%40';
    $self->{ftp_push_dir} =  defined $arg{FTP_PUSH_DIR} ?
                                     $arg{FTP_PUSH_DIR} :
                                     '/ftp/private/s4pa/push/';
    $self->{ftp_push_quiescent_time} =  defined $arg{FTP_PUSH_QUIESCENT_TIME} ?
                                        $arg{FTP_PUSH_QUIESCENT_TIME} :
                                        60;
    $self->{temp_dir} =  defined $arg{TEMP_DIR} ?
                                 $arg{TEMP_DIR} :
                                 '/var/tmp';
    # local_push_dir is the ftp server chroot directory concatenated
    # with ftp_push_dir
    $self->{local_push_dir} =  defined $arg{LOCAL_PUSH_DIR} ?
                               $arg{LOCAL_PUSH_DIR} :
                               '';
    # Allow local_host value to be overridden for testing,
    # otherwise use the value returned by the hostname function
    if ( defined $arg{LOCAL_HOST_NAME} ) {
        $self->{local_host} = $arg{LOCAL_HOST_NAME};
    } else {
        my $local_host = Sys::Hostname::hostname();
        $self->{local_host} = $local_host;
    }

    bless( $self, $class );
    $self->_init(%arg);

    return $self;
}


sub login {
    my ( $self ) = @_;

    return 1;
}


sub fetch_ftp_file {
    my ( $self, %arg ) = @_;

    # Poll for a file being pushed to a (possibly) remote ftp directory,
    # and once it is found, transfer it to a local directory if it is not
    # already local.

    my $pushed_file = $arg{PUSHED_FILE};
    my $max_attempts_for_appearing = ( defined $arg{PULL_TIMEOUT} ) ?
        $arg{PULL_TIMEOUT} : 30;

    my $local_dir = $self->getLocalPushDir();
    my $local_host = $self->getLocalHost();
    my $ftp_host = $self->getFtpPushHost();
    my $ftp_quiescent_time = $self->getFtpPushQuiescentTime();
    my $sleep_seconds;
    my $max_attempts_for_starting;
    if ( $local_dir && (-d $local_dir) &&
        $local_host && ( $ftp_host =~ /^$local_host/ ) ) {
        my $local_file = "$local_dir/$pushed_file";
        $sleep_seconds = 10;
        $max_attempts_for_starting = int( $ftp_quiescent_time / $sleep_seconds ) + 1;

        # Look for the pushed file to show up in the local directory
        my $found_it;
        for (my $attempt=1; $attempt <= $max_attempts_for_appearing; $attempt++) {
            if ( -f $local_file ) {
                $found_it = 1;
                last;
            }
            sleep $sleep_seconds;
        }
        unless ( $found_it ) {
            $self->{_error} = "Timed out after waiting " .
                              ( $max_attempts_for_appearing * $sleep_seconds ) .
                              " seconds for $local_file to appear";
            return;
        }

        # The file shows up, but the push may not have completed yet.
        # Poll until the size is at least the smallest size a
        # gzipped file can be (22 bytes).
        # Set the number of attempts to see if the file has started to be
        # transferred so that the maximum time to wait is the configured
        # quiescent time.
        my $pushed_size1 = 0;
        my $pushed_size2 = -s $local_file;
        for (my $attempt=1; $attempt <= $max_attempts_for_starting; $attempt++) {
            last if ( $pushed_size2 > 21 );
            sleep $sleep_seconds;
            $pushed_size2 = -s $local_file;
        }
        unless ( $pushed_size2 > 21 ) {
            $self->{_error} = "Timed out after waiting $ftp_quiescent_time" .
                              " seconds for transfer to $local_file";
            return;
        }

        # Poll again until the size
        # remains the same for a particular duration (the quiescent period),
        # at which point assume that the push has completed.
        while ( $pushed_size1 != $pushed_size2 ) {
            sleep $ftp_quiescent_time;
            $pushed_size1 = $pushed_size2;
            $pushed_size2 = -s $local_file;
        }

        return $local_file;
    } else {
        # Allow longer wait time for ftp poll, since both dotchart and giovanni
        # have switched to ftp-pull mechanism and database query runs much
        # longer.
        $sleep_seconds = 60;
        $max_attempts_for_starting = int( $ftp_quiescent_time / $sleep_seconds ) + 1;

        my $ftp_user = $self->getFtpPushUser();
        my $ftp_pwd = $self->getFtpPushPwd();
        # my ( $ftp_host, $ftp_path ) = $pushed_file =~ m|.+://(.+?)/(.+)$|;
        my ( $ftp_host, $ftp_path );
        if ( $pushed_file =~ m|.+://(.+?)/(.+)$| ) {
            $ftp_host = $1;
            $ftp_path = $2;
        } else {
            $ftp_host = $self->getFtpPushHost();
            $ftp_path = $self->getFtpPushDir();
        }
        my $pull_file = basename( $ftp_path );
        my $ftp_dir = "/" . dirname( $ftp_path );

        my $ftp;
        if ( $ENV{FTP_FIREWALL} ) {
            my $firewall = $ENV{FTP_FIREWALL};
            my $firewallType = defined $ENV{FTP_FIREWALL_TYPE}
                               ? $ENV{FTP_FIREWALL_TYPE} : 1;
            my $ftpPassive = defined $ENV{FTP_PASSIVE}
                             ? $ENV{FTP_PASSIVE} : 1;
            $ftp = Net::FTP->new( $ftp_host,
                                  Firewall     => $firewall,
                                  FirewallType => $firewallType,
                                  Passive      => $ftpPassive );
        } else {
            $ftp = Net::FTP->new( $ftp_host );
        }
        unless ( $ftp ) {
            $self->{_error} = "Could not ftp to $ftp_host";
            return;
        }
        unless ( $ftp->login( $ftp_user, $ftp_pwd ) ) {
            $self->{_error} = "Could not login to $ftp_host as $ftp_user: " .
                              $ftp->message;
            $ftp->quit();
            return;
        }
        unless ( $ftp->cwd( $ftp_dir ) ) {
            $self->{_error} = "Could not cwd to $ftp_dir on $ftp_host: " .
                              $ftp->message;
            $ftp->quit();
            return;
        }

        # Look for the pushed file to show up in a directory listing
        my @dir_listing;
        my $found_it;
        for (my $attempt=1; $attempt <= $max_attempts_for_appearing; $attempt++) {
            @dir_listing = $ftp->ls();
            # Since we cannot distinguish between an empty directory and
            # other conditions, such as no read permission, we won't
            # do any error checking after the ls command is issued.
            my %found_files;
            @found_files{@dir_listing} = @dir_listing;
            if ( exists $found_files{ $pull_file } ) {
                $found_it = 1;
                last;
            }
            sleep $sleep_seconds;
        }
        unless ( $found_it ) {
            $self->{_error} = "Timed out after waiting " .
                              ( $max_attempts_for_appearing * $sleep_seconds ) .
                              " seconds for $pull_file to appear in $ftp_dir" .
                              " on $ftp_host";
            $ftp->quit();
            return;
        }

        # The file shows up in a directory listing, but the push may not have
        # completed yet. Poll until the size is at least the smallest size a
        # gzipped file can be (22 bytes).
        # Set the number of attempts to see if the file has started to be
        # transferred so that the maximum time to wait is the configured
        # quiescent time.
        $ftp->binary();
        my $pushed_size1 = 0;
        my $pushed_size2 = $ftp->size( $pull_file );
        for (my $attempt=1; $attempt <= $max_attempts_for_starting; $attempt++) {
            last if ( $pushed_size2 > 21 );
            sleep $sleep_seconds;
            $pushed_size2 = $ftp->size( $pull_file );
        }
        unless ( $pushed_size2 > 21 ) {
            $self->{_error} = "Timed out after waiting $ftp_quiescent_time" .
                              " seconds for transfer of $pull_file to $ftp_dir" .
                              " to begin on $ftp_host";
            $ftp->quit();
            return;
        }

        # Poll again until the size
        # remains the same for a particular duration (the quiescent period),
        # at which point assume that the push has completed.
        while ( $pushed_size1 != $pushed_size2 ) {
            sleep $ftp_quiescent_time;
            $pushed_size1 = $pushed_size2;
            $pushed_size2 = $ftp->size( $pull_file );
        }

        # The push has completed. Transfer to a local directory.
        my $temp_dir = $self->getTempDir();
        my $local_file = "$temp_dir/$pull_file";
        my $transferred_file = $ftp->get( $pull_file, $local_file );
        $ftp->quit();
        if ( $transferred_file ne $local_file ) {
            $self->{_error} = "Error transferring $pull_file to $local_file: " .
                              $ftp->message;
            return;
        }

        my $transferred_size = -s $local_file;
        if ( $transferred_size != $pushed_size2 ) {
            $self->{_error} = "Remote size = $pushed_size2, transferred size = $transferred_size\n";
            return;
        }

        return $local_file;
    }
}

sub fetch_sftp_file {
    my ( $self, %arg ) = @_;

    # Poll for a file being pushed to a (possibly) remote ftp directory,
    # and once it is found, transfer it to a local directory if it is not
    # already local.

    my $pushed_file = $arg{PUSHED_FILE};
    my $max_attempts_for_appearing = ( defined $arg{PULL_TIMEOUT} ) ?
        $arg{PULL_TIMEOUT} : 30;

    my $local_dir = $self->getLocalPushDir();
    my $local_host = $self->getLocalHost();
    my $ftp_host = $self->getFtpPushHost();
    my $ftp_quiescent_time = $self->getFtpPushQuiescentTime();
    my $sleep_seconds;
    my $max_attempts_for_starting;
    if ( $local_dir && (-d $local_dir) &&
        $local_host && ( $ftp_host =~ /^$local_host/ ) ) {
        my $local_file = "$local_dir/$pushed_file";
        $sleep_seconds = 10;
        $max_attempts_for_starting = int( $ftp_quiescent_time / $sleep_seconds ) + 1;

        # Look for the pushed file to show up in the local directory
        my $found_it;
        for (my $attempt=1; $attempt <= $max_attempts_for_appearing; $attempt++) {
            if ( -f $local_file ) {
                $found_it = 1;
                last;
            }
            sleep $sleep_seconds;
        }
        unless ( $found_it ) {
            $self->{_error} = "Timed out after waiting " .
                              ( $max_attempts_for_appearing * $sleep_seconds ) .
                              " seconds for $local_file to appear";
            return;
        }

        # The file shows up, but the push may not have completed yet.
        # Poll until the size is at least the smallest size a
        # gzipped file can be (22 bytes).
        # Set the number of attempts to see if the file has started to be
        # transferred so that the maximum time to wait is the configured
        # quiescent time.
        my $pushed_size1 = 0;
        my $pushed_size2 = -s $local_file;
        for (my $attempt=1; $attempt <= $max_attempts_for_starting; $attempt++) {
            last if ( $pushed_size2 > 21 );
            sleep $sleep_seconds;
            $pushed_size2 = -s $local_file;
        }
        unless ( $pushed_size2 > 21 ) {
            $self->{_error} = "Timed out after waiting $ftp_quiescent_time" .
                              " seconds for transfer to $local_file";
            return;
        }

        # Poll again until the size
        # remains the same for a particular duration (the quiescent period),
        # at which point assume that the push has completed.
        while ( $pushed_size1 != $pushed_size2 ) {
            sleep $ftp_quiescent_time;
            $pushed_size1 = $pushed_size2;
            $pushed_size2 = -s $local_file;
        }

        return $local_file;
    } else {
        # Allow longer wait time for sftp poll, since both dotchart and mirador
        # have switched to ftp-pull mechanism and database query runs much longer.
        $sleep_seconds = 60;
        $max_attempts_for_starting = int( $ftp_quiescent_time / $sleep_seconds ) + 1;

        my ( $sftp_host, $sftp_path );
        if ( $pushed_file =~ m|.+://(.+?)/(.+)$| ) {
            $sftp_host = $1;
            $sftp_path = $2;
        } else {
            $sftp_host = $self->getFtpPushHost();
            $sftp_path = $self->getFtpPushDir();
        }
        my $pull_file = basename($sftp_path);
        my $sftp_dir = "/" . dirname($sftp_path);
        my $remote_file = "$sftp_dir/$pull_file";
        my $temp_dir = $self->getTempDir();
        my $local_file = "$temp_dir/$pull_file";

        # sftp login to partner host
        my $sftp = S4PA::Receiving::SftpConnect($sftp_host);
        if ($sftp->error) {
            $self->{_error} = "Could not sftp to $sftp_host";
            return;
        }

        my $found_it;
        for (my $attempt=1; $attempt <= $max_attempts_for_appearing; $attempt++) {
            # check if target file was created
            my $entry = $sftp->stat($remote_file);
            if ($entry) {
                # make sure the target file size does not change
                my $size = $entry->{size};
                sleep $sleep_seconds;
                my $new_entry = $sftp->stat($remote_file);
                my $new_size = $new_entry->{size};
                if ($size == $new_size) {
                    $found_it = 1;
                    last;
                }
            }
            sleep $sleep_seconds;
        }

        unless ($found_it) {
            $self->{_error} = "Timed out after waiting " .
                              ( $max_attempts_for_appearing * $sleep_seconds ) .
                              " seconds for $pull_file to appear in $sftp_dir" .
                              " on $sftp_host";
            $sftp->disconnect();
            return;
        }

        # transfer file to local directory without the remote file timestamp
        $sftp->get($remote_file, $local_file, perm => 0644, copy_time => 0);
        if ($sftp->error) {
            $self->{_error} = "Error transferring $pull_file to $local_file: " .
                              $sftp->error;
            return;
        }
        $sftp->disconnect();
        return $local_file;
    }
}


sub fetch_local_file {
    my ( $self, %arg ) = @_;

    # Poll for a file being staged at local ftp directory,
    # and once it is found, copy it to a local directory if it is not
    # already local.

    my $pushed_file = $arg{PUSHED_FILE};
    my $max_attempts_for_appearing = ( defined $arg{PULL_TIMEOUT} ) ?
        $arg{PULL_TIMEOUT} : 30;

    my $local_dir = $self->getLocalPushDir();
    my $local_host = $self->getLocalHost();
    my $ftp_host = $self->getFtpPushHost();
    my $ftp_quiescent_time = $self->getFtpPushQuiescentTime();
    my $sleep_seconds;
    my $sleep_seconds = 60;
    my $max_attempts_for_starting = int( $ftp_quiescent_time / $sleep_seconds ) + 1;

    my ( $host, $path );
    if ( $pushed_file =~ m|.+://(.+?)/(.+)$| ) {
        $host = $1;
        $path = $2;
    } else {
        $host = $self->getFtpPushHost();
        $path = $self->getFtpPushDir();
    }
    my $pull_file = basename($path);
    my $pull_dir = "/" . dirname($path);
    my $remote_file = "$pull_dir/$pull_file";
    my $temp_dir = $self->getTempDir();
    my $local_file = "$temp_dir/$pull_file";

    my $found_it;
    for (my $attempt=1; $attempt <= $max_attempts_for_appearing; $attempt++) {
        # check if target file was created
        if (-f $remote_file) {
            my $stat = stat($remote_file);
            # make sure the target file size does not change
            my $size = $stat->size;
            sleep $sleep_seconds;
            my $new_stat = stat($remote_file);
            my $new_size = $new_stat->size;
            if ($size == $new_size) {
                $found_it = 1;
                last;
            }
        }
        sleep $sleep_seconds;
    }

    unless ($found_it) {
        $self->{_error} = "Timed out after waiting " .
                         ( $max_attempts_for_appearing * $sleep_seconds ) .
                         " seconds for $pull_file to appear in $pull_dir";
        return;
    }

    # copy file to local directory without the remote file timestamp
    if (File::Copy::copy($remote_file, $local_file)) {
        return $local_file;
    } else {
        $self->{_error} = "Error copying $remote_file to $local_file.";
    }
}


sub AUTOLOAD {
    my ( $self, @arg ) = @_;
    if ( $AUTOLOAD =~ /.*::getLocalHost/ ) {
        return $self->{local_host};
    } elsif ( $AUTOLOAD =~ /.*::getLocalPushDir/ ) {
        return $self->{local_push_dir};
    } elsif ( $AUTOLOAD =~ /.*::getTempDir/ ) {
        return $self->{temp_dir};
    } else {
        die "Method $AUTOLOAD not supported\n";
    }
}
1;


package S4PA::Reconciliation::ECHO;
@S4PA::Reconciliation::ECHO::ISA = qw( S4PA::Reconciliation );

use vars '$AUTOLOAD';

sub _init {
    my ( $self, %arg ) = @_;

    $self->{_login} = 0;
    $self->{large_count_threshold} = defined $arg{LARGE_COUNT_THRESHOLD} ?
                                             $arg{LARGE_COUNT_THRESHOLD} :
                                             500;
    $self->{client}->{username} = defined $arg{USERNAME} ?
                                  $arg{USERNAME} :
                                  'guest';
    $self->{client}->{password} = defined $arg{PASSWORD} ?
                                  $arg{PASSWORD} :
                                  'guest';
    $self->{client}->{provider} = defined $arg{PROVIDER} ?
                                  $arg{PROVIDER} :
                                  undef;
    $self->{client}->{endpoint_root} = defined $arg{ENDPOINT_ROOT} ?
                                       $arg{ENDPOINT_ROOT} :
                                       'https://api.echo.nasa.gov/echo-v10/';
    $self->{client}->{service_timeout} = defined $arg{SERVICE_TIMEOUT} ?
                                         $arg{SERVICE_TIMEOUT} :
                                         600;
    my $echo_version = 10;
    $echo_version = $1 if $self->{client}->{endpoint_root} =~ m#/echo-v(\d+)#;

    $self->{client}->{service_ns} = defined $arg{SERVICE_NS} ?
                                    $arg{SERVICE_NS} :
                                   "http://echo.nasa.gov/echo/v${echo_version}";
    $self->{client}->{types_ns} = defined $arg{TYPES_NS} ?
                                  $arg{TYPES_NS} :
                                 "http://echo.nasa.gov/echo/v${echo_version}/types";
}


sub login {
    my ( $self ) = @_;

    my $usernameElement = $self->soapServiceNsData( NAME  => 'username',
                                                    TYPE  => 'echoType:StringMax50',
                                                    VALUE => $self->getUsername() );
    my $passwordElement = $self->soapServiceNsData( NAME  => 'password',
                                                    TYPE  => 'echoType:StringMax1000',
                                                    VALUE => $self->getPassword() );
    my $clientIdElement = $self->soapTypesNsData( NAME  => 'ClientId',
                                                  TYPE  => 'echoType:StringMax50',
                                                  VALUE => $self->getProvider() );
    my $hostname = Sys::Hostname::hostname();
    my $packed_ip_address = (gethostbyname( $hostname ))[4];
    my $ip_address = join('.', unpack('C4', $packed_ip_address));
    my $userIpAddress = $self->soapTypesNsData( NAME  => 'UserIpAddress',
                                                TYPE  => 'echoType:StringMax39',
                                                VALUE => $ip_address );
    # The string identifier of the ECHO client used to make this request
    my $clientInfoElement = $self->soapServiceNsData( NAME  => 'clientInfo',
                                                      VALUE =>
                      \SOAP::Data->value( $clientIdElement, $userIpAddress ) );
    # Name of the user an Admin wants to act as,
    # null for non-ECHO-administrator users
    my $actAsUserNameElement = $self->soapServiceNsData( NAME  => 'actAsUserName',
                                                         TYPE  => 'echoType:StringMax50',
                                                         VALUE => undef );
    # Provider the user wants to act as,
    # null for guests and registered users with no ProviderRoles
    my $behalfOfProviderElement = $self->soapServiceNsData( NAME  => 'behalfOfProvider',
                                                            TYPE  => 'echoType:StringMax50',
                                                            VALUE => $self->getProvider() );

    # Execute the Login operation of the Authentication service
    my $soap = SOAP::Lite
        ->uri( $self->getServiceNs() )
        ->autotype(0)
        ->proxy( $self->getEndpointRoot() . 'AuthenticationServicePortImpl',
                 timeout => $self->getServiceTimeout() );
    # We use eval here to prevent a die on a transport error
    my $som = eval {
        $soap->Login( $usernameElement, $passwordElement, $clientInfoElement,
                      $actAsUserNameElement, $behalfOfProviderElement );
    };
    if ( $@ ) {
        $self->{_error} = "$@";
    } elsif ( $som->fault ) {
        $self->{_error} = $som->fault->{faultstring};
    } else {
        my $token = $som->result();
        my $tokenElement = $self->soapServiceNsData( NAME  => 'token',
                                                     TYPE  => 'echoType:StringMax200',
                                                     VALUE => $token );
        $self->{client}->{token} = $tokenElement;
    }
    $self->{_login} = $self->onError() ? 0 : 1;

    return $self->{_login};
}


sub logout {
    my ( $self ) = @_;

    if ( $self->loggedIn() ) {
        my $serviceName = 'AuthenticationServicePortImpl';
        my $response = SOAP::Lite->uri( $self->getServiceNs() )
            ->proxy( $self->getEndpointRoot() . $serviceName,
                     timeout => $self->getServiceTimeout() )
            ->outputxml( 1 )
            ->Logout( $self->getTokenElement() );
        $self->{_login} = 0;
        print "Logout response: $response\n" if $self->onDebug();
    }
}


sub getPartnerDatasetVersions {
    my ( $self, %arg ) = @_;

    unless ( $self->loggedIn() ) {
        my $routine = (caller(0))[3];
        print STDERR "$routine(): not logged in!";
        return;
    }

    my $serviceNs = $self->getServiceNs();
    my $endpointRoot = $self->getEndpointRoot();
    my $provider = $self->getProvider();
    my $dataCenterId = ( defined $provider ) ?
                       "<value>$provider</value>" :
                       '<all/>';
    my $query = qq(
<s0:query xmlns:s0=\"$serviceNs\"><![CDATA[<?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE query SYSTEM "$endpointRoot/echo/dtd/IIMSAQLQueryLanguage.dtd">
      <query>
        <for value="collections"/>
        <dataCenterId>$dataCenterId</dataCenterId>
        <where>
          <collectionCondition>
            <shortName><value>'$arg{SHORTNAME}'</value></shortName>
          </collectionCondition>
        </where>
      </query>
    ]]>
</s0:query>
);

    my $queryElement = SOAP::Data->type( xml => $query );
    my $queryResultTypeElement = $self->soapServiceNsData( NAME  => 'queryResultType',
                                                           VALUE => 'HITS' );
    my $iteratorSizeElement = $self->soapServiceNsData( NAME  => 'iteratorSize',
                                                        TYPE  => 'int',
                                                        VALUE => 0 );
    my $cursorElement = $self->soapServiceNsData( NAME  => 'cursor',
                                                  TYPE  => 'int',
                                                  VALUE => 0 );
    my $maxResultsElement = $self->soapServiceNsData( NAME  => 'maxResults',
                                                      TYPE  => 'int',
                                                      VALUE => 0 );
    my $metadataAttributeElement = $self->soapServiceNsData(
                                                NAME => 'metadataAttributes',
                                                VALUE => '' );

    # Execute the ExecuteQuery operation of the Catalog service
    my $som = SOAP::Lite
        ->uri( $self->getServiceNs() )
        ->proxy( $self->getEndpointRoot . 'CatalogServicePortImpl',
                 timeout => $self->getServiceTimeout() )
        ->autotype(0)
        ->ExecuteQuery( $self->getTokenElement(), $queryElement,
                        $queryResultTypeElement, $iteratorSizeElement,
                        $cursorElement, $maxResultsElement,
                        $metadataAttributeElement );

    if ( $som->fault ) {
        $self->{_error} = "Error executing catalog query: "
            . $som->fault->{faultstring};
        return;
    }

#    my $hits = $som->valueof( '//Hits/Size' );
    my $resultSetGuid = $som->valueof( '//ResultSetGuid' );

    my $resultSetGuidElement = $self->soapServiceNsData( NAME  => 'resultSetGuid',
                                                         TYPE  => 'echoType:Guid',
                                                         VALUE => $resultSetGuid );
    my $shortNameItemElement = $self->soapTypesNsAttrData( NAME => 'ShortName',
                                                           TYPE => 'STRING' );
    my $versionIdItemElement = $self->soapTypesNsAttrData( NAME => 'VersionId',
                                                           TYPE => 'STRING' );
    my $dataSetIdItemElement = $self->soapTypesNsAttrData( NAME => 'DataSetId',
                                                           TYPE => 'STRING' );
    my $metadataAttrElement = $self->soapServiceNsData( NAME  => 'metadataAttributes',
                                                        VALUE =>
                                                    \SOAP::Data->value(
                                                      $shortNameItemElement,
                                                      $versionIdItemElement,
                                                      $dataSetIdItemElement ) );
    my $iteratorSize = 2000;
    $iteratorSizeElement = $self->soapServiceNsData( NAME  => 'iteratorSize',
                                                     TYPE  => 'int',
                                                     VALUE => $iteratorSize );
    $cursorElement = $self->soapServiceNsData( NAME  => 'cursor',
                                               TYPE  => 'int',
                                               VALUE => 1 );
    $som = SOAP::Lite
        ->uri( $self->getServiceNs() )
        ->proxy( $self->getEndpointRoot . 'CatalogServicePortImpl',
                 timeout => $self->getServiceTimeout() )
        ->autotype(0)
        ->GetQueryResults( $self->getTokenElement(), $resultSetGuidElement,
                           $metadataAttrElement, $iteratorSizeElement,
                           $cursorElement );
    if ( $som->fault ) {
        $self->{_error} = "Error getting catalog query results: " .
            $som->fault->{faultstring};
        return;
    }

    my $result = $som->valueof( '//ReturnData' );
    my $xmlParser = XML::LibXML->new();
    my $dom = $xmlParser->parse_string( $result );
    my $doc = $dom->documentElement();

    my @versions;
    foreach my $node ( $doc->findnodes( '//CollectionMetaData' ) ) {

        my ( $shortNameNode ) = $node->getChildrenByTagName( 'ShortName' );
        next unless defined $shortNameNode;
        my $shortName = $shortNameNode->textContent();

        my ( $versionIdNode ) = $node->getChildrenByTagName( 'VersionId' );
        my $versionId = $versionIdNode ? $versionIdNode->textContent() : '';

        my ( $dataSetIdNode ) = $node->getChildrenByTagName( 'DataSetId' );
        my $dataSetId = $dataSetIdNode->textContent();

        # Add the version to the list of versions for the shortName
        push @versions, $versionId;

        # Save the DataSetId asscoiated with the version
        $self->{echoDataSetIds}->{$versionId} = $dataSetId;
    }

    return @versions;
}


sub getPartnerGranuleCount {
    my ( $self, %arg ) = @_;

    unless ( $self->loggedIn() ) {
        my $routine = (caller(0))[3];
        $self->{_error} = "$routine(): not logged in!";
        return 0;
    }

    my $dataSetId = $self->getEchoDatasetId( VERSION => $arg{VERSION} );
    $dataSetId =~ s/>/&gt;/g;
    $dataSetId =~ s/</&lt;/g;
    $dataSetId =~ s/"/&quot;/g;
    $dataSetId =~ s/'/&apos;/g;
    $dataSetId =~ s/&/&amp;/g;
    my $serviceNs = $self->getServiceNs();
    my $endpointRoot = $self->getEndpointRoot();
    my $provider = $self->getProvider();
    my $dataCenterId = ( defined $provider ) ?
                       "<value>$provider</value>" :
                       '<all/>';
    my $query = qq(
<s0:query xmlns:s0=\"$serviceNs\"><![CDATA[<?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE query SYSTEM "$endpointRoot/echo/dtd/IIMSAQLQueryLanguage.dtd">
      <query>
        <for value="granules"/>
        <dataCenterId>$dataCenterId</dataCenterId>
        <where>
           <granuleCondition>
             <dataSetId><value>'$dataSetId'</value></dataSetId>
           </granuleCondition>
        </where>
      </query>
    ]]>
</s0:query>
);

    my $queryElement = SOAP::Data->type( xml => $query );
    my $queryResultTypeElement = $self->soapServiceNsData( NAME  => 'queryResultType',
                                                           TYPE  => '',
                                                           VALUE => 'HITS' );
    my $iteratorSizeElement = $self->soapServiceNsData( NAME  => 'iteratorSize',
                                                        TYPE  => 'int',
                                                        VALUE => 0 );
    my $cursorElement = $self->soapServiceNsData( NAME  => 'cursor',
                                                  TYPE  => 'int',
                                                  VALUE => 0 );
    my $maxResultsElement = $self->soapServiceNsData( NAME  => 'maxResults',
                                                      TYPE  => 'int',
                                                      VALUE => 0 );
    my $metadataAttributeElement = $self->soapServiceNsData(
                                                NAME  => 'metadataAttributes',
                                                VALUE => '' );

    # Execute the ExecuteQuery operation of the Catalog service
    my $som = SOAP::Lite
        ->uri( $self->getServiceNs() )
        ->proxy( $self->getEndpointRoot . 'CatalogServicePortImpl',
                 timeout => $self->getServiceTimeout() )
        ->autotype(0)
        ->ExecuteQuery( $self->getTokenElement(), $queryElement,
                        $queryResultTypeElement, $iteratorSizeElement,
                        $cursorElement, $maxResultsElement,
                        $metadataAttributeElement );

    if ( $som->fault ) {
        $self->{_error} = "Error executing catalog query: "
            . $som->fault->{faultstring};
        return;
    }

    my $hits = $som->valueof( '//Hits/Size' );
    my $resultSetGuid = $som->valueof( '//ResultSetGuid' );

    return ( $hits, $resultSetGuid );
}


sub getPartnerSmallGranuleList {
    my ( $self, %arg ) = @_;

    my $resultSetGuid = $arg{RESULTSETGUID};
    my $hits = $arg{HITS};

    my $resultSetGuidElement = $self->soapServiceNsData( NAME  => 'resultSetGuid',
                                                         TYPE  => 'echoType:Guid',
                                                         VALUE => $resultSetGuid );
    my $granuleURItemElement = $self->soapTypesNsAttrData( NAME => 'GranuleUR',
                                                           TYPE => 'STRING' );
    my $onlineAccessURLsItemElement = $self->soapTypesNsAttrData( NAME => 'OnlineAccessURLs',
                                                                  TYPE => 'STRING' );
    my $metadataAttrElement = $self->soapServiceNsData( NAME  => 'metadataAttributes',
                                                        VALUE =>
                                             \SOAP::Data->value(
                                               $granuleURItemElement,
                                               $onlineAccessURLsItemElement ) );
    my $iteratorSize = 2000;
    my $iteratorSizeElement = $self->soapServiceNsData( NAME  => 'iteratorSize',
                                                        TYPE  => 'int',
                                                        VALUE => $iteratorSize );


    my %partner_granules;
    my $cursor = 1;
    my $xmlParser = XML::LibXML->new();
    while ( $cursor <= $hits ) {
        my $cursorElement = $self->soapServiceNsData( NAME  => 'cursor',
                                                      TYPE  => 'int',
                                                      VALUE => $cursor );
        my $som = SOAP::Lite
            ->uri( $self->getServiceNs() )
            ->proxy( $self->getEndpointRoot . 'CatalogServicePortImpl',
                     timeout => $self->getServiceTimeout() )
            ->autotype(0)
            ->GetQueryResults( $self->getTokenElement(), $resultSetGuidElement,
                               $metadataAttrElement, $iteratorSizeElement,
                               $cursorElement );
        if ( $som->fault ) {
            $self->{_error} = "Error getting catalog query results: " .
                $som->fault->{faultstring};
            return;
        }

        my $result = $som->valueof( '//ReturnData' );
        my $dom = $xmlParser->parse_string( $result );
        my $doc = $dom->documentElement();
        my $local_host = $self->getLocalHost();

        foreach my $node ( $doc->findnodes( '//GranuleURMetaData' ) ) {

            my ( $GranuleURNode ) = $node->getChildrenByTagName( 'GranuleUR' );
            next unless defined $GranuleURNode;
            my $GranuleUR = $GranuleURNode->textContent();
            my ( $ds, $ver, $gran_ur_name ) = $GranuleUR =~ /([^. ]+)\.([^: ]+)\:(\S+)/;
            # Instead of the GranuleUR, we will obtain the OnlineAccessUrls.
            # This allows us to retrieve the information about all of the
            # files in the partner's metadata, which can be used to match
            # against the contents of the granule.db file, which contain all
            # of the files in the S4PA instance.
            my ( $OnlineAccessUrlsNode ) = $node->getChildrenByTagName( 'OnlineAccessURLs' );
            # There should always be an OnlineAccessUrls node
            next unless defined $OnlineAccessUrlsNode;
            foreach my $oa_node ( $OnlineAccessUrlsNode->getChildrenByTagName( 'OnlineAccessURL' ) ) {
                my $OnlineAccessUrl = $oa_node->textContent();
                # Get rid of any leading or trailing whitespace
                $OnlineAccessUrl =~ s/^\s+//;
                $OnlineAccessUrl =~ s/\s+$//;
                my ( $online_host, $online_file ) = $OnlineAccessUrl =~ m|.+://(.+?)/.+/(.+)$|;
                ## Ignore files whose URL host does not match the S4PA host

                # Due to hosts/instances consolidation and host domain relocation,
                # published granule might have different hostname than the current
                # hostname. Besides, there are no more dataset that are split onto
                # different instances. Therefore, there is no need to compare 
                # hostname any more. Comparing is actually breaking the reconciliation.
                # next unless ( $online_host =~ /^$local_host/ );

                # With multi-file granules it is possible for the
                # same file to appear in more than one granule. Therefore,
                # we save the filenames in a hash in order to eliminate
                # duplications. Doing this, we won't be able to detect if
                # the partner has a granule more than once, we will assume
                # that they don't.

                if ( $self->reconcileAllFiles() ) {
                    # Return the name of every online file the partner has
                    $partner_granules{$online_file} = 1;
                } else {

                    # Ignore any OPeNDAP URLs
                    next if $OnlineAccessUrl =~ /opendap/i;
                    next if $OnlineAccessUrl =~ /thredds/i;

                    # Return only the names of the online files that match
                    # the granule_ur name, i.e. those that are granule files
                    next unless ( $online_file eq $gran_ur_name );
                    $partner_granules{$gran_ur_name} = 1;
                }
            }
        }
        $cursor += $iteratorSize;
    }

    return sort( keys %partner_granules );
}


sub getPartnerLargeGranuleList {
    my ( $self, %arg ) = @_;

    my %partner_granules;
    my $dataSetId = $self->getEchoDatasetId( VERSION => $arg{VERSION} );

    my $ftp_host = $self->getFtpPushHost();
    my $ftp_user = $self->getFtpPushUser();
    my $ftp_pwd = $self->getFtpPushPwd();
    my $ftp_dir = $self->getFtpPushDir();
    my $ftp_uri = "ftp://$ftp_user:$ftp_pwd\@${ftp_host}${ftp_dir}";

    # ECHO seems to require that the URI end in '/' or else it fails to
    # push it properly.
    $ftp_uri .= '/' unless $ftp_uri =~ /\/$/;

    my $datasetIdElement = $self->soapServiceNsData( NAME  => 'datasetId',
                                                     TYPE  => 'echoType:StringMax1030',
                                                     VALUE => $dataSetId );

    my $temporalRangesElement = $self->soapServiceNsData( NAME  => 'temporalRanges',
                                                          VALUE => '' );

    my $availableOnlineElement = $self->soapServiceNsData( NAME  => 'availableOnline',
                                                           TYPE  => 'boolean',
                                                           VALUE => 1 );

    my $browseAvailableElement = $self->soapServiceNsData( NAME  => 'browseAvailable',
                                                           TYPE  => 'boolean',
                                                           VALUE => 0 );

    my $ftpUrlElement = $self->soapServiceNsData( NAME  => 'ftpUrl',
                                                  TYPE  => 'anyURI',
                                                  VALUE => $ftp_uri );

    my $som = SOAP::Lite
        ->uri( $self->getServiceNs() )
        ->proxy( $self->getEndpointRoot . 'DataManagementServicePortImpl',
                 timeout => $self->getServiceTimeout() )
        ->autotype(0)
        ->GetDatasetInformation( $self->getTokenElement(),
                                 $datasetIdElement,
                                 $temporalRangesElement,
                                 $availableOnlineElement,
                                 $browseAvailableElement,
                                 $ftpUrlElement );
    if ( $som->fault ) {
        $self->{_error} = "Error getting GetDatasetInformation results: " .
            $som->fault->{faultstring};
        return;
    }

    # Get name of compressed file created by ECHO containing dataset info
    my $compressed_info_file = $som->valueof('//result');

    my $local_file = $self->fetch_ftp_file( PUSHED_FILE => $compressed_info_file );
    return if $self->onError();

    # Uncompress the file
#    $local_file =~ s/\.gz$//;
#    open( FH, "| gunzip - > $local_file" )
#        or S4P::perish( 17, "Could not open pipe to write $local_file" );
#    print FH $compressed_info;
#    close( FH ) or S4P::perish( 18, "Failed to gunzip $local_file" );
    `gunzip $local_file`;
    $local_file =~ s/\.gz$//;
    unless ( -f $local_file ) {
        $self->{_error} = "Failed to create $local_file";
        return;
    }

    my $local_host = $self->getLocalHost();

# This next section is commented out and replaced by code that uses
# XML::Twig
#
#    # Parse the file to obtain a list of files
#    my $xmlParser = XML::LibXML->new();
#    my $dom = $xmlParser->parse_file( $local_file );
#    my $doc = $dom->documentElement();
#    my ( $GranulesNode ) = $doc->getChildrenByTagName( 'Granules' );
##    foreach my $node ( $doc->findnodes( '//Granule' ) ) {
#    foreach my $node ( $GranulesNode->getChildrenByTagName( 'Granule' ) ) {
#        my ( $GranuleURNode ) = $node->getChildrenByTagName( 'GranuleUR' );
#        next unless defined $GranuleURNode;
#        my $GranuleUR = $GranuleURNode->textContent();
#        my ( $ds, $ver, $gran_ur_name ) = $GranuleUR =~ /([^. ]+)\.([^: ]+)\:(\S+)/;
#        # Instead of the GranuleUR, we will obtain the OnlineAccessUrls.
#        # This allows us to retrieve the information about all of the
#        # files in the partner's metadata, which can be used to match against
#        # the contents of the granule.db file, which contain all of the
#        # files in the S4PA instance.
#        my ( $OnlineAccessUrlsNode ) = $node->getChildrenByTagName( 'OnlineAccessUrls' );
#        # There should always be an OnlineAccessUrls node
#        next unless defined $OnlineAccessUrlsNode;
#        foreach my $oa_node ( $OnlineAccessUrlsNode->getChildrenByTagName( 'OnlineAccessUrl' ) ) {
#            my $OnlineAccessUrl = $oa_node->textContent();
#            # Get rid of any leading or trailing whitespace
#            $OnlineAccessUrl =~ s/^\s+//;
#            $OnlineAccessUrl =~ s/\s+$//;
#            my ( $online_host, $online_file ) = $OnlineAccessUrl =~ m|.+://(.+?)/.+/(.+)$|;
#            # Ignore files whose URL host does not match the S4PA host
#            next unless ( $online_host =~ /^$local_host/ );
#
#            # With multi-file granules it is possible for the
#            # same file to appear in more than one granule. Therefore,
#            # we save the filenames in a hash in order to eliminate
#            # duplications. Doing this, we won't be able to detect if
#            # the partner has a granule more than once, we will assume
#            # that they don't.

#            if ( $self->reconcileAllFiles() ) {
#                # Return the name of every online file the partner has
#                $partner_granules{$online_file} = 1;
#            } else {
#                # Return only the names of the online files that match
#                # the granule_ur name, i.e. those that are granule files
#                next unless ( $online_file eq $gran_ur_name );
#                $partner_granules{$gran_ur_name} = 1;
#            }
#        }
#    }

    # Define a reference to a subroutine that will be a handler for
    # each Granule element in the xml file.
    # Because tha handler is defined in this subroutine, it will have access
    # to the %partner_granules hash defined in this subroutine.
    my $GranuleHandler = sub {
        my ( $twig, $node ) = @_;

        my $GranuleUR = $node->first_child_text( 'GranuleUR' );
        return unless defined $GranuleUR;
        my ( $ds, $ver, $gran_ur_name ) = $GranuleUR =~ /([^. ]+)\.([^: ]+)\:(\S+)/;

        # Instead of the GranuleUR, we will obtain the OnlineAccessUrls.
        # This allows us to retrieve the information about all of the
        # files in the partner's metadata, which can be used to match against
        # the contents of the granule.db file, which contain all of the
        # files in the S4PA instance.
        my ( $OnlineAccessUrlsNode ) = $node->first_child( 'OnlineAccessUrls' );
        # There should always be an OnlineAccessUrls node
        return unless defined $OnlineAccessUrlsNode;
        foreach my $OnlineAccessUrl ( $OnlineAccessUrlsNode->children_text( 'OnlineAccessUrl' ) ) {
            # Get rid of any leading or trailing whitespace
#            $OnlineAccessUrl =~ s/^\s+//;
#            $OnlineAccessUrl =~ s/\s+$//;
            my ( $online_host, $online_file ) = $OnlineAccessUrl =~ m|.+://(.+?)/.+/(.+)$|;
            ## Ignore files whose URL host does not match the S4PA host

            # Due to hosts/instances consolidation and host domain relocation,
            # published granule might have different hostname than the current
            # hostname. Besides, there are no more dataset that are split onto
            # different instances. Therefore, there is no need to compare 
            # hostname any more. Comparing is actually breaking the reconciliation.
            # next unless ( $online_host =~ /^$local_host/ );

            # With multi-file granules it is possible for the
            # same file to appear in more than one granule. Therefore,
            # we save the filenames in a hash in order to eliminate
            # duplications. Doing this, we won't be able to detect if
            # the partner has a granule more than once, we will assume
            # that they don't.

            if ( $self->reconcileAllFiles() ) {
                # Return the name of every online file the partner has
                $partner_granules{$online_file} = 1;
            } else {

                # Ignore any OPeNDAP URLs
                next if $OnlineAccessUrl =~ /opendap/i;
                next if $OnlineAccessUrl =~ /thredds/i;

                # Return only the names of the online files that match
                # the granule_ur name, i.e. those that are granule files
                next unless ( $online_file eq $gran_ur_name );
                $partner_granules{$gran_ur_name} = 1;
            }
        }
        $twig->purge;
    };

    my $handlers = {'Granule' => $GranuleHandler};
    my $twig = new XML::Twig( TwigRoots => {'Granule' => 1},
                              TwigHandlers => $handlers );
    $twig->parsefile( $local_file );
    $twig->purge;

    unlink $local_file;

    return sort( keys %partner_granules );
}


sub getPartnerGranuleList {
    my ( $self, %arg ) = @_;

    unless ( $self->loggedIn() ) {
        my $routine = (caller(0))[3];
        $self->{_error} = "$routine(): not logged in!";
        return 0;
    }

    my ( $hits, $resultSetGuid ) = $self->getPartnerGranuleCount( VERSION => $arg{VERSION} );

    if ( wantarray ) {
        if ( $hits < $self->getLargeCountThreshold ) {
            return $self->getPartnerSmallGranuleList( RESULTSETGUID => $resultSetGuid,
                                                      HITS => $hits );
        } else {
            return $self->getPartnerLargeGranuleList( VERSION => $arg{VERSION} );
        }
    } else {
        return $hits;
    }
}


sub AUTOLOAD {
    my ( $self, @arg ) = @_;
    if ( $AUTOLOAD =~ /.*::onError/ ) {
        return ( $self->getErrorMessage() eq '' ? 0 : 1 );
    } elsif ( $AUTOLOAD =~ /.*::getErrorMessage/ ) {
        return $self->{_error};
    } elsif ( $AUTOLOAD =~ /.*::onDebug/ ) {
        return $self->{debug};
    } elsif ( $AUTOLOAD =~ /.*::reconcileAllFiles/ ) {
        return $self->{reconcile_all_files};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPushHost/ ) {
        return $self->{ftp_push_host};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPushUser/ ) {
        return $self->{ftp_push_user};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPushPwd/ ) {
        return $self->{ftp_push_pwd};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPushDir/ ) {
        return $self->{ftp_push_dir};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPushQuiescentTime/ ) {
        return $self->{ftp_push_quiescent_time};
    } elsif ( $AUTOLOAD =~ /.*::getLargeCountThreshold/ ) {
        return $self->{large_count_threshold};
    } elsif ( $AUTOLOAD =~ /.*::getEndpointRoot/ ) {
        return $self->{client}->{endpoint_root};
    } elsif ( $AUTOLOAD =~ /.*::getServiceTimeout/ ) {
        return $self->{client}->{service_timeout};
    } elsif ( $AUTOLOAD =~ /.*::getServiceNs/ ) {
        return $self->{client}->{service_ns};
    } elsif ( $AUTOLOAD =~ /.*::soapServiceNsData/ ) {
        my %arg = @arg;
        my $data = SOAP::Data->new();
        $data->uri( $self->getServiceNs() );
        $data->name( $arg{NAME} ) if defined $arg{NAME};
        if (defined $arg{TYPE}) {
            if ($arg{TYPE} =~ /(\w+):(\w+)/) {
                # TYPE has a prefix, so assume the prefix corresponds to
                # the namespace $self->getTypesNs(), and add the namespace
                # attribute to the data element
                $data->attr( {"xmlns:$1" => $self->getTypesNs()} );
            }
            $data->type( $arg{TYPE} );
        }
        $data->value( $arg{VALUE} ) if defined $arg{VALUE};
        return $data;
    } elsif ( $AUTOLOAD =~ /.*::loggedIn/ ) {
        return $self->{_login};
    } elsif ( $AUTOLOAD =~ /.*::getUsername/ ) {
        return $self->{client}->{username};
    } elsif ( $AUTOLOAD =~ /.*::getPassword/ ) {
        return $self->{client}->{password};
    } elsif ( $AUTOLOAD =~ /.*::getProvider/ ) {
        return $self->{client}->{provider};
    } elsif ( $AUTOLOAD =~ /.*::getTypesNs/ ) {
        return $self->{client}->{types_ns};
    } elsif ( $AUTOLOAD =~ /.*::getTokenElement/ ) {
        return $self->{client}->{token};
    } elsif ( $AUTOLOAD =~ /.*::getEchoDatasetId/ ) {
        my %arg = @arg;
        return  $self->{echoDataSetIds}->{$arg{VERSION}};
    } elsif ( $AUTOLOAD =~ /.*::soapTypesNsData/ ) {
        my %arg = @arg;
        my $data = SOAP::Data->new();
        $data->uri( $self->getTypesNs() );
        $data->name( $arg{NAME} ) if defined $arg{NAME};
        if (defined $arg{TYPE}) {
            if ($arg{TYPE} =~ /(\w+):(\w+)/) {
                # TYPE has a prefix, so assume the prefix corresponds to
                # the namespace $self->getTypesNs(), and add the namespace
                # attribute to the data element
                $data->attr( {"xmlns:$1" => $self->getTypesNs()} );
            }
            $data->type( $arg{TYPE} );
        }
        $data->value( $arg{VALUE} ) if defined $arg{VALUE};
        return $data;
    } elsif ( $AUTOLOAD =~ /.*::soapTypesNsAttrData/ ) {
        my %arg = @arg;
        my $AttrElement = SOAP::Data->uri( $self->getTypesNs() )
                                    ->name( 'AttributeName' )
                                    ->attr( {"xmlns:echoType" => $self->getTypesNs()} )
                                    ->type( 'echoType:StringMax50' )
                                    ->value( $arg{NAME} );
        my $TypeElement = SOAP::Data->uri( $self->getTypesNs() )
                                    ->name( 'PrimitiveValueType' )
                                    ->type( '' )
                                    ->value( $arg{TYPE} );
        return SOAP::Data->uri( $self->getTypesNs() )
                         ->name( 'Item' )
                         ->value( \SOAP::Data->value( $AttrElement,
                                                      $TypeElement ) );

    } elsif ( $AUTOLOAD =~ /.*::DESTROY/ ) {
        $self->logout() if $self->loggedIn();
    } else {
        my $method = $AUTOLOAD;
        $method =~ s/.*:://;
        $method = "SUPER::$method";
        $self->$method(@_);
    }
}
1;


package S4PA::Reconciliation::CMR;
@S4PA::Reconciliation::CMR::ISA = qw( S4PA::Reconciliation );

use vars '$AUTOLOAD';

sub _init {
    my ( $self, %arg ) = @_;

    $self->{_login} = 0;
    $self->{large_count_threshold} = defined $arg{LARGE_COUNT_THRESHOLD} ?
                                             $arg{LARGE_COUNT_THRESHOLD} :
                                             500;
    $self->{client}->{username} = defined $arg{USERNAME} ?
                                  $arg{USERNAME} :
                                  undef;
    $self->{client}->{password} = defined $arg{PASSWORD} ?
                                  $arg{PASSWORD} :
                                  undef;
    $self->{client}->{certfile} = defined $arg{CERTFILE} ?
                                  $arg{CERTFILE} :
                                  undef;
    $self->{client}->{certpass} = defined $arg{CERTPASS} ?
                                  $arg{CERTPASS} :
                                  undef;
    $self->{client}->{provider} = defined $arg{PROVIDER} ?
                                  $arg{PROVIDER} :
                                  undef;
    $self->{client}->{endpoint_root} = defined $arg{ENDPOINT_ROOT} ?
                                       $arg{ENDPOINT_ROOT} :
                                       'https://api.echo.nasa.gov/echo-rest/';
    $self->{client}->{token_uri} = defined $arg{TOKEN_URI} ?
                                       $arg{TOKEN_URI} :
                                       'https://api.launchpad.nasa.gov/icam/api/sm/v1';
    $self->{client}->{catalog_endpoint_root} = defined $arg{CATALOG_ENDPOINT_ROOT} ?
                                       $arg{CATALOG_ENDPOINT_ROOT} :
                                       'https://cmr.earthdata.nasa.gov/ingest/';
    $self->{client}->{service_timeout} = defined $arg{SERVICE_TIMEOUT} ?
                                         $arg{SERVICE_TIMEOUT} :
                                         600;
}


sub login {
    my ( $self ) = @_;

    my ($cmrToken, $errmsg);
    if (defined $self->getCertFile()) {
        my $tokenParam = {};
        $tokenParam->{LP_URI} = $self->getTokenUri();
        $tokenParam->{CMR_CERTFILE} = $self->getCertFile();
        $tokenParam->{CMR_CERTPASS} = $self->getCertPass();
        ($cmrToken, $errmsg) = S4PA::get_cmr_token($tokenParam);
        if (defined $cmrToken) {
            $self->{client}->{token} = $cmrToken;
        } else {
            $self->{_error} = "$errmsg";
        }

    } else {
        my $tokenParam = {};
        $tokenParam->{ECHO_URI} = $self->getEndpointRoot();
        $tokenParam->{CMR_USERNAME} = $self->getUsername();
        $tokenParam->{CMR_PASSWORD} = $self->getPassword();
        $tokenParam->{CMR_PROVIDER} = $self->getProvider();
        ($cmrToken, $errmsg) = S4PA::get_cmr_token($tokenParam);
        if (defined $cmrToken) {
            $self->{client}->{token} = $cmrToken;
        } else {
            $self->{_error} = "$errmsg";
        }
    }

    $self->{_login} = $self->onError() ? 0 : 1;
    return $self->{_login};
}


sub logout {
    my ( $self ) = @_;

    if ( $self->loggedIn() ) {
        $self->{_login} = 0;
    }
}


sub getPartnerDatasetVersions {
    my ( $self, %arg ) = @_;

    unless ( $self->loggedIn() ) {
        my $routine = (caller(0))[3];
        print STDERR "$routine(): not logged in!";
        return;
    }

    my $endpointRoot = $self->getCatalogEndpointRoot();
    my $provider = $self->getProvider();

    $endpointRoot =~ s#ingest/##;
    $endpointRoot .= 'search/collections.echo10';

    my $queryUrl = $endpointRoot . '?provider=' .
                   $provider . '&short_name=' . $arg{SHORTNAME};
    my $user_agent = LWP::UserAgent->new();
    $user_agent->env_proxy;

    my $request = HTTP::Request->new( 'GET',
                                      $queryUrl,
                                      [Echo_Token => $self->getTokenElement()]
                                    );

    my $response = $user_agent->request( $request );
    unless ( $response->is_success ) {
        $self->{_error} = "Error executing catalog query from $queryUrl: " .
                          $response->message;
        return;
    }

    my $xmlParser = XML::LibXML->new();
    my $dom;
    eval { $dom = $xmlParser->parse_string( $response->content ); };
    S4P::perish( 2, "Could not parse response from $queryUrl:  $@\n" ) if $@;
    my $doc = $dom->documentElement();

    my @versions;
    foreach my $node ( $doc->findnodes( '//Collection' ) ) {

        my ( $shortNameNode ) = $node->getChildrenByTagName( 'ShortName' );
        next unless defined $shortNameNode;
        my $shortName = $shortNameNode->textContent();

        my ( $versionIdNode ) = $node->getChildrenByTagName( 'VersionId' );
        my $versionId = $versionIdNode ? $versionIdNode->textContent() : '';

        my ( $dataSetIdNode ) = $node->getChildrenByTagName( 'DataSetId' );
        my $dataSetId = $dataSetIdNode->textContent();

        # Add the version to the list of versions for the shortName
        push @versions, $versionId;

        # Save the DataSetId asscoiated with the version
        $self->{cmrDataSetIds}->{$versionId} = $dataSetId;
    }

    return @versions;
}


sub getPartnerGranuleCount {
    my ( $self, %arg ) = @_;

    unless ( $self->loggedIn() ) {
        my $routine = (caller(0))[3];
        $self->{_error} = "$routine(): not logged in!";
        return 0;
    }

    my $dataSetId = $self->getCmrDatasetId( VERSION => $arg{VERSION} );
    my $endpointRoot = $self->getCatalogEndpointRoot();
    my $provider = $self->getProvider();

    $endpointRoot =~ s#ingest/##;
    $endpointRoot .= 'search/granules';
    my $queryUrl = $endpointRoot . '?provider=' . $provider . 
                   '&short_name=' . $arg{SHORTNAME} .
                   '&version=' . $arg{VERSION} .
                   '&page_size=0';
    my $user_agent = LWP::UserAgent->new();
    $user_agent->env_proxy;

    my $request = HTTP::Request->new( 'GET',
                                      $queryUrl,
                                      [Echo_Token => $self->getTokenElement()]
                                    );

    my $response = $user_agent->request( $request );
    unless ( $response->is_success ) {
        $self->{_error} = "Error executing catalog query from $queryUrl: " .
                          $response->message;
        return;
    }

    # Obtain the count of granules from the response header
    my $hits;
    $hits = $response->header( 'cmr-hits' );
    unless ( defined $hits ) {
        $hits = $response->header( 'CMR-Hits' );
    }

    return ( $hits, $dataSetId );
}


sub getPartnerSmallGranuleList {
    my ( $self, %arg ) = @_;

    my $endpointRoot = $self->getCatalogEndpointRoot();
    my $provider = $self->getProvider();

    $endpointRoot =~ s#ingest/##;
    $endpointRoot .= 'search/granules.echo10';

    # Assume that the small granule list limit is less than or equal to
    # the maximum number of granules returned for a single query
    my $maxPageSize = 2000;

    my $queryUrl = $endpointRoot . '?provider=' . $provider .
                   '&short_name=' . $arg{SHORTNAME} .
                   '&version=' . $arg{VERSION} .
                   '&page_size=' . $maxPageSize;
    my $user_agent = LWP::UserAgent->new();
    $user_agent->env_proxy;

    my $request = HTTP::Request->new( 'GET',
                                      $queryUrl,
                                      [Echo_Token => $self->getTokenElement()]
                                    );

    my $response = $user_agent->request( $request );
    unless ( $response->is_success ) {
        $self->{_error} = "Error executing catalog query from $queryUrl: " .
                          $response->message;
        return;
    }

#    my $granuleURItemElement = $self->soapTypesNsAttrData( NAME => 'GranuleUR',
#                                                           TYPE => 'STRING' );
#    my $onlineAccessURLsItemElement = $self->soapTypesNsAttrData( NAME => 'OnlineAccessURLs',
#                                                                  TYPE => 'STRING' );

    my %partner_granules;
    my $xmlParser = XML::LibXML->new();
    my $dom;
    eval { $dom = $xmlParser->parse_string( $response->content ); };
    S4P::perish( 2, "Could not parse response from $queryUrl:  $@\n" ) if $@;
    my $doc = $dom->documentElement();
    my $local_host = $self->getLocalHost();

    foreach my $node ( $doc->findnodes( '//Granule' ) ) {
#    foreach my $lnode ( $doc->findnodes( '//location' ) ) {

#        my $url = $lnode->textContent;
#        $request = HTTP::Request->new( 'GET', $url );

#        $response = $user_agent->request( $request );
#        unless ( $response->is_success ) {
#            $self->{_error} = "Error requesting $url: " .
#                          $response->message;
#            return;
#        }
#        my $gdom = $xmlParser->parse_string( $response->content );
#        my $gdoc = $dom->documentElement();
#        my $node ( $gdoc->findnodes( '/Granule' ) ) {

            my ( $GranuleURNode ) = $node->getChildrenByTagName( 'GranuleUR' );
            next unless defined $GranuleURNode;
            my $GranuleUR = $GranuleURNode->textContent();
            my ( $ds, $ver, $gran_ur_name ) = $GranuleUR =~ /([^. ]+)\.([^: ]+)\:(\S+)/;
            # Instead of the GranuleUR, we will obtain the OnlineAccessUrls.
            # This allows us to retrieve the information about all of the
            # files in the partner's metadata, which can be used to match
            # against the contents of the granule.db file, which contain all
            # of the files in the S4PA instance.
            my ( $OnlineAccessUrlsNode ) = $node->getChildrenByTagName( 'OnlineAccessURLs' );
            # There should always be an OnlineAccessUrls node
            next unless defined $OnlineAccessUrlsNode;
            foreach my $oa_node ( $OnlineAccessUrlsNode->getChildrenByTagName( 'OnlineAccessURL' ) ) {
                my $OnlineAccessUrl = $oa_node->textContent();
                # Get rid of any leading or trailing whitespace
                $OnlineAccessUrl =~ s/^\s+//;
                $OnlineAccessUrl =~ s/\s+$//;
                my ( $online_host, $online_file ) = $OnlineAccessUrl =~ m|.+://(.+?)/.+/(.+)$|;
                ## Ignore files whose URL host does not match the S4PA host

                # Due to hosts/instances consolidation and host domain relocation,
                # published granule might have different hostname than the current
                # hostname. Besides, there are no more dataset that are split onto
                # different instances. Therefore, there is no need to compare 
                # hostname any more. Comparing is actually breaking the reconciliation.
                # next unless ( $online_host =~ /^$local_host/ );

                # With multi-file granules it is possible for the
                # same file to appear in more than one granule. Therefore,
                # we save the filenames in a hash in order to eliminate
                # duplications. Doing this, we won't be able to detect if
                # the partner has a granule more than once, we will assume
                # that they don't.

                if ( $self->reconcileAllFiles() ) {
                    # Return the name of every online file the partner has
                    $partner_granules{$online_file} = 1;
                } else {

                    # Ignore any OPeNDAP URLs
                    next if $OnlineAccessUrl =~ /opendap/i;
                    next if $OnlineAccessUrl =~ /thredds/i;

                    # Return only the names of the online files that match
                    # the granule_ur name, i.e. those that are granule files
                    next unless ( $online_file eq $gran_ur_name );

                    $partner_granules{$gran_ur_name} = 1;
                }
            }
    }

    return sort( keys %partner_granules );
}


sub getPartnerLargeGranuleList {
    my ( $self, %arg ) = @_;

    my $endpointRoot = $self->getCatalogEndpointRoot();
    my $provider = $self->getProvider();

    $endpointRoot =~ s#ingest/##;
    $endpointRoot .= 'search/granules.echo10';

    my $baseQueryUrl = $endpointRoot .
                       '?provider=' . $provider .
                       '&short_name=' . $arg{SHORTNAME} .
                       '&version=' . $arg{VERSION};

    # Get the count of granules for the dataset
    my $count_queryUrl = $baseQueryUrl . '&page_size=0';
    my $user_agent = LWP::UserAgent->new();
    $user_agent->env_proxy;

    my $count_request = HTTP::Request->new( 'GET',
                                            $count_queryUrl,
                                            [Echo_Token => $self->getTokenElement()]
                                          );

    # Obtain the count of granules from the response
    my $count_response = $user_agent->request( $count_request );
    unless ( $count_response->is_success ) {
        $self->{_error} = "Error executing catalog query from $count_queryUrl: " .
                          $count_response->message;
        return;
    }

    # Obtain the count of granules from the response header
    my $hits;
    $hits = $count_response->header( 'cmr-hits' );
    unless ( defined $hits ) {
        $hits = $count_response->header( 'CMR-Hits' );
    }

    # when specified page_size=0, the response content is empty
    # my $count_dom;
    # my $xmlParser = XML::LibXML->new();
    # eval { $count_dom = $xmlParser->parse_string( $count_response->content ); };
    # S4P::perish( 2, "Could not parse response from $count_queryUrl:  $@\n" ) if $@;
    # my $count_doc = $count_dom->documentElement();
    # my $hitsNode = $count_doc->findnodes( '/results/hits' );
    # my $hits = $hitsNode->textContent if $hitsNode;
    return unless $hits;

    # The maximum number of granules returned for a single query
    my $maxPageSize = 2000;
    my $maxPageNum = 500;
    my $maxGranule = $maxPageSize * $maxPageNum;
    my $maxBatch = POSIX::ceil( $hits / $maxGranule );
    my $page_num = 0;
    my $batch_num = 0;
    my $lastBeginTime;
    # Register this dataset as huge collection when granule count is over 1 million
    my $hugeCollection = ( $hits > $maxGranule ) ? 1 : 0;

    # adding sort by beginningdatetime for all collections
    my $partialQueryUrl = $baseQueryUrl . '&sort_key[]=start_date' . '&page_size=' . $maxPageSize;
    my %partner_granules;

    while ( $batch_num < $maxBatch ) {
        $page_num++;
        my $queryUrl;
        if ( $hugeCollection && ($batch_num >= 1) ) {
            $queryUrl = $partialQueryUrl . '&temporal=' . $lastBeginTime . '&page_num=' . $page_num;
        } else { 
            $queryUrl = $partialQueryUrl . '&page_num=' . $page_num;
        }
        my $request = HTTP::Request->new( 'GET',
                                          $queryUrl,
                                          [Echo_Token => $self->getTokenElement()]
                                        );
        my $response = $user_agent->request( $request );
        unless ( $response->is_success ) {
            $self->{_error} = "Error executing catalog query from $queryUrl: " .
                $response->message;
            return;
        }

        my $xmlParser = XML::LibXML->new();
        my $dom = $xmlParser->parse_string( $response->content );
        my $doc = $dom->documentElement();
#        my $local_host = $self->getLocalHost();

        my $gCount = 0;
        foreach my $node ( $doc->findnodes( '//Granule' ) ) {
            $gCount++;

            my ( $GranuleURNode ) = $node->getChildrenByTagName( 'GranuleUR' );
            next unless defined $GranuleURNode;
            my $GranuleUR = $GranuleURNode->textContent();
            my ( $ds, $ver, $gran_ur_name ) = $GranuleUR =~ /([^. ]+)\.([^: ]+)\:(\S+)/;

            # We also want to get the last granule's BeginDateTime to continue
            # with the next batch of request for huge collection    
            if ( $hugeCollection && ($page_num == $maxPageNum) && ($gCount == $maxPageSize)) {
                # we reach the last granule of this batch, increase the batch number
                # and reset page number to get ready for next batch.
                $batch_num++;
                $page_num = 0;
                my ( $beginTimeNode ) = $node->findnodes( 'Temporal/RangeDateTime/BeginningDateTime' );
                $lastBeginTime = $beginTimeNode->textContent() if ( defined $beginTimeNode );
            }

            # Instead of the GranuleUR, we will obtain the OnlineAccessUrls.
            # This allows us to retrieve the information about all of the
            # files in the partner's metadata, which can be used to match
            # against the contents of the granule.db file, which contain all
            # of the files in the S4PA instance.
            my ( $OnlineAccessUrlsNode ) = $node->getChildrenByTagName( 'OnlineAccessURLs' );
            # There should always be an OnlineAccessUrls node
            next unless defined $OnlineAccessUrlsNode;
            foreach my $oa_node ( $OnlineAccessUrlsNode->getChildrenByTagName( 'OnlineAccessURL' ) ) {
                my $OnlineAccessUrl = $oa_node->textContent();
                # Get rid of any leading or trailing whitespace
                $OnlineAccessUrl =~ s/^\s+//;
                $OnlineAccessUrl =~ s/\s+$//;
                my ( $online_host, $online_file ) = $OnlineAccessUrl =~ m|.+://(.+?)/.+/(.+)$|;
                ## Ignore files whose URL host does not match the S4PA host

                # Due to hosts/instances consolidation and host domain relocation,
                # published granule might have different hostname than the current
                # hostname. Besides, there are no more dataset that are split onto
                # different instances. Therefore, there is no need to compare 
                # hostname any more. Comparing is actually breaking the reconciliation.
                # next unless ( $online_host =~ /^$local_host/ );

                # With multi-file granules it is possible for the
                # same file to appear in more than one granule. Therefore,
                # we save the filenames in a hash in order to eliminate
                # duplications. Doing this, we won't be able to detect if
                # the partner has a granule more than once, we will assume
                # that they don't.

                if ( $self->reconcileAllFiles() ) {
                    # Return the name of every online file the partner has
                    $partner_granules{$online_file} = 1;
                } else {

                    # Ignore any OPeNDAP URLs
                    next if $OnlineAccessUrl =~ /opendap/i;
                    next if $OnlineAccessUrl =~ /thredds/i;

                    # Return only the names of the online files that match
                    # the granule_ur name, i.e. those that are granule files
                    next unless ( $online_file eq $gran_ur_name );

                    $partner_granules{$gran_ur_name} = 1;
                }
            }
        }
        last unless $gCount;
    }

    return sort( keys %partner_granules );
}


sub getPartnerGranuleList {
    my ( $self, %arg ) = @_;

    unless ( $self->loggedIn() ) {
        my $routine = (caller(0))[3];
        $self->{_error} = "$routine(): not logged in!";
        return 0;
    }

    my ( $hits, $dataSetId ) = $self->getPartnerGranuleCount( VERSION => $arg{VERSION} );

    if ( wantarray ) {
        if ( $hits < $self->getLargeCountThreshold ) {
            return $self->getPartnerSmallGranuleList( SHORTNAME => $arg{SHORTNAME},
                                                      VERSION => $arg{VERSION} );
        } else {
            return $self->getPartnerLargeGranuleList( SHORTNAME => $arg{SHORTNAME},
                                                      VERSION => $arg{VERSION} );
        }
    } else {
        return $hits;
    }
}


sub AUTOLOAD {
    my ( $self, @arg ) = @_;
    if ( $AUTOLOAD =~ /.*::onError/ ) {
        return ( $self->getErrorMessage() eq '' ? 0 : 1 );
    } elsif ( $AUTOLOAD =~ /.*::getErrorMessage/ ) {
        return $self->{_error};
    } elsif ( $AUTOLOAD =~ /.*::onDebug/ ) {
        return $self->{debug};
    } elsif ( $AUTOLOAD =~ /.*::reconcileAllFiles/ ) {
        return $self->{reconcile_all_files};
    } elsif ( $AUTOLOAD =~ /.*::getLargeCountThreshold/ ) {
        return $self->{large_count_threshold};
    } elsif ( $AUTOLOAD =~ /.*::getEndpointRoot/ ) {
        return $self->{client}->{endpoint_root};
    } elsif ( $AUTOLOAD =~ /.*::getCatalogEndpointRoot/ ) {
        return $self->{client}->{catalog_endpoint_root};
    } elsif ( $AUTOLOAD =~ /.*::getServiceTimeout/ ) {
        return $self->{client}->{service_timeout};
    } elsif ( $AUTOLOAD =~ /.*::getServiceNs/ ) {
        return $self->{client}->{service_ns};
    } elsif ( $AUTOLOAD =~ /.*::loggedIn/ ) {
        return $self->{_login};
    } elsif ( $AUTOLOAD =~ /.*::getUsername/ ) {
        return $self->{client}->{username};
    } elsif ( $AUTOLOAD =~ /.*::getPassword/ ) {
        return $self->{client}->{password};
    } elsif ( $AUTOLOAD =~ /.*::getCertFile/ ) {
        return $self->{client}->{certfile};
    } elsif ( $AUTOLOAD =~ /.*::getCertPass/ ) {
        return $self->{client}->{certpass};
    } elsif ( $AUTOLOAD =~ /.*::getTokenUri/ ) {
        return $self->{client}->{token_uri};
    } elsif ( $AUTOLOAD =~ /.*::getProvider/ ) {
        return $self->{client}->{provider};
    } elsif ( $AUTOLOAD =~ /.*::getTypesNs/ ) {
        return $self->{client}->{types_ns};
    } elsif ( $AUTOLOAD =~ /.*::getTokenElement/ ) {
        return $self->{client}->{token};
    } elsif ( $AUTOLOAD =~ /.*::getCmrDatasetId/ ) {
        my %arg = @arg;
        return  $self->{cmrDataSetIds}->{$arg{VERSION}};

    } elsif ( $AUTOLOAD =~ /.*::DESTROY/ ) {
        $self->logout() if $self->loggedIn();
    } else {
        my $method = $AUTOLOAD;
        $method =~ s/.*:://;
        $method = "SUPER::$method";
        $self->$method(@_);
    }
}
1;


package S4PA::Reconciliation::Mirador;
@S4PA::Reconciliation::Mirador::ISA = qw( S4PA::Reconciliation );

use vars '$AUTOLOAD';

sub _init {
    my ( $self, %arg ) = @_;

    $self->{large_count_threshold} = defined $arg{LARGE_COUNT_THRESHOLD} ?
                                             $arg{LARGE_COUNT_THRESHOLD} :
                                             500;
    $self->{client}->{endpoint_root} = defined $arg{ENDPOINT_ROOT} ?
                                       $arg{ENDPOINT_ROOT} :
                                       'https://mirador.gsfc.nasa.gov/cgi-bin/mirador/';
    $self->{client}->{service_timeout} = defined $arg{SERVICE_TIMEOUT} ?
                                         $arg{SERVICE_TIMEOUT} :
                                         600;
    $self->{client}->{service_ns} = defined $arg{SERVICE_NS} ?
                                    $arg{SERVICE_NS} :
                                    'https://mirador.gsfc.nasa.gov/MiradorDatasetInfo';
    $self->{client}->{types_ns} = defined $arg{TYPES_NS} ?
                                  $arg{TYPES_NS} :
                                 'https://mirador.gsfc.nasa.gov/MiradorDatasetInfo';
    $self->{ftp_pull_timeout} = $arg{FTP_PULL_TIMEOUT};
    $self->{partner_protocol} = $arg{PARTNER_PROTOCOL};
}


sub login {
    my ( $self ) = @_;

    my $soap = SOAP::Lite
        ->uri( $self->getServiceNs() )
        ->proxy( $self->getEndpointRoot . 'MiradorDatasetInfo.cgi',
                 timeout => $self->getServiceTimeout() );
    # We use eval here to prevent a die on a transport error
    my $som = eval {
        $soap->Login( );
    };
    if ( $@ ) {
        $self->{_error} = "$@";
    } elsif ( $som->fault ) {
        $self->{_error} = $som->fault->{faultstring};
    } else {
        my $result = $som->result();
    }
    $self->{_login} = $self->onError() ? 0 : 1;

    return $self->{_login};
}


sub getPartnerDatasetVersions {
    my ( $self, %arg ) = @_;

    my $shortnameElement = $self->soapServiceNsData( NAME  => 'shortname',
                                                     TYPE  => 'string',
                                                     VALUE => $arg{SHORTNAME} );
    my $som = SOAP::Lite
        ->uri( $self->getServiceNs() )
        ->proxy( $self->getEndpointRoot . 'MiradorDatasetInfo.cgi',
                 timeout => $self->getServiceTimeout() )
        ->GetDatasets( $shortnameElement );
    if ( $som->fault ) {
        $self->{_error} = "Error getting GetDatasets results: " .
            $som->fault->{faultstring};
        return;
    }
    my $result = $som->result();

    my $xmlParser = XML::LibXML->new();
    my $dom;
    eval { $dom = $xmlParser->parse_string( $result ); };
    S4P::perish( 2, "Could not parse response from MiradorDatasetInfo.cgi:  $@\n" ) if $@;
    my $doc = $dom->documentElement();

    my @versions;
    foreach my $node ( $doc->findnodes( '//CollectionMetadata' ) ) {
        my ( $shortnameNode ) = $node->getChildrenByTagName( 'shortname' );
        my $shortName = $shortnameNode->textContent();
        my ( $versionidNode ) = $node->getChildrenByTagName( 'versionid' );
        my $versionId = $versionidNode ? $versionidNode->textContent() : '';

        # Add the version to the list of versions for the shortName
        push @versions, $versionId;
    }

    return @versions;
}


sub getPartnerGranuleCount {
    my ( $self, %arg ) = @_;

    my $shortnameElement = $self->soapServiceNsData( NAME  => 'shortname',
                                                     TYPE  => 'string',
                                                     VALUE => $arg{SHORTNAME} );
    my $versionIdElement = $self->soapServiceNsData( NAME  => 'versionid',
                                                     TYPE  => 'string',
                                                     VALUE => $arg{VERSION} );
    my $som = SOAP::Lite
        ->uri( $self->getServiceNs() )
        ->proxy( $self->getEndpointRoot . 'MiradorDatasetInfo.cgi',
                 timeout => $self->getServiceTimeout() )
        ->GetDatasetGranCount( $shortnameElement, $versionIdElement );
    if ( $som->fault ) {
        $self->{_error} = "Error getting GetDatasets results: " .
            $som->fault->{faultstring};
        return;
    }
    my $result = $som->result();

    my $xmlParser = XML::LibXML->new();
    my $countNode;
    eval { $countNode = $xmlParser->parse_string( $result ); };
    S4P::perish( 2, "Could not parse response from MiradorDatasetInfo.cgi:  $@\n" ) if $@;
    my $count = $countNode->textContent();

    return $count;
}


sub getPartnerSmallGranuleList {
    my ( $self, %arg ) = @_;

    my $shortnameElement = $self->soapServiceNsData( NAME  => 'shortname',
                                                     TYPE  => 'string',
                                                     VALUE => $arg{SHORTNAME} );
    my $versionIdElement = $self->soapServiceNsData( NAME  => 'versionid',
                                                     TYPE  => 'string',
                                                     VALUE => $arg{VERSION} );
    my $ftpUrlElement;

    my $som = SOAP::Lite
        ->uri( $self->getServiceNs() )
        ->proxy( $self->getEndpointRoot . 'MiradorDatasetInfo.cgi',
                 timeout => $self->getServiceTimeout() )
        ->GetDatasetInformation( $shortnameElement, $versionIdElement );
    if ( $som->fault ) {
        $self->{_error} = "Error getting GetDatasetInformation results: " .
            $som->fault->{faultstring};
        return;
    }
    my $result = $som->result();

    my $xmlParser = XML::LibXML->new();
    my $dom;
    eval { $dom = $xmlParser->parse_string( $result ); };
    S4P::perish( 2, "Could not parse response from MiradorDatasetInfo.cgi':  $@\n" ) if $@;
    my $doc = $dom->documentElement();
    my $local_host = $self->getLocalHost();

    my %partner_granules;
    foreach my $node ( $doc->findnodes( '//Granule' ) ) {
        my ( $GranuleIdNode ) = $node->getChildrenByTagName( 'GranuleId' );
        next unless defined $GranuleIdNode;
        my $GranuleId = $GranuleIdNode->textContent();
        my ( $OnlineAccessUrlsNode ) = $node->getChildrenByTagName( 'OnlineAccessUrls' );
        # There should always be an OnlineAccessUrls node
        return unless defined $OnlineAccessUrlsNode;
        foreach my $oa_node ( $OnlineAccessUrlsNode->getChildrenByTagName( 'OnlineAccessUrl' ) ) {
            my $OnlineAccessUrl = $oa_node->textContent();
            # Get rid of any leading or trailing whitespace
            $OnlineAccessUrl =~ s/^\s+//;
            $OnlineAccessUrl =~ s/\s+$//;
            my ( $online_host, $online_file ) = $OnlineAccessUrl =~ m|.+://(.+?)/.+/(.+)$|;
            ## Ignore files whose URL host does not match the S4PA host

            # Due to hosts/instances consolidation and host domain relocation,
            # published granule might have different hostname than the current
            # hostname. Besides, there are no more dataset that are split onto
            # different instances. Therefore, there is no need to compare 
            # hostname any more. Comparing is actually breaking the reconciliation.
            # next unless ( $online_host =~ /^$local_host/ );

            # With multi-file granules it is possible for the
            # same file to appear in more than one granule. Therefore,
            # we save the filenames in a hash in order to eliminate
            # duplications. Doing this, we won't be able to detect if
            # the partner has a granule more than once, we will assume
            # that they don't.

            if ( $self->reconcileAllFiles() ) {
                # Return the name of every online file the partner has
                $partner_granules{$online_file} = 1;
            } else {
                # Return only the names of the online files that match
                # the granule id, i.e. those that are granule files
                next unless ( $online_file eq $GranuleId );
                $partner_granules{$GranuleId} = 1;
            }
        }
    }

    return sort( keys %partner_granules );
}


sub getPartnerLargeGranuleList {
    my ( $self, %arg ) = @_;

    my $tmpDir = '/var/tmp';
    my $ftp_host = $self->getFtpPushHost();
    my $ftp_user = $self->getFtpPushUser();
    my $ftp_pwd = $self->getFtpPushPwd();
    my $ftp_dir = $self->getFtpPushDir();
    my $partner_protocol = $self->getPartnerProtocol();
    my $pull_timeout = $self->getFtpPullTimeout();

    my $ftp_uri = "ftp://$ftp_user:$ftp_pwd\@${ftp_host}${ftp_dir}";

    my $shortnameElement = $self->soapServiceNsData( NAME  => 'shortname',
                                                     TYPE  => 'string',
                                                     VALUE => $arg{SHORTNAME} );
    my $versionIdElement = $self->soapServiceNsData( NAME  => 'versionid',
                                                     TYPE  => 'string',
                                                     VALUE => $arg{VERSION} );
    my $ftpUrlElement = $self->soapServiceNsData( NAME  => 'ftpUrl',
                                                  TYPE  => 'string',
                                                  VALUE => $ftp_uri );
    my $pullTimeoutElement = $self->soapServiceNsData( NAME  => 'pullTimeout',
                                                       TYPE  => 'string',
                                                       VALUE => $pull_timeout );

    my $som = SOAP::Lite
        ->uri( $self->getServiceNs() )
        ->proxy( $self->getEndpointRoot . 'MiradorDatasetInfo.cgi',
                 timeout => $self->getServiceTimeout(), keepalive => 1 )
        ->GetDatasetInformation( $shortnameElement,
                                 $versionIdElement,
                                 $ftpUrlElement,
                                 $pullTimeoutElement );

    if ( $som->fault ) {
        $self->{_error} = "Error getting GetDatasetInformation results: " .
            $som->fault->{faultstring};
        return;
    }

    # Get name of compressed file created by Mirador containing dataset info
    my $compressed_info_file = $som->result();

    my $local_file;
    if ($partner_protocol eq 'sftp') {
        $local_file = $self->fetch_sftp_file( PUSHED_FILE => $compressed_info_file,
            PULL_TIMEOUT => $pull_timeout );
    } elsif ($partner_protocol eq 'file') {
        $local_file = $self->fetch_local_file( PUSHED_FILE => $compressed_info_file,
            PULL_TIMEOUT => $pull_timeout );
    } else {
        $local_file = $self->fetch_ftp_file( PUSHED_FILE => $compressed_info_file,
            PULL_TIMEOUT => $pull_timeout );
    }
    return if $self->onError();

    # Uncompress the file
#    $local_file =~ s/\.gz$//;
#    open( FH, "| gunzip - > $local_file" )
#        or S4P::perish( 17, "Could not open pipe to write $local_file" );
#    print FH $compressed_info;
#    close( FH ) or S4P::perish( 18, "Failed to gunzip $local_file" );
    `gunzip $local_file`;
    $local_file =~ s/\.gz$//;
    unless (-f $local_file) {
        $self->{_error} = "Failed to create $local_file";
        return;
    }

    my $local_host = $self->getLocalHost();

    # Define a reference to a subroutine that will be a handler for
    # each Granule element in the xml file.
    # Because the handler is defined in this subroutine, it will have access
    # to the %partner_granules hash defined in this subroutine.
    my %partner_granules;
    my $GranuleHandler = sub {
        my ( $twig, $node ) = @_;

        my $GranuleId = $node->first_child_text( 'GranuleId' );
        my ( $OnlineAccessUrlsNode ) = $node->first_child( 'OnlineAccessUrls' );
        # There should always be an OnlineAccessUrls node
        return unless defined $OnlineAccessUrlsNode;
        foreach my $OnlineAccessUrl ( $OnlineAccessUrlsNode->children_text( 'OnlineAccessUrl' ) ) {
            # Get rid of any leading or trailing whitespace
#            $OnlineAccessUrl =~ s/^\s+//;
#            $OnlineAccessUrl =~ s/\s+$//;
            my ( $online_host, $online_file ) = $OnlineAccessUrl =~ m|.+://(.+?)/.+/(.+)$|;
            ## Ignore files whose URL host does not match the S4PA host

            # Due to hosts/instances consolidation and host domain relocation,
            # published granule might have different hostname than the current
            # hostname. Besides, there are no more dataset that are split onto
            # different instances. Therefore, there is no need to compare 
            # hostname any more. Comparing is actually breaking the reconciliation.
            # next unless ( $online_host =~ /^$local_host/ );

            # With multi-file granules it is possible for the
            # same file to appear in more than one granule. Therefore,
            # we save the filenames in a hash in order to eliminate
            # duplications. Doing this, we won't be able to detect if
            # the partner has a granule more than once, we will assume
            # that they don't.

            if ( $self->reconcileAllFiles() ) {
                # Return the name of every online file the partner has
                $partner_granules{$online_file} = 1;
            } else {
                # Return only the names of the online files that match
                # the granule id, i.e. those that are granule files
                next unless ( $online_file eq $GranuleId );
                $partner_granules{$GranuleId} = 1;
            }
        }
        $twig->purge;
    };

    my $handlers = {'Granule' => $GranuleHandler};
    my $twig = new XML::Twig( TwigRoots => {'Granule' => 1},
                              TwigHandlers => $handlers );
    $twig->parsefile( $local_file );
    $twig->purge;

    unlink $local_file;

    return sort( keys %partner_granules );
}


sub getPartnerGranuleList {
    my ( $self, %arg ) = @_;

    my ( $granuleCount ) = $self->getPartnerGranuleCount( %arg );

    if ( wantarray ) {
        if ( $granuleCount < $self->getLargeCountThreshold ) {
            return $self->getPartnerSmallGranuleList( %arg );
        } else {
            return $self->getPartnerLargeGranuleList( %arg );
        }
    } else {
        return $granuleCount;
    }
}


sub AUTOLOAD {
    my ( $self, @arg ) = @_;
    if ( $AUTOLOAD =~ /.*::onError/ ) {
        return ( $self->getErrorMessage() eq '' ? 0 : 1 );
    } elsif ( $AUTOLOAD =~ /.*::getErrorMessage/ ) {
        return $self->{_error};
    } elsif ( $AUTOLOAD =~ /.*::onDebug/ ) {
        return $self->{debug};
    } elsif ( $AUTOLOAD =~ /.*::reconcileAllFiles/ ) {
        return $self->{reconcile_all_files};
    } elsif ( $AUTOLOAD =~ /.*::getPartnerProtocol/ ) {
        return $self->{partner_protocol};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPushHost/ ) {
        return $self->{ftp_push_host};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPushUser/ ) {
        return $self->{ftp_push_user};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPushPwd/ ) {
        return $self->{ftp_push_pwd};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPushDir/ ) {
        return $self->{ftp_push_dir};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPushQuiescentTime/ ) {
        return $self->{ftp_push_quiescent_time};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPullTimeout/ ) {
        return $self->{ftp_pull_timeout};
    } elsif ( $AUTOLOAD =~ /.*::getLargeCountThreshold/ ) {
        return $self->{large_count_threshold};
    } elsif ( $AUTOLOAD =~ /.*::getEndpointRoot/ ) {
        return $self->{client}->{endpoint_root};
    } elsif ( $AUTOLOAD =~ /.*::getServiceTimeout/ ) {
        return $self->{client}->{service_timeout};
    } elsif ( $AUTOLOAD =~ /.*::getServiceNs/ ) {
        return $self->{client}->{service_ns};
    } elsif ( $AUTOLOAD =~ /.*::soapServiceNsData/ ) {
        my %arg = @arg;
        return SOAP::Data->uri( $self->getServiceNs() )
                         ->name( $arg{NAME} )
                         ->type( $arg{TYPE} )
                         ->value( $arg{VALUE} );
    } elsif ( $AUTOLOAD =~ /.*::DESTROY/ ) {
    } else {
        my $method = $AUTOLOAD;
        $method =~ s/.*:://;
        $method = "SUPER::$method";
        $self->$method(@_);
    }
}
1;


package S4PA::Reconciliation::EMS;
@S4PA::Reconciliation::EMS::ISA = qw( S4PA::Reconciliation );

use vars '$AUTOLOAD';

sub _init {
    my ( $self, %arg ) = @_;

    my $url = $arg{URL};
    my $user_agent = LWP::UserAgent->new();
    $user_agent->env_proxy;
    my $request = HTTP::Request->new( 'GET', $url );
    my $response = $user_agent->request( $request );
    if ( $response->is_success ) {
        foreach my $line ( split "\n", $response->content() ) {
            my ( $shortName, $versionId, $count ) = split( ',', $line );
            next unless $count =~ /^\d+$/;
            $self->{counts}->{$shortName}->{$versionId} = $count;
        }
    } else {
        $self->{_error} = "Error obtaining statistics from $url: " .
                          $response->message;
    }
}


sub getPartnerDatasetVersions {
    my ( $self, %arg ) = @_;

    my $shortName = $arg{SHORTNAME};

    return $shortName ? keys %{$self->{counts}->{$shortName}} : ();
}


sub getPartnerGranuleCount {
    my ( $self, %arg ) = @_;

    my $shortName = $arg{SHORTNAME};
    my $version = $arg{VERSION};

    return $self->{counts}->{$shortName}->{$version};
}


sub getPartnerGranuleList {
    my ( $self, %arg ) = @_;

    return;
}


sub AUTOLOAD {
    my ( $self, @arg ) = @_;
    if ( $AUTOLOAD =~ /.*::onError/ ) {
        return ( $self->getErrorMessage() eq '' ? 0 : 1 );
    } else {
        my $method = $AUTOLOAD;
        $method =~ s/.*:://;
        $method = "SUPER::$method";
        $self->$method(@_);
    }
}
1;


package S4PA::Reconciliation::Dotchart;
@S4PA::Reconciliation::Dotchart::ISA = qw( S4PA::Reconciliation );

use File::Basename;
use vars '$AUTOLOAD';

sub _init {
    my ( $self, %arg ) = @_;

    $self->{large_count_threshold} = defined $arg{LARGE_COUNT_THRESHOLD} ?
                                     $arg{LARGE_COUNT_THRESHOLD} :
                                     500;
    $self->{ftp_push_quiescent_time} = defined $arg{FTP_PUSH_QUIESCENT_TIME} ?
                                       $arg{FTP_PUSH_QUIESCENT_TIME} :
                                       5;
    $self->{client}->{endpoint_root} = defined $arg{ENDPOINT_ROOT} ?
                                       $arg{ENDPOINT_ROOT} :
                                       'https://tads1.gesdisc.eosdis.nasa.gov/cgi-bin/dotchart/';
    $self->{client}->{service_timeout} = defined $arg{SERVICE_TIMEOUT} ?
                                         $arg{SERVICE_TIMEOUT} :
                                         600;
    $self->{client}->{service_ns} = defined $arg{SERVICE_NS} ?
                                    $arg{SERVICE_NS} :
                                    'https://tads1.gesdisc.eosdis.nasa.gov/DotchartDatasetInfo';
    $self->{client}->{types_ns} = defined $arg{TYPES_NS} ?
                                  $arg{TYPES_NS} :
                                 'https://tads1.gesdisc.eosdis.nasa.gov/DotchartDatasetInfo';
    $self->{ftp_pull_timeout} = $arg{FTP_PULL_TIMEOUT};
    $self->{partner_protocol} = $arg{PARTNER_PROTOCOL};
}


sub login {
    my ( $self ) = @_;

    my $soap = SOAP::Lite
        ->uri( $self->getServiceNs() )
        ->proxy( $self->getEndpointRoot . 'DotchartDatasetInfo.cgi',
                 timeout => $self->getServiceTimeout() );
    # We use eval here to prevent a die on a transport error
    my $som = eval {
        $soap->Login( );
    };
    if ( $@ ) {
        $self->{_error} = "$@";
    } elsif ( $som->fault ) {
        $self->{_error} = $som->fault->{faultstring};
    } else {
        my $result = $som->result();
    }
    $self->{_login} = $self->onError() ? 0 : 1;

    return $self->{_login};
}


sub getPartnerDatasetVersions {
    my ( $self, %arg ) = @_;

    my $instanceElement = $self->soapServiceNsData( NAME  => 'instance',
                                                    TYPE  => 'string',
                                                    VALUE => $arg{INSTANCE} );
    my $shortnameElement = $self->soapServiceNsData( NAME  => 'shortname',
                                                     TYPE  => 'string',
                                                     VALUE => $arg{SHORTNAME} );
    my $som = SOAP::Lite
        ->uri( $self->getServiceNs() )
        ->proxy( $self->getEndpointRoot . 'DotchartDatasetInfo.cgi',
                 timeout => $self->getServiceTimeout() )
        ->GetDatasets( $instanceElement, $shortnameElement );
    if ( $som->fault ) {
        $self->{_error} = "Error getting GetDatasets results: " .
            $som->fault->{faultstring};
        return;
    }
    my $result = $som->result();

    my $xmlParser = XML::LibXML->new();
    my $dom;
    eval { $dom = $xmlParser->parse_string( $result ); };
    S4P::perish( 2, "Could not parse response from DotchartDatasetInfo.cgi:  $@\n" ) if $@;
    my $doc = $dom->documentElement();

    my @versions;
    foreach my $node ( $doc->findnodes( '//CollectionMetadata' ) ) {
        my ( $shortnameNode ) = $node->getChildrenByTagName( 'shortname' );
        my $shortName = $shortnameNode->textContent();
        my ( $versionidNode ) = $node->getChildrenByTagName( 'versionid' );
        my $versionId = $versionidNode ? $versionidNode->textContent() : '';

        # Add the version to the list of versions for the shortName
        push @versions, $versionId;
    }

    return @versions;
}


sub getPartnerGranuleCount {
    my ( $self, %arg ) = @_;

    my $instanceElement = $self->soapServiceNsData( NAME  => 'instance',
                                                    TYPE  => 'string',
                                                    VALUE => $arg{INSTANCE} );
    my $shortnameElement = $self->soapServiceNsData( NAME  => 'shortname',
                                                     TYPE  => 'string',
                                                     VALUE => $arg{SHORTNAME} );
    my $versionIdElement = $self->soapServiceNsData( NAME  => 'versionid',
                                                     TYPE  => 'string',
                                                     VALUE => $arg{VERSION} );
    my $som = SOAP::Lite
        ->uri( $self->getServiceNs() )
        ->proxy( $self->getEndpointRoot . 'DotchartDatasetInfo.cgi',
                 timeout => $self->getServiceTimeout() )
        ->GetDatasetGranCount( $instanceElement, $shortnameElement, $versionIdElement );
    if ( $som->fault ) {
        $self->{_error} = "Error getting GetDatasets results: " .
            $som->fault->{faultstring};
        return;
    }
    my $result = $som->result();

    my $xmlParser = XML::LibXML->new();
    my $countNode = $xmlParser->parse_string( $result );
    eval { $countNode = $xmlParser->parse_string( $result ); };
    S4P::perish( 2, "Could not parse response from DotchartDatasetInfo.cgi:  $@\n" ) if $@;
    my $count = $countNode->textContent();

    return $count;
}


sub getPartnerSmallGranuleList {
    my ( $self, %arg ) = @_;

    my $instanceElement = $self->soapServiceNsData( NAME  => 'instance',
                                                    TYPE  => 'string',
                                                    VALUE => $arg{INSTANCE} );
    my $shortnameElement = $self->soapServiceNsData( NAME  => 'shortname',
                                                     TYPE  => 'string',
                                                     VALUE => $arg{SHORTNAME} );
    my $versionIdElement = $self->soapServiceNsData( NAME  => 'versionid',
                                                     TYPE  => 'string',
                                                     VALUE => $arg{VERSION} );

    # Construct the dataset directory name
    # from the current s4pa shortname and s4pa versionid
    my $s4paDatasetDir = ( $arg{S4PA_VERSION} eq '' ) ?
        $arg{SHORTNAME} : "$arg{SHORTNAME}.$arg{S4PA_VERSION}";

    my $som = SOAP::Lite
        ->uri( $self->getServiceNs() )
        ->proxy( $self->getEndpointRoot . 'DotchartDatasetInfo.cgi',
                 timeout => $self->getServiceTimeout() )
        ->GetDatasetInformation( $instanceElement, $shortnameElement, $versionIdElement );
    if ( $som->fault ) {
        $self->{_error} = "Error getting GetDatasetInformation results: " .
            $som->fault->{faultstring};
        return;
    }
    my $result = $som->result();

    my $xmlParser = XML::LibXML->new();
    my $dom;
    eval { $dom = $xmlParser->parse_string( $result ); };
    S4P::perish( 2, "Could not parse response from DotchartDatasetInfo.cgi:  $@\n" ) if $@;
    my $doc = $dom->documentElement();
    my $local_host = $self->getLocalHost();

    my %partner_granules;
    foreach my $node ( $doc->findnodes( '//Granule' ) ) {
        my ( $GranuleIdNode ) = $node->getChildrenByTagName( 'GranuleId' );
        next unless defined $GranuleIdNode;
        my $GranuleId = $GranuleIdNode->textContent();
        my ( $OnlineAccessUrlsNode ) = $node->getChildrenByTagName( 'OnlineAccessUrls' );
        # There should always be an OnlineAccessUrls node
        return unless defined $OnlineAccessUrlsNode;
        foreach my $oa_node ( $OnlineAccessUrlsNode->getChildrenByTagName( 'OnlineAccessUrl' ) ) {
            my $OnlineAccessUrl = $oa_node->textContent();
            # Get rid of any leading or trailing whitespace
            $OnlineAccessUrl =~ s/^\s+//;
            $OnlineAccessUrl =~ s/\s+$//;
            my ( $online_host, $online_file ) = $OnlineAccessUrl =~ m|.+://(.+?)/(.+)$|;

            # Can't ignore files whose URL host does not match the S4PA host
            # since the replication server might publish granule to dotchart
            # with the primary server hostname in the URL.
            # next unless ( $online_host =~ /^$local_host/ );

            my $filename = File::Basename::basename( $online_file );
            my $dirname = File::Basename::dirname( $online_file );
            # However, we do want to make sure that the directory path matches
            # the shortname, versionid combination. The s4pa versionless
            # dataset could have any versionid in dotchart database, however
            # the file url should always reflect that the dataset directory
            # name does not carry a versionid in it.
            # Any s4pa granule should reside under the directory
            # '/<$shortname>[.$version]/YYYY'
            # So, skip those that do not have this pattern in the
            # directory structure.
            # next unless ( $dirname =~ m|/+$s4paDatasetDir/+\d{4}| );

            # the above directory match is not true any more after the
            # introduction of climatology dataset which have no year subdirectory
            # all granules are directly under the dataset directory itself.
            # So, make the year in directory path an optional.
            next unless ( $dirname =~ m|/+$s4paDatasetDir(/+\d{4})?| );

            # With multi-file granules it is possible for the
            # same file to appear in more than one granule. Therefore,
            # we save the filenames in a hash in order to eliminate
            # duplications. Doing this, we won't be able to detect if
            # the partner has a granule more than once, we will assume
            # that they don't.

            if ( $self->reconcileAllFiles() ) {
                # Return the name of every online file the partner has
                $partner_granules{$filename} = 1;
            } else {
                # Return only the names of the online files that match
                # the granule id, i.e. those that are granule files
                next unless ( $filename eq $GranuleId );
                $partner_granules{$GranuleId} = 1;
            }
        }
    }

    return sort( keys %partner_granules );
}


sub getPartnerLargeGranuleList {
    my ( $self, %arg ) = @_;

    my $ftp_host = $self->getFtpPushHost();
    my $ftp_user = $self->getFtpPushUser();
    my $ftp_pwd = $self->getFtpPushPwd();
    my $ftp_dir = $self->getFtpPushDir();
    my $pull_timeout = $self->getFtpPullTimeout();
    my $partner_protocol = $self->getPartnerProtocol();

    my $ftp_uri = "ftp://$ftp_user:$ftp_pwd\@${ftp_host}${ftp_dir}";

    my $instanceElement = $self->soapServiceNsData( NAME  => 'instance',
                                                    TYPE  => 'string',
                                                    VALUE => $arg{INSTANCE} );
    my $shortnameElement = $self->soapServiceNsData( NAME  => 'shortname',
                                                     TYPE  => 'string',
                                                     VALUE => $arg{SHORTNAME} );
    my $versionIdElement = $self->soapServiceNsData( NAME  => 'versionid',
                                                     TYPE  => 'string',
                                                     VALUE => $arg{VERSION} );
    my $ftpUrlElement = $self->soapServiceNsData( NAME  => 'ftpUrl',
                                                  TYPE  => 'string',
                                                  VALUE => $ftp_uri );
    my $pullTimeoutElement = $self->soapServiceNsData( NAME  => 'pullTimeout',
                                                       TYPE  => 'string',
                                                       VALUE => $pull_timeout );

    # Construct the dataset directory name
    # from the current s4pa shortname and s4pa versionid
    my $s4paDatasetDir = ( $arg{S4PA_VERSION} eq '' ) ?
        $arg{SHORTNAME} : "$arg{SHORTNAME}.$arg{S4PA_VERSION}";

    my $som;
    $som = SOAP::Lite
        ->uri( $self->getServiceNs() )
        ->proxy( $self->getEndpointRoot . 'DotchartDatasetInfo.cgi',
                 timeout => $self->getServiceTimeout(), keepalive => 1 )
        ->GetDatasetInformation( $instanceElement,
                                 $shortnameElement,
                                 $versionIdElement,
                                 $ftpUrlElement,
                                 $pullTimeoutElement );

    if ( $som->fault ) {
        $self->{_error} = "Error getting GetDatasetInformation results: " .
            $som->fault->{faultstring};
        return;
    }

    # Get name of compressed file created by Dotchart containing dataset info
    my $compressed_info_file = $som->result();

    my $local_file;
    if ($partner_protocol eq 'sftp') {
        $local_file = $self->fetch_sftp_file( PUSHED_FILE => $compressed_info_file,
            PULL_TIMEOUT => $pull_timeout );
    } elsif ($partner_protocol eq 'file') {
        $local_file = $self->fetch_local_file( PUSHED_FILE => $compressed_info_file,
            PULL_TIMEOUT => $pull_timeout );
    } else {
        $local_file = $self->fetch_ftp_file( PUSHED_FILE => $compressed_info_file,
            PULL_TIMEOUT => $pull_timeout );
    }
    return if $self->onError();

    # Uncompress the file
    `gunzip -f $local_file`;
    $local_file =~ s/\.gz$//;
    unless (-f $local_file) {
        $self->{_error} = "Failed to create $local_file";
        return;
    }

    # my $local_host = $self->getLocalHost();

    # Define a reference to a subroutine that will be a handler for
    # each Granule element in the xml file.
    # Because that handler is defined in this subroutine, it will have access
    # to the %partner_granules hash defined in this subroutine.
    my %partner_granules;
    my $GranuleHandler = sub {
        my ( $twig, $node ) = @_;

        my $GranuleId = $node->first_child_text( 'GranuleId' );
        my ( $OnlineAccessUrlsNode ) = $node->first_child( 'OnlineAccessUrls' );
        # There should always be an OnlineAccessUrls node
        return unless defined $OnlineAccessUrlsNode;
        foreach my $OnlineAccessUrl ( $OnlineAccessUrlsNode->children_text( 'OnlineAccessUrl' ) ) {
            my ( $online_host, $online_file ) = $OnlineAccessUrl =~ m|.+://(.+?)/(.+)$|;

            # Can't ignore files whose URL host does not match the S4PA host
            # since the replication server might publish granule to dotchart
            # with the primary server hostname in the URL.
            # next unless ( $online_host =~ /^$local_host/ );

            my $filename = File::Basename::basename( $online_file );
            my $dirname = File::Basename::dirname( $online_file );
            # However, we do want to make sure that the directory path matches
            # the shortname, versionid combination. The s4pa versionless
            # dataset could have any versionid in dotchart database, however
            # the file url should always reflect that the dataset directory
            # name does not carry a versionid in it.
            # Any s4pa granule should reside under the directory
            # '/<$shortname>[.$version]/YYYY'
            # So, skip those that do not have this pattern in the
            # directory structure.
            # next unless ( $dirname =~ m|/+$s4paDatasetDir/+\d{4}| );

            # the above directory match is not true any more after the
            # introduction of climatology dataset which have no year subdirectory
            # all granules are directly under the dataset directory itself.
            # So, make the year in directory path an optional.
            next unless ( $dirname =~ m|/+$s4paDatasetDir(/+\d{4})?| );

            # With multi-file granules it is possible for the
            # same file to appear in more than one granule. Therefore,
            # we save the filenames in a hash in order to eliminate
            # duplications. Doing this, we won't be able to detect if
            # the partner has a granule more than once, we will assume
            # With multi-file granules it is possible for the
            # same file to appear in more than one granule. Therefore,
            # we save the filenames in a hash in order to eliminate
            # duplications. Doing this, we won't be able to detect if
            # the partner has a granule more than once, we will assume
            # that they don't.

            if ( $self->reconcileAllFiles() ) {
                # Return the name of every online file the partner has
                $partner_granules{$filename} = 1;
            } else {
                # Return only the names of the online files that match
                # the granule id, i.e. those that are granule files
                next unless ( $filename eq $GranuleId );
                $partner_granules{$GranuleId} = 1;
            }
        }
        $twig->purge;
    };

    my $handlers = {'Granule' => $GranuleHandler};
    my $twig = new XML::Twig( TwigRoots => {'Granule' => 1},
                              TwigHandlers => $handlers );
    $twig->parsefile( $local_file );
    $twig->purge;

    unlink $local_file;

    return sort( keys %partner_granules );
}


sub getPartnerGranuleList {
    my ( $self, %arg ) = @_;

    my ( $granuleCount ) = $self->getPartnerGranuleCount( %arg );

    if ( wantarray ) {
        if ( $granuleCount < $self->getLargeCountThreshold ) {
            return $self->getPartnerSmallGranuleList( %arg );
        } else {
            return $self->getPartnerLargeGranuleList( %arg );
        }
    } else {
        return $granuleCount;
    }
}


sub AUTOLOAD {
    my ( $self, @arg ) = @_;
    if ( $AUTOLOAD =~ /.*::onError/ ) {
        return ( $self->getErrorMessage() eq '' ? 0 : 1 );
    } elsif ( $AUTOLOAD =~ /.*::getErrorMessage/ ) {
        return $self->{_error};
    } elsif ( $AUTOLOAD =~ /.*::onDebug/ ) {
        return $self->{debug};
    } elsif ( $AUTOLOAD =~ /.*::reconcileAllFiles/ ) {
        return $self->{reconcile_all_files};
    } elsif ( $AUTOLOAD =~ /.*::getPartnerProtocol/ ) {
        return $self->{partner_protocol};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPushHost/ ) {
        return $self->{ftp_push_host};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPushUser/ ) {
        return $self->{ftp_push_user};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPushPwd/ ) {
        return $self->{ftp_push_pwd};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPushDir/ ) {
        return $self->{ftp_push_dir};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPushQuiescentTime/ ) {
        return $self->{ftp_push_quiescent_time};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPullTimeout/ ) {
        return $self->{ftp_pull_timeout};
    } elsif ( $AUTOLOAD =~ /.*::getLargeCountThreshold/ ) {
        return $self->{large_count_threshold};
    } elsif ( $AUTOLOAD =~ /.*::getEndpointRoot/ ) {
        return $self->{client}->{endpoint_root};
    } elsif ( $AUTOLOAD =~ /.*::getServiceTimeout/ ) {
        return $self->{client}->{service_timeout};
    } elsif ( $AUTOLOAD =~ /.*::getServiceNs/ ) {
        return $self->{client}->{service_ns};
    } elsif ( $AUTOLOAD =~ /.*::soapServiceNsData/ ) {
        my %arg = @arg;
        return SOAP::Data->uri( $self->getServiceNs() )
                         ->name( $arg{NAME} )
                         ->type( $arg{TYPE} )
                         ->value( $arg{VALUE} );
    } elsif ( $AUTOLOAD =~ /.*::DESTROY/ ) {
    } else {
        my $method = $AUTOLOAD;
        $method =~ s/.*:://;
        $method = "SUPER::$method";
        $self->$method(@_);
    }
}
1;


package S4PA::Reconciliation::Giovanni;
@S4PA::Reconciliation::Giovanni::ISA = qw( S4PA::Reconciliation );

use vars '$AUTOLOAD';

sub _init {
    my ( $self, %arg ) = @_;

    $self->{large_count_threshold} = defined $arg{LARGE_COUNT_THRESHOLD} ?
                                             $arg{LARGE_COUNT_THRESHOLD} :
                                             500;
    $self->{client}->{endpoint_root} = defined $arg{ENDPOINT_ROOT} ?
                                       $arg{ENDPOINT_ROOT} :
                                       'http://gdata1.sci.gsfc.nasa.gov/cgi-bin/S4PA/';
    $self->{client}->{service_timeout} = defined $arg{SERVICE_TIMEOUT} ?
                                         $arg{SERVICE_TIMEOUT} :
                                         600;
    $self->{client}->{service_ns} = defined $arg{SERVICE_NS} ?
                                    $arg{SERVICE_NS} :
                                    'http://gdata1.sci.gsfc.nasa.gov/GiovanniDatasetInfo';
    $self->{client}->{types_ns} = defined $arg{TYPES_NS} ?
                                  $arg{TYPES_NS} :
                                 'http://gdata1.sci.gsfc.nasa.gov/GiovanniDatasetInfo';
    $self->{ftp_pull_timeout} = $arg{FTP_PULL_TIMEOUT};
}


sub login {
    my ( $self ) = @_;

    my $soap = SOAP::Lite
        ->uri( $self->getServiceNs() )
        ->proxy( $self->getEndpointRoot . 'GiovanniDatasetInfo.cgi',
                 timeout => $self->getServiceTimeout() );
    # We use eval here to prevent a die on a transport error
    my $som = eval {
        $soap->Login( );
    };
    if ( $@ ) {
        $self->{_error} = "$@";
    } elsif ( $som->fault ) {
        $self->{_error} = $som->fault->{faultstring};
    } else {
        my $result = $som->result();
    }
    $self->{_login} = $self->onError() ? 0 : 1;

    return $self->{_login};
}


sub getPartnerDatasetVersions {
    my ( $self, %arg ) = @_;

    my $shortnameElement = $self->soapServiceNsData( NAME  => 'shortname',
                                                     TYPE  => 'string',
                                                     VALUE => $arg{SHORTNAME} );
    my $som = SOAP::Lite
        ->uri( $self->getServiceNs() )
        ->proxy( $self->getEndpointRoot . 'GiovanniDatasetInfo.cgi',
                 timeout => $self->getServiceTimeout() )
        ->GetDatasets( $shortnameElement );
    if ( $som->fault ) {
        $self->{_error} = "Error getting GetDatasets results: " .
            $som->fault->{faultstring};
        return;
    }
    my $result = $som->result();

    my $xmlParser = XML::LibXML->new();
    my $dom;
    eval { $dom = $xmlParser->parse_string( $result ); };
    S4P::perish( 2, "Could not parse response from GiovanniDatasetInfo.cgi:  $@\n" ) if $@;
    my $doc = $dom->documentElement();

    my @versions;
    foreach my $node ( $doc->findnodes( '//CollectionMetadata' ) ) {
        my ( $shortnameNode ) = $node->getChildrenByTagName( 'shortname' );
        my $shortName = $shortnameNode->textContent();
        my ( $versionidNode ) = $node->getChildrenByTagName( 'versionid' );
        my $versionId = $versionidNode ? $versionidNode->textContent() : '';

        # Add the version to the list of versions for the shortName
        push @versions, $versionId;
    }

    return @versions;
}


sub getPartnerGranuleCount {
    my ( $self, %arg ) = @_;

    my $shortnameElement = $self->soapServiceNsData( NAME  => 'shortname',
                                                     TYPE  => 'string',
                                                     VALUE => $arg{SHORTNAME} );
    my $versionIdElement = $self->soapServiceNsData( NAME  => 'versionid',
                                                     TYPE  => 'string',
                                                     VALUE => $arg{VERSION} );
    my $som = SOAP::Lite
        ->uri( $self->getServiceNs() )
        ->proxy( $self->getEndpointRoot . 'GiovanniDatasetInfo.cgi',
                 timeout => $self->getServiceTimeout() )
        ->GetDatasetGranCount( $shortnameElement, $versionIdElement );
    if ( $som->fault ) {
        $self->{_error} = "Error getting GetDatasets results: " .
            $som->fault->{faultstring};
        return;
    }
    my $result = $som->result();

    my $xmlParser = XML::LibXML->new();
    my $countNode;
    eval { $countNode = $xmlParser->parse_string( $result ); };
    S4P::perish( 2, "Could not parse response from GiovanniDatasetInfo.cgi:  $@\n" ) if $@;
    my $count = $countNode->textContent();

    return $count;
}


sub getPartnerSmallGranuleList {
    my ( $self, %arg ) = @_;

    my $shortnameElement = $self->soapServiceNsData( NAME  => 'shortname',
                                                     TYPE  => 'string',
                                                     VALUE => $arg{SHORTNAME} );
    my $versionIdElement = $self->soapServiceNsData( NAME  => 'versionid',
                                                     TYPE  => 'string',
                                                     VALUE => $arg{VERSION} );
    my $ftpUrlElement;

    my $som = SOAP::Lite
        ->uri( $self->getServiceNs() )
        ->proxy( $self->getEndpointRoot . 'GiovanniDatasetInfo.cgi',
                 timeout => $self->getServiceTimeout() )
        ->GetDatasetInformation( $shortnameElement, $versionIdElement );
    if ( $som->fault ) {
        $self->{_error} = "Error getting GetDatasetInformation results: " .
            $som->fault->{faultstring};
        return;
    }
    my $result = $som->result();

    my $xmlParser = XML::LibXML->new();
    my $dom;
    eval { $dom = $xmlParser->parse_string( $result ); };
    S4P::perish( 2, "Could not parse response from GiovanniDatasetInfo.cgi:  $@\n" ) if $@;
    my $doc = $dom->documentElement();
    my $local_host = $self->getLocalHost();

    my %partner_granules;
    foreach my $node ( $doc->findnodes( '//Granule' ) ) {
        my ( $GranuleIdNode ) = $node->getChildrenByTagName( 'GranuleId' );
        next unless defined $GranuleIdNode;
        my $GranuleId = $GranuleIdNode->textContent();
        my ( $OnlineAccessUrlsNode ) = $node->getChildrenByTagName( 'OnlineAccessUrls' );
        # There should always be an OnlineAccessUrls node
        return unless defined $OnlineAccessUrlsNode;
        foreach my $oa_node ( $OnlineAccessUrlsNode->getChildrenByTagName( 'OnlineAccessUrl' ) ) {
            my $OnlineAccessUrl = $oa_node->textContent();
            # Get rid of any leading or trailing whitespace
            $OnlineAccessUrl =~ s/^\s+//;
            $OnlineAccessUrl =~ s/\s+$//;
            my ( $online_host, $online_file ) = $OnlineAccessUrl =~ m|.+://(.+?)/.+/(.+)$|;

            ## Ignore files whose URL host does not match the S4PA host

            # Due to hosts/instances consolidation and host domain relocation,
            # published granule might have different hostname than the current
            # hostname. Besides, there are no more dataset that are split onto
            # different instances. Therefore, there is no need to compare 
            # hostname any more. Comparing is actually breaking the reconciliation.
            # next unless ( $online_host =~ /^$local_host/ );

            # With multi-file granules it is possible for the
            # same file to appear in more than one granule. Therefore,
            # we save the filenames in a hash in order to eliminate
            # duplications. Doing this, we won't be able to detect if
            # the partner has a granule more than once, we will assume
            # that they don't.

            if ( $self->reconcileAllFiles() ) {
                # Return the name of every online file the partner has
                $partner_granules{$online_file} = $OnlineAccessUrl;
            } else {
                # Return only the names of the online files that match
                # the granule id, i.e. those that are granule files
                next unless ( $online_file eq $GranuleId );
                $partner_granules{$GranuleId} = $OnlineAccessUrl;
            }
        }
    }

    # For gionvanni deletion, we need to carry the online URL.
    # So, instead of returning just an array, we will return a hash.
    my %partner_urls;
    foreach my $granule ( sort keys %partner_granules ) {
        $partner_urls{$granule} = $partner_granules{$granule};
    }
    return %partner_urls;
}


sub getPartnerLargeGranuleList {
    my ( $self, %arg ) = @_;

    my $tmpDir = '/var/tmp';
    my $ftp_host = $self->getFtpPushHost();
    my $ftp_user = $self->getFtpPushUser();
    my $ftp_pwd = $self->getFtpPushPwd();
    my $ftp_dir = $self->getFtpPushDir();
    my $pull_timeout = $self->getFtpPullTimeout();

    my $ftp_uri = "ftp://$ftp_user:$ftp_pwd\@${ftp_host}${ftp_dir}";

    my $shortnameElement = $self->soapServiceNsData( NAME  => 'shortname',
                                                     TYPE  => 'string',
                                                     VALUE => $arg{SHORTNAME} );
    my $versionIdElement = $self->soapServiceNsData( NAME  => 'versionid',
                                                     TYPE  => 'string',
                                                     VALUE => $arg{VERSION} );
    my $ftpUrlElement = $self->soapServiceNsData( NAME  => 'ftpUrl',
                                                  TYPE  => 'string',
                                                  VALUE => $ftp_uri );
    my $pullTimeoutElement = $self->soapServiceNsData( NAME  => 'pullTimeout',
                                                       TYPE  => 'string',
                                                       VALUE => $pull_timeout );

    my $som = SOAP::Lite
        ->uri( $self->getServiceNs() )
        ->proxy( $self->getEndpointRoot . 'GiovanniDatasetInfo.cgi',
                 timeout => $self->getServiceTimeout(), keepalive => 1 )
        ->GetDatasetInformation( $shortnameElement,
                                 $versionIdElement,
                                 $ftpUrlElement,
                                 $pullTimeoutElement );

    if ( $som->fault ) {
        $self->{_error} = "Error getting GetDatasetInformation results: " .
            $som->fault->{faultstring};
        return;
    }

    # Get name of compressed file created by Giovanni containing dataset info
    my $compressed_info_file = $som->result();

    my $local_file = $self->fetch_ftp_file( PUSHED_FILE => $compressed_info_file,
        PULL_TIMEOUT => $pull_timeout );
    return if $self->onError();

    # Uncompress the file
#    $local_file =~ s/\.gz$//;
#    open( FH, "| gunzip - > $local_file" )
#        or S4P::perish( 17, "Could not open pipe to write $local_file" );
#    print FH $compressed_info;
#    close( FH ) or S4P::perish( 18, "Failed to gunzip $local_file" );
    `gunzip $local_file`;
    $local_file =~ s/\.gz$//;
    unless (-f $local_file) {
        $self->{_error} = "Failed to create $local_file";
        return;
    }

    my $local_host = $self->getLocalHost();

    # Define a reference to a subroutine that will be a handler for
    # each Granule element in the xml file.
    # Because the handler is defined in this subroutine, it will have access
    # to the %partner_granules hash defined in this subroutine.
    my %partner_granules;
    my $GranuleHandler = sub {
        my ( $twig, $node ) = @_;

        my $GranuleId = $node->first_child_text( 'GranuleId' );
        my ( $OnlineAccessUrlsNode ) = $node->first_child( 'OnlineAccessUrls' );
        # There should always be an OnlineAccessUrls node
        return unless defined $OnlineAccessUrlsNode;
        foreach my $OnlineAccessUrl ( $OnlineAccessUrlsNode->children_text( 'OnlineAccessUrl' ) ) {
            # Get rid of any leading or trailing whitespace
            $OnlineAccessUrl =~ s/^\s+//;
            $OnlineAccessUrl =~ s/\s+$//;
            my ( $online_host, $online_file ) = $OnlineAccessUrl =~ m|.+://(.+?)/.+/(.+)$|;
            ## Ignore files whose URL host does not match the S4PA host

            # Due to hosts/instances consolidation and host domain relocation,
            # published granule might have different hostname than the current
            # hostname. Besides, there are no more dataset that are split onto
            # different instances. Therefore, there is no need to compare 
            # hostname any more. Comparing is actually breaking the reconciliation.
            # next unless ( $online_host =~ /^$local_host/ );

            # With multi-file granules it is possible for the
            # same file to appear in more than one granule. Therefore,
            # we save the filenames in a hash in order to eliminate
            # duplications. Doing this, we won't be able to detect if
            # the partner has a granule more than once, we will assume
            # that they don't.

            if ( $self->reconcileAllFiles() ) {
                # Return the name of every online file the partner has
                $partner_granules{$online_file} = $OnlineAccessUrl;
            } else {
                # Return only the names of the online files that match
                # the granule id, i.e. those that are granule files
                next unless ( $online_file eq $GranuleId );
                $partner_granules{$GranuleId} = $OnlineAccessUrl;
            }
        }
        $twig->purge;
    };

    my $handlers = {'Granule' => $GranuleHandler};
    my $twig = new XML::Twig( TwigRoots => {'Granule' => 1},
                              TwigHandlers => $handlers );
    $twig->parsefile( $local_file );
    $twig->purge;

    unlink $local_file;

    # For gionvanni deletion, we need to carry the online URL.
    # So, instead of returning just an array, we will return a hash.
    # return sort( keys %partner_granules );
    my %partner_urls;
    foreach my $granule ( sort keys %partner_granules ) {
        $partner_urls{$granule} = $partner_granules{$granule};
    }
    return %partner_urls;
}


sub getPartnerGranuleList {
    my ( $self, %arg ) = @_;

    my ( $granuleCount ) = $self->getPartnerGranuleCount( %arg );

    if ( wantarray ) {
        if ( $granuleCount < $self->getLargeCountThreshold ) {
            return $self->getPartnerSmallGranuleList( %arg );
        } else {
            return $self->getPartnerLargeGranuleList( %arg );
        }
    } else {
        return $granuleCount;
    }
}


sub AUTOLOAD {
    my ( $self, @arg ) = @_;
    if ( $AUTOLOAD =~ /.*::onError/ ) {
        return ( $self->getErrorMessage() eq '' ? 0 : 1 );
    } elsif ( $AUTOLOAD =~ /.*::getErrorMessage/ ) {
        return $self->{_error};
    } elsif ( $AUTOLOAD =~ /.*::onDebug/ ) {
        return $self->{debug};
    } elsif ( $AUTOLOAD =~ /.*::reconcileAllFiles/ ) {
        return $self->{reconcile_all_files};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPushHost/ ) {
        return $self->{ftp_push_host};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPushUser/ ) {
        return $self->{ftp_push_user};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPushPwd/ ) {
        return $self->{ftp_push_pwd};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPushDir/ ) {
        return $self->{ftp_push_dir};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPushQuiescentTime/ ) {
        return $self->{ftp_push_quiescent_time};
    } elsif ( $AUTOLOAD =~ /.*::getFtpPullTimeout/ ) {
        return $self->{ftp_pull_timeout};
    } elsif ( $AUTOLOAD =~ /.*::getLargeCountThreshold/ ) {
        return $self->{large_count_threshold};
    } elsif ( $AUTOLOAD =~ /.*::getEndpointRoot/ ) {
        return $self->{client}->{endpoint_root};
    } elsif ( $AUTOLOAD =~ /.*::getServiceTimeout/ ) {
        return $self->{client}->{service_timeout};
    } elsif ( $AUTOLOAD =~ /.*::getServiceNs/ ) {
        return $self->{client}->{service_ns};
    } elsif ( $AUTOLOAD =~ /.*::soapServiceNsData/ ) {
        my %arg = @arg;
        return SOAP::Data->uri( $self->getServiceNs() )
                         ->name( $arg{NAME} )
                         ->type( $arg{TYPE} )
                         ->value( $arg{VALUE} );
    } elsif ( $AUTOLOAD =~ /.*::DESTROY/ ) {
    } else {
        my $method = $AUTOLOAD;
        $method =~ s/.*:://;
        $method = "SUPER::$method";
        $self->$method(@_);
    }
}
1;
