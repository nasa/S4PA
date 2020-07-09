#!/usr/bin/perl

=head1 NAME

s4pa_launchpad_token.pl - script for accquiring and validating Launchpad token

=head1 SYNOPSIS

s4pa_launchpad_token.pl
B<-c> I<certificate_PFX_file>
B<-p> I<passcode_file>
B<-u> I<Launchpad_service_base_URI>
[B<-s> I<existing_token_for_validation>]

=head1 DESCRIPTION

s4pa_launchpad.pl uses the combination of our CMR service account PFX certificate 
and the passcode to request a token from NASA Launchpad for our CMR publication
related tasks. CMR is transitioning from ECHO token to Launchpad toke for each
data provider to ingest both collections and granules metadata. The Launchpad token
has only one hour of lifespan. This script will also do a validation on the
provided token to make sure it is still active. If not, it will request for a
new token. Token will be return in a JSON format object via STDOUT.

=head1 ARGUMENTS

=over 4

=item B<-c> I<certificate_PFX_file>

Path to the certificate FPX file.

=item B<-p> I<passcode_file>

Path to the certificate file's passcode.

=item B<-u> I<Launchpad_service_base_URI>

Base URI to Launchpad service.

=item B<-s> I<existing_token_for_validation>

Optional. Token validation if provided.

=back

=head1 AUTHORS

Guang-Dih Lei (Guang-Dih.Lei@nasa.gov)

=cut

################################################################################
## $Id: s4pa_launchpad_token.pl,v 1.1 2020/05/14 11:49:00 s4pa Exp $
## -@@@ S4PA, Version $Name:  $
#################################################################################

use strict;
use JSON;
use Getopt::Std;
use HTTP::Request;
use HTTP::Headers;
use LWP::UserAgent;
# use LWP::Protocol::https;
# use Crypt::SSLeay;
use vars qw($opt_c $opt_p $opt_u $opt_s);

getopts('c:p:u:s:');
usage() unless ($opt_c && $opt_p && $opt_u);
my $pfxFile = $opt_c;
my $passFile = $opt_p;
my $tokenUri = $opt_u;
my $smToken = $opt_s;

my ($cmrToken, $errmsg);
if (defined $smToken) {
    ($cmrToken, $errmsg) = launchpad_token($tokenUri, $pfxFile, $passFile, $smToken);
} else {
    ($cmrToken, $errmsg) = launchpad_token($tokenUri, $pfxFile, $passFile);
}

my $result;
if ($cmrToken) {
    $result->{'status'} = 'success';
    $result->{'sm_token'} = $cmrToken;
} else {
    $result->{'status'} = 'faile';
    $result->{'message'} = $errmsg;
}
print to_json($result, {pretty => 1});
exit;

sub launchpad_token {
    my ($tokenUri, $pfxFile, $passFile, $smToken) = @_;

    # overloading with existing token
    # if specified, do validation. Return token if validated, otherwise get new token
    # if not specified, get new token
    my ($passCode, $token, $errmsg);
    if (open(FH, "<$passFile")) {
        $passCode = <FH>;
        chomp($passCode);
        close(FH);
    } else {
        $errmsg = "Can not open passcode file $passFile: $!";
        return (0, $errmsg); 
    }

    $tokenUri =~ s#/$##;
    my $tokenUrl = $tokenUri . "/gettoken";
    my $validateUrl = $tokenUri . "/validate";

    # settting up environments for PFX file and passcode
    local $ENV{HTTPS_PKCS12_FILE} = $pfxFile;
    local $ENV{HTTPS_PKCS12_PASSWORD} = $passCode;
    # force LWP::UserAgent to use Net::SSL socket instead of the default IO::Socket:SSL.
    local $ENV{PERL_NET_HTTPS_SSL_SOCKET_CLASS} = "Net::SSL";

    # disable hostname verfication
    my $ua = LWP::UserAgent->new(ssl_opts => {verify_hostname => 0});

    my $validated;
    if ($smToken) {
        ($validated, $errmsg) = validate_token($ua, $validateUrl, $smToken);
        return ($smToken, $errmsg) if ($validated);
    }

    ($token, $errmsg) = get_token($ua, $tokenUrl);
    undef $ua;
    delete $ENV{HTTPS_PKCS12_FILE};
    delete $ENV{HTTPS_PKCS12_PASSWORD};
    delete $ENV{PERL_NET_HTTPS_SSL_SOCKET_CLASS};
    return ($token, $errmsg);
}

sub validate_token {
    my ($ua, $url, $token) = @_;

    # curl command for validate token from Launchpad
    # curl -XPOST --cert-type P12 --cert $pfxFile:$pcode -H "Content-Type: application/json" https://api.launchpad.nasa.gov/icam/api/sm/v1/validate -d@{"token": "xxxxxxxx"}
    my $validated = 0;
    # to validate a token, post the token in JSON
    my $headers = HTTP::Headers->new();
    $headers->header('Content-Type', 'application/json');
    my $tokenJson = {};
    $tokenJson->{'token'} = $token;
    my $json = to_json($tokenJson);
    my $request = HTTP::Request->new('POST', $url, $headers, $json);
    my $response = $ua->request($request);

    # validation return result also in JSON format if success
    my $errmsg;
    if ($response->is_success) {
        my $lpResponse = from_json($response->content);
        $validated = 1 if ($lpResponse->{'status'} eq 'success');
    # otherwise, error message should be in content
    } else {
        $errmsg = $response->message;
        chomp($errmsg);
    }

    return ($validated, $errmsg);
}

sub get_token {
    my ($ua, $url) = @_;

    # curl command for getting token from Launchpad
    # curl -i --cert $pfxFile:$pcode --cert-type P12 https://api.launchpad.nasa.gov/icam/api/sm/v1/gettoken
    my $token = 0;
    my $request = HTTP::Request->new('GET', $url);
    my $response = $ua->request($request);

    # token agent return in JSON format
    my $errmsg;
    if ($response->is_success) {
        my $lpResponse = from_json($response->content);
        if (exists $lpResponse->{'status'} and $lpResponse->{'status'} = "success") {
            $token = $lpResponse->{'sm_token'};
        } else {
            $errmsg = $response->message;
            chomp($errmsg);
        }
    } else {
        $errmsg = $response->message;
        chomp($errmsg);
    }

    return ($token, $errmsg);
}

sub usage {
    my $usage = "Usage: $0 -c <certificate_PFX_file> -p <passcode_file> -u <Launchpad_base_URI>
    [-s existing_token_for_validation]";
    print "$usage\n";
    exit 1;
}

