#!/usr/bin/perl

=head1 NAME

s4pa_fetch_docs.pl - a script to HTTP fetch or CVS checkout/fetch documents

=head1 SYNOPSIS

s4pa_fetch_docs.pl 
[B<-w> I<URL base for HTTP fetch>]
[B<-u> I<Username on discette>]
[B<-d> I<Local path for files>]
[B<-h> I<switch for helpline>]

=head1 DESCRIPTION

Fetches documents from CVS repository.

=head1 ARGUMENTS

=over 4

=item B<-w> I<file URL base>

URL base for files to fetch

=item B<-u> I<Username>

uid on discette

=item B<-d> I<Local path>

Local directory to hold the files

=item B<-h> I<Switch to print help>

Switch to print help lines.

=back

=head1 AUTHOR

F. Fang

=cut
################################################################################
# $Id: s4pa_fetch_docs.pl,v 1.2 2008/05/28 17:40:31 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use Getopt::Std;
use File::Basename;
use LWP::Simple;
use S4P;

my $opt = {};

# Get command line arguments
getopts( "w:u:d:h", $opt );

usage() if $opt->{h};

die "Please provide documents to be placed in CVS"
   unless @ARGV;

my @documentPathList = @ARGV;
my $destinDir = ".";
$destinDir = $opt->{d} if defined $opt->{d};
my $errorCode = 0;

my $url = $opt->{w};
if ($url) {
    $url .= "/" if ($url !~ /\/$/);
    $destinDir .= "/" if ($destinDir !~ /\/$/);
    foreach my $filePath (@documentPathList) {
        my $fileName = basename $filePath;
        my $fromURL = $url . "$fileName";
        my $toURL = $destinDir . "$fileName";
        my $status = getstore($fromURL, $toURL);
        if (is_error($status)) {
            $errorCode = 1;
            print "Error: cannot fetch $fromURL to $toURL ($status)\n";
            next;
        } else {
            print "File $fromURL fetched to $toURL\n";
        }
    }       
} else {
    die "Specify discette username (-u)"
       unless defined $opt->{u};
    my $cvsRepos = "S4PA/doc";
    my @documents = ();
    my $chkStatusList = '';
    foreach my $filePath (@documentPathList) {
        my $fileName = basename $filePath;
        push @documents, $fileName;
        $chkStatusList .= "cvs -d /tools/gdaac/cvsroot status ~/$cvsRepos/$fileName;";
    }

    # SSH to the CVS repository on $host
    # Assume account for $username is already set up on $host
    # Public/private keys can be set up for no-password access

    my $userName = $opt->{u};
    my $host = "discette.gsfc.nasa.gov";
    my $ssh = "ssh $userName\@$host ";

    # check out CVS repository for the documents at user's
    # home directory on $host

    my $cvs_co = "$ssh 'cvs -d /tools/gdaac/cvsroot checkout $cvsRepos;$chkStatusList' ";
    my $status = `$cvs_co `;
    print "CVS checkin status string: $status\n";
    if ($?) {
        print "Error: cannot execute $cvs_co on $host ($?)\n";
        exit 1;
    }

    # check if file has older version in CVS
    # and establish file copy command;
    my @addCVSList = ();
    my $cvsCopy = '';
    foreach my $fileName (@documents) {
        if ($status =~ /no\sfile\s$fileName/) {
            print "Error: no file $fileName in CVS\n";
            $errorCode = 1;
            next;
        }
        $cvsCopy .= "$userName\@$host:~/$cvsRepos/$fileName ";
    }
    $cvsCopy .= "$destinDir ";
    my @status = `$cvsCopy `;
    print "SCP status string: @status\n";
    if ($?) {
        print "Error: cannot execute $cvsCopy on $host ($?)\n";
        exit 1;
    }
}

exit $errorCode;

sub usage {
    my $usage = "
Usage: $0 -w baseURL -u userName -d localPath -h files
  -w URL base to fetch the files\r
  -u username to log on to CVS host\r
  -d local directory to hold the files; default is '.'\r
  -h print this help\r
Specify either -w or -u\n";
    S4P::perish(1, $usage);
}
