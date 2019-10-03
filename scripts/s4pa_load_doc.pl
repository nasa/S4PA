#!/usr/bin/perl

=head1 NAME

s4pa_load_doc.pl - a CGI script to receive and place uploaded documents

=head1 SYNOPSIS

s4pa_load_doc.pl

=head1 DESCRIPTION

Receives and places uploaded documents.

=head1 ARGUMENTS

=head1 AUTHOR

F. Fang

=cut
################################################################################
# $Id: s4pa_load_doc.pl,v 1.5 2008/04/28 21:06:09 ffang Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use CGI;
use File::Path;
use File::Copy;
use File::Basename;
use Fcntl qw(:DEFAULT :flock);
use strict;
use constant BUFFER_SIZE	=> 16384;
use constant MAX_FILE_SIZE	=> 33554432;		# limit each upload to 33MB
use constant MAX_DIR_SIZE	=> 1000*MAX_FILE_SIZE;	# limit total uploads to 33GB

$CGI::DISABLE_UPLOADS = 0;
$CGI::POST_MAX = MAX_FILE_SIZE;

my $query = new CGI;
$query->cgi_error and error($query, "Error transferring file: " . $query->cgi_error);

# define location where uploaded files will be stored; notice that permissions to
# write are needed for upload directory
require "./cfg/s4pa_load_doc.cfg" ;
our ($uploadDir, $uploadLog);

unless (-d $uploadDir) {
    mkpath $uploadDir || error($query, "Failed to create upload directory");
    chmod 0777, $uploadDir;
}

if (dir_size($uploadDir)+$ENV{CONTENT_LENGTH} > MAX_DIR_SIZE) {
    error ($query, "Upload directory is full");
}

my $userString;
my $fileName;
my $filePath;
my $tempFile;
my $upload_filehandle;
# first-round of uploading without confirmation
if (!($query->param("continue"))) {
    if ( $query->param("goback") && $query->param("tempfile") ) {
        unlink($query->param("tempfile"));
        load_form($query);
        exit(0);
    }
    # parse userID
    $userString = $query->param("username") || error($query, "Please provide your username");;

    # parse the name of file
    $fileName = $query->param("document") || error($query, "No file received");
    $upload_filehandle = $query->upload("document");
    error($query, "Failed to obtain filehandle for upload file") if (!$upload_filehandle);

    # define upload file
    $fileName =~ s/.*[\/\\](.*)/$1/;
    $filePath = "$uploadDir/$fileName";

    # Write to a temporary local file
    $tempFile = "/var/tmp/" . $fileName . ".temp";
    open TEMPFILE, ">$tempFile" || error($query, "Cannot open file $tempFile for writing");
    # tells Perl to write the file in binary mode, rather than in text mode.
    binmode $upload_filehandle;
    my $buffer='';
    while (read($upload_filehandle, $buffer, BUFFER_SIZE)) {
        print TEMPFILE $buffer;
    }
    # close the file
    close TEMPFILE;

    # upon filename collision, warn uploader and provide continue option
    if ( -e $filePath ) {
        my %existFile = {};
        open HISTORY, "<$uploadLog" || error($query, "Cannot read $uploadLog");
        while (<HISTORY>) {
            chomp;
            my ($user, $time, $file) = $_ =~ /(\S+)\s([\w\s\:]+)\s(\S+)$/;
            if ($file eq $fileName) {
                $existFile{USER} = $user;
                $existFile{TIME} = $time;
                $existFile{FILE} = $file;
            }
        }
        close HISTORY;
        upload_warn($query, \%existFile, $tempFile, $filePath, $uploadLog);
    } else {
        upload_done($query, $userString, $tempFile, $filePath, $uploadLog);
    }
} else {
    my $user = $query->param("username");
    my $temp = $query->param("tempfile");
    my $file = $query->param("document");
    my $log = $query->param("logfile");
    upload_done($query, $user, $temp, $file, $log);
}
exit(0);

sub upload_done {
    my ($query, $userString, $tempFile, $filePath, $uploadLog) = @_;
    # overwrite existing with temporary file Upon confirmation
    my $fileName = basename $filePath;
    move($tempFile, $filePath) || error($query, "Cannot overwrite $filePath with $tempFile");
    # get time for upload log
    my $time = localtime;
    # obtain a lock to hsitory file
    open( LOCKFH, ">$uploadLog.lock" ) || error($query, "Failed to open lock file" );
    unless( flock( LOCKFH, 2 ) ) {
        close( LOCKFH );
        error($query, "Failed to get a file lock" );
    }
    # append upload info to history file
    open UPLOADHISTORY, ">>$uploadLog";
    print UPLOADHISTORY "$userString $time $fileName\n";
    close UPLOADHISTORY;

    # Remove lock
    close( LOCKFH );
    flock( LOCKFH, 8 );

    print $query->header();
    print <<END_HTML;

<HTML>
<HEAD>
<TITLE>Thanks!</TITLE>
</HEAD>

<BODY>

<P>Your have successfully uploaded file $fileName! </P>

</BODY>
</HTML>

END_HTML

}

sub upload_warn {
    my ($q, $fileHash, $tempFile, $filePath, $uploadLog) = @_;

    my $message = "File already exists";
    $message = "User $fileHash->{USER} last uploaded file $fileHash->{FILE} " .
               "at $fileHash->{TIME}" if ($fileHash);
    my $uploadFile = $q->param("document");
    my $userString = $q->param("username");
    my $action = "http://discette-internal.gsfc.nasa.gov/documentUpload_test.html";
    print $q->header();
    print <<WARN_PAGE;

<html>
<head>
<title>Warning message</title>
</head>
<body>
<p> $message. </p>
<p> Do you wish to overwrite existing file $uploadFile? </p>
<p> Please select to confirm or go back: </p>
<form method="post" enctype="multipart/form-data" action="s4pa_load_doc_test.pl">
<input type="hidden" name="username" value="$userString"/>
<input type="hidden" name="document" value="$filePath"/>
<input type="hidden" name="tempfile" value="$tempFile"/>
<input type="hidden" name="logfile" value="$uploadLog"/>
<input type="submit" name="continue" value="Confirm"/>
</form>
<form method="post" enctype="multipart/form-data" action="s4pa_load_doc_test.pl">
<input type="hidden" name="username" value="$userString"/>
<input type="hidden" name="tempfile" value="$tempFile"/>
<input type="submit" name="goback" value="Back"/>
</form>
</body>
</html>

WARN_PAGE
}

sub load_form {
    my $q = shift;
    my $userString = $q->param("username");
    print $q->header();
    print <<LOAD_PAGE;

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
          "http://www.w3.org/TR/html4/loose.dtd">
<html lang="en">
<head>
 <meta http-equiv="Content-Type" content="text/html;charset=iso-8859-1">
<title>Document Upload in HTML forms</title>
</head>
<body>

<h1>
<a name="start">Document Upload</a>
</h1>

<form action="http://discette-internal.gsfc.nasa.gov/cgi-bin/uploads/s4pa_load_doc_test.pl"
      enctype="multipart/form-data" method="post">
<p>
<label for="UNAME">
Please type in your user name:<br>
</label>
<input type="text" name="username" value="$userString" id="UNAME"/>
</p>

<p>
<label for="FNAME">
Please specify a file to upload:<br>
</label>
<input type="file" name="document" value="docinput" id="FNAME"/>
</p>
<div><input type="submit" value="Upload"></div>
</form>

</body>
</html>

LOAD_PAGE
}

sub dir_size {
    my $dir = shift;
    my $dirSize = 0;
    opendir DIR, $dir or die "Unable to open $dir: $!";
    while (readdir DIR) {
        $dirSize += -s "$dir/$_";
    }
    return $dirSize;
}

sub error {
    my ($q, $info) = @_;
    print $q->header("text/html"),
          $q->start_html("Error"),
          $q->h1("Error"),
          $q->p("Upload encountered the following error"),
          $q->p($q->i($info)),
          $q->end_html;
    exit;
}

sub info {
    my ($q, $info) = @_;
    print $q->header("text/html"),
          $q->start_html("INFO"),
          $q->h1("info"),
          $q->p($q->i($info)),
          $q->end_html;
}
