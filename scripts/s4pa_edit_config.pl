#!/usr/bin/perl

=head1 NAME

s4pa_edit_config.pl - a script to modify (using oxygen) subscription config
or descriptor file and install for S4PA

=head1 SYNOPSIS

s4pa_edit_config.pl 
[B<-n> I<S4PA instance name>]
[B<-u> I<Username on discette>]
[B<-r> I<S4PA release number>]
[B<-e> I<descriptor/subscription switch>]
[B<-d> I<editor specifier>]
[B<-s> I<switch to reverse installation>]
[B<-h> I<switch for helpline>]

=head1 DESCRIPTION

Updates subscription information or descriptor file in XML format and
run s4pa_update_subscription.pl

=head1 ARGUMENTS

=over 4

=item B<-n> I<S4PA instance name>

S4PA instance name

=item B<-u> I<Username>

uid on discette

=item B<-r> I<S4PA release>

S4PA release tag.

=item B<-e> I<description/subscription file>

Specifying editing descriptor or subscription config file

=item B<-d> I<editor specifier>

Specifying editor for XML file

=item B<-s> I<switch to reverse installation>

switch to reverse installation to previous version

=item B<-h> I<Switch to print help>

Switch to print help lines.

=back

=head1 AUTHOR

F. Fang

=cut
################################################################################
# $Id: s4pa_edit_config.pl,v 1.7 2008/05/28 17:40:31 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use Getopt::Std;
use S4P;

my $opt = {};

# Get command line arguments
getopts( "n:u:e:d:r:sh", $opt );

usage() if $opt->{h};

die "Specify S4PA instance name (-n)" 
    unless defined $opt->{n};
die "Specify discette username (-u)"
    unless defined $opt->{u};
die "Specify editing descriptor or subscription file (-e)"
    unless defined $opt->{e};

my $s4pa_instance = $opt->{n};
my $s4pa_releaseID;
if ($opt->{r}) {
    if ($opt->{r} =~ /(\d+)\.(\d+)\.(\d+)/) {
        $s4pa_releaseID = "Release-$1_$2_$3";
    } else {
        die "Specified S4PA release ID shall contain 3 release numbers separated by dots (.)\n"
    }
}
my $cvstag = $s4pa_releaseID;
    
# SSH to the CVS repository on $host
# Assume public/private keys for $username is already setup

my $username = $opt->{u};
my $host = "discette.gsfc.nasa.gov";
my $ssh = "ssh $username\@$host ";

# CVS checkout project SUBSCRIBE_CONFIG

my $project = "SUBSCRIBE_CONFIG";
my $cvs_chkout = "cvs -d /tools/gdaac/cvsroot checkout $project";
my @status = `$ssh $cvs_chkout `;
print "CVS checkout status string: @status\n";
if ($?) {
    die "cannot execute $cvs_chkout on $host\n";
}

# CVS checkout the schema file

my $schema_file = "S4paSubscription.xsd";
my $schema_path = "S4PA/doc/xsd/$schema_file";
my $cvs_schema = "cvs -d /tools/gdaac/cvsroot checkout $schema_path";
my $cvs_co_schema;
if ($cvstag) {
    print "checking out schema file with cvs tag $cvstag\n";
    $cvs_co_schema = "cvs -d /tools/gdaac/cvsroot checkout -r $cvstag $cvs_schema";
} else {
    $cvs_co_schema = "cvs -d /tools/gdaac/cvsroot checkout $cvs_schema";
}
@status = `$ssh $cvs_schema `;
print "CVS checkout status string: @status\n";
if ($?) {
    die "cannot execute $cvs_schema on $host\n";
}

# place a copy of schema file under $project

my $makecopy = "$ssh 'cp $schema_path $project' ";
@status = `$makecopy `;
print "copy schema file status string: @status\n";
if ($?) {
    die "cannot copy $schema_path to $project on $host\n";
}

my $edit_file;
my $copy_file;
my $descriptor_file = $s4pa_instance . "_descriptor.xml";
my $subscription_file = $s4pa_instance . "_subscription.xml";

if ($opt->{e} =~ /^[dD]/) {
    $edit_file = "$project/$descriptor_file";
    $copy_file = "$project/$subscription_file";
} else {
    $edit_file = "$project/$subscription_file";
    $copy_file = "$project/$descriptor_file";
}

# specify editor
my $editor = "oxygen";
my $editorpath = "/usr/local/bin";
if ($opt->{d}) {
    $editor = $opt->{d};
}

if ($editor eq "vi" || $editor eq "vim" || $editor eq "emacs") {
    $editorpath = "/usr/bin";
} elsif ($editor eq "nedit") {
    $editorpath = "/usr/X11R6/bin";
} elsif ($editor eq "oxygen") {
    $editorpath = "/usr/local/bin";
} else {
    die "editor $editor not supported\n";
}

# Find the current cvs revision of $edit_file if reverse install;
# call oxygen to edit otherwise

if ($opt->{s}) {
    my $cvs_version;
    my $logfile = "revision.log";
    my $revision_session = "cvs -d /tools/gdaac/cvsroot status $edit_file > $logfile";
    @status = `$ssh $revision_session `;
    print "Revision status string: @status\n";
    if ($?) {
        die "cannot execute $revision_session on $host\n";
    }

    if (-e $logfile) {
        open LOG, "<$logfile";
        while (<LOG>) {
            if (/Working\srevision:\s+(\d+)\.(\d+)\s+/) {
                my $vs1 = $1;
                my $vs2 = $2;
                if ($vs2<1) {
                    $vs1 -= 1;
                } else {
                    $vs2 -= 1;
                }
                $cvs_version = $vs1 . ".$vs2";
                last;
            }
        }
        close LOG;
        die "CVS current version not found\n" if (!$cvs_version);
    } else {
        die "file $logfile does not exist locally\n";
    }

    # CVS checkout the version to be restored
    my $config_delete = "$ssh rm -rf $edit_file ";
    @status = `$config_delete `;
    print "Configuration file deletion status string: @status\n";
    if ($?) {
        die "cannot execute $config_delete on $host\n";
    }

    print "Checking out cvs version $cvs_version for $edit_file\n";

    my $cvs_chkout = "cvs -d /tools/gdaac/cvsroot checkout -r $cvs_version $edit_file";
    my @status = `$ssh $cvs_chkout `;
    print "CVS checkout status string: @status\n";
    if ($?) {
        die "cannot execute $cvs_chkout on $host\n";
    }

} else {
    # edit the subscription file using specified editor; using oxygen as default
    if ($editor eq "vi" || $editor eq "vim") {
        $editor = "vim";
        my $edit_session = "ssh -t $username\@$host '$editorpath/$editor $edit_file' ";
        my @status = system($edit_session);
        print "Editing status string: @status\n";
        if ($?) {
            die "cannot execute $edit_session on $host\n";
        }
    } else {
        my $edit_session = "$editorpath/$editor $edit_file ";
        @status = `$ssh $edit_session `;
        print "Editing status string: @status\n";
        if ($?) {
            die "cannot execute $edit_session on $host\n";
        }
    }

    # user specify CVS commit log
    print "Please specify log for CVS commit $edit_file\n";
    print "Hit <return> when finish\n";
    my $cvslog = <STDIN>;
    chomp $cvslog;
    print "You enterred CVS commit log:\n";
    print "$cvslog\n";

    my $cvs_chkin = "$ssh 'cvs -d /tools/gdaac/cvsroot commit -m \"$cvslog \" $edit_file' ";
    @status = `$cvs_chkin `;
    print "CVS checkin status string: @status\n";
    if ($?) {
        die "cannot execute $cvs_chkin on $host\n";
    }

}

# copy the CVS-check-out files to local directory

my $scp = "scp -r $username\@$host:$edit_file . ";
@status = `$scp `;
print "SCP status string: @status\n";
if ($?) {
    die "cannot execute $scp $edit_file on $host\n";
}

$scp = "scp -r $username\@$host:$copy_file . ";
@status = `$scp `;
print "SCP status string: @status\n";
if ($?) {
    die "cannot execute $scp $copy_file on $host\n";
}

$scp = "scp -r $username\@$host:$schema_path . ";
@status = `$scp `;
print "SCP status string: @status\n";
if ($?) {
    die "cannot execute $scp $cvs_schema on $host\n";
}

# run installation script

die "no valid $descriptor_file locally\n" if (-z $descriptor_file);
die "no valid $subscription_file locally\n" if (-z $subscription_file);
die "no valid $schema_file locally\n" if (-z $schema_file);

my $run_install = "s4pa_update_subscription.pl -d $descriptor_file -f $subscription_file -s $schema_file ";
my $rc = S4P::exec_system($run_install);
if ($rc) {
    die "execution failure for $run_install; returning $rc\n";
}

# clean up
my $config_delete = "$ssh rm -rf $project ";
@status = `$config_delete `;
print "Configuration file deletion status string: @status\n";
if ($?) {
    die "cannot execute $config_delete on $host\n";
}
$config_delete = "$ssh rm -rf S4PA ";
@status = `$config_delete `;
print "Configuration file deletion status string: @status\n";
if ($?) {
    die "cannot execute $config_delete on $host\n";
}

exit 0;

sub usage {
    my $usage = "
Usage: $0 -n instance_name -u user_name -r release_tag -e d(escriptor)|s(ubscribe_config) [-d editor -s|(-b CVS_log -c) -h]
  -n S4PA instance name\r
  -u username to log on to CVS host\r
  -e s(ubscription) or d(escriptor)\r
  -d editor specification (vi, emacs, nedit, or oxygen; oxygen as default)\r
  -r S4PA release tag; optional\r
  -s switch to reverse installation to previous version; optional\r
  -h print this help\n";
    S4P::perish(1, $usage);
}
