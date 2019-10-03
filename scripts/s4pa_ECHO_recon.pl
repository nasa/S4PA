#!/usr/bin/perl

=head1 NAME

s4pa_ECHO_recon.pl - ECHO reconciliation

=head1 PROJECT

GES DISC

=head1 SYNOPSIS

s4pa_ECHO_recon.pl
B<-r> I<s4pa_root_directory>
B<-e> I<ECHO_granule_dump>
[B<-c> I<csv-output-file>]
[B<-d> I<daacless_output_file>]


=head1 DESCRIPTION

I<s4pa_ECHO_recon.pl> processes the <ECHO_granule_dump> text file listing the
Granule URs (and possibly other fields) of the ECHOS4PA group.  It generates a
report for all the datasets on this S4PA instance indicating the number of
matches, the number of granules missing from ECHO (ECHOLESS), the number missing
from the instance (according to the granule.db) but present in ECHO (DAACLESS),
and the percent of those in the granule.db that are in ECHO.  It also lists
dataset present in the instance (according to
<s4pa_root_directory>/storage/dataset.cfg) but missing entirely from ECHO.
Datasets present in <ECHO_granule_dump> but not in this instance are passed over
and presumed present on other instances.

=head1 OPTIONS

=over 4

=item B<-p> I<s4pa_root_directory>

The root of S4PA, typically /vol1/<mode>/s4pa.

=item B<-s> I<ECHO_granule_dump>

Text file containing Granule URs.  The lines may contain any information as long
as the Granule UR is the only space-delimited item that looks like
<shortname>.<version>:<filename>. <ECHO_granule_dump> is presumed to be sorted
in order by dataset, version, and filename (i.e., by Granule UR).

=item [B<-c> I<csv-output-file>]

Optional comma-separated-value version of statistics for input to spreadsheets.  Same as
stdout without headings and totals.

=item [B<-d> I<daacless_output_file>]

Optional list of granule URs that are DAACLESS and should be deleted from ECHO.

=back

=head1 AUTHOR

Randy Barth
Tim Dorman

=head1 ADDRESS

ADNET, Code 610.2, Greenbelt, MD  20771

=head1 CREATED

8/31/06
9/12/06  Added Totals, CVS and DAACLESS files; reformatted stdout
9/18/06  Added recovery for missing granule.db file; allow ECHO dump to start with UR; die if ECHO input not sorted

=cut

# $Id: s4pa_ECHO_recon.pl,v 1.5 2006/10/10 15:00:30 s4pa Exp $
# -@@@ s4pa_ECHO_recon.pl, Version $Name:  $
#########################################################################

use Getopt::Std;
use strict;
use Safe;
use S4PA::Storage;


use vars qw($opt_r $opt_e $opt_c $opt_d);

getopts('r:e:c:d:');

# Report usage if not correct.
die "Usage: s4pa_ECHO_recon.pl\n\t-r <s4pa_root_directory>\n\t-e <ECHO_granule_dump>" .
    "\n\t[-c <csv-output-file>]\n\t[-d <daacless_output_file>]\n\t\t"
    if $opt_r eq '' or $opt_e eq '';

our $class = read_config_file("$opt_r/storage/dataset.cfg") or die "Can't read dataset.cfg.";

# List of datasets on this S4PA instance.
our @daac_datasets = sort(keys(%{$class}));
# Entry in above list we are currently processing.
our $lastdaacds = -1;
# List of files in $daac_datasets[$lastdaacds].
our @daacfiles = ();
# File in above list we are curently processing (initially beyond the end of empty list.
our $lastfile = 0;
# New dataset, version, filename being remembered while '~' file returned to mark
#   end of previous dataset in ECHO list.
our ($holdechods, $holdechover, $holdechofn);
# Most recently returned dataset, version, and file name from ECHO list.
our ($currechods, $currechover, $currechofn);
# Counters for each dataset.
my ($daacless, $echoless, $matchcnt) = (0, 0, 0);
# Grand totals
my ($totdaacless, $totecholess, $totmatchcnt) = (0, 0, 0);

open ECHOFILE, $opt_e or die "Can't open -e $opt_e.";
if ($opt_c) {
    open CSVFILE, ">$opt_c" or die "Can't open -c $opt_c for writing.";
}
if ($opt_d) {
    open DAACLESSFILE, ">$opt_d" or die "Can't open -d $opt_d for writing.";
}

# Write header line.
print " PERCENT    MATCHES   DAACLESS   ECHOLESS DATASET\n";


# Prime the variables.
my ($echods, $echover, $echofn) = readecholine();
my ($daacds, $daacfn) = readdaacline();

# Find matching datasets.
while ($echods ne '~' or $daacds ne '~') {
    while ($echods lt $daacds) { # Extra ECHO dataset(s) probably on another machine; ignore.
        ($echods, $echover, $echofn) = readecholine();
    }
    if ($echods gt $daacds) { # Extra DAAC dataset missing in ECHO; note
        my $missingds = $daacds;
        while ($daacds eq $missingds) { # Skip over missing granules.
            ($daacds, $daacfn) = readdaacline();
            ++$echoless;
        }
        # Must subtract 1 from daacless count for '~'.
        --$echoless;
        print '  0.000%       Missing dataset ',
            sprintf('%10d', $echoless), " $missingds\n";
        $totecholess += $echoless;
        print CSVFILE "0%,,,$echoless,$missingds\n" if $opt_c;
        # Reset counters.
        ($daacless, $echoless, $matchcnt) = (0, 0, 0);
    } else { # Same dataset, match files.
        my $matchds = $daacds;
        if ($echofn lt $daacfn) { # Count and note DAACLESS.
            delete_daacless($echods, $echover, $echofn); # Tell ECHO to drop it.
            ++$daacless;
            ($echods, $echover, $echofn) = readecholine();
        } elsif ($echofn gt $daacfn) { # Count ECHOLESS.
            ++$echoless;
            ($daacds, $daacfn) = readdaacline();
        } else { # Count match (except phony ~ file).
            ++$matchcnt if $echofn ne '~';
            ($echods, $echover, $echofn) = readecholine();
            ($daacds, $daacfn) = readdaacline();
        }
        if ($matchds ne $daacds) { # We're on to a new daac data set.
            print sprintf('%7.3f%', ($matchcnt/($matchcnt + $echoless) * 100)),
                  sprintf(' %10d %10d %10d', $matchcnt, $daacless, $echoless),
                  " $matchds\n";
            print CSVFILE ($matchcnt/($matchcnt + $echoless) * 100),
                    "%,$matchcnt,$daacless,$echoless,$matchds\n" if $opt_c;
            $totdaacless += $daacless;
            $totecholess += $echoless;
            $totmatchcnt += $matchcnt;
            ($daacless, $echoless, $matchcnt) = (0, 0, 0);
        }
    }
}
print sprintf('%7.3f%', ($totmatchcnt/($totmatchcnt + $totecholess) * 100)),
                  sprintf(' %10d %10d %10d', $totmatchcnt, $totdaacless, $totecholess),
                                    " TOTALS\n";
close CSVFILE if $opt_c;

###############################################################
# readdaacline returns next dataset and filename from granule.db list.
# It inserts an artificial '~' file at the end of each dataset
# and an artifical '~' dataset at the end of the whole list.

sub readdaacline {
    # Global variables we use.
    our @daac_datasets, $lastdaacds, @daacfiles, $lastfile;

    while ($lastfile >= $#daacfiles) { # Previous dataset complete or first dataset.
        # Note this loop normally executes only once unless there is an entry in
        # the dataset.cfg file that isn't in the tree structure.
        ++$lastdaacds;
        return ('~', '~') if $lastdaacds > $#daac_datasets;
        my $dbname = "$opt_r/storage/$class->{$daac_datasets[$lastdaacds]}/$daac_datasets[$lastdaacds]/granule.db";
        if (! -e $dbname) { # Misconfigured, so go on to next one.
            print "Missing granule.db for\t$daac_datasets[$lastdaacds]\n";
            next;
        }
        my ($granuleHashRef, $fileHandle) = S4PA::Storage::OpenGranuleDB($dbname, "r");
        die "Can't open $dbname." if not defined $granuleHashRef;
        @daacfiles = sort(keys(%$granuleHashRef));
        S4PA::Storage::CloseGranuleDB($granuleHashRef, $fileHandle);
        # Start just before first entry.
        $lastfile = -1;
    }
    while ($lastfile < $#daacfiles) { # Find a file to send back.
        ++$lastfile;
        # Return this file if it's the kind of file ECHO should have.
        return ($daac_datasets[$lastdaacds], $daacfiles[$lastfile]) if
            echoshouldhave($daac_datasets[$lastdaacds], $daacfiles[$lastfile]);
    }
    return ($daac_datasets[$lastdaacds], '~');
}

###############################################################
# readecholine returns next dataset, version, and filename from ECHO list.
# It inserts an artificial '~' file at the end of each dataset
# and an artifical '~' dataset at the end of the whole list.

sub readecholine {
    # Global variables we use.
    our ($holdechods, $holdechover, $holdechofn);
    our ($currechods, $currechover, $currechofn);
    if ($holdechods ne '') { # Then we sent '~' as last file.
        ($currechods, $currechover, $currechofn) =
                ($holdechods, $holdechover, $holdechofn);
        $holdechods = '';   # Only do it once.
        return ($currechods, $currechover, $currechofn);
    }
    my $line = <ECHOFILE>;
    $line = ' ~.~:~ ' if eof(ECHOFILE);
    # Pick pieces of Granule UR.
    my ($ignore, $ds, $ver, $fn) = $line =~ /(^|\s)([^. ]+)\.([^: ]+)\:(\S+)/;
    while ($ds eq '') { # Skip lines not containing granule URs from ECHO listing.
        $line = <ECHOFILE>;
        $line = ' ~.~:~ ' if eof(ECHOFILE);
        ($ignore, $ds, $ver, $fn) = $line =~ /(^|\s)([^. ]+)\.([^: ]+)\:(\S+)/;
    }
    # If first call, fill in $currechods and ver.
    ($currechods, $currechover, $currechofn) = ($ds, $ver, '') if $currechods eq '';
    if ($ds ne $currechods) { # Changing datasets, insert dummy file.
        die "ECHO dump is not sorted, $ds after $currechods" if $ds lt $currechods;
        ($holdechods, $holdechover, $holdechofn) = ($ds, $ver, $fn);
        return ($currechods, $currechover, '~');
    } else {
        die "ECHO dump is not sorted, $fn after $currechofn" if $fn le $currechofn and $fn ne '~';
        $currechofn = $fn;
    }
    return ($ds, $ver, $fn);
}


###############################################################
# Standard safe configuration read routine (from Subscription.pm).
# Returns only %data_class from named file.

sub read_config_file {
    my ($cfg_file) = shift;

    # Setup compartment and read config file
    my $cpt = new Safe('CFG');
    $cpt->share('%data_class');

    # Read config file
    $cpt->rdo($cfg_file) or die "Cannot read config file $cfg_file";

    # Check for required variables
    die "No data_class in $cfg_file" if ! %CFG::data_class;
    return (\%CFG::data_class);
}



###############################################################
# Must add write to file.
sub delete_daacless {
    my ($fn, $ver, $ds) = @_;
    print DAACLESSFILE "$fn.$ver:$ds\n" if $opt_d;
    return;
}

###############################################################
# Simple-minded test for ECHO holdings: it doesn't have .xml files.
sub echoshouldhave {
    my ($ds, $fn) = @_;
    return ($fn !~ /\.xml$/);
}
