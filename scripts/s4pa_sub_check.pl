#!/usr/bin/perl

=head1 NAME

s4pa_sub_check.pl - Check an XML metadata file to see if it matches specified crteria

=head1 SYNOPSIS

s4pa_sub_check.pl
[B<-b> I<YYYY-MM-DDTHH:MM:SSZ>]
[B<-e> I<YYYY-MM-DDTHH:MM:SSZ>]
[B<-n> I<data file count per granule>]
[B<-v>
[attr=value]
data_file
metadata_file

=head1 DESCRIPTION

s4pa_sub_check.pl checks an S4PA XML metadata file to see if fulfills a given
set of basic subscription criteria.
It returns 0 if there is a match, 1 on error, and 2 if there is no match.

=head1 ARGUMENTS

=over 4

=item B<-v>

Verbose mode.

=item B<-n>

Data file count per granule. By default it is 1.

=item B<-b> I<YYYY-MM-DDTHH:MM:SSZ>

Beginning of subscription period:
exit 0 only if data end date/time is greater than or equal to this date/time.

=item B<-e> I<YYYY-MM-DDTHH:MM:SSZ>

End of subscription period:
exit 0 only if data begin date/time is less than or equal to this date/time.

=item attr=value

Product-specific attributes.  
Only exact matches are supported at this time, e.g., ExpeditedData=FALSE.

=back

=head1 LIMITATIONS

This will work only on granules consisting of a single data file and a metadata file.

=head1 AUTHOR

Christopher Lynnes, NASA/GSFC, Code 610.2.

=cut
# -@@@ S4PA, Version $Name:  $

use vars qw($opt_b $opt_e $opt_v $opt_n);
use strict;
use S4PA::Metadata;
use Getopt::Std;
getopts('n:b:e:v');
my $met_file = pop @ARGV;
$opt_n = 1 unless ( $opt_n > 0 );

while( $opt_n-- ) {
    pop @ARGV;
}
my @qualifierList = @ARGV;
usage() unless $met_file;
my $metadata = S4PA::Metadata->new( FILE => $met_file );
if ( $metadata->onError() ) {
    warn "Error reading $met_file: " . $metadata->errorMessage();
    exit( 1 );
}

my $rc = check_qualify( $metadata, $opt_b, $opt_e, @qualifierList );
exit( $rc ? 0 : 2 );


sub usage {
    die "Usage: $0 [-n <data file count>] [-b <begin>] [-e <end>] [-v]"
        . " [attr=val] .... data_file met_file\n";
}

sub check_qualify
{
    my ( $metadata, $beginTime, $endTime, @qualifierList ) = @_;
    
    my ( $granBeginDate, $granBeginTime, $granEndDate, $granEndTime ) = (
            $metadata->getBeginDate(),
            $metadata->getBeginTime(),
            $metadata->getEndDate(),
            $metadata->getEndTime()
            );   
    my ( $granBeginDateTime, $granEndDateTime );
    if ( $beginTime =~ /^\d{4}-\d{2}-\d{2}/ 
        || $endTime =~ /^\d{4}-\d{2}-\d{2}/ ) {
        # Case of 'YYYY-MM-DDTHH:MM:SS'
        $granBeginDateTime = $granBeginDate . 'T' . $granBeginTime;
        $granEndDateTime = $granEndDate . 'T' . $granEndTime; 
    } elsif ( $beginTime =~ /^T\d{2}:\d{2};\d{2}/ 
        || $endTime =~ /^T\d{2}:\d{2}:\d{2}/ ) {
        # Case of 'THH:MM:SS'
        $granBeginDateTime = 'T' . $granBeginTime;
        $granEndDateTime = 'T' . $granEndTime; 
    } else {
        warn "Format of begin and end time not supported!";
        return 0;
    }
    
    $granBeginDateTime =~ s/Z$//;
    $granEndDateTime =~ s/Z$//;
    
    print STDERR "Begin=$granBeginDateTime, End=$granEndDateTime\n" if $opt_v;

    return 0 if ( defined $endTime && ($granBeginDateTime gt $endTime) );
    return 0 if ( defined $beginTime && ($granEndDateTime lt $beginTime) );
    
    # Loop through each "PSA name=value"; compare with the value in metadata
    foreach my $qualifier ( @qualifierList ) {
        my ( $psaName, $psaValue ) = split( /=/, $qualifier );
        my $granPsaValue = $metadata->getValue(
            qq(//PSA/PSAName[text()="$psaName"]/../PSAValue) );
        # Remove leading and trailing white spaces
        $granPsaValue =~ s/^\s+|\s+$//g;
        # If a PSA value doesn't match, return false.
        return 0 if ( $granPsaValue ne $psaValue );
    }
    return 1;
}

