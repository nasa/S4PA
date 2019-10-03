#!/usr/bin/perl 

=head1 NAME

s4pa_create_mirador_dpp.pl - Creates mirador Data Product Pages from 
GCMD DIFS

=head1 SYNOPSIS

s4pa_create_mirador_dpp.pl B<-m> I<shortNameMapFile> B<-t> I<templateFileName> 
 B<-l> I<logDirectoryName> B<-o> I<outputFileName> 

=head1 DESCRIPTION

s4pa_create_mirador_dpp.pl uses XML::Simple and LWP::Simple

=head1 AUTHOR

Lou Fenichel

=cut

################################################################################
# $Id: s4pa_create_mirador_dpp.pl,v 1.1 2006/03/30 20:30:09 fenichel Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################
# revised: 02/11/2006 lhf, creation
# revised: 03/21/2006 lhf, reworked as part of s4pa
# revised: 03/23/2006 lhf, add LWP::Simple
#
 
use Safe ;
use Getopt::Std ;
use XML::Simple ;
use LWP::Simple ;
use strict ;
 
$ENV{"SYBASE"} = "/tools/sybOC";
 
MAIN: {
    my @args = @ARGV ;
    my $opts = {} ; 
    getopts('m:t:l:o:h', $opts) ;
    usage() if $opts->{h} ;
 
    usage() if  checkArgs($opts) ;
    my $dataType = shift(@ARGV) or usage();

    our ( %shortNameMap ) ;
 
    my $cpt = new Safe 'CFG';
    $cpt->share( '%shortNameMap') ;
    $cpt->rdo( $opts->{m} ) or
        print "Unable to read $opts->{m}\n" ;
    my $snm = \%shortNameMap ;
 
    # create log file
    my $datetimestamp = `/bin/date +%Y%m%d%H%M%S` ;
    chomp( $datetimestamp ) ;
    my $logFileName = "$opts->{l}/s4pa_create_mirador_dpp_$datetimestamp.log" ;
    open( my $logFH, ">$logFileName")
      || die "Can't open logfile $logFileName\n" ;
    print $logFH "STARTTIME : ", scalar localtime, "\n", "$0 @args\n";
  
    # open template, create outfile
    open ( TEMPLATE, "$opts->{t}" ) || die "Can't open $opts->{t}\n" ;
    my $outFileName = "$opts->{l}/$0.$datetimestamp" ;
    open ( OUTFILE, ">$opts->{o}" ) || die "Can't open $opts->{o}\n" ;
 
    my $difFile =  LWP::Simple::get("http://gcmd.nasa.gov/OpenAPI/getdifs.py?query=[Entry_ID%3D'" . $snm->{$dataType} . "']") ;

    my $xml = new XML::Simple();
    my $root = $xml->XMLin( $difFile );
 
    while ( <TEMPLATE> ) {
        # not exactly sure why I need this, perhaps the way I xferred the template
        s/// ;
        if ( /<DPP/ ) {
            my $replacementString = extract_dif ( $_, $root, $snm ) ;
            #print "debug: \$replacementString(returned): $replacementString\n" ;
            print OUTFILE "$replacementString<p>\n" ;
        } else {
            print OUTFILE "$_" ;
        }
    }
    close ( OUTFILE ) ;
    print $logFH "ENDTIME : ", scalar localtime, "\n";
}
 
sub extract_dif {

    my ( $tag, $root, $snm ) = @_ ;
    my $replacementString ; 

    if ( $tag =~ /<DPPSHORTNAME>/ ) {
        $replacementString = $snm->{$root->{DIF}{'Entry_ID'}} ;
    }
    if ( $tag =~ /<DPPLONGNAME>/ ) {
        $replacementString = $root->{DIF}{'Entry_Title'} ;
    }
    if ( $tag =~ /<DPPPLATFORM>/ ) {
        $replacementString = list_values ( "Platform", "Long_Name", $root->{DIF}{'Source_Name'} ) ;
    }
    if ( $tag =~ /<DPPSENSOR>/ ) {
        $replacementString = list_values ( "Sensor", "Long_Name", $root->{DIF}{'Sensor_Name'} ) ;
    }
    if ( $tag =~ /<DPPRESOLUTION>/ ) {
        $replacementString .= "$root->{DIF}{'Data_Resolution'}{'Vertical_Resolution'}," if ( $root->{DIF}{'Data_Resolution'}{'Vertical_Resolution'} ) ;
        $replacementString .= "$root->{DIF}{'Data_Resolution'}{'Temporal_Resolution'}," if ( $root->{DIF}{'Data_Resolution'}{'Vertical_Resolution'} ) ;
        $replacementString .= "$root->{DIF}{'Data_Resolution'}{'Temporal_Resolution_Range'}," if ( $root->{DIF}{'Data_Resolution'}{'Temporal_Resolution_Range'} ) ;
        chop( $replacementString ) ;
    } 
    if ( $tag =~ /<DPPPARAMETERS>/ ) {
        $replacementString = list_values ( "Parameter", "Variable", $root->{DIF}{'Parameters'} ) ;
    }
    if ( $tag =~ /<DPPSUMMARY>/ ) {
        $replacementString = $root->{DIF}{'Summary'} ;
    }

    return ( $replacementString ) ;
}

sub list_values {
 
    my ( $name, $target, $ref ) = @_ ;
    my $values ;
    if ( ref($ref) eq 'ARRAY' ) {
      foreach my $hsh ( @{$ref} ) {
        foreach my $key ( keys %{$hsh} ) {
           $values .= "$hsh->{$key}," if ( $key eq $target ) ; 
        }
      }
      chop ( $values ) ;
    } else {
      $values = $ref->{$target} ;
    }
 
    return ( $values )  ;
}
 
sub checkArgs {
    my $opts = shift ;
 
    my $status = 0 ;
 
    if ( !($opts->{m}) ) {
        print "Failure to specify -m <shortNameMapFile> on command line.\n" ;
        $status = 1 ;
    }
    elsif ( !($opts->{t}) ) { 
        print "Failure to specify -t <templateFileName> on command line.\n" ;
        $status = 1 ;
    }
    elsif ( !($opts->{l}) ) { 
        print "Failure to specify -l <logDirectoryName> on command line.\n" ;
        $status = 1 ;
    }
    elsif ( !($opts->{o}) ) { 
        print "Failure to specify -o <outputFileName> on command line.\n" ;
        $status = 1 ;
    }
 
    return $status ;
}

sub usage {
    die << "EOF";
Usage: $0 -m <shortNameMapFile> -t <templateFileName> -l <logDirectoryName> <dataType>
EOF
}
 
exit ;
############ end of s4pa_create_mirador_dpp.pl #############
