#!/usr/bin/perl

=head1 NAME

s4pa_get_test_data.pl - Test Data Generator

=head1 SYNOPSIS

s4pa_get_test_data.pl 
[B<-c> I<configFileName>]
[B<-s> I<secondsDelay>]

=head1 DESCRIPTION

s4pa_get_test_data.pl - The Test Data Generator is designed to simulate
data flows identical to those that arrive at ECS.  Both 'Golden' and
otherwise data are stored in 'staging' areas and are transferred by
ftp to s4pa polling locations.  

An example configuration file is given:

    type => 'EDOS'
    source => '/vol1/test/Golden_AIRS_Data'
    destination => '/ftp/data/TS2/EDOS_Simulator'
    [ pdr_destination => '/ftp/data/TS2/SIPS_NCEP/PDR' ] (SIPS only)
    hostname => 'discette.gsfc.nasa.gov'
    [ pdr_hostname => 'discette.gsfc.nasa.gov' ] ( SIPS only)
    username => 'anonymous'
    passwd => 'john.public\@gsfc.nasa.gov'
    providercfg => '/local/tools/gdaac/TS2/cfg/providers.cfg'
    provider => 'EDOS_AIRS'
    ignore_datatypes => [ 'AIRB0CAP' ]
    danseqno => 10000
    days => [ 245, 246 ]

Additional configurations (from providercfg above):
    EDOS_AIRS => [ 'AIR10SCC','AIR20SCI','AIRB0CAL','AIRB0SCI','AIRH1ENC',
                   'AIR10SCI','AIRB0CAH','AIRB0CAP','AIRH0ScE','AIRH2ENC' ] 
    SIPS_NCEP => [ 'PREPQC' ]

=head1 AUTHORS

Steve Kreisler (steve.kreisler@gsfc.nasa.gov)
Lou Fenichel (lou.fenichel@gsfc.nasa.gov)

=cut

################################################################################
# $Id: s4pa_get_test_data.pl,v 1.6 2006/05/18 12:17:43 lei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################
# 
# name: s4pa_get_test_data.pl
# 4/13/2006 added PDR.XFR file, same delay
# 4/17/2006 added processing for Expedited data sets, for these the
#           PDR is named EDR, and the PDS is an EDS that starts with 
#           an E instead of a P.  On the other hand, the PDR (for the
#           PDS, starts with an X while the EDR (for the EDS)((of course)),
#           starts with a Y.
# 4/20/2006 added type=SIPS
# 4/24/2006 added hours cfg, removal of temp files (after put)
# 5/15/2006 added ftp firewall server option
#

use strict ;
use Getopt::Std ;
use Net::FTP ;
use S4P ;
use Safe ;
use vars qw( $opt_c $opt_s $opt_v );

getopts('c:s:v');

MAIN: {

    # check command line parameters
    unless (defined($opt_c)) {
       S4P::logger("ERROR","Failure to specify -c <ConfigFile> on command line.") ;
       exit(2) ;
    }
    unless (defined($opt_s)) {
       S4P::logger("ERROR","Failure to specify -s <secondsDelay> on command line.") ;
       exit(2) ;
    }
    
    # retrieve configs
    my $cpt = new Safe 'CFG';
    $cpt->share( '$simcfg' ) ;
    $cpt->rdo($opt_c) or
        S4P::perish(3, "Cannot read config file $opt_c in safe mode: $!\n");
    
    # retrieve providers from $CFG::simcfg->{providercfg}
    my $cpt = new Safe 'CFG' ;
    $cpt->share( '$providers' ) ;
    $cpt->rdo($CFG::simcfg->{providercfg}) or
        S4P::perish(3, "Cannot read config file $CFG::simcfg->{providercfg} in safe mode: $!\n");
    
    # verbose
    if ( $opt_v ) {
        print "\nValues from $opt_c:\n" ;
        print "source: $CFG::simcfg->{source}\n" ;
        print "destination: $CFG::simcfg->{destination}\n" ;
        print "pdr_destination: $CFG::simcfg->{pdr_destination}\n" ;
        print "hostname: $CFG::simcfg->{hostname}\n" ;
        print "pdr_hostname: $CFG::simcfg->{pdr_hostname}\n" ;
        print "provider: $CFG::simcfg->{provider}\n" ;
        print "providercfg: $CFG::simcfg->{providercfg}\n" ;
        print "ignore_datatypes: @{$CFG::simcfg->{ignore_datatypes}}\n" ;
        print "days: @{$CFG::simcfg->{days}}\n" ;
        print "$CFG::simcfg->{provider}: @{$CFG::providers->{$CFG::simcfg->{provider}}}\n" ; 
        print "\n" ;
    }

    # make ftp connection for data
    my $data_debug ;

    # specify $FTP_FIREWALL shell env. variable to enale ftp through firewall
    my $firewall = $ENV{FT_FIREWALL} ? $$ENV{FT_FIREWALL} : undef;
    my $firewallType = $ENV{FTP_FIREWALL_TYPE} ? $ENV{FTP_FIREWALL_TYPE} : 1;

    # Open FTP connection
    my $data_ftp = Net::FTP->new($CFG::simcfg->{hostname}, Debug => $data_debug,
      Firewall => $firewall, FirewallType => $firewallType );

    unless ($data_ftp) {
      S4P::logger('ERROR', "$@\n");
      return -1;
    }
    unless ($data_ftp->login($CFG::simcfg->{username},$CFG::simcfg->{passwd}) ) {
      S4P::logger('ERROR', "Couldn't login\n");
      return -1;
    }

    # set destination directory
    $data_ftp->cwd( $CFG::simcfg->{destination} ) ;

    # set for binary
    $data_ftp->binary ;
  
    # make ftp connnection for pdrs (SIPS only)
    my $pdr_debug ;
    my $pdr_ftp ;
    if ( $CFG::simcfg->{type} eq 'SIPS' ) {
        $pdr_debug ;
        $pdr_ftp = Net::FTP->new($CFG::simcfg->{pdr_hostname}, Debug => $pdr_debug);
        unless ($pdr_ftp) {
          S4P::logger('ERROR', "$@\n");
          return -1;
        }
        unless ($pdr_ftp->login($CFG::simcfg->{username},$CFG::simcfg->{passwd}) ) {
          S4P::logger('ERROR', "Couldn't login\n");
          return -1;
        }
    }

    # set destination directory for the pdr (SIPS only)
    $pdr_ftp->cwd( $CFG::simcfg->{pdr_destination} ) if ( $CFG::simcfg->{type} eq 'SIPS' ) ;

    # do one day at-a-time, all datatypes
    my $dan_seq_no = $CFG::simcfg->{danseqno} ;
    foreach my $day ( @{$CFG::simcfg->{days}} ) {
        # by hour
        foreach my $hour ( @{$CFG::simcfg->{hours}} ) {
            # each datatype in turn
            foreach my $datatype ( @{$CFG::providers->{$CFG::simcfg->{provider}}} ) {
                next if ( grep /$datatype/, @{$CFG::simcfg->{ignore_datatypes}} ) ;
                put_files ( "$CFG::simcfg->{source}/$datatype/$day/$hour", $data_ftp, $pdr_ftp, $datatype, ++$dan_seq_no ) 
                  if ( -e "$CFG::simcfg->{source}/$datatype/$day/$hour" ) ;
            }
        }
    }
    $data_ftp->quit() or S4P::logger('WARN', "couldn't quit.\n");
    $pdr_ftp->quit() or S4P::logger('WARN', "couldn't quit.\n") if ( $CFG::simcfg->{type} eq 'SIPS' ) ;

}

# given a day/hour directory of files, create the PDR and push it,
# push the data and XFR files in order, pausing $opt_s seconds before
# the XFR file is sent 

sub put_files {

  my ( $dir, $data_ftp, $pdr_ftp, $datatype, $dan_seq_no ) = @_ ;

  my @files ;
  # some SIPS deliver .met files (I think)
  if ( $CFG::simcfg->{ignore_metfiles} ) {
      @files = readpipe ( "ls $dir | grep -v met" ) ;
  } else {
      @files = readpipe ( "ls $dir" ) ;
  }
 
  my $startPDRSession = time ;
  
  # foreach file, if XFR then wait $opt_s
  foreach my $filename ( @files ) {

      chomp ( $filename ) ;
      `sleep $opt_s` if ( $filename =~ /XFR$/ && $CFG::simcfg->{type} eq 'EDOS' ) ;

      # transfer the file
      return -1 if ( put_single_file ( "$dir/$filename", $data_ftp, 0 ) ) ;

      # for SIPS there is no transfer file so we sleep after the data file is staged
      `sleep $opt_s` if ( $CFG::simcfg->{type} eq 'SIPS' ) ;

  }

  # create and push pdr
  my $filecount = @files ;
  if ( $filecount ) {
      if ( $CFG::simcfg->{type} eq 'EDOS' ) {
         return -1 if ( put_pdr ( $data_ftp, $dir, $datatype, $startPDRSession, $dan_seq_no, @files ) ) ;
      }
    
      if ( $CFG::simcfg->{type} eq 'SIPS' ) {
         return -1 if ( put_SIPS_pdr ( $pdr_ftp, $dir, $datatype, $startPDRSession, @files ) ) ;
      }
  }

  return(0) ;

}

sub put_single_file {
 
    my ( $filename, $data_ftp, $remove ) = @_ ;

    if ( $opt_v ) {
        my @results = readpipe ( "ls -ltr $filename" ) ;
        print "@results\n\n" ;
    }

    if ($data_ftp->put( $filename )) {
        S4P::logger('INFO', 
         "Processed file $filename\n");
    } else {
        S4P::logger('ERROR', 
         "Failure to put $filename\n  to $CFG::simcfg->{destination} on $CFG::simcfg->{hostname}");
        return 1;
    }

    if ( $remove ) {
        my $results = readpipe( "rm $filename" ) ;
        #print "\$results (rm $filename) are-->$results<--\n" ;
    }

    if ( $opt_v ) {
        print "processing is over now $CFG::simcfg->{destination} is this size:\n" ;
        my @results = readpipe ( "ls -ltr $CFG::simcfg->{destination}/*" ) ;
        print "@results\n\n" ;
    }

    return ;

}

#
# creates and pushes a PDR for the directory passed - stubbed out at this point
# this sub needs some work OR can be replaced by s4pa_mk_pdr.pl which I can't find
sub put_pdr {

    my ( $data_ftp, $dir, $datatype, $startEpoch, $dan_seq_no, @files ) = @_ ;
    my $endEpoch = time ;
    my $bdt = convert_time ( $startEpoch ) ;
    my $edt = convert_time ( $endEpoch ) ;
    my $mission = { '042' => 'AM-1',
                    '154' => 'PM-1',
                    '204' => 'AURA' } ;
    my $expiration_time = convert_time ( $startEpoch+1036800) ;

    # build file names for PDR
    # take the first element in the array (the one with .00) and make the pdrFileName
    my $pdrFileName = $files[0] ;
    chomp ( $pdrFileName ) ;
    my $dataSetID = $pdrFileName ;
    $dataSetID =~ s/\.[EP]DS// ;

    my $ScienceFile1 = $pdrFileName ;
    #my $SFSize1 = -s "$dir/$ScienceFile1" ;
    my $SFSize1 = (stat("$dir/$ScienceFile1"))[7] ;
    my $ScienceFile2 = $pdrFileName ;
    if ( $ScienceFile2 =~ /PDS$/ ) {
        $ScienceFile2 =~ s/00\.PDS/01\.PDS/ ;
    } else {
        $ScienceFile2 =~ s/00\.EDS/01\.EDS/ ;
    }
    #my $SFSize2 = -s "$dir/$ScienceFile2" ;
    my $SFSize2 = (stat("$dir/$ScienceFile2"))[7] ;
    #my ( undef, undef, undef, undef, $SFSize2 ) = split " ", readpipe ( "ls -ltr $dir/$ScienceFile2" ) ;
    my $ScienceFile3 = $pdrFileName ;


    my $cmd = "nslookup $CFG::simcfg->{hostname} | tail -2 | grep Address | cut -d\":\" -f2" ;
    my $consumer_system = readpipe ( $cmd ) ;
    chomp ( $consumer_system ) ;
    $consumer_system =~ s/^\s+// ;

    if ( $ScienceFile3 =~ /PDS$/ ) {
        $pdrFileName =~ s/\d\d\.PDS$/\.PDR/ ;
        $pdrFileName =~ s/^P/X/ ;
    } else {
        $pdrFileName =~ s/\d\d\.EDS$/\.EDR/ ;
        $pdrFileName =~ s/^E/Y/ ;
    }
    my $pdrXFRFileName = "$pdrFileName.XFR" ;

    print "PDR filename: $pdrFileName from\n$files[0]\n" if ( $opt_v ) ;

    open ( PDROUT, ">/var/tmp/$pdrFileName" ) || die "Can't open /var/tmp/$pdrFileName\n" ;

    print PDROUT "         é*¿X00000Z000001    1068000000000000    104848\n" ;
    print PDROUT "ORIGINATING_SYSTEM = 198.118.237.151;\n" ;
    print PDROUT "CONSUMER_SYSTEM = $consumer_system;\n" ;
    print PDROUT "DAN_SEQ_NO = $dan_seq_no;\n" ;
    print PDROUT "PRODUCT_NAME = PDS;\n" ;
    print PDROUT "MISSION = $mission->{substr( $pdrFileName,1,3)};\n" ;
    print PDROUT "TOTAL_FILE_COUNT = 0002;\n" ;
    print PDROUT "AGGREGATE_LENGTH = ", $SFSize1+$SFSize2, ";\n" ;
    print PDROUT "EXPIRATION_TIME = $expiration_time;\n" ;
    print PDROUT "OBJECT = FILE_GROUP;\n" ;
    print PDROUT "     DATA_SET_ID = $dataSetID;\n" ;
    print PDROUT "     DATA_TYPE = $datatype;\n" ;
    print PDROUT "     DESCRIPTOR = NOT USED;\n" ;
    print PDROUT "     DATA_VERSION = 00;\n" ;
    print PDROUT "     NODE_NAME = $CFG::simcfg->{hostname};\n" ;
    print PDROUT "     OBJECT = FILE_SPEC;\n" ;
    print PDROUT "          DIRECTORY_ID = $CFG::simcfg->{destination};\n" ;
    print PDROUT "          FILE_ID = $ScienceFile1;\n" ;
    print PDROUT "          FILE_TYPE = METADATA;\n" ;
    print PDROUT "          FILE_SIZE = $SFSize1;\n" ;
    print PDROUT "     END_OBJECT = FILE_SPEC;\n" ;
    print PDROUT "     OBJECT = FILE_SPEC;\n" ;
    print PDROUT "          DIRECTORY_ID = $CFG::simcfg->{destination};\n" ;
    print PDROUT "          FILE_ID = $ScienceFile2;\n" ;
    print PDROUT "          FILE_TYPE = DATA;\n" ;
    print PDROUT "          FILE_SIZE = $SFSize2;\n" ;
    print PDROUT "     END_OBJECT = FILE_SPEC;\n" ;
    print PDROUT "     BEGINNING_DATE/TIME = $bdt;\n" ;
    print PDROUT "     ENDING_DATE/TIME = $edt\n" ;
    print PDROUT "END_OBJECT = FILE_GROUP;" ;

# notes: still need ORIGINATING_SYSTEM, CONSUMER_SYSTEM, DAN_SEQ_NO, 


    close ( PDROUT ) ;

    return 1 if ( put_single_file ( "/var/tmp/$pdrFileName", $data_ftp, 1 ) ) ;

    # only create and send a PDR XFR file for EDOS, there won't be any regular XFR files for
    # non-EDOS
    if ( $CFG::simcfg->{type} eq "EDOS" ) {
        open ( PDRXFROUT, ">/var/tmp/$pdrXFRFileName" ) || die "Can't create /var/tmp/$pdrXFRFileName\n" ;
        print PDRXFROUT "$pdrFileName" ;
        close ( PDRXFROUT ) ;
        return 1 if ( put_single_file ( "/var/tmp/$pdrXFRFileName", $data_ftp, 1 ) ) ;
    }

    return ;

}

#
# creates and pushes a SIPS PDR for the directory passed 
#
sub put_SIPS_pdr {

    my ( $pdr_ftp, $dir, $datatype, $startEpoch, @files ) = @_ ;
    my ( $hour, $doy ) = split ( "/", reverse($dir) ) ;
    $hour = reverse ( $hour ) ;
    $doy = reverse ( $doy ) ;
    
    my $sips_pdrFileName = "/var/tmp/$datatype.$doy.$hour.PDR" ;
    my $expiration_time = convert_time ( $startEpoch+1036800) ;
    my ( undef, undef, undef, undef, $file_size ) = split " ", readpipe ( "ls -ltr $dir/$files[0]" ) ;
    print "in put_SIPS \$sips_pdrFileName = $sips_pdrFileName with exp: $expiration_time and size: $file_size\n" if ( $opt_v) ;

    open ( PDROUT, ">$sips_pdrFileName" ) 
        || die "Can't open $sips_pdrFileName\n" ;

    #begin creating pdr for SIPS
    print PDROUT "ORIGINATING_SYSTEM = $CFG::simcfg->{originating_system};\n";
    print PDROUT "TOTAL_FILE_COUNT =1;\n" ;
    print PDROUT "EXPIRATION_TIME = $expiration_time;\n" ;
    print PDROUT "OBJECT = FILE_GROUP;\n" ;
    print PDROUT "  DATA_TYPE = $datatype;\n" ;
    print PDROUT "  NODE_NAME = $CFG::simcfg->{destination};\n" ;
    print PDROUT "  OBJECT = FILE_SPEC;\n" ;
    print PDROUT "          DIRECTORY_ID = $dir;\n" ;
    print PDROUT "          FILE_ID = $files[0];\n" ;
    print PDROUT "          FILE_TYPE = Science;\n" ;
    print PDROUT "          FILE_SIZE = $file_size;\n" ;
    print PDROUT "  END_OBJECT = FILE_SPEC;\n" ;
    print PDROUT "END_OBJECT = FILE_GROUP;\n" ;

    close ( PDROUT ) ;

    return 1 if ( put_single_file ( "$sips_pdrFileName", $pdr_ftp, 1 ) ) ;

    return ( 0 ) ;

}


sub convert_time {

    my ( $epoch ) = @_ ;

    my ( $sec, $min, $hour, $day, $month, $year ) = (gmtime($epoch))[0..5];
    $month++ ;
    $year += 1900 ;

    my $gmtime = sprintf "$year-%02d-%02dT%02d:%02d:%02dZ", 
                  $month, $day, $hour, $min,$sec ;
    return ( $gmtime ) ; 

}

exit ;
