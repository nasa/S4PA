#!/usr/bin/perl

#
# Name: s4pa_calc_complete.pl
# Author: Andrew Eye
# created: 06/08/2005 
#
# 

# Requirements - 
#
#    <s4paroot>/storage/dataset.cfg must be present on machine running this script.
#
#    Either -d or both (-b and -e) options must be specified when calling this script.
#
#
#
# Interface -
#
#    -c :    Required.  Fully qualified path to config file.  Naming convention <dataset>.cfg recomended but not required.  Recomended location - <s4paroot>/storage/<data_class>/<dataset>
#    
#    -t :    Data Type.  Data set name, must be found in the <s4paroot>/storage/dataset.cfg file.
#    
#    -b :    Begin date. Format mm/dd/yyyy.  Must be specified in conjunction with end date (-e).
#
#    -e :    End date.  Format mm/dd/yyyy.  Must be specified in conjunction with end date (-e).
#
#    -d :    Days.  Number of days (24 hr periods) prior to current system that you would like to check for completeness.
#
#    -k :    Optional.  Known Gaps File.  Defaults to <s4paroot>/storage/<data_class>/<dataset>/<dataset>_knownGaps.cfg if not specified.    Naming convention <dataset>_knownGaps.cfg.
#
#    -o :    Optional.  Output file.  Defaults to <s4paroot>/storage/<data_class>/<dataset>/<dataset>_gaps.cfg if not specified.    Naming convention <dataset>_gaps.cfg.
#
#
# Configuration Files:
#
#
#
#    Known Gaps Config
#
#    -contains only a perl array of strings @knownGaps
#    -each string follows the format "yyyy-mm-dd hh:mm:ss,yyyy-mm-dd hh:mm:ss" with the first date time corresponding to the begining of the gap  in data coverage and the second corresponding to the end of the gap.   
#    -gap strings are seperated by commas per the perl array format.
#
#    Ex.    
#    @knownGaps = (
#    "1997-08-23 20:45:29,1997-08-24 00:05:33",
#    "1997-04-24 16:09:35,1997-04-25 00:24:42",
#    "1997-06-13 01:38:29,1997-06-13 03:23:48"
#    );
#
#
#
#    Data Set Config
#
#    -required vars - $s4paroot, $allowableGap
#    -optional vars - @knownGaps, @expectedGaps
#    
#    -allowableGap gaps are used to specify the amount of time (in seconds) that can fall between two consecutive granules witout being of consequence
#    
#    -expected gaps are used to specify daily expected gaps in coverage, for example on instruments that do not report data at night
#    
#    -@expectedGaps format  - @expectedGaps = ("hh:mm:ss,hh:mm:ss"); 
#    -with the first time corresponding to the begining of the expected gap  in data coverage and the second corresponding to the end of the gap.   
#    
#    Ex.
#    $s4paroot = "/vol1/OPS/s4pa";
#    $allowableGap = 1000;
#    @expectedGaps = ("23:00:00,00:50:00");
#    
#
# Results:
#
#    In addition to the information passed back to standard out, s4paCalcComplete.pl produces a text file contain a perl format array of string specifying all of the gaps that have been detected that are not contained within any of the 3 exception cases (allowable, expected, or known gaps.)  This file will be created as <s4paroot>/storage/<data_class>/<dataset>/<dataset>_gaps.cfg by default unless a -o option is specified.  Subsequent runs of this script for the same data set will append newly identified gaps to the default _gaps file, or the -o file if it is specified and already exists.
#
# Examples:
#
#    s4pa_calc_complete.pl -c /home/aeye/cfg/SL38MOPAR.cfg -t SL38MOPAR -b 10/1/2004 -e 11/30/2004 -k /home/aeye/cfg/SL38MOPAR_knownGaps.cfg -o /home/aeye/results/SL38MOPAR_gaps.cfg
#    s4pa_calc_complete.pl -c /home/aeye/cfg/SL3MCM4.cfg -t SL3MCM4 -b 10/01/1997 -e 11/30/1997 -k /home/aeye/cfg/SL3MCM4_knownGaps.cfg -o /home/aeye/results/SL3MCM4_gaps.cfg
#    s4pa_calc_complete.pl -c PALDAILY.cfg -t PALDAILY -b 12/11/1996 -e 1/11/1998 -k ./PALDAILY_knownGaps.cfg -o ./PALDAILY_gaps.cfg
#    s4pa_calc_complete.pl -c OL1AGAC.cfg -t OL1AGAC -b 12/11/1996 -e 1/11/1997 -k ./OL1AGAC_knownGaps.cfg -o ./OL1AGAC_gaps.cfg
#    s4pa_calc_complete.pl -c OL1AGAC.cfg -t OL1AGAC -d 5 -k ./OL1AGAC_knownGaps.cfg -o ./OL1AGAC_gaps.cfg
#
#

################################################################################
# $Id: s4pa_calc_complete.pl,v 1.2 2010/05/11 11:51:19 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################


use Getopt::Std ;
use strict ;
use Safe ;
use XML::LibXML;
use Time::Local;
         

MAIN: {
    use vars qw( $opt_c $opt_t $opt_b $opt_e $opt_d $opt_k $opt_o) ;
    getopts( 'c:t:b:e:d:k:o:' ) ;
    
    unless ( defined( $opt_c ) ) { print "ERROR: Failure to specify -c <ConfigFileName> on command line.\n" ; exit(2) ; }
    unless ( defined( $opt_t ) ) { print "ERROR: Failure to specify -t <DataType> on command line.\n" ; exit(2) ; }
    unless ( defined( $opt_d ) || ( defined( $opt_b ) && defined( $opt_e ))) 
        { print "ERROR: Must specify either -d <days> or both (-b <BeginDate MM/DD/YY> and -e <EndDate MM/DD/YY>) on command line.\n" ; exit(2) ; }
    if ( defined( $opt_d ) && ( defined( $opt_b ) || defined( $opt_e ))) 
            { print "ERROR: Can not specify both -d <days> and either (-b <BeginDate MM/DD/YY> or -e <EndDate MM/DD/YY>) on command line.\n" ; exit(2) ; }
    
    require "$opt_c";
    our $s4paroot;
    our $allowableGap;
    
    require "$s4paroot/storage/dataset.cfg";
    our %data_class;
    
    my $unexpGapFound = 0;
    my @newUnknownGaps;
    
    my $dataClass = $data_class{$opt_t};
    
    my ($start_mm, $start_dd, $start_yyyy);
    my ($end_mm, $end_dd, $end_yyyy);

    #set the start and stop days for this run
    #if number of days specified
    if(defined( $opt_d )){
        ($start_dd, $start_mm, $start_yyyy, $end_dd, $end_mm, $end_yyyy) = determineDayOffsetStartStop($opt_d);
    #if begin and end days specified
    } else {
        ($start_mm, $start_dd, $start_yyyy) = $opt_b =~ /(\d+)\/(\d+)\/(\d+)/;
        ($end_mm, $end_dd, $end_yyyy) = $opt_e =~ /(\d+)\/(\d+)\/(\d+)/;
    }
    
    print "COMMENT: Determining gaps in range $start_mm/$start_dd/$start_yyyy - $end_mm/$end_dd/$end_yyyy\n";
            

    #determine Julian day of year
    my $startJulDayOfYear = date2dayofyear($start_yyyy,$start_mm,$start_dd);
    my $endJulDayOfYear = date2dayofyear($end_yyyy,$end_mm,$end_dd);
    
    my %startStopHash;
    
    #initialize first hash value with first sec of first day
    #COMMENT: should be verifying that last available granule prior to start date does not cover the begining of this day
    #$startStopHash{"$start_yyyy-$start_mm-$start_dd 00:00:00"}="$start_yyyy-$start_mm-$start_dd 00:00:00";
    
    #determine if data set is daily, monthly, or yearly
    my  $granuleType = determineGranType("$s4paroot/storage/$dataClass/$opt_t/data", $opt_b, $opt_e);
    
    #itterate over each year in the specified time range
    for (my $yr = $start_yyyy; $yr <= $end_yyyy; $yr++) {
        my $dir = "$s4paroot/storage/$dataClass/$opt_t/data/$yr";
        
        if ($granuleType ne "yearly") {
            #if all granules fall in the same year
            if($start_yyyy eq $end_yyyy){
                if ($granuleType eq "daily") {
                    %startStopHash = (%startStopHash,processJulDirs($dir,$startJulDayOfYear,$endJulDayOfYear));
                } else {%startStopHash = (%startStopHash,processMonthDirs($dir,$start_mm,$end_mm));}

            #if first of multiple years
            } elsif ($yr eq $start_yyyy) {
                if ($granuleType eq "daily") {
                    %startStopHash = (%startStopHash,processJulDirs($dir,$startJulDayOfYear,366));
                } else {%startStopHash = processMonthDirs($dir,$start_mm,12);}
            #if last of multiple years
            } elsif ($yr eq $end_yyyy) {
                if ($granuleType eq "daily") {
                    %startStopHash = (%startStopHash,processJulDirs($dir,1,$endJulDayOfYear));
                } else {%startStopHash = processMonthDirs($dir,1,$end_mm);}
            #if middle of multiple years
            } else {
                if ($granuleType eq "daily") {
                    %startStopHash = (%startStopHash,processJulDirs($dir,1,366));
                } else {%startStopHash = processMonthDirs($dir,1,12);}

            }
            
        } else { %startStopHash = (%startStopHash,buildOneDirStartStopHash($dir));}
        
    }
    
    #set last hash value to last sec of last day
    #COMMENT: should be verifying that next available granule after end date does not cover the end of this day
    #$startStopHash{"$end_yyyy-$end_mm-$end_dd 23:59:59"}="$end_yyyy-$end_mm-$end_dd 23:59:59";
    
    #load known gaps 
    my $knownGapsFileName;
    #load specified file if opt_k given
    if (defined( $opt_k )){$knownGapsFileName = $opt_k}
    #otherwise load default file
    else {$knownGapsFileName = "$s4paroot/storage/$dataClass/$opt_t/$opt_t"."_knownGaps.cfg";;}
    
    if (open (KNOWNGAPS,$knownGapsFileName)){
        require $knownGapsFileName;
    } else {print "COMMENT: Could not find known gaps file $knownGapsFileName \n";}
    our @knownGaps;
        
    
    # create a sorted array of stop times from the key values of the start stop hash
    my @sortedStartStop = sort keys %startStopHash;
    my $arryLen = @sortedStartStop;
    my $i = 0;
    
    #find the gaps between all consecutive granules 
    while ( $i < $arryLen) {
        if ($i>0){
            my $gapStart = $startStopHash{$sortedStartStop[$i-1]};
            my $gapStop = $sortedStartStop[$i];
            #determine the number of seconds between the end of one granule and the begin of the next
            my $gap = DateTimeDiff($gapStart,$gapStop);
            #print "COMMENT: granule[$i] startTime=$sortedStartStop[$i] stopTime=$startStopHash{$sortedStartStop[$i]} \n";
            #print "COMMENT: gap = $gapStop - $gapStart = $gap\n";
            
            #ignore gaps smaller then the configured allowable gap
            if($gap>$allowableGap){
                my $knownGap = isKnownGap($gapStart,$gapStop);
                my $expGap = isExpectedGap($gapStart,$gapStop);

                #determine if the gap falls in the expected gap time range for this data set
                if ( $expGap ne 1){
                    print "EXPECTED: $opt_t Gap of $gap seconds ($gapStart : $gapStop) falls in the expected gap range of $expGap.\n";
                #determine if the gap falls in a known gap range for this data set
                } elsif ($knownGap ne 1){
                    print "KNOWN: $opt_t Gap of $gap seconds ($gapStart : $gapStop) falls in the known gap range of $knownGap.\n";
                #if not known, allowable, or expected, this is a new identified gap
                } else {
                    print "MISSING: $opt_t Gap of $gap seconds ($gapStart : $gapStop) missing.\n";
                    $unexpGapFound = 1;
                    push(@newUnknownGaps,"$gapStart,$gapStop");
                }
            }
        }
        
        $i = $i+1;
    }
    
    #determine file name for unknown gaps
    my $gapsFileName;
    if (defined( $opt_o )){$gapsFileName = $opt_o}
    else {$gapsFileName = "$s4paroot/storage/$dataClass/$opt_t/$opt_t"."_gaps.cfg";;}
    
    
    #read existing unknownGaps for this data set
    if (open (OLDUNKNOWNGAPS,$gapsFileName)){
        require $gapsFileName;
        close(OLDUNKNOWNGAPS);
        our @unknownGaps;
        
        
            
        #add any gaps that currently appear in the unknownGaps array stored in the existing file that are not in the knownGaps array
        foreach my $oldUnknownGap(@unknownGaps){
            if ((arrayContains($oldUnknownGap,@knownGaps) eq 1) && (arrayContains($oldUnknownGap,@newUnknownGaps) eq 1))
                {push(@newUnknownGaps,$oldUnknownGap);}
        }
    
    }
    
    
    
    #create output file containing array of all new known gaps
    if (@newUnknownGaps > 0){
        open (UNKNOWNGAPS,">$gapsFileName") or die "Couldn't open file $gapsFileName \n";

        print UNKNOWNGAPS '@unknownGaps = ( '."\n";

        @newUnknownGaps = sort @newUnknownGaps;

        for (my $i = 0; $i < @newUnknownGaps; $i++) {

            print UNKNOWNGAPS '"'.$newUnknownGaps[$i].'"';
            if ($i + 1 ne @newUnknownGaps){ print UNKNOWNGAPS ",";}
            print UNKNOWNGAPS "\n";        
        }

        print UNKNOWNGAPS ");\n";
    
        close(UNKNOWNGAPS);
    # if there are no gaps identified and all of the old gaps are now unknown, unexpected, and non-allowable, remove any existing gaps file 
    } else { 
        if (open (OLDUNKNOWNGAPS,$gapsFileName)){
            unlink $gapsFileName || warn "ERROR: having trouble deleting old gaps file - $gapsFileName \n";
        }
    }


    exit $unexpGapFound;
    
}

sub determineDayOffsetStartStop {

    # given a number of days relative to the current system date, determine the start and stop dates
    my ($days)=@_;
    
    my ($end_dd, $end_mm, $end_yyyy) = (localtime)[3,4,5];
    $end_mm = $end_mm+1;
    $end_yyyy = $end_yyyy+1900;

    my $daysEpochSecs = $opt_d * 60 * 60 * 24;
    my $test = time - $daysEpochSecs;
    my $time = time;
    my ($start_dd, $start_mm, $start_yyyy) = (localtime(time - $daysEpochSecs))[3,4,5];
    $start_mm = $start_mm+1;
    $start_yyyy = $start_yyyy+1900;
    
    return ($start_dd, $start_mm, $start_yyyy, $end_dd, $end_mm, $end_yyyy);


}

sub arrayContains {
    # search an array for the specified value
    my ($testValue, @array) = @_;
    
    foreach my $value(@array){
        if ($value eq $testValue){ return 0;}
    }
    
    return 1;
}

sub determineGranType {

    #determine if a data set is based on a daily, monthly, or yearly dir structure by searching for 3 digit jul day dirs, 
    #    2 digit month dirs, or xml files directly under the yr dir
    my ($rootdir,$beginDate,$endDate) = @_;
    
    my ($start_mm, $start_dd, $start_yyyy) = $beginDate =~ /(\d+)\/(\d+)\/(\d+)/;
    my ($end_mm, $end_dd, $end_yyyy) = $endDate =~ /(\d+)\/(\d+)\/(\d+)/;
    my $startJulDayOfYear = date2dayofyear($start_yyyy,$start_mm,$start_dd);
    my $endJulDayOfYear = date2dayofyear($end_yyyy,$end_mm,$end_dd);
    
    for (my $yr = $start_yyyy; $yr <= $end_yyyy; $yr++) {
        
        my $dir = "$rootdir/$yr";
            
        if($start_yyyy eq $end_yyyy){
            if(foundJulDir($dir,$startJulDayOfYear,$endJulDayOfYear) eq 0){return "daily";}
            if(foundMonthDir($dir,$start_mm,$end_mm) eq 0){return "monthly";}        
        } elsif ($yr eq $start_yyyy) {
            if(foundJulDir($dir,$startJulDayOfYear,366) eq 0){return "daily";}
            if(foundMonthDir($dir,$start_mm,12) eq 0){return "monthly";}

        } elsif ($yr eq $end_yyyy) {
            if(foundJulDir($dir,1,$endJulDayOfYear) eq 0){return "daily";}
            if(foundMonthDir($dir,1,$end_mm) eq 0){return "monthly";}

        } else {
            if(foundJulDir($dir,1,366) eq 0){return "daily";}
            if(foundMonthDir($dir,1,12) eq 0){return "monthly";}

        }
        
    }
    
    return "yearly";
}

sub foundJulDir {
    # determine if 3 digit jul dir exists
    my ($rootdir,$beginJulDay,$endJulDay) =@_;
    
    for (my $julDay = $beginJulDay; $julDay <= $endJulDay; $julDay++) {
        #prepend 0s for 1 & 2 digit days
        if (length($julDay) eq 2){$julDay = "0".$julDay;}
        if (length($julDay) eq 1){$julDay = "00".$julDay;} 
        my $dir .= "$rootdir/$julDay";
        if (opendir(DIR,$dir)){return 0;}
        
    }
    
    return 1;
}


sub foundMonthDir {
    #determine if 2 digit monthly dir exists
    my ($rootdir,$beginMonth,$endMonth) =@_;
    
    for (my $month = $beginMonth; $month <= $endMonth; $month++) {
        #prepend 0 for 1 digit month
        if (length($month) eq 1){$month = "0".$month;} 
        my $dir .= "$rootdir/$month";
        if (opendir(DIR,$dir)){return 0;}
        
    }
    
    return 1;
}

sub processJulDirs {
    # build a hash of start and stop times for one year of a jul day dir structure
    my ($rootdir,$beginJulDay,$endJulDay) = @_;
    my %startStopHash;
    
    for (my $julDay = $beginJulDay; $julDay <= $endJulDay; $julDay++) {
        #prepend 0s for 1 & 2 digit days
        if (length($julDay) eq 2){$julDay = "0".$julDay;}
        if (length($julDay) eq 1){$julDay = "00".$julDay;} 
        my $dir .= "$rootdir/$julDay";
        %startStopHash = (%startStopHash,buildOneDirStartStopHash($dir));
    }
    
    return %startStopHash;
    
}

sub processMonthDirs {
    # build a hash of start and stop times for one year of a monthly dir structure
    my ($rootdir,$beginMonth,$endMonth) = @_;
    my %startStopHash;
    
    for (my $month = $beginMonth; $month <= $endMonth; $month++) {
        #prepend 0 for 1 digit month
        if (length($month) eq 1){$month = "0".$month;}
        my $dir .= "$rootdir/$month";
        %startStopHash = (%startStopHash,buildOneDirStartStopHash($dir));
    }
    
    return %startStopHash;
    
}

sub buildOneDirStartStopHash {
    # given one dir containing xml metadata files, grab the start and stop times from every xml file in this dir
    my ($dir) = @_;
    
    opendir(DIR,$dir) or return;
    my $file;

    my %startStopHash;
    
    #build a hash of filenames and end date/times keyed on start datetime
    #COMMENT: what happens if two file have same exact start date?  - print error warning duplicate data
    while (defined($file = readdir(DIR))) {
        if ($file =~ /xml/){
        my ($startDateTime, $endDateTime) = GetDateTime("$dir/$file");
        $startStopHash{$startDateTime}=$endDateTime;
        #print "COMMENT: $dir/$file $startDateTime, $endDateTime \n";
        }
    }
    
    return %startStopHash;
}




    
sub isExpectedGap {
    # determine if the specified gap falls within the expected gap time ranges for this data set
    my ($startGapDateTime, $endGapDateTime) = @_;
    my $returnVal = 1;
    
    our @expectedGaps;
    
    my $gap = DateTimeDiff($startGapDateTime,$endGapDateTime);
    
    #expected gaps must be less then 24 hours
    if($gap < 8640){
        
        my ($startGapDate, $startGapTime) = split (" ",$startGapDateTime);
        my ($endGapDate, $endGapTime) = split (" ",$endGapDateTime);

        #use 01/01/2001 as bogus date for expected gaps (apply to a given time on any day)
        my $begGapSecs = getEpochSec("2001-01-01 $startGapTime");
        my $endGapSecs = getEpochSec("2001-01-01 $endGapTime" );
        my $expectedGap;

        foreach $expectedGap (@expectedGaps){

            my ($expGapBeg, $expGapEnd) = split (",",$expectedGap);
            my $begExpGapSecs = getEpochSec("2001-01-01 $expGapBeg");
            my $endExpGapSecs = getEpochSec("2001-01-01 $expGapEnd");

            if ($startGapDate ne $endGapDate){
                $endGapSecs = getEpochSec("2001-01-02 $endGapTime" );
                $endExpGapSecs = getEpochSec("2001-01-02 $expGapEnd");
            } 

            if ($begExpGapSecs < $begGapSecs && $endExpGapSecs > $endGapSecs){
                $returnVal = $expectedGap;
                last;
            }

        }
    }
    
    return $returnVal;


}

sub isKnownGap {
    # determine if the specified gap falls within any known gap ranges for this data set
    my ($startGapDateTime, $endGapDateTime) = @_;
    
    our @knownGaps;
    
    my ($startGapDate, $startGapTime) = split (" ",$startGapDateTime);
    my ($endGapDate, $endGapTime) = split (" ",$endGapDateTime);
        
    #use 01/01/2001 as bogus date for expected gaps (apply to a given time on any day)
    my $begGapSecs = getEpochSec($startGapDateTime);
    my $endGapSecs = getEpochSec($endGapDateTime);
    my $knownGap;
    my $returnVal = 1;
    
    foreach $knownGap (@knownGaps){
    
        my ($knownGapBeg, $knownGapEnd) = split (",",$knownGap);
        my $begKnownGapSecs = getEpochSec($knownGapBeg);
        my $endKnownGapSecs = getEpochSec($knownGapEnd);
        
        if ($begKnownGapSecs <= $begGapSecs && $endKnownGapSecs >= $endGapSecs){
            $returnVal = $knownGap;
            last;
        } 
    
    }
    
    return $returnVal;


}


sub date2dayofyear {
    #return the Jul Day of yr for given date
    my ($year,$month,$day) = @_;
    my @monthlengths=calculate_month_lengths($year);
    my $dayofyear=0;

    for (my $monthnum=1; $monthnum<$month;$monthnum++) {
        $dayofyear += $monthlengths[$monthnum-1];
    }

    $dayofyear += $day;
    return $dayofyear;
}
 

 
sub calculate_month_lengths {
    my $year = shift;

    my @monthlengths = (
                31,     # Jan
                28,     # Feb
                31,     # Mar
                30,     # Apr
                31,     # May
                30,     # Jun
                31,     # Jul
                31,     # Aug
                30,     # Sep
                31,     # Oct
                30,     # Nov
                31      # Dec
             );
    if($year%4 == 0) {          # Test for leap year. Will NOT work
                    # In 2100!!!!!
        $monthlengths[1]=29;
    }
    return(@monthlengths);
}
 
sub GetDateTime {
    #return the start date/time and end date/time from a given xml meta data file
    
    my ($file) = @_;

    my $xmlParser = XML::LibXML->new();
    my $dom = $xmlParser->parse_file( $file );
    unless ( defined $dom ) {
        warn( "Failed to get DOM from $file" );
        return undef;
    }
    my $doc = $dom->documentElement();
    unless ( defined $doc ) {
        warn( "Failed to find document element in $file" );
        return undef;
    }
    
    my ( $begDateNode ) = $doc->findnodes( '//RangeBeginningDate' );
    my ( $begTimeNode ) = $doc->findnodes( '//RangeBeginningTime' );
    my ( $endDateNode ) = $doc->findnodes( '//RangeEndingDate' );
    my ( $endTimeNode ) = $doc->findnodes( '//RangeEndingTime' );
     
    if ( defined $begDateNode && defined $begTimeNode && defined $endDateNode && defined $endTimeNode) {
        my $begDateString = $begDateNode->textContent();
            my $begTimeString = $begTimeNode->textContent();
            my $begStr = $begDateString . " " . $begTimeString;
            $begStr =~ s/^\s+|\s+$//g;
            
            my $endDateString = $endDateNode->textContent();
        my $endTimeString = $endTimeNode->textContent();
        my $endStr = $endDateString . " " . $endTimeString;
        $endStr =~ s/^\s+|\s+$//g;
            
            my @returnArray = ($begStr,$endStr);        
            return @returnArray;
    }
        
        return undef;
}


sub DateTimeDiff {
    #return the time in seconds between two date/times
    my ( $dateTime1, $dateTime2 ) = @_;
    
    my $begSecs = getEpochSec($dateTime1);
    my $endSecs = getEpochSec($dateTime2);

    my $seconds = $endSecs - $begSecs;
    
    return $seconds;
}

sub getEpochSec {
    #return the epoch secons for a given date/time
    my ($dateTime) = @_;

    my ( $datepart, $timepart ) = split (" ",$dateTime)  ;
    my ($yr,$mon,$day) = split /-/, $datepart;
    my ($hh,$mi,$sc) = split /:/, $timepart;

    $mon--;
    # use 4-digit year to avoid epoch time error for
    # timestamp earlier than 1970-01-01.
    $yr = (length($yr) == 4) ? $yr : $yr+1900;

    my $epochSec = timegm($sc,$mi,$hh,$day,$mon,$yr);

    return $epochSec;
}
