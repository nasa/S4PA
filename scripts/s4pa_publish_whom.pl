#!/usr/bin/perl

=head1 NAME

s4pa_publish_whom.pl - script for WHOM publication

=head1 SYNOPSIS

s4pa_publish_whom.pl 
[B<-f> I<config_file>]
[B<-w> I<PDR directory/data_class directory>]
[B<-a>]
[B<-d> I<datasets delimited by space>]
[B<-m> I<mode>]
[B<-P> I<publication_url>]
[B<-v>]
[B<-l> I<local directory for publication>]

=head1 ABSTRACT

B<Pseudo code:>

for UPDATE
    Extract all work order (PDR) files from whom pending directory
    For each PDR file
      Read the PDR
        Extract file groups from the PDR
           For each file group
	     Extract data type and metadata file name (type=METADATA) with the file directory 
             Constract metadata file name from metadata file name and file directory
             Store metadata file name in the array for the given data type
           End for
        End for
      End for
    End for
    For each data type
      Call S4PA::Storage::PublishWHOM() with dataset name, datatype name, metadata filename array, 
	  and publication directory
      Exit with error if S4PA::Storage::PublishWHOM() returns false.
      Store csv filename returned by S4PA::Storage::PublishWHOM() in the array
    End for
    publish csv files from publication directory to external FTP URL
    End

for publication of ALL metadata (OR metadata for dataset OR metadata for data type)
    Extract all dataset directory names from /ftp/data/ directory
    For each dataset extract all datatype directory names
      Foreach datatype
        Extract metadata file name (using find .xml) and store them in the array
        Call S4PA::Storage::PublishWHOM() with dataset name, datatype name, metadata filename array,
             and publication directory
        Exit with error if S4PA::Storage::PublishWHOM() returns false.
        Store csv filename returned by S4PA::Storage::PublishWHOM() in the array
      End for
    End for
    publish csv files from publication directory to external FTP URL
    End   
    
=head1 DESCRIPTION

s4pa_publish_whom.pl

=head1 AUTHORS

Irina Gerasimov (gerasimo@daac.gsfc.nasa.gov)

=cut

################################################################################
# $Id: s4pa_publish_whom.pl,v 1.9 2019/05/06 15:48:01 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################


use strict;
use lib '.';
use Getopt::Std;
use File::Basename;
use Safe;
use S4P;
use S4P::PDR;
use S4PA::Storage;
use S4PA;
use Log::Log4perl;

use vars qw($opt_f $opt_w $opt_a $opt_p $opt_l $opt_v $opt_m $opt_d);
use vars qw($cfg_metadata_methods);
my @config_vars = qw($MODE $FILENAME_PREFIX $LOCAL_DIR %PUBLICATION_URL
	     	@CORE_COLUMNS @BBOX_COLUMNS @POLYGON_COLUMNS @EQUATOR_COLUMNS
		@ORBIT_COLUMNS @NODE_COLUMNS @CENTER_COLUMNS @STATION_COLUMNS
		%COLUMN_MAP %URL %ESDT_COLUMNS);

getopts('f:w:ap:l:vd:m:');
usage() if (!$opt_f || !$opt_w);
my $cp = new Safe 'CFG';
$cp->share(@config_vars) if (@config_vars);
my $rc = $cp->rdo($opt_f) or S4P::perish( 1, "Failed to read conf file $opt_f ($@)" );;

my $logger = S4PA::CreateLogger( $CFG::cfg_logger{FILE},
    $CFG::cfg_logger{LEVEL} ) if ( %CFG::cfg_logger );

my $update = $opt_a ? 0 : 1;
my $workdir = $opt_w;
my $localdir = $opt_l ? $opt_l : $CFG::LOCAL_DIR;
my $mode = $opt_m ? $opt_m : $CFG::MODE;
S4P::perish( 1, "Mode (-m or \$MODE in $opt_f) not defined" ) unless defined $mode;
my $url = $opt_p ? $opt_p : $CFG::PUBLICATION_URL{$mode};
undef $url if ($url eq ".");

S4P::perish( 1,
    "Publication destination (-p or \%PUBLICATION_URL in $opt_f for MODE=$mode) not defined" )
    unless defined $url;

print STDERR "update=\"$update\" workdir=\"$workdir\" localdir=\"$localdir\" url=\"$url\"\n"
    if ($opt_v);

my $file_prefix = $CFG::FILENAME_PREFIX;
$file_prefix .= "_UPDATE" if ($update);
my @date = localtime(time);
my $CSVFILENAME = sprintf("%s/%s.DATATYPE.OPS.%04d%02d%02d%02d%02d%02d.csv",
                 $localdir, $file_prefix,
                 $date[5]+1900, $date[4]+1, $date[3], $date[2], $date[1], $date[0]);

my $pdrfiles = [];
my @csvfiles;

if ( $update ) {
  my $metadata = {};
  ($metadata, $pdrfiles) = get_files_from_pdr( $workdir );
  foreach my $datatype (keys %$metadata) {
    my $csvfile = create_csvfile( $datatype, $metadata->{$datatype} );
    if ($csvfile) {
        push @csvfiles, $csvfile;
        $logger->info( "Created $csvfile for whom publishing" )
            if defined $logger;
    }
  }
} else {
  my @dirList = ();
  if ($opt_d) {
    @dirList = split / /, "$opt_d @ARGV";
  } else {
    opendir DATADIR, $workdir;
    @dirList = readdir (DATADIR);
    closedir DATADIR;
  }
  foreach my $dir (@dirList) {
    chomp $dir;
    my $datatype = $dir;
    $datatype =~ s/\.[^\.]+$//;
    next if (!$CFG::ESDT_COLUMNS{$datatype});
    my $metafiles = get_files_from_datadir($workdir.$dir);
    my $csvfile = create_csvfile ($datatype, $metafiles);
    if ($csvfile) {
        push @csvfiles, $csvfile;
        $logger->info( "Created $csvfile for whom publishing" )
            if defined $logger;
    }
  }
}

# Construct work order file name
my $wo = 'WHOMIns';
my $wo_type = 'PUSH';
my @date = localtime(time);
my $wo_file = sprintf("%s.%s.T%04d%02d%02d%02d%02d%02d.wo", $wo_type, $wo,
    $date[5]+1900, $date[4]+1, $date[3], $date[2], $date[1], $date[0]);

# create work order
my $csvCount = scalar( @csvfiles );
my $status = 1;
if ( $csvCount ) {
    $status = create_wo( $wo_file, @csvfiles );
    unless ( $status ) {
        foreach my $pdr ( @$pdrfiles ) {
            unlink $pdr;
            S4P::logger('INFO', "Processed $pdr.");
            $logger->debug( "Deleted $pdr" ) if defined $logger;
        }
        S4P::logger( "INFO", "$wo_file created for $csvCount CSVs" );
        $logger->info( "Created work order $wo_file for $csvCount CSV files" )
            if defined $logger;
    }
}
else {
   S4P::logger( "INFO", "No CSVs created" );
   $logger->info( "No CSV file created" ) if defined $logger;
   $status = 0;
}

exit ( $status );

# Subroutine usage:  print usage and die
sub usage {
  die << "EOF";
usage: $0 <-f config_file> <-w work_order_dir|data_class_dir> [options]
Options are:
        -f                      Configuration file containing WHOM data specs	
	-a 			Option to publish all metadata recursively from <xml_files_dir>
	-p publication_url	URL with host to publish (set to . to publish to local_dir only)
	-l local_dir		Local directory to which csv files should be placed
	-m mode			mode of location to publish the metadata
	-d ESDT name		Option to provide specific ESDT name to publish from work_order_dir
				(applied with -a option only)
	-v			Verbose
EOF
}


sub create_csvfile {
  my ($datatype, $metafiles_ref) = @_;
  my $csvfile = $CSVFILENAME;
  $csvfile  =~ s/DATATYPE/$datatype/;

  if (!$CFG::ESDT_COLUMNS{$datatype}) {
    S4P::logger('ERROR', "No ESDT_COLUMNS value in config for $datatype");
    $logger->error( "Missing ESDT_COLUMNS value in config for $datatype" )
        if defined $logger;
    return 0;
  }

  my %attributes = ();
  foreach my $attr (@{$CFG::ESDT_COLUMNS{$datatype}}) {
    if (exists $CFG::COLUMN_MAP{$attr}) {
      $attributes{$attr} = $CFG::COLUMN_MAP{$attr};
    } else {
      $attributes{$attr} = $attr;
    }
  }
  S4PA::Storage::PublishWHOM(\%attributes, $metafiles_ref, $csvfile, \%CFG::URL);

  if (!-f $csvfile) {  
    S4P::logger('ERROR', "Failed to create $csvfile");
    $logger->error( "Failed creating $csvfile" ) if defined $logger;
    return 0;
  }
  return $csvfile;
}

sub get_files_from_pdr {
  my ($pdrdir) = @_;
  my %metadata = ();

  opendir (PDRDIR, "$pdrdir") || die "Can't open $pdrdir: $!";
  my @files = readdir (PDRDIR);
  closedir (PDRDIR);
  my $numFiles = scalar(@files); 
  $logger->debug( "Found $numFiles files under $pdrdir" )
      if defined $logger;

  my @pdrfiles;
  foreach my $pdrfile (@files) {
    next if ($pdrfile !~ /PDR$/); 
    $pdrfile = "$pdrdir/$pdrfile";
    my $pdr = S4P::PDR::read_pdr($pdrfile);
    if (!$pdr) {
      S4P::logger('ERROR', "Cannot read PDR file $pdrfile: $!");
      $logger->error( "Failed reading $pdrfile" ) if defined $logger;
      next;
    }  
    push @pdrfiles, $pdrfile;
    $logger->info( "Processing $pdrfile" ) if defined $logger;
    foreach my $fg (@{$pdr->file_groups}) {
      my $datatype = $fg->data_type();
      my $meta_file = $fg->met_file();
      if (-e $meta_file) {
        push @{$metadata{$datatype}}, $meta_file;
        $logger->info( "Added $meta_file for whom publishing" )
            if defined $logger;
      } else {
        S4P::logger('ERROR', "Meta file $meta_file does not exist");
        $logger->error( "Failed locating $meta_file" ) if defined $logger;
        next;
      }
    }
  }

  return \%metadata, \@pdrfiles;
}

sub get_files_from_datadir {
  my ($datadir) = @_;
  my @metafiles;

  my @meta_files = `find '$datadir' -name '*.xml'`;
  my $numMetaFile = scalar( @meta_files );
  S4P::logger('INFO', "Found $numMetaFile .xml files in $datadir");
  $logger->debug( "Found $numMetaFile metadata files under $datadir" )
      if defined $logger;

  foreach my $meta_file (@meta_files) {
    chomp $meta_file;
    if ( $meta_file =~ /\/+\d{4}\/+\d{3}\/+\.hidden/
         || $meta_file =~ /\/+\d{4}\/+\d{3}\/+/
	 || $meta_file =~ /\/+\d{4}\/+\d{2}\/+\.hidden/
	 || $meta_file =~ /\/+\d{4}\/+\d{2}\/+/
	 || $meta_file =~ /\/+\d{4}\/+\.hidden/
	 || $meta_file =~ /\/+\d{4}\/+/ ) {
      push @metafiles, $meta_file;
      $logger->info( "Added $meta_file for whom publishing" )
          if defined $logger;
    } else {
      S4P::logger('ERROR', "Cannot extract datatype from filename $meta_file\n");
      $logger->error( "Failed extracting datatype from $meta_file" )
          if defined $logger;
      next;
    }
  }

  return \@metafiles;
}

sub create_wo {
    my ( $wo, @csvfiles ) = @_;

    my $wo_parser = XML::LibXML->new();
    my $wo_dom = $wo_parser->parse_string('<FilePacket/>');
    my $wo_doc = $wo_dom->documentElement();

    my ($filePacketNode) = $wo_doc->findnodes('/FilePacket');
    $filePacketNode->setAttribute('status', "I");

    my ($protocol, $host, $bla, $dest_dir) = $url =~ /^(\w+):\/\/((\w|\.)+)(\/\S+)/;
    $dest_dir = "/ftp" . $dest_dir;
    S4P::logger ( "INFO", "protocol=$protocol, host=$host, dest_dir=\"".$dest_dir."\"" )
         if $opt_v;
    my $destination = "$protocol\:" . $host . $dest_dir;
    $filePacketNode->setAttribute('destination', $destination);

    foreach my $csvfile ( @csvfiles ) {
        my $filegroupNode = XML::LibXML::Element->new('FileGroup');
        my $fileNode = XML::LibXML::Element->new('File');
        $fileNode->setAttribute('localPath', $csvfile);
        $fileNode->setAttribute('status', "I");
        $fileNode->setAttribute('cleanup', "Y");

        $filegroupNode->appendChild($fileNode);
        $wo_doc->appendChild($filegroupNode);
    }

    open (WO, ">$wo") || S4P::perish(2, "Failed to open workorder file $wo: $!");
    print WO $wo_dom->toString(1);
    close WO;

    return(0) ;
}

sub mon2num {
  my ($mon) = @_;
  my %months = ('Jan'=>'01', 'Feb'=>'02', 'Mar'=>'03', 'Apr'=>'04', 
	        'May'=>'05', 'Jun'=>'06', 'Jul'=>'07', 'Aug'=>'08', 
                'Sep'=>'09', 'Oct'=>'10', 'Nov'=>'11', 'Dec'=>'12');
  return $months{$mon};
}
