#!/usr/bin/perl

=head1 NAME

s4pa_replication_recon.pl - a script to dump contents of granule.db files in to a text 
file and ship it to a remote host via PostOffice.

=head1 SYNOPSIS

s4pa_replication_recon.pl -f <Configuration filename>

=head1 ABSTRACT

B<Pseudo code:>
    Obtain dataset->dataclass mappings
    For each dataset/version
        Dump the granule.db file and append the contents to a text file.
    Endfor
    Create a PostOffice work order to ship out the text file.
    End

=head1 DESCRIPTION

s4pa_replication_recon.pl is script for dumping granule DBM file contents in
to a text file.

=head1 SEE ALSO

L<s4pa_dbdump.pl>

=cut

use strict;
use Getopt::Std;
use File::Temp;
use File::Basename;
use Safe;
use S4P;
use Data::Dumper;

use vars qw($opt_f $opt_r $opt_h $opt_l $opt_d $opt_H $opt_x $opt_p);
getopts('f:x:r:l:d:H:h:p');

usage() if ( ($opt_h) || ( ( !$opt_f ) && ( !$opt_r && !$opt_l && !$opt_H ) ) );

# Read the script configuration file
my $cpt = new Safe 'CFG';
$cpt->rdo($opt_f)
  or S4P::perish( 1, "Cannot read config file $opt_f in safe mode: ($!)" );

# Read StoreData configuration to get data versions
my $cfgFile = "$CFG::cfg_root/storage/dataset.cfg";
$cpt = new Safe 'DATASET';
$cpt->rdo("$CFG::cfg_root/storage/dataset.cfg")
  or S4P::perish( 1, "Cannot read config file $cfgFile in safe mode: ($!)" );

my $remote_host  = $opt_H || $CFG::cfg_remote_host;
my $local_root   = $opt_l || $CFG::cfg_root;
my $remote_root  = $opt_r || $CFG::cfg_remote_root;
my @cfg_datasets = $opt_x || @CFG::cfg_datasets;
my $descriptor   = $opt_d || $CFG::cfg_descriptor;

my ( $storageDir, $groups ) = read_descriptor($descriptor);

print "storage dir $storageDir\n";

my $ftp_root = $opt_p || $CFG::cfg_ftp_root || $storageDir;

# Load hash of datasets to include

# If specified only consider user configured datasets
if ( scalar @cfg_datasets )

{
    my %included_datasets;
    foreach my $kkey (@cfg_datasets) {
        $included_datasets{$kkey} = 1;
    }

    # Only consider user configured datasets
    foreach my $x ( keys %DATASET::data_class ) {
        if ( !$included_datasets{$x} ) {
            delete $DATASET::data_class{$x};
        }
    }
}

# Determine available version from local store_data_config file
my $versions;

# Loop over all datasets
foreach my $dataset ( keys %DATASET::data_class ) {
    my $storeDataConfig =
        "$local_root/storage/"
      . "$DATASET::data_class{$dataset}/"
      . "store_$DATASET::data_class{$dataset}/s4pa_store_data.cfg";

    $cpt = new Safe 'STORE';
    $cpt->rdo($storeDataConfig)
      or S4P::perish( 1, "Cannot read config file $storeDataConfig in safe mode: ($!)" );

    # Loop over all versions of the dataset
    $versions->{$dataset} = [ @{ $STORE::cfg_data_version{$dataset} } ],;

    # Read local files in instance cfg_root;
    my $local_report = dump_granules( $local_root, $dataset, "", "", $versions );

    # Get remote files on $remote_host
    my $remote_report = dump_granules( $remote_root, $dataset, "$remote_host", "ssh $remote_host", $versions );

    open( LOCAL,  ">local" );
    open( REMOTE, ">remote" );

    # Print what was found to terminal and file
    print REMOTE "$remote_report";

    # Print what was found to terminal and file
    print LOCAL $local_report;

    close(REMOTE);
    close(LOCAL);

    # Read the files into hashs for computations
    # Hash key is line of file but with cksum and fs fields removed
    my $local_hash  = read_report_files( "local",  $ftp_root );
    my $remote_hash = read_report_files( "remote", $ftp_root );

    # Count the files on each system
    my $local_count  = keys %{$local_hash};
    my $remote_count = keys %{$remote_hash};

    # Find the differences
    remove_common_files( $local_hash, $remote_hash );
    print "Dataset: $dataset\n";
    print "There are $local_count local files and $remote_count remote files\n";

    print "Files exclusive on local system for dataset $dataset\n";
    foreach my $key ( keys %{$local_hash} ) {
        my $date_time = extract_date_time( $local_hash->{$key} );
        print "$ftp_root/$groups->{$dataset}/$dataset/$date_time/$key\n";
    }
    print "\n";

    print "Files exclusively on remote system for dataset $dataset\n";
    foreach my $key ( keys %{$remote_hash} ) {
        my $date_time = extract_date_time( $remote_hash->{$key} );
        print "$ftp_root/$groups->{$dataset}/$dataset/$date_time/$key\n";
    }
    print "\n";
}

exit;

sub dump_granules {

    my ( $cfg_root, $dataset, $remote_host, $ssh_command, $versions ) = @_;
    my $return_str;

    # Loop over all versions of the dataset
    foreach my $version ( @{ $versions->{$dataset} } ) {

        #print "Version $version\n";
        my $granuleDbFile =
            "$cfg_root/storage/"
          . "$DATASET::data_class{$dataset}/$dataset"
          . ( $version ne '' ? ".$version/" : '/' )
          . 'granule.db';

        # Print dataset, version, data class and current time
        "DataClass=$DATASET::data_class{$dataset}, Time=" . S4P::timestamp() . "\n";

        # Dump the granule DB
        my $str = `$ssh_command /tools/gdaac/OPS/bin/s4pa_dbdump.pl -f $granuleDbFile`;
        if ($?) {
            S4P::logger( "ERROR", "Failed to dump $granuleDbFile ($!)" );
            exit(1);
        }
        else {
            $return_str .= ( $str . "\n" );
        }
    }
    return $return_str;
}

sub read_report_files {
    my ( $report_file, $ftp_root ) = @_;
    my $hash;

    open( REPORT, "<$report_file" ) || S4P::perish( 1, "Cannot read report file $report_file ($!)" );

    while ( my $line = <REPORT> ) {
        next if ( length($line) < 2 );    # Don't process empty lines
        my @fields = split( /\|/, $line );
        splice( @fields, 3, 2 );
        splice( @fields, 1, 1 );
        my @file = split( /:/, $fields[0] );
        my $file_name = $file[1];

        # Create hash using filename as hash key
        $hash->{$file_name} = $line;

    }

    return $hash;
}

sub remove_common_files {
    my ( $hash_one, $hash_two ) = @_;

    # Remove duplicated keys

    foreach my $key ( keys %{$hash_two} ) {
        if ( exists $hash_one->{$key} ) {
            delete $hash_one->{$key};
            delete $hash_two->{$key};
        }
    }

}

sub read_descriptor {
    my ($descriptor_file) = @_;
    use XML::Simple;

    # Read in descriptor file
    my $x    = new XML::Simple();
    my $dref = $x->XMLin(
        $descriptor_file,
        keeproot   => 1,
        forcearray => 1
    );

    my $storage_dir = $dref->{s4pa}[0]->{storageDir}[0];
    my $group_name  = {};

    # Form hash with datset as key and contianing data group as data
    foreach my $dataClass_ref ( @{ $dref->{s4pa}[0]->{provider}[0]->{dataClass} } ) {
        my $group = $dataClass_ref->{GROUP};
        foreach my $dataset_ref ( @{ $dataClass_ref->{dataset} } ) {
            $group_name->{ $dataset_ref->{NAME} } = $group;
        }
    }

    return $storage_dir, $group_name;

}

sub extract_date_time {
    my ($line) = @_;
    my $date_time = ( split( /:/, ( split( /\|/, $line ) )[2] ) )[1];
    return $date_time;

}

sub usage {

    my $message = <<USAGE;
           
    options
    -f configuration file; Elements are cfg_root, cfg_remote_root and cfg_remote_host
    
    -r cfg_remote_root; Remote s4pa root; Overrides element in config file if present.
    -l cfg_root; Local s4pa rootl;  Overrides element in config file if present.
    -H cfg_remote_host: Name of remote host; Overrides element in config file if present.
    -p ftp_root: For for root of storage tree.  ; Overrides storageDir from descriptor and element in config file if present.
    -d descriptor: Descriptor file for storage dir and group info.  Overrides element in config file if present.
            
   
    
USAGE

    print "$message\n";
    exit 1;
}
