#!/usr/local/bin/perl -w

use strict;
use File::Copy qw(cp);
use FileHandle;
use Getopt::Std;
use vars qw($opt_d $opt_h);

# Get directory 
getopt("d:h");
if ($opt_h) {usage()};
my $dir = $opt_d || "";
print "directory used is $dir\n";


print "Edit or restore (E|R)? \n";
my $choice = <>;
if ($choice =~ /^e/i) {

	my $file = "subscription.xml";
	chomp($file);
	$file = $dir.$file;

	print "String to change\n";
	my $old_string = <>;
	chomp($old_string);

	# Check if oldstring is present in file
{	my $found;
	my $fh_check = new FileHandle("<$file") || die "Can't open file $file";;
	while (my $line = $fh_check->getline()) {
		if ($line =~/\W$old_string\W/) {
			$found = 1;
			last;
		}
	}
	if (!$found) {
		print "String $old_string not found in $file\n";
		exit;
	}
	$fh_check->close();
};

	print "New String\n"; 
	my $new_string = <>;
	chomp($new_string);
	
	# Get date for back up
	my ($min,$hr,$day,$mth,$yr) =  (localtime())[1,2,3,4,5];
	my $date = sprintf("%4d%02d%02d%02d%02d",$yr+1900,$mth+1,$day,$hr,$min);
	
	print "$old_string will be changed to $new_string in file $file\nProceed Y/N?";
	my $answer= <>;
	if ($answer =~ /^y/i) 
	{
		my $tmp_file_name = $file.".tmp";
		my $bak_file_name = $file.".bak".".".$date;		

		cp $file, $bak_file_name;

		my  $fh = FileHandle->new("<$file") || die ("Could not open $dir$file");
		my $tmp_file = FileHandle->new(">$tmp_file_name") || die ("Could not open $tmp_file_name to do edit");

		my @changed_lines;
		while (my $line = $fh->getline) {
			my $old_line = $line;
			if ($line =~ s/(\W)$old_string(\W)/$1$new_string$2/g) {
				push @changed_lines,[$old_line,$line];
			}
			$tmp_file->print($line);
		}

		$fh->close;
		$tmp_file->close;
		
		print "Summary of changed lines\n";
		if (!(scalar @changed_lines)) {
			print "No lines match\n";
			exit;
		}

		foreach my $line_check (@changed_lines) {
			print "$line_check->[0] to $line_check->[1]\n"; 
		}
		
		
		print "Proceed with changes\n";
		my $answer = <>;
		if ($answer =~ /^y/i) {
			cp $tmp_file_name, $file || die "Temp file $tmp_file_name not copied over to $file";
		}
		unlink($tmp_file_name);
    }

} elsif ($choice =~ /^r/i)  {

	opendir (D,$dir);
	my @files = readdir(D);
	print join("\n",@files);
	close(D);
	
	print "File to restore:\n";
	my $f = <>;
	chomp($f);
	$f=$dir.$f;
	
	$f =~ m/(^.*)\.bak\.(\d+$)/; 
	my $f1 = $dir.$1;
	cp $f , $f1;

}


sub usage {
	print " -d option to specify directory\n";
	exit;
	}
	