#!/usr/bin/perl

=head1 NAME

s4pa_sudo_config.pl - script to set up environment (display mainly) for running
S4PA under sudo.

=head1 SYNOPSIS

s4pa_sudo_config.pl I<Sudo user>

=head1 DESCRIPTION

s4pa_sudo_config.pl sets up the environment (mainly display) for running 
tkstat.pl under Sudo.

=head1 ARGUMENTS

=over 4

=item B I<Sudo user>

The user name of the account under which S4PA is run.

=head1 AUTHOR

M. Hegde, SSAI

=cut

################################################################################
# $Id: s4pa_sudo_config.pl,v 1.2 2008/05/28 17:40:31 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################
use strict;

# Expect the sudo username for S4PA operations from the command line
unless ( @ARGV ) {
    print STDERR "Use: $0 <sudo user>\n";
    exit( 1 );
}

# Check whether home directory is defined
unless ( defined $ENV{HOME} ) {
    print STDERR "Home directory (HOME environment variable) not defined!\n";
    exit( 2 );
}

# Get the display
my ( undef, $display ) = split( ':', $ENV{DISPLAY} );
$display =~ s/\..*//;
my ( $xauthEntry ) = `xauth list | grep ':$display'`;
unless ( defined $xauthEntry ) {
    print STDERR "Unable to obtain entry for the display from .Xauthroity\n";
    exit( 2 );
}
chomp( $xauthEntry );

# Derive the home directory of the S4PA sudo user
my $homeDir = $ENV{HOME};
$homeDir =~ s/$ENV{USER}$/$ARGV[0]/;

# Change to the home directory of the S4PA sudo user
unless ( chdir( $homeDir ) ) {
    print STDERR "Failed to change to $homeDir\n";
    exit( 2 );
}

# Add the display to S4PA sudo user's .Xauthority
`echo "add $xauthEntry\nexit\n" | sudo -H -u $ARGV[0] xauth`;
if ( $? ) {
    print STDERR "Failed to update .Xauthority ($!)\n";
    exit( 3 );
}
