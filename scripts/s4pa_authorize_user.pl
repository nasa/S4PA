#!/usr/bin/perl

=head1 NAME

s4pa_authorize_user.pl 

=head1 SYNOPSIS

s4pa_authorize_user.pl [B<-u> <user_id>]  [B<-p> <pwd>] |
                        [B<-g> "group1 ... groupN"] [B<-f> <db_user_file>]
                        add | update | delete

=head1 DESCRIPTION

The user option B<-u> specifies the user to be added to, deleted from,
or updated in the B<-f> DB user file and, if the groups option B<-g>
is specified, the user to be added to or deleted from the B<-a> group
file for each group specified. Entire groups can be deleted from the
group file by selecting B<-g> groups.

=head1 ARGUMENTS

=over 4

=item B<-f> <db_user_file>

Fully qualified DB user filename.

=item B<-g> "group1, ... ,groupN"

The groups on which to perform the user authorization action.

=item B<-p> <pwd>

The password to set for user in the DB user file. Omitting this option causes
the utility to generate a password as required. Only valid with the 'add'
and 'update' commands.

=item B<-u> <user_id>

The user login ID on which to perform the authorization action.

=item B<authorization_action>

The user authorization action to perform. Either add, update, or delete.

=back

=head1 OUTPUT

Altered DB user and/or group files depending on the user authorization
actions that were performed.

=head1 EXAMPLES

Add a user to the DB user file and assign a password:
    $ s4pa_authorize_user.pl -u s4pa
                             -f /ftp/data/restricted/.usersdb
                             add

Add a user to the DB user file with a specified password:
    $ s4pa_authorize_user.pl -u s4pa -p passwd
                             -f /ftp/data/restricted/.usersdb
                             add
                             
Add a user to the DB user file and to the selected groups:
    $ s4pa_authorize_user.pl -u s4pa -g "GROUP1,GROUP2"
                             -f /ftp/data/restricted/.usersdb
                             add

Update a user's password in the DB user file:
    $ s4pa_authorize_user.pl -u s4pa -p passwd
                             -f /ftp/data/restricted/.usersdb
                             update

Delete a user from the DB user file:
    $ s4pa_authorize_user.pl -u s4pa
                             -f /ftp/data/restricted/.usersdb
                             delete

Delete a user from the group:
    $ s4pa_authorize_user.pl -u s4pa -g GROUP1
                             -f /ftp/data/restricted/.usersdb
                             delete


=head1 AUTHOR

Robert Kummerer, SSAI
Yangling Huang, L-3
M. Hegde, SSAI

=cut

################################################################################
# $Id: s4pa_authorize_user.pl,v 1.2 2006/07/05 13:54:01 hegde Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use strict;
use Getopt::Std;
use DB_File;
use Fcntl;
use S4P;

use vars qw($opt_f $opt_u $opt_p $opt_g);
getopts('f:u:p:g:');

usage() unless defined $opt_u;

# Make sure that arguments don't contain invalid characters.
die "User, '$opt_u', can not contain ':'" if ( $opt_u =~ /:/ );
die "Groups can not contain ':'" if ( $opt_g =~ /:/ );
die "Password can not contain ':'" if ( $opt_p =~ /:/ );

my $action = shift( @ARGV ) 
    or die "ERROR: missing user authorization action (add|update|delete)";
my $dbm_tool = '/usr/LOCAL/etc/httpd/bin/dbmmanage';

die "Action, '$action', not supported"
    unless ( $action eq 'add' || $action eq 'update' || $action eq 'delete' );
AddUser( $action, $opt_f, $opt_u, $opt_p, $opt_g ) 
    if ( $action eq 'add' || $action eq 'update' );
DelUser( $opt_f, $opt_u, $opt_g ) if ( $action eq 'delete' );

################################################################################

=head1 DelUser

Description:
    

Input:
    $db_file    - DB filename
    $user       - Username
    $group      - A string with group names delimited by comma.

Output:
    An updated user DB file.
    
Algorithm:

    Function DelUser
        If group not specified
            Delete user's entry from the DB file.
        Else
            Get groups the user belongs to.
            Delete specified groups from the list of current groups.
            Update the user's record with the new list of groups.
        End
    End

=head1 AUTHOR

M. Hegde

=cut
sub DelUser
{
    my ( $db_file, $user, $group ) = @_;
    
    # Check whether a user exists already; if the user doesn't exist, stop.
    my ( $cur_user, @cur_group ) = CheckUser( $db_file, $user );
    die "User, '$user', doesn't exist!" unless defined $cur_user;
       
    if ( not defined $group ) {
        # If groups are not specified, delete user (+ all groups).
        my $options = "$db_file delete $user";
        `$dbm_tool $options`;
        die "Failed to delete '$user' ($!)" if ( $? );
        print STDERR "Deleted user '$user'\n";
    } else {
        # If at least a group is specfied:
        
        # If the user doesn't belong to any group, stop.
        die "User doesn't belong to any group" unless @cur_group;
        
        # Remove specified groups from the list of groups.
        my %dummy =  @cur_group ? map { $_ => 1 } @cur_group : ();
        my @group_list = split( /,/, $group );
        map { delete $dummy{$_} } @group_list;

        @group_list = sort( keys %dummy );
        my $options = "$db_file update $user . ";
        $options .= join( ",", @group_list );
        `$dbm_tool $options`;
        die "Failed to delete '$user' from groups $group ($!)" if ( $? );
        print STDERR "Delete user, '$user', from groups, $group\n";
    }
}
################################################################################

=head1 AddUser

Description:
    

Input:
    $action     - action (add, delete, or update)    
    $db_file    - DB filename
    $user       - Username
    $passwd     - password
    $group      - A string containing group names.

Output:

    
Algorithm:

    Function AddUser
    End

=head1 AUTHOR

M. Hegde

=cut
sub AddUser
{
    my ( $action, $db_file, $user, $passwd, $group ) = @_;

    # Get groups
    my @group_list =  ( defined $group ) ? split( /,/, $group ) : ();
    
    # Need to generate a password only when adding a user.
    $passwd = GenPassword( ) if ( ($action eq 'add') && (not defined $passwd) );
    my $cpasswd = crypt( $passwd, int( rand( 1E6 ) ) );
    my ( $cur_user, @cur_group ) = CheckUser( $db_file, $user );
    
    # Retain only unique groups (combine specified groups with existing ones)
    my %dummy =  @group_list ? (map{ $_ => 1 } @group_list): ();
    %dummy = map { $_ => 1 } @cur_group if ( @cur_group );
    @group_list = sort keys ( %dummy );
    undef %dummy;
    # Form a string consisting of groups separated by comma.
    my $groups = ( @group_list ) ? join( ",", @group_list ) : '';

    # Perform actions.
    my $options = "$db_file $action $user";
    if ( $action eq 'add' ) {
        die "'$user' already exists!" 
            if ( defined $cur_user && ( $action = 'add' ) );
        $options .= ( length( $groups ) > 0 )  
                      ? " $cpasswd '$groups'" : " $cpasswd";

    } elsif ( $action eq 'update' ) {
        $options .= ( defined $passwd ) ? " $cpasswd" : " .";
        $options .= ( length( $groups ) > 0 ) ? " $groups" : " .";
    } else {
        die "Action, '$action', not supported";
    }
    `$dbm_tool $options`;
    if ( $? ) {
        my $message = "Failed to $action '$user'";
        $message .= " to group" . (@group_list ? 's' : '') 
                    . join( ", ", @group_list ) . ".";
        die $message;
    } else {
        my $message = "User, '$user', ";
        $message .= "with password, '$passwd', " if ( defined $passwd );
        $message .= "added";
        $message .= " to group" . (@group_list ? 's' : '' )
                    . " " . join( ", ", @group_list ) . ".";
        print STDERR $message, "\n";
    }
}
################################################################################

=head1 CheckUser

Description:
    Checks whether a user already exists in the DB file.

Input:
    $db_file    - DB filename
    $user       - Username

Output:
    If the user exists, a list containing username and groups the user belongs.
    Otherwise, returns undefined.
    
Algorithm:

    Function CheckUser
        Checks the DB file with the tool provided by Apache HTTP server.
        If the user information is found, split the output from above step
            based on the delimiter (:).
        If the user information is not found return undefined.
    End

=head1 AUTHOR

M. Hegde

=cut
sub CheckUser
{
    my ( $db_file, $user ) = @_;
    
    # Query for usr information.
    my $options = "$db_file view $user";
    return undef unless ( -f $db_file );    
    my ( $info ) = `$dbm_tool $options`;
    die "Failed to check user status using $db_file ($!)" if ( $? );
    chomp ( $info );
    # If the user information is not found, return undef.
    return ( undef ) unless ( $info =~ /:\S+/ );
    my @list = split( /:/, $info );
    # If a username, password and group(s) exist, return username and group(s).
    return ( $list[0], split( /,/, $list[$#list] ) ) if ( @list >= 3 );
    # Othrwise, just return the username.
    return ( $list[0] );
}
################################################################################

=head1 GenPassword

Description:
    Generates a random password.

Input:
    $length - length of password; default is 6.   

Output:
    Returns a password of specified length.
    
Algorithm:

    Function GenPassword
        Set the first character of the password to a random alphabet.
        Set rest of the characters of the password to a character selected
            from a pool of valid characters randomly.
        Return the password.

    End

=head1 AUTHOR

M. Hegde

=cut
sub GenPassword
{
    my ( $length ) = @_;
    
    my( $password );
    $length = 6 unless ( defined $length && ( $length > 0 ) );

    my @chars = split(" ",
        "a b c d e f g h i j k l m n o p q r s t u v w x y z
         A B C D E F G H I J K L M N O P Q R S T U V W X Y Z
         0 1 2 3 4 5 6 7 8 9" );
    srand;
    
    my $max_seed = scalar( @chars ) - .1;
    # Make sure the first character is alphabetical
    $password = $chars[int(rand 51.9)];
    for (my $i=1; $i < $length ;$i++) {
        my $index = int(rand $max_seed);
        $password .= $chars[$index];
    }
    return $password;
}
################################################################################
sub usage {
    die "Usage: $0 [-u <user_id>] [-f <db_user_file>] [-p <password>] |
                                 [-g \"group1,group2 ... groupN\"] 
                                 <authorization_action>

           -f <db_user_file>          Fully qualified DB user filename.
           -g \"group1,...,groupN\"   The groups on which to perform the
                                      user authorization action.
           -p <password>              The password to set for user in the DB
                                      user file. Omitting this option causes
                                      the utility to prompt for a password
                                      as required. Only valid with the 'add'
                                      and 'update' commands.
           -u <user_id>               The user login ID on which to perform
                                      the authorization action.

           <authorization_action>     The user authorization action to perform.
                                      Either add, update, or delete.\n";
    exit 1;
}
