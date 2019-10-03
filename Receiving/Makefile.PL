# $Id: Makefile.PL,v 1.1.1.1 2006/03/08 12:31:19 hegde Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################
use 5.00503;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    'NAME'		=> 'S4PA::Receiving',
    'VERSION_FROM'	=> 'Receiving.pm', # finds $VERSION
    'PREREQ_PM'		=> {}, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?    ## Add these new keywords supported since 5.005
      (AUTHOR     => 'Chris Lynnes <lynnes@localdomain>') : ()),
);
