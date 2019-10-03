# $Id: Makefile.PL,v 1.1 2007/02/28 15:07:01 s4pa Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################
use 5.00503;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    'NAME'		=> 'S4PA::MachineSearch',
    'PREREQ_PM'		=> {}, # e.g., Module::Name => 1.1
    'VERSION'           => '1.0',
    ($] >= 5.005 ?    ## Add these new keywords supported since 5.005
      (AUTHOR     => 'M. Hegde<mhegde@localdomain>') : ()),
);
