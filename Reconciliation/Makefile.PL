# $Id: Makefile.PL,v 1.1 2007/11/30 16:23:29 eseiler Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################
use 5.00503;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    'NAME'              => 'S4PA::Reconciliation',
    'VERSION_FROM'      => 'Reconciliation.pm', # finds $VERSION
    'PREREQ_PM'         => {}, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?    ## Add these new keywords supported since 5.005
      ( AUTHOR     => 'GES DAAC S4PA Team <s4pa@daac.gsfc.nasa.gov>') : ()),
);
