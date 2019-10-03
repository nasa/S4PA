################################################################################
# $Id: Makefile.PL,v 1.4 2019/04/05 11:19:00 glei Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use 5.00503;
use ExtUtils::MakeMaker;

sub MY::install{
  my($self)=@_;
  package MY; # so that "SUPER" works right
  my $str = $self->SUPER::install();
  $str =~ s/(pure_site_install\s*::)/\1 cfg/;
  return $str;
}

# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    'NAME'          => 'S4PA_services',
    'VERSION'       => '.01',
    'EXE_FILES' => [ 's4pservices.pl',
                   ],
    'PM' => {},	# This should be empty to force MakeMaker to not recognize *.pl as packages
    ($] >= 5.005 ?    ## Add these new keywords supported since 5.005
      ( AUTHOR     => 'GES DAAC S4PA Team <s4pa@daac.gsfc.nasa.gov>') : ()),
);

sub MY::postamble {
return <<BLOCK;
CFGDIR = \$(PREFIX)/cfg
cfg:
	install -d \$(CFGDIR)
	install s4paservices.cfg \$(CFGDIR)/s4paservices.cfg
	chmod 664 \$(CFGDIR)/s4paservices.cfg
	install s4pd \$(CFGDIR)/s4pd
	chmod 755 \$(CFGDIR)/s4pd
BLOCK
}

