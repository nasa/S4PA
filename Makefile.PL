# $Id: Makefile.PL,v 1.11 2019/04/05 12:02:08 glei Exp $
# -@@@ S4PA, Version $Name:  $

use 5.008;
use ExtUtils::MakeMaker;

push(@ExtUtils::MakeMaker::MM_Unix::Overridable, "install");

sub MY::install{
  my($self)=@_;
  package MY; # so that "SUPER" works right
  my $str = $self->SUPER::install();
  $str =~ s/(pure_site_install\s*::)/\1 cgi/;
  return $str;
}

# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    'NAME'		=> 'S4PA',
    'VERSION_FROM'	=> 'S4PA.pm', # finds $VERSION
    'PREREQ_PM'		=> {}, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?    ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM => 'S4PA.pm', # retrieve abstract from module
       AUTHOR     => 'GES DAAC S4PA team <s4pa@daac.gsfc.nasa.gov>') : ()),
);



sub MY::postamble {
return <<BLOCK;
CGIDIR = \$(PREFIX)/cgi-bin/s4pa
CFGDIR = \$(PREFIX)/cfg
cgi:
	install -d \$(CGIDIR)
	install scripts/s4pa_m2m_cgi.pl \$(CGIDIR)/s4pa_m2m_cgi.pl
	chmod 775 \$(CGIDIR)/s4pa_m2m_cgi.pl
	install -d \$(CFGDIR)
	install service/s4paservices.cfg \$(CFGDIR)/s4paservices.cfg
	chmod 664 \$(CFGDIR)/s4paservices.cfg
	install service/s4pd \$(CFGDIR)/s4pd
	chmod 755 \$(CFGDIR)/s4pd
BLOCK
}
