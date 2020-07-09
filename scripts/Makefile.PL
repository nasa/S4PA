################################################################################
# $Id: Makefile.PL,v 1.59 2020/05/14 11:52:28 s4pa Exp $
# -@@@ S4PA, Version $Name:  $
################################################################################

use 5.00503;
use ExtUtils::MakeMaker;

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
    'NAME'          => 'S4PA_Scripts',
    'VERSION'       => '.01',
    'EXE_FILES' => [ 's4pa_recv_data.pl',
                     's4pa_subscribe.pl',
                     's4pa_store_data.pl',
                     's4pa_remote_polling_pdr.pl',
                     's4pa_giovanni.pl',
                     's4pa_giovanni_update.pl',
                     's4pa_publish_whom.pl',
                     's4pa_publish_echo.pl',
                     's4pa_publish_mirador.pl',
                     's4pa_delete_data.pl',
                     's4pa_check_integrity.pl',
                     's4pa_prune_recovery.pl',
                     's4pa_whom_dfa_from_pdr.pl',
                     's4pa_unrestrict_data.pl',
                     's4pa_dbdump.pl',
                     's4pa_write_physical_media.pl',
                     's4pa_remote_polling_without_pdr.pl',
                     's4pa_authorize_user.pl',
                     's4pa_calc_complete.pl',
                     's4pa_deploy.pl',
                     's4pa_file_pusher.pl',
                     's4pa_extract_ODL.pl',
                     's4pa_create_DN.pl',
                     's4pa_convert_DIF.pl',
                     's4pa_odl2xml.pl',
                     's4pa_create_mirador_dpp.pl',
                     's4pa_edos_poller.pl',
                     's4pa_provider_sim.pl',
                     's4pa_update_subscription.pl',
                     's4pa_dn2pdr.pl',
                     's4pa_sub_check.pl',
                     's4pa_DNacq.pl',
                     's4pa_controller.pl',
                     's4pa_reconcile_disk.pl',
                     's4pa_convert_DIF.pl',
                     's4pa_fetch_revised_DIFs.pl',
                     's4pa_active_fs_monitor.pl',
                     's4pa_ECHO_recon.pl',
                     's4pa_recon.pl',
                     's4pa_publish_dotchart.pl',
                     's4pa_get_echo_access.pl',
                     's4pa_republish.pl',
                     's4pa_m2m_search.pl',
                     's4pa_dataset_relocate.pl',
                     's4pa_mkpdrs.pl',
                     's4pa_relocate_cleanup.pl',
                     's4pa_relocate_metadata_method.pl', 
                     's4pa_pdr_cleanup.pl',
                     's4pa_edit_config.pl',
                     's4pa_check_instance_integrity.pl',
                     's4pa_transient_archive.pl',
                     's4pa_db_export.pl',
                     's4pa_filter_ODL.pl',
                     's4pa_modify_access.pl',
                     's4pa_sudo_config.pl',
                     's4pa_recovery.pl',
                     's4pa_dbmerge.pl',
                     's4pa_db_import.pl',
                     's4pa_backup_file.pl',
                     's4pa_publish_giovanni.pl',
                     's4pa_merge_pan.pl',
                     's4pa_recon_associate.pl',
                     's4pa_browse_ingest.pl',
                     's4pa_echo_dataset.pl',
                     's4pa_remote_polling_data.pl',
                     's4pa_create_h4map.pl',
                     's4pa_add_h4map.pl',
                     's4pa_publish_user.pl',
                     's4pa_rest_worker.pl',
                     's4pa_publish_cmr.pl',
                     's4pa_get_cmr_access.pl',
                     's4pa_cmr_dataset.pl',
                     's4pa_recycle_volumes.pl',
                     's4pa_zombie_monitor.pl',
                     's4pa_launchpad_token.pl',
                   ],
    'PM' => {},	# This should be empty to force MakeMaker to not recognize *.pl as packages
    ($] >= 5.005 ?    ## Add these new keywords supported since 5.005
      ( AUTHOR     => 'GES DAAC S4PA Team <s4pa@daac.gsfc.nasa.gov>') : ()),
);

sub MY::postamble {
return <<BLOCK;
CGIDIR = \$(PREFIX)/cgi-bin/s4pa
cgi:
	install -d \$(CGIDIR)
	install s4pa_m2m_cgi.pl \$(CGIDIR)/s4pa_m2m_cgi.pl
	chmod 775 \$(CGIDIR)/s4pa_m2m_cgi.pl
BLOCK
}
