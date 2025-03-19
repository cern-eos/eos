#######################################################################
## Makefile.am for eos apmond
##
##
## Author: Andreas Joachim Peters <andreas.joachim.peters@cern.ch>
## Initial version: 13.7.2010
##
## Version info: $Id: Makefile.am,v 1.1 2006/07/24 07:38:57 apeters Exp $
## Checked in by $Author: apeters $
#######################################################################

EXTRA_DIST = opt/eos/apmon/eosapmond \
		opt/eos/apmon/functions \
		etc/logrotate.d/apmon-logs \
		perl/ApMon/ApMon/BgMonitor.pm \
		perl/ApMon/ApMon/Common.pm \
		perl/ApMon/ApMon/ConfigLoader.pm \
		perl/ApMon/ApMon/ProcInfo.pm \
		perl/ApMon/ApMon/XDRUtils.pm \
		perl/ApMon/ApMon.pm \
		perl/ApMon/sendToML.sh \
		perl/ApMon/servMon.sh \
		eos_apmonpl \
		configure.ac \
		Makefile.am \
		bootstrap.sh \
		eos-apmon.spec 


install-exec-local :
		mkdir -p $(DESTDIR)/$(prefix)/perl/ApMon/ApMon/
		mkdir -p $(DESTDIR)/etc/logrotate.d/ $(DESTDIR)/opt/eos/apmon $(DESTDIR)/etc/sysconfig/ $(DESTDIR)/var/log/eos  $(DESTDIR)/`perl -V:installsitearch | cut -d "'" -f 2`/ApMon/ApMon;
	    mkdir -p $(DESTDIR)/usr/sbin/ $(DESTDIR)/usr/lib/systemd/system/
		for name in ` find etc -type f | grep -v svn`; do $(INSTALL) -m 755 $(srcdir)/$$name $(DESTDIR)/$$name; done
		cd perl ; for name in ` find . -type f | grep -v svn`; do $(INSTALL) -m 755 $(srcdir)/$$name $(DESTDIR)/`perl -V:installsitearch | cut -d "'" -f 2`/$$name; done
		$(INSTALL) -m 755 $(srcdir)/eos_apmonpl $(DESTDIR)/usr/sbin/
		$(INSTALL) -m 755 $(srcdir)/opt/eos/apmon/eosapmond $(DESTDIR)/opt/eos/apmon/
		$(INSTALL) -m 755 $(srcdir)/opt/eos/apmon/functions $(DESTDIR)/opt/eos/apmon/
		$(INSTALL) -m 644 $(srcdir)/usr/lib/systemd/system/eosapmond.service $(DESTDIR)/usr/lib/systemd/system/eosapmond.service

uninstall-local :
		for name in ` find etc -type f | grep -v svn`; do rm $(DESTDIR)/$$name; done
		cd perl ; for name in ` find . -type f | grep -v svn`; do rm $(DESTDIR)/`perl -V:installsitearch | cut -d "'" -f 2`/$$name; done
		rm $(DESTDIR)/eos_apmonpl
		rm -rf $(DESTDIR)/opt/eos/apmon

rpm:	dist
	cp $(DIST_ARCHIVES) /usr/src/redhat/SOURCES/
	rpmbuild -bb eos-apmon.spec

rpmnodist:      
	cp $(DIST_ARCHIVES) /usr/src/redhat/SOURCES/
	rpmbuild -bb eos-apmon.spec

sbin_PROGRAMS = eos_apmond

eos_apmond_SOURCES = eosapmond.c



