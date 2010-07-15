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

EXTRA_DIST =	etc/init.d/eosapmond \
		perl/ApMon/ApMon/BgMonitor.pm \
		perl/ApMon/ApMon/Common.pm \
		perl/ApMon/ApMon/ConfigLoader.pm \
		perl/ApMon/ApMon/ProcInfo.pm \
		perl/ApMon/ApMon/XDRUtils.pm \
		perl/ApMon/ApMon.pm \
		perl/ApMon/sendToML.sh \
		perl/ApMon/servMon.sh \
		configure.ac \
		Makefile.am \
		eos-apmon.spec \
	        eos-ns-monitor.pl


install-exec-local :
		mkdir -p $(DESTDIR)/$(prefix)/perl/ApMon/ApMon/
		mkdir -p $(DESTDIR)/etc/logrotate.d/ $(DESTDIR)/etc/init.d $(DESTDIR)/etc/sysconfig/ $(DESTDIR)/var/log/xroot $(DESTDIR)/var/spool/xrootd/adm 
		for name in ` find etc -type f | grep -v svn`; do $(INSTALL) -m 755 $(srcdir)/$$name $(DESTDIR)/$$name; done
		for name in ` find perl -type f | grep -v svn`; do $(INSTALL) -m 755 $(srcdir)/$$name $(DESTDIR)/$(prefix)/$$name; done
	        $(INSTALL) -m 755 $(srcdir)/eos-ns-monitor.pl $(DESTDIR)/$(prefix)/perl/eos-ns-monitor.pl

uninstall-local :
		for name in ` find etc -type f | grep -v svn`; do rm $(DESTDIR)/$$name; done
		for name in ` find perl -type f | grep -v svn`; do rm $(DESTDIR)/$(prefix)/$$name; done
	        rm $(DESTDIR)/$(prefix)/perl/eos-ns-monitor.pl

bin_PROGRAMS = eos_apmond eos_ns_apmond


eos_apmond_SOURCES = eosapmond.c 

eos_ns_apmond_SOURCES= eosnsapmond.c

rpm:    dist
	cp $(DIST_ARCHIVES) /usr/src/redhat/SOURCES/
	rpmbuild -bb eos-apmon.spec







