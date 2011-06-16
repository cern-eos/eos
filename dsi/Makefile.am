## Process this file with automake to produce Makefile.in

INCLUDES = -I$(XROOTD_LOCATION)/include/xrootd/ -I$(GLOBUS_LOCATION)/include/$(GLOBUS_FLAVOR)/

lib_LTLIBRARIES = libglobus_gridftp_server_eos.la

libglobus_gridftp_server_eos_la_SOURCES = dsi_eos.hh  globus_gridftp_server_eos.cc
libglobus_gridftp_server_eos_la_LIBADD =
libglobus_gridftp_server_eos_la_LDFLAGS = -L$(XROOTD_LIBDIR)/ -lXrdPosix -L$(GLOBUS_LOCATION)/lib/ -lglobus_ftp_control_$(GLOBUS_FLAVOR)

install-exec-local:	
	if [ -e "$(DESTDIR)/$(GLOBUS_LOCATION)/lib/libglobus_gridftp_server_eos_$(GLOBUS_FLAVOR).so" ]; then  unlink $(DESTDIR)/$(GLOBUS_LOCATION)/lib/libglobus_gridftp_server_eos_$(GLOBUS_FLAVOR).so; fi 
	mkdir -p $(DESTDIR)/$(GLOBUS_LOCATION)/lib
	ln -s  $(libdir)/libglobus_gridftp_server_eos.so $(DESTDIR)/$(GLOBUS_LOCATION)/lib/libglobus_gridftp_server_eos_$(GLOBUS_FLAVOR).so 
	mkdir -p $(DESTDIR)/$(sysconfdir)/init.d/
	$(INSTALL) -m 755 $(srcdir)/etc/init.d/eos-gridftp $(DESTDIR)/$(sysconfdir)/init.d/eos-gridftp

uninstall:		
	unlink $(GLOBUS_LOCATION)/lib/libglobus_gridftp_server_eos_$(GLOBUS_FLAVOR).so 
	unlink $(DESTDIR)/$(sysconfdir)/init.d/eos-gridftp


EXTRA_DIST = bootstrap.sh etc/init.d/eos-gridftp eos-dsi.spec eos-dsi.spec.in

rpm:    dist
	cp $(DIST_ARCHIVES) /usr/src/redhat/SOURCES/
	rpmbuild -bb eos-dsi.spec

