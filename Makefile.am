$.NOTPARALLEL:

AUTOMAKE_OPTIONS = foreign 1.6

SUBDIRS = XrdCommon XrdMqOfs XrdCapability XrdFstOfs Namespace XrdMgmOfs Console 

.PHONY: doc silent

silent:
	@cd src; $(MAKE) | sed -n -e '/libtool/d' \
            -e 's/.*-o  *\([^ ]*\).*/++++ Creating \1 ++++/p' \
            -e '/make/p'

EXTRA_DIST = bootstrap.sh Doxyfile etc/xrd.cf.mgm etc/xrd.cf.fst etc/xrd.cf.mq etc/logrotate.d/xrd etc/init.d/xrd etc/init.d/cmsd etc/sysconfig/xrd.example configure.ac m4 Namespace/tests/*.cc Namespace/tests/Makefile.am Namespace/utils Namespace/views Namespace/persistency

rpm:    dist
	cp $(DIST_ARCHIVES) /usr/src/redhat/SOURCES/
	rpmbuild -bb eos.spec

doc:
	@export DOXYGEN=`which doxygen`; \
	if test -x "$${DOXYGEN}"; then \
	echo "****| BUILDING DOXYGEN DOCUMENTATION"; \
	$${DOXYGEN} Doxyfile >& doxy.log; \
	fi 


clean-doc:
	@rm -rf doxydoc/ doxy.log

install-exec-local: 
	@mkdir -p $(DESTDIR)/$(localstatedir)/log/xroot/fst/
	@mkdir -p $(DESTDIR)/$(localstatedir)/log/xroot/mgm/
	@mkdir -p $(DESTDIR)/$(localstatedir)/log/xroot/mq/
	@mkdir -p $(DESTDIR)/$(localstatedir)/log/xroot/empty
	@mkdir -p $(DESTDIR)/$(localstatedir)/spool/xroot/admin
	@mkdir -p $(DESTDIR)/$(localstatedir)/spool/xroot/core
	@mkdir -p $(DESTDIR)/$(sysconfdir)/rc.d/init.d
	@mkdir -p $(DESTDIR)/$(sysconfdir)/logrotate.d
	@mkdir -p $(DESTDIR)/$(sysconfdir)/sysconfig
	@$(INSTALL) -m 644 $(srcdir)/etc/xrd.cf.mgm $(DESTDIR)/$(sysconfdir)/xrd.cf.mgm
	@$(INSTALL) -m 644 $(srcdir)/etc/xrd.cf.fst $(DESTDIR)/$(sysconfdir)/xrd.cf.fst
	@$(INSTALL) -m 644 $(srcdir)/etc/xrd.cf.mq  $(DESTDIR)/$(sysconfdir)/xrd.cf.mq
	@$(INSTALL) -m 755 $(srcdir)/etc/init.d/xrd $(DESTDIR)/$(sysconfdir)/rc.d/init.d/xrd
	@$(INSTALL) -m 755 $(srcdir)/etc/init.d/cmsd $(DESTDIR)/$(sysconfdir)/rc.d/init.d/cmsd
	@$(INSTALL) -m 644 $(srcdir)/etc/logrotate.d/xrd $(DESTDIR)/$(sysconfdir)/logrotate.d/xrd
	@$(INSTALL) -m 644 $(srcdir)/etc/sysconfig/xrd.example $(DESTDIR)/$(sysconfdir)/sysconfig/xrd.example
#	@mv  $(DESTDIR)/$(prefix)/bin/eosfstfsck $(DESTDIR)/$(prefix)/bin/eosfstfsck.v${PACKAGE_VERSION}
#	@ln -s $(DESTDIR)/$(prefix)/bin/eosfstfsck.v${PACAKGE_VERSION} $(DESTDIR)/$(prefix)/bin/eosfstfsck

