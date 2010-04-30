$.NOTPARALLEL:

AUTOMAKE_OPTIONS = foreign 1.6

SUBDIRS = XrdCommon XrdMqOfs XrdCapability XrdFstOfs XrdMgmOfs Console

.PHONY: doc silent

silent:
	@cd src; $(MAKE) | sed -n -e '/libtool/d' \
            -e 's/.*-o  *\([^ ]*\).*/++++ Creating \1 ++++/p' \
            -e '/make/p'

EXTRA_DIST = bootstrap.sh Doxyfile etc/xrd.cf.mgm etc/xrd.cf.fst etc/xrd.cf.mq etc/logrotate.d/xrd etc/init.d/xrd etc/init.d/cmsd etc/sysconfig/xrd.example configure.ac m4

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
	@mkdir -p $(DESTDIR)/var/log/xroot/fst/
	@mkdir -p $(DESTDIR)/var/log/xroot/mgm/
	@mkdir -p $(DESTDIR)/var/log/xroot/mq/
	@mkdir -p $(DESTDIR)/var/log/xroot/empty
	@mkdir -p $(DESTDIR)/var/spool/xroot/admin
	@mkdir -p $(DESTDIR)/var/spool/xroot/core
	@mkdir -p $(DESTDIR)/etc/rc.d/init.d
	@mkdir -p $(DESTDIR)/etc/logrotate.d
	@mkdir -p $(DESTDIR)/etc/sysconfig
	@$(INSTALL) -m 644 $(srcdir)/etc/xrd.cf.mgm $(DESTDIR)/etc/xrd.cf.mgm
	@$(INSTALL) -m 644 $(srcdir)/etc/xrd.cf.fst $(DESTDIR)/etc/xrd.cf.fst
	@$(INSTALL) -m 644 $(srcdir)/etc/xrd.cf.mq  $(DESTDIR)/etc/xrd.cf.mq
	@$(INSTALL) -m 755 $(srcdir)/etc/init.d/xrd $(DESTDIR)/etc/rc.d/init.d/xrd
	@$(INSTALL) -m 755 $(srcdir)/etc/init.d/cmsd $(DESTDIR)/etc/rc.d/init.d/cmsd
	@$(INSTALL) -m 644 $(srcdir)/etc/logrotate.d/xrd $(DESTDIR)/etc/logrotate.d/xrd
	@$(INSTALL) -m 644 $(srcdir)/etc/sysconfig/xrd.example $(DESTDIR)/etc/sysconfig/xrd.example
