$.NOTPARALLEL:

AUTOMAKE_OPTIONS = foreign 1.6

SUBDIRS = XrdCommon XrdMqOfs XrdCapability XrdFstOfs Namespace XrdMgmOfs Console Sync

.PHONY: doc silent

silent:
	@cd src; $(MAKE) | sed -n -e '/libtool/d' \
            -e 's/.*-o  *\([^ ]*\).*/++++ Creating \1 ++++/p' \
            -e '/make/p'

EXTRA_DIST = bootstrap.sh Doxyfile etc/xrd.cf.mgm etc/xrd.cf.fst etc/xrd.cf.mq etc/xrd.cf.sync etc/cron.d/xrd-logs etc/init.d/xrd etc/init.d/cmsd etc/init.d/eossync etc/sysconfig/xrd.example configure.ac m4 Namespace/tests/*.cc Namespace/tests/Makefile.am Namespace/utils Namespace/views Namespace/persistency sss/eos-sss.keytab XrdFstOfs/eosfsttrim

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
	@mkdir -p $(DESTDIR)/$(sysconfdir)/cron.d
	@mkdir -p $(DESTDIR)/$(sysconfdir)/sysconfig
	@$(INSTALL) -m 644 $(srcdir)/etc/xrd.cf.mgm $(DESTDIR)/$(sysconfdir)/xrd.cf.mgm
	@$(INSTALL) -m 644 $(srcdir)/etc/xrd.cf.fst $(DESTDIR)/$(sysconfdir)/xrd.cf.fst
	@$(INSTALL) -m 644 $(srcdir)/etc/xrd.cf.mq  $(DESTDIR)/$(sysconfdir)/xrd.cf.mq
	@$(INSTALL) -m 644 $(srcdir)/etc/xrd.cf.sync  $(DESTDIR)/$(sysconfdir)/xrd.cf.sync
	@$(INSTALL) -m 755 $(srcdir)/etc/init.d/xrd $(DESTDIR)/$(sysconfdir)/rc.d/init.d/xrd
	@$(INSTALL) -m 755 $(srcdir)/etc/init.d/eossync $(DESTDIR)/$(sysconfdir)/rc.d/init.d/eossync
	@$(INSTALL) -m 755 $(srcdir)/etc/init.d/cmsd $(DESTDIR)/$(sysconfdir)/rc.d/init.d/cmsd
	@$(INSTALL) -m 644 $(srcdir)/etc/cron.d/xrd-logs $(DESTDIR)/$(sysconfdir)/cron.d/xrd-logs
	@$(INSTALL) -m 644 $(srcdir)/etc/sysconfig/xrd.example $(DESTDIR)/$(sysconfdir)/sysconfig/xrd.example
#	@mv  $(DESTDIR)/$(prefix)/bin/eosfstfsck $(DESTDIR)/$(prefix)/bin/eosfstfsck.v${PACKAGE_VERSION}
#	@ln -s $(DESTDIR)/$(prefix)/bin/eosfstfsck.v${PACAKGE_VERSION} $(DESTDIR)/$(prefix)/bin/eosfstfsck
	@$(INSTALL) -m 755 $(srcdir)/XrdFstOfs/eosfsttrim  $(DESTDIR)/$(prefix)/bin/eosfsttrim
	@echo "0 u:nobody g:nobody n:eos-test N:5506672669367468033 c:1282122142 e:0 k:9ae1ada78699e0ecd0320060e33d0bfa6b60c5c911e67b9b1b89e01dacf4d5b0" > $(srcdir)/sss/eos-sss.keytab
	@chmod 600 $(srcdir)/sss/eos-sss.keytab
	@xrdsssadmin -g daemon -u daemon -k eos-test add $(srcdir)/sss/eos-sss.keytab
	@mkdir -p $(DESTDIR)/$(prefix)/sss/
	@test -e $(DESTDIR)/$(prefix)/sss/eos-sss.keytab || $(INSTALL) -m 600 $(srcdir)/sss/eos-sss.keytab $(DESTDIR)/$(prefix)/sss/
	@chown daemon.daemon $(DESTDIR)/$(prefix)/sss/eos-sss.keytab
