## Process this file with automake to produce Makefile.in


bin_PROGRAMS = eosd eosfs
INCLUDES = -I$(XROOTD_LOCATION)/include/xrootd/

noinst_LTLIBRARIES = libXrdPosixFuse.la

libXrdPosixFuse_la_SOURCES = xrdposix.cc xrdposix.hh

eosd_SOURCES = eosd.c 

eosfs_SOURCDES = eosfs.c 

eosd_LDADD = -L$(XROOTD_LOCATION)/lib -lXrdPosix ./libXrdPosixFuse.la -L$(FUSE_LOCATION)/lib -lfuse -lstdc++ $(LIBGCCS)
eosfs_LDADD = -L$(XROOTD_LOCATION)/lib -lXrdPosix ./libXrdPosixFuse.la -L$(FUSE_LOCATION)/lib -lfuse -lstdc++ $(LIBGCCS)

LDADD = -L$(XROOTD_LOCATION)/lib -lXrdPosix -L$(FUSE_LOCATION)/lib -lfuse


install-exec-local:
	mkdir -p $(DESTDIR)/usr/bin/
	mkdir -p $(DESTDIR)/etc/init.d/
	mkdir -p $(DESTDIR)/etc/sysconfig/
	$(INSTALL) -m 755 etc/fuse.conf $(DESTDIR)/etc/fuse.conf
	$(INSTALL) -m 755 etc/init.d/eosd $(DESTDIR)/etc/init.d/eosd
	$(INSTALL) -m 755 eosfs.start $(DESTDIR)/bin/eosfs.start


uninstall-local:
	rm -f $(DESTDIR)/etc/fuse.conf
	rm -f $(DESTDIR)/etc/init.d/eosd
	rm -f $(DESTDIR)/bin/eosfs.start

EXTRA_DIST =	etc/fuse.conf \
		etc/init.d/eosd \
		eos-fuse.spec \
	        eosfs.start \
	        bootstrap.sh

rpm		: dist
		cp $(DIST_ARCHIVES) /usr/src/redhat/SOURCES/
		rpmbuild -bb eos-fuse.spec





