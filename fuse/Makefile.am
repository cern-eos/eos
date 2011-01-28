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
	mkdir -p $(DESTDIR)/${prefix}/bin/
	mkdir -p $(DESTDIR)/${sysconfdir}/init.d/
	mkdir -p $(DESTDIR)/${sysconfdir}/sysconfig/
	$(INSTALL) -m 755 etc/fuse.conf $(DESTDIR)/${sysconfdir}/fuse.conf
	$(INSTALL) -m 755 etc/init.d/eosd $(DESTDIR)/${sysconfdir}/init.d/eosd
	$(INSTALL) -m 755 eosfs.start $(DESTDIR)/${prefix}/bin/eosfs.start


uninstall-local:
	rm -f $(DESTDIR)/${sysconfdir}/fuse.conf
	rm -f $(DESTDIR)/${sysconfdir}/init.d/eosd
	rm -f $(DESTDIR)/${prefix}/bin/eosfs.start

EXTRA_DIST =	etc/fuse.conf \
		etc/init.d/eosd \
		eos-fuse.spec \
	        eosfs.start \
	        bootstrap.sh



