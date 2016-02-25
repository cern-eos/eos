## Process this file with automake to produce Makefile.in
bin_PROGRAMS = eosfs
INCLUDES = -I$(XROOTD)/include/xrootd/

noinst_LTLIBRARIES = libXrdPosixFuse.la

libXrdPosixFuse_la_SOURCES = xrdposix.cc xrdposix.hh

eosfs_SOURCDES = eosfs.c 

eosfs_LDADD = -L$(XROOTD)/lib -lXrdPosix ./libXrdPosixFuse.la -L$(FUSE_LOCATION)/lib -lfuse -lstdc++ $(LIBGCCS)

LDADD = -L$(XROOTD)/lib -lXrdPosix -L$(FUSE_LOCATION)/lib -lfuse


uninstall-local:
	rm -f $(DESTDIR)/${sysconfdir}/fuse.conf
	rm -f $(DESTDIR)/${sysconfdir}/init.d/eosd
	rm -f $(DESTDIR)/${prefix}/bin/eosfs.start

EXTRA_DIST =      bootstrap.sh




