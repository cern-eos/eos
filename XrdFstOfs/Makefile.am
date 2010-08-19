INCLUDES = -I$(EOS_INCDIR) -I$(XROOTD_INCDIR) -I../


noinst_LTLIBRARIES = libXrdFstChecksum.la

libXrdFstChecksum_la_SOURCES = XrdFstOfsChecksum.hh XrdFstOfsChecksum.cc XrdFstOfsAdler.hh XrdFstOfsCRC32.hh XrdFstOfsMD5.hh XrdFstOfsSHA1.hh XrdFstOfsChecksumPlugins.hh 

libXrdFstChecksum_la_LIBADD =
libXrdFstChecksum_la_LDFLAGS = -L${XROOTD_LIBDIR}/ -lXrdOuc $(top_srcdir)/XrdCommon/libXrdCommon.la 

lib_LTLIBRARIES = libXrdFstOfs.la

libXrdFstOfs_la_SOURCES = XrdFstOfsConfig.cc XrdFstOfsConfig.hh XrdFstOfs.cc XrdFstOfs.hh XrdFstOfsFile.hh XrdFstOfsClientAdmin.hh XrdFstOfsStorage.hh XrdFstOfsStorage.cc \
	XrdFstOfsLayout.hh XrdFstOfsPlainLayout.hh XrdFstOfsPlainLayout.cc XrdFstOfsReplicaLayout.hh XrdFstOfsReplicaLayout.cc \
	XrdFstOfsReplicaParLayout.hh XrdFstOfsReplicaParLayout.cc \
	XrdFstOfsRaid5Layout.cc XrdFstOfsRaid5Layout.hh \
	XrdFstOfsLayoutPlugins.hh XrdFstDeletion.hh XrdFstTransfer.hh XrdFstTransfer.cc

libXrdFstOfs_la_LIBADD = libXrdFstChecksum.la
libXrdFstOfs_la_LDFLAGS = -L${XROOTD_LIBDIR}/ -lXrdSys -lXrdOuc -lXrdNet -lXrdOfs -lXrdSec $(top_srcdir)/XrdCommon/libXrdCommon.la $(top_srcdir)/XrdCapability/libXrdCapability.la $(top_srcdir)/XrdMqOfs/libXrdMqClient.la 


bin_PROGRAMS = eosfstfsck

eosfstfsck_SOURCES = \
                FsckFst.cc 

eosfstfsck_LDFLAGS = -static -export-dynamic
eosfstfsck_LDADD = -lncurses -lreadline -ltermcap -ldl -L${XROOTD_LIBDIR}/ -lXrdClient $(top_srcdir)/XrdCommon/libXrdCommon.la libXrdFstChecksum.la
