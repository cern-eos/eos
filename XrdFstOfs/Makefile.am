INCLUDES = -I$(EOS_INCDIR) -I$(XROOTD_INCDIR) -I../

lib_LTLIBRARIES = libXrdFstOfs.la

libXrdFstOfs_la_SOURCES = XrdFstOfsConfig.cc XrdFstOfsConfig.hh XrdFstOfs.cc XrdFstOfs.hh XrdFstOfsClientAdmin.hh XrdFstOfsStorage.hh XrdFstOfsStorage.cc
libXrdFstOfs_la_LIBADD =
libXrdFstOfs_la_LDFLAGS = -L${XROOTD_LIBDIR}/ -lXrdSys -lXrdOuc -lXrdNet -lXrdOfs -lXrdSec $(top_srcdir)/XrdCommon/libXrdCommon.la $(top_srcdir)/XrdCapability/libXrdCapability.la $(top_srcdir)/XrdMqOfs/libXrdMqClient.la

