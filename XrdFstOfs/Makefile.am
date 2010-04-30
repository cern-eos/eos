INCLUDES = -I$(XROOTD_INCDIR) -I../

lib_LTLIBRARIES = libXrdFstOfs.la

libXrdFstOfs_la_SOURCES = XrdFstOfs.cc XrdFstOfs.hh XrdFstOfsClientAdmin.hh
libXrdFstOfs_la_LIBADD =
libXrdFstOfs_la_LDFLAGS = -L${XROOTD_LIBDIR}/ -lXrdSys -lXrdOuc -lXrdNet -lXrdOfs -lXrdSec $(top_srcdir)/XrdCommon/libXrdCommon.la $(top_srcdir)/XrdCapability/libXrdCapability.la $(top_srcdir)/XrdMqOfs/libXrdMqClient.la

