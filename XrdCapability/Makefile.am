INCLUDES = -I$(XROOTD_INCDIR) -I../

lib_LTLIBRARIES = libXrdCapability.la

libXrdCapability_la_SOURCES = XrdCapability.cc XrdCapability.hh 
libXrdCapability_la_LIBADD =
libXrdCapability_la_LDFLAGS = -L${XROOTD_LIBDIR}/ -lXrdSec -lXrdSys -lXrdOuc -lXrdNet  $(top_srcdir)/XrdMqOfs/libXrdMqClient.la $(top_srcdir)/XrdCommon/libXrdCommon.la


