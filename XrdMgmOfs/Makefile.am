INCLUDES = -I$(EOS_INCDIR) -I$(XROOTD_INCDIR) -I../

lib_LTLIBRARIES = libXrdMgmOfs.la

libXrdMgmOfs_la_SOURCES = XrdMgmOfs.cc XrdMgmOfs.hh XrdMgmOfsConfigure.cc XrdMgmOfsTrace.hh XrdMgmOfsSecurity.hh XrdMgmFstNode.cc XrdMgmFstNode.hh XrdMgmMessaging.cc XrdMgmProcInterface.cc XrdMgmProcInterface.hh XrdMgmFstFileSystem.hh
libXrdMgmOfs_la_LIBADD =
libXrdMgmOfs_la_LDFLAGS = -L${XROOTD_LIBDIR}/ -lXrdSec -lXrdSys -lXrdOuc -lXrdNet $(top_srcdir)/XrdCommon/libXrdCommon.la $(top_srcdir)/XrdCapability/libXrdCapability.la $(top_srcdir)/XrdMqOfs/libXrdMqClient.la 
