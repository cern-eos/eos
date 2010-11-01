INCLUDES = -I$(EOS_INCDIR) -I$(XROOTD_INCDIR) -I../

lib_LTLIBRARIES = libXrdMgmOfs.la

libXrdMgmOfs_la_SOURCES = XrdMgmOfs.cc XrdMgmOfs.hh XrdMgmOfsConfigure.cc XrdMgmOfsTrace.hh XrdMgmOfsSecurity.hh XrdMgmFstNode.cc XrdMgmFstNode.hh XrdMgmMessaging.cc XrdMgmProcInterface.cc XrdMgmProcInterface.hh XrdMgmFstFileSystem.hh XrdMgmQuota.cc XrdMgmQuota.hh XrdMgmConfigEngine.cc XrdMgmConfigEngine.hh XrdMgmPolicy.cc XrdMgmPolicy.hh XrdMgmVid.cc XrdMgmVid.hh XrdMgmOfsStat.hh
libXrdMgmOfs_la_LIBADD =
libXrdMgmOfs_la_LDFLAGS = -L${XROOTD_LIBDIR}/ -lXrdSec -lXrdSys -lXrdOuc -lXrdNet -lXrdNetUtil $(top_srcdir)/XrdCommon/libXrdCommon.la $(top_srcdir)/XrdCapability/libXrdCapability.la $(top_srcdir)/XrdMqOfs/libXrdMqClient.la $(top_srcdir)/Namespace/libEosNamespace.la
