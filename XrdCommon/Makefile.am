INCLUDES = -I$(EOS_INCDIR) -I$(XROOTD_INCDIR) ${OPENSSL_INCDIR} -I../

lib_LTLIBRARIES = libXrdCommon.la

libXrdCommon_la_SOURCES = XrdCommon.cc XrdCommonTiming.hh XrdCommonProc.cc XrdCommonProc.hh XrdCommonStringStore.cc XrdCommonStringStore.hh XrdCommonMapping.cc XrdCommonMapping.hh XrdCommonSymKeys.cc XrdCommonSymKeys.hh XrdCommonFileId.hh XrdCommonLayoutId.hh XrdCommonFileSystem.hh XrdCommonLogging.cc XrdCommonLogging.hh XrdCommonStatfs.cc XrdCommonStatfs.hh XrdCommonFmd.hh XrdCommonFmd.cc XrdCommonPath.hh XrdCommonXrdClientLock.hh XrdCommonClientAdmin.hh
libXrdCommon_la_LIBADD =
libXrdCommon_la_LDFLAGS = -L${XROOTD_LIBDIR}/ -lXrdSec -lXrdSys -lXrdOuc -lXrdNet ${OPENSSL_LIBDIR} -lcrypto -lz -luuid

