$.NOTPARALLEL:

AUTOMAKE_OPTIONS = foreign 1.6

INCLUDES = -I$(EOS_INCDIR) -I$(XROOTD_INCDIR) ${OPENSSL_INCDIR} -I../

#-------------------------------------------------------------------------------
# Libraries and programs
#-------------------------------------------------------------------------------
lib_LTLIBRARIES = \
        libEosNamespace.la

libEosNamespace_la_SOURCES = \
	FileMD.cc            \
	FileMD.hh            \
	ContainerMD.cc       \
	ContainerMD.hh       \
	IFileMDSvc.hh        \
	IContainerMDSvc.hh   \
	IView.hh             \
	MDException.hh       \
	persistency/ChangeLogFile.hh      \
	persistency/ChangeLogFile.cc      \
	persistency/Buffer.hh             \
	persistency/ChangeLogConstants.hh \
	persistency/ChangeLogConstants.cc \
	persistency/ChangeLogContainerMDSvc.hh \
	persistency/ChangeLogContainerMDSvc.cc \
	persistency/ChangeLogFileMDSvc.hh      \
	persistency/ChangeLogFileMDSvc.cc      \
	views/HierarchicalView.hh         \
	views/HierarchicalView.cc


