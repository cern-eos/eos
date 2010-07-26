$.NOTPARALLEL:

AUTOMAKE_OPTIONS = foreign 1.6

INCLUDES = -I../ -I. -I$(EOS_INCDIR) -I$(XROOTD_INCDIR) ${OPENSSL_INCDIR}

#-------------------------------------------------------------------------------
# The namespace library
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
	utils/Descriptor.hh   \
	utils/Descriptor.cc   \
	utils/SmartPtrs.hh    \
	utils/PathProcessor.hh \
	persistency/ChangeLogFile.hh      \
	persistency/ChangeLogFile.cc      \
	persistency/Buffer.hh             \
	persistency/ChangeLogConstants.hh \
	persistency/ChangeLogConstants.cc \
	persistency/ChangeLogContainerMDSvc.hh \
	persistency/ChangeLogContainerMDSvc.cc \
	persistency/ChangeLogFileMDSvc.hh      \
	persistency/ChangeLogFileMDSvc.cc      \
	persistency/LogManager.hh              \
	persistency/LogManager.cc              \
	views/HierarchicalView.hh              \
	views/HierarchicalView.cc              \
	accounting/FileSystemView.hh           \
	accounting/FileSystemView.cc

libEosNamespace_la_LIBADD = -lrt

#-------------------------------------------------------------------------------
# The log checker
#-------------------------------------------------------------------------------
bin_PROGRAMS = eos-log-repair

eos_log_repair_SOURCES = progs/EOSLogRepair.cc
eos_log_repair_CPPFLAGS = $(INCLUDES)
eos_log_repair_DEPENDENCIES = libEosNamespace.la
eos_log_repair_LDADD  = libEosNamespace.la
