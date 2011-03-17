# Try to find xrd
# Once done, this will define
#
# XRD_FOUND - system has xrd
# XRD_INCLUDE_DIRS - the xrd include directories
# XRD_LIBRARIES - xrd libraries directories

if(XRD_INCLUDE_DIRS AND XRD_LIBRARIES)
set(XRD_FIND_QUIETLY TRUE)
endif(XRD_INCLUDE_DIRS AND XRD_LIBRARIES)

# find XrdSys include and library directories
find_path(XRDSYS_INCLUDE_DIR xrootd/XrdSys/XrdSysDir.hh)
find_library(XRDSYS_LIBRARY XrdSys)


# find XrdClient include and library directories
find_path(XRDCLIENT_INCLUDE_DIR xrootd/XrdClient/XrdClient.hh)
find_library(XRDCLIENT_LIBRARY XrdClient)


# find XrdNet include and library directories
find_path(XRDNET_INCLUDE_DIR xrootd/XrdNet/XrdNet.hh)
find_library(XRDNET_LIBRARY XrdNet)


# find XrdNetUtil library directories
find_library(XRDNETUTIL_LIBRARY XrdNet)


# find XrdOuc include and library directories
find_path(XRDOUC_INCLUDE_DIR xrootd/XrdOuc/XrdOucArgs.hh)
find_library(XRDOUC_LIBRARY XrdOuc)


# find XrdOfs include and library directories
find_path(XRDOFS_INCLUDE_DIR xrootd/XrdOfs/XrdOfs.hh)
find_library(XRDOFS_LIBRARY XrdOfs)


# find XrdOss include and library directories
find_path(XRDOSS_INCLUDE_DIR xrootd/XrdOss/XrdOss.hh)
find_library(XRDOSS_LIBRARY XrdOss)


set(XRD_INCLUDE_DIRS ${XRDSYS_INCLUDE_DIR} ${XRDCLIENT_INCLUDE_DIR} ${XRDNET_INCLUDE_DIR} ${XRDOUC_INCLUDE_DIR} ${XRDOFS_INCLUDE_DIR} ${XRDOSS_INCLUDE_DIR})
set(XRD_LIBRARIES ${XRDSYS_LIBRARY} ${XRDCLIENT_LIBRARY} ${XRDNET_LIBRARY} ${XRDNETUTIL_LIBRARY} ${XRDOUC_LIBRARY} ${XRDOFS_LIBRARY} ${XRDOSS_LIBRARY})

# handle the QUIETLY and REQUIRED arguments and set XRD_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(xrd DEFAULT_MSG XRDSYS_INCLUDE_DIR XRDCLIENT_INCLUDE_DIR XRDNET_INCLUDE_DIR XRDOUC_INCLUDE_DIR XRDOFS_INCLUDE_DIR XRDOSS_INCLUDE_DIR XRDSYS_LIBRARY XRDCLIENT_LIBRARY XRDNET_LIBRARY XRDNETUTIL_LIBRARY XRDOUC_LIBRARY XRDOFS_LIBRARY XRDOSS_LIBRARY)

mark_as_advanced(XRDSYS_INCLUDE_DIR XRDCLIENT_INCLUDE_DIR XRDNET_INCLUDE_DIR XRDOUC_INCLUDE_DIR XRDOFS_INCLUDE_DIR XRDOSS_INCLUDE_DIR XRDSYS_LIBRARY XRDCLIENT_LIBRARY XRDNET_LIBRARY XRDNETUTIL_LIBRARY XRDOUC_LIBRARY XRDOFS_LIBRARY XRDOSS_LIBRARY)
