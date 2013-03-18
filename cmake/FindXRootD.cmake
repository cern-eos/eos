# Try to find XROOTD
# Once done, this will define
#
# XROOTD_FOUND       - system has XRootD
# XROOTD_INCLUDE_DIR - the XRootD include directory
# XROOTD_LIB_DIR     - the XRootD library directory
# XROOTD_PRIVATE_INDCLUDE_DIR - the XRootD private include directory
#
# XROOTD_DIR may be defined as a hint for where to look

FIND_PATH(XROOTD_INCLUDE_DIR XrdVersion.hh
  HINTS
  $PWD/xrootd/src/include/ 	
  ${XROOTD_DIR}
  $ENV{XROOTD_DIR}
  /usr
  /opt/xrootd/
  PATH_SUFFIXES include/xrootd
  PATHS /opt/xrootd
)

FIND_PATH(XROOTD_PRIVATE_INCLUDE_DIR XrdOss/XrdOssApi.hh
  HINTS
  ${XROOTD_DIR}
  $ENV{XROOTD_DIR}
  /usr
  /opt/xrootd/
  PATH_SUFFIXES include/xrootd/private
  PATHS /opt/xrootd/private
)

FIND_LIBRARY(XROOTD_UTILS XrdUtils
  HINTS
  ${XROOTD_DIR}
  $ENV{XROOTD_DIR}
  /usr
  /opt/xrootd/
  PATH_SUFFIXES lib
)

GET_FILENAME_COMPONENT( XROOTD_LIB_DIR ${XROOTD_UTILS} PATH )

INCLUDE( FindPackageHandleStandardArgs )
FIND_PACKAGE_HANDLE_STANDARD_ARGS( XRootD DEFAULT_MSG 
                                          XROOTD_LIB_DIR 
                                          XROOTD_INCLUDE_DIR
                                          XROOTD_PRIVATE_INCLUDE_DIR )
