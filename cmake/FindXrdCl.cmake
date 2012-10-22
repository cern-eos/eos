# Try to find XrdCl
# Once done, this will define
#
# XRDCL_FOUND       - system has XrdCl
# XRDCL_INCLUDES    - the XrdCl include directory
# XRDCL_LIB_DIR     - the XrdCl library directory
#
# XRDCL_DIR may be defined as a hint for where to look

FIND_PATH(XRDCL_INCLUDES XrdCl/XrdClFile.hh
  HINTS
  ${XRDCL_DIR}
  $ENV{XRDCL_DIR}
  /usr
  /usr/local
  /opt/xrootd/
  PATH_SUFFIXES include/xrootd/
  PATHS /opt/xrootd/
)

FIND_LIBRARY(XRDCL_LIB XrdCl
  HINTS
  ${XRDCL_DIR}
  $ENV{XRDCL_DIR}
  /usr
  /usr/local
  /opt/xrootd/
  PATH_SUFFIXES lib
)

GET_FILENAME_COMPONENT( XRDCL_LIB_DIR ${XRDCL_LIB} PATH )

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(XrdCl DEFAULT_MSG XRDCL_LIB_DIR XRDCL_INCLUDES )
