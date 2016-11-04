# Try to find hiredis
# Once done, this will define
#
# HIREDIS_FOUND       - system has hiredis
# HIREDIS_INCLUDE_DIR - the hiredis include directory
# HIREDIS_LIB_DIR     - the hiredis library directory
#
# HIREDIS_DIR may be defined as a hint for where to look
# HIREDIS_LIBRARIES   - the level db library name(s)

FIND_PATH(HIREDIS_INCLUDE_DIR hiredis/hiredis.h
  HINTS
  ${HIREDIS_DIR}
  $ENV{HIREDIS_DIR}
  /usr
  /usr/local
  /opt/
  PATH_SUFFIXES include/
  PATHS /opt
)

FIND_LIBRARY(HIREDIS_LIB hiredis
  HINTS
  ${HIREDIS_DIR}
  $ENV{HIREDIS_DIR}
  /usr
  /usr/local
  /opt/hiredis/
  PATH_SUFFIXES lib
)


set(HIREDIS_LIBRARIES ${HIREDIS_LIB})


GET_FILENAME_COMPONENT( HIREDIS_LIB_DIR ${HIREDIS_LIB} PATH )

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(hiredis DEFAULT_MSG HIREDIS_LIB_DIR HIREDIS_INCLUDE_DIR )
