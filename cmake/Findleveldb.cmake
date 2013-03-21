# Try to find leveldb
# Once done, this will define
#
# LEVELDB_FOUND       - system has leveldb
# LEVELDB_INCLUDE_DIR - the leveldb include directory
# LEVELDB_LIB_DIR     - the leveldb library directory
#
# LEVELDB_DIR may be defined as a hint for where to look
# LEVELDB_LIBRARIES   - the level db library name(s)

FIND_PATH(LEVELDB_INCLUDE_DIR leveldb/db.h
  HINTS
  ${LEVELDB_DIR}
  $ENV{LEVELDB_DIR}
  /usr
  /usr/local
  /opt/
  PATH_SUFFIXES include/
  PATHS /opt
)

FIND_LIBRARY(LEVELDB_LIB leveldb
  HINTS
  ${LEVELDB_DIR}
  $ENV{LEVELDB_DIR}
  /usr
  /usr/local
  /opt/leveldb/
  PATH_SUFFIXES lib
)


set(LEVELDB_LIBRARIES ${LEVELDB_LIB})


GET_FILENAME_COMPONENT( LEVELDB_LIB_DIR ${LEVELDB_LIB} PATH )

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(leveldb DEFAULT_MSG LEVELDB_LIB_DIR LEVELDB_INCLUDE_DIR )
