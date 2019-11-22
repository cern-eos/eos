# Try to find leveldb
# Once done, this will define
#
# LEVELDB_FOUND        -  system has leveldb
# LEVELDB_INCLUDE_DIRS -  leveldb include directories
# LEVELDB_LIBRARIES    -  libraries needed to use leveldb
#
# and the following imported target
#
# LEVELDB::LEVELDB

find_package(PkgConfig)
pkg_check_modules(PC_leveldb QUIET leveldb)
set(LEVELDB_VERSION ${PC_leveldb_VERSION})

find_path(LEVELDB_INCLUDE_DIR
  NAMES leveldb/db.h
  HINTS ${LEVELDB_ROOT} ${PC_leveldb_INCLUDEDIR} ${PC_leveldb_INCLUDE_DIRS})

find_library(LEVELDB_LIBRARY
  NAMES leveldb
  HINTS ${LEVELDB_ROOT} ${PC_leveldb_LIBDIR} ${PC_leveldb_LIBRARY_DIRS}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(leveldb
  REQUIRED_VARS LEVELDB_LIBRARY LEVELDB_INCLUDE_DIR
  VERSION_VAR LEVELDB_VERSION)

if (LEVELDB_FOUND AND NOT TARGET LEVELDB::LEVELDB)
  mark_as_advanced(LEVELDB_LIBRARY LEVELDB_INCLUDE_DIR)
  add_library(LEVELDB::LEVELDB UNKNOWN IMPORTED)
  set_target_properties(LEVELDB::LEVELDB PROPERTIES
    IMPORTED_LOCATION "${LEVELDB_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${LEVELDB_INCLUDE_DIR}")
endif()

set(LEVELDB_INCLUDE_DIRS ${LEVELDB_INCLUDE_DIR})
set(LEVELDB_LIBRARIES ${LEVELDB_LIBRARY})
unset(LEVELDB_INCLUDE_DIR)
unset(LEVELDB_LIBRARY)
