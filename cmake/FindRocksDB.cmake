# Try to find rocksdb
# Once done, this will define
#
# ROCKSDB_FOUND              - system has rocksdb
#
# and the following imported targets
#
# ROCKSDB::ROCKSDB

find_path(ROCKSDB_INCLUDE_DIR
  NAMES rocksdb/version.h
  HINTS ${ROCKSDB_ROOT}
  PATH_SUFFIXES include)

find_library(ROCKSDB_LIBRARY
  NAMES rocksdb
  HINTS ${ROCKSDB_ROOT})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(RocksDB
  REQUIRE_VARS ROCKSDB_LIBRARY ROCKSDB_INCLUDE_DIR)
mark_as_advanced(ROCKSDB_FOUND ROCKSDB_LIBRARY ROCKSDB_INCLUDE_DIR)

if (ROCKSDB_FOUND AND NOT TARGET ROCKSDB::ROCKSDB)
  add_library(ROCKSDB::ROCKSDB UNKNOWN IMPORTED)
  set_target_properties(ROCKSDB::ROCKSDB PROPERTIES
    IMPORTED_LOCATION "${ROCKSDB_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${ROCKSDB_INCLUDE_DIR}"
    INTERFACE_COMPILE_DEFINITIONS "HAVE_ROCKSDB=1")
else ()
  message(WARNING "Notice: rocksdb not found, no rockdb support")
  add_library(ROCKSDB::ROCKSDB INTERFACE IMPORTED)
endif()

#unset(ROCKSDB_INCLUDE_DIR)
#unset(ROCKSDB_LIBRARY)
