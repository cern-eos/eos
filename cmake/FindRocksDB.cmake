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
  PATHS /opt/eos/rocksdb/ /usr/local /usr
  PATH_SUFFIXES include
  NO_DEFAULT_PATH)

find_library(ROCKSDB_LIBRARY
  NAMES rocksdb
  HINTS ${ROCKSDB_ROOT}
  PATHS /opt/eos/rocksdb/ /usr/local /usr
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR} lib
  NO_DEFAULT_PATH)

find_library(ROCKSDB_TOOLS_LIBRARY
  NAMES rocksdb_tools
  HINTS ${ROCKSDB_ROOT}
  PATHS /opt/eos/rocksdb /usr/local /usr
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR} lib
  NO_DEFAULT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(RocksDB
  REQUIRED_VARS ROCKSDB_LIBRARY ROCKSDB_INCLUDE_DIR)
mark_as_advanced(ROCKSDB_FOUND ROCKSDB_LIBRARY ROCKSDB_INCLUDE_DIR)

if (ROCKSDB_FOUND AND NOT TARGET ROCKSDB::ROCKSDB)
  find_package(zstd REQUIRED)
  find_package(lz4 REQUIRED)
  find_package(BZip2 REQUIRED)
  find_package(Snappy REQUIRED)

  set(ROCKSDB_LIBRARIES "ZSTD::ZSTD;LZ4::LZ4;BZip2::BZip2;Snappy::snappy")

  #@note: The ROCKSDB_LIBRARY must be specified again after
  # the ROCKSDB_TOOLS_LIBRARY since the latter has a symbol
  # that only the former provides and since these are both
  # static libraries the linker searches from left to right
  # and notes unresolved symbols as it goes!!!
  if (ROCKSDB_TOOLS_LIBRARY)
    set(ROCKSDB_LIBRARIES "${ROCKSDB_LIBRARIES};${ROCKSDB_TOOLS_LIBRARY};${ROCKSDB_LIBRARY}")
  endif()

  add_library(ROCKSDB::ROCKSDB UNKNOWN IMPORTED)
  set_target_properties(ROCKSDB::ROCKSDB PROPERTIES
    IMPORTED_LOCATION "${ROCKSDB_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${ROCKSDB_INCLUDE_DIR}"
    INTERFACE_LINK_LIBRARIES "${ROCKSDB_LIBRARIES}"
    INTERFACE_COMPILE_DEFINITIONS "HAVE_ROCKSDB=1")
else ()
  message(WARNING "Notice: rocksdb not found, no rocksdb support")
endif()

unset(ROCKSDB_INCLUDE_DIR)
unset(ROCKSDB_LIBRARY)
