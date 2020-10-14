# Try to find rocksdb
# Once done, this will define
#
# ROCKSDB_FOUND              - system has rocksdb
# ROCKSDB_INCLUDE_DIRS       - rocksdb include directories
# ROCKSDB_LIBRARY            - rocksdb library
#
# ROCKSDB_ROOT_DIR may be defined as a hint for where to look

include(FindPackageHandleStandardArgs)

if(ROCKSDB_INCLUDE_DIRS AND ROCKSDB_LIBRARIES)
  set(ROCKSDB_FIND_QUIETLY TRUE)
else()
  find_path(
    ROCKSDB_INCLUDE_DIR
    NAMES rocksdb/version.h
    HINTS ${ROCKSDB_ROOT_DIR}
    PATH_SUFFIXES include)

  find_library(
    ROCKSDB_LIBRARY
    NAMES librocksdb.a
    HINTS ${ROCKSDB_ROOT_DIR})

  if (AARCH64)
    find_path(
      ROCKSDB_ZSTD_INCLUDE_DIR
      NAMES zstd.h
      HINTS ${BZ2_ROOT_DIR}
      PATH_SUFFIXES include)
    find_library(
      ROCKSDB_ZSTD_LIBRARY
      NAMES libzstd.so
      HINTS ${BZ2_ROOT_DIR}
      PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

    find_package_handle_standard_args(
      rocksdb
      DEFAULT_MSG
      ROCKSDB_LIBRARY
      ROCKSDB_BZ2_LIBRARY
      ROCKSDB_ZSTD_LIBRARY
      ROCKSDB_INCLUDE_DIR
      ROCKSDB_BZ2_INCLUDE_DIR
      ROCKSDB_ZSTD_INCLUDE_DIR
    )
    set(ROCKSDB_LIBRARIES ${ROCKSDB_LIBRARY} ${ROCKSDB_BZ2_LIBRARY} ${ROCKSDB_ZSTD_LIBRARY})
    set(ROCKSDB_INCLUDE_DIRS ${ROCKSDB_INCLUDE_DIR} ${BZ2_INCLUDE_DIR} ${ROCKSDB_ZSTD_INCLUDE_DIR})

    LIST(APPEND ROCKSDB_LIBRARY ${BZ2_LIBRARY} ${ROCKSDB_ZSTD_LIBRARY})
  else()
    set(ROCKSDB_LIBRARIES ${ROCKSDB_LIBRARY})
    set(ROCKSDB_INCLUDE_DIRS ${ROCKSDB_INCLUDE_DIR})

    find_package_handle_standard_args(
      rocksdb
      DEFAULT_MSG
      ROCKSDB_LIBRARY
      ROCKSDB_INCLUDE_DIR)
  endif()

  if(ROCKSDB_FOUND)
    add_library(rocksdb STATIC IMPORTED)
    set_property(TARGET rocksdb PROPERTY IMPORTED_LOCATION ${ROCKSDB_LIBRARY})
  endif()
endif()
