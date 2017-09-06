# Try to find ZSTD
# Once done, this will define
#
# ZSTD_FOUND               - system has zstd
# ZSTD_INCLUDE_DIR         - zstd include directories
# ZSTD_LIB_DIR             - libraries needed to use zstd
# ZSTD_PRIVATE_INCLUDE_DIR - zstd private include directory
#
# ZSTD_DIR may be defined as a hint for where to look

include(FindPackageHandleStandardArgs)

if(ZSTD_INCLUDE_DIRS AND ZSTD_LIBRARIES)
  set(ZSTD_FIND_QUIETLY TRUE)
else()
  find_path(
    ZSTD_INCLUDE_DIR
    NAMES zstd.h
    HINTS ${ZSTD_ROOT_DIR} $ENV{ZSTD_ROOT_DIR})

  find_library(
    ZSTD_LIBRARY
    NAMES zstd
    HINTS ${ZSTD_ROOT_DIR} $ENV{ZSTD_ROOT_DIR}
    PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

  set(ZSTD_INCLUDE_DIRS ${ZSTD_INCLUDE_DIR})
  set(ZSTD_LIBRARIES ${ZSTD_LIBRARY})

  find_package_handle_standard_args(
    zstd DEFAULT_MSG
    ZSTD_LIBRARY
    ZSTD_INCLUDE_DIR)

  mark_as_advanced(
    ZSTD_LIBRARY
    ZSTD_INCLUDE_DIR)
endif()
