# Try to find bz2
# Once done, this will define
#
# BZ2_FOUND          - system has zlib
# BZ2_INCLUDE_DIRS   - zlib include directories
# BZ2_LIBRARY        - zlib library
# BZ2_LIBRARY_STATIC - static zlib library

include(FindPackageHandleStandardArgs)

if(BZ2_INCLUDE_DIRS AND BZ2_LIBRARY)
  set(BZ2_FIND_QUIETLY TRUE)
else()
  find_path(
    BZ2_INCLUDE_DIR
    NAMES bzlib.h
    HINTS ${BZ2_ROOT_DIR}
    PATH_SUFFIXES include)

  find_library(
    BZ2_LIBRARY
    NAMES bz2
    HINTS ${BZ2_ROOT_DIR}
    PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

  set(BZ2_INCLUDE_DIRS ${BZ2_INCLUDE_DIR})

  find_package_handle_standard_args(
    bz2
    DEFAULT_MSG
    BZ2_LIBRARY BZ2_INCLUDE_DIR)
  mark_as_advanced(BZ2_LIBRARY BZ2_INCLUDE_DIR)
endif()
