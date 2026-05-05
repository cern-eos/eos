# Try to find zstd
# Once done, this will define
#
# ZSTD_FOUND          - system has zstd
# ZSTD_LIBRARIES      - zstd library
#
# and the following imported target
#
# ZSTD::ZSTD

find_path(ZSTD_INCLUDE_DIR
  NAMES zstd.h
  HINTS ${ZSTD_ROOT}
  PATH_SUFFIXES include)

find_library(ZSTD_LIBRARY
  NAMES zstd
  HINTS ${ZSTD_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(zstd DEFAULT_MSG ZSTD_LIBRARY ZSTD_INCLUDE_DIR)

if (ZSTD_FOUND AND NOT TARGET ZSTD::ZSTD)
  mark_as_advanced(ZSTD_FOUND ZSTD_LIBRARY ZSTD_INCLUDE_DIR)
  add_library(ZSTD::ZSTD UNKNOWN IMPORTED)
  set_target_properties(ZSTD::ZSTD PROPERTIES
    IMPORTED_LOCATION "${ZSTD_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${ZSTD_INCLUDE_DIR}")
endif()

unset(ZSTD_LIBRARY)
unset(ZSTD_INCLUDE_DIR)
