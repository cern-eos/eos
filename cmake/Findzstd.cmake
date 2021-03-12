# Try to find zstd
# Once done, this will define
#
# ZSTD_FOUND          - system has zstd
# ZSTD_LIBRARIES      - zstd library
#
# and the following imported target
#
# ZSTD::ZSTD

find_library(ZSTD_LIBRARY
  NAMES zstd
  HINTS ${ZSTD_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(zstd DEFAULT_MSG ZSTD_LIBRARY)

if (ZSTD_FOUND AND NOT TARGET ZSTD::ZSTD)
  mark_as_advanced(ZSTD_FOUND ZSTD_LIBRARY)
  add_library(ZSTD::ZSTD UNKNOWN IMPORTED)
  set_target_properties(ZSTD::ZSTD PROPERTIES
    IMPORTED_LOCATION "${ZSTD_LIBRARY}")
endif()

unset(ZSTD_LIBRARY)
