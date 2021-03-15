# Try to find jemalloc
# Once done, this will define
#
# JEMALLOC_FOUND          - system has jemalloc
# JEMALLOC_INCLUDE_DIRS   - jemalloc include directories
# JEMALLOC_LIBRARIES      - libraries needed to use jemalloc
#
# and the following imported targets
#
# JEMALLOC::JEMALLOC
#
find_path(JEMALLOC_INCLUDE_DIR
  NAMES jemalloc.h
  HINTS ${JEMALLOC_ROOT}
  PATH_SUFFIXES include jemalloc)

find_library(JEMALLOC_LIBRARY
  NAME jemalloc
  HINTS ${JEMALLOC_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(jemalloc
  REQUIRED_VARS JEMALLOC_LIBRARY JEMALLOC_INCLUDE_DIR)

mark_as_advanced(JEMALLOC_FOUND JEMALLOC_INCLUDE_DIR JEMALLOC_LIBRARY)
message(STATUS "Jemalloc include path: ${JEMALLOC_INCLUDE_DIR}")

if (JEMALLOC_FOUND AND NOT TARGET JEMALLOC::JEMALLOC)
  add_library(JEMALLOC::JEMALLOC UNKNOWN IMPORTED)
  set_target_properties(JEMALLOC::JEMALLOC PROPERTIES
    IMPORTED_LOCATION "${JEMALLOC_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${JEMALLOC_INCLUDE_DIR}")
endif()
