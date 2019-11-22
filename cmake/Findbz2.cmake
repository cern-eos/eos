# Try to find bz2
# Once done, this will define
#
# BZ2_FOUND          - system has bz2
# BZ2_INCLUDE_DIRS   - bz2 include directories
# BZ2_LIBRARIES        - bz2 library
#
# and the following imported target
#
# BZ2::BZ2

find_path(BZ2_INCLUDE_DIR
  NAMES bzlib.h
  HINTS ${BZ2_ROOT})

find_library(BZ2_LIBRARY
  NAMES bz2
  HINTS ${BZ2_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(bz2
  DEFAULT_MSG BZ2_LIBRARY BZ2_INCLUDE_DIR)

if (BZ2_FOUND AND NOT TARGET BZ2::BZ2)
  mark_as_advanced(BZ2_FOUND BZ2_LIBRARY BZ2_INCLUDE_DIR)
  add_library(BZ2::BZ2 UNKNOWN IMPORTED)
  set_target_properties(BZ2::BZ2 PROPERTIES
    IMPORTED_LOCATION "${BZ2_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${BZ2_INCLUDE_DIR}")
endif()

set(BZ2_INCLUDE_DIRS ${BZ2_INCLUDE_DIR})
set(BZ2_LIBRARIES ${BZ2_LIBRARY})
unset(BZ2_INCLUDE_DIR)
unset(BZ2_LIBRARY)
