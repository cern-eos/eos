# Try to find lz4
# Once done, this will define
#
# LZ4_FOUND          - system has bz2
# LZ4_INCLUDE_DIRS   - bz2 include directories
# LZ4_LIBRARIES        - bz2 library
#
# and the following imported target
#
# LZ4::LZ4

find_library(LZ4_LIBRARY
  NAMES lz4 lz4_static
  HINTS ${LZ4_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(lz4 DEFAULT_MSG LZ4_LIBRARY)

if (LZ4_FOUND AND NOT TARGET LZ4::LZ4)
  mark_as_advanced(LZ4_FOUND LZ4_LIBRARY)
  add_library(LZ4::LZ4 UNKNOWN IMPORTED)
  set_target_properties(LZ4::LZ4 PROPERTIES
    IMPORTED_LOCATION "${LZ4_LIBRARY}")
endif()

unset(LZ4_LIBRARY)
