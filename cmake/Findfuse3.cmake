# Try to find fuse (devel)
# Once done, this will define
#
# FUSE3_FOUND - system has fuse
#
# and the following imported target
#
# FUSE3::FUSE3

find_path(FUSE3_INCLUDE_DIR
  NAMES fuse3/fuse_lowlevel.h
  HINTS ${FUSE3_ROOT})

find_library(FUSE3_LIBRARY
  NAMES fuse3
  HINTS ${FUSE3_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(fuse3
  REQUIRED_VARS FUSE3_LIBRARY FUSE3_INCLUDE_DIR)

if (FUSE3_FOUND AND NOT TARGET FUSE3::FUSE3)
  mark_as_advanced(FUSE3_INCLUDE_DIR FUSE3_LIBRARY)
  add_library(FUSE3::FUSE3 UNKNOWN IMPORTED)
  set_target_properties(FUSE3::FUSE3 PROPERTIES
    IMPORTED_LOCATION "${FUSE3_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${FUSE3_INCLUDE_DIR}"
    INTERFACE_COMPILE_DEFINITIONS "USE_FUSE3=1")
else()
  message(WARNING "Notice: fuse3 not found, no fuse3 support")
  add_library(FUSE3::FUSE3 INTERFACE IMPORTED)
endif()

unset(FUSE3_INCLUDE_DIR)
unset(FUSE3_LIBRARY)
