# Try to find hiredis
# Once done, this will define
#
# HIREDIS_FOUND        - system has hiredis
# HIREDIS_INCLUDE_DIRS - hiredis include directories
# HIREDIS_LIBRARIES    - libraries need to use hiredis
#
# and the following imported target
#
# HIREDIS::HIREDIS

find_path(HIREDIS_INCLUDE_DIR
  NAMES hiredis/hiredis.h
  HINTS ${HIREDIS_ROOT})

find_library(HIREDIS_LIBRARY
  NAMES hiredis
  HINTS ${HIREDIS_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(hiredis
  DEFAULT_MSG HIREDIS_LIBRARY HIREDIS_INCLUDE_DIR)

if (HIREDIS_FOUND AND NOT TARGET HIREDIS::HIREDIS)
  mark_as_advanced(HIREDIS_LIBRARY HIREDIS_INCLUDE_DIR)
  add_library(HIREDIS::HIREDIS UNKNOWN IMPORTED)
  set_target_properties(HIREDIS::HIREDIS PROPERTIES
    IMPORTED_LOCATION "${HIREDIS_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${HIREDIS_INCLUDE_DIR}"
    INTERFACE_COMPILE_DEFINITIONS HAVE_HIREDIS)
endif()

set(HIREDIS_INCLUDE_DIRS ${HIREDIS_INCLUDE_DIR})
set(HIREDIS_LIBRARIES ${HIREDIS_LIBRARY})
unset(HIREDIS_INCLUDE_DIR)
unset(HIREDIS_LIBRARY)
