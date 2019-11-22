# Try to find libevent
# Once done, this will define
#
# LIBEVENT_FOUND        - system has libevent
# LIBEVENT_INCLUDE_DIRS - libevent include directories
# LIBEVENT_LIBRARIES    - libraries needed to use libevent
#
# and the following imported targets
#
# LIBEVENT::LIBEVENT

find_package(PkgConfig)
pkg_check_modules(PC_libevent QUIET libevent)
set(LIBEVENT_VERSION ${PC_libevent_VERSION})

find_path(LIBEVENT_INCLUDE_DIR
  NAMES event.h
  HINTS ${LIBEVENT_ROOT} ${PC_libevent_INCLUDEDIR} ${PC_libevent_INCLUDE_DIRS} /usr/include/event2)

find_library(
  LIBEVENT_LIBRARY
  NAMES event2 event
  HINTS ${LIBEVENT_ROOT} ${PC_libevent_LIBDIR} ${PC_libevent_LIBRARY_DIRS}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(libevent
  REQUIRED_VARS LIBEVENT_LIBRARY LIBEVENT_INCLUDE_DIR)

if (LIBEVENT_FOUND AND NOT TARGET LIBEVENT::LIBEVENT)
  mark_as_advanced(LIBEVENT_FOUND LIBEVENT_INCLUDE_DIR LIBEVENT_LIBRARY)
  add_library(LIBEVENT::LIBEVENT UNKNOWN IMPORTED)
  set_target_properties(LIBEVENT::LIBEVENT PROPERTIES
    IMPORTED_LOCATION "${LIBEVENT_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${LIBEVENT_INCLUDE_DIR}")
endif()

set(LIBEVENT_INCLUDE_DIRS ${LIBEVENT_INCLUDE_DIR})
set(LIBEVENT_LIBRARIES ${LIBEVENT_LIBRARY})
unset(LIBEVENT_INCLUDE_DIR)
unset(LIBEVENT_LIBRARY)
