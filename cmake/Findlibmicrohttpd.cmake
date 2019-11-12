# Try to find /usr/include/microhttpd.h
# Once done, this will define
#
# LIBMICROHTTPD_FOUND        - system has libmicrohttpd developement header
# LIBMICROHTTPD_INCLUDE_DIRS - libmicrohttpd include directories
# LIBMICROHTTPD_LIBRARIES    - libraries needed to use libmicrohttpd
#
# and the following imported targets
#
# LIBMICROTHTTPD::LIBMICROHTTPD

find_package(PkgConfig)
pkg_check_modules(PC_libmicrohttpd QUIET libmicrohttpd)
set(LIBMICROHTTPD_VERSION ${PC_libmicrohttpd_VERSION})

find_path(LIBMICROHTTPD_INCLUDE_DIR
  NAMES microhttpd.h
  HINTS ${LIBMICROHTTPD_ROOT} ${PC_libmicrohttpd_INCLUDEDIR} ${PC_libmicrohttpd_INCLUDE_DIRS}
  PATH_SUFFIXES include)

find_library(LIBMICROHTTPD_LIBRARY
  NAMES microhttpd
  HINTS ${MICROHTTPD_ROOT} ${PC_libmicrohttpd_LIBDIR} ${PC_libmicrohttpd_LIBRARY_DIRS}
  PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(libmicrohttpd
  REQUIRED_VARS LIBMICROHTTPD_LIBRARY LIBMICROHTTPD_INCLUDE_DIR
  VERSION_VAR LIBMICROHTTPD_VERSION)

mark_as_advanced(LIBMICROHTTPD_LIBRARY LIBMICROHTTPD_INCLUDE_DIR)

if (LIBMICROHTTPD_FOUND AND NOT TARGET LIBMICROHTTPD::LIBMICROHTTPD)
  add_library(LIBMICROHTTPD::LIBMICROHTTPD UNKNOWN IMPORTED)
  set_target_properties(LIBMICROHTTPD::LIBMICROHTTPD PROPERTIES
    IMPORTED_LOCATION "${LIBMICROHTTPD_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${LIBMICROHTTPD_INCLUDE_DIR}"
    INTERFACE_COMPILE_DEFINITIONS "EOS_MICRO_HTTPD=1")
else ()
  message (STATUS "LibMicroHttpd not found, no httpd access available.")
  add_library(LIBMICROHTTPD::LIBMICROHTTPD UNKNOWN IMPORTED)
endif()

set(LIBMICROHTTPD_INCLUDE_DIRS ${LIBMICROHTTPD_INCLUDE_DIR})
set(LIBMICROHTTPD_LIBRARIES ${LIBMICROHTTPD_LIBRARY})
unset(LIBMICROHTTPD_INCLUDE_DIR)
unset(LIBMICROHTTPD_LIBRARY)
