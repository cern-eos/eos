# Try to find procps
# Once done, this will define
#
# PROCPS_FOUND        - system has uuid
# PROCPS_INCLUDE_DIRS - uuid include directories
# PROCPS_LIBRARIES    - libraries needed to use uuid
#
# and the following imported target
#
# PROCPS::PROCPS

find_package(PkgConfig)
pkg_check_modules(PC_procps QUIET libprocps)
set(PROCPS_VERSION ${PC_procps_VERSION})

find_path(PROCPS_INCLUDE_DIR
  NAMES readproc.h
  HINTS ${PROCPS_ROOT} ${PC_procps_INCLUDEDIR} ${PC_procps_INCLUDE_DIRS}
  PATH_SUFFIXES include proc)

find_library(PROCPS_LIBRARY
  NAMES procps
  HINTS ${PROCPS_ROOT} ${PC_procps_LIBDIR} ${PC_procps_LIBRARY_DIRS}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(procps
  REQUIRED_VARS PROCPS_LIBRARY PROCPS_INCLUDE_DIR
  VERSION_VAR PROCPS_VERSION)

if (PROCPS_FOUND AND NOT TARGET PROCPS::PROCPS)
  mark_as_advanced(PROCPS_FOUND PROCPS_INCLUDE_DIR PROCPS_LIBRARY)
  add_library(PROCPS::PROCPS UNKNOWN IMPORTED)
  set_target_properties(PROCPS::PROCPS PROPERTIES
    IMPORTED_LOCATION "${PROCPS_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${PROCPS_INCLUDE_DIR}")
endif()

unset(PROCPS_INCLUDE_DIR)
unset(PROCPS_LIBRARY)
