# Try to find glibc-devel
# Once done, this will define
#
# GLIBC_FOUND      - system has glibc-devel
# and the following imported targets
#
# GLIBC::DL
# GLIBC::RT

find_library(GLIBC_DL_LIBRARY
  NAMES dl
  HINTS ${GLIBC_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

find_library(GLIBC_RT_LIBRARY
  NAMES rt
  HINTS ${GLIBC_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

# Math library
find_library(GLIBC_M_LIBRARY
  NAMES m
  HINTS ${GLIBC_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(glibc
  REQUIRED_VARS GLIBC_DL_LIBRARY GLIBC_RT_LIBRARY GLIBC_M_LIBRARY)
mark_as_advanced(GLIBC_DL_LIBRARY GLIBC_RT_LIBRARY GLIBC_M_LIBRARY)

if (GLIBC_FOUND AND NOT TARGET GLIBC::DL)
  add_library(GLIBC::DL UNKNOWN IMPORTED)
  set_target_properties(GLIBC::DL PROPERTIES
    IMPORTED_LOCATION "${GLIBC_DL_LIBRARY}")
  add_library(GLIBC::RT UNKNOWN IMPORTED)
  set_target_properties(GLIBC::RT PROPERTIES
    IMPORTED_LOCATION "${GLIBC_RT_LIBRARY}")
  add_library(GLIBC::M UNKNOWN IMPORTED)
  set_target_properties(GLIBC::M PROPERTIES
    IMPORTED_LOCATION "${GLIBC_M_LIBRARY}")
endif()

unset(GLIBC_DL_LIBRARY)
unset(GLIBC_RT_LIBRARY)
unset(GLIBC_M_LIBRARY)
