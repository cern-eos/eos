# Try to find richacl
# Once done, this will define
#
# RICHACL_FOUND         - system has richacl
#
# and the following targets
#
# RICHACL::RICHACL

find_package(PkgConfig)
pkg_check_modules(PC_richacl QUIET librichacl)
set(RICHACL_VERSION ${PC_richacl_VERSION})

find_path(RICHACL_INCLUDE_DIR
  NAMES sys/richacl.h
  HINTS ${RICHACL_ROOT}
  PATH_SUFFIXES include)

find_library(RICHACL_LIBRARY
  NAMES richacl
  HINTS ${RICHACL_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(richacl
  REQUIRED_VARS RICHACL_LIBRARY RICHACL_INCLUDE_DIR
  VERSION_VAR RICHACL_VERSION)

mark_as_advanced(RICHACL_FOUND RICHACL_LIBRARY RICHACL_INCLUDE_DIR)

if (RICHACL_FOUND AND NOT TARGET RICHACL::RICHACL)
  add_library(RICHACL::RICHACL UNKNOWN IMPORTED)
  set_target_properties(RICHACL::RICHACL PROPERTIES
    IMPORTED_LOCATION "${RICHACL_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${RICHACL_INCLUDE_DIR}")
else()
  message ("Notice: librichacl not found, no richacl support")
  add_library(RICHACL::RICHACL INTERFACE IMPORTED)
endif()

unset(RICHACL_INCLUDE_DIR)
unset(RICHACL_LIBRARY)
