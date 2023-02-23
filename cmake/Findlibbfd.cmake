# Try to find libbfd
# Once done, this will define
#
# LIBBFD_FOUND               - system has libbfd
# LIBBFD_INCLUDE_DIRS        - libbfd include directories
# LIBBFD_LIBRARIES           - libbfd library
#
# LIBBFD_ROOT_DIR may be defined as a hint for where to look

find_path(LIBBFD_INCLUDE_DIR
  NAMES bfd.h
  HINTS /opt/rh/devtoolset-8/root ${LIBBFD_ROOT}
  PATH_SUFFIXES include usr/include)

find_library(LIBBFD_LIBRARY
  NAMES bfd
  HINTS /opt/rh/devtoolset-8/root ${LIBBFD_ROOT}
  PATH_SUFFIXES lib lib64)

find_library(LIBIBERTY_LIBRARY
  NAMES iberty
  HINTS /opt/rh/devtoolset-8/root ${LIBBFD_ROOT}
  PATH_SUFFIXES lib lib64)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(libbfd
  REQUIRE_VARS LIBBFD_LIBRARY LIBIBERTY_LIBRARY LIBBFD_INCLUDE_DIR)

if (LIBBFD_FOUND AND NOT TARGET LIBBFD::LIBBFD)
  add_library(LIBBFD::LIBBFD UNKNOWN IMPORTED)
  set_target_properties(LIBBFD::LIBBFD PROPERTIES
    IMPORTED_LOCATION "${LIBBFD_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${LIBBFD_INCLUDE_DIR}")
  add_library(LIBBFD::IBERTY UNKNOWN IMPORTED)
  set_target_properties(LIBBFD::IBERTY PROPERTIES
    IMPORTED_LOCATION "${LIBIBERTY_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${LIBBFD_INCLUDE_DIR}")
else()
  message(WARNING "Notice: libbfd not found, no libbfd support")
  add_library(LIBBFD::LIBBFD INTERFACE IMPORTED)
  add_library(LIBBFD::IBERTY INTERFACE IMPORTED)
endif()

unset(LIBBFD_INCLUDE_DIR)
unset(LIBBFD_LIBRARY)
unset(LIBIBERTY_LIBRARY)
