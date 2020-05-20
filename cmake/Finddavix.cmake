# - Locate DAVIX library
# Defines:
#
# DAVIX_FOUND         -  system has davix
#
# and the following imported targets
#
# DAVIX::DAVIX

find_path(DAVIX_INCLUDE_DIR
  NAMES davix.hpp
  HINTS
  /usr ${DAVIX_ROOT} $ENV{DAVIX_ROOT}
  PATH_SUFFIXES include/davix)

find_library(DAVIX_LIBRARY
  NAMES davix
  HINTS /usr ${DAVIX_ROOT} $ENV{DAVIX_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(davix
  REQUIRED_VARS DAVIX_LIBRARY DAVIX_INCLUDE_DIR)
mark_as_advanced(DAVIX_FOUND DAVIX_LIBRARY DAVIX_INCLUDE_DIR)

if (DAVIX_FOUND AND NOT TARGET DAVIX::DAVIX)
  add_library(DAVIX::DAVIX UNKNOWN IMPORTED)
  set_target_properties(DAVIX::DAVIX PROPERTIES
    IMPORTED_LOCATION "${DAVIX_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${DAVIX_INCLUDE_DIR}"
    INTERFACE_COMPILE_DEFINITIONS HAVE_DAVIX)
else()
  message(WARNING "Notice: davix not found, no davix support")
  add_library(DAVIX::DAVIX INTERFACE IMPORTED)
endif()

unset(DAVIX_LIBRARY)
unset(DAVIX_INCLUDE_DIR)
