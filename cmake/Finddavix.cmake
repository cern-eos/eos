# - Locate DAVIX library
# Defines:
#
#  DAVIX_FOUND         -  system has davix
#  DAVIX_INCLUDE_DIRS  -  davix include directories
#  DAVIX_LIBRARIES     -  davix libraries

include(FindPackageHandleStandardArgs)

if (DAVIX_INCLUDE_DIRS AND DAVIX_LIBRARIES)
  set(DAVIX_FIND_QUIETLY TRUE)
else()
  find_path(
    DAVIX_INCLUDE_DIR
    NAMES davix.hpp
    HINTS
    /usr ${DAVIX_DIR} $ENV{DAVIX_DIR}
    PATH_SUFFIXES include/davix)

  find_library(
    DAVIX_LIBRARY
    NAMES davix
    HINTS
    /usr ${DAVIX_DIR} $ENV{DAVIX_DIR}
    PATH_SUFFIXES lib lib64)

  set(DAVIX_INCLUDE_DIRS ${DAVIX_INCLUDE_DIR})
  set(DAVIX_LIBRARIES ${DAVIX_LIBRARY})

  find_package_handle_standard_args(
    davix
    DEFAULT_MSG
    DAVIX_LIBRARY DAVIX_INCLUDE_DIR)

  mark_as_advanced(DAVIX_LIBRARY DAVIX_INCLUDE_DIR)
endif()
