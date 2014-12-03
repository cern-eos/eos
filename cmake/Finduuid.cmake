# Try to find uuid
# Once done, this will define
#
# UUID_FOUND        - system has uuid
# UUID_INCLUDE_DIRS - uuid include directories
# UUID_LIBRARIES    - libraries needed to use uuid

include(FindPackageHandleStandardArgs)

if(UUID_INCLUDE_DIRS AND UUID_LIBRARIES)
  set(UUID_FIND_QUIETLY TRUE)
else()
  find_path(
    UUID_INCLUDE_DIR
    NAMES uuid.h
    HINTS ${UUID_ROOT_DIR}
    PATH_SUFFIXES include)

  find_library(
    UUID_LIBRARY
    NAMES uuid
    HINTS ${UUID_ROOT_DIR}
    PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

  set(UUID_INCLUDE_DIRS ${UUID_INCLUDE_DIR})
  set(UUID_LIBRARIES ${UUID_LIBRARY})

  find_package_handle_standard_args(
    uuid DEFAULT_MSG UUID_LIBRARY UUID_INCLUDE_DIR)

  mark_as_advanced(UUID_INCLUDE_DIR UUID_LIBRARY)
endif()