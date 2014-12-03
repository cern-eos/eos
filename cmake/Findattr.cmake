# Try to find attr
# Once done, this will define
#
# ATTR_FOUND        - system has attr
# ATTR_INCLUDE_DIRS - attr include directories
# ATTR_LIBRARIES    - libraries needed to use attr

include(FindPackageHandleStandardArgs)

if(ATTR_INCLUDE_DIRS AND ATTR_LIBRARIES)
  set(ATTR_FIND_QUIETLY TRUE)
else()
  find_path(
    ATTR_INCLUDE_DIR
    NAMES attr/xattr.h
    HINTS ${ATTR_ROOT_DIR}
    PATH_SUFFIXES include)

  find_library(
    ATTR_LIBRARY
    NAMES attr
    HINTS ${ATTR_ROOT_DIR}
    PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

  set(ATTR_INCLUDE_DIRS ${ATTR_INCLUDE_DIR})
  set(ATTR_LIBRARIES ${ATTR_LIBRARY})

  find_package_handle_standard_args(
    attr DEFAULT_MSG ATTR_LIBRARY ATTR_INCLUDE_DIR)

  mark_as_advanced(ATTR_LIBRARY ATTR_INCLUDE_DIR)
endif()
