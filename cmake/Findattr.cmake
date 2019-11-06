# Try to find attr
# Once done, this will define
#
# ATTR_FOUND        - system has attr
# ATTR_INCLUDE_DIRS - attr include directories
# ATTR_LIBRARIES    - libraries needed to use attr
#
# and the following imported target
#
# ATTR::ATTR

find_path(ATTR_INCLUDE_DIR
  NAMES attr/xattr.h sys/xattr.h
  HINTS ${ATTR_ROOT}
  PATH_SUFFIXES include)

find_library(ATTR_LIBRARY
  NAMES attr
  HINTS ${ATTR_ROOT}
  PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(attr
  REQUIRED_VARS ATTR_LIBRARY ATTR_INCLUDE_DIR)

mark_as_advanced(ATTR_FOUND ATTR_LIBRARY ATTR_INCLUDE_DIR)

if (ATTR_FOUND AND NOT TARGET ATTR::ATTR)
  add_library(ATTR::ATTR UNKNOWN IMPORTED)
  set_target_properties(ATTR::ATTR PROPERTIES
    IMPORTED_LOCATION "${ATTR_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${ATTR_INCLUDE_DIR}")
endif()

set(ATTR_INCLUDE_DIRS ${ATTR_INCLUDE_DIR})
set(ATTR_LIBRARIES ${ATTR_LIBRARY})
unset(ATTR_INCLUDE_DIR)
unset(ATTR_LIBRARU)
