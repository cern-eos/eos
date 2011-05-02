# Try to find attr
# Once done, this will define
#
# ATTR_FOUND - system has attr
# ATTR_INCLUDE_DIRS - the attr include directories
# ATTR_LIBRARIES - attr libraries directories

if(ATTR_INCLUDE_DIRS AND ATTR_LIBRARIES)
set(ATTR_FIND_QUIETLY TRUE)
endif(ATTR_INCLUDE_DIRS AND ATTR_LIBRARIES)

find_path(ATTR_INCLUDE_DIR attr/xattr.h)
find_library(ATTR_LIBRARY attr)

set(ATTR_INCLUDE_DIRS ${ATTR_INCLUDE_DIR})
set(ATTR_LIBRARIES ${ATTR_LIBRARY})

# handle the QUIETLY and REQUIRED arguments and set ATTR_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(attr DEFAULT_MSG ATTR_INCLUDE_DIR ATTR_LIBRARY)

mark_as_advanced(ATTR_INCLUDE_DIR ATTR_LIBRARY)
