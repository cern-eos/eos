# - Locate RADOS library
# Defines:
#
#  RADOS_FOUND
#  RADOS_INCLUDE_DIR
#  RADOS_INCLUDE_DIRS (not cached)
#  RADOS_LIBRARIES

find_path(RADOS_INCLUDE_DIR NAMES librados.h PATH_SUFFIXES rados HINTS ${RADOS_DIR}/include $ENV{RADOS_DIR}/include)
find_library(RADOS_LIBRARY NAMES rados HINTS ${RADOS_DIR}/lib $ENV{RADOS_DIR}/lib64 ${RADOS_DIR}/lib $ENV{RADOS_DIR}/lib64)

set(RADOS_INCLUDE_DIRS ${RADOS_INCLUDE_DIR})
set(RADOS_LIBRARIES ${RADOS_LIBRARY})

# Handle the QUIETLY and REQUIRED arguments and set RADOS_FOUND to TRUE if all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(RADOS DEFAULT_MSG RADOS_INCLUDE_DIRS RADOS_LIBRARIES)

mark_as_advanced(RADOS_FOUND RADOS_INCLUDE_DIRS RADOS_LIBRARIES)
