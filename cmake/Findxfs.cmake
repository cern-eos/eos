# Try to find xfs
# Once done, this will define
#
# XFS_FOUND        - system has xfs
# XFS_INCLUDE_DIRS - xfs include directories
#
# and the following imported target
#
# XFS::XFS

find_path(XFS_INCLUDE_DIR
  NAMES xfs/xfs.h
  HINTS ${XFS_ROOT}
  PATH_SUFFIXES include)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(xfs
  DEFAULT_MSG XFS_INCLUDE_DIR)
mark_as_advanced(XFS_INCLUDE_DIR)

if (XFS_FOUND AND NOT TARGET XFS::XFS)
  add_library(XFS::XFS INTERFACE IMPORTED)
  set_target_properties(XFS::XFS PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${XFS_INCLUDE_DIR}")
endif()

set(XFS_INCLUDE_DIRS ${XFS_INCLUDE_DIR})
unset(XFS_INCLUDE_DIR)
