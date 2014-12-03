# Try to find xfs
# Once done, this will define
#
# XFS_FOUND        - system has xfs
# XFS_INCLUDE_DIRS - xfs include directories

include(FindPackageHandleStandardArgs)

if(XFS_INCLUDE_DIRS)
  set(XFS_FIND_QUIETLY TRUE)
end()
  find_path(
    XFS_INCLUDE_DIR
    NAMES xfs/xfs.h
    HINTS ${XFS_ROOT_DIR}
    PATH_SUFFIXES include)

  set(XFS_INCLUDE_DIRS ${XFS_INCLUDE_DIR})

  find_package_handle_standard_args(
    xfs DEFAULT_MSG XFS_INCLUDE_DIR)

  mark_as_advanced(XFS_INCLUDE_DIR)
endif()
