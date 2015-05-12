# Try to find xfs.h
# Once done, this will define
#
# XFS_FOUND - system has xfs.h
# XFS_INCLUDE_DIR - the xfs include directories

if(XFS_INCLUDE_DIR)
set(XFS_FIND_QUIETLY TRUE)
endif(XFS_INCLUDE_DIR)

find_path( XFS_INCLUDE_DIR xfs/xfs.h
  HINTS
  ${XFS_DIR}
  /usr
  PATH_SUFFIXES include)

# handle the QUIETLY and REQUIRED arguments and set XFS_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(SparseHash DEFAULT_MSG XFS_INCLUDE_DIR)

mark_as_advanced(XFS_INCLUDE_DIR)
