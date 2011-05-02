# Try to find fuse (devel)
# Once done, this will define
#
# FUSE_FOUND - system has fuse
# FUSE_INCLUDE_DIRS - the fuse include directories
# FUSE_LIBRARIES - fuse libraries directories

if(FUSE_INCLUDE_DIRS AND FUSE_LIBRARIES)
set(FUSE_FIND_QUIETLY TRUE)
endif(FUSE_INCLUDE_DIRS AND FUSE_LIBRARIES)

find_path(FUSE_INCLUDE_DIR fuse/fuse_lowlevel.h)
find_library(FUSE_LIBRARY fuse)

set(FUSE_INCLUDE_DIRS ${FUSE_INCLUDE_DIR})
set(FUSE_LIBRARIES ${FUSE_LIBRARY})

# handle the QUIETLY and REQUIRED arguments and set FUSE_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(fuse DEFAULT_MSG FUSE_INCLUDE_DIR FUSE_LIBRARY)

mark_as_advanced(FUSE_INCLUDE_DIR FUSE_LIBRARY)
