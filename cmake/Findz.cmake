# Try to find zlib
# Once done, this will define
#
# ZLIB_FOUND - system has zlib
# ZLIB_INCLUDE_DIRS - the zlib include directories
# ZLIB_LIBRARIES - zlib libraries directories

if(Z_INCLUDE_DIRS AND Z_LIBRARIES)
set(Z_FIND_QUIETLY TRUE)
endif(Z_INCLUDE_DIRS AND Z_LIBRARIES)

find_path(Z_INCLUDE_DIR zlib.h)
find_library(Z_LIBRARY z)

set(Z_INCLUDE_DIRS ${Z_INCLUDE_DIR})
set(Z_LIBRARIES ${Z_LIBRARY})

# handle the QUIETLY and REQUIRED arguments and set Z_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(z DEFAULT_MSG Z_INCLUDE_DIR Z_LIBRARY)

mark_as_advanced(Z_INCLUDE_DIR Z_LIBRARY)
