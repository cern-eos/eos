# Try to find DAVIX library
# Once done, this will define
#
# DAVIX_FOUND        - system has DAVIX library
# DAVIX_INCLUDE_DIRS - the DAVIX include directories
# DAVIX_LIBRARIES    - DAVIX libraries

if(DAVIX_INCLUDE_DIRS AND DAVIX_LIBRARIES)
set(DAVIX_FIND_QUIETLY TRUE)
endif(DAVIX_INCLUDE_DIRS AND DAVIX_LIBRARIES)

find_path(DAVIX_INCLUDE_DIR davix.hpp
                    HINTS
                    /usr/include/davix/
                    /usr/local/include/davix/
                    )

find_library(DAVIX_LIBRARY davix
                    PATHS /usr/ /usr/local/
                    PATH_SUFFIXES lib lib64
                    )

set(DAVIX_INCLUDE_DIRS ${DAVIX_INCLUDE_DIR})
set(DAVIX_LIBRARIES ${DAVIX_LIBRARY})

# handle the QUIETLY and REQUIRED arguments and set DAVIX_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(davix DEFAULT_MSG DAVIX_INCLUDE_DIRS DAVIX_LIBRARIES)

mark_as_advanced(DAVIX_INCLUDE_DIRS DAVIX_LIBRARIES)
