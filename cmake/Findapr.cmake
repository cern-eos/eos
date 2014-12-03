# Try to find /usr/include/httpd/httpd.h
# Once done, this will define
#
# APR_FOUND        - system has httpd developement header
# APR_INCLUDE_DIRS - httpd include directories

if(APR_INCLUDE_DIRS AND APR_LIBRARIES)
set(APR_FIND_QUIETLY TRUE)
endif(APR_INCLUDE_DIRS AND APR_LIBRARIES)

find_path(APR_INCLUDE_DIR apr-1/apr_strings.h)

set(APR_INCLUDE_DIRS ${APR_INCLUDE_DIR})

# handle the QUIETLY and REQUIRED arguments and set APR_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(apr DEFAULT_MSG APR_INCLUDE_DIR )

mark_as_advanced(APR_INCLUDE_DIR)
