# Try to find /usr/include/httpd/httpd.h
# Once done, this will define
#
# HTTPD_FOUND - system has httpd developement header
# HTTPD_INCLUDE_DIRS - the httpd include directories

if(HTTPD_INCLUDE_DIRS AND HTTPD_LIBRARIES)
set(HTTPD_FIND_QUIETLY TRUE)
endif(HTTPD_INCLUDE_DIRS AND HTTPD_LIBRARIES)

find_path(HTTPD_INCLUDE_DIR httpd/httpd.h)

set(HTTPD_INCLUDE_DIRS ${HTTPD_INCLUDE_DIR})

# handle the QUIETLY and REQUIRED arguments and set HTTPD_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(httpd DEFAULT_MSG HTTPD_INCLUDE_DIR )

mark_as_advanced(HTTPD_INCLUDE_DIR)
