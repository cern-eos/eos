# Try to find /usr/include/httpd/httpd.h
# Once done, this will define
#
# HTTPD_FOUND        - system has httpd developement header
# HTTPD_INCLUDE_DIRS - httpd include directories

include(FindPackageHandleStandardArgs)

if(HTTPD_INCLUDE_DIRS AND HTTPD_LIBRARIES)
  set(HTTPD_FIND_QUIETLY TRUE)
else()
  find_path(
    HTTPD_INCLUDE_DIR
    NAMES httpd/httpd.h
    HINTS ${HTTPD_ROOT_DIR}
    PATH_SUFFIXES include)

  set(HTTPD_INCLUDE_DIRS ${HTTPD_INCLUDE_DIR})

  find_package_handle_standard_args(
    httpd DEFAULT_MSG HTTPD_INCLUDE_DIR)

  mark_as_advanced(HTTPD_INCLUDE_DIR)
endif()