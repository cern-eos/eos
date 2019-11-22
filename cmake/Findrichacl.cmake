# Try to find richacl
# Once done, this will define
#
# RICHACL_FOUND         - system has richacl
# RICHRACL_INCLUDE_DIRS - richacl include directories
# RICHACL_LIBRARIES     - libraries needed to use richacl

include(FindPackageHandleStandardArgs)

if(RICHACL_INCLUDE_DIRS AND RICHACL_LIBRARIES)
  set(RICHACL_FIND_QUIETLY TRUE)
else()
  find_path(
    RICHACL_INCLUDE_DIR
    NAMES sys/richacl.h
    HINTS ${RICHACL_ROOT_DIR}
    PATH_SUFFIXES include)

  find_library(
    RICHACL_LIBRARY
    NAMES richacl
    HINTS ${RICHACL_ROOT_DIR}
    PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

  set(RICHACL_INCLUDE_DIRS ${RICHACL_INCLUDE_DIR})
  set(RICHACL_LIBRARIES ${RICHACL_LIBRARY})

  if(RICHACL_FOUND)
    add_definitions(-DRICHACL_FOUND)
  endif()

  find_package_handle_standard_args(
    richacl
    DEFAULT_MSG
    RICHACL_LIBRARY RICHACL_INCLUDE_DIR)

  mark_as_advanced(RICHACL_LIBRARIES RICHACL_INCLUDE_DIR)
endif()
