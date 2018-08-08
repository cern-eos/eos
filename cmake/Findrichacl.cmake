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
    RICHACL_LIB
    NAMES richacl
    HINTS ${RICHACL_ROOT_DIR}
    PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

  if(RICHACL_LIB)
    message(STATUS "RICHACL_LIB is set to ${RICHACL_LIB}")
  else()
    message(STATUS "RICHACL_LIB is not set")
  endif()

  if(RICHACL_LIB)
    set(RICHACL_LIBRARIES ${RICHACL_LIB})
  else()
    set(RICHACL_LIBRARIES "")
  endif()

  if(RICHACL_INCLUDE_DIR)
    set(RICHACL_INCLUDE_DIRS ${RICHACL_INCLUDE_DIR})
  else()
    set(RICHACL_INCLUDE_DIRS "")
  endif()

  find_package_handle_standard_args(
    richacl
    DEFAULT_MSG
    RICHACL_LIB RICHACL_INCLUDE_DIR)

  ## mark_as_advanced(RICHACL_LIBRARIES RICHACL_INCLUDE_DIR)

  if(RICHACL_FOUND)
      add_definitions(-DRICHACL_FOUND)
  endif()
endif()
