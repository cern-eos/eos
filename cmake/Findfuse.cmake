# Try to find fuse (devel)
# Once done, this will define
#
# FUSE_FOUND        - system has fuse
# FUSE_INCLUDE_DIRS - fuse include directories
# FUSE_LIBRARIES    - libraries need to use fuse
# FUSE_MOUNT_VERSION - major version reported by fusermount 

include(FindPackageHandleStandardArgs)

if(FUSE_INCLUDE_DIRS AND FUSE_LIBRARIES)
  set(FUSE_FIND_QUIETLY TRUE)
else()
  find_path(
    FUSE_INCLUDE_DIR
    NAMES fuse/fuse_lowlevel.h
    HINTS ${FUSE_ROOT_DIR}
    PATH_SUFFIXES include include/osxfuse)

  if(MacOSX)
    find_library(
      FUSE_LIBRARY
      NAMES osxfuse
      HINTS ${FUSE_ROOT_DIR}
      PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})
  else(MacOSX)
    find_library(
      FUSE_LIBRARY
      NAMES fuse
      HINTS ${FUSE_ROOT_DIR}
      PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

     execute_process(
      COMMAND sh -c "fusermount --version | cut -d ' ' -f 3 | cut -d '.' -f 1,2 | sed s/'\\.'//g"
      OUTPUT_VARIABLE FUSE_MOUNT_VERSION
      OUTPUT_STRIP_TRAILING_WHITESPACE
      RESULT_VARIABLE RETC)
      if(NOT ("${RETC}" STREQUAL "0") )
        set(${FUSE_MOUNT_VERSION} "" PARENT_SCOPE)
      endif()
      message(STATUS "Setting FUSE_MOUNT_VERSION: ${FUSE_MOUNT_VERSION}")
  endif(MacOSX)

  set(FUSE_INCLUDE_DIRS ${FUSE_INCLUDE_DIR})
  set(FUSE_LIBRARIES ${FUSE_LIBRARY})

  find_package_handle_standard_args(
    fuse
    DEFAULT_MSG FUSE_INCLUDE_DIR FUSE_LIBRARY)

  mark_as_advanced(FUSE_INCLUDE_DIR FUSE_LIBRARY)
endif()
