# Try to find RAMCloud client
# Once done, this will define
#
# RAMCLOUD_FOUND          - system has RAMCloud
# RAMCLOUD_INCLUDE_DIRS   - RAMCloud include directories
# RAMCLOUD_LIBRARY        - RAMCloud library directory

include(FindPackageHandleStandardArgs)

if(RAMCLOUD_INCLUDE_DIRS AND RAMCLOUD_LIBRARIES)
  set(RAMCLOUD_FIND_QUIETLY TRUE)
else()
  find_path(
    RAMCLOUD_INCLUDE_DIR
    NAMES RamCloud.h
    HINTS ${RAMCLOUD_ROOT_DIR}
    PATH_SUFFIXES include/ramcloud)

  find_library(
    RAMCLOUD_LIBRARY
    NAMES ramcloud
    HINTS ${RAMCLOUD_ROOT_DIR}
    PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

  set(RAMCLOUD_INCLUDE_DIRS ${RAMCLOUD_INCLUDE_DIR})
  set(RAMCLOUD_LIBRARIES ${RAMCLOUD_LIBRARY})

  find_package_handle_standard_args(
    RAMCloud
    DEFAULT_MSG
    RAMCLOUD_LIBRARY
    RAMCLOUD_INCLUDE_DIR)

  mark_as_advanced(
    RAMCLOUD_LIBRARY
    RAMCLOUD_INCLUDE_DIR)
endif()
