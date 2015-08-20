# Try to find glibc-devel
# Once done, this will define
#
# GLIBC_FOUND      - system has glibc-devel
# GLIBC_LIBRARIES  - libraries needed to use glibc
# GLIBC_DL_LIBRARY - libraries needed to use dl
# GLIBC_RT_LIBRARY - libraries needed to use rt

include(FindPackageHandleStandardArgs)

if(GLIBC_LIBRARIES)
  set(GLIBC_FIND_QUIETLY TRUE)
else()
  find_library(
    GLIBC_DL_LIBRARY
    NAMES dl
    HINTS ${GLIBC_ROOT_DIR}
    PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

  find_library(
    GLIBC_RT_LIBRARY
    NAMES rt
    HINTS ${GLIBC_ROOT_DIR}
    PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

  # Math library
  find_library(
    GLIBC_M_LIBRARY
    NAMES m
    HINTS ${GLIBC_ROOT_DIR}
    PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

  set(
    GLIBC_LIBRARIES
    ${GLIBC_M_LIBRARY}
    ${GLIBC_DL_LIBRARY}
    ${GLIBC_RT_LIBRARY})

  find_package_handle_standard_args(
    glibc
    DEFAULT_MSG
    GLIBC_DL_LIBRARY GLIBC_RT_LIBRARY)

  mark_as_advanced(GLIBC_DL_LIBRARY GLIBC_RT_LIBRARY)
endif()
