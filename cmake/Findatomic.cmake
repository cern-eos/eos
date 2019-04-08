# Try to find libatmoic
# Once done, this will define
#
# ATOMIC_FOUND        - system has libatomic
# ATOMIC_LIBRARIES    - libraries needed to use libatomic

include(FindPackageHandleStandardArgs)

if(ATOMIC_INCLUDE_DIRS AND ATOMIC_LIBRARIES)
  set(ATOMIC_FIND_QUIETLY TRUE)
else()
  find_library(
    ATOMIC_LIBRARY
    NAMES atomic atomic.so.1 libatomic.so.1
    HINTS ${ATOMIC_ROOT_DIR}
    PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

  set(ATOMIC_LIBRARIES ${ATOMIC_LIBRARY})

  find_package_handle_standard_args(
    atomic 
    DEFAULT_MSG 
    ATOMIC_LIBRARY)

  mark_as_advanced(ATOMIC_LIBRARY)
endif()
