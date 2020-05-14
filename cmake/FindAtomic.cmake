# Try to find libatmoic
# Once done, this will define
#
# ATOMIC_FOUND        - system has libatomic
# ATOMIC_LIBRARIES    - libraries needed to use libatomic
#
# and the following imported target
# ATOMIC::ATOMIC

find_library(ATOMIC_LIBRARY
  NAMES atomic atomic.so.1 libatomic.so.1
  HINTS ${ATOMIC_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Atomic
  DEFAULT_MSG ATOMIC_LIBRARY)

if (ATOMIC_FOUND AND NOT TARGET ATOMIC::ATOMIC)
  mark_as_advanced(ATOMIC_LIBRARY)
  add_library(ATOMIC::ATOMIC UNKNOWN IMPORTED)
  set_target_properties(ATOMIC::ATOMIC PROPERTIES
    IMPORTED_LOCATION ${ATOMIC_LIBRARY})
endif()

set(ATOMIC_LIBRARIES ${ATOMIC_LIBRARY})
unset(ATOMIC_LIBRARY)
