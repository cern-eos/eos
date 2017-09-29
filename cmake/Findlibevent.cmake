# Try to find libevent
# Once done, this will define
#
# LIBEVENT_FOUND        - system has libevent
# LIBEVENT_INCLUDE_DIRS - libevent include directories
# LIBEVENT_LIBRARIES    - libraries needed to use libevent

if(LIBEVENT_INCLUDE_DIRS AND LIBEVENT_LIBRARIES)
  set(LIBEVENT_FIND_QUIETLY TRUE)
else()
  find_path(
    LIBEVENT_INCLUDE_DIR
    NAMES event.h
    HINTS ${LIBEVENT_ROOT_DIR} /usr/include/event2
    PATH_SUFFIXES include)

  find_library(
    LIBEVENT_LIBRARY
    NAMES event2 event
    HINTS ${LIBEVENT_ROOT_DIR}
    PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

  set(LIBEVENT_INCLUDE_DIRS ${LIBEVENT_INCLUDE_DIR})
  set(LIBEVENT_LIBRARIES ${LIBEVENT_LIBRARY})

  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(
    libevent DEFAULT_MSG LIBEVENT_LIBRARY LIBEVENT_INCLUDE_DIR)

  mark_as_advanced(LIBEVENT_LIBRARY LIBEVENT_INCLUDE_DIR)
endif()
