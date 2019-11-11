# Try to find ZMQ
# Once done, this will define
#
#  ZMQ_FOUND           - system has ZMQ
#  ZMQ_INCLUDE_DIRS    - ZMQ include directories
#  ZMQ_CPP_INCLUDE_DIR - ZMQ CPP binding i.e. zmq.hpp
#  ZMQ_LIBRARIES       - libraries needed to use ZMQ
#
# and the following imported targets
#
# ZMQ::ZMQ

find_path(ZMQ_INCLUDE_DIR
  NAMES zmq.h
  HINTS ${ZMQ_ROOT}
  PATH_SUFFIXES include)

find_path(ZMQ_CPP_INCLUDE_DIR
  NAMES zmq.hpp
  HINTS ${ZMQ_ROOT}
  PATHS ${CMAKE_SOURCE_DIR}/utils
  PATH_SUFFIXES include)

find_library(ZMQ_LIBRARY
  NAMES zmq
  HINTS ${ZMQ_ROOT}
  PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ZMQ
  REQUIRED_VARS ZMQ_LIBRARY ZMQ_INCLUDE_DIR ZMQ_CPP_INCLUDE_DIR)
mark_as_advanced(ZMQ_LIBRARY ZMQ_INCLUDE_DIR ZMQ_CPP_INCLUDE_DIR)

if (ZMQ_FOUND AND NOT TARGET ZMQ::ZMQ)
  add_library(ZMQ::ZMQ UNKNOWN IMPORTED)
  set_target_properties(ZMQ::ZMQ PROPERTIES
    IMPORTED_LOCATION "${ZMQ_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${ZMQ_INCLUDE_DIR};${ZMQ_CPP_INCLUDE_DIR}")

  message(STATUS "ZMQ_CPP_INCLUDE_DIR=${ZMQ_CPP_INCLUDE_DIR}")

  # Set variable in case we are using our own ZMQ C++ bindings
  if(NOT "${ZMQ_CPP_INCLUDE_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}/utils")
    set_target_properties(ZMQ::ZMQ PROPERTIES
      INTERFACE_COMPILE_DEFINITIONS HAVE_DEFAULT_ZMQ)
  endif()
endif()

set(ZMQ_INCLUDE_DIRS ${ZMQ_CPP_INCLUDE_DIR})
set(ZMQ_LIBRARIES ${ZMQ_LIBRARY})
unset(ZMQ_CPP_INCLUDE_DIR)
unset(ZMQ_INCLUDE_DIR)
unset(ZMQ_LIBRARY)
