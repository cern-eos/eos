# Try to find ZMQ
# Once done, this will define
#
#  ZMQ_FOUND           - system has ZMQ
#  ZMQ_INCLUDE_DIRS    - ZMQ include directories
#  ZMQ_CPP_INCLUDE_DIR - ZMQ CPP binding i.e. zmq.hpp
#  ZMQ_LIBRARIES       - libraries needed to use ZMQ

include(FindPackageHandleStandardArgs)

if(ZMQ_LIBRARIES AND ZMQ_INCLUDE_DIRS AND ZMQ_CPP_INCLUDE_DIR)
  set(ZMQ_FIND_QUIETLY TRUE)
else()
  find_path(
    ZMQ_INCLUDE_DIR
    NAMES zmq.h
    HINTS ${ZMQ_ROOT_DIR}
    PATH_SUFFIXES include)

  find_path(
    ZMQ_CPP_INCLUDE_DIR
    NAMES zmq.hpp
    HINTS ${ZMQ_ROOT_DIR}
    PATHS ${CMAKE_SOURCE_DIR}/utils
    PATH_SUFFIXES include)

  # Set variable in case we are using our own ZMQ C++ bindings
  if("${ZMQ_CPP_INCLUDE_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}/utils")
    add_definitions(-DHAVE_DEFAULT_ZMQ)
  endif()

  find_library(
    ZMQ_LIBRARY
    NAMES zmq
    HINTS ${ZMQ_ROOT_DIR}
    PATH_SUFFIXES ${LIBRARY_PATH_PREFIX})

  set(ZMQ_INCLUDE_DIRS ${ZMQ_INCLUDE_DIR} ${ZMQ_CPP_INCLUDE_DIR})
  set(ZMQ_LIBRARIES ${ZMQ_LIBRARY})

  find_package_handle_standard_args(
    ZMQ
    DEFAULT_MSG
    ZMQ_LIBRARY ZMQ_INCLUDE_DIR ZMQ_CPP_INCLUDE_DIR)

  mark_as_advanced(ZMQ_LIBRARY ZMQ_INCLUDE_DIR ZMQ_CPP_INCLUDE_DIR)
endif()
