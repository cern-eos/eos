# - Find LibEvent (a cross event library)
# This module defines
# LIBEVENT_INCLUDE_DIR - the LibEvent include directory
# LBEVENT_LIB_DIR      - the LibEvent library directory
# LIBEVENT_LIB         - the LibEvent library name
# LIBEVENT_FOUND       - system has LibEvent 
#
# LIBEVENT_DIR may be defined as a hint for where to look

find_path(
  LIBEVENT_INCLUDE_DIR
  event2/event.h
  HINTS
  ${LIBEVENT_DIR}
  $ENV{LIBEVENT_DIR}
  /usr
  /opt
  PATH_SUFFIXES include )

find_library(
  LIBEVENT_LIB
  NAMES event
  HINTS
  ${LIBEVENT_DIR}
  $ENV{LIBEVENT_DIR}
  /usr
  /opt
  /usr/local
  PATH_SUFFIXES lib )

GET_FILENAME_COMPONENT( LIBEVENT_LIB_DIR ${LIBEVENT_LIB} PATH )

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LibEvent DEFAULT_MSG LIBEVENT_LIB_DIR LIBEVENT_INCLUDE_DIR)
