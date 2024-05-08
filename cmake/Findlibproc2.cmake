#.rst:
# Findlibproc2
# -------
#
# Find libproc2 from procps 4.x.
#
# Imported Targets
# ^^^^^^^^^^^^^^^^
#
# This module defines :prop_tgt:`IMPORTED` target:
#
# ``procps::libproc2``
#   The libproc2 library, if found.
#
# Result Variables
# ^^^^^^^^^^^^^^^^
#
# This module will set the following variables in your project:
#
# ``LIBPROC2_FOUND``
#   True if libproc2 has been found.
# ``LIBPROC2_INCLUDE_DIRS``
#   Where to find libproc2/misc.h, etc.
# ``LIBPROC2_LIBRARIES``
#   The libraries to link against to use libproc2.
# ``LIBPROC2_VERSION``
#   The version of the libproc2 library found (e.g. 4.0.2)
#

find_package(PkgConfig QUIET)

if(PKG_CONFIG_FOUND)
  if(${libproc2_FIND_REQUIRED})
    set(LIBPROC2_REQUIRED REQUIRED)
  endif()

  if(NOT DEFINED LIBPROC2_FIND_VERSION)
    pkg_check_modules(LIBPROC2 ${LIBPROC2_REQUIRED} libproc2)
  else()
    pkg_check_modules(LIBPROC2 ${LIBPROC2_REQUIRED} libproc2>=${LIBPROC2_FIND_VERSION})
  endif()

  set(LIBPROC2_LIBRARIES ${LIBPROC2_LDFLAGS})
  set(LIBPROC2_LIBRARY ${LIBPROC2_LIBRARIES})
  set(LIBPROC2_INCLUDE_DIRS ${LIBPROC2_INCLUDE_DIRS})
  set(LIBPROC2_INCLUDE_DIR ${LIBPROC2_INCLUDE_DIRS})
else ()
  if (DEFINED LIBPROC2_FIND_VERSION)
    find_program(PS_BIN ps REQUIRED)
    message(DEBUG "info: found ps binary in ${PS_BIN}")

    if (PS_BIN)
      execute_process(COMMAND sh -c "${PS_BIN} -V | cut -d ' ' -f 4"
        OUTPUT_VARIABLE LIBPROC2_VER
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE RETC)

      if (NOT ("${RETC}" STREQUAL "0"))
        message(FATAL_ERROR "error: failed while calling ${PS_BIN} to get version")
      endif()

      message(DEBUG "info: ps version is ${LIBPROC2_VER}")

      if (NOT "${LIBPROC2_VER}" VERSION_GREATER_EQUAL "${LIBPROC2_FIND_VERSION}")
        message(FATAL_ERROR
          "error: procps version ${LIBPROC2_VER} less than "
          "requested ${LIBPROC_FIND_VERSION}")
      endif()
    else ()
      message(FATAL_ERROR "error: could not find ps binary")
    endif()
  endif()

  find_path(LIBPROC2_INCLUDE_DIR
    NAMES libproc2/pids.h
    HINTS ${LIBPROC2_ROOT}
    PATH_SUFFIXES include)

  find_library(LIBPROC2_LIBRARY
    NAMES proc2
    HINTS ${LIBPROC2_ROOT}
    PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

  set(LIBPROC2_LIBRARIES ${LIBPROC2_LIBRARY})
  set(LIBPROC2_INCLUDE_DIRS ${LIBPROC2_INCLUDE_DIR})
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(libproc2
  REQUIRED_VARS LIBPROC2_LIBRARY LIBPROC2_INCLUDE_DIR)

mark_as_advanced(LIBPROC2_INCLUDE_DIR LIBPROC2_LIBRARY)

if(LIBPROC2_FOUND AND NOT TARGET procps::libproc2)
  add_library(procps::libproc2 INTERFACE IMPORTED)
  set_property(TARGET procps::libproc2 PROPERTY INTERFACE_INCLUDE_DIRECTORIES "${LIBPROC2_INCLUDE_DIRS}")
  set_property(TARGET procps::libproc2 PROPERTY INTERFACE_LINK_LIBRARIES "${LIBPROC2_LIBRARIES}")
endif()

message(DEBUG "LIBPROC2_FOUND = ${LIBPROC2_FOUND}")
message(DEBUG "LIBPROC2_VERSION = ${LIBPROC2_VERSION}")
message(DEBUG "LIBPROC2_INCLUDE_DIRS = ${LIBPROC2_INCLUDE_DIRS}")
message(DEBUG "LIBPROC2_LIBRARIES = ${LIBPROC2_LIBRARIES}")
message(DEBUG "libproc2_FIND_REQUIRED = ${libproc2_FIND_REQUIRED}")
