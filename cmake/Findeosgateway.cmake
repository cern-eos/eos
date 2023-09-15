# Try to find abseil library
# Once done, this will define
#
# GATEWAY_FOUND          - system has grpc gateway library
# GATEWAY_INCLUDE_DIRS   - gateway include directories
# GATEWAY_LIBRARY        - gateway library
#

find_path(GATEWAY_INCLUDE_DIR
  NAMES libgateway.h
  HINTS /usr ${GATEWAY_ROOT}
  PATH_SUFFIXES include)

find_library(GATEWAY_LIBRARY NAMES libgateway.so
  HINTS /usr/lib64 ${GATEWAY_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

if(GATEWAY_LIBRARY)
  set(GATEWAY_FOUND 1)
  message ("${GATEWAY_LIBRARY}")
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(eosgateway
  REQUIRED_VARS GATEWAY_LIBRARY GATEWAY_INCLUDE_DIR)

mark_as_advanced(GATEWAY_INCLUDE_DIR GATEWAY_LIBRARY)
message(STATUS "eos-gateway include path: ${GATEWAY_INCLUDE_DIR}")

if (GATEWAY_FOUND AND NOT TARGET GATEWAY::GATEWAY)
  add_library(GATEWAY::GATEWAY UNKNOWN IMPORTED)
  set_target_properties(GATEWAY::GATEWAY PROPERTIES
    IMPORTED_LOCATION "${GATEWAY_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${GATEWAY_INCLUDE_DIR}"
    INTERFACE_COMPILE_DEFINITIONS EOS_GRPC_GATEWAY=1)
endif ()

if (TARGET GATEWAY::GATEWAY)
  message(STATUS "Successfully created target GATEWAY::GATEWAY")
endif()
