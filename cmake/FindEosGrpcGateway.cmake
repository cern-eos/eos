# Try to find eos-grpc-gateway library and header files
# Once done, this will define
#
# EosGrpcGateway_FOUND          - system has grpc gateway library
# EosGrpcGateway_INCLUDE_DIRS   - gateway include directories
# EosGrpcGateway_LIBRARY        - gateway library
#

find_path(EosGrpcGateway_INCLUDE_DIR
  NAMES libgateway.h
  HINTS /usr ${EosGrpcGateway_ROOT}
  PATH_SUFFIXES include)

find_library(EosGrpcGateway_LIBRARY NAMES libgateway.so
  HINTS /usr/lib64 ${EosGrpcGateway_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

if(EosGrpcGateway_LIBRARY)
  set(EosGrpcGateway_FOUND 1)
  message (STATUS "EosGrpcGateway_LIBRARY=${EosGrpcGateway_LIBRARY}")
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(eosgateway
  REQUIRED_VARS EosGrpcGateway_LIBRARY EosGrpcGateway_INCLUDE_DIR)

mark_as_advanced(EosGrpcGateway_INCLUDE_DIR EosGrpcGateway_LIBRARY)
message(STATUS "EosGrpcGateway_INCLUDE_DIR=${EosGrpcGateway_INCLUDE_DIR}")

if (EosGrpcGateway_FOUND AND NOT TARGET EosGrpcGateway::EosGrpcGateway)
  add_library(EosGrpcGateway::EosGrpcGateway UNKNOWN IMPORTED)
  set_target_properties(EosGrpcGateway::EosGrpcGateway PROPERTIES
    IMPORTED_LOCATION "${EosGrpcGateway_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${EosGrpcGateway_INCLUDE_DIR}"
    INTERFACE_COMPILE_DEFINITIONS EOS_GRPC_GATEWAY=1)
else()
  add_library(EosGrpcGateway::EosGrpcGateway INTERFACE IMPORTED)
endif ()

if (TARGET EosGrpcGateway::EosGrpcGateway)
  message(STATUS "Successfully created target EosGrpcGateway::EosGrpcGateway")
endif()

unset(EosGrpcGateway_INCLUDE_DIR)
unset(EosGrpcGateway_LIBRARY)
