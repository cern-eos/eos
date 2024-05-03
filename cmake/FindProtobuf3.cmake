# Try to find PROTOBUF3
# Once done, this will define
#
# PROTOBUF3_FOUND               - system has Protobuf3
# PROTOBUF3_INCLUDE_DIRS        - Protobuf3 include directories
# PROTOBUF3_LIBRARIES           - libraries needed to use Protobuf3
#
# and the following imported targets
#
# PROTOBUF::PROTOBUF
#
# PROTOBUF_ROOT may be defined as a hint for where to look

find_program(PROTOBUF3_PROTOC_EXECUTABLE
  NAMES protoc
  HINTS ${PROTOBUF_ROOT}
  PATHS /opt/eos/grpc/ /opt/eos/ /usr/local /usr /
  PATH_SUFFIXES bin
  DOC "Version 3 of The Google Protocol Buffers Compiler (protoc)"
  NO_DEFAULT_PATH)

message(STATUS "Found protoc: ${PROTOBUF3_PROTOC_EXECUTABLE}")

find_path(PROTOBUF3_INCLUDE_DIR
  NAMES google/protobuf/message.h
  HINTS ${PROTOBUF_ROOT}
  PATHS /opt/eos/grpc/ /opt/eos/include/protobuf3 /usr/include/protobuf3 /usr/local /usr
  PATH_SUFFIXES include
  NO_DEFAULT_PATH)

find_library(PROTOBUF3_LIBRARY
  NAME protobuf
  HINTS ${PROTOBUF_ROOT}
  PATHS /opt/eos/grpc/ /usr/lib64/protobuf3 /usr/lib/protobuf3
	/usr/local /usr /usr/lib/x86_64-linux-gnu
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR} lib64 lib
  NO_DEFAULT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Protobuf3
  REQUIRED_VARS PROTOBUF3_LIBRARY PROTOBUF3_INCLUDE_DIR PROTOBUF3_PROTOC_EXECUTABLE)
mark_as_advanced(PROOBUF3_FOUND PROTOBUF3_INCLUDE_DIR PROTOBUF3_LIBRARY
  PROTOBUF3_PROTOC_EXECUTABLE)

if (PROTOBUF3_FOUND AND NOT TARGET PROTOBUF::PROTOBUF)
  # These are set for make the find_package(Protobuf) happy at the end and
  # at the same time include the PROTOBUF_GENERATE_CPP function
  set(Protobuf_FOUND ${PROTOBUF3_FOUND})
  set(Protobuf_INCLUDE_DIR ${PROTOBUF3_INCLUDE_DIR})
  set(Protobuf_LIBRARY ${PROTOBUF3_LIBRARY})
  set(Protobuf_PROTOC_EXECUTABLE ${PROTOBUF3_PROTOC_EXECUTABLE})

  add_library(PROTOBUF::PROTOBUF UNKNOWN IMPORTED)
  set_target_properties(PROTOBUF::PROTOBUF PROPERTIES
    IMPORTED_LOCATION "${PROTOBUF3_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${PROTOBUF3_INCLUDE_DIR}")
  target_compile_definitions(PROTOBUF::PROTOBUF INTERFACE PROTOBUF_USE_DLLS=1)

  # Overwrite these since they are used in generating the Protobuf files
  if (NOT TARGET protobuf::protoc)
    add_executable(protobuf::protoc IMPORTED)
  endif()

  set_target_properties(protobuf::protoc PROPERTIES
    IMPORTED_LOCATION ${PROTOBUF3_PROTOC_EXECUTABLE})

  if (NOT TARGET protobuf::libprotobuf)
    add_library(protobuf::libprotobuf UNKNOWN IMPORTED)
  endif()

  set_target_properties(protobuf::libprotobuf PROPERTIES
   INTERFACE_INCLUDE_DIRECTORIES ${PROTOBUF3_INCLUDE_DIR}
   IMPORTED_LOCATION ${PROTOBUF3_LIBRARY})
endif ()

# Include Protobuf package for the generation commands like PROTOBUF_GENERATE_CPP
find_package(Protobuf)
