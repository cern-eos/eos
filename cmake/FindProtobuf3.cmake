# Try to find PROTOBUF3
# Once done, this will define
#
# PROTOBUF_FOUND               - system has PROTOBUF
# PROTOBUF_INCLUDE_DIR         - Protobuf include directories
# PROTOBUF_LIBRARIES           - libraries needed to use Protobuf
#
# PROTOBUF_DIR may be defined as a hint for where to look

include(FindPackageHandleStandardArgs)

if(PROTOBUF_INCLUDE_DIRS AND PROTOBUF_LIBRARIES)
  set(PROTOBUF_FIND_QUIETLY TRUE)
else()
  find_program(PROTOBUF_PROTOC_EXECUTABLE
    NAMES protoc3 protoc
    DOC "Version 3 of The Google Protocol Buffers Compiler")
  message(STATUS "protoc is at ${PROTOBUF_PROTOC_EXECUTABLE}")

  find_path(PROTOBUF_INCLUDE_DIR
    google/protobuf/message.h
    PATHS /usr/include/protobuf3 /usr/include
    HINTS ${PROTOBUF_DIR}
    NO_DEFAULT_PATH)

  set(PROTOBUF_INCLUDE_DIRS ${PROTOBUF_INCLUDE_DIR})

  find_library(PROTOBUF_LIBRARY
    NAME protobuf
    PATHS /usr/lib64/protobuf3 /usr/lib/protobuf3 /usr/lib64 /usr/lib/x86_64-linux-gnu
    HINTS ${PROTOBUF_DIR}
    NO_DEFAULT_PATH)

  set(PROTOBUF_LIBRARIES ${PROTOBUF_LIBRARY})

  find_package_handle_standard_args(
    Protobuf3 DEFAULT_MSG
    PROTOBUF_INCLUDE_DIRS PROTOBUF_LIBRARIES)

  find_package(Protobuf)
endif()
