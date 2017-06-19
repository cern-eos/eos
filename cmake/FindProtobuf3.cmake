# Try to find PROTOBUF3
# Once done, this will define
#
# PROTOBUF_FOUND               - system has PROTOBUF
# PROTOBUF_INCLUDE_DIR         - Prorobuf include directories
# PROTOBUF_LIBRARIES           - libraries needed to use XRootD
#
# PROTOBUF_DIR may be defined as a hint for where to look

find_program(PROTOBUF_PROTOC_EXECUTABLE
  NAMES protoc3
  DOC "Version 3 of The Google Protocol Buffers Compiler")
message(STATUS "protoc is at ${PROTOBUF_PROTOC_EXECUTABLE} ")

find_path(PROTOBUF_INCLUDE_DIR
  google/protobuf/message.h
  PATHS /usr/include/protobuf3
  HINTS ${PROTOBUF_DIR}
  NO_DEFAULT_PATH)
set(PROTOBUF_INCLUDE_DIRS ${PROTOBUF_INCLUDE_DIR})
message(STATUS "PROTOBUF_INCLUDE_DIRS=${PROTOBUF_INCLUDE_DIRS}")

find_library(PROTOBUF_LIBRARY
  NAME protobuf
  PATHS /usr/lib64/protobuf3
  HINTS ${PROTOBUF_DIR}
  NO_DEFAULT_PATH)
set(PROTOBUF_LIBRARIES ${PROTOBUF_LIBRARY})
message(STATUS "PROTOBUF_LIBRARIES=${PROTOBUF_LIBRARIES}")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Protobuf3 DEFAULT_MSG
  PROTOBUF_INCLUDE_DIRS PROTOBUF_LIBRARIES)

find_package(Protobuf)
