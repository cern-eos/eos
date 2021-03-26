# Try to find PROTOBUF3
# Once done, this will define
#
# PROTOBUF_FOUND               - system has PROTOBUF
# PROTOBUF_INCLUDE_DIRs        - Protobuf include directories
# PROTOBUF_LIBRARIES           - libraries needed to use Protobuf
#
# and the following imported targets
#
# PROTOBUF::PROTOBUF
#
# PROTOBUF_ROOT may be defined as a hint for where to look

find_program(PROTOBUF_PROTOC_EXECUTABLE
  NAMES protoc
  HINTS ${PROTOBUF_ROOT}/bin
  PATHS /opt/eos/grpc /opt/eos /usr/local /usr /bin
  PATH_SUFFIXES bin
  DOC "Version 3 of The Google Protocol Buffers Compiler"
  NO_DEFAULT_PATH)

if (PROTOBUF_PROTOC_EXECUTABLE)
  message(STATUS "Found protoc: ${PROTOBUF_PROTOC_EXECUTABLE}")
else()
  message(STATUS "Trying to search for protoc3 instead for protoc")
  unset(PROTOBUF_PROTOC_EXECUTABLE)
  find_program(PROTOBUF_PROTOC_EXECUTABLE
    NAMES protoc3
    HINTS ${PROTOBUF_ROOT}/bin
    PATHS /opt/eos/grpc /opt/eos /usr/local /usr /
    PATH_SUFFIXES bin
    DOC "Version 3 of The Google Protocol Buffers Compiler"
    NO_DEFAULT_PATH)
endif()

find_path(PROTOBUF_INCLUDE_DIR
  NAMES google/protobuf/message.h
  HINTS ${PROTOBUF_ROOT}
  PATHS /opt/eos/grpc /opt/eos/include/protobuf3 /usr/include/protobuf3
        /usr/local /usr
  PATH_SUFFIXES include
  NO_DEFAULT_PATH)

find_library(PROTOBUF_LIBRARY
  NAME protobuf
  HINTS ${PROTOBUF_ROOT}
  PATHS /opt/eos/grpc /opt/eos/lib64/protobuf3 /usr/lib64/protobuf3 /usr/lib/protobuf3
	/usr/local /usr /usr/lib/x86_64-linux-gnu
  PATH_SUFFIXES lib64 lib
  NO_DEFAULT_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Protobuf3
  REQUIRED_VARS PROTOBUF_INCLUDE_DIR PROTOBUF_LIBRARY PROTOBUF_PROTOC_EXECUTABLE)
mark_as_advanced(PROOBUF3_FOUND PROTOBUF_INCLUDE_DIR PROTOBUF_LIBRARY
  PROTOBUF_PROTOC_EXECUTABLE)

if (PROTOBUF3_FOUND AND NOT TARGET PROTOBUF::PROTOBUF)
  add_library(PROTOBUF::PROTOBUF UNKNOWN IMPORTED)
  set_target_properties(PROTOBUF::PROTOBUF PROPERTIES
    IMPORTED_LOCATION "${PROTOBUF_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${PROTOBUF_INCLUDE_DIR}")
endif ()

# Include Protobuf package from the generation commands
find_package(Protobuf)

set(PROTOBUF_INCLUDE_DIRS ${PROTOBUF_INCLUDE_DIR})
set(PROTOBUF_LIBRARIES ${PROTOBUF_LIBRARY})
#unset(PROTOBUF_INCLUDE_DIR)
#unset(PROTOBUF_LIBRARY)
