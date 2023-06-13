# Try to find abseil library
# Once done, this will define
#
# ABSL_FOUND          - system has absl library
# ABSL_INCLUDE_DIRS   - absl include directories
# ABSL_LIBRARIES      - libraries needed to use absl
#
# and the following imported targets
#
# ABSL::ABSL
#
find_path(ABSL_INCLUDE_DIR
  NAMES absl/base/config.h
  HINTS /opt/eos/grpc/include ${ABSL_ROOT}
  PATH_SUFFIXES include jemalloc)

find_library(ABSL_LIBRARY
  NAME absl_synchronization
  HINTS /opt/eos/grpc/lib64 ${ABSL_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(absl
  REQUIRED_VARS ABSL_LIBRARY ABSL_INCLUDE_DIR)

mark_as_advanced(ABSL_INCLUDE_DIR ABSL_LIBRARY)
message(STATUS "Abseil include path: ${ABSL_INCLUDE_DIR}")

if (ABSL_FOUND AND NOT TARGET ABSL::ABSL)
  add_library(ABSL::ABSL UNKNOWN IMPORTED)
  set_target_properties(ABSL::ABSL PROPERTIES
    IMPORTED_LOCATION "${ABSL_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${ABSL_INCLUDE_DIR}")
else()
  message(WARNING "Notice: Abseil  not found, no abseil support")
endif()
