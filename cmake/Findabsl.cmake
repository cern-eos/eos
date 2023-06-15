# Try to find abseil library
# Once done, this will define
#
# ABSL_FOUND          - system has absl library
# ABSL_INCLUDE_DIRS   - absl include directories
# ABSL_LIBRARIES      - libraries needed to use absl
#
#
find_path(ABSL_INCLUDE_DIR
  NAMES absl/base/config.h
  HINTS /opt/eos/grpc/include ${ABSL_ROOT}
  PATH_SUFFIXES include jemalloc)

set(libraries absl_synchronization absl_graphcycles_internal absl_stacktrace absl_symbolize absl_time absl_civil_time absl_time_zone
  absl_malloc_internal absl_debugging_internal absl_demangle_internal absl_strings absl_int128
  absl_strings_internal absl_base absl_spinlock_wait absl_throw_delegate absl_raw_logging_internal absl_log_severity)

foreach( lib ${libraries})
  find_library(ABSL_${lib}_LIBRARY NAMES ${lib} HINTS 
    HINTS /opt/eos/grpc/lib64 ${ABSL_ROOT}
    PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})
  
  if(ABSL_${lib}_LIBRARY)
    set(ABSL_${lib}_FOUND 1)
    list(APPEND ABSL_LIBRARIES ${ABSL_${lib}_LIBRARY})
    mark_as_advanced(ABSL_${lib}_LIBRARY)
    message ("ABSL_${lib}_LIBRARY")
  endif()
endforeach()

string (REPLACE ";" " " ABSL_LIBRARY "${ABSL_LIBRARIES}")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(absl
  REQUIRED_VARS ABSL_LIBRARY ABSL_INCLUDE_DIR)

mark_as_advanced(ABSL_INCLUDE_DIR ABSL_LIBRARY)
message(STATUS "Abseil include path: ${ABSL_INCLUDE_DIR}")


