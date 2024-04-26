# Try to find abseil library
# Once done, this will define
#
# ABSL_FOUND          - system has absl library
# ABSL_INCLUDE_DIRS   - absl include directories
# ABSL_LIBRARIES      - libraries needed to use absl
##
find_path(ABSL_INCLUDE_DIR
  NAMES absl/base/config.h
  HINTS /opt/eos/grpc ${ABSL_ROOT}
  PATH_SUFFIXES include)

set(libraries absl_synchronization absl_graphcycles_internal absl_stacktrace absl_symbolize absl_time absl_civil_time absl_time_zone
  absl_malloc_internal absl_debugging_internal absl_demangle_internal absl_strings absl_int128
  absl_strings_internal absl_base absl_spinlock_wait absl_throw_delegate absl_raw_logging_internal absl_log_severity
  absl_log_internal_check_op absl_log_internal_message absl_cord_internal absl_cordz_info absl_cordz_sample_token
  absl_cord absl_cord_functions absl_hash absl_status absl_log_internal_nullguard absl_cordz_functions)

foreach( lib ${libraries})
  find_library(ABSL_${lib}_LIBRARY
    NAMES ${lib}
    HINTS /opt/eos/grpc ${ABSL_ROOT}
    PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})
  
  if(ABSL_${lib}_LIBRARY)
    set(ABSL_${lib}_FOUND 1)
    list(APPEND ABSL_LIBRARIES ${ABSL_${lib}_LIBRARY})
    mark_as_advanced(ABSL_${lib}_LIBRARY)
    message(VERBOSE "ABSL_${lib}_LIBRARY")
  endif()
endforeach()

string (REPLACE ";" " " ABSL_LIBRARY "${ABSL_LIBRARIES}")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(absl
  REQUIRED_VARS ABSL_LIBRARY ABSL_INCLUDE_DIR)

mark_as_advanced(ABSL_INCLUDE_DIR ABSL_LIBRARY)
message(VERBOSE "Abseil include path: ${ABSL_INCLUDE_DIR}")

if (ABSL_FOUND AND NOT TARGET ABSL::ABSL)
  add_library(ABSL::ABSL UNKNOWN IMPORTED)
  set_target_properties(ABSL::ABSL PROPERTIES
    IMPORTED_LOCATION "${ABSL_absl_base_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${ABSL_INCLUDE_DIR}"
    INTERFACE_LINK_LIBRARIES "${ABSL_LIBRARIES}")
endif ()

unset(ABSL_INCLUDE_DIR)
unset(ABSL_LIBRARIES)
