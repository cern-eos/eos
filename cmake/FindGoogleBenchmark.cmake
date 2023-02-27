# Try to find google-benchmark
# Once done, this will define
#
# GOOGLEBENCHMARK_FOUND           - system has google-benchmark
# GOOGLEBENCHMARK_INCLUDE_DIRS    - google-benchmark include directories
# GOOGLEBENCHMARK_LIBRARIES       - benchmark library
#
# and the following imported targets
#
# GOOGLEBENCHMARK::GOOGLEBENCHMARK


# Benchmark exports no version information in the include header unlike many others
# so if using the system version of package, use pkg-config to determine version
find_package(PkgConfig)
if (PkgConfig_FOUND)
  pkg_search_module(GOOGLEBENCHMARK_PC benchmark)
endif()

find_path(GOOGLEBENCHMARK_INCLUDE_DIR
  NAMES benchmark/benchmark.h
  HINTS ${GOOGLEBENCHMARK_ROOT}
  SUFFIXES include)

find_library(GOOGLEBENCHMARK_LIBRARY
  NAMES benchmark
  HINTS ${GOOGLEBENCHMARK_ROOT}
  PATH_SUFFIXES ${CMAKE_INSTALL_LIBDIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(GoogleBenchmark
  REQUIRED_VARS GOOGLEBENCHMARK_LIBRARY GOOGLEBENCHMARK_INCLUDE_DIR
  VERSION_VAR GOOGLEBENCHMARK_PC_VERSION
)

mark_as_advanced(GOOGLEBENCHMARK_FOUND GOOGLEBENCHMARK_LIBRARY GOOGLEBENCHMARK_INCLUDE_DIR
  GOOGLEBENCHMARK_PC_VERSION)

if (GOOGLEBENCHMARK_FOUND AND NOT TARGET GOOGLEBENCHMARK::GOOGLEBENCHMARK)
  add_library(GOOGLEBENCHMARK::GOOGLEBENCHMARK UNKNOWN IMPORTED)
  set_target_properties(GOOGLEBENCHMARK::GOOGLEBENCHMARK PROPERTIES
    IMPORTED_LOCATION "${GOOGLEBENCHMARK_LIBRARY}"
    INTERFACE_INCLUDE_DIRECTORIES "${GOOGLEBENCHMARK_INCLUDE_DIR}")
endif()

unset(GOOGLEBENCHMARK_INCLUDE_DIR)
unset(GOOGLEBENCHMARK_LIBRARY)
