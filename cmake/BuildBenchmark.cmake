# We use Benchmark as an external project, instead of add_subdirectory - we
# still use submodules, but this allows for customizations in build args, this
# is necessary as we define RELEASE to be a macro pointing to an eos release
# whereas benchmark libary has its internal RELEASE definition which means
# something else, also allows, if we didn't need to customize build options, we
# could just use FetchProject which directly exports the necessary
# benchmark::benchmark cmake target, however with ExternalProject this work is
# largely manual

macro(build_gbench gbench_root)
  include(ExternalProject)
  set(BENCHMARK_CMAKE_CXX_FLAGS "-std=c++17")
  set(BENCHMARK_CMAKE_ARGS
    "-DCMAKE_BUILD_TYPE=Release"
    "-DCMAKE_CXX_FLAGS=${BENCHMARK_CMAKE_CXX_FLAGS}"
    "-DBENCHMARK_ENABLE_TESTING=OFF")

  set(benchmark_library ${CMAKE_STATIC_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX})

  ExternalProject_Add(googlebenchmark
    SOURCE_DIR "${gbench_root}"
    CMAKE_ARGS "${BENCHMARK_CMAKE_ARGS}"
    BUILD_BYPRODUCTS
    <BINARY_DIR>/src/${benchmark_library}
    INSTALL_COMMAND ""
    LOG_CONFIGURE ON
    LOG_BUILD ON)

  ExternalProject_Get_Property(googlebenchmark source_dir)
  find_path(BENCHMARK_INCLUDE_DIRS
    NAMES benchmark/benchmark.h
    PATHS ${source_dir}/include)

  ExternalProject_Get_Property(googlebenchmark binary_dir)
  message(STATUS "Setting benchmark binary_dir ${binary_dir}")
  set(BENCHMARK_LIBRARY_PATH ${binary_dir}/src/${benchmark_library})
  unset(benchmark_library)
  set(BENCHMARK_LIBRARY benchmark::benchmark)
  add_library(${BENCHMARK_LIBRARY} STATIC IMPORTED)
  set_target_properties(${BENCHMARK_LIBRARY} PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${BENCHMARK_INCLUDE_DIRS}"
    IMPORTED_LOCATION "${BENCHMARK_LIBRARY_PATH}"
    IMPORTED_LINK_INTERFACE_LIBRARIES "${CMAKE_THREAD_LIBS_INIT}")
  add_dependencies(${BENCHMARK_LIBRARY} googlebenchmark)
endmacro()
