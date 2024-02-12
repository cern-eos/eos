#-------------------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Abhishek Lekshmanan CERN
#-------------------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2022 CERN/Switzerland                                  *
# *                                                                      *
# * This program is free software: you can redistribute it and/or modify *
# * it under the terms of the GNU General Public License as published by *
# * the Free Software Foundation, either version 3 of the License, or    *
# * (at your option) any later version.                                  *
# *                                                                      *
# * This program is distributed in the hope that it will be useful,      *
# * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
# * GNU General Public License for more details.                         *
# *                                                                      *
# * You should have received a copy of the GNU General Public License    *
# * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
# ************************************************************************

option(USE_SYSTEM_GBENCH "Use Google benchmark installed in the system if found" ON)

if(USE_SYSTEM_GBENCH)
  find_package(benchmark 1.6.0)
else()
  if(IS_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/benchmark)
    set(BENCHMARK_FOUND TRUE)
    set(BENCHMARK_ENABLE_GTEST_TESTS OFF)
    add_subdirectory(benchmark EXCLUDE_FROM_ALL)
    # benchmark redefines VERSION and fails with -Werror which is set internally
    # override this, we don't care about warnings in the benchmark build itself
    target_compile_options(benchmark PRIVATE -w)
  endif()
endif()

if(NOT BENCHMARK_FOUND)
  return()
endif()

add_executable(eos-microbenchmarks main.cc)
add_executable(eos-idmap-microbenchmark common/BM_IdMap.cc)
add_executable(eos-atomic-ptr-microbenchmark common/BM_AtomicPtr.cc)
add_executable(eos-random-microbenchmark common/BM_Random.cc)
add_executable(eos-flatscheduler-microbenchmark mgm/BM_FlatScheduler.cc
        ${CMAKE_SOURCE_DIR}/mgm/placement/ClusterMap.cc
        ${CMAKE_SOURCE_DIR}/mgm/placement/FlatScheduler.cc
        ${CMAKE_SOURCE_DIR}/mgm/placement/RoundRobinPlacementStrategy.cc
        ${CMAKE_SOURCE_DIR}/mgm/placement/WeightedRandomStrategy.cc
        ${CMAKE_SOURCE_DIR}/mgm/placement/WeightedRoundRobinStrategy.cc
        ${CMAKE_SOURCE_DIR}/mgm/placement/ThreadLocalRRSeed.cc
        )
add_executable(eos-rrseed-microbenchmark mgm/BM_RRSeed.cc
        ${CMAKE_SOURCE_DIR}/mgm/placement/ThreadLocalRRSeed.cc)
add_executable(eos-threadid-microbenchmark common/BM_ThreadId.cc)

target_link_libraries(eos-microbenchmarks PRIVATE
  benchmark::benchmark
  XROOTD::UTILS)

target_link_libraries(eos-idmap-microbenchmark PRIVATE
  benchmark::benchmark
  EosCommon-Static)

target_link_libraries(eos-atomic-ptr-microbenchmark PRIVATE
  benchmark::benchmark
  EosCommon-Static
  ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries(eos-rrseed-microbenchmark PRIVATE
  benchmark::benchmark
  ${CMAKE_THREAD_LIBS_INIT}
  EosCommon-Static)

target_link_libraries(eos-flatscheduler-microbenchmark PRIVATE
  benchmark::benchmark
  EosCommonServer-Static)

target_link_libraries(eos-random-microbenchmark PRIVATE
  benchmark::benchmark)

target_link_libraries(eos-threadid-microbenchmark PRIVATE
  benchmark::benchmark)
