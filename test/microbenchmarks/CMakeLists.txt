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

include(BuildBenchmark)
build_gbench(${CMAKE_SOURCE_DIR}/test/microbenchmarks/benchmark)
set(BM_COMMON_SRCS
  common/BM_StringUtils.cc
  common/BM_XrdString.cc
  common/BM_ReportRE.cc
  main.cc)

set(EOS_BM_SRCS
  ${BM_COMMON_SRCS})

add_executable(eos-microbenchmarks ${EOS_BM_SRCS})
add_dependencies(eos-microbenchmarks ${BENCHMARK_LIBRARY})
target_link_libraries(eos-microbenchmarks PRIVATE
  benchmark::benchmark
  XROOTD::UTILS)
