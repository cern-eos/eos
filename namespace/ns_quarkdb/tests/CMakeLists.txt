# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Andreas-Joachim Peters - CERN
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2016 CERN/Switzerland                                  *
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
include_directories(
  ${CMAKE_SOURCE_DIR}
  "${gtest_SOURCE_DIR}/include"
  "${gmock_SOURCE_DIR}/include")

#-------------------------------------------------------------------------------
# QuarkDB namespace unit tests
#
# Note: These tests require a running QuarkDB instance
#-------------------------------------------------------------------------------
add_executable(
  eos-ns-quarkdb-tests
  ContainerMDSvcTest.cc
  FileMDSvcTest.cc
  FileSystemViewTest.cc
  HierarchicalViewTest.cc
  Main.cc
  MetadataFiltering.cc
  MetadataTests.cc
  NextInodeProviderTest.cc
  OtherTests.cc
  TestUtils.cc
  VariousTests.cc)

target_link_libraries(
  eos-ns-quarkdb-tests
  gtest
  gmock_main
  EosNsCommon-Static
  FOLLY::FOLLY
  XROOTD::UTILS
  OPENSSL::OPENSSL)

install(TARGETS eos-ns-quarkdb-tests
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})

#-------------------------------------------------------------------------------
# eosnsbench executable
#-------------------------------------------------------------------------------
add_executable(eosnsbench EosNamespaceBenchmark.cc)
target_compile_options(eosnsbench PUBLIC -DFILE_OFFSET_BITS=64)
target_link_libraries(eosnsbench PRIVATE
  EosNsCommon-Static
  XROOTD::UTILS
  OPENSSL::OPENSSL)
add_executable(eos-lru-benchmark LruBenchmark.cc)
target_link_libraries(eos-lru-benchmark EosCommon)

install(TARGETS eosnsbench eos-lru-benchmark
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
