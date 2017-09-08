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
  ${CPPUNIT_INCLUDE_DIRS}
  "${gtest_SOURCE_DIR}/include"
  "${gmock_SOURCE_DIR}/include")

#-------------------------------------------------------------------------------
# ProtoBuf representation tests
#-------------------------------------------------------------------------------
add_executable(
  test_metadata
  ContainerMDSvcTest.cc
  FileMDSvcTest.cc
  HierarchicalViewTest.cc
  MetadataTests.cc
  NextInodeProviderTest.cc
  OtherTests.cc
)

target_link_libraries(
  test_metadata
  gtest
  gmock_main
  EosNsQuarkdb-Static
  ${CMAKE_THREAD_LIBS_INIT})

#-------------------------------------------------------------------------------
# EosNsQuarkdbTests library
#-------------------------------------------------------------------------------
add_library(
  EosNsQuarkdbTests MODULE
  FileSystemViewTest.cc
)

target_link_libraries(
  EosNsQuarkdbTests
  EosNsQuarkdb-Static
  ${CPPUNIT_LIBRARIES})

#-------------------------------------------------------------------------------
# eosnsbench executable
#-------------------------------------------------------------------------------
add_executable(eosnsbench EosNamespaceBenchmark.cc)

target_compile_options(
  eosnsbench
  PUBLIC -DFILE_OFFSET_BITS=64)

target_link_libraries(eosnsbench EosNsQuarkdb-Static eosCommon-Static)

install(
  TARGETS
  eosnsbench
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
