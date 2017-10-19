# ------------------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Andreas-Joachim Peters <apeers@cern.ch> CERN
# ------------------------------------------------------------------------------

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
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CPPUNIT_INCLUDE_DIRS}

  ${CMAKE_SOURCE_DIR}
  ${CMAKE_BINARY_DIR}
  ${XROOTD_INCLUDE_DIRS}
  ${XROOTD_PRIVATE_INCLUDE_DIR}
  ${FUSE_INCLUDE_DIRS}
  ${SPARSEHASH_INCLUDE_DIRS}
  ${KRB5_INCLUDE_DIR}
  ${OPENSSL_INCLUDE_DIR}
  ${PROTOBUF_INCLUDE_DIRS}
  ${JSONCPP_INCLUDE_DIR}
  ${HIREDIS_INCLUDE_DIR}
  ${ZMQ_INCLUDE_DIR}
  ${LIBEVENT_INCLUDE_DIRS}
)


link_directories( ${CPPUNIT_LIBRARY} )

add_executable(eos-fusex-tests
  ${TEST_SOURCES_IF_ROCKSDB_WAS_FOUND}
  Main.cc
)

target_link_libraries(
  eos-fusex-tests
  eosxd-test
  gtest
  ${GTEST_LIBRARIES}
  ${FUSE_LIBRARY}
  ${UUID_LIBRARIES}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT}
  ${KRB5_LIBRARIES}
  ${OPENSSL_CRYPTO_LIBRARY}
  ${PROTOBUF_LIBRARY}
  ${JSONCPP_LIBRARIES}
  ${HIREDIS_LIBRARIES}
  ${LIBEVENT_LIBRARIES}
  eosCommonServer eosCommon
  ${CPPUNIT_LIBRARIES} )

set_target_properties(
  eos-fusex-tests
  PROPERTIES
  COMPILE_FLAGS "-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -fPIC")


add_library(
  EosFuseXTests SHARED
  RBTreeTest.cc
  IntervalTreeTest.cc
  JournalCacheTest.cc
  XrdClProxyTest.cc
)

target_link_libraries(
  EosFuseXTests
  eosxd-test
  ${FUSE_LIBRARY}
  ${UUID_LIBRARIES}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT}
  ${KRB5_LIBRARIES}
  ${OPENSSL_CRYPTO_LIBRARY}
  ${PROTOBUF_LIBRARY}
  ${JSONCPP_LIBRARIES}
  ${HIREDIS_LIBRARIES}
  ${LIBEVENT_LIBRARIES}
  eosCommonServer eosCommon
  ${CPPUNIT_LIBRARIES} )

if (Linux)
  set_target_properties(
    EosFuseXTests
    PROPERTIES
    COMPILE_FLAGS "-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -fPIC")
else (Linux)
  set_target_properties(
    EosFuseXTests
    PROPERTIES
    COMPILE_FLAGS "-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64")
endif (Linux)



add_executable(fusex-tester TextRunner.cc)
target_link_libraries( fusex-tester EosFuseXTests dl eosCommonServer eosCommon)

set_target_properties(
  fusex-tester
  PROPERTIES
  COMPILE_FLAGS "-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64")

add_executable(fusex-benchmark fusex-benchmark.cc )
target_link_libraries( fusex-benchmark eosCommonServer eosCommon)
