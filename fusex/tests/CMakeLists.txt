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
  "${gtest_SOURCE_DIR}/include"
  "${gmock_SOURCE_DIR}/include"
)

set(LIBRARIES_TO_LINK_TEST_EXECUTABLES_WITH
  gtest_main
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
  ${BACKWARD_LIBRARIES}
  ${LIBRARIES_IF_ROCKSDB_WAS_FOUND}
  eosCommonServer eosCommon
)

set(COMPILE_FLAGS_FOR_TEST_EXECUTABLES "${EOSXD_COMPILE_FLAGS} -fPIC")

add_executable(eos-fusex-tests
  auth/credential-finder.cc
  auth/environment-reader.cc
  auth/login-identifier.cc
  auth/process-cache.cc
  auth/process-info.cc
  auth/rm-info.cc
  auth/security-checker.cc
  ${TEST_SOURCES_IF_ROCKSDB_WAS_FOUND}
  interval-tree.cc
  journal-cache.cc
  rb-tree.cc
  ${BACKWARD_ENABLE}
  ${EOSXD_COMMON_SOURCES}
)

target_link_libraries(
  eos-fusex-tests
  ${LIBRARIES_TO_LINK_TEST_EXECUTABLES_WITH}
)

target_compile_definitions(eos-fusex-tests
  PUBLIC -DGTEST_USE_OWN_TR1_TUPLE=0)

set_target_properties(
  eos-fusex-tests
  PROPERTIES
  COMPILE_FLAGS ${COMPILE_FLAGS_FOR_TEST_EXECUTABLES}
)

add_executable(eos-fusex-stress-tests
  stress/xrdcl-proxy.cc
  ${CMAKE_SOURCE_DIR}/fusex/data/xrdclproxy.cc
  ${CMAKE_SOURCE_DIR}/fusex/data/xrdclproxy.hh
  ${BACKWARD_ENABLE}
)

target_link_libraries(
  eos-fusex-stress-tests
  ${LIBRARIES_TO_LINK_TEST_EXECUTABLES_WITH}
)

target_compile_definitions(eos-fusex-stress-tests
  PUBLIC -DGTEST_USE_OWN_TR1_TUPLE=0)

set_target_properties(
  eos-fusex-stress-tests
  PROPERTIES
  COMPILE_FLAGS ${COMPILE_FLAGS_FOR_TEST_EXECUTABLES}
)