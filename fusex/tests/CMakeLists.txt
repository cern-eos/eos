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
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_BINARY_DIR}
  ${FUSE_INCLUDE_DIRS}
  ${SPARSEHASH_INCLUDE_DIRS}
  ${KRB5_INCLUDE_DIR}
  ${OPENSSL_INCLUDE_DIR}
  ${JSONCPP_INCLUDE_DIR}
  ${HIREDIS_INCLUDE_DIR}
  ${ZMQ_INCLUDE_DIRS}
  ${LIBEVENT_INCLUDE_DIRS})

set(LIBRARIES_TO_LINK_TEST_EXECUTABLES_WITH
  gtest_main
  EosCommon
  ${FUSE_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT}
  ${KRB5_LIBRARIES}
  ${OPENSSL_CRYPTO_LIBRARY}
  ${JSONCPP_LIBRARIES}
  ${HIREDIS_LIBRARIES}
  ${LIBEVENT_LIBRARIES}
  ${LIBRARIES_IF_ROCKSDB_WAS_FOUND}
  ${ZMQ_LIBRARIES})

if (RICHACL_FOUND)
  include_directories(${RICHACL_INCLUDE_DIRS})
  LIST(APPEND LIBRARIES_TO_LINK_TEST_EXECUTABLES_WITH "${RICHACL_LIBRARIES}")
endif()

set(COMPILE_FLAGS_FOR_TEST_EXECUTABLES "${EOSXD_COMPILE_FLAGS} -fPIC")

add_executable(eos-fusex-tests
  auth/credential-finder.cc
  auth/environment-reader.cc
  auth/logbook.cc
  auth/login-identifier.cc
  auth/process-cache.cc
  auth/process-info.cc
  auth/rm-info.cc
  auth/security-checker.cc
  auth/test-utils.cc
  auth/utils.cc
  interval-tree.cc
  journal-cache.cc
  rb-tree.cc
  ${TEST_SOURCES_IF_ROCKSDB_WAS_FOUND}
  ${EOSXD_COMMON_SOURCES})

target_link_libraries(eos-fusex-tests PRIVATE
  eosxd-objects
  ${LIBRARIES_TO_LINK_TEST_EXECUTABLES_WITH})

set_target_properties(eos-fusex-tests
  PROPERTIES COMPILE_FLAGS ${COMPILE_FLAGS_FOR_TEST_EXECUTABLES})

add_executable(eos-fusex-stress-tests
  stress/xrdcl-proxy.cc
  ${CMAKE_SOURCE_DIR}/fusex/data/xrdclproxy.cc
  ${CMAKE_SOURCE_DIR}/fusex/data/xrdclproxy.hh
  ${CMAKE_SOURCE_DIR}/common/ShellCmd.cc
  ${CMAKE_SOURCE_DIR}/common/ShellExecutor.cc)

target_link_libraries(eos-fusex-stress-tests PRIVATE
  ${LIBRARIES_TO_LINK_TEST_EXECUTABLES_WITH})

set_target_properties(eos-fusex-stress-tests
  PROPERTIES COMPILE_FLAGS ${COMPILE_FLAGS_FOR_TEST_EXECUTABLES})

add_executable(eos-fusex-ioverify ioverify.cc)

target_link_libraries(eos-fusex-ioverify PRIVATE
  ${LIBRARIES_TO_LINK_TEST_EXECUTABLES_WITH})

set_target_properties(eos-fusex-ioverify
  PROPERTIES COMPILE_FLAGS ${COMPILE_FLAGS_FOR_TEST_EXECUTABLES})

install(PROGRAMS eos-fusex-recovery eos-fusex-ioverify eos-fusex-tests
  DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
  PERMISSIONS OWNER_READ OWNER_EXECUTE
              GROUP_READ GROUP_EXECUTE
              WORLD_READ WORLD_EXECUTE)
