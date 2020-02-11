# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Jozsef Makai <jmakai@cern.ch> CERN
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2017 CERN/Switzerland                                  *
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
  ${CMAKE_BINARY_DIR}
  ${XROOTD_INCLUDE_DIRS}
  ${PROTOBUF_INCLUDE_DIRS}
  ${FOLLY_INCLUDE_DIRS}
  ${JSONCPP_INCLUDE_DIR}
  ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/
  ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/qclient/include
  ${CMAKE_BINARY_DIR}/namespace/ns_quarkdb
  "${gtest_SOURCE_DIR}/include"
  "${gmock_SOURCE_DIR}/include")

set(EOS_UT_RPATH
  "${CMAKE_BINARY_DIR}/fst/;${CMAKE_BINARY_DIR}/mgm/;"
  "${CMAKE_BINARY_DIR}/common/;${CMAKE_BINARY_DIR}/mq/;"
  "${CMAKE_BINARY_DIR}/auth_plugin/;${CMAKE_BINARY_DIR}/namespace/ns_quarkdb/;"
  "${CMAKE_BINARY_DIR}/namespace/;${CMAKE_BINARY_DIR}/proto/;")

set(MQ_UT_SRCS
  mq/XrdMqMessageTests.cc)

set(CONSOLE_UT_SRCS
  console/AclCmdTest.cc
  console/ParseCommentTest.cc
  console/RegexUtilTest.cc
  console/ConsoleUtilTests.cc
  console/CmdsTests.cc)

set(MGM_UT_SRCS
  mgm/AccessTests.cc
  mgm/AclCmdTests.cc
  mgm/EgroupTests.cc
  mgm/FileSystemRegistryTests.cc
  mgm/FsViewTests.cc
  mgm/HttpTests.cc
  mgm/LockTrackerTests.cc
  mgm/LRUTests.cc
  mgm/ProcFsTests.cc
  mgm/RoutingTests.cc
  mgm/IdTrackerTests.cc
  mgm/FsckEntryTests.cc
  mgm/XrdMgmOfsTests.cc)

set(COMMON_UT_SRCS
  common/FileIdTests.cc
  common/FileMapTests.cc
  common/FutureWrapperTests.cc
  common/InodeTests.cc
  common/LoggingTests.cc
  common/LoggingTestsUtils.cc
  common/MappingTests.cc
  common/PathTests.cc
  common/RWMutexTest.cc
  common/StringConversionTests.cc
  common/StringTokenizerTests.cc
  common/SymKeysTests.cc
  common/ThreadPoolTest.cc
  common/TimingTests.cc
  common/VariousTests.cc
  common/XrdConnPoolTests.cc
  common/RateLimitTests.cc
  common/EosTokenTests.cc
)

set(FST_UT_SRCS
  #fst/XrdFstOssFileTest.cc
  fst/XrdFstOfsTests.cc
  fst/HealthTest.cc
  fst/UtilsTest.cc
  fst/XrdFstOfsFileTest.cc
  fst/ScanDirTests.cc)

#-------------------------------------------------------------------------------
# unit tests source files
#-------------------------------------------------------------------------------
set(UT_SRCS
  ${MQ_UT_SRCS}
  ${CONSOLE_UT_SRCS}
  ${MGM_UT_SRCS}
  ${COMMON_UT_SRCS})

set(UT_FST_SRCS
  ${FST_UT_SRCS})

#-------------------------------------------------------------------------------
# eos-unit-tests executable
#-------------------------------------------------------------------------------
add_executable(
  eos-unit-tests
  ${UT_SRCS}
  $<TARGET_OBJECTS:EosConsoleHelpers-Objects>
  $<TARGET_OBJECTS:EosConsoleCommands-Objects>)

target_link_libraries(
  eos-unit-tests
  gtest_main
  gmock
  qclient
  XrdEosMgm-Static
  ${READLINE_LIBRARY}
  ${JSONCPP_LIBRARIES}
  ${XROOTD_SERVER_LIBRARY})

install(TARGETS
  eos-unit-tests
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})

#-------------------------------------------------------------------------------
# eos-fst-unit-tests executable
#-------------------------------------------------------------------------------
add_executable(eos-fst-unit-tests ${UT_FST_SRCS})

target_link_libraries(
  eos-fst-unit-tests
  gtest_main
  gmock
  qclient
  XrdEosFst-Static)

install(TARGETS
  eos-fst-unit-tests
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})
