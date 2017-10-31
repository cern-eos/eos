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
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DGTEST_USE_OWN_TR1_TUPLE=0")

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_BINARY_DIR}
  ${XROOTD_INCLUDE_DIRS}
  ${PROTOBUF_INCLUDE_DIRS}
  ${JSONCPP_INCLUDE_DIR}
  "${gtest_SOURCE_DIR}/include"
  "${gmock_SOURCE_DIR}/include")

set(MQ_UT_SRCS
  mq/XrdMqMessageTests.cc)

set(MGM_UT_SRCS
  mgm/ProcFsTests.cc
  mgm/AclCmdTests.cc)
  mgm/LockTrackerTests.cc)

set(COMMON_UT_SRCS
  common/TimingTests.cc
  common/MappingTests.cc
  common/SymKeysTests.cc
  common/ThreadPoolTest.cc)

set(UT_SRCS ${MQ_UT_SRCS} ${MGM_UT_SRCS} ${COMMON_UT_SRCS})
add_executable(eos-unit-tests ${UT_SRCS})

# @todo all these should be moved to gtest and simplified
# set(SOURCE_FILES
#   fst/XrdFstOssFileTest.cc
#   ${CMAKE_SOURCE_DIR}/fst/XrdFstOssFile.cc
#   ${CMAKE_SOURCE_DIR}/fst/tests/TestEnv.cc
#   ${CMAKE_SOURCE_DIR}/fst/XrdFstOss.cc)

set(EOS_UT_RPATH
"${CMAKE_BINARY_DIR}/fst/;${CMAKE_BINARY_DIR}/mgm/;${CMAKE_BINARY_DIR}/common/;"
"${CMAKE_BINARY_DIR}/mq/;${CMAKE_BINARY_DIR}/auth_plugin/")

set_target_properties(
  eos-unit-tests PROPERTIES
  INSTALL_RPATH "${EOS_UT_RPATH}"
  SKIP_RPATH FALSE
  SKIP_BUILD_RPATH  FALSE
  BUILD_WITH_INSTALL_RPATH TRUE
  INSTALL_RPATH_USE_LINK_PATH TRUE)

target_link_libraries(
  eos-unit-tests
  gtest_main
  gmock
  XrdEosMgm-Shared
  XrdEosFst-Shared
  ${XROOTD_SERVER_LIBRARY})
