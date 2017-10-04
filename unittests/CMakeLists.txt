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
  ${XROOTD_INCLUDE_DIRS}
  "${gtest_SOURCE_DIR}/include"
  "${gmock_SOURCE_DIR}/include")

file(GLOB MQ_UT_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/mq/*.cc")
file(GLOB MGM_UT_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/mgm/*.cc")
file(GLOB COMMON_UT_SRCS "${CMAKE_CURRENT_SOURCE_DIR}/common/*.cc")
set(UT_SRCS ${MQ_UT_SRCS} ${MGM_UT_SRCS} ${COMMON_UT_SRCS})
add_executable(eos-unit-tests ${UT_SRCS})

# @todo all these should be moved to gtest and simplified
# set(SOURCE_FILES
#   fst/XrdFstOssFileTest.cc
#   ${CMAKE_SOURCE_DIR}/fst/XrdFstOssFile.cc
#   ${CMAKE_SOURCE_DIR}/fst/tests/TestEnv.cc
#   ${CMAKE_SOURCE_DIR}/fst/XrdFstOss.cc)

target_link_libraries(eos-unit-tests
  gtest_main
  XrdEosMgm-Static
  EosFstIo-Static
  XrdMqClient-Static
  ${XROOTD_SERVER_LIBRARY})
