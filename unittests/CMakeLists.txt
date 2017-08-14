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

add_subdirectory(common)

include_directories(
  ${CMAKE_SOURCE_DIR} ${XROOTD_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/googletest-src/googletest/include/
  ${CMAKE_SOURCE_DIR}/googletest-src/googlemock/include/)

set(SOURCE_FILES
  fst/XrdFstOssFileTest.cc
  ${CMAKE_SOURCE_DIR}/fst/XrdFstOssFile.cc
  ${CMAKE_SOURCE_DIR}/fst/tests/TestEnv.cc
  ${CMAKE_SOURCE_DIR}/fst/XrdFstOss.cc)
add_executable(eos-unit-tests ${SOURCE_FILES})

set( CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -DGTEST_USE_OWN_TR1_TUPLE=0" )
target_link_libraries(eos-unit-tests gtest gtest_main)

target_link_libraries(
  eos-unit-tests
  EosFstIo-Static
  ${XROOTD_SERVER_LIBRARY})
