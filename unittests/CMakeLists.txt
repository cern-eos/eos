# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Jozsef Makai <jmakai@cern.ch> CERN
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2013 CERN/Switzerland                                  *
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

include_directories("${CMAKE_SOURCE_DIR}/googletest-src/googletest/include/")
include_directories("${CMAKE_SOURCE_DIR}/googletest-src/googlemock/include/")
include_directories(${CMAKE_SOURCE_DIR}
                    ${XROOTD_INCLUDE_DIRS})

set(SOURCE_FILES fst/XrdFstOssFileTest.cpp
        ${CMAKE_SOURCE_DIR}/fst/XrdFstOssFile.cc ${CMAKE_SOURCE_DIR}/fst/tests/TestEnv.cc
        ${CMAKE_SOURCE_DIR}/fst/XrdFstOss.cc)
add_executable(eos-unit-tests ${SOURCE_FILES})

set( CMAKE_CXX_FLAGS  "-DGTEST_HAS_TR1_TUPLE=0 -DGTEST_USE_OWN_TR1_TUPLE=0" )
target_link_libraries(eos-unit-tests gtest gtest_main)

target_link_libraries(
        eos-unit-tests
        EosFstIo-Static
        ${XROOTD_SERVER_LIBRARY})