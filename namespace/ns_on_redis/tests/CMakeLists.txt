# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Andreas-Joachim Peters - CERN
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2011 CERN/Switzerland                                  *
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

include_directories(${CPPUNIT_INCLUDE_DIRS})

#-------------------------------------------------------------------------------
# EosNsOnRedisTests library
#-------------------------------------------------------------------------------
add_library(
  EosNsOnRedisTests SHARED
  ChangeLogContainerMDSvcTest.cc
  ChangeLogFileMDSvcTest.cc
  ChangeLogTest.cc
  FileSystemViewTest.cc
  HierarchicalViewTest.cc
  HierarchicalSlaveTest.cc
  OtherTests.cc
  ${CMAKE_SOURCE_DIR}/namespace/utils/TestHelpers.hh
  ${CMAKE_SOURCE_DIR}/namespace/utils/TestHelpers.cc)

target_link_libraries(
  EosNsOnRedisTests
  ${Z_LIBRARY}
  ${CPPUNIT_LIBRARIES}
  EosNsOnRedis-Static)

set_target_properties(
  EosNsOnRedisTests
  PROPERTIES
  VERSION ${VERSION}
  SOVERSION ${VERSION_MAJOR}
  INTERFACE_LINK_LIBRARIES ""
  POSITION_INDEPENDENT_CODE True)
