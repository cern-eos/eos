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
# EosNsOnRamCloudTests library
#-------------------------------------------------------------------------------
add_library(
  EosNsOnRamcloudTests MODULE
  ContainerMDSvcTest.cc
  FileMDSvcTest.cc
  ../RamCloudClient.cc
#  HierarchicalViewTest.cc
#  FileSystemViewTest.cc
#  OtherTests.cc
)

target_link_libraries(
  EosNsOnRamcloudTests
  EosNsOnRamcloud-Static
  ${CPPUNIT_LIBRARIES})

#add_executable(eosnsbench EosNamespaceBenchmark.cc)
#target_link_libraries(eosnsbench EosNsOnRedis eosCommon-Static)
#set_target_properties(eosnsbench PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64")

#install(
#  TARGETS
#  eosnsbench
#  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
#  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
#  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
