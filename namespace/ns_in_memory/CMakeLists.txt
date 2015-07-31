#-------------------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Lukasz Janyst - CERN
#-------------------------------------------------------------------------------

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

include_directories(
 ${CMAKE_SOURCE_DIR}
 ${CMAKE_SOURCE_DIR}/namespace/
 ${SPARSEHASH_INCLUDE_DIR}
 ${XROOTD_INCLUDE_DIR})

link_directories(${XROOTD_LIB_DIR})

if(CPPUNIT_FOUND)
  add_subdirectory(tests)
endif(CPPUNIT_FOUND)

#-----------------------------------------------------------------------------
# EosNsInMemory library sources
#-----------------------------------------------------------------------------
set(EOS_NS_MEMORY_SRCS
  NsInMemoryPlugin.cc    NsInMemoryPlugin.hh
  FileMD.cc              FileMD.hh
  ContainerMD.cc         ContainerMD.hh

  persistency/ChangeLogConstants.hh
  persistency/ChangeLogConstants.cc
  persistency/ChangeLogContainerMDSvc.hh
  persistency/ChangeLogContainerMDSvc.cc
  persistency/ChangeLogFile.hh
  persistency/ChangeLogFile.cc
  persistency/ChangeLogFileMDSvc.hh
  persistency/ChangeLogFileMDSvc.cc
  persistency/LogManager.hh
  persistency/LogManager.cc

  views/HierarchicalView.cc     views/HierarchicalView.hh
  accounting/QuotaStats.cc      accounting/QuotaStats.hh
  accounting/FileSystemView.cc  accounting/FileSystemView.hh)

add_library(
  EosNsInMemory MODULE
  ${EOS_NS_MEMORY_SRCS})

add_library(
  EosNsInMemory-Static STATIC 
  ${EOS_NS_MEMORY_SRCS}
  progs/EOSLogCompact.cc
  progs/EOSLogRepair.cc)

if (Linux)
  target_link_libraries(EosNsInMemory pthread z rt XrdUtils EosNsCommon)
  target_link_libraries(EosNsInMemory-Static pthread z rt XrdUtils EosNsCommon)

  set_target_properties(
    EosNsInMemory-Static 
    PROPERTIES COMPILE_FLAGS -fPIC)
else (Linux)
  target_link_libraries(EosNsInMemory pthread z XrdUtils EosNsCommon)
  target_link_libraries(EosNsInMemory-Static pthread z XrdUtils EosNsCommon)
endif (Linux)

add_executable(eos-log-compact progs/EOSLogCompact.cc)
add_executable(eos-log-repair  progs/EOSLogRepair.cc)

target_link_libraries(eos-log-compact EosNsInMemory-Static)
target_link_libraries(eos-log-repair EosNsInMemory-Static)

install(
  TARGETS eos-log-compact eos-log-repair EosNsInMemory
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

