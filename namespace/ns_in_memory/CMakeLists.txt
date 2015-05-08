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

set(EOS_MEMORY_NS_SRCS
  ${CMAKE_SOURCE_DIR}/namespace/MDException.hh
  ${CMAKE_SOURCE_DIR}/namespace/Constants.cc           
  ${CMAKE_SOURCE_DIR}/namespace/Constants.hh

  ${CMAKE_SOURCE_DIR}/namespace/utils/DataHelper.cc
  ${CMAKE_SOURCE_DIR}/namespace/utils/Descriptor.cc
  ${CMAKE_SOURCE_DIR}/namespace/utils/ThreadUtils.cc
  ${CMAKE_SOURCE_DIR}/namespace/utils/Buffer.hh

  ${CMAKE_SOURCE_DIR}/namespace/interface/IView.hh
  ${CMAKE_SOURCE_DIR}/namespace/interface/IFileMDSvc.hh
  ${CMAKE_SOURCE_DIR}/namespace/interface/IContainerMDSvc.hh
  ${CMAKE_SOURCE_DIR}/namespace/interface/IFileMD.hh
  ${CMAKE_SOURCE_DIR}/namespace/interface/IContainerMD.hh

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
  eosMemoryNs SHARED 
  ${EOS_MEMORY_NS_SRCS})

add_library(
  eosMemoryNs-Static STATIC 
  ${EOS_MEMORY_NS_SRCS}
  progs/EOSLogCompact.cc
  progs/EOSLogRepair.cc)

if (Linux)
  target_link_libraries(eosMemoryNs pthread z rt XrdUtils)
  target_link_libraries(eosMemoryNs-Static pthread z rt XrdUtils)
else (Linux)
  target_link_libraries(eosMemoryNs pthread z XrdUtils)
  target_link_libraries(eosMemoryNs-Static pthread z XrdUtils)
endif (Linux)

add_executable(eos-log-compact progs/EOSLogCompact.cc)
add_executable(eos-log-repair  progs/EOSLogRepair.cc)

target_link_libraries(eos-log-compact eosMemoryNs-Static)
target_link_libraries(eos-log-repair eosMemoryNs-Static)

if (Linux)
  set_target_properties(
    eosMemoryNs-Static 
    PROPERTIES COMPILE_FLAGS -fPIC)
endif (Linux)

install(
  TARGETS eos-log-compact eos-log-repair
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
