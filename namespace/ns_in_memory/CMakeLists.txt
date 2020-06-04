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

if(CPPUNIT_FOUND)
  add_subdirectory(tests)
endif(CPPUNIT_FOUND)

#-----------------------------------------------------------------------------
# EosNsInMemory-Sources library
#-----------------------------------------------------------------------------
add_library(
  EosNsInMemory-Objects OBJECT

  NsInMemoryPlugin.cc    NsInMemoryPlugin.hh
  FileMD.cc              FileMD.hh
  ContainerMD.cc         ContainerMD.hh
  NamespaceGroup.cc      NamespaceGroup.hh

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
  accounting/FileSystemView.cc  accounting/FileSystemView.hh
  accounting/ContainerAccounting.cc  accounting/ContainerAccounting.hh
  accounting/SyncTimeAccounting.cc   accounting/SyncTimeAccounting.hh

  ${CMAKE_SOURCE_DIR}/common/ShellCmd.cc
  ${CMAKE_SOURCE_DIR}/common/ShellExecutor.cc)

target_link_libraries(EosNsInMemory-Objects PUBLIC
  EosNsCommon
  XROOTD::UTILS
  GOOGLE::SPARSEHASH)

target_compile_definitions(EosNsInMemory-Objects
  PUBLIC -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

set_target_properties(EosNsInMemory-Objects
  PROPERTIES POSITION_INDEPENDENT_CODE True)

#-------------------------------------------------------------------------------
# EosNsInMemory library
#-------------------------------------------------------------------------------
add_library(EosNsInMemory MODULE $<TARGET_OBJECTS:EosNsInMemory-Objects>)

target_link_libraries(EosNsInMemory PUBLIC EosNsInMemory-Objects)

#-------------------------------------------------------------------------------
# EosNsInMemory-Static library
#-------------------------------------------------------------------------------
if (Linux)
  add_library(EosNsInMemory-Static STATIC
    $<TARGET_OBJECTS:EosNsInMemory-Objects>)

  target_link_libraries(EosNsInMemory-Static PUBLIC EosNsCommon-Static)

  set_target_properties(EosNsInMemory-Static
    PROPERTIES POSITION_INDEPENDENT_CODE True)

  add_executable(eos-log-compact progs/EOSLogCompact.cc)
  add_executable(eos-log-repair  progs/EOSLogRepair.cc)

  target_link_libraries(eos-log-compact EosNsInMemory-Static)
  target_link_libraries(eos-log-repair EosNsInMemory-Static)

  install(
    TARGETS eos-log-compact eos-log-repair
    LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
endif()

install(
  TARGETS EosNsInMemory
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
