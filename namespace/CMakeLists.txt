# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Lukasz Janyst - CERN
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

include_directories(
 ${CMAKE_SOURCE_DIR}
 ${XROOTD_INCLUDE_DIRS}
 ${SPARSEHASH_INCLUDE_DIRS})

if(CPPUNIT_FOUND)
  add_subdirectory(tests)
endif(CPPUNIT_FOUND)

#-------------------------------------------------------------------------------
# eosNamespace library
#-------------------------------------------------------------------------------
set(
  EOSNAMESPACE_SRCS
  accounting/FileSystemView.hh
  ContainerMD.hh
  FileMD.hh
  IContainerMDSvc.hh
  IFileMDSvc.hh
  IView.hh
  MDException.hh
  Constants.hh
  Constants.cc
  persistency/Buffer.hh
  persistency/ChangeLogConstants.hh
  persistency/ChangeLogContainerMDSvc.hh
  persistency/ChangeLogFile.hh
  persistency/ChangeLogFileMDSvc.hh
  persistency/LogManager.hh
  utils/DataHelper.hh
  utils/Descriptor.hh
  utils/DisplayHelper.hh
  utils/PathProcessor.hh
  utils/SmartPtrs.hh
  views/HierarchicalView.hh
  accounting/FileSystemView.cc
  accounting/FileSystemView.hh
  ContainerMD.cc
  FileMD.cc
  persistency/ChangeLogConstants.cc
  persistency/ChangeLogContainerMDSvc.cc
  persistency/ChangeLogFile.cc
  persistency/ChangeLogFileMDSvc.cc
  persistency/LogManager.cc
  utils/DataHelper.cc
  utils/Descriptor.cc
  utils/ThreadUtils.cc
  utils/ThreadUtils.hh
  views/HierarchicalView.cc
  accounting/QuotaStats.hh
  accounting/QuotaStats.cc)

add_library(eosNamespace SHARED ${EOSNAMESPACE_SRCS})
add_library(eosNamespace-Static STATIC ${EOSNAMESPACE_SRCS})

target_link_libraries(
  eosNamespace-Static
  ${Z_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT})

if (Linux)
  set_target_properties(eosNamespace-Static PROPERTIES COMPILE_FLAGS -fPIC)

  target_link_libraries(
    eosNamespace
    ${Z_LIBRARY}
    ${GLIBC_RT_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT})
else()
  target_link_libraries(
    eosNamespace
    ${Z_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT})
endif()

#-------------------------------------------------------------------------------
# Namespace related executables
#-------------------------------------------------------------------------------
add_executable(eos-log-compact progs/EOSLogCompact.cc)
add_executable(eos-log-repair  progs/EOSLogRepair.cc)

target_link_libraries(eos-log-compact eosNamespace-Static)
target_link_libraries(eos-log-repair eosNamespace-Static)

install(
  TARGETS eos-log-compact eos-log-repair
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
