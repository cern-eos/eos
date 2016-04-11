#-------------------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Elvin Sindrilaru <esindril@.cern.ch>
#-------------------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2016 CERN/Switzerland                                  *
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
 ${CMAKE_SOURCE_DIR}/namespace/ns_on_ramcloud/
 ${SPARSEHASH_INCLUDE_DIR}
 ${XROOTD_INCLUDE_DIR}
 ${RAMCLOUD_INCLUDE_DIR})

if(CPPUNIT_FOUND)
  add_subdirectory(tests)
endif(CPPUNIT_FOUND)

#-----------------------------------------------------------------------------
# EosNsOnRamcloud library sources
#-----------------------------------------------------------------------------
set(EOS_NS_RAMCLOUD_SRCS
  NsOnRamcloudPlugin.cc  NsOnRamcloudPlugin.hh
  FileMD.cc              FileMD.hh
  ContainerMD.cc         ContainerMD.hh
  RamCloudClient.cc      RamCloudClient.hh

  persistency/ContainerMDSvc.hh
  persistency/ContainerMDSvc.cc
  persistency/FileMDSvc.hh
  persistency/FileMDSvc.cc

  views/HierarchicalView.cc          views/HierarchicalView.hh
  accounting/QuotaStats.cc           accounting/QuotaStats.hh
  accounting/FileSystemView.cc       accounting/FileSystemView.hh
  accounting/ContainerAccounting.cc  accounting/ContainerAccounting.hh
  accounting/SyncTimeAccounting.cc   accounting/SyncTimeAccounting.hh

  ${CMAKE_SOURCE_DIR}/common/ShellCmd.cc
  ${CMAKE_SOURCE_DIR}/common/ShellExecutor.cc)

#-------------------------------------------------------------------------------
# EosNsOnRamcloud library
#-------------------------------------------------------------------------------
add_library(
  EosNsOnRamcloud MODULE
  ${EOS_NS_RAMCLOUD_SRCS})

target_link_libraries(
  EosNsOnRamcloud
  EosNsCommon
  ${Z_LIBRARY}
  ${UUID_LIBRARIES}
  ${RAMCLOUD_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${GLIBC_RT_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT})

#-------------------------------------------------------------------------------
# EosNsOnRamcloud-Static library
#-------------------------------------------------------------------------------
if (Linux)
  add_library(
    EosNsOnRamcloud-Static STATIC
    ${EOS_NS_RAMCLOUD_SRCS})

  target_link_libraries(
    EosNsOnRamcloud-Static
    EosNsCommon-Static
    ${UUID_LIBRARIES}
    ${RAMCLOUD_LIBRARIES}
    ${LIBEV_LIBRARIES}
    ${Z_LIBRARY_STATIC}
    ${XROOTD_UTILS_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT})

  set_target_properties(
    EosNsOnRamcloud-Static
    PROPERTIES
    POSITION_INDEPENDENT_CODE True)

  install(
    TARGETS EosNsOnRamcloud-Static
    LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
endif()

install(
  TARGETS EosNsOnRamcloud
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

add_executable(
  ramcloud_cli
  RamCloudCLI.cc)

target_link_libraries(
  ramcloud_cli
  ${RAMCLOUD_LIBRARIES}
  /usr/lib64/libboost_program_options.so)
