#-------------------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Lukasz Janyst - CERN
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
 ${CMAKE_SOURCE_DIR}/namespace/ns_on_redis/
 ${SPARSEHASH_INCLUDE_DIR}
 ${XROOTD_INCLUDE_DIR})

if(CPPUNIT_FOUND)
  add_subdirectory(tests)
endif(CPPUNIT_FOUND)

#-----------------------------------------------------------------------------
# EosNsOnRedis library sources
#-----------------------------------------------------------------------------
set(EOS_NS_REDIS_SRCS
  NsOnRedisPlugin.cc     NsOnRedisPlugin.hh
  FileMD.cc              FileMD.hh
  ContainerMD.cc         ContainerMD.hh
  RedisClient.cc         RedisClient.hh
  LRU.hh

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
# EosNsOnRedis library
#-------------------------------------------------------------------------------
add_library(
  EosNsOnRedis MODULE
  ${EOS_NS_REDIS_SRCS})

target_link_libraries(
  EosNsOnRedis
  EosNsCommon
  eosCommon
  ${Z_LIBRARY}
  ${UUID_LIBRARIES}
  ${REDOX_LIBRARIES}
  ${XROOTD_UTILS_LIBRARY}
  ${GLIBC_RT_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT})

#-------------------------------------------------------------------------------
# EosNsOnRedis-Static library
#-------------------------------------------------------------------------------
if (Linux)
  add_library(
    EosNsOnRedis-Static STATIC
    ${EOS_NS_REDIS_SRCS})

  target_link_libraries(
    EosNsOnRedis-Static
    EosNsCommon-Static
    eosCommon-Static
    ${UUID_LIBRARIES}
    ${REDOX_LIBRARIES}
    ${LIBEV_LIBRARIES}
    ${Z_LIBRARY_STATIC}
    ${XROOTD_UTILS_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT})

  set_target_properties(
    EosNsOnRedis-Static
    PROPERTIES
    POSITION_INDEPENDENT_CODE True)

  install(
    TARGETS EosNsOnRedis-Static
    LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
endif()

install(
  TARGETS EosNsOnRedis
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

#-------------------------------------------------------------------------------
# convert_mem_to_kv executable
#-------------------------------------------------------------------------------
add_executable(
  convert_mem_to_kv
  ConvertMemToKV.cc
  ${CMAKE_SOURCE_DIR}/namespace/ns_on_redis/RedisClient.cc)

target_link_libraries(
  convert_mem_to_kv
  EosNsInMemory-Static
  ${LIBEV_LIBRARIES}
  ${REDOX_LIBRARIES})

install(
  TARGETS convert_mem_to_kv
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
