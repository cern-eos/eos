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
 ${CMAKE_SOURCE_DIR}
 ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb
 ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/qclient/include
 ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/qclient/src
 ${SPARSEHASH_INCLUDE_DIR}
 ${PROTOBUF_INCLUDE_DIRS}
 ${XROOTD_INCLUDE_DIR}
 ${CMAKE_CURRENT_BINARY_DIR}) # for the generated proto files


if(BUILD_TESTS)
  add_subdirectory(tests)
endif(BUILD_TESTS)

#-------------------------------------------------------------------------------
# Generate all protocol buffer files
#-------------------------------------------------------------------------------
PROTOBUF_GENERATE_CPP(FMD_SRCS FMD_HDRS proto/FileMd.proto)
PROTOBUF_GENERATE_CPP(CMD_SRCS CMD_HDRS proto/ContainerMd.proto)
set(NS_PROTO_SRCS ${FMD_SRCS} ${CMD_SRCS})
set(NS_PROTO_HDRS ${FMD_HDRS} ${CMD_HDRS})
set_source_files_properties(
 ${NS_PROTO_SRCS}
 ${NS_PROTO_HDRS}
 PROPERTIES GENERATED 1)

#-----------------------------------------------------------------------------
# Build qclient static library
#-----------------------------------------------------------------------------
add_subdirectory(qclient)

#-------------------------------------------------------------------------------
# EosNsQuarkdb-Objects library
#-------------------------------------------------------------------------------
add_library(
  EosNsQuarkdb-Objects OBJECT

  FileMD.cc              FileMD.hh
  ContainerMD.cc         ContainerMD.hh
  BackendClient.cc       BackendClient.hh
  LRU.hh

  ${NS_PROTO_SRCS}       ${NS_PROTO_HDRS}
  persistency/ContainerMDSvc.cc      persistency/ContainerMDSvc.hh
  persistency/FileMDSvc.cc           persistency/FileMDSvc.hh
  persistency/NextInodeProvider.cc   persistency/NextInodeProvider.hh

  flusher/MetadataFlusher.cc         flusher/MetadataFlusher.hh

  views/HierarchicalView.cc          views/HierarchicalView.hh
  accounting/QuotaStats.cc           accounting/QuotaStats.hh
  accounting/FileSystemView.cc       accounting/FileSystemView.hh
  accounting/ContainerAccounting.cc  accounting/ContainerAccounting.hh
  accounting/SyncTimeAccounting.cc   accounting/SyncTimeAccounting.hh)

set_target_properties(
  EosNsQuarkdb-Objects
  PROPERTIES
  POSITION_INDEPENDENT_CODE True)

#-------------------------------------------------------------------------------
# EosNsQuarkdb library
#-------------------------------------------------------------------------------
add_library(
  EosNsQuarkdb MODULE
  $<TARGET_OBJECTS:EosNsQuarkdb-Objects>
  NsQuarkdbPlugin.cc     NsQuarkdbPlugin.hh)

target_link_libraries(
  EosNsQuarkdb
  EosNsCommon
  eosCommon
  EosCrc32c-Static
  qclient
  bz2
  rocksdb
  ${Z_LIBRARY}
  ${UUID_LIBRARIES}
  ${XROOTD_UTILS_LIBRARY}
  ${PROTOBUF_LIBRARY}
  ${GLIBC_RT_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT})

#-------------------------------------------------------------------------------
# EosNsQuarkdb-Static library
#-------------------------------------------------------------------------------
if (Linux)
  add_library(
    EosNsQuarkdb-Static STATIC
    $<TARGET_OBJECTS:EosNsQuarkdb-Objects>
    NsQuarkdbPlugin.cc     NsQuarkdbPlugin.hh)

  target_link_libraries(
    EosNsQuarkdb-Static
    EosNsCommon-Static
    eosCommon-Static
    EosCrc32c-Static
    qclient
    bz2
    rocksdb
    ${UUID_LIBRARIES}
    ${Z_LIBRARY_STATIC}
    ${XROOTD_UTILS_LIBRARY}
    ${PROTOBUF_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT})

  set_target_properties(
    EosNsQuarkdb-Static
    PROPERTIES
    POSITION_INDEPENDENT_CODE True)

  install(
    TARGETS EosNsQuarkdb-Static
    LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
endif()

install(
  TARGETS EosNsQuarkdb
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

#-------------------------------------------------------------------------------
# convert_mem_to_kv executable
#-------------------------------------------------------------------------------
add_executable(
  convert_mem_to_kv
  tools/ConvertMemToKV.cc
  ${NS_PROTO_SRCS}
  ${NS_PROTO_HDRS}
  accounting/ContainerAccounting.cc  accounting/ContainerAccounting.hh
  accounting/SyncTimeAccounting.cc   accounting/SyncTimeAccounting.hh
  ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/BackendClient.cc)

target_link_libraries(
  convert_mem_to_kv
  jemalloc
  qclient
  eosCommon
  EosCrc32c-Static
  EosNsInMemory-Static
  ${PROTOBUF_LIBRARY})

install(
  TARGETS convert_mem_to_kv
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
