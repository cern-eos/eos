#-------------------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Elvin Sindrilaru <esindril@cern.ch>
#-------------------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2015 CERN/Switzerland                                  *
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

#-------------------------------------------------------------------------------
# EosNsCommon-Sources library
#-------------------------------------------------------------------------------
include_directories(
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
  ${XROOTD_INCLUDE_DIR}
  ${PROTOBUF_INCLUDE_DIRS}
  ${JSONCPP_INCLUDE_DIRS}
  ${FOLLY_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/qclient/include)

add_library(
  EosNsCommon-Objects OBJECT

  Constants.cc
  MDException.cc
  MDException.hh

  PermissionHandler.cc                PermissionHandler.hh
  Prefetcher.cc                       Prefetcher.hh
  Resolver.cc                         Resolver.hh

  # Common helper classes, common to both namespaces
  common/QuotaNodeCore.cc             common/QuotaNodeCore.hh

  # Namespace interface
  interface/IView.hh
  interface/IFsView.hh
  interface/IFileMDSvc.hh
  interface/IContainerMDSvc.hh
  interface/Identifiers.hh
  interface/IFileMD.hh
  interface/IContainerMD.hh
  interface/IChLogContainerMDSvc.hh
  interface/IChLogFileMDSvc.hh

  # Namespace utils
  utils/DataHelper.cc
  utils/Descriptor.cc
  utils/FileListRandomPicker.cc
  utils/ThreadUtils.cc
  utils/TestHelpers.cc
  utils/Buffer.hh

  utils/Etag.cc                       utils/Etag.hh

  # non-loadable classes used in QDB namespace
  ns_quarkdb/accounting/ContainerAccounting.cc            ns_quarkdb/accounting/ContainerAccounting.hh
  ns_quarkdb/accounting/SyncTimeAccounting.cc             ns_quarkdb/accounting/SyncTimeAccounting.hh
  ns_quarkdb/accounting/FileSystemHandler.cc              ns_quarkdb/accounting/FileSystemHandler.hh
  ns_quarkdb/accounting/FileSystemView.cc                 ns_quarkdb/accounting/FileSystemView.hh
  ns_quarkdb/accounting/QuotaStats.cc                     ns_quarkdb/accounting/QuotaStats.hh
                                                          ns_quarkdb/accounting/SetChangeList.hh

  ns_quarkdb/explorer/NamespaceExplorer.cc                ns_quarkdb/explorer/NamespaceExplorer.hh
  ns_quarkdb/flusher/MetadataFlusher.cc                   ns_quarkdb/flusher/MetadataFlusher.hh

  ns_quarkdb/inspector/ContainerScanner.cc                ns_quarkdb/inspector/ContainerScanner.hh
  ns_quarkdb/inspector/FileScanner.cc                     ns_quarkdb/inspector/FileScanner.hh
  ns_quarkdb/inspector/Inspector.cc                       ns_quarkdb/inspector/Inspector.hh
  ns_quarkdb/inspector/OutputSink.cc                      ns_quarkdb/inspector/OutputSink.hh
  ns_quarkdb/inspector/Printing.cc                        ns_quarkdb/inspector/Printing.hh

  ns_quarkdb/persistency/ContainerMDSvc.cc                ns_quarkdb/persistency/ContainerMDSvc.hh
  ns_quarkdb/persistency/FileMDSvc.cc                     ns_quarkdb/persistency/FileMDSvc.hh
  ns_quarkdb/persistency/FileSystemIterator.cc            ns_quarkdb/persistency/FileSystemIterator.hh
  ns_quarkdb/persistency/MetadataFetcher.cc               ns_quarkdb/persistency/MetadataFetcher.hh
  ns_quarkdb/persistency/MetadataProvider.cc              ns_quarkdb/persistency/MetadataProvider.hh
  ns_quarkdb/persistency/MetadataProviderShard.cc         ns_quarkdb/persistency/MetadataProviderShard.hh
  ns_quarkdb/persistency/NextInodeProvider.cc             ns_quarkdb/persistency/NextInodeProvider.hh
  ns_quarkdb/persistency/RequestBuilder.cc                ns_quarkdb/persistency/RequestBuilder.hh
  ns_quarkdb/persistency/Serialization.cc                 ns_quarkdb/persistency/Serialization.hh
  ns_quarkdb/persistency/UnifiedInodeProvider.cc          ns_quarkdb/persistency/UnifiedInodeProvider.hh

  ns_quarkdb/utils/QuotaRecomputer.cc                     ns_quarkdb/utils/QuotaRecomputer.hh

  ns_quarkdb/views/HierarchicalView.cc                    ns_quarkdb/views/HierarchicalView.hh

  ns_quarkdb/BackendClient.cc                             ns_quarkdb/BackendClient.hh
  ns_quarkdb/CacheRefreshListener.cc                      ns_quarkdb/CacheRefreshListener.hh
  ns_quarkdb/ContainerMD.cc                               ns_quarkdb/ContainerMD.hh
  ns_quarkdb/FileMD.cc                                    ns_quarkdb/FileMD.hh
                                                          ns_quarkdb/LRU.hh
  ns_quarkdb/NamespaceGroup.cc                            ns_quarkdb/NamespaceGroup.hh
  ns_quarkdb/VersionEnforcement.cc                        ns_quarkdb/VersionEnforcement.hh)

add_dependencies(EosNsCommon-Objects EosCliProto-Objects)

target_link_libraries(EosNsCommon-Objects PUBLIC
  ${JSONCPP_LIBRARIES})

set_target_properties(
  EosNsCommon-Objects
  PROPERTIES
  POSITION_INDEPENDENT_CODE True)

add_library(EosNsCommon SHARED $<TARGET_OBJECTS:EosNsCommon-Objects>)


target_include_directories(
  EosNsCommon PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/utils/fmt>
  $<INSTALL_INTERFACE:include>)

target_link_libraries(
  EosNsCommon PUBLIC
  EosCrc32c-Static
  EosCommon
  qclient
  rocksdb
  ${FOLLY_LIBRARIES}
  ${Z_LIBRARY} fmt)

set_target_properties(
  EosNsCommon
  PROPERTIES
  VERSION ${VERSION}
  SOVERSION ${VERSION_MAJOR})

install(
  TARGETS EosNsCommon
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

#-------------------------------------------------------------------------------
# EosNsCommon-Static library
#-------------------------------------------------------------------------------
if (Linux)
  add_library(EosNsCommon-Static STATIC $<TARGET_OBJECTS:EosNsCommon-Objects> )

  target_include_directories(
    EosNsCommon-Static PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/utils/fmt>
    $<INSTALL_INTERFACE:include>)

  target_link_libraries(
    EosNsCommon-Static PUBLIC
    EosCrc32c-Static
    EosCommon-Static
    fmt
    qclient
    rocksdb
    ${FOLLY_LIBRARIES}
    ${Z_LIBRARY_STATIC})

  set_target_properties(
    EosNsCommon-Static
    PROPERTIES
    POSITION_INDEPENDENT_CODE True)
endif ()

#-------------------------------------------------------------------------------
# text-runner used for running CPPUnit tests
#-------------------------------------------------------------------------------
if (CPPUNIT_FOUND)
  add_executable(
    text-runner
    utils/TextRunner.cc)

  target_include_directories(
    text-runner PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
    $<BUILD_INTERFACE:${CPPUNIT_INCLUDE_DIRS}>
    $<INSTALL_INTERFACE:include>)

  target_link_libraries(
    text-runner PUBLIC
    ${CPPUNIT_LIBRARIES}
    ${GLIBC_DL_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT})
endif()

add_subdirectory(ns_in_memory)
add_subdirectory(ns_quarkdb)
