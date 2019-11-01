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
 ${CMAKE_BINARY_DIR}
 ${CMAKE_SOURCE_DIR}
 ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb
 ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/qclient/include
 ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/qclient/src
 ${SPARSEHASH_INCLUDE_DIR}
 ${PROTOBUF_INCLUDE_DIRS}
 ${XROOTD_INCLUDE_DIR}
 ${FOLLY_INCLUDE_DIRS})

add_subdirectory(tests)

#-------------------------------------------------------------------------------
# EosNsQuarkdb library
#-------------------------------------------------------------------------------
add_library(
  EosNsQuarkdb MODULE
  NsQuarkdbPlugin.cc     NsQuarkdbPlugin.hh)

add_dependencies(EosNsQuarkdb EosNsQuarkdbProto-Objects)

target_link_libraries(
  EosNsQuarkdb
  EosNsCommon
  qclient
  rocksdb
  dl
  ${FOLLY_LIBRARIES}
  ${Z_LIBRARY}
  ${BZ2_LIBRARY}
  ${UUID_LIBRARIES}
  ${XROOTD_UTILS_LIBRARY}
  ${GLIBC_RT_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT})

install(
  TARGETS EosNsQuarkdb
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

#-------------------------------------------------------------------------------
# eos_convert_mem_to_kv executable
#-------------------------------------------------------------------------------
add_executable(eos-ns-convert
  tools/ConvertMemToKV.cc
  persistency/RequestBuilder.cc      persistency/RequestBuilder.hh
  ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/BackendClient.cc)

target_link_libraries(eos-ns-convert
  jemalloc
  qclient
  EosCommon
  EosNsInMemory-Static
  ${FOLLY_LIBRARIES})

add_executable(eos-ns-convert-to-locality-hashes tools/EosConvertToLocalityHashes.cc)

target_link_libraries(eos-ns-convert-to-locality-hashes
  EosNsCommon-Static
  ${PROTOBUF_LIBRARY})

add_executable(eos-ns-inspect tools/InspectionTool.cc)

target_link_libraries(eos-ns-inspect
  EosNsCommon-Static)

add_executable(eos-fid-to-path tools/Fid2PathTool.cc)

target_link_libraries(eos-fid-to-path
  EosNsCommon-Static)

install(TARGETS eos-ns-convert eos-ns-convert-to-locality-hashes eos-ns-inspect eos-fid-to-path
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
