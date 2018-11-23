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

if(BUILD_TESTS)
  add_subdirectory(tests)
endif(BUILD_TESTS)

#-------------------------------------------------------------------------------
# EosNsQuarkdb library
#-------------------------------------------------------------------------------
add_library(
  EosNsQuarkdb MODULE
  NsQuarkdbPlugin.cc     NsQuarkdbPlugin.hh)

add_dependencies(EosNsQuarkdb EosProtobuf)

target_link_libraries(
  EosNsQuarkdb
  EosNsCommon
  eosCommon
  EosCrc32c-Static
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
  ${FOLLY_LIBRARIES}
  eosCommon
  EosCrc32c-Static
  EosNsInMemory-Static
  EosProtobuf)

add_executable(eos-dump-proto-md tools/EosDumpProtoMd.cc)

target_link_libraries(eos-dump-proto-md
  EosCrc32c-Static
  EosNsCommon-Static
  ${PROTOBUF_LIBRARY})

add_executable(eos-ns-convert-to-locality-hashes tools/EosConvertToLocalityHashes.cc)

target_link_libraries(eos-ns-convert-to-locality-hashes
  EosNsCommon-Static
  EosCrc32c-Static
  ${PROTOBUF_LIBRARY})

install(TARGETS eos-ns-convert eos-dump-proto-md eos-ns-convert-to-locality-hashes
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
