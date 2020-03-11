# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Elvin-Alin Sindrilaru <esindril@cern.ch> CERN
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2013 CERN/Switzerland                                  *
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
  ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/
  ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/qclient/include
  ${CMAKE_BINARY_DIR}/namespace/ns_quarkdb)  # for the generated protobuf

add_executable(
  eos-fst-test
  main.cc
  TestEnv.cc
  FstFileTest.cc
  MonitorVarPartitionTest.cc
  ${CMAKE_SOURCE_DIR}/common/FileSystem.cc
  ${CMAKE_SOURCE_DIR}/common/TransferQueue.cc
  ${CMAKE_SOURCE_DIR}/common/TransferJob.cc
  ${CMAKE_SOURCE_DIR}/fst/XrdFstOss.cc
  ${CMAKE_SOURCE_DIR}/fst/XrdFstOssFile.cc
  ${CMAKE_SOURCE_DIR}/fst/checksum/CRC32C.hh
  ${CMAKE_SOURCE_DIR}/fst/checksum/CheckSum.cc
  ${CMAKE_SOURCE_DIR}/fst/checksum/CheckSum.hh)

target_link_libraries(eos-fst-test PRIVATE
  gtest
  qclient
  gmock_main
  XrdMqClient
  EosFstIo-Static)

target_compile_definitions(
  eos-fst-test PRIVATE
  -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64)

install(TARGETS eos-fst-test
  DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
  PERMISSIONS OWNER_READ OWNER_EXECUTE
  GROUP_READ GROUP_EXECUTE
  WORLD_READ WORLD_EXECUTE)
