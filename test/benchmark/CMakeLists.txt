# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Elvin-Alin Sindrilaru - CERN
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
 ${PROTOBUF_INCLUDE_DIRS}
 ${XROOTD_INCLUDE_DIRS}
 ${UUID_INCLUDE_DIRS})

#-------------------------------------------------------------------------------
# Generate protocol buffer files
#-------------------------------------------------------------------------------
PROTOBUF_GENERATE_CPP(CONFIG_SRCS CONFIG_HDRS PROTOFILES ConfigProto.proto)
PROTOBUF_GENERATE_CPP(RESULT_SRCS RESULT_HDRS PROTOFILES ResultProto.proto)
set_source_files_properties(
  ${CONFIG_SRCS} ${CONFIG_HDRS} ${RESULT_SRCS} ${RESULT_HDRS}
  PROPERTIES GENERATED 1)

#-------------------------------------------------------------------------------
# eos-benchmark executable
#-------------------------------------------------------------------------------
add_executable(
  eos-benchmark
  eosbenchmark.cc     eosbenchmark.hh
  Configuration.cc    Configuration.hh
  DirEos.cc           DirEos.hh
  FileEos.cc          FileEos.hh
  Result.cc           Result.hh
  ProtoIo.cc          ProtoIo.hh
  ${CONFIG_SRCS}      ${CONFIG_HDRS}
  ${RESULT_SRCS}      ${RESULT_HDRS}
  ${CMAKE_SOURCE_DIR}/common/StringConversion.cc
  ${CMAKE_SOURCE_DIR}/common/StringConversion.hh
  ${CMAKE_SOURCE_DIR}/fst/io/FileIoPlugin.cc)

target_link_libraries(
  eos-benchmark
  EosFstIo-Static
  ${XROOTD_CL_LIBRARY}
  ${PROTOBUF_LIBRARIES}
  ${UUID_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT})

install(
  PROGRAMS eos-benchmark
  DESTINATION ${CMAKE_INSTALL_SBINDIR})
