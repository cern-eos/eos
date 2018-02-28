#-------------------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Jozsef Makai - CERN
#-------------------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2018 CERN/Switzerland                                  *
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
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PROTOBUF_INCLUDE_DIRS})

#-------------------------------------------------------------------------------
# Generate protobol buffer object for the Namespace
#-------------------------------------------------------------------------------
PROTOBUF_GENERATE_CPP(FMD_SRCS FMD_HDRS namespace/ns_quarkdb/FileMd.proto)
PROTOBUF_GENERATE_CPP(CMD_SRCS CMD_HDRS namespace/ns_quarkdb/ContainerMd.proto)
set(NS_PROTO_SRCS ${FMD_SRCS} ${CMD_SRCS})
set(NS_PROTO_HDRS ${FMD_HDRS} ${CMD_HDRS})
set_source_files_properties(
  ${NS_PROTO_SRCS}
  ${NS_PROTO_HDRS}
  PROPERTIES GENERATED 1)

add_library(
  EosNsQuarkdbProto-Objects OBJECT
  ${NS_PROTO_SRCS}  ${NS_PROTO_HDRS})

set_target_properties(EosNsQuarkdbProto-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# Generate protobol buffer object for FST
#-------------------------------------------------------------------------------
PROTOBUF_GENERATE_CPP(FMDBASE_SRCS FMDBASE_HDRS fst/FmdBase.proto)
set(FMDBASE_SRCS ${FMDBASE_SRCS} PARENT_SCOPE)
set(FMDBASE_HDRS ${FMDBASE_HDRS} PARENT_SCOPE)
set_source_files_properties(
  ${FMDBASE_SRCS}
  ${FMDBASE_HDRS}
  PROPERTIES GENERATED TRUE)

add_library(EosFstProto-Objects OBJECT
  ${FMDBASE_SRCS}
  ${FMDBASE_HDRS})

set_target_properties(EosFstProto-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# Generate protobol buffer object for the CLI
#-------------------------------------------------------------------------------
PROTOBUF_GENERATE_CPP(ACL_SRCS ACL_HDRS common/cli_proto/Acl.proto)
PROTOBUF_GENERATE_CPP(NS_SRCS NS_HDRS common/cli_proto/Ns.proto)
PROTOBUF_GENERATE_CPP(DRAIN_SRCS DRAIN_HDRS common/cli_proto/Drain.proto)
PROTOBUF_GENERATE_CPP(FIND_SRCS FIND_HDRS common/cli_proto/Find.proto)
PROTOBUF_GENERATE_CPP(FS_SRCS FS_HDRS common/cli_proto/Fs.proto)
PROTOBUF_GENERATE_CPP(RM_SRCS RM_HDRS common/cli_proto/Rm.proto)
PROTOBUF_GENERATE_CPP(REQ_SRCS REQ_HDRS common/cli_proto/ConsoleRequest.proto)
PROTOBUF_GENERATE_CPP(REP_SRCS REP_HDRS common/cli_proto/ConsoleReply.proto)

set(CLI_PROTO_SRCS
  ${NS_SRCS} ${ACL_SRCS} ${DRAIN_SRCS} ${FIND_SRCS} ${REQ_SRCS} ${REP_SRCS}
  ${FS_SRCS} ${RM_SRCS})

set(CLI_PROTO_HDRS
  ${NS_HDRS} ${ACL_HDRS} ${DRAIN_HDRS} ${FIND_HDRS} ${REQ_HDRS} ${REP_HDRS}
  ${FS_HDRS} ${RM_HDRS})

set_source_files_properties(
  ${CLI_PROTO_SRCS} ${CLI_PROTO_HDRS}
  PROPERTIES GENERATED 1)

add_library(EosCliProto-Objects OBJECT
  ${CLI_PROTO_SRCS} ${CLI_PROTO_HDRS})

set_target_properties(EosCliProto-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

add_library(EosProtobuf SHARED
  $<TARGET_OBJECTS:EosAuthProto-Objects>
  $<TARGET_OBJECTS:EosNsQuarkdbProto-Objects>
  $<TARGET_OBJECTS:EosFstProto-Objects>
  $<TARGET_OBJECTS:EosCliProto-Objects>)

target_link_libraries(EosProtobuf PUBLIC
  eosCommon
  ${PROTOBUF_LIBRARIES})

set_target_properties(EosProtobuf PROPERTIES
  VERSION ${VERSION}
  SOVERSION ${VERSION_MAJOR}
  MACOSX_RPATH TRUE)

add_library(EosProtobuf-Static STATIC
  $<TARGET_OBJECTS:EosAuthProto-Objects>
  $<TARGET_OBJECTS:EosNsQuarkdbProto-Objects>
  $<TARGET_OBJECTS:EosFstProto-Objects>
  $<TARGET_OBJECTS:EosCliProto-Objects>)

target_link_libraries(EosProtobuf-Static PRIVATE
  eosCommon-Static
  ${PROTOBUF_LIBARIES})

install(
  TARGETS EosProtobuf EosProtobuf-Static
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
