#-------------------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Jozsef Makai - CERN
# Author: Fabio Luchetti - CERN
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

set (PROTOBUF_IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR})

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
  PROPERTIES GENERATED TRUE)

add_library(EosNsQuarkdbProto-Objects OBJECT
  ${NS_PROTO_SRCS}  ${NS_PROTO_HDRS})

set_target_properties(EosNsQuarkdbProto-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# Generate protobol buffer object for FST
#-------------------------------------------------------------------------------
PROTOBUF_GENERATE_CPP(FMDBASE_SRCS FMDBASE_HDRS fst/FmdBase.proto)

add_library(EosFstProto-Objects OBJECT
  ${FMDBASE_SRCS}
  ${FMDBASE_HDRS})

set_target_properties(EosFstProto-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# Generate protobol buffer object for the CLI
#-------------------------------------------------------------------------------
PROTOBUF_GENERATE_CPP(REQ_SRCS REQ_HDRS common/cli_proto/ConsoleRequest.proto)
PROTOBUF_GENERATE_CPP(REP_SRCS REP_HDRS common/cli_proto/ConsoleReply.proto)
PROTOBUF_GENERATE_CPP(RECY_SRCS RECY_HDRS common/cli_proto/Recycle.proto)
PROTOBUF_GENERATE_CPP(ACL_SRCS ACL_HDRS common/cli_proto/Acl.proto)
PROTOBUF_GENERATE_CPP(NS_SRCS NS_HDRS common/cli_proto/Ns.proto)
PROTOBUF_GENERATE_CPP(DRAIN_SRCS DRAIN_HDRS common/cli_proto/Drain.proto)
PROTOBUF_GENERATE_CPP(FIND_SRCS FIND_HDRS common/cli_proto/Find.proto)
PROTOBUF_GENERATE_CPP(FS_SRCS FS_HDRS common/cli_proto/Fs.proto)
PROTOBUF_GENERATE_CPP(RM_SRCS RM_HDRS common/cli_proto/Rm.proto)
PROTOBUF_GENERATE_CPP(TOKEN_SRCS TOKEN_HDRS common/cli_proto/Token.proto)
PROTOBUF_GENERATE_CPP(STAGER_RM_SRCS STAGER_RM_HDRS common/cli_proto/StagerRm.proto)
PROTOBUF_GENERATE_CPP(ROUTE_SRCS ROUTE_HDRS common/cli_proto/Route.proto)
PROTOBUF_GENERATE_CPP(IO_SRCS IO_HDRS common/cli_proto/Io.proto)
PROTOBUF_GENERATE_CPP(GROUP_SRCS GROUP_HDRS common/cli_proto/Group.proto)
PROTOBUF_GENERATE_CPP(DEBUG_SRCS DEBUG_HDRS common/cli_proto/Debug.proto)
PROTOBUF_GENERATE_CPP(NODE_SRCS NODE_HDRS common/cli_proto/Node.proto)
PROTOBUF_GENERATE_CPP(QUOTA_SRCS QUOTA_HDRS common/cli_proto/Quota.proto)
PROTOBUF_GENERATE_CPP(SPACE_SRCS SPACE_HDRS common/cli_proto/Space.proto)
PROTOBUF_GENERATE_CPP(CONFIG_SRCS CONFIG_HDRS common/cli_proto/Config.proto)
PROTOBUF_GENERATE_CPP(ACCESS_SRCS ACCESS_HDRS common/cli_proto/Access.proto)
PROTOBUF_GENERATE_CPP(FSCK_SRCS FSCK_HDRS common/cli_proto/Fsck.proto)
PROTOBUF_GENERATE_CPP(GRPC_SRCS GRPC_HDRS ${CMAKE_SOURCE_DIR}/common/grpc-proto/protobuf/Rpc.proto)
PROTOBUF_GENERATE_CPP(QOS_SRCS QOS_HDRS common/cli_proto/QoS.proto)

set(CLI_PROTO_SRCS
  ${REQ_SRCS} ${REP_SRCS} ${RECY_SRCS} ${ACL_SRCS} ${NS_SRCS} ${DRAIN_SRCS} ${FIND_SRCS}
  ${FS_SRCS} ${RM_SRCS} ${TOKEN_SRCS} ${STAGER_RM_SRCS} ${ROUTE_SRCS} ${IO_SRCS} ${GROUP_SRCS}
  ${DEBUG_SRCS} ${NODE_SRCS} ${QUOTA_SRCS} ${SPACE_SRCS} ${CONFIG_SRCS} ${ACCESS_SRCS}
  ${FSCK_SRCS} ${SHARE_SRCS} ${GRPC_SRCS} ${QOS_SRCS})

set(CLI_PROTO_HDRS
  ${REQ_HDRS} ${REP_HDRS} ${RECY_HDRS} ${ACL_HDRS} ${NS_HDRS} ${DRAIN_HDRS} ${FIND_HDRS}
  ${FS_HDRS} ${RM_HDRS} ${TOKEN_HDRS} ${STAGER_RM_HDRS} ${ROUTE_HDRS} ${IO_HDRS} ${GROUP_HDRS}
  ${DEBUG_HDRS} ${NODE_HDRS} ${QUOTA_HDRS} ${SPACE_HDRS} ${CONFIG_HDRS} ${ACCESS_HDRS}
  ${FSCK_HDRS} ${SHARE_HDRS} ${GRPC_HDRS} ${QOS_HDRS})

set_source_files_properties(
  ${CLI_PROTO_SRCS} ${CLI_PROTO_HDRS}
  PROPERTIES GENERATED 1)

add_library(EosCliProto-Objects OBJECT
  ${CLI_PROTO_SRCS} ${CLI_PROTO_HDRS})

set_target_properties(EosCliProto-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# Generate protobol buffer object for GRPC
#-------------------------------------------------------------------------------
if (GRPC_FOUND)
  set(GRPC_PROTOS ${CMAKE_SOURCE_DIR}/common/grpc-proto/protobuf/Rpc.proto)
  set(GRPC_PROTOBUF_PATH "${CMAKE_BINARY_DIR}/proto/")
  grpc_generate_cpp(GRPC_SVC_SRCS GRPC_SVC_HDRS ${GRPC_PROTOBUF_PATH} ${GRPC_PROTOS})

  set(GRPC_SRCS ${GRPC_SRCS} PARENT_SCOPE)
  set(GRPC_HDRS ${GRPC_HDRS} PARENT_SCOPE)
  set(GRPC_SVC_SRCS ${GRPC_SVC_SRCS} PARENT_SCOPE)
  set(GRPC_SVC_HDRS ${GRPC_SVC_HDRS} PARENT_SCOPE)

  set_source_files_properties(
    ${GRPC_SRCS}
    ${GRPC_HDRS}
    ${GRPC_SVC_SRCS}
    ${GRPC_SVC_HDRS}
    PROPERTIES GENERATED TRUE)

  add_library(EosGrpcProto-Objects OBJECT
    ${GRPC_SVC_SRCS}
    ${GRPC_SVC_HDRS})

  set_target_properties(EosGrpcProto-Objects PROPERTIES
    POSITION_INDEPENDENT_CODE TRUE)

  add_dependencies(EosGrpcProto-Objects EosCliProto-Objects)
endif()

