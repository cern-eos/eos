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

#-------------------------------------------------------------------------------
# Generate protocol buffer object for the Namespace
#-------------------------------------------------------------------------------
PROTOBUF_GENERATE_CPP(FMD_SRCS FMD_HDRS namespace/ns_quarkdb/FileMd.proto)
PROTOBUF_GENERATE_CPP(CMD_SRCS CMD_HDRS namespace/ns_quarkdb/ContainerMd.proto)
PROTOBUF_GENERATE_CPP(CHANGELOG_SRCS CHANGELOG_HDRS namespace/ns_quarkdb/ChangelogEntry.proto)

set(NS_PROTO_SRCS ${FMD_SRCS} ${CMD_SRCS} ${CHANGELOG_SRCS})
set(NS_PROTO_HDRS ${FMD_HDRS} ${CMD_HDRS} ${CHANGELOG_HDRS})
set_source_files_properties(
  ${NS_PROTO_SRCS}
  ${NS_PROTO_HDRS}
  PROPERTIES GENERATED TRUE)

add_library(EosNsQuarkdbProto-Objects OBJECT
  ${NS_PROTO_SRCS}  ${NS_PROTO_HDRS})

target_link_libraries(EosNsQuarkdbProto-Objects PUBLIC
  PROTOBUF::PROTOBUF)

target_include_directories(EosNsQuarkdbProto-Objects PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)

set_target_properties(EosNsQuarkdbProto-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# Generate protocol buffer object for FST
#-------------------------------------------------------------------------------
PROTOBUF_GENERATE_CPP(FMDBASE_SRCS FMDBASE_HDRS fst/FmdBase.proto)
PROTOBUF_GENERATE_CPP(DELETE_SRCS DELETE_HDRS fst/Delete.proto)

add_library(EosFstProto-Objects OBJECT
  ${FMDBASE_SRCS} ${FMDBASE_HDRS}
  ${DELETE_SRCS}  ${DELETE_HDRS})

target_link_libraries(EosFstProto-Objects PUBLIC
  PROTOBUF::PROTOBUF)

target_include_directories(EosFstProto-Objects PUBLIC
  $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)

set_target_properties(EosFstProto-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# Generate protocol buffer object for the CLI
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
PROTOBUF_GENERATE_CPP(CONVERT_SRCS CONVERT_HDRS common/cli_proto/Convert.proto)
PROTOBUF_GENERATE_CPP(ATTR_SRCS ATTR_HDRS common/cli_proto/Attr.proto)
PROTOBUF_GENERATE_CPP(AUTH_SRCS AUTH_HDRS common/cli_proto/Authentication.proto)
PROTOBUF_GENERATE_CPP(CHMOD_SRCS CHMOD_HDRS common/cli_proto/ChangeMode.proto)
PROTOBUF_GENERATE_CPP(CHOWN_SRCS CHOWN_HDRS common/cli_proto/Chown.proto)
PROTOBUF_GENERATE_CPP(CP_SRCS CP_HDRS common/cli_proto/Cp.proto)
PROTOBUF_GENERATE_CPP(FILE_SRCS FILE_HDRS common/cli_proto/File.proto)
PROTOBUF_GENERATE_CPP(FILEINFO_SRCS FILEINFO_HDRS common/cli_proto/Fileinfo.proto)
PROTOBUF_GENERATE_CPP(LIST_SRCS LIST_HDRS common/cli_proto/List.proto)
PROTOBUF_GENERATE_CPP(LS_SRCS LS_HDRS common/cli_proto/Ls.proto)
PROTOBUF_GENERATE_CPP(MD_REP_SRCS MD_REP_HDRS common/cli_proto/ConsoleStreamReply.proto)
PROTOBUF_GENERATE_CPP(METADATA_SRCS METADATA_HDRS common/cli_proto/Metadata.proto)
PROTOBUF_GENERATE_CPP(MKDIR_SRCS MKDIR_HDRS common/cli_proto/MakeDirectory.proto)
PROTOBUF_GENERATE_CPP(MOVE_SRCS MOVE_HDRS common/cli_proto/Move.proto)
PROTOBUF_GENERATE_CPP(RMDIR_SRCS RMDIR_HDRS common/cli_proto/Rmdir.proto)
PROTOBUF_GENERATE_CPP(STAT_SRCS STAT_HDRS common/cli_proto/Status.proto)
PROTOBUF_GENERATE_CPP(TOUCH_SRCS TOUCH_HDRS common/cli_proto/Touch.proto)
PROTOBUF_GENERATE_CPP(TRANSFER_SRCS TRANSFER_HDRS common/cli_proto/Transfer.proto)
PROTOBUF_GENERATE_CPP(VERSION_SRCS VERSION_HDRS common/cli_proto/Version.proto)
PROTOBUF_GENERATE_CPP(VID_SRCS VID_HDRS common/cli_proto/Vid.proto)
PROTOBUF_GENERATE_CPP(WHO_SRCS WHO_HDRS common/cli_proto/Who.proto)
PROTOBUF_GENERATE_CPP(WHOAMI_SRCS WHOAMI_HDRS common/cli_proto/Whoami.proto)
PROTOBUF_GENERATE_CPP(GEOSCHED_SRCS GEOSCHED_HDRS common/cli_proto/Geosched.proto)

set(CLI_PROTO_SRCS
  ${REQ_SRCS} ${REP_SRCS} ${RECY_SRCS} ${ACL_SRCS} ${NS_SRCS} ${DRAIN_SRCS} ${FIND_SRCS}
  ${FS_SRCS} ${RM_SRCS} ${TOKEN_SRCS} ${STAGER_RM_SRCS} ${ROUTE_SRCS} ${IO_SRCS} ${GROUP_SRCS}
  ${DEBUG_SRCS} ${NODE_SRCS} ${QUOTA_SRCS} ${SPACE_SRCS} ${CONFIG_SRCS} ${ACCESS_SRCS}
  ${FSCK_SRCS} ${SHARE_SRCS} ${GRPC_SRCS} ${QOS_SRCS} ${CONVERT_SRCS} ${ATTR_SRCS}
  ${AUTH_SRCS} ${CHMOD_SRCS} ${CHOWN_SRCS} ${CP_SRCS} ${FILE_SRCS} ${FILEINFO_SRCS} ${LS_SRCS}
  ${MD_REP_SRCS} ${METADATA_SRCS} ${MKDIR_SRCS} ${MOVE_SRCS} ${RMDIR_SRCS} ${STAT_SRCS}
  ${TOUCH_SRCS} ${TRANSFER_SRCS} ${VERSION_SRCS} ${VID_SRCS} ${WHO_SRCS} ${WHOAMI_SRCS}
  ${LIST_SRCS} ${GEOSCHED_SRCS})

set(CLI_PROTO_HDRS
  ${REQ_HDRS} ${REP_HDRS} ${RECY_HDRS} ${ACL_HDRS} ${NS_HDRS} ${DRAIN_HDRS} ${FIND_HDRS}
  ${FS_HDRS} ${RM_HDRS} ${TOKEN_HDRS} ${STAGER_RM_HDRS} ${ROUTE_HDRS} ${IO_HDRS} ${GROUP_HDRS}
  ${DEBUG_HDRS} ${NODE_HDRS} ${QUOTA_HDRS} ${SPACE_HDRS} ${CONFIG_HDRS} ${ACCESS_HDRS}
  ${FSCK_HDRS} ${SHARE_HDRS} ${GRPC_HDRS} ${QOS_HDRS} ${CONVERT_HDRS} ${ATTR_HDRS}
  ${AUTH_HDRS} ${CHMOD_HDRS} ${CHOWN_HDRS} ${CP_HDRS} ${FILE_HDRS} ${FILEINFO_HDRS} ${LS_HDRS}
  ${MD_REP_HDRS} ${METADATA_HDRS} ${MKDIR_HDRS} ${MOVE_HDRS} ${RMDIR_HDRS} ${STAT_HDRS}
  ${TOUCH_HDRS} ${TRANSFER_HDRS} ${VERSION_HDRS} ${VID_HDRS} ${WHO_HDRS} ${WHOAMI_HDRS}
  ${LIST_HDRS} ${GEOSCHED_HDRS})

set_source_files_properties(
  ${CLI_PROTO_SRCS} ${CLI_PROTO_HDRS}
  PROPERTIES GENERATED 1)

add_library(EosCliProto-Objects OBJECT
  ${CLI_PROTO_SRCS} ${CLI_PROTO_HDRS})

target_link_libraries(EosCliProto-Objects PUBLIC
  PROTOBUF::PROTOBUF)

target_include_directories(EosCliProto-Objects PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)

set_target_properties(EosCliProto-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# Generate protocol buffer objects for GRPC
#-------------------------------------------------------------------------------
if (GRPC_FOUND)
  add_custom_target(RpcFileGeneration DEPENDS
    ${GRPC_SRCS} ${GRPC_HDRS})

  set(GRPC_PROTOS ${CMAKE_SOURCE_DIR}/common/grpc-proto/protobuf/Rpc.proto)
  set(GRPC_PROTOBUF_PATH "${CMAKE_BINARY_DIR}/proto/")
  grpc_generate_cpp(GRPC_SVC_SRCS GRPC_SVC_HDRS ${GRPC_PROTOBUF_PATH} ${GRPC_PROTOS})

  set(GRPC_SVC_SRCS ${GRPC_SVC_SRCS} PARENT_SCOPE)
  set(GRPC_SVC_HDRS ${GRPC_SVC_HDRS} PARENT_SCOPE)

  set_source_files_properties(
    ${GRPC_SVC_SRCS}
    ${GRPC_SVC_HDRS}
    PROPERTIES GENERATED TRUE)

  add_library(EosGrpcProto-Objects OBJECT
    ${GRPC_SVC_SRCS} ${GRPC_SVC_HDRS})

  add_dependencies(EosGrpcProto-Objects RpcFileGeneration)

  target_link_libraries(EosGrpcProto-Objects PUBLIC
    GRPC::grpc++
    PROTOBUF::PROTOBUF)

  target_include_directories(EosGrpcProto-Objects PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)

  set_target_properties(EosGrpcProto-Objects PROPERTIES
    POSITION_INDEPENDENT_CODE TRUE)
else ()
  add_library(EosGrpcProto-Objects INTERFACE)
endif()

#------------------------------------------------------------------------------
# Generate gRPC objects for the CLI
#------------------------------------------------------------------------------
if (GRPC_FOUND)
  grpc_generate_cpp(ACCESS_GRPC_SRCS ACCESS_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Access.proto)
  grpc_generate_cpp(ACL_GRPC_SRCS ACL_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Acl.proto)
  grpc_generate_cpp(ATTR_GRPC_SRCS ATTR_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Attr.proto)
  grpc_generate_cpp(AUTH_GRPC_SRCS AUTH_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Authentication.proto)
  grpc_generate_cpp(CHMOD_GRPC_SRCS CHMOD_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/ChangeMode.proto)
  grpc_generate_cpp(CHOWN_GRPC_SRCS CHOWN_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Chown.proto)
  grpc_generate_cpp(CONFIG_GRPC_SRCS CONFIG_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Config.proto)
  grpc_generate_cpp(CP_GRPC_SRCS CP_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Cp.proto)
  grpc_generate_cpp(DEBUG_GRPC_SRCS DEBUG_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Debug.proto)
  grpc_generate_cpp(DRAIN_GRPC_SRCS DRAIN_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Drain.proto)
  grpc_generate_cpp(FILE_GRPC_SRCS FILE_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/File.proto)
  grpc_generate_cpp(FILEINFO_GRPC_SRCS FILEINFO_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Fileinfo.proto)
  grpc_generate_cpp(FIND_GRPC_SRCS FIND_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Find.proto)
  grpc_generate_cpp(FS_GRPC_SRCS FS_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Fs.proto)
  grpc_generate_cpp(FSCK_GRPC_SRCS FSCK_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Fsck.proto)
  grpc_generate_cpp(GROUP_GRPC_SRCS GROUP_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Group.proto)
  grpc_generate_cpp(IO_GRPC_SRCS IO_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Io.proto)
  grpc_generate_cpp(LIST_GRPC_SRCS LIST_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/List.proto)
  grpc_generate_cpp(LS_GRPC_SRCS LS_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Ls.proto)
  grpc_generate_cpp(MD_REP_GRPC_SRCS MD_REP_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/ConsoleStreamReply.proto)
  grpc_generate_cpp(METADATA_GRPC_SRCS METADATA_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Metadata.proto)
  grpc_generate_cpp(MKDIR_GRPC_SRCS MKDIR_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/MakeDirectory.proto)
  grpc_generate_cpp(MOVE_GRPC_SRCS MOVE_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Move.proto)
  grpc_generate_cpp(NODE_GRPC_SRCS NODE_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Node.proto)
  grpc_generate_cpp(NS_GRPC_SRCS NS_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Ns.proto)
  grpc_generate_cpp(QUOTAGRPC_SRCS QUOTA_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Quota.proto)
  grpc_generate_cpp(RECY_GRPC_SRCS RECY_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Recycle.proto)
  grpc_generate_cpp(REP_GRPC_SRCS REP_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/ConsoleReply.proto)
  grpc_generate_cpp(REQ_GRPC_SRCS REQ_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/ConsoleRequest.proto)
  grpc_generate_cpp(RM_GRPC_SRCS RM_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Rm.proto)
  grpc_generate_cpp(RMDIR_GRPC_SRCS RMDIR_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Rmdir.proto)
  grpc_generate_cpp(ROUTE_GRPC_SRCS ROUTE_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Route.proto)
  grpc_generate_cpp(SPACE_GRPC_SRCS SPACE_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Space.proto)
  grpc_generate_cpp(STAGER_GRPC_SRCS STAGER_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/StagerRm.proto)
  grpc_generate_cpp(STAT_GRPC_SRCS STAT_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Status.proto)
  grpc_generate_cpp(TOUCH_GRPC_SRCS TOUCH_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Touch.proto)
  grpc_generate_cpp(TRANSFER_GRPC_SRCS TRANSFER_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Transfer.proto)
  grpc_generate_cpp(VERSION_GRPC_SRCS VERSION_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Version.proto)
  grpc_generate_cpp(VID_GRPC_SRCS VID_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Vid.proto)
  grpc_generate_cpp(WHO_GRPC_SRCS WHO_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Who.proto)
  grpc_generate_cpp(WHOAMI_GRPC_SRCS WHOAMI_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Whoami.proto)
  grpc_generate_cpp(GEOSCHED_GRPC_SRCS GEOSCHED_GRPC_HDRS ${GRPC_PROTOBUF_PATH} common/cli_proto/Geosched.proto)

  set(CLI_GRPC_SRCS
    ${ACCESS_GRPC_SRCS} ${ACL_GRPC_SRCS} ${ATTR_GRPC_SRCS} ${AUTH_GRPC_SRCS}
    ${CHMOD_GRPC_SRCS} ${CHOWN_GRPC_SRCS} ${CONFIG_GRPC_SRCS} ${DEBUG_GRPC_SRCS}
    ${DRAIN_GRPC_SRCS} ${FILE_GRPC_SRCS} ${FILEINFO_GRPC_SRCS} ${FIND_GRPC_SRCS}
    ${FS_GRPC_SRCS} ${FSCK_GRPC_SRCS} ${GROUP_GRPC_SRCS} ${IO_GRPC_SRCS}
    ${LS_GRPC_SRCS} ${MD_REP_GRPC_SRCS} ${METADATA_GRPC_SRCS} ${MKDIR_GRPC_SRCS}
    ${MOVE_GRPC_SRCS} ${NODE_GRPC_SRCS} ${NS_GRPC_SRCS} ${QUOTA_GRPC_SRCS}
    ${RECY_GRPC_SRCS} ${REP_GRPC_SRCS} ${REQ_GRPC_SRCS} ${RM_GRPC_SRCS}
    ${RMDIR_GRPC_SRCS} ${ROUTE_GRPC_SRCS} ${SPACE_GRPC_SRCS} ${STAGER_RM_GRPC_SRCS}
    ${STAT_GRPC_SRCS} ${TOUCH_GRPC_SRCS} ${TRANSFER_GRPC_SRCS} ${VERSION_GRPC_SRCS}
    ${VID_GRPC_SRCS} ${WHO_GRPC_SRCS} ${WHOAMI_GRPC_SRCS} ${LIST_GRPC_SRCS}
    ${CP_GRPC_SRCS} ${GEOSCHED_GRPC_SRCS})

  set(CLI_GRPC_HDRS
    ${ACCESS_GRPC_HDRS} ${ACL_GRPC_HDRS} ${ATTR_GRPC_HDRS} ${AUTH_GRPC_HDRS}
    ${CHMOD_GRPC_HDRS} ${CHOWN_GRPC_HDRS} ${CONFIG_GRPC_HDRS} ${DEBUG_GRPC_HDRS}
    ${DRAIN_GRPC_HDRS} ${FILE_GRPC_HDRS} ${FILEINFO_GRPC_HDRS} ${FIND_GRPC_HDRS}
    ${FS_GRPC_HDRS} ${FSCK_GRPC_HDRS} ${GROUP_GRPC_HDRS} ${IO_GRPC_HDRS}
    ${LS_GRPC_HDRS} ${MD_REP_GRPC_HDRS} ${METADATA_GRPC_HDRS} ${MKDIR_GRPC_HDRS}
    ${MOVE_GRPC_HDRS} ${NODE_GRPC_HDRS} ${NS_GRPC_HDRS} ${QUOTA_GRPC_HDRS}
    ${RECY_GRPC_HDRS} ${REP_GRPC_HDRS} ${REQ_GRPC_HDRS} ${RM_GRPC_HDRS}
    ${RMDIR_GRPC_HDRS} ${ROUTE_GRPC_HDRS} ${SPACE_GRPC_HDRS} ${STAGER_RM_GRPC_HDRS}
    ${STAT_GRPC_HDRS} ${TOUCH_GRPC_HDRS} ${TRANSFER_GRPC_HDRS} ${VERSION_GRPC_HDRS}
    ${VID_GRPC_HDRS} ${WHO_GRPC_HDRS} ${WHOAMI_GRPC_HDRS} ${LIST_GRPC_HDRS}
    ${CP_GRPC_HDRS} ${GEOSCHED_GRPC_HDRS})

  set_source_files_properties(
    ${CLI_GRPC_SRCS}
    ${CLI_GRPC_HDRS}
    PROPERTIES GENERATED TRUE)

  add_library(EosCliGrpc-Objects OBJECT
    ${CLI_GRPC_SRCS}
    ${CLI_GRPC_HDRS})

  add_dependencies(EosCliGrpc-Objects EosCliProto-Objects)

  target_link_libraries(EosCliGrpc-Objects PUBLIC
    GRPC::grpc++
    PROTOBUF::PROTOBUF)

  target_include_directories(EosCliGrpc-Objects PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)

  set_target_properties(EosCliGrpc-Objects PROPERTIES
    POSITION_INDEPENDENT_CODE TRUE)
endif()

#------------------------------------------------------------------------------
# Generate protobuf and gRPC objects for EOS Windows native client
#------------------------------------------------------------------------------
if (GRPC_FOUND)
  set(WNC_PROTO common/cli_proto/EosWnc.proto)
  PROTOBUF_GENERATE_CPP(WNC_SRCS WNC_HDRS ${WNC_PROTO})
  grpc_generate_cpp(WNC_GRPC_SRCS WNC_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${WNC_PROTO})

  set_source_files_properties(
    ${WNC_SRCS}
    ${WNC_HDRS}
    ${WNC_GRPC_SRCS}
    ${WNC_GRPC_HDRS}
    PROPERTIES GENERATED TRUE)

  add_library(EosWncGrpcProto-Objects OBJECT
    ${WNC_SRCS}
    ${WNC_HDRS}
    ${WNC_GRPC_SRCS}
    ${WNC_GRPC_HDRS})

  add_dependencies(EosWncGrpcProto-Objects
    EosCliProto-Objects
    EosCliGrpc-Objects)

  target_link_libraries(EosWncGrpcProto-Objects PUBLIC
    GRPC::grpc++
    PROTOBUF::PROTOBUF)

  target_include_directories(EosWncGrpcProto-Objects PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)

  set_target_properties(EosWncGrpcProto-Objects PROPERTIES
    POSITION_INDEPENDENT_CODE TRUE)
endif()
