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
# Disable -Wsign-compare warnings due to
# grpcpp/support/proto_buffer_reader.h:157:24: warning: comparison of
# integer expressions of different signedness: ‘uint64_t’ {aka ‘long
# unsigned int’} and ‘int’ [-Wsign-compare]
#-------------------------------------------------------------------------------
add_compile_options(-Wno-sign-compare)

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
  PROTOBUF::PROTOBUF
  ABSL::ABSL)

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
  PROTOBUF::PROTOBUF
  ABSL::ABSL)

target_include_directories(EosFstProto-Objects PUBLIC
  $<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)

set_target_properties(EosFstProto-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# Generate protocol buffer object for Audit logging
#-------------------------------------------------------------------------------
PROTOBUF_GENERATE_CPP(AUDIT_SRCS AUDIT_HDRS Audit.proto)

add_library(EosAuditProto-Objects OBJECT
  ${AUDIT_SRCS} ${AUDIT_HDRS})

target_link_libraries(EosAuditProto-Objects PUBLIC
  PROTOBUF::PROTOBUF
  ABSL::ABSL)

target_include_directories(EosAuditProto-Objects PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)

set_target_properties(EosAuditProto-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# Generate protocol buffer object for the CLI
#-------------------------------------------------------------------------------
set(Protobuf_IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/eos-protobuf-spec ${CMAKE_SOURCE_DIR}/common/grpc-proto)
set(PROTOBUF3_IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/eos-protobuf-spec ${CMAKE_SOURCE_DIR}/common/grpc-proto)
# common protos
PROTOBUF_GENERATE_CPP(GRPC_SRCS GRPC_HDRS ${CMAKE_SOURCE_DIR}/common/grpc-proto/Rpc.proto)
PROTOBUF_GENERATE_CPP(RECY_SRCS RECY_HDRS ${CMAKE_SOURCE_DIR}/common/grpc-proto/Recycle.proto)
PROTOBUF_GENERATE_CPP(TRAFFIC_SHAPING_SRCS TRAFFIC_SHAPING_HDRS ${CMAKE_SOURCE_DIR}/common/grpc-proto/TrafficShaping.proto)
# eos-protobuf-spec protos
set(EOS_PROTO_ROOT eos-protobuf-spec)
PROTOBUF_GENERATE_CPP(REQ_SRCS REQ_HDRS ${EOS_PROTO_ROOT}/ConsoleRequest.proto)
PROTOBUF_GENERATE_CPP(REP_SRCS REP_HDRS ${EOS_PROTO_ROOT}/ConsoleReply.proto)
PROTOBUF_GENERATE_CPP(ACL_SRCS ACL_HDRS ${EOS_PROTO_ROOT}/Acl.proto)
PROTOBUF_GENERATE_CPP(NS_SRCS NS_HDRS ${EOS_PROTO_ROOT}/Ns.proto)
PROTOBUF_GENERATE_CPP(FIND_SRCS FIND_HDRS ${EOS_PROTO_ROOT}/Find.proto)
PROTOBUF_GENERATE_CPP(FS_SRCS FS_HDRS ${EOS_PROTO_ROOT}/Fs.proto)
PROTOBUF_GENERATE_CPP(RM_SRCS RM_HDRS ${EOS_PROTO_ROOT}/Rm.proto)
PROTOBUF_GENERATE_CPP(TOKEN_SRCS TOKEN_HDRS ${EOS_PROTO_ROOT}/Token.proto)
PROTOBUF_GENERATE_CPP(EVICT_SRCS EVICT_HDRS ${EOS_PROTO_ROOT}/Evict.proto)
PROTOBUF_GENERATE_CPP(ROUTE_SRCS ROUTE_HDRS ${EOS_PROTO_ROOT}/Route.proto)
PROTOBUF_GENERATE_CPP(IO_SRCS IO_HDRS ${EOS_PROTO_ROOT}/Io.proto)
PROTOBUF_GENERATE_CPP(GROUP_SRCS GROUP_HDRS ${EOS_PROTO_ROOT}/Group.proto)
PROTOBUF_GENERATE_CPP(DEBUG_SRCS DEBUG_HDRS ${EOS_PROTO_ROOT}/Debug.proto)
PROTOBUF_GENERATE_CPP(DF_SRCS DF_HDRS ${EOS_PROTO_ROOT}/Df.proto)
PROTOBUF_GENERATE_CPP(DEVICES_SRCS DEVICES_HDRS ${EOS_PROTO_ROOT}/Devices.proto)
PROTOBUF_GENERATE_CPP(NODE_SRCS NODE_HDRS ${EOS_PROTO_ROOT}/Node.proto)
PROTOBUF_GENERATE_CPP(QUOTA_SRCS QUOTA_HDRS ${EOS_PROTO_ROOT}/Quota.proto)
PROTOBUF_GENERATE_CPP(SPACE_SRCS SPACE_HDRS ${EOS_PROTO_ROOT}/Space.proto)
PROTOBUF_GENERATE_CPP(CONFIG_SRCS CONFIG_HDRS ${EOS_PROTO_ROOT}/Config.proto)
PROTOBUF_GENERATE_CPP(ACCESS_SRCS ACCESS_HDRS ${EOS_PROTO_ROOT}/Access.proto)
PROTOBUF_GENERATE_CPP(FSCK_SRCS FSCK_HDRS ${EOS_PROTO_ROOT}/Fsck.proto)
PROTOBUF_GENERATE_CPP(CONVERT_SRCS CONVERT_HDRS ${EOS_PROTO_ROOT}/Convert.proto)
PROTOBUF_GENERATE_CPP(ATTR_SRCS ATTR_HDRS ${EOS_PROTO_ROOT}/Attr.proto)
PROTOBUF_GENERATE_CPP(AUTH_SRCS AUTH_HDRS ${EOS_PROTO_ROOT}/Authentication.proto)
PROTOBUF_GENERATE_CPP(CHMOD_SRCS CHMOD_HDRS ${EOS_PROTO_ROOT}/ChangeMode.proto)
PROTOBUF_GENERATE_CPP(CHOWN_SRCS CHOWN_HDRS ${EOS_PROTO_ROOT}/Chown.proto)
PROTOBUF_GENERATE_CPP(CP_SRCS CP_HDRS ${EOS_PROTO_ROOT}/Cp.proto)
PROTOBUF_GENERATE_CPP(FILE_SRCS FILE_HDRS ${EOS_PROTO_ROOT}/File.proto)
PROTOBUF_GENERATE_CPP(FILEINFO_SRCS FILEINFO_HDRS ${EOS_PROTO_ROOT}/Fileinfo.proto)
PROTOBUF_GENERATE_CPP(LS_SRCS LS_HDRS ${EOS_PROTO_ROOT}/Ls.proto)
PROTOBUF_GENERATE_CPP(METADATA_SRCS METADATA_HDRS ${EOS_PROTO_ROOT}/Metadata.proto)
PROTOBUF_GENERATE_CPP(MKDIR_SRCS MKDIR_HDRS ${EOS_PROTO_ROOT}/MakeDirectory.proto)
PROTOBUF_GENERATE_CPP(MOVE_SRCS MOVE_HDRS ${EOS_PROTO_ROOT}/Move.proto)
PROTOBUF_GENERATE_CPP(RMDIR_SRCS RMDIR_HDRS ${EOS_PROTO_ROOT}/Rmdir.proto)
PROTOBUF_GENERATE_CPP(STAT_SRCS STAT_HDRS ${EOS_PROTO_ROOT}/StatWnc.proto)
PROTOBUF_GENERATE_CPP(TOUCH_SRCS TOUCH_HDRS ${EOS_PROTO_ROOT}/Touch.proto)
PROTOBUF_GENERATE_CPP(VERSION_SRCS VERSION_HDRS ${EOS_PROTO_ROOT}/Version.proto)
PROTOBUF_GENERATE_CPP(VID_SRCS VID_HDRS ${EOS_PROTO_ROOT}/Vid.proto)
PROTOBUF_GENERATE_CPP(WHO_SRCS WHO_HDRS ${EOS_PROTO_ROOT}/Who.proto)
PROTOBUF_GENERATE_CPP(WHOAMI_SRCS WHOAMI_HDRS ${EOS_PROTO_ROOT}/Whoami.proto)
PROTOBUF_GENERATE_CPP(GEOSCHED_SRCS GEOSCHED_HDRS ${EOS_PROTO_ROOT}/Geosched.proto)
PROTOBUF_GENERATE_CPP(HEALTH_SRCS HEALTH_HDRS ${EOS_PROTO_ROOT}/Health.proto)
PROTOBUF_GENERATE_CPP(ARCHIVE_SRCS ARCHIVE_HDRS ${EOS_PROTO_ROOT}/Archive.proto)
PROTOBUF_GENERATE_CPP(BACKUP_SRCS BACKUP_HDRS ${EOS_PROTO_ROOT}/Backup.proto)
PROTOBUF_GENERATE_CPP(MAP_SRCS MAP_HDRS ${EOS_PROTO_ROOT}/Map.proto)
PROTOBUF_GENERATE_CPP(MEMBER_SRCS MEMBER_HDRS ${EOS_PROTO_ROOT}/Member.proto)
PROTOBUF_GENERATE_CPP(STATUS_SRCS STATUS_HDRS ${EOS_PROTO_ROOT}/Status.proto)
PROTOBUF_GENERATE_CPP(SCHED_SRCS SCHED_HDRS ${EOS_PROTO_ROOT}/Sched.proto)

set(CLI_PROTO_SRCS
  ${REQ_SRCS} ${REP_SRCS} ${RECY_SRCS} ${TRAFFIC_SHAPING_SRCS} ${ACL_SRCS} ${NS_SRCS} ${FIND_SRCS}
  ${FS_SRCS} ${RM_SRCS} ${TOKEN_SRCS} ${EVICT_SRCS} ${ROUTE_SRCS} ${IO_SRCS} ${GROUP_SRCS}
  ${DEBUG_SRCS} ${DF_SRCS} ${DEVICES_SRCS} ${NODE_SRCS} ${QUOTA_SRCS} ${SPACE_SRCS} ${CONFIG_SRCS} ${ACCESS_SRCS}
  ${FSCK_SRCS} ${SHARE_SRCS} ${GRPC_SRCS} ${CONVERT_SRCS} ${ATTR_SRCS}
  ${AUTH_SRCS} ${CHMOD_SRCS} ${CHOWN_SRCS} ${CP_SRCS} ${FILE_SRCS} ${FILEINFO_SRCS} ${LS_SRCS}
  ${METADATA_SRCS} ${MKDIR_SRCS} ${MOVE_SRCS} ${RMDIR_SRCS} ${STAT_SRCS}
  ${TOUCH_SRCS} ${VERSION_SRCS} ${VID_SRCS} ${WHO_SRCS} ${WHOAMI_SRCS}
  ${GEOSCHED_SRCS} ${HEALTH_SRCS} ${ARCHIVE_SRCS} ${BACKUP_SRCS} ${MAP_SRCS}
  ${MEMBER_SRCS} ${STATUS_SRCS} ${SCHED_SRCS}
)

set(CLI_PROTO_HDRS
  ${REQ_HDRS} ${REP_HDRS} ${RECY_HDRS} ${TRAFFIC_SHAPING_HDRS} ${ACL_HDRS} ${NS_HDRS} ${FIND_HDRS}
  ${FS_HDRS} ${RM_HDRS} ${TOKEN_HDRS} ${EVICT_HDRS} ${ROUTE_HDRS} ${IO_HDRS} ${GROUP_HDRS}
  ${DEBUG_HDRS} ${DF_HDRS} ${DEVICES_HDRS} ${NODE_HDRS} ${QUOTA_HDRS} ${SPACE_HDRS} ${CONFIG_HDRS} ${ACCESS_HDRS}
  ${FSCK_HDRS} ${SHARE_HDRS} ${GRPC_HDRS} ${CONVERT_HDRS} ${ATTR_HDRS}
  ${AUTH_HDRS} ${CHMOD_HDRS} ${CHOWN_HDRS} ${CP_HDRS} ${FILE_HDRS} ${FILEINFO_HDRS} ${LS_HDRS}
  ${METADATA_HDRS} ${MKDIR_HDRS} ${MOVE_HDRS} ${RMDIR_HDRS} ${STAT_HDRS}
  ${TOUCH_HDRS} ${VERSION_HDRS} ${VID_HDRS} ${WHO_HDRS} ${WHOAMI_HDRS}
  ${GEOSCHED_HDRS} ${HEALTH_HDRS} ${ARCHIVE_HDRS} ${BACKUP_HDRS} ${MAP_HDRS}
  ${MEMBER_HDRS} ${STATUS_HDRS} ${SCHED_HDRS}
)

set_source_files_properties(
  ${CLI_PROTO_SRCS} ${CLI_PROTO_HDRS}
  PROPERTIES GENERATED 1)

add_library(EosCliProto-Objects OBJECT
  ${CLI_PROTO_SRCS} ${CLI_PROTO_HDRS})

target_link_libraries(EosCliProto-Objects PUBLIC
  PROTOBUF::PROTOBUF
  ABSL::ABSL)

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

  set(GRPC_PROTOS ${CMAKE_SOURCE_DIR}/common/grpc-proto/Rpc.proto)
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

  # @note see remark from RestGrpc-Objects
  target_compile_options(EosGrpcProto-Objects PRIVATE -Wno-sign-compare)

  add_dependencies(EosGrpcProto-Objects RpcFileGeneration)

  target_link_libraries(EosGrpcProto-Objects PUBLIC
    GRPC::grpc
    GRPC::grpc++
    PROTOBUF::PROTOBUF
    ABSL::ABSL)

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
  grpc_generate_cpp(ACCESS_GRPC_SRCS ACCESS_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Access.proto)
  grpc_generate_cpp(ACL_GRPC_SRCS ACL_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Acl.proto)
  grpc_generate_cpp(ATTR_GRPC_SRCS ATTR_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Attr.proto)
  grpc_generate_cpp(AUTH_GRPC_SRCS AUTH_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Authentication.proto)
  grpc_generate_cpp(CHMOD_GRPC_SRCS CHMOD_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/ChangeMode.proto)
  grpc_generate_cpp(CHOWN_GRPC_SRCS CHOWN_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Chown.proto)
  grpc_generate_cpp(CONFIG_GRPC_SRCS CONFIG_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Config.proto)
  grpc_generate_cpp(CP_GRPC_SRCS CP_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Cp.proto)
  grpc_generate_cpp(DEBUG_GRPC_SRCS DEBUG_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Debug.proto)
  grpc_generate_cpp(FILE_GRPC_SRCS FILE_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/File.proto)
  grpc_generate_cpp(FILEINFO_GRPC_SRCS FILEINFO_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Fileinfo.proto)
  grpc_generate_cpp(FIND_GRPC_SRCS FIND_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Find.proto)
  grpc_generate_cpp(FS_GRPC_SRCS FS_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Fs.proto)
  grpc_generate_cpp(FSCK_GRPC_SRCS FSCK_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Fsck.proto)
  grpc_generate_cpp(GROUP_GRPC_SRCS GROUP_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Group.proto)
  grpc_generate_cpp(IO_GRPC_SRCS IO_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Io.proto)
  grpc_generate_cpp(LS_GRPC_SRCS LS_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Ls.proto)
  grpc_generate_cpp(METADATA_GRPC_SRCS METADATA_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Metadata.proto)
  grpc_generate_cpp(MKDIR_GRPC_SRCS MKDIR_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/MakeDirectory.proto)
  grpc_generate_cpp(MOVE_GRPC_SRCS MOVE_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Move.proto)
  grpc_generate_cpp(NODE_GRPC_SRCS NODE_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Node.proto)
  grpc_generate_cpp(NS_GRPC_SRCS NS_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Ns.proto)
  grpc_generate_cpp(QUOTAGRPC_SRCS QUOTA_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Quota.proto)
  grpc_generate_cpp(RECY_GRPC_SRCS RECY_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${CMAKE_SOURCE_DIR}/common/grpc-proto/Recycle.proto)
  grpc_generate_cpp(REP_GRPC_SRCS REP_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/ConsoleReply.proto)
  grpc_generate_cpp(REQ_GRPC_SRCS REQ_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/ConsoleRequest.proto)
  grpc_generate_cpp(RM_GRPC_SRCS RM_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Rm.proto)
  grpc_generate_cpp(RMDIR_GRPC_SRCS RMDIR_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Rmdir.proto)
  grpc_generate_cpp(ROUTE_GRPC_SRCS ROUTE_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Route.proto)
  grpc_generate_cpp(SPACE_GRPC_SRCS SPACE_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Space.proto)
  grpc_generate_cpp(STAT_GRPC_SRCS STAT_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/StatWnc.proto)
  grpc_generate_cpp(TOUCH_GRPC_SRCS TOUCH_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Touch.proto)
  grpc_generate_cpp(VERSION_GRPC_SRCS VERSION_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Version.proto)
  grpc_generate_cpp(VID_GRPC_SRCS VID_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Vid.proto)
  grpc_generate_cpp(WHO_GRPC_SRCS WHO_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Who.proto)
  grpc_generate_cpp(WHOAMI_GRPC_SRCS WHOAMI_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Whoami.proto)
  grpc_generate_cpp(GEOSCHED_GRPC_SRCS GEOSCHED_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Geosched.proto)
  grpc_generate_cpp(HEALTH_GRPC_SRCS HEALTH_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Health.proto)
  grpc_generate_cpp(ARCHIVE_GRPC_SRCS ARCHIVE_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Archive.proto)
  grpc_generate_cpp(BACKUP_GRPC_SRCS BACKUP_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Backup.proto)
  grpc_generate_cpp(MAP_GRPC_SRCS MAP_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Map.proto)
  grpc_generate_cpp(MEMBER_GRPC_SRCS MEMBER_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Member.proto)
  grpc_generate_cpp(STATUS_GRPC_SRCS STATUS_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Status.proto)
  grpc_generate_cpp(SCHED_GRPC_SRCS SCHED_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Sched.proto)
  grpc_generate_cpp(SHAPING_GRPC_SRCS SHAPING_GRPC_HDRS ${GRPC_PROTOBUF_PATH} ${EOS_PROTO_ROOT}/Shaping.proto)

  set(CLI_GRPC_SRCS
    ${ACCESS_GRPC_SRCS} ${ACL_GRPC_SRCS} ${ATTR_GRPC_SRCS} ${AUTH_GRPC_SRCS}
    ${CHMOD_GRPC_SRCS} ${CHOWN_GRPC_SRCS} ${CONFIG_GRPC_SRCS} ${DEBUG_GRPC_SRCS}
    ${FILE_GRPC_SRCS} ${FILEINFO_GRPC_SRCS} ${FIND_GRPC_SRCS}
    ${FS_GRPC_SRCS} ${FSCK_GRPC_SRCS} ${GROUP_GRPC_SRCS} ${IO_GRPC_SRCS}
    ${LS_GRPC_SRCS} ${METADATA_GRPC_SRCS} ${MKDIR_GRPC_SRCS}
    ${MOVE_GRPC_SRCS} ${NODE_GRPC_SRCS} ${NS_GRPC_SRCS} ${QUOTA_GRPC_SRCS}
    ${RECY_GRPC_SRCS} ${REP_GRPC_SRCS} ${REQ_GRPC_SRCS} ${RM_GRPC_SRCS}
    ${RMDIR_GRPC_SRCS} ${ROUTE_GRPC_SRCS} ${SPACE_GRPC_SRCS} ${EVICT_GRPC_SRCS}
    ${STAT_GRPC_SRCS} ${TOUCH_GRPC_SRCS} ${VERSION_GRPC_SRCS}
    ${VID_GRPC_SRCS} ${WHO_GRPC_SRCS} ${WHOAMI_GRPC_SRCS}
    ${CP_GRPC_SRCS} ${GEOSCHED_GRPC_SRCS} ${HEALTH_GRPC_SRCS} ${ARCHIVE_GRPC_SRCS}
    ${BACKUP_GRPC_SRCS} ${MAP_GRPC_SRCS} ${MEMBER_GRPC_SRCS} ${STATUS_GRPC_SRCS}
    ${SCHED_GRPC_SRCS})

  set(CLI_GRPC_HDRS
    ${ACCESS_GRPC_HDRS} ${ACL_GRPC_HDRS} ${ATTR_GRPC_HDRS} ${AUTH_GRPC_HDRS}
    ${CHMOD_GRPC_HDRS} ${CHOWN_GRPC_HDRS} ${CONFIG_GRPC_HDRS} ${DEBUG_GRPC_HDRS}
    ${FILE_GRPC_HDRS} ${FILEINFO_GRPC_HDRS} ${FIND_GRPC_HDRS}
    ${FS_GRPC_HDRS} ${FSCK_GRPC_HDRS} ${GROUP_GRPC_HDRS} ${IO_GRPC_HDRS}
    ${LS_GRPC_HDRS} ${METADATA_GRPC_HDRS} ${MKDIR_GRPC_HDRS}
    ${MOVE_GRPC_HDRS} ${NODE_GRPC_HDRS} ${NS_GRPC_HDRS} ${QUOTA_GRPC_HDRS}
    ${RECY_GRPC_HDRS} ${REP_GRPC_HDRS} ${REQ_GRPC_HDRS} ${RM_GRPC_HDRS}
    ${RMDIR_GRPC_HDRS} ${ROUTE_GRPC_HDRS} ${SPACE_GRPC_HDRS} ${EVICT_GRPC_HDRS}
    ${STAT_GRPC_HDRS} ${TOUCH_GRPC_HDRS} ${VERSION_GRPC_HDRS}
    ${VID_GRPC_HDRS} ${WHO_GRPC_HDRS} ${WHOAMI_GRPC_HDRS}
    ${CP_GRPC_HDRS} ${GEOSCHED_GRPC_HDRS} ${HEALTH_GRPC_HDRS} ${ARCHIVE_GRPC_HDRS}
    ${BACKUP_GRPC_HDRS} ${MAP_GRPC_HDRS} ${MEMBER_GRPC_HDRS} ${STATUS_GRPC_HDRS}
    ${SCHED_GRPC_SRCS})

  set_source_files_properties(
    ${CLI_GRPC_SRCS}
    ${CLI_GRPC_HDRS}
    PROPERTIES GENERATED TRUE)

  add_library(EosCliGrpc-Objects OBJECT
    ${CLI_GRPC_SRCS}
    ${CLI_GRPC_HDRS})

  # @note see remark from RestGrpc-Objects
  target_compile_options(EosCliGrpc-Objects PRIVATE -Wno-sign-compare)

  add_dependencies(EosCliGrpc-Objects EosCliProto-Objects)

  target_link_libraries(EosCliGrpc-Objects PUBLIC
    GRPC::grpc++
    PROTOBUF::PROTOBUF
    ABSL::ABSL)

  target_include_directories(EosCliGrpc-Objects PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)

  set_target_properties(EosCliGrpc-Objects PROPERTIES
    POSITION_INDEPENDENT_CODE TRUE)
endif()

#------------------------------------------------------------------------------
# Generate protobuf and gRPC objects for EOS Windows native client
#------------------------------------------------------------------------------
if (GRPC_FOUND)
  set(WNC_PROTO ${EOS_PROTO_ROOT}/EosWnc.proto)
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

  # @note see remark from RestGrpc-Objects
  target_compile_options(EosWncGrpcProto-Objects PRIVATE -Wno-sign-compare)

  add_dependencies(EosWncGrpcProto-Objects
    EosCliProto-Objects
    EosCliGrpc-Objects)

  target_link_libraries(EosWncGrpcProto-Objects PUBLIC
    GRPC::grpc++
    PROTOBUF::PROTOBUF
    ABSL::ABSL)

  target_include_directories(EosWncGrpcProto-Objects PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)

  set_target_properties(EosWncGrpcProto-Objects PROPERTIES
    POSITION_INDEPENDENT_CODE TRUE)
endif()

set(REST_GW_PROTO eos_rest_gateway_service)
set(PROTO_PATH ${CMAKE_SOURCE_DIR}/proto/eos_rest_gateway/${REST_GW_PROTO}.proto)
set(REST_GW_PROTOS ${CMAKE_SOURCE_DIR}/proto/eos_rest_gateway/${REST_GW_PROTO}.proto)
set(REST_GW_SRCS ${CMAKE_BINARY_DIR}/proto/eos_rest_gateway/${REST_GW_PROTO}.pb.cc)
set(REST_GW_HDRS ${CMAKE_BINARY_DIR}/proto/eos_rest_gateway/${REST_GW_PROTO}.pb.h)
set(REST_GW_GRPC_SRCS  ${CMAKE_BINARY_DIR}/proto/eos_rest_gateway/${REST_GW_PROTO}.grpc.pb.cc)
set(REST_GW_GRPC_HDRS  ${CMAKE_BINARY_DIR}/proto/eos_rest_gateway/${REST_GW_PROTO}.grpc.pb.h)
set(GW_GO_GEN ${CMAKE_BINARY_DIR}/go/eos.rest.gateway.service/${REST_GW_PROTO}.pb.gw.go)
set(PROTO_GRPC_GO ${CMAKE_BINARY_DIR}/go/eos.rest.gateway.service/${REST_GW_PROTO}_grpc.pb.go)
set(PROTO_GO ${CMAKE_BINARY_DIR}/go/eos.rest.gateway.service/${REST_GW_PROTO}.pb.go)

set(ANN_PROTO1 annotations)
set(ANN_PROTO2 http)
set(ANN_PROTO1_PATH ${CMAKE_SOURCE_DIR}/proto/eos_rest_gateway/google/api/${ANN_PROTO1}.proto)
set(ANN_PROTO2_PATH ${CMAKE_SOURCE_DIR}/proto/eos_rest_gateway/google/api/${ANN_PROTO2}.proto)
set(ANN_PROTOS ${ANN_PROTO1_PATH} ${ANN_PROTO2_PATH})
set(ANN_SRCS ${CMAKE_BINARY_DIR}/proto/eos_rest_gateway/google/api/${ANN_PROTO1}.pb.cc
             ${CMAKE_BINARY_DIR}/proto/eos_rest_gateway/google/api/${ANN_PROTO2}.pb.cc)
set(ANN_HDRS ${CMAKE_BINARY_DIR}/proto/eos_rest_gateway/google/api/${ANN_PROTO1}.pb.h
             ${CMAKE_BINARY_DIR}/proto/eos_rest_gateway/google/api//${ANN_PROTO2}.pb.h)

set_source_files_properties(
    ${ANN_SRCS} ${ANN_HDRS}
    PROPERTIES GENERATED TRUE)

# protoc command for http.proto and annotations.proto
add_custom_command(
    OUTPUT ${ANN_SRCS} ${ANN_HDRS}
    COMMAND ${PROTOBUF3_PROTOC_EXECUTABLE} -I${CMAKE_SOURCE_DIR} -I${CMAKE_SOURCE_DIR}/common/grpc-proto/ -I${CMAKE_SOURCE_DIR}/proto/eos_rest_gateway -I${GRPC_INCLUDE_DIR}
            --cpp_out=../ ${ANN_PROTOS})

add_library(RestAnnot-Objects OBJECT
    ${ANN_SRCS} ${ANN_HDRS})

target_include_directories(RestAnnot-Objects PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_BINARY_DIR}>")

set_target_properties(RestAnnot-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

target_link_libraries(RestAnnot-Objects PUBLIC
    GRPC::grpc++)

add_custom_target(
  GoogleApis-Target
  COMMAND echo "Running protoc for annotations and http"
  DEPENDS RestAnnot-Objects)

# # protoc command for echo_service.proto
add_custom_command(
    OUTPUT ${REST_GW_SRCS} ${REST_GW_HDRS} ${REST_GW_GRPC_SRCS} ${REST_GW_GRPC_HDRS}
    COMMAND ${CMAKE_COMMAND} -E env
    "LD_LIBRARY_PATH=${GRPC_LD_LIBRARY_PATH}:${LD_LIBRARY_PATH}"
    ${PROTOBUF3_PROTOC_EXECUTABLE}
            -I${CMAKE_SOURCE_DIR}
            -I${CMAKE_SOURCE_DIR}/common/grpc-proto/
            -I${CMAKE_SOURCE_DIR}/proto/${EOS_PROTO_ROOT}
            -I${GRPC_INCLUDE_DIR}
            --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
            --cpp_out=../
            --grpc_out=../
            ${REST_GW_PROTOS}
    DEPENDS GoogleApis-Target EosCliProto-Objects)

set_source_files_properties(
    ${REST_GW_SRCS} ${REST_GW_HDRS} ${REST_GW_GRPC_SRCS} ${REST_GW_GRPC_HDRS}
    PROPERTIES GENERATED TRUE)

add_library(RestGrpc-Objects OBJECT
  ${REST_GW_SRCS} ${REST_GW_HDRS}
  ${REST_GW_GRPC_SRCS} ${REST_GW_GRPC_HDRS})

# @note avoid warning coming from from protobuf implementation
# /opt/eos/grpc/include/grpcpp/support/proto_buffer_reader.h:157:24:
# warning: comparison of integer expressions of different signedness:
# ‘uint64_t’ {aka ‘long unsigned int’} and ‘int’ [-Wsign-compare]
#       if (slice_length <= count) {
#           ~~~~~~~~~~~~~^~~~~~~~
target_compile_options(RestGrpc-Objects PRIVATE -Wno-sign-compare)

target_include_directories(RestGrpc-Objects PUBLIC
  "$<BUILD_INTERFACE:${CMAKE_BINARY_DIR};${CMAKE_CURRENT_BINARY_DIR}>;")

target_link_libraries(RestGrpc-Objects PUBLIC
    GRPC::grpc++)

set_target_properties(RestGrpc-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)
