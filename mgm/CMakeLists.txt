# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Andreas-Joachim Peters - CERN
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
  ${XROOTD_INCLUDE_DIR}
  ${XROOTD_PRIVATE_INCLUDE_DIR}
  ${Z_INCLUDE_DIRS}
  ${ZMQ_INCLUDE_DIRS}
  ${JSONCPP_INCLUDE_DIR}
  ${LDAP_INCLUDE_DIRS}
  ${XROOTD_INCLUDE_DIRS}
  ${NCURSES_INCLUDE_DIRS}
  ${PROTOBUF_INCLUDE_DIRS}
  ${SPARSEHASH_INCLUDE_DIRS}
  ${FOLLY_INCLUDE_DIRS}
  ${CMAKE_BINARY_DIR}
  ${CMAKE_BINARY_DIR}/auth_plugin/
  ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/qclient/include
  ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/qclient/src
  ${CMAKE_SOURCE_DIR}/common/xrootd-ssi-protobuf-interface/include
  ${CMAKE_BINARY_DIR}/common/eos_cta_pb)

# Hide Clang "inconsistent-missing-override" compiler warnings
# XrdMgmOfs inherits XrdSfsFileSystem, leading to many warnings
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-inconsistent-missing-override")
endif()

#-------------------------------------------------------------------------------
# Generate protocol buffer files
#-------------------------------------------------------------------------------
PROTOBUF_GENERATE_CPP(FUSEX_SRCS FUSEX_HDRS fusex.proto)
set_source_files_properties(
  ${FUSEX_SRCS}
  ${FUSEX_HDRS}
  PROPERTIES GENERATED 1)

add_library(
  EosMgmProto-Objects OBJECT
  ${FUSEX_SRCS} ${FUSEX_HDRS})

target_sources(EosMgmProto-Objects PUBLIC
  ${FUSEX_SRCS} ${FUSEX_HDRS})

#-------------------------------------------------------------------------------
# EosMgmHttp library
#-------------------------------------------------------------------------------
add_library(
  EosMgmHttp-Objects OBJECT
  http/xrdhttp/EosMgmHttpHandler.hh
  http/xrdhttp/EosMgmHttpHandler.cc)

set_target_properties(
  EosMgmHttp-Objects
  PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#add_dependencies(EosMgmHttp-Objects EosProtobuf)
add_dependencies(EosMgmHttp-Objects EosAuthProto-Objects)

add_library(EosMgmHttp-${PLUGIN_VERSION} MODULE
  $<TARGET_OBJECTS:EosMgmHttp-Objects>)

target_link_libraries(EosMgmHttp-${PLUGIN_VERSION} PUBLIC
  EosCommon
  EosCommonServer
  ${XROOTD_HTTP_UTILS_LIBRARY})

install(
  TARGETS EosMgmHttp-${PLUGIN_VERSION}
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

set (MGM_TGC_SRC_FILES
  tgc/MultiSpaceTapeGc.cc
  tgc/SpaceToTapeGcMap.cc
  tgc/DummyTapeGcMgm.cc
  tgc/FreedBytesHistogram.cc
  tgc/IClock.cc
  tgc/ITapeGcMgm.cc
  tgc/Lru.cc
  tgc/MaxLenExceeded.cc
  tgc/RealClock.cc
  tgc/RealTapeGcMgm.cc
  tgc/SmartSpaceStats.cc
  tgc/SpaceNotFound.cc
  tgc/TapeGc.cc
  tgc/Utils.cc)

foreach(MGM_TGC_SRC_FILE_SRC ${MGM_TGC_SRC_FILES})
  set_property(SOURCE ${MGM_TGC_SRC_FILE_SRC} APPEND_STRING PROPERTY COMPILE_FLAGS " -Werror")
endforeach(MGM_TGC_SRC_FILE_SRC)

#-------------------------------------------------------------------------------
# XrdEosMgm-Objects library
#-------------------------------------------------------------------------------
add_library(
  XrdEosMgm-Objects OBJECT
  auth/AccessChecker.cc                             auth/AccessChecker.hh
  config/FileConfigEngine.cc                        config/FileConfigEngine.hh
  config/IConfigEngine.cc                           config/IConfigEngine.hh
  config/QuarkConfigHandler.cc                      config/QuarkConfigHandler.hh
  config/QuarkDBConfigEngine.cc                     config/QuarkDBConfigEngine.hh
  Access.cc
  Macros.cc
  GeoTreeEngine.cc
  Messaging.cc
  ${MGM_TGC_SRC_FILES}
  Policy.cc
  proc/IProcCommand.cc
  proc/ProcInterface.cc
  proc/ProcCommand.cc
  proc/proc_fs.cc
  proc/admin/Backup.cc
  proc/admin/FsCmd.cc
  proc/admin/FsckCmd.cc
  proc/admin/Fusex.cc
  proc/admin/GeoSched.cc
  proc/admin/NsCmd.cc
  proc/admin/Rtlog.cc
  proc/admin/StagerRmCmd.cc proc/admin/StagerRmCmd.hh
  proc/admin/Transfer.cc
  proc/admin/Vid.cc
  proc/admin/Access.cc # @todo (faluchet) drop when move to 5.0.0
  proc/admin/AccessCmd.cc proc/admin/AccessCmd.hh
  proc/admin/Config.cc # @todo (faluchet) drop when move to 5.0.0
  proc/admin/ConfigCmd.cc proc/admin/ConfigCmd.hh
  proc/admin/ConvertCmd.cc proc/admin/ConvertCmd.hh
  proc/admin/Debug.cc # @todo (faluchet) drop when move to 5.0.0
  proc/admin/DebugCmd.cc proc/admin/DebugCmd.hh
  proc/admin/Group.cc # @todo (faluchet) drop when move to 5.0.0
  proc/admin/GroupCmd.cc proc/admin/GroupCmd.hh
  proc/admin/Io.cc # @todo (faluchet) drop when move to 5.0.0
  proc/admin/IoCmd.cc proc/admin/IoCmd.hh
  proc/admin/Node.cc # @todo (faluchet) drop when move to 5.0.0
  proc/admin/NodeCmd.cc proc/admin/NodeCmd.hh
  proc/admin/Space.cc # @todo (faluchet) drop when move to 5.0.0
  proc/admin/SpaceCmd.cc proc/admin/SpaceCmd.hh
  proc/admin/Quota.cc # @todo (faluchet) drop when move to 5.0.0
  proc/admin/QuotaCmd.cc proc/admin/QuotaCmd.hh
  proc/user/Quota.cc # @todo (faluchet) drop when move to 5.0.0
  proc/user/Accounting.cc
  proc/user/AclCmd.cc
  proc/user/Attr.cc
  proc/user/Archive.cc
  proc/user/Cd.cc
  proc/user/Chmod.cc
  proc/user/Chown.cc
  proc/user/File.cc
  proc/user/Fileinfo.cc
  proc/user/Find.cc
  proc/user/FindCmd.cc
  proc/user/Fuse.cc
  proc/user/FuseX.cc
  proc/user/Ls.cc
  proc/user/Map.cc
  proc/user/Member.cc
  proc/user/Mkdir.cc
  proc/user/Motd.cc
  proc/user/QoSCmd.cc
  proc/user/RecycleOld.cc
  proc/user/RecycleCmd.cc
  proc/user/Rm.cc
  proc/user/RmCmd.cc
  proc/user/Rmdir.cc
  proc/user/RouteCmd.cc
  proc/user/TokenCmd.cc
  proc/user/Version.cc
  proc/user/Who.cc
  proc/user/Whoami.cc
  qos/QoSConfig.cc                                  qos/QoSConfig.hh
  qos/QoSClass.hh
  Quota.cc
  Scheduler.cc
  Vid.cc
  FsView.cc
  XrdMgmOfsConfigure.cc
  XrdMgmOfsFile.cc
  XrdMgmOfsDirectory.cc
  XrdMgmOfs.cc
  XrdMgmAuthz.cc
  XrdMgmOfs/fsctl/Access.cc
  XrdMgmOfs/fsctl/AdjustReplica.cc
  XrdMgmOfs/fsctl/Checksum.cc
  XrdMgmOfs/fsctl/Chmod.cc
  XrdMgmOfs/fsctl/Chown.cc
  XrdMgmOfs/fsctl/Commit.cc
  XrdMgmOfs/fsctl/CommitHelper.cc
  XrdMgmOfs/fsctl/Drop.cc
  XrdMgmOfs/fsctl/Event.cc
  XrdMgmOfs/fsctl/Fusex.cc
  XrdMgmOfs/fsctl/Getfmd.cc
  XrdMgmOfs/fsctl/GetFusex.cc
  XrdMgmOfs/fsctl/MasterSignal.cc
  XrdMgmOfs/fsctl/Mkdir.cc
  XrdMgmOfs/fsctl/Open.cc
  XrdMgmOfs/fsctl/Readlink.cc
  XrdMgmOfs/fsctl/Redirect.cc
  XrdMgmOfs/fsctl/Schedule2Balance.cc
  XrdMgmOfs/fsctl/Schedule2Delete.cc
  XrdMgmOfs/fsctl/Stat.cc
  XrdMgmOfs/fsctl/Statvfs.cc
  XrdMgmOfs/fsctl/Symlink.cc
  XrdMgmOfs/fsctl/Txstate.cc
  XrdMgmOfs/fsctl/Utimes.cc
  XrdMgmOfs/fsctl/Version.cc
  XrdMgmOfs/fsctl/Xattr.cc
  Balancer.cc
  CommandMap.cc
  FileSystem.cc
  convert/ConversionInfo.cc
  convert/ConversionJob.cc
  convert/ConverterDriver.cc
  drain/DrainFs.cc
  drain/DrainTransferJob.cc
  drain/Drainer.cc
  Egroup.cc
  AdminSocket.cc
  Acl.cc
  Stat.cc
  Iostat.cc
  fsck/Fsck.cc
  fsck/FsckEntry.cc
  utils/FileSystemRegistry.cc                  utils/FileSystemRegistry.hh
  utils/FilesystemUuidMapper.cc                utils/FilesystemUuidMapper.hh
  txengine/TransferEngine.cc
  txengine/TransferFsDB.cc
  Converter.cc
  GroupBalancer.cc
  GeoBalancer.cc
  Features.cc
  ZMQ.cc
  FuseNotificationGuard.cc FuseNotificationGuard.hh
  FuseServer/Server.cc FuseServer/Server.hh
  FuseServer/Clients.cc FuseServer/Clients.hh
  FuseServer/Locks.cc FuseServer/Locks.hh
  FuseServer/Caps.cc FuseServer/Caps.hh
  FuseServer/Flush.cc FuseServer/Flush.hh
  fuse-locks/LockTracker.cc   fuse-locks/LockTracker.hh
  IMaster.cc                  IMaster.hh
  Master.cc
  QdbMaster.cc
  Recycle.cc
  PathRouting.cc
  RouteEndpoint.cc
  LRU.cc
  WFE.cc
  Workflow.cc
  grpc/GrpcServer.cc   grpc/GrpcServer.hh
  grpc/GrpcNsInterface.cc   grpc/GrpcNsInterface.hh
  grpc/GrpcManilaInterface.cc   grpc/GrpcManilaInterface.hh
  http/HttpServer.cc
  http/HttpHandler.cc
  http/s3/S3Handler.cc
  http/s3/S3Store.cc
  http/webdav/WebDAVHandler.cc
  http/webdav/WebDAVResponse.cc
  http/webdav/PropFindResponse.cc
  http/webdav/PropPatchResponse.cc
  http/webdav/LockResponse.cc
  geotree/SchedulingTreeTest.cc
  geotree/SchedulingSlowTree.cc
  geotree/SchedulingTreeCommon.cc
  tracker/ReplicationTracker.cc
  inspector/FileInspector.cc
  ${FUSEX_SRCS})

#-------------------------------------------------------------------------------
# Add dependency to protobuf objects so we guarantee that the protocol
# buffer files are generated when we try to build XrdEosMgm
#-------------------------------------------------------------------------------
add_dependencies(XrdEosMgm-Objects EosCliProto-Objects)
add_dependencies(XrdEosMgm-Objects EosAuthProto-Objects)
add_dependencies(XrdEosMgm-Objects XrdSsiPbEosCta-Objects)

if (GRPC_FOUND)
  add_dependencies(XrdEosMgm-Objects EosGrpcProto-Objects)
endif ()

target_compile_definitions(XrdEosMgm-Objects PUBLIC
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

set_target_properties(
  XrdEosMgm-Objects
  PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

add_library(XrdEosMgm-${PLUGIN_VERSION} MODULE
  $<TARGET_OBJECTS:EosAuthProto-Objects>
  $<TARGET_OBJECTS:XrdEosMgm-Objects>
  $<TARGET_OBJECTS:XrdSsiPbEosCta-Objects>)

target_compile_definitions(
  XrdEosMgm-${PLUGIN_VERSION} PUBLIC
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

if (MICROHTTPD_FOUND)
  target_compile_definitions(
    XrdEosMgm-${PLUGIN_VERSION} PUBLIC -DEOS_MICRO_HTTPD=1)
endif()

target_link_libraries(
  XrdEosMgm-${PLUGIN_VERSION} PUBLIC
  qclient
  EosCommon
  EosCommonServer
  EosNsCommon
  bfd
  ${FOLLY_LIBRARIES}
  ${Z_LIBRARY}
  ${ZMQ_LIBRARIES}
  ${LDAP_LIBRARIES}
  ${UUID_LIBRARIES}
  ${NCURSES_LIBRARY}
  ${GLIBC_DL_LIBRARY}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_POSIX_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${XROOTD_SSI_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT}
  ${JSONCPP_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT})

if (GRPC_FOUND)
  target_link_libraries(
    XrdEosMgm-${PLUGIN_VERSION} PUBLIC
    ${GRPC_LIBRARY}
    ${GRPC_GRPC++_LIBRARY})
endif()

install(
  TARGETS XrdEosMgm-${PLUGIN_VERSION}
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

install(
  PROGRAMS eos-repair-tool
  DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
  PERMISSIONS OWNER_READ OWNER_EXECUTE
              GROUP_READ GROUP_EXECUTE
              WORLD_READ WORLD_EXECUTE)

#-------------------------------------------------------------------------------
# Create executables for testing the scheduling part
#-------------------------------------------------------------------------------
# @todo (esindril): Move these to the test directory
add_executable(
  testschedulingtree
  geotree/SchedulingTreeTest.cc
  geotree/SchedulingSlowTree.cc
  geotree/SchedulingTreeCommon.cc)

target_link_libraries(
  testschedulingtree
  EosCommon
  ${XROOTD_UTILS_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT})

add_executable(eos-config-inspect
  config/eos-config-inspect.cc
  ../common/config/ConfigParsing.cc                       ../common/config/ConfigParsing.hh
)

target_link_libraries(
  eos-config-inspect
  EosCommon
  EosNsCommon-Static
  ${CMAKE_THREAD_LIBS_INIT}
)

#-------------------------------------------------------------------------------
# Create executables for testing the MGM configuration
#-------------------------------------------------------------------------------
if(Linux)
  add_library(XrdEosMgm-Static STATIC
    $<TARGET_OBJECTS:EosAuthProto-Objects>
    $<TARGET_OBJECTS:XrdEosMgm-Objects>
    $<TARGET_OBJECTS:XrdSsiPbEosCta-Objects>)

  target_compile_definitions(
    XrdEosMgm-Static PUBLIC
    -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

  target_link_libraries(
    XrdEosMgm-Static PUBLIC
    EosNsCommon-Static
    EosCommonServer-Static
    ${FOLLY_LIBRARIES}
    ${Z_LIBRARY}
    ${ZMQ_LIBRARIES}
    ${LDAP_LIBRARIES}
    ${UUID_LIBRARIES}
    ${NCURSES_LIBRARY}
    ${GLIBC_DL_LIBRARY}
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_POSIX_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${XROOTD_SSI_LIBRARY}
    ${OPENSSL_CRYPTO_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT}
    ${JSONCPP_LIBRARIES}
    ${CMAKE_THREAD_LIBS_INIT})

  if (GRPC_FOUND)
    target_link_libraries(
      XrdEosMgm-Static PUBLIC
      ${GRPC_LIBRARY}
      ${GRPC_GRPC++_LIBRARY})
  endif()

  install(TARGETS eos-config-inspect
    LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

endif()
