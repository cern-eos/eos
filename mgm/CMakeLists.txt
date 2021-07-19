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

include_directories(${CMAKE_SOURCE_DIR}/common/xrootd-ssi-protobuf-interface/include)

#-------------------------------------------------------------------------------
# Generate protocol buffer files
#-------------------------------------------------------------------------------
PROTOBUF_GENERATE_CPP(FUSEX_SRCS FUSEX_HDRS fusex.proto)
set_source_files_properties(
  ${FUSEX_SRCS}
  ${FUSEX_HDRS}
  PROPERTIES GENERATED 1)

add_library(EosMgmProto-Objects OBJECT
  ${FUSEX_SRCS} ${FUSEX_HDRS})

target_link_libraries(EosMgmProto-Objects PUBLIC
  PROTOBUF::PROTOBUF)

#-------------------------------------------------------------------------------
# EosMgmHttp library
#-------------------------------------------------------------------------------
add_library(EosMgmHttp-Objects OBJECT
  http/xrdhttp/EosMgmHttpHandler.hh
  http/xrdhttp/EosMgmHttpHandler.cc)

target_link_libraries(EosMgmHttp-Objects PUBLIC
  EosAuthProto-Objects
  EosCommonServer
  XROOTD::PRIVATE
  XROOTD::HTTP)

set_target_properties(EosMgmHttp-Objects
  PROPERTIES POSITION_INDEPENDENT_CODE TRUE)

add_library(EosMgmHttp-${XRDPLUGIN_SOVERSION} MODULE
  $<TARGET_OBJECTS:EosMgmHttp-Objects>)

target_link_libraries(EosMgmHttp-${XRDPLUGIN_SOVERSION} PUBLIC
  EosMgmHttp-Objects)

install(
  TARGETS EosMgmHttp-${XRDPLUGIN_SOVERSION}
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

set (MGM_TGC_SRC_FILES
  tgc/AsyncUint64ShellCmd.cc
  tgc/DummyTapeGcMgm.cc
  tgc/FreedBytesHistogram.cc
  tgc/IClock.cc
  tgc/ITapeGcMgm.cc
  tgc/Lru.cc
  tgc/MaxLenExceeded.cc
  tgc/MultiSpaceTapeGc.cc
  tgc/RealClock.cc
  tgc/RealTapeGcMgm.cc
  tgc/SmartSpaceStats.cc
  tgc/SpaceNotFound.cc
  tgc/SpaceToTapeGcMap.cc
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
  proc/user/NewfindCmd.cc proc/user/NewfindCmd.hh
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
  convert/old/Converter.cc
  GroupBalancer.cc
  GeoBalancer.cc
  Features.cc
  ZMQ.cc
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
  InFlightTracker.cc
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
  bulk-request/File.cc
  bulk-request/FileCollection.cc
  bulk-request/prepare/PrepareManager.cc
  bulk-request/prepare/BulkRequestPrepareManager.cc
  bulk-request/BulkRequest.cc
  bulk-request/prepare/StageBulkRequest.cc
  bulk-request/prepare/EvictBulkRequest.cc
  bulk-request/business/BulkRequestBusiness.cc
  bulk-request/BulkRequestFactory.cc
  bulk-request/dao/factories/ProcDirectoryDAOFactory.cc
  bulk-request/dao/proc/ProcDirectoryBulkRequestDAO.cc
  bulk-request/dao/proc/ProcDirectoryBulkRequestLocations.cc
  bulk-request/exception/PersistencyException.cc
  bulk-request/exception/BulkRequestException.cc
  bulk-request/interface/RealMgmFileSystemInterface.cc
  bulk-request/prepare/PrepareUtils.cc
  bulk-request/utils/json/Jsonifier.cc
  bulk-request/utils/json/JSONCppJsonifier.cc
  bulk-request/prepare/query-prepare/QueryPrepareResult.cc
  ${FUSEX_SRCS})

target_link_libraries(XrdEosMgm-Objects PUBLIC
  EosCliProto-Objects
  EosAuthProto-Objects
  XrdSsiPbEosCta-Objects
  EosGrpcProto-Objects
  EosCommonServer
  LIBMICROHTTPD::LIBMICROHTTPD
  GOOGLE::SPARSEHASH
  JSONCPP::JSONCPP
  XROOTD::PRIVATE
  LDAP::LDAP
  ZMQ::ZMQ)

target_compile_definitions(XrdEosMgm-Objects PUBLIC
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

set_target_properties(XrdEosMgm-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

add_library(XrdEosMgm-${XRDPLUGIN_SOVERSION} MODULE
  $<TARGET_OBJECTS:XrdEosMgm-Objects>)

target_compile_definitions(XrdEosMgm-${XRDPLUGIN_SOVERSION} PUBLIC
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

target_link_libraries(XrdEosMgm-${XRDPLUGIN_SOVERSION} PUBLIC
  XrdEosMgm-Objects
  EosAuthProto-Objects
  EosGrpcProto-Objects
  ZLIB::ZLIB
  XROOTD::CL
  XROOTD::SSI
  XROOTD::PRIVATE
  EosCommon
  EosNsCommon)

install(TARGETS XrdEosMgm-${XRDPLUGIN_SOVERSION}
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

install(PROGRAMS eos-repair-tool
  DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
  PERMISSIONS OWNER_READ OWNER_EXECUTE
              GROUP_READ GROUP_EXECUTE
              WORLD_READ WORLD_EXECUTE)

#-------------------------------------------------------------------------------
# Create executables for testing the scheduling part
#-------------------------------------------------------------------------------
# @todo (esindril): Move these to the test directory
add_executable(testschedulingtree
  geotree/SchedulingTreeTest.cc
  geotree/SchedulingSlowTree.cc
  geotree/SchedulingTreeCommon.cc)

target_link_libraries(
  testschedulingtree
  EosCommon
  ${CMAKE_THREAD_LIBS_INIT})

add_executable(eos-config-inspect
  config/eos-config-inspect.cc
  config/QuarkConfigHandler.cc

  ../common/config/ConfigParsing.cc
)

target_link_libraries(
  eos-config-inspect
  EosCommon
  EosNsCommon-Static)

#-------------------------------------------------------------------------------
# Create executables for testing the MGM configuration
#-------------------------------------------------------------------------------
if(Linux)
  add_library(XrdEosMgm-Static STATIC
    $<TARGET_OBJECTS:XrdEosMgm-Objects>)

  target_compile_definitions(XrdEosMgm-Static PUBLIC
    -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

  target_link_libraries(XrdEosMgm-Static PUBLIC
    XrdEosMgm-Objects
    EosAuthProto-Objects
    EosGrpcProto-Objects
    ZLIB::ZLIB
    XROOTD::SSI
    XROOTD::PRIVATE
    EosNsCommon-Static
    EosCommonServer-Static)

  install(TARGETS eos-config-inspect
    LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
endif()
