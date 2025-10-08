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

include_directories(${CMAKE_SOURCE_DIR}/common/xrootd-ssi-protobuf-interface/include ${CMAKE_SOURCE_DIR}/common/ ${CMAKE_SOURCE_DIR}/mgm ${CMAKE_SOURCE_DIR}/mgm/ofs/) 

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
  XROOTD::HTTP
  CURL::libcurl)

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
  CtaUtils.cc)

#-------------------------------------------------------------------------------
# XrdEosMgm-Objects library
#-------------------------------------------------------------------------------
add_library(
  XrdEosMgm-Objects OBJECT
  auth/AccessChecker.cc                             auth/AccessChecker.hh
  config/IConfigEngine.cc                           config/IConfigEngine.hh
  config/QuarkConfigHandler.cc                      config/QuarkConfigHandler.hh
  config/QuarkDBConfigEngine.cc                     config/QuarkDBConfigEngine.hh
  access/Access.cc
  macros/Macros.cc
  geotreeengine/GeoTreeEngine.cc
  ${MGM_TGC_SRC_FILES}
  policy/Policy.cc
  namespacestats/NamespaceStats.cc
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
  proc/admin/EvictCmd.cc proc/admin/EvictCmd.hh
  proc/admin/Vid.cc
  proc/admin/Access.cc # @todo (faluchet) drop when move to 5.0.0
  proc/admin/AccessCmd.cc proc/admin/AccessCmd.hh
  proc/admin/ConfigCmd.cc proc/admin/ConfigCmd.hh
  proc/admin/ConvertCmd.cc proc/admin/ConvertCmd.hh
  proc/admin/DebugCmd.cc proc/admin/DebugCmd.hh
  proc/admin/DevicesCmd.cc proc/admin/DevicesCmd.hh
  proc/admin/Group.cc # @todo (faluchet) drop when move to 5.0.0
  proc/admin/GroupCmd.cc proc/admin/GroupCmd.hh
  proc/admin/IoCmd.cc proc/admin/IoCmd.hh
  proc/admin/FileRegisterCmd.cc proc/admin/FileRegisterCmd.hh
  proc/admin/NodeCmd.cc proc/admin/NodeCmd.hh
  proc/admin/SchedCmd.cc
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
  proc/user/DfCmd.cc
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
  proc/user/RecycleCmd.cc
  proc/user/Rm.cc
  proc/user/RmCmd.cc
  proc/user/Rmdir.cc
  proc/user/RouteCmd.cc
  proc/user/TokenCmd.cc
  proc/user/Version.cc
  proc/user/Who.cc
  proc/user/Whoami.cc
  quota/Quota.cc
  scheduler/Scheduler.cc
  vid/Vid.cc
  fsview/FsView.cc
  ofs/XrdMgmOfsConfigure.cc
  ofs/XrdMgmOfsFile.cc
  ofs/XrdMgmOfsDirectory.cc
  ofs/XrdMgmOfs.cc
  authz/XrdMgmAuthz.cc
  ofs/fsctl/Access.cc
  ofs/fsctl/AdjustReplica.cc
  ofs/fsctl/Checksum.cc
  ofs/fsctl/Chmod.cc
  ofs/fsctl/Chown.cc
  ofs/fsctl/Commit.cc
  ofs/fsctl/CommitHelper.cc
  ofs/fsctl/Drop.cc
  ofs/fsctl/Event.cc
  ofs/fsctl/Fusex.cc
  ofs/fsctl/Getfmd.cc
  ofs/fsctl/GetFusex.cc
  ofs/fsctl/Mkdir.cc
  ofs/fsctl/Open.cc
  ofs/fsctl/Readlink.cc
  ofs/fsctl/Redirect.cc
  ofs/fsctl/Schedule2Delete.cc
  ofs/fsctl/Stat.cc
  ofs/fsctl/Statvfs.cc
  ofs/fsctl/Symlink.cc
  ofs/fsctl/Utimes.cc
  ofs/fsctl/Version.cc
  balancer/FsBalancer.cc
  balancer/FsBalancerStats.cc
  commandmap/CommandMap.cc
  filesystem/FileSystem.cc
  convert/ConversionInfo.cc
  convert/ConversionJob.cc
  convert/ConverterEngine.cc
  drain/DrainFs.cc
  drain/DrainTransferJob.cc
  drain/Drainer.cc
  egroup/Egroup.cc
  adminsocket/AdminSocket.cc
  acl/Acl.cc
  stat/Stat.cc
  iostat/Iostat.cc
  fsck/Fsck.cc
  fsck/FsckEntry.cc
  utils/AttrHelper.cc
  utils/FileSystemRegistry.cc                  utils/FileSystemRegistry.hh
  utils/FilesystemUuidMapper.cc                utils/FilesystemUuidMapper.hh
  utils/FileSystemStatusUtils.cc
  groupbalancer/GroupBalancer.cc
  groupdrainer/GroupDrainer.cc
  groupbalancer/BalancerEngine.cc
  groupbalancer/ConverterUtils.cc
  groupbalancer/FreeSpaceBalancerEngine.cc
  groupbalancer/MinMaxBalancerEngine.cc
  groupbalancer/StdDevBalancerEngine.cc
  groupbalancer/StdDrainerEngine.cc
  groupbalancer/GroupsInfoFetcher.cc
  groupdrainer/DrainProgressTracker.cc
  geobalancer/GeoBalancer.cc
  features/Features.cc
  zmq/ZMQ.cc
  FuseServer/Server.cc FuseServer/Server.hh
  FuseServer/Clients.cc FuseServer/Clients.hh
  FuseServer/Locks.cc FuseServer/Locks.hh
  FuseServer/Caps.cc FuseServer/Caps.hh
  FuseServer/Flush.cc FuseServer/Flush.hh
  fuse-locks/LockTracker.cc   fuse-locks/LockTracker.hh
  imaster/IMaster.cc                  imaster/IMaster.hh
  qdbmaster/QdbMaster.cc
  devices/Devices.cc
  recycle/RecyclePolicy.cc      recycle/RecyclePolicy.hh
  recycle/RecycleEntry.cc       recycle/RecycleEntry.hh
  recycle/Recycle.cc            recycle/Recycle.hh
  pathrouting/PathRouting.cc
  routeendpoint/RouteEndpoint.cc
  lru/LRU.cc
  wfe/WFE.cc
  EosCtaReporter.cc
  workflow/Workflow.cc
  inflighttracker/InFlightTracker.cc
  grpc/GrpcServer.cc   grpc/GrpcServer.hh
  grpc/GrpcNsInterface.cc   grpc/GrpcNsInterface.hh
  grpc/GrpcWncServer.cc      grpc/GrpcWncServer.hh
  grpc/GrpcWncInterface.cc      grpc/GrpcWncInterface.hh
  grpc/GrpcRestGwServer.cc grpc/GrpcRestGwServer.hh
  grpc/GrpcRestGwInterface.cc      grpc/GrpcRestGwInterface.hh
  http/HttpServer.cc
  http/HttpHandler.cc
  http/s3/S3Handler.cc
  http/s3/S3Store.cc
  http/webdav/WebDAVHandler.cc
  http/webdav/WebDAVResponse.cc
  http/webdav/PropFindResponse.cc
  http/webdav/PropPatchResponse.cc
  http/webdav/LockResponse.cc
  http/rest-api/manager/RestApiManager.cc
  http/rest-api/handler/RestHandler.cc
  http/rest-api/handler/wellknown/WellKnownHandler.cc
  http/rest-api/config/tape/TapeRestApiConfig.cc
  http/rest-api/handler/tape/TapeRestHandler.cc
  # factories removed in favor of in-place creators
  http/rest-api/action/tape/stage/CreateStageBulkRequest.cc
  http/rest-api/action/tape/stage/CancelStageBulkRequest.cc
  http/rest-api/action/tape/stage/GetStageBulkRequest.cc
  http/rest-api/action/tape/stage/DeleteStageBulkRequest.cc
  http/rest-api/action/tape/archiveinfo/GetArchiveInfo.cc
  http/rest-api/action/tape/release/CreateReleaseBulkRequest.cc
  # http/rest-api/action/wellknown/tape/GetTapeRestApiWellKnown.cc removed (inlined in WellKnownHandler)
  http/rest-api/business/tape/TapeRestApiBusiness.cc
  # Controller layer removed in favor of a lightweight router
  # http/rest-api/controllers/Controller.cc
  # http/rest-api/controllers/tape/factories/TapeControllerFactory.cc
  # http/rest-api/controllers/wellknown/factories/WellKnownControllerFactory.cc
  # http/rest-api/controllers/ControllerManager.cc
  # http/rest-api/controllers/ControllerActionDispatcher.cc
  # http/rest-api/controllers/tape/stage/StageController.cc
  # http/rest-api/controllers/tape/archiveinfo/ArchiveInfoController.cc
  # http/rest-api/controllers/tape/release/ReleaseController.cc
  # http/rest-api/controllers/wellknown/tape/TapeWellKnownController.cc
  # http/rest-api/exception/JsonValidationException.cc inlined
  # http/rest-api/model/tape/common/FilesContainer.cc header-only
  http/rest-api/model/tape/common/ErrorModel.cc
  # http/rest-api/model/tape/archiveinfo/GetArchiveInfoResponseModel.cc header-only
  # http/rest-api/model/tape/stage/PathsModel.cc header-only
  # http/rest-api/model/tape/stage/CreateStageBulkRequestModel.cc header-only
  # http/rest-api/model/tape/stage/CreatedStageBulkRequestResponseModel.cc header-only
  # http/rest-api/model/tape/stage/GetStageBulkRequestResponseModel.cc header-only
  # http/rest-api/model/wellknown/tape/GetTapeWellKnownModel.cc header-only
  # grouped under TapeModelBuilders.hh / TapeJsonifiers.hh
  http/rest-api/response/RestResponseFactory.cc
  http/rest-api/response/ErrorHandling.hh
  http/rest-api/router/Router.hh
  http/rest-api/utils/URLParser.cc
  http/rest-api/utils/URLBuilder.cc
  http/rest-api/wellknown/tape/TapeWellKnownInfos.cc
  http/rest-api/wellknown/tape/TapeRestApiEndpoint.cc
  geotree/SchedulingTreeTest.cc
  geotree/SchedulingSlowTree.cc
  geotree/SchedulingTreeCommon.cc
  tracker/ReplicationTracker.cc
  inspector/FileInspector.cc
  inspector/FileInspectorStats.cc
  # bulk-request: File.* and FileCollection.* are now header-only
  bulk-request/prepare/manager/PrepareManager.cc
  bulk-request/prepare/manager/BulkRequestPrepareManager.cc
  bulk-request/BulkRequest.cc
  # bulk-request prepare classes are now header-only
  bulk-request/business/BulkRequestBusiness.cc
  bulk-request/BulkRequestFactory.cc
  bulk-request/dao/factories/ProcDirectoryDAOFactory.cc
  bulk-request/dao/proc/ProcDirectoryBulkRequestDAO.cc
  bulk-request/dao/proc/ProcDirectoryBulkRequestLocations.cc
  bulk-request/dao/proc/ProcDirBulkRequestFile.cc
  # bulk-request exceptions are now header-only
  bulk-request/interface/RealMgmFileSystemInterface.cc
  bulk-request/prepare/PrepareUtils.cc
  bulk-request/prepare/query-prepare/QueryPrepareResult.cc
  bulk-request/dao/proc/cleaner/BulkRequestProcCleaner.cc
  bulk-request/dao/proc/cleaner/BulkRequestProcCleanerConfig.cc
  placement/ClusterMap.cc
  placement/FsScheduler.cc
  placement/FlatScheduler.cc
  placement/RoundRobinPlacementStrategy.cc
  placement/WeightedRoundRobinStrategy.cc
  placement/WeightedRandomStrategy.cc
  placement/ThreadLocalRRSeed.cc
  shaping/IoShaping.cc
  ${FUSEX_SRCS})

target_compile_options(XrdEosMgm-Objects PRIVATE -Wno-sign-compare)

target_link_libraries(XrdEosMgm-Objects PUBLIC
  EosCliProto-Objects
  EosAuthProto-Objects
  XrdSsiPbEosCta-Objects
  EosGrpcProto-Objects
  EosCliGrpc-Objects
  EosWncGrpcProto-Objects
  RestGrpc-Objects
  RestAnnot-Objects
  EosCommonServer
  EosGrpcGateway::EosGrpcGateway
  GOOGLE::SPARSEHASH
  JSONCPP::JSONCPP
  XROOTD::PRIVATE
  XROOTD::POSIX
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
  EosCliProto-Objects
  EosCliGrpc-Objects
  EosConsoleHelpers-Objects
  EosConsoleCommands-Objects
  EosWncGrpcProto-Objects
  RestGrpc-Objects
  RestAnnot-Objects
  EosGrpcGateway::EosGrpcGateway
  ZLIB::ZLIB
  XROOTD::CL
  XROOTD::SSI
  XROOTD::PRIVATE
  XROOTD::POSIX
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
    EosCliProto-Objects
    EosCliGrpc-Objects
    RestGrpc-Objects
    RestAnnot-Objects
    EosWncGrpcProto-Objects
    ZLIB::ZLIB
    XROOTD::SSI
    XROOTD::PRIVATE
    XROOTD::POSIX
    EosGrpcGateway::EosGrpcGateway
    EosNsCommon-Static
    EosCommon-Static)

  install(TARGETS eos-config-inspect
    LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
endif()
