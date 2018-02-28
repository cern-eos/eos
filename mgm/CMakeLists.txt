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
  ${CMAKE_BINARY_DIR}
  ${CMAKE_BINARY_DIR}/auth_plugin/
  ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/qclient/include
  ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/qclient/src
  ${CMAKE_SOURCE_DIR}/mgm/cta_interface/include
  ${CMAKE_BINARY_DIR}/mgm/eos_cta_pb)

#-------------------------------------------------------------------------------
# CTA integration related operations
#-------------------------------------------------------------------------------
set(EOS_CTA_PB_DIR ${CMAKE_CURRENT_SOURCE_DIR}/cta_interface/eos_cta/protobuf/)
add_subdirectory(eos_cta_pb)

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

#-------------------------------------------------------------------------------
# XrdEosMgm-Objects library
#-------------------------------------------------------------------------------
add_library(
  XrdEosMgm-Objects OBJECT
  Access.cc
  IConfigEngine.cc
  FileConfigEngine.cc
  RedisConfigEngine.cc
  GeoTreeEngine.cc
  Messaging.cc
  VstMessaging.cc
  Policy.cc
  proc/IProcCommand.cc
  proc/ProcInterface.cc
  proc/ProcCommand.cc
  proc/proc_fs.cc
  proc/admin/Access.cc
  proc/admin/Backup.cc
  proc/admin/Config.cc
  proc/admin/Debug.cc
  proc/admin/DrainCmd.cc
  proc/admin/Fs.cc
  proc/admin/FsCmd.cc
  proc/admin/Fsck.cc
  proc/admin/Fusex.cc
  proc/admin/GeoSched.cc
  proc/admin/Group.cc
  proc/admin/Io.cc
  proc/admin/Node.cc
  proc/admin/NsCmd.cc
  proc/admin/Quota.cc
  proc/admin/Rtlog.cc
  proc/admin/Space.cc
  proc/admin/Transfer.cc
  proc/admin/Vid.cc
  proc/admin/Vst.cc
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
  proc/user/Quota.cc
  proc/user/Recycle.cc
  proc/user/Rm.cc
  proc/user/Rmdir.cc
  proc/user/Version.cc
  proc/user/Who.cc
  proc/user/Whoami.cc
  Quota.cc
  Scheduler.cc
  Vid.cc
  FsView.cc
  VstView.cc
  XrdMgmOfsConfigure.cc
  XrdMgmOfsFile.cc
  XrdMgmOfsDirectory.cc
  XrdMgmOfs.cc
  DrainJob.cc
  Balancer.cc
  FileSystem.cc
  drain/DrainFS.cc
  drain/DrainTransferJob.cc
  drain/Drainer.cc
  Egroup.cc
  Acl.cc
  Stat.cc
  Iostat.cc
  Fsck.cc
  txengine/TransferEngine.cc
  txengine/TransferFsDB.cc
  Converter.cc
  GroupBalancer.cc
  GeoBalancer.cc
  Features.cc
  ZMQ.cc
  FuseServer.cc FuseServer.hh
  fuse-locks/LockTracker.cc   fuse-locks/LockTracker.hh
  Master.cc
  Recycle.cc
  LRU.cc
  WFE.cc
  Workflow.cc
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
  TableFormatter/TableFormatterBase.cc
  TableFormatter/TableCell.cc
  ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/BackendClient.cc
  ${FUSEX_SRCS})

#-------------------------------------------------------------------------------
# Add dependecy to EosAuthProto-Objects so we guarantee that the protocol
# buffer files are generated when we try to build XrdEosMgm
#-------------------------------------------------------------------------------
add_dependencies(XrdEosMgm-Objects EosProtobuf)

target_compile_definitions(XrdEosMgm-Objects PUBLIC
  -DDAEMONUID=${DAEMONUID} -DBACKWARD_HAS_DW=1 -DDAEMONGID=${DAEMONGID})

set_target_properties(
  XrdEosMgm-Objects
  PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

add_library(XrdEosMgm MODULE $<TARGET_OBJECTS:XrdEosMgm-Objects>)

target_compile_definitions(
  XrdEosMgm PUBLIC
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

if (MICROHTTPD_FOUND)
  target_compile_definitions(
    XrdEosMgm PUBLIC -DEOS_MICRO_HTTPD=1)
endif()

target_link_libraries(
  XrdEosMgm PUBLIC
  EosProtobuf
  qclient
  eosCommon
  eosCommonServer
  EosNsCommon
  eosCapability-Static
  XrdSsiPbEosCta
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

install(
  TARGETS XrdEosMgm
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
  eosCommon
  ${XROOTD_UTILS_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT})

#-------------------------------------------------------------------------------
# Create executables for testing the MGM configuration
#-------------------------------------------------------------------------------
if(Linux)
  add_library(XrdEosMgm-Shared SHARED
    $<TARGET_OBJECTS:XrdEosMgm-Objects>)

  target_compile_definitions(
    XrdEosMgm-Shared PUBLIC
    -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

  target_link_libraries(
    XrdEosMgm-Shared PUBLIC
    EosNsCommon-Static
    EosProtobuf
    eosCommon
    eosCommonServer
    eosCapability-Static
    qclient
    XrdSsiPbEosCta
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
endif()
