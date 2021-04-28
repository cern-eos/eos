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

add_subdirectory(tests)

#-------------------------------------------------------------------------------
# eos executable
#-------------------------------------------------------------------------------
add_library(EosConsoleHelpers-Objects OBJECT
  commands/ICmdHelper.cc            commands/ICmdHelper.hh
  commands/helpers/FsHelper.cc      commands/helpers/FsHelper.hh
  commands/helpers/RecycleHelper.cc commands/helpers/RecycleHelper.hh
  commands/helpers/FsckHelper.cc    commands/helpers/FsckHelper.hh
  commands/helpers/NodeHelper.cc    commands/helpers/NodeHelper.hh
  commands/helpers/TokenHelper.cc   commands/helpers/TokenHelper.hh
  commands/helpers/ShareHelper.cc   commands/helpers/ShareHelper.hh
)

target_link_libraries(EosConsoleHelpers-Objects PUBLIC
  EosFstProto-Objects
  EosCliProto-Objects
  ZMQ::ZMQ
  OPENSSL::OPENSSL
  GOOGLE::SPARSEHASH
  XROOTD::UTILS)

set_target_properties(EosConsoleHelpers-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

add_library(EosConsoleCommands-Objects OBJECT
  ConsoleMain.cc  ConsoleMain.hh
  ConsolePipe.cc  ConsolePipe.hh
  ConsoleCompletion.cc ConsoleCompletion.hh
  RegexUtil.cc RegexUtil.hh
  commands/helpers/AclHelper.cc     commands/helpers/AclHelper.hh
  commands/HealthCommand.cc         commands/HealthCommand.hh
  commands/com_accounting.cc
  commands/com_archive.cc
  commands/com_attr.cc
  commands/com_backup.cc
  commands/com_cd.cc
  commands/com_chmod.cc
  commands/com_chown.cc
  commands/com_clear.cc
  commands/com_console.cc
  commands/com_cp.cc
  commands/com_file.cc
  commands/com_find.cc
  commands/com_proto_newfind.cc
  commands/com_fs.cc
  commands/com_fuse.cc
  commands/com_fusex.cc
  commands/com_geosched.cc
  commands/com_health.cc
  commands/com_info.cc
  commands/com_inspector.cc
  commands/com_json.cc
  commands/com_license.cc
  commands/com_ls.cc
  commands/com_ln.cc
  commands/com_map.cc
  commands/com_member.cc
  commands/com_mkdir.cc
  commands/com_motd.cc
  commands/com_mv.cc
  commands/com_print.cc
  commands/com_pwd.cc
  commands/com_quit.cc
  commands/com_reconnect.cc
  commands/com_rmdir.cc
  commands/com_role.cc
  commands/com_rtlog.cc
  commands/com_silent.cc
  commands/com_stat.cc
  commands/com_status.cc
  commands/com_squash.cc
  commands/com_test.cc
  commands/com_timing.cc
  commands/com_touch.cc
  commands/com_proto_token.cc
  commands/com_proto_share.cc
  commands/com_tracker.cc
  commands/com_transfer.cc
  commands/com_version.cc
  commands/com_vid.cc
  commands/com_whoami.cc
  commands/com_who.cc
  commands/com_proto_acl.cc
  commands/com_proto_convert.cc
  commands/com_proto_fs.cc
  commands/com_proto_qos.cc
  commands/com_proto_route.cc
  commands/com_stagerrm.cc
  commands/com_proto_ns.cc
  commands/com_proto_debug.cc
  commands/com_proto_group.cc
  commands/com_proto_recycle.cc
  #@todo (esindril) drop com_config when move to 5.0.0
  commands/com_config.cc
  commands/com_proto_config.cc
  commands/com_proto_rm.cc
  #@todo (esindril) drop com_quota when move to 5.0.0
  commands/com_quota.cc
  commands/com_proto_quota.cc
  #@todo (esindril) drop com_node when move to 5.0.0
  commands/com_node.cc
  commands/com_proto_node.cc
  #@todo (esindril) drop com_space when move to 5.0.0
  commands/com_space.cc
  commands/com_proto_space.cc
  #@todo (esindril) drop com_io when move to 5.0.0
  commands/com_io.cc
  commands/com_proto_io.cc
  #@todo (esindril) drop com_access when move to 5.0.0
  commands/com_access.cc
  commands/com_proto_access.cc
  commands/com_proto_fsck.cc)

target_link_libraries(EosConsoleCommands-Objects PUBLIC
  EosConsoleHelpers-Objects
  READLINE::READLINE
  XROOTD::UTILS)

set_target_properties(EosConsoleCommands-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

add_executable(eos ConsoleMainExecutable.cc
  ${CMAKE_SOURCE_DIR}/mq/XrdMqClient.cc
  ${CMAKE_SOURCE_DIR}/mq/XrdMqMessage.cc
  ${CMAKE_SOURCE_DIR}/mq/ErrorReportListener.cc)

#-------------------------------------------------------------------------------
# Add dependency which guarantees that the protocol buffer files are generated
# when we build the "eos" executable.
#-------------------------------------------------------------------------------
target_link_libraries(
  eos PUBLIC
  EosConsoleCommands-Objects
  EosConsoleHelpers-Objects
  ZMQ::ZMQ
  XROOTD::POSIX
  EosFstIo-Static)

install(PROGRAMS eosadmin
  DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})

install(TARGETS eos
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
