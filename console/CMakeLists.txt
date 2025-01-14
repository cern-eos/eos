# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Andreas-Joachim Peters - CERN
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2023 CERN/Switzerland                                  *
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

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/commands/helpers/)

#-------------------------------------------------------------------------------
# eos executable
#-------------------------------------------------------------------------------
add_library(EosConsoleHelpers-Objects OBJECT
  commands/ICmdHelper.cc            commands/ICmdHelper.hh
  commands/helpers/FsHelper.cc      commands/helpers/FsHelper.hh
  commands/helpers/RecycleHelper.cc commands/helpers/RecycleHelper.hh
  commands/helpers/FsckHelper.cc    commands/helpers/FsckHelper.hh
  commands/helpers/NodeHelper.cc    commands/helpers/NodeHelper.hh
  commands/helpers/TokenHelper.cc   commands/helpers/TokenHelper.hh)

target_link_libraries(EosConsoleHelpers-Objects PUBLIC
  EosFstProto-Objects
  EosCliProto-Objects
  ZMQ::ZMQ
  OpenSSL::SSL
  GOOGLE::SPARSEHASH
  XROOTD::UTILS
  XROOTD::POSIX
  XROOTD::PRIVATE
  SCITOKENS::SCITOKENS
  stdc++fs 
  )

set_target_properties(EosConsoleHelpers-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

add_library(EosConsoleCommands-Objects OBJECT
  ConsoleMain.cc  ConsoleMain.hh
  ConsolePipe.cc  ConsolePipe.hh
  ConsoleCompletion.cc ConsoleCompletion.hh
  RegexUtil.cc RegexUtil.hh
  commands/helpers/NewfindHelper.cc commands/helpers/NewfindHelper.hh
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
  commands/com_daemon.cc
  commands/com_file.cc
  commands/com_old_find.cc
  commands/com_proto_find.cc
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
  commands/com_rclone.cc
  commands/com_report.cc
  commands/com_reconnect.cc
  commands/com_rmdir.cc
  commands/com_role.cc
  commands/com_rtlog.cc
  commands/com_scitoken.cc
  commands/com_silent.cc
  commands/com_stat.cc
  commands/com_status.cc
  commands/com_squash.cc
  commands/com_test.cc
  commands/com_timing.cc
  commands/com_touch.cc
  commands/com_proto_token.cc
  commands/com_tracker.cc
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
  commands/com_evict.cc
  commands/com_proto_ns.cc
  commands/com_proto_debug.cc
  commands/com_du.cc
  commands/com_proto_df.cc
  commands/com_proto_group.cc
  commands/com_proto_recycle.cc
  #@todo (esindril) drop com_config when move to 5.0.0
  commands/com_config.cc
  commands/com_proto_config.cc
  commands/com_proto_register.cc
  commands/com_proto_rm.cc
  #@todo (esindril) drop com_quota when move to 5.0.0
  commands/com_quota.cc
  commands/com_proto_quota.cc
  commands/com_proto_node.cc
  commands/com_proto_devices.cc
  commands/com_proto_space.cc
  commands/com_proto_io.cc
  #@todo (esindril) drop com_access when move to 5.0.0
  commands/com_access.cc
  commands/com_proto_access.cc
  commands/com_proto_fsck.cc
  commands/com_proto_sched.cc)

target_link_libraries(EosConsoleCommands-Objects PUBLIC
  EosConsoleHelpers-Objects
  READLINE::READLINE
  JSONCPP::JSONCPP
  SCITOKENS::SCITOKENS
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
  SCITOKENS::SCITOKENS
  EosFstIo-Static
  stdc++fs 
  )

install(PROGRAMS eosadmin eos-iam-mapfile
  DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})

install(TARGETS eos
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
