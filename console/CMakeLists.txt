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
  CommandFramework.cc CommandFramework.hh
  ConsolePipe.cc  ConsolePipe.hh
  ConsoleCompletion.cc ConsoleCompletion.hh
  ConsoleArgParser.cc ConsoleArgParser.hh
  RegexUtil.cc RegexUtil.hh
  commands/native/CoreNativeCommands.cc
  commands/native/pwd-native.cc
  commands/native/cd-native.cc
  commands/native/ls-native.cc
  commands/native/cp-native.cc
  commands/native/version-native.cc
  commands/native/status-native.cc
  # split per-command modules
  commands/native/mkdir-native.cc
  commands/native/rm-native.cc
  commands/native/info-native.cc
  commands/native/stat-native.cc
  # BasicWrappersNativeCommands removed; using RemainingLegacyNativeCommands instead
  commands/native/mv-native.cc
  commands/native/ln-native.cc
  # RemainingLegacyNativeCommands removed
  commands/native/rmdir-native.cc
  commands/native/touch-native.cc
  commands/native/who-native.cc
  commands/native/whoami-native.cc
  commands/native/archive-native.cc
  commands/native/attr-native.cc
  commands/native/chmod-native.cc
  commands/native/chown-native.cc
  # split proto commands
  commands/native/access-proto-native.cc
  commands/native/acl-proto-native.cc
  commands/native/config-proto-native.cc
  commands/native/convert-proto-native.cc
  commands/native/devices-proto-native.cc
  commands/native/df-proto-native.cc
  commands/native/find-proto-native.cc
  commands/native/fs-proto-native.cc
  commands/native/fsck-proto-native.cc
  commands/native/group-proto-native.cc
  commands/native/io-proto-native.cc
  commands/native/node-proto-native.cc
  commands/native/ns-proto-native.cc
  commands/native/qos-proto-native.cc
  commands/native/quota-proto-native.cc
  commands/native/recycle-proto-native.cc
  commands/native/register-proto-native.cc
  commands/native/rm-proto-native.cc
  commands/native/route-proto-native.cc
  commands/native/token-proto-native.cc
  commands/native/space-proto-native.cc
  commands/native/sched-proto-native.cc
  commands/native/file-native.cc
  commands/native/fuse-native.cc
  commands/native/fusex-native.cc
  commands/native/backup-native.cc
  commands/native/clear-native.cc
  commands/native/console-native.cc
  commands/native/debug-native.cc
  commands/native/du-native.cc
  commands/native/evict-native.cc
  commands/native/motd-native.cc
  commands/native/oldfind-native.cc
  commands/native/rclone-native.cc
  commands/native/squash-native.cc
  commands/native/test-native.cc
  commands/native/daemon-native.cc
  commands/native/geosched-native.cc
  commands/native/inspector-native.cc
  commands/native/license-native.cc
  commands/native/map-native.cc
  commands/native/member-native.cc
  commands/native/accounting-native.cc
  commands/native/health-native.cc
  commands/native/reconnect-native.cc
  commands/native/report-native.cc
  commands/native/rtlog-native.cc
  commands/native/role-native.cc
  commands/native/scitoken-native.cc
  commands/native/tracker-native.cc
  commands/native/vid-native.cc
  commands/native/LegacySymbols.cc
  commands/helpers/NewfindHelper.cc commands/helpers/NewfindHelper.hh
  commands/helpers/AclHelper.cc     commands/helpers/AclHelper.hh
  commands/HealthCommand.cc         commands/HealthCommand.hh
 
  )

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

