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
# CLI11 parser submodule (console/parser)
#-------------------------------------------------------------------------------
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/parser/CMakeLists.txt")
  set(CLI11_BUILD_TESTS OFF CACHE BOOL "Disable CLI11 tests" FORCE)
  set(CLI11_BUILD_EXAMPLES OFF CACHE BOOL "Disable CLI11 examples" FORCE)
  set(CLI11_BUILD_DOCS OFF CACHE BOOL "Disable CLI11 docs" FORCE)
  set(CLI11_INSTALL ON CACHE BOOL "Install CLI11 headers" FORCE)
  add_subdirectory(parser)
endif()

#-------------------------------------------------------------------------------
# eos executable
#-------------------------------------------------------------------------------
add_library(EosConsoleHelpers-Objects OBJECT
  commands/helpers/ICmdHelper.cc    commands/helpers/ICmdHelper.hh
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
  # Core console framework
  ConsoleMain.cc  ConsoleMain.hh
  CommandFramework.cc CommandFramework.hh
  ConsoleCompletion.cc ConsoleCompletion.hh
  RegexUtil.cc RegexUtil.hh

  # Core/native base
  commands/native/CoreNativeCommands.cc
  commands/native/pwd-native.cc
  commands/native/status-native.cc

  # Native aliases
  commands/native/info-alias.cc
  commands/native/fileinfo-alias.cc
  commands/native/mv-alias.cc
  commands/native/ls-compat.cc

  # Native com wrappers
  commands/native/cat-com-native.cc
  commands/native/cp-com-native.cc
  commands/native/oldfind-cmd-native.cc
  commands/native/rclone-com-native.cc
  commands/native/squash-com-native.cc

  # Native cmd implementations (mgm.cmd=...)
  commands/native/accounting-cmd-native.cc
  commands/native/archive-cmd-native.cc
  commands/native/attr-cmd-native.cc
  commands/native/backup-cmd-native.cc
  commands/native/cd-cmd-native.cc
  commands/native/chmod-cmd-native.cc
  commands/native/chown-cmd-native.cc
  commands/native/clear-cmd-native.cc
  commands/native/debug-cmd-native.cc
  commands/native/evict-cmd-native.cc
  commands/native/file-cmd-native.cc
  commands/native/geosched-cmd-native.cc
  commands/native/ln-cmd-native.cc
  commands/native/ls-cmd-native.cc
  commands/native/map-cmd-native.cc
  commands/native/member-cmd-native.cc
  commands/native/mkdir-cmd-native.cc
  commands/native/motd-cmd-native.cc
  commands/native/rmdir-cmd-native.cc
  commands/native/rtlog-cmd-native.cc
  commands/native/touch-cmd-native.cc
  commands/native/version-cmd-native.cc
  commands/native/vid-cmd-native.cc
  commands/native/who-cmd-native.cc
  commands/native/whoami-cmd-native.cc

  # Native proto implementations
  commands/native/access-proto-native.cc
  commands/native/acl-proto-native.cc
  commands/native/config-proto-native.cc
  commands/native/convert-proto-native.cc
  commands/native/devices-proto-native.cc
  commands/native/df-proto-native.cc
  commands/native/du-proto-native.cc
  commands/native/find-proto-native.cc
  commands/native/fs-proto-native.cc
  commands/native/fsck-proto-native.cc
  commands/native/group-proto-native.cc
  commands/native/inspector-proto-native.cc
  commands/native/io-proto-native.cc
  commands/native/node-proto-native.cc
  commands/native/ns-proto-native.cc
  commands/native/quota-proto-native.cc
  commands/native/recycle-proto-native.cc
  commands/native/register-proto-native.cc
  commands/native/rm-proto-native.cc
  commands/native/route-proto-native.cc
  commands/native/sched-proto-native.cc
  commands/native/space-proto-native.cc
  commands/native/token-proto-native.cc
  commands/native/tracker-proto-native.cc

  # Other native commands
  commands/native/daemon-native.cc
  commands/native/fuse-native.cc
  commands/native/fusex-cmd-native.cc
  commands/native/health-native.cc
  commands/native/license-native.cc
  commands/native/reconnect-native.cc
  commands/native/report-native.cc
  commands/native/role-native.cc
  commands/native/scitoken-native.cc
  commands/native/stat-native.cc
  commands/native/test-native.cc

  # Legacy symbol aggregation
  commands/native/LegacySymbols.cc

  # Shared helpers used by native/proto commands
  commands/helpers/NewfindHelper.cc commands/helpers/NewfindHelper.hh
  commands/helpers/AclHelper.cc     commands/helpers/AclHelper.hh
  commands/HealthCommand.cc         commands/HealthCommand.hh
)

target_link_libraries(EosConsoleCommands-Objects PUBLIC
  EosConsoleHelpers-Objects
  READLINE::READLINE
  JSONCPP::JSONCPP
  SCITOKENS::SCITOKENS
  XROOTD::UTILS
  CLI11::CLI11)

target_include_directories(EosConsoleCommands-Objects PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/parser/include)

set_target_properties(EosConsoleCommands-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

add_executable(eos ConsoleMainExecutable.cc)

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

