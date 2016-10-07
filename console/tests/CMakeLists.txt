# ----------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Stefan Isidorovic <stefan.isidorovic@comtrade.com>
# ----------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2016 CERN/Switzerland                                  *
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
  ${OPENSSL_INCLUDE_DIRS}
  ${CPPUNIT_INCLUDE_DIRS})

add_definitions(-DBUILD_TESTS)

#-------------------------------------------------------------------------------
# EosConsoleTests library
#-------------------------------------------------------------------------------
add_library(
  EosConsoleTests SHARED
  AclCommandTest.hh         AclCommandTest.cc
  ${CMAKE_SOURCE_DIR}/fst/FmdClient.cc
  ${CMAKE_SOURCE_DIR}/fst/Fmd.cc
  ${CMAKE_SOURCE_DIR}/console/commands/AclCommand.hh
  ${CMAKE_SOURCE_DIR}/console/commands/AclCommand.cc
  ${CMAKE_SOURCE_DIR}/console/ConsoleMain.cc
  ${CMAKE_SOURCE_DIR}/console/ConsoleMain.hh
  ${CMAKE_SOURCE_DIR}/console/ConsolePipe.cc
  ${CMAKE_SOURCE_DIR}/console/ConsolePipe.hh
  ${CMAKE_SOURCE_DIR}/console/commands/AclCommand.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_access.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_acl.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_archive.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_attr.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_backup.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_cd.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_chmod.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_chown.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_clear.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_config.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_console.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_cp.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_debug.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_dropbox.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_file.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_find.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_fs.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_fsck.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_fuse.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_geosched.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_group.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_info.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_io.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_kinetic.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_json.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_license.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_ln.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_ls.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_map.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_member.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_mkdir.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_motd.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_mv.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_node.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_ns.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_print.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_pwd.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_quit.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_quota.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_reconnect.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_recycle.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_rm.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_rmdir.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_role.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_rtlog.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_silent.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_space.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_stat.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_test.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_timing.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_touch.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_transfer.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_version.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_vid.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_vst.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_whoami.cc
  ${CMAKE_SOURCE_DIR}/console/commands/com_who.cc)

target_link_libraries(
  EosConsoleTests
  EosFstIo-Static
  ${CMAKE_THREAD_LIBS_INIT}
  ${CPPUNIT_LIBRARIES}
  ${NCURSES_LIBRARIES}
  ${OPENSSL_CRYPTO_LIBRARY}
  ${READLINE_LIBRARIES}
  ${PROTOBUF_LIBRARY}
  ${UUID_LIBRARIES}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_POSIX_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${Z_LIBRARIES})

install(
  TARGETS EosConsoleTests
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
