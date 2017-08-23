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
  ${CMAKE_BINARY_DIR}
  ${PROTOBUF_INCLUDE_DIRS}
  ${XROOTD_INCLUDE_DIRS}
  ${KINETICIO_INCLUDE_DIRS}
  ${OPENSSL_INCLUDE_DIRS})

if (BUILD_TESTS)
  add_subdirectory(tests)
endif()

#-------------------------------------------------------------------------------
# eos executable
#-------------------------------------------------------------------------------
add_library(
  EosConsoleCommands-Objects OBJECT
  ConsoleMain.cc  ConsoleMain.hh
  ConsolePipe.cc  ConsolePipe.hh
  RegexUtil.cc RegexUtil.hh
  MgmExecute.cc MgmExecute.hh
  commands/HealthCommand.cc commands/HealthCommand.hh
  commands/com_access.cc
  commands/com_accounting.cc
  commands/com_acl.cc
  commands/com_archive.cc
  commands/com_attr.cc
  commands/com_backup.cc
  commands/com_cd.cc
  commands/com_chmod.cc
  commands/com_chown.cc
  commands/com_clear.cc
  commands/com_config.cc
  commands/com_console.cc
  commands/com_cp.cc
  commands/com_debug.cc
  commands/com_dropbox.cc
  commands/com_file.cc
  commands/com_find.cc
  commands/com_fs.cc
  commands/com_fsck.cc
  commands/com_fuse.cc
  commands/com_fusex.cc
  commands/com_geosched.cc
  commands/com_group.cc
  commands/com_health.cc
  commands/com_info.cc
  commands/com_io.cc
  commands/com_kinetic.cc
  commands/com_json.cc
  commands/com_license.cc
  commands/com_ls.cc
  commands/com_ln.cc
  commands/com_map.cc
  commands/com_member.cc
  commands/com_mkdir.cc
  commands/com_motd.cc
  commands/com_mv.cc
  commands/com_node.cc
  commands/com_ns.cc
  commands/com_print.cc
  commands/com_pwd.cc
  commands/com_quit.cc
  commands/com_quota.cc
  commands/com_reconnect.cc
  commands/com_recycle.cc
  commands/com_rm.cc
  commands/com_rmdir.cc
  commands/com_role.cc
  commands/com_rtlog.cc
  commands/com_silent.cc
  commands/com_space.cc
  commands/com_stat.cc
  commands/com_test.cc
  commands/com_timing.cc
  commands/com_touch.cc
  commands/com_transfer.cc
  commands/com_version.cc
  commands/com_vid.cc
  commands/com_vst.cc
  commands/com_whoami.cc
  commands/com_who.cc)

target_compile_definitions(
  EosConsoleCommands-Objects PUBLIC
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID} -DHAVE_ATOMICS=1)

set_target_properties(
  EosConsoleCommands-Objects
  PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

add_executable(
  eos
  ${CMAKE_SOURCE_DIR}/fst/FmdClient.cc
  ${CMAKE_SOURCE_DIR}/fst/Fmd.cc
  # @TODO (esindril) Add the table formatter in common
  ${CMAKE_SOURCE_DIR}/mgm/TableFormatter/TableFormatterBase.cc
  $<TARGET_OBJECTS:CommonCli-Objects>
  $<TARGET_OBJECTS:EosConsoleCommands-Objects>)

#-------------------------------------------------------------------------------
# Add dependency which guarantees that the protocol buffer files are generated
# when we build the "eos" executable.
#-------------------------------------------------------------------------------
target_link_libraries(
  eos PUBLIC
  EosFstIo-Static
  ${Z_LIBRARY}
  ${RT_LIBRARIES}
  ${UUID_LIBRARIES}
  ${NCURSES_LIBRARY}
  ${READLINE_LIBRARY}
  ${PROTOBUF_LIBRARY}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_POSIX_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT})

set(EOS_RPATH "/usr/lib64/;/lib64/")

set_target_properties(
  eos PROPERTIES
  INSTALL_RPATH  "${EOS_RPATH}"
  SKIP_RPATH FALSE
  SKIP_BUILD_RPATH  FALSE
  BUILD_WITH_INSTALL_RPATH TRUE
  INSTALL_RPATH_USE_LINK_PATH TRUE)

add_executable(eosdropboxd dropbox/eosdropboxd.cc)

if(Linux)
  target_link_libraries(
    eosdropboxd PRIVATE
    eosCommon-Static
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${XROOTD_POSIX_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT})
else()
  target_link_libraries(
    eosdropboxd PRIVATE
    eosCommon
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${XROOTD_POSIX_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT})
endif()

install(
  PROGRAMS eosadmin
  DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})

install(
  TARGETS eos eosdropboxd
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
