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
  ${CMAKE_CURRENT_BINARY_DIR}
  ${Z_INCLUDE_DIRS}
  ${PROTOBUF_INCLUDE_DIRS}
  ${XROOTD_INCLUDE_DIRS}
  ${NCURSES_INCLUDE_DIRS}
  ${ZMQ_INCLUDE_DIRS}
  ${SPARSEHASH_INCLUDE_DIRS}
  ${OPENSSL_INCLUDE_DIRS})

#-------------------------------------------------------------------------------
# Generate protobol buffer object for the CLI
#-------------------------------------------------------------------------------
PROTOBUF_GENERATE_CPP(ACL_SRCS ACL_HDRS cli_proto/Acl.proto)
PROTOBUF_GENERATE_CPP(REQ_SRCS REQ_HDRS cli_proto/ConsoleRequest.proto)
PROTOBUF_GENERATE_CPP(REP_SRCS REP_HDRS cli_proto/ConsoleReply.proto)

set(CLI_PROTO_SRCS
  ${ACL_SRCS} ${REQ_SRCS} ${REP_SRCS})

set(CLI_PROTO_HDRS
  ${ACL_HDRS} ${REQ_HDRS} ${REP_HDRS})

set_source_files_properties(
  ${CLI_PROTO_SRCS} ${CLI_PROTO_HDRS}
  PROPERTIES GENERATED 1)

add_library(CommonCli-Objects OBJECT
  ${CLI_PROTO_SRCS} ${CLI_PROTO_HDRS})

set_target_properties(CommonCli-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# Generate protocol buffer files
#-------------------------------------------------------------------------------
PROTOBUF_GENERATE_CPP(DBMAPTEST_SRCS DBMAPTEST_HDRS dbmaptest/test.proto)
set_source_files_properties(
  ${DBMAPTEST_SRCS}
  ${DBMAPTEST_HDRS}
  PROPERTIES GENERATED 1)

#-------------------------------------------------------------------------------
# EosCrc32c-Static library
#-------------------------------------------------------------------------------
add_library(EosCrc32c-Static STATIC
  crc32c/crc32c.cc
  crc32c/crc32ctables.cc)

set_target_properties(EosCrc32c-Static PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# eosCommon library
#-------------------------------------------------------------------------------
set(EOSCOMMON_SRCS
  Fmd.cc
  Mapping.cc
  Statfs.cc
  SymKeys.cc
  GlobalConfig.cc
  Report.cc
  StringTokenizer.cc
  StringConversion.cc
  CommentLog.cc
  RWMutex.cc
  XrdErrorMap.cc
  JeMallocHandler.cc
  plugin_manager/Plugin.hh
  plugin_manager/PluginManager.cc
  plugin_manager/DynamicLibrary.cc)

add_library(eosCommon SHARED ${EOSCOMMON_SRCS})

target_link_libraries(eosCommon PUBLIC
  XrdMqClient
  fmt
  ${Z_LIBRARY}
  ${UUID_LIBRARIES}
  ${ATTR_LIBRARIES}
  ${NCURSES_LIBRARY}
  ${CURL_LIBRARIES}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY})

# !!!!!
# TODO: the HAVE_ATOMICS flags should be removed and the code in RWMutex should
# use std::atomic and the <atomic> header.
# !!!!!
target_compile_definitions(eosCommon PUBLIC
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID} -DHAVE_ATOMICS=1)

target_include_directories(eosCommon PUBLIC ${CURL_INCLUDE_DIRS})

set_target_properties(eosCommon PROPERTIES
  VERSION ${VERSION}
  SOVERSION ${VERSION_MAJOR}
  MACOSX_RPATH TRUE)

if(Linux)
  target_link_libraries(eosCommon PUBLIC ${GLIBC_RT_LIBRARY})
endif()

install(TARGETS eosCommon
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

add_library(eosCommon-Static STATIC ${EOSCOMMON_SRCS})

target_include_directories(eosCommon-Static PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR};${CURL_INCLUDE_DIRS}>
  $<INSTALL_INTERFACE:include>)

# !!!!!
# TODO: the HAVE_ATOMICS flags should be removed and the code in RWMutex should
# use std::atomic and the <atomic> header.
# !!!!!
target_compile_definitions(eosCommon-Static
  PUBLIC -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID} -DHAVE_ATOMICS=1)

set_target_properties(eosCommon-Static PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

target_link_libraries(eosCommon-Static PRIVATE
  XrdMqClient-Static
  fmt
  ${Z_LIBRARY}
  ${GLIBC_RT_LIBRARY}
  ${UUID_LIBRARIES}
  ${ATTR_LIBRARIES}
  ${NCURSES_LIBRARY}
  ${CURL_LIBRARIES}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT})

#-------------------------------------------------------------------------------
# eosCommonServer and eosCommon-Static libraries
#-------------------------------------------------------------------------------
if (Linux)
  add_library(eosCommonServer SHARED
    ShellExecutor.cc
    ShellCmd.cc
    FileSystem.cc
    TransferQueue.cc
    TransferJob.cc
#    ZMQ.cc
    sqlite/sqlite3.c
    DbMap.cc
    DbMapLevelDb.cc
    DbMapCommon.cc
    http/HttpServer.cc
    http/HttpRequest.cc
    http/HttpResponse.cc
    http/s3/S3Handler.cc
    stringencoders/modp_numtoa.c)

  target_include_directories(
   eosCommonServer PUBLIC ${LEVELDB_INCLUDE_DIRS})

  target_link_libraries(eosCommonServer PUBLIC
    XrdMqClient
    ${Z_LIBRARY}
    ${ZMQ_LIBRARIES}
    ${UUID_LIBRARIES}
    ${ATTR_LIBRARIES}
    ${NCURSES_LIBRARY}
    ${LEVELDB_LIBRARIES}
    ${CURL_LIBRARIES}
    ${GLIBC_RT_LIBRARY}
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${OPENSSL_CRYPTO_LIBRARY})

  if(MICROHTTPD_FOUND)
    target_link_libraries(eosCommonServer PUBLIC ${MICROHTTPD_LIBRARIES})
  endif()

  set_target_properties(eosCommonServer PROPERTIES
    VERSION ${VERSION}
    SOVERSION ${VERSION_MAJOR}
    MACOSX_RPATH TRUE)

  target_compile_definitions(eosCommonServer PUBLIC -DSQLITE_NO_SYNC=1)

  install(TARGETS eosCommonServer
    LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
endif()

#-------------------------------------------------------------------------------
# Create helper executables
#-------------------------------------------------------------------------------i
if(NOT CLIENT AND Linux)
  add_executable(dbmaptestburn dbmaptest/DbMapTestBurn.cc)
  add_executable(mutextest mutextest/RWMutexTest.cc)
  add_executable(dbmaptestfunc
    dbmaptest/DbMapTestFunc.cc
    ${DBMAPTEST_SRCS}
    ${DBMAPTEST_HDRS})

  target_link_libraries(dbmaptestburn PRIVATE
    eosCommon
    eosCommonServer
    ${CMAKE_THREAD_LIBS_INIT})

  target_link_libraries(mutextest PRIVATE
    eosCommon
    ${CMAKE_THREAD_LIBS_INIT})

  target_link_libraries(dbmaptestfunc PRIVATE
    eosCommonServer
    eosCommon
    ${PROTOBUF_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT})
endif()
