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
  ${XROOTD_INCLUDE_DIRS}
  ${NCURSES_INCLUDE_DIRS}
  ${ZMQ_INCLUDE_DIRS}
  ${SPARSEHASH_INCLUDE_DIRS}
  ${OPENSSL_INCLUDE_DIRS})

#-------------------------------------------------------------------------------
# Generate protocol buffer files
#-------------------------------------------------------------------------------
PROTOBUF_GENERATE_CPP(DBMAPTEST_SRCS DBMAPTEST_HDRS PROTOFILES dbmaptest/test.proto)
set_source_files_properties(
  ${DBMAPTEST_SRCS}
  ${DBMAPTEST_HDRS}
  PROPERTIES GENERATED 1)

#-------------------------------------------------------------------------------
# eosCommon library
#-------------------------------------------------------------------------------
set(
  EOSCOMMON_SRCS
  Fmd.cc
  Logging.cc
  Mapping.cc
  Statfs.cc
  SymKeys.cc
  GlobalConfig.cc
  Attr.cc
  Report.cc
  StringTokenizer.cc
  StringConversion.cc
  CommentLog.cc
  RWMutex.cc)

add_library(eosCommon SHARED ${EOSCOMMON_SRCS})

# !!!!!
# TODO: the HAVE_ATOMICS flags should be removed and the code in RWMutex should
# use std::atomic and the <atomic> header.
# !!!!!
target_compile_definitions(
  eosCommon
  PUBLIC -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID} -DHAVE_ATOMICS=1)

target_include_directories(
  eosCommon PUBLIC ${CURL_INCLUDE_DIRS})

if(MacOSX)
  target_link_libraries(
    eosCommon
    XrdMqClient
    ${Z_LIBRARY}
    ${UUID_LIBRARIES}
    ${ATTR_LIBRARIES}
    ${NCURSES_LIBRARY}
    ${CURL_LIBRARIES}
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${OPENSSL_CRYPTO_LIBRARY})
else()
  target_link_libraries(
    eosCommon
    XrdMqClient
    ${Z_LIBRARY}
    ${UUID_LIBRARIES}
    ${ATTR_LIBRARIES}
    ${NCURSES_LIBRARY}
    ${CURL_LIBRARIES}
    ${GLIBC_RT_LIBRARY}
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${OPENSSL_CRYPTO_LIBRARY})

  add_library(
    eosCommonServer SHARED
    ShellExecutor.cc
    ShellCmd.cc
    FileSystem.cc
    TransferQueue.cc
    TransferJob.cc
    ZMQ.cc
    sqlite/sqlite3.c
    DbMap.cc
    DbMapSqlite.cc
    DbMapLevelDb.cc
    DbMapCommon.cc
    http/HttpServer.cc
    http/HttpRequest.cc
    http/HttpResponse.cc
    http/s3/S3Handler.cc
    stringencoders/modp_numtoa.c)

  target_include_directories(
   eosCommonServer PUBLIC ${LEVELDB_INCLUDE_DIRS})

  target_link_libraries(
    eosCommonServer
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
    target_link_libraries(eosCommonServer ${MICROHTTPD_LIBRARIES})
  endif()

  set_target_properties(
    eosCommonServer
    PROPERTIES
    VERSION ${VERSION}
    SOVERSION ${VERSION_MAJOR}
    CLEAN_DIRECT_OUTPUT 1
    COMPILE_FLAGS -DSQLITE_NO_SYNC=1)

  install(
    TARGETS eosCommonServer
    LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

  add_library(eosCommon-Static STATIC ${EOSCOMMON_SRCS})

  target_include_directories(
    eosCommon-Static PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR};${CURL_INCLUDE_DIRS}>
    $<INSTALL_INTERFACE:include>)

  # !!!!!
  # TODO: the HAVE_ATOMICS flags should be removed and the code in RWMutex should
  # use std::atomic and the <atomic> header.
  # !!!!!
  target_compile_definitions(
    eosCommon-Static
    PUBLIC -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID} -DHAVE_ATOMICS=1)

  target_compile_options(
    eosCommon-Static
    PUBLIC -fPIC)

  target_link_libraries(
    eosCommon-Static
    XrdMqClient-Static
    ${Z_LIBRARY}
    ${UUID_LIBRARIES}
    ${ATTR_LIBRARIES}
    ${NCURSES_LIBRARY}
    ${CURL_LIBRARIES}
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${OPENSSL_CRYPTO_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT})
endif()

set_target_properties(
  eosCommon
  PROPERTIES
  VERSION ${VERSION}
  SOVERSION ${VERSION_MAJOR}
  CLEAN_DIRECT_OUTPUT 1
  MACOSX_RPATH TRUE)

install(
  TARGETS eosCommon
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

#-------------------------------------------------------------------------------
# Create helper executables
#-------------------------------------------------------------------------------i
if(NOT CLIENT)
if (Linux)
  add_executable(dbmaptestburn dbmaptest/DbMapTestBurn.cc)
  add_executable(mutextest mutextest/RWMutexTest.cc)
  add_executable(
    dbmaptestfunc
    dbmaptest/DbMapTestFunc.cc
    ${DBMAPTEST_SRCS}
    ${DBMAPTEST_HDRS})

  target_link_libraries(dbmaptestburn eosCommonServer eosCommon ${CMAKE_THREAD_LIBS_INIT})
  target_link_libraries(mutextest eosCommon ${CMAKE_THREAD_LIBS_INIT})
  target_link_libraries(
    dbmaptestfunc
    eosCommonServer
    eosCommon
    ${PROTOBUF_LIBRARIES}
    ${CMAKE_THREAD_LIBS_INIT})
endif()
endif()

#-------------------------------------------------------------------------------
# Plugin Manager library
#-------------------------------------------------------------------------------
add_library(
  EosPluginManager SHARED
  plugin_manager/Plugin.hh
  Namespace.hh
  Logging.cc                        Logging.hh
  plugin_manager/PluginManager.cc   plugin_manager/PluginManager.hh
  plugin_manager/DynamicLibrary.cc  plugin_manager/DynamicLibrary.hh)

target_link_libraries(
  EosPluginManager
  ${XROOTD_UTILS_LIBRARY})

set_target_properties(
  EosPluginManager
  PROPERTIES
  VERSION ${VERSION}
  SOVERSION ${VERSION_MAJOR}
  INTERFACE_LINK_LIBRARIES ""
  LINK_INTERFACE_LIBRARIES ""
  MACOSX_RPATH TRUE)

install(
  TARGETS EosPluginManager
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
