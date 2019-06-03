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
  ${OPENSSL_INCLUDE_DIRS}
  ${LIBBFD_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/qclient/include
)

#-------------------------------------------------------------------------------
# CTA integration related operations
#-------------------------------------------------------------------------------
set(EOS_CTA_PB_DIR ${CMAKE_CURRENT_SOURCE_DIR}/xrootd-ssi-protobuf-interface/eos_cta/protobuf/)
add_subdirectory(eos_cta_pb)

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
# eosCommon-Objects
#-------------------------------------------------------------------------------
add_library(eosCommon-Objects OBJECT
  SymKeys.cc
  Mapping.cc
  RWMutex.cc
  SharedMutex.cc
  PthreadRWMutex.cc
  ClockGetTime.cc
  StacktraceHere.cc
  Logging.cc
  StringConversion.cc
  Statfs.cc
  Report.cc
  StringTokenizer.cc
  CommentLog.cc
  IntervalStopwatch.cc                                       IntervalStopwatch.hh
  VirtualIdentity.cc                                         VirtualIdentity.hh
  XrdConnPool.cc                                             XrdConnPool.hh
  XrdErrorMap.cc                                             XrdErrorMap.hh
  JeMallocHandler.cc
  plugin_manager/Plugin.hh
  plugin_manager/PluginManager.cc
  plugin_manager/DynamicLibrary.cc
  ${CMAKE_SOURCE_DIR}/mgm/TableFormatter/TableCell.cc)

set_target_properties(eosCommon-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

target_compile_definitions(eosCommon-Objects PUBLIC
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

set(EOSCOMMON_LIBS
  fmt
  ${Z_LIBRARY}
  ${CURL_LIBRARIES}
  ${GLIBC_DL_LIBRARY}
  ${GLIBC_RT_LIBRARY}
  ${LIBBFD_LIBRARIES}
  ${UUID_LIBRARIES}
  ${ATTR_LIBRARIES}
  ${NCURSES_LIBRARY}
  ${PROTOBUF_LIBRARY}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT})

#-------------------------------------------------------------------------------
# eosCommon and eosCommon-Static library
#-------------------------------------------------------------------------------
add_library(eosCommon SHARED $<TARGET_OBJECTS:eosCommon-Objects>)

target_link_libraries(eosCommon PUBLIC
  ${EOSCOMMON_LIBS})

set_target_properties(eosCommon PROPERTIES
  VERSION ${VERSION}
  SOVERSION ${VERSION_MAJOR}
  MACOSX_RPATH TRUE)

target_compile_definitions(eosCommon PUBLIC
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

target_include_directories(eosCommon PUBLIC ${CURL_INCLUDE_DIRS})

add_library(eosCommon-Static STATIC $<TARGET_OBJECTS:eosCommon-Objects>)

target_link_libraries(eosCommon-Static PUBLIC
  ${EOSCOMMON_LIBS})

set_target_properties(eosCommon-Static PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

target_compile_definitions(eosCommon-Static PUBLIC
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

target_include_directories(eosCommon-Static PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:include>)

install(TARGETS eosCommon
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

#-------------------------------------------------------------------------------
# eosCommonServer and eosCommonServer-Static libraries
#-------------------------------------------------------------------------------
if (Linux)
  add_library(eosCommonServer-Objects OBJECT
    GlobalConfig.cc
    ShellExecutor.cc
    ShellCmd.cc
    FileSystem.cc
    TransferQueue.cc
    TransferJob.cc
    sqlite/sqlite3.c
    DbMap.cc
    DbMapLevelDb.cc
    DbMapCommon.cc
    http/HttpServer.cc
    http/HttpRequest.cc
    http/HttpResponse.cc
    http/s3/S3Handler.cc
    stringencoders/modp_numtoa.c)

  set_target_properties(eosCommonServer-Objects PROPERTIES
    POSITION_INDEPENDENT_CODE TRUE)

  target_compile_definitions(eosCommonServer-Objects PUBLIC
    -DSQLITE_NO_SYNC=1)

  set(EOSCOMMONSERVER_LIBS
    ${Z_LIBRARY}
    ${ZMQ_LIBRARIES}
    ${UUID_LIBRARIES}
    ${ATTR_LIBRARIES}
    ${NCURSES_LIBRARY}
    ${LEVELDB_LIBRARIES}
    ${GLIBC_RT_LIBRARY}
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${OPENSSL_CRYPTO_LIBRARY})

  if(MICROHTTPD_FOUND)
    LIST(APPEND EOSCOMMONSERVER_LIBS "${MICROHTTPD_LIBRARIES}")
  endif()

  add_library(eosCommonServer SHARED
    $<TARGET_OBJECTS:eosCommonServer-Objects>)

  target_link_libraries(eosCommonServer PUBLIC
    fmt
    XrdMqClient
    ${EOSCOMMONSERVER_LIBS})

  set_target_properties(eosCommonServer PROPERTIES
    VERSION ${VERSION}
    SOVERSION ${VERSION_MAJOR}
    MACOSX_RPATH TRUE)

  target_compile_definitions(eosCommonServer PUBLIC
    -DSQLITE_NO_SYNC=1)

  target_include_directories(
    eosCommonServer PUBLIC ${LEVELDB_INCLUDE_DIRS})

  add_library(eosCommonServer-Static STATIC
    $<TARGET_OBJECTS:eosCommonServer-Objects>)

  target_link_libraries(eosCommonServer-Static PUBLIC
    fmt
    XrdMqClient-Static
    ${EOSCOMMONSERVER_LIBS})

  target_compile_definitions(eosCommonServer-Static PUBLIC
    -DSQLITE_NO_SYNC=1)

  target_include_directories(
    eosCommonServer-Static PUBLIC ${LEVELDB_INCLUDE_DIRS})

  install(TARGETS eosCommonServer
    LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
endif()

#-------------------------------------------------------------------------------
# Create helper executables
#-------------------------------------------------------------------------------
if(NOT CLIENT AND Linux)
  add_executable(dbmaptestburn dbmaptest/DbMapTestBurn.cc)
  add_executable(mutextest mutextest/RWMutexTest.cc RWMutex.cc PthreadRWMutex.cc StacktraceHere.cc)
  add_executable(dbmaptestfunc
    dbmaptest/DbMapTestFunc.cc
    ${DBMAPTEST_SRCS}
    ${DBMAPTEST_HDRS})

  target_link_libraries(dbmaptestburn PRIVATE
    eosCommon
    eosCommonServer
    ${CMAKE_THREAD_LIBS_INIT})

  target_link_libraries(mutextest PRIVATE
    eosCommon-Static
    ${CMAKE_THREAD_LIBS_INIT})

  target_link_libraries(dbmaptestfunc PRIVATE
    eosCommon
    eosCommonServer
    ${CMAKE_THREAD_LIBS_INIT})
endif()
