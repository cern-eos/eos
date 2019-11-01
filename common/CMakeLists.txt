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
  ${CMAKE_BINARY_DIR}
  ${PROTOBUF_INCLUDE_DIRS}
  ${XROOTD_INCLUDE_DIRS}
  ${JSONCPP_INCLUDE_DIR}
  ${ZMQ_INCLUDE_DIRS}
  ${SPARSEHASH_INCLUDE_DIRS}
  ${OPENSSL_INCLUDE_DIRS}
  ${LIBBFD_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/qclient/include)

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
# EosCrc32c-Objects library
#-------------------------------------------------------------------------------
add_library(EosCrc32c-Objects OBJECT
  crc32c/crc32c.cc
  crc32c/crc32ctables.cc)

target_include_directories(EosCrc32c-Objects
  PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

set_target_properties(EosCrc32c-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# EosCommon-Objects
#-------------------------------------------------------------------------------
add_library(EosCommon-Objects OBJECT
  config/ConfigParsing.cc                                    config/ConfigParsing.hh

  Fmd.cc
  SymKeys.cc
  InstanceName.cc
  Mapping.cc
  RWMutex.cc
  SharedMutex.cc
  PthreadRWMutex.cc
  ClockGetTime.cc
  StacktraceHere.cc
  Locators.cc
  Logging.cc
  StringConversion.cc
  Statfs.cc
  Report.cc
  StringTokenizer.cc
  CommentLog.cc
  RateLimit.cc
  IntervalStopwatch.cc                                       IntervalStopwatch.hh
  VirtualIdentity.cc                                         VirtualIdentity.hh
  XrdConnPool.cc                                             XrdConnPool.hh
  XrdErrorMap.cc                                             XrdErrorMap.hh
  OAuth.cc                                                   OAuth.hh
  Strerror_r_wrapper.cc
  ErrnoToString.cc
  JeMallocHandler.cc
  plugin_manager/Plugin.hh
  plugin_manager/PluginManager.cc
  plugin_manager/DynamicLibrary.cc
  table_formatter/TableCell.cc
  table_formatter/TableFormatterBase.cc
  token/EosTok.cc)

# Avoid warnings related to bfd_get_section* macros being redefined
target_compile_options(EosCommon-Objects PRIVATE -Wno-macro-redefined)
set_target_properties(EosCommon-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

target_include_directories(EosCommon-Objects
  PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
  PRIVATE $<TARGET_PROPERTY:ZLIB::ZLIB,INTERFACE_INCLUDE_DIRECTORIES>)

#-------------------------------------------------------------------------------
# Add dependency to protobuf objects so we guarantee that the protocol
# buffer files are generated when we try to build XrdEosFst
#-------------------------------------------------------------------------------
add_dependencies(EosCommon-Objects EosFstProto-Objects EosCliProto-Objects)

target_compile_definitions(EosCommon-Objects PUBLIC
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

set(EOSCOMMON_LIBS
  fmt
  ${CURL_LIBRARIES}
  ${GLIBC_DL_LIBRARY}
  ${GLIBC_RT_LIBRARY}
  ${LIBBFD_LIBRARIES}
  ${ATTR_LIBRARIES}
  ${NCURSES_LIBRARY}
  ${JSONCPP_LIBRARIES}
  ${PROTOBUF_LIBRARIES}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT})

#-------------------------------------------------------------------------------
# EosCommon and EosCommon-Static library
#-------------------------------------------------------------------------------
if (GRPC_FOUND)
  add_library(EosCommon SHARED
    $<TARGET_OBJECTS:EosCommon-Objects>
    $<TARGET_OBJECTS:EosCliProto-Objects>
    $<TARGET_OBJECTS:EosNsQuarkdbProto-Objects>
    $<TARGET_OBJECTS:EosFstProto-Objects>
    $<TARGET_OBJECTS:EosGrpcProto-Objects>)

  target_link_libraries(EosCommon PUBLIC
    ${GRPC_GRPC++_LIBRARY})
else ()
  add_library(EosCommon SHARED
    $<TARGET_OBJECTS:EosCommon-Objects>
    $<TARGET_OBJECTS:EosCliProto-Objects>
    $<TARGET_OBJECTS:EosNsQuarkdbProto-Objects>
    $<TARGET_OBJECTS:EosFstProto-Objects>)
endif ()

target_link_libraries(EosCommon
  PUBLIC
  ZLIB::ZLIB
  UUID::UUID
  ${EOSCOMMON_LIBS})

target_compile_definitions(EosCommon-Objects PUBLIC
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

set_target_properties(EosCommon PROPERTIES
  VERSION ${VERSION}
  SOVERSION ${VERSION_MAJOR}
  MACOSX_RPATH TRUE)

if (GRPC_FOUND)
  add_library(EosCommon-Static STATIC
    $<TARGET_OBJECTS:EosCommon-Objects>
    $<TARGET_OBJECTS:EosCliProto-Objects>
    $<TARGET_OBJECTS:EosNsQuarkdbProto-Objects>
    $<TARGET_OBJECTS:EosFstProto-Objects>
    $<TARGET_OBJECTS:EosGrpcProto-Objects>)

  target_link_libraries(EosCommon-Static PRIVATE
    ${GRPC_GRPC++_LIBRARY_STATIC})
else ()
  add_library(EosCommon-Static STATIC
    $<TARGET_OBJECTS:EosCommon-Objects>
    $<TARGET_OBJECTS:EosCliProto-Objects>
    $<TARGET_OBJECTS:EosNsQuarkdbProto-Objects>
    $<TARGET_OBJECTS:EosFstProto-Objects>)
endif ()

target_link_libraries(EosCommon-Static
  PRIVATE
  ${EOSCOMMON_LIBS}
  UUID::UUID
  ZLIB::ZLIB_STATIC)

set_target_properties(EosCommon-Static PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

target_compile_definitions(EosCommon-Static PUBLIC
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

target_include_directories(EosCommon-Static PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
  $<INSTALL_INTERFACE:include>)

install(TARGETS EosCommon
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

#-------------------------------------------------------------------------------
# EosCommonServer and EosCommonServer-Static libraries
#-------------------------------------------------------------------------------
if (Linux)
  add_library(EosCommonServer-Objects OBJECT
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

  set_target_properties(EosCommonServer-Objects PROPERTIES
    POSITION_INDEPENDENT_CODE TRUE)

  target_include_directories(EosCommonServer-Objects PUBLIC
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>)

  target_compile_definitions(EosCommonServer-Objects PUBLIC
    -DSQLITE_NO_SYNC=1)

  set(EOSCOMMONSERVER_LIBS
    fmt
    qclient
    ${ZMQ_LIBRARIES}
    ${ATTR_LIBRARIES}
    ${NCURSES_LIBRARY}
    ${JSONCPP_LIBRARIES}
    ${LEVELDB_LIBRARIES}
    ${GLIBC_RT_LIBRARY}
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${OPENSSL_CRYPTO_LIBRARY})

  if(MICROHTTPD_FOUND)
    LIST(APPEND EOSCOMMONSERVER_LIBS "${MICROHTTPD_LIBRARIES}")
  endif()

  add_library(EosCommonServer SHARED
    $<TARGET_OBJECTS:EosCommonServer-Objects>)

  target_link_libraries(EosCommonServer PUBLIC
    XrdMqClient
    UUID::UUID
    ${EOSCOMMONSERVER_LIBS})

  set_target_properties(EosCommonServer PROPERTIES
    VERSION ${VERSION}
    SOVERSION ${VERSION_MAJOR}
    MACOSX_RPATH TRUE)

  target_compile_definitions(EosCommonServer PUBLIC
    -DSQLITE_NO_SYNC=1)

  target_include_directories(
    EosCommonServer PUBLIC ${LEVELDB_INCLUDE_DIRS})

  add_library(EosCommonServer-Static STATIC
    $<TARGET_OBJECTS:EosCommonServer-Objects>)

  target_link_libraries(EosCommonServer-Static PUBLIC
    XrdMqClient-Static
    UUID::UUID
    ${EOSCOMMONSERVER_LIBS})

  target_compile_definitions(EosCommonServer-Static PUBLIC
    -DSQLITE_NO_SYNC=1)

  target_include_directories(
    EosCommonServer-Static PUBLIC ${LEVELDB_INCLUDE_DIRS})

  install(TARGETS EosCommonServer
    LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
endif()

#-------------------------------------------------------------------------------
# Create helper executables
#-------------------------------------------------------------------------------
if(NOT CLIENT AND Linux)
  add_executable(dbmaptestburn dbmaptest/DbMapTestBurn.cc)
  add_executable(mutextest
    mutextest/RWMutexTest.cc RWMutex.cc PthreadRWMutex.cc StacktraceHere.cc)

  add_executable(dbmaptestfunc
    dbmaptest/DbMapTestFunc.cc
    ${DBMAPTEST_SRCS}
    ${DBMAPTEST_HDRS})

  target_link_libraries(dbmaptestburn PRIVATE
    EosCommon
    EosCommonServer
    ${CMAKE_THREAD_LIBS_INIT})

  target_include_directories(dbmaptestburn PRIVATE
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>)

  target_link_libraries(mutextest PRIVATE
    EosCommon-Static
    ${CMAKE_THREAD_LIBS_INIT})

  target_include_directories(mutextest PRIVATE
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>)

  target_link_libraries(dbmaptestfunc PRIVATE
    EosCommon
    EosCommonServer
    ${CMAKE_THREAD_LIBS_INIT})

  target_include_directories(dbmaptestfunc PRIVATE
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>)
endif()
