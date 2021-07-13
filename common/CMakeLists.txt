#-------------------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Andreas-Joachim Peters - CERN
#-------------------------------------------------------------------------------

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

include_directories(${CMAKE_SOURCE_DIR}/common/jwt-cpp/include/)

#-------------------------------------------------------------------------------
# CTA integration related operations
#-------------------------------------------------------------------------------
add_subdirectory(eos_cta_pb)

#-------------------------------------------------------------------------------
# EosCrc32c-Objects library
#-------------------------------------------------------------------------------
add_library(EosCrc32c-Objects OBJECT
  crc32c/crc32c.cc
  crc32c/crc32ctables.cc)

set_target_properties(EosCrc32c-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# EosBlake3-Objects library
#-------------------------------------------------------------------------------
set(BLAKE3_PORTABLE_SRCS
  blake3/blake3.c
  blake3/blake3_dispatch.c
  blake3/blake3_portable.c)


if (ARM64_BUILD)
# For 64 bit arm neon is mandatory, check HAVE_NEON only for arm32
    set(BLAKE3_NATIVE_SRCS
      blake3/blake3_neon.c)
    message(STATUS "Building Blake3 with ARM NEON intrinsics")
else()
  if (HAVE_AVX2)
    set(BLAKE3_AVX_SRCS blake3/blake3_avx2_x86-64_unix.S)
    if (HAVE_AVX512)
      set(BLAKE3_AVX_SRCS
        "${BLAKE3_AVX_SRCS}"
        blake3/blake3_avx512_x86-64_unix.S)
    endif() # AVX_512
  endif() #AVX2

  if (HAVE_SSE42)
    set(BLAKE3_SSE_SRCS
      blake3/blake3_sse2_x86-64_unix.S
      blake3/blake3_sse41_x86-64_unix.S)
  endif() # SSE

  message(STATUS "Building Blake3 on x86_64 with intrinsics AVX2=${HAVE_AVX2} "
    "AVX512=${HAVE_AVX512} SSE42=${HAVE_SSE42}")
  set(BLAKE3_NATIVE_SRCS
    "${BLAKE3_SSE_SRCS}"
    "${BLAKE3_AVX_SRCS}")
  set_property(SOURCE ${BLAKE3_NATIVE_SRCS}
    PROPERTY LANGUAGE C)
endif()

add_library(EosBlake3-Objects OBJECT
  ${BLAKE3_PORTABLE_SRCS}
  ${BLAKE3_NATIVE_SRCS}
  )

set_target_properties(EosBlake3-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

if (ARM64_BUILD)
  target_compile_definitions(EosBlake3-Objects PUBLIC BLAKE3_USE_NEON)
endif()

# FIXME: This is a workaround for clang < 7 see
# https://bugs.llvm.org/show_bug.cgi?id=39875 &&
# https://bugs.llvm.org/show_bug.cgi?id=36202 and remove this when we drop
# clang5 support or we enable this globally for all options
if (HAVE_AVX512 AND
    ("${CMAKE_CXX_COMPILER_ID}" MATCHES Clang) AND
    ("${CMAKE_CXX_COMPILER_VERSION}" VERSION_LESS "7.0"))
  target_compile_options(EosBlake3-Objects PRIVATE -mavx512f -mavx512vl)
endif() # CLANG5 hack
#-------------------------------------------------------------------------------
# EosCommon-Objects
#-------------------------------------------------------------------------------
add_library(EosCommon-Objects OBJECT
  Fmd.cc
  SymKeys.cc
  InstanceName.cc
  Mapping.cc
  MutexLatencyWatcher.cc
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
  IntervalStopwatch.cc
  VirtualIdentity.cc
  XrdConnPool.cc
  XrdErrorMap.cc
  OAuth.cc
  Strerror_r_wrapper.cc
  ErrnoToString.cc
  JeMallocHandler.cc
  plugin_manager/Plugin.hh
  plugin_manager/PluginManager.cc
  plugin_manager/DynamicLibrary.cc
  table_formatter/TableCell.cc
  table_formatter/TableFormatterBase.cc
  config/ConfigParsing.cc
  token/EosTok.cc
  utils/XrdUtils.cc
  exception/Exception.cc)

# Avoid warnings related to bfd_get_section* macros being redefined
target_compile_options(EosCommon-Objects PRIVATE -Wno-macro-redefined)
set_target_properties(EosCommon-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

target_link_libraries(EosCommon-Objects PUBLIC
  fmt
  EosFstProto-Objects
  EosCliProto-Objects
  CURL::libcurl
  ZLIB::ZLIB
  JSONCPP::JSONCPP
  OPENSSL::OPENSSL
  XROOTD::UTILS
  GLIBC::DL
  GLIBC::RT
  LIBBFD::LIBBFD
  LIBBFD::IBERTY
  GOOGLE::SPARSEHASH
  ${CMAKE_THREAD_LIBS_INIT})

set_target_properties(EosCommon-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

target_compile_definitions(EosCommon-Objects PUBLIC
  DAEMONUID=${DAEMONUID} DAEMONGID=${DAEMONGID})

#-------------------------------------------------------------------------------
# EosCommon and EosCommon-Static library
#-------------------------------------------------------------------------------
add_library(EosCommon SHARED $<TARGET_OBJECTS:EosCommon-Objects>)

target_link_libraries(EosCommon PUBLIC
  EosCommon-Objects
  EosFstProto-Objects
  EosCliProto-Objects
  EosNsQuarkdbProto-Objects
  NCURSES::NCURSES
  UUID::UUID
  XROOTD::CL)

set_target_properties(EosCommon PROPERTIES
  VERSION ${VERSION}
  SOVERSION ${VERSION_MAJOR}
  MACOSX_RPATH TRUE)

add_library(EosCommon-Static STATIC $<TARGET_OBJECTS:EosCommon-Objects>)

target_link_libraries(EosCommon-Static PUBLIC
  EosCommon-Objects
  EosFstProto-Objects
  EosCliProto-Objects
  EosNsQuarkdbProto-Objects
  XROOTD::CL
  UUID::UUID
  ZLIB::ZLIB_STATIC
  NCURSES::NCURSES)

set_target_properties(EosCommon-Static PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

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

  target_link_libraries(EosCommonServer-Objects PUBLIC
    qclient
    XROOTD::UTILS
    LEVELDB::LEVELDB
    PROTOBUF::PROTOBUF
    LIBMICROHTTPD::LIBMICROHTTPD)

  target_compile_definitions(EosCommonServer-Objects PUBLIC
    -DSQLITE_NO_SYNC=1)

  set_target_properties(EosCommonServer-Objects PROPERTIES
    POSITION_INDEPENDENT_CODE TRUE)

  add_library(EosCommonServer SHARED
    $<TARGET_OBJECTS:EosCommonServer-Objects>)

  target_link_libraries(EosCommonServer PUBLIC
    EosCommonServer-Objects
    XrdSsiPbEosCta-Objects
    XrdMqClient)

  target_compile_definitions(EosCommonServer PUBLIC
    -DSQLITE_NO_SYNC=1)

  set_target_properties(EosCommonServer PROPERTIES
    VERSION ${VERSION}
    SOVERSION ${VERSION_MAJOR}
    MACOSX_RPATH TRUE)

  add_library(EosCommonServer-Static STATIC
    $<TARGET_OBJECTS:EosCommonServer-Objects>)

  target_link_libraries(EosCommonServer-Static PUBLIC
    EosCommonServer-Objects
    XrdSsiPbEosCta-Objects
    XrdMqClient-Static
    qclient)

  target_compile_definitions(EosCommonServer-Static PUBLIC
    -DSQLITE_NO_SYNC=1)

  install(TARGETS EosCommonServer
    LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
endif()

#-------------------------------------------------------------------------------
# Create helper executables
#-------------------------------------------------------------------------------
if(NOT CLIENT AND Linux)
  add_executable(eos-layout-print EosLayoutPrint.cc)
  target_link_libraries(eos-layout-print PUBLIC EosCommon)
  add_executable(dbmaptestburn dbmaptest/DbMapTestBurn.cc)
  add_executable(mutextest
    mutextest/RWMutexTest.cc RWMutex.cc PthreadRWMutex.cc StacktraceHere.cc)
  # Avoid warnings related to bfd_get_section* macros being redefined
  target_compile_options(mutextest PRIVATE -Wno-macro-redefined)

  PROTOBUF_GENERATE_CPP(DBMAPTEST_SRCS DBMAPTEST_HDRS dbmaptest/test.proto)
  set_source_files_properties(
    ${DBMAPTEST_SRCS}
    ${DBMAPTEST_HDRS}
    PROPERTIES GENERATED 1)

  add_executable(dbmaptestfunc
    dbmaptest/DbMapTestFunc.cc
    ${DBMAPTEST_SRCS}
    ${DBMAPTEST_HDRS})

  target_link_libraries(dbmaptestburn PRIVATE
    EosCommon
    EosCommonServer)

  target_link_libraries(mutextest PRIVATE
    EosCommon-Static)

  target_link_libraries(dbmaptestfunc PRIVATE
    EosCommon
    EosCommonServer)
endif()
