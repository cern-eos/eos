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

if(DAVIX_FOUND)
  add_definitions(-DDAVIX_FOUND)
  set(DAVIX_SRC "io/davix/DavixIo.cc")
  set(DAVIX_HDR "io/davix/DavixIo.hh")
else()
  set(DAVIX_INCLUDE_DIRS "")
  set(DAVIX_LIBRARIES "")
  set(DAVIX_SRC "")
  set(DAVIX_HDR "")
endif()

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${DAVIX_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/qclient/include
  ${CMAKE_SOURCE_DIR}/common/xrootd-ssi-protobuf-interface/include)

#-------------------------------------------------------------------------------
# Add tests if not on MacOSX
#-------------------------------------------------------------------------------
if (NOT MacOSX)
  add_subdirectory(tests)
endif ()

#-------------------------------------------------------------------------------
# Add XrdCl RAIN plugin library only if explicitly requested by using the
# BUILD_XRDCL_RAIN_PLUGIN flag.
#-------------------------------------------------------------------------------
if(BUILD_XRDCL_RAIN_PLUGIN)
  add_subdirectory(xrdcl_plugins)
endif()

#-------------------------------------------------------------------------------
# Gfcomplete objects
#-------------------------------------------------------------------------------
set(GFCOMPLETE_SRCS
  layout/gf-complete/src/gf.c
  layout/gf-complete/src/gf_w128.c
  layout/gf-complete/src/gf_rand.c
  layout/gf-complete/src/gf_method.c
  layout/gf-complete/src/gf_general.c
  layout/gf-complete/src/gf_w16.c
  layout/gf-complete/src/gf_w32.c
  layout/gf-complete/src/gf_w8.c
  layout/gf-complete/src/gf_w64.c
  layout/gf-complete/src/gf_w4.c
  layout/gf-complete/src/gf_wgen.c)

set(GFCOMPLETE_HDRS
  layout/gf-complete/include/gf_complete.h
  layout/gf-complete/include/gf_rand.h
  layout/gf-complete/include/gf_method.h
  layout/gf-complete/include/gf_int.h
  layout/gf-complete/include/gf_general.h
  layout/gf-complete/include/gf_w8.h
  layout/gf-complete/include/gf_w64.h
  layout/gf-complete/include/gf_w4.h
  layout/gf-complete/include/gf_w32.h
  layout/gf-complete/include/gf_w16.h)

add_library(GfComplete-Objects OBJECT
  ${GFCOMPLETE_SRCS}
  ${GFCOMPLETE_HDRS})

target_include_directories(GfComplete-Objects PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/layout/gf-complete/include>)

target_compile_definitions(GfComplete-Objects PRIVATE
  -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64)

set_target_properties(GfComplete-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# jerasure 2.0 objects
#-------------------------------------------------------------------------------
set(JERASURE_SRCS
  layout/jerasure/src/cauchy.c
  layout/jerasure/src/galois.c
  layout/jerasure/src/jerasure.c
  layout/jerasure/src/liberation.c
  layout/jerasure/src/reed_sol.c
  layout/jerasure/src/timing.c)

set(JERASURE_HDRS
  layout/jerasure/include/cauchy.h
  layout/jerasure/include/galois.h
  layout/jerasure/include/jerasure.h
  layout/jerasure/include/liberation.h
  layout/jerasure/include/reed_sol.h
  layout/jerasure/include/timing.h)

add_library(Jerasure-Objects OBJECT
  ${JERASURE_SRCS}
  ${JERASURE_HDRS})

target_link_libraries(Jerasure-Objects PUBLIC
  GfComplete-Objects)

target_include_directories(Jerasure-Objects PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/layout/jerasure/include>)

target_compile_definitions(Jerasure-Objects PRIVATE
  -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64)

set_target_properties(Jerasure-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# EosFstIo-Objects library
#-------------------------------------------------------------------------------
add_library(EosFstIo-Objects OBJECT
  # File IO interface
  io/FileIo.hh
  io/local/FsIo.cc               io/local/FsIo.hh
  ${DAVIX_SRC}                   ${DAVIX_HDR}
  io/xrd/XrdIo.cc                io/xrd/XrdIo.hh
  io/AsyncMetaHandler.cc         io/AsyncMetaHandler.hh
  io/ChunkHandler.cc             io/ChunkHandler.hh
  io/VectChunkHandler.cc         io/VectChunkHandler.hh
  io/SimpleHandler.cc            io/SimpleHandler.hh
  io/FileIoPlugin.cc             io/FileIoPlugin.hh

  # Checksum interface
  checksum/CheckSum.cc           checksum/CheckSum.hh
  checksum/Adler.cc              checksum/Adler.hh

  # File layout interface
  layout/LayoutPlugin.cc         layout/LayoutPlugin.hh
  layout/Layout.cc               layout/Layout.hh
  layout/PlainLayout.cc          layout/PlainLayout.hh
  layout/HeaderCRC.cc            layout/HeaderCRC.hh
  layout/ReplicaParLayout.cc     layout/ReplicaParLayout.hh
  layout/RaidMetaLayout.cc       layout/RaidMetaLayout.hh
  layout/RaidDpLayout.cc         layout/RaidDpLayout.hh
  layout/ReedSLayout.cc          layout/ReedSLayout.hh)

target_link_libraries(EosFstIo-Objects PUBLIC
  Jerasure-Objects
  EosCommon
  XROOTD::PRIVATE)

target_compile_definitions(EosFstIo-Objects PRIVATE
  -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64)

target_include_directories(EosFstIo-Objects PUBLIC
  $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>)

set_target_properties(EosFstIo-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

add_library(EosFstIo SHARED $<TARGET_OBJECTS:EosFstIo-Objects>)

target_link_libraries(EosFstIo PUBLIC
  EosFstIo-Objects
  Jerasure-Objects
  GfComplete-Objects
  EosCrc32c-Objects
  XROOTD::SERVER
  ISAL::ISAL
  ISAL::ISAL_CRYPTO
  XXHASH::XXHASH
  ${GLIBC_DL_LIBRARY}
  ${DAVIX_LIBRARIES})

set_target_properties(EosFstIo PROPERTIES
  VERSION ${VERSION}
  SOVERSION ${VERSION_MAJOR}
  MACOSX_RPATH TRUE)

install(TARGETS EosFstIo
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

#-------------------------------------------------------------------------------
# EosFstIo-Static library
#-------------------------------------------------------------------------------
add_library(EosFstIo-Static STATIC $<TARGET_OBJECTS:EosFstIo-Objects>)

target_link_libraries(EosFstIo-Static PUBLIC
  EosFstIo-Objects
  Jerasure-Objects
  GfComplete-Objects
  EosCrc32c-Objects
  XROOTD::SERVER
  ISAL::ISAL
  ISAL::ISAL_CRYPTO
  XXHASH::XXHASH
  ${DAVIX_LIBRARIES})

target_compile_definitions(EosFstIo-Static PRIVATE
  -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64)

set_target_properties(EosFstIo-Static PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# eoscp executable
#-------------------------------------------------------------------------------
add_executable(eoscp txqueue/eoscp.cc)

target_link_libraries(eoscp PRIVATE EosFstIo-Static)

target_compile_definitions(eoscp PRIVATE
  -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64)

set_target_properties(
  eoscp PROPERTIES
  INSTALL_RPATH "${EOS_RPATH}"
  SKIP_RPATH FALSE
  SKIP_BUILD_RPATH FALSE
  BUILD_WITH_INSTALL_RPATH TRUE
  INSTALL_RPATH_USE_LINK_PATH TRUE)

install(TARGETS eoscp
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

if(NOT CLIENT)
#-------------------------------------------------------------------------------
# EosFstOss library
#-------------------------------------------------------------------------------
add_library(EosFstOss MODULE
  XrdFstOss.cc XrdFstOss.hh
  XrdFstOssFile.cc XrdFstOssFile.hh
  checksum/CheckSum.cc checksum/CheckSum.hh
  checksum/Adler.cc checksum/Adler.hh
  $<TARGET_OBJECTS:EosCrc32c-Objects>)

target_link_libraries(EosFstOss PRIVATE
  EosCommon
  ISAL::ISAL
  ISAL::ISAL_CRYPTO
  XXHASH::XXHASH)

#-------------------------------------------------------------------------------
# XrdEosFst library
#-------------------------------------------------------------------------------
set(XRDEOSFST_SRCS
  Config.cc
  Load.cc
  Health.cc
  ScanDir.cc
  Messaging.cc
  io/FileIoPlugin-Server.cc

  # OFS layer implementation
  XrdFstOfs.cc                   XrdFstOfs.hh
  XrdFstOfsFile.cc               XrdFstOfsFile.hh

  # Storage interface
  storage/Balancer.cc
  storage/Cleaner.cc             storage/Communicator.cc
  storage/ErrorReport.cc
  storage/FileSystem.cc          storage/MgmSyncer.cc
  storage/Publish.cc             storage/Remover.cc
  storage/Report.cc              storage/Scrub.cc
  storage/Storage.cc             storage/Supervisor.cc
  storage/Trim.cc                storage/Verify.cc

  # Transfer interface
  txqueue/TransferMultiplexer.cc
  txqueue/TransferJob.cc
  txqueue/TransferQueue.cc

  # Utils
  utils/OpenFileTracker.cc

  # File metadata interface
  FmdDbMap.cc          FmdDbMap.hh

  # HTTP interface
  http/HttpServer.cc    http/HttpServer.hh
  http/HttpHandler.cc   http/HttpHandler.hh
  http/s3/S3Handler.cc  http/s3/S3Handler.hh

  # EosFstIo interface
  io/local/LocalIo.cc  io/local/LocalIo.hh)

add_library(XrdEosFst-Objects OBJECT ${XRDEOSFST_SRCS})

target_link_libraries(XrdEosFst-Objects PUBLIC
  EosFstProto-Objects
  EosCliProto-Objects
  EosNsQuarkdbProto-Objects
  XrdSsiPbEosCta-Objects
  EosConsoleHelpers-Objects
  EosCommonServer
  EosNsCommon
  XROOTD::UTILS
  XROOTD::PRIVATE
  XFS::XFS)

set_target_properties(XrdEosFst-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

target_compile_definitions(XrdEosFst-Objects PUBLIC
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

add_library(XrdEosFst-${PLUGIN_VERSION} MODULE
  $<TARGET_OBJECTS:XrdEosFst-Objects>)

target_link_libraries(XrdEosFst-${PLUGIN_VERSION} PRIVATE
  XrdEosFst-Objects
  EosConsoleHelpers-Objects
  EosFstIo

  XROOTD::SSI)

target_compile_definitions(XrdEosFst-${PLUGIN_VERSION} PUBLIC
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

add_library(XrdEosFst-Static STATIC $<TARGET_OBJECTS:XrdEosFst-Objects>)

target_compile_definitions(XrdEosFst-Static PUBLIC
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

target_link_libraries(XrdEosFst-Static PUBLIC
  XrdEosFst-Objects
  EosConsoleHelpers-Objects
  EosCommonServer
  EosFstIo
  # @todo(gbitzes) move calculateEtag to common so that we don't have to couple
  # the fst lib with the ns one
  EosNsCommon
  XROOTD::SSI)

install(TARGETS EosFstOss XrdEosFst-${PLUGIN_VERSION}
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

#-------------------------------------------------------------------------------
# EosFstHttp library
#-------------------------------------------------------------------------------
add_library(EosFstHttp MODULE
  http/xrdhttp/EosFstHttpHandler.hh
  http/xrdhttp/EosFstHttpHandler.cc)

target_link_libraries(EosFstHttp PUBLIC
  XrdEosFst-Objects
  EosConsoleHelpers-Objects
  EosFstIo
  # @todo(gbitzes) move calculateEtag to common so that we don't have to couple
  # the fst lib with the ns one
  EosNsCommon
  EosCommonServer
  XROOTD::SSI
  XROOTD::HTTP)

install(TARGETS EosFstHttp
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

#-------------------------------------------------------------------------------
# Other executables
#-------------------------------------------------------------------------------
add_executable(eos-rain-hd-dump
  tools/RainHdrDump.cc)

target_link_libraries(eos-rain-hd-dump
  EosFstIo)

add_executable(eos-leveldb-inspect
  tools/eos-leveldb-inspect.cc)

target_link_libraries(eos-leveldb-inspect PRIVATE EosCommonServer)

add_executable(eos-check-blockxs
  tools/CheckBlockXS.cc
  checksum/Adler.cc
  checksum/CheckSum.cc)

add_executable(eos-compute-blockxs
  tools/ComputeBlockXS.cc
  checksum/Adler.cc
  checksum/CheckSum.cc)

add_executable(eos-scan-fs
  tools/ScanXS.cc
  ScanDir.cc
  Load.cc
  FmdDbMap.cc
  checksum/Adler.cc
  checksum/CheckSum.cc)

target_compile_definitions(eos-scan-fs PUBLIC -D_NOOFS=1)

add_executable(eos-fsck-fs
  Fsck.cc
  Load.cc
  FmdDbMap.cc
  tools/Fsck.cc
  checksum/Adler.cc
  checksum/CheckSum.cc)

target_compile_definitions(eos-fsck-fs PUBLIC -D_NOOFS=1)

add_executable(eos-adler32
  tools/Adler32.cc
  checksum/Adler.cc
  checksum/CheckSum.cc)

add_executable(eos-checksum
  tools/CheckSum.cc
  checksum/CheckSum.cc)

set_target_properties(eos-scan-fs PROPERTIES COMPILE_FLAGS -D_NOOFS=1)
set_target_properties(eos-fsck-fs PROPERTIES COMPILE_FLAGS -D_NOOFS=1)

add_executable(eos-ioping tools/IoPing.c)
target_compile_options(eos-ioping PRIVATE -std=gnu99)
target_link_libraries(eos-ioping PRIVATE ${GLIBC_M_LIBRARY} ${GLIBC_RT_LIBRARY})
target_link_libraries(eos-check-blockxs PRIVATE EosFstIo)
target_link_libraries(eos-compute-blockxs PRIVATE EosFstIo)
target_link_libraries(eos-adler32 PRIVATE EosFstIo)
target_link_libraries(eos-checksum PRIVATE EosFstIo)
target_link_libraries(eos-scan-fs PRIVATE
  EosFstIo
  EosCommonServer
  EosNsCommon-Static)
target_link_libraries(eos-fsck-fs PRIVATE
  EosFstIo
  EosCommonServer
  EosNsCommon-Static)

install(PROGRAMS
  tools/eosfstregister
  tools/eosfstinfo
  tools/eos-iobw
  tools/eos-iops
  DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})

install(TARGETS
  eos-ioping eos-adler32 eos-checksum eos-leveldb-inspect eos-rain-hd-dump
  eos-check-blockxs eos-compute-blockxs eos-scan-fs eos-fsck-fs
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})

endif()
