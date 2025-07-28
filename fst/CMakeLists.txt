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
  ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/qclient/include
  ${CMAKE_SOURCE_DIR}/common/xrootd-ssi-protobuf-interface/include
  ${CMAKE_SOURCE_DIR}/common
)

#-------------------------------------------------------------------------------
# Add XrdCl RAIN plugin library only if explicitly requested by using the
# BUILD_XRDCL_RAIN_PLUGIN flag.
#-------------------------------------------------------------------------------
if(BUILD_XRDCL_RAIN_PLUGIN)
  add_subdirectory(xrdcl_plugins)
endif()

#-------------------------------------------------------------------------------
# Add css plugin library only if explicitly requested by using the
# BUILD_CSS_PLUGIN flag.
#-------------------------------------------------------------------------------
if(BUILD_CSS_PLUGIN)
  add_subdirectory(css_plugin)
  install(TARGETS EosFstCss
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
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
  io/davix/DavixIo.cc            io/davix/DavixIo.hh
  io/nfs/NfsIo.cc                io/nfs/NfsIo.hh
  io/xrd/XrdIo.cc                io/xrd/XrdIo.hh
  io/xrd/ResponseCollector.cc    io/xrd/ResponseCollector.hh
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
  layout/RainBlock.cc            layout/RainBlock.hh
  layout/RainGroup.cc            layout/RainGroup.hh
  layout/RainMetaLayout.cc       layout/RainMetaLayout.hh
  layout/RaidDpLayout.cc         layout/RaidDpLayout.hh
  layout/ReedSLayout.cc          layout/ReedSLayout.hh
  utils/FSPathHandler.cc
  utils/IoPriority.cc)

target_link_libraries(EosFstIo-Objects PUBLIC
  Jerasure-Objects
  EosCommon
  DAVIX::DAVIX
  XROOTD::PRIVATE
  NFS::NFS)

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
  EosBlake3-Objects
  ISAL::ISAL
  ISAL::ISAL_CRYPTO
  XXHASH::XXHASH
  NFS::NFS)

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
  EosBlake3-Objects
  ISAL::ISAL
  ISAL::ISAL_CRYPTO
  XXHASH::XXHASH
  NFS::NFS)

target_compile_definitions(EosFstIo-Static PRIVATE
  -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64)

set_target_properties(EosFstIo-Static PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

#-------------------------------------------------------------------------------
# eoscp executable
#-------------------------------------------------------------------------------
add_executable(eoscp eoscp.cc)

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
  $<TARGET_OBJECTS:EosCrc32c-Objects>
  $<TARGET_OBJECTS:EosBlake3-Objects>)

target_link_libraries(EosFstOss PRIVATE
  EosCommon
  ISAL::ISAL
  ISAL::ISAL_CRYPTO
  XXHASH::XXHASH)

add_library(EosFstOss-Static STATIC
  XrdFstOss.cc XrdFstOss.hh
  XrdFstOssFile.cc XrdFstOssFile.hh
  checksum/CheckSum.cc checksum/CheckSum.hh
  checksum/Adler.cc checksum/Adler.hh
  $<TARGET_OBJECTS:EosCrc32c-Objects>
  $<TARGET_OBJECTS:EosBlake3-Objects>)

target_link_libraries(EosFstOss-Static PRIVATE
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
  storage/Communicator.cc
  storage/ErrorReport.cc
  storage/FileSystem.cc          storage/MgmSyncer.cc
  storage/Publish.cc             storage/Remover.cc
  storage/Report.cc              storage/Scrub.cc
  storage/Storage.cc             storage/Supervisor.cc
  storage/Verify.cc
  # Utils
  utils/OpenFileTracker.cc
  # File metadata interface
  filemd/FmdHandler.cc
  filemd/FmdMgm.cc
  filemd/FmdAttr.cc
  # HTTP interface
  http/HttpServer.cc    http/HttpServer.hh
  http/HttpHandler.cc   http/HttpHandler.hh
  http/HttpHandlerFstFileCache.cc   http/HttpHandlerFstFileCache.hh
  http/s3/S3Handler.cc  http/s3/S3Handler.hh
  # EosFstIo interface
  io/local/LocalIo.cc  io/local/LocalIo.hh
  utils/XrdOfsPathHandler.cc
  utils/DiskMeasurements.cc)

add_library(XrdEosFst-Objects OBJECT ${XRDEOSFST_SRCS})

#-------------------------------------------------------------------------------
# Disable -Wsign-compare warnings due to
# grpcpp/support/proto_buffer_reader.h:157:24: warning: comparison of
# integer expressions of different signedness: ‘uint64_t’ {aka ‘long
# unsigned int’} and ‘int’ [-Wsign-compare
#-------------------------------------------------------------------------------
target_compile_options(XrdEosFst-Objects PUBLIC -Wno-sign-compare)

target_link_libraries(XrdEosFst-Objects PUBLIC
  EosFstProto-Objects
  EosCliProto-Objects
  EosNsQuarkdbProto-Objects
  XrdSsiPbEosCta-Objects
  EosConsoleHelpers-Objects
  EosFstIo
  EosCommonServer
  EosNsCommon
  XROOTD::UTILS
  XROOTD::PRIVATE
  XROOTD::SERVER
  XFS::XFS
  stdc++fs)

if(TARGET PROCPS::PROCPS)
  target_compile_definitions(XrdEosFst-Objects PRIVATE PROCPS3)
  target_link_libraries(XrdEosFst-Objects PUBLIC PROCPS::PROCPS)
endif()

if(TARGET procps::libproc2)
  target_link_libraries(XrdEosFst-Objects PUBLIC procps::libproc2)
endif()

set_target_properties(XrdEosFst-Objects PROPERTIES
  POSITION_INDEPENDENT_CODE TRUE)

target_compile_definitions(XrdEosFst-Objects PUBLIC
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

add_library(XrdEosFst-${XRDPLUGIN_SOVERSION} MODULE
  $<TARGET_OBJECTS:XrdEosFst-Objects>)

target_link_libraries(XrdEosFst-${XRDPLUGIN_SOVERSION} PRIVATE
  XrdEosFst-Objects
  EosConsoleHelpers-Objects
  XROOTD::SSI)

target_compile_definitions(XrdEosFst-${XRDPLUGIN_SOVERSION} PUBLIC
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

add_library(XrdEosFst-Static STATIC $<TARGET_OBJECTS:XrdEosFst-Objects>)

target_compile_definitions(XrdEosFst-Static PUBLIC
  -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

target_link_libraries(XrdEosFst-Static PUBLIC
  XrdEosFst-Objects
  EosConsoleHelpers-Objects
  EosCommonServer
  EosFstIo
  EosNsCommon
  XROOTD::SSI)

install(TARGETS EosFstOss XrdEosFst-${XRDPLUGIN_SOVERSION}
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
add_executable(eos-rain-check tools/RainCheck.cc)
target_link_libraries(eos-rain-check PRIVATE EosFstIo-Static)
target_compile_definitions(eos-rain-check PRIVATE
  -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64)

add_executable(eos-rain-hd-dump  tools/RainHdrDump.cc)
target_link_libraries(eos-rain-hd-dump PRIVATE EosFstIo XROOTD::SERVER)

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
  filemd/FmdHandler.cc
  filemd/FmdMgm.cc
  Config.cc
  checksum/Adler.cc
  checksum/CheckSum.cc)

target_compile_definitions(eos-scan-fs PUBLIC -D_NOOFS=1)

add_executable(eos-adler32
  tools/Adler32.cc
  checksum/Adler.cc
  checksum/CheckSum.cc)

add_executable(eos-checksum
  tools/CheckSum.cc
  checksum/CheckSum.cc)

add_executable(eos-ioping tools/IoPing.c)
target_compile_options(eos-ioping PRIVATE -std=gnu99)
target_link_libraries(eos-ioping PRIVATE GLIBC::RT GLIBC::M)
target_link_libraries(eos-check-blockxs PRIVATE EosFstIo XROOTD::SERVER)
target_link_libraries(eos-compute-blockxs PRIVATE EosFstIo XROOTD::SERVER)
target_link_libraries(eos-adler32 PRIVATE EosFstIo XROOTD::SERVER)
target_link_libraries(eos-checksum PRIVATE EosFstIo XROOTD::SERVER)
target_link_libraries(eos-scan-fs PRIVATE
  EosFstIo EosCommonServer EosNsCommon-Static XROOTD::SERVER stdc++fs)

add_executable(eos-create-file-pattern
  utils/CreateFileWithPattern.cc)

add_executable(eos-readv-pattern
  utils/CheckFileReadWithPattern.cc)

target_link_libraries(eos-readv-pattern PRIVATE XROOTD::CL)

add_executable(eos-fmd-tool
  tools/ConvertFileMD.cc
  # File metadata interface
  filemd/FmdHandler.cc
  filemd/FmdAttr.cc
  filemd/FmdMgm.cc
  utils/FSPathHandler.cc)

target_link_libraries(eos-fmd-tool PRIVATE
  EosFstIo
  EosCommonServer
  XrdEosFst-Static)

add_executable(eos-disk-measurements
  utils/DiskMeasurementsMain.cc
  utils/DiskMeasurements.cc)

target_link_libraries(eos-disk-measurements PRIVATE EosCommon)

install(PROGRAMS
  tools/eosfstregister
  tools/eosfstinfo
  DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})

install(TARGETS
  eos-ioping eos-adler32 eos-checksum eos-rain-hd-dump
  eos-check-blockxs eos-compute-blockxs eos-scan-fs
  eos-create-file-pattern eos-readv-pattern eos-fmd-tool
  eos-rain-check
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})

endif()
