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
  ${SPARSEHASH_INCLUDE_DIRS}
  ${NCURSES_INCLUDE_DIRS}
  ${Z_INCLUDE_DIRS}
  ${XFS_INCLUDE_DIRS})

if (CPPUNIT_FOUND)
  add_subdirectory(tests)
endif(CPPUNIT_FOUND)

#-------------------------------------------------------------------------------
# Generate protocol buffer files used in several targets and make them visible
# at the global scope
#-------------------------------------------------------------------------------
PROTOBUF_GENERATE_CPP(FMDBASE_SRCS FMDBASE_HDRS PROTOFILES FmdBase.proto)
set_source_files_properties(
  ${FMDBASE_SRCS}
  ${FMDBASE_HDRS}
  PROPERTIES GENERATED 1)

set(FMDBASE_SRCS ${FMDBASE_SRCS} PARENT_SCOPE)
set(FMDBASE_HDRS ${FMDBASE_HDRS} PARENT_SCOPE)

#-------------------------------------------------------------------------------
# EosFstOss library
#-------------------------------------------------------------------------------

add_library(
  EosFstOss MODULE
  XrdFstOss.cc XrdFstOss.hh
  XrdFstOssFile.cc XrdFstOssFile.hh
  checksum/CheckSum.cc checksum/CheckSum.hh
  checksum/Adler.cc checksum/Adler.hh
  checksum/crc32c.cc checksum/crc32ctables.cc
  ${CMAKE_SOURCE_DIR}/common/LayoutId.hh)

target_link_libraries(
  EosFstOss
  eosCommon
  ${UUID_LIBRARIES}
  ${XROOTD_SERVER_LIBRARY})

set_target_properties(
  EosFstOss
  PROPERTIES
  VERSION ${VERSION}
  SOVERSION ${VERSION_MAJOR}
  INTERFACE_LINK_LIBRARIES "")


#-------------------------------------------------------------------------------
# EosFstIo library
#-------------------------------------------------------------------------------
set(
  EOSFSTIO_SRCS
  ${FMDBASE_SRCS}
  ${FMDBASE_HDRS}

  #-----------------------------------------------------------------------------
  # File IO interface
  #-----------------------------------------------------------------------------
  io/FileIo.hh
  io/LocalIo.cc                  io/LocalIo.hh
  io/XrdIo.cc                    io/XrdIo.hh
  io/HeaderCRC.cc                io/HeaderCRC.hh
  io/AsyncMetaHandler.cc         io/AsyncMetaHandler.hh
  io/ChunkHandler.cc             io/ChunkHandler.hh
  io/VectChunkHandler.cc         io/VectChunkHandler.hh
  io/SimpleHandler.cc            io/SimpleHandler.hh

  #-----------------------------------------------------------------------------
  # Checksum interface
  #-----------------------------------------------------------------------------
  checksum/CheckSum.cc           checksum/CheckSum.hh
  checksum/Adler.cc              checksum/Adler.hh
  checksum/crc32c.cc             checksum/crc32ctables.cc

  #-----------------------------------------------------------------------------
  # File layout interface
  #-----------------------------------------------------------------------------
  layout/LayoutPlugin.cc             layout/LayoutPlugin.hh
  layout/Layout.cc                   layout/Layout.hh
  layout/PlainLayout.cc              layout/PlainLayout.hh
  layout/ReplicaParLayout.cc         layout/ReplicaParLayout.hh
  layout/RaidMetaLayout.cc           layout/RaidMetaLayout.hh
  layout/RaidDpLayout.cc             layout/RaidDpLayout.hh
  layout/ReedSLayout.cc              layout/ReedSLayout.hh
  layout/jerasure/jerasure.cc        layout/jerasure/jerasure.hh
  layout/jerasure/reed_sol.cc        layout/jerasure/reed_sol.hh
  layout/jerasure/liberation.cc      layout/jerasure/liberation.hh
  layout/jerasure/galois.cc          layout/jerasure/galois.hh
  layout/jerasure/cauchy_best_r6.cc  layout/jerasure/cauchy.hh)

add_library(
  EosFstIo SHARED
  ${EOSFSTIO_SRCS}
  io/FileIoPlugin.hh  io/FileIoPlugin.cc)

target_link_libraries(
  EosFstIo
  eosCommon
  XrdEosFst
  ${Z_LIBRARY}
  ${UUID_LIBRARIES}
  ${ATTR_LIBRARIES}
  ${GLIBC_RT_LIBRARY}
  ${GLIBC_DL_LIBRARY}
  ${NCURSES_LIBRARIES}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT})

if(Linux)
  add_library(
    EosFstIo-Static STATIC
    ${EOSFSTIO_SRCS}
    io/FileIoPlugin.hh  io/FileIoPlugin.cc)

  target_link_libraries(
    EosFstIo-Static
    eosCommon-Static
    ${UUID_LIBRARIES}
    ${ATTR_LIBRARIES}
    ${Z_LIBRARY_STATIC}
    ${GLIBC_DL_LIBRARY}
    ${GLIBC_RT_LIBRARY}
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${NCURSES_LIBRARY_STATIC}
    ${OPENSSL_CRYPTO_LIBRARY_STATIC}
    ${CMAKE_THREAD_LIBS_INIT})
endif()

#-------------------------------------------------------------------------------
# XrdEosFst library
#-------------------------------------------------------------------------------
set(
  XRDEOSFST_SRCS
  Config.cc
  Load.cc
  ScanDir.cc
  Messaging.cc
  io/FileIoPlugin-Server.cc
  ${CMAKE_SOURCE_DIR}/common/LayoutId.hh

  #-----------------------------------------------------------------------------
  # OFS layer implementation
  #-----------------------------------------------------------------------------
  XrdFstOfs.cc                   XrdFstOfs.hh
  XrdFstOfsFile.cc               XrdFstOfsFile.hh

  #-----------------------------------------------------------------------------
  # Storage interface
  #-----------------------------------------------------------------------------
  storage/Balancer.cc
  storage/Cleaner.cc             storage/Comunicator.cc
  storage/Drainer.cc             storage/ErrorReport.cc
  storage/FileSystem.cc          storage/MgmSyncer.cc
  storage/Publish.cc             storage/Remover.cc
  storage/Report.cc              storage/Scrub.cc
  storage/Storage.cc             storage/Supervisor.cc
  storage/Trim.cc                storage/Verify.cc

  #-----------------------------------------------------------------------------
  # Transfer interface
  #-----------------------------------------------------------------------------
  txqueue/TransferMultiplexer.cc
  txqueue/TransferJob.cc
  txqueue/TransferQueue.cc

  #-----------------------------------------------------------------------------
  # File metadata interface
  #-----------------------------------------------------------------------------
  Fmd.cc               Fmd.hh
  FmdHandler.cc        FmdHandler.hh
  FmdSqlite.cc         FmdSqlite.hh
  FmdDbMap.cc          FmdDbMap.hh
  FmdClient.cc         FmdClient.hh
  ${FMDBASE_SRCS}
  ${FMDBASE_HDRS}

  #-----------------------------------------------------------------------------
  # HTTP interface
  #-----------------------------------------------------------------------------
  http/HttpServer.cc    http/HttpServer.hh
  http/HttpHandler.cc   http/HttpHandler.hh
  http/s3/S3Handler.cc  http/s3/S3Handler.hh

  #-----------------------------------------------------------------------------
  # EosFstIo interface
  #-----------------------------------------------------------------------------
  ${EOSFSTIO_SRCS})

add_library(XrdEosFst MODULE ${XRDEOSFST_SRCS})

target_link_libraries(
   XrdEosFst
   eosCommon
   eosCommonServer
   eosCapability-Static
   XrdMqClient-Static
   ${Z_LIBRARY}
   ${UUID_LIBRARIES}
   ${ATTR_LIBRARIES}
   ${GLIBC_RT_LIBRARY}
   ${GLIBC_DL_LIBRARY}
   ${PROTOBUF_LIBRARY}
   ${NCURSES_LIBRARIES}
   ${XROOTD_CL_LIBRARY}
   ${XOORTD_UTILS_LIBRARY}
   ${OPENSSL_CRYPTO_LIBRARY}
   ${CMAKE_THREAD_LIBS_INIT})

#-------------------------------------------------------------------------------
# eoscp executable 
#-------------------------------------------------------------------------------
add_executable(eoscp txqueue/eoscp.cc)
set_target_properties(eoscp PROPERTIES COMPILE_FLAGS -D_FILE_OFFSET_BITS=64)

add_executable(
  eos-check-blockxs
  tools/CheckBlockXS.cc
  checksum/Adler.cc
  checksum/CheckSum.cc
  checksum/crc32c.cc
  checksum/crc32ctables.cc)

add_executable(
  eos-compute-blockxs
  tools/ComputeBlockXS.cc
  checksum/Adler.cc
  checksum/CheckSum.cc
  checksum/crc32c.cc
  checksum/crc32ctables.cc)

add_executable(
  eos-scan-fs
  ScanDir.cc             Load.cc
  Fmd.cc                 FmdHandler.cc
  FmdSqlite.cc           FmdDbMap.cc
  FmdClient.cc           tools/ScanXS.cc
  checksum/Adler.cc      checksum/CheckSum.cc
  checksum/crc32c.cc     checksum/crc32ctables.cc
  ${FMDBASE_SRCS}
  ${FMDBASE_HDRS})

add_executable(
  eos-adler32
  tools/Adler32.cc
  checksum/Adler.cc
  checksum/CheckSum.cc
  checksum/crc32c.cc
  checksum/crc32ctables.cc)

add_executable(eos-ioping tools/IoPing.cc)
add_executable(FstLoad Load.cc tools/FstLoad.cc)
target_link_libraries(
  FstLoad
  ${GLIBC_RT_LIBRARY}
  ${GLIBC_DL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries(eos-check-blockxs eosCommon ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(eos-compute-blockxs eosCommon ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(eos-adler32 eosCommon ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries(
  eos-scan-fs
  eosCommon
  eosCommonServer
  ${GLIBC_RT_LIBRARY}
  ${XROOTD_CL_LIBRARY}
  ${PROTOBUF_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT})

if(MacOSX)
  target_link_libraries(eoscp EosFstIo ${XROOTD_CL_LIBRARY})

  set_target_properties(
    EosFstIo XrdEosFst
    PROPERTIES 
    MACOSX_RPATH TRUE)
else()
  target_link_libraries(eoscp EosFstIo-Static ${XROOT_CL_LIBRARY})
  set_target_properties(EosFstIo-Static PROPERTIES COMPILE_FLAGS "-fPIC")

  set_target_properties(
    XrdEosFst
    PROPERTIES
    VERSION ${VERSION}
    SOVERSION ${VERSION_MAJOR}
    INTERFACE_LINK_LIBRARIES "")
endif()

target_link_libraries(eos-ioping ${GLIBC_M_LIBRARY})

install(
  PROGRAMS
  tools/eosfstregister
  tools/eosfstinfo
  tools/eos-iobw
  tools/eos-iops
  DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})

install(
  TARGETS
  eos-ioping eos-adler32
  eos-check-blockxs eos-compute-blockxs eos-scan-fs
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})

if (MacOSX)
  install(
    TARGETS
    EosFstIo XrdEosFst eoscp
    LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
else()
  install(
    TARGETS
    EosFstOss XrdEosFst eoscp
    LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
endif()
