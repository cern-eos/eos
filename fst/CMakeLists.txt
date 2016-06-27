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

if(KINETICIO_FOUND)
    set(KINETICIO_SRC "io/kinetic/KineticIo.cc")
    set(KINETICIO_HDR "io/kinetic/KineticIo.hh")
else()
    set(KINETICIO_SRC "")
    set(KINETICIO_HDR "")
endif()

if(DAVIX_FOUND)
    add_definitions(-DDAVIX_FOUND)
    set(DAVIX_SRC "io/davix/DavixIo.cc")
    set(DAVIX_HDR "io/davix/DavixIo.hh")
else()
    set(DAVIX_INCLUDE_DIR "")
    set(DAVIX_LIBRARIES "")
    set(DAVIX_SRC "")
    set(DAVIX_HDR "")
endif()

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_BINARY_DIR}
  ${PROTOBUF_INCLUDE_DIRS}
  ${XROOTD_INCLUDE_DIRS}
  ${SPARSEHASH_INCLUDE_DIRS}
  ${NCURSES_INCLUDE_DIRS}
  ${OPENSSL_INCLUDE_DIRS}
  ${Z_INCLUDE_DIRS}
  ${XFS_INCLUDE_DIRS}
  ${KINETICIO_INCLUDE_DIR}
  ${JSONC_INCLUDE_DIR}
  ${DAVIX_INCLUDE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/layout/gf-complete/include
  ${CMAKE_CURRENT_SOURCE_DIR}/layout/jerasure/include)

#-------------------------------------------------------------------------------
# Add CppUnit tests if possible
#-------------------------------------------------------------------------------
if(CPPUNIT_FOUND)
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
# gf-complete static library
#-------------------------------------------------------------------------------
set(
  GFCOMPLETE_SRCS
  ./layout/gf-complete/src/gf.c
  ./layout/gf-complete/src/gf_w128.c
  ./layout/gf-complete/src/gf_rand.c
  ./layout/gf-complete/src/gf_method.c
  ./layout/gf-complete/src/gf_general.c
  ./layout/gf-complete/src/gf_w16.c
  ./layout/gf-complete/src/gf_w32.c
  ./layout/gf-complete/src/gf_w8.c
  ./layout/gf-complete/src/gf_w64.c
  ./layout/gf-complete/src/gf_w4.c
  ./layout/gf-complete/src/gf_wgen.c)

set(
  GFCOMPLETE_HDRS
  ./layout/gf-complete/include/gf_complete.h
  ./layout/gf-complete/include/gf_rand.h
  ./layout/gf-complete/include/gf_method.h
  ./layout/gf-complete/include/gf_int.h
  ./layout/gf-complete/include/gf_general.h
  ./layout/gf-complete/include/gf_w8.h
  ./layout/gf-complete/include/gf_w64.h
  ./layout/gf-complete/include/gf_w4.h
  ./layout/gf-complete/include/gf_w32.h
  ./layout/gf-complete/include/gf_w16.h)

add_library(
  gf-complete STATIC
  ${GFCOMPLETE_SRCS}
  ${GFCOMPLETE_HDRS})

target_compile_definitions(
  gf-complete
  PRIVATE -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64)

target_compile_options(gf-complete PUBLIC -fPIC)

#-------------------------------------------------------------------------------
# jerasure 2.0 static library
#-------------------------------------------------------------------------------
set(
  JERASURE_SRCS
  layout/jerasure/src/cauchy_best_r6.c
  layout/jerasure/src/cauchy.c
  layout/jerasure/src/galois.c
  layout/jerasure/src/jerasure.c
  layout/jerasure/src/liberation.c
  layout/jerasure/src/reed_sol.c
  layout/jerasure/src/timing.c)

set(
  JERASURE_HDRS
  layout/jerasure/include/cauchy.h
  layout/jerasure/include/galois.h
  layout/jerasure/include/jerasure.h
  layout/jerasure/include/liberation.h
  layout/jerasure/include/reed_sol.h
  layout/jerasure/include/timing.h)

add_library(
  jerasure STATIC
  ${JERASURE_SRCS}
  ${JERASURE_HDRS})

target_compile_definitions(
  jerasure
  PRIVATE -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64)

target_compile_options(
  jerasure PUBLIC -fPIC)

target_link_libraries(jerasure gf-complete)

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
  io/local/LocalIo.cc	         io/local/LocalIo.hh
  io/local/FsIo.cc               io/local/FsIo.hh
  ${KINETICIO_SRC}               ${KINETICIO_HDR}
  ${DAVIX_SRC}		         ${DAVIX_HDR}
  io/rados/RadosIo.cc            io/rados/RadosIo.hh
  io/xrd/XrdIo.cc                io/xrd/XrdIo.hh
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
  layout/ReedSLayout.cc              layout/ReedSLayout.hh)

add_library(
  EosFstIo SHARED
  ${EOSFSTIO_SRCS}
  io/FileIoPlugin.hh  io/FileIoPlugin.cc)

target_link_libraries(
  EosFstIo
  eosCommon
  jerasure
  ${Z_LIBRARY}
  ${UUID_LIBRARIES}
  ${ATTR_LIBRARIES}
  ${GLIBC_RT_LIBRARY}
  ${GLIBC_DL_LIBRARY}
  ${NCURSES_LIBRARIES}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${XROOTD_SERVER_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY}
  ${JSONC_LIBRARIES}
  ${KINETICIO_LIBRARIES}
  ${DAVIX_LIBRARIES}			
  ${CMAKE_THREAD_LIBS_INIT}
  ${CMAKE_THREAD_LIBS_INIT}
  ${PROTOBUF_LIBRARIES})

#-------------------------------------------------------------------------------
# eoscp executable
#-------------------------------------------------------------------------------
add_executable(eoscp txqueue/eoscp.cc)

set_target_properties(
  eoscp
  PROPERTIES
  COMPILE_FLAGS "-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64")

if(MacOSX)
  set_target_properties(
    EosFstIo
    PROPERTIES
    VERSION ${VERSION}
    SOVERSION ${VERSION_MAJOR}
    MACOSX_RPATH TRUE)

  target_link_libraries(eoscp EosFstIo ${XROOTD_CL_LIBRARY})

  install(
    TARGETS EosFstIo
    LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
else()
  add_library(
    EosFstIo-Static STATIC
    ${EOSFSTIO_SRCS}
    io/FileIoPlugin.hh  io/FileIoPlugin.cc)

  target_link_libraries(
    EosFstIo-Static
    eosCommon-Static
    jerasure
    ${UUID_LIBRARIES}
    ${ATTR_LIBRARIES}
    ${Z_LIBRARY_STATIC}
    ${GLIBC_DL_LIBRARY}
    ${GLIBC_RT_LIBRARY}
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${NCURSES_LIBRARY_STATIC}
    ${OPENSSL_CRYPTO_LIBRARY_STATIC}
    ${JSONC_LIBRARIES}
    ${KINETICIO_LIBRARIES}
    ${DAVIX_LIBRARIES}			
    ${CMAKE_THREAD_LIBS_INIT})

  set_target_properties(
    EosFstIo-Static
    PROPERTIES
    COMPILE_FLAGS "-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -fPIC")
  target_link_libraries(eoscp EosFstIo-Static)
endif()

install(
  TARGETS eoscp
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

if(NOT CLIENT)
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

target_compile_definitions(
  EosFstOss PUBLIC -DHAVE_ATOMICS=1)

target_link_libraries(
  EosFstOss
  EosFstIo
  eosCommon
  ${UUID_LIBRARIES}
  ${XROOTD_SERVER_LIBRARY})

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
  XrdFstOfs.cc                   XrdFstOfs.hh
  XrdFstOfsFile.cc               XrdFstOfsFile.hh
  storage/Balancer.cc
  storage/Cleaner.cc             storage/Comunicator.cc
  storage/Drainer.cc             storage/ErrorReport.cc
  storage/FileSystem.cc          storage/MgmSyncer.cc
  storage/Publish.cc             storage/Remover.cc
  storage/Report.cc              storage/Scrub.cc
  storage/Storage.cc             storage/Supervisor.cc
  storage/Trim.cc                storage/Verify.cc
  txqueue/TransferMultiplexer.cc
  txqueue/TransferJob.cc
  txqueue/TransferQueue.cc
  Fmd.cc               Fmd.hh
  FmdHandler.cc        FmdHandler.hh
  FmdDbMap.cc          FmdDbMap.hh
  FmdClient.cc         FmdClient.hh
  ${FMDBASE_SRCS}
  ${FMDBASE_HDRS}
  http/HttpServer.cc    http/HttpServer.hh
  http/HttpHandler.cc   http/HttpHandler.hh
  http/s3/S3Handler.cc  http/s3/S3Handler.hh
  io/FileIoPlugin-Server.cc
  ${EOSFSTIO_SRCS_SERVER})

add_library(XrdEosFst MODULE 
            ${XRDEOSFST_SRCS})

target_compile_definitions(
  XrdEosFst PUBLIC -DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID})

target_link_libraries(
   XrdEosFst
   EosFstIo
   eosCommon
   eosCommonServer
   EosFstIo-Static
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

install(
  TARGETS
  EosFstOss XrdEosFst
  LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})

#-------------------------------------------------------------------------------
# Other executables
#-------------------------------------------------------------------------------
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
  FmdDbMap.cc
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

set_target_properties(eos-scan-fs PROPERTIES COMPILE_FLAGS -D_NOOFS=1)

add_executable(eos-ioping tools/IoPing.cc)
add_executable(FstLoad Load.cc tools/FstLoad.cc)
target_link_libraries(
  FstLoad
  ${GLIBC_RT_LIBRARY}
  ${GLIBC_DL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries(eos-check-blockxs  EosFstIo-Static eosCommon ${CMAKE_THREAD_LIBS_INIT} )
target_link_libraries(eos-compute-blockxs  EosFstIo-Static eosCommon  ${CMAKE_THREAD_LIBS_INIT} )
target_link_libraries(eos-adler32  EosFstIo-Static eosCommon ${CMAKE_THREAD_LIBS_INIT} )

target_link_libraries(
  eos-scan-fs
  eosCommon
  eosCommonServer
  EosFstIo-Static
  ${GLIBC_RT_LIBRARY}
  ${XROOTD_CL_LIBRARY}
  ${KINETICIO_LIBRARIES} 
  ${DAVIX_LIBRARIES}
  ${PROTOBUF_LIBRARIES}
  ${CMAKE_THREAD_LIBS_INIT})

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

endif()

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
    EosFstIo EosFstOss XrdEosFst eoscp
    LIBRARY DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_FULL_LIBDIR})
endif()
