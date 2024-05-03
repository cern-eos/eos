# ------------------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Andreas-Joachim Peters - CERN
# ------------------------------------------------------------------------------

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
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/eosxd/ ${PROTOBUF3_INCLUDE_DIR} )

if (Linux)
  add_subdirectory(benchmark)
endif (Linux)

#-------------------------------------------------------------------------------
# Compile flags
#-------------------------------------------------------------------------------
set(EOSXD_COMPILE_FLAGS
  "-DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID} -DFUSE_MOUNT_VERSION=${FUSE_MOUNT_VERSION}0 -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -DVERSION=\\\"${VERSION}\\\""
)
set(EOSXD3_COMPILE_FLAGS
  "-DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID} -DFUSE_MOUNT_VERSION=${FUSE_MOUNT_VERSION}0 -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -DVERSION=\\\"${VERSION}\\\""
)

#-------------------------------------------------------------------------------
# Add tests directory
#-------------------------------------------------------------------------------
add_subdirectory(tests)

#-------------------------------------------------------------------------------
# Add auth subsystem
#-------------------------------------------------------------------------------
add_subdirectory(auth)

#-------------------------------------------------------------------------------
# Generate protocol buffer files
#-------------------------------------------------------------------------------
PROTOBUF_GENERATE_CPP(PROTO_SRC PROTO_HEADER fusex.proto)

set_source_files_properties(
  ${PROTO_SRC} ${PROTO_HEADER}
  PROPERTIES GENERATED 1)

set(PROTO_SRC ${PROTO_SRC} PARENT_SCOPE)
set(PROTO_HEADER ${PROTO_HEADER} PARENT_SCOPE)

#-------------------------------------------------------------------------------
# Link libraries
#-------------------------------------------------------------------------------
set(EOSXD_COMMON_LINK_LIBRARIES
  EosFuseAuth
  EosCommon
  XROOTD::CL
  XROOTD::UTILS
  BZ2::BZ2
  LIBBFD::LIBBFD)

#-------------------------------------------------------------------------------
# eosxd common objects, shared between tests and main code.
# An object library prevents having to compile them twice.
#-------------------------------------------------------------------------------
add_library(eosxd-objects OBJECT
  eosxd/eosfuse.cc eosxd/eosfuse.hh
  stat/Stat.cc stat/Stat.hh
  md/md.cc md/md.hh
  cap/cap.cc cap/cap.hh
  data/data.cc data/data.hh
  kv/RocksKV.cc kv/RocksKV.hh
  kv/NoKV.cc    kv/NoKV.hh
  kv/kv.hh
  misc/longstring.cc misc/longstring.hh
  misc/fusexrdlogin.cc misc/fusexrdlogin.hh
  misc/RunningPidScanner.cc misc/RunningPidScanner.hh
  misc/ConcurrentMount.cc misc/ConcurrentMount.hh
  data/cache.cc data/cache.hh data/bufferll.hh
  data/diskcache.cc data/diskcache.hh
  data/memorycache.cc data/memorycache.hh
  data/journalcache.cc data/journalcache.hh
  data/cachesyncer.cc data/cachesyncer.hh
  data/xrdclproxy.cc data/xrdclproxy.hh
  data/dircleaner.cc data/dircleaner.hh
  backend/backend.cc backend/backend.hh
  ${CMAKE_SOURCE_DIR}/common/ShellCmd.cc
  ${CMAKE_SOURCE_DIR}/common/ShellExecutor.cc
  submount/SubMount.cc submount/SubMount.hh
  ${PROTO_SRC} ${PROTO_HEADER})

target_link_libraries(eosxd-objects PUBLIC
  ROCKSDB::ROCKSDB
  LIBEVENT::LIBEVENT
  OpenSSL::SSL
  PROTOBUF::PROTOBUF
  JSONCPP::JSONCPP
  XROOTD::PRIVATE
  XROOTD::UTILS
  GOOGLE::SPARSEHASH
  ZMQ::ZMQ
  fmt::fmt-header-only)

if (FUSE3_FOUND)
#-------------------------------------------------------------------------------
# eosxd common objects, shared between tests and main code.
# An object library prevents having to compile them twice.
#-------------------------------------------------------------------------------
add_library(eosxd3-objects OBJECT
  eosxd/eosfuse.cc eosxd/eosfuse.hh
  stat/Stat.cc stat/Stat.hh
  md/md.cc md/md.hh
  cap/cap.cc cap/cap.hh
  data/data.cc data/data.hh
  kv/RocksKV.cc kv/RocksKV.hh
  kv/NoKV.cc    kv/NoKV.hh
  kv/kv.hh
  misc/longstring.cc misc/longstring.hh
  misc/fusexrdlogin.cc misc/fusexrdlogin.hh
  misc/RunningPidScanner.cc misc/RunningPidScanner.hh
  misc/ConcurrentMount.cc misc/ConcurrentMount.hh
  data/cache.cc data/cache.hh data/bufferll.hh
  data/diskcache.cc data/diskcache.hh
  data/memorycache.cc data/memorycache.hh
  data/journalcache.cc data/journalcache.hh
  data/cachesyncer.cc data/cachesyncer.hh
  data/xrdclproxy.cc data/xrdclproxy.hh
  data/dircleaner.cc data/dircleaner.hh
  backend/backend.cc backend/backend.hh
  ${CMAKE_SOURCE_DIR}/common/ShellCmd.cc
  ${CMAKE_SOURCE_DIR}/common/ShellExecutor.cc
  submount/SubMount.cc submount/SubMount.hh
  ${PROTO_SRC} ${PROTO_HEADER})

target_link_libraries(eosxd3-objects PUBLIC
  ROCKSDB::ROCKSDB
  LIBEVENT::LIBEVENT
  OpenSSL::SSL
  PROTOBUF::PROTOBUF
  JSONCPP::JSONCPP
  XROOTD::PRIVATE
  XROOTD::UTILS
  GOOGLE::SPARSEHASH
  ZMQ::ZMQ
  fmt::fmt-header-only)
endif()

if (FUSE3_FOUND)
  target_link_libraries(eosxd3-objects PUBLIC FUSE3::FUSE3)
  set_target_properties(eosxd3-objects
    PROPERTIES COMPILE_FLAGS ${EOSXD3_COMPILE_FLAGS})
endif()

target_link_libraries(eosxd-objects PUBLIC FUSE::FUSE)

set_target_properties(eosxd-objects
  PROPERTIES COMPILE_FLAGS ${EOSXD_COMPILE_FLAGS})

#-------------------------------------------------------------------------------
# eosxd executables
#-------------------------------------------------------------------------------
add_executable(eosxd eosxd/main.cc)

target_link_libraries(eosxd PRIVATE
  eosxd-objects
  ${EOSXD_COMMON_LINK_LIBRARIES})

if(Linux)
  target_link_libraries(eosxd PRIVATE JEMALLOC::JEMALLOC)
endif()

set_target_properties(eosxd
  PROPERTIES COMPILE_FLAGS ${EOSXD_COMPILE_FLAGS})

install(TARGETS eosxd
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR})

install(FILES eosfusebind
  DESTINATION ${CMAKE_INSTALL_FULL_BINDIR}
  PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
  GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ )

#-------------------------------------------------------------------------------
# eoscfsd executables
#-------------------------------------------------------------------------------

if (FUSE3_FOUND)
  set(EOSCFSD_COMPILE_FLAGS
    "-DFUSE_MOUNT_VERSION=${FUSE_MOUNT_VERSION}0 -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -DVERSION=\\\"${VERSION}\\\"")

  set(EOSCFSD_COMMON_LINK_LIBRARIES
    EosFuseAuth
    EosCommon
    XROOTD::CL
    XROOTD::UTILS
    PROTOBUF::PROTOBUF
    JSONCPP::JSONCPP
    LIBBFD::LIBBFD)

  add_library(eoscfsd-objects OBJECT
    eoscfsd/cfslogin.cc eoscfsd/cfslogin.hh
    eoscfsd/cfsrecycle.cc eoscfsd/cfsrecycle.hh
    stat/Stat.cc stat/Stat.hh
    ../common/ShellCmd.cc ../common/ShellCmd.hh
    ../common/ShellExecutor.cc ../common/ShellExecutor.hh)

  set_target_properties(eoscfsd-objects
    PROPERTIES COMPILE_FLAGS ${EOSCFSD_COMPILE_FLAGS})

  target_link_libraries(eoscfsd-objects PUBLIC
    FUSE3::FUSE3
    XROOTD::PRIVATE
    XROOTD::UTILS
    JSONCPP::JSONCPP
    fmt::fmt-header-only )

  add_executable(eoscfsd eoscfsd/eoscfsd.cc)
  add_executable(eosxd3 eosxd/main.cc)

  target_link_libraries(eoscfsd PUBLIC
    eoscfsd-objects
    JEMALLOC::JEMALLOC
    ${EOSCFSD_COMMON_LINK_LIBRARIES})

  set_target_properties(eoscfsd
    PROPERTIES COMPILE_FLAGS ${EOSCFSD_COMPILE_FLAGS})

  install(TARGETS eoscfsd
    RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR})

  target_link_libraries(eosxd3 PUBLIC
    eosxd3-objects
    JEMALLOC::JEMALLOC
    ${EOSXD_COMMON_LINK_LIBRARIES})

  set_target_properties(eosxd3
    PROPERTIES COMPILE_FLAGS ${EOSXD3_COMPILE_FLAGS})

  install(TARGETS eosxd3
    RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR})
endif()
