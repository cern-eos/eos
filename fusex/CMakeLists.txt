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
  ${CMAKE_CURRENT_SOURCE_DIR})

if (Linux)
  add_subdirectory(benchmark)
endif (Linux)

#-------------------------------------------------------------------------------
# Compile flags
#-------------------------------------------------------------------------------
set(EOSXD_COMPILE_FLAGS
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
  eosfuse.cc eosfuse.hh
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
  ZSTD::ZSTD
  ROCKSDB::ROCKSDB
  LIBEVENT::LIBEVENT
  OPENSSL::OPENSSL
  PROTOBUF::PROTOBUF
  JSONCPP::JSONCPP
  XROOTD::PRIVATE
  XROOTD::UTILS
  RICHACL::RICHACL
  GOOGLE::SPARSEHASH
  ZMQ::ZMQ)

if (FUSE3_FOUND)
  target_link_libraries(eosxd-objects PUBLIC FUSE3::FUSE3)
else()
  target_link_libraries(eosxd-objects PUBLIC FUSE::FUSE)
endif()

set_target_properties(eosxd-objects
  PROPERTIES COMPILE_FLAGS ${EOSXD_COMPILE_FLAGS})

#-------------------------------------------------------------------------------
# eosxd executables
#-------------------------------------------------------------------------------
add_executable(eosxd main.cc)

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

if (FUSE3_FOUND)
  add_executable(eosxd3 main.cc)

  target_link_libraries(eosxd3 PUBLIC
    eosxd-objects
    JEMALLOC::JEMALLOC
    ${EOSXD_COMMON_LINK_LIBRARIES})

  set_target_properties(eosxd3
    PROPERTIES COMPILE_FLAGS ${EOSXD_COMPILE_FLAGS})

  install(TARGETS eosxd3
    RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR})
endif()
