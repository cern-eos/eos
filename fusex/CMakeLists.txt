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
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_BINARY_DIR}
  ${XROOTD_INCLUDE_DIRS}
  ${XROOTD_PRIVATE_INCLUDE_DIR}
  ${FUSE_INCLUDE_DIRS}
  ${SPARSEHASH_INCLUDE_DIRS}
  ${KRB5_INCLUDE_DIR}
  ${OPENSSL_INCLUDE_DIR}
  ${PROTOBUF_INCLUDE_DIRS}
  ${JSONCPP_INCLUDE_DIR}
  ${HIREDIS_INCLUDE_DIR}
  ${ZMQ_INCLUDE_DIRS}
  ${LIBEVENT_INCLUDE_DIRS}
  ${BZ2_INCLUDE_DIRS}
  ${LIBBFD_INCLUDE_DIRS})

if (RICHACL_FOUND)
  include_directories(${RICHACL_INCLUDE_DIRS})
endif()

find_package(RocksDB)

if (ROCKSDB_FOUND)
  set(SOURCES_IF_ROCKSDB_WAS_FOUND kv/RocksKV.cc kv/RocksKV.hh)
  set(LIBRARIES_IF_ROCKSDB_WAS_FOUND ${ROCKSDB_LIBRARY} ${BZ2_LIBRARY})
  set(FLAGS_IF_ROCKSDB_WAS_FOUND "-DROCKSDB_FOUND")
  set(TEST_SOURCES_IF_ROCKSDB_WAS_FOUND rocks-kv.cc)
endif()

if (Linux)
  add_subdirectory(benchmark)
endif (Linux)

#-------------------------------------------------------------------------------
# Compile flags
#-------------------------------------------------------------------------------
set(EOSXD_COMPILE_FLAGS
  "-DDAEMONUID=${DAEMONUID} -DDAEMONGID=${DAEMONGID} -DFUSE_MOUNT_VERSION=${FUSE_MOUNT_VERSION}0 -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -DVERSION=\\\"${VERSION}\\\" ${FLAGS_IF_ROCKSDB_WAS_FOUND}"
)

#-------------------------------------------------------------------------------
# auth subsystem
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
  ${FUSE_LIBRARIES}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT}
  ${KRB5_LIBRARIES}
  ${OPENSSL_CRYPTO_LIBRARY}
  ${PROTOBUF_LIBRARY}
  ${JSONCPP_LIBRARIES}
  ${HIREDIS_LIBRARIES}
  ${LIBEVENT_LIBRARIES}
  ${ZMQ_LIBRARIES}
  ${LIBRARIES_IF_ROCKSDB_WAS_FOUND}
  ${BZ2_LIBRARY}
  ${LIBBFD_LIBRARIES}
  EosCommon)

if (RICHACL_FOUND)
  LIST(APPEND EOSXD_COMMON_LINK_LIBRARIES "${RICHACL_LIBRARIES}")
endif()

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
  ${SOURCES_IF_ROCKSDB_WAS_FOUND}
  kv/RedisKV.cc           kv/RedisKV.hh
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

set_target_properties(
  eosxd-objects
  PROPERTIES
  COMPILE_FLAGS ${EOSXD_COMPILE_FLAGS})

#-------------------------------------------------------------------------------
# eosxd common sources for all executables. We have to do this to combine
# object libraries EosFuseAuth and eosxd-objects, since apparently, one object
# library cannot nest another.
#-------------------------------------------------------------------------------
set(EOSXD_COMMON_SOURCES
  $<TARGET_OBJECTS:EosFuseAuth>
  $<TARGET_OBJECTS:eosxd-objects>)

#-------------------------------------------------------------------------------
# eosxd executables
#-------------------------------------------------------------------------------
add_executable(
  eosxd
  main.cc
  ${EOSXD_COMMON_SOURCES})

if(MacOSX)
  target_link_libraries(
    eosxd
    ${EOSXD_COMMON_LINK_LIBRARIES})

  set_target_properties(
    eosxd
    PROPERTIES
    COMPILE_FLAGS ${EOSXD_COMPILE_FLAGS})
else()
  target_link_libraries(
    eosxd
    ${EOSXD_COMMON_LINK_LIBRARIES}
    jemalloc)

  set_target_properties(
    eosxd
    PROPERTIES
    COMPILE_FLAGS ${EOSXD_COMPILE_FLAGS})
endif()

install(
  TARGETS eosxd
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR})

if (FUSE3a_FOUND)
  add_executable(
    eosxd3
    main.cc
    ${EOSXD_COMMON_SOURCES})

  target_link_libraries(
    eosxd3
    ${EOSXD_COMMON_LINK_LIBRARIES}
    jemalloc)

  set_target_properties(
    eosxd3
    PROPERTIES
    COMPILE_FLAGS ${EOSXD_COMPILE_FLAGS})

  install(
    TARGETS eosxd3
    RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR})
endif()

#-------------------------------------------------------------------------------
# Tests
#-------------------------------------------------------------------------------

add_subdirectory(tests)
