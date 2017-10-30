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
  ${ZMQ_INCLUDE_DIR}
  ${LIBEVENT_INCLUDE_DIRS}
  ./
)

find_package(RocksDB)

if(ROCKSDB_FOUND)
  set(SOURCES_IF_ROCKSDB_WAS_FOUND kv/RocksKV.cc kv/RocksKV.hh)
  set(LIBRARIES_IF_ROCKSDB_WAS_FOUND ${ROCKSDB_LIBRARY} bz2)
  set(FLAGS_IF_ROCKSDB_WAS_FOUND "-DROCKSDB_FOUND")
  set(TEST_SOURCES_IF_ROCKSDB_WAS_FOUND rocks-kv.cc)
endif()


if(CPPUNIT_FOUND)
  add_subdirectory(tests)
endif(CPPUNIT_FOUND)

add_subdirectory(benchmark)

#-------------------------------------------------------------------------------
# auth subsystem
#-------------------------------------------------------------------------------
add_subdirectory(auth)

#-------------------------------------------------------------------------------
# Generate protocol buffer files
#-------------------------------------------------------------------------------
PROTOBUF_GENERATE_CPP(PROTO_SRC PROTO_HEADER fusex.proto )

set_source_files_properties(
	${PROTO_SRC} ${PROTO_HEADER}
	PROPERTIES GENERATED 1
)

set(PROTO_SRC ${PROTO_SRC} PARENT_SCOPE)
set(PROTO_HEADER ${PROTO_HEADER} PARENT_SCOPE)

#-------------------------------------------------------------------------------
# eosxd executables
#-------------------------------------------------------------------------------
add_executable(
  eosxd
  main.cc
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
  data/cache.cc data/cache.hh data/bufferll.hh
  data/diskcache.cc data/diskcache.hh
  data/memorycache.cc data/memorycache.hh
  data/journalcache.cc data/journalcache.hh
  data/cachesyncer.cc data/cachesyncer.hh
  data/xrdclproxy.cc data/xrdclproxy.hh
  data/dircleaner.cc data/dircleaner.hh
  backend/backend.cc backend/backend.hh
  $<TARGET_OBJECTS:EosFuseAuth>
  ${PROTO_SRC} ${PROTO_HEADER}
)

if(MacOSX)
  target_link_libraries(
    eosxd
    ${UUID_LIBRARIES}
    ${FUSE_LIBRARIES}
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UNTILS_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT}
    ${KRB5_LIBRARIES}
    ${OPENSSL_CRYPTO_LIBRARY}
    ${PROTOBUF_LIBRARY}
    ${JSONCPP_LIBRARIES}
    ${HIREDIS_LIBRARIES}
    ${LIBEVENT_LIBRARIES}
    ${ZMQ_LIBRARIES}
    ${LIBRARIES_IF_ROCKSDB_WAS_FOUND}
    bz2
    eosCommon
    )

  set_target_properties(
    eosxd
    PROPERTIES
  COMPILE_FLAGS "-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -DVERSION=\\\"${VERSION}\\\" ${FLAGS_IF_ROCKSDB_WAS_FOUND}")
else()

  target_link_libraries(
    eosxd
    ${FUSE_LIBRARY}
    ${UUID_LIBRARIES}
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
    eosCommon
    jemalloc
)

  set_target_properties(
    eosxd
    PROPERTIES
  COMPILE_FLAGS "-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -DVERSION=\\\"${VERSION}\\\" ${FLAGS_IF_ROCKSDB_WAS_FOUND}")
endif()

install(
  TARGETS eosxd
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR})


if (FUSE3_FOUND)
add_executable(
  eosxd3
  main.cc
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
  data/cache.cc data/cache.hh data/bufferll.hh
  data/diskcache.cc data/diskcache.hh
  data/memorycache.cc data/memorycache.hh
  data/journalcache.cc data/journalcache.hh
  data/cachesyncer.cc data/cachesyncer.hh
  data/xrdclproxy.cc data/xrdclproxy.hh
  data/dircleaner.cc data/dircleaner.hh
  backend/backend.cc backend/backend.hh
  $<TARGET_OBJECTS:EosFuseAuth>
  ${PROTO_SRC} ${PROTO_HEADER}
  )

  target_link_libraries(
    eosxd3
    ${FUSE3_LIBRARY}
    ${UUID_LIBRARIES}
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
    eosCommon
    jemalloc)

  set_target_properties(
    eosxd3
    PROPERTIES
  COMPILE_FLAGS "-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FUSE3 -D_FILE_OFFSET_BITS=64 -DVERSION=\\\"${VERSION}\\\" ${FLAGS_IF_ROCKSDB_WAS_FOUND}")

install(
  TARGETS eosxd3
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR})

endif()

#-------------------------------------------------------------------------------
# eosxd library
#-------------------------------------------------------------------------------
add_library(
  eosxd-test
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
  data/cache.cc data/cache.hh data/bufferll.hh
  data/diskcache.cc data/diskcache.hh
  data/memorycache.cc data/memorycache.hh
  data/journalcache.cc data/journalcache.hh
  data/cachesyncer.cc data/cachesyncer.hh
  data/xrdclproxy.cc data/xrdclproxy.hh
  data/dircleaner.cc data/dircleaner.hh
  backend/backend.cc backend/backend.hh
  $<TARGET_OBJECTS:EosFuseAuth>
  ${PROTO_SRC} ${PROTO_HEADER}
)


  target_link_libraries(
    eosxd-test
    ${FUSE_LIBRARY}
    ${UUID_LIBRARIES}
    ${XROOTD_CL_LIBRARY}
    ${XROOTD_UTILS_LIBRARY}
    ${CMAKE_THREAD_LIBS_INIT}
    ${KRB5_LIBRARIES}
    ${OPENSSL_CRYPTO_LIBRARY}
    ${PROTOBUF_LIBRARY}
    ${JSONCPP_LIBRARIES}
    ${HIREDIS_LIBRARIES}
    ${LIBEVENT_LIBRARIES}
    ${LIBRARIES_IF_ROCKSDB_WAS_FOUND}
    eosCommon
    jemalloc
)

  set_target_properties(
    eosxd-test
    PROPERTIES
  COMPILE_FLAGS "-D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -DVERSION=\\\"${VERSION}\\\" -fPIC ${FLAGS_IF_ROCKSDB_WAS_FOUND}")
