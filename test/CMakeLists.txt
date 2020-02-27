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
  ${FOLLY_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/common/ulib/
  ${KINETIC_INCLUDE_DIR}
  ${OPENSSL_INCLUDE_DIRS}
  ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/
  ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/qclient/include
  ${CMAKE_BINARY_DIR}/namespace/ns_quarkdb)  # for the generated protobuf

add_subdirectory(benchmark)

add_executable(xrdstress.exe XrdStress.cc XrdStress.hh)
add_executable(xrdcpabort XrdCpAbort.cc)
add_executable(xrdcprandom XrdCpRandom.cc)
add_executable(xrdcpextend XrdCpExtend.cc)
add_executable(xrdcpappend XrdCpAppend.cc)
add_executable(xrdcpposixcache XrdCpPosixCache.cc)
add_executable(xrdcpholes XrdCpHoles.cc)
add_executable(xrdcpbackward XrdCpBackward.cc)
add_executable(xrdcpdownloadrandom XrdCpDownloadRandom)
add_executable(xrdcpshrink XrdCpShrink.cc)
add_executable(xrdcptruncate XrdCpTruncate.cc)
add_executable(xrdcppartial XrdCpPartial.cc)
add_executable(xrdcpupdate XrdCpUpdate.cc)
add_executable(xrdcpslowwriter XrdCpSlowWriter.cc)
add_executable(eos-udp-dumper EosUdpDumper.cc)
add_executable(eos-mmap EosMmap.cc)
add_executable(eoshashbench EosHashBenchmark.cc)
add_executable(eos-io-tool eos_io_tool.cc)
add_executable(eos-open-trunc-update EosOpenTruncUpdate.cc)
add_executable(eos-crypto-timing-test EosCryptoTimingTest.cc)

add_executable(
  testhmacsha256
  TestHmacSha256.cc
  ${CMAKE_SOURCE_DIR}/common/SymKeys.hh
  ${CMAKE_SOURCE_DIR}/common/SymKeys.cc)

add_executable(
  eoschecksumbench
  EosChecksumBenchmark.cc
  ${CMAKE_SOURCE_DIR}/fst/checksum/Adler.cc
  ${CMAKE_SOURCE_DIR}/fst/checksum/CheckSum.cc)

target_link_libraries(xrdcpabort ${XROOTD_POSIX_LIBRARY} ${XROOTD_UTILS_LIBRARY})
target_link_libraries(xrdcprandom ${XROOTD_POSIX_LIBRARY} ${XROOTD_UTILS_LIBRARY})
target_link_libraries(xrdcpextend ${XROOTD_POSIX_LIBRARY} ${XROOTD_UTILS_LIBRARY})
target_link_libraries(xrdcpappend ${XROOTD_POSIX_LIBRARY} ${XROOTD_UTILS_LIBRARY})
target_link_libraries(xrdcpholes ${XROOTD_POSIX_LIBRARY} ${XROOTD_UTILS_LIBRARY})
target_link_libraries(xrdcpbackward ${XROOTD_POSIX_LIBRARY} ${XROOTD_UTILS_LIBRARY})
target_link_libraries(xrdcpdownloadrandom ${XROOTD_POSIX_LIBRARY} ${XROOTD_UTILS_LIBRARY})
target_link_libraries(xrdcpshrink ${XROOTD_POSIX_LIBRARY} ${XROOTD_UTILS_LIBRARY})
target_link_libraries(xrdcptruncate ${XROOTD_POSIX_LIBRARY} ${XROOTD_UTILS_LIBRARY})
target_link_libraries(xrdcpposixcache ${XROOTD_POSIX_LIBRARY} ${XROOTD_UTILS_LIBRARY})
target_link_libraries(xrdcppartial ${XROOTD_POSIX_LIBRARY} ${XROOTD_UTILS_LIBRARY})
target_link_libraries(xrdcpupdate ${XROOTD_POSIX_LIBRARY} ${XROOTD_UTILS_LIBRARY})
target_link_libraries(xrdcpslowwriter ${XROOTD_CL_LIBRARY})
target_link_libraries(eoshashbench EosCommon ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(testhmacsha256 EosCommon ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(eos-open-trunc-update ${XROOTD_CL_LIBRARY} ${XROOTD_POSIX_LIBRARY} ${XROOTD_UTILS_LIBRARY})
target_link_libraries(eos-crypto-timing-test PRIVATE EosCommon)
target_link_libraries(eos-udp-dumper)

target_link_libraries(
  eos-io-tool
  EosFstIo
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_SERVER_LIBRARY}
  ${PROTOBUF_LIBRARY}
  ${KINETIC_LIBRARIES})

target_link_libraries(
  xrdstress.exe
  ${UUID_LIBRARIES}
  ${XROOTD_CL_LIBRARY}
  ${XROOTD_POSIX_LIBRARY}
  ${XROOTD_UTILS_LIBRARY}
  ${CMAKE_THREAD_LIBS_INIT})

target_link_libraries(
  eoschecksumbench
  EosFstIo
  ${XROOTD_POSIX_LIBRARY})

set_target_properties(xrdstress.exe PROPERTIES COMPILE_FLAGS "-std=gnu++0x -D_FILE_OFFSET_BITS=64")
set_target_properties(xrdcpabort PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64")
set_target_properties(xrdcprandom PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64")
set_target_properties(xrdcpextend PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64")
set_target_properties(xrdcpappend PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64")
set_target_properties(xrdcpholes PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64")
set_target_properties(xrdcpbackward PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64")
set_target_properties(xrdcpdownloadrandom PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64")
set_target_properties(xrdcpshrink PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64")
set_target_properties(xrdcptruncate PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64")
set_target_properties(xrdcppartial PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64")
set_target_properties(xrdcpupdate PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64")
set_target_properties(xrdcpposixcache PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64")
set_target_properties(eoshashbench PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64")
set_target_properties(eoschecksumbench PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64 -msse4.2")
set_target_properties(eos-open-trunc-update PROPERTIES COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64")

install(
  TARGETS xrdstress.exe xrdcpabort xrdcprandom xrdcpextend xrdcpshrink xrdcpappend
          xrdcptruncate xrdcpholes xrdcpbackward xrdcpdownloadrandom xrdcppartial xrdcpupdate
          xrdcpposixcache xrdcpslowwriter eoschecksumbench eos-udp-dumper eos-mmap eos-io-tool
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})

install(
  PROGRAMS xrdstress eos-instance-test eos-instance-test-ci fuse/eos-fuse-test
           eos-rain-test eoscp-rain-test eos-io-test eos-oc-test eos-drain-test
           eos-http-upload-test eos-mq-tests eos-rename-test eos-grpc-test eos-fsck-test eos-token-test
	       eos-backup eos-backup-browser
  DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
  PERMISSIONS OWNER_READ OWNER_EXECUTE
              GROUP_READ GROUP_EXECUTE
              WORLD_READ WORLD_EXECUTE)

add_executable(threadpooltest ThreadPoolTest.cc)
target_link_libraries(threadpooltest PRIVATE EosCommon)
