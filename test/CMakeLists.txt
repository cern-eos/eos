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
  ${CMAKE_BINARY_DIR}
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
add_executable(testhmacsha256
  TestHmacSha256.cc
  ${CMAKE_SOURCE_DIR}/common/SymKeys.hh
  ${CMAKE_SOURCE_DIR}/common/SymKeys.cc)
add_executable(eos-checksum-benchmark
  EosChecksumBenchmark.cc
  ${CMAKE_SOURCE_DIR}/fst/checksum/Adler.cc
  ${CMAKE_SOURCE_DIR}/fst/checksum/CheckSum.cc)

target_link_libraries(xrdcpabort PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcprandom PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcpextend PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcpappend PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcpholes PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcpbackward PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcpdownloadrandom PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcpshrink PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcptruncate PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcpposixcache PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcppartial PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcpupdate PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcpslowwriter PRIVATE XROOTD::CL)
target_link_libraries(eoshashbench PRIVATE EosCommon)
target_link_libraries(eos-crypto-timing-test PRIVATE EosCommon)
target_link_libraries(testhmacsha256 PRIVATE EosCommon)
target_link_libraries(eos-open-trunc-update PRIVATE XROOTD::CL XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(eos-io-tool PRIVATE EosFstIo XROOTD::SERVER)
target_link_libraries(xrdstress.exe PRIVATE
  UUID::UUID XROOTD::CL XROOTD::UTILS XROOTD::POSIX
  ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(eos-checksum-benchmark PRIVATE EosFstIo XROOTD::SERVER XROOTD::POSIX)
target_compile_definitions(xrdstress.exe PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcpabort PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcprandom PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcpextend PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcpappend PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcpholes PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcpbackward PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcpdownloadrandom PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcpshrink PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcptruncate PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcppartial PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcpupdate PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcpposixcache PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(eoshashbench PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(eos-checksum-benchmark PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_options(eos-checksum-benchmark PRIVATE -msse4.2)
target_compile_definitions(eos-open-trunc-update PUBLIC -D_FILE_OFFSET_BITS=64)

add_executable(threadpooltest ThreadPoolTest.cc)
target_link_libraries(threadpooltest PRIVATE EosCommon)

install(TARGETS xrdstress.exe xrdcpabort xrdcprandom xrdcpextend xrdcpshrink xrdcpappend
  xrdcptruncate xrdcpholes xrdcpbackward xrdcpdownloadrandom xrdcppartial xrdcpupdate
  xrdcpposixcache xrdcpslowwriter eos-checksum-benchmark eos-udp-dumper eos-mmap eos-io-tool
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})

install(PROGRAMS xrdstress eos-instance-test eos-instance-test-ci fuse/eos-fuse-test
  eos-rain-test eoscp-rain-test eos-io-test eos-oc-test eos-drain-test
  eos-http-upload-test eos-mq-tests eos-rename-test eos-grpc-test
  eos-fsck-test eos-token-test eos-backup eos-backup-browser eos-test-utils
  eos-converter-test
  DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
  PERMISSIONS OWNER_READ OWNER_EXECUTE
  GROUP_READ GROUP_EXECUTE
  WORLD_READ WORLD_EXECUTE)
