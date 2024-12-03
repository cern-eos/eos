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
  ${CMAKE_SOURCE_DIR}/common/
  ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/
  ${CMAKE_SOURCE_DIR}/namespace/ns_quarkdb/qclient/include
  ${CMAKE_BINARY_DIR}/namespace/ns_quarkdb)  # for the generated protobuf

add_subdirectory(benchmark)
add_subdirectory(microbenchmarks)

add_executable(xrdstress.exe XrdStress.cc)
set_target_properties(xrdstress.exe PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(xrdcpabort XrdCpAbort.cc)
set_target_properties(xrdcpabort PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(xrdcprandom XrdCpRandom.cc)
set_target_properties(xrdcprandom PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(xrdcppgread XrdCpPgRead.cc)
set_target_properties(xrdcppgread PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(xrdcpextend XrdCpExtend.cc)
set_target_properties(xrdcpextend PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(xrdcpappend XrdCpAppend.cc)
set_target_properties(xrdcpappend PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(xrdcpappendoverlap XrdCpAppendOverlap.cc)
set_target_properties(xrdcpappendoverlap PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(xrdcpposixcache XrdCpPosixCache.cc)
set_target_properties(xrdcpposixcache PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(xrdcpholes XrdCpHoles.cc)
set_target_properties(xrdcpholes PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(xrdcpbackward XrdCpBackward.cc)
set_target_properties(xrdcpbackward PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(xrdcpdownloadrandom XrdCpDownloadRandom.cc)
set_target_properties(xrdcpdownloadrandom PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(xrdcpshrink XrdCpShrink.cc)
set_target_properties(xrdcpshrink PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(xrdcptruncate XrdCpTruncate.cc)
set_target_properties(xrdcptruncate PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(xrdcppartial XrdCpPartial.cc)
set_target_properties(xrdcppartial PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(xrdcpupdate XrdCpUpdate.cc)
set_target_properties(xrdcpupdate PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(xrdcpslowwriter XrdCpSlowWriter.cc)
set_target_properties(xrdcpslowwriter PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(xrdcpnonstreaming XrdCpNonStreaming.cc)
set_target_properties(xrdcpnonstreaming PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(eos-udp-dumper EosUdpDumper.cc)
set_target_properties(eos-udp-dumper PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(eos-mmap EosMmap.cc)
set_target_properties(eos-mmap PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(eoshashbench EosHashBenchmark.cc)
set_target_properties(eoshashbench PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(eoslogbench EosLoggingBenchmark.cc)
set_target_properties(eoslogbench PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(eos-io-tool eos_io_tool.cc)
set_target_properties(eos-io-tool PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(eos-open-trunc-update EosOpenTruncUpdate.cc)
set_target_properties(eos-open-trunc-update PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(eos-crypto-timing-test EosCryptoTimingTest.cc)
set_target_properties(eos-crypto-timing-test PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(testhmacsha256
  TestHmacSha256.cc
  ${CMAKE_SOURCE_DIR}/common/SymKeys.hh
  ${CMAKE_SOURCE_DIR}/common/SymKeys.cc)
set_target_properties(testhmacsha256 PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(eos-checksum-benchmark
  EosChecksumBenchmark.cc
  ${CMAKE_SOURCE_DIR}/fst/checksum/Adler.cc
  ${CMAKE_SOURCE_DIR}/fst/checksum/CheckSum.cc)
set_target_properties(eos-checksum-benchmark PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(threadpooltest ThreadPoolTest.cc)
set_target_properties(threadpooltest PROPERTIES POSITION_INDEPENDENT_CODE TRUE)
add_executable(eos-idmap-benchmark EosIdMapBenchmark.cc)

target_link_libraries(xrdcpabort PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcprandom PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcppgread PRIVATE XROOTD::CL XROOTD::UTILS)
target_link_libraries(xrdcpextend PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcpappend PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcpappendoverlap PRIVATE XROOTD::POSIX XROOTD::UTILS XROOTD::CL)
target_link_libraries(xrdcpholes PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcpbackward PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcpdownloadrandom PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcpshrink PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcptruncate PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcpposixcache PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcppartial PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcpupdate PRIVATE XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(xrdcpslowwriter PRIVATE XROOTD::CL)
target_link_libraries(xrdcpnonstreaming PRIVATE XROOTD::CL)
target_link_libraries(eoshashbench PRIVATE EosCommon)
target_link_libraries(eoslogbench PRIVATE EosCommon)
target_link_libraries(eos-crypto-timing-test PRIVATE EosCommon)
target_link_libraries(testhmacsha256 PRIVATE EosCommon)
target_link_libraries(eos-open-trunc-update PRIVATE XROOTD::CL XROOTD::POSIX XROOTD::UTILS)
target_link_libraries(eos-io-tool PRIVATE EosFstIo XROOTD::SERVER)
target_link_libraries(xrdstress.exe PRIVATE
  UUID::UUID XROOTD::CL XROOTD::UTILS XROOTD::POSIX
  ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(eos-checksum-benchmark PRIVATE EosFstIo XROOTD::SERVER XROOTD::POSIX)
target_link_libraries(eos-idmap-benchmark PRIVATE EosCommon)
target_compile_definitions(xrdstress.exe PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcpabort PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcprandom PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcppgread PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcpextend PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcpappend PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcpappendoverlap PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcpholes PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcpbackward PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcpdownloadrandom PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcpshrink PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcptruncate PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcppartial PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcpupdate PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(xrdcpposixcache PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(eoshashbench PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(eoslogbench PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_definitions(eos-checksum-benchmark PUBLIC -D_FILE_OFFSET_BITS=64)
target_compile_options(eos-checksum-benchmark PRIVATE ${CPU_ARCH_FLAGS})
target_compile_definitions(eos-open-trunc-update PUBLIC -D_FILE_OFFSET_BITS=64)
target_link_libraries(threadpooltest PRIVATE EosCommon)

#-------------------------------------------------------------------------------
# MQ test related executables
#-------------------------------------------------------------------------------
if (NOT CLIENT)
  add_executable(eos-shared-hash-test mq/SharedHashLoadTest.cc)
  add_executable(xrdmqclienttest mq/XrdMqClientTest.cc)
  add_executable(eos-mq-dumper mq/XrdMqQueueDumper.cc)
  add_executable(eos-mq-feeder mq/XrdMqQueueFeeder.cc)
  add_executable(xrdmqinjection mq/XrdMqQueueInjection.cc)
  add_executable(xrdmqclientmaster mq/XrdMqClientMaster.cc)
  add_executable(xrdmqclientworker mq/XrdMqClientWorker.cc)
  add_executable(xrdmqsharedobjectclient mq/XrdMqSharedObjectClient.cc)
  add_executable(xrdmqsharedobjectqueueclient mq/XrdMqSharedObjectQueueClient.cc)
  add_executable(xrdmqsharedobjectbroadcastclient mq/XrdMqSharedObjectBroadCastClient.cc)
  target_link_libraries(eos-shared-hash-test PRIVATE XrdMqClient-Static qclient)
  target_link_libraries(xrdmqclienttest PRIVATE XrdMqClient-Static)
  target_link_libraries(eos-mq-dumper PRIVATE XrdMqClient-Static)
  target_link_libraries(eos-mq-feeder PRIVATE XrdMqClient-Static)
  target_link_libraries(xrdmqinjection PRIVATE XrdMqClient-Static)
  target_link_libraries(xrdmqclientmaster PRIVATE XrdMqClient-Static)
  target_link_libraries(xrdmqclientworker PRIVATE XrdMqClient-Static)
  target_link_libraries(xrdmqsharedobjectclient PRIVATE XrdMqClient-Static)
  target_link_libraries(xrdmqsharedobjectqueueclient PRIVATE XrdMqClient-Static)
  target_link_libraries(xrdmqsharedobjectbroadcastclient PRIVATE XrdMqClient-Static)

  install(TARGETS eos-mq-feeder eos-mq-dumper eos-shared-hash-test
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_BINDIR})
endif()

install(TARGETS xrdstress.exe xrdcpabort xrdcprandom xrdcppgread
  xrdcpextend xrdcpshrink xrdcpappend xrdcpappendoverlap xrdcptruncate
  xrdcpholes xrdcpbackward xrdcpdownloadrandom xrdcppartial xrdcpupdate
  xrdcpposixcache xrdcpslowwriter xrdcpnonstreaming eos-checksum-benchmark
  eos-udp-dumper eos-mmap eos-io-tool
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})

install(PROGRAMS xrdstress eos-instance-test eos-instance-test-ci
  fuse/eos-fuse-test eos-rain-test eoscp-rain-test eos-io-test eos-accounting-test
  eos-oc-test eos-drain-test eos-groupdrain-test eos-mq-tests
  eos-http-upload-test eos-https-functional-test eos-token-test
  eos-fst-close-test eos-rename-test eos-grpc-test eos-fsck-test
  eos-squash-test eos-backup eos-backup-browser eos-test-utils
  eos-convert-test eos-balance-test eos-qos-test eos-timestamp-test
  eos-squash-test eos-defaultcc-test eos-quota-test eos-macaroon-init
  eos-synctime-test eos-rclone-test eos-bash test-eos-iam-mapfile.py
  fusex/eos-test-credential-bindings fusex/eos-fusex-functional-test
  DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR}
  PERMISSIONS OWNER_READ OWNER_EXECUTE
  GROUP_READ GROUP_EXECUTE
  WORLD_READ WORLD_EXECUTE)
