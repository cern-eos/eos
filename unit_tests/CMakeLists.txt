#-------------------------------------------------------------------------------
# File: CMakeLists.txt
# Author: Jozsef Makai <jmakai@cern.ch> CERN
#-------------------------------------------------------------------------------

# ************************************************************************
# * EOS - the CERN Disk Storage System                                   *
# * Copyright (C) 2017 CERN/Switzerland                                  *
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
  ${CMAKE_BINARY_DIR}/namespace/ns_quarkdb)

set(EOS_UT_RPATH
  "${CMAKE_BINARY_DIR}/fst/;${CMAKE_BINARY_DIR}/mgm/;"
  "${CMAKE_BINARY_DIR}/common/;${CMAKE_BINARY_DIR}/mq/;"
  "${CMAKE_BINARY_DIR}/auth_plugin/;${CMAKE_BINARY_DIR}/namespace/ns_quarkdb/;"
  "${CMAKE_BINARY_DIR}/namespace/;${CMAKE_BINARY_DIR}/proto/;")

set(MQ_UT_SRCS
  mq/XrdMqMessageTests.cc)

set(CONSOLE_UT_SRCS
  console/AclCmdTest.cc
  console/ParseCommentTest.cc
  console/RegexUtilTest.cc
  console/ConsoleUtilTests.cc
  console/CmdsTests.cc)

set(MGM_UT_SRCS
  mgm/AccessTests.cc
  mgm/AclCmdTests.cc
  mgm/ConversionInfoTests.cc
  mgm/EgroupTests.cc
  mgm/FileSystemRegistryTests.cc
  mgm/FsViewTests.cc
  mgm/HttpTests.cc
  mgm/IostatTests.cc
  mgm/LockTrackerTests.cc
  mgm/LRUTests.cc
  mgm/QoSClassTests.cc
  mgm/ProcFsTests.cc
  mgm/RoutingTests.cc
  mgm/IdTrackerTests.cc
  mgm/FsckEntryTests.cc
  mgm/FusexCastBatchTests.cc
  mgm/CapsTests.cc
  mgm/CommitHelperTests.cc
  mgm/QuarkDBConfigTests.cc
  mgm/groupbalancer/BalancerEngineTypeTests.cc
  mgm/groupbalancer/FreeSpaceBalancerTests.cc
  mgm/groupbalancer/StdDevBalancerEngineTests.cc
  mgm/groupbalancer/StdDrainerTests.cc
  mgm/groupbalancer/MinMaxBalancerEngineTests.cc
  mgm/groupbalancer/GroupBalancerUtilsTests.cc
  mgm/groupbalancer/GroupsInfoFetcherTests.cc
  mgm/groupdrainer/GroupDrainerRetry.cc
  mgm/groupdrainer/GroupDrainerTests.cc
  mgm/groupdrainer/DrainProgressTrackerTests.cc
  mgm/tgc/CachedValueTests.cc
  mgm/tgc/FreedBytesHistogramTests.cc
  mgm/tgc/LruTests.cc
  mgm/tgc/MultiSpaceTapeGcTests.cc
  mgm/tgc/SmartSpaceStatsTests.cc
  mgm/tgc/SpaceToTapeGcMapTests.cc
  mgm/tgc/TapeGcTests.cc
  mgm/CtaUtilsTests.cc
  mgm/XrdMgmOfsTests.cc
  mgm/XrdMgmOfsFileTests.cc
  mgm/bulk-request/PrepareManagerTest.cc
  mgm/bulk-request/BulkRequestPrepareManagerTest.cc
  mgm/bulk-request/MockPrepareMgmFSInterface.cc
  mgm/http/HttpServerTests.cc
  mgm/http/rest-api/tape/RestApiTest.cc
  mgm/http/rest-api/tape/JsonCPPTapeModelBuilderTest.cc
  mgm/utils/AttrHelperTests.cc
  mgm/PolicyTests.cc
  mgm/placement/ClusterMapTests.cc
  mgm/placement/RRSeedTests.cc
  mgm/placement/SchedulerTests.cc
  mgm/placement/ThreadLocalRRSeedTests.cc
  mgm/placement/FsSchedulerTests.cc
  mgm/placement/PlacementStrategyTests.cc)

set(COMMON_UT_SRCS
  common/UtilsTests.cc
  common/FileIdTests.cc
  common/FileMapTests.cc
  common/FutureWrapperTests.cc
  common/InodeTests.cc
  common/LoggingTests.cc
  common/LoggingTestsUtils.cc
  common/MappingTests.cc
  common/PathTests.cc
  common/GlobTests.cc
  common/RWMutexTest.cc
  common/RegexWrapperTests.cc
  common/StringConversionTests.cc
  common/StringTokenizerTests.cc
  common/StringSplitTests.cc
  common/StringUtilsTests.cc
  common/SymKeysTests.cc
  common/ThreadPoolTest.cc
  common/TimingTests.cc
  common/VariousTests.cc
  common/XrdConnPoolTests.cc
  common/RateLimitTests.cc
  common/EosTokenTests.cc
  common/ConfigTests.cc
  common/BufferManagerTests.cc
  common/ConcurrentQueueTests.cc
  common/ContainerUtilsTests.cc
  common/ObserverMgrTests.cc
  common/ConfigStoreTests.cc
  common/async/OpaqueFutureTests.cc
  common/async/ExecutorMgrTests.cc
  common/CounterTests.cc
  common/ShardedCacheTests.cc
  common/concurrency/AlignedAtomicArrayTests.cc
  common/concurrency/AtomicUniquePtrTests.cc
  common/concurrency/ThreadEpochCounterTests.cc
  common/concurrency/RCUTests.cc
  common/RandTests.cc
  common/SciTokensTests.cc
  common/WebNotifyTests.cc
)

set(FUSEX_UT_SRCS
  fusex/StatTests.cc
  ${CMAKE_SOURCE_DIR}/fusex/stat/Stat.cc # We'll consider linking directly to fusex later
)

set(FST_UT_SRCS
  fst/XrdFstOfsTests.cc
  fst/XrdFstOssFileTest.cc
  fst/HealthTest.cc
  fst/UtilsTest.cc
  fst/XrdFstOfsFileInternalTest.cc
  fst/ScanDirTests.cc
  ${CMAKE_SOURCE_DIR}/fst/ScanDir.cc # To pass on the NOOFS flag explicitly
  fst/LoadTests.cc
  fst/MonitorVarPartitionTest.cc
  fst/ResponseCollectorTests.cc
  fst/WalkDirTreeTests.cc
  fst/HttpHandlerFstFileCacheTests.cc)

#-------------------------------------------------------------------------------
# unit tests source files
#-------------------------------------------------------------------------------
set(UT_SRCS
  ${MQ_UT_SRCS}
  ${CONSOLE_UT_SRCS}
  ${MGM_UT_SRCS}
  ${COMMON_UT_SRCS}
  ${FUSEX_UT_SRCS})

set(UT_FST_SRCS
  ${FST_UT_SRCS})

#-------------------------------------------------------------------------------
# eos-unit-tests executable
#-------------------------------------------------------------------------------
add_executable(eos-unit-tests ${UT_SRCS})

target_link_libraries(eos-unit-tests PRIVATE
  EosConsoleHelpers-Objects
  EosConsoleCommands-Objects
  EosSciToken-Objects
  EosMgmHttp-Objects
  GTest::Main
  GTest::gmock
  qclient
  XROOTD::POSIX
  XROOTD::SERVER
  RestAnnot-Objects
  XrdEosMgm-Static)

#-------------------------------------------------------------------------------
# eos-unit-tests-fst executable
#-------------------------------------------------------------------------------
add_executable(eos-unit-tests-fst ${UT_FST_SRCS})
target_compile_definitions(eos-unit-tests-fst PUBLIC -D_NOOFS=1)

target_include_directories(eos-unit-tests-fst PUBLIC
  ${CMAKE_SOURCE_DIR}/common)

target_link_libraries(eos-unit-tests-fst PRIVATE
  GTest::Main
  GTest::gmock
  EosFstOss-Static
  XrdEosFst-Static
  XXHASH::XXHASH)

#-------------------------------------------------------------------------------
# eos-unit-tests-with-instance executable
#-------------------------------------------------------------------------------
add_executable(eos-unit-tests-with-instance
  fst/main_fst.cc
  fst/TestEnv.cc
  fst/XrdFstOfsFileTest.cc
  fst/XrdIoTests.cc)

target_link_libraries(eos-unit-tests-with-instance PRIVATE
  GTest::GTest
  XrdEosFst-Static)

#-------------------------------------------------------------------------------
# eos-unit-tests-with-qdb executable
#-------------------------------------------------------------------------------
add_executable(eos-unit-tests-with-qdb
  with_qdb/Main.cc
  with_qdb/configuration.cc
  with_qdb/TestUtils.cc)

target_link_libraries(eos-unit-tests-with-qdb
  GTest::Main
  GTest::gmock
  qclient
  RestAnnot-Objects
  XrdEosMgm-Static
  READLINE::READLINE
  JSONCPP::JSONCPP)

install(TARGETS
  eos-unit-tests eos-unit-tests-with-instance eos-unit-tests-fst
  eos-unit-tests-with-qdb
  RUNTIME DESTINATION ${CMAKE_INSTALL_FULL_SBINDIR})
