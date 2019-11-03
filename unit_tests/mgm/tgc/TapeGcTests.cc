//------------------------------------------------------------------------------
// File: TapeGcTests.cc
// Author: Steven Murray <smurray at cern dot ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "mgm/tgc/DummyTapeGcMgm.hh"
#include "mgm/tgc/MaxLenExceeded.hh"
#include "mgm/tgc/TestingTapeGc.hh"

#include <gtest/gtest.h>
#include <ctime>

class TgcTapeGcTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcTapeGcTest, constructor)
{
  using namespace eos::mgm::tgc;

  const std::string space = "space";
  const std::time_t maxConfigCacheAgeSecs = 0; // Always renew cached value

  DummyTapeGcMgm mgm;
  TapeGc gc(mgm, space, maxConfigCacheAgeSecs);

  const auto now = std::time(nullptr);
  const auto stats = gc.getStats();

  ASSERT_EQ(0, stats.nbStagerrms);
  ASSERT_EQ(0, stats.lruQueueSize);
  ASSERT_EQ(0, stats.spaceStats.totalBytes);
  ASSERT_EQ(0, stats.spaceStats.availBytes);
  ASSERT_TRUE(now <= stats.queryTimestamp && stats.queryTimestamp <= (now +5));
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcTapeGcTest, enable)
{
  using namespace eos::mgm::tgc;

  const std::string space = "space";
  const std::time_t maxConfigCacheAgeSecs = 0; // Always renew cached value

  DummyTapeGcMgm mgm;
  TapeGc gc(mgm, space, maxConfigCacheAgeSecs);

  gc.enable();
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcTapeGcTest, enableWithoutStartingWorkerThread)
{
  using namespace eos::mgm::tgc;

  const std::string space = "space";
  const std::time_t maxConfigCacheAgeSecs = 0; // Always renew cached value

  DummyTapeGcMgm mgm;
  TestingTapeGc gc(mgm, space, maxConfigCacheAgeSecs);

  gc.enableWithoutStartingWorkerThread();
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcTapeGcTest, tryToGarbageCollectASingleFile)
{
  using namespace eos::mgm::tgc;

  const std::string space = "space";
  const std::time_t maxConfigCacheAgeSecs = 0; // Always renew cached value

  DummyTapeGcMgm mgm;
  TestingTapeGc gc(mgm, space, maxConfigCacheAgeSecs);

  gc.enableWithoutStartingWorkerThread();

  ASSERT_EQ(0, mgm.getNbCallsToGetTapeGcSpaceConfig());

  SpaceStats initialSpaceStats;
  initialSpaceStats.availBytes = 10;
  initialSpaceStats.totalBytes = 100;
  mgm.setSpaceStats(space, initialSpaceStats);

  {
    const auto spaceStats = mgm.getSpaceStats(space);
    ASSERT_EQ(initialSpaceStats.availBytes, spaceStats.availBytes);
    ASSERT_EQ(initialSpaceStats.totalBytes, spaceStats.totalBytes);
  }

  gc.tryToGarbageCollectASingleFile();

  ASSERT_EQ(2, mgm.getNbCallsToGetTapeGcSpaceConfig());
  ASSERT_EQ(0, mgm.getNbCallsToFileInNamespaceAndNotScheduledForDeletion());
  ASSERT_EQ(0, mgm.getNbCallsToGetFileSizeBytes());
  ASSERT_EQ(0, mgm.getNbCallsToStagerrmAsRoot());

  const std::string path = "the_file_path";
  eos::IFileMD::id_t fid = 1;
  gc.fileOpened(path, fid);

  gc.tryToGarbageCollectASingleFile();

  ASSERT_EQ(4, mgm.getNbCallsToGetTapeGcSpaceConfig());
  ASSERT_EQ(0, mgm.getNbCallsToFileInNamespaceAndNotScheduledForDeletion());
  ASSERT_EQ(0, mgm.getNbCallsToGetFileSizeBytes());
  ASSERT_EQ(0, mgm.getNbCallsToStagerrmAsRoot());


  {
    SpaceConfig config;
    config.availBytes = initialSpaceStats.availBytes + 1;
    mgm.setTapeGcSpaceConfig(space, config);
  }

  gc.tryToGarbageCollectASingleFile();

  ASSERT_EQ(6, mgm.getNbCallsToGetTapeGcSpaceConfig());
  ASSERT_EQ(0, mgm.getNbCallsToFileInNamespaceAndNotScheduledForDeletion());
  ASSERT_EQ(0, mgm.getNbCallsToGetFileSizeBytes());
  ASSERT_EQ(0, mgm.getNbCallsToStagerrmAsRoot());

  {
    SpaceConfig config;
    config.availBytes = initialSpaceStats.availBytes + 1;
    config.totalBytes = initialSpaceStats.totalBytes - 1;
    mgm.setTapeGcSpaceConfig(space, config);
  }

  gc.tryToGarbageCollectASingleFile();

  ASSERT_EQ(8, mgm.getNbCallsToGetTapeGcSpaceConfig());
  ASSERT_EQ(0, mgm.getNbCallsToFileInNamespaceAndNotScheduledForDeletion());
  ASSERT_EQ(1, mgm.getNbCallsToGetFileSizeBytes());
  ASSERT_EQ(1, mgm.getNbCallsToStagerrmAsRoot());
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcTapeGcTest, toJson)
{
  using namespace eos::mgm::tgc;

  const std::string space = "space";
  const std::time_t maxConfigCacheAgeSecs = 0; // Always renew cached value

  DummyTapeGcMgm mgm;
  TestingTapeGc gc(mgm, space, maxConfigCacheAgeSecs);

  gc.enableWithoutStartingWorkerThread();

  for (eos::IFileMD::id_t fid = 1; fid <= 3; fid++) {
    const std::string path = std::string("the_file_path_") + std::to_string(fid);
    gc.fileOpened(path, fid);
  }

  const std::string expectedJson =
    "{\"spaceName\":\"space\",\"enabled\":\"true\",\"lruQueue\":{\"size\":\"3\","
    "\"fids_from_MRU_to_LRU\":[\"0x0000000000000003\",\"0x0000000000000002\",\"0x0000000000000001\"]}}";
  std::ostringstream json;
  gc.toJson(json);
  ASSERT_EQ(expectedJson, json.str());
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcTapeGcTest, toJson_exceed_maxLen)
{
  using namespace eos::mgm::tgc;

  const std::string space = "space";
  const std::time_t maxConfigCacheAgeSecs = 0; // Always renew cached value

  DummyTapeGcMgm mgm;
  TestingTapeGc gc(mgm, space, maxConfigCacheAgeSecs);

  gc.enableWithoutStartingWorkerThread();

  std::ostringstream json;
  const std::string::size_type maxLen = 1;
  ASSERT_THROW(gc.toJson(json, maxLen), MaxLenExceeded);
}
