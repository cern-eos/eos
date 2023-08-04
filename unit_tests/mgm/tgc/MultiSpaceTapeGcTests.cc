//------------------------------------------------------------------------------
// File: MultiSpaceTapeGcTests.cc
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
#include "mgm/tgc/MultiSpaceTapeGc.hh"

#include <ctime>
#include <gtest/gtest.h>

class TgcMultiSpaceTapeGcTest : public ::testing::Test {
protected:

  void SetUp() override {
  }

  void TearDown() override {
  }
};

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcMultiSpaceTapeGcTest, constructor)
{
  using namespace eos::mgm::tgc;

  DummyTapeGcMgm mgm;
  MultiSpaceTapeGc gc(mgm);

  const auto stats = gc.getStats();
  ASSERT_TRUE(stats.empty());
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcMultiSpaceTapeGcTest, start_with_one_eos_space)
{
  using namespace eos::mgm::tgc;

  DummyTapeGcMgm mgm;
  MultiSpaceTapeGc gc(mgm);

  const std::string space = "space";
  std::set<std::string> spaces;
  spaces.insert(space);
  gc.setTapeEnabled(spaces);
  gc.start();
 
  const auto now = std::time(nullptr);
  const auto stats = gc.getStats();
  ASSERT_EQ(1, stats.size());

  auto itor = stats.begin();
  ASSERT_EQ(space, itor->first);
  ASSERT_EQ(0, itor->second.nbEvicts);
  ASSERT_EQ(0, itor->second.lruQueueSize);
  ASSERT_TRUE(now <= itor->second.queryTimestamp && itor->second.queryTimestamp <= (now + 5));
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcMultiSpaceTapeGcTest, start_with_two_eos_spaces)
{
  using namespace eos::mgm::tgc;

  DummyTapeGcMgm mgm;
  MultiSpaceTapeGc gc(mgm);

  const std::string space1 = "space1";
  const std::string space2 = "space2";
  std::set<std::string> spaces;
  spaces.insert(space1);
  spaces.insert(space2);
  gc.setTapeEnabled(spaces);
  gc.start();

  const auto stats = gc.getStats();
  ASSERT_EQ(2, stats.size());

  auto itor = stats.begin();
  ASSERT_EQ(space1, itor->first);
  ASSERT_EQ(0, itor->second.nbEvicts);
  ASSERT_EQ(0, itor->second.lruQueueSize);

  itor++;
  ASSERT_EQ(space2, itor->first);
  ASSERT_EQ(0, itor->second.nbEvicts);
  ASSERT_EQ(0, itor->second.lruQueueSize);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcMultiSpaceTapeGcTest, start_and_stop_with_one_eos_space)
{
  using namespace eos::mgm::tgc;

  DummyTapeGcMgm mgm;
  MultiSpaceTapeGc gc(mgm);

  const std::string space = "space";
  std::set<std::string> spaces;
  spaces.insert(space);
  gc.setTapeEnabled(spaces);
  gc.start();
 
  const auto statsBefore = gc.getStats();
  ASSERT_EQ(1, statsBefore.size());

  gc.stop();

  const auto statsAfter = gc.getStats();
  ASSERT_EQ(0, statsAfter.size());
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcMultiSpaceTapeGcTest, start_and_restart_with_one_eos_space)
{
  using namespace eos::mgm::tgc;

  DummyTapeGcMgm mgm;
  MultiSpaceTapeGc gc(mgm);

  const std::string space = "space";
  std::set<std::string> spaces;
  spaces.insert(space);
  gc.setTapeEnabled(spaces);
  gc.start();
 
  gc.stop();
  gc.start();

  const auto now = std::time(nullptr);
  const auto stats = gc.getStats();
  ASSERT_EQ(1, stats.size());

  auto itor = stats.begin();
  ASSERT_EQ(space, itor->first);
  ASSERT_EQ(0, itor->second.nbEvicts);
  ASSERT_EQ(0, itor->second.lruQueueSize);
  ASSERT_TRUE(now <= itor->second.queryTimestamp && itor->second.queryTimestamp <= (now + 5));
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcMultiSpaceTapeGcTest, start_and_stop_with_two_eos_spaces)
{
  using namespace eos::mgm::tgc;

  DummyTapeGcMgm mgm;
  MultiSpaceTapeGc gc(mgm);

  const std::string space1 = "space1";
  const std::string space2 = "space2";
  std::set<std::string> spaces;
  spaces.insert(space1);
  spaces.insert(space2);
  gc.setTapeEnabled(spaces);
  gc.start();

  const auto statsBefore = gc.getStats();
  ASSERT_EQ(2, statsBefore.size());

  gc.stop();

  const auto statsAfter = gc.getStats();
  ASSERT_EQ(0, statsAfter.size());
}

