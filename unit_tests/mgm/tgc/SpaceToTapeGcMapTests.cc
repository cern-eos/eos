//------------------------------------------------------------------------------
// File: SpaceToTapeGcMapTests.cc
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
#include "mgm/tgc/SpaceToTapeGcMap.hh"

#include <gtest/gtest.h>

class TgcSpaceToTapeGcMapTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcSpaceToTapeGcMapTest, Constructor)
{
  using namespace eos::mgm::tgc;

  DummyTapeGcMgm mgm;
  SpaceToTapeGcMap map(mgm);

  const auto spaces = map.getSpaces();
  ASSERT_TRUE(spaces.empty());
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcSpaceToTapeGcMapTest, getGc_unknown_eos_space)
{
  using namespace eos::mgm::tgc;

  DummyTapeGcMgm mgm;
  SpaceToTapeGcMap map(mgm);

  const auto spaces = map.getSpaces();
  ASSERT_TRUE(spaces.empty());

  const std::string space = "space";

  ASSERT_THROW(map.getGc(space), SpaceToTapeGcMap::UnknownEOSSpace);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcSpaceToTapeGcMapTest, createGc)
{
  using namespace eos::mgm::tgc;

  DummyTapeGcMgm mgm;
  SpaceToTapeGcMap map(mgm);
  const std::string space = "space";

  TapeGc &gc1 = map.createGc(space);
  TapeGc &gc2 = map.getGc(space);
  ASSERT_EQ(&gc1, &gc2);

  const auto spaces = map.getSpaces();
  ASSERT_EQ(1, spaces.size());
  ASSERT_EQ(1, spaces.count(space));
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcSpaceToTapeGcMapTest, createGc_already_exists)
{
  using namespace eos::mgm::tgc;

  DummyTapeGcMgm mgm;
  SpaceToTapeGcMap map(mgm);
  const std::string space = "space";

  map.createGc(space);

  const auto spaces = map.getSpaces();
  ASSERT_EQ(1, spaces.size());
  ASSERT_EQ(1, spaces.count(space));

  ASSERT_THROW(map.createGc(space), SpaceToTapeGcMap::GcAlreadyExists);
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcSpaceToTapeGcMapTest, toJson)
{
  using namespace eos::mgm::tgc;

  DummyTapeGcMgm mgm;
  SpaceToTapeGcMap map(mgm);
  map.createGc("space1");
  map.createGc("space2");

  const auto spaces = map.getSpaces();
  ASSERT_EQ(2, spaces.size());
  ASSERT_EQ(1, spaces.count("space1"));
  ASSERT_EQ(1, spaces.count("space2"));

  {
    auto &gc = map.getGc("space1");
    for (eos::IFileMD::id_t fid = 1; fid <= 2; fid++) {
      gc.fileAccessed(fid);
    }
  }
  {
    auto &gc = map.getGc("space2");
    for (eos::IFileMD::id_t fid = 3; fid <= 4; fid++) {
      gc.fileAccessed(fid);
    }
  }

  const std::string expectedJson =
    "{"
    "\"space1\":{\"spaceName\":\"space1\",\"lruQueue\":{\"size\":\"2\","
    "\"fids_from_MRU_to_LRU\":[\"0x0000000000000002\",\"0x0000000000000001\"]}},"
    "\"space2\":{\"spaceName\":\"space2\",\"lruQueue\":{\"size\":\"2\","
    "\"fids_from_MRU_to_LRU\":[\"0x0000000000000004\",\"0x0000000000000003\"]}}"
    "}";
  std::ostringstream json;
  map.toJson(json);
  ASSERT_EQ(expectedJson, json.str());
}

//------------------------------------------------------------------------------
// Test
//------------------------------------------------------------------------------
TEST_F(TgcSpaceToTapeGcMapTest, toJson_exceed_maxLen)
{
  using namespace eos::mgm::tgc;

  DummyTapeGcMgm mgm;
  SpaceToTapeGcMap map(mgm);
  map.createGc("space1");
  map.createGc("space2");

  const auto spaces = map.getSpaces();
  ASSERT_EQ(2, spaces.size());
  ASSERT_EQ(1, spaces.count("space1"));
  ASSERT_EQ(1, spaces.count("space2"));

  const std::string::size_type maxLen = 1;
  std::ostringstream json;
  ASSERT_THROW(map.toJson(json, maxLen), MaxLenExceeded);
}
