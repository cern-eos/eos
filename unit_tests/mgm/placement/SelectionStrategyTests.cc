//------------------------------------------------------------------------------
//! @file SelectionStrategyTests.cc
//! @author Abhishek Lekshmanan <abhishek.lekshmanan@cern.ch>
//-----------------------------------------------------------------------------

/************************************************************************
  * EOS - the CERN Disk Storage System                                   *
  * Copyright (C) 2024 CERN/Switzerland                           *
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

#include "mgm/placement/FlatScheduler.hh"
#include "mgm/placement/SelectionStrategy.hh"
#include "gtest/gtest.h"

TEST(PlacementResult, default)
{
  eos::mgm::placement::PlacementResult result;
  EXPECT_EQ(result.ret_code, -1);
  EXPECT_EQ(result.ErrorString(), "");
  EXPECT_FALSE(result.IsValidPlacement(2));
}

TEST(PlacementResult, IsValidPlacement)
{
  eos::mgm::placement::PlacementResult result(2);
  EXPECT_TRUE(result.Add(1));
  EXPECT_TRUE(result.Add(2));
  EXPECT_TRUE(result.IsValidPlacement(2));

  // A bucket id among the selected items means the descent is not finished
  eos::mgm::placement::PlacementResult result2(2);
  EXPECT_TRUE(result2.Add(1));
  EXPECT_TRUE(result2.Add(-1));
  EXPECT_FALSE(result2.IsValidPlacement(2));

  // Fewer items than requested is a partial placement, not a valid one
  eos::mgm::placement::PlacementResult partial(2);
  EXPECT_TRUE(partial.Add(1));
  EXPECT_FALSE(partial.IsValidPlacement(2));
  EXPECT_EQ(partial.n_replicas, 2);
  EXPECT_EQ(partial.n_filled, 1);
}

TEST(PlacementResult, contains)
{
  eos::mgm::placement::PlacementResult result(2);
  result.Add(1);
  result.Add(2);
  EXPECT_TRUE(result.Contains(1));
  EXPECT_TRUE(result.Contains(2));
  EXPECT_FALSE(result.Contains(3));
}

TEST(PlacementResult, contains_scans_only_filled_entries)
{
  // Contains() must be bounded by what was actually added, not by the number
  // of replicas that were requested
  eos::mgm::placement::PlacementResult result(4);
  result.Add(4);
  result.Add(3);
  EXPECT_TRUE(result.Contains(4));
  EXPECT_TRUE(result.Contains(3));
  EXPECT_FALSE(result.Contains(2)) << "slot never filled";
  EXPECT_FALSE(result.Contains(1)) << "slot never filled";
}

TEST(PlacementResult, add_respects_capacity)
{
  eos::mgm::placement::PlacementResult result(2);

  for (size_t i = 0; i < result.ids.size(); ++i) {
    EXPECT_TRUE(result.Add(static_cast<eos::mgm::placement::ItemIdT>(i + 1)));
  }

  EXPECT_EQ((size_t)result.n_filled, result.ids.size());
  EXPECT_FALSE(result.Add(999)) << "must refuse to overflow ids";
  EXPECT_EQ((size_t)result.n_filled, result.ids.size());
}

TEST(StrategyFromStr, UnknownAndEmptyFallBackToTheLegacyEngine)
{
  using namespace eos::mgm::placement;
  // A space with no scheduler.type configured is pushed through here as an
  // empty string at startup, so this is what decides which engine an untouched
  // installation runs. It has to stay geotree until the flat scheduler soaks.
  EXPECT_EQ(StrategyFromStr(""), PlacementStrategyT::kGeoTreeLegacy);
  EXPECT_EQ(StrategyFromStr("nonsense"), PlacementStrategyT::kGeoTreeLegacy);
  EXPECT_EQ(StrategyFromStr("geotree"), PlacementStrategyT::kGeoTreeLegacy);
  EXPECT_EQ(StrategyFromStr("legacy"), PlacementStrategyT::kGeoTreeLegacy);
  // ... and the flat geo scheduler has to be asked for by name
  EXPECT_EQ(StrategyFromStr("geo"), PlacementStrategyT::kGeoScheduler);
  EXPECT_EQ(StrategyFromStr("geoscheduler"), PlacementStrategyT::kGeoScheduler);
  EXPECT_EQ(StrategyFromStr("roundrobin"), PlacementStrategyT::kRoundRobin);
}

TEST(MakeSelectionStrategy, LegacyEngineHasNoImplementation)
{
  using namespace eos::mgm::placement;
  // kGeoTreeLegacy is a routing marker, not a strategy: Scheduler.cc sees it
  // and never reaches the flat scheduler at all
  EXPECT_EQ(MakeSelectionStrategy(PlacementStrategyT::kGeoTreeLegacy, 128), nullptr);
  EXPECT_NE(MakeSelectionStrategy(PlacementStrategyT::kGeoScheduler, 128), nullptr);
}
