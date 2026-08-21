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

namespace {
//------------------------------------------------------------------------------
//! The flat scheduler running the given strategy
//------------------------------------------------------------------------------
eos::mgm::placement::SchedConfig
Flat(eos::mgm::placement::PlacementStrategyT strategy)
{
  return {eos::mgm::placement::SchedEngineT::kFlat, strategy};
}
} // namespace

TEST(SchedConfigFromStr, UnknownAndEmptyFallBackToTheLegacyEngine)
{
  using namespace eos::mgm::placement;
  // A space with no scheduler.type configured is pushed through here as an
  // empty string at startup, so this is what decides which engine an untouched
  // installation runs. It has to stay geotree until the flat scheduler soaks.
  EXPECT_EQ(SchedConfigFromStr(""), kGeoTreeSchedConfig);
  EXPECT_EQ(SchedConfigFromStr("nonsense"), kGeoTreeSchedConfig);
  EXPECT_EQ(SchedConfigFromStr("geotree"), kGeoTreeSchedConfig);
  EXPECT_EQ(SchedConfigFromStr("legacy"), kGeoTreeSchedConfig);
  // ... and the flat scheduler has to be asked for by name
  EXPECT_EQ(SchedConfigFromStr("flat:geo"), Flat(PlacementStrategyT::kGeoScheduler));
  EXPECT_EQ(SchedConfigFromStr("geo"), Flat(PlacementStrategyT::kGeoScheduler));
}

TEST(ParseSchedConfig, EnginePrefixedNames)
{
  using namespace eos::mgm::placement;
  // The canonical grammar: the engine is the first thing in the value
  EXPECT_EQ(ParseSchedConfig("geotree"), kGeoTreeSchedConfig);
  EXPECT_EQ(ParseSchedConfig("flat:geo"), Flat(PlacementStrategyT::kGeoScheduler));
  EXPECT_EQ(ParseSchedConfig("flat:roundrobin"), Flat(PlacementStrategyT::kRoundRobin));
  EXPECT_EQ(ParseSchedConfig("flat:threadlocalroundrobin"),
            Flat(PlacementStrategyT::kThreadLocalRoundRobin));
  EXPECT_EQ(ParseSchedConfig("flat:random"), Flat(PlacementStrategyT::kRandom));
  EXPECT_EQ(ParseSchedConfig("flat:fidrandom"), Flat(PlacementStrategyT::kFidRandom));
  EXPECT_EQ(ParseSchedConfig("flat:weightedrandom"),
            Flat(PlacementStrategyT::kWeightedRandom));
  EXPECT_EQ(ParseSchedConfig("flat:weightedroundrobin"),
            Flat(PlacementStrategyT::kWeightedRoundRobin));
  // Aliases carry the prefix too
  EXPECT_EQ(ParseSchedConfig("flat:tlrr"),
            Flat(PlacementStrategyT::kThreadLocalRoundRobin));
  EXPECT_EQ(ParseSchedConfig("flat:weightedrr"),
            Flat(PlacementStrategyT::kWeightedRoundRobin));
  EXPECT_EQ(ParseSchedConfig("flat:fid"), Flat(PlacementStrategyT::kFidRandom));
  // The legacy engine is not a flat strategy, so it cannot wear the prefix
  EXPECT_FALSE(ParseSchedConfig("flat:geotree").has_value());
  EXPECT_FALSE(ParseSchedConfig("flat:legacy").has_value());
}

TEST(ParseSchedConfig, PrePrefixSpellingsStillParse)
{
  using namespace eos::mgm::placement;
  // Every value a configuration written before the engine prefix can hold has
  // to keep meaning exactly what it meant, or an upgrade would silently move
  // spaces between engines
  EXPECT_EQ(ParseSchedConfig("geo"), Flat(PlacementStrategyT::kGeoScheduler));
  EXPECT_EQ(ParseSchedConfig("geoscheduler"), Flat(PlacementStrategyT::kGeoScheduler));
  EXPECT_EQ(ParseSchedConfig("legacy"), kGeoTreeSchedConfig);
  EXPECT_EQ(ParseSchedConfig("roundrobin"), Flat(PlacementStrategyT::kRoundRobin));
  EXPECT_EQ(ParseSchedConfig("rr"), Flat(PlacementStrategyT::kRoundRobin));
  EXPECT_EQ(ParseSchedConfig("tlrr"), Flat(PlacementStrategyT::kThreadLocalRoundRobin));
  EXPECT_EQ(ParseSchedConfig("threadlocalrr"),
            Flat(PlacementStrategyT::kThreadLocalRoundRobin));
  EXPECT_EQ(ParseSchedConfig("fid"), Flat(PlacementStrategyT::kFidRandom));
  EXPECT_EQ(ParseSchedConfig("weightedrr"),
            Flat(PlacementStrategyT::kWeightedRoundRobin));
}

TEST(ParseSchedConfig, RejectsWhatItCannotName)
{
  using namespace eos::mgm::placement;
  // The whole point of the strict parse: a typo is an error here, where the
  // command layer can refuse it, instead of a silent downgrade to geotree
  EXPECT_FALSE(ParseSchedConfig("").has_value());
  EXPECT_FALSE(ParseSchedConfig("nonsense").has_value());
  EXPECT_FALSE(ParseSchedConfig("roundrobbin").has_value());
  EXPECT_FALSE(ParseSchedConfig("flat:").has_value());
  EXPECT_FALSE(ParseSchedConfig("flat:nonsense").has_value());
  EXPECT_FALSE(ParseSchedConfig("flat").has_value());
  EXPECT_FALSE(ParseSchedConfig("geotree:flat").has_value());
}

TEST(SchedConfigToStr, RendersTheCanonicalPrefixedName)
{
  using namespace eos::mgm::placement;
  EXPECT_EQ(SchedConfigToStr(kGeoTreeSchedConfig), "geotree");
  EXPECT_EQ(SchedConfigToStr(Flat(PlacementStrategyT::kGeoScheduler)), "flat:geo");
  EXPECT_EQ(SchedConfigToStr(Flat(PlacementStrategyT::kRoundRobin)), "flat:roundrobin");
  EXPECT_EQ(SchedConfigToStr(Flat(PlacementStrategyT::kThreadLocalRoundRobin)),
            "flat:threadlocalroundrobin");
  EXPECT_EQ(SchedConfigToStr(Flat(PlacementStrategyT::kRandom)), "flat:random");
  EXPECT_EQ(SchedConfigToStr(Flat(PlacementStrategyT::kFidRandom)), "flat:fidrandom");
  EXPECT_EQ(SchedConfigToStr(Flat(PlacementStrategyT::kWeightedRandom)),
            "flat:weightedrandom");
  EXPECT_EQ(SchedConfigToStr(Flat(PlacementStrategyT::kWeightedRoundRobin)),
            "flat:weightedroundrobin");
}

TEST(SchedConfigToStr, RoundTripsThroughTheStrictParse)
{
  using namespace eos::mgm::placement;
  EXPECT_EQ(ParseSchedConfig(SchedConfigToStr(kGeoTreeSchedConfig)), kGeoTreeSchedConfig);

  for (uint8_t i = 0; i < static_cast<uint8_t>(PlacementStrategyT::Count); ++i) {
    const auto config = Flat(static_cast<PlacementStrategyT>(i));
    const std::string name = SchedConfigToStr(config);
    EXPECT_NE(name, "flat:unknown") << "strategy " << (int)i << " has no name";
    EXPECT_EQ(ParseSchedConfig(name), config) << "name " << name;
    // What SchedConfigToStr renders is by definition the canonical spelling
    EXPECT_FALSE(IsDeprecatedSchedConfigSpelling(name)) << "name " << name;
  }
}

TEST(IsDeprecatedSchedConfigSpelling, FlagsOnlyThePrePrefixNames)
{
  using namespace eos::mgm::placement;
  EXPECT_TRUE(IsDeprecatedSchedConfigSpelling("geo"));
  EXPECT_TRUE(IsDeprecatedSchedConfigSpelling("roundrobin"));
  EXPECT_TRUE(IsDeprecatedSchedConfigSpelling("weightedrr"));
  EXPECT_TRUE(IsDeprecatedSchedConfigSpelling("legacy"));
  EXPECT_FALSE(IsDeprecatedSchedConfigSpelling("geotree"));
  EXPECT_FALSE(IsDeprecatedSchedConfigSpelling("flat:geo"));
  EXPECT_FALSE(IsDeprecatedSchedConfigSpelling("flat:weightedrr"));
  // Not a scheduler type at all, so nothing to deprecate
  EXPECT_FALSE(IsDeprecatedSchedConfigSpelling("nonsense"));
}

TEST(MakeSelectionStrategy, EveryEnumeratorHasAnImplementation)
{
  using namespace eos::mgm::placement;

  // The routing marker used to sit in this enum and punch a nullptr hole in the
  // strategy array. Now that the engine is its own type, every value is real.
  for (uint8_t i = 0; i < static_cast<uint8_t>(PlacementStrategyT::Count); ++i) {
    EXPECT_NE(MakeSelectionStrategy(static_cast<PlacementStrategyT>(i), 128), nullptr)
        << "strategy " << (int)i;
  }
}
