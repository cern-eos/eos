//------------------------------------------------------------------------------
// File: FsOpsTests.cc
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#include "common/FileSystem.hh"
#include "common/FsOps.hh"
#include "gtest/gtest.h"
#include <set>

using eos::common::AllowsAnyRead;
using eos::common::AllowsAnyWrite;
using eos::common::AllowsOp;
using eos::common::ConfigStatus;
using eos::common::DeriveLegacyConfigStatus;
using eos::common::DeriveMaskFromLegacy;
using eos::common::FormatSchedMask;
using eos::common::FsOpMask;
using eos::common::kMaskAll;
using eos::common::kMaskNone;
using eos::common::kNumSchedOps;
using eos::common::ParseSchedSpec;
using eos::common::SchedActivity;
using eos::common::SchedDirection;
using eos::common::SchedOp;

static constexpr SchedOp kClientRead{SchedActivity::kClient, SchedDirection::kRead};
static constexpr SchedOp kClientUpdate{SchedActivity::kClient, SchedDirection::kUpdate};
static constexpr SchedOp kClientCreate{SchedActivity::kClient, SchedDirection::kCreate};
static constexpr SchedOp kInternalRead{SchedActivity::kInternal, SchedDirection::kRead};
static constexpr SchedOp kInternalUpdate{SchedActivity::kInternal,
                                         SchedDirection::kUpdate};
static constexpr SchedOp kInternalCreate{SchedActivity::kInternal,
                                         SchedDirection::kCreate};

//------------------------------------------------------------------------------
// Every operation must occupy a distinct bit, and all of them must fit in the
// mask - this is what lets the mask live in the byte ConfigStatus used to take
// in the scheduler's per-disk snapshot.
//------------------------------------------------------------------------------
TEST(FsOps, BitLayout)
{
  const SchedOp all_ops[] = {kClientRead,   kClientUpdate,   kClientCreate,
                             kInternalRead, kInternalUpdate, kInternalCreate};
  std::set<uint8_t> indices;

  for (const auto& op : all_ops) {
    EXPECT_LT(op.Index(), kNumSchedOps);
    EXPECT_TRUE(indices.insert(op.Index()).second) << "duplicate index";
  }

  EXPECT_EQ(indices.size(), kNumSchedOps);
  EXPECT_EQ(kMaskAll, 0b111111);
  EXPECT_EQ(sizeof(FsOpMask), 1u);
}

//------------------------------------------------------------------------------
// Presets expand to the combinations documented in the plan
//------------------------------------------------------------------------------
TEST(FsOps, PresetExpansion)
{
  const auto rw = ParseSchedSpec("rw");
  ASSERT_TRUE(rw.has_value());
  EXPECT_EQ(*rw, kMaskAll);

  const auto ro = ParseSchedSpec("ro");
  ASSERT_TRUE(ro.has_value());
  EXPECT_TRUE(AllowsOp(*ro, kClientRead));
  EXPECT_TRUE(AllowsOp(*ro, kInternalRead));
  EXPECT_FALSE(AllowsAnyWrite(*ro));

  const auto wo = ParseSchedSpec("wo");
  ASSERT_TRUE(wo.has_value());
  EXPECT_FALSE(AllowsAnyRead(*wo));
  EXPECT_TRUE(AllowsOp(*wo, kClientCreate));
  EXPECT_TRUE(AllowsOp(*wo, kInternalCreate));

  // The headline case: no client traffic of any kind, internal untouched
  const auto internal = ParseSchedSpec("internal");
  ASSERT_TRUE(internal.has_value());
  EXPECT_FALSE(AllowsOp(*internal, kClientRead));
  EXPECT_FALSE(AllowsOp(*internal, kClientUpdate));
  EXPECT_FALSE(AllowsOp(*internal, kClientCreate));
  EXPECT_TRUE(AllowsOp(*internal, kInternalRead));
  EXPECT_TRUE(AllowsOp(*internal, kInternalUpdate));
  EXPECT_TRUE(AllowsOp(*internal, kInternalCreate));

  const auto clientro = ParseSchedSpec("clientro");
  ASSERT_TRUE(clientro.has_value());
  EXPECT_TRUE(AllowsOp(*clientro, kClientRead));
  EXPECT_FALSE(AllowsOp(*clientro, kClientCreate));
  EXPECT_TRUE(AllowsOp(*clientro, kInternalCreate));

  const auto none = ParseSchedSpec("none");
  ASSERT_TRUE(none.has_value());
  EXPECT_EQ(*none, kMaskNone);

  // drain and ro carry the same bits - they differ only in whether draining is
  // requested, which is a separate key
  EXPECT_EQ(ParseSchedSpec("drain"), ParseSchedSpec("ro"));
}

//------------------------------------------------------------------------------
// The explicit r/u/c/w grammar
//------------------------------------------------------------------------------
TEST(FsOps, SpecGrammar)
{
  const auto mask = ParseSchedSpec("client:ru,internal:ruc");
  ASSERT_TRUE(mask.has_value());
  EXPECT_TRUE(AllowsOp(*mask, kClientRead));
  EXPECT_TRUE(AllowsOp(*mask, kClientUpdate));
  // no new client replicas here, but existing ones stay writable
  EXPECT_FALSE(AllowsOp(*mask, kClientCreate));
  EXPECT_TRUE(AllowsOp(*mask, kInternalCreate));

  // 'w' is a convenience alias for update+create
  EXPECT_EQ(ParseSchedSpec("client:rw"), ParseSchedSpec("client:ruc"));
  EXPECT_EQ(ParseSchedSpec("client:w"), ParseSchedSpec("client:uc"));

  // an unmentioned class contributes nothing
  const auto only_internal_read = ParseSchedSpec("internal:r");
  ASSERT_TRUE(only_internal_read.has_value());
  EXPECT_EQ(*only_internal_read, kInternalRead.Bit());

  // the drain-without-client-access recipe
  EXPECT_FALSE(AllowsOp(*only_internal_read, kClientRead));
  EXPECT_TRUE(AllowsOp(*only_internal_read, kInternalRead));

  // malformed input is an error, never a silent default
  EXPECT_FALSE(ParseSchedSpec("").has_value());
  EXPECT_FALSE(ParseSchedSpec("client").has_value());
  EXPECT_FALSE(ParseSchedSpec("client:").has_value());
  EXPECT_FALSE(ParseSchedSpec("bogus:r").has_value());
  EXPECT_FALSE(ParseSchedSpec("client:x").has_value());
  EXPECT_FALSE(ParseSchedSpec("client:r,").has_value());
  EXPECT_FALSE(ParseSchedSpec("draindead").has_value());
}

//------------------------------------------------------------------------------
// Every mask must survive a format/parse round trip
//------------------------------------------------------------------------------
TEST(FsOps, FormatParseRoundTrip)
{
  for (FsOpMask mask = 0; mask <= kMaskAll; ++mask) {
    const std::string rendered = FormatSchedMask(mask);
    ASSERT_FALSE(rendered.empty()) << "mask " << int(mask) << " rendered empty";
    const auto reparsed = ParseSchedSpec(rendered);
    ASSERT_TRUE(reparsed.has_value()) << "cannot reparse '" << rendered << "'";
    EXPECT_EQ(*reparsed, mask) << "round trip changed '" << rendered << "'";
  }
}

//------------------------------------------------------------------------------
// The six surviving legacy values map onto presets; anything else is unset
//------------------------------------------------------------------------------
TEST(FsOps, LegacyMigration)
{
  EXPECT_EQ(DeriveMaskFromLegacy(ConfigStatus::kRW), ParseSchedSpec("rw"));
  EXPECT_EQ(DeriveMaskFromLegacy(ConfigStatus::kRO), ParseSchedSpec("ro"));
  EXPECT_EQ(DeriveMaskFromLegacy(ConfigStatus::kWO), ParseSchedSpec("wo"));
  EXPECT_EQ(DeriveMaskFromLegacy(ConfigStatus::kDrain), ParseSchedSpec("drain"));
  EXPECT_EQ(DeriveMaskFromLegacy(ConfigStatus::kOff), ParseSchedSpec("none"));
  EXPECT_EQ(DeriveMaskFromLegacy(ConfigStatus::kEmpty), ParseSchedSpec("none"));

  // A retired status has no successor: the caller fences the filesystem off
  // rather than guessing at an intent the model no longer has
  EXPECT_FALSE(DeriveMaskFromLegacy(ConfigStatus::kUnknown).has_value());
}

//------------------------------------------------------------------------------
// The projection back to ConfigStatus must only ever produce a surviving value
//------------------------------------------------------------------------------
TEST(FsOps, LegacyProjectionRange)
{
  const std::set<ConfigStatus> accepted{ConfigStatus::kRW,  ConfigStatus::kRO,
                                        ConfigStatus::kWO,  ConfigStatus::kDrain,
                                        ConfigStatus::kOff, ConfigStatus::kEmpty};

  for (FsOpMask mask = 0; mask <= kMaskAll; ++mask) {
    for (const bool empty : {false, true}) {
      const ConfigStatus derived = DeriveLegacyConfigStatus(mask, empty);
      EXPECT_TRUE(accepted.count(derived) != 0)
          << "mask " << int(mask) << " projected outside the accepted set";
    }
  }
}

//------------------------------------------------------------------------------
// A mask that allows only internal traffic must project to kDrain. kOff would
// stop the FST booting the filesystem (ShouldBoot needs > kOff) and would fail
// IsFsOperational (needs >= kDrain), so the internal traffic we just enabled
// could never reach the disk.
//------------------------------------------------------------------------------
TEST(FsOps, InternalOnlyProjectsToDrain)
{
  for (FsOpMask mask = 1; mask <= kMaskAll; ++mask) {
    const bool any_client = AllowsOp(mask, kClientRead) ||
                            AllowsOp(mask, kClientUpdate) ||
                            AllowsOp(mask, kClientCreate);
    const bool any_internal = AllowsOp(mask, kInternalRead) ||
                              AllowsOp(mask, kInternalUpdate) ||
                              AllowsOp(mask, kInternalCreate);

    if (!any_client && any_internal) {
      EXPECT_EQ(DeriveLegacyConfigStatus(mask, false), ConfigStatus::kDrain)
          << "mask " << int(mask) << " must stay booted and operational";
      EXPECT_EQ(DeriveLegacyConfigStatus(mask, true), ConfigStatus::kDrain);
    }
  }

  // An empty mask is the only one that may fence the filesystem off entirely
  EXPECT_EQ(DeriveLegacyConfigStatus(kMaskNone, false), ConfigStatus::kOff);
  EXPECT_EQ(DeriveLegacyConfigStatus(kMaskNone, true), ConfigStatus::kEmpty);
}

//------------------------------------------------------------------------------
// configstatus=X -> mask -> configstatus must be the identity for the six
// values the compatibility command still accepts
//------------------------------------------------------------------------------
TEST(FsOps, LegacyRoundTrip)
{
  for (const ConfigStatus status :
       {ConfigStatus::kRW, ConfigStatus::kRO, ConfigStatus::kWO, ConfigStatus::kOff}) {
    const auto mask = DeriveMaskFromLegacy(status);
    ASSERT_TRUE(mask.has_value());
    EXPECT_EQ(DeriveLegacyConfigStatus(*mask, false), status)
        << "round trip lost " << int(static_cast<int8_t>(status));
  }

  // kEmpty is distinguished from kOff by the lifecycle, not by the mask
  const auto empty_mask = DeriveMaskFromLegacy(ConfigStatus::kEmpty);
  ASSERT_TRUE(empty_mask.has_value());
  EXPECT_EQ(DeriveLegacyConfigStatus(*empty_mask, true), ConfigStatus::kEmpty);

  // kDrain shares its bits with kRO, so it projects back as kRO - the drain
  // request itself lives in its own key
  const auto drain_mask = DeriveMaskFromLegacy(ConfigStatus::kDrain);
  ASSERT_TRUE(drain_mask.has_value());
  EXPECT_EQ(DeriveLegacyConfigStatus(*drain_mask, false), ConfigStatus::kRO);
}
