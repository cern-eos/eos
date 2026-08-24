/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2023 CERN/Switzerland                           *
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

#include "mgm/placement/ClusterBuilder.hh"
#include "mgm/placement/FsScheduler.hh"
#include "gtest/gtest.h"

using eos::mgm::placement::ClusterMgr;

struct TestClusterMgrHandler : public eos::mgm::placement::ClusterMgrHandler
{
  static void
  BuildDefault(ClusterMgr& mgr)
  {
    int n_disks_per_group = 16;
    int n_groups = 32;
    using namespace eos::mgm::placement;
    auto sh = mgr.GetSnapshotBuilder(2048);
    sh.AddBucket(GetBucketType(BucketType::ROOT), 0);
    for (int i = 0; i < n_groups; ++i) {
      sh.AddBucket(GetBucketType(BucketType::GROUP), -100 - i, 0);
    }

    for (int i = 0; i < n_groups * n_disks_per_group; i++) {
      sh.AddDisk(Disk(i + 1, kMaskAll, ActiveStatus::kOnline, 1),
                 -100 - i / n_disks_per_group);
    }
  }

  eos::mgm::placement::ClusterMgrMapT
  MakeClusterMgr(const eos::mgm::placement::ClusterMgrMapT& existing) override
  {
    // Rebuild on top of the existing manager so the configuration it owns
    // survives the rebuild, mirroring the production handler.
    eos::mgm::placement::ClusterMgrMapT cluster_map;
    std::shared_ptr<ClusterMgr> mgr;

    if (auto it = existing.find("default"); it != existing.end()) {
      mgr = it->second;
    } else {
      mgr = std::make_shared<ClusterMgr>();
    }

    BuildDefault(*mgr);
    cluster_map.insert_or_assign("default", std::move(mgr));
    return cluster_map;
  }
};

using eos::mgm::placement::FsScheduler;

TEST(FsScheduler, construction)
{
  FsScheduler fs_scheduler(2048, std::make_unique<TestClusterMgrHandler>());
  fs_scheduler.UpdateClusterData();
}

TEST(FsScheduler, null_handler)
{
  FsScheduler null_scheduler(2048, nullptr);
  null_scheduler.UpdateClusterData();
}

//------------------------------------------------------------------------------
//! The flat scheduler running the given strategy, which is what every value
//! below the routing decision looks like
//------------------------------------------------------------------------------
static eos::mgm::placement::SchedConfig
FlatConfig(eos::mgm::placement::PlacementStrategyT strategy)
{
  return {eos::mgm::placement::SchedEngineT::kFlat, strategy};
}

TEST(FsScheduler, default_scheduler)
{
  // An unconfigured scheduler routes to the legacy engine rather than picking
  // one of its own strategies
  FsScheduler fs_scheduler(2048, std::make_unique<TestClusterMgrHandler>());
  auto strategy = fs_scheduler.GetSchedConfig();
  ASSERT_EQ(strategy, eos::mgm::placement::kGeoTreeSchedConfig);
}

TEST(FsScheduler, legacy_engine_is_not_schedulable)
{
  // A space routed to the geotree engine is refused here even though every
  // PlacementStrategyT value is now a real strategy. Scheduler.cc checks the
  // engine before it ever calls in; this is the backstop that keeps the flat
  // scheduler from silently answering for a space that is not its to serve.
  FsScheduler fs_scheduler(2048, std::make_unique<TestClusterMgrHandler>());
  fs_scheduler.UpdateClusterData();
  auto result = fs_scheduler.Schedule("default", 2);
  ASSERT_FALSE(result);
  EXPECT_EQ(result.ret_code, EINVAL);
}

TEST(FsScheduler, geo_scheduler_places)
{
  FsScheduler fs_scheduler(2048, std::make_unique<TestClusterMgrHandler>());
  fs_scheduler.UpdateClusterData();
  fs_scheduler.SetSchedConfig("geo");
  ASSERT_EQ(fs_scheduler.GetSchedConfig(),
            FlatConfig(eos::mgm::placement::PlacementStrategyT::kGeoScheduler));
  auto result = fs_scheduler.Schedule("default", 2);
  ASSERT_TRUE(result);
}

TEST(FsScheduler, round_robin)
{
  FsScheduler fs_scheduler(2048, std::make_unique<TestClusterMgrHandler>());
  fs_scheduler.UpdateClusterData();
  fs_scheduler.SetSchedConfig("roundrobin");
  auto strategy = fs_scheduler.GetSchedConfig();
  ASSERT_EQ(strategy, FlatConfig(eos::mgm::placement::PlacementStrategyT::kRoundRobin));
  auto result = fs_scheduler.Schedule("default", 2);
  ASSERT_TRUE(result);
}

TEST(FsScheduler, SetSchedConfig)
{
  FsScheduler fs_scheduler(2048, std::make_unique<TestClusterMgrHandler>());
  fs_scheduler.UpdateClusterData();
  fs_scheduler.SetSchedConfig("roundrobin");
  EXPECT_EQ(fs_scheduler.GetSchedConfig(),
            FlatConfig(eos::mgm::placement::PlacementStrategyT::kRoundRobin));
  EXPECT_EQ(fs_scheduler.GetSchedConfig("default"),
            FlatConfig(eos::mgm::placement::PlacementStrategyT::kRoundRobin));
  EXPECT_EQ(fs_scheduler.GetSchedConfig("foobar"),
            FlatConfig(eos::mgm::placement::PlacementStrategyT::kRoundRobin));
}

TEST(FsScheduler, SetSchedConfigSpace)
{
  FsScheduler fs_scheduler(2048, std::make_unique<TestClusterMgrHandler>());
  fs_scheduler.UpdateClusterData();
  fs_scheduler.SetSchedConfig("default", "weightedrandom");
  // Only the named space is opted in, everything else keeps routing to the
  // legacy engine
  EXPECT_EQ(fs_scheduler.GetSchedConfig(), eos::mgm::placement::kGeoTreeSchedConfig);
  EXPECT_EQ(fs_scheduler.GetSchedConfig("default"),
            FlatConfig(eos::mgm::placement::PlacementStrategyT::kWeightedRandom));
  EXPECT_EQ(fs_scheduler.GetSchedConfig("tape"),
            eos::mgm::placement::kGeoTreeSchedConfig);
}

TEST(FsScheduler, GlobalDefaultReachesASpaceWithNoOverride)
{
  using namespace eos::mgm::placement;
  FsScheduler fs_scheduler(2048, std::make_unique<TestClusterMgrHandler>());
  fs_scheduler.UpdateClusterData();
  // What LoadConfig does for a space carrying no scheduler.type: nothing. It
  // used to push the empty value through anyway, which stored an explicit
  // geotree override and made the global default unreachable for every space
  // that existed at boot.
  fs_scheduler.SetSchedConfig("roundrobin");
  EXPECT_EQ(fs_scheduler.GetSchedConfig("default"),
            FlatConfig(PlacementStrategyT::kRoundRobin));
  // An override still wins over it, and clearing the global default does not
  // reach back into the space
  fs_scheduler.SetSchedConfig("default", "weightedrandom");
  fs_scheduler.SetSchedConfig("geotree");
  EXPECT_EQ(fs_scheduler.GetSchedConfig("default"),
            FlatConfig(PlacementStrategyT::kWeightedRandom));
  EXPECT_EQ(fs_scheduler.GetSchedConfig("tape"), kGeoTreeSchedConfig);
}

TEST(FsScheduler, FillLimitsValidation)
{
  using namespace eos::mgm::placement;
  FsScheduler fs_scheduler(2048, std::make_unique<TestClusterMgrHandler>());
  fs_scheduler.UpdateClusterData();
  // An unconfigured space reports the compile time defaults
  auto limits = fs_scheduler.GetFillLimits("default");
  EXPECT_EQ(limits.cap, kDefaultFillCapPercent);
  EXPECT_EQ(limits.warn, kDefaultFillWarnPercent);
  // Not a percentage, warn at or above the cap, empty space: all rejected
  EXPECT_FALSE(fs_scheduler.SetFillRatioLimit("default", 101));
  EXPECT_FALSE(fs_scheduler.SetFillRatioWarn("default", kDefaultFillCapPercent));
  EXPECT_FALSE(fs_scheduler.SetFillLimits("", 60, 40));
  // Lowering the cap below the configured warning level needs the warning
  // level lowered first - single key updates validate against the stored pair
  EXPECT_FALSE(fs_scheduler.SetFillRatioLimit("default", kDefaultFillWarnPercent));
  EXPECT_TRUE(fs_scheduler.SetFillRatioWarn("default", 40));
  EXPECT_TRUE(fs_scheduler.SetFillRatioLimit("default", 60));
  limits = fs_scheduler.GetFillLimits("default");
  EXPECT_EQ(limits.cap, 60);
  EXPECT_EQ(limits.warn, 40);
  // Nothing above leaked onto other spaces
  limits = fs_scheduler.GetFillLimits("tape");
  EXPECT_EQ(limits.cap, kDefaultFillCapPercent);
  EXPECT_EQ(limits.warn, kDefaultFillWarnPercent);
}

TEST(FsScheduler, FillLimitsSurviveRebuild)
{
  using namespace eos::mgm::placement;
  FsScheduler fs_scheduler(2048, std::make_unique<TestClusterMgrHandler>());
  fs_scheduler.UpdateClusterData();
  ASSERT_TRUE(fs_scheduler.SetFillLimits("default", 60, 40));
  // The configured pair lands on the live snapshot ...
  EXPECT_NE(
      fs_scheduler.GetState("default", "disk").find("fill limits: warn=40% cap=60%"),
      std::string::npos);
  // ... and gets re-stamped onto the fresh one a rebuild starts from defaults
  fs_scheduler.UpdateClusterData();
  EXPECT_NE(
      fs_scheduler.GetState("default", "disk").find("fill limits: warn=40% cap=60%"),
      std::string::npos);
}

TEST(FsScheduler, GetSpaces)
{
  FsScheduler fs_scheduler(2048, std::make_unique<TestClusterMgrHandler>());
  EXPECT_TRUE(fs_scheduler.GetSpaces().empty());
  fs_scheduler.UpdateClusterData();
  auto spaces = fs_scheduler.GetSpaces();
  ASSERT_EQ(spaces.size(), 1u);
  EXPECT_EQ(spaces[0], "default");
}

TEST(FsScheduler, SpaceStateSummary)
{
  using namespace eos::mgm::placement;
  FsScheduler fs_scheduler(2048, std::make_unique<TestClusterMgrHandler>());
  fs_scheduler.UpdateClusterData();
  fs_scheduler.SetSchedConfig("default", "weightedrandom");
  const auto state = fs_scheduler.GetSpaceState("default");
  EXPECT_NE(state.find("running   : yes"), std::string::npos) << state;
  EXPECT_NE(state.find("strategy  : flat:weightedrandom "
                       "(space override, global default: geotree)"),
            std::string::npos)
      << state;
  EXPECT_NE(state.find("fill      : warn=80% cap=95% (defaults)"), std::string::npos)
      << state;
  EXPECT_NE(state.find("topology  : 32 groups, 512 disks"), std::string::npos) << state;
  EXPECT_NE(state.find("status    : 512 online / 0 offline; "
                       "512 rw, 0 ro, 0 drain, 0 other"),
            std::string::npos)
      << state;
  EXPECT_NE(state.find("weight    : capacity=512 effective=512"), std::string::npos)
      << state;
  EXPECT_NE(state.find("space     : free=0 GiB booked=0 GiB writable=0 GiB"),
            std::string::npos)
      << state;
  // An unknown space reports its configuration but has no topology
  const auto unknown = fs_scheduler.GetSpaceState("tape");
  EXPECT_NE(unknown.find("topology  : none"), std::string::npos) << unknown;
}

TEST(FsScheduler, SpaceStateTracksDiskChanges)
{
  using namespace eos::mgm::placement;
  FsScheduler fs_scheduler(2048, std::make_unique<TestClusterMgrHandler>());
  fs_scheduler.UpdateClusterData();
  // One disk offline, one read-only, one closed to client traffic but still
  // open to the internal engines, and one full enough to lose its weight
  ASSERT_TRUE(fs_scheduler.SetDiskStatus("default", 1, ActiveStatus::kOffline,
                                         eos::common::BootStatus::kBooted));
  ASSERT_TRUE(fs_scheduler.SetDiskOps("default", 2, kOpsReadOnly));
  ASSERT_TRUE(fs_scheduler.SetDiskOps(
      "default", 4, eos::common::MaskOfActivity(SchedActivity::kInternal)));
  ASSERT_TRUE(fs_scheduler.SetDiskPercentUsed("default", 3, 100));
  ASSERT_TRUE(fs_scheduler.SetFillLimits("default", 90, 70));
  const auto state = fs_scheduler.GetSpaceState("default");
  EXPECT_NE(state.find("fill      : warn=70% cap=90% "
                       "(defaults warn=80% cap=95%)"),
            std::string::npos)
      << state;
  // A mask with no client bit but some internal bit is what the legacy drain
  // status projects to, so it lands in the drain column
  EXPECT_NE(state.find("status    : 511 online / 1 offline; "
                       "510 rw, 1 ro, 1 drain, 0 other"),
            std::string::npos)
      << state;
  // The full disk took its weight out of the effective total
  EXPECT_NE(state.find("weight    : capacity=512 effective=511"), std::string::npos)
      << state;
}

TEST(FsScheduler, LoweredFillCapStopsPlacement)
{
  using namespace eos::mgm::placement;
  FsScheduler fs_scheduler(2048, std::make_unique<TestClusterMgrHandler>());
  fs_scheduler.UpdateClusterData();
  fs_scheduler.SetSchedConfig("weightedrandom");

  // Every disk sits at 70%, below the default cap of 95 - placement works
  for (int fsid = 1; fsid <= 32 * 16; ++fsid) {
    ASSERT_TRUE(fs_scheduler.SetDiskPercentUsed("default", fsid, 70));
  }

  ASSERT_TRUE(fs_scheduler.Schedule("default", 2));
  // Lowering the cap to 60 puts every disk above it and dries the space up,
  // with no change to any disk in between
  ASSERT_TRUE(fs_scheduler.SetFillLimits("default", 60, 40));
  auto result = fs_scheduler.Schedule("default", 2);
  ASSERT_FALSE(result);
}

//------------------------------------------------------------------------------
// Geo-tagged variant of the test handler: 4 groups of 8 disks, the first 4 of
// each in site0, the rest in site1. fsids run 1..32 group by group, so a disk
// sits in site0 iff ((fsid - 1) % 8) < 4.
//------------------------------------------------------------------------------
struct GeoTestClusterMgrHandler : public eos::mgm::placement::ClusterMgrHandler {
  static void
  BuildDefault(ClusterMgr& mgr)
  {
    using namespace eos::mgm::placement;
    std::vector<FsDescription> fs_list;
    eos::mgm::placement::fsid_t fsid = 1;

    for (unsigned int g = 0; g < 4; ++g) {
      for (unsigned int s = 0; s < 2; ++s) {
        for (unsigned int d = 0; d < 4; ++d) {
          FsDescription desc;
          desc.fsid = fsid++;
          desc.group_index = g;
          desc.geotag = "site" + std::to_string(s);
          desc.capacity = 1ULL << 40;
          desc.free_bytes = desc.capacity;
          desc.ops = kMaskAll;
          desc.active_status = ActiveStatus::kOnline;
          fs_list.push_back(std::move(desc));
        }
      }
    }

    BuildClusterData(mgr, fs_list);
  }

  eos::mgm::placement::ClusterMgrMapT
  MakeClusterMgr(const eos::mgm::placement::ClusterMgrMapT& existing) override
  {
    eos::mgm::placement::ClusterMgrMapT cluster_map;
    std::shared_ptr<ClusterMgr> mgr;

    if (auto it = existing.find("default"); it != existing.end()) {
      mgr = it->second;
    } else {
      mgr = std::make_shared<ClusterMgr>();
    }

    BuildDefault(*mgr);
    cluster_map.insert_or_assign("default", std::move(mgr));
    return cluster_map;
  }
};

//------------------------------------------------------------------------------
// Check if a disk of the geo test cluster sits in site0
//------------------------------------------------------------------------------
static bool
InSite0(eos::mgm::placement::ItemIdT fsid)
{
  return ((fsid - 1) % 8) < 4;
}

TEST(FsScheduler, DisabledBranchRules)
{
  using namespace eos::mgm::placement;
  FsScheduler fs_scheduler(1024, std::make_unique<GeoTestClusterMgrHandler>());
  fs_scheduler.UpdateClusterData();
  // Empty space, empty geotag and an empty operations mask are all rejected
  EXPECT_FALSE(fs_scheduler.AddDisabledBranch("", "site0", kDenyPlct));
  EXPECT_FALSE(fs_scheduler.AddDisabledBranch("default", "", kDenyPlct));
  EXPECT_FALSE(fs_scheduler.AddDisabledBranch("default", "::", kDenyPlct));
  EXPECT_FALSE(fs_scheduler.AddDisabledBranch("default", "site0", 0));
  EXPECT_TRUE(fs_scheduler.GetDisabledBranches("default").empty());
  // The geotag is canonicalized, so a trailing separator cannot make the same
  // branch show up twice or dodge a later rm
  ASSERT_TRUE(fs_scheduler.AddDisabledBranch("default", "site0::", kDenyPlct));
  auto rules = fs_scheduler.GetDisabledBranches("default");
  ASSERT_EQ(rules.size(), 1u);
  EXPECT_EQ(rules.count("site0"), 1u);
  EXPECT_EQ(rules["site0"], kDenyPlct);
  // Adding the other operation merges into one rule
  ASSERT_TRUE(fs_scheduler.AddDisabledBranch("default", "site0", kDenyAccess));
  rules = fs_scheduler.GetDisabledBranches("default");
  ASSERT_EQ(rules.size(), 1u);
  EXPECT_EQ(rules["site0"], kDenyAll);
  // Removing takes single operations back out, and only ones actually set
  ASSERT_TRUE(fs_scheduler.RmDisabledBranch("default", "site0", kDenyPlct));
  EXPECT_FALSE(fs_scheduler.RmDisabledBranch("default", "site0", kDenyPlct));
  EXPECT_FALSE(fs_scheduler.RmDisabledBranch("default", "nowhere", kDenyAll));
  EXPECT_FALSE(fs_scheduler.RmDisabledBranch("tape", "site0", kDenyAll));
  rules = fs_scheduler.GetDisabledBranches("default");
  EXPECT_EQ(rules["site0"], kDenyAccess);
  // Removing the last operation drops the rule and the space entry with it
  ASSERT_TRUE(fs_scheduler.RmDisabledBranch("default", "site0", kDenyAccess));
  EXPECT_TRUE(fs_scheduler.GetDisabledBranches("default").empty());
  EXPECT_TRUE(fs_scheduler.GetAllDisabledBranches().empty());
}

TEST(FsScheduler, DisabledBranchesSurviveRebuild)
{
  using namespace eos::mgm::placement;
  FsScheduler fs_scheduler(1024, std::make_unique<GeoTestClusterMgrHandler>());
  fs_scheduler.UpdateClusterData();
  fs_scheduler.SetSchedConfig("roundrobin");
  ASSERT_TRUE(fs_scheduler.AddDisabledBranch("default", "site0", kDenyPlct));

  auto avoids_site0 = [&]() {
    for (int i = 0; i < 50; ++i) {
      auto result = fs_scheduler.Schedule("default", 2);

      if (!result) {
        return testing::AssertionFailure() << result.ErrorString();
      }

      for (int j = 0; j < result.n_filled; ++j) {
        if (InSite0(result.ids[j])) {
          return testing::AssertionFailure()
                 << "replica " << result.ids[j] << " landed in the disabled site";
        }
      }
    }

    return testing::AssertionSuccess();
  };
  // The rule lands on the live snapshot ...
  EXPECT_TRUE(avoids_site0());
  // ... and gets re-resolved onto the fresh one a rebuild starts from scratch
  fs_scheduler.UpdateClusterData();
  EXPECT_TRUE(avoids_site0());
  // Re-enabling brings the site back into rotation
  ASSERT_TRUE(fs_scheduler.RmDisabledBranch("default", "site0", kDenyPlct));
  bool site0_seen = false;

  for (int i = 0; (i < 100) && !site0_seen; ++i) {
    auto result = fs_scheduler.Schedule("default", 2);
    ASSERT_TRUE(result) << result.ErrorString();

    for (int j = 0; j < result.n_filled; ++j) {
      site0_seen = site0_seen || InSite0(result.ids[j]);
    }
  }

  EXPECT_TRUE(site0_seen) << "the re-enabled site never got a replica";
}

TEST(FsScheduler, SpaceStateShowsDisabledBranches)
{
  using namespace eos::mgm::placement;
  FsScheduler fs_scheduler(1024, std::make_unique<GeoTestClusterMgrHandler>());
  fs_scheduler.UpdateClusterData();
  EXPECT_NE(fs_scheduler.GetSpaceState("default").find("disabled  : none"),
            std::string::npos);
  ASSERT_TRUE(fs_scheduler.AddDisabledBranch("default", "site0", kDenyPlct));
  ASSERT_TRUE(fs_scheduler.AddDisabledBranch("default", "site1", kDenyAll));
  const auto state = fs_scheduler.GetSpaceState("default");
  EXPECT_NE(state.find("disabled  : site0=plct, site1=all"), std::string::npos) << state;
}

//------------------------------------------------------------------------------
// Describe one file system for the incremental tests, everything healthy
//------------------------------------------------------------------------------
static eos::mgm::placement::FsDescription
MakeFsDesc(uint32_t fsid, unsigned int group_index, const std::string& geotag)
{
  eos::mgm::placement::FsDescription desc;
  desc.fsid = fsid;
  desc.group_index = group_index;
  desc.geotag = geotag;
  desc.capacity = 1ULL << 40;
  desc.free_bytes = desc.capacity;
  desc.ops = eos::mgm::placement::kMaskAll;
  desc.active_status = eos::mgm::placement::ActiveStatus::kOnline;
  return desc;
}

TEST(FsScheduler, InsertFsUpdatesTheLiveSpace)
{
  using namespace eos::mgm::placement;
  FsScheduler fs_scheduler(1024, std::make_unique<GeoTestClusterMgrHandler>());
  const auto desc = MakeFsDesc(33, 0, "site0");
  // before the first full build the hook is a no-op - that build absorbs the
  // boot time registrations
  EXPECT_FALSE(fs_scheduler.InsertFs("default", desc));
  fs_scheduler.UpdateClusterData();
  fs_scheduler.SetSchedConfig("roundrobin");
  ASSERT_TRUE(fs_scheduler.SetFillLimits("default", 60, 40));
  EXPECT_FALSE(fs_scheduler.InsertFs("", desc));
  ASSERT_TRUE(fs_scheduler.InsertFs("default", desc));
  // the per disk update stream now reaches the new disk
  EXPECT_TRUE(fs_scheduler.SetDiskPercentUsed("default", 33, 10));
  // and it takes part in placement
  bool seen = false;

  for (int i = 0; (i < 200) && !seen; ++i) {
    auto result = fs_scheduler.Schedule("default", 2);
    ASSERT_TRUE(result) << result.ErrorString();

    for (int j = 0; j < result.n_filled; ++j) {
      seen = seen || (result.ids[j] == 33);
    }
  }

  EXPECT_TRUE(seen) << "the inserted disk never got a replica";
  // a re-insert replaces rather than duplicates
  ASSERT_TRUE(fs_scheduler.InsertFs("default", desc));
  EXPECT_NE(fs_scheduler.GetSpaceState("default").find("4 groups, 33 disks"),
            std::string::npos);
  // the configured fill thresholds survived both snapshot copies
  EXPECT_NE(
      fs_scheduler.GetState("default", "disk").find("fill limits: warn=40% cap=60%"),
      std::string::npos);
}

TEST(FsScheduler, RemoveFsUpdatesTheLiveSpace)
{
  using namespace eos::mgm::placement;
  FsScheduler fs_scheduler(1024, std::make_unique<GeoTestClusterMgrHandler>());
  fs_scheduler.UpdateClusterData();
  fs_scheduler.SetSchedConfig("roundrobin");
  EXPECT_FALSE(fs_scheduler.RemoveFs("default", 99));
  EXPECT_FALSE(fs_scheduler.RemoveFs("nospace", 1));
  ASSERT_TRUE(fs_scheduler.RemoveFs("default", 1));
  EXPECT_FALSE(fs_scheduler.RemoveFs("default", 1));
  EXPECT_NE(fs_scheduler.GetSpaceState("default").find("4 groups, 31 disks"),
            std::string::npos);

  for (int i = 0; i < 50; ++i) {
    auto result = fs_scheduler.Schedule("default", 2);
    ASSERT_TRUE(result) << result.ErrorString();

    for (int j = 0; j < result.n_filled; ++j) {
      EXPECT_NE(result.ids[j], 1) << "the removed disk got a replica";
    }
  }
}

TEST(FsScheduler, InsertFsCreatesANewSpace)
{
  using namespace eos::mgm::placement;
  FsScheduler fs_scheduler(1024, std::make_unique<GeoTestClusterMgrHandler>());
  fs_scheduler.UpdateClusterData();
  // thresholds stored before the space has any topology
  ASSERT_TRUE(fs_scheduler.SetFillLimits("newspace", 60, 40));
  ASSERT_TRUE(fs_scheduler.InsertFs("newspace", MakeFsDesc(1, 0, "siteX")));
  const auto spaces = fs_scheduler.GetSpaces();
  EXPECT_EQ(spaces.size(), 2u);
  fs_scheduler.SetSchedConfig("newspace", "roundrobin");
  auto result = fs_scheduler.Schedule("newspace", 1);
  ASSERT_TRUE(result) << result.ErrorString();
  EXPECT_EQ(result.ids[0], 1);
  // the stored thresholds were stamped onto the new snapshot
  EXPECT_NE(
      fs_scheduler.GetState("newspace", "disk").find("fill limits: warn=40% cap=60%"),
      std::string::npos);
  // and the pre-existing space is untouched
  EXPECT_NE(fs_scheduler.GetSpaceState("default").find("4 groups, 32 disks"),
            std::string::npos);
}

TEST(FsScheduler, InsertedBranchHonoursAStoredDisabledRule)
{
  using namespace eos::mgm::placement;
  FsScheduler fs_scheduler(1024, std::make_unique<GeoTestClusterMgrHandler>());
  fs_scheduler.UpdateClusterData();
  fs_scheduler.SetSchedConfig("roundrobin");
  // the rule names a branch that does not exist yet
  ASSERT_TRUE(fs_scheduler.AddDisabledBranch("default", "site2", kDenyPlct));
  // the insert creates it - and must resolve the rule onto it
  ASSERT_TRUE(fs_scheduler.InsertFs("default", MakeFsDesc(33, 0, "site2")));
  ASSERT_TRUE(fs_scheduler.InsertFs("default", MakeFsDesc(34, 0, "site2")));

  for (int i = 0; i < 100; ++i) {
    auto result = fs_scheduler.Schedule("default", 2);
    ASSERT_TRUE(result) << result.ErrorString();

    for (int j = 0; j < result.n_filled; ++j) {
      EXPECT_LT(result.ids[j], 33) << "a replica landed in the disabled branch";
    }
  }

  // lifting the rule opens the branch
  ASSERT_TRUE(fs_scheduler.RmDisabledBranch("default", "site2", kDenyPlct));
  bool seen = false;

  for (int i = 0; (i < 200) && !seen; ++i) {
    auto result = fs_scheduler.Schedule("default", 2);
    ASSERT_TRUE(result) << result.ErrorString();

    for (int j = 0; j < result.n_filled; ++j) {
      seen = seen || (result.ids[j] >= 33);
    }
  }

  EXPECT_TRUE(seen) << "the re-enabled branch never got a replica";
}

TEST(FsScheduler, PlacementCapacity)
{
  using namespace eos::mgm::placement;
  FsScheduler fs_scheduler(1024, std::make_unique<GeoTestClusterMgrHandler>());
  // not running yet - the bridge falls back to the legacy engine
  EXPECT_FALSE(fs_scheduler.GetPlacementCapacity("default").has_value());
  fs_scheduler.UpdateClusterData();
  // a space routed to the legacy engine gets its figure from that engine too
  EXPECT_FALSE(fs_scheduler.GetPlacementCapacity("default").has_value());
  fs_scheduler.SetSchedConfig("geo");
  // an unknown space has no topology to sum
  EXPECT_FALSE(fs_scheduler.GetPlacementCapacity("tape").has_value());
  // 32 disks with 1 TiB free each, converted to bytes
  const auto capacity = fs_scheduler.GetPlacementCapacity("default");
  ASSERT_TRUE(capacity.has_value());
  EXPECT_EQ(*capacity, 32 * (1ULL << 40));
}

TEST(FsScheduler, PlacementCapacityIsCached)
{
  using namespace eos::mgm::placement;
  using namespace std::chrono_literals;
  FsScheduler fs_scheduler(1024, std::make_unique<GeoTestClusterMgrHandler>());
  fs_scheduler.UpdateClusterData();
  fs_scheduler.SetSchedConfig("geo");
  fs_scheduler.SetCapacityCacheTTL(1h);
  ASSERT_EQ(fs_scheduler.GetPlacementCapacity("default").value(), 32 * (1ULL << 40));
  // an FST publish is an in-place update without an epoch bump, so within the
  // TTL the cached figure is served - that staleness is the price of not
  // paying the disk pass on every FUSE create
  ASSERT_TRUE(fs_scheduler.SetDiskFreeSpace("default", 1, 0));
  EXPECT_EQ(fs_scheduler.GetPlacementCapacity("default").value(), 32 * (1ULL << 40));
  // a zero TTL bypasses the cache and sees the publish
  fs_scheduler.SetCapacityCacheTTL(0ms);
  EXPECT_EQ(fs_scheduler.GetPlacementCapacity("default").value(), 31 * (1ULL << 40));
}

TEST(FsScheduler, PlacementCapacityTracksTopologyChanges)
{
  using namespace eos::mgm::placement;
  using namespace std::chrono_literals;
  FsScheduler fs_scheduler(1024, std::make_unique<GeoTestClusterMgrHandler>());
  fs_scheduler.UpdateClusterData();
  fs_scheduler.SetSchedConfig("geo");
  fs_scheduler.SetCapacityCacheTTL(1h);
  ASSERT_EQ(fs_scheduler.GetPlacementCapacity("default").value(), 32 * (1ULL << 40));
  // a topology change commits a new snapshot with an epoch bump, which
  // invalidates the cache immediately - no TTL wait
  ASSERT_TRUE(fs_scheduler.InsertFs("default", MakeFsDesc(33, 0, "site0")));
  EXPECT_EQ(fs_scheduler.GetPlacementCapacity("default").value(), 33 * (1ULL << 40));
  ASSERT_TRUE(fs_scheduler.RemoveFs("default", 33));
  ASSERT_TRUE(fs_scheduler.RemoveFs("default", 1));
  EXPECT_EQ(fs_scheduler.GetPlacementCapacity("default").value(), 31 * (1ULL << 40));
}

TEST(FsScheduler, PlacementCapacityCountsASharedBackendOnce)
{
  using namespace eos::mgm::placement;
  using namespace std::chrono_literals;
  FsScheduler fs_scheduler(1024, std::make_unique<GeoTestClusterMgrHandler>());
  fs_scheduler.UpdateClusterData();
  fs_scheduler.SetSchedConfig("geo");
  fs_scheduler.SetCapacityCacheTTL(0ms);
  ASSERT_EQ(fs_scheduler.GetPlacementCapacity("default").value(), 32 * (1ULL << 40));
  // Two file systems on one backing store both publish that store's free
  // space, so the pair adds one file system's worth of capacity, not two
  auto first = MakeFsDesc(33, 0, "site0");
  first.sharedfs = "cephfs";
  auto second = MakeFsDesc(34, 0, "site0");
  second.sharedfs = "cephfs";
  ASSERT_TRUE(fs_scheduler.InsertFs("default", first));
  ASSERT_TRUE(fs_scheduler.InsertFs("default", second));
  EXPECT_EQ(fs_scheduler.GetPlacementCapacity("default").value(), 33 * (1ULL << 40));
  // one on a backing store of its own is counted in full
  ASSERT_TRUE(fs_scheduler.InsertFs("default", MakeFsDesc(35, 0, "site0")));
  EXPECT_EQ(fs_scheduler.GetPlacementCapacity("default").value(), 34 * (1ULL << 40));
  // dropping one of the pair leaves the shared store counted, once
  ASSERT_TRUE(fs_scheduler.RemoveFs("default", 33));
  EXPECT_EQ(fs_scheduler.GetPlacementCapacity("default").value(), 34 * (1ULL << 40));
  ASSERT_TRUE(fs_scheduler.RemoveFs("default", 34));
  EXPECT_EQ(fs_scheduler.GetPlacementCapacity("default").value(), 33 * (1ULL << 40));
}
