//------------------------------------------------------------------------------
//! @file SchedulerBridgeTests.cc
//! @author Elvin Sindrilaru - CERN
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

//------------------------------------------------------------------------------
// Tests of the Scheduler.cc bridge between the MGM entry points and the flat
// scheduler: FlatSchedulerPlacement and FlatSchedulerAccess. The
// bridge is where the caller's inputs are translated into placement engine
// arguments, and historically where inputs were silently dropped - every case
// here guards one of those translations. The bridge is driven through the
// injectable overloads, so no gOFS instance is involved.
//------------------------------------------------------------------------------

#include "common/LayoutId.hh"
#include "common/VirtualIdentity.hh"
#include "mgm/placement/ClusterBuilder.hh"
#include "mgm/placement/FsScheduler.hh"
#include "mgm/scheduler/Scheduler.hh"
#include "gtest/gtest.h"
#include <set>

using eos::common::LayoutId;
using eos::mgm::Scheduler;
using eos::mgm::placement::ClusterMgr;
using eos::mgm::placement::FsScheduler;
using eos::mgm::placement::kFreeSpaceUnit;

namespace {

//------------------------------------------------------------------------------
// Describe one file system, everything but the identity has a usable default
//------------------------------------------------------------------------------
eos::mgm::placement::FsDescription
MakeFs(eos::mgm::placement::fsid_t fsid, unsigned int group_index,
       const std::string& geotag)
{
  eos::mgm::placement::FsDescription desc;
  desc.fsid = fsid;
  desc.group_index = group_index;
  desc.geotag = geotag;
  desc.capacity = 1ULL << 40;
  desc.free_bytes = 100 * kFreeSpaceUnit;
  desc.percent_used = 0;
  desc.ops = eos::mgm::placement::kMaskAll;
  desc.active_status = eos::mgm::placement::ActiveStatus::kOnline;
  return desc;
}

//------------------------------------------------------------------------------
// Build the topology of the "default" space: two scheduling groups, each with
// two disks at siteA and two at siteB.
//   group 0: fsids 1, 2 -> siteA          group 1: fsids 5, 6 -> siteA
//            fsids 3, 4 -> siteB                   fsids 7, 8 -> siteB
//------------------------------------------------------------------------------
struct BridgeClusterMgrHandler : public eos::mgm::placement::ClusterMgrHandler {
  static void
  BuildDefault(ClusterMgr& mgr)
  {
    std::vector<eos::mgm::placement::FsDescription> fs_list;

    for (eos::mgm::placement::fsid_t fsid = 1; fsid <= 8; ++fsid) {
      const unsigned int group = (fsid - 1) / 4;
      const bool site_a = (((fsid - 1) % 4) < 2);
      fs_list.push_back(MakeFs(fsid, group, site_a ? "siteA::room0" : "siteB::room0"));
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

//! Replica layout with two stripes
const unsigned long kReplica2Lid = LayoutId::GetId(LayoutId::kReplica, 1, 2);
//! RAID6 layout with six stripes, four of which must be online for a read
const unsigned long kRaid6Lid = LayoutId::GetId(LayoutId::kRaid6, 1, 6);

//------------------------------------------------------------------------------
// Fixture driving the bridge against an injected flat scheduler
//------------------------------------------------------------------------------
class SchedulerBridgeF : public ::testing::Test {
protected:
  void
  SetUp() override
  {
    mFsSched =
        std::make_unique<FsScheduler>(2048, std::make_unique<BridgeClusterMgrHandler>());
    mFsSched->UpdateClusterData();
    mFsSched->SetSchedConfig("geo");
    mVid.geolocation = "siteA::room0";
  }

  //----------------------------------------------------------------------------
  // Build placement arguments the way XrdMgmOfsFile does, everything wired to
  // the members of the fixture
  //----------------------------------------------------------------------------
  Scheduler::PlacementArguments
  MakePlctArgs(unsigned long lid = kReplica2Lid)
  {
    Scheduler::PlacementArguments args;
    args.setFileParams(mSpace, eos::mgm::scheduler::Path("/eos/test/file"),
                       eos::mgm::scheduler::GroupTag(nullptr),
                       eos::mgm::scheduler::Lid(lid), 1234,
                       eos::mgm::scheduler::BookingSize(1 * kFreeSpaceUnit), false, mVid);
    args.setFsParams(&mAlreadyUsed, &mExclude, &mSelected);
    return args;
  }

  //----------------------------------------------------------------------------
  // Build access arguments for a file with replicas on the given locations
  //----------------------------------------------------------------------------
  Scheduler::AccessArguments
  MakeAccessArgs(unsigned long lid = kReplica2Lid)
  {
    Scheduler::AccessArguments args;
    args.forcedspace = mSpace.c_str();
    args.lid = lid;
    args.inode = 1234;
    args.vid = &mVid;
    args.locationsfs = &mLocations;
    args.fsindex = &mFsIndex;
    args.unavailfs = &mUnavail;
    return args;
  }

  //----------------------------------------------------------------------------
  // Check that every selected file system is in the expected set
  //----------------------------------------------------------------------------
  void
  ExpectSelectedWithin(const std::set<unsigned int>& allowed)
  {
    for (const auto fsid : mSelected) {
      EXPECT_TRUE(allowed.count(fsid)) << "unexpected fsid " << fsid;
    }
  }

  std::unique_ptr<FsScheduler> mFsSched;
  eos::common::VirtualIdentity mVid;
  const std::string mSpace{"default"};
  std::vector<unsigned int> mAlreadyUsed;
  std::vector<unsigned int> mExclude;
  std::vector<unsigned int> mSelected;
  //! Replicas of the file the access cases operate on, group 0
  std::vector<unsigned int> mLocations{1, 2, 3, 4};
  unsigned long mFsIndex{99};
  std::vector<unsigned int> mUnavail;
};

//------------------------------------------------------------------------------
// Placement through the bridge
//------------------------------------------------------------------------------
TEST_F(SchedulerBridgeF, PlacesReplicaLayout)
{
  auto args = MakePlctArgs();
  ASSERT_EQ(Scheduler::FlatSchedulerPlacement(&args, *mFsSched), 0);
  // The number of replicas comes out of the layout id, two distinct disks
  ASSERT_EQ(mSelected.size(), 2u);
  EXPECT_NE(mSelected[0], mSelected[1]);
  ExpectSelectedWithin({1, 2, 3, 4, 5, 6, 7, 8});
}

TEST_F(SchedulerBridgeF, HonoursAlreadyUsedFilesystems)
{
  // The disks already holding a replica are just as ineligible as the
  // excluded ones - group 0 cannot serve two more replicas, so everything has
  // to come out of group 1
  mAlreadyUsed = {1, 2, 3, 4};

  for (int trial = 0; trial < 20; ++trial) {
    mSelected.clear();
    auto args = MakePlctArgs();
    args.inode = trial;
    ASSERT_EQ(Scheduler::FlatSchedulerPlacement(&args, *mFsSched), 0);
    ExpectSelectedWithin({5, 6, 7, 8});
  }
}

TEST_F(SchedulerBridgeF, HonoursExcludeFilesystems)
{
  mExclude = {5, 6, 7, 8};

  for (int trial = 0; trial < 20; ++trial) {
    mSelected.clear();
    auto args = MakePlctArgs();
    args.inode = trial;
    ASSERT_EQ(Scheduler::FlatSchedulerPlacement(&args, *mFsSched), 0);
    ExpectSelectedWithin({1, 2, 3, 4});
  }
}

TEST_F(SchedulerBridgeF, MergesExcludeAndAlreadyUsed)
{
  // Both lists apply at once; together they rule out all of group 0
  mExclude = {1, 2};
  mAlreadyUsed = {3, 4};

  for (int trial = 0; trial < 20; ++trial) {
    mSelected.clear();
    auto args = MakePlctArgs();
    args.inode = trial;
    ASSERT_EQ(Scheduler::FlatSchedulerPlacement(&args, *mFsSched), 0);
    ExpectSelectedWithin({5, 6, 7, 8});
  }
}

TEST_F(SchedulerBridgeF, BookingsizeTooLargeFails)
{
  // No disk has room for a booking twice its free space
  auto args = MakePlctArgs();
  args.bookingsize = 200 * kFreeSpaceUnit;
  EXPECT_EQ(Scheduler::FlatSchedulerPlacement(&args, *mFsSched), ENOSPC);
  // The engine ran and failed - the facade must time this as a flat sample
  EXPECT_TRUE(args.flat_engine_ran);
}

TEST_F(SchedulerBridgeF, BookingsizeSteersToDisksWithRoom)
{
  // The siteB disks report only 2 GiB free, an 8 GiB booking does not fit
  // there any more
  for (const auto fsid : {3u, 4u, 7u, 8u}) {
    ASSERT_TRUE(mFsSched->SetDiskFreeSpace(mSpace, fsid, 2 * kFreeSpaceUnit));
  }

  for (int trial = 0; trial < 20; ++trial) {
    mSelected.clear();
    auto args = MakePlctArgs();
    args.inode = trial;
    args.bookingsize = 8 * kFreeSpaceUnit;
    ASSERT_EQ(Scheduler::FlatSchedulerPlacement(&args, *mFsSched), 0);
    ExpectSelectedWithin({1, 2, 5, 6});
  }
}

TEST_F(SchedulerBridgeF, HonoursForcedGroupIndex)
{
  for (int trial = 0; trial < 20; ++trial) {
    mSelected.clear();
    auto args = MakePlctArgs();
    args.inode = trial;
    args.forced_scheduling_group_index = 1;
    ASSERT_EQ(Scheduler::FlatSchedulerPlacement(&args, *mFsSched), 0);
    ExpectSelectedWithin({5, 6, 7, 8});
  }
}

TEST_F(SchedulerBridgeF, TargetGeotagOverridesClientLocation)
{
  // A forced target geotag replaces the client's geolocation as the anchor
  // the gathered replicas collect around
  const std::string target = "siteB";

  for (int trial = 0; trial < 20; ++trial) {
    mSelected.clear();
    auto args = MakePlctArgs();
    args.inode = trial;
    args.plctpolicy = Scheduler::kGathered;
    args.plctTrgGeotag = &target;
    ASSERT_EQ(Scheduler::FlatSchedulerPlacement(&args, *mFsSched), 0);
    ExpectSelectedWithin({3, 4, 7, 8});
  }
}

TEST_F(SchedulerBridgeF, LegacyStrategyOptsOut)
{
  // Asking for the geotree engine is the one way left to opt out of the flat
  // scheduler, the bridge reports EINVAL and the caller falls through
  auto args = MakePlctArgs();
  args.sched_strategy_cstr = "geotree";
  EXPECT_EQ(Scheduler::FlatSchedulerPlacement(&args, *mFsSched), EINVAL);
  EXPECT_TRUE(mSelected.empty());
  // The engine never ran, so the facade must not file a flat timing sample -
  // otherwise every geotree space open drags the flat average towards zero
  EXPECT_FALSE(args.flat_engine_ran);
}

TEST_F(SchedulerBridgeF, OpaqueStrategyOverridesSpaceDefault)
{
  // The space default routes to the legacy engine, but the opaque string of
  // the request wins over it
  mFsSched->SetSchedConfig("geotree");
  auto args = MakePlctArgs();
  EXPECT_EQ(Scheduler::FlatSchedulerPlacement(&args, *mFsSched), EINVAL);
  EXPECT_FALSE(args.flat_engine_ran);
  args.sched_strategy_cstr = "rr";
  EXPECT_EQ(Scheduler::FlatSchedulerPlacement(&args, *mFsSched), 0);
  EXPECT_EQ(mSelected.size(), 2u);
  EXPECT_TRUE(args.flat_engine_ran);
}

TEST(SchedulerBridge, GetCollocatedReplicas)
{
  // scattered: one replica next to the client - if there is a client to be
  // next to
  EXPECT_EQ(Scheduler::GetCollocatedReplicas(Scheduler::kScattered, kReplica2Lid, true),
            1u);
  EXPECT_EQ(Scheduler::GetCollocatedReplicas(Scheduler::kScattered, kReplica2Lid, false),
            0u);
  // gathered: everything together
  EXPECT_EQ(Scheduler::GetCollocatedReplicas(Scheduler::kGathered, kReplica2Lid, true),
            2u);
  // hybrid: all but one replica together, and for RAIN all but the redundancy
  EXPECT_EQ(Scheduler::GetCollocatedReplicas(Scheduler::kHybrid, kReplica2Lid, true), 1u);
  EXPECT_EQ(Scheduler::GetCollocatedReplicas(Scheduler::kHybrid, kRaid6Lid, true), 4u);
}

//------------------------------------------------------------------------------
// Access through the bridge
//------------------------------------------------------------------------------
TEST_F(SchedulerBridgeF, AccessSelectsALocation)
{
  auto args = MakeAccessArgs();
  ASSERT_EQ(Scheduler::FlatSchedulerAccess(&args, *mFsSched), 0);
  EXPECT_LT(mFsIndex, mLocations.size());
}

TEST_F(SchedulerBridgeF, AccessForWriteSkipsNonWritable)
{
  // isRW selects the minimum config status: a read is happy with read-only
  // replicas, an update is not. An update to a replica-2 file also needs both
  // replicas writable - GetOnlineStripeNumber - so exactly two disks may be
  // taken away.
  ASSERT_TRUE(mFsSched->SetDiskOps(mSpace, 1, eos::mgm::placement::kOpsReadOnly));
  ASSERT_TRUE(mFsSched->SetDiskOps(mSpace, 2, eos::mgm::placement::kOpsReadOnly));

  auto args = MakeAccessArgs();
  args.isRW = true;
  ASSERT_EQ(Scheduler::FlatSchedulerAccess(&args, *mFsSched), 0);
  EXPECT_GE(mFsIndex, 2u) << "an update must land on a writable replica";

  // Taking a third disk away leaves fewer writable replicas than the update
  // needs, better to fail here than at the FST
  ASSERT_TRUE(mFsSched->SetDiskOps(mSpace, 3, eos::mgm::placement::kOpsReadOnly));
  mFsIndex = 99;
  auto down_args = MakeAccessArgs();
  down_args.isRW = true;
  EXPECT_EQ(Scheduler::FlatSchedulerAccess(&down_args, *mFsSched), ENETUNREACH);

  // The same replicas still serve a plain read
  mFsIndex = 99;
  mUnavail.clear();
  auto ro_args = MakeAccessArgs();
  EXPECT_EQ(Scheduler::FlatSchedulerAccess(&ro_args, *mFsSched), 0);
}

TEST_F(SchedulerBridgeF, AccessHonoursForcedFsid)
{
  auto args = MakeAccessArgs();
  args.forcedfsid = 2;
  ASSERT_EQ(Scheduler::FlatSchedulerAccess(&args, *mFsSched), 0);
  EXPECT_EQ(mFsIndex, 1u) << "forced fsid 2 lives at position 1";
}

TEST_F(SchedulerBridgeF, AccessHonoursExcludeFilesystems)
{
  const std::vector<uint32_t> exclude{1, 2, 3};
  auto args = MakeAccessArgs();
  args.exclude_filesystems = &exclude;
  ASSERT_EQ(Scheduler::FlatSchedulerAccess(&args, *mFsSched), 0);
  EXPECT_EQ(mFsIndex, 3u) << "only fsid 4 is not excluded";
}

TEST_F(SchedulerBridgeF, AccessRespectsPreflaggedUnavail)
{
  // Locations flagged unavailable before the call, e.g. by the tried-hosts
  // translation, are not handed back
  mUnavail = {1, 2, 3};
  auto args = MakeAccessArgs();
  ASSERT_EQ(Scheduler::FlatSchedulerAccess(&args, *mFsSched), 0);
  EXPECT_EQ(mFsIndex, 3u) << "only fsid 4 is not flagged";
}

TEST_F(SchedulerBridgeF, RainAccessRequiresEnoughStripes)
{
  // A 4+2 RAID6 read needs four online stripes. With exactly four locations
  // it works, and taking one of them down makes it fail rather than hand back
  // a stripe set the driver cannot reconstruct from.
  auto args = MakeAccessArgs(kRaid6Lid);
  ASSERT_EQ(Scheduler::FlatSchedulerAccess(&args, *mFsSched), 0);

  ASSERT_TRUE(mFsSched->SetDiskStatus(mSpace, 4,
                                      eos::mgm::placement::ActiveStatus::kOffline,
                                      eos::common::BootStatus::kBooted));
  mFsIndex = 99;
  mUnavail.clear();
  auto down_args = MakeAccessArgs(kRaid6Lid);
  EXPECT_EQ(Scheduler::FlatSchedulerAccess(&down_args, *mFsSched), ENETUNREACH);
  EXPECT_EQ(mFsIndex, 99u);
  // The down stripe is reported so the RAIN driver knows what to route around
  EXPECT_EQ(std::count(mUnavail.begin(), mUnavail.end(), 4u), 1);
}

TEST_F(SchedulerBridgeF, AccessLegacySpaceFallsThrough)
{
  mFsSched->SetSchedConfig("geotree");
  auto args = MakeAccessArgs();
  EXPECT_EQ(Scheduler::FlatSchedulerAccess(&args, *mFsSched), EINVAL);
  EXPECT_EQ(mFsIndex, 99u);
  // A routing rejection, not an engine failure - and on the access path the
  // return code alone cannot tell the two apart
  EXPECT_FALSE(args.flat_engine_ran);
}

TEST_F(SchedulerBridgeF, AccessWithoutForcedSpaceUsesDefaultStrategy)
{
  // A request without a forced space must not crash on the null pointer and
  // falls back to the global default strategy
  auto args = MakeAccessArgs();
  args.forcedspace = nullptr;
  // The global default is "geo", but the space lookup with an empty name finds
  // no topology, so the request is refused rather than served
  EXPECT_NE(Scheduler::FlatSchedulerAccess(&args, *mFsSched), 0);
  EXPECT_EQ(mFsIndex, 99u);
}

//------------------------------------------------------------------------------
// The eos.excludefsid post-filter applied on top of either engine's choice
//------------------------------------------------------------------------------
TEST_F(SchedulerBridgeF, ExclusionFilterRepointsAnExcludedChoice)
{
  const std::vector<uint32_t> exclude{2};
  auto args = MakeAccessArgs();
  args.exclude_filesystems = &exclude;
  mFsIndex = 1; // the engine chose fsid 2, which is excluded
  ASSERT_EQ(Scheduler::ApplyAccessExclusionFilter(&args), 0);
  EXPECT_NE(mFsIndex, 1u);
  EXPECT_LT(mFsIndex, mLocations.size());
}

TEST_F(SchedulerBridgeF, ExclusionFilterSkipsUnavailableAlternatives)
{
  const std::vector<uint32_t> exclude{1, 2};
  mUnavail = {3};
  auto args = MakeAccessArgs();
  args.exclude_filesystems = &exclude;
  mFsIndex = 0;
  ASSERT_EQ(Scheduler::ApplyAccessExclusionFilter(&args), 0);
  EXPECT_EQ(mFsIndex, 3u) << "fsid 4 is the only location neither excluded nor down";
}

TEST_F(SchedulerBridgeF, ExclusionFilterFailsWhenNothingIsLeft)
{
  const std::vector<uint32_t> exclude{1, 2, 3, 4};
  auto args = MakeAccessArgs();
  args.exclude_filesystems = &exclude;
  mFsIndex = 0;
  EXPECT_EQ(Scheduler::ApplyAccessExclusionFilter(&args), ENODATA);
}

TEST_F(SchedulerBridgeF, ExclusionFilterLeavesAnUnexcludedChoiceAlone)
{
  const std::vector<uint32_t> exclude{3};
  auto args = MakeAccessArgs();
  args.exclude_filesystems = &exclude;
  mFsIndex = 0;
  ASSERT_EQ(Scheduler::ApplyAccessExclusionFilter(&args), 0);
  EXPECT_EQ(mFsIndex, 0u);
}

//------------------------------------------------------------------------------
// Pure helpers of the Scheduler class
//------------------------------------------------------------------------------
TEST(SchedulerBridge, PlctPolicyFromString)
{
  EXPECT_EQ(Scheduler::PlctPolicyFromString("scattered"), Scheduler::kScattered);
  EXPECT_EQ(Scheduler::PlctPolicyFromString("hybrid"), Scheduler::kHybrid);
  EXPECT_EQ(Scheduler::PlctPolicyFromString("gathered"), Scheduler::kGathered);
  EXPECT_EQ(Scheduler::PlctPolicyFromString("nonsense"), -1);
}

TEST(SchedulerBridge, ReshuffleFs)
{
  // odd sum puts the lowest fsid first, even sum the highest
  std::vector<unsigned int> odd{2, 4, 1}; // sum 7
  Scheduler::ReshuffleFs(odd);
  EXPECT_EQ(odd.front(), 1u);
  ASSERT_EQ(odd.size(), 3u);

  std::vector<unsigned int> even{2, 4, 6}; // sum 12
  Scheduler::ReshuffleFs(even);
  EXPECT_EQ(even.front(), 6u);
  ASSERT_EQ(even.size(), 3u);
}

//------------------------------------------------------------------------------
// Resolving the traffic class of a request from eos.schedclass. The key is a
// scheduling privilege, so the interesting cases are the ones where it must
// *not* be granted.
//------------------------------------------------------------------------------
//! A trusted internal engine: sss-authenticated and mapped to the daemon uid
eos::common::VirtualIdentity
MakeDaemonVid()
{
  eos::common::VirtualIdentity vid;
  vid.prot = "sss";
  vid.uid = DAEMONUID;
  return vid;
}

TEST(SchedulerBridge, SchedClassInternalOnTrustedIdentity)
{
  const auto vid = MakeDaemonVid();
  EXPECT_EQ(Scheduler::SchedActivityFromRequest("internal", vid),
            eos::common::SchedActivity::kInternal);
}

TEST(SchedulerBridge, SchedClassAbsentOrUnrecognisedIsClient)
{
  const auto vid = MakeDaemonVid();
  // Absent - by far the common case, every plain client open
  EXPECT_EQ(Scheduler::SchedActivityFromRequest(nullptr, vid),
            eos::common::SchedActivity::kClient);
  // There is no way to *ask* for client class, it is what anything else means
  EXPECT_EQ(Scheduler::SchedActivityFromRequest("client", vid),
            eos::common::SchedActivity::kClient);
  EXPECT_EQ(Scheduler::SchedActivityFromRequest("", vid),
            eos::common::SchedActivity::kClient);
  EXPECT_EQ(Scheduler::SchedActivityFromRequest("Internal", vid),
            eos::common::SchedActivity::kClient);
}

TEST(SchedulerBridge, SchedClassIgnoredOnUntrustedIdentity)
{
  // A client that simply types the key gets no privilege out of it
  eos::common::VirtualIdentity user;
  user.prot = "krb5";
  user.uid = 12345;
  EXPECT_EQ(Scheduler::SchedActivityFromRequest("internal", user),
            eos::common::SchedActivity::kClient);
  // Right protocol, wrong account
  eos::common::VirtualIdentity sss_user;
  sss_user.prot = "sss";
  sss_user.uid = 12345;
  EXPECT_EQ(Scheduler::SchedActivityFromRequest("internal", sss_user),
            eos::common::SchedActivity::kClient);
  // Right account, wrong protocol
  eos::common::VirtualIdentity unix_daemon;
  unix_daemon.prot = "unix";
  unix_daemon.uid = DAEMONUID;
  EXPECT_EQ(Scheduler::SchedActivityFromRequest("internal", unix_daemon),
            eos::common::SchedActivity::kClient);
}

//------------------------------------------------------------------------------
// The label reaching the engine: a file system that takes no client traffic is
// still a valid target for an internal request. This is the headline case of
// the whole permission model, driven end to end through the bridge.
//------------------------------------------------------------------------------
TEST_F(SchedulerBridgeF, InternalPlacementUsesAClientFencedFilesystem)
{
  // Fence off group 1 entirely, and leave only fsid 4 of group 0 open to
  // internal traffic - a client placement of two replicas then has nowhere to
  // go, while an internal one can still fill group 0
  for (unsigned int fsid = 5; fsid <= 8; ++fsid) {
    ASSERT_TRUE(mFsSched->SetDiskOps(mSpace, fsid, eos::mgm::placement::kMaskNone));
  }

  const eos::common::FsOpMask internal_only =
      eos::common::MaskOfActivity(eos::common::SchedActivity::kInternal);
  ASSERT_TRUE(mFsSched->SetDiskOps(mSpace, 3, internal_only));
  ASSERT_TRUE(mFsSched->SetDiskOps(mSpace, 4, internal_only));
  ASSERT_TRUE(mFsSched->SetDiskOps(mSpace, 2, eos::mgm::placement::kMaskNone));
  // Client traffic sees a single usable disk, not enough for two replicas
  auto client_args = MakePlctArgs();
  EXPECT_NE(Scheduler::FlatSchedulerPlacement(&client_args, *mFsSched), 0);
  mSelected.clear();
  // The same request labelled internal is served by the fenced-off disks
  auto internal_args = MakePlctArgs();
  internal_args.setSchedActivity(eos::common::SchedActivity::kInternal);
  ASSERT_EQ(Scheduler::FlatSchedulerPlacement(&internal_args, *mFsSched), 0);
  ASSERT_EQ(mSelected.size(), 2u);
  ExpectSelectedWithin({1, 3, 4});
}

TEST_F(SchedulerBridgeF, InternalAccessReadsAClientFencedFilesystem)
{
  const eos::common::FsOpMask internal_only =
      eos::common::MaskOfActivity(eos::common::SchedActivity::kInternal);

  // Every replica of the file lives on a disk closed to client traffic
  for (const auto fsid : mLocations) {
    ASSERT_TRUE(mFsSched->SetDiskOps(mSpace, fsid, internal_only));
  }

  auto client_args = MakeAccessArgs();
  EXPECT_NE(Scheduler::FlatSchedulerAccess(&client_args, *mFsSched), 0);
  mUnavail.clear();
  auto internal_args = MakeAccessArgs();
  internal_args.activity = eos::common::SchedActivity::kInternal;
  EXPECT_EQ(Scheduler::FlatSchedulerAccess(&internal_args, *mFsSched), 0);
  EXPECT_LT(mFsIndex, mLocations.size());
}

} // anonymous namespace
