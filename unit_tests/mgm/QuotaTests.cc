//------------------------------------------------------------------------------
// File: QuotaTests.cc
// Author: Cedric Caffy - CERN
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

#include "gtest/gtest.h"
#define IN_TEST_HARNESS
#include "mgm/quota/Quota.hh"
#undef IN_TEST_HARNESS
#include "namespace/interface/IQuota.hh"
#include <memory>

//------------------------------------------------------------------------------
// These tests cover the two parts of SpaceQuota which read counters out of the
// ns quota node without touching the namespace: the import of the usage
// counters and the decision whether a write fits. Everything else in the class
// resolves paths and evaluates space policies through gOFS, and belongs to the
// instance tests in test/eos-quota-test.
//
// What is pinned here that a shell test cannot pin:
//
//  - the exact boundaries of SpaceQuota::CheckWriteQuota, including the '+1'
//    of the inode check and the rule that a quota node with nothing configured
//    refuses a write. An instance test can only observe that a write is
//    refused eventually, since the file count at which that happens depends on
//    the layout size factor of the space.
//  - that the project usage refresh interval is per quota node. It used to be
//    kept in a function local static, therefore shared by every quota node, so
//    whichever node asked first won the interval and the others kept a project
//    usage which could be arbitrarily old - which is the value CheckWriteQuota
//    compares against the project target. Reproducing that from a shell needs
//    two project quota nodes written to inside the same interval.
//------------------------------------------------------------------------------

using eos::mgm::Quota;
using eos::mgm::SpaceQuota;

//------------------------------------------------------------------------------
//! A ns quota node which is not backed by a namespace. The counters are the
//! ones IQuotaNode already serves out of its QuotaNodeCore, so only the
//! mutators which would reach the namespace need to be stubbed out.
//------------------------------------------------------------------------------
class FakeQuotaNode : public eos::IQuotaNode {
public:
  explicit FakeQuotaNode(eos::IContainerMD::id_t id)
      : eos::IQuotaNode(nullptr, id)
  {
  }

  void
  addFile(const eos::IFileMD*) override
  {
  }
  void
  removeFile(const eos::IFileMD*) override
  {
  }
  void
  meld(const eos::IQuotaNode*) override
  {
  }
  void
  replaceCore(const eos::QuotaNodeCore&) override
  {
  }
  void
  updateCore(const eos::QuotaNodeCore&) override
  {
  }

  //----------------------------------------------------------------------------
  //! Account one file to uid/gid, as the namespace would on a commit
  //----------------------------------------------------------------------------
  void
  Account(uid_t uid, gid_t gid, uint64_t logical, uint64_t physical)
  {
    pCore.addFile(uid, gid, logical, physical);
  }
};

//------------------------------------------------------------------------------
//! Fixture holding one quota node and the SpaceQuota bound to it
//------------------------------------------------------------------------------
class SpaceQuotaTest : public ::testing::Test {
protected:
  static constexpr uid_t kUid = 1000;
  static constexpr gid_t kGid = 2000;

  void
  SetUp() override
  {
    mNode = std::make_unique<FakeQuotaNode>(1);
    mQuota = std::make_unique<SpaceQuota>("/eos/test/quota/", 1, mNode.get());
  }

  std::unique_ptr<FakeQuotaNode> mNode;
  std::unique_ptr<SpaceQuota> mQuota;
};

//------------------------------------------------------------------------------
// A quota node with nothing configured refuses a write - this is the
// "quota not defined or exhausted" the client is told about
//------------------------------------------------------------------------------
TEST_F(SpaceQuotaTest, NoQuotaConfiguredRefusesWrite)
{
  ASSERT_FALSE(mQuota->CheckWriteQuota(kUid, kGid, 1024, 1));
}

//------------------------------------------------------------------------------
// Root is exempt from every check
//------------------------------------------------------------------------------
TEST_F(SpaceQuotaTest, RootIsExempt)
{
  ASSERT_TRUE(mQuota->CheckWriteQuota(0, kGid, 1024, 1));
}

//------------------------------------------------------------------------------
// User volume quota - the headroom has to be strictly larger than the write
//------------------------------------------------------------------------------
TEST_F(SpaceQuotaTest, UserVolumeQuota)
{
  mQuota->SetQuota(SpaceQuota::kUserLogicalBytesTarget, kUid, 1000);
  // nothing used yet
  ASSERT_TRUE(mQuota->CheckWriteQuota(kUid, kGid, 100, 1));

  // 900 used, 100 left - a write of exactly the remaining 100 does not fit,
  // the comparison is strict
  mNode->Account(kUid, kGid, 900, 900);
  ASSERT_FALSE(mQuota->CheckWriteQuota(kUid, kGid, 100, 1));
  ASSERT_TRUE(mQuota->CheckWriteQuota(kUid, kGid, 99, 1));

  // exhausted
  mNode->Account(kUid, kGid, 100, 100);
  ASSERT_FALSE(mQuota->CheckWriteQuota(kUid, kGid, 1, 1));

  // the counters are read from the ns quota node on every call, so raising the
  // target is visible at once
  mQuota->SetQuota(SpaceQuota::kUserLogicalBytesTarget, kUid, 100000);
  ASSERT_TRUE(mQuota->CheckWriteQuota(kUid, kGid, 1, 1));
}

//------------------------------------------------------------------------------
// The volume check is on the logical size, not on the physical one
//------------------------------------------------------------------------------
TEST_F(SpaceQuotaTest, UserVolumeQuotaUsesLogicalSize)
{
  mQuota->SetQuota(SpaceQuota::kUserLogicalBytesTarget, kUid, 1000);
  // 500 logical stored as 1500 physical - a replicated layout. The logical
  // target still has 500 to give.
  mNode->Account(kUid, kGid, 500, 1500);
  mQuota->UpdateFromQuotaNode(kUid, kGid, false);
  ASSERT_EQ(500, mQuota->GetQuota(SpaceQuota::kUserLogicalBytesIs, kUid));
  ASSERT_EQ(1500, mQuota->GetQuota(SpaceQuota::kUserBytesIs, kUid));
  ASSERT_TRUE(mQuota->CheckWriteQuota(kUid, kGid, 400, 1));
  ASSERT_FALSE(mQuota->CheckWriteQuota(kUid, kGid, 500, 1));
}

//------------------------------------------------------------------------------
// User inode quota - pins the '+1' of the inode check, which is there because
// the file being placed is already accounted to the ns quota node by the open
//------------------------------------------------------------------------------
TEST_F(SpaceQuotaTest, UserInodeQuotaBoundary)
{
  mQuota->SetQuota(SpaceQuota::kUserFilesTarget, kUid, 2);
  ASSERT_TRUE(mQuota->CheckWriteQuota(kUid, kGid, 0, 1));

  // as many files as the target still fits, the current one is one of them
  mNode->Account(kUid, kGid, 1, 1);
  mNode->Account(kUid, kGid, 1, 1);
  mQuota->UpdateFromQuotaNode(kUid, kGid, false);
  ASSERT_EQ(2, mQuota->GetQuota(SpaceQuota::kUserFilesIs, kUid));
  ASSERT_TRUE(mQuota->CheckWriteQuota(kUid, kGid, 0, 1));

  // one over the target is where it stops
  mNode->Account(kUid, kGid, 1, 1);
  ASSERT_FALSE(mQuota->CheckWriteQuota(kUid, kGid, 0, 1));
}

//------------------------------------------------------------------------------
// A volume target and an inode target both have to be satisfied
//------------------------------------------------------------------------------
TEST_F(SpaceQuotaTest, VolumeAndInodeQuotaBothApply)
{
  mQuota->SetQuota(SpaceQuota::kUserLogicalBytesTarget, kUid, 1000);
  mQuota->SetQuota(SpaceQuota::kUserFilesTarget, kUid, 2);
  ASSERT_TRUE(mQuota->CheckWriteQuota(kUid, kGid, 100, 1));

  // volume is fine, inodes are not
  mNode->Account(kUid, kGid, 1, 1);
  mNode->Account(kUid, kGid, 1, 1);
  mNode->Account(kUid, kGid, 1, 1);
  ASSERT_FALSE(mQuota->CheckWriteQuota(kUid, kGid, 100, 1));
}

//------------------------------------------------------------------------------
// When a user and a group target are both defined, both have to be satisfied
//------------------------------------------------------------------------------
TEST_F(SpaceQuotaTest, UserAndGroupQuotaBothApply)
{
  mQuota->SetQuota(SpaceQuota::kUserLogicalBytesTarget, kUid, 1000);
  mQuota->SetQuota(SpaceQuota::kGroupLogicalBytesTarget, kGid, 1000);
  ASSERT_TRUE(mQuota->CheckWriteQuota(kUid, kGid, 100, 1));

  // the group is out of space although the user is not - a second uid of the
  // same group used it up
  mNode->Account(kUid + 1, kGid, 1000, 1000);
  mQuota->UpdateFromQuotaNode(kUid, kGid, false);
  ASSERT_EQ(0, mQuota->GetQuota(SpaceQuota::kUserLogicalBytesIs, kUid));
  ASSERT_EQ(1000, mQuota->GetQuota(SpaceQuota::kGroupLogicalBytesIs, kGid));
  ASSERT_FALSE(mQuota->CheckWriteQuota(kUid, kGid, 100, 1));
}

//------------------------------------------------------------------------------
// Group quota alone is enough to allow a write
//------------------------------------------------------------------------------
TEST_F(SpaceQuotaTest, GroupQuotaAlone)
{
  mQuota->SetQuota(SpaceQuota::kGroupLogicalBytesTarget, kGid, 1000);
  ASSERT_TRUE(mQuota->CheckWriteQuota(kUid, kGid, 100, 1));
  mNode->Account(kUid, kGid, 1000, 1000);
  ASSERT_FALSE(mQuota->CheckWriteQuota(kUid, kGid, 100, 1));
}

//------------------------------------------------------------------------------
// Project quota is the sum over every uid of the node, and is only consulted
// when neither a user nor a group target is defined for the writer
//------------------------------------------------------------------------------
TEST_F(SpaceQuotaTest, ProjectQuota)
{
  mQuota->SetQuota(SpaceQuota::kGroupLogicalBytesTarget, Quota::gProjectId, 1000);
  ASSERT_TRUE(mQuota->CheckWriteQuota(kUid, kGid, 100, 1));

  // three different users share the one project allowance. The refresh
  // interval is reset by hand each time, otherwise the calls below land inside
  // the interval opened by the one above and the sum is not walked again -
  // which is the throttle ProjectUsageIsThrottled covers.
  mNode->Account(kUid, kGid, 400, 400);
  mNode->Account(kUid + 1, kGid + 1, 400, 400);
  mQuota->mLastProjectRefresh = 0;
  ASSERT_TRUE(mQuota->CheckWriteQuota(kUid, kGid, 100, 1));
  ASSERT_EQ(800, mQuota->GetQuota(SpaceQuota::kGroupLogicalBytesIs, Quota::gProjectId));

  // the allowance is now used up, whoever of the three writes next
  mNode->Account(kUid + 2, kGid + 2, 200, 200);
  mQuota->mLastProjectRefresh = 0;
  ASSERT_FALSE(mQuota->CheckWriteQuota(kUid, kGid, 100, 1));
}

//------------------------------------------------------------------------------
// A user target takes precedence - the project allowance is then not consulted
//------------------------------------------------------------------------------
TEST_F(SpaceQuotaTest, ProjectQuotaIgnoredWhenUserQuotaIsDefined)
{
  mQuota->SetQuota(SpaceQuota::kGroupLogicalBytesTarget, Quota::gProjectId, 1000);
  mQuota->SetQuota(SpaceQuota::kUserLogicalBytesTarget, kUid, 100000);
  // the project allowance is exhausted by another user, the writer's own
  // target is not
  mNode->Account(kUid + 1, kGid, 1000, 1000);
  mQuota->mLastProjectRefresh = 0;
  ASSERT_TRUE(mQuota->CheckWriteQuota(kUid, kGid, 100, 1));
}

//------------------------------------------------------------------------------
// The project usage refresh interval is per quota node
//
// This is the regression test for the shared function local static: importing
// the counters of one quota node must not stop another quota node from
// importing its own.
//------------------------------------------------------------------------------
TEST_F(SpaceQuotaTest, ProjectUsageRefreshIsPerQuotaNode)
{
  FakeQuotaNode other_node(2);
  SpaceQuota other("/eos/test/other/", 2, &other_node);
  mQuota->SetQuota(SpaceQuota::kGroupLogicalBytesTarget, Quota::gProjectId, 10000);
  other.SetQuota(SpaceQuota::kGroupLogicalBytesTarget, Quota::gProjectId, 10000);
  mNode->Account(kUid, kGid, 111, 111);
  other_node.Account(kUid, kGid, 222, 222);

  // both imports happen well inside the refresh interval of each other
  mQuota->UpdateFromQuotaNode(kUid, kGid, true);
  other.UpdateFromQuotaNode(kUid, kGid, true);

  ASSERT_EQ(111, mQuota->GetQuota(SpaceQuota::kGroupLogicalBytesIs, Quota::gProjectId));
  ASSERT_EQ(222, other.GetQuota(SpaceQuota::kGroupLogicalBytesIs, Quota::gProjectId));
}

//------------------------------------------------------------------------------
// The interval still throttles - the sum is not walked on every call
//------------------------------------------------------------------------------
TEST_F(SpaceQuotaTest, ProjectUsageIsThrottled)
{
  mQuota->SetQuota(SpaceQuota::kGroupLogicalBytesTarget, Quota::gProjectId, 10000);
  mNode->Account(kUid, kGid, 111, 111);
  mQuota->UpdateFromQuotaNode(kUid, kGid, true);
  ASSERT_EQ(111, mQuota->GetQuota(SpaceQuota::kGroupLogicalBytesIs, Quota::gProjectId));

  // a second file, then an import inside the interval - the project sum keeps
  // the value it had
  mNode->Account(kUid, kGid, 222, 222);
  mQuota->UpdateFromQuotaNode(kUid, kGid, true);
  ASSERT_EQ(111, mQuota->GetQuota(SpaceQuota::kGroupLogicalBytesIs, Quota::gProjectId));

  // once the interval has elapsed the sum follows again
  mQuota->mLastProjectRefresh = 0;
  mQuota->UpdateFromQuotaNode(kUid, kGid, true);
  ASSERT_EQ(333, mQuota->GetQuota(SpaceQuota::kGroupLogicalBytesIs, Quota::gProjectId));
}

//------------------------------------------------------------------------------
// A write by the project gid bypasses the interval, so that a project space
// sees its own writes without waiting
//------------------------------------------------------------------------------
TEST_F(SpaceQuotaTest, ProjectUsageRefreshedForProjectGid)
{
  mQuota->SetQuota(SpaceQuota::kGroupLogicalBytesTarget, Quota::gProjectId, 10000);
  mNode->Account(kUid, kGid, 111, 111);
  mQuota->UpdateFromQuotaNode(kUid, kGid, true);
  ASSERT_EQ(111, mQuota->GetQuota(SpaceQuota::kGroupLogicalBytesIs, Quota::gProjectId));

  // inside the interval, but the writer is the project gid itself
  mNode->Account(kUid, Quota::gProjectId, 222, 222);
  mQuota->UpdateFromQuotaNode(kUid, Quota::gProjectId, true);
  ASSERT_EQ(333, mQuota->GetQuota(SpaceQuota::kGroupLogicalBytesIs, Quota::gProjectId));
}

//------------------------------------------------------------------------------
// Importing the counters of one identity leaves the others alone
//------------------------------------------------------------------------------
TEST_F(SpaceQuotaTest, ImportIsPerIdentity)
{
  mNode->Account(kUid, kGid, 100, 100);
  mNode->Account(kUid + 1, kGid + 1, 200, 200);
  mQuota->UpdateFromQuotaNode(kUid, kGid, false);
  ASSERT_EQ(100, mQuota->GetQuota(SpaceQuota::kUserLogicalBytesIs, kUid));
  // the other identity was not asked for and is therefore not there yet
  ASSERT_EQ(0, mQuota->GetQuota(SpaceQuota::kUserLogicalBytesIs, kUid + 1));

  mQuota->UpdateFromQuotaNode(kUid + 1, kGid + 1, false);
  ASSERT_EQ(200, mQuota->GetQuota(SpaceQuota::kUserLogicalBytesIs, kUid + 1));
  ASSERT_EQ(100, mQuota->GetQuota(SpaceQuota::kUserLogicalBytesIs, kUid));
}

//------------------------------------------------------------------------------
// A quota node which lost its ns quota node reports no usage at all, and the
// write is then allowed against an apparently untouched target
//
// This is why the binding to the ns quota node has to stay alive:
// UpdateFromQuotaNode is a no-op without it, so the decision is taken on stale
// or absent counters rather than being refused. Keep this pinned - if the
// binding is ever changed to be resolved lazily, this is what it costs.
//------------------------------------------------------------------------------
TEST_F(SpaceQuotaTest, DetachedQuotaNodeReportsNoUsage)
{
  SpaceQuota detached("/eos/test/detached/", 3, nullptr);
  detached.SetQuota(SpaceQuota::kUserLogicalBytesTarget, kUid, 1000);
  ASSERT_EQ(0, detached.GetQuota(SpaceQuota::kUserLogicalBytesIs, kUid));
  ASSERT_TRUE(detached.CheckWriteQuota(kUid, kGid, 100, 1));
}

//------------------------------------------------------------------------------
// The aggregate target rows are the sum of the per-id targets
//------------------------------------------------------------------------------
TEST_F(SpaceQuotaTest, TargetSums)
{
  mQuota->SetQuota(SpaceQuota::kUserLogicalBytesTarget, kUid, 100);
  mQuota->SetQuota(SpaceQuota::kUserLogicalBytesTarget, kUid + 1, 200);
  mQuota->SetQuota(SpaceQuota::kUserFilesTarget, kUid, 7);
  mQuota->SetQuota(SpaceQuota::kGroupLogicalBytesTarget, kGid, 400);
  mQuota->UpdateTargetSums();
  ASSERT_EQ(300, mQuota->GetQuota(SpaceQuota::kAllUserLogicalBytesTarget, 0));
  ASSERT_EQ(7, mQuota->GetQuota(SpaceQuota::kAllUserFilesTarget, 0));
  ASSERT_EQ(400, mQuota->GetQuota(SpaceQuota::kAllGroupLogicalBytesTarget, 0));

  // a target added afterwards is picked up by the next sum
  mQuota->SetQuota(SpaceQuota::kUserLogicalBytesTarget, kUid + 2, 300);
  mQuota->UpdateTargetSums();
  ASSERT_EQ(600, mQuota->GetQuota(SpaceQuota::kAllUserLogicalBytesTarget, 0));
}
