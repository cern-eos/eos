//------------------------------------------------------------------------------
//! @file TreeSizeMetadataPublisherTest.cc
//! @brief Tests for publishing tree-size counters into namespace metadata
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

#include "namespace/interface/IContainerMDSvc.hh"
#include "namespace/interface/IView.hh"
#include "namespace/ns_quarkdb/accounting/tree_size/TreeSizePublisher.hh"
#include "namespace/ns_quarkdb/tests/TestUtils.hh"
#include <gtest/gtest.h>
#include <initializer_list>
#include <vector>

namespace {

class TreeSizeMetadataPublisherF : public eos::ns::testing::NsTestsFixture {};

eos::TreeSizePublishPlanResult
MakePlan(std::initializer_list<eos::TreeSizePublishTarget> targets)
{
  eos::TreeSizePublishPlanResult plan;
  plan.targets.assign(targets.begin(), targets.end());
  return plan;
}

} // namespace

TEST_F(TreeSizeMetadataPublisherF, AppliesAbsoluteCounters)
{
  auto container = view()->createContainer("/publisher/write", true);
  const auto container_id = container->getId();
  eos::TreeSizeMetadataPublisher metadata_publisher(*containerSvc());

  const auto result = eos::TreeSizePublisher().Apply(
      MakePlan({eos::TreeSizePublishTarget{container_id,
                                           eos::TreeSizeSubtreeCounters{123, 4, 5}}}),
      metadata_publisher);

  EXPECT_EQ(eos::TreeSizePublishApplyStatus::Success, result.status);
  EXPECT_EQ(1ull, result.attemptedTargets);
  EXPECT_EQ(0ull, result.skippedMissingTargets);
  EXPECT_TRUE(result.retryContainerIds.empty());
  EXPECT_TRUE(result.missingContainerIds.empty());
  EXPECT_TRUE(result.writeFailedContainerIds.empty());
  ASSERT_EQ(1u, result.publishedContainerIds.size());
  EXPECT_EQ(container_id, result.publishedContainerIds.front());

  auto stored = containerSvc()->getContainerMD(container_id);
  EXPECT_EQ(123ull, stored->getTreeSize());
  EXPECT_EQ(4ull, stored->getTreeFiles());
  EXPECT_EQ(5ull, stored->getTreeContainers());
}

TEST_F(TreeSizeMetadataPublisherF, IgnoresMissingContainerAndContinues)
{
  auto container = view()->createContainer("/publisher/after-missing", true);
  const auto container_id = container->getId();
  const auto missing_id = containerSvc()->getFirstFreeId() + 1000;
  eos::TreeSizeMetadataPublisher metadata_publisher(*containerSvc());

  const auto result = eos::TreeSizePublisher().Apply(
      MakePlan(
          {eos::TreeSizePublishTarget{missing_id, eos::TreeSizeSubtreeCounters{10, 1, 1}},
           eos::TreeSizePublishTarget{container_id,
                                      eos::TreeSizeSubtreeCounters{77, 8, 9}}}),
      metadata_publisher);

  EXPECT_EQ(eos::TreeSizePublishApplyStatus::Success, result.status);
  EXPECT_EQ(2ull, result.attemptedTargets);
  EXPECT_EQ(1ull, result.skippedMissingTargets);
  EXPECT_EQ((std::vector<uint64_t>{missing_id}), result.retryContainerIds);
  EXPECT_EQ((std::vector<uint64_t>{missing_id}), result.missingContainerIds);
  EXPECT_TRUE(result.writeFailedContainerIds.empty());
  ASSERT_EQ(1u, result.publishedContainerIds.size());
  EXPECT_EQ(container_id, result.publishedContainerIds.front());

  auto stored = containerSvc()->getContainerMD(container_id);
  EXPECT_EQ(77ull, stored->getTreeSize());
  EXPECT_EQ(8ull, stored->getTreeFiles());
  EXPECT_EQ(9ull, stored->getTreeContainers());
}
