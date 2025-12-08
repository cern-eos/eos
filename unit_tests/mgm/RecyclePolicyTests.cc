//------------------------------------------------------------------------------
// File: RecyclePolicyTests.cc
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#define IN_TEST_HARNESS
#include "mgm/recycle/RecyclePolicy.hh"
#undef IN_TEST_HARNESS
#include "mgm/Quota.hh"
#include <map>

using ::testing::Return;
using ::testing::NiceMock;

//------------------------------------------------------------------------------
// MockRecyclePolicy
//------------------------------------------------------------------------------
class MockRecyclePolicy: public eos::mgm::RecyclePolicy
{
public:
  MOCK_METHOD((std::map<int, unsigned long long>), GetQuotaStats, (), (override));
  MOCK_METHOD(bool, StoreConfig, (), (override));
};

//------------------------------------------------------------------------------
// Recycle policy with no configuration
//------------------------------------------------------------------------------
TEST(RecyclePolicyTests, NoLimits)
{
  NiceMock<MockRecyclePolicy> mock_policy;
  ASSERT_FALSE(mock_policy.mEnforced);
  EXPECT_CALL(mock_policy, GetQuotaStats)
  .WillOnce(Return(std::map<int, unsigned long long>()));
  mock_policy.RefreshWatermarks();
  ASSERT_EQ(mock_policy.mLowInodeWatermark, 0ull);
  ASSERT_EQ(mock_policy.mLowSpaceWatermark,  0ull);
  // There are no space limits yet so clean up should be performed
  ASSERT_FALSE(mock_policy.IsWithinLimits());
}

//------------------------------------------------------------------------------
// Recycle policy above the watermark limits
//------------------------------------------------------------------------------
TEST(RecyclePolicyTests, AboveWatermark)
{
  // Update the space keep ratio and the quota information
  // so that the limits are overrun
  NiceMock<MockRecyclePolicy> mock_policy;
  mock_policy.mSpaceKeepRatio = 0.4;
  EXPECT_CALL(mock_policy, GetQuotaStats)
  .Times(2)
  .WillRepeatedly(Return(std::map<int, unsigned long long> {
    {eos::mgm::SpaceQuota::kGroupLogicalBytesIs, 5000},
    {eos::mgm::SpaceQuota::kGroupLogicalBytesTarget, 10000},
    {eos::mgm::SpaceQuota::kGroupFilesIs, 100},
    {eos::mgm::SpaceQuota::kGroupFilesTarget, 200}
  }));
  mock_policy.RefreshWatermarks();
  ASSERT_DOUBLE_EQ((mock_policy.mSpaceKeepRatio - 0.1) * 10000,
                   mock_policy.mLowSpaceWatermark);
  ASSERT_DOUBLE_EQ((mock_policy.mSpaceKeepRatio - 0.1) * 200,
                   mock_policy.mLowInodeWatermark);
  ASSERT_FALSE(mock_policy.IsWithinLimits());
}

//------------------------------------------------------------------------------
// Recycle policy below the watermark limits
//------------------------------------------------------------------------------
TEST(RecyclePolicyTests, BelowWatermark)
{
  // Update the quota information so that we are within the limits
  NiceMock<MockRecyclePolicy> mock_policy;
  mock_policy.mSpaceKeepRatio = 0.4;
  EXPECT_CALL(mock_policy, GetQuotaStats)
  .Times(2)
  .WillRepeatedly(Return(std::map<int, unsigned long long> {
    {eos::mgm::SpaceQuota::kGroupLogicalBytesIs, 5000},
    {eos::mgm::SpaceQuota::kGroupLogicalBytesTarget, 10000},
    {eos::mgm::SpaceQuota::kGroupFilesIs, 100},
    {eos::mgm::SpaceQuota::kGroupFilesTarget, 200}
  }));
  mock_policy.RefreshWatermarks();
  ASSERT_DOUBLE_EQ((mock_policy.mSpaceKeepRatio - 0.1) * 10000,
                   mock_policy.mLowSpaceWatermark);
  ASSERT_DOUBLE_EQ((mock_policy.mSpaceKeepRatio - 0.1) * 200,
                   mock_policy.mLowInodeWatermark);
  ASSERT_FALSE(mock_policy.IsWithinLimits());
  // Update the quota information so that we are back withing the limits
  EXPECT_CALL(mock_policy, GetQuotaStats)
  .WillOnce(Return(std::map<int, unsigned long long> {
    {eos::mgm::SpaceQuota::kGroupLogicalBytesIs, 3000},
    {eos::mgm::SpaceQuota::kGroupLogicalBytesTarget, 10000},
    {eos::mgm::SpaceQuota::kGroupFilesIs, 50},
    {eos::mgm::SpaceQuota::kGroupFilesTarget, 200}
  }));
  ASSERT_TRUE(mock_policy.IsWithinLimits());
}

//------------------------------------------------------------------------------
// Recycle policy configuration tests
//------------------------------------------------------------------------------
TEST(RecyclePolicyTests, ConfigTest)
{
  NiceMock<MockRecyclePolicy> policy;
  EXPECT_CALL(policy, StoreConfig).WillRepeatedly(Return(true));
  std::string msg;
  // Test valid configurations
  ASSERT_TRUE(policy.Config(eos::mgm::RecyclePolicy::sKeepTimeKey, "3600", msg));
  ASSERT_EQ(policy.mKeepTimeSec, 3600ull);
  ASSERT_TRUE(policy.mEnforced);
  ASSERT_TRUE(policy.Config(eos::mgm::RecyclePolicy::sRatioKey, "0.5", msg));
  ASSERT_DOUBLE_EQ(policy.mSpaceKeepRatio, 0.5);
  ASSERT_TRUE(policy.mEnforced);
  ASSERT_TRUE(policy.Config(eos::mgm::RecyclePolicy::sCollectKey, "300", msg));
  ASSERT_EQ(policy.mCollectInterval.load().count(), 300);
  ASSERT_TRUE(policy.Config(eos::mgm::RecyclePolicy::sRemoveKey, "60", msg));
  ASSERT_EQ(policy.mRemoveInterval.load().count(), 60);
  ASSERT_TRUE(policy.Config(eos::mgm::RecyclePolicy::sDryRunKey, "yes", msg));
  ASSERT_TRUE(policy.mDryRun);
  ASSERT_TRUE(policy.Config(eos::mgm::RecyclePolicy::sDryRunKey, "no", msg));
  ASSERT_FALSE(policy.mDryRun);
  // Test invalid configurations
  ASSERT_FALSE(policy.Config(eos::mgm::RecyclePolicy::sKeepTimeKey, "invalid",
                             msg));
  ASSERT_FALSE(policy.Config(eos::mgm::RecyclePolicy::sRatioKey, "invalid", msg));
  ASSERT_FALSE(policy.Config(eos::mgm::RecyclePolicy::sCollectKey, "invalid",
                             msg));
  ASSERT_FALSE(policy.Config(eos::mgm::RecyclePolicy::sRemoveKey, "invalid",
                             msg));
  // Test reset/unenforce
  ASSERT_TRUE(policy.Config(eos::mgm::RecyclePolicy::sKeepTimeKey, "0", msg));
  ASSERT_TRUE(policy.Config(eos::mgm::RecyclePolicy::sRatioKey, "0.0", msg));
  ASSERT_FALSE(policy.mEnforced);
}

