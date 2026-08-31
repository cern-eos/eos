#include "mgm/utils/FileSystemStatusUtils.hh"

#include <gtest/gtest.h>

namespace eos::mgm::fsutils {
namespace {

TEST(AutoDrainActions, IgnoresFilesystemsWithoutClientTraffic)
{
  auto actions = GetAutoDrainActions(eos::common::kMaskNone, false);
  EXPECT_FALSE(actions.set_read_only);
  EXPECT_FALSE(actions.request_drain);

  actions = GetAutoDrainActions(eos::common::kMaskInternalAll, false);
  EXPECT_FALSE(actions.set_read_only);
  EXPECT_FALSE(actions.request_drain);
}

TEST(AutoDrainActions, AppliesBothChangesFromReadWrite)
{
  const auto actions = GetAutoDrainActions(eos::common::kMaskAll, false);
  EXPECT_TRUE(actions.set_read_only);
  EXPECT_TRUE(actions.request_drain);
}

TEST(AutoDrainActions, RequestsDrainForReadOnlyFilesystem)
{
  const auto actions = GetAutoDrainActions(eos::common::kMaskAllReads, false);
  EXPECT_FALSE(actions.set_read_only);
  EXPECT_TRUE(actions.request_drain);
}

TEST(AutoDrainActions, RestrictsExistingDrainToReads)
{
  const auto actions = GetAutoDrainActions(eos::common::kMaskAll, true);
  EXPECT_TRUE(actions.set_read_only);
  EXPECT_FALSE(actions.request_drain);
}

TEST(AutoDrainActions, DoesNothingOnceDrainStateHasConverged)
{
  const auto actions = GetAutoDrainActions(eos::common::kMaskAllReads, true);
  EXPECT_FALSE(actions.set_read_only);
  EXPECT_FALSE(actions.request_drain);
}

} // namespace
} // namespace eos::mgm::fsutils
