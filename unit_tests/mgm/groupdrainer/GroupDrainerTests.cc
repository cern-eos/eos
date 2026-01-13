#include "gtest/gtest.h"
#include "mgm/groupdrainer/GroupDrainer.hh"

using eos::mgm::GroupDrainer;
using eos::mgm::GroupStatus;
using eos::mgm::fsutils::FsidStatus;
using eos::mgm::fsutils::fs_status_map_t;
using drain_fs_map_t = eos::mgm::GroupDrainer::drain_fs_map_t;

TEST(GroupDrainerStatus, DrainComplete)
{
  fs_status_map_t fsmap {{
      1, FsidStatus{
        eos::common::ActiveStatus::kOnline,
        eos::common::DrainStatus::kDrained}
    },
    {
      2, FsidStatus{
        eos::common::ActiveStatus::kOnline,
        eos::common::DrainStatus::kDrained}
    }
  };
  auto status = GroupDrainer::checkGroupDrainStatus(fsmap);
  ASSERT_EQ(status, GroupStatus::DRAINCOMPLETE);
  // Populate with a single offline drained entity
  fsmap.insert_or_assign(4, FsidStatus{eos::common::ActiveStatus::kOffline,
                                       eos::common::DrainStatus::kDrained});
  ASSERT_EQ(GroupStatus::OFF,
            GroupDrainer::checkGroupDrainStatus(fsmap));
  // Further entries won't change the offline outcome
  fsmap.insert_or_assign(3, FsidStatus{eos::common::ActiveStatus::kOnline,
                                       eos::common::DrainStatus::kDrainFailed});
  ASSERT_EQ(GroupStatus::OFF,
            GroupDrainer::checkGroupDrainStatus(fsmap));
  // Bring fs back online, since we've a single failed and rest drained, it
  // is a failed drain
  fsmap.insert_or_assign(4, FsidStatus{eos::common::ActiveStatus::kOnline,
                                       eos::common::DrainStatus::kDrained});
  ASSERT_EQ(GroupStatus::DRAINFAILED,
            GroupDrainer::checkGroupDrainStatus(fsmap));
}

TEST(GroupDrainerStatus, Offline)
{
  fs_status_map_t fsmap {{
      1, FsidStatus{
        eos::common::ActiveStatus::kOnline,
        eos::common::DrainStatus::kDrained}
    },
    {
      2, FsidStatus{
        eos::common::ActiveStatus::kOnline,
        eos::common::DrainStatus::kDrained}
    },
    {
      3, FsidStatus{
        eos::common::ActiveStatus::kOffline,
        eos::common::DrainStatus::kDrained}
    }
  };
  auto status = GroupDrainer::checkGroupDrainStatus(fsmap);
  ASSERT_EQ(status, GroupStatus::OFF);
  fsmap.insert_or_assign(4, FsidStatus{eos::common::ActiveStatus::kOnline,
                                       eos::common::DrainStatus::kDrainFailed});
  fsmap.insert_or_assign(5, FsidStatus{eos::common::ActiveStatus::kUndefined,
                                       eos::common::DrainStatus::kDrainExpired});
  ASSERT_EQ(GroupStatus::OFF,
            GroupDrainer::checkGroupDrainStatus(fsmap));
}

TEST(GroupDrainerStatus, Failed)
{
  fs_status_map_t fsmap {{
      1, FsidStatus{
        eos::common::ActiveStatus::kOnline,
        eos::common::DrainStatus::kDrained}
    },
    {
      2, FsidStatus{
        eos::common::ActiveStatus::kOnline,
        eos::common::DrainStatus::kDrained}
    },
    {
      3, FsidStatus{
        eos::common::ActiveStatus::kOnline,
        eos::common::DrainStatus::kDrainFailed}
    }
  };
  auto status = GroupDrainer::checkGroupDrainStatus(fsmap);
  ASSERT_EQ(status, GroupStatus::DRAINFAILED);
  // Not sure whether this is practical, but currently the method doesn't
  // specially parse this state
  fsmap.insert_or_assign(4, FsidStatus{eos::common::ActiveStatus::kUndefined,
                                       eos::common::DrainStatus::kDrained});
  ASSERT_EQ(GroupStatus::DRAINFAILED,
            GroupDrainer::checkGroupDrainStatus(fsmap));
}

TEST(GroupDrainerStatus, Online)
{
  // Catchall for everything else
  fs_status_map_t fsmap {{
      1, FsidStatus{
        eos::common::ActiveStatus::kOnline,
        eos::common::DrainStatus::kDrained}
    },
    {
      2, FsidStatus{
        eos::common::ActiveStatus::kOnline,
        eos::common::DrainStatus::kDrained}
    },
    {
      3, FsidStatus{
        eos::common::ActiveStatus::kOnline,
        eos::common::DrainStatus::kDrainFailed}
    }
  };
  auto status = GroupDrainer::checkGroupDrainStatus(fsmap);
  ASSERT_EQ(status, GroupStatus::DRAINFAILED);
  // Introduce one of the unknown drain states
  fsmap.insert_or_assign(4, FsidStatus{eos::common::ActiveStatus::kOnline,
                                       eos::common::DrainStatus::kDrainExpired});
  ASSERT_EQ(GroupStatus::ON,
            GroupDrainer::checkGroupDrainStatus(fsmap));
}

TEST(GroupDrainer, isDrainFSMapEmpty)
{
  ASSERT_TRUE(GroupDrainer::isDrainFSMapEmpty({}));
  drain_fs_map_t m = {{"group1", {}},
    {"group2", {}}
  };
  ASSERT_TRUE(GroupDrainer::isDrainFSMapEmpty(m));
  drain_fs_map_t m2 = {{"group1", {}},
    {"group2", {}},
    {"group3", {10}}
  };
  ASSERT_FALSE(GroupDrainer::isDrainFSMapEmpty(m2));
}