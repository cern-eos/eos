#include "gtest/gtest.h"
#include "mgm/GroupDrainer.hh"

using eos::mgm::GroupDrainer;
using eos::mgm::GroupDrainStatus;
using eos::mgm::fsutils::FsidStatus;
using eos::mgm::fsutils::fs_status_map_t;

TEST(GroupDrainerStatus, DrainComplete)
{
  fs_status_map_t fsmap {{1, FsidStatus{eos::common::ActiveStatus::kOnline,
                                       eos::common::DrainStatus::kDrained}},
                        {2, FsidStatus{eos::common::ActiveStatus::kOnline,
                                       eos::common::DrainStatus::kDrained}}
  };

  auto status = GroupDrainer::checkGroupDrainStatus(fsmap);
  ASSERT_EQ(status, GroupDrainStatus::COMPLETE);

  // Populate with a single offline drained entity
  fsmap.insert_or_assign(4, FsidStatus{eos::common::ActiveStatus::kOffline,
                                       eos::common::DrainStatus::kDrained});
  ASSERT_EQ(GroupDrainStatus::OFFLINE,
            GroupDrainer::checkGroupDrainStatus(fsmap));

  // Further entries won't change the offline outcome
  fsmap.insert_or_assign(3, FsidStatus{eos::common::ActiveStatus::kOnline,
                                       eos::common::DrainStatus::kDrainFailed});
  ASSERT_EQ(GroupDrainStatus::OFFLINE,
            GroupDrainer::checkGroupDrainStatus(fsmap));

  // Bring fs back online, since we've a single failed and rest drained, it
  // is a failed drain
  fsmap.insert_or_assign(4, FsidStatus{eos::common::ActiveStatus::kOnline,
                                       eos::common::DrainStatus::kDrained});
  ASSERT_EQ(GroupDrainStatus::FAILED,
            GroupDrainer::checkGroupDrainStatus(fsmap));
}

TEST(GroupDrainerStatus, Offline)
{
  fs_status_map_t fsmap {{1, FsidStatus{eos::common::ActiveStatus::kOnline,
                                       eos::common::DrainStatus::kDrained}},
                         {2, FsidStatus{eos::common::ActiveStatus::kOnline,
                                        eos::common::DrainStatus::kDrained}},
                         {3, FsidStatus{eos::common::ActiveStatus::kOffline,
                                        eos::common::DrainStatus::kDrained}}
  };

  auto status = GroupDrainer::checkGroupDrainStatus(fsmap);
  ASSERT_EQ(status, GroupDrainStatus::OFFLINE);

  fsmap.insert_or_assign(4, FsidStatus{eos::common::ActiveStatus::kOnline,
                                       eos::common::DrainStatus::kDrainFailed});
  fsmap.insert_or_assign(5, FsidStatus{eos::common::ActiveStatus::kUndefined,
                                       eos::common::DrainStatus::kDrainExpired});

  ASSERT_EQ(GroupDrainStatus::OFFLINE,
            GroupDrainer::checkGroupDrainStatus(fsmap));
}

TEST(GroupDrainerStatus, Failed)
{
  fs_status_map_t fsmap {{1, FsidStatus{eos::common::ActiveStatus::kOnline,
                                       eos::common::DrainStatus::kDrained}},
                        {2, FsidStatus{eos::common::ActiveStatus::kOnline,
                                       eos::common::DrainStatus::kDrained}},
                        {3, FsidStatus{eos::common::ActiveStatus::kOnline,
                                       eos::common::DrainStatus::kDrainFailed}}
  };

  auto status = GroupDrainer::checkGroupDrainStatus(fsmap);
  ASSERT_EQ(status, GroupDrainStatus::FAILED);

  // Not sure whether this is practical, but currently the method doesn't
  // specially parse this state
  fsmap.insert_or_assign(4, FsidStatus{eos::common::ActiveStatus::kUndefined,
                                       eos::common::DrainStatus::kDrained});
  ASSERT_EQ(GroupDrainStatus::FAILED,
            GroupDrainer::checkGroupDrainStatus(fsmap));
}

TEST(GroupDrainerStatus, Online)
{
  // Catchall for everything else
  fs_status_map_t fsmap {{1, FsidStatus{eos::common::ActiveStatus::kOnline,
                                       eos::common::DrainStatus::kDrained}},
                        {2, FsidStatus{eos::common::ActiveStatus::kOnline,
                                       eos::common::DrainStatus::kDrained}},
                        {3, FsidStatus{eos::common::ActiveStatus::kOnline,
                                       eos::common::DrainStatus::kDrainFailed}}
  };

  auto status = GroupDrainer::checkGroupDrainStatus(fsmap);
  ASSERT_EQ(status, GroupDrainStatus::FAILED);
  // Introduce one of the unknown drain states
  fsmap.insert_or_assign(4, FsidStatus{eos::common::ActiveStatus::kOnline,
                                       eos::common::DrainStatus::kDrainExpired});

  ASSERT_EQ(GroupDrainStatus::ONLINE,
            GroupDrainer::checkGroupDrainStatus(fsmap));
}