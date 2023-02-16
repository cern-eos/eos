// ----------------------------------------------------------------------
// File: ClusterMapFixture.hh
// Author: Abhishek Lekshmanan - CERN
// ----------------------------------------------------------------------

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


#ifndef EOS_CLUSTERMAPFIXTURE_HH
#define EOS_CLUSTERMAPFIXTURE_HH

#include "mgm/placement/ClusterMap.hh"
#include "gtest/gtest.h"

class SimpleClusterF : public ::testing::Test {
protected:
  void SetUp() override {
    using namespace eos::mgm::placement;
    auto sh = mgr.getStorageHandler();
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::ROOT), 0));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::SITE), -1, 0));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::SITE), -2, 0));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::GROUP), -100, -1));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::GROUP), -101, -1));
    ASSERT_TRUE(sh.addBucket(get_bucket_type(StdBucketType::GROUP), -102, -2));

    // Every group has 10 disks!
    for (int i=0; i < 30; i++) {
      ASSERT_TRUE(sh.addDisk(Disk(i+1, ConfigStatus::kRW, ActiveStatus::kOnline, 1), -100 - i/10));
    }

  }

  eos::mgm::placement::ClusterMgr mgr;
};



#endif // EOS_CLUSTERMAPFIXTURE_HH
