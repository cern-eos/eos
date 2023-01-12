// ----------------------------------------------------------------------
// File: SchedulerTests.cc
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

#include "mgm/placement/Scheduler.hh"
#include "unit_tests/mgm/placement/ClusterMapFixture.hh"
#include "gtest/gtest.h"

TEST_F(SimpleClusterF, RoundRobinBasic)
{
  eos::mgm::placement::RoundRobinPlacement rr_placement(256);

  auto cluster_data_ptr = mgr.getClusterData();

  // TODO: write a higher level function to do recursive descent
  // Choose 1 site - from ROOT
  auto res = rr_placement.chooseItems(*cluster_data_ptr,{0,1});
  ASSERT_TRUE(res);
  EXPECT_EQ(res.ids.size(), 1);
  EXPECT_EQ(res.ids[0], -1);

  // Choose 1 group from SITE
  auto site_id = res.ids[0];
  auto group_res = rr_placement.chooseItems(*cluster_data_ptr,{site_id,1});
  ASSERT_TRUE(group_res);
  EXPECT_EQ(group_res.ids.size(), 1);


  // choose 2 disks from group!
  auto disks_res = rr_placement.chooseItems(*cluster_data_ptr,{group_res.ids[0],2});
  ASSERT_TRUE(disks_res);
  EXPECT_EQ(disks_res.ids.size(), 2);

}

TEST_F(SimpleClusterF, RoundRobinBasicLoop)
{
  eos::mgm::placement::RoundRobinPlacement rr_placement(256);

  auto cluster_data_ptr = mgr.getClusterData();

  std::map<int32_t,uint32_t> site_id_ctr;
  std::map<int32_t,uint32_t> group_id_ctr;
  std::map<int32_t,uint32_t> disk_id_ctr;
  std::vector<int32_t> disk_ids_vec;
  // TODO: write a higher level function to do recursive descent
  // Choose 1 site - from ROOT
  // Loop over 30 times, which is the total size of the disks to ensure that all
  // elements are chosen
  for (int i = 0; i < 30; i++)
  {
    auto res = rr_placement.chooseItems(*cluster_data_ptr, {0, 1});

    ASSERT_TRUE(res);
    ASSERT_EQ(res.ids.size(), 1);

    site_id_ctr[res.ids[0]]++;

    // Choose 1 group from SITE
    auto site_id = res.ids[0];
    auto group_res = rr_placement.chooseItems(*cluster_data_ptr, {site_id, 1});

    ASSERT_TRUE(group_res);
    ASSERT_EQ(group_res.ids.size(), 1);
    group_id_ctr[group_res.ids[0]]++;


    // choose 2 disks from group!
    auto disks_res =
        rr_placement.chooseItems(*cluster_data_ptr, {group_res.ids[0], 2});

    ASSERT_TRUE(disks_res);
    ASSERT_EQ(disks_res.ids.size(), 2);
    disk_id_ctr[disks_res.ids[0]]++;
    disk_id_ctr[disks_res.ids[1]]++;


    disk_ids_vec.push_back(disks_res.ids[0]);
    disk_ids_vec.push_back(disks_res.ids[1]);

  }

  // SITE1 gets 15 requests, SITE2 gets 15 requests;
  ASSERT_EQ(site_id_ctr[-1], 15);
  ASSERT_EQ(site_id_ctr[-2], 15);


  // 30 items chosen in site1 among 20 disks
  // 30 items chosen in site2 among 10 disks
  ASSERT_EQ(group_id_ctr[-102], 15);

  // This is a bit more involved to reason, actually just a consequence of an
  // empty starting cluster, where we'd expect roundrobin to start from the initial
  // elements, hence, group1 is chosen first, and thus gets a request extra
  // if you do the LCM you'd be able to reach a point where you'd schedule equally
  // group1 & group2; group3 would still have 2X requests if you RR over the sites first

  EXPECT_EQ(group_id_ctr[-100], 8);
  EXPECT_EQ(group_id_ctr[-101], 7);
  // All the disks are chosen at least once, due to the non uniform nature here,
  // site 2 would have its disks chosen twice as often as site 1
  ASSERT_EQ(disk_ids_vec.size(), 60);
  ASSERT_EQ(disk_id_ctr.size(), 30);



  // Check SITE1 ctr, atleast 1; initial disks would be twice as filled as latter
  for (int i=1; i <=20; i++) {
    ASSERT_GE(disk_id_ctr[i], 1);
  }

  // Check SITE2 ctr, all disks would've been scheduled twice, initial disks twice often as the others

  for (int i=21; i <=30; i++) {
    ASSERT_GE(disk_id_ctr[i],2);
  }
}

