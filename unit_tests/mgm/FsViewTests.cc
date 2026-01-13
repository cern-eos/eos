//------------------------------------------------------------------------------
// File: FsViewsTests.cc
// Author: Elvin-Alin Sindrilaru <esindril at cern dot ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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
#include "mgm/fsview/FsView.hh"
#define IN_TEST_HARNESS
#include "mgm/balancer/FsBalancerStats.hh"
#include "mgm/balancer/FsBalancer.hh"
#undef IN_TEST_HARNESS
#include "mgm/utils/FilesystemUuidMapper.hh"
#include "common/config/ConfigParsing.hh"
#include "common/StringUtils.hh"

//------------------------------------------------------------------------------
// Test const_iterator implementation
//------------------------------------------------------------------------------
TEST(FsView, ConstIteratorTest)
{
  using namespace eos::mgm;
  GeoTree geo_tree;

  for (int i = 0; i < 100; ++i) {
    ASSERT_TRUE(geo_tree.insert(i));
  }

  for (int i = 0; i < 10; ++i) {
    ASSERT_FALSE(geo_tree.insert(i));
  }

  for (auto iter = geo_tree.begin(); iter != geo_tree.end(); ++iter) {
    ASSERT_TRUE(*iter >= 0);
    ASSERT_TRUE(*iter < 100);
  }

  auto iter = geo_tree.begin();
  --iter;
  ASSERT_TRUE(iter == geo_tree.begin());

  while (iter != geo_tree.end()) {
    ++iter;
  }

  ++iter;
  ASSERT_TRUE(iter == geo_tree.end());
}

//------------------------------------------------------------------------------
// Test FilesystemUuidMapper
//------------------------------------------------------------------------------
TEST(FilesystemUuidMapper, BasicSanity)
{
  eos::mgm::FilesystemUuidMapper mapper;
  ASSERT_FALSE(mapper.injectMapping(0, "test"));
  ASSERT_EQ(mapper.size(), 0u);
  ASSERT_FALSE(mapper.injectMapping(0, "aaa"));
  ASSERT_EQ(mapper.size(), 0u);
  ASSERT_FALSE(mapper.injectMapping(1, ""));
  ASSERT_EQ(mapper.size(), 0u);
  ASSERT_TRUE(mapper.injectMapping(1, "fs-1"));
  ASSERT_EQ(mapper.size(), 1u);
  // conflict with fsid "1"
  ASSERT_FALSE(mapper.injectMapping(1, "fs-2"));
  ASSERT_EQ(mapper.size(), 1u);
  // conflict with uuid "fs-1"
  ASSERT_FALSE(mapper.injectMapping(2, "fs-1"));
  ASSERT_EQ(mapper.size(), 1u);
  // conflict with itself, fine, nothing changes
  ASSERT_TRUE(mapper.injectMapping(1, "fs-1"));
  ASSERT_EQ(mapper.size(), 1u);
  // accessor tests
  ASSERT_TRUE(mapper.hasFsid(1));
  ASSERT_FALSE(mapper.hasFsid(2));
  ASSERT_TRUE(mapper.hasUuid("fs-1"));
  ASSERT_FALSE(mapper.hasUuid("fs-2"));
  ASSERT_EQ(mapper.lookup("fs-1"), 1);
  ASSERT_EQ(mapper.lookup("fs-2"), 0);
  ASSERT_EQ(mapper.lookup(1), "fs-1");
  ASSERT_EQ(mapper.lookup(2), "");
  // Removal tests
  ASSERT_FALSE(mapper.remove(2));
  ASSERT_TRUE(mapper.remove(1));
  ASSERT_EQ(mapper.size(), 0u);
  ASSERT_FALSE(mapper.hasFsid(1));
  ASSERT_FALSE(mapper.hasUuid("fs-1"));
  ASSERT_FALSE(mapper.remove(1));
  ASSERT_FALSE(mapper.remove("fs-1"));
  ASSERT_TRUE(mapper.injectMapping(2, "fs-2"));
  ASSERT_TRUE(mapper.injectMapping(3, "fs-3"));
  ASSERT_TRUE(mapper.injectMapping(4, "fs-4"));
  ASSERT_FALSE(mapper.injectMapping(5, "fs-4"));
  ASSERT_FALSE(mapper.injectMapping(3, "fs-5"));
  ASSERT_TRUE(mapper.injectMapping(3, "fs-3")); // exists already
  ASSERT_EQ(mapper.size(), 3u);
  ASSERT_FALSE(mapper.remove("fs-5"));
  ASSERT_TRUE(mapper.remove("fs-3"));
  ASSERT_EQ(mapper.size(), 2u);
  ASSERT_FALSE(mapper.hasUuid("fs-3"));
  ASSERT_FALSE(mapper.hasFsid(3));
  // Try to allocate existing uuid
  ASSERT_EQ(mapper.allocate("fs-2"), 2);
  ASSERT_EQ(mapper.allocate("fs-4"), 4);
  ASSERT_EQ(mapper.size(), 2u);
  ASSERT_EQ(mapper.allocate("fs-5"), 5);
  ASSERT_EQ(mapper.allocate("fs-6"), 6);
  ASSERT_EQ(mapper.allocate("fs-7"), 7);
  ASSERT_EQ(mapper.size(), 5u);
  ASSERT_TRUE(mapper.injectMapping(63999, "fs-63999"));
  ASSERT_EQ(mapper.allocate("fs-64000"), 64000);
  ASSERT_EQ(mapper.allocate("fs-1"), 1);
  ASSERT_EQ(mapper.allocate("fs-3"), 3);
  ASSERT_EQ(mapper.allocate("fs-8"), 8);
  ASSERT_EQ(mapper.allocate("fs-9"), 9);
  ASSERT_EQ(mapper.lookup("fs-8"), 8);
  ASSERT_EQ(mapper.lookup(8), "fs-8");
}

TEST(ConfigParsing, FilesystemEntry)
{
  std::map<std::string, std::string> results;
  ASSERT_TRUE(eos::common::ConfigParsing::parseFilesystemConfig(
                "bootcheck=0 bootsenttime=1480576520 configstatus=empty drainperiod=86400 drainstatus=drained graceperiod=3600 headroom=25000000000 host=p05798818d95041.cern.ch hostport=p05798818d95041.cern.ch:1095 id=7259 path=/data46 port=1095 queue=/eos/p05798818d95041.cern.ch:1095/fst queuepath=/eos/p05798818d95041.cern.ch:1095/fst/data46 scaninterval=604800 schedgroup=spare uuid=62dce94a-71de-4904-8105-534c61ce2eaa",
                results));
  ASSERT_EQ(results["bootcheck"], "0");
  ASSERT_EQ(results["bootsenttime"], "1480576520");
  ASSERT_EQ(results["configstatus"], "empty");
  ASSERT_EQ(results["drainperiod"], "86400");
  ASSERT_EQ(results["drainstatus"], "drained");
  ASSERT_EQ(results["graceperiod"], "3600");
  ASSERT_EQ(results["headroom"], "25000000000");
  ASSERT_EQ(results["host"], "p05798818d95041.cern.ch");
  ASSERT_EQ(results["hostport"], "p05798818d95041.cern.ch:1095");
  ASSERT_EQ(results["id"], "7259");
  ASSERT_EQ(results["path"], "/data46");
  ASSERT_EQ(results["port"], "1095");
  ASSERT_EQ(results["queue"], "/eos/p05798818d95041.cern.ch:1095/fst");
  ASSERT_EQ(results["queuepath"], "/eos/p05798818d95041.cern.ch:1095/fst/data46");
  ASSERT_EQ(results["scaninterval"], "604800");
  ASSERT_EQ(results["schedgroup"], "spare");
  ASSERT_EQ(results["uuid"], "62dce94a-71de-4904-8105-534c61ce2eaa");
  ASSERT_EQ(results.size(), 17u);
}

TEST(ConfigParsing, ParseAndJoin)
{
  std::string entry =
    "configstatus=rw drainperiod=86400 graceperiod=86400 host=example.cern.ch hostport=example.cern.ch:3001 id=1 path=/volume1/fst-space/1 port=3001 queue=/eos/example.cern.ch:3001/fst queuepath=/eos/example.cern.ch:3001/fst/volume1/fst-space/1 scan_disk_interval=14400 scan_ns_interval=259200 scan_ns_rate=50 scaninterval=604800 scanrate=100 schedgroup=default.0 uuid=fst-1";
  std::map<std::string, std::string> configEntry;
  ASSERT_TRUE(eos::common::ConfigParsing::parseFilesystemConfig(entry,
              configEntry));
  ASSERT_EQ(eos::common::joinMap(configEntry, " "), entry);
}

TEST(ConfigParsing, RelocateFilesystem)
{
  std::map<std::string, std::string> configEntry;
  ASSERT_TRUE(eos::common::ConfigParsing::parseFilesystemConfig(
                "configstatus=rw drainperiod=86400 graceperiod=86400 host=example.cern.ch hostport=example.cern.ch:3001 id=1 path=/volume1/fst-space/1 port=3001 queue=/eos/example.cern.ch:3001/fst queuepath=/eos/example.cern.ch:3001/fst/volume1/fst-space/1 scan_disk_interval=14400 scan_ns_interval=259200 scan_ns_rate=50 scaninterval=604800 scanrate=100 schedgroup=default.0 uuid=fst-1",
                configEntry));
  ASSERT_TRUE(eos::common::ConfigParsing::relocateFilesystem("example-2.cern.ch",
              5001, configEntry));
  ASSERT_EQ(eos::common::joinMap(configEntry, " "),
            "configstatus=rw drainperiod=86400 graceperiod=86400 host=example-2.cern.ch hostport=example-2.cern.ch:5001 id=1 path=/volume1/fst-space/1 port=5001 queue=/eos/example-2.cern.ch:5001/fst queuepath=/eos/example-2.cern.ch:5001/fst/volume1/fst-space/1 scan_disk_interval=14400 scan_ns_interval=259200 scan_ns_rate=50 scaninterval=604800 scanrate=100 schedgroup=default.0 uuid=fst-1"
           );
}

//------------------------------------------------------------------------------
// MockFsGroup class
//------------------------------------------------------------------------------
class MockFsGroup: public eos::mgm::FsGroup
{
public:
  MockFsGroup(const char* name):
    eos::mgm::FsGroup(name)
  {}

  MOCK_METHOD3(MaxAbsDeviation, double(const char*, bool,
                                       const std::set<eos::common::FileSystem::fsid_t>*));
};

//------------------------------------------------------------------------------
// Test GetUnbalancedGroups
//------------------------------------------------------------------------------
TEST(FsBalancerStats, Update)
{
  using eos::mgm::FsView;
  using ::testing::_;
  using ::testing::Return;
  int num_groups = 9;
  const std::string space = "default";
  eos::common::InstanceName::set("unitest");
  eos::mgm::FsView fs_view;
  auto& set_grps = fs_view.mSpaceGroupView[space];
  std::map<int, eos::mgm::FsGroup*> map_groups;

  for (int i = 1; i <= num_groups; ++i) {
    MockFsGroup* mock = new MockFsGroup(SSTR(space << "." << i).c_str());
    EXPECT_CALL(*mock, MaxAbsDeviation(_, _, _)).WillRepeatedly(Return(i * 10));
    set_grps.emplace(mock);
    map_groups.emplace(i, mock);
  }

  // Map threshold values to number of unbalanced groups
  std::map<double, int> map_results {
    {90, 0}, {80, 1}, {70, 2}, {60, 3}, {50, 4}, {40, 5}
  };

  // This part actually check the GetUnbalancerdGroups functionality
  for (const auto& pair : map_results) {
    ASSERT_EQ(pair.second, fs_view.GetUnbalancedGroups(space, pair.first).size());
  }

  // Now test the FsBalancerSTats::Update
  double threshold = 50;
  eos::mgm::FsBalancerStats fsb_stats(space);
  fsb_stats.UpdateInfo(&fs_view, threshold);
  ASSERT_EQ(4, fsb_stats.mGrpToMaxDev.size());
  fsb_stats.UpdateInfo(&fs_view, threshold + 10);
  ASSERT_EQ(3, fsb_stats.mGrpToMaxDev.size());
  fsb_stats.UpdateInfo(&fs_view, threshold - 10);
  ASSERT_EQ(5, fsb_stats.mGrpToMaxDev.size());

  for (auto& elem : set_grps) {
    delete elem;
  }

  eos::common::InstanceName::clear();
}

//------------------------------------------------------------------------------
// Test GetRandomIter
//------------------------------------------------------------------------------
TEST(FsBalancer, GetRandomIter)
{
  std::vector<int> vect {11, 21, 35, 43, 59};
  std::set<int> set_expected(vect.begin(), vect.end());
  std::set<int> set_obtained;
  int attempts = vect.size() * 2;

  for (int i = 0; i < attempts; ++i) {
    auto it = eos::mgm::FsBalancer::GetRandomIter(vect);
    ASSERT_FALSE(it == vect.end());
    set_obtained.insert(*it);
  }

  for (const auto& elem : set_obtained) {
    ASSERT_TRUE(set_expected.find(elem) != set_expected.end());
  }
}
