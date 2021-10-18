//------------------------------------------------------------------------------
// File: IostatTests.cc
// Author: Jaroslav Guenther <jaroslav dot guenther at cern dot ch>
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

#include <map>
#include "gtest/gtest.h"
#include "mgm/Namespace.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "mgm/Iostat.hh"
#include "mgm/FsView.hh"
//#include "common/InstanceName.hh"

using namespace eos::mgm;

// to avoid assertion of empty instance name we set one here
//eos::common::InstanceName::set("eosdevtest");

// using Google Test Fixtures (TEST_T); a new instance
// of the following class will get created before each test
class IostatTest : public :: testing::Test
{
protected:
  Iostat iostat;
};

class MockFsView: public FsView
{
public:
  std::map<std::string, std::string> kvdict = {
    {"iostat::collect", ""},
    {"iostat::report", ""},
    {"iostat::reportnamespace", ""},
    {"iostat::popularity", ""},
    {"iostat::udptargets", ""}
  };
  std::string GetGlobalConfig(const std::string& key) override
  {
    return kvdict.find(key)->second;
  };
  bool SetGlobalConfig(const std::string& key, const std::string& value) override
  {
    auto it = kvdict.find(key);

    if (it != kvdict.end()) {
      kvdict.erase(it);
    }

    kvdict[key] = value;
    return true;
  }

};

//------------------------------------------------------------------------------
// Test basic Iostat functionality
//------------------------------------------------------------------------------
TEST_F(IostatTest, InitConfig)
{
  EXPECT_STREQ("iostat::collect", iostat.gIostatCollect);
  EXPECT_STREQ("iostat::report", iostat.gIostatReport);
  EXPECT_STREQ("iostat::reportnamespace", iostat.gIostatReportNamespace);
  EXPECT_STREQ("iostat::popularity", iostat.gIostatPopularity);
  EXPECT_STREQ("iostat::udptargets", iostat.gIostatUdpTargetList);
  ASSERT_EQ(0, iostat.gOpenReportFD);
  ASSERT_EQ(false, iostat.mRunning);
}

TEST_F(IostatTest, StartStop)
{
  ASSERT_EQ(false, iostat.mRunning);
  iostat.Start();
  ASSERT_EQ(true, iostat.mRunning);
  iostat.Stop();
  ASSERT_EQ(false, iostat.mRunning);
}

TEST_F(IostatTest, StoreApplyIostatConfig)
{
  MockFsView mock_fsview;
  std::string udplist = mock_fsview.GetGlobalConfig(iostat.gIostatUdpTargetList);
  std::string iocollect = mock_fsview.GetGlobalConfig(iostat.gIostatCollect);
  std::string ioreport = mock_fsview.GetGlobalConfig(iostat.gIostatReport);
  std::string ioreportns = mock_fsview.GetGlobalConfig(
                             iostat.gIostatReportNamespace);
  std::string iopopularity = mock_fsview.GetGlobalConfig(
                               iostat.gIostatPopularity);
  ASSERT_EQ("", udplist);
  ASSERT_EQ("", iocollect);
  ASSERT_EQ("", ioreport);
  ASSERT_EQ("", ioreportns);
  ASSERT_EQ("", iopopularity);
  iostat.StoreIostatConfig(&mock_fsview);
  // we check that all the remaining global config values were saved as expected
  udplist = mock_fsview.GetGlobalConfig(iostat.gIostatUdpTargetList);
  iocollect = mock_fsview.GetGlobalConfig(iostat.gIostatCollect);
  ioreport = mock_fsview.GetGlobalConfig(iostat.gIostatReport);
  ioreportns = mock_fsview.GetGlobalConfig(
                 iostat.gIostatReportNamespace);
  iopopularity = mock_fsview.GetGlobalConfig(
                   iostat.gIostatPopularity);
  ASSERT_EQ("", udplist);
  ASSERT_EQ("false", iocollect);
  ASSERT_EQ("true", ioreport);
  ASSERT_EQ("false", ioreportns);
  ASSERT_EQ("true", iopopularity);
  mock_fsview.kvdict["iostat::udptargets"] = "udptarget1";
  std::string out = iostat.EncodeUdpPopularityTargets();
  ASSERT_EQ("", out);
  //EXPECT_CALL(mock_fsview, mock_fsview::GetGlobalConfig).Times(AtLeast(5));
  iostat.ApplyIostatConfig(&mock_fsview);
  // Start collection should not get called
  ASSERT_EQ(false, iostat.mRunning);
  // AddUdpTarget gets called
  out = iostat.EncodeUdpPopularityTargets();
  ASSERT_EQ("udptarget1", out);
}

TEST_F(IostatTest, AddRemoveUdpTargets)
{
  std::string expected = "";
  std::string out = iostat.EncodeUdpPopularityTargets();
  ASSERT_EQ(expected, out);
  iostat.AddUdpTarget("target_1", false);
  iostat.AddUdpTarget("target_2", false);
  iostat.AddUdpTarget("target_3", false);
  expected = "target_1|target_2|target_3";
  out = iostat.EncodeUdpPopularityTargets();
  ASSERT_EQ(expected, out);
  iostat.RemoveUdpTarget("target_2");
  expected = "target_1|target_3";
  out = iostat.EncodeUdpPopularityTargets();
  ASSERT_EQ(expected, out);
}

TEST(IostatAvg, GetAvgStampZeroAdd)
{
  using namespace std::chrono;
  IostatAvg iostatavg;
  // the values summed up finish in integer array The GetAvg methods add doubles
  // of them which is pointless unless we want to change the array definition as
  // well (which is int !!!) (???)
  ASSERT_EQ(0, iostatavg.GetAvg86400());
  ASSERT_EQ(0, iostatavg.GetAvg3600());
  ASSERT_EQ(0, iostatavg.GetAvg300());
  ASSERT_EQ(0, iostatavg.GetAvg60());

  for (int i = 0; i < 60; i++) {
    iostatavg.avg86400[i] = 1440;
    iostatavg.avg3600[i] = 60;
    iostatavg.avg300[i] = 5;
    iostatavg.avg60[i] = 1;
  }

  ASSERT_EQ(86400, iostatavg.GetAvg86400());
  ASSERT_EQ(3600, iostatavg.GetAvg3600());
  ASSERT_EQ(300, iostatavg.GetAvg300());
  ASSERT_EQ(60, iostatavg.GetAvg60());
  // the bin assignment currently depends on the reference point
  // being number of seconds since epoch, we need to sync the test start time
  // with the boundary of the bin start
  // aka - this would not work as a reference for counting:
  // auto now = std::chrono::system_clock::now();
  time_t now = 0;

  for (int i = 0; i < 86400; ++i) {
    if (i != 0) {
      now = now + 1;
    }
    iostatavg.StampZero(now);
    if (i < 60) {
      ASSERT_EQ(60 - (i + 1) * 1, iostatavg.GetAvg60());
    }
    if (i < 300) {
      // in 5 sec interval the same bin gets marked zero
      // cout << i << " time: "<<  now << endl;
      ASSERT_EQ(300 - (i / 5 + 1) * 5, iostatavg.GetAvg300());
    }
    if (i < 3600) {
      // in 60 sec interval the same bin gets marked zero
      ASSERT_EQ(3600 - (i / 60 + 1) * 60, iostatavg.GetAvg3600());
    }
    if (i < 86400) {
      // in 1440 sec interval the same bin gets marked zero
      ASSERT_EQ(86400 - (i / 1440 + 1) * 1440, iostatavg.GetAvg86400());
    }
  }

  ASSERT_EQ(0, iostatavg.GetAvg60());
  ASSERT_EQ(0, iostatavg.GetAvg300());
  ASSERT_EQ(0, iostatavg.GetAvg3600());
  ASSERT_EQ(0, iostatavg.GetAvg86400());

  auto start = system_clock::now() - seconds(2*86400); // started measurement 2 days ago
  time_t start_time = std::chrono::system_clock::to_time_t(start);
  // currently no protection in the code against:
  // * start - stop times fits within the time window of last day, hour or minute
  // * tdiff (stop - start) <= 0; will get added to all averages
  // * consider adding check in the code against toff < 0 and tdiff < 0 and testing against it
  for (int i = 0; i < 2*86400 + 1; ++i) {
    auto stopAvg = start + seconds(i);
    time_t stopAvg_time = std::chrono::system_clock::to_time_t(stopAvg);
    iostatavg.Add(1, start_time, stopAvg_time);
    if (i < 86401){
      ASSERT_EQ(0, iostatavg.GetAvg60());
      ASSERT_EQ(0, iostatavg.GetAvg300());
      ASSERT_EQ(0, iostatavg.GetAvg3600());
      ASSERT_EQ(0, iostatavg.GetAvg86400());
    } else {
      if (i > 2*86400 - 60) {
        ASSERT_EQ(i - (2*86400 - 60), iostatavg.GetAvg60());
      } else {
        ASSERT_EQ(0, iostatavg.GetAvg60());
      }
      if (i > 2*86400 - 300) {
        ASSERT_EQ(i - (2*86400 - 300), iostatavg.GetAvg300());
      } else {
        ASSERT_EQ(0, iostatavg.GetAvg300());
      }
      if (i > 2*86400 - 3600) {
        //std::cout << i << " AVG: " << iostatavg.GetAvg3600() <<std::endl;
        ASSERT_EQ(i - (2*86400 - 3600), iostatavg.GetAvg3600());
      } else {
        ASSERT_EQ(0, iostatavg.GetAvg3600());
      }
      if (i > 86400) {
        ASSERT_EQ(i - 86400, iostatavg.GetAvg86400());
      } else {
        ASSERT_EQ(0, iostatavg.GetAvg86400());
      }
    }
  }
}

