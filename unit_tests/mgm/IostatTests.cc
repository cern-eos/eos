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
class IostatTest : public :: testing::Test {
  protected: 
    Iostat iostat;
};

class MockFsView {
  public:
    std::map<std::string, std::string> kvdict = {
      {"iostat::collect", ""},
      {"iostat::report", ""},
      {"iostat::reportnamespace", ""},
      {"iostat::popularity", ""},
      {"iostat::udptargets", ""}
    };
    std::string GetGlobalConfig(const std::string& key)  { 
      return kvdict.find(key)->second;
    };
    bool SetGlobalConfig(const std::string& key, const std::string& value)  {
      auto it = kvdict.find(key);             
      if(it != kvdict.end()) {
        kvdict.erase (it);
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
  
  ASSERT_EQ("iostat::collect", iostat.gIostatCollect);
  ASSERT_EQ("iostat::report", iostat.gIostatReport);
  ASSERT_EQ("iostat::reportnamespace", iostat.gIostatReportNamespace);
  ASSERT_EQ("iostat::popularity", iostat.gIostatPopularity);
  ASSERT_EQ("iostat::udptargets", iostat.gIostatUdpTargetList);
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
  iostat.StoreIostatConfig<MockFsView>(&mock_fsview);
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
  iostat.ApplyIostatConfig<MockFsView>(&mock_fsview);
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
  // the values summed up finish in integer array The GetAvg methods add doubles o them 
  // - seems pointless unless we want to change the array definition as well (???)
  ASSERT_EQ(0, iostatavg.GetAvg86400());
  ASSERT_EQ(0, iostatavg.GetAvg3600());
  ASSERT_EQ(0, iostatavg.GetAvg300());
  ASSERT_EQ(0, iostatavg.GetAvg60());
  
  for (int i = 0; i < 60; i++) {
    iostatavg.avg86400[i] = 1;
    iostatavg.avg3600[i] = 1;
    iostatavg.avg300[i] = 1;
    iostatavg.avg60[i] = 1;
  }
  ASSERT_EQ(60, iostatavg.GetAvg86400());
  ASSERT_EQ(60, iostatavg.GetAvg3600());
  ASSERT_EQ(60, iostatavg.GetAvg300());
  ASSERT_EQ(60, iostatavg.GetAvg60());
  
  auto now = system_clock::now();
  
  for (int i = 0; i <  86400; ++i) {
    now = now + seconds(1);
    time_t now_time = std::chrono::system_clock::to_time_t(now);
    iostatavg.StampZero(now_time);
  }

  ASSERT_EQ(0, iostatavg.GetAvg86400());
  ASSERT_EQ(0, iostatavg.GetAvg3600());
  ASSERT_EQ(0, iostatavg.GetAvg300());
  ASSERT_EQ(0, iostatavg.GetAvg60());
 
  auto start = system_clock::now() - seconds(3610);
  auto stopAvg60 = start + seconds(3590);
  auto stopAvg300 = start + seconds(3400);
  auto stopAvg3600 = start + seconds(900);
  auto stopAvg86400 = start + seconds(5);
  
  time_t start_time = std::chrono::system_clock::to_time_t(start);
  time_t stopAvg60_time = std::chrono::system_clock::to_time_t(stopAvg60);
  time_t stopAvg300_time = std::chrono::system_clock::to_time_t(stopAvg300);
  time_t stopAvg3600_time = std::chrono::system_clock::to_time_t(stopAvg3600);
  time_t stopAvg86400_time = std::chrono::system_clock::to_time_t(stopAvg86400);
  
  // THE FOLLOWING NEEDS A REVIEW OF THE FUNCTIONALITY ITSELF; The AVGs provided by the method 
  // are not exact and maybe misleading depending on the use-case for this this method was constructed.
  // When a value is added with the Add function, it is divided by mbins and 
  // result converted to integer norm_val; if val/mbins < 0 we have 0, is this what we want ? 
  // for small values the stat avgs just show 0 

  iostatavg.Add(10000, start_time, stopAvg86400_time);
  ASSERT_EQ(10000, iostatavg.GetAvg86400());
  ASSERT_EQ(0, iostatavg.GetAvg3600());
  ASSERT_EQ(0, iostatavg.GetAvg300());
  ASSERT_EQ(0, iostatavg.GetAvg60());
  iostatavg.Add(10000, start_time, stopAvg3600_time);
  ASSERT_EQ(20000, iostatavg.GetAvg86400());
  ASSERT_EQ(9990, iostatavg.GetAvg3600());
  ASSERT_EQ(0, iostatavg.GetAvg300());
  ASSERT_EQ(0, iostatavg.GetAvg60());
  iostatavg.Add(10000, start_time, stopAvg300_time);
  ASSERT_EQ(30000, iostatavg.GetAvg86400());
  ASSERT_EQ(19958, iostatavg.GetAvg3600());
  ASSERT_EQ(9520, iostatavg.GetAvg300());
  ASSERT_EQ(0, iostatavg.GetAvg60());
  iostatavg.Add(10000, start_time, stopAvg60_time);
  ASSERT_EQ(40000, iostatavg.GetAvg86400());
  ASSERT_EQ(29929, iostatavg.GetAvg3600());
  ASSERT_EQ(18854, iostatavg.GetAvg300());
  ASSERT_EQ(7180, iostatavg.GetAvg60());

}

// methods needing some gOFS object work around (to be done):
//Receive
//PrintNs
//NamespaceReport
//UdpBroadCast

// method manipulating private Iostat members - not being tested: 
//StartCirculate
//StartPopularity
//StopPopularity
//StartReport
//StopReport
//StartCollection
//StopCollection
//StartReportNamespace
//StopReportNamespace
//AddToPopularity
//Add
//Circulate

// external file operations (not sure if worth testing as this will change to QDB soon)
//Store  
//Restore
//WriteRecord
