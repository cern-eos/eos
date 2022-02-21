//------------------------------------------------------------------------------
// File: IostatTests.cc
// Author: Jaroslav Guenther <jaroslav dot guenther at cern dot ch> - CERN
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

#include "gtest/gtest.h"
#define IN_TEST_HARNESS
#include "mgm/Iostat.hh"
#undef IN_TEST_HARNESS
#include "mgm/FsView.hh"
#include <map>

using namespace eos::mgm;

// to avoid assertion of empty instance name we set one here
//eos::common::InstanceName::set("eosdevtest");

// using Google Test Fixtures (TEST_T); a new instance
// of the following class will get created before each test
Period LAST_DAY = Period::DAY;
Period LAST_HOUR = Period::HOUR;
Period LAST_5MIN = Period::FIVEMIN;
Period LAST_1MIN = Period::ONEMIN;
PercentComplete P90 = PercentComplete::p90;
PercentComplete P95 = PercentComplete::p95;
PercentComplete P99 = PercentComplete::p99;
PercentComplete ALL = PercentComplete::p100;

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
  iostat.StartCollection();
  ASSERT_EQ(true, iostat.mRunning);
  iostat.StopCollection();
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

TEST(IostatPeriods, GetAddBufferData)
{
  using namespace std::chrono;
  IostatPeriods iostattbins;
  time_t now = 86400;
  ASSERT_EQ(0, iostattbins.GetDataInPeriod(86400, 0, now));
  ASSERT_EQ(0, iostattbins.GetDataInPeriod(86400, 86400, now));
  ASSERT_EQ(0, iostattbins.GetDataInPeriod(0, 86400, now));
  ASSERT_EQ(0, iostattbins.GetDataInPeriod(0, 0, now));
  ASSERT_EQ(0, iostattbins.GetLongestTransferTime());

  for (int i = 0; i < 86400; i++) {
    iostattbins.mDataBuffer[i] = 1;
  }

  for (int i = 0; i < 86401; i++) {
    ASSERT_EQ(i, iostattbins.GetDataInPeriod(i, 0, now));
  }

  for (int i = 0; i < 86400; i++) {
    iostattbins.mDataBuffer[i] = 0;
    iostattbins.mIntegralBuffer[i] = 0;
  }

  time_t stop = 86400;
  time_t start = 0;
  // (unsigned long long val,
  //                             time_t start, time_t stop, time_t now)
  iostattbins.AddToDataBuffer(86400, start, stop, now);

  for (int i = 0; i < 86401; i++) {
    ASSERT_EQ(i, iostattbins.GetDataInPeriod(i, 0, now));

    if (i > 43200) {
      ASSERT_EQ(86400 - i, iostattbins.GetDataInPeriod(i, i, now));
    } else {
      ASSERT_EQ(i, iostattbins.GetDataInPeriod(i, i, now));
    }
  }

  // UpdateTransferSampleInfo called every 5 min
  // call to StampBufferZero triggers it (every 512 ms)
  iostattbins.UpdateTransferSampleInfo(now);
  ASSERT_EQ(77760, iostattbins.GetTimeToPercComplete(P90));
  ASSERT_EQ(82080, iostattbins.GetTimeToPercComplete(P95));
  ASSERT_EQ(85536, iostattbins.GetTimeToPercComplete(P99));
  ASSERT_EQ(86400, iostattbins.GetTimeToPercComplete(ALL));
  ASSERT_EQ(86400, iostattbins.GetLongestTransferTime());

  // test stamping zero all bins
  for (int i = 0; i < 86401; i++) {
    time_t stamp = now + i;
    iostattbins.StampBufferZero(stamp);
    ASSERT_EQ(86400 - i, iostattbins.GetDataInPeriod(86400, 0, stamp));

    if (i < 86399) {
      ASSERT_EQ(1, iostattbins.GetDataInPeriod(1, i, stamp));
    }

    if (i < 86400 && i > 0) {
      ASSERT_EQ(86400 - i, iostattbins.GetDataInPeriod(86399, 1, stamp));
    }

    if (i < 86340) {
      ASSERT_EQ(60, iostattbins.GetDataInPeriod(60, i, stamp));
    }

    if (i >= 86340) {
      ASSERT_EQ(86400 - i, iostattbins.GetDataInPeriod(60, i, stamp));
    }
  }

  // testing adding and getting transfers of the same length and size
  // but different start and stop times (adding integer value per bin)
  double total = 0;
  now = 2 * 86400;

  for (int i = 0; i < 2 * 86400 - 100; i++) {
    start += 1;
    stop = start + 100;
    iostattbins.AddToDataBuffer(2000, start, stop, now);

    if (stop < 86400) {
      ASSERT_EQ(0, iostattbins.GetDataInPeriod(86400, 0, now));
    } else {
      int out = (86400 - start);

      if (out > 0) {
        total += (100 - out) * 20;
      } else {
        total += 2000;
      }

      ASSERT_EQ(std::ceil(total), iostattbins.GetDataInPeriod(86400, 0, now));
    }
  }

  // zero the data buffer
  for (int i = 0; i < 86401; i++) {
    time_t stamp = now + i;
    iostattbins.StampBufferZero(stamp);
  }

  ASSERT_EQ(0, iostattbins.GetDataInPeriod(86400, 0, now));
  // testing adding and getting transfers of the same length and size
  // but different start and stop times (adding double value per bin)
  total = 0;
  now = 2 * 86400;
  start = 0;

  for (int i = 0; i < 2 * 86400 - 10; i++) {
    start += 1;
    stop = start + 10;
    iostattbins.AddToDataBuffer(1, start, stop, now);

    if (stop < 86400) {
      ASSERT_EQ(0, iostattbins.GetDataInPeriod(86400, 0, now));
    } else {
      int out = (86400 - start);

      if (out > 0) {
        total += (10 - out) * 0.1;
      } else {
        total += 1;
      }

      std::cout << "out" << out << " start" << start << " stop" << stop << " total" <<
                total << std::endl;
      ASSERT_EQ(std::ceil(total), iostattbins.GetDataInPeriod(86400, 0, now));
    }
  }

  // zero the data buffer
  for (int i = 0; i < 86401; i++) {
    time_t stamp = now + i;
    iostattbins.StampBufferZero(stamp);
  }

  ASSERT_EQ(0, iostattbins.GetDataInPeriod(86400, 0, now));
  // testing Integral of the buffer for 4 transfers
  // with same rate 1B/s, but with different length
  // and start time
  now = 86400;
  iostattbins.UpdateTransferSampleInfo(now);

  for (int i = 1; i < 5; i++) {
    start = i * 10000;
    stop = start + i * 10000;
    //4*10000 + 3*10000 + 2*10000 + 1*10000
    iostattbins.AddToDataBuffer(i * 10000, start, stop, now);
  }

  iostattbins.UpdateTransferSampleInfo(now);
  ASSERT_EQ(30000, iostattbins.GetTimeToPercComplete(P90));
  ASSERT_EQ(35000, iostattbins.GetTimeToPercComplete(P95));
  ASSERT_EQ(39000, iostattbins.GetTimeToPercComplete(P99));
  ASSERT_EQ(40000, iostattbins.GetTimeToPercComplete(ALL));
}

TEST(IostatPeriods, GetStatStampZeroAdd)
{
  using namespace std::chrono;
  IostatPeriods iostattbins;
  ASSERT_EQ(0, iostattbins.GetSumForPeriod(LAST_DAY));
  ASSERT_EQ(0, iostattbins.GetSumForPeriod(LAST_HOUR));
  ASSERT_EQ(0, iostattbins.GetSumForPeriod(LAST_5MIN));
  ASSERT_EQ(0, iostattbins.GetSumForPeriod(LAST_1MIN));

  for (int pidx = 0; pidx < 4; pidx++) {
    for (int i = 0; i < 60; i++) {
      iostattbins.mPeriodBins[pidx][i] = iostattbins.mPeriodBinWidth[pidx];
    }
  }

  ASSERT_EQ(86400, iostattbins.GetSumForPeriod(LAST_DAY));
  ASSERT_EQ(3600, iostattbins.GetSumForPeriod(LAST_HOUR));
  ASSERT_EQ(300, iostattbins.GetSumForPeriod(LAST_5MIN));
  ASSERT_EQ(60, iostattbins.GetSumForPeriod(LAST_1MIN));
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

    iostattbins.StampZero(now);

    if (i < 60) {
      ASSERT_EQ(60 - (i + 1) * 1, iostattbins.GetSumForPeriod(LAST_1MIN));
    }

    if (i < 300) {
      // in 5 sec interval the same bin gets marked zero
      // cout << i << " time: "<<  now << endl;
      ASSERT_EQ(300 - (i / 5 + 1) * 5, iostattbins.GetSumForPeriod(LAST_5MIN));
    }

    if (i < 3600) {
      // in 60 sec interval the same bin gets marked zero
      ASSERT_EQ(3600 - (i / 60 + 1) * 60, iostattbins.GetSumForPeriod(LAST_HOUR));
    }

    if (i < 86400) {
      // in 1440 sec interval the same bin gets marked zero
      ASSERT_EQ(86400 - (i / 1440 + 1) * 1440, iostattbins.GetSumForPeriod(LAST_DAY));
    }
  }

  ASSERT_EQ(0, iostattbins.GetSumForPeriod(LAST_1MIN));
  ASSERT_EQ(0, iostattbins.GetSumForPeriod(LAST_5MIN));
  ASSERT_EQ(0, iostattbins.GetSumForPeriod(LAST_HOUR));
  ASSERT_EQ(0, iostattbins.GetSumForPeriod(LAST_DAY));
  time_t start_time = 0;
  time_t mark_time = 2 * 86400;

  // currently no protection in the code against:
  // * start - stop times fits within the time window of last day, hour or minute
  // * tdiff (stop - start) <= 0; will get added to all averages
  // * consider adding check in the code against toff < 0 and tdiff < 0 and testing against it
  for (int i = 0; i < 2 * 86400 + 1; ++i) {
    time_t stop_time = i;
    iostattbins.Add(1, start_time, stop_time, mark_time);

    if (i < 86400) {
      ASSERT_EQ(0, iostattbins.GetSumForPeriod(LAST_1MIN));
      ASSERT_EQ(0, iostattbins.GetSumForPeriod(LAST_5MIN));
      ASSERT_EQ(0, iostattbins.GetSumForPeriod(LAST_HOUR));
      ASSERT_EQ(0, iostattbins.GetSumForPeriod(LAST_DAY));
    } else {
      if (i >= 2 * 86400 - 60) {
        ASSERT_EQ(i - (2 * 86400 - 60), iostattbins.GetSumForPeriod(LAST_1MIN));
      } else {
        ASSERT_EQ(0, iostattbins.GetSumForPeriod(LAST_1MIN));
      }

      if (i >= 2 * 86400 - 300) {
        ASSERT_EQ(i - (2 * 86400 - 300), iostattbins.GetSumForPeriod(LAST_5MIN));
      } else {
        ASSERT_EQ(0, iostattbins.GetSumForPeriod(LAST_5MIN));
      }

      if (i >= 2 * 86400 - 3600) {
        //std::cout << i << " AVG: " << iostattbins.GetSumForPeriod("stat3600() <<std::endl;
        ASSERT_EQ(i - (2 * 86400 - 3600), iostattbins.GetSumForPeriod(LAST_HOUR));
      } else {
        ASSERT_EQ(0, iostattbins.GetSumForPeriod(LAST_HOUR));
      }

      if (i >= 86400) {
        ASSERT_EQ(i - 86400, iostattbins.GetSumForPeriod(LAST_DAY));
      } else {
        ASSERT_EQ(0, iostattbins.GetSumForPeriod(LAST_DAY));
      }
    }
  }
}
