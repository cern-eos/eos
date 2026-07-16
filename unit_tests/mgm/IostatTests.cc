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
#include "mgm/iostat/Iostat.hh"
#undef IN_TEST_HARNESS
#include "mgm/fsview/FsView.hh"
#include <map>
#include <random>

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
  std::map<std::string, std::string> kvdict = {{"iostat", ""},
                                               {"iostat::collect", ""},
                                               {"iostat::report", ""},
                                               {"iostat::reportnamespace", ""},
                                               {"iostat::popularity", ""},
                                               {"iostat::udptargets", ""}};
  std::string GetGlobalConfig(const std::string& key) override
  {
    auto it = kvdict.find(key);
    return (it == kvdict.end()) ? "" : it->second;
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
  EXPECT_STREQ("iostat::report", iostat.gIostatReportSave);
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

TEST_F(IostatTest, StoreApplyConfig)
{
  MockFsView mock_fsview;
  std::string config = mock_fsview.GetGlobalConfig(Iostat::sIostatKey);
  ASSERT_EQ("", config);
  iostat.StoreIostatConfig(&mock_fsview);
  // The whole configuration now lives under a single consolidated key,
  // mirroring how Fsck::StoreConfig persists its "fsck" blob
  config = mock_fsview.GetGlobalConfig(Iostat::sIostatKey);
  ASSERT_EQ("collect=off popularity=on report=on reportnamespace=off", config);
  mock_fsview.kvdict[Iostat::sIostatKey] =
      "collect=off popularity=on report=on reportnamespace=off "
      "udptargets=udptarget1";
  std::string out = iostat.EncodeUdpPopularityTargets();
  ASSERT_EQ("", out);
  iostat.ApplyConfig(&mock_fsview);
  // collect=off in the stored config, so collection should not get started
  ASSERT_EQ(false, iostat.mRunning);
  // AddUdpTarget gets called
  out = iostat.EncodeUdpPopularityTargets();
  ASSERT_EQ("udptarget1", out);
}

TEST_F(IostatTest, ApplyConfigLegacyMigration)
{
  // Instance configured before the individual iostat::* keys got
  // consolidated into a single "iostat" key - ApplyConfig must still honor
  // them and migrate the configuration to the new format
  MockFsView mock_fsview;
  mock_fsview.kvdict["iostat::collect"] = "true";
  mock_fsview.kvdict["iostat::popularity"] = "true";
  mock_fsview.kvdict["iostat::report"] = "false";
  mock_fsview.kvdict["iostat::reportnamespace"] = "true";
  ASSERT_EQ("", mock_fsview.GetGlobalConfig(Iostat::sIostatKey));
  iostat.ApplyConfig(&mock_fsview);
  ASSERT_EQ(true, iostat.mRunning);
  ASSERT_EQ(true, iostat.mReportPopularity);
  ASSERT_EQ(false, iostat.mReportSave);
  ASSERT_EQ(true, iostat.mReportNamespace);
  // the legacy settings must have been migrated to the consolidated key
  ASSERT_NE("", mock_fsview.GetGlobalConfig(Iostat::sIostatKey));
  iostat.StopCollection();
}

TEST_F(IostatTest, ConsolidatedKeyTakesPrecedenceOverLegacyKeys)
{
  // Both formats present at once (e.g. a rollback/roll-forward, or a
  // partial migration) - the consolidated key must win outright and the
  // legacy keys must be ignored entirely
  MockFsView mock_fsview;
  mock_fsview.kvdict["iostat::collect"] = "true";
  mock_fsview.kvdict["iostat::popularity"] = "true";
  mock_fsview.kvdict["iostat::report"] = "true";
  mock_fsview.kvdict["iostat::reportnamespace"] = "true";
  mock_fsview.kvdict[Iostat::sIostatKey] =
      "collect=off popularity=off report=off reportnamespace=off";
  iostat.ApplyConfig(&mock_fsview);
  ASSERT_EQ(false, iostat.mRunning);
  ASSERT_EQ(false, iostat.mReportPopularity);
  ASSERT_EQ(false, iostat.mReportSave);
  ASSERT_EQ(false, iostat.mReportNamespace);
}

TEST_F(IostatTest, ApplyConfigFreshInstanceEagerlyWritesDefaults)
{
  // Finding: on a completely fresh instance (no consolidated key, no
  // legacy keys at all) the legacy-fallback branch still unconditionally
  // calls StoreIostatConfig() at the end, so the very first config load
  // silently materializes a "iostat" key full of defaults - unlike Fsck,
  // which never writes anything until an admin runs "fsck config ...".
  // This is intentional/harmless (values are correct defaults) but is
  // documented here so it doesn't surprise anyone diffing config dumps
  // across an upgrade.
  MockFsView mock_fsview;
  mock_fsview.kvdict.clear();
  ASSERT_EQ("", mock_fsview.GetGlobalConfig(Iostat::sIostatKey));
  iostat.ApplyConfig(&mock_fsview);
  ASSERT_NE("", mock_fsview.GetGlobalConfig(Iostat::sIostatKey));
  iostat.StopCollection();
}

TEST_F(IostatTest, ConfigInvalidValueRejected)
{
  std::string msg;
  ASSERT_FALSE(iostat.Config(Iostat::sCollectKey, "maybe", msg));
  ASSERT_FALSE(msg.empty());
  // live state must be untouched by the rejected change
  ASSERT_EQ(false, iostat.mRunning);
  msg.clear();
  ASSERT_FALSE(iostat.Config(Iostat::sPopularityKey, "", msg));
  ASSERT_FALSE(msg.empty());
  msg.clear();
  ASSERT_FALSE(iostat.Config(Iostat::sReportKey, "1", msg));
  ASSERT_FALSE(msg.empty());
}

TEST_F(IostatTest, ConfigUnknownKeyRejected)
{
  std::string msg;
  ASSERT_FALSE(iostat.Config("bogus", "on", msg));
  ASSERT_FALSE(msg.empty());
}

TEST_F(IostatTest, ConfigUdpTargetsEmptyValueClearsList)
{
  std::string msg;
  iostat.AddUdpTarget("target_1", false);
  ASSERT_EQ("target_1", iostat.EncodeUdpPopularityTargets());
  ASSERT_TRUE(iostat.Config(Iostat::sUdpTargetsKey, "", msg));
  ASSERT_EQ("", iostat.EncodeUdpPopularityTargets());
}

TEST_F(IostatTest, StoreApplyConfigRoundTripIsIdempotent)
{
  // Bring the source instance into a non-default state (Config() persists
  // through the real FsView::gFsView singleton, same as Start*/Stop*, so we
  // explicitly persist to the mock afterwards to capture the blob) and
  // verify a fresh instance applying that blob reaches the same state
  MockFsView mock_fsview;
  iostat.StartCollection();
  iostat.StopPopularity();
  iostat.StopReport();
  iostat.StartReportNamespace();
  iostat.AddUdpTarget("host1", false);
  iostat.AddUdpTarget("host2", false);
  iostat.StoreIostatConfig(&mock_fsview);
  std::string stored = mock_fsview.GetGlobalConfig(Iostat::sIostatKey);
  ASSERT_NE("", stored);
  // A fresh instance applying that exact blob must reach the same state
  Iostat replay;
  replay.ApplyConfig(&mock_fsview);
  ASSERT_EQ(iostat.mRunning.load(), replay.mRunning.load());
  ASSERT_EQ(iostat.mReportPopularity.load(), replay.mReportPopularity.load());
  ASSERT_EQ(iostat.mReportSave.load(), replay.mReportSave.load());
  ASSERT_EQ(iostat.mReportNamespace.load(), replay.mReportNamespace.load());
  ASSERT_EQ(iostat.EncodeUdpPopularityTargets(), replay.EncodeUdpPopularityTargets());
  // Re-serializing the replayed state must produce the exact same blob
  // (no drift through a store/apply/store round trip)
  replay.StoreIostatConfig(&mock_fsview);
  ASSERT_EQ(stored, mock_fsview.GetGlobalConfig(Iostat::sIostatKey));
  replay.StopCollection();
  iostat.StopCollection();
}

TEST_F(IostatTest, ApplyConfigMalformedBlobDoesNotCrash)
{
  MockFsView mock_fsview;
  // missing '=' token, duplicate key (first occurrence wins - kv_map is
  // built with emplace, same as Fsck::ApplyConfig), trailing space, unknown
  // key mixed in - none of this should crash or throw
  mock_fsview.kvdict[Iostat::sIostatKey] =
      "collect popularity=on popularity=off bogus=1 report=on ";
  ASSERT_NO_THROW(iostat.ApplyConfig(&mock_fsview));
  // first "popularity=on" wins - "popularity=off" is silently ignored
  ASSERT_EQ(true, iostat.mReportPopularity);
  ASSERT_EQ(true, iostat.mReportSave);
  iostat.StopCollection();
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
  time_t stop = 86399;
  time_t start = 0;
  iostattbins.Add(86400, start, stop, now);

  for (int i = 0; i < 86401; i++) {
    ASSERT_EQ(i, iostattbins.GetDataInPeriod(i, 0, now));

    if (i > 43200) {
      ASSERT_EQ(86400 - i, iostattbins.GetDataInPeriod(i, i, now));
    } else {
      ASSERT_EQ(i, iostattbins.GetDataInPeriod(i, i, now));
    }
  }

  // UpdateTransferSampleInfo called every 5 min
  iostattbins.UpdateTransferSampleInfo(now);
  ASSERT_EQ(77760, iostattbins.GetTimeToPercComplete(P90));
  ASSERT_EQ(82080, iostattbins.GetTimeToPercComplete(P95));
  ASSERT_EQ(85536, iostattbins.GetTimeToPercComplete(P99));
  ASSERT_EQ(86400, iostattbins.GetTimeToPercComplete(ALL));
  ASSERT_EQ(86400, iostattbins.GetLongestTransferTime());

  // Test stamping zero all bins
  for (int i = 0; i < 86400; i++) {
    time_t stamp = now + i;
    iostattbins.StampBufferZero(stamp);
    ASSERT_EQ(86400 - i - 1, iostattbins.GetDataInPeriod(86400, 0, stamp));

    if (i < 86399) {
      ASSERT_EQ(1, iostattbins.GetDataInPeriod(1, i, stamp));
    }

    if (i < 86400 && i > 0) {
      ASSERT_EQ(86400 - i - 1, iostattbins.GetDataInPeriod(86399, 1, stamp));
    }

    if (i < 86340) {
      ASSERT_EQ(60, iostattbins.GetDataInPeriod(60, i, stamp));
    }

    if (i >= 86340) {
      ASSERT_EQ(86400 - i - 1, iostattbins.GetDataInPeriod(60, i, stamp));
    }
  }

  // Test adding and getting transfers of the same length and size
  // but different start and stop times (adding integer value per bin)
  double total = 0;
  now = 2 * 86400;

  for (int i = 0; i < 2 * 86400 - 99; i++) {
    stop = start + 99;
    iostattbins.Add(2000, start, stop, now);

    if (stop < 86400) {
      if (i % 100 == 0) {
        // modulo operation to speed up the testing
        ASSERT_EQ(0, iostattbins.GetDataInPeriod(86400, 0, now));
      }
    } else {
      int out = (86400 - start + 1);

      if (out > 0) {
        total += (100 - out) * 20;
      } else {
        total += 2000;
      }

      if (i % 100 == 0) {
        // modulo operation to speed up the testing
        ASSERT_EQ(std::ceil(total), iostattbins.GetDataInPeriod(86400, 0, now));
      }

      start += 1;
    }
  }

  // Zero the data buffer
  for (int i = 0; i < 86401; i++) {
    time_t stamp = now + i;
    iostattbins.StampBufferZero(stamp);
  }

  auto ts_start = std::chrono::system_clock::now();
  ASSERT_EQ(0, iostattbins.GetDataInPeriod(86400, 0, now));
  auto ts_end = std::chrono::system_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>
                  (ts_end - ts_start);
  std::cerr << "Time to GetDataInPeriod for all bins: " << duration.count() <<
            " us\n";
  // Test adding and getting transfers of the same length and size
  // but different start and stop times (adding double value per bin)
  total = 0;
  now = 2 * 86400;
  start = 0;

  for (int i = 0; i < 2 * 86400 - 9; i++) {
    stop = start + 9;
    iostattbins.Add(1, start, stop, now);

    if (stop < 86400) {
      if (i % 100 == 0) {
        // modulo operation to speed up the testing
        ASSERT_EQ(0, iostattbins.GetDataInPeriod(86400, 0, now));
      }
    } else {
      int out = (86400 - start + 1);

      if (out > 0) {
        total += (10 - out) * 0.1;
      } else {
        total += 1;
      }

      //std::cout << "out" << out << " start" << start << " stop" << stop << " total" <<
      //          total << std::endl;
      if (i % 100 == 0) {
        // modulo operation to speed up the testing
        ASSERT_EQ(std::ceil(total), iostattbins.GetDataInPeriod(86400, 0, now));
      }

      start += 1;
    }
  }

  // Zero the data buffer
  for (int i = 0; i < 86401; i++) {
    time_t stamp = now + i;
    iostattbins.StampBufferZero(stamp);
  }

  ASSERT_EQ(0, iostattbins.GetDataInPeriod(86400, 0, now));
  // Test integral of the buffer for 4 transfers with same rate 1B/s,
  // but with different length and start time
  now = 86400;
  iostattbins.UpdateTransferSampleInfo(now);

  for (int i = 1; i < 5; i++) {
    start = i * 10000;
    stop = start + i * 10000 - 1;
    //4*10000 + 3*10000 + 2*10000 + 1*10000
    iostattbins.Add(i * 10000, start, stop, now);
  }

  iostattbins.UpdateTransferSampleInfo(now);
  ASSERT_EQ(30000, iostattbins.GetTimeToPercComplete(P90));
  ASSERT_EQ(35000, iostattbins.GetTimeToPercComplete(P95));
  ASSERT_EQ(39000, iostattbins.GetTimeToPercComplete(P99));
  ASSERT_EQ(40000, iostattbins.GetTimeToPercComplete(ALL));
}

//------------------------------------------------------------------------------
// Generate sequential 1GB transfers taking between 1 and 5 min with
// uniform probability. There are no overlapping transfers by design.
//------------------------------------------------------------------------------
TEST(IostatPeriods, SequentialTx)
{
  IostatPeriods iop;
  time_t now = IostatPeriods::sBins * IostatPeriods::sBinWidth;
  time_t start_ts = 0;
  time_t stop_ts = 0;
  unsigned long long val = 1024 * 1025 * 1024;
  int count = 0;

  while (stop_ts < now) {
    stop_ts = start_ts + eos::common::getRandom(1, 5) * 60;
    iop.Add(val, start_ts, stop_ts, now);
    start_ts = stop_ts;
    ++count;
  }

  ASSERT_GE(301, iop.GetLongestTransferTime());
  ASSERT_EQ(count * val, iop.GetTotalSum());
}
