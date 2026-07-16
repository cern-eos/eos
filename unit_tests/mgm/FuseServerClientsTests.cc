//------------------------------------------------------------------------------
// File: FuseServerClientsTests.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
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

#include "mgm/FuseServer/Clients.hh"
#include "mgm/fsview/FsView.hh"
#include "gtest/gtest.h"
#include <atomic>
#include <map>
#include <thread>

using namespace eos::mgm::FuseServer;
using eos::mgm::FsView;

class MockClientsFsView : public FsView {
public:
  std::map<std::string, std::string> kvdict;
  std::string
  GetGlobalConfig(const std::string& key) override
  {
    auto it = kvdict.find(key);
    return (it == kvdict.end()) ? "" : it->second;
  }
  bool
  SetGlobalConfig(const std::string& key, const std::string& value) override
  {
    kvdict[key] = value;
    return true;
  }
};

class ClientsTest : public ::testing::Test {
protected:
  Clients clients;
};

TEST_F(ClientsTest, HeartbeatIntervalBoundaries)
{
  std::string msg;
  // valid range [1..15]
  ASSERT_TRUE(clients.Config(Clients::sHbiKey, "1", msg));
  ASSERT_EQ(1, clients.HeartbeatInterval());
  ASSERT_TRUE(clients.Config(Clients::sHbiKey, "15", msg));
  ASSERT_EQ(15, clients.HeartbeatInterval());
  // out of range must be rejected and must not modify live state
  msg.clear();
  ASSERT_FALSE(clients.Config(Clients::sHbiKey, "0", msg));
  ASSERT_FALSE(msg.empty());
  ASSERT_EQ(15, clients.HeartbeatInterval());
  msg.clear();
  ASSERT_FALSE(clients.Config(Clients::sHbiKey, "16", msg));
  ASSERT_FALSE(msg.empty());
  ASSERT_EQ(15, clients.HeartbeatInterval());
  msg.clear();
  ASSERT_FALSE(clients.Config(Clients::sHbiKey, "-5", msg));
  ASSERT_EQ(15, clients.HeartbeatInterval());
  // non-numeric input is atoi'd to 0, which is out of range too
  msg.clear();
  ASSERT_FALSE(clients.Config(Clients::sHbiKey, "abc", msg));
  ASSERT_EQ(15, clients.HeartbeatInterval());
}

TEST_F(ClientsTest, QuotaCheckIntervalBoundaries)
{
  std::string msg;
  ASSERT_TRUE(clients.Config(Clients::sQtiKey, "1", msg));
  ASSERT_EQ(1, clients.QuotaCheckInterval());
  ASSERT_TRUE(clients.Config(Clients::sQtiKey, "60", msg));
  ASSERT_EQ(60, clients.QuotaCheckInterval());
  msg.clear();
  ASSERT_FALSE(clients.Config(Clients::sQtiKey, "0", msg));
  ASSERT_EQ(60, clients.QuotaCheckInterval());
  msg.clear();
  ASSERT_FALSE(clients.Config(Clients::sQtiKey, "61", msg));
  ASSERT_EQ(60, clients.QuotaCheckInterval());
}

TEST_F(ClientsTest, BroadcastAudienceSetters)
{
  std::string msg;
  ASSERT_TRUE(clients.Config(Clients::sBcaKey, "42", msg));
  ASSERT_EQ(42, clients.BroadCastMaxAudience());
  ASSERT_TRUE(clients.Config(Clients::sBcaMatchKey, "myhost.*", msg));
  ASSERT_EQ("myhost.*", clients.BroadCastAudienceSuppressMatch());
  // bca has no range validation (matches pre-existing behavior - atoi of
  // garbage input silently becomes 0)
  ASSERT_TRUE(clients.Config(Clients::sBcaKey, "not-a-number", msg));
  ASSERT_EQ(0, clients.BroadCastMaxAudience());
}

TEST_F(ClientsTest, UnknownKeyRejected)
{
  std::string msg;
  ASSERT_FALSE(clients.Config("bogus", "1", msg));
  ASSERT_FALSE(msg.empty());
}

TEST_F(ClientsTest, DefaultBroadcastAudienceIsZero)
{
  ASSERT_EQ(0, clients.BroadCastMaxAudience());
}

TEST_F(ClientsTest, BcaMatchCanBeCleared)
{
  std::string msg;
  ASSERT_TRUE(clients.Config(Clients::sBcaMatchKey, "somehost.*", msg));
  ASSERT_EQ("somehost.*", clients.BroadCastAudienceSuppressMatch());
  ASSERT_TRUE(clients.Config(Clients::sBcaMatchKey, "", msg));
  ASSERT_EQ("", clients.BroadCastAudienceSuppressMatch());
}

TEST_F(ClientsTest, ConcurrentBroadcastConfigAccessIsSafe)
{
  std::atomic<bool> stop{false};
  std::thread reader([&] {
    while (!stop.load()) {
      volatile int a = clients.BroadCastMaxAudience();
      std::string m = clients.BroadCastAudienceSuppressMatch();
      (void)a;
      (void)m;
    }
  });

  for (int i = 0; i < 20000; ++i) {
    clients.SetBroadCastMaxAudience(i % 100);
    clients.SetBroadCastAudienceSuppressMatch("match-" + std::to_string(i % 50));
  }

  stop.store(true);
  reader.join();
  SUCCEED();
}

TEST_F(ClientsTest, StoreConfigProducesExpectedBlob)
{
  MockClientsFsView mock_fsview;
  std::string msg;
  // Config() itself persists through the real FsView::gFsView singleton
  // (same pattern as Iostat's Start*/Stop*), so set live state via Config()
  // and then explicitly persist to the mock via the parameterized
  // StoreConfig(fsview) to inspect the exact blob shape
  clients.Config(Clients::sHbiKey, "7", msg);
  clients.Config(Clients::sQtiKey, "20", msg);
  clients.Config(Clients::sBcaKey, "5", msg);
  ASSERT_TRUE(clients.StoreConfig(&mock_fsview));
  ASSERT_EQ("hbi=7 qti=20 bca=5 bca_match=",
            mock_fsview.GetGlobalConfig(Clients::sFusexKey));
}

TEST_F(ClientsTest, ApplyConfigNewFormatRoundTrip)
{
  MockClientsFsView mock_fsview;
  mock_fsview.kvdict[Clients::sFusexKey] = "hbi=12 qti=45 bca=3 bca_match=abc";
  clients.ApplyConfig(&mock_fsview);
  ASSERT_EQ(12, clients.HeartbeatInterval());
  ASSERT_EQ(45, clients.QuotaCheckInterval());
  ASSERT_EQ(3, clients.BroadCastMaxAudience());
  ASSERT_EQ("abc", clients.BroadCastAudienceSuppressMatch());
}

TEST_F(ClientsTest, ApplyConfigMalformedBlobDoesNotCrash)
{
  MockClientsFsView mock_fsview;
  // missing '=', out-of-range hbi (ignored, not applied), unknown key
  mock_fsview.kvdict[Clients::sFusexKey] = "hbi qti=999 bogus=1 bca=2";
  ASSERT_NO_THROW(clients.ApplyConfig(&mock_fsview));
  // qti=999 is out of [1..60], must be rejected, live value stays default
  ASSERT_EQ(10, clients.QuotaCheckInterval());
  // bca=2 is a valid trailing key and must still get applied
  ASSERT_EQ(2, clients.BroadCastMaxAudience());
}

TEST_F(ClientsTest, ApplyConfigLegacyFallbackNoDefaultSpaceIsSafe)
{
  MockClientsFsView mock_fsview;
  ASSERT_TRUE(mock_fsview.mSpaceView.empty());
  ASSERT_NO_THROW(clients.ApplyConfig(&mock_fsview));
  ASSERT_EQ(10, clients.HeartbeatInterval());
  ASSERT_EQ(10, clients.QuotaCheckInterval());
}

TEST_F(ClientsTest, BcaMatchWithSpaceCorruptsOnRoundTrip)
{
  std::string msg;
  ASSERT_TRUE(clients.Config(Clients::sBcaMatchKey, "foo bar", msg));
  ASSERT_EQ("foo bar", clients.BroadCastAudienceSuppressMatch());
  MockClientsFsView mock_fsview;
  mock_fsview.kvdict[Clients::sFusexKey] = "hbi=10 qti=10 bca=0 bca_match=foo bar";
  Clients replay;
  replay.ApplyConfig(&mock_fsview);
  // "bar" is parsed as its own bogus token instead of being part of
  // bca_match's value
  ASSERT_EQ("foo", replay.BroadCastAudienceSuppressMatch());
}

TEST_F(ClientsTest, BcaMatchWithEqualsSignAlsoCorruptsOnRoundTrip)
{
  MockClientsFsView mock_fsview;
  mock_fsview.kvdict[Clients::sFusexKey] = "hbi=10 qti=10 bca=0 bca_match=a=b";
  ASSERT_NO_THROW(clients.ApplyConfig(&mock_fsview));
  // only "a" survives; "=b" is silently dropped by the key=value split
  ASSERT_EQ("a", clients.BroadCastAudienceSuppressMatch());
}

TEST_F(ClientsTest, ConsolidatedKeyTakesPrecedenceOverLegacyFallback)
{
  MockClientsFsView mock_fsview;
  mock_fsview.kvdict[Clients::sFusexKey] = "hbi=3 qti=6 bca=9 bca_match=xyz";
  clients.ApplyConfig(&mock_fsview);
  ASSERT_EQ(3, clients.HeartbeatInterval());
  ASSERT_EQ(6, clients.QuotaCheckInterval());
  ASSERT_EQ(9, clients.BroadCastMaxAudience());
  ASSERT_EQ("xyz", clients.BroadCastAudienceSuppressMatch());
}
