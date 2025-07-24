//------------------------------------------------------------------------------
// File: XrdFstOfsFileTest.cc
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

#define IN_TEST_HARNESS
#include "fst/XrdFstOfsFile.hh"
#include "fst/XrdFstOfs.hh"
#undef IN_TEST_HARNESS
#include "common/LayoutId.hh"
#include "gtest/gtest.h"
#include <XrdSys/XrdSysError.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <algorithm>
#include <chrono>
#include <memory>
#include <new>
#include <sys/time.h>

using namespace eos::fst;

//! XrdOfs globals that are normally wired up by XrdSfsGetFileSystem2, which a
//! unit test never calls. OfsEroute is built with a null logger and XrdOfsFS
//! stays NULL.
extern XrdSysError OfsEroute;
extern XrdOfs* XrdOfsFS;

namespace {
//------------------------------------------------------------------------------
// Bring the XrdOfs globals into the state XrdSfsGetFileSystem2 would leave them
// in, otherwise anything built on top of XrdOfsFile dereferences a null pointer:
//   - every gOFS.Emsg() ends up in XrdSysError::Emsg which unconditionally
//     dereferences OfsEroute's XrdSysLogger
//   - ~XrdOfsFile() is "if (oh) close()" and oh is always XrdOfs::dummyHandle,
//     so destroying any file object calls XrdOfsFile::close() which takes
//     XrdOfsFS->ocMutex
// This has to happen after static initialization since both globals live in
// other translation units, hence a gtest environment rather than a global object.
//------------------------------------------------------------------------------
class XrdOfsGlobalsEnv : public ::testing::Environment {
public:
  void
  SetUp() override
  {
    static XrdSysLogger logger;
    OfsEroute.logger(&logger);

    if (XrdOfsFS == nullptr) {
      XrdOfsFS = &gOFS;
    }
  }
};

[[maybe_unused]] const auto* const sXrdOfsGlobalsEnv =
    ::testing::AddGlobalTestEnvironment(new XrdOfsGlobalsEnv());

//------------------------------------------------------------------------------
// Build a file object ready for a ProcessMixedOpaque call. The capability
// opaque deliberately misses mgm.fsid so that ProcessMixedOpaque bails out
// right after the checksum handling, without touching the storage layer.
// Note that this bail-out goes through gOFS.Emsg, see OfsErouteEnv above.
//------------------------------------------------------------------------------
std::unique_ptr<XrdFstOfsFile>
MakeFileForMixedOpaque(const std::string& open_opaque, unsigned long lid)
{
  auto file = std::make_unique<XrdFstOfsFile>("test-user", 0);
  file->mOpenOpaque.reset(new XrdOucEnv(open_opaque.c_str()));
  file->mCapOpaque.reset(new XrdOucEnv(""));
  file->mLid = lid;
  // Skip the full close machinery when the object is destroyed
  file->mClosed = true;
  return file;
}
} // namespace

//------------------------------------------------------------------------------
// ProcessMixedOpaque must hand the layout checksum object over to the
// checksum group, otherwise no checksum is computed, stored or verified
// during file IO. Regression test for the lost SetDefault hand-off.
//------------------------------------------------------------------------------
TEST(XrdFstOfsFileTest, ProcessMixedOpaqueSetsChecksumGroupDefault)
{
  using eos::common::LayoutId;
  const auto adler_lid = LayoutId::GetId(LayoutId::kPlain, LayoutId::kAdler);
  {
    // Plain open without any client-provided checksum expectation
    auto file = MakeFileForMixedOpaque("", adler_lid);
    (void)file->ProcessMixedOpaque();
    ASSERT_FALSE(file->HasChecksumValidationError());
    ASSERT_TRUE(file->mChecksumGroup->HasChecksums());
    ASSERT_TRUE(file->mChecksumGroup->GetDefault() != nullptr);
    ASSERT_STREQ("adler", file->mChecksumGroup->GetDefault()->GetName());
  }
  {
    // Client provided an expected checksum value and a type matching the layout
    auto file = MakeFileForMixedOpaque("mgm.checksum=12345678&mgm.checksumtypereq=adler",
                                       adler_lid);
    (void)file->ProcessMixedOpaque();
    ASSERT_FALSE(file->HasChecksumValidationError());
    ASSERT_TRUE(file->mChecksumGroup->HasChecksums());
    ASSERT_TRUE(file->mChecksumGroup->GetDefault() != nullptr);
    ASSERT_STREQ("adler", file->mChecksumGroup->GetDefault()->GetName());
  }
  {
    // The adler32 alias must be accepted as well
    auto file = MakeFileForMixedOpaque(
        "mgm.checksum=12345678&mgm.checksumtypereq=adler32", adler_lid);
    (void)file->ProcessMixedOpaque();
    ASSERT_FALSE(file->HasChecksumValidationError());
    ASSERT_TRUE(file->mChecksumGroup->HasChecksums());
  }
  {
    // Checksumming disabled by the client
    auto file = MakeFileForMixedOpaque("mgm.checksum=ignore", adler_lid);
    (void)file->ProcessMixedOpaque();
    ASSERT_FALSE(file->HasChecksumValidationError());
    ASSERT_FALSE(file->mChecksumGroup->HasChecksums());
  }
}

//------------------------------------------------------------------------------
// ProcessMixedOpaque must reject a requested checksum type it cannot verify
// and expose the reason via the checksum validation error
//------------------------------------------------------------------------------
TEST(XrdFstOfsFileTest, ProcessMixedOpaqueChecksumTypeValidation)
{
  using eos::common::LayoutId;
  const auto adler_lid = LayoutId::GetId(LayoutId::kPlain, LayoutId::kAdler);
  const auto no_xs_lid = LayoutId::GetId(LayoutId::kPlain, LayoutId::kNone);
  {
    // Unknown checksum type
    auto file = MakeFileForMixedOpaque(
        "mgm.checksum=12345678&mgm.checksumtypereq=does_not_exist", adler_lid);
    ASSERT_NE(SFS_OK, file->ProcessMixedOpaque());
    ASSERT_TRUE(file->HasChecksumValidationError());
    ASSERT_NE(std::string::npos, file->GetChecksumErrText().find("does not exist"));
  }
  {
    // Layout without any checksum configured
    auto file = MakeFileForMixedOpaque("mgm.checksum=12345678&mgm.checksumtypereq=adler",
                                       no_xs_lid);
    ASSERT_NE(SFS_OK, file->ProcessMixedOpaque());
    ASSERT_TRUE(file->HasChecksumValidationError());
    ASSERT_NE(std::string::npos,
              file->GetChecksumErrText().find("does not have checksum"));
  }
  {
    // Requested type differs from the layout checksum type
    auto file = MakeFileForMixedOpaque("mgm.checksum=12345678&mgm.checksumtypereq=md5",
                                       adler_lid);
    ASSERT_NE(SFS_OK, file->ProcessMixedOpaque());
    ASSERT_TRUE(file->HasChecksumValidationError());
    ASSERT_NE(std::string::npos, file->GetChecksumErrText().find("does not match"));
  }
}

TEST(XrdFstOfsFileTest, FilterTags)
{
  std::set<std::string> tags {"xrdcl.secuid", "xrdcl.secgid"};
  std::string opaque =
    "eos.app=demo&oss.size=13&xrdcl.secuid=2134&xrdcl.secgid=99";
  eos::fst::XrdFstOfsFile::FilterTagsInPlace(opaque, tags);
  ASSERT_STREQ(opaque.c_str(), "eos.app=demo&oss.size=13");
  opaque = "eos.app=demo&oss.size=13";
  eos::fst::XrdFstOfsFile::FilterTagsInPlace(opaque, tags);
  ASSERT_STREQ(opaque.c_str(), "eos.app=demo&oss.size=13");
  opaque = "eos.app=demo&oss.size=13&xrdcl.secuid=2134&xrdcl.secgid=99&"
           "xrdcl.other=tag&eos.lfn=/some/dummy/path&";
  eos::fst::XrdFstOfsFile::FilterTagsInPlace(opaque, tags);
  ASSERT_STREQ(opaque.c_str(),
               "eos.app=demo&oss.size=13&xrdcl.other=tag&eos.lfn=/some/dummy/path");
  opaque = "";
  eos::fst::XrdFstOfsFile::FilterTagsInPlace(opaque, tags);
  ASSERT_STREQ(opaque.c_str(), "");
}

TEST(XrdFstOfsFileTest, GetHostFromTident)
{
  std::string hostname;
  std::string tident = "root.1.2@eospps.cern.ch";
  ASSERT_TRUE(XrdFstOfsFile::GetHostFromTident(tident, hostname));
  ASSERT_STREQ(hostname.c_str(), "eospps");
  tident = "root@eospps.ipv6.cern.ch";
  ASSERT_TRUE(XrdFstOfsFile::GetHostFromTident(tident, hostname));
  ASSERT_STREQ(hostname.c_str(), "eospps");
  tident = "root.1.1@eospps.dyndns.some.other.ipv6.cern.ch";
  ASSERT_TRUE(XrdFstOfsFile::GetHostFromTident(tident, hostname));
  ASSERT_STREQ(hostname.c_str(), "eospps");
  tident = "root.1.1@eospps";
  ASSERT_TRUE(XrdFstOfsFile::GetHostFromTident(tident, hostname));
  ASSERT_STREQ(hostname.c_str(), "eospps");
  tident = "root.1.1_eospps.dyndns.some.other.ipv6.cern.ch";
  ASSERT_FALSE(XrdFstOfsFile::GetHostFromTident(tident, hostname));
  ASSERT_STREQ(hostname.c_str(), "");
  tident = "root.1.1@";
  ASSERT_FALSE(XrdFstOfsFile::GetHostFromTident(tident, hostname));
  ASSERT_STREQ(hostname.c_str(), "");
  tident = "root.1.1";
  ASSERT_FALSE(XrdFstOfsFile::GetHostFromTident(tident, hostname));
  ASSERT_STREQ(hostname.c_str(), "");
}

//------------------------------------------------------------------------------
// Test tpc.ttl value enforcement
//------------------------------------------------------------------------------
TEST(XrdFstOfsFileTest, GetTpcKeyExpireTS)
{
  time_t now_test = time(nullptr);
  ASSERT_EQ(now_test + 120,
            eos::fst::XrdFstOfsFile::GetTpcKeyExpireTS("", now_test));
  ASSERT_EQ(now_test + 120,
            eos::fst::XrdFstOfsFile::GetTpcKeyExpireTS("61",  now_test));
  ASSERT_EQ(now_test + 200,
            eos::fst::XrdFstOfsFile::GetTpcKeyExpireTS("200", now_test));
  ASSERT_EQ(now_test + 900,
            eos::fst::XrdFstOfsFile::GetTpcKeyExpireTS("1000", now_test));
  setenv("EOS_FST_TPC_KEY_MIN_VALIDITY_SEC", "30", true);
  setenv("EOS_FST_TPC_KEY_MAX_VALIDITY_SEC", "4000", true);
  gOFS.UpdateTpcKeyValidity();
  ASSERT_STREQ("30", getenv("EOS_FST_TPC_KEY_MIN_VALIDITY_SEC"));
  ASSERT_STREQ("4000", getenv("EOS_FST_TPC_KEY_MAX_VALIDITY_SEC"));
  ASSERT_EQ(60, gOFS.mTpcKeyMinValidity.count());
  ASSERT_EQ(3600, gOFS.mTpcKeyMaxValidity.count());
  ASSERT_EQ(now_test + 60,
            eos::fst::XrdFstOfsFile::GetTpcKeyExpireTS("", now_test));
  ASSERT_EQ(now_test + 61,
            eos::fst::XrdFstOfsFile::GetTpcKeyExpireTS("61", now_test));
  ASSERT_EQ(now_test + 200,
            eos::fst::XrdFstOfsFile::GetTpcKeyExpireTS("200", now_test));
  ASSERT_EQ(now_test + 1000,
            eos::fst::XrdFstOfsFile::GetTpcKeyExpireTS("1000", now_test));
  ASSERT_EQ(now_test + 3600,
            eos::fst::XrdFstOfsFile::GetTpcKeyExpireTS("4000", now_test));
}

//------------------------------------------------------------------------------
// Test the bandwidth limitation schedule (mgm.iobw given in MB/s)
//------------------------------------------------------------------------------
TEST(XrdFstOfsFileTest, GetBandwidthSleepMs)
{
  constexpr unsigned long long ten_mb = 10ull * 1024 * 1024;
  // No limit configured
  ASSERT_EQ(0, XrdFstOfsFile::GetBandwidthSleepMs(0, ten_mb, 0));
  ASSERT_EQ(0, XrdFstOfsFile::GetBandwidthSleepMs(-1, ten_mb, 0));
  // 10 MiB at 5 MB/s is scheduled to take 2097 ms
  ASSERT_EQ(2097, XrdFstOfsFile::GetBandwidthSleepMs(5, ten_mb, 0));
  ASSERT_EQ(1097, XrdFstOfsFile::GetBandwidthSleepMs(5, ten_mb, 1000));
  // On or behind the schedule there is nothing to sleep
  ASSERT_EQ(0, XrdFstOfsFile::GetBandwidthSleepMs(5, ten_mb, 2097));
  ASSERT_EQ(0, XrdFstOfsFile::GetBandwidthSleepMs(5, ten_mb, 5000));
  // Deficits below the millisecond are deferred to a subsequent call
  ASSERT_EQ(0, XrdFstOfsFile::GetBandwidthSleepMs(5, 4096, 0));
  // Long transfers at a low limit must not overflow - 10 TB at 1 MB/s
  ASSERT_EQ(10000000000ll, XrdFstOfsFile::GetBandwidthSleepMs(1, 10000000000000ull, 0));
}

//------------------------------------------------------------------------------
// The bandwidth regulation must sleep the current deficit with respect to the
// schedule and never an accumulated total, otherwise the individual stalls
// grow without bound over the transfer while the stream runs completely
// unthrottled in between them.
//------------------------------------------------------------------------------
TEST(XrdFstOfsFileTest, RegulateBandwidthNoSleepAccumulation)
{
  constexpr int bandwidth_mb = 10;
  constexpr unsigned long long chunk = 1024 * 1024;
  // Milliseconds one chunk is scheduled to take
  constexpr int64_t chunk_ms = chunk / (bandwidth_mb * 1000);
  // Constructed in place and deliberately never destroyed - the inherited
  // XrdOfsFile destructor closes the file and dereferences the XrdOfs globals
  // which are not set up outside of a running FST
  alignas(XrdFstOfsFile) static char storage[sizeof(XrdFstOfsFile)];
  XrdFstOfsFile* file = new (storage) XrdFstOfsFile("test", 0);
  file->mBandwidth = bandwidth_mb;
  gettimeofday(&file->openTime, &file->tz);
  int64_t max_stall_ms = 0;

  for (int i = 0; i < 12; ++i) {
    const auto start = std::chrono::steady_clock::now();
    file->RegulateBandwidth();
    const auto stall = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now() - start)
                           .count();
    max_stall_ms = std::max(max_stall_ms, (int64_t)stall);
    file->totalBytes += chunk;
  }

  // No individual stall drifts beyond one chunk of the schedule. Sleeping an
  // accumulated deficit instead reaches 4 chunks within this loop already and
  // keeps growing from there for the rest of the transfer.
  ASSERT_LE(max_stall_ms, 2 * chunk_ms);
  // The throttle did engage and the sleeping is accounted in the io report
  ASSERT_GT(file->msSleep, 0u);
}
