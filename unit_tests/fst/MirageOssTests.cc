//------------------------------------------------------------------------------
// File: MirageOssTests.cc
// Author: Andreas-Joachim Peters - CERN
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

#define IN_TEST_HARNESS
#include "fst/XrdFstOss.hh"
#include "fst/XrdFstOssFile.hh"
#undef IN_TEST_HARNESS

#include "common/Mirage.hh"
#include "gtest/gtest.h"
#include <XrdOuc/XrdOucEnv.hh>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

using eos::fst::XrdFstOss;
using eos::fst::XrdFstOssFile;

namespace {

XrdFstOss& GetTestOss()
{
  static XrdFstOss* oss = []() {
    auto* instance = new XrdFstOss();
    instance->Init(nullptr, nullptr);
    return instance;
  }();
  return *oss;
}

struct MirageOssTestInit {
  MirageOssTestInit()
  {
    (void)GetTestOss();
  }
};

const MirageOssTestInit g_mirage_oss_test_init;

class MirageOssTest : public ::testing::Test {
protected:
  void SetUp() override {
    (void)GetTestOss();
    char tmpl[] = "/tmp/eos-mirage-oss-XXXXXX";
    mFd = mkstemp(tmpl);
    ASSERT_GE(mFd, 0);
    mPath = tmpl;
    close(mFd);
    mFd = -1;
  }

  void TearDown() override {
    if (mOss) {
      mOss->Close();
      delete mOss;
      mOss = nullptr;
    }
    if (mFd >= 0) {
      close(mFd);
      mFd = -1;
    }
    if (!mPath.empty()) {
      unlink(mPath.c_str());
    }
  }

  int openMirage(const char* mirage_value, int flags) {
    if (mOss) {
      mOss->Close();
      delete mOss;
      mOss = nullptr;
    }
    mOss = new XrdFstOssFile("mirage-test");
    std::string opaque = "&eos.mirage=";
    opaque += mirage_value;
    XrdOucEnv env(opaque.c_str());
    return mOss->Open(mPath.c_str(), flags, 0644, env);
  }

  std::string mPath;
  int mFd = -1;
  XrdFstOssFile* mOss = nullptr;
};

} // namespace

TEST_F(MirageOssTest, RejectsInvalidMirageValue) {
  mOss = new XrdFstOssFile("mirage-test");
  XrdOucEnv env("&eos.mirage=not-a-mirage");
  EXPECT_LT(mOss->Open(mPath.c_str(), O_RDWR | O_CREAT, 0644, env), 0);
}

TEST_F(MirageOssTest, PatternWriteDiscardsAndReadSynthesizes) {
  ASSERT_EQ(openMirage("pattern:abc", O_RDWR | O_CREAT), XrdOssOK);

  const char garbage[] = "XXXXXXXXX";
  const std::size_t payload_len = sizeof(garbage) - 1;
  EXPECT_EQ(mOss->Write(garbage, 0, payload_len),
            static_cast<ssize_t>(payload_len));

  struct stat statinfo {};
  ASSERT_EQ(mOss->Fstat(&statinfo), 0);
  EXPECT_EQ(statinfo.st_size, static_cast<off_t>(payload_len));

  std::vector<char> disk_buf(statinfo.st_size, '\1');
  ASSERT_EQ(pread(mOss->getFD(), disk_buf.data(), disk_buf.size(), 0),
            static_cast<ssize_t>(disk_buf.size()));
  EXPECT_EQ(std::string(disk_buf.begin(), disk_buf.end()),
            std::string(disk_buf.size(), '\0'))
      << "mirage write should not persist client payload on disk";

  char read_buf[10] = {};
  EXPECT_EQ(mOss->Read(read_buf, 0, sizeof(read_buf)),
            static_cast<ssize_t>(payload_len));
  EXPECT_EQ(std::string(read_buf, payload_len), "abcabcabc");
}

TEST_F(MirageOssTest, DeterministicSupportsRandomAccess) {
  ASSERT_EQ(openMirage("algorithm:deterministic:1", O_RDWR | O_CREAT), XrdOssOK);

  const char payload[128] = {};
  ASSERT_EQ(mOss->Write(payload, 0, sizeof(payload)),
            static_cast<ssize_t>(sizeof(payload)));

  char first[16] = {};
  char second[16] = {};
  EXPECT_EQ(mOss->Read(first, 0, sizeof(first)),
            static_cast<ssize_t>(sizeof(first)));
  EXPECT_EQ(mOss->Read(second, 64, sizeof(second)),
            static_cast<ssize_t>(sizeof(second)));

  eos::common::MirageSpec spec;
  spec.kind = eos::common::MirageSpec::Kind::Deterministic;
  spec.seed = 1;
  char expected[16] = {};
  eos::common::mirage_fill(spec, 64, expected, sizeof(expected));
  EXPECT_EQ(std::memcmp(second, expected, sizeof(second)), 0);
}

TEST_F(MirageOssTest, XoshiroRequiresSequentialReads) {
  ASSERT_EQ(openMirage("algorithm:xoshiro256pp:0", O_RDWR | O_CREAT), XrdOssOK);

  std::vector<char> payload(2048, 'z');
  ASSERT_EQ(mOss->Write(payload.data(), 0, payload.size()),
            static_cast<ssize_t>(payload.size()));

  char buf[128] = {};
  EXPECT_EQ(mOss->Read(buf, 0, 64), 64);
  EXPECT_EQ(mOss->Read(buf, 64, 64), 64);
  EXPECT_EQ(mOss->Read(buf, 200, 64), -ESPIPE);
}

TEST_F(MirageOssTest, WriteExtendsSparseFile) {
  ASSERT_EQ(openMirage("pattern:x", O_RDWR | O_CREAT), XrdOssOK);

  EXPECT_EQ(mOss->Write("a", 0, 1), 1);

  struct stat after_first {};
  ASSERT_EQ(mOss->Fstat(&after_first), 0);
  EXPECT_EQ(after_first.st_size, 1);

  EXPECT_EQ(mOss->Write("b", 1024, 1), 1);

  struct stat after_extend {};
  ASSERT_EQ(mOss->Fstat(&after_extend), 0);
  EXPECT_EQ(after_extend.st_size, 1025);
}
