//------------------------------------------------------------------------------
// File: CompressOssTests.cc
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

#include "common/LayoutId.hh"
#include "fst/compress/BlockCompress.hh"
#include "fst/compress/Entropy.hh"
#include "gtest/gtest.h"
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <cerrno>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/xattr.h>
#include <unistd.h>

#include <string>
#include <vector>

using eos::common::LayoutId;
using eos::fst::XrdFstOss;
using eos::fst::XrdFstOssFile;

namespace {

XrdFstOss& GetTestOss()
{
  static XrdSysLogger logger(fileno(stderr), 0);
  static XrdFstOss* oss = []() {
    auto* instance = new XrdFstOss();
    instance->Init(&logger, nullptr);
    return instance;
  }();
  return *oss;
}

class CompressOssTest : public ::testing::Test {
protected:
  void SetUp() override {
    (void)GetTestOss();
    char tmpl[] = "/tmp/eos-compress-oss-XXXXXX";
    const int fd = mkstemp(tmpl);
    ASSERT_GE(fd, 0);
    mPath = tmpl;
    close(fd);
  }

  void TearDown() override {
    if (mOss) {
      mOss->Close();
      delete mOss;
      mOss = nullptr;
    }
    if (!mPath.empty()) {
      unlink(mPath.c_str());
      unlink((mPath + ".eoscompress").c_str());
    }
  }

  unsigned long CompressedLid() const {
    unsigned long lid = LayoutId::GetId(LayoutId::kPlain, LayoutId::kAdler);
    LayoutId::SetCompression(lid, true);
    return lid;
  }

  int OpenWithLid(unsigned long lid, int flags) {
    if (mOss) {
      mOss->Close();
      delete mOss;
      mOss = nullptr;
    }
    mOss = new XrdFstOssFile("compress-test");
    const std::string opaque = "&mgm.lid=" + std::to_string(lid);
    XrdOucEnv env(opaque.c_str());
    return mOss->Open(mPath.c_str(), flags, 0644, env);
  }

  int OpenCompressed(int flags) {
    return OpenWithLid(CompressedLid(), flags);
  }

  int OpenPlain(int flags) {
    return OpenWithLid(LayoutId::GetId(LayoutId::kPlain, LayoutId::kAdler), flags);
  }

  static int SetXattr(const std::string& path, const char* name,
                      const std::string& value) {
#ifdef __APPLE__
    return setxattr(path.c_str(), name, value.data(), value.size(), 0, 0);
#else
    return setxattr(path.c_str(), name, value.data(), value.size(), 0);
#endif
  }

  static bool AppendByte(void* arg) {
    const char* path = static_cast<const char*>(arg);
    const int fd = open(path, O_WRONLY | O_APPEND);

    if (fd < 0) {
      return true;
    }

    const char extra = 'Z';
    const ssize_t n = write(fd, &extra, 1);
    close(fd);
    return n != 1;
  }

  static std::string GetXattr(const std::string& path, const char* name) {
    char buf[64];
#ifdef __APPLE__
    const ssize_t n = getxattr(path.c_str(), name, buf, sizeof(buf), 0, 0);
#else
    const ssize_t n = getxattr(path.c_str(), name, buf, sizeof(buf));
#endif

    if (n < 0) {
      return {};
    }

    return std::string(buf, static_cast<std::size_t>(n));
  }

  std::string mPath;
  XrdFstOssFile* mOss = nullptr;
};

} // namespace

TEST(CompressionBlockSize, RaisesLayoutsBelow128k)
{
  using eos::fst::compress::EffectiveCompressionBlockSize;
  EXPECT_EQ(EffectiveCompressionBlockSize(4096), 128u * 1024u);
  EXPECT_EQ(EffectiveCompressionBlockSize(0), 128u * 1024u);
  EXPECT_EQ(EffectiveCompressionBlockSize(128u * 1024u), 128u * 1024u);
  EXPECT_EQ(EffectiveCompressionBlockSize(1024u * 1024u), 1024u * 1024u);
}

TEST(CompressionPotential, MapsEntropyToPercent)
{
  using eos::fst::compress::CompressionPotential;
  std::uint64_t counts[256] = {};
  EXPECT_EQ(CompressionPotential(counts, 0), 0u);
  counts['A'] = 1000;
  EXPECT_EQ(CompressionPotential(counts, 1000), 100u);
  counts['A'] = 50;
  counts['B'] = 50;
  EXPECT_EQ(CompressionPotential(counts, 100), 88u);

  for (int i = 0; i < 256; ++i) {
    counts[i] = 1;
  }

  EXPECT_EQ(CompressionPotential(counts, 256), 0u);
}

TEST(LayoutCompressionBit, RoundTripKeepsTheOtherFields) {
  unsigned long lid = LayoutId::GetId(LayoutId::kReplica, LayoutId::kCRC32C, 2,
                                      LayoutId::k1M, LayoutId::kAdler);
  EXPECT_FALSE(LayoutId::HasCompression(lid));
  LayoutId::SetCompression(lid, true);
  EXPECT_TRUE(LayoutId::HasCompression(lid));
  EXPECT_EQ(LayoutId::GetLayoutType(lid), LayoutId::kReplica);
  EXPECT_EQ(LayoutId::GetChecksum(lid),
            static_cast<unsigned long>(LayoutId::kCRC32C));
  EXPECT_EQ(LayoutId::GetBlocksize(lid), 1024ul * 1024ul);
  EXPECT_NE(LayoutId::PrintLayoutString(lid).find(":zstd"),
            std::string::npos);
  LayoutId::SetCompression(lid, false);
  EXPECT_FALSE(LayoutId::HasCompression(lid));
  EXPECT_EQ(LayoutId::CompressionRatio(2500, 1000), "1:2.5");
  EXPECT_TRUE(LayoutId::CompressionRatio(0, 1000).empty());
  EXPECT_TRUE(LayoutId::CompressionRatio(1000, 0).empty());
}

TEST_F(CompressOssTest, RoundTripAndOverwrite) {
  ASSERT_EQ(OpenCompressed(O_RDWR | O_CREAT), XrdOssOK);
  const std::string payload(3 * 4096, 'A');
  ASSERT_EQ(mOss->Write(payload.data(), 0, payload.size()),
            static_cast<ssize_t>(payload.size()));

  struct stat logical {};
  ASSERT_EQ(mOss->Fstat(&logical), 0);
  EXPECT_EQ(logical.st_size, static_cast<off_t>(payload.size()));

  char head[4] = {};
  ASSERT_EQ(pread(mOss->getFD(), head, sizeof(head), 0),
            static_cast<ssize_t>(sizeof(head)));
  EXPECT_EQ(std::string(head, sizeof(head)), "AAAA");

  std::string read_back(payload.size(), '\0');
  ASSERT_EQ(mOss->Read(read_back.data(), 0, read_back.size()),
            static_cast<ssize_t>(read_back.size()));
  EXPECT_EQ(read_back, payload);
  ASSERT_EQ(mOss->Close(), XrdOssOK);
  EXPECT_FALSE(eos::fst::compress::IsConverted(mPath));

  std::uint64_t csize = 0;
  ASSERT_EQ(eos::fst::compress::ConvertToCompressed(mPath, 4096, &csize), 0);
  EXPECT_GT(csize, 0u);
  EXPECT_LT(csize, payload.size());
  EXPECT_EQ(LayoutId::CompressionRatio(payload.size(), csize).substr(0, 2),
            "1:");

  ASSERT_EQ(OpenCompressed(O_RDWR), XrdOssOK);
  ASSERT_EQ(mOss->Read(read_back.data(), 0, read_back.size()),
            static_cast<ssize_t>(read_back.size()));
  EXPECT_EQ(read_back, payload);

  const std::string patch(100, 'B');
  ASSERT_EQ(mOss->Write(patch.data(), 5000, patch.size()),
            static_cast<ssize_t>(patch.size()));
  ASSERT_EQ(mOss->Read(read_back.data(), 0, read_back.size()),
            static_cast<ssize_t>(read_back.size()));
  EXPECT_EQ(read_back.substr(5000, patch.size()), patch);
  EXPECT_EQ(read_back.substr(0, 5000), payload.substr(0, 5000));
  EXPECT_EQ(read_back.substr(5100), payload.substr(5100));
}

TEST_F(CompressOssTest, ConvertPlainFileAtomically) {
  const std::string payload(2 * 4096, 'C');
  const int fd = open(mPath.c_str(), O_RDWR | O_TRUNC);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(pwrite(fd, payload.data(), payload.size(), 0),
            static_cast<ssize_t>(payload.size()));
  close(fd);

  std::uint64_t csize = 0;
  ASSERT_EQ(eos::fst::compress::ConvertToCompressed(mPath, 4096, &csize), 0);
  EXPECT_GT(csize, 0u);
  EXPECT_LT(csize, payload.size());
  EXPECT_EQ(LayoutId::CompressionRatio(payload.size(), csize).substr(0, 2),
            "1:");
  ASSERT_EQ(OpenCompressed(O_RDONLY), XrdOssOK);

  std::string read_back(payload.size(), '\0');
  ASSERT_EQ(mOss->Read(read_back.data(), 0, read_back.size()),
            static_cast<ssize_t>(read_back.size()));
  EXPECT_EQ(read_back, payload);

  struct stat on_disk {};
  ASSERT_EQ(stat(mPath.c_str(), &on_disk), 0);
  EXPECT_GT(on_disk.st_size, 0);
#if defined(__linux__)
  EXPECT_LT(static_cast<off_t>(on_disk.st_blocks) * 512, on_disk.st_size);
#endif
}

TEST_F(CompressOssTest, DiscardConversionWhenSourceChanges) {
  const std::string payload(4096, 'C');
  const int fd = open(mPath.c_str(), O_RDWR | O_TRUNC);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(pwrite(fd, payload.data(), payload.size(), 0),
            static_cast<ssize_t>(payload.size()));
  close(fd);

  EXPECT_EQ(eos::fst::compress::ConvertToCompressed(
              mPath, 4096, nullptr, CompressOssTest::AppendByte,
              const_cast<char*>(mPath.c_str())),
            -EAGAIN);
  EXPECT_FALSE(eos::fst::compress::IsConverted(mPath));
  EXPECT_NE(access((mPath + ".eoscompress").c_str(), F_OK), 0);

  const int check = open(mPath.c_str(), O_RDONLY);
  ASSERT_GE(check, 0);
  std::string got(payload.size() + 1, '\0');
  ASSERT_EQ(read(check, got.data(), got.size()),
            static_cast<ssize_t>(got.size()));
  close(check);
  EXPECT_EQ(got, payload + "Z");
}

TEST_F(CompressOssTest, UncompressRestoresPlainBytes) {
  std::string payload(4096, 'A');
  payload.resize(4096 + 200);

  for (std::size_t i = 4096; i < payload.size(); ++i) {
    payload[i] = static_cast<char>(i & 0xff);
  }

  const int fd = open(mPath.c_str(), O_RDWR | O_TRUNC);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(pwrite(fd, payload.data(), payload.size(), 0),
            static_cast<ssize_t>(payload.size()));
  close(fd);
  ASSERT_EQ(SetXattr(mPath, "user.eos.checksum", "abcd"), 0);
  ASSERT_EQ(eos::fst::compress::ConvertToCompressed(mPath, 4096), 0);
  ASSERT_TRUE(eos::fst::compress::IsConverted(mPath));
  const int blockfd = open(mPath.c_str(), O_RDONLY);
  ASSERT_GE(blockfd, 0);
  std::uint64_t stored_block = 0;
  ASSERT_TRUE(eos::fst::compress::ReadBlockSize(blockfd, stored_block));
  close(blockfd);
  EXPECT_EQ(stored_block, 128u * 1024u);
  ASSERT_EQ(eos::fst::compress::ConvertToUncompressed(mPath, 0, true), 0);
  EXPECT_FALSE(eos::fst::compress::IsConverted(mPath));
  EXPECT_TRUE(eos::fst::compress::CSizeClearPending(mPath));
  std::uint64_t leftover = 1;
  EXPECT_FALSE(eos::fst::compress::ReadCompressedSize(mPath, leftover));
  EXPECT_EQ(GetXattr(mPath, "user.eos.checksum"), "abcd");
  EXPECT_TRUE(eos::fst::compress::DropCSizeClear(mPath));
  EXPECT_FALSE(eos::fst::compress::CSizeClearPending(mPath));

  struct stat on_disk {};
  ASSERT_EQ(stat(mPath.c_str(), &on_disk), 0);
  EXPECT_EQ(on_disk.st_size, static_cast<off_t>(payload.size()));

  const int again = eos::fst::compress::ConvertToUncompressed(mPath, 4096, true);
  EXPECT_EQ(again, -EINVAL);
  EXPECT_FALSE(eos::fst::compress::CSizeClearPending(mPath));

  ASSERT_EQ(OpenPlain(O_RDONLY), XrdOssOK);
  std::string read_back(payload.size(), '\0');
  ASSERT_EQ(mOss->Read(read_back.data(), 0, read_back.size()),
            static_cast<ssize_t>(read_back.size()));
  EXPECT_EQ(read_back, payload);
}
