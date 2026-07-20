//------------------------------------------------------------------------------
//! @file SparseJournalTests.cc
//! @brief Unit tests for FST sparse-range cache journal
//------------------------------------------------------------------------------

#include "gtest/gtest.h"
#include "fst/cache/SparseJournal.hh"
#include "fst/cache/CacheLru.hh"
#include <cstdlib>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

using eos::fst::SparseJournal;
using eos::fst::CacheLru;

namespace
{
std::string MakeTempDir()
{
  char tmpl[] = "/tmp/eos-cache-test-XXXXXX";
  char* dir = mkdtemp(tmpl);
  EXPECT_NE(dir, nullptr);
  return dir ? std::string(dir) : std::string();
}
}

TEST(SparseJournal, WriteReadAndMissingRanges)
{
  const std::string root = MakeTempDir();
  ASSERT_FALSE(root.empty());
  SparseJournal journal;
  ASSERT_EQ(0, journal.Open(root, 0xabc, 4096, 12345));

  const std::string payload = "hello-cache-range";
  ASSERT_EQ(0, journal.Write(payload.data(), payload.size(), 100));
  ASSERT_EQ(payload.size(), journal.CachedBytes());

  char buf[64] = {};
  ASSERT_EQ((ssize_t) payload.size(), journal.Read(buf, payload.size(), 100));
  ASSERT_EQ(payload, std::string(buf, payload.size()));

  // Request covering a hole before the cached range
  auto missing = journal.MissingRanges(0, 200);
  ASSERT_FALSE(missing.empty());
  EXPECT_EQ(0, missing.front().offset);
  EXPECT_EQ(100u, missing.front().size);

  // Leading hole returns 0 from Read
  EXPECT_EQ(0, journal.Read(buf, 50, 0));

  ASSERT_EQ(0, journal.Truncate());
  EXPECT_EQ(0u, journal.CachedBytes());
  EXPECT_EQ(0, journal.Read(buf, payload.size(), 100));

  ASSERT_EQ(0, journal.Unlink());
  rmdir((root + "/.eoscache").c_str());
  rmdir(root.c_str());
}

TEST(SparseJournal, MergeAdjacentRanges)
{
  const std::string root = MakeTempDir();
  ASSERT_FALSE(root.empty());
  SparseJournal journal;
  ASSERT_EQ(0, journal.Open(root, 0xdef, 1024, 1));

  const char a[] = "AAAA";
  const char b[] = "BBBB";
  ASSERT_EQ(0, journal.Write(a, 4, 0));
  ASSERT_EQ(0, journal.Write(b, 4, 4));
  EXPECT_EQ(8u, journal.CachedBytes());

  char buf[8] = {};
  ASSERT_EQ(8, journal.Read(buf, 8, 0));
  EXPECT_EQ(std::string("AAAABBBB"), std::string(buf, 8));

  auto missing = journal.MissingRanges(0, 8);
  EXPECT_TRUE(missing.empty());

  ASSERT_EQ(0, journal.Unlink());
  rmdir((root + "/.eoscache").c_str());
  rmdir(root.c_str());
}

TEST(SparseJournal, StaleIdentityTruncates)
{
  const std::string root = MakeTempDir();
  ASSERT_FALSE(root.empty());
  {
    SparseJournal journal;
    ASSERT_EQ(0, journal.Open(root, 42, 100, 10));
    ASSERT_EQ(0, journal.Write("x", 1, 0));
    EXPECT_EQ(1u, journal.CachedBytes());
  }
  {
    SparseJournal journal;
    // Different mtime => stale, should reset
    ASSERT_EQ(0, journal.Open(root, 42, 100, 11));
    EXPECT_EQ(0u, journal.CachedBytes());
    ASSERT_EQ(0, journal.Unlink());
  }
  rmdir((root + "/.eoscache").c_str());
  rmdir(root.c_str());
}

TEST(CacheLru, WatermarkAdmissionAndEviction)
{
  const std::string root = MakeTempDir();
  ASSERT_FALSE(root.empty());
  CacheLru lru(1, root);
  lru.SetCapacityBytes(1000);
  lru.SetWatermarks(40, 80); // low=400, high=800

  EXPECT_TRUE(lru.CanAdmit(700));
  lru.FileAccessed(1, 700);
  EXPECT_FALSE(lru.CanAdmit(200)); // 900 > 800

  lru.FileAccessed(2, 100);
  // Force used above high then evict
  lru.FileAccessed(1, 750);
  lru.FileAccessed(2, 100);
  // Used = 850; evict to <= 400
  const size_t n = lru.EvictToLowWatermark();
  EXPECT_GE(n, 1u);
  EXPECT_LE(lru.UsedBytes(), 400u);

  rmdir(root.c_str());
}
