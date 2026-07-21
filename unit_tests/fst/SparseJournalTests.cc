//------------------------------------------------------------------------------
//! @file SparseJournalTests.cc
//! @brief Unit tests for FST sparse-range cache journal and LRU
//------------------------------------------------------------------------------

#include "gtest/gtest.h"
#include "fst/cache/SparseJournal.hh"
#include "fst/cache/CacheLru.hh"
#include <cstdlib>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string>
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

void RemoveTree(const std::string& path)
{
  DIR* dp = ::opendir(path.c_str());

  if (dp) {
    struct dirent* entry = nullptr;

    while ((entry = ::readdir(dp))) {
      const std::string name = entry->d_name;

      if ((name == ".") || (name == "..")) {
        continue;
      }

      const std::string full = path + "/" + name;
      struct stat st {};

      if (!::stat(full.c_str(), &st) && S_ISDIR(st.st_mode)) {
        RemoveTree(full);
      } else {
        ::unlink(full.c_str());
      }
    }

    ::closedir(dp);
  }

  ::rmdir(path.c_str());
}

void RemoveTempDir(const std::string& root)
{
  RemoveTree(SparseJournal::CacheDir(root));
  ::rmdir(root.c_str());
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
  RemoveTempDir(root);
}

TEST(SparseJournal, UsesShardedDataPath)
{
  const std::string root = MakeTempDir();
  ASSERT_FALSE(root.empty());
  // fid 0x3859 = 14425 -> bucket 00000001
  const std::string path = SparseJournal::DataPath(root, 0x3859);
  EXPECT_NE(std::string::npos, path.find("/.eoscache/00000001/00003859"));

  SparseJournal journal;
  ASSERT_EQ(0, journal.Open(root, 0x3859, 16, 1));
  ASSERT_EQ(0, journal.Write("abcdefghijklmnop", 16, 0));
  journal.Close();

  struct stat st {};
  ASSERT_EQ(0, ::stat(path.c_str(), &st));
  EXPECT_EQ(16, st.st_size);

  ASSERT_EQ(0, journal.Open(root, 0x3859, 16, 1));
  ASSERT_EQ(0, journal.Unlink());
  RemoveTempDir(root);
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
  RemoveTempDir(root);
}

//------------------------------------------------------------------------------
// Regression: a request window with a cached range in the middle must report
// exactly the leading and trailing holes so a caller can interleave journal
// and backend reads (mid-range segments used to be skipped in CacheLayout).
//------------------------------------------------------------------------------
TEST(SparseJournal, MidRequestCachedSegment)
{
  const std::string root = MakeTempDir();
  ASSERT_FALSE(root.empty());
  SparseJournal journal;
  ASSERT_EQ(0, journal.Open(root, 0x123, 1000, 7));

  std::vector<char> mid(20, 'M');
  ASSERT_EQ(0, journal.Write(mid.data(), mid.size(), 40));

  const auto missing = journal.MissingRanges(0, 100);
  ASSERT_EQ(2u, missing.size());
  EXPECT_EQ(0, missing[0].offset);
  EXPECT_EQ(40u, missing[0].size);
  EXPECT_EQ(60, missing[1].offset);
  EXPECT_EQ(40u, missing[1].size);

  // The cached middle segment must be readable exactly at its window
  char buf[20] = {};
  ASSERT_EQ(20, journal.Read(buf, 20, 40));
  EXPECT_EQ(std::string(20, 'M'), std::string(buf, 20));

  ASSERT_EQ(0, journal.Unlink());
  RemoveTempDir(root);
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
  RemoveTempDir(root);
}

//------------------------------------------------------------------------------
// The index is persisted on close and reloaded with identical content
//------------------------------------------------------------------------------
TEST(SparseJournal, PersistOnCloseAndReload)
{
  const std::string root = MakeTempDir();
  ASSERT_FALSE(root.empty());
  {
    SparseJournal journal;
    ASSERT_EQ(0, journal.Open(root, 77, 500, 3));
    ASSERT_EQ(0, journal.Write("0123456789", 10, 0));
    ASSERT_EQ(0, journal.Write("abcde", 5, 100));
    // Close() (via destructor) must persist the dirty index
  }
  {
    SparseJournal journal;
    ASSERT_EQ(0, journal.Open(root, 77, 500, 3));
    EXPECT_EQ(15u, journal.CachedBytes());
    char buf[10] = {};
    ASSERT_EQ(10, journal.Read(buf, 10, 0));
    EXPECT_EQ(std::string("0123456789"), std::string(buf, 10));
    ASSERT_EQ(5, journal.Read(buf, 5, 100));
    EXPECT_EQ(std::string("abcde"), std::string(buf, 5));
    ASSERT_EQ(0, journal.Unlink());
  }
  RemoveTempDir(root);
}

//------------------------------------------------------------------------------
// A corrupt index (torn write, bad ranges) resets the journal instead of
// serving bogus ranges
//------------------------------------------------------------------------------
TEST(SparseJournal, CorruptIndexResets)
{
  const std::string root = MakeTempDir();
  ASSERT_FALSE(root.empty());
  std::string idx_path;
  {
    SparseJournal journal;
    ASSERT_EQ(0, journal.Open(root, 99, 500, 3));
    ASSERT_EQ(0, journal.Write("0123456789", 10, 0));
    idx_path = journal.GetPath() + ".idx";
  }
  // Corrupt the index body: overlapping/out-of-order ranges
  {
    const int fd = ::open(idx_path.c_str(), O_WRONLY);
    ASSERT_GE(fd, 0);
    const off_t bad[2] = {400, 200}; // stop < start
    // Header is 6x uint64_t
    ASSERT_EQ((ssize_t) sizeof(bad),
              ::pwrite(fd, bad, sizeof(bad), 6 * sizeof(uint64_t)));
    ::close(fd);
  }
  {
    SparseJournal journal;
    ASSERT_EQ(0, journal.Open(root, 99, 500, 3));
    // Invalid index must have been rejected and reset
    EXPECT_EQ(0u, journal.CachedBytes());
    char buf[10] = {};
    EXPECT_EQ(0, journal.Read(buf, 10, 0));
    ASSERT_EQ(0, journal.Unlink());
  }
  RemoveTempDir(root);
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

  RemoveTempDir(root);
}

//------------------------------------------------------------------------------
// All opens of the same fid share one journal instance
//------------------------------------------------------------------------------
TEST(CacheLru, SharedJournalPerFid)
{
  const std::string root = MakeTempDir();
  ASSERT_FALSE(root.empty());
  CacheLru lru(1, root);
  auto j1 = lru.GetJournal(0x42, 1000, 5);
  auto j2 = lru.GetJournal(0x42, 1000, 5);
  ASSERT_TRUE(j1 != nullptr);
  EXPECT_EQ(j1.get(), j2.get());
  // Data written through one handle is visible through the other
  ASSERT_EQ(0, j1->Write("shared", 6, 0));
  char buf[6] = {};
  ASSERT_EQ(6, j2->Read(buf, 6, 0));
  EXPECT_EQ(std::string("shared"), std::string(buf, 6));
  // A different fid gets a different journal
  auto j3 = lru.GetJournal(0x43, 1000, 5);
  ASSERT_TRUE(j3 != nullptr);
  EXPECT_NE(j1.get(), j3.get());
  (void) j1->Unlink();
  (void) j3->Unlink();
  RemoveTempDir(root);
}

//------------------------------------------------------------------------------
// Eviction skips journals that are still referenced by open files
//------------------------------------------------------------------------------
TEST(CacheLru, EvictionSkipsJournalsInUse)
{
  const std::string root = MakeTempDir();
  ASSERT_FALSE(root.empty());
  CacheLru lru(1, root);
  lru.SetCapacityBytes(1000);
  lru.SetWatermarks(10, 20); // low=100, high=200
  // fid 1 is in use (live shared_ptr), fid 2 is not
  auto in_use = lru.GetJournal(1, 1000, 5);
  ASSERT_TRUE(in_use != nullptr);
  lru.FileAccessed(1, 500);
  lru.FileAccessed(2, 500);
  (void) lru.EvictToLowWatermark();
  // fid 2 was evictable; fid 1 must survive since its journal is referenced
  EXPECT_EQ(500u, lru.UsedBytes());
  EXPECT_EQ(1u, lru.Size());
  (void) in_use->Unlink();
  RemoveTempDir(root);
}

//------------------------------------------------------------------------------
// A fresh CacheLru recovers accounting from journals found on disk
//------------------------------------------------------------------------------
TEST(CacheLru, StartupScanRecoversAccounting)
{
  const std::string root = MakeTempDir();
  ASSERT_FALSE(root.empty());
  {
    SparseJournal journal;
    ASSERT_EQ(0, journal.Open(root, 0x11, 1 << 20, 9));
    std::vector<char> blob(64 * 1024, 'x');
    ASSERT_EQ(0, journal.Write(blob.data(), blob.size(), 0));
  }
  CacheLru lru(1, root);
  EXPECT_EQ(1u, lru.Size());
  // Allocated blocks of data + index files - at least the payload size
  EXPECT_GE(lru.UsedBytes(), 64u * 1024);
  RemoveTempDir(root);
}
