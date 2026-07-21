//------------------------------------------------------------------------------
//! @file CacheLru.cc
//------------------------------------------------------------------------------

#include "fst/cache/CacheLru.hh"
#include "fst/cache/SparseJournal.hh"
#include "common/Logging.hh"
#include <dirent.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <algorithm>
#include <cstdlib>
#include <iterator>
#include <thread>
#include <vector>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// CacheLru
//------------------------------------------------------------------------------
CacheLru::CacheLru(eos::common::FileSystem::fsid_t fsid, std::string fs_path)
  : mFsId(fsid), mFsPath(std::move(fs_path))
{
  struct statvfs buf {};

  if (!::statvfs(mFsPath.c_str(), &buf)) {
    mCapacityBytes = (uint64_t) buf.f_blocks * buf.f_frsize;
  }

  ScanExistingJournals();
}

//------------------------------------------------------------------------------
// Rebuild accounting from journals left on disk (e.g. after FST restart).
// Uses allocated blocks so sparse data files are accounted at real disk usage.
// Entries are inserted in mtime order so the coldest files are evicted first.
//------------------------------------------------------------------------------
namespace
{
struct JournalStat {
  uint64_t fid{0};
  uint64_t bytes{0};
  time_t mtime{0};
};

//------------------------------------------------------------------------------
//! Accumulate journal data/.idx sizes under .eoscache/<bucket>/<fid>
//------------------------------------------------------------------------------
void
ScanJournalDir(const std::string& dir,
               std::unordered_map<uint64_t, JournalStat>& found)
{
  DIR* dp = ::opendir(dir.c_str());

  if (!dp) {
    return;
  }

  struct dirent* entry = nullptr;

  while ((entry = ::readdir(dp))) {
    std::string name = entry->d_name;

    if ((name == ".") || (name == "..")) {
      continue;
    }

    std::string full = dir;

    if (!full.empty() && (full.back() != '/')) {
      full += '/';
    }

    full += name;
    struct stat st {};

    if (::stat(full.c_str(), &st)) {
      continue;
    }

    if (S_ISDIR(st.st_mode)) {
      ScanJournalDir(full, found);
      continue;
    }

    const bool is_index = (name.size() > 4) &&
                          (name.compare(name.size() - 4, 4, ".idx") == 0);

    if (is_index) {
      name.erase(name.size() - 4);
    }

    char* end = nullptr;
    const uint64_t fid = std::strtoull(name.c_str(), &end, 16);

    if (!fid || !end || (*end != '\0')) {
      continue;
    }

    auto& js = found[fid];
    js.fid = fid;
    js.bytes += (uint64_t) st.st_blocks * 512;
    js.mtime = std::max(js.mtime, st.st_mtime);
  }

  ::closedir(dp);
}
}

void
CacheLru::ScanExistingJournals()
{
  std::unordered_map<uint64_t, JournalStat> found;
  ScanJournalDir(SparseJournal::CacheDir(mFsPath), found);
  std::vector<JournalStat> ordered;
  ordered.reserve(found.size());

  for (const auto& kv : found) {
    ordered.push_back(kv.second);
  }

  // Newest first so that push_back leaves the oldest at the LRU tail
  std::sort(ordered.begin(), ordered.end(),
  [](const JournalStat & a, const JournalStat & b) {
    return a.mtime > b.mtime;
  });
  std::lock_guard<std::mutex> scope_lock(mMutex);

  for (const auto& js : ordered) {
    mLru.push_back(js.fid);
    Entry e;
    e.it = std::prev(mLru.end());
    e.bytes = js.bytes;
    mMap[js.fid] = e;
    mUsedBytes += js.bytes;
  }

  if (!ordered.empty()) {
    eos_static_info("msg=\"recovered cache journal accounting\" fsid=%u "
                    "entries=%lu used_bytes=%llu", mFsId, ordered.size(),
                    mUsedBytes);
  }
}

void
CacheLru::SetWatermarks(unsigned low_percent, unsigned high_percent)
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
  mLowPercent = std::min(low_percent, 100u);
  mHighPercent = std::min(high_percent, 100u);

  if (mLowPercent > mHighPercent) {
    mLowPercent = mHighPercent;
  }
}

void
CacheLru::SetCapacityBytes(uint64_t capacity_bytes)
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
  mCapacityBytes = capacity_bytes;
}

//------------------------------------------------------------------------------
// Shared per-fid journal
//------------------------------------------------------------------------------
std::shared_ptr<SparseJournal>
CacheLru::GetJournal(uint64_t fid, uint64_t expected_size,
                     time_t expected_mtime)
{
  std::lock_guard<std::mutex> scope_lock(mJournalMutex);
  auto it = mJournals.find(fid);

  if (it != mJournals.end()) {
    if (auto journal = it->second.lock()) {
      if (journal->Open(mFsPath, fid, expected_size, expected_mtime)) {
        return nullptr;
      }

      return journal;
    }
  }

  auto journal = std::make_shared<SparseJournal>();

  if (journal->Open(mFsPath, fid, expected_size, expected_mtime)) {
    return nullptr;
  }

  mJournals[fid] = journal;

  // Opportunistic cleanup of expired entries
  for (auto jit = mJournals.begin(); jit != mJournals.end();) {
    if (jit->second.expired()) {
      jit = mJournals.erase(jit);
    } else {
      ++jit;
    }
  }

  return journal;
}

void
CacheLru::FileAccessed(uint64_t fid, uint64_t cached_bytes)
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
  FileAccessedLocked(fid, cached_bytes, true);
}

void
CacheLru::FileAccessedLocked(uint64_t fid, uint64_t cached_bytes,
                             bool to_front)
{
  auto it = mMap.find(fid);

  if (it == mMap.end()) {
    mLru.push_front(fid);
    Entry e;
    e.it = mLru.begin();
    e.bytes = cached_bytes;
    mMap[fid] = e;
    mUsedBytes += cached_bytes;
    return;
  }

  if (cached_bytes >= it->second.bytes) {
    mUsedBytes += (cached_bytes - it->second.bytes);
  } else {
    mUsedBytes -= (it->second.bytes - cached_bytes);
  }

  it->second.bytes = cached_bytes;

  if (to_front) {
    mLru.erase(it->second.it);
    mLru.push_front(fid);
    it->second.it = mLru.begin();
  }
}

void
CacheLru::FileRemoved(uint64_t fid)
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
  auto it = mMap.find(fid);

  if (it == mMap.end()) {
    return;
  }

  mUsedBytes -= it->second.bytes;
  mLru.erase(it->second.it);
  mMap.erase(it);
}

bool
CacheLru::CanAdmit(uint64_t additional_bytes)
{
  std::lock_guard<std::mutex> scope_lock(mMutex);

  if (!mCapacityBytes) {
    return true;
  }

  if ((mUsedBytes + additional_bytes) <= HighLimit()) {
    return true;
  }

  return false;
}

size_t
CacheLru::EvictToLowWatermark()
{
  std::vector<uint64_t> victims;
  {
    std::lock_guard<std::mutex> journal_lock(mJournalMutex);
    std::lock_guard<std::mutex> scope_lock(mMutex);
    auto lit = mLru.rbegin();

    while ((lit != mLru.rend()) && (mUsedBytes > LowLimit())) {
      const uint64_t fid = *lit;
      ++lit;
      // Skip journals still referenced by open files
      auto jit = mJournals.find(fid);

      if ((jit != mJournals.end()) && !jit->second.expired()) {
        continue;
      }

      auto it = mMap.find(fid);

      if (it != mMap.end()) {
        mUsedBytes -= it->second.bytes;
        // reverse_iterator points one past the erased element - recompute
        lit = std::make_reverse_iterator(mLru.erase(it->second.it));
        mMap.erase(it);
      }

      victims.push_back(fid);
    }
  }

  for (const auto fid : victims) {
    (void) DropJournalFiles(fid);
    mEvictions++;
  }

  return victims.size();
}

//------------------------------------------------------------------------------
// Background eviction trigger (CacheLru instances live for the process
// lifetime in the registry, so a detached thread is safe)
//------------------------------------------------------------------------------
void
CacheLru::MaybeEvictAsync()
{
  {
    std::lock_guard<std::mutex> scope_lock(mMutex);

    if (!mCapacityBytes || (mUsedBytes <= HighLimit())) {
      return;
    }
  }

  bool expected = false;

  if (!mEvicting.compare_exchange_strong(expected, true)) {
    return;
  }

  std::thread([this]() {
    (void) EvictToLowWatermark();
    mEvicting = false;
  }).detach();
}

uint64_t
CacheLru::UsedBytes() const
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
  return mUsedBytes;
}

size_t
CacheLru::Size() const
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
  return mMap.size();
}

uint64_t
CacheLru::LowLimit() const
{
  return (mCapacityBytes * mLowPercent) / 100;
}

uint64_t
CacheLru::HighLimit() const
{
  return (mCapacityBytes * mHighPercent) / 100;
}

int
CacheLru::DropJournalFiles(uint64_t fid) const
{
  const std::string data_path = SparseJournal::DataPath(mFsPath, fid);
  int rc = 0;

  if (::unlink(data_path.c_str()) && (errno != ENOENT)) {
    rc = -1;
  }

  if (::unlink((data_path + ".idx").c_str()) && (errno != ENOENT)) {
    rc = -1;
  }

  return rc;
}

//------------------------------------------------------------------------------
// Registry
//------------------------------------------------------------------------------
CacheLruRegistry&
CacheLruRegistry::Instance()
{
  static CacheLruRegistry inst;
  return inst;
}

CacheLru*
CacheLruRegistry::GetOrCreate(eos::common::FileSystem::fsid_t fsid,
                              const std::string& fs_path)
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
  auto it = mCaches.find(fsid);

  if (it != mCaches.end()) {
    return it->second.get();
  }

  auto lru = std::make_unique<CacheLru>(fsid, fs_path);
  CacheLru* ptr = lru.get();
  mCaches.emplace(fsid, std::move(lru));
  return ptr;
}

CacheLru*
CacheLruRegistry::Find(eos::common::FileSystem::fsid_t fsid)
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
  auto it = mCaches.find(fsid);
  return (it == mCaches.end()) ? nullptr : it->second.get();
}

void
CacheLruRegistry::SetConfigResolver(ConfigResolver resolver)
{
  std::lock_guard<std::mutex> scope_lock(mResolverMutex);
  mConfigResolver = std::move(resolver);
}

std::string
CacheLruRegistry::GetConfig(eos::common::FileSystem::fsid_t fsid,
                            const std::string& key)
{
  ConfigResolver resolver;
  {
    std::lock_guard<std::mutex> scope_lock(mResolverMutex);
    resolver = mConfigResolver;
  }

  if (!resolver) {
    return {};
  }

  return resolver(fsid, key);
}

int
CacheLruRegistry::TruncateJournal(eos::common::FileSystem::fsid_t fsid,
                                  uint64_t fid,
                                  const std::string& fs_path)
{
  CacheLru* lru = Find(fsid);
  std::string path;

  if (lru) {
    path = lru->GetFsPath();
  } else {
    path = fs_path;
  }

  if (path.empty()) {
    return ENOENT;
  }

  if (lru) {
    // Use the shared journal so open readers see the truncation coherently.
    // Passing size 0/mtime 0 marks the cached identity as stale.
    auto journal = lru->GetJournal(fid, 0, 0);

    if (!journal) {
      return EIO;
    }

    lru->FileAccessed(fid, 0);
  } else {
    SparseJournal journal;

    if (journal.Open(path, fid, 0, 0)) {
      return EIO;
    }

    if (journal.Truncate()) {
      return EIO;
    }
  }

  eos_static_info("msg=\"truncated cache journal\" fxid=%08llx fsid=%u", fid,
                  fsid);
  return 0;
}

EOSFSTNAMESPACE_END
