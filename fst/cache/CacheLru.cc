//------------------------------------------------------------------------------
//! @file CacheLru.cc
//------------------------------------------------------------------------------

#include "fst/cache/CacheLru.hh"
#include "fst/cache/SparseJournal.hh"
#include "common/Logging.hh"
#include "fst/XrdFstOfs.hh"
#include <sys/statvfs.h>
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

void
CacheLru::FileAccessed(uint64_t fid, uint64_t cached_bytes)
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
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
  mLru.erase(it->second.it);
  mLru.push_front(fid);
  it->second.it = mLru.begin();
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
    std::lock_guard<std::mutex> scope_lock(mMutex);

    while (!mLru.empty() && (mUsedBytes > LowLimit())) {
      const uint64_t fid = mLru.back();
      victims.push_back(fid);
      auto it = mMap.find(fid);

      if (it != mMap.end()) {
        mUsedBytes -= it->second.bytes;
        mLru.pop_back();
        mMap.erase(it);
      } else {
        mLru.pop_back();
      }
    }
  }

  for (const auto fid : victims) {
    (void) DropJournalFiles(fid);
    (void) ClearCacheLocation(fid);
    mEvictions++;
  }

  return victims.size();
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

bool
CacheLru::AboveHigh() const
{
  return mCapacityBytes && (mUsedBytes > HighLimit());
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
  SparseJournal journal;
  // Open just to resolve paths then unlink
  const int rc = journal.Open(mFsPath, fid, 0, 0);

  if (rc) {
    return rc;
  }

  return journal.Unlink();
}

bool
CacheLru::ClearCacheLocation(uint64_t fid) const
{
  // Best-effort: log intent to clear MD cache_location. The journal unlink is
  // authoritative locally; MGM self-heals when the cache FST is next selected
  // or when operators inspect file info.
  eos_static_info("msg=\"cache eviction clear location\" fxid=%08llx fsid=%u",
                  fid, mFsId);
  return true;
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

int
CacheLruRegistry::TruncateJournal(eos::common::FileSystem::fsid_t fsid,
                                  uint64_t fid)
{
  CacheLru* lru = Find(fsid);
  std::string path;

  if (lru) {
    path = lru->GetFsPath();
  } else {
    path = gOFS.Storage->GetStoragePath(fsid).c_str();
  }

  if (path.empty()) {
    return ENOENT;
  }

  SparseJournal journal;

  if (journal.Open(path, fid, 0, 0)) {
    return EIO;
  }

  if (journal.Truncate()) {
    return EIO;
  }

  if (lru) {
    lru->FileAccessed(fid, 0);
  }

  eos_static_info("msg=\"truncated cache journal\" fxid=%08llx fsid=%u", fid, fsid);
  return 0;
}

EOSFSTNAMESPACE_END
