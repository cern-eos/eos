//------------------------------------------------------------------------------
//! @file CacheLru.hh
//! @brief Local LRU manager for FST read-through cache journals
//------------------------------------------------------------------------------

#pragma once

#include "fst/Namespace.hh"
#include "common/FileSystem.hh"
#include <atomic>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Per-filesystem LRU of cached journal entries with low/high watermarks.
//------------------------------------------------------------------------------
class CacheLru
{
public:
  CacheLru(eos::common::FileSystem::fsid_t fsid, std::string fs_path);
  ~CacheLru() = default;

  void SetWatermarks(unsigned low_percent, unsigned high_percent);
  void SetCapacityBytes(uint64_t capacity_bytes);

  void FileAccessed(uint64_t fid, uint64_t cached_bytes);
  void FileRemoved(uint64_t fid);

  //! Return false if new data should not be admitted (bridge mode)
  bool CanAdmit(uint64_t additional_bytes);

  //! Evict until used bytes <= low watermark. Returns number of files evicted.
  size_t EvictToLowWatermark();

  uint64_t UsedBytes() const;
  size_t Size() const;

  eos::common::FileSystem::fsid_t GetFsId() const
  {
    return mFsId;
  }

  const std::string& GetFsPath() const
  {
    return mFsPath;
  }

  // Stats
  std::atomic<uint64_t> mHits{0};
  std::atomic<uint64_t> mMisses{0};
  std::atomic<uint64_t> mBridges{0};
  std::atomic<uint64_t> mEvictions{0};

private:
  using FidList = std::list<uint64_t>;
  struct Entry {
    FidList::iterator it;
    uint64_t bytes{0};
  };

  bool AboveHigh() const;
  uint64_t LowLimit() const;
  uint64_t HighLimit() const;
  int DropJournalFiles(uint64_t fid) const;
  bool ClearCacheLocation(uint64_t fid) const;

  mutable std::mutex mMutex;
  eos::common::FileSystem::fsid_t mFsId{0};
  std::string mFsPath;
  unsigned mLowPercent{70};
  unsigned mHighPercent{85};
  uint64_t mCapacityBytes{0};
  uint64_t mUsedBytes{0};
  FidList mLru; // front = MRU, back = LRU
  std::unordered_map<uint64_t, Entry> mMap;
};

//------------------------------------------------------------------------------
//! Global registry of per-fs CacheLru instances on this FST
//------------------------------------------------------------------------------
class CacheLruRegistry
{
public:
  static CacheLruRegistry& Instance();

  CacheLru* GetOrCreate(eos::common::FileSystem::fsid_t fsid,
                        const std::string& fs_path);
  CacheLru* Find(eos::common::FileSystem::fsid_t fsid);

  //! Truncate journal for fid on the given fs (used by FSctl)
  int TruncateJournal(eos::common::FileSystem::fsid_t fsid, uint64_t fid);

private:
  CacheLruRegistry() = default;
  std::mutex mMutex;
  std::unordered_map<eos::common::FileSystem::fsid_t,
      std::unique_ptr<CacheLru>> mCaches;
};

EOSFSTNAMESPACE_END
