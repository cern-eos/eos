//------------------------------------------------------------------------------
//! @file CacheLru.hh
//! @brief Local LRU manager for FST read-through cache journals
//------------------------------------------------------------------------------

#pragma once

#include "fst/Namespace.hh"
#include "common/FileSystem.hh"
#include <atomic>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

EOSFSTNAMESPACE_BEGIN

class SparseJournal;

//------------------------------------------------------------------------------
//! Per-filesystem LRU of cached journal entries with low/high watermarks.
//! Also hands out the shared per-fid SparseJournal instances so that all
//! concurrent opens of a file use a single journal (single index writer).
//------------------------------------------------------------------------------
class CacheLru
{
public:
  CacheLru(eos::common::FileSystem::fsid_t fsid, std::string fs_path);
  ~CacheLru() = default;

  void SetWatermarks(unsigned low_percent, unsigned high_percent);
  void SetCapacityBytes(uint64_t capacity_bytes);

  //----------------------------------------------------------------------------
  //! Get (or create) the shared journal for fid. Returns nullptr if the
  //! journal cannot be opened. All opens of the same fid share one instance.
  //----------------------------------------------------------------------------
  std::shared_ptr<SparseJournal> GetJournal(uint64_t fid,
      uint64_t expected_size, time_t expected_mtime);

  void FileAccessed(uint64_t fid, uint64_t cached_bytes);
  void FileRemoved(uint64_t fid);

  //! Return false if new data should not be admitted (bridge mode)
  bool CanAdmit(uint64_t additional_bytes);

  //! Evict until used bytes <= low watermark, skipping journals still in use.
  //! Returns number of files evicted.
  size_t EvictToLowWatermark();

  //! Trigger eviction in a background thread if above the high watermark;
  //! no-op when an eviction is already running
  void MaybeEvictAsync();

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

  //! Rebuild LRU accounting from the on-disk .eoscache directory
  void ScanExistingJournals();
  uint64_t LowLimit() const;
  uint64_t HighLimit() const;
  int DropJournalFiles(uint64_t fid) const;
  void FileAccessedLocked(uint64_t fid, uint64_t cached_bytes, bool to_front);

  mutable std::mutex mMutex;
  std::atomic<bool> mEvicting{false};
  eos::common::FileSystem::fsid_t mFsId{0};
  std::string mFsPath;
  unsigned mLowPercent{70};
  unsigned mHighPercent{85};
  uint64_t mCapacityBytes{0};
  uint64_t mUsedBytes{0};
  FidList mLru; // front = MRU, back = LRU
  std::unordered_map<uint64_t, Entry> mMap;
  //! Live shared journals per fid
  std::mutex mJournalMutex;
  std::unordered_map<uint64_t, std::weak_ptr<SparseJournal>> mJournals;
};

//------------------------------------------------------------------------------
//! Global registry of per-fs CacheLru instances on this FST
//------------------------------------------------------------------------------
class CacheLruRegistry
{
public:
  //! Resolver for per-filesystem configuration values. Registered by the FST
  //! server at startup; this library must not reference gOFS/Storage directly
  //! since it is also linked into standalone tools.
  using ConfigResolver =
    std::function<std::string(eos::common::FileSystem::fsid_t,
                              const std::string&)>;

  static CacheLruRegistry& Instance();

  CacheLru* GetOrCreate(eos::common::FileSystem::fsid_t fsid,
                        const std::string& fs_path);
  CacheLru* Find(eos::common::FileSystem::fsid_t fsid);

  //! Truncate journal for fid on the given fs (used by FSctl)
  int TruncateJournal(eos::common::FileSystem::fsid_t fsid, uint64_t fid,
                      const std::string& fs_path = {});

  //! Register the filesystem-config resolver (called by the FST server)
  void SetConfigResolver(ConfigResolver resolver);

  //! Get a filesystem config value; empty when no resolver is registered
  std::string GetConfig(eos::common::FileSystem::fsid_t fsid,
                        const std::string& key);

private:
  CacheLruRegistry() = default;
  std::mutex mMutex;
  std::unordered_map<eos::common::FileSystem::fsid_t,
      std::unique_ptr<CacheLru>> mCaches;
  std::mutex mResolverMutex;
  ConfigResolver mConfigResolver;
};

EOSFSTNAMESPACE_END
