//------------------------------------------------------------------------------
//! @file SparseJournal.hh
//! @brief Sparse-range journal for FST read-through cache
//------------------------------------------------------------------------------

#pragma once

#include "fst/Namespace.hh"
#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! On-disk sparse journal storing cached byte ranges for a single file.
//! Ranges are tracked in an interval map; data lives in a sidecar data file.
//!
//! All public methods are thread-safe. Instances are meant to be shared
//! between all concurrent opens of the same file id (see CacheLru::GetJournal)
//! so that the on-disk index has a single writer.
//!
//! Crash consistency: the index is persisted on Truncate/Close and every
//! kPersistThresholdBytes of newly admitted data, with the data file synced
//! before the index. A crash loses at most the ranges admitted since the last
//! persist - they are simply re-fetched from the backend. An index that fails
//! validation on load resets the journal to empty.
//------------------------------------------------------------------------------
class SparseJournal
{
public:
  struct Range {
    off_t offset{0};
    size_t size{0};
  };

  //----------------------------------------------------------------------------
  //! Path helpers (also used for unlinking without opening).
  //! DataPath uses the same fid/10000 sharding as FST replicas:
  //!   <cache-fs>/.eoscache/<fid/10000>/<hexfid>
  //----------------------------------------------------------------------------
  static std::string CacheDir(const std::string& cache_fs_path);
  static std::string DataPath(const std::string& cache_fs_path, uint64_t fid);

  SparseJournal() = default;
  ~SparseJournal();

  SparseJournal(const SparseJournal&) = delete;
  SparseJournal& operator=(const SparseJournal&) = delete;

  //----------------------------------------------------------------------------
  //! Open or create journal files under cache_fs_path for the given fid.
  //! Idempotent if already open with the same identity (path, fid, size,
  //! mtime). A mismatching identity truncates the cached content.
  //----------------------------------------------------------------------------
  int Open(const std::string& cache_fs_path, uint64_t fid,
           uint64_t expected_size, time_t expected_mtime);

  //----------------------------------------------------------------------------
  //! Close journal files, persisting the index if dirty
  //----------------------------------------------------------------------------
  void Close();

  //----------------------------------------------------------------------------
  //! Truncate journal (invalidate all cached ranges)
  //----------------------------------------------------------------------------
  int Truncate();

  //----------------------------------------------------------------------------
  //! Unlink journal files from disk
  //----------------------------------------------------------------------------
  int Unlink();

  //----------------------------------------------------------------------------
  //! Read from journal into buf. Returns bytes served from cache (may be less
  //! than count if only a prefix of the request is cached). Holes return as
  //! gaps - caller must fill from backend.
  //----------------------------------------------------------------------------
  ssize_t Read(void* buf, size_t count, off_t offset);

  //----------------------------------------------------------------------------
  //! Return list of missing ranges within [offset, offset+count)
  //----------------------------------------------------------------------------
  std::vector<Range> MissingRanges(off_t offset, size_t count) const;

  //----------------------------------------------------------------------------
  //! Write a contiguous range into the journal
  //----------------------------------------------------------------------------
  int Write(const void* buf, size_t count, off_t offset);

  //----------------------------------------------------------------------------
  //! Total cached bytes
  //----------------------------------------------------------------------------
  uint64_t CachedBytes() const;

  //----------------------------------------------------------------------------
  //! Logical file size recorded in the journal header
  //----------------------------------------------------------------------------
  uint64_t FileSize() const;

  time_t MTime() const;

  uint64_t GetFid() const;

  std::string GetPath() const;

  bool IsOpen() const;

private:
  struct Header {
    uint64_t magic{0};
    uint64_t fid{0};
    uint64_t file_size{0};
    int64_t mtime{0};
    uint64_t cached_bytes{0};
    uint64_t nentries{0};
  };

  static constexpr uint64_t kMagic = 0x45534f534a524e32ULL; // EOSJRN2
  //! Persist the index every this many newly admitted bytes
  static constexpr uint64_t kPersistThresholdBytes = 64 * 1024 * 1024;

  int LoadIndexLocked();
  int PersistIndexLocked();
  int TruncateLocked();
  void CloseLocked(bool persist);
  void ResetLocked();
  void InsertRangeLocked(off_t offset, size_t size);

  std::string IndexPathLocked() const
  {
    return mJournalPath + ".idx";
  }

  mutable std::mutex mMutex;
  int mDataFd{-1};
  int mIndexFd{-1};
  uint64_t mFid{0};
  uint64_t mFileSize{0};
  time_t mMTime{0};
  uint64_t mCachedBytes{0};
  uint64_t mUnpersistedBytes{0};
  bool mDirty{false};
  std::string mJournalPath;
  //! Map of start offset -> end offset (exclusive) for cached ranges
  std::map<off_t, off_t> mRanges;
};

EOSFSTNAMESPACE_END
