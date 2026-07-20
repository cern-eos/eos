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
//------------------------------------------------------------------------------
class SparseJournal
{
public:
  struct Range {
    off_t offset{0};
    size_t size{0};
  };

  SparseJournal() = default;
  ~SparseJournal();

  SparseJournal(const SparseJournal&) = delete;
  SparseJournal& operator=(const SparseJournal&) = delete;

  //----------------------------------------------------------------------------
  //! Open or create journal files under cache_fs_path for the given fid
  //----------------------------------------------------------------------------
  int Open(const std::string& cache_fs_path, uint64_t fid,
           uint64_t expected_size, time_t expected_mtime);

  //----------------------------------------------------------------------------
  //! Close journal files
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
  //! than count if only a prefix/suffix of the request is cached). Holes return
  //! as gaps — caller must fill from backend.
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
  uint64_t FileSize() const
  {
    return mFileSize;
  }

  time_t MTime() const
  {
    return mMTime;
  }

  uint64_t GetFid() const
  {
    return mFid;
  }

  const std::string& GetPath() const
  {
    return mJournalPath;
  }

  bool IsOpen() const
  {
    return mDataFd >= 0;
  }

private:
  struct Header {
    uint64_t magic{0};
    uint64_t fid{0};
    uint64_t file_size{0};
    int64_t mtime{0};
    uint64_t cached_bytes{0};
  };

  static constexpr uint64_t kMagic = 0x45534f534a524e4cULL; // EOSJRNL

  int LoadIndex();
  int PersistHeader();
  void InsertRange(off_t offset, size_t size);
  std::string IndexPath() const
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
  std::string mJournalPath;
  //! Map of start offset -> end offset (exclusive) for cached ranges
  std::map<off_t, off_t> mRanges;
};

EOSFSTNAMESPACE_END
