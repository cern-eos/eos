//------------------------------------------------------------------------------
//! @file SparseJournal.cc
//------------------------------------------------------------------------------

#include "fst/cache/SparseJournal.hh"
#include "common/FileId.hh"
#include "common/Logging.hh"
#include "common/Path.hh"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <algorithm>

EOSFSTNAMESPACE_BEGIN

namespace
{
std::string JournalDir(const std::string& cache_fs_path)
{
  std::string path = cache_fs_path;

  if (!path.empty() && path.back() != '/') {
    path += '/';
  }

  path += ".eoscache/";
  return path;
}
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
SparseJournal::~SparseJournal()
{
  Close();
}

//------------------------------------------------------------------------------
// Open or create journal
//------------------------------------------------------------------------------
int
SparseJournal::Open(const std::string& cache_fs_path, uint64_t fid,
                    uint64_t expected_size, time_t expected_mtime)
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
  Close();
  mFid = fid;
  mFileSize = expected_size;
  mMTime = expected_mtime;
  const std::string dir = JournalDir(cache_fs_path);
  mJournalPath = dir + eos::common::FileId::Fid2Hex(fid);
  eos::common::Path cpath(mJournalPath.c_str());
  (void) cpath.MakeParentPath(S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
  mDataFd = ::open(mJournalPath.c_str(), O_RDWR | O_CREAT, 0644);
  mIndexFd = ::open(IndexPath().c_str(), O_RDWR | O_CREAT, 0644);

  if ((mDataFd < 0) || (mIndexFd < 0)) {
    eos_static_err("msg=\"failed to open cache journal\" path=%s errno=%d",
                   mJournalPath.c_str(), errno);
    Close();
    return -1;
  }

  Header hdr{};
  const ssize_t nread = ::pread(mIndexFd, &hdr, sizeof(hdr), 0);

  if (nread == (ssize_t) sizeof(hdr) && hdr.magic == kMagic &&
      hdr.fid == fid) {
    if ((hdr.file_size != expected_size) || (hdr.mtime != expected_mtime)) {
      eos_static_info("msg=\"cache journal stale, truncating\" fxid=%08llx "
                      "old_size=%llu new_size=%llu", fid, hdr.file_size,
                      expected_size);
      mRanges.clear();
      mCachedBytes = 0;
      (void) ::ftruncate(mDataFd, 0);
      (void) ::ftruncate(mIndexFd, 0);
    } else {
      mFileSize = hdr.file_size;
      mMTime = hdr.mtime;
      mCachedBytes = hdr.cached_bytes;

      if (LoadIndex()) {
        Close();
        return -1;
      }

      return 0;
    }
  }

  return PersistHeader();
}

//------------------------------------------------------------------------------
// Close
//------------------------------------------------------------------------------
void
SparseJournal::Close()
{
  if (mDataFd >= 0) {
    ::close(mDataFd);
    mDataFd = -1;
  }

  if (mIndexFd >= 0) {
    ::close(mIndexFd);
    mIndexFd = -1;
  }

  mRanges.clear();
  mCachedBytes = 0;
}

//------------------------------------------------------------------------------
// Truncate journal contents
//------------------------------------------------------------------------------
int
SparseJournal::Truncate()
{
  std::lock_guard<std::mutex> scope_lock(mMutex);

  if (mDataFd < 0) {
    return -1;
  }

  mRanges.clear();
  mCachedBytes = 0;
  (void) ::ftruncate(mDataFd, 0);
  (void) ::ftruncate(mIndexFd, 0);
  return PersistHeader();
}

//------------------------------------------------------------------------------
// Unlink journal files
//------------------------------------------------------------------------------
int
SparseJournal::Unlink()
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
  Close();
  int rc = 0;

  if (!mJournalPath.empty()) {
    if (::unlink(mJournalPath.c_str()) && errno != ENOENT) {
      rc = -1;
    }

    if (::unlink(IndexPath().c_str()) && errno != ENOENT) {
      rc = -1;
    }
  }

  return rc;
}

//------------------------------------------------------------------------------
// Read from cached ranges
//------------------------------------------------------------------------------
ssize_t
SparseJournal::Read(void* buf, size_t count, off_t offset)
{
  std::lock_guard<std::mutex> scope_lock(mMutex);

  if ((mDataFd < 0) || (count == 0)) {
    return 0;
  }

  char* out = static_cast<char*>(buf);
  size_t done = 0;
  off_t cur = offset;
  const off_t end = offset + (off_t) count;

  while (cur < end) {
    auto it = mRanges.upper_bound(cur);

    if (it != mRanges.begin()) {
      --it;

      if (it->first <= cur && cur < it->second) {
        const off_t seg_end = std::min(it->second, end);
        const size_t n = (size_t)(seg_end - cur);
        const ssize_t nread = ::pread(mDataFd, out + done, n, cur);

        if (nread < 0) {
          return -1;
        }

        done += (size_t) nread;
        cur += nread;

        if ((size_t) nread < n) {
          break;
        }

        continue;
      }
    }

    // Hole at cur — stop so caller can fill from backend
    break;
  }

  return (ssize_t) done;
}

//------------------------------------------------------------------------------
// Missing ranges within request window
//------------------------------------------------------------------------------
std::vector<SparseJournal::Range>
SparseJournal::MissingRanges(off_t offset, size_t count) const
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
  std::vector<Range> missing;

  if (count == 0) {
    return missing;
  }

  off_t cur = offset;
  const off_t end = offset + (off_t) count;

  while (cur < end) {
    auto it = mRanges.upper_bound(cur);

    if (it != mRanges.begin()) {
      --it;

      if (it->first <= cur && cur < it->second) {
        cur = it->second;
        continue;
      }
    }

    off_t hole_end = end;

    if (it != mRanges.end() && it->first < end) {
      // it was upper_bound; if we decremented, advance to next
      auto next = mRanges.lower_bound(cur);

      if (next != mRanges.end() && next->first < end) {
        hole_end = next->first;
      }
    }

    if (hole_end > cur) {
      missing.push_back(Range{cur, (size_t)(hole_end - cur)});
      cur = hole_end;
    } else {
      break;
    }
  }

  return missing;
}

//------------------------------------------------------------------------------
// Write range into journal
//------------------------------------------------------------------------------
int
SparseJournal::Write(const void* buf, size_t count, off_t offset)
{
  std::lock_guard<std::mutex> scope_lock(mMutex);

  if ((mDataFd < 0) || (count == 0)) {
    return 0;
  }

  const ssize_t nwritten = ::pwrite(mDataFd, buf, count, offset);

  if (nwritten != (ssize_t) count) {
    return -1;
  }

  InsertRange(offset, count);
  return PersistHeader();
}

//------------------------------------------------------------------------------
// Cached bytes
//------------------------------------------------------------------------------
uint64_t
SparseJournal::CachedBytes() const
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
  return mCachedBytes;
}

//------------------------------------------------------------------------------
// Load range index from sidecar
//------------------------------------------------------------------------------
int
SparseJournal::LoadIndex()
{
  mRanges.clear();
  const off_t idx_size = ::lseek(mIndexFd, 0, SEEK_END);

  if (idx_size < (off_t) sizeof(Header)) {
    return 0;
  }

  const size_t nbytes = (size_t) idx_size - sizeof(Header);
  const size_t nentries = nbytes / (2 * sizeof(off_t));
  std::vector<off_t> raw(nentries * 2);

  if (nentries &&
      ::pread(mIndexFd, raw.data(), nentries * 2 * sizeof(off_t),
              sizeof(Header)) != (ssize_t)(nentries * 2 * sizeof(off_t))) {
    return -1;
  }

  mCachedBytes = 0;

  for (size_t i = 0; i < nentries; ++i) {
    const off_t start = raw[2 * i];
    const off_t stop = raw[2 * i + 1];

    if (stop > start) {
      mRanges[start] = stop;
      mCachedBytes += (uint64_t)(stop - start);
    }
  }

  return 0;
}

//------------------------------------------------------------------------------
// Persist header + index
//------------------------------------------------------------------------------
int
SparseJournal::PersistHeader()
{
  Header hdr{};
  hdr.magic = kMagic;
  hdr.fid = mFid;
  hdr.file_size = mFileSize;
  hdr.mtime = mMTime;
  hdr.cached_bytes = mCachedBytes;

  if (::pwrite(mIndexFd, &hdr, sizeof(hdr), 0) != (ssize_t) sizeof(hdr)) {
    return -1;
  }

  std::vector<off_t> raw;
  raw.reserve(mRanges.size() * 2);

  for (const auto& kv : mRanges) {
    raw.push_back(kv.first);
    raw.push_back(kv.second);
  }

  const size_t nbytes = raw.size() * sizeof(off_t);

  if (nbytes) {
    if (::pwrite(mIndexFd, raw.data(), nbytes, sizeof(Header)) !=
        (ssize_t) nbytes) {
      return -1;
    }
  }

  if (::ftruncate(mIndexFd, (off_t)(sizeof(Header) + nbytes))) {
    return -1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// Insert/merge a cached range
//------------------------------------------------------------------------------
void
SparseJournal::InsertRange(off_t offset, size_t size)
{
  if (size == 0) {
    return;
  }

  off_t start = offset;
  off_t stop = offset + (off_t) size;
  auto it = mRanges.lower_bound(start);

  if (it != mRanges.begin()) {
    auto prev = std::prev(it);

    if (prev->second >= start) {
      start = prev->first;
      stop = std::max(stop, prev->second);
      mCachedBytes -= (uint64_t)(prev->second - prev->first);
      mRanges.erase(prev);
      it = mRanges.lower_bound(start);
    }
  }

  while (it != mRanges.end() && it->first <= stop) {
    stop = std::max(stop, it->second);
    mCachedBytes -= (uint64_t)(it->second - it->first);
    it = mRanges.erase(it);
  }

  mRanges[start] = stop;
  mCachedBytes += (uint64_t)(stop - start);
}

EOSFSTNAMESPACE_END
