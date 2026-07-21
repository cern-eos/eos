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

//------------------------------------------------------------------------------
// Path helpers
//------------------------------------------------------------------------------
std::string
SparseJournal::CacheDir(const std::string& cache_fs_path)
{
  std::string path = cache_fs_path;

  if (!path.empty() && path.back() != '/') {
    path += '/';
  }

  path += ".eoscache/";
  return path;
}

std::string
SparseJournal::DataPath(const std::string& cache_fs_path, uint64_t fid)
{
  // Same sharding as FST replicas: .eoscache/<fid/10000>/<hexfid>
  // so a large cache does not dump millions of files into one directory.
  const std::string hex = eos::common::FileId::Fid2Hex(fid);
  return eos::common::FileId::FidPrefix2FullPath(hex.c_str(),
         CacheDir(cache_fs_path).c_str());
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
  const std::string journal_path = DataPath(cache_fs_path, fid);

  if (mDataFd >= 0) {
    if ((journal_path == mJournalPath) && (fid == mFid)) {
      if ((expected_size == mFileSize) && (expected_mtime == mMTime)) {
        // Same identity - shared re-open
        return 0;
      }

      // Identity changed (file mutated) - drop cached content
      mFileSize = expected_size;
      mMTime = expected_mtime;
      return TruncateLocked();
    }

    CloseLocked(true);
  }
  mFid = fid;
  mFileSize = expected_size;
  mMTime = expected_mtime;
  mJournalPath = journal_path;
  eos::common::Path cpath(mJournalPath.c_str());
  (void) cpath.MakeParentPath(S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
  mDataFd = ::open(mJournalPath.c_str(), O_RDWR | O_CREAT, 0644);
  mIndexFd = ::open(IndexPathLocked().c_str(), O_RDWR | O_CREAT, 0644);

  if ((mDataFd < 0) || (mIndexFd < 0)) {
    eos_static_err("msg=\"failed to open cache journal\" path=%s errno=%d",
                   mJournalPath.c_str(), errno);
    CloseLocked(false);
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
      ResetLocked();
    } else {
      mFileSize = hdr.file_size;
      mMTime = hdr.mtime;
      mCachedBytes = hdr.cached_bytes;

      if (LoadIndexLocked()) {
        eos_static_warning("msg=\"cache journal index invalid, resetting\" "
                           "fxid=%08llx", fid);
        ResetLocked();
      }

      return 0;
    }
  } else {
    // No/foreign header - make sure stale data does not survive
    ResetLocked();
  }

  return PersistIndexLocked();
}

//------------------------------------------------------------------------------
// Close
//------------------------------------------------------------------------------
void
SparseJournal::Close()
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
  CloseLocked(true);
}

void
SparseJournal::CloseLocked(bool persist)
{
  if (persist && mDirty && (mDataFd >= 0) && (mIndexFd >= 0)) {
    (void) PersistIndexLocked();
  }

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
  mUnpersistedBytes = 0;
  mDirty = false;
}

//------------------------------------------------------------------------------
// Reset in-memory + on-disk state (under lock)
//------------------------------------------------------------------------------
void
SparseJournal::ResetLocked()
{
  mRanges.clear();
  mCachedBytes = 0;
  mUnpersistedBytes = 0;
  mDirty = false;
  (void) ::ftruncate(mDataFd, 0);
  (void) ::ftruncate(mIndexFd, 0);
}

//------------------------------------------------------------------------------
// Truncate journal contents
//------------------------------------------------------------------------------
int
SparseJournal::Truncate()
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
  return TruncateLocked();
}

int
SparseJournal::TruncateLocked()
{
  if (mDataFd < 0) {
    return -1;
  }

  ResetLocked();
  return PersistIndexLocked();
}

//------------------------------------------------------------------------------
// Unlink journal files
//------------------------------------------------------------------------------
int
SparseJournal::Unlink()
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
  CloseLocked(false);
  int rc = 0;

  if (!mJournalPath.empty()) {
    if (::unlink(mJournalPath.c_str()) && errno != ENOENT) {
      rc = -1;
    }

    if (::unlink(IndexPathLocked().c_str()) && errno != ENOENT) {
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

    // Hole at cur - stop so caller can fill from backend
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
    auto next = mRanges.lower_bound(cur);

    if (next != mRanges.end() && next->first < end) {
      hole_end = next->first;
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

  InsertRangeLocked(offset, count);
  mDirty = true;
  mUnpersistedBytes += count;

  if (mUnpersistedBytes >= kPersistThresholdBytes) {
    return PersistIndexLocked();
  }

  return 0;
}

//------------------------------------------------------------------------------
// Accessors
//------------------------------------------------------------------------------
uint64_t
SparseJournal::CachedBytes() const
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
  return mCachedBytes;
}

uint64_t
SparseJournal::FileSize() const
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
  return mFileSize;
}

time_t
SparseJournal::MTime() const
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
  return mMTime;
}

uint64_t
SparseJournal::GetFid() const
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
  return mFid;
}

std::string
SparseJournal::GetPath() const
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
  return mJournalPath;
}

bool
SparseJournal::IsOpen() const
{
  std::lock_guard<std::mutex> scope_lock(mMutex);
  return mDataFd >= 0;
}

//------------------------------------------------------------------------------
// Load range index from sidecar with validation
//------------------------------------------------------------------------------
int
SparseJournal::LoadIndexLocked()
{
  mRanges.clear();
  Header hdr{};

  if (::pread(mIndexFd, &hdr, sizeof(hdr), 0) != (ssize_t) sizeof(hdr)) {
    return -1;
  }

  const off_t idx_size = ::lseek(mIndexFd, 0, SEEK_END);
  const size_t expected =
    sizeof(Header) + hdr.nentries * 2 * sizeof(off_t);

  // A truncated or overlong index means a torn write - reject it
  if ((idx_size < 0) || ((size_t) idx_size != expected)) {
    return -1;
  }

  std::vector<off_t> raw(hdr.nentries * 2);

  if (hdr.nentries &&
      ::pread(mIndexFd, raw.data(), hdr.nentries * 2 * sizeof(off_t),
              sizeof(Header)) != (ssize_t)(hdr.nentries * 2 * sizeof(off_t))) {
    return -1;
  }

  uint64_t cached = 0;
  off_t prev_stop = 0;

  for (size_t i = 0; i < hdr.nentries; ++i) {
    const off_t start = raw[2 * i];
    const off_t stop = raw[2 * i + 1];

    // Entries must be positive, sorted, non-overlapping and within file size
    if ((start < prev_stop) || (stop <= start) ||
        (mFileSize && ((uint64_t) stop > mFileSize))) {
      return -1;
    }

    mRanges[start] = stop;
    cached += (uint64_t)(stop - start);
    prev_stop = stop;
  }

  if (cached != hdr.cached_bytes) {
    return -1;
  }

  mCachedBytes = cached;
  return 0;
}

//------------------------------------------------------------------------------
// Persist header + index (data synced first for crash consistency)
//------------------------------------------------------------------------------
int
SparseJournal::PersistIndexLocked()
{
  // Data must be durable before the index references it
#ifdef __APPLE__
  (void) ::fsync(mDataFd);
#else
  (void) ::fdatasync(mDataFd);
#endif
  Header hdr{};
  hdr.magic = kMagic;
  hdr.fid = mFid;
  hdr.file_size = mFileSize;
  hdr.mtime = mMTime;
  hdr.cached_bytes = mCachedBytes;
  hdr.nentries = mRanges.size();
  std::vector<char> blob(sizeof(Header) + mRanges.size() * 2 * sizeof(off_t));
  memcpy(blob.data(), &hdr, sizeof(hdr));
  off_t* entries = reinterpret_cast<off_t*>(blob.data() + sizeof(Header));
  size_t i = 0;

  for (const auto& kv : mRanges) {
    entries[i++] = kv.first;
    entries[i++] = kv.second;
  }

  if (::pwrite(mIndexFd, blob.data(), blob.size(), 0) !=
      (ssize_t) blob.size()) {
    return -1;
  }

  if (::ftruncate(mIndexFd, (off_t) blob.size())) {
    return -1;
  }

#ifdef __APPLE__
  (void) ::fsync(mIndexFd);
#else
  (void) ::fdatasync(mIndexFd);
#endif
  mDirty = false;
  mUnpersistedBytes = 0;
  return 0;
}

//------------------------------------------------------------------------------
// Insert/merge a cached range (under lock)
//------------------------------------------------------------------------------
void
SparseJournal::InsertRangeLocked(off_t offset, size_t size)
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
