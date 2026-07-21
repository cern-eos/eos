//------------------------------------------------------------------------------
//! @file CacheLayout.cc
//------------------------------------------------------------------------------

#include "fst/layout/CacheLayout.hh"
#include "fst/cache/CacheLru.hh"
#include "fst/cache/SparseJournal.hh"
#include "fst/io/FileIoPlugin.hh"
#include "fst/XrdFstOfsFile.hh"
#include "common/Constants.hh"
#include "common/FileId.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include <XrdOuc/XrdOucEnv.hh>
#include <cstdlib>
#include <cstring>
#include <vector>

EOSFSTNAMESPACE_BEGIN

namespace
{
//------------------------------------------------------------------------------
//! Parse a watermark percent string; returns fallback when out of range
//------------------------------------------------------------------------------
unsigned
ParseWatermark(const char* value, unsigned fallback)
{
  if (!value || !*value) {
    return fallback;
  }

  char* end = nullptr;
  const long v = std::strtol(value, &end, 10);

  if ((end == value) || (v < 0) || (v > 100)) {
    return fallback;
  }

  return (unsigned) v;
}
}

CacheLayout::CacheLayout(XrdFstOfsFile* file, unsigned long lid,
                         const XrdSecEntity* client, XrdOucErrInfo* outError,
                         const char* path, eos::fst::FmdHandler* fmdHandler,
                         uint16_t timeout)
  : Layout(file, lid, client, outError, path, fmdHandler, timeout)
{
  mName = "Cache";
  mIsEntryServer = true;
}

CacheLayout::~CacheLayout()
{
  (void) Close();
}

int
CacheLayout::Open(XrdSfsFileOpenMode flags, mode_t mode, const char* opaque)
{
  if (flags & (SFS_O_CREAT | SFS_O_TRUNC | SFS_O_WRONLY | SFS_O_RDWR)) {
    return Emsg("CacheLayout::Open", *mError, EPERM,
                "open cache layout for write", mLocalPath.c_str());
  }

  if (!mOfsFile || !mOfsFile->mCapOpaque) {
    return Emsg("CacheLayout::Open", *mError, EINVAL,
                "open cache layout - missing capability", opaque);
  }

  XrdOucEnv* cap = mOfsFile->mCapOpaque.get();
  const char* sfid = cap->Get("mgm.fid");
  const char* cache_fsid = cap->Get("mgm.cache.fsid");
  const char* ssize = cap->Get("mgm.size");
  // The MGM-signed mtime is authoritative for journal staleness detection
  const char* smtime = cap->Get("mgm.cache.mtime");

  if (!smtime && mOfsFile->mOpenOpaque) {
    smtime = mOfsFile->mOpenOpaque->Get("mgm.mtime");
  }

  if (!sfid || !cache_fsid) {
    return Emsg("CacheLayout::Open", *mError, EINVAL,
                "open cache layout - missing fid/cache.fsid", opaque);
  }

  mFid = eos::common::FileId::Hex2Fid(sfid);
  mCacheFsId = atoi(cache_fsid);
  mFileSize = ssize ? strtoull(ssize, nullptr, 10) : 0;
  mMTime = smtime ? (time_t) strtoll(smtime, nullptr, 10) : 0;
  mCacheFsPath = mOfsFile->mLocalPrefix.c_str();

  if (mCacheFsPath.empty()) {
    return Emsg("CacheLayout::Open", *mError, EINVAL,
                "open cache layout - unknown cache filesystem", opaque);
  }

  mLru = CacheLruRegistry::Instance().GetOrCreate(mCacheFsId, mCacheFsPath);

  if (mLru) {
    // Watermarks: local filesystem config (pushed by the MGM via
    // "space config") takes precedence, the capability value is the
    // fallback, then the compiled-in defaults.
    unsigned low = ParseWatermark(
                     eos::common::SPACE_CACHE_LOW_WATERMARK_DEFAULT, 70);
    unsigned high = ParseWatermark(
                      eos::common::SPACE_CACHE_HIGH_WATERMARK_DEFAULT, 85);
    low = ParseWatermark(cap->Get("mgm.cache.low_watermark"), low);
    high = ParseWatermark(cap->Get("mgm.cache.high_watermark"), high);
    // The resolver is registered by the FST server; in standalone tools it
    // is absent and the capability values apply
    const std::string fs_low = CacheLruRegistry::Instance().GetConfig(
                                 mCacheFsId,
                                 eos::common::SPACE_CACHE_LOW_WATERMARK_NAME);
    const std::string fs_high = CacheLruRegistry::Instance().GetConfig(
                                  mCacheFsId,
                                  eos::common::SPACE_CACHE_HIGH_WATERMARK_NAME);
    low = ParseWatermark(fs_low.c_str(), low);
    high = ParseWatermark(fs_high.c_str(), high);
    mLru->SetWatermarks(low, high);
  }

  // Under pressure trigger background eviction; the journal is still opened
  // since already-cached ranges can be served without admitting new bytes
  // (admission is gated per-write via CanAdmit in Read)
  if (mLru && !mLru->CanAdmit(0)) {
    mLru->MaybeEvictAsync();
  }

  mJournal = mLru ? mLru->GetJournal(mFid, mFileSize, mMTime) : nullptr;

  if (!mJournal) {
    eos_warning("msg=\"journal open failed, bridging\" fxid=%08llx", mFid);
    mBridgeOnly = true;

    if (mLru) {
      mLru->mBridges++;
    }
  }

  if (OpenBackend(opaque)) {
    return Emsg("CacheLayout::Open", *mError, EIO,
                "open cache backend", opaque);
  }

  mOpened = true;
  return SFS_OK;
}

int
CacheLayout::OpenBackend(const char* /*opaque*/)
{
  XrdOucEnv* cap = mOfsFile->mCapOpaque.get();
  const char* host = cap->Get("mgm.cache.backend.host");
  const char* sport = cap->Get("mgm.cache.backend.port");
  const char* path = cap->Get("mgm.path");

  if (!host || !sport || !path) {
    eos_err("%s", "msg=\"cache backend contact missing from capability\"");
    return -1;
  }

  // Forward the original open opaque (includes encrypted capability) to the
  // backend FST. Backend uses mgm.fsid from the capability for its local path.
  int envlen = 0;
  const char* open_env = mOfsFile->mOpenOpaque ?
                         mOfsFile->mOpenOpaque->Env(envlen) : "";
  std::string url = "root://";
  url += host;
  url += ":";
  url += sport;
  url += "/";
  url += path;
  url += "?";
  url += open_env;
  // Mark this open as the bridge fetch so that an FST hosting both the cache
  // and the backend filesystem serves it as a normal (non-cache) open instead
  // of recursing into CacheLayout again.
  url += "&eos.cache.bridge=1";
  mBackendIo.reset(FileIoPlugin::GetIoObject(url, mOfsFile, mSecEntity));

  if (!mBackendIo) {
    return -1;
  }

  const int rc = mBackendIo->fileOpen(0, 0, open_env, mTimeout);
  mLastUrl = mBackendIo->GetLastUrl();
  mLastTriedUrl = mBackendIo->GetLastTriedUrl();
  mLastErrCode = mBackendIo->GetLastErrCode();
  mLastErrNo = mBackendIo->GetLastErrNo();
  return rc;
}

int64_t
CacheLayout::BridgeRead(XrdSfsFileOffset offset, char* buffer,
                        XrdSfsXferSize length)
{
  if (!mBackendIo) {
    return -1;
  }

  return mBackendIo->fileRead(offset, buffer, length, mTimeout);
}

//------------------------------------------------------------------------------
// Read: alternate between cached segments (journal) and holes (backend) so
// the user buffer is filled contiguously from 'offset'. Returns the number of
// contiguous bytes placed at the start of 'buffer'.
//------------------------------------------------------------------------------
int64_t
CacheLayout::Read(XrdSfsFileOffset offset, char* buffer,
                  XrdSfsXferSize length, bool /*readahead*/)
{
  if (!mOpened || length <= 0) {
    return 0;
  }

  if ((uint64_t) offset >= mFileSize) {
    return 0;
  }

  if ((uint64_t)(offset + length) > mFileSize) {
    length = (XrdSfsXferSize)(mFileSize - offset);
  }

  if (mBridgeOnly || !mJournal || !mJournal->IsOpen()) {
    if (mLru) {
      mLru->mMisses++;
    }

    return BridgeRead(offset, buffer, length);
  }

  bool bridged = false;
  off_t cur = offset;
  const off_t end = offset + (off_t) length;

  while (cur < end) {
    const auto missing = mJournal->MissingRanges(cur, (size_t)(end - cur));
    const off_t seg_end = missing.empty() ? end : missing.front().offset;

    if (seg_end > cur) {
      // Cached segment [cur, seg_end)
      const ssize_t n = mJournal->Read(buffer + (cur - offset),
                                       (size_t)(seg_end - cur), cur);

      if (n < 0) {
        return Emsg("CacheLayout::Read", *mError, EIO, "read cache journal");
      }

      cur += n;

      if (cur < seg_end) {
        // Journal claims the range but returned short (e.g. concurrent
        // truncation) - bridge the remainder of the segment to make progress
        const int64_t bn = BridgeRead(cur, buffer + (cur - offset),
                                      (XrdSfsXferSize)(seg_end - cur));
        bridged = true;

        if (bn < 0) {
          return Emsg("CacheLayout::Read", *mError, EIO, "read cache backend");
        }

        cur += bn;

        if (cur < seg_end) {
          break; // backend short read
        }
      }

      continue;
    }

    // Hole starting at cur
    const auto& hole = missing.front();
    const int64_t bn = BridgeRead(hole.offset, buffer + (hole.offset - offset),
                                  (XrdSfsXferSize) hole.size);
    bridged = true;

    if (bn < 0) {
      return Emsg("CacheLayout::Read", *mError, EIO, "read cache backend");
    }

    if (bn == 0) {
      break;
    }

    // Admit into journal if possible
    bool admitted = false;

    if (mLru && mLru->CanAdmit((uint64_t) bn)) {
      if (mJournal->Write(buffer + (hole.offset - offset), (size_t) bn,
                          hole.offset) == 0) {
        admitted = true;
      }
    }

    if (!admitted && mLru) {
      mLru->mBridges++;
      mLru->MaybeEvictAsync();
    }

    cur = hole.offset + bn;

    if ((size_t) bn < hole.size) {
      break; // backend short read
    }
  }

  if (mLru) {
    if (bridged) {
      mLru->mMisses++;
    } else {
      mLru->mHits++;
    }

    mLru->FileAccessed(mFid, mJournal->CachedBytes());
  }

  return (int64_t)(cur - offset);
}

int64_t
CacheLayout::ReadV(XrdCl::ChunkList& chunkList, uint32_t len)
{
  int64_t total = 0;

  for (auto& chunk : chunkList) {
    const int64_t n = Read(chunk.offset, static_cast<char*>(chunk.buffer),
                           chunk.length, false);

    if (n < 0) {
      return n;
    }

    total += n;
  }

  (void) len;
  return total;
}

int64_t
CacheLayout::Write(XrdSfsFileOffset, const char*, XrdSfsXferSize)
{
  return Emsg("CacheLayout::Write", *mError, EPERM, "write cache layout");
}

int
CacheLayout::Truncate(XrdSfsFileOffset)
{
  return Emsg("CacheLayout::Truncate", *mError, EPERM, "truncate cache layout");
}

int
CacheLayout::Remove()
{
  if (mJournal && mJournal->IsOpen()) {
    (void) mJournal->Unlink();
  }

  if (mLru) {
    mLru->FileRemoved(mFid);
  }

  return SFS_OK;
}

int
CacheLayout::Sync()
{
  return SFS_OK;
}

int
CacheLayout::Stat(struct stat* buf)
{
  if (!buf) {
    return SFS_ERROR;
  }

  memset(buf, 0, sizeof(*buf));
  buf->st_size = (off_t) mFileSize;
  buf->st_mtime = mMTime;
  buf->st_mode = S_IFREG | 0644;
  return SFS_OK;
}

int
CacheLayout::Close()
{
  if (!mOpened) {
    return SFS_OK;
  }

  if (mBackendIo) {
    (void) mBackendIo->fileClose(mTimeout);
    mBackendIo.reset();
  }

  // Drop our reference; the journal persists its index when the last
  // reference goes away
  mJournal.reset();
  mOpened = false;
  return SFS_OK;
}

int
CacheLayout::Fctl(const std::string& cmd, const XrdSecEntity* client)
{
  (void) client;
  return mFileIO->fileFctl(cmd);
}

EOSFSTNAMESPACE_END
