//------------------------------------------------------------------------------
//! @file CacheLayout.cc
//------------------------------------------------------------------------------

#include "fst/layout/CacheLayout.hh"
#include "fst/cache/CacheLru.hh"
#include "fst/io/FileIoPlugin.hh"
#include "fst/XrdFstOfsFile.hh"
#include "fst/XrdFstOfs.hh"
#include "common/Constants.hh"
#include "common/FileId.hh"
#include "common/Logging.hh"
#include "common/StringConversion.hh"
#include "fst/storage/FileSystem.hh"
#include "fst/storage/Storage.hh"
#include <XrdOuc/XrdOucEnv.hh>
#include <cstdlib>
#include <cstring>
#include <vector>

EOSFSTNAMESPACE_BEGIN

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
  const char* smtime = mOfsFile->mOpenOpaque ?
                       mOfsFile->mOpenOpaque->Get("mgm.mtime") : nullptr;

  if (!sfid || !cache_fsid) {
    return Emsg("CacheLayout::Open", *mError, EINVAL,
                "open cache layout - missing fid/cache.fsid", opaque);
  }

  mFid = eos::common::FileId::Hex2Fid(sfid);
  mCacheFsId = atoi(cache_fsid);
  mFileSize = ssize ? strtoull(ssize, nullptr, 10) : 0;
  mMTime = smtime ? (time_t) strtoll(smtime, nullptr, 10) : 0;
  mCacheFsPath = gOFS.Storage->GetStoragePath(mCacheFsId).c_str();

  if (mCacheFsPath.empty()) {
    return Emsg("CacheLayout::Open", *mError, EINVAL,
                "open cache layout - unknown cache filesystem", opaque);
  }

  mLru = CacheLruRegistry::Instance().GetOrCreate(mCacheFsId, mCacheFsPath);

  // Refresh watermarks from filesystem shared-hash (set via eos space config)
  if (mLru) {
    auto* fs = gOFS.Storage->GetFileSystemById(mCacheFsId);
    unsigned low = 70;
    unsigned high = 85;

    if (fs) {
      const std::string cfg_low =
        fs->GetString(eos::common::SPACE_CACHE_LOW_WATERMARK_NAME);
      const std::string cfg_high =
        fs->GetString(eos::common::SPACE_CACHE_HIGH_WATERMARK_NAME);

      if (!cfg_low.empty()) {
        const long v = std::strtol(cfg_low.c_str(), nullptr, 10);

        if (v >= 0 && v <= 100) {
          low = (unsigned) v;
        }
      }

      if (!cfg_high.empty()) {
        const long v = std::strtol(cfg_high.c_str(), nullptr, 10);

        if (v >= 0 && v <= 100) {
          high = (unsigned) v;
        }
      }
    }

    mLru->SetWatermarks(low, high);
  }

  // Prefer bridge when cache is under pressure
  if (mLru && !mLru->CanAdmit(0)) {
    mLru->EvictToLowWatermark();

    if (!mLru->CanAdmit(0)) {
      mBridgeOnly = true;
      mLru->mBridges++;
    }
  }

  if (!mBridgeOnly) {
    if (mJournal.Open(mCacheFsPath, mFid, mFileSize, mMTime)) {
      eos_warning("msg=\"journal open failed, bridging\" fxid=%08llx", mFid);
      mBridgeOnly = true;

      if (mLru) {
        mLru->mBridges++;
      }
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

  if (mBridgeOnly || !mJournal.IsOpen()) {
    if (mLru) {
      mLru->mMisses++;
    }

    return BridgeRead(offset, buffer, length);
  }

  // Serve any leading cached prefix
  const ssize_t from_cache = mJournal.Read(buffer, length, offset);

  if (from_cache < 0) {
    return Emsg("CacheLayout::Read", *mError, EIO, "read cache journal");
  }

  if (from_cache == length) {
    if (mLru) {
      mLru->mHits++;
      mLru->FileAccessed(mFid, mJournal.CachedBytes());
    }

    return from_cache;
  }

  if (mLru) {
    mLru->mMisses++;
  }

  // Fill remaining hole(s) from backend
  XrdSfsFileOffset cur = offset + from_cache;
  XrdSfsXferSize left = length - (XrdSfsXferSize) from_cache;
  char* out = buffer + from_cache;
  int64_t total = from_cache;

  while (left > 0) {
    auto missing = mJournal.MissingRanges(cur, left);

    if (missing.empty()) {
      // Rest should be in journal
      const ssize_t n = mJournal.Read(out, left, cur);

      if (n < 0) {
        return -1;
      }

      total += n;
      break;
    }

    for (const auto& hole : missing) {
      std::vector<char> tmp(hole.size);
      const int64_t n = BridgeRead(hole.offset, tmp.data(),
                                   (XrdSfsXferSize) hole.size);

      if (n < 0) {
        return Emsg("CacheLayout::Read", *mError, EIO, "read cache backend");
      }

      // Copy into user buffer for the overlapping part of this Read call
      const off_t copy_off = hole.offset - offset;
      const size_t copy_len = std::min((size_t) n, (size_t) length - copy_off);

      if (copy_off >= 0 && copy_off < length) {
        memcpy(buffer + copy_off, tmp.data(), copy_len);
      }

      // Admit into journal if possible
      bool admitted = false;

      if (mLru && mLru->CanAdmit((uint64_t) n)) {
        if (mJournal.Write(tmp.data(), (size_t) n, hole.offset) == 0) {
          admitted = true;
          mLru->FileAccessed(mFid, mJournal.CachedBytes());
        }
      }

      if (!admitted && mLru) {
        mLru->mBridges++;
      }

      total += (int64_t) copy_len;
      cur = hole.offset + n;
      left = length - (XrdSfsXferSize)(cur - offset);
      out = buffer + (cur - offset);

      if (left <= 0) {
        break;
      }
    }

    break;
  }

  return total;
}

int64_t
CacheLayout::ReadV(XrdCl::ChunkList& chunkList, uint32_t len)
{
  int64_t total = 0;

  for (auto& chunk : chunkList) {
    const int64_t n = Read(chunk.offset, chunk.buffer, chunk.length, false);

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
  if (mJournal.IsOpen()) {
    (void) mJournal.Unlink();
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

  mJournal.Close();
  mOpened = false;
  return SFS_OK;
}

EOSFSTNAMESPACE_END
