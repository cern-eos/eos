//------------------------------------------------------------------------------
//! @file CacheLayout.hh
//! @brief Read-through cache layout with sparse journal + backend bridge
//------------------------------------------------------------------------------

#pragma once

#include "fst/layout/Layout.hh"
#include "fst/cache/SparseJournal.hh"
#include <memory>
#include <string>

EOSFSTNAMESPACE_BEGIN

class CacheLru;

//------------------------------------------------------------------------------
//! Layout that serves reads from a local sparse journal and fetches/bridges
//! missing ranges from a backend FST URL provided by the MGM.
//------------------------------------------------------------------------------
class CacheLayout : public Layout
{
public:
  CacheLayout(XrdFstOfsFile* file, unsigned long lid,
              const XrdSecEntity* client, XrdOucErrInfo* outError,
              const char* path, eos::fst::FmdHandler* fmdHandler,
              uint16_t timeout = 0);

  virtual ~CacheLayout();

  virtual int Open(XrdSfsFileOpenMode flags, mode_t mode,
                   const char* opaque = "") override;

  virtual int64_t Read(XrdSfsFileOffset offset, char* buffer,
                       XrdSfsXferSize length, bool readahead = false) override;

  virtual int64_t ReadV(XrdCl::ChunkList& chunkList, uint32_t len) override;

  virtual int64_t Write(XrdSfsFileOffset offset, const char* buffer,
                        XrdSfsXferSize length) override;

  virtual int Truncate(XrdSfsFileOffset offset) override;
  virtual int Remove() override;
  virtual int Sync() override;
  virtual int Stat(struct stat* buf) override;
  virtual int Close() override;

private:
  int OpenBackend(const char* opaque);
  int64_t BridgeRead(XrdSfsFileOffset offset, char* buffer,
                     XrdSfsXferSize length);

  SparseJournal mJournal;
  std::unique_ptr<FileIo> mBackendIo;
  CacheLru* mLru{nullptr};
  bool mBridgeOnly{false};
  bool mOpened{false};
  uint64_t mFid{0};
  uint64_t mFileSize{0};
  time_t mMTime{0};
  std::string mCacheFsPath;
  eos::common::FileSystem::fsid_t mCacheFsId{0};
};

EOSFSTNAMESPACE_END
