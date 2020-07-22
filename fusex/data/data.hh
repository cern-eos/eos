//------------------------------------------------------------------------------
//! @file data.hh
//! @author Andreas-Joachim Peters CERN
//! @brief data handling class
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2016 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#ifndef FUSE_DATA_HH_
#define FUSE_DATA_HH_

#include <sys/stat.h>
#include <sys/types.h>
#include "data/cache.hh"
#include "data/io.hh"
#include "data/cachehandler.hh"
#include "md/md.hh"
#include "cap/cap.hh"
#include "common/AssistedThread.hh"
#include "common/FileId.hh"
#include "bufferll.hh"
#include "llfusexx.hh"
#include "fusex/fusex.pb.h"
#include "common/Logging.hh"
#include "XrdCl/XrdClFile.hh"
#include "XrdSys/XrdSysPthread.hh"
#include <memory>
#include <map>
#include <set>
#include <atomic>
#include <deque>
#include <exception>
#include <stdexcept>
#include <thread>

class data
{
public:

  //----------------------------------------------------------------------------

  class datax : public LogId
  //----------------------------------------------------------------------------
  {
  public:

    datax() : mIno(0), mReq(0), mFile(0), mSize(0), mAttached(0), mMd(0),
	      mPrefetchHandler(0),
	      mSimulateWriteErrorInFlush(false),
	      mSimulateWriteErrorInFlusher(false),
	      mFlags(0), mXoff(false), mIsInlined(false), mInlineMaxSize(0),
	      mInlineCompressor("none"), mIsUnlinked(false),
	      mCanRecoverRead(true)
	      
    {
      inline_buffer = nullptr;
    }

    datax(metad::shared_md md) : mIno(0), mReq(0), mFile(0), mSize(0),
				 mAttached(0), mMd(md), mPrefetchHandler(0),
				 mSimulateWriteErrorInFlush(false),
				 mSimulateWriteErrorInFlusher(false),
				 mFlags(0), mXoff(false),
				 mIsInlined(false), mInlineMaxSize(0), mInlineCompressor("none"),
				 mIsUnlinked(false), mCanRecoverRead(true) { }

    virtual ~datax()
    {
      dump_recovery_stack();
    }

    XrdSysMutex& Locker()
    {
      return mLock;
    }

    void set_id(uint64_t ino, fuse_req_t req);

    uint64_t id() const
    {
      return mIno;
    }

    fuse_req_t req() const
    {
      return mReq;
    }

    shared_io file()
    {
      return mFile;
    }

    void remove_file_cache() {
      mFile->disable_file_cache();
    }

    int flags()
    {
      return mFlags;
    }

    int flush(fuse_req_t req);
    int flush_nolock(fuse_req_t req, bool wait_open = true,
                     bool wait_writes = false);
    bool is_wopen(fuse_req_t req);
    int journalflush(fuse_req_t req);
    int journalflush(std::string cid);
    int journalflush_async(std::string cid);
    int attach(fuse_req_t req, std::string& cookie, int flags);
    bool inline_file(ssize_t size = -1);
    int detach(fuse_req_t req, std::string& cookie, int flags);
    int store_cookie(std::string& cookie);
    int unlink(fuse_req_t req);

    void set_remote(const std::string& hostport,
                    const std::string& basename,
                    const uint64_t md_ino,
                    const uint64_t md_pino,
                    fuse_req_t req,
                    bool isRW);

    // IO bridge interface
    ssize_t pread(fuse_req_t req, void* buf, size_t count, off_t offset);
    ssize_t pwrite(fuse_req_t req, const void* buf, size_t count, off_t offset);
    ssize_t peek_pread(fuse_req_t req, char*& buf, size_t count, off_t offset);
    void release_pread();
    int truncate(fuse_req_t req, off_t offset);
    int sync();
    size_t size();
    int cache_invalidate();
    bool prefetch(fuse_req_t req, bool lock = true);
    void WaitPrefetch(fuse_req_t req, bool lock = true);
    void WaitOpen();
    void FlagDeleted();

    // IO recovery functions
    int TryRecovery(fuse_req_t req, bool is_write);

    int recover_ropen(fuse_req_t req);
    int try_ropen(fuse_req_t req, XrdCl::Proxy*& proxy, std::string open_url);
    int try_wopen(fuse_req_t req, XrdCl::Proxy*& proxy, std::string open_url);
    int recover_read(fuse_req_t req);
    int recover_write(fuse_req_t req);

    int begin_flush(fuse_req_t req);
    int end_flush(fuse_req_t req);


    bool can_recover_read() {
      return mCanRecoverRead;
    }
 
    void disable_read_recovery() {
      mCanRecoverRead = false;
    }

    // ref counting for this object

    void attach()
    {
      XrdSysMutexHelper lLock(mLock);
      mAttached++;
    }

    bool detach()
    {
      XrdSysMutexHelper lLock(mLock);
      return (--mAttached);
    }

    bool detach_nolock()
    {
      return (--mAttached);
    }

    bool attached()
    {
      XrdSysMutexHelper lLock(mLock);
      return mAttached ? true : false;
    }

    bool attached_nolock()
    {
      return (mAttached) ? true : false;
    }

    bool attached_once_nolock()
    {
      return (mAttached == 1) ? true : false;
    }

    bool unlinked()
    {
      // caller has to have this object locked
      return mIsUnlinked;
    }

    static bufferllmanager sBufferManager;

    bool simulate_write_error_in_flusher()
    {
      return mSimulateWriteErrorInFlusher;
    }

    bool simulate_write_error_in_flush()
    {
      return mSimulateWriteErrorInFlush;
    }

    bool inlined()
    {
      return mIsInlined;
    }

    static std::string kInlineAttribute;
    static std::string kInlineMaxSize;
    static std::string kInlineCompressor;


    metad::shared_md md() {
      return mMd;
    }

    std::string fullpath()
    {
      return mMd->fullpath();
    }

    std::string fid()
    {
      return std::to_string(eos::common::FileId::InodeToFid(mMd->md_ino()));
    }

    std::deque<std::string> recoverystack()
    {
      return mRecoveryStack;
    }

    void dump_recovery_stack();

    const char* Dump(std::string& out);

  private:
    XrdSysMutex mLock;
    uint64_t mIno;
    fuse_req_t mReq;
    shared_io mFile;
    off_t mSize;
    std::string mRemoteUrlRW;
    std::string mRemoteUrlRO;
    std::string mBaseName;
    size_t mAttached;
    metad::shared_md mMd;
    XrdCl::Proxy::read_handler mPrefetchHandler;
    std::deque<std::string> mReadErrorStack;
    std::deque<std::string> mRecoveryStack;

    bufferllmanager::shared_buffer buffer;
    bool mSimulateWriteErrorInFlush;
    bool mSimulateWriteErrorInFlusher;
    int mFlags;

    bool mXoff;
    bool mIsInlined;
    uint64_t mInlineMaxSize;
    std::string mInlineCompressor;
    bufferllmanager::shared_buffer inline_buffer;
    bool mIsUnlinked;
    bool mCanRecoverRead;
  };

  typedef std::shared_ptr<datax> shared_data;

  typedef struct _data_fh {
    shared_data data;
    cap::shared_cap cap_;
    metad::shared_md md;
    bool rw;
    bool flocked;
    std::string _authid;
    std::atomic<bool> update_mtime_on_flush;
    std::atomic<time_t> next_size_flush;
    uint64_t _maxfilesize; // maximum allowed file size
    uint64_t _opensize; // size at the moment of opening the file

    _data_fh(shared_data _data, metad::shared_md _md, bool _rw)
    {
      data = _data;
      md = _md;
      rw = _rw;
      update_mtime_on_flush.store(false, std::memory_order_seq_cst);
      next_size_flush.store(0, std::memory_order_seq_cst);
      _maxfilesize = 0;
      _opensize = md->size();
    }

    ~_data_fh() { }

    static struct _data_fh* Instance(shared_data io, metad::shared_md md, bool rw)
    {
      return new struct _data_fh(io, md, rw);
    }

    shared_data ioctx()
    {
      return data;
    }

    metad::shared_md mdctx()
    {
      return md;
    }

    std::string authid() const
    {
      return _authid;
    }

    uint64_t maxfilesize() const
    {
      return _maxfilesize;
    }

    uint64_t opensize() const
    {
      return _opensize;
    }

    void set_authid(const std::string& authid)
    {
      _authid = authid;
    }

    void set_update()
    {
      update_mtime_on_flush.store(true, std::memory_order_seq_cst);
    }

    bool has_update()
    {
      if (update_mtime_on_flush.load()) {
        update_mtime_on_flush.store(false, std::memory_order_seq_cst);
        return true;
      }

      return false;
    }

    void set_maxfilesize(uint64_t size)
    {
      _maxfilesize = size;
    }

  } data_fh;

  //----------------------------------------------------------------------------

  class dmap : public std::map<fuse_ino_t, shared_data>, public XrdSysMutex
  //----------------------------------------------------------------------------
  {
  public:

    dmap() { }

    virtual ~dmap() { }

    void run()
    {
      tIOFlush.reset(&dmap::ioflush, this);
    }

    void ioflush(ThreadAssistant&
                 assistant); // thread for delayed asynchronous close

    bool waitflush(uint64_t seconds);
    void join() { tIOFlush.join(); }

  private:
    AssistedThread tIOFlush;
  };

  data();

  virtual ~data();

  void init();
  void terminate(uint64_t seconds);

  shared_data get(fuse_req_t req,
                  fuse_ino_t ino,
                  metad::shared_md m);

  bool has(fuse_ino_t ino, bool checkwriteopen = false);
  metad::shared_md retrieve_wr_md(fuse_ino_t ino);

  void release(fuse_req_t req,
               fuse_ino_t ino);

  uint64_t commit(fuse_req_t req,
                  shared_data io);

  void unlink(fuse_req_t req, fuse_ino_t ino);

  void update_cookie(uint64_t ino, std::string& cookie);

  void invalidate_cache(fuse_ino_t ino);

  size_t size()
  {
    XrdSysMutexHelper mLock(datamap);
    return datamap.size();
  }

  void set_xoff()
  {
    xoffCounter.fetch_add(1, std::memory_order_seq_cst);
  }

  uint64_t get_xoff()
  {
    return xoffCounter.load();
  }



private:
  dmap datamap;
  std::atomic<uint64_t>  xoffCounter;
};

#endif /* FUSE_DATA_HH_ */
