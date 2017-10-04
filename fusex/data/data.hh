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
#include "md/md.hh"
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

    datax() : mIno(0), mReq(0), mFile(0), mSize(0), mRemoteInode(0), mRemoteParentInode(0), mAttached(0), mMd(0), mPrefetchHandler(0), mWaitForOpen(false), mFlags(0)
    {
    }

    datax(metad::shared_md md) : mIno(0), mReq(0), mFile(0), mSize(0), mRemoteInode(0), mRemoteParentInode(0), mAttached(0), mMd(md), mPrefetchHandler(0), mWaitForOpen(false), mFlags(0)
    {
    }

    virtual ~datax()
    {
    }

    XrdSysMutex& Locker()
    {
      return mLock;
    }

    void set_id( uint64_t ino, fuse_req_t req)
    {
      XrdSysMutexHelper mLock(Locker());
      mIno = ino;
      mReq = req;
      mFile = cachehandler::get(ino);
      char lid[64];
      snprintf(lid, sizeof (lid), "logid:ino:%016lx", ino);
      SetLogId(lid);
    }

    uint64_t id () const
    {
      return mIno;
    }

    fuse_req_t req () const
    {
      return mReq;
    }

    cache::shared_io file()
    {
      return mFile;
    }

    int flush(fuse_req_t req);
    int flush_nolock(fuse_req_t req);
    int journalflush(fuse_req_t req);
    int journalflush(std::string cid);
    int attach(fuse_req_t req, std::string& cookie, int flags);
    int detach(fuse_req_t req, std::string& cookie, int flags);
    int store_cookie(std::string& cookie);
    int unlink(fuse_req_t req);

    void set_remote(const std::string& hostport,
                    const std::string& basename,
                    const uint64_t md_ino,
                    const uint64_t md_pino,
                    fuse_req_t req);

    // IO bridge interface
    ssize_t pread(fuse_req_t req, void *buf, size_t count, off_t offset);
    ssize_t pwrite(fuse_req_t req, const void *buf, size_t count, off_t offset);
    ssize_t peek_pread(fuse_req_t req, char* &buf, size_t count, off_t offset);
    void release_pread();
    int truncate(fuse_req_t req, off_t offset);
    int sync();
    size_t size();
    int cache_invalidate();
    bool prefetch(fuse_req_t req, bool lock=true);
    void WaitPrefetch(fuse_req_t req, bool lock=true);
    void WaitOpen();

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
  private:
    XrdSysMutex mLock;
    uint64_t mIno;
    fuse_req_t mReq;
    cache::shared_io mFile;
    off_t mSize;
    std::string mRemoteUrl;
    std::string mBaseName;
    uint64_t mRemoteInode;
    uint64_t mRemoteParentInode;
    size_t mAttached;
    metad::shared_md mMd;
    XrdCl::Proxy::read_handler mPrefetchHandler;

    bufferllmanager::shared_buffer buffer;
    static bufferllmanager sBufferManager;
    bool mWaitForOpen;
    int mFlags;
  } ;

  typedef std::shared_ptr<datax> shared_data;

  typedef struct _data_fh
  {
    shared_data data;

    metad::shared_md md;
    bool rw;
    std::string _authid;
    std::atomic<bool> update_mtime_on_flush;
    uint64_t _maxfilesize; // maximum allowed file size
    uint64_t _opensize; // size at the moment of opening the file
    
    _data_fh(shared_data _data, metad::shared_md _md, bool _rw)
    {
      data = _data;
      md = _md;
      rw = _rw;
      update_mtime_on_flush.store(false, std::memory_order_seq_cst);
      _maxfilesize = 0;
      _opensize = md->size();
    }

    ~_data_fh()
    {
    }

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
      if (update_mtime_on_flush.load())
      {
        update_mtime_on_flush.store(false, std::memory_order_seq_cst);
        return true;
      }
      return false;
    }
    
    void set_maxfilesize(uint64_t size)
    {
      _maxfilesize=size;
    }
    
  } data_fh;

  //----------------------------------------------------------------------------

  class dmap : public std::map<fuse_ino_t, shared_data> , public XrdSysMutex
  //----------------------------------------------------------------------------
  {
  public:

    dmap()
    {
      tIOFlush = std::thread(&dmap::ioflush, this);
    }

    virtual ~dmap()
    {
      pthread_cancel(tIOFlush.native_handle());
      tIOFlush.join();
    }

    void ioflush(); // thread for delayed asynchronous close

  private:
    std::thread tIOFlush;
  } ;

  data();

  virtual ~data();

  void init();

  shared_data get(fuse_req_t req,
                  fuse_ino_t ino,
                  metad::shared_md m);

  void release(fuse_req_t req,
               fuse_ino_t ino);

  uint64_t commit(fuse_req_t req,
                  shared_data io);
  
  void unlink(fuse_req_t req, fuse_ino_t ino);

  void invalidate_cache(fuse_ino_t ino);
private:
  dmap datamap;
} ;
#endif /* FUSE_DATA_HH_ */
