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

class data
{
public:

  //----------------------------------------------------------------------------

  class datax
  //----------------------------------------------------------------------------
  {
  public:

    datax() : mIno(0), mReq(0), mFile(0), mSize(0)
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
    }

    uint64_t id () const
    {
      return mIno;
    }

    fuse_req_t req () const
    {
      return mReq;
    }

    void flush();
    int attach(std::string& cookie, bool isRW);
    int detach(std::string& cookie);
    int store_cookie(std::string& cookie);
    int unlink();

    // IO bridge interface
    ssize_t pread(void *buf, size_t count, off_t offset);
    ssize_t pwrite(const void *buf, size_t count, off_t offset);
    ssize_t peek_pread(char* &buf, size_t count, off_t offset);
    void release_pread();
    int truncate(off_t offset);
    int sync();
    size_t size();
    int cache_invalidate();

  private:
    XrdSysMutex mLock;
    uint64_t mIno;
    fuse_req_t mReq;
    cache::shared_io mFile;
    off_t mSize;
  } ;

  typedef std::shared_ptr<datax> shared_data;

  typedef struct _data_fh
  {
    shared_data data;

    metad::shared_md md;
    std::string _authid;
    std::atomic<bool> update_mtime_on_flush;

    _data_fh(shared_data _data, metad::shared_md _md)
    {
      data = _data;
      md = _md;
      update_mtime_on_flush.store(false, std::memory_order_seq_cst);
    }

    ~_data_fh()
    {
    }

    static struct _data_fh* Instance(shared_data io, metad::shared_md md)
    {
      return new struct _data_fh(io, md);
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
  } data_fh;

  //----------------------------------------------------------------------------

  class dmap : public std::map<fuse_ino_t, shared_data> , public XrdSysMutex
  //----------------------------------------------------------------------------
  {
  public:

    dmap()
    {
    }

    virtual ~dmap()
    {
    }
  } ;

  data();

  virtual ~data();

  void init();

  shared_data get(fuse_req_t req,
                  fuse_ino_t ino);

  uint64_t commit(fuse_req_t req,
                  shared_data io);

  void unlink(fuse_req_t req, fuse_ino_t ino);


private:
  dmap datamap;

} ;
#endif /* FUSE_DATA_HH_ */
