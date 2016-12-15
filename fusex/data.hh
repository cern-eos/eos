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
#include "cache.hh"
#include "md.hh"
#include "llfusexx.hh"
#include "fusex/fusex.pb.h"
#include "common/Logging.hh"
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

    datax()
    {
    }

    virtual ~datax()
    {
    }

    XrdSysMutex& Locker()
    {
      return mLock;
    }

    void set_id( uint64_t ino)
    {
      XrdSysMutexHelper mLock(Locker());
      mIno = ino;
      mFile = cachehandler::get(ino);
    }

    uint64_t id () const
    {
      return mIno;
    }

    void flush()
    {
    }

    int attach()
    {
      return mFile->attach();
    }

    int detach()
    {
      return mFile->detach();
    }
    
    int unlink()
    {
      cachehandler::rm(mIno);
      return mFile->unlink();
    }

    // IO bridgeinterface

    ssize_t pread(void *buf, size_t count, off_t offset)
    {
      return mFile->pread(buf, count, offset);
    }

    ssize_t pwrite(const void *buf, size_t count, off_t offset)
    {
      cache::shared_file lfile = mFile;

      return mFile->pwrite(buf, count, offset);
    }

    ssize_t peek_pread(char* &buf, size_t count, off_t offset)
    {
      return mFile->peek_read(buf, count, offset);
    }

    void release_pread()
    {
      return mFile->release_read();
    }

    int truncate(off_t offset)
    {
      return mFile->truncate(offset);
    }

    int sync()
    {
      return mFile->sync();
    }

    size_t size()
    {
      return mFile->size();
    }

  private:
    XrdSysMutex mLock;
    uint64_t mIno;
    cache::shared_file mFile;

  } ;

  typedef std::shared_ptr<datax> shared_data;

  typedef struct _data_fh
  {
    shared_data data;
    metad::shared_md md;
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

  void unlink(fuse_ino_t ino);

  void dataxflush(); // thread pushing into data cache


private:
  dmap datamap;

  XrdSysCondVar ioflush;
  std::set<uint64_t> ioqueue;

  size_t ioqueue_max_backlog;

} ;
#endif /* FUSE_DATA_HH_ */
