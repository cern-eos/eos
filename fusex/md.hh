//------------------------------------------------------------------------------
//! @file md.hh
//! @author Andreas-Joachim Peters CERN
//! @brief meta data handling class
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

#ifndef FUSE_MD_HH_
#define FUSE_MD_HH_

#include <sys/stat.h>
#include <sys/types.h>
#include "llfusexx.hh"
#include "fusex/fusex.pb.h"
#include "kv.hh"
#include "common/Logging.hh"
#include "XrdSys/XrdSysPthread.hh"
#include <memory>
#include <map>
#include <set>
#include <atomic>
#include <exception>
#include <stdexcept>

class metad
{
public:

  //----------------------------------------------------------------------------

  class mdx : public eos::fusex::md
  //----------------------------------------------------------------------------
  {
  public:

    enum md_op
    {
      ADD, DELETE, SETSIZE
    } ;

    mdx()
    {
      setop_add();
      lookup_cnt = 0;
    }

    mdx(fuse_ino_t ino)
    {
      set_id(ino);
      setop_add();
      lookup_cnt = 0;
    }

    virtual ~mdx()
    {
    }

    XrdSysMutex& Locker()
    {
      return mLock;
    }

    void convert(fuse_entry_param &e);
    std::string dump();
    static std::string dump(struct fuse_entry_param &e);

    void setop_delete()
    {
      op = DELETE;
    }

    void setop_add()
    {
      op = ADD;
    }

    void setop_setsize()
    {
      op = SETSIZE;
    }

    void lookup_inc()
    {
      // requires to have Locker outside
      lookup_cnt++;
    }

    bool lookup_dec(int n)
    {
      // requires to have Lock outside
      lookup_cnt-=n;
      if (lookup_cnt > 0)
        return false;
      return true;
    }

    md_op getop() const
    {
      return op;
    }

    bool deleted() const
    {
      return (op == DELETE) ;
    }
  private:
    XrdSysMutex mLock;
    md_op op;
    int lookup_cnt;

  } ;

  typedef std::shared_ptr<mdx> shared_md;

  //----------------------------------------------------------------------------

  class vnode_gen : public XrdSysMutex
  //----------------------------------------------------------------------------
  {
  public:

    static std::string cInodeKey;

    vnode_gen()
    {
    }

    virtual ~vnode_gen()
    {
    }

    void init()
    {
      mNextInode=1;
      // load the stored next indoe
      if (kv::Instance().get(cInodeKey, mNextInode))
      {
        // otherwise store it for the first time
        inc();
      }
      eos_static_info("next-inode=%08lx", mNextInode);
    }

    uint64_t inc()
    {
      XrdSysMutexHelper mLock(this);
      if (0)
      {
        //sync - works for eosxd shared REDIS backend
        if (!kv::Instance().inc(cInodeKey, mNextInode))
        {
          return mNextInode;
        }
        else
        {
          // throw an exception
          throw std::runtime_error("REDIS backend failure - nextinode");
        }
      }
      else
      {
        //async - works for eosxd exclusive REDIS backend
        uint64_t s_inode = mNextInode + 1;
        kv::Instance().put(cInodeKey, s_inode);
        return mNextInode++;
      }
    }
  private:
    uint64_t mNextInode;
  } ;

  //----------------------------------------------------------------------------

  class vmap : public std::map<fuse_ino_t, fuse_ino_t>, public XrdSysMutex
  //----------------------------------------------------------------------------
  {
  public:

    vmap()
    {
    }

    virtual ~vmap()
    {
    }
  } ;

  class pmap : public std::map<fuse_ino_t, shared_md> , public XrdSysMutex
  //----------------------------------------------------------------------------
  {
  public:

    pmap()
    {
    }

    virtual ~pmap()
    {
    }
  } ;

  //----------------------------------------------------------------------------
  metad();

  virtual ~metad();

  void init();

  shared_md lookup(fuse_req_t req,
                   fuse_ino_t parent,
                   const char* name);

  int forget(fuse_req_t req,
             fuse_ino_t ino,
             int nlookup);

  shared_md get(fuse_req_t req,
                fuse_ino_t ino);

  uint64_t insert(fuse_req_t req,
                  shared_md md);

  void update(fuse_req_t req,
              shared_md md);

  void add(shared_md pmd, shared_md md);
  void remove(shared_md pmd, shared_md md);
  void mv(shared_md p1md, shared_md p2md, shared_md md, std::string newname);

  void mdcflush(); // thread pushing into md cache

  class mdstat
  {
  public:

    mdstat()
    {
      reset();
    }

    virtual ~mdstat()
    {
    }

    void reset()
    {
      _inodes.store(0, std::memory_order_seq_cst);
      _inodes_ever.store(0, std::memory_order_seq_cst);
      _inodes_deleted.store(0, std::memory_order_seq_cst);
      _inodes_deleted_ever.store(0, std::memory_order_seq_cst);
      _inodes_backlog.store(0, std::memory_order_seq_cst);
    }

    void inodes_inc()
    {
      _inodes.fetch_add(1, std::memory_order_seq_cst);
    }

    void inodes_ever_inc()
    {
      _inodes_ever.fetch_add(1, std::memory_order_seq_cst);
    }

    void inodes_dec()
    {
      _inodes.fetch_sub(1, std::memory_order_seq_cst);
    }

    void inodes_deleted_inc()
    {
      _inodes_deleted.fetch_add(1, std::memory_order_seq_cst);
    }

    void inodes_deleted_ever_inc()
    {
      _inodes_deleted_ever.fetch_add(1, std::memory_order_seq_cst);
    }

    void inodes_deleted_dec()
    {
      _inodes_deleted.fetch_sub(1, std::memory_order_seq_cst);
    }

    void inodes_backlog_store(ssize_t n)
    {
      _inodes_backlog.store(n, std::memory_order_seq_cst);
    }

    ssize_t inodes()
    {
      return _inodes.load();
    }

    ssize_t inodes_ever()
    {
      return _inodes_ever.load();
    }

    ssize_t inodes_deleted()
    {
      return _inodes_deleted.load();
      ;
    }

    ssize_t inodes_deleted_ever()
    {
      return _inodes_deleted_ever.load();
    }

    ssize_t inodes_backlog()
    {
      return _inodes_backlog.load();
    }

  private:
    std::atomic<ssize_t> _inodes;
    std::atomic<ssize_t> _inodes_deleted;
    std::atomic<ssize_t> _inodes_backlog;
    std::atomic<ssize_t> _inodes_ever;
    std::atomic<ssize_t> _inodes_deleted_ever;
  } ;

  mdstat& stats()
  {
    return stat;
  }

private:

  pmap mdmap;
  vmap inomap;
  mdstat stat;

  vnode_gen next_ino;

  XrdSysCondVar mdflush;
  std::set<uint64_t> mdqueue;

  size_t mdqueue_max_backlog;


} ;
#endif /* FUSE_MD_HH_ */
