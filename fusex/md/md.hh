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
#include <fcntl.h>
#include "llfusexx.hh"
#include "fusex/fusex.pb.h"
#include "kv/kv.hh"
#include "backend/backend.hh"
#include "common/Logging.hh"
#include "common/RWMutex.hh"
#include "XrdSys/XrdSysPthread.hh"
#include <memory>
#include <map>
#include <set>
#include <deque>
#include <vector>
#include <atomic>
#include <exception>
#include <stdexcept>
#include <zmq.hpp>

class metad
{
public:

  class mdx : public eos::fusex::md
  //----------------------------------------------------------------------------
  {
  public:

    // local operations

    enum md_op
    {
      ADD, MV, UPDATE, RM, SETSIZE, LSTORE, NONE
    } ;

    mdx() : mSync(0)
    {
      setop_add();
      lookup_cnt = 0;
      lock_remote = true;
      cap_count_reset();
    }

    mdx(fuse_ino_t ino) : mSync(0)
    {
      set_id(ino);
      setop_add();
      lookup_cnt = 0;
      lock_remote = true;
      cap_count_reset();
    }

    mdx& operator=(eos::fusex::md other)
    {
      (*((eos::fusex::md*)(this))) = other;
      return *this;
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
      op = RM;
    }

    void setop_add()
    {
      op = ADD;
    }

    void setop_setsize()
    {
      op = SETSIZE;
    }

    void setop_localstore()
    {
      op = LSTORE;
    }

    void setop_update()
    {
      op = UPDATE;
    }

    void setop_none()
    {
      op = NONE;
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
      return (op == RM) ;
    }

    void set_lock_remote()
    {
      lock_remote = true;
    }

    void set_lock_local()
    {
      lock_remote = false;
    }

    bool locks_remote()
    {
      return lock_remote;
    }

    void cap_inc()
    {
      // requires to have Lock outside
      cap_cnt++;
    }

    void cap_dec()
    {
      // requires to have Lock outside
      cap_cnt--;
    }

    void cap_count_reset()
    {
      cap_cnt = 0;
    }

    int cap_count()
    {
      return cap_cnt;
    }

    std::vector<struct flock> &LockTable()
    {
      return locktable;
    }

    int WaitSync(int ms)
    {
      return mSync.WaitMS(ms);
    }

    void Signal()
    {
      mSync.Signal();
    }
  private:
    XrdSysMutex mLock;
    XrdSysCondVar mSync;
    md_op op;
    int lookup_cnt;
    int cap_cnt;
    bool lock_remote;
    bool refresh;
    std::vector<struct flock> locktable;

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

  class vmap
  //----------------------------------------------------------------------------
  {
  public:

    vmap()
    {
    }

    virtual ~vmap()
    {
    }

    void insert(fuse_ino_t a, fuse_ino_t b);

    std::string dump();

    void erase_fwd(fuse_ino_t lookup);
    void erase_bwd(fuse_ino_t lookup);
    

    fuse_ino_t forward(fuse_ino_t lookup);
    fuse_ino_t backward(fuse_ino_t lookup);
  
  private:
    std::map<fuse_ino_t, fuse_ino_t> fwd_map; // forward map points from remote to local inode
    std::map<fuse_ino_t, fuse_ino_t> bwd_map; // backward map points from local remote inode

    XrdSysMutex mMutex;
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

  void init(backend* _mdbackend);

  shared_md load_from_kv(fuse_ino_t ino);
  void load_mapping_from_kv(fuse_ino_t ino);
  
  bool map_children_to_local(shared_md md);


  shared_md lookup(fuse_req_t req,
                   fuse_ino_t parent,
                   const char* name);

  int forget(fuse_req_t req,
             fuse_ino_t ino,
             int nlookup);

  shared_md get(fuse_req_t req,
                fuse_ino_t ino,
                bool listing=false,
                shared_md pmd = 0 ,
                const char* name = 0,
                bool readdir=false
                );

  uint64_t insert(fuse_req_t req,
                  shared_md md,
                  std::string authid);

  int wait_flush(fuse_req_t req,
                 shared_md md);

  void update(fuse_req_t req,
              shared_md md,
              std::string authid, 
	      bool localstore=false);

  void add(shared_md pmd, shared_md md, std::string authid);
  void remove(shared_md pmd, shared_md md, std::string authid, bool upstream=true);
  void mv(shared_md p1md, shared_md p2md, shared_md md, std::string newname,
          std::string authid1, std::string authid2);

  std::string dump_md(shared_md md);
  std::string dump_md(eos::fusex::md& md);
  std::string dump_container(eos::fusex::container & cont);

  uint64_t apply(fuse_req_t req, eos::fusex::container& cont, bool listing);

  int getlk(fuse_req_t req, shared_md md, struct flock* lock);
  int setlk(fuse_req_t req, shared_md md, struct flock* lock, int sleep);

  bool should_terminate()
  {
    return mdterminate.load();
  } // check if threads should terminate 

  void terminate()
  {
    mdterminate.store(true, std::memory_order_seq_cst);
  } // indicate to terminate

  void mdcflush(); // thread pushing into md cache

  void mdcommunicate(); // thread interacting with the MGM for meta data

  int connect(std::string zmqtarget, std::string zmqidentity, std::string zmqname, std::string zmqclienthost, std::string zmqclientuuid);

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

  vmap& vmaps()
  {
    return inomap;
  }

  void
  reset_cap_count(uint64_t ino)
  {
    XrdSysMutexHelper mLock(mdmap);
    auto item = mdmap.find(ino);
    if ( item != mdmap.end() )
    {
      XrdSysMutexHelper iLock(item->second->Locker());
      item->second->cap_count_reset();
      eos_static_err("reset cap counter for ino=%lx", ino);
    }
    else
    {
      eos_static_err("no cap counter change for ino=%lx", ino);
    }
  }

  void
  decrease_cap(uint64_t ino)
  {
    XrdSysMutexHelper mLock(mdmap);
    auto item = mdmap.find(ino);
    if ( item != mdmap.end() )
    {
      XrdSysMutexHelper iLock(item->second->Locker());
      item->second->cap_dec();
      eos_static_err("decrease cap counter for ino=%lx", ino);
    }
    else
    {
      eos_static_err("no cap counter change for ino=%lx", ino);
    }
  }

  void
  increase_cap(uint64_t ino)
  {
    auto item = mdmap.find(ino);
    if ( item != mdmap.end() )
    {
      //      XrdSysMutexHelper iLock(item->second->Locker());
      item->second->cap_inc();
      eos_static_err("increase cap counter for ino=%lx", ino);
    }
    else
    {
      eos_static_err("no cap counter change for ino=%lx", ino);
    }
  }

  std::string get_clientuuid() const
  {
    return zmq_clientuuid;
  }

  class flushentry
  {
  public:

    flushentry(const std::string& aid, mdx::md_op o) : _authid(aid), _op(o)
    {
    };

    ~flushentry()
    {
    }

    std::string authid() const
    {
      return _authid;
    }

    mdx::md_op op() const
    {
      return _op;
    }

    static std::deque<flushentry> merge(std::deque<flushentry>& f)
    {
      return f;
    }

    static std::string dump(std::deque<flushentry>& e)
    {
      std::string out;
      for (auto it = e.begin(); it != e.end(); ++it)
      {
        char line[1024];
        snprintf(line, sizeof (line), "\nauthid=%s op=%d", it->authid().c_str(), (int) it->op());
        out += line;
      }
      return out;
    }

  private:
    std::string _authid;
    mdx::md_op _op;
  } ;

  typedef std::deque<flushentry> flushentry_set_t;

private:
  pmap mdmap;
  vmap inomap;
  mdstat stat;

  vnode_gen next_ino;

  XrdSysCondVar mdflush;

  std::map<uint64_t, flushentry_set_t> mdqueue; // inode => flushenty

  size_t mdqueue_max_backlog;

  // ZMQ objects
  zmq::context_t z_ctx;
  zmq::socket_t z_socket;
  std::string zmq_target;
  std::string zmq_identity;
  std::string zmq_name;
  std::string zmq_clienthost;
  std::string zmq_clientuuid;

  std::atomic<bool> mdterminate;

  backend* mdbackend;
} ;
#endif /* FUSE_MD_HH_ */
