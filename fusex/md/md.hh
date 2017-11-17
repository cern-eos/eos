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
#include "md/InodeGenerator.hh"
#include "backend/backend.hh"
#include "common/Logging.hh"
#include "common/RWMutex.hh"
#include "misc/AssistedThread.hh"
#include "misc/FuseId.hh"
#include "XrdSys/XrdSysPthread.hh"
#include <memory>
#include <map>
#include <set>
#include <deque>
#include <vector>
#include <atomic>
#include <exception>
#include <stdexcept>
#include <sys/statvfs.h>

#ifdef HAVE_DEFAULT_ZMQ
#include <zmq.hpp>
#else
#include "utils/zmq.hpp"
#endif

class metad
{
public:

  class mdx : public eos::fusex::md
  //----------------------------------------------------------------------------
  {
  public:

    // local operations

    enum md_op {
      ADD, MV, UPDATE, RM, SETSIZE, LSTORE, NONE
    } ;

    mdx() : mSync(0)
    {
      setop_add();
      lookup_cnt.store(0, std::memory_order_seq_cst);
      lock_remote = true;
      cap_count_reset();
      refresh = false;
    }

    mdx(fuse_ino_t ino) : mdx()
    {
      set_id(ino);
    }

    mdx& operator=(const eos::fusex::md& other)
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

    void convert(fuse_entry_param& e);
    std::string dump();
    static std::string dump(struct fuse_entry_param& e);

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
      // atomic operation, no need to lock before calling
      int prevLookup = lookup_cnt.fetch_add(1, std::memory_order_seq_cst);
      eos_static_info("ino=%16x lookup=%d => lookup=%d", id(), prevLookup,
                      prevLookup + 1);
    }

    bool lookup_dec(int n)
    {
      // atomic operation, no need to lock before calling
      int prevLookup = lookup_cnt.fetch_sub(n, std::memory_order_seq_cst);

      if (prevLookup - n > 0) {
        return false;
      }

      return true;
    }

    int lookup_is()
    {
      return lookup_cnt.load();
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
      // atomic operation, no need to lock before calling
      cap_cnt.fetch_add(1, std::memory_order_seq_cst);
    }

    void cap_dec()
    {
      // atomic operation, no need to lock before calling
      cap_cnt.fetch_sub(1, std::memory_order_seq_cst);
    }

    void cap_count_reset()
    {
      cap_cnt.store(0, std::memory_order_seq_cst);
    }

    int cap_count()
    {
      return cap_cnt.load();
    }

    std::vector<struct flock>& LockTable()
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

    std::string Cookie()
    {
      char s[256];
      snprintf(s, sizeof(s), "%lx:%lu.%lu:%lu", (unsigned long) id(),
               (unsigned long) mtime(),
               (unsigned long) mtime_ns(),
               (unsigned long) size());
      return s;
    }

    std::set<std::string>& get_todelete()
    {
      return todelete;
    }

    std::map<std::string, uint64_t>& get_childrentomap()
    {
      return childrentomap;
    }

  private:
    XrdSysMutex mLock;
    XrdSysCondVar mSync;
    std::atomic<md_op> op;
    std::atomic<int> lookup_cnt;
    std::atomic<int> cap_cnt;
    bool lock_remote;
    bool refresh;
    std::vector<struct flock> locktable;
    std::set<std::string> todelete;
    std::map<std::string, uint64_t> childrentomap;
  } ;

  typedef std::shared_ptr<mdx> shared_md;

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
    std::map<fuse_ino_t, fuse_ino_t>
    fwd_map; // forward map points from remote to local inode
    std::map<fuse_ino_t, fuse_ino_t>
    bwd_map; // backward map points from local remote inode

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

    void retrieveOrCreateTS(fuse_ino_t ino, shared_md& ret)
    {
      XrdSysMutexHelper mLock(this);

      if (this->retrieve(ino, ret)) {
        return;
      }

      ret = std::make_shared<mdx>();

      if (ino) {
        (*this)[ino] = ret;
      }
    }

    // TS stands for "thread-safe"
    bool retrieveTS(fuse_ino_t ino, shared_md& ret)
    {
      XrdSysMutexHelper mLock(this);
      return this->retrieve(ino, ret);
    }

    bool retrieve(fuse_ino_t ino, shared_md& ret)
    {
      auto it = this->find(ino);

      if (it == this->end()) {
        return false;
      }

      ret = it->second;
      return true;
    }

    // TS stands for "thread-safe"
    void insertTS(fuse_ino_t ino, shared_md& md)
    {
      XrdSysMutexHelper mLock(this);
      (*this)[ino] = md;
    }

    // TS stands for "thread-safe"
    void eraseTS(fuse_ino_t ino)
    {
      XrdSysMutexHelper mLock(this);
      this->erase(ino);
    }

  } ;

  //----------------------------------------------------------------------------
  metad();

  virtual ~metad();

  void init(backend* _mdbackend);

  bool map_children_to_local(shared_md md);


  shared_md lookup(fuse_req_t req,
                   fuse_ino_t parent,
                   const char* name);

  int forget(fuse_req_t req,
             fuse_ino_t ino,
             int nlookup);

  shared_md get(fuse_req_t req,
                fuse_ino_t ino,
                const std::string authid = "",
                bool listing = false,
                shared_md pmd = 0 ,
                const char* name = 0,
                bool readdir = false
               );

  uint64_t insert(fuse_req_t req,
                  shared_md md,
                  std::string authid);

  int wait_flush(fuse_req_t req,
                 shared_md md);

  void update(fuse_req_t req,
              shared_md md,
              std::string authid,
              bool localstore = false);

  void add(fuse_req_t req, shared_md pmd, shared_md md, std::string authid,
           bool localstore = false);
  int add_sync(fuse_req_t req, shared_md pmd, shared_md md, std::string authid);
  int begin_flush(fuse_req_t req, shared_md md, std::string authid);
  int end_flush(fuse_req_t req, shared_md md, std::string authid);

  void remove(fuse_req_t req, shared_md pmd, shared_md md, std::string authid,
              bool upstream = true);
  void mv(fuse_req_t req, shared_md p1md, shared_md p2md, shared_md md,
          std::string newname,
          std::string authid1, std::string authid2);

  std::string dump_md(shared_md md, bool lock = true);
  std::string dump_md(eos::fusex::md& md);
  std::string dump_container(eos::fusex::container& cont);

  uint64_t apply(fuse_req_t req, eos::fusex::container& cont, bool listing);

  int getlk(fuse_req_t req, shared_md md, struct flock* lock);
  int setlk(fuse_req_t req, shared_md md, struct flock* lock, int sleep);

  int statvfs(fuse_req_t req, struct statvfs* svfs);

  void mdcflush(ThreadAssistant& assistant); // thread pushing into md cache

  void mdcommunicate(ThreadAssistant&
                     assistant); // thread interacting with the MGM for meta data

  int connect(std::string zmqtarget, std::string zmqidentity, std::string zmqname, std::string zmqclienthost, std::string zmqclientuuid);
  int calculateDepth(shared_md md);

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
    shared_md md;

    if (!mdmap.retrieveTS(ino, md)) {
      eos_static_err("no cap counter change for ino=%lx", ino);
      return;
    }

    XrdSysMutexHelper iLock(md->Locker());
    md->cap_count_reset();
    eos_static_err("reset cap counter for ino=%lx", ino);
  }

  void
  decrease_cap(uint64_t ino)
  {
    shared_md md;

    if (!mdmap.retrieveTS(ino, md)) {
      eos_static_info("no cap counter change for ino=%lx", ino);
      return;
    }

    md->cap_dec();
    eos_static_debug("decrease cap counter for ino=%lx", ino);
  }

  void
  increase_cap(uint64_t ino, bool lock = false)
  {
    shared_md md;

    if (!mdmap.retrieveTS(ino, md)) {
      eos_static_err("no cap counter change for ino=%lx", ino);
      return;
    }

    if (lock) {
      md->Locker().Lock();
    }

    md->cap_inc();

    if (lock) {
      md->Locker().UnLock();
    }

    eos_static_err("increase cap counter for ino=%lx", ino);
  }

  std::string get_clientuuid() const
  {
    return zmq_clientuuid;
  }

  class flushentry
  {
  public:
    flushentry(const uint64_t id, const std::string& aid, mdx::md_op o,
               fuse_req_t req = 0): _id(id), _authid(aid), _op(o)
    {
      if (req) {
        _fuse_id = fuse_id(req);
      }
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

    uint64_t id() const
    {
      return _id;
    }

    fuse_id get_fuse_id() const
    {
      return _fuse_id;
    }

    static std::deque<flushentry> merge(std::deque<flushentry>& f)
    {
      return f;
    }

    static std::string dump(flushentry& e)
    {
      std::string out;
      char line[1024];
      snprintf(line, sizeof(line), "authid=%1023s op=%d id=%lu uid=%u gid=%u pid=%u",
               e.authid().c_str(), (int) e.op(), e.id(), e.get_fuse_id().uid,
               e.get_fuse_id().gid, e.get_fuse_id().pid);
      out += line;
      return out;
    }

  private:
    uint64_t _id;
    std::string _authid;
    mdx::md_op _op;
    fuse_id _fuse_id;
  } ;

  typedef std::deque<flushentry> flushentry_set_t;

private:
  pmap mdmap;
  vmap inomap;
  mdstat stat;

  InodeGenerator next_ino;

  XrdSysCondVar mdflush;

  std::map<uint64_t, size_t> mdqueue;  // inode, counter of mds to flush
  std::deque<flushentry> mdflushqueue; // linear queue with all entries to flush

  size_t mdqueue_max_backlog;

  // ZMQ objects
  zmq::context_t* z_ctx;
  zmq::socket_t* z_socket;
  std::string zmq_target;
  std::string zmq_identity;
  std::string zmq_name;
  std::string zmq_clienthost;
  std::string zmq_clientuuid;

  backend* mdbackend;
} ;
#endif /* FUSE_MD_HH_ */
