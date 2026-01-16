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
#include "backend/backend.hh"
#include "common/ConcurrentQueue.hh"
#include "common/Logging.hh"
#include "common/RWMutex.hh"
#include "common/AssistedThread.hh"
#include "common/SymKeys.hh"
#include "kv/kv.hh"
#include "misc/FuseId.hh"
#include <XrdSys/XrdSysPthread.hh>
#include <memory>
#include <map>
#include <set>
#include <deque>
#include <vector>
#include <atomic>
#include <string>
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

  class mdx
  //----------------------------------------------------------------------------
  {
  public:

    // local operations

    enum md_op {
      ADD, UPDATE, RM, SETSIZE, LSTORE, NONE
    };

    mdx() : mSync(1)
    {
      setop_add();
      lookup_cnt.store(0, std::memory_order_seq_cst);
      opendir_cnt.store(0, std::memory_order_seq_cst);
      lock_remote = true;
      cap_count_set(0);
      clear_refresh();
      rmrf = false;
      inline_size = 0;
      _lru_prev.store(0, std::memory_order_seq_cst);
      _lru_next.store(0, std::memory_order_seq_cst);
    }

    mdx(fuse_ino_t ino) : mdx()
    {
      proto.set_id(ino);
    }

    // make sure nobody copies us as we some members that will
    // likely lead to problems if they are copied, e.g. XrdSysMutex
    mdx(const mdx& other) = delete;
    mdx& operator=(const mdx& other) = delete;

    void UpdateProtoFrom(const eos::fusex::md &src) {
      // Typically this method is used to copy a metadata entry received
      // from the remote to our member protobuf object: However id and pid
      // are usually 0 (unset) in src since they are only meaningful locally.
      // The caller will explictly reset them in our protobuf. However they
      // are accessed in many places without lock. Furthermore, a protobuf
      // copy (with = operator) or CopyFrom() will zero all current values
      // before merging the new values, leaving race where 0 may be read.
      // For now we avoid resettig id and pid to 0 here and explictly copy
      // all the other elements.

      if (src.id() != 0 && proto.id() != src.id()) {
        proto.set_id(src.id());
      }
      if (src.pid() != 0 && proto.pid() != src.pid()) {
        proto.set_pid(src.pid());
      }

      proto.set_ctime(src.ctime());
      proto.set_ctime_ns(src.ctime_ns());
      proto.set_mtime(src.mtime());
      proto.set_mtime_ns(src.mtime_ns());
      proto.set_atime(src.atime());
      proto.set_atime_ns(src.atime_ns());
      proto.set_btime(src.btime());
      proto.set_btime_ns(src.btime_ns());
      proto.set_ttime(src.ttime());
      proto.set_ttime_ns(src.ttime_ns());
      proto.set_pmtime(src.pmtime());
      proto.set_pmtime_ns(src.pmtime_ns());
      proto.set_size(src.size());
      proto.set_uid(src.uid());
      proto.set_gid(src.gid());
      proto.set_mode(src.mode());
      proto.set_nlink(src.nlink());
      proto.set_name(src.name());
      proto.set_target(src.target());
      proto.set_authid(src.authid());
      proto.set_clientid(src.clientid());
      proto.set_clientuuid(src.clientuuid());
      proto.set_clock(src.clock());
      proto.set_reqid(src.reqid());
      proto.set_md_ino(src.md_ino());
      proto.set_md_pino(src.md_pino());
      proto.set_operation(src.operation());
      proto.set_type(src.type());
      proto.set_err(src.err());
      *proto.mutable_attr() = src.attr();
      *proto.mutable_children() = src.children();
      *proto.mutable_capability() = src.capability();
      proto.set_implied_authid(src.implied_authid());
      *proto.mutable_flock() = src.flock();
      proto.set_nchildren(src.nchildren());
      proto.set_fullpath(src.fullpath());
      proto.set_pt_mtime(src.pt_mtime());
      proto.set_pt_mtime_ns(src.pt_mtime_ns());
      proto.set_creator(src.creator());
      proto.set_mv_authid(src.mv_authid());
      proto.set_bc_time(src.bc_time());
      proto.set_opflags(src.opflags());
    }

    virtual ~mdx() { }

    XrdSysMutex& Locker()
    {
      return mLock;
    }

    void convert(fuse_entry_param& e, double lifetime = 180.0);
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
      eos_static_info("ino=%16x lookup=%d => lookup=%d", (*this)()->id(), prevLookup,
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

    void opendir_inc()
    {
      // atomic operation, no need to lock before calling
      int prevOpendir = opendir_cnt.fetch_add(1, std::memory_order_seq_cst);
      eos_static_info("ino=%16x opendir=%d => opendir=%d", (*this)()->id(),
                      prevOpendir,
                      prevOpendir + 1);
    }

    bool opendir_dec(int n)
    {
      // atomic operation, no need to lock before calling
      int prevOpendir = opendir_cnt.fetch_sub(n, std::memory_order_seq_cst);

      if (prevOpendir - n > 0) {
        return false;
      }

      return true;
    }

    int opendir_is()
    {
      return opendir_cnt.load();
    }

    md_op getop() const
    {
      return op;
    }

    bool deleted() const
    {
      return (op == RM);
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

    void cap_count_set(uint64_t cnt)
    {
      cap_cnt.store(cnt, std::memory_order_seq_cst);
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
      snprintf(s, sizeof(s), "%lx:%lu.%lu:%lu", (unsigned long)(*this)()->id(),
               (unsigned long)(*this)()->mtime(),
               (unsigned long)(*this)()->mtime_ns(),
               (unsigned long)(*this)()->size());
      return s;
    }

    std::map<std::string, uint64_t >& get_todelete()
    {
      return todelete;
    }

    size_t sizeTS()
    {
      XrdSysMutexHelper lLock(mLock);
      return (*this)()->size();
    }

    std::map<std::string, uint64_t>& local_children()
    {
      return _local_children;
    }

    std::set<std::string>& local_enoent()
    {
      return _local_enoent;
    }

    void store_fullpath(const std::string& pfp, const std::string& name)
    {
      std::string fullpath = pfp;

      if (fullpath.length() && fullpath.back() != '/') {
        fullpath += "/";
      }

      fullpath += name;
      (*this)()->set_fullpath(fullpath.c_str());
    }

    uint64_t inlinesize()
    {
      return inline_size;
    }

    bool obfuscate()
    {
      auto xattr = proto.attr();

      if (xattr.count("sys.file.obfuscate")) {
        return (xattr["sys.file.obfuscate"] == "1");
      } else {
        return false;
      }
    }

    void set_obfuscate_key(const std::string& key, bool encryption = false,
                           std::string encryptionhash = "")
    {
      (*proto.mutable_attr())["user.obfuscate.key"] = key;

      if (encryption) {
        (*proto.mutable_attr())["user.encrypted"] = "1";
        (*proto.mutable_attr())["user.encrypted.fp"] = encryptionhash;
      }
    }

    std::string keyprint16(const std::string& key1, const std::string& key2)
    {
      std::hash<std::string> secrethash;
      return std::to_string(secrethash(key1 + key2) % 65536);
    }

    bool wrong_key(const std::string& keyprint)
    {
      auto xattr = proto.attr();

      if (!xattr.count("user.encrypted.fp")) {
        return false;
      }

      return (xattr["user.encrypted.fp"] != keyprint);
    }

    bool encrypted()
    {
      auto xattr = proto.attr();
      return (xattr.count("user.encrypted"));
    }

    std::string obfuscate_key()
    {
      auto xattr = proto.attr();

      if (xattr.count("user.obfuscate.key")) {
        return xattr["user.obfuscate.key"];
      } else {
        return "";
      }
    }

    void set_inlinesize(uint64_t inlinesize)
    {
      inline_size = inlinesize;
    }

    void force_refresh()
    {
      refresh.store(1, std::memory_order_seq_cst);
    }

    bool needs_refresh() const
    {
      return refresh.load() ? true : false;
    }

    void clear_refresh()
    {
      refresh.store(0, std::memory_order_seq_cst);
    }

    void set_lru_prev(uint64_t prev)
    {
      _lru_prev.store(prev, std::memory_order_seq_cst);
    }

    void set_lru_next(uint64_t next)
    {
      _lru_next.store(next, std::memory_order_seq_cst);
    }

    uint64_t lru_prev() const
    {
      return _lru_prev.load();
    }
    uint64_t lru_next() const
    {
      return _lru_next.load();
    }

    void set_rmrf()
    {
      rmrf = true;
    }

    bool get_rmrf() const
    {
      return rmrf;
    }

    void unset_rmrf()
    {
      rmrf = false;
    }

    int state_serialize(std::string& out);
    int state_deserialize(std::string& out);

    eos::fusex::md* operator()()
    {
      return &proto;
    }

    uint64_t pidTS()
    {
      XrdSysMutexHelper cLock(mLock);
      return proto.pid();
    }

    uint64_t pid()
    {
      return proto.pid();
    }

  private:
    XrdSysMutex mLock;
    XrdSysCondVar mSync;
    std::atomic<md_op> op;
    std::atomic<int> lookup_cnt;
    std::atomic<int> cap_cnt;
    std::atomic<int> opendir_cnt;
    bool lock_remote;
    std::atomic<int> refresh;
    bool rmrf;
    uint64_t inline_size;
    std::vector<struct flock> locktable;
    std::map<std::string, uint64_t> todelete;
    std::map<std::string, uint64_t> _local_children;
    std::set<std::string> _local_enoent;

    std::atomic<uint64_t> _lru_prev;
    std::atomic<uint64_t> _lru_next;
    eos::fusex::md proto;

    struct hmac_t {
      std::string key;
      std::string hmac;
    };

    hmac_t hmac;
  };

  typedef std::shared_ptr<mdx> shared_md;

  // used to provide serialisaiton during get() for an inode
  XrdSysMutex mGetMapLock;
  std::map<uint64_t, std::shared_ptr<XrdSysCondVar>> mGetMap;
  struct GetLockHelper_s {
      GetLockHelper_s(metad *meta, fuse_ino_t ino) : meta_(meta), ino_(ino) { }
      GetLockHelper_s( const GetLockHelper_s& ) = delete;
      GetLockHelper_s &operator=( const GetLockHelper_s& ) = delete;
      ~GetLockHelper_s() { UnLock(); }

      void UnLock() {
        if (meta_) meta_->GetMtxRelease(*this);
      }

      fuse_ino_t InoAndClear() {
        fuse_ino_t ino = ino_;
        meta_ = 0;
        ino_  = 0;
        return ino;
      }

      metad *meta_;
      fuse_ino_t ino_;
  };

  GetLockHelper_s GetMtxAcquire(fuse_ino_t ino) {
    if (!ino) return GetLockHelper_s(nullptr, 0);
    std::shared_ptr<XrdSysCondVar> cv;
    do {
      XrdSysCondVarHelper lkcv;
      {
        XrdSysMutexHelper lk(mGetMapLock);
        auto it = mGetMap.find(ino);
        if (it == mGetMap.end()) {
          if (!cv) cv = std::make_shared<XrdSysCondVar>(0);
          mGetMap[ino] = cv;
          break;
        }
        cv = it->second;
        lkcv.Lock(cv.get());
      }
      cv->Wait();
    } while(1);
    return GetLockHelper_s(this, ino);
  }

  void GetMtxRelease(GetLockHelper_s &lh) {
    fuse_ino_t ino = lh.InoAndClear();
    XrdSysMutexHelper lk(mGetMapLock);
    auto it = mGetMap.find(ino);
    if (it == mGetMap.end()) return;
    auto cv = it->second;
    mGetMap.erase(it);
    XrdSysCondVarHelper lkcv(*cv);
    cv->Broadcast();
  }

  //----------------------------------------------------------------------------

  class vmap
  //----------------------------------------------------------------------------
  {
  public:

    vmap() { }

    virtual ~vmap() { }

    void insert(fuse_ino_t a, fuse_ino_t b);

    std::string dump();

    void erase_fwd(fuse_ino_t lookup);
    void erase_bwd(fuse_ino_t lookup);


    fuse_ino_t forward(fuse_ino_t lookup);
    fuse_ino_t backward(fuse_ino_t lookup);

    void clear()
    {
      XrdSysMutexHelper mLock(mMutex);
      fwd_map.clear();
      bwd_map.clear();
    }

    size_t size()
    {
      XrdSysMutexHelper mLock(mMutex);
      return fwd_map.size();
    }

  private:
    std::map<fuse_ino_t, fuse_ino_t>
    fwd_map; // forward map points from remote to local inode
    std::map<fuse_ino_t, fuse_ino_t>
    bwd_map; // backward map points from local remote inode

    XrdSysMutex mMutex;
  };

  class pmap : public std::map<fuse_ino_t, shared_md>, public XrdSysMutex
  //----------------------------------------------------------------------------
  {
  public:

    pmap()
    {
      lru_first = 0;
      lru_last = 0;
      store = 0 ;
    }

    void init(kv* _kv)
    {
      store = _kv;
    }

    virtual ~pmap() { }

    // TS stands for "thread-safe"

    size_t sizeTS();

    bool retrieveOrCreateTS(fuse_ino_t ino, shared_md& ret);
    bool retrieveTS(fuse_ino_t ino, shared_md& ret);
    bool retrieve(fuse_ino_t ino, shared_md& ret);
    void insertTS(fuse_ino_t ino, shared_md& md);
    bool eraseTS(fuse_ino_t ino);
    void retrieveWithParentTS(fuse_ino_t ino, shared_md& md, shared_md& pmd,
                              std::string& md_name);

    uint64_t lru_oldest() const;
    uint64_t lru_newest() const;
    void lru_add(fuse_ino_t ino, shared_md md);
    void lru_remove(fuse_ino_t ino);
    void lru_update(fuse_ino_t ino, shared_md md);
    void lru_dump(bool force = false);
    void lru_reset();

    int swap_out(fuse_ino_t ino, shared_md md);
    int swap_in(fuse_ino_t ino, shared_md md);
    int swap_rm(fuse_ino_t ino);

  private:
    uint64_t lru_first;
    uint64_t lru_last;
    kv* store;
  };

  //----------------------------------------------------------------------------
  metad();

  virtual ~metad();

  void init(backend* _mdbackend);

  bool map_children_to_local(shared_md md);


  shared_md lookup(fuse_req_t req,
                   fuse_ino_t parent,
                   const char* name);

  shared_md lookup_ll(fuse_req_t req,
                      fuse_ino_t parent,
                      const char* name);

  int forget(fuse_req_t req,
             fuse_ino_t ino,
             int nlookup);

  void wait_backlog(shared_md md);

  void wait_upstream(fuse_req_t req,
                     fuse_ino_t ino);

  shared_md getlocal(fuse_req_t req,
                     fuse_ino_t ino);

  std::string getpath(fuse_ino_t ino);

  shared_md get(fuse_req_t req,
                fuse_ino_t ino,
                const std::string authid = "",
                bool listing = false,
                shared_md pmd = 0,
                const char* name = 0,
                bool readdir = false
               );

  uint64_t insert(shared_md md,
                  std::string authid);

  int wait_flush(fuse_req_t req,
                 shared_md md);

  bool has_flush(fuse_ino_t ino);

  void update(fuse_req_t req,
              shared_md md,
              std::string authid,
              bool localstore = false);

  void update(fuse_id id,
              shared_md md,
              std::string authid,
              bool localstore = false);

  void add(fuse_req_t req, shared_md pmd, shared_md md, std::string authid,
           bool localstore = false);
  int add_sync(fuse_req_t req, shared_md pmd, shared_md md, std::string authid);
  int begin_flush(fuse_req_t req, shared_md md, std::string authid);
  int end_flush(fuse_req_t req, shared_md md, std::string authid);

  void remove(fuse_req_t req, shared_md pmd, shared_md md, std::string authid,
              bool upstream = true, bool norecycle = false);
  void mv(fuse_req_t req, shared_md p1md, shared_md p2md, shared_md md,
          std::string newname,
          std::string authid1, std::string authid2);

  int rmrf(fuse_req_t req, shared_md md);

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

  void mdcallback(ThreadAssistant&
                  assistant); // thread applying MGM callback responses

  void mdstackfree(ThreadAssistant&
                   assistant); // thread removing stacked inodes

  int connect(std::string zmqtarget, std::string zmqidentity = "",
              std::string zmqname = "", std::string zmqclienthost = "",
              std::string zmqclientuuid = "");

  int calculateDepth(shared_md md);

  std::string calculateLocalPath(shared_md md);

  void cleanup(shared_md md);
  void cleanup(fuse_ino_t ino);

  class mdstat
  {
  public:

    mdstat()
    {
      reset();
    }

    virtual ~mdstat() { }

    void reset()
    {
      _inodes.store(0, std::memory_order_seq_cst);
      _inodes_stacked.store(0, std::memory_order_seq_cst);
      _inodes_ever.store(0, std::memory_order_seq_cst);
      _inodes_deleted.store(0, std::memory_order_seq_cst);
      _inodes_deleted_ever.store(0, std::memory_order_seq_cst);
      _inodes_backlog.store(0, std::memory_order_seq_cst);
      _lru_resets.store(0, std::memory_order_seq_cst);
    }

    void inodes_inc()
    {
      _inodes.fetch_add(1, std::memory_order_seq_cst);
    }

    void inodes_stacked_inc()
    {
      _inodes_stacked.fetch_add(1, std::memory_order_seq_cst);
    }

    void inodes_ever_inc()
    {
      _inodes_ever.fetch_add(1, std::memory_order_seq_cst);
    }

    void inodes_dec()
    {
      _inodes.fetch_sub(1, std::memory_order_seq_cst);
    }

    void inodes_stacked_dec()
    {
      _inodes_stacked.fetch_sub(1, std::memory_order_seq_cst);
    }

    void inodes_deleted_inc()
    {
      _inodes_deleted.fetch_add(1, std::memory_order_seq_cst);
    }

    void inodes_deleted_ever_inc()
    {
      _inodes_deleted_ever.fetch_add(1, std::memory_order_seq_cst);
    }

    void lru_resets_inc()
    {
      _lru_resets.fetch_add(1, std::memory_order_seq_cst);
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

    ssize_t inodes_stacked()
    {
      return _inodes_stacked.load();
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

    ssize_t lru_resets()
    {
      return _lru_resets.load();
    }

  private:
    std::atomic<ssize_t> _inodes;
    std::atomic<ssize_t> _inodes_stacked;
    std::atomic<ssize_t> _inodes_deleted;
    std::atomic<ssize_t> _inodes_backlog;
    std::atomic<ssize_t> _inodes_ever;
    std::atomic<ssize_t> _inodes_deleted_ever;
    std::atomic<ssize_t> _lru_resets;
  };

  mdstat& stats()
  {
    return stat;
  }

  vmap& vmaps()
  {
    return inomap;
  }

  void mdreset()
  {
    XrdSysMutexHelper lock(mdmap);
    shared_md md1 = mdmap[1];
    (*md1)()->set_type((*md1)()->MD);
    md1->force_refresh();
    mdmap.clear();
    mdmap[1] = md1;
    uint64_t i_root = inomap.backward(1);
    inomap.clear();
    inomap.insert(i_root, 1);
  }

  void lrureset()
  {
    stat.lru_resets_inc();
    mdmap.lru_reset();
  }

  void
  set_cap_count(uint64_t ino, uint64_t cnt)
  {
    shared_md md;

    if (!mdmap.retrieveTS(ino, md)) {
      eos_static_info("no cap counter change for ino=%lx", ino);
      return;
    }

    md->cap_count_set(cnt);
    eos_static_debug("set cap counter for ino=%lx cnt=%lx", ino, cnt);
  }

  std::string get_clientuuid() const
  {
    return zmq_clientuuid;
  }

  class mdqwaiter
  {
  public:

    mdqwaiter() : done_(false) { }

    void wait()
    {
      std::unique_lock<std::mutex> lck(mtx_);
      cv_.wait(lck, [this] { return this->done_; });
    }

    void notify()
    {
      std::unique_lock<std::mutex> lck(mtx_);
      done_ = true;
      lck.unlock();
      cv_.notify_one();
    }

    bool done() const { return done_; }

  private:
    bool done_;
    std::mutex mtx_;
    std::condition_variable cv_;
  };

  class flushentry
  {
  public:

    flushentry(const uint64_t id, const std::string& aid, mdx::md_op o,
               fuse_req_t req = 0) : _id(id), _authid(aid), _op(o)
    {
      if (req) {
        _fuse_id = fuse_id(req);
      }
    };

    flushentry(const uint64_t id, const std::string& aid, mdx::md_op o,
               fuse_id fuseid) : _id(id), _authid(aid), _op(o)
    {
      _fuse_id = fuseid;
    };

    ~flushentry()
    {
      for(auto &w: _waiters)
      {
        w->notify();
      }
    }

    std::string authid() const
    {
      return _authid;
    }

    void updateauthid(const std::string &aid)
    {
      _authid = aid;
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

    void bind()
    {
      _fuse_id.bind();
    }

    void registerwaiter(mdqwaiter *w)
    {
      _waiters.push_back(w);
    }

    static std::deque<flushentry> merge(std::deque<flushentry>& f)
    {
      return f;
    }

    static std::string dump(flushentry& e)
    {
      std::string out;
      char line[1024];
      snprintf(line, sizeof(line), "authid=%s op=%d id=%lu uid=%u gid=%u pid=%u",
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
    std::vector<mdqwaiter*> _waiters;
  };

  typedef std::deque<flushentry> flushentry_set_t;

  void set_zmq_wants_to_connect(int val)
  {
    want_zmq_connect.store(val, std::memory_order_seq_cst);
  }

  int zmq_wants_to_connect()
  {
    return want_zmq_connect.load();
  }

  void set_is_visible(int val)
  {
    fusex_visible.store(val, std::memory_order_seq_cst);
  }

  int is_visible()
  {
    return fusex_visible.load();
  }

  bool should_flush_write_size()
  {
    XrdSysMutexHelper cLock(ConfigMutex);
    return writesizeflush;
  }

  std::string server_version()
  {
    XrdSysMutexHelper cLock(ConfigMutex);
    return serverversion;
  }

  bool supports_appname()
  {
    XrdSysMutexHelper cLock(ConfigMutex);
    return appname;
  }

  bool supports_mdquery()
  {
    XrdSysMutexHelper cLock(ConfigMutex);
    return mdquery;
  }

  bool supports_hideversion()
  {
    XrdSysMutexHelper cLock(ConfigMutex);
    return hideversion;
  }

  std::atomic<time_t> last_heartbeat; // timestamp of the last heartbeat sent

private:

  // Lock _two_ md objects in the given order.

  class MdLocker
  {
  public:

    MdLocker(shared_md& m1, shared_md& m2, bool ordr)
      : md1(m1), md2(m2), order(ordr)
    {
      if (order) {
        md1->Locker().Lock();
        md2->Locker().Lock();
      } else {
        md2->Locker().Lock();
        md1->Locker().Lock();
      }
    }

    ~MdLocker()
    {
      if (order) {
        md2->Locker().UnLock();
        md1->Locker().UnLock();
      } else {
        md1->Locker().UnLock();
        md2->Locker().UnLock();
      }
    }

  private:
    shared_md md1;
    shared_md md2;
    bool order; // true if lock order is md1 -> md2, false if md2 -> md1
  };

  bool determineLockOrder(shared_md md1, shared_md md2);
  bool isChild(shared_md potentialChild, fuse_ino_t parentId);

  pmap mdmap;
  vmap inomap;
  mdstat stat;

  // broadcasted config
  XrdSysMutex ConfigMutex;
  bool dentrymessaging;
  bool writesizeflush;
  bool appname;
  bool mdquery;
  bool hideversion;
  std::atomic<int>  hb_interval;

  std::string serverversion;

  XrdSysCondVar mdflush;

  std::map<uint64_t, size_t> mdqueue; // inode, counter of mds to flush
  std::deque<flushentry> mdflushqueue; // linear queue with all entries to flush
  std::deque<const uint64_t*> mdbacklogqueue; // to give order to waiters for mdqueue capacity
  uint64_t mdqueue_current{0}; // ino of entry currently being flushed

  typedef std::shared_ptr<eos::fusex::response> shared_response;
  std::deque<shared_response> mCbQueue; // queue will callbacks
  XrdSysCondVar mCb; // condition variable for queue
  std::string mCbTrace; // stack trace response
  std::string mCbLog; // logging response

  size_t mdqueue_max_backlog;

  // ZMQ objects
  zmq::context_t* z_ctx;
  zmq::socket_t* z_socket;
  std::string zmq_target;
  std::string zmq_identity;
  std::string zmq_name;
  std::string zmq_clienthost;
  std::string zmq_clientuuid;
  std::mutex zmq_socket_mutex;
  std::atomic<int> want_zmq_connect;
  std::atomic<int> fusex_visible;
  backend* mdbackend;


};

#endif /* FUSE_MD_HH_ */
