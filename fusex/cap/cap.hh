//------------------------------------------------------------------------------
//! @file cap.hh
//! @author Andreas-Joachim Peters CERN
//! @brief cap handling class
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

#ifndef FUSE_CAP_HH_
#define FUSE_CAP_HH_

#include <sys/stat.h>
#include <sys/types.h>
#include "llfusexx.hh"
#include "backend/backend.hh"
#include "md/md.hh"
#include "fusex/fusex.pb.h"

#include "XrdSys/XrdSysPthread.hh"
#include <memory>
#include <map>


// extension to permission capabilities
#define D_OK 8     // delete
#define M_OK 16    // chmod
#define C_OK 32    // chown
#define SA_OK 64   // set xattr
#define U_OK 128   // can update
#define SU_OK 256  // set utimes

class cap
{
public:

  //----------------------------------------------------------------------------

  class quotax : public eos::fusex::quota
  {
  public:

    virtual ~quotax() { }

    XrdSysMutex& Locker()
    {
      return mLock;
    }

    quotax& operator=(eos::fusex::quota other)
    {
      (*((eos::fusex::quota*)(this))) = other;
      updated();
      return *this;
    }

    int writer() { return writer_cnt; }
    void updated() { last_update = time(NULL); }
    time_t timestamp() { return last_update; }

    std::string dump();
    void inc_writer() {writer_cnt++;}
    void dec_writer() {writer_cnt--;}
    void inc_inode() {local_inode++;}
    void dec_inode() {local_inode--;}
    void inc_volume(uint64_t size) { local_volume += size;}
    void dec_volume(uint64_t size) { local_volume -= size;}
    void local_reset() { local_inode = 0; local_volume = 0;}
    void local_inode_reset() { local_inode = 0; }

    void set_vtime(uint64_t _vt, uint64_t _vt_ns) { vtime = _vt; vtime_ns = _vt_ns; }
    uint64_t get_vtime() const { return vtime; }
    uint64_t get_vtime_ns() const { return vtime_ns; }
    int64_t get_local_inode() const { return local_inode; }
    int64_t get_local_volume() const { return local_volume; }
  private:
    XrdSysMutex mLock;
    std::atomic<uint64_t> vtime;
    std::atomic<uint64_t> vtime_ns;
    std::atomic<int> writer_cnt;
    std::atomic<int64_t> local_volume;
    std::atomic<int64_t> local_inode;
    std::atomic<time_t> last_update;
  };

  class capx : public eos::fusex::cap
  //----------------------------------------------------------------------------
  {
  public:

    virtual ~capx() { }

    capx& operator=(eos::fusex::cap other)
    {
      (*((eos::fusex::cap*)(this))) = other;
      return *this;
    }

    XrdSysMutex& Locker()
    {
      return mLock;
    }

    static std::string capid(fuse_req_t req, fuse_ino_t ino);
    static std::string capid(fuse_ino_t ino, std::string clientid);
    static std::string getclientid(fuse_req_t req);


    std::string dump(bool dense = false);

    capx() : lastusage(0) { }

    capx(fuse_req_t req, fuse_ino_t ino)
    {
      set_id(ino);
      std::string cid = getclientid(req);
      set_clientid(cid);
      set_authid("");
    }

    bool satisfy(mode_t mode);

    bool valid(bool debug = true);

    double lifetime();

    void invalidate();

    void use()
    {
      lastusage = time(NULL);
    }

    const time_t used() const
    {
      return lastusage;
    }

  private:
    XrdSysMutex mLock;
    time_t lastusage;
  };

  typedef std::shared_ptr<capx> shared_cap;
  typedef std::shared_ptr<quotax> shared_quota;

  typedef std::set<fuse_ino_t> cinodes;
  typedef std::map<std::string, shared_quota> qmap_t;

  //----------------------------------------------------------------------------

  class qmap : public qmap_t, public XrdSysMutex
  {
    // map from quota inode to quota information
  public:

    qmap() { }

    virtual ~qmap() { }

    shared_quota get(shared_cap cap);
  };

  class cmap : public std::map<std::string, shared_cap>, public XrdSysMutex
  //----------------------------------------------------------------------------
  {
  public:

    cmap() { }

    virtual ~cmap() { }
  };

  //----------------------------------------------------------------------------
  cap();

  virtual ~cap();

  shared_cap get(fuse_req_t req,
                 fuse_ino_t ino,
                 bool lock = false);

  shared_cap get(fuse_ino_t ino,
                 std::string clientid
                );

  shared_cap acquire(fuse_req_t req,
                     fuse_ino_t ino,
                     mode_t mode,
                     bool lock = false
                    );

  bool share_quotanode(shared_cap cap1, shared_cap cap2) 
  {
    return ( cap1->_quota().quota_inode() == cap2->_quota().quota_inode() );
  }


  void open_writer_inode(shared_cap cap)
  {
    shared_quota q = quotamap.get(cap);
    q->inc_writer();
  }

  void close_writer_inode(shared_cap cap)
  {
    shared_quota q = quotamap.get(cap);
    q->dec_writer();
  }

  void book_inode(shared_cap cap)
  {
    shared_quota q = quotamap.get(cap);
    XrdSysMutexHelper qLock(q->Locker());
    q->inc_inode();
    eos_static_debug("%s", q->dump().c_str());
  }

  void free_inode(shared_cap cap)
  {
    shared_quota q = quotamap.get(cap);
    q->dec_inode();
    eos_static_debug("%s", q->dump().c_str());
  }

  void book_volume(shared_cap cap, uint64_t size)
  {
    shared_quota q = quotamap.get(cap);
    q->inc_volume(size);
    eos_static_debug("%s", q->dump().c_str());
  }

  void free_volume(shared_cap cap, uint64_t size)
  {
    shared_quota q = quotamap.get(cap);
    q->dec_volume(size);
    eos_static_debug("%s", q->dump().c_str());
  }

  uint64_t has_quota(shared_cap cap, uint64_t size)
  {
    shared_quota q = quotamap.get(cap);
    XrdSysMutexHelper qLock(q->Locker());

    ssize_t volume = q->volume_quota() - q->get_local_volume();
    ssize_t inodes = q->inode_quota()  - q->get_local_inode();

    if ( ((volume > 0) && (volume > (ssize_t)size)) &&
	 ( (inodes > 0) || (!size) ) ) {
      return volume;
    }
    // no quota, let's manifest this in the log file
    eos_static_warning("no-quota: i=%08lx\n%s,cap = {%s}\n", cap->id(), q->dump().c_str(), cap->dump().c_str());
    return 0;
  }

  void set_volume_edquota(shared_cap cap)
  {
    shared_quota q = quotamap.get(cap);
    XrdSysMutexHelper qLock(q->Locker());

    if (q) {
      q->set_volume_quota(0);
    }
  }

  void update_quota(shared_cap cap, const eos::fusex::quota& new_quota)
  {
    shared_quota q = quotamap.get(cap);
    XrdSysMutexHelper qLock(q->Locker());
    *q = new_quota;
    q->set_vtime(cap->vtime(), cap->vtime_ns());
  }

  shared_quota quota(shared_cap cap)
  {
    return quotamap.get(cap);
  }

  std::string imply(shared_cap cap, std::string imply_authid, mode_t mode,
                    fuse_ino_t inode);

  fuse_ino_t forget(const std::string& capid);

  void store(fuse_req_t req,
             eos::fusex::cap cap);

  int refresh(fuse_req_t req, shared_cap cap);

  void init(backend* _mdbackend, metad* _metad);

  void reset();

  void clear() {
    capmap.clear();
    capextionsmap.clear();
    quotamap.clear();  
  }

  std::string ls();

  void capflush(ThreadAssistant& assistant); // thread removing capabilities

  XrdSysMutex& get_revocationLock()
  {
    return revocationLock;
  }

  typedef std::set<std::string> revocation_set_t;;

  size_t size()
  {
    XrdSysMutexHelper mLock(capmap);
    return capmap.size();
  }

  revocation_set_t& get_revocationmap()
  {
    return revocationset;
  }

private:

  cmap capmap;
  cmap capextionsmap;
  qmap quotamap;

  backend* mdbackend;
  metad* mds;

  XrdSysMutex revocationLock;
  revocation_set_t revocationset; // set containing all authids to revoke

};
#endif /* FUSE_CAP_HH_ */
