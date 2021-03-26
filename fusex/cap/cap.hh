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
  // Class quotax
  //----------------------------------------------------------------------------
  class quotax {
  public:
    virtual ~quotax() = default;

    eos::fusex::quota* operator()() {
      return &mQuotaProto;
    }

    XrdSysMutex& Locker()
    {
      return mLock;
    }

    quotax& operator=(eos::fusex::quota other)
    {
      mQuotaProto = other;
      return *this;
    }

    inline int& writer() {return writer_cnt;}

  private:
    XrdSysMutex mLock;
    int writer_cnt;
    eos::fusex::quota mQuotaProto;
  };

  //----------------------------------------------------------------------------
  //! Class capx
  //----------------------------------------------------------------------------
  class capx
  {
  public:
    static std::string capid(fuse_req_t req, fuse_ino_t ino);
    static std::string capid(fuse_ino_t ino, std::string clientid);
    static std::string getclientid(fuse_req_t req);

    capx() : lastusage(0) { }

    capx(fuse_req_t req, fuse_ino_t ino)
    {
      mCapProto.set_id(ino);
      std::string cid = getclientid(req);
      mCapProto.set_clientid(cid);
      mCapProto.set_authid("");
    }

    virtual ~capx() = default;

    eos::fusex::cap* operator()() {
      return &mCapProto;
    }

    capx& operator=(eos::fusex::cap other)
    {
      mCapProto = other;
      return *this;
    }

    XrdSysMutex& Locker()
    {
      return mLock;
    }

    std::string dump(bool dense = false);

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
    eos::fusex::cap mCapProto;
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
    return ( (*cap1)()->_quota().quota_inode() == (*cap2)()->_quota().quota_inode() );
  }


  void open_writer_inode(shared_cap cap)
  {
    shared_quota q = quotamap.get(cap);
    XrdSysMutexHelper qLock(q->Locker());
    q->writer()++;
  }

  void close_writer_inode(shared_cap cap)
  {
    shared_quota q = quotamap.get(cap);
    XrdSysMutexHelper qLock(q->Locker());
    q->writer()--;
  }

  void book_inode(shared_cap cap)
  {
    shared_quota q = quotamap.get(cap);
    XrdSysMutexHelper qLock(q->Locker());
    (*q)()->set_inode_quota((*q)()->inode_quota() - 1);
  }

  void free_inode(shared_cap cap)
  {
    shared_quota q = quotamap.get(cap);
    XrdSysMutexHelper qLock(q->Locker());
    (*q)()->set_inode_quota((*q)()->inode_quota() + 1);
  }

  void book_volume(shared_cap cap, uint64_t size)
  {
    shared_quota q = quotamap.get(cap);
    XrdSysMutexHelper qLock(q->Locker());

    if (size < (*q)()->volume_quota()) {
      (*q)()->set_volume_quota((*q)()->volume_quota() - size);
    } else {
      (*q)()->set_volume_quota(0);
    }

    eos_static_debug("volume=%llu", (*q)()->volume_quota());
  }

  void free_volume(shared_cap cap, uint64_t size)
  {
    shared_quota q = quotamap.get(cap);
    XrdSysMutexHelper qLock(q->Locker());
    (*q)()->set_volume_quota((*q)()->volume_quota() + size);
  }

  uint64_t has_quota(shared_cap cap, uint64_t size)
  {
    shared_quota q = quotamap.get(cap);
    XrdSysMutexHelper qLock(q->Locker());

    if (((*q)()->volume_quota() > size) &&
        (((*q)()->inode_quota() > 0) || (!size))) {
      // it size is 0, we should not check for inodes
      return (*q)()->volume_quota();
    }

    return 0;
  }

  void set_volume_edquota(shared_cap cap)
  {
    shared_quota q = quotamap.get(cap);
    XrdSysMutexHelper qLock(q->Locker());

    if (q) {
      (*q)()->set_volume_quota(0);
    }
  }

  void update_quota(shared_cap cap, const eos::fusex::quota& new_quota)
  {
    shared_quota q = quotamap.get(cap);
    XrdSysMutexHelper qLock(q->Locker());
    *q = new_quota;
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
