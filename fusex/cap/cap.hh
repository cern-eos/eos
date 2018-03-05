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

    virtual ~quotax()
    {
    }

    XrdSysMutex& Locker()
    {
      return mLock;
    }

    quotax& operator=(eos::fusex::quota other)
    {
      (*((eos::fusex::quota*)(this))) = other;
      return *this;
    }

  private:
    XrdSysMutex mLock;
  } ;

  class capx : public eos::fusex::cap
  //----------------------------------------------------------------------------
  {
  public:

    virtual ~capx()
    {
    }

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

    capx() : lastusage(0)
    {
    }

    capx(fuse_req_t req, fuse_ino_t ino)
    {
      set_id(ino);
      std::string cid = getclientid(req);
      set_clientid(cid);
      set_authid("");
    }

    bool satisfy(mode_t mode);

    bool valid(bool debug = true);

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
  } ;

  typedef std::shared_ptr<capx> shared_cap;
  typedef std::shared_ptr<quotax> shared_quota;

  typedef std::set<fuse_ino_t> cinodes;
  typedef std::map<std::string, shared_quota> qmap_t;

  //----------------------------------------------------------------------------

  class qmap : public qmap_t, public XrdSysMutex
  {
    // map from quota inode to quota information
  public:

    qmap()
    {
    }

    virtual ~qmap()
    {
    }

    shared_quota get(shared_cap cap);
  } ;

  class cmap : public std::map<std::string, shared_cap> , public XrdSysMutex
  //----------------------------------------------------------------------------
  {
  public:

    cmap()
    {
    }

    virtual ~cmap()
    {
    }
  } ;

  //----------------------------------------------------------------------------
  class forgotten
  {
    // used to remove caps which point forgotten directory inodes
  public:

    forgotten() {}
    virtual ~forgotten() {}

    void add(fuse_ino_t ino) {
      XrdSysMutexHelper mLock(mLocker);
      mUnlinkedInodes.insert(ino);
    }
    bool has(fuse_ino_t ino) {
      XrdSysMutexHelper mLock(mLocker);
      return mUnlinkedInodes.count(ino);
    }

    void clear() {
      XrdSysMutexHelper mLock(mLocker);
      mUnlinkedInodes.clear();
    }

  private:
    XrdSysMutex mLocker;
    std::set<fuse_ino_t> mUnlinkedInodes;
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

  void book_inode(shared_cap cap)
  {
    shared_quota q = quotamap.get(cap);
    XrdSysMutexHelper qLock(q->Locker());
    q->set_inode_quota(q->inode_quota() - 1);
  }

  void book_volume(shared_cap cap, uint64_t size)
  {
    shared_quota q = quotamap.get(cap);
    XrdSysMutexHelper qLock(q->Locker());

    if (size < q->volume_quota()) {
      q->set_volume_quota(q->volume_quota() - size);
    } else {
      q->set_volume_quota(0);
    }

    eos_static_debug("volume=%llu", q->volume_quota());
  }

  void free_volume(shared_cap cap, uint64_t size)
  {
    shared_quota q = quotamap.get(cap);
    XrdSysMutexHelper qLock(q->Locker());
    q->set_volume_quota(q->volume_quota() + size);
  }

  uint64_t has_quota(shared_cap cap, uint64_t size)
  {
    shared_quota q = quotamap.get(cap);
    XrdSysMutexHelper qLock(q->Locker());

    if ((q->volume_quota() > size) &&
        (q->inode_quota() > 0)) {
      return q->volume_quota();
    }

    return 0;
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

  std::string ls();

  void capflush(ThreadAssistant& assistant); // thread removing capabilities

  XrdSysMutex& get_extensionLock()
  {
    return extensionLock;
  }

  typedef std::map<std::string, size_t> extension_map_t;

  extension_map_t& get_extensionmap()
  {
    return extensionmap;
  }

  size_t size()
  {
    XrdSysMutexHelper mLock( capmap );
    return capmap.size();
  }


  forgotten forgetlist;

private:

  cmap capmap;
  cmap capextionsmap;
  qmap quotamap;

  backend* mdbackend;
  metad* mds;

  XrdSysMutex extensionLock;
  extension_map_t extensionmap; // map containing all authids
  // with their lifetime increment to be sent by the heartbeat

} ;
#endif /* FUSE_CAP_HH_ */
