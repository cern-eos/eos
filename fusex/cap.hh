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
#include "backend.hh"
#include "md.hh"
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

class cap
{
public:

  //----------------------------------------------------------------------------

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

    XrdSysMutex & Locker()
    {
      return mLock;
    }

    static std::string capid(fuse_req_t req, fuse_ino_t ino)
    {
      char sid[128];
      snprintf(sid, sizeof (sid),
               "%lx:%u:%u@%s",
               ino,
               fuse_req_ctx(req)->uid,
               fuse_req_ctx(req)->gid,
               "localhost"
               );
      return sid;
    }
    
     static std::string capid(fuse_ino_t ino, std::string clientid)
    {
      char sid[128];
      snprintf(sid, sizeof (sid),
               "%lx:%s",
               ino,
               clientid.c_str()
               );
      return sid;
    }
     
    static std::string getclientid(fuse_req_t req)
    {
      char sid[128];
      snprintf(sid, sizeof (sid),
               "%u:%u@%s",
               fuse_req_ctx(req)->uid,
               fuse_req_ctx(req)->gid,
               "localhost"
               );
      return sid;
    }

    std::string dump(bool dense=false);

    capx()
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

    bool valid(bool debug=true);

  private:
    XrdSysMutex mLock;
  } ;

  typedef std::shared_ptr<capx> shared_cap;
  typedef std::set<fuse_ino_t> cinodes;
  
  //----------------------------------------------------------------------------

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
  cap();

  virtual ~cap();

  static cap* sCAP;

  static cap& Instance()
  {
    return *sCAP;
  }
  
  
  shared_cap get(fuse_req_t req,
                 fuse_ino_t ino);

  shared_cap acquire(fuse_req_t req,
                     fuse_ino_t ino,
                     mode_t mode
                     );

  void imply(shared_cap cap, std::string imply_authid, mode_t mode, fuse_ino_t inode);
  
  fuse_ino_t forget(const std::string& capid);
  
  void store(fuse_req_t req,
             eos::fusex::cap cap);

  void refresh(fuse_req_t req, shared_cap cap);

  void init(backend* _mdbackend, metad* _metad);

  void reset();
  
  std::string ls();
  
  bool should_terminate()
  {
    return capterminate.load();
  } // check if threads should terminate 

  void terminate()
  {
    capterminate.store(true, std::memory_order_seq_cst);
  } // indicate to terminate

  void capflush(); // thread removing capabilities

private:

  cmap capmap;
  backend* mdbackend;
  metad* mds;

  std::atomic<bool> capterminate;
} ;
#endif /* FUSE_CAP_HH_ */
