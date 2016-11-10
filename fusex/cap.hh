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
#include "fusex/fusex.pb.h"

#include "XrdSys/XrdSysPthread.hh"
#include <memory>
#include <map>


// extension to check for deletion permission
#define D_OK 8     // delete
#define M_OK 16    // chmod
#define C_OK 32    // chown
#define SA_OK 64    // set xattr

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

    XrdSysMutex & Locker()
    {
      return mLock;
    }

    static std::string capid(fuse_req_t req, fuse_ino_t ino)
    {
      // TODO: in the future we have to translate this from the auth environment
      char sid[128];
      snprintf(sid, sizeof (sid),
               "%u:%u@%s",
               fuse_req_ctx(req)->uid,
               fuse_req_ctx(req)->gid,
               "localhost"
               );
      return sid;
    }

    std::string dump();

    capx()
    {
    }

    capx(fuse_req_t req, fuse_ino_t ino)
    {
      set_id(ino);

      std::string cid = capid(req, ino);

      set_clientid(cid);
      set_authid(cid);
    }

    bool satisfy(mode_t mode);

    void renew(mode_t mode);

    bool valid();
    
  private:
    XrdSysMutex mLock;
  } ;

  typedef std::shared_ptr<capx> shared_cap;

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

  shared_cap get(fuse_req_t req,
                 fuse_ino_t ino);

  shared_cap acquire(fuse_req_t req,
                    fuse_ino_t ino,
                    mode_t mode
                    );

private:

  cmap capmap;
} ;
#endif /* FUSE_CAP_HH_ */
