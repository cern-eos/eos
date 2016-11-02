//------------------------------------------------------------------------------
//! @file md.hh
//! @author Andreas-Joachim Peters CERN
//! @brief meta data handling class
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#include "XrdSys/XrdSysPthread.hh"
#include <memory>
#include <map>
#include <set>

class metad
{
public:

  //----------------------------------------------------------------------------

  class mdx : public eos::fusex::md
  //----------------------------------------------------------------------------
  {
  public:

    mdx()
    {
    }

    mdx(fuse_ino_t ino)
    {
      set_id(ino);
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

    void add(std::shared_ptr<mdx> shared_md);

  private:
    XrdSysMutex mLock;
  } ;

  typedef std::shared_ptr<mdx> shared_md;

  //----------------------------------------------------------------------------

  class vnode_gen : public XrdSysMutex
  //----------------------------------------------------------------------------
  {
  public:

    vnode_gen()
    {
      mNextInode=1;
    }

    virtual ~vnode_gen()
    {
    }

    uint64_t inc()
    {
      XrdSysMutexHelper mLock(this);
      return mNextInode++;
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

  shared_md get(fuse_req_t req,
                fuse_ino_t ino);

  uint64_t insert(fuse_req_t req,
                  shared_md md);

  void add(shared_md pmd, shared_md md);
  

  void mdcflush(); // thread pushing into md cache


private:

  pmap mdmap;
  vmap inomap;

  vnode_gen next_ino;

  XrdSysCondVar mdflush;
  std::set<uint64_t> mdqueue;

  size_t mdqueue_max_backlog;
  
} ;
#endif /* FUSE_MD_HH_ */
