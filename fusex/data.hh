//------------------------------------------------------------------------------
//! @file data.hh
//! @author Andreas-Joachim Peters CERN
//! @brief data handling class
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

#ifndef FUSE_DATA_HH_
#define FUSE_DATA_HH_

#include <sys/stat.h>
#include <sys/types.h>
#include "llfusexx.hh"
#include "fusex/fusex.pb.h"
#include "common/Logging.hh"
#include "XrdSys/XrdSysPthread.hh"
#include <memory>
#include <map>
#include <set>
#include <exception>
#include <stdexcept>

class data
{
public:

  //----------------------------------------------------------------------------

  class datax
  //----------------------------------------------------------------------------
  {
  public:

    datax()
    {
    }

    virtual ~datax()
    {
    }

    XrdSysMutex& Locker()
    {
      return mLock;
    }

    void set_id( uint64_t ino)
    {
      XrdSysMutexHelper mLock(Locker());
      mIno = ino;
    }

    uint64_t id () const
    {
      return mIno;
    }

    void flush()
    {
    }

  private:
    XrdSysMutex mLock;
    uint64_t mIno;
  } ;

  typedef std::shared_ptr<datax> shared_data;

  //----------------------------------------------------------------------------

  class dmap : public std::map<fuse_ino_t, shared_data> , public XrdSysMutex
  //----------------------------------------------------------------------------
  {
  public:

    dmap()
    {
    }

    virtual ~dmap()
    {
    }
  } ;

  data();

  virtual ~data();

  void init();

  shared_data get(fuse_req_t req,
                  fuse_ino_t ino);

  uint64_t insert(fuse_req_t req,
                  shared_data dd);


  void dataxflush(); // thread pushing into data cache


private:
  dmap datamap;

  XrdSysCondVar ioflush;
  std::set<uint64_t> ioqueue;

  size_t ioqueue_max_backlog;

} ;
#endif /* FUSE_DATA_HH_ */
