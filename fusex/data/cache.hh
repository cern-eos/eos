//------------------------------------------------------------------------------
//! @file cache.hh
//! @author Andreas-Joachim Peters CERN
//! @brief data cache handling base class
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

#ifndef FUSE_CACHE_HH_
#define FUSE_CACHE_HH_

#include <sys/stat.h>
#include <sys/types.h>
#include "llfusexx.hh"
#include "bufferll.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdCl/XrdClFile.hh"
#include "xrdclproxy.hh"
#include <map>
#include <string>

class cache
{
public:

  virtual ~cache()
  {
  }

  // base class interface
  virtual int attach(fuse_req_t req, std::string& cookie, int flags) = 0;
  virtual int detach(std::string& cookie) = 0;
  virtual int unlink() = 0;

  virtual ssize_t pread(void *buf, size_t count, off_t offset) = 0;
  virtual ssize_t pwrite(const void *buf, size_t count, off_t offset) = 0;

  virtual int truncate(off_t) = 0;
  virtual int sync() = 0;

  virtual size_t size() = 0;

  virtual off_t prefetch_size()
  {
    return 0;
  }

  virtual int set_attr(const std::string& key, const std::string& value) = 0;
  virtual int attr(const std::string &key, std::string& value) = 0;

  virtual int set_cookie(const std::string &cookie)
  {
    return set_attr("user.eos.cache.cookie", cookie);
  }

  virtual int cookie(std::string& acookie)
  {
    return attr("user.eos.cache.cookie", acookie);
  }

  virtual int rescue(std::string& location)
  {
    return 0;
  }

  virtual int reset()
  {
    return 0;
  }

} ;


#endif /* FUSE_CACHE_HH_ */
