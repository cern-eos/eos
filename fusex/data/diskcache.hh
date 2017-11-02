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

#ifndef FUSE_DISKCACHE_HH_
#define FUSE_DISKCACHE_HH_

#include <sys/stat.h>
#include <sys/types.h>
#include "llfusexx.hh"
#include "bufferll.hh"
#include "data/cache.hh"
#include "data/dircleaner.hh"
#include "data/cacheconfig.hh"
#include "XrdSys/XrdSysPthread.hh"
#include <map>
#include <string>

class diskcache : public cache
{
public:
  diskcache(fuse_ino_t _ino);
  virtual ~diskcache();

  // base class interface
  virtual int attach(fuse_req_t req, std::string& cookie, int flags) override;
  virtual int detach(std::string& cookie) override;
  virtual int unlink() override;

  virtual ssize_t pread(void *buf, size_t count, off_t offset) override;
  virtual ssize_t pwrite(const void *buf, size_t count, off_t offset) override;

  virtual int truncate(off_t) override;
  virtual int sync() override;

  virtual size_t size() override;

  virtual int set_attr(const std::string& key, const std::string& value) override;
  virtual int attr(const std::string &key, std::string& value) override;

  static int init(const cacheconfig &config);
  static int init_daemonized(const cacheconfig &config);

  virtual int rescue(std::string& location) override;

  virtual off_t prefetch_size() override { return sMaxSize; }

private:
  XrdSysMutex mMutex;
  int location(std::string &path, bool mkpath=true);
  static off_t sMaxSize;

  fuse_ino_t ino;
  size_t nattached;
  int fd;
  struct stat attachstat;
  struct stat detachstat;

  bufferllmanager::shared_buffer buffer;

  static std::string sLocation;

  static bufferllmanager sBufferManager;

  static shared_ptr<dircleaner> sDirCleaner;

} ;

#endif /* FUSE_JOURNALCACHE_HH_ */
