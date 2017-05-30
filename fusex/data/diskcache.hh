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
#include "cache.hh"
#include "XrdSys/XrdSysPthread.hh"
#include <map>
#include <string>

class diskcache : public cache, XrdSysMutex
{
public:
  diskcache();
  diskcache(fuse_ino_t _ino);

  virtual ~diskcache();

  // base class interface
  virtual int attach(std::string& cookie);
  virtual int detach(std::string& cookie);
  virtual int unlink();

  virtual ssize_t pread(void *buf, size_t count, off_t offset);
  virtual ssize_t peek_read(char* &buf, size_t count, off_t offset);
  virtual void release_read();

  virtual ssize_t pwrite(const void *buf, size_t count, off_t offset);

  virtual int truncate(off_t);
  virtual int sync();

  virtual size_t size();

  virtual int set_attr(std::string& key, std::string& value);
  virtual int attr(std::string key, std::string& value);  
 
  static int init();

  int location(std::string &path, bool mkpath=true);

private:

  fuse_ino_t ino;
  size_t nattached;
  int fd;

  bufferllmanager::shared_buffer buffer;

  static std::string sLocation;

  static bufferllmanager sBufferManager;
  static off_t sMaxSize;
};

#endif /* FUSE_JOURNALCACHE_HH_ */
