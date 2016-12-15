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
#include <map>
#include <string>

class cache
{
public:

  cache() : ino(0)
  {
  }

  cache(fuse_ino_t _ino) : ino(_ino)
  {
  }

  virtual ~cache()
  {
  }

  // base class interface
  virtual int attach() = 0;
  virtual int detach() = 0;
  virtual int unlink() = 0;

  virtual ssize_t pread(void *buf, size_t count, off_t offset) = 0;
  virtual ssize_t peek_read(char* &buf, size_t count, off_t offset) = 0;
  virtual void release_read() = 0;

  virtual ssize_t pwrite(const void *buf, size_t count, off_t offset) = 0;

  virtual int truncate(off_t) = 0;
  virtual int sync() = 0;

  virtual size_t size() = 0;

  typedef shared_ptr<cache> shared_file;

private:

  fuse_ino_t ino;
} ;

class memorycache : public cache, bufferll
{
public:
  memorycache();
  memorycache(fuse_ino_t _ino);
  virtual ~memorycache();

  // base class interface
  virtual int attach();
  virtual int detach();
  virtual int unlink();

  virtual ssize_t pread(void *buf, size_t count, off_t offset);
  virtual ssize_t peek_read(char* &buf, size_t count, off_t offset);
  virtual void release_read();

  virtual ssize_t pwrite(const void *buf, size_t count, off_t offset);

  virtual int truncate(off_t);
  virtual int sync();

  virtual size_t size();

private:

  fuse_ino_t ino;
} ;

class diskcache : public cache, XrdSysMutex
{
public:
  diskcache();
  diskcache(fuse_ino_t _ino);

  virtual ~diskcache();

  // base class interface
  virtual int attach();
  virtual int detach();
  virtual int unlink();

  virtual ssize_t pread(void *buf, size_t count, off_t offset);
  virtual ssize_t peek_read(char* &buf, size_t count, off_t offset);
  virtual void release_read();

  virtual ssize_t pwrite(const void *buf, size_t count, off_t offset);

  virtual int truncate(off_t);
  virtual int sync();

  virtual size_t size();

  static int init();
  
  int location(std::string &path, bool mkpath=true);
  
private:

  fuse_ino_t ino;
  size_t nattached;
  int fd;
  
  bufferllmanager::shared_buffer buffer;
  
  static std::string sLocation;
  static bufferllmanager sBufferManager; 
} ;

class cachehandler : public std::map<fuse_ino_t, cache::shared_file>,
public XrdSysMutex
{
public:

  cachehandler()
  {
  }

  virtual ~cachehandler()
  {
  };

  // static member functions

  static cachehandler&
  instance()
  {
    static cachehandler i;
    return i;
  }

  static cache::shared_file get(fuse_ino_t ino);
  static int rm(fuse_ino_t ino);

  enum cache_t
  {
    INVALID, MEMORY, DISK
  } ;

  struct cacheconfig
  {
    cache_t type;
    std::string location;
    int mbsize;
  } ;

  int init(cacheconfig &config);
  void logconfig();

  bool inmemory()
  {
    return (config.type == cache_t::MEMORY);
  }

  cacheconfig& get_config() {return config;}
  
private:

  cacheconfig config;
} ;



#endif /* FUSE_CACHE_HH_ */
