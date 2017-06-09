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
  virtual int attach(std::string& cookie, bool isRW) = 0;
  virtual int detach(std::string& cookie) = 0;
  virtual int unlink() = 0;

  virtual ssize_t pread(void *buf, size_t count, off_t offset) = 0;
  virtual ssize_t peek_read(char* &buf, size_t count, off_t offset) = 0;
  virtual void release_read() = 0;

  virtual ssize_t pwrite(const void *buf, size_t count, off_t offset) = 0;

  virtual int truncate(off_t) = 0;
  virtual int sync() = 0;

  virtual size_t size() = 0;

  virtual int set_attr(std::string& key, std::string& value) = 0;
  virtual int attr(std::string key, std::string& value) = 0;

  virtual int set_cookie(std::string cookie)
  {
    std::string ecc = "user.eos.cache.cookie";
    return set_attr(ecc , cookie);
  }

  virtual int cookie(std::string& acookie)
  {
    return attr("user.eos.cache.cookie", acookie);
  }

  class io
  {
  public:

    io()
    {
      _file = 0;
      _journal = 0;
      _xrdioro = 0;
      _xrdiorw = 0;
      ino = 0;
    }

    io(fuse_ino_t _ino)
    {
      _file = 0;
      _journal = 0;
      _xrdioro = 0;
      _xrdiorw = 0;
      ino = _ino;
    }

    ~io()
    {
      delete _file;
      delete _journal;
      delete _xrdioro;
      delete _xrdiorw;
    }

    void set_file(cache* file)
    {
      _file = file;
    }

    void set_journal(cache* journal)
    {
      _journal = journal;
    }

    void set_xrdioro(XrdCl::Proxy* _cl)
    {
      _xrdioro = _cl;
    }

    void set_xrdiorw(XrdCl::Proxy* _cl)
    {
      _xrdiorw = _cl;
    }

    cache* file()
    {
      return _file;
    }

    cache* journal()
    {
      return _journal;
    }

    XrdCl::Proxy* xrdioro()
    {
      return _xrdioro;
    }

    XrdCl::Proxy* xrdiorw()
    {
      return _xrdiorw;
    }

  private:
    cache* _file;
    cache* _journal;
    XrdCl::Proxy* _xrdioro;
    XrdCl::Proxy* _xrdiorw;
    fuse_ino_t ino;
  } ;

  typedef shared_ptr<cache::io> shared_io;

private:

  fuse_ino_t ino;
} ;

class cachehandler : public std::map<fuse_ino_t, cache::shared_io>,
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

  static cache::shared_io get(fuse_ino_t ino);

  static int rm(fuse_ino_t ino);

  enum cache_t
  {
    INVALID, MEMORY, DISK
  } ;

  struct cacheconfig
  {
    cache_t type;
    std::string location;
    uint64_t total_file_cache_size; // total size of the file cache
    uint64_t per_file_cache_max_size; // per file maximum file cache size
    uint64_t total_file_journal_size; // total size of the journal cache
    uint64_t per_file_journal_max_size; // per file maximum journal cache size
    std::string journal;
  } ;

  int init(cacheconfig &config);
  void logconfig();

  bool inmemory()
  {

    return (config.type == cache_t::MEMORY);
  }

  bool journaled()
  {

    return (config.journal.length());
  }

  cacheconfig& get_config()
  {
    return config;
  }

private:

  cacheconfig config;
} ;



#endif /* FUSE_CACHE_HH_ */
