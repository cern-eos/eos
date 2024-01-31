//------------------------------------------------------------------------------
//! @file io.hh
//! @author Andreas-Joachim Peters CERN
//! @brief io class
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

#ifndef FUSE_IO_HH_
#define FUSE_IO_HH_

#include "data/cache.hh"
#include "data/journalcache.hh"

#define O_CACHE 040000000

namespace XrdCl
{
class Proxy;
typedef std::shared_ptr<XrdCl::Proxy> shared_proxy;
}

class io
{
public:

  io()
  {
    _file = 0;
    _journal = 0;
    ino = 0;
    caching = true;
  }

  io(fuse_ino_t _ino)
  {
    _file = 0;
    _journal = 0;
    ino = _ino;
    caching = true;
  }

  ~io()
  {
    delete _file;
    delete _journal;
  }

  void disable_caches()
  {
    delete _file;
    delete _journal;
    _file = 0;
    _journal = 0;
    caching = false;
  }

  void disable_file_cache()
  {
    delete _file;
    _file = 0;
  }

  bool is_caching()
  {
    return caching;
  }

  void set_file(cache* file)
  {
    _file = file;
  }

  void set_journal(journalcache* journal)
  {
    _journal = journal;
  }
  void set_xrdioro(fuse_req_t req, XrdCl::shared_proxy _cl)
  {
    _xrdioro["default"] = _cl;
  }

  void set_xrdiorw(fuse_req_t req, XrdCl::shared_proxy _cl)
  {
    _xrdiorw["default"] = _cl;
  }

  cache* file()
  {
    return _file;
  }

  journalcache* journal()
  {
    return _journal;
  }

  XrdCl::shared_proxy xrdioro(fuse_req_t req)
  {
    return _xrdioro["default"];
  }

  XrdCl::shared_proxy xrdiorw(fuse_req_t req)
  {
    return _xrdiorw["default"];
  }

  bool has_xrdioro(fuse_req_t req)
  {
    return _xrdioro.count("default");
  }

  bool has_xrdiorw(fuse_req_t req)
  {
    return _xrdiorw.count("default");
  }

  XrdCl::shared_proxy xrdioro(std::string& id)
  {
    return _xrdioro[id];
  }

  XrdCl::shared_proxy xrdiorw(std::string& id)
  {
    return _xrdiorw[id];
  }

  std::map<std::string, XrdCl::shared_proxy>& get_xrdiorw()
  {
    return _xrdiorw;
  }

  std::map<std::string, XrdCl::shared_proxy>& get_xrdioro()
  {
    return _xrdioro;
  }

  bool erase_xrdioro(fuse_req_t req)
  {
    _xrdioro.erase("default");
    return true;
  }

  bool erase_xrdioro(const std::string& id)
  {
    _xrdioro.erase(id);
    return true;
  }

private:
  cache* _file;
  journalcache* _journal;
  std::map<std::string, XrdCl::shared_proxy> _xrdioro;
  std::map<std::string, XrdCl::shared_proxy> _xrdiorw;
  fuse_ino_t ino;
  bool caching;
};

typedef shared_ptr<io> shared_io;

#endif
