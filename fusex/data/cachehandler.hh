//------------------------------------------------------------------------------
//! @file cachehandler.hh
//! @author Andreas-Joachim Peters CERN
//! @brief cachehandler class
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

#ifndef FUSE_CACHEHANDLER_HH_
#define FUSE_CACHEHANDLER_HH_

#include "cache.hh"
#include "io.hh"
#include "cacheconfig.hh"
#include <mutex>

class cachehandler
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

  shared_io get(fuse_ino_t ino);
  int rm(fuse_ino_t ino);

  int init(cacheconfig &config); // called before becoming a daemon

  int init_daemonized(); // called after becoming a daemon

  void logconfig();

  bool inmemory()
  {

    return (config.type == cache_t::MEMORY);
  }

  bool journaled()
  {

    return (config.journal.length());
  }

private:
  std::map<fuse_ino_t, shared_io> contents;
  std::mutex mtx;
  cacheconfig config;
} ;

#endif
