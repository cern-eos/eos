//------------------------------------------------------------------------------
//! @file kv.hh
//! @author Andreas-Joachim Peters CERN
//! @brief kv persistency class
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

#ifndef FUSE_KV_HH_
#define FUSE_KV_HH_

#include <sys/stat.h>
#include <sys/types.h>
#include "llfusexx.hh"
#include "fusex/fusex.pb.h"
#include "hiredis/hiredis.h"
#include "hiredis/async.h"
#include "XrdSys/XrdSysPthread.hh"
#include <memory>
#include <map>
#include <event.h>


//------------------------------------------------------------------------------
// Interface to a key-value store implementation.
//------------------------------------------------------------------------------
class kv : public XrdSysMutex
{
public:
  kv() {}
  virtual ~kv() {}

  virtual int get(const std::string &key, std::string &value) = 0;
  virtual int get(const std::string &key, uint64_t &value) = 0;
  virtual int put(const std::string &key, const std::string &value) = 0;
  virtual int put(const std::string &key, uint64_t value) = 0;
  virtual int inc(const std::string &key, uint64_t value) = 0;

  virtual int erase(const std::string &key) = 0;

  virtual int get(uint64_t key, std::string &value, std::string name_space="i") = 0;
  virtual int put(uint64_t key, const std::string &value, std::string name_space="i") = 0;

  virtual int get(uint64_t key, uint64_t &value, std::string name_space="i") = 0;
  virtual int put(uint64_t key, uint64_t &value, std::string name_space="i") = 0;

  virtual int erase(uint64_t key, std::string name_space="i") = 0;

  static kv* sKV;

  static kv& Instance()
  {
    return *sKV;
  }
};
#endif /* FUSE_KV_HH_ */
