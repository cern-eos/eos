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
#include "misc/longstring.hh"
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

  kv() { }

  virtual ~kv() { }

  virtual int get(const std::string& key, std::string& value) = 0;
  virtual int get(const std::string& key, uint64_t& value) = 0;
  virtual int put(const std::string& key, const std::string& value) = 0;
  virtual int put(const std::string& key, uint64_t value) = 0;
  virtual int inc(const std::string& key, uint64_t& value) = 0;

  virtual int erase(const std::string& key) = 0;

  virtual int get(uint64_t key, std::string& value,
                  const std::string& name_space = "i") = 0;
  virtual int put(uint64_t key, const std::string& value,
                  const std::string& name_space = "i") = 0;

  virtual int get(uint64_t key, uint64_t& value,
                  const std::string& name_space = "i") = 0;
  virtual int put(uint64_t key, uint64_t value,
                  const std::string& name_space = "i") = 0;

  virtual int erase(uint64_t key, const std::string& name_space = "i") = 0;

  virtual int clean_stores(const std::string& storedir, const std::string& newdb) = 0 ;
  virtual std::string statistics() = 0;

protected:

  std::string buildKey(uint64_t key, const std::string& name_space)
  {
    char buffer[128];
    longstring::unsigned_to_decimal(key, buffer);
    std::string sbuf(buffer);

    if (!name_space.empty()) {
      sbuf = name_space + ":" + sbuf;
    }

    return sbuf;
  }

};
#endif /* FUSE_KV_HH_ */
