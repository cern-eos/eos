//------------------------------------------------------------------------------
// File: FuseServer/Flush.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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

#include <string>
#include <thread>

#include "mgm/FuseServer/Flush.hh"
#include "common/Logging.hh"
#include "mgm/ofs/XrdMgmOfs.hh"


EOSMGMNAMESPACE_BEGIN


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
FuseServer::Flush::beginFlush(uint64_t id, std::string client)
{
  eos_static_info("ino=%016x client=%s", id, client.c_str());
  XrdSysMutexHelper lock(this);
  flush_info_t finfo(client);
  flushmap[id][client].Add(finfo);
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
FuseServer::Flush::endFlush(uint64_t id, std::string client)
{
  eos_static_info("ino=%016x client=%s", id, client.c_str());
  XrdSysMutexHelper lock(this);
  flush_info_t finfo(client);

  if (flushmap[id][client].Remove(finfo)) {
    flushmap[id].erase(client);

    if (!flushmap[id].size()) {
      flushmap.erase(id);
    }
  }
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
bool
FuseServer::Flush::hasFlush(uint64_t id)
{
  // this function takes maximum 255ms and waits for a flush to be removed
  // this function might block a client connection/thread for the given time
  bool has = false;
  size_t delay = 1;

  for (size_t i = 0 ; i < 8; ++i) {
    {
      XrdSysMutexHelper lock(this);
      has = validateFlush(id);
    }

    if (!has) {
      return false;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(delay));
    delay *= 2;
  }

  return true;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
bool
FuseServer::Flush::validateFlush(uint64_t id)
{
  bool has = false;

  if (flushmap.count(id)) {
    for (auto it = flushmap[id].begin(); it != flushmap[id].end();) {
      if (eos::common::Timing::GetAgeInNs(&it->second.ftime) < 0) {
        has = true;
        ++it;
      } else {
        it = flushmap[id].erase(it);
      }
    }

    if (!flushmap[id].size()) {
      flushmap.erase(id);
    }
  }

  return has;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
FuseServer::Flush::expireFlush()
{
  XrdSysMutexHelper lock(this);

  for (auto it = flushmap.begin(); it != flushmap.end();) {
    for (auto fit = it->second.begin(); fit != it->second.end();) {
      if (eos::common::Timing::GetAgeInNs(&fit->second.ftime) < 0) {
        ++fit;
      } else {
        fit = it->second.erase(fit);
      }
    }

    if (!it->second.size()) {
      it = flushmap.erase(it);
    } else {
      ++it;
    }
  }
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
FuseServer::Flush::Print(std::string& out)
{
  XrdSysMutexHelper lock(this);

  for (auto it = flushmap.begin(); it != flushmap.end(); ++it) {
    for (auto fit = it->second.begin(); fit != it->second.end(); ++fit) {
      long long valid = eos::common::Timing::GetAgeInNs(&fit->second.ftime);
      char formatline[4096];
      snprintf(formatline, sizeof(formatline),
               "flush : ino : %016lx client : %-8s valid=%.02f sec\n",
               it->first,
               fit->first.c_str(),
               1.0 * valid / 1000000000.0);
      out += formatline;
    }
  }
}

EOSMGMNAMESPACE_END
