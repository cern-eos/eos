//------------------------------------------------------------------------------
// File: FuseServer.cc
// Author: Andreas-Joachim Peters - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
#include <cstdlib>

#include "mgm/FuseServer/Locks.hh"
#include "common/Logging.hh"
#include "mgm/fuse-locks/LockTracker.hh"

EOSMGMNAMESPACE_BEGIN


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
FuseServer::Lock::shared_locktracker
FuseServer::Lock::getLocks(uint64_t id)
{
  XrdSysMutexHelper lock(this);

  // make sure you have this object locked
  if (!lockmap.count(id)) {
    lockmap[id] = std::make_shared<LockTracker>();
  }

  return lockmap[id];
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
FuseServer::Lock::purgeLocks()
{
  XrdSysMutexHelper lock(this);
  std::set<uint64_t>purgeset;

  for (auto it = lockmap.begin(); it != lockmap.end(); ++it) {
    if (!it->second->inuse()) {
      purgeset.insert(it->first);
    }
  }

  for (auto it = purgeset.begin(); it != purgeset.end(); ++it) {
    lockmap.erase(*it);
  }
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
int
FuseServer::Lock::dropLocks(uint64_t id, pid_t pid)
{
  eos_static_info("id=%llu pid=%u", id, pid);
  // drop locks for a given inode/pid pair
  int retc = 0;
  {
    XrdSysMutexHelper lock(this);

    if (lockmap.count(id)) {
      lockmap[id]->removelk(pid);
      retc = 0;
    } else {
      retc = ENOENT;
    }
  }
  purgeLocks();
  return retc;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
int
FuseServer::Lock::dropLocks(const std::string& owner)
{
  if (EOS_LOGS_DEBUG) {
    eos_static_debug("owner=%s", owner.c_str());
  }

  // drop locks for a given owner
  int retc = 0;
  {
    XrdSysMutexHelper lock(this);

    for (auto it = lockmap.begin(); it != lockmap.end(); ++it) {
      it->second->removelk(owner);
    }
  }
  purgeLocks();
  return retc;
}

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
int
FuseServer::Lock::lsLocks(const std::string& owner,
                          std::map<uint64_t, std::set < pid_t >>& rlocks,
                          std::map<uint64_t, std::set < pid_t >>& wlocks)
{
  int retc = 0;
  {
    XrdSysMutexHelper lock(this);

    for (auto it = lockmap.begin(); it != lockmap.end(); ++it) {
      std::set<pid_t> rlk = it->second->getrlks(owner);
      std::set<pid_t> wlk = it->second->getwlks(owner);
      rlocks[it->first].insert(rlk.begin(), rlk.end());
      wlocks[it->first].insert(wlk.begin(), wlk.end());
    }
  }
  return retc;
}

EOSMGMNAMESPACE_END
