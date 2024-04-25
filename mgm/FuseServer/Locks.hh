// ----------------------------------------------------------------------
// File: FuseServer/Locks.hh
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

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

#pragma once

#include "mgm/Namespace.hh"
#include <map>
#include <memory>
#include "mgm/fuse-locks/LockTracker.hh"
#include <XrdSys/XrdSysPthread.hh>

EOSFUSESERVERNAMESPACE_BEGIN

//----------------------------------------------------------------------------
//! Class Lock
//----------------------------------------------------------------------------

class Lock : XrdSysMutex
{
public:

  Lock() = default;

  virtual ~Lock() = default;

  typedef std::shared_ptr<LockTracker> shared_locktracker;

  typedef std::map<uint64_t, shared_locktracker > lockmap_t;

  shared_locktracker getLocks(uint64_t id);

  void purgeLocks();

  int dropLocks(uint64_t id, pid_t pid);

  int dropLocks(const std::string& owner);

  int lsLocks(const std::string& owner,
              std::map<uint64_t, std::set<pid_t>>&rlocks,
              std::map<uint64_t, std::set<pid_t>>&wlocks);
private:
  lockmap_t lockmap;
};


EOSFUSESERVERNAMESPACE_END
