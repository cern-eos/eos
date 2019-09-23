// ----------------------------------------------------------------------
// File: FileSystem.cc
// Author: Andreas-Joachim Peters - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2011 CERN/Switzerland                                  *
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

#include "mgm/FileSystem.hh"
#include "mgm/FsView.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/Master.hh"

EOSMGMNAMESPACE_BEGIN

//----------------------------------------------------------------------------
// Set the configuration status of a file system. This can be used to trigger
// the draining.
//----------------------------------------------------------------------------
bool
FileSystem::SetConfigStatus(eos::common::ConfigStatus new_status)
{
  using eos::mgm::FsView;
  using eos::common::DrainStatus;
  eos::common::ConfigStatus old_status = GetConfigStatus();
  int drain_tx = IsDrainTransition(old_status, new_status);

  // Only master drains
  if (ShouldBroadCast()) {
    std::string out_msg;
    
    if (drain_tx > 0) {
      if (!gOFS->mDrainEngine.StartFsDrain(this, 0, out_msg)) {
        eos_static_err("%s", out_msg.c_str());
        return false;
      }
    } else {
      if (!gOFS->mDrainEngine.StopFsDrain(this, out_msg)) {
        eos_static_debug("%s", out_msg.c_str());
        // // Drain already stopped make sure we also update the drain status
        // // if this was a finished drain ie. has status drained or failed
        // DrainStatus st = GetDrainStatus();
        // if ((st == DrainStatus::kDrained) ||
        //     (st == DrainStatus::kDrainFailed)) {
        //   SetDrainStatus(eos::common::DrainStatus::kNoDrain);
        // }
      }
    }
  }

  std::string val = eos::common::FileSystem::GetConfigStatusAsString(new_status);
  return eos::common::FileSystem::SetString("configstatus", val.c_str());
}

//------------------------------------------------------------------------------
// Set a 'key' describing the filesystem
//------------------------------------------------------------------------------
bool
FileSystem::SetString(const char* key, const char* str, bool broadcast)
{
  std::string skey = key;

  if (skey == "configstatus") {
    return SetConfigStatus(GetConfigStatusFromString(str));
  }

  return eos::common::FileSystem::SetString(key, str, broadcast);
}

//------------------------------------------------------------------------------
// Check if this is a drain transition i.e. enables or disabled draining
//------------------------------------------------------------------------------
int
FileSystem::IsDrainTransition(const eos::common::ConfigStatus old,
                              const eos::common::ConfigStatus status)
{
  using eos::common::FileSystem;

  // Enable draining
  if (((old != common::ConfigStatus::kDrain) &&
       (old != common::ConfigStatus::kDrainDead) &&
       ((status == common::ConfigStatus::kDrain) ||
        (status == common::ConfigStatus::kDrainDead))) ||
      (((old == common::ConfigStatus::kDrain) ||
        (old == common::ConfigStatus::kDrainDead)) &&
       (status == old))) {
    return 1;
  }

  // Stop draining
  if (((old == common::ConfigStatus::kDrain) ||
       (old == common::ConfigStatus::kDrainDead)) &&
      ((status != common::ConfigStatus::kDrain) &&
       (status != common::ConfigStatus::kDrainDead))) {
    return -1;
  }

  // Not a drain transition
  return 0;
}

EOSMGMNAMESPACE_END
