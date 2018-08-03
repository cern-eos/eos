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

/*----------------------------------------------------------------------------*/
/**
 * @brief Start a drain job on this filesystem
 * @return true if started otherwise false
 */
/*----------------------------------------------------------------------------*/
bool
FileSystem::StartDrainJob()
{
  if (!ShouldBroadCast()) {
    // this is a filesystem on a ro-slave MGM e.g. it does not drain
    return true;
  }

  // check if there is already a drainjob
  mDrainJobMutex.Lock();

  if (mDrainJob) {
    mDrainJobMutex.UnLock();
    return false;
  }

  // no drain job
  mDrainJob = new DrainJob(GetId(), true);
  mDrainJobMutex.UnLock();
  return true;
}

/*----------------------------------------------------------------------------*/
/**
 * @brief Stop a drain job on this filesystem
 * @return true if stopped otherwise false
 */
/*----------------------------------------------------------------------------*/
bool
FileSystem::StopDrainJob()
{
  eos::common::FileSystem::fsstatus_t isstatus = GetConfigStatus();

  if ((isstatus == kDrainDead) || (isstatus == kDrain)) {
    // if this is in drain mode, we leave the drain job
    return false;
  }

  mDrainJobMutex.Lock();

  if (mDrainJob) {
    delete mDrainJob;
    mDrainJob = 0;
    SetDrainStatus(eos::common::FileSystem::kNoDrain);
    mDrainJobMutex.UnLock();
    return true;
  }

  mDrainJobMutex.UnLock();
  return false;
}

//----------------------------------------------------------------------------
// Set the configuration status of a file system. This can be used to trigger
// the draining.
//----------------------------------------------------------------------------
bool
FileSystem::SetConfigStatus(eos::common::FileSystem::fsstatus_t new_status)
{
  using eos::mgm::FsView;
  eos::common::FileSystem::fsstatus_t old_status = GetConfigStatus();

  if (gOFS->mIsCentralDrain) {
    eos_static_info("fsid=%d, centralized drain type", GetId());
    int drain_tx = IsDrainTransition(old_status, new_status);

    if (drain_tx) {
      std::string out_msg;

      if (drain_tx > 0) {
        if (!gOFS->mDrainEngine.StartFsDrain(this, 0, out_msg)) {
          eos_static_err("%s", out_msg.c_str());
          return false;
        }
      } else if (drain_tx < 0) {
        if (!gOFS->mDrainEngine.StopFsDrain(this, out_msg)) {
          eos_static_err("%s", out_msg.c_str());
        }
      }
    }
  } else {
    eos_static_info("fsid=%d, distributed drain type", GetId());

    if ((old_status == kDrainDead) || (old_status == kDrain)) {
      // Stop draining
      XrdSysMutexHelper scop_lock(mDrainJobMutex);

      if (mDrainJob) {
        delete mDrainJob;
        mDrainJob = 0;
        SetDrainStatus(eos::common::FileSystem::kNoDrain);
      }
    }

    if ((new_status == kDrain) || (new_status == kDrainDead)) {
      // Create a drain job
      XrdSysMutexHelper scope_lock(mDrainJobMutex);

      // Check if there is still a drain job
      if (mDrainJob) {
        delete mDrainJob;
        mDrainJob = 0;
      }

      if (ShouldBroadCast()) {
        mDrainJob = new DrainJob(GetId());
      } else {
        // this is a filesystem on a ro-slave MGM e.g. it does not drain
      }
    } else {
      if (new_status == kEmpty) {
        SetDrainStatus(eos::common::FileSystem::kDrained);
        SetLongLong("stat.drainprogress", 100);
      } else {
        SetDrainStatus(eos::common::FileSystem::kNoDrain);
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
// Check if this is a config transition or noop
//------------------------------------------------------------------------------
bool
FileSystem::IsConfigTransition(const eos::common::FileSystem::fsstatus_t
                               old_status,
                               const eos::common::FileSystem::fsstatus_t new_status)
{
  return old_status != new_status;
}

//------------------------------------------------------------------------------
// Check if this is a drain transition i.e. enables or disabled draining
//------------------------------------------------------------------------------
int
FileSystem::IsDrainTransition(const eos::common::FileSystem::fsstatus_t
                              old_status,
                              const eos::common::FileSystem::fsstatus_t new_status)
{
  using eos::common::FileSystem;

  // Enable draining
  if ((old_status != FileSystem::kDrain) &&
      (old_status != FileSystem::kDrainDead) &&
      ((new_status == FileSystem::kDrain) ||
       (new_status == FileSystem::kDrainDead))) {
    return 1;
  }

  // Stop draining
  if (((old_status == FileSystem::kDrain) ||
       (old_status == FileSystem::kDrainDead)) &&
      ((new_status != FileSystem::kDrain) &&
       (new_status != FileSystem::kDrainDead))) {
    return -1;
  }

  // Not a drain transition
  return 0;
}

EOSMGMNAMESPACE_END
