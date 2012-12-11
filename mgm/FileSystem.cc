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

/*----------------------------------------------------------------------------*/
#include "mgm/FileSystem.hh"
/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
bool
FileSystem::StartDrainJob() 
{
  //----------------------------------------------------------------
  //! start a drain job after stat.errc!=0 (e.g. opserror)
  //----------------------------------------------------------------

  if (!ShouldBroadCast()) {
    // this is a filesystem on a ro-slave MGM e.g. it does not drain
    return true;
  }
  // check if there is already a drainjob
  drainJobMutex.Lock();
  if (drainJob) {
    drainJobMutex.UnLock();
    return false;
  }

  // no drain job
  drainJob = new DrainJob(GetId(),true);
  drainJobMutex.UnLock();
  return true;
}

/*----------------------------------------------------------------------------*/
bool
FileSystem::StopDrainJob()
{
  eos::common::FileSystem::fsstatus_t isstatus = GetConfigStatus();

  if ( (isstatus == kDrainDead) || (isstatus == kDrain) ) {
    // if this is in drain mode, we leave the drain job
    return false;
  }
  
  drainJobMutex.Lock();
  if (drainJob) {
    delete drainJob;
    drainJob = 0;
    SetDrainStatus(eos::common::FileSystem::kNoDrain);
    drainJobMutex.UnLock();
    return true;
  }
  drainJobMutex.UnLock();
  return false;
}

/*----------------------------------------------------------------------------*/
bool
FileSystem::SetConfigStatus(eos::common::FileSystem::fsstatus_t status)
{
  //----------------------------------------------------------------
  //! catch any status change from/to 'drain' or 'draindead' 
  //----------------------------------------------------------------

  // check the current status
  eos::common::FileSystem::fsstatus_t isstatus = GetConfigStatus();

  if ( (isstatus == kDrainDead) || (isstatus == kDrain) ) {
    // stop draining
    drainJobMutex.Lock();
    if (drainJob) {
      delete drainJob;
      drainJob = 0;
      drainJobMutex.UnLock();
      SetDrainStatus(eos::common::FileSystem::kNoDrain);
    } else {
      drainJobMutex.UnLock();
    }
  }

  if ( (status == kDrain) || (status == kDrainDead) ) {
    // create a drain job
    drainJobMutex.Lock();
    // check if there is still a drain job
    if (drainJob) {
      delete drainJob;
      drainJob=0;
    }
    if (!ShouldBroadCast()) {
      // this is a filesystem on a ro-slave MGM e.g. it does not drain
    } else {
      drainJob = new DrainJob(GetId());
    }
    drainJobMutex.UnLock();
  } else {
    if (status == kEmpty) {
      SetDrainStatus(eos::common::FileSystem::kDrained);
      SetLongLong("stat.drainprogress",100);
    } else {
      SetDrainStatus(eos::common::FileSystem::kNoDrain);
    }
  }

  return eos::common::FileSystem::SetConfigStatus(status);
}

/*----------------------------------------------------------------------------*/
bool
FileSystem::SetString(const char* key, const char* str, bool broadcast)
{
  std::string skey=key;
  std::string sval=str;
  if (skey == "configstatus") {
    return SetConfigStatus(GetConfigStatusFromString(str));
  }
  
  return eos::common::FileSystem::SetString(key,str,broadcast);
}

EOSMGMNAMESPACE_END
 
