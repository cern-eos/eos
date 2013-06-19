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
FileSystem::StartDrainJob ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Start a drain job on this filesystem
 * @return true if started otherwise false
 */
/*----------------------------------------------------------------------------*/
{
  if (!ShouldBroadCast())
  {
    // this is a filesystem on a ro-slave MGM e.g. it does not drain
    return true;
  }
  // check if there is already a drainjob
  mDrainJobMutex.Lock();
  if (mDrainJob)
  {
    mDrainJobMutex.UnLock();
    return false;
  }

  // no drain job
  mDrainJob = new DrainJob(GetId(), true);
  mDrainJobMutex.UnLock();
  return true;
}

/*----------------------------------------------------------------------------*/
bool
FileSystem::StopDrainJob ()
/*----------------------------------------------------------------------------*/
/**
 * @brief Stop a drain job on this filesystem
 * @return true if stopped otherwise false
 */
/*----------------------------------------------------------------------------*/
{
  eos::common::FileSystem::fsstatus_t isstatus = GetConfigStatus();

  if ((isstatus == kDrainDead) || (isstatus == kDrain))
  {
    // if this is in drain mode, we leave the drain job
    return false;
  }

  mDrainJobMutex.Lock();
  if (mDrainJob)
  {
    delete mDrainJob;
    mDrainJob = 0;
    SetDrainStatus(eos::common::FileSystem::kNoDrain);
    mDrainJobMutex.UnLock();
    return true;
  }
  mDrainJobMutex.UnLock();
  return false;
}

/*----------------------------------------------------------------------------*/
bool
FileSystem::SetConfigStatus (eos::common::FileSystem::fsstatus_t status)
/*----------------------------------------------------------------------------*/
/*
 * @brief Set the configuration status of this filesystem
 * @return true if successfull otherwise false
 * 
 * Depending on the given status this routine also manages drain jobs on the
 * underlying filesystem.
 */
/*----------------------------------------------------------------------------*/
{
  //----------------------------------------------------------------
  // catch any status change from/to 'drain' or 'draindead' 
  //----------------------------------------------------------------

  // check the current status
  eos::common::FileSystem::fsstatus_t isstatus = GetConfigStatus();

  if ((isstatus == kDrainDead) || (isstatus == kDrain))
  {
    // stop draining
    mDrainJobMutex.Lock();
    if (mDrainJob)
    {
      delete mDrainJob;
      mDrainJob = 0;
      mDrainJobMutex.UnLock();
      SetDrainStatus(eos::common::FileSystem::kNoDrain);
    }
    else
    {
      mDrainJobMutex.UnLock();
    }
  }

  if ((status == kDrain) || (status == kDrainDead))
  {
    // -------------------------------------------------------------------------
    // create a drain job
    // -------------------------------------------------------------------------
    mDrainJobMutex.Lock();

    // -------------------------------------------------------------------------
    // check if there is still a drain job
    // -------------------------------------------------------------------------
    if (mDrainJob)
    {
      delete mDrainJob;
      mDrainJob = 0;
    }
    if (!ShouldBroadCast())
    {
      // -----------------------------------------------------------------------
      // this is a filesystem on a ro-slave MGM e.g. it does not drain
      // -----------------------------------------------------------------------
    }
    else
    {
      mDrainJob = new DrainJob(GetId());
    }
    mDrainJobMutex.UnLock();
  }
  else
  {
    if (status == kEmpty)
    {
      SetDrainStatus(eos::common::FileSystem::kDrained);
      SetLongLong("stat.drainprogress", 100);
    }
    else
    {
      SetDrainStatus(eos::common::FileSystem::kNoDrain);
    }
  }

  return eos::common::FileSystem::SetConfigStatus(status);
}

/*----------------------------------------------------------------------------*/
bool
FileSystem::SetString (const char* key, const char* str, bool broadcast)
/*----------------------------------------------------------------------------*/
/**
 * @brief Set a 'key' describing the filesystem
 * @param key key to set
 * @param str value of the key
 * @param broadcast if true broadcast the change around
 * @return true if successfull otherwise false
 */
/*----------------------------------------------------------------------------*/
{
  std::string skey = key;
  std::string sval = str;
  if (skey == "configstatus")
  {
    return SetConfigStatus(GetConfigStatusFromString(str));
  }

  return eos::common::FileSystem::SetString(key, str, broadcast);
}

EOSMGMNAMESPACE_END

