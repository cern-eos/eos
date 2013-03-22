// ----------------------------------------------------------------------
// File: FileSystem.hh
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

#ifndef __EOSMGM_FILESYSTEM_HH__
#define __EOSMGM_FILESYSTEM_HH__

/*----------------------------------------------------------------------------*/
#include "common/FileSystem.hh"
#include "mgm/DrainJob.hh"
/*----------------------------------------------------------------------------*/

/*----------------------------------------------------------------------------*/

EOSMGMNAMESPACE_BEGIN
/*----------------------------------------------------------------------------*/
class FileSystem : public eos::common::FileSystem
{
private:
  XrdSysMutex drainJobMutex;
  DrainJob* drainJob;
public:

  FileSystem (const char* queuepath, const char* queue, XrdMqSharedObjectManager* som) : eos::common::FileSystem (queuepath, queue, som)
  {
    drainJob = 0;
  }

  virtual
  ~FileSystem ()
  {
    drainJobMutex.Lock();
    if (drainJob)
    {
      delete drainJob;
      drainJob = 0;
    }
    drainJobMutex.UnLock();
  }

  bool
  ShouldBroadCast ()
  {
    if (mSom)
    {
      return mSom->ShouldBroadCast();
    }
    else
    {
      return false;
    }
  }

  bool SetConfigStatus (eos::common::FileSystem::fsstatus_t status); // this method is overwritten to catch any status change to/from 'drain' or 'draindead'

  bool SetString (const char* key, const char* str, bool broadcast = true); // see above

  bool StartDrainJob (); // starts a drain job with the opserror flag - this is triggered by stat.errc!= 0 via the FsListener Thread
  bool StopDrainJob (); // stops  a drain job with the opserror flag - this is triggered by stat.errc = 0 via the FsListener Thread
};

EOSMGMNAMESPACE_END

#endif
