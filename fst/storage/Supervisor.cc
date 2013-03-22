// ----------------------------------------------------------------------
// File: Supervisor.cc
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
#include "fst/storage/Storage.hh"
#include "fst/XrdFstOfs.hh"

/*----------------------------------------------------------------------------*/

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
Storage::Supervisor ()
{
  // this thread does an automatic self-restart if this storage node has filesystems configured but they don't boot
  // - this can happen by a timing issue during the autoboot phase

  eos_static_info("Supervisor activated ...");

  while (1)
  {
    {
      size_t ndown = 0;
      size_t nfs = 0;
      {
        eos::common::RWMutexReadLock lock(fsMutex);
        nfs = fileSystemsVector.size();
        for (size_t i = 0; i < fileSystemsVector.size(); i++)
        {
          if (!fileSystemsVector[i])
          {
            continue;
          }
          else
          {
            eos::common::FileSystem::fsstatus_t bootstatus = fileSystemsVector[i]->GetStatus();
            eos::common::FileSystem::fsstatus_t configstatus = fileSystemsVector[i]->GetConfigStatus();
            if ((bootstatus == eos::common::FileSystem::kDown) &&
                (configstatus > eos::common::FileSystem::kDrain))
            {
              ndown++;
            }
          }
        }

        if (ndown)
        {
          // we give one more minute to get things going
          XrdSysTimer sleeper;
          sleeper.Snooze(10);
          ndown = 0;
          {
            // check the status a second time
            eos::common::RWMutexReadLock lock(fsMutex);
            nfs = fileSystemsVector.size();
            for (size_t i = 0; i < fileSystemsVector.size(); i++)
            {
              if (!fileSystemsVector[i])
              {
                continue;
              }
              else
              {
                eos::common::FileSystem::fsstatus_t bootstatus = fileSystemsVector[i]->GetStatus();
                eos::common::FileSystem::fsstatus_t configstatus = fileSystemsVector[i]->GetConfigStatus();
                if ((bootstatus == eos::common::FileSystem::kDown) &&
                    (configstatus > eos::common::FileSystem::kDrain))
                {
                  ndown++;
                }
              }
            }
          }
          if (ndown == nfs)
          {
            // shutdown this daemon
            eos_static_alert("found %d/%d filesystems in <down> status - committing suicide !", ndown, nfs);
            XrdSysTimer sleeper;
            sleeper.Snooze(10);
            kill(getpid(), SIGQUIT);
          }
        }
      }
      XrdSysTimer sleeper;
      sleeper.Snooze(60);
    }
  }
}

EOSFSTNAMESPACE_END


