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

#include "fst/storage/Storage.hh"
#include "fst/XrdFstOfs.hh"
#include "fst/storage/FileSystem.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Supervisor thread
//------------------------------------------------------------------------------
void
Storage::Supervisor()
{
  // this thread does an automatic self-restart if this storage node has
  // filesystems configured but they don't boot - this can happen by a
  //timing issue during the autoboot phase
  eos_static_info("Supervisor activated ...");

  while (true) {
    size_t ndown = 0;
    size_t nfs = 0;
    {
      eos::common::RWMutexReadLock fs_rd_lock(mFsMutex);

      for (const auto& elem : mFsMap) {
        auto fs = elem.second;

        if (!fs) {
          eos_warning("msg=\"skip file system id without object in map\" "
                      "fsid=%lu", elem.first);
          continue;
        }

        eos::common::BootStatus bootstatus = fs->GetStatus();
        eos::common::ConfigStatus configstatus = fs->GetConfigStatus();

        if ((bootstatus == eos::common::BootStatus::kDown) &&
            (configstatus > eos::common::ConfigStatus::kDrain)) {
          ++ndown;
        }
      }
    }

    if (ndown) {
      // We give one more minute to get things going
      std::this_thread::sleep_for(std::chrono::seconds(60));
      ndown = 0;
      {
        eos::common::RWMutexReadLock fs_rd_lock(mFsMutex);
        nfs = mFsMap.size();

        for (const auto& elem : mFsMap) {
          auto fs = elem.second;

          if (!fs) {
            eos_warning("msg=\"skip file system id without object in map\" "
                        "fsid=%lu", elem.first);
            continue;
          }

          eos::common::BootStatus bootstatus = fs->GetStatus();
          eos::common::ConfigStatus configstatus = fs->GetConfigStatus();

          if ((bootstatus == eos::common::BootStatus::kDown) &&
              (configstatus > eos::common::ConfigStatus::kDrain)) {
            ++ndown;
          }
        }
      }

      if (ndown == nfs) {
        // shutdown this daemon
        eos_static_alert("found %d/%d filesystems in <down> status - committing suicide !",
                         ndown, nfs);
        std::this_thread::sleep_for(std::chrono::seconds(10));
        kill(getpid(), SIGQUIT);
      }
    }

    std::this_thread::sleep_for(std::chrono::seconds(60));
  }
}

EOSFSTNAMESPACE_END
