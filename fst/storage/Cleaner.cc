// ----------------------------------------------------------------------
// File: Cleaner.cc
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
#include "fst/Config.hh"
#include "fst/storage/FileSystem.hh"

EOSFSTNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Cleaner thread
//------------------------------------------------------------------------------
void
Storage::Cleaner()
{
  eos_info("%s", "msg=\"start cleaner\"");
  // Wait that we know our node config queue
  std::string nodeconfigqueue =
    eos::fst::Config::gConfig.getFstNodeConfigQueue("Cleaner").c_str();

  while (true) {
    eos_notice("%s", "msg=\"cleaning transactions\"");
    std::string manager = eos::fst::Config::gConfig.GetManager();

    if (manager.empty()) {
      eos_err("%s", "msg=\"don't know the manager name\"");
    } else {
      eos::common::RWMutexReadLock fs_rd_lock(mFsMutex);

      for (const auto& elem : mFsMap) {
        auto fs = elem.second;

        if (fs->GetStatus() == eos::common::BootStatus::kBooted) {
          if (fs->SyncTransactions(manager.c_str())) {
            fs->CleanTransactions();
          }
        }
      }
    }

    // Sleep for a day since we allow a transaction to stay for 1 week
    std::this_thread::sleep_for(std::chrono::hours(24));
  }
}

EOSFSTNAMESPACE_END
