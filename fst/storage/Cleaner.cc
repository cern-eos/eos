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

/*----------------------------------------------------------------------------*/
#include "fst/storage/Storage.hh"

EOSFSTNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
void
Storage::Cleaner ()
{
  eos_static_info("Start Cleaner ...");

  std::string nodeconfigqueue = "";

  const char* val = 0;
  // we have to wait that we know our node config queue
  while (!(val = eos::fst::Config::gConfig.FstNodeConfigQueue.c_str()))
  {
    XrdSysTimer sleeper;
    sleeper.Snooze(5);
    eos_static_info("Snoozing ...");
  }

  nodeconfigqueue = eos::fst::Config::gConfig.FstNodeConfigQueue.c_str();

  while (1)
  {
    eos_static_debug("Doing cleaning round ...");

    unsigned int nfs = 0;
    {
      eos::common::RWMutexReadLock lock(fsMutex);
      nfs = fileSystemsVector.size();
    }
    for (unsigned int i = 0; i < nfs; i++)
    {
      eos::common::RWMutexReadLock lock(fsMutex);
      if (i < fileSystemsVector.size())
      {

        if (fileSystemsVector[i]->GetStatus() == eos::common::FileSystem::kBooted)
          fileSystemsVector[i]->CleanTransactions();
      }
    }

    // go to sleep for a day since we allow a transaction to stay for 1 week
    XrdSysTimer sleeper;
    sleeper.Snooze(24 * 3600);
  }
}

EOSFSTNAMESPACE_END


