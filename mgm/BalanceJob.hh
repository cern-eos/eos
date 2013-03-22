// ----------------------------------------------------------------------
// File: BalanceJob.hh
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

#ifndef __EOSMGM_BALANCEJOB_HH__
#define __EOSMGM_BALANCEJOB_HH__
/*----------------------------------------------------------------------------*/
#include <pthread.h>
/*----------------------------------------------------------------------------*/
#include "common/FileSystem.hh"
#include "common/TransferQueue.hh"
#include "mgm/Namespace.hh"
#include "mgm/FsView.hh"
/*----------------------------------------------------------------------------*/
#include "semaphore.h"
#include <queue>
/*----------------------------------------------------------------------------*/


EOSMGMNAMESPACE_BEGIN

class FsGroup;

class BalanceJob
{
  // ---------------------------------------------------------------------------
  //! This class implements the balance procedure of a group
  // ---------------------------------------------------------------------------
private:
  FsGroup* mGroup;
  std::string mName;
  pthread_t thread;
  XrdSysMutex mThreadRunningLock;
  bool mThreadRunning;

  std::map<eos::common::FileSystem::fsid_t, std::set<unsigned long long > > SourceFidMap; // the sources to schedule
  std::set<unsigned long long > SourceFidSet; // the alls fids to schedule within a group
  std::map<eos::common::FileSystem::fsid_t, long long> SourceSizeMap;
  std::map<eos::common::FileSystem::fsid_t, long long> TargetSizeMap;
  std::map<eos::common::FileSystem::fsid_t, eos::common::TransferQueue* > TargetQueues;
  std::map<eos::common::FileSystem::fsid_t, std::set<unsigned long long > > TargetFidMap; // the scheduled targets

  static XrdSysMutex gSchedulingMutex; // serializes the scheduling part between groups to avoid overload 

public:

  BalanceJob (FsGroup* group);
  bool ReActivate ();

  static void* StaticThreadProc (void*);
  void* Balance (); // the function scheduling from the balance map into shared queues

  virtual ~BalanceJob ();
};

EOSMGMNAMESPACE_END

#endif
