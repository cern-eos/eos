// ----------------------------------------------------------------------
// File: TransferQueue.hh
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

#ifndef __EOSFST_TRANSFERQUEUE__
#define __EOSFST_TRANSFERQUEUE__

/* ------------------------------------------------------------------------- */
#include "fst/Namespace.hh"
#include "common/TransferQueue.hh"
/* ------------------------------------------------------------------------- */
#include "Xrd/XrdScheduler.hh"
/* ------------------------------------------------------------------------- */
#include <string>
#include <deque>
#include <cstring>
#include <pthread.h>

/* ------------------------------------------------------------------------- */

EOSFSTNAMESPACE_BEGIN

class TransferQueue
{
private:
  //  std::deque <std::string> queue;
  eos::common::TransferQueue** mQueue;
  std::string mName;

  size_t nslots, bandwidth;

  size_t mJobsRunning;
  XrdSysMutex mJobsRunningMutex;
  XrdSysMutex mBandwidthMutex;
  XrdSysMutex mSlotsMutex;

  XrdSysCondVar mJobTerminateCondition;

public:

  TransferQueue (eos::common::TransferQueue** queue, const char* name, int slots = 2, int band = 100);
  ~TransferQueue ();

  eos::common::TransferQueue*
  GetQueue ()
  {
    return *mQueue;
  }

  const char*
  GetName ()
  {
    return mName.c_str();
  }

  size_t GetSlots ();
  void SetSlots (size_t slots);

  size_t GetBandwidth ();
  void SetBandwidth (size_t band);

  void
  IncRunning ()
  {
    XrdSysMutexHelper(mJobsRunningMutex);
    mJobsRunning++;
  }

  void
  DecRunning ()
  {
    XrdSysMutexHelper(mJobsRunningMutex);
    mJobsRunning--;
    // signal threads waiting for a job to finish
    mJobTerminateCondition.Signal();
  }

  size_t
  GetRunning ()
  {
    size_t nrun = 0;
    {
      XrdSysMutexHelper(mJobsRunningMutex);
      nrun = mJobsRunning;
    }
    return nrun;
  }

  size_t
  GetRunningAndQueued ()
  {
    return (GetRunning() + GetQueue()->Size());
  }

};

EOSFSTNAMESPACE_END
#endif

